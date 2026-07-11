"""Re-ingest a non-WHG gazetteer via the Pitt CRC gateway.

The Django admin's "Re-ingest" action calls into this module to enqueue
fresh-source-data ingestions of an authority (e.g. a refreshed GeoNames
or OSM dump). The actual work runs as a Slurm job on Pitt CRC; the
gateway relays the request to a CRC login node which invokes a generic
re-ingest script with the gazetteer's namespace as a parameter.

Authentication
==============

The Pitt firewall whitelists the DigitalOcean app server's IP so no
shared-secret token is required for the reingest endpoint. (The
existing inventory push at ``POST /api/registry/inventory`` is the
inverse direction — Pitt → DO — and uses DRF Bearer auth there because
the request originates outside DO's network.)


Endpoint contract (Pitt-side, to be implemented)
================================================

``POST {CRC_GATEWAY_URL}/api/registry/reingest``

  Request body::

      {"namespace": "osm"}

  ``namespace`` is the registry id of the gazetteer to re-ingest. The
  gateway hands this to the generic re-ingest script on the login node;
  what that script actually runs (``authorities/<ns>-places.py`` plus,
  for boundary-bearing namespaces, the boundary pass and tileset
  rebuild) is the gateway's responsibility, not Django's.

  Success response (HTTP 202)::

      {"job_id": "12345.cluster", "status": "queued"}

  ``job_id`` is the Slurm job id (or any opaque string the gateway
  assigns); Django stores it on the registry row for subsequent polls.

  Error responses:
    * ``409 Conflict`` if a job for the same namespace is already
      queued or running. Body: ``{"error": "...", "job_id": "..."}``
      so Django can adopt the existing job.
    * ``400 Bad Request`` for unknown ``namespace``.

``GET {CRC_GATEWAY_URL}/api/registry/reingest/<job_id>``

  Response body::

      {"job_id": "12345.cluster",
       "status": "queued|running|completed|failed",
       "started_at":  "2026-05-04T16:30:00Z",     // optional
       "finished_at": "2026-05-04T17:42:11Z",     // optional
       "message":     "..."                       // optional, e.g. error text}

The response shape is intentionally narrow — Django doesn't render
log output, so the gateway can summarise progress in ``message`` if it
wants to.


Concurrency
===========

Django guards against accidentally requesting a second re-ingest while
one is still running by checking ``reingest_status`` before issuing.
The gateway is the second line of defence (409 above). The two-tier
guard is intentional: a stale ``running`` row in Django (e.g. after a
process crash that lost the polling loop) shouldn't permanently block
re-ingestion — staff can retry from admin and the gateway either
adopts the in-flight job (returning the same ``job_id``) or starts a
new one.
"""

from __future__ import annotations

import logging
from typing import Any

import requests
from django.conf import settings
from django.utils import timezone

from .models import GazetteerRegistryEntry

logger = logging.getLogger(__name__)


REINGEST_PATH = "/api/registry/reingest"


class ReingestError(RuntimeError):
    """Raised when the gateway can't accept a re-ingest request."""


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _gateway_base() -> str:
    url = getattr(settings, "CRC_GATEWAY_URL", "")
    if not url:
        raise ReingestError("CRC_GATEWAY_URL is not configured")
    return url.rstrip("/")


def _timeout_request() -> int:
    return getattr(settings, "CRC_GATEWAY_TIMEOUT", 10)


def _timeout_poll() -> int:
    # Polling is a cheap GET, but we keep a separate setting so admins
    # can tune the two independently.
    return getattr(settings, "CRC_GATEWAY_POLL_TIMEOUT", 5)


def _apply_response(entry: GazetteerRegistryEntry, payload: dict[str, Any]) -> None:
    """Copy gateway-reported fields onto the registry row."""
    status_value = payload.get("status")
    if status_value in {"idle", "queued", "running", "completed", "failed"}:
        entry.reingest_status = status_value
    if payload.get("job_id"):
        entry.reingest_job_id = str(payload["job_id"])
    if payload.get("started_at"):
        entry.reingest_started_at = _parse_datetime(payload["started_at"])
    if payload.get("finished_at"):
        entry.reingest_finished_at = _parse_datetime(payload["finished_at"])
    if "message" in payload:
        entry.reingest_message = payload.get("message") or None


def _parse_datetime(value):
    """Best-effort ISO-8601 parse — gateway should return timezone-aware
    strings, but tolerate naïve ones by treating them as UTC."""
    from datetime import datetime
    if isinstance(value, datetime):
        return value
    try:
        # fromisoformat handles "2026-05-04T16:30:00+00:00" and
        # "2026-05-04T16:30:00" (naïve). It does NOT handle the trailing
        # "Z" suffix that the contract suggests, so swap that first.
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def request_reingest(entry: GazetteerRegistryEntry) -> GazetteerRegistryEntry:
    """Ask the gateway to enqueue a re-ingest for ``entry``.

    On success the entry's ``reingest_status`` is set to ``queued`` (or
    whatever status the gateway reports) with the returned ``job_id``
    and ``started_at = now()``. On gateway 409 (already-running), the
    existing job_id is adopted. On any other failure, ``reingest_status``
    becomes ``failed`` and the exception text is stored in
    ``reingest_message``.

    Returns the updated ``entry`` (already saved).
    """
    url = _gateway_base() + REINGEST_PATH
    body = {"namespace": entry.id}

    entry.reingest_status = "queued"
    entry.reingest_started_at = timezone.now()
    entry.reingest_finished_at = None
    entry.reingest_message = None

    try:
        resp = requests.post(url, json=body, timeout=_timeout_request())
    except requests.RequestException as exc:
        logger.warning("reingest %s: request failed: %s", entry.id, exc)
        entry.reingest_status = "failed"
        entry.reingest_message = f"gateway request failed: {exc}"
        entry.save()
        raise ReingestError(str(exc)) from exc

    # 409 Conflict — adopt the existing job rather than treating as error.
    if resp.status_code == 409:
        try:
            payload = resp.json()
        except ValueError:
            payload = {}
        if payload.get("job_id"):
            _apply_response(entry, {**payload, "status": "running"})
            entry.save()
            return entry
        entry.reingest_status = "failed"
        entry.reingest_message = "gateway returned 409 with no job_id"
        entry.save()
        raise ReingestError(entry.reingest_message)

    if not resp.ok:
        entry.reingest_status = "failed"
        entry.reingest_message = f"gateway HTTP {resp.status_code}: {resp.text[:200]}"
        entry.save()
        raise ReingestError(entry.reingest_message)

    try:
        payload = resp.json()
    except ValueError as exc:
        entry.reingest_status = "failed"
        entry.reingest_message = f"gateway returned non-JSON: {exc}"
        entry.save()
        raise ReingestError(entry.reingest_message) from exc

    _apply_response(entry, payload)
    entry.save()
    logger.info("reingest %s queued: job_id=%s", entry.id, entry.reingest_job_id)
    return entry


def poll_reingest(entry: GazetteerRegistryEntry) -> GazetteerRegistryEntry:
    """Refresh ``entry``'s reingest fields from the gateway.

    No-op if the entry has no ``reingest_job_id`` or its status is
    already terminal (``completed`` / ``failed`` / ``idle``). Otherwise
    fetches the latest status and saves.
    """
    if not entry.reingest_job_id:
        return entry
    if entry.reingest_status in {"idle", "completed", "failed"}:
        return entry

    url = f"{_gateway_base()}{REINGEST_PATH}/{entry.reingest_job_id}"
    try:
        resp = requests.get(url, timeout=_timeout_poll())
    except requests.RequestException as exc:
        logger.warning("reingest poll %s: request failed: %s", entry.id, exc)
        return entry  # leave status as-is; transient failures shouldn't trash it

    if not resp.ok:
        # 404 from the gateway = the job is no longer tracked. Treat as
        # failed so staff can retry — leaving it stuck on "running"
        # would block all future re-ingests for this gazetteer.
        if resp.status_code == 404:
            entry.reingest_status = "failed"
            entry.reingest_message = "gateway no longer tracks this job"
            entry.reingest_finished_at = timezone.now()
            entry.save()
        return entry

    try:
        payload = resp.json()
    except ValueError:
        return entry

    _apply_response(entry, payload)
    entry.save()
    return entry
