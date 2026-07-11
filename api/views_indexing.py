# api/views_indexing.py
"""API views added for the WHG indexing rebuild.

See ``../indexing/developer/plan-ingestionRebuild.execution.md``:

* ``GazetteerInventoryView`` — Batch 11 inventory push target.
* ``RetentionNotifyView``   — Batch 14a retention sweep notifier.
* ``ContributorAttestationView`` — live writes from the contribution UI;
  forwards to the gateway via ``api/signals.py`` so the Pitt SQLite stays
  in sync.

All views accept JSON, are CSRF-exempt (token auth via the existing
``AuthenticatedAPIView``), and are deliberately permissive about
incidental schema drift — the indexing rebuild is the canonical caller and
will adjust if Django shapes change.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from django.http import JsonResponse
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from rest_framework import status
from rest_framework.response import Response

from api.authentication import AuthenticatedAPIView
from api.models import ContributorAttestation, GazetteerRegistryEntry

logger = logging.getLogger("indexing")


def _parse_json_body(request) -> dict | None:
    """Return a parsed JSON dict from the request body, or ``None``."""
    if not request.body:
        return None
    try:
        body = json.loads(request.body)
    except json.JSONDecodeError:
        return None
    return body if isinstance(body, dict) else None


# ---------------------------------------------------------------------------
# Batch 11 — gazetteer inventory push
# ---------------------------------------------------------------------------


@method_decorator(csrf_exempt, name="dispatch")
class GazetteerInventoryView(AuthenticatedAPIView):
    """Idempotent upsert of the per-gazetteer registry.

    Body shape (matches ``processing/push_gazetteer_inventory.py``)::

        {
          "gazetteers": [
            {
              "id": "gn",
              "name": "GeoNames",
              "description": "...",
              "namespace": "gn",
              "class": "authority",
              "owner_user_id": null,
              "record_count": 13000000,
              "status": "published",
              "h3_coverage": "global",
              "temporal_extent": [-2000, 2025],

              // Phase 4 attribution fields (all optional; the indexing
              // AUTHORITIES config is the source of truth):
              "citation_text": "GeoNames geographical database. ...",
              "license_spdx":  "CC-BY-4.0",      // resolved to a License FK
              "license_url":   "https://...",    // optional deed override
              "rights_holder": "Unxos GmbH",
              "source_url":    "https://www.geonames.org/",
              "contributors":  [{"name": "...", "role": "...", "orcid": "..."}]
            },
            ...
          ]
        }

    Returns ``{"upserted": <int>, "errors": [...]}``.

    Unknown ``license_spdx`` codes are logged and skipped (the row's other
    fields still upsert). Fields absent from a payload are left untouched, so
    a not-yet-upgraded push is fully backward compatible.
    """

    http_method_names = ["post", "put", "options"]

    def _upsert_one(self, entry: dict[str, Any]) -> tuple[bool, str | None]:
        try:
            entry_id = str(entry["id"])
            namespace = str(entry["namespace"])
            entry_class = str(entry.get("class") or "authority")
        except KeyError as exc:
            return False, f"missing field: {exc}"

        # Inventory-derived fields only. Curatorial fields (``core``,
        # ``no_explore``, ``region_source``, ``gazetteer_type``) are
        # admin-managed via api/admin.py::GazetteerRegistryEntryAdmin and
        # MUST NOT be added here — ``update_or_create(defaults=…)`` would
        # silently reset them on every push, blowing away staff curation.
        # Re-ingest tracking fields are likewise excluded; they're owned
        # by the admin "Re-ingest" action and the gateway poll loop.
        defaults = {
            "name": str(entry.get("name") or entry_id),
            "description": entry.get("description"),
            "namespace": namespace,
            "entry_class": entry_class,
            "record_count": int(entry.get("record_count") or 0),
            "status": str(entry.get("status") or "published"),
            "h3_coverage": entry.get("h3_coverage", []),
            "temporal_extent": entry.get("temporal_extent", []),
        }
        owner_user_id = entry.get("owner_user_id")
        if owner_user_id is not None:
            defaults["owner_id"] = owner_user_id

        # Attribution / licence / rights (citations design Phase 4). All
        # optional — only fields actually present in the payload are written,
        # so a push that omits them leaves any prior values intact. These are
        # push-managed: the indexing ``AUTHORITIES`` config is the SoT.
        for key in ("citation_text", "license_url", "rights_holder",
                    "source_url"):
            if key in entry:
                defaults[key] = entry.get(key)
        if "contributors" in entry:
            defaults["contributors_csl"] = entry.get("contributors") or []
        # Resolve the SPDX code to a seeded ``License`` row. Unknown codes are
        # logged and skipped (license left untouched) rather than failing the
        # whole entry — flexibility over strictness (design decision 3).
        license_spdx = entry.get("license_spdx")
        if license_spdx:
            from licensing.models import License
            lic = License.objects.filter(spdx_id=license_spdx).first()
            if lic is not None:
                defaults["license_id"] = lic.pk
            else:
                logger.warning(
                    "GazetteerInventoryView: unknown license_spdx %r for %s "
                    "— leaving license unset", license_spdx, entry_id,
                )

        try:
            GazetteerRegistryEntry.objects.update_or_create(
                id=entry_id, defaults=defaults,
            )
            return True, None
        except Exception as exc:  # pragma: no cover — defensive
            return False, f"{type(exc).__name__}: {exc}"

    def post(self, request, *args, **kwargs):
        return self._handle(request)

    def put(self, request, *args, **kwargs):
        return self._handle(request)

    def _handle(self, request):
        body = _parse_json_body(request)
        if body is None:
            return Response(
                {"error": "request body must be JSON object"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        gazetteers = body.get("gazetteers")
        if not isinstance(gazetteers, list):
            return Response(
                {"error": "expected 'gazetteers' list"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        upserted = 0
        errors: list[dict[str, Any]] = []
        for entry in gazetteers:
            if not isinstance(entry, dict):
                errors.append({"entry": str(entry)[:120], "error": "not a dict"})
                continue
            ok, err = self._upsert_one(entry)
            if ok:
                upserted += 1
            else:
                errors.append({"id": entry.get("id"), "error": err})

        logger.info(
            "GazetteerInventoryView: upserted=%d errors=%d total=%d",
            upserted, len(errors), len(gazetteers),
        )
        return Response({"upserted": upserted, "errors": errors})


# ---------------------------------------------------------------------------
# Batch 14a — retention sweep notifier
# ---------------------------------------------------------------------------


@method_decorator(csrf_exempt, name="dispatch")
class RetentionNotifyView(AuthenticatedAPIView):
    """Receive 11-month-boundary notification batches from
    ``processing/retention_sweep.py``.

    Body shape::

        {
          "boundary": "11_months",
          "datasets": [
            {"dataset_id": "whg:1234",
             "owner_user_id": 42,
             "submission_date": "2025-05-27T00:00:00+00:00",
             "last_modified":   "2025-05-27T00:00:00+00:00",
             "record_count":    137},
            ...
          ]
        }

    Records the notification batch in the application log and (when
    ``settings.WHG_RETENTION_DISPATCH_FN`` is configured) hands the
    payload off to the configured dispatch callable for actual email /
    in-platform notification — so the email path can be added later
    without re-deploying the ingestion side.
    """

    http_method_names = ["post", "options"]

    def post(self, request, *args, **kwargs):
        body = _parse_json_body(request)
        if body is None:
            return Response(
                {"error": "request body must be JSON object"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        boundary = body.get("boundary")
        datasets = body.get("datasets")
        if not isinstance(datasets, list):
            return Response(
                {"error": "expected 'datasets' list"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        logger.info(
            "RetentionNotifyView: boundary=%s datasets=%d",
            boundary, len(datasets),
        )
        for ds in datasets:
            if not isinstance(ds, dict):
                continue
            logger.info(
                "  retention notify: %s owner=%s last_modified=%s records=%s",
                ds.get("dataset_id"), ds.get("owner_user_id"),
                ds.get("last_modified"), ds.get("record_count"),
            )

        # Optional dispatch hook — set
        # ``settings.WHG_RETENTION_DISPATCH_FN = "myapp.notify.send_retention"``
        # to wire the actual email path. Called as ``fn(boundary, datasets)``;
        # exceptions are logged and don't fail the request.
        from django.conf import settings as _s
        dispatch_path = getattr(_s, "WHG_RETENTION_DISPATCH_FN", None)
        if dispatch_path:
            try:
                module_path, _, attr = dispatch_path.rpartition(".")
                fn = getattr(__import__(module_path, fromlist=[attr]), attr)
                fn(boundary, datasets)
            except Exception as exc:
                logger.warning("retention dispatch failed: %s", exc)

        return Response({
            "received": len(datasets),
            "boundary": boundary,
        })


# ---------------------------------------------------------------------------
# Live contributor attestation API
# ---------------------------------------------------------------------------


@method_decorator(csrf_exempt, name="dispatch")
class ContributorAttestationView(AuthenticatedAPIView):
    """Create / list a contributor's hard-link assertions.

    POST body::

        {
          "dataset_id":     "whg:1234",
          "place_a":        "wd:Q90",
          "place_b":        "gn:2988507",
          "relation_type":  "sameAs",
          "justification":  "...",
          "status":         "active"      // optional, default 'active'
        }

    The post_save signal on ``ContributorAttestation`` forwards the new
    row to the gateway's ``/api/links`` endpoint (best-effort) so the
    Pitt SQLite hard-link overlay stays in sync without waiting for the
    next batch run. ``DELETE`` revokes by id and triggers the
    corresponding gateway delete.
    """

    http_method_names = ["get", "post", "delete", "options"]

    def get(self, request, *args, **kwargs):
        if not request.user.is_authenticated:
            return Response({"error": "auth required"},
                            status=status.HTTP_401_UNAUTHORIZED)
        rows = list(
            ContributorAttestation.objects
            .filter(user=request.user)
            .order_by("-modified_at")
            .values("id", "dataset_id", "place_a", "place_b",
                    "relation_type", "status", "asserted_at",
                    "modified_at", "justification", "legacy_v3_2")
        )
        return Response({"result": rows, "count": len(rows)})

    def post(self, request, *args, **kwargs):
        if not request.user.is_authenticated:
            return Response({"error": "auth required"},
                            status=status.HTTP_401_UNAUTHORIZED)
        body = _parse_json_body(request)
        if body is None:
            return Response({"error": "JSON body required"},
                            status=status.HTTP_400_BAD_REQUEST)
        try:
            row = ContributorAttestation.objects.create(
                user=request.user,
                dataset_id=str(body["dataset_id"]),
                place_a=str(body["place_a"]),
                place_b=str(body["place_b"]),
                relation_type=str(body["relation_type"]),
                justification=body.get("justification"),
                status=str(body.get("status") or "active"),
                legacy_v3_2=bool(body.get("legacy_v3_2", False)),
            )
        except KeyError as exc:
            return Response(
                {"error": f"missing field: {exc}"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        except Exception as exc:
            return Response(
                {"error": f"{type(exc).__name__}: {exc}"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        return Response({
            "id": row.id,
            "dataset_id": row.dataset_id,
            "place_a": row.place_a,
            "place_b": row.place_b,
            "relation_type": row.relation_type,
            "status": row.status,
            "asserted_at": row.asserted_at.isoformat(),
        }, status=status.HTTP_201_CREATED)

    def delete(self, request, *args, **kwargs):
        if not request.user.is_authenticated:
            return Response({"error": "auth required"},
                            status=status.HTTP_401_UNAUTHORIZED)
        body = _parse_json_body(request) or {}
        row_id = body.get("id") or request.GET.get("id")
        if not row_id:
            return Response({"error": "missing 'id'"},
                            status=status.HTTP_400_BAD_REQUEST)
        try:
            row = ContributorAttestation.objects.get(id=row_id, user=request.user)
        except ContributorAttestation.DoesNotExist:
            return Response({"error": "not found"},
                            status=status.HTTP_404_NOT_FOUND)
        row.delete()
        return Response({"deleted": row_id})
