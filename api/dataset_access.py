"""Per-dataset visibility and redistribution decisions for contributed place ids
(``whg:<dataset_id>:<src_id>``), and the grant that carries a positive
decision to the CRC gateway (place#319 follow-up).

An authority namespace (``osm:``, ``clio:``, …) has one registry row that says
whether WHG may re-serve its content (``redistributable``). Contributed data
has none of that: the ``whg`` registry row is an umbrella, and the facts that
decide whether one dataset's outline may leave the platform live on the
``Dataset`` row and around it —

* **visibility**: ``Dataset.public`` and the owner / co-owner / collaborator /
  staff circle ``Dataset.user_can_view`` already defines (the place#310 rule);
* **embargo**: the registry's ``whg:<dataset_id>`` row may be ``embargoed``
  (place#162's mechanism, admin-set), with an optional ``embargo_release_at``;
* **licence**: ``Dataset.license`` — recorded for new contributions and the
  backfilled cohort A of place#158, null for everything else.

Policy (fail closed; every branch is a refusal unless it is the one that
serves):

1. The id must be the canonical ``whg:<dataset_id>:<src_id>`` naming a dataset
   that exists. Anything else is **404 not found** — the same answer as an
   unknown place, so a private dataset's id is not confirmed to exist.
2. The user must pass ``user_can_view``, except that an embargoed dataset is
   treated as non-public whatever ``public`` says: only its insiders see it,
   and ``can_access_beta`` on its own is not enough. Beta is a feature flag,
   not a right to someone else's unpublished data; the owner / collaborator /
   staff circle is the one every other private-dataset surface uses, and the
   gateway index can be stale (a dataset made private after the last ingest is
   still in it), so the live row is what decides. Refused: **404**.
3. The dataset must carry a licence that permits redistribution of an
   adaptation: a known, non-custom (SPDX) licence with ``no_derivatives`` False.
   No licence recorded ⇒ **451 "source licence not determined"**; a custom
   licence (bespoke terms this code cannot evaluate: all-rights-reserved,
   academic-use, even public-domain-by-assertion) or an ND licence (the
   gateway may simplify the outline, and the Atlas re-uses it as a derived
   search constraint) ⇒ **451 "source not redistributable"**, naming the
   licence. NC licences are served: WHG is non-commercial and the terms travel
   in the attribution block.

The positive decision reaches the gateway as ``X-WHG-Geometry-Grant``, an
HMAC-SHA256 over ``"geometry|<place_id>|<expires>"`` keyed with
``settings.CRC_GATEWAY_API_KEY`` — the secret the gateway now verifies
(indexing ``gateway/geometry.py``). Bound to the id, short-lived, and
unmintable when the setting is empty, so no path serves contributed geometry
on an assumption.
"""
from __future__ import annotations

import hashlib
import hmac
import logging
import time
from dataclasses import dataclass
from typing import Optional

from django.conf import settings
from django.utils import timezone

logger = logging.getLogger(__name__)

WHG_NAMESPACE = "whg"
GRANT_HEADER = "X-WHG-Geometry-Grant"
GRANT_VERSION = "v1"
# Must stay under the gateway's GRANT_MAX_TTL_S (600): a grant is minted per
# request and used at once, so two minutes covers any proxy latency.
GRANT_TTL_S = 120

_ATTRIBUTION_LICENSE_FIELDS = (
    "license__spdx_id", "license__label", "license__url",
    "license__permits_commercial", "license__share_alike",
    "license__attribution_required", "license__no_derivatives",
    "license__custom",
)


@dataclass
class DatasetDecision:
    """The outcome of a contributed-id check: ``status`` 200 with ``dataset``
    and ``attribution`` set, or a refusal ``status`` (404 / 451) with the JSON
    ``body`` to answer with and nothing else."""
    status: int
    body: Optional[dict] = None
    dataset: object = None
    attribution: Optional[dict] = None

    @property
    def allowed(self) -> bool:
        return self.status == 200


def parse_contributed_id(pid: str) -> Optional[tuple[int, str]]:
    """``(dataset_pk, src_id)`` for a canonical ``whg:<dataset_id>:<src_id>``;
    None for anything else, including the pre-namespacing ``whg:<pk>`` form
    (which names no dataset) and a bare number. ``src_id`` may itself contain
    colons (the indexing side disambiguates duplicates as ``whg:20:20155:91040``)."""
    parts = (pid or "").strip().split(":", 2)
    if len(parts) != 3 or parts[0].lower() != WHG_NAMESPACE or not parts[1].isdigit() or not parts[2]:
        return None
    return int(parts[1]), parts[2]


def _not_found(pid: str) -> DatasetDecision:
    return DatasetDecision(404, {"error": "not found", "id": pid})


def is_dataset_embargoed(dataset_pk: int, now=None) -> bool:
    """True when the registry row for this dataset (``whg:<pk>``) is under an
    embargo that has not yet lifted. The row is admin-managed (place#162) and
    survives a re-push; a release date in the past counts as lifted, mirroring
    ``GazetteerRegistryEntryQuerySet.visible_to``'s lazy auto-release."""
    from api.models import GazetteerRegistryEntry
    row = (GazetteerRegistryEntry.objects
           .filter(id=f"{WHG_NAMESPACE}:{dataset_pk}", status="embargoed")
           .values("embargo_release_at").first())
    if row is None:
        return False
    release = row["embargo_release_at"]
    return release is None or release > (now or timezone.now())


def dataset_attribution(dataset) -> dict:
    """Flat, ``registry_attribution``-shaped attribution for a contributed
    dataset, so the Atlas portal and geometry client read one shape for
    authority and contributed sources alike. ``license__*`` may all be None —
    an unlicensed dataset is reported honestly as such, never as the WHG
    overlay (place#157)."""
    from datasets.models import Dataset
    row = (Dataset.objects.filter(pk=dataset.pk)
           .values("id", "label", "title", "creator", "webpage", "citation",
                   "rights_statement", "license_source", *_ATTRIBUTION_LICENSE_FIELDS)
           .first()) or {}
    return {
        "id": f"{WHG_NAMESPACE}:{dataset.pk}",
        "dataset": row.get("label"),
        "name": row.get("title") or dataset.title,
        "citation_text": row.get("citation") or "",
        "rights_holder": row.get("creator") or "",
        "source_url": row.get("webpage") or "",
        "rights_statement": row.get("rights_statement") or "",
        "license_source": row.get("license_source"),
        **{k: row.get(k) for k in _ATTRIBUTION_LICENSE_FIELDS},
        # Set by dataset_redistribution(); None until the licence has been judged.
        "redistributable": None,
    }


def dataset_visibility(pid: str, user) -> DatasetDecision:
    """Steps 1–2 of the policy: does this user get to know this place exists?
    404 for anything short of that; 200 with the dataset and its attribution
    otherwise. Used on its own by ``/atlas/place/`` (a record, which existing
    surfaces serve with its licence reported, licensed or not) and as the
    first half of ``contributed_geometry_decision``."""
    from datasets.models import Dataset
    parsed = parse_contributed_id(pid)
    if parsed is None:
        return _not_found(pid)
    dataset_pk, _src = parsed
    dataset = Dataset.objects.filter(pk=dataset_pk).first()
    if dataset is None:
        return _not_found(pid)
    if is_dataset_embargoed(dataset_pk):
        visible = dataset.user_has_private_access(user)
    else:
        visible = dataset.user_can_view(user)
    if not visible:
        return _not_found(pid)
    return DatasetDecision(200, dataset=dataset, attribution=dataset_attribution(dataset))


def dataset_redistribution(decision: DatasetDecision, pid: str) -> DatasetDecision:
    """Step 3: may the dataset's content leave the platform as geometry?
    Takes a positive ``dataset_visibility`` decision and returns it with
    ``attribution["redistributable"]`` set, or a 451."""
    if not decision.allowed:
        return decision
    dataset, attribution = decision.dataset, dict(decision.attribution or {})
    lic = dataset.license
    name = attribution.get("name") or f"dataset {dataset.pk}"
    if lic is None:
        attribution["redistributable"] = False
        return DatasetDecision(451, {
            "error": "source licence not determined",
            "detail": (f"No licence is recorded for {name}, so the terms of its geometry "
                       f"cannot be determined; it is withheld rather than served on an "
                       f"assumption."),
            "id": pid,
            "namespace": WHG_NAMESPACE,
            "source": _source_block(attribution),
        })
    if lic.custom or lic.no_derivatives is not False:
        attribution["redistributable"] = False
        why = ("bespoke terms this service cannot evaluate" if lic.custom
               else "a licence that does not permit adaptations, and a served outline may be simplified")
        return DatasetDecision(451, {
            "error": "source not redistributable",
            "detail": (f"{name} is published under {lic.spdx_id}: {why}. Its geometry is "
                       f"withheld; obtain the data from the dataset under its own terms."),
            "id": pid,
            "namespace": WHG_NAMESPACE,
            "source": _source_block(attribution),
        })
    attribution["redistributable"] = True
    return DatasetDecision(200, dataset=dataset, attribution=attribution)


def _source_block(attribution: dict) -> dict:
    return {
        "name": attribution.get("name"),
        "rights_holder": attribution.get("rights_holder"),
        "source_url": attribution.get("source_url"),
        "license": attribution.get("license__spdx_id") or attribution.get("license__label"),
    }


def contributed_geometry_decision(pid: str, user) -> DatasetDecision:
    """The whole policy for one contributed id: visibility, then licence."""
    return dataset_redistribution(dataset_visibility(pid, user), pid)


# ---------------------------------------------------------------------------
# The grant (must match indexing gateway/geometry.py exactly)
# ---------------------------------------------------------------------------

def grant_message(place_id: str, expires: int) -> bytes:
    return f"geometry|{place_id}|{int(expires)}".encode("utf-8")


def sign_geometry_grant(place_id: str, secret: Optional[str] = None,
                        ttl: int = GRANT_TTL_S, now: Optional[float] = None) -> Optional[str]:
    """``v1.<expires>.<hex>`` for ``place_id``, or None when no shared secret is
    configured — in which case the caller must not ask the gateway at all,
    since it would only be refused, and must say why rather than "not found"."""
    secret = secret if secret is not None else (getattr(settings, "CRC_GATEWAY_API_KEY", "") or "")
    if not secret:
        return None
    expires = int(time.time() if now is None else now) + int(ttl)
    sig = hmac.new(secret.encode("utf-8"), grant_message(place_id, expires), hashlib.sha256).hexdigest()
    return f"{GRANT_VERSION}.{expires}.{sig}"
