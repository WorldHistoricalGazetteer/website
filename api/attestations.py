"""Rules for writing and reading a ContributorAttestation (place#334, Phases 0-1).

``POST /api/links`` used to take every field from the client: any place pair, any ``status``,
any ``legacy_v3_2``, with no check that the caller may see either place, and ``DELETE``
removed the row outright. Active rows are forwarded to the gateway's live hard-link overlay,
so a single request could assert a link between datasets the caller cannot see, or forge a
migrated-from-v3.2 row. The rules below are the single place those writes are decided.

Policy (fail closed):

* ``relation_type`` must be one of the model's choices.
* Both place ids must be ``<namespace>:<id>``, differ, and be visible to the caller. A
  contributed id (``whg:<dataset>:<src>``) must name a visible dataset and an existing record
  (``api.dataset_access.dataset_visibility``); any other namespace must have a registry row
  ``visible_to`` the caller. A refusal on either side is the same 404 whichever side it was,
  so a private id is not confirmed to exist.
* ``dataset_id`` is derived, never read from the client: the dataset of the first contributed
  side (blank when neither side is a contributed record).
* ``status`` and ``legacy_v3_2`` are never read from the client. A row whose contributed side
  belongs to a dataset that is private or embargoed is ``pending``: never forwarded to the
  gateway, visible only to its author and that dataset's circle, so the overlay never learns
  the id of an unpublished record. Publication flips it to ``active`` (Phase 3). Any other row
  is ``active``. Legacy rows come only from the governed migration (Phase 5).
* At most ``CONTRIBUTOR_ATTESTATION_DAILY_CAP`` new or reactivated rows per user per 24 h.
* A row is never hard-deleted by a request: ``withdraw`` sets ``status='withdrawn'``, which the
  ``post_save`` handler turns into a gateway revocation, and the row is kept as the record.
  Every change is also appended to ``AttestationEvent``.
"""
import re
from datetime import timedelta

from django.conf import settings
from django.db.models import Q
from django.utils import timezone

from api.dataset_access import (dataset_visibility, is_dataset_embargoed,
                                parse_contributed_id, WHG_NAMESPACE)

RELATIONS = {"sameAs", "exactMatch", "closeMatch", "distinct"}
AGGREGATING = {"sameAs", "exactMatch", "closeMatch"}
_PLACE_ID = re.compile(r"^([a-z][a-z0-9_]{0,15}):(\S{1,110})$")
DEFAULT_DAILY_CAP = 200
CONCORDANCE_VIEWER_CLASSES = ("beta", "signed_in", "anonymous_open", "anonymous")


class AttestationRefused(Exception):
    """A write the rules above refuse; ``status`` is the HTTP status to answer with."""

    def __init__(self, status, message):
        super().__init__(message)
        self.status = status
        self.message = message


def _not_found():
    return AttestationRefused(404, "not found")


def _namespace_visible(namespace, user):
    from api.models import GazetteerRegistryEntry
    return GazetteerRegistryEntry.objects.visible_to(user).filter(id=namespace).exists()


def _place_pk(dataset_pk, src_id):
    from places.models import Place
    return (Place.objects.filter(dataset__id=dataset_pk, src_id=src_id)
            .values_list("id", flat=True).first())


def _place_ref(place_pk):
    """``(dataset pk, src_id)`` of a Place by its pk, or None."""
    from places.models import Place
    return (Place.objects.filter(pk=place_pk).values_list("dataset__id", "src_id").first())


def _place_side(pid, user):
    """Check one side and describe it: ``{"pid", "dataset_pk", "place_pk", "public"}``.
    ``pid`` is returned canonical: a record may be named by its Django pk (``whg:<pk>``, as
    the Record page does) or by its gateway id (``whg:<dataset>:<src>``); both become the
    gateway form. ``dataset_pk`` / ``place_pk`` are None for an authority id; ``public`` is
    False when the side belongs to a dataset that is private or embargoed."""
    m = _PLACE_ID.match(pid)
    if m is None:
        raise AttestationRefused(400, f"malformed place id: {pid[:40]!r}")
    if m.group(1) == WHG_NAMESPACE and m.group(2).isdigit():
        ref = _place_ref(int(m.group(2)))
        if ref is None:
            raise _not_found()
        pid = f"{WHG_NAMESPACE}:{ref[0]}:{ref[1]}"
        m = _PLACE_ID.match(pid)
    if m.group(1) != WHG_NAMESPACE:
        if not _namespace_visible(m.group(1), user):
            raise _not_found()
        return {"pid": pid, "dataset_pk": None, "place_pk": None, "public": True}
    parsed = parse_contributed_id(pid)
    if parsed is None:
        raise AttestationRefused(400, f"malformed place id: {pid[:40]!r}")
    decision = dataset_visibility(pid, user)
    if not decision.allowed:
        raise _not_found()
    dataset_pk, src_id = parsed
    place_pk = _place_pk(dataset_pk, src_id)
    if place_pk is None:
        raise _not_found()
    public = bool(decision.dataset.public) and not is_dataset_embargoed(dataset_pk)
    return {"pid": pid, "dataset_pk": dataset_pk, "place_pk": place_pk, "public": public}


def can_attest(user, subject_id, target_id):
    """True when ``user`` may link these two records now: both visible to them, ids well
    formed, and under the daily cap. The rules ``create`` enforces, asked without writing."""
    try:
        _place_side(subject_id, user)
        _place_side(target_id, user)
        _check_cap(user)
    except AttestationRefused:
        return False
    return True


def validate(user, body):
    """The cleaned fields of a create request, or ``AttestationRefused``; ``_sides`` carries
    the two checked place descriptions (``_place_side``)."""
    try:
        place_a, place_b = str(body["place_a"]), str(body["place_b"])
        relation_type = str(body["relation_type"])
    except KeyError as exc:
        raise AttestationRefused(400, f"missing field: {exc}")
    if relation_type not in RELATIONS:
        raise AttestationRefused(400, f"relation_type must be one of {sorted(RELATIONS)}")
    if place_a == place_b:
        raise AttestationRefused(400, "place_a and place_b must differ")
    justification = body.get("justification")
    if justification is not None and not isinstance(justification, str):
        raise AttestationRefused(400, "justification must be text")
    certainty = body.get("certainty") or ""
    if certainty not in ("", "sure", "probable", "unsure"):
        raise AttestationRefused(400, "certainty must be sure, probable or unsure")
    # Both sides are checked before either answer is given, so the refusal does not
    # reveal which side was the hidden one.
    sides = [_place_side(place_a, user), _place_side(place_b, user)]
    place_a, place_b = sides[0]["pid"], sides[1]["pid"]
    if place_a == place_b:
        raise AttestationRefused(400, "place_a and place_b must differ")
    first_ds = next((x["dataset_pk"] for x in sides if x["dataset_pk"] is not None), None)
    return {
        "dataset_id": f"{WHG_NAMESPACE}:{first_ds}" if first_ds is not None else "",
        "place_a": place_a,
        "place_b": place_b,
        "relation_type": relation_type,
        "certainty": certainty,
        "justification": (justification or "").strip()[:2000] or None,
        "_sides": sides,
    }


def _check_cap(user):
    from api.models import ContributorAttestation
    cap = getattr(settings, "CONTRIBUTOR_ATTESTATION_DAILY_CAP", DEFAULT_DAILY_CAP)
    since = timezone.now() - timedelta(hours=24)
    if ContributorAttestation.objects.filter(user=user, asserted_at__gte=since).count() >= cap:
        raise AttestationRefused(429, f"daily limit of {cap} links reached")


def _event(row, user, action, **detail):
    from api.models import AttestationEvent
    AttestationEvent.objects.create(attestation=row, user=user, action=action, detail=detail)


def create(user, body, candidate=None, evidence=None):
    """``(row, created)``. An existing active row for the same edge is returned unchanged;
    a withdrawn / rejected / superseded one is reactivated (the unique constraint allows one
    row per edge and user).

    ``candidate`` (a ``datasets.MatchCandidate``) and ``evidence`` are set by the contributor
    feed (place#338), never read from a request body: the row is then ``suggested_by='candidate'``
    and keeps the frozen evidence the person saw. Every rule above applies unchanged."""
    from api.models import ContributorAttestation
    clean = validate(user, body)
    if candidate is not None:
        clean.update(candidate=candidate, suggested_by="candidate", evidence=dict(evidence or {}))
    sides = clean.pop("_sides")
    status = "active" if all(x["public"] for x in sides) else "pending"
    # place_a / place_b are stored in canonical order, so the stored pks follow the sort.
    ordered = sorted(sides, key=lambda x: x["pid"])
    a, b = ordered[0]["pid"], ordered[1]["pid"]
    pks = {"place_a_pk": ordered[0]["place_pk"], "place_b_pk": ordered[1]["place_pk"]}
    clean["place_a"], clean["place_b"] = a, b
    existing = ContributorAttestation.objects.filter(
        user=user, place_a=a, place_b=b, relation_type=clean["relation_type"]).first()
    if existing is not None and existing.status in ("active", "pending"):
        return existing, False
    _check_cap(user)
    if existing is not None:
        existing.status = status
        existing.withdrawn_at = None
        existing.withdrawn_by = None
        existing.dataset_id = clean["dataset_id"]
        existing.justification = clean["justification"]
        existing.certainty = clean["certainty"]
        existing.asserted_at = timezone.now()
        for k in ("candidate", "suggested_by", "evidence"):
            if k in clean:
                setattr(existing, k, clean[k])
        for k, v in pks.items():
            setattr(existing, k, v)
        existing.save()
        _event(existing, user, "reactivated", status=status)
        return existing, True
    clean.setdefault("suggested_by", "manual")
    row = ContributorAttestation(user=user, status=status, legacy_v3_2=False, **clean, **pks)
    row.save()
    _event(row, user, "created", status=status)
    return row, True


def withdraw(row, by=None):
    """Withdraw one attestation: kept as a record, revoked from the gateway by the
    ``post_save`` handler. Idempotent."""
    if row.status == "withdrawn":
        return row
    row.status = "withdrawn"
    row.withdrawn_at = timezone.now()
    row.withdrawn_by = by
    row.save()
    _event(row, by, "withdrawn")
    return row


def withdraw_for_dataset(dataset_pk):
    """Withdraw every attestation made for, or touching a place of, a dataset that is going
    away. ``dataset_id`` is a CharField and not a foreign key, so deleting the dataset does
    not reach these rows and they would stay active in the gateway overlay. Rows are saved one
    at a time so each revocation is forwarded. Returns the number withdrawn."""
    from api.models import ContributorAttestation
    prefix = f"{WHG_NAMESPACE}:{int(dataset_pk)}"
    rows = (ContributorAttestation.objects
            .filter(Q(dataset_id=prefix) | Q(place_a__startswith=prefix + ":")
                    | Q(place_b__startswith=prefix + ":"))
            .exclude(status="withdrawn"))
    n = 0
    for row in rows:
        withdraw(row)
        n += 1
    return n


# ---------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------

def record_ids(place):
    """The ids an attestation may use for a WHG record: its gateway id and its Django pk
    form (``whg:<pk>``, still used by older rows)."""
    ids = {f"{WHG_NAMESPACE}:{place.id}"}
    # Place.dataset is a FK to Dataset.label, so ``place.dataset_id`` is the label; the
    # gateway id carries the dataset's pk.
    if place.src_id and place.dataset_id:
        ids.add(f"{WHG_NAMESPACE}:{place.dataset.pk}:{place.src_id}")
    return ids


def attester(user):
    """How an attester is shown: name and ORCiD, nothing else."""
    if user is None:
        return None
    return {"name": getattr(user, "name", None) or getattr(user, "username", ""),
            "orcid": getattr(user, "orcid", None) or None}


def public_rows_about(ids):
    """Active rows naming any of ``ids`` (viewer-independent: both sides are public)."""
    from api.models import ContributorAttestation
    ids = sorted(ids)
    return (ContributorAttestation.objects
            .filter(status="active").filter(Q(place_a__in=ids) | Q(place_b__in=ids))
            .select_related("user").order_by("id"))


def pending_rows_visible_to(ids, user):
    """Pending rows naming any of ``ids`` that ``user`` may see: their own, plus those whose
    contributed side belongs to a dataset in the user's circle."""
    from api.models import ContributorAttestation
    from datasets.models import Dataset
    if not getattr(user, "is_authenticated", False):
        return []
    ids = sorted(ids)
    rows = list(ContributorAttestation.objects
                .filter(status="pending").filter(Q(place_a__in=ids) | Q(place_b__in=ids))
                .select_related("user").order_by("id"))
    out, circle = [], {}
    for r in rows:
        if r.user_id == user.id:
            out.append(r)
            continue
        ds_pks = {parse_contributed_id(p)[0] for p in (r.place_a, r.place_b) if parse_contributed_id(p)}
        for pk in ds_pks:
            if pk not in circle:
                ds = Dataset.objects.filter(pk=pk).first()
                circle[pk] = bool(ds and ds.user_has_private_access(user))
        if any(circle[pk] for pk in ds_pks):
            out.append(r)
    return out


def row_dict(row, viewer=None):
    """The API / Concordance shape of one attestation."""
    return {
        "id": row.id,
        "place_a": row.place_a,
        "place_b": row.place_b,
        "relation_type": row.relation_type,
        "status": row.status,
        "asserted_at": row.asserted_at.isoformat() if row.asserted_at else None,
        "justification": row.justification,
        "certainty": row.certainty or None,
        # So a client holding the Django-pk form (``whg:<pk>``) can match a row stored under
        # the gateway form.
        "place_a_pk": row.place_a_pk,
        "place_b_pk": row.place_b_pk,
        "mine": bool(viewer is not None and getattr(viewer, "is_authenticated", False)
                     and row.user_id == viewer.id),
        "attested_by": attester(row.user),
    }


def rows_about(user, pid):
    """What ``GET /api/links?about=<id>`` returns: active rows plus the pending ones the
    caller may see, for one place id. A contributed id the caller cannot see answers like an
    unknown one (empty), so the endpoint confirms nothing."""
    m = _PLACE_ID.match(pid or "")
    if m is None:
        raise AttestationRefused(400, "about must be a place id like 'wd:Q90'")
    try:
        pid = _place_side(pid, user)["pid"]
    except AttestationRefused:
        return []
    rows = list(public_rows_about({pid})) + pending_rows_visible_to({pid}, user)
    return [row_dict(r, user) for r in sorted(rows, key=lambda r: r.id)]


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

def invalidate_concordances(row):
    """Drop the cached Concordance of every WHG record the row names, for every viewer
    class, so a write shows on the Record page at once instead of after the cache TTL."""
    from django.core.cache import cache
    from places.concordance import CONCORDANCE_CACHE_VERSION
    keys = []
    for pk in (row.place_a_pk, row.place_b_pk):
        if pk:
            keys += [f"concordance:v{CONCORDANCE_CACHE_VERSION}:{int(pk)}:{c}"
                     for c in CONCORDANCE_VIEWER_CLASSES]
    if keys:
        try:
            cache.delete_many(keys)
        except Exception:  # noqa: BLE001 — a broken cache must not fail a write
            pass
