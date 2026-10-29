"""Rules for writing a ContributorAttestation (place#334, Phase 0).

``POST /api/links`` used to take every field from the client: any place pair, any ``status``,
any ``legacy_v3_2``, with no check that the caller may see either place, and ``DELETE``
removed the row outright. Active rows are forwarded to the gateway's live hard-link overlay,
so a single request could assert a link between datasets the caller cannot see, or forge a
migrated-from-v3.2 row. The rules below are the single place those writes are decided.

Policy (fail closed):

* ``relation_type`` must be one of the model's choices.
* Both place ids must be ``<namespace>:<id>``, differ, and be visible to the caller. A
  contributed id (``whg:<dataset>:<src>``) goes through ``api.dataset_access.dataset_visibility``;
  any other namespace must have a registry row ``visible_to`` the caller. A refusal on either
  side is the same 404 whichever side it was, so a private id is not confirmed to exist.
* ``dataset_id`` must be ``whg:<pk>`` naming a dataset the caller may view.
* ``status`` and ``legacy_v3_2`` are never read from the client: a new row is ``active`` and
  not legacy. Legacy rows come only from the governed migration (place#334 Phase 5).
* At most ``CONTRIBUTOR_ATTESTATION_DAILY_CAP`` new rows per user per 24 h.
* A row is never hard-deleted by a request: ``withdraw`` sets ``status='withdrawn'``, which the
  ``post_save`` handler turns into a gateway revocation, and the row is kept as the record.
"""
import re
from datetime import timedelta

from django.conf import settings
from django.utils import timezone

from api.dataset_access import dataset_visibility, parse_contributed_id, WHG_NAMESPACE

RELATIONS = {"sameAs", "exactMatch", "closeMatch", "distinct"}
_PLACE_ID = re.compile(r"^([a-z][a-z0-9_]{0,15}):(\S{1,110})$")
_DATASET_ID = re.compile(r"^whg:(\d+)$")
DEFAULT_DAILY_CAP = 200


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


def _place_visible(pid, user):
    m = _PLACE_ID.match(pid)
    if m is None:
        raise AttestationRefused(400, f"malformed place id: {pid[:40]!r}")
    if m.group(1) == WHG_NAMESPACE:
        if parse_contributed_id(pid) is None:
            raise AttestationRefused(400, f"malformed place id: {pid[:40]!r}")
        if not dataset_visibility(pid, user).allowed:
            raise _not_found()
        return
    if not _namespace_visible(m.group(1), user):
        raise _not_found()


def _dataset_visible(dataset_id, user):
    from api.dataset_access import dataset_visible_to
    from datasets.models import Dataset
    m = _DATASET_ID.match(dataset_id)
    if m is None:
        raise AttestationRefused(400, "dataset_id must be 'whg:<dataset id>'")
    if not dataset_visible_to(Dataset.objects.filter(pk=int(m.group(1))).first(), user):
        raise _not_found()


def validate(user, body):
    """The cleaned fields of a create request, or ``AttestationRefused``."""
    try:
        dataset_id = str(body["dataset_id"])
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
    # Both sides are checked before either answer is given, so the refusal does not
    # reveal which side was the hidden one.
    _dataset_visible(dataset_id, user)
    _place_visible(place_a, user)
    _place_visible(place_b, user)
    return {
        "dataset_id": dataset_id,
        "place_a": place_a,
        "place_b": place_b,
        "relation_type": relation_type,
        "justification": (justification or "").strip()[:2000] or None,
    }


def _check_cap(user):
    from api.models import ContributorAttestation
    cap = getattr(settings, "CONTRIBUTOR_ATTESTATION_DAILY_CAP", DEFAULT_DAILY_CAP)
    since = timezone.now() - timedelta(hours=24)
    if ContributorAttestation.objects.filter(user=user, asserted_at__gte=since).count() >= cap:
        raise AttestationRefused(429, f"daily limit of {cap} links reached")


def create(user, body):
    """``(row, created)``. An existing active row for the same edge is returned unchanged;
    a withdrawn / rejected / superseded one is reactivated (the unique constraint allows one
    row per edge and user)."""
    from api.models import ContributorAttestation
    clean = validate(user, body)
    a, b = sorted((clean["place_a"], clean["place_b"]))
    existing = ContributorAttestation.objects.filter(
        user=user, place_a=a, place_b=b, relation_type=clean["relation_type"]).first()
    if existing is not None:
        if existing.status == "active":
            return existing, False
        existing.status = "active"
        existing.withdrawn_at = None
        existing.dataset_id = clean["dataset_id"]
        existing.justification = clean["justification"]
        existing.asserted_at = timezone.now()
        existing.save()
        return existing, True
    _check_cap(user)
    row = ContributorAttestation(user=user, status="active", legacy_v3_2=False, **clean)
    row.save()
    return row, True


def withdraw(row):
    """Withdraw one attestation: kept as a record, revoked from the gateway by the
    ``post_save`` handler. Idempotent."""
    if row.status == "withdrawn":
        return row
    row.status = "withdrawn"
    row.withdrawn_at = timezone.now()
    row.save()
    return row


def withdraw_for_dataset(dataset_pk):
    """Withdraw every attestation made for, or touching a place of, a dataset that is going
    away. ``dataset_id`` is a CharField and not a foreign key, so deleting the dataset does
    not reach these rows and they would stay active in the gateway overlay. Rows are saved one
    at a time so each revocation is forwarded. Returns the number withdrawn."""
    from django.db.models import Q
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
