"""
Community record corrections — "Suggestions" (plan-record-suggestions).

Any beta/staff user proposes a fix to one published gazetteer record; it is reviewed by the dataset
owner (if opted in) and/or WHG staff and, on accept, applied via the SAME record apply-path the direct
editor uses (``publish.apply_record_fields`` + ``_reindex_place``), guarded by the record's
``base_version`` captured at proposal time. Every suggestion is kept permanently (accept/reject/
withdraw/supersede) as a provenance record — the forward-compatible seed for v4 attestations.

Scoped to error-correction: a suggestion that changes nothing is refused; competing/parallel claims are
out of scope (v4). The proposal is composed in the existing full-LPF record editor (a throwaway
``WorkbenchProject(place_record)`` working copy), then materialised here as a ``RecordSuggestion``.
"""
import logging

from django.db import transaction
from django.utils import timezone

from .models import RecordSuggestion

logger = logging.getLogger(__name__)


class SuggestionError(Exception):
    """A caller/data error surfaced to the client as a 400 (e.g. an empty diff)."""


class SuggestionSuperseded(Exception):
    """Accept failed because the record changed since the suggestion was made (optimistic-lock)."""


# ── field diff (current record ↔ proposed snapshot) ────────────────────────────
def _changed_fields(place, proposed):
    """Which editable field-groups the proposal changes vs the record's current state. Mirrors the
    normalisation in ``checkout.record_state_hash`` so the diff matches what apply/accept would write."""
    from .checkout import record_snapshot
    cur = record_snapshot(place)
    changed = []

    pt = str(proposed.get('title') or '').strip()
    if pt and pt != (cur['title'] or ''):
        changed.append('name')

    def cc(x):
        return sorted(str(c).upper()[:2] for c in (x or []) if str(c).strip())
    if 'ccodes' in proposed and cc(proposed.get('ccodes')) != cc(cur['ccodes']):
        changed.append('ccodes')

    def toks(lst, k):
        return sorted((i.get(k) or '').strip() for i in (lst or []) if (i.get(k) or '').strip())
    if 'names' in proposed and toks(proposed['names'], 'toponym') != toks(cur['names'], 'toponym'):
        changed.append('names')

    def pairs(lst):
        return sorted(((i.get('label') or '').strip(), (i.get('identifier') or '').strip())
                      for i in (lst or []) if ((i.get('label') or '').strip() or (i.get('identifier') or '').strip()))
    if 'types' in proposed and pairs(proposed['types']) != pairs(cur['types']):
        changed.append('types')

    def idents(lst):
        return sorted((i.get('identifier') or '').strip().rstrip('/') for i in (lst or []) if (i.get('identifier') or '').strip())
    if 'links' in proposed and idents(proposed['links']) != idents(cur['links']):
        changed.append('links')

    if 'descriptions' in proposed and toks(proposed['descriptions'], 'value') != toks(cur['descriptions'], 'value'):
        changed.append('descriptions')

    pd, cd = proposed.get('dates') or {}, cur.get('dates') or {}
    if (pd.get('start'), pd.get('end')) != (cd.get('start'), cd.get('end')):
        changed.append('dates')

    if proposed.get('point_editable') and cur.get('point_editable'):
        def rnd(v):
            try:
                return round(float(v), 6) if v not in (None, '') else None
            except (TypeError, ValueError):
                return None
        if (rnd(proposed.get('lng')), rnd(proposed.get('lat'))) != (rnd(cur.get('lng')), rnd(cur.get('lat'))):
            changed.append('coordinate')
    return changed


# ── create / lifecycle ─────────────────────────────────────────────────────────
def create_suggestion(project, user, rationale=''):
    """Materialise a ``place_record`` working-copy project into a pending ``RecordSuggestion``.
    Refuses a no-op (empty diff). Returns the suggestion; the caller deletes the working copy."""
    from places.models import Place
    from .checkout import record_state_hash
    snap = project.snapshot or {}
    place = Place.objects.filter(pk=snap.get('record_id')).select_related('dataset').first()
    if not place:
        raise SuggestionError('that record no longer exists')
    changed = _changed_fields(place, snap)
    if not changed:
        raise SuggestionError('Nothing to suggest yet — change a field first.')
    return RecordSuggestion.objects.create(
        place=place, dataset=place.dataset, proposer=user,
        proposed_snapshot=snap, base_version=(project.base_version or record_state_hash(place)),
        changed_fields=changed, rationale=(rationale or '')[:4000],
        status=RecordSuggestion.STATUS_PENDING)


def apply_suggestion(suggestion, reviewer):
    """Accept + apply a pending suggestion via the shared record apply-path, guarded by the record's
    ``base_version``. On a stale record → mark ``superseded`` and raise ``SuggestionSuperseded``.
    Returns ``{record_id, changed, reindexed}``.

    The optimistic-lock pre-checks run OUTSIDE the apply transaction so that a ``superseded`` mark is
    committed (raising inside the atomic apply would roll the mark back). Only the apply + accept-mark
    are atomic together."""
    from places.models import Place
    from .checkout import record_state_hash
    from .publish import apply_record_fields, _reindex_place

    if suggestion.status != RecordSuggestion.STATUS_PENDING:
        raise SuggestionError('this suggestion has already been reviewed')

    place = Place.objects.filter(pk=suggestion.place_id).select_related('dataset').first()
    if not place:
        _mark(suggestion, RecordSuggestion.STATUS_SUPERSEDED, reviewer, 'the record no longer exists')
        raise SuggestionSuperseded('the record no longer exists')
    if suggestion.base_version and record_state_hash(place) != suggestion.base_version:
        _mark(suggestion, RecordSuggestion.STATUS_SUPERSEDED, reviewer,
              'the record changed since this correction was proposed')
        raise SuggestionSuperseded('the record changed since this correction was proposed')

    with transaction.atomic():
        changed = apply_record_fields(place, suggestion.proposed_snapshot)
        place.refresh_from_db()
        reindexed = _reindex_place(place)
        suggestion.applied_changed = sorted(set(changed))
        _mark(suggestion, RecordSuggestion.STATUS_ACCEPTED, reviewer, '',
              extra_fields=['applied_changed'])
    return {'record_id': place.id, 'changed': sorted(set(changed)), 'reindexed': reindexed}


def reject_suggestion(suggestion, reviewer, note=''):
    if suggestion.status != RecordSuggestion.STATUS_PENDING:
        raise SuggestionError('this suggestion has already been reviewed')
    _mark(suggestion, RecordSuggestion.STATUS_REJECTED, reviewer, (note or '')[:4000])


def withdraw_suggestion(suggestion, user):
    if suggestion.proposer_id != user.id:
        raise SuggestionError('only the proposer can withdraw this suggestion')
    if suggestion.status != RecordSuggestion.STATUS_PENDING:
        raise SuggestionError('this suggestion has already been reviewed')
    suggestion.status = RecordSuggestion.STATUS_WITHDRAWN
    suggestion.save(update_fields=['status'])


def _mark(suggestion, status, reviewer, note, extra_fields=None):
    suggestion.status = status
    suggestion.reviewed_by = reviewer
    suggestion.reviewed_at = timezone.now()
    if note:
        suggestion.review_note = note
    fields = ['status', 'reviewed_by', 'reviewed_at', 'review_note'] + (extra_fields or [])
    suggestion.save(update_fields=fields)


# ── authorisation + views helpers ──────────────────────────────────────────────
def can_review(user, suggestion):
    """Staff, or an owner of the suggestion's gazetteer, may review it (Dataset.can_edit = staff|owner)."""
    return bool(user and user.is_authenticated and suggestion.dataset.can_edit(user))


def can_see_content(user, dataset, proposer_id=None):
    """Who may see a pending suggestion's CONTENT (plan §1e): staff / gazetteer owner / the proposer.
    Everyone else sees only the count."""
    if not (user and user.is_authenticated):
        return False
    return bool(user.is_staff or dataset.can_edit(user) or (proposer_id and proposer_id == user.id))


def sug_brief(s):
    """Compact dict for the inset/queue: who, when, what changed, rationale — no full snapshot."""
    return {
        'id': s.id, 'record_id': s.place_id, 'dataset_id': s.dataset_id,
        'status': s.status, 'changed_fields': s.changed_fields or [],
        'rationale': s.rationale or '', 'created': s.created.isoformat() if s.created else None,
        'proposer': (getattr(s.proposer, 'name', '') or getattr(s.proposer, 'username', '') or '—') if s.proposer_id else '—',
    }


def inset_for_place(place, user):
    """Pending-suggestions inset for one place (plan §1d/§1e). Count always; items only for
    staff/owner/proposer. ``can_review`` tells the client whether to show accept/reject controls."""
    qs = RecordSuggestion.objects.filter(place=place, status=RecordSuggestion.STATUS_PENDING).select_related('proposer')
    count = qs.count()
    reviewer = bool(user and user.is_authenticated and place.dataset.can_edit(user))
    if user and user.is_authenticated and (user.is_staff or place.dataset.can_edit(user)):
        items = [sug_brief(s) for s in qs]
    elif user and user.is_authenticated:
        items = [sug_brief(s) for s in qs if s.proposer_id == user.id]   # a proposer sees their own
    else:
        items = []
    return {'count': count, 'items': items, 'can_review': reviewer}


def queue_for(user):
    """Pending suggestions this user may review: staff → all; otherwise those on gazetteers they own."""
    qs = (RecordSuggestion.objects.filter(status=RecordSuggestion.STATUS_PENDING)
          .select_related('proposer', 'dataset', 'place'))
    if not user.is_staff:
        qs = qs.filter(dataset__owners=user)
    return qs
