"""
Dataset ("Gazetteer") delta publish-back (plan-dataset-checkout §1c/§2).

A ``dataset_edit`` project holds a checked-out set of a gazetteer's records. Publish-back is a
**delta**: only records the editor marked ``_dirty`` are applied — never a full re-accession. Each
dirty record is:

  1. optimistic-locked **individually** against the per-record hash captured at check-out (a concurrent
     edit to a *different* record never blocks this one; a clash on the *same* record is reported, not
     clobbered — plan §1d),
  2. applied via the shared record apply-path (``publish.apply_record_fields`` — the exact code the
     single-record editor uses, so behaviour is identical), then
  3. targeted-re-indexed (``publish._reindex_place``, one doc each).

Conflicts and successes are reported per record; a conflict on one record does not abort the others.
The record's embedded ``base_version`` is refreshed on success so the same working copy can be
re-published repeatedly without a false conflict, while still catching a *third party* editing that
record between publishes.

NB (plan §1e): a ``DatasetFile.rev`` backup before publish-back — valuable before widening beyond
staff — is a documented follow-up. Delta-only + per-record optimistic locking gives this the SAME
per-record blast radius as the already-shipped single-record correction (DB authoritative, ES
best-effort); it is staff-only for now.
"""
import logging

from django.db import transaction

from .publish import apply_record_fields, _reindex_place, PublishError

logger = logging.getLogger(__name__)


@transaction.atomic
def publish_dataset_edit(project, user):
    """Apply the delta of a ``dataset_edit`` project's dirty records to the published gazetteer.
    Returns ``{dataset_id, dataset, changed_records, applied:[…], reindexed, conflicts:[…]}``."""
    from places.models import Place
    from .checkout import record_state_hash

    snap = project.snapshot or {}
    dataset_id = snap.get('dataset_id')
    records = snap.get('records') or []
    if not dataset_id or not isinstance(records, list):
        raise PublishError('this project has no gazetteer records to publish')

    dirty = [r for r in records if r.get('_dirty') and r.get('record_id')]
    if not dirty:
        return {'dataset_id': dataset_id, 'dataset': snap.get('dataset_label'),
                'changed_records': 0, 'applied': [], 'reindexed': 0, 'conflicts': []}

    ids = [r['record_id'] for r in dirty]
    places = {p.id: p for p in Place.objects.filter(id__in=ids, dataset_id=dataset_id)
              .select_related('dataset')}

    applied, conflicts, reindexed = [], [], 0
    for r in dirty:
        place = places.get(r['record_id'])
        if not place:
            conflicts.append({'record_id': r['record_id'], 'title': r.get('title') or '',
                              'reason': 'no longer in this gazetteer'})
            continue
        bv = r.get('base_version')
        if bv and record_state_hash(place) != bv:
            conflicts.append({'record_id': place.id, 'title': place.title,
                              'reason': 'changed since you checked it out'})
            continue

        changed = apply_record_fields(place, r)
        place.refresh_from_db()
        if _reindex_place(place):
            reindexed += 1
        applied.append({'record_id': place.id, 'title': place.title, 'changed': sorted(set(changed))})
        # New per-record baseline: lets a re-publish of the same working copy avoid a false conflict,
        # while still catching a THIRD party editing this record between publishes. Clear the dirty flag.
        r['base_version'] = record_state_hash(place)
        r['_dirty'] = False

    # Persist cleared dirty flags + refreshed hashes back onto the project (bump nothing else; the
    # optimistic-lock version machinery is the collab layer's, not this delta apply's).
    project.snapshot = snap
    project.published_dataset_id = dataset_id
    project.status = 'published'
    project.save(update_fields=['snapshot', 'published_dataset', 'status', 'updated'])

    return {'dataset_id': dataset_id, 'dataset': snap.get('dataset_label'),
            'changed_records': len(applied), 'applied': applied,
            'reindexed': reindexed, 'conflicts': conflicts}
