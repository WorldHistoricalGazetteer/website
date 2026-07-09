"""
Check-out: materialise an already-published item into a Workbench snapshot for editing
(plan-collaborativeCollections §6).

"Edit in Workbench" on a published Collection calls the doc-type's ``checkout_loader``, which
serialises the canonical server record → a Workbench snapshot (the same shape the editor and
``publish.py`` speak). The view wraps the result in a new, team-owned ``WorkbenchProject`` marked
with ``source_published_id`` + ``base_version`` so publish-back can run the optimistic-lock check.

``base_version`` is a **content hash** of the collection's editable state at checkout (Collection has
no reliable modified-timestamp). At publish-back, publish.py recomputes the current hash and compares:
if it differs, someone changed the collection under us and we surface a conflict rather than clobber.

The published server record stays the single source of truth; the project is an explicit, versioned
working copy. Place ids are emitted in the ``whg:<pk>`` form so they round-trip cleanly back through
``publish._local_place_pk``; each carries its ``lng/lat`` so the editor's map/colocation work
immediately on a checked-out collection.

v3.3 scope (plan §6 phasing): check-out for Place Collections + Gazetteer Groups (both self-owned,
modest size). Legacy *Gazetteers* (whole datasets) are the highest-blast-radius case and need the
record-level / delta / re-index path of §6.1 — a separate later phase (see developer note).
"""
import hashlib
import json
import logging

logger = logging.getLogger(__name__)


class CheckoutError(Exception):
    """Raised when a published item can't be checked out (missing / wrong type)."""


def collection_state_hash(collection):
    """A stable SHA-1 of a collection's editable state — title, description, keywords, and its
    members (places with sequence/note/relations, or member dataset ids). Used as the optimistic-lock
    ``base_version``: any edit to the collection changes the hash, so publish-back can detect a
    concurrent change. Canonical ordering (sorted) makes it insertion-order-independent."""
    from collection.models import CollPlace
    from traces.models import TraceAnnotation
    state = {'title': collection.title or '', 'description': collection.description or '',
             'keywords': sorted(collection.keywords or [])}
    if collection.collection_class == 'place':
        annos = {a.place_id: a for a in TraceAnnotation.objects.filter(
            collection=collection, anno_type='place', archived=False)}
        members = []
        for cp in CollPlace.objects.filter(collection=collection):
            a = annos.get(cp.place_id)
            members.append([cp.place_id, cp.sequence, (a.note if a else None),
                            sorted(a.relation) if (a and a.relation and a.relation != ['']) else []])
        state['places'] = sorted(members, key=lambda m: m[0])
    else:  # dataset (Gazetteer Group)
        state['datasets'] = sorted(collection.datasets.values_list('id', flat=True))
    blob = json.dumps(state, sort_keys=True, default=str)
    return hashlib.sha1(blob.encode('utf-8')).hexdigest()


def _repr_lnglat(place):
    """[lng, lat] of a place's representative point, or (None, None). Best-effort — repr_point walks
    the place's geometries, so guard against places with none/broken geometry."""
    try:
        rp = place.repr_point
        if rp and len(rp) >= 2:
            return rp[0], rp[1]
    except Exception:
        pass
    return None, None


def checkout_place_collection(collection):
    """Serialise a published ``Collection(collection_class='place')`` → a Place-Collection snapshot.
    Returns ``(snapshot, base_version)``."""
    from collection.models import CollPlace
    from traces.models import TraceAnnotation

    if collection.collection_class != 'place':
        raise CheckoutError('not a Place Collection')

    annos = {a.place_id: a for a in TraceAnnotation.objects.filter(
        collection=collection, anno_type='place', archived=False)}

    places = []
    for cp in CollPlace.objects.filter(collection=collection).select_related('place').order_by('sequence'):
        a = annos.get(cp.place_id)
        lng, lat = _repr_lnglat(cp.place)
        ref = {'id': f'whg:{cp.place_id}', 'title': getattr(cp.place, 'title', '') or '',
               'seq': cp.sequence, 'lng': lng, 'lat': lat}
        if a is not None:
            if a.note:
                ref['note'] = a.note
            if a.relation and a.relation != ['']:
                ref['relation'] = list(a.relation)
            if a.when:
                ref['when'] = a.when
        places.append(ref)

    snapshot = {
        'title': collection.title, 'description': collection.description or '',
        'keywords': list(collection.keywords or []),
        'license': (collection.license.spdx_id if getattr(collection, 'license', None) else ''),
        'places': places,
    }
    return snapshot, collection_state_hash(collection)


def checkout_gazetteer_group(collection):
    """Serialise a published ``Collection(collection_class='dataset')`` → a Gazetteer-Group snapshot.
    Returns ``(snapshot, base_version)``. Each member dataset carries its bbox centroid for the
    editor's colocation ranking."""
    if collection.collection_class != 'dataset':
        raise CheckoutError('not a Gazetteer Group')
    gazetteers = []
    for d in collection.datasets.all():
        c = None
        if getattr(d, 'bbox', None):
            try:
                pt = d.bbox.centroid
                c = [pt.x, pt.y]
            except Exception:
                c = None
        gazetteers.append({'dataset_id': d.id, 'title': d.title, 'centroid': c})
    snapshot = {
        'title': collection.title, 'description': collection.description or '',
        'keywords': list(collection.keywords or []), 'gazetteers': gazetteers,
    }
    return snapshot, collection_state_hash(collection)
