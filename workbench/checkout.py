"""
Check-out: materialise an already-published item into a Workbench snapshot for editing
(plan-collaborativeCollections §6).

"Edit in Workbench" on a published Collection calls the doc-type's ``checkout_loader``, which
serialises the canonical server record → a Workbench snapshot (the same shape the editor and
``publish.py`` speak). The view wraps the result in a new, team-owned ``WorkbenchProject`` marked
with ``source_published_id`` + ``base_version`` so publish-back can run the optimistic-lock check.

The published server record stays the single source of truth; the project is an explicit, versioned
working copy. Place ids are emitted in the ``whg:<pk>`` form so they round-trip cleanly back through
``publish._local_place_pk`` (and match the reconciliation matcher's id shape).

v3.3 scope (plan §6 phasing): check-out is built for Place Collections first (self-owned, lowest
blast radius). Full record-level / delta check-out for large gazetteers (§6.1) is a later phase; the
loader here does a whole-collection materialisation, which is safe for the modest sizes Place
Collections reach.
"""
import logging

logger = logging.getLogger(__name__)


class CheckoutError(Exception):
    """Raised when a published item can't be checked out (missing / wrong type)."""


def checkout_place_collection(collection):
    """Serialise a published ``Collection(collection_class='place')`` → a Place-Collection snapshot.

    Returns ``(snapshot, base_version)``. ``base_version`` is the collection's version string (or its
    pk-scoped modified marker) captured at checkout, for the publish-back optimistic-lock check."""
    from collection.models import CollPlace
    from traces.models import TraceAnnotation

    if collection.collection_class != 'place':
        raise CheckoutError('not a Place Collection')

    # Annotations keyed by place for merging note/relation/when onto each member.
    annos = {a.place_id: a for a in TraceAnnotation.objects.filter(
        collection=collection, anno_type='place', archived=False)}

    places = []
    for cp in CollPlace.objects.filter(collection=collection).select_related('place').order_by('sequence'):
        a = annos.get(cp.place_id)
        ref = {'id': f'whg:{cp.place_id}',
               'title': getattr(cp.place, 'title', '') or '',
               'seq': cp.sequence}
        if a is not None:
            if a.note:
                ref['note'] = a.note
            if a.relation and a.relation != ['']:
                ref['relation'] = list(a.relation)
            if a.when:
                ref['when'] = a.when
        places.append(ref)

    snapshot = {
        'title': collection.title,
        'description': collection.description or '',
        'keywords': list(collection.keywords or []),
        'license': (collection.license.spdx_id if getattr(collection, 'license', None) else ''),
        'places': places,
    }
    base_version = str(collection.version or collection.pk)
    return snapshot, base_version
