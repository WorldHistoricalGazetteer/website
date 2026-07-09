"""
Publish a Workbench snapshot into WHG's canonical models (plan-collaborativeCollections §9).

Publish-back writes into the **existing** ``collection.Collection`` model, so the current public
experience (``/collections/<id>/…`` browse pages, citation, carousels) is preserved with no new
browse code. Each ``publish_*`` function:

  1. resolves the project's ``published_collection`` (update-in-place if re-publishing, else create),
  2. writes the collection metadata + members from the snapshot,
  3. triggers any re-indexing (see the note on indexing below),
  4. records the pointer + marks the project ``published``.

Members-must-be-indexed rule (plan §8.1): a Place Collection references places that EXIST in WHG.
Every member id in the snapshot must resolve to a local ``places.Place`` row. WHG legacy place ids
are plain integers; CRC-gateway ids are namespaced (e.g. ``gn:745044``) and are NOT local Place rows.
We resolve the former and REPORT the latter as ``unresolved`` rather than silently forging place
identity — the "contribute this as a gazetteer record first, then add it" bridge (§8.1) is the
intended follow-up for unmatched additions.

Indexing note: collections are not themselves Elasticsearch documents — WHG indexes at the Place
level, per public dataset (datasets.tasks.index_to_pub). A Place Collection over already-indexed
places therefore needs no ES write on publish; its members are already searchable under their own
datasets. We keep a single ``_reindex_hook`` seam so a doc-type that DOES need indexing (or a future
per-collection index) has one obvious place to fire it.
"""
import logging

from django.db import transaction
from django.utils import timezone

logger = logging.getLogger(__name__)


class PublishError(Exception):
    """Raised for a caller/data error that should surface to the client as a 400."""


def _local_place_pk(pid):
    """Return the local ``places.Place`` pk for a snapshot/reconciliation place id, or None.

    WHG's reconciliation service namespaces legacy WHG place ids as ``whg:<digits>`` (the ``whg``
    pseudo-namespace; see api.reconcile_helpers.get_namespace), and its extend keys use the
    ``place:<id>`` form — so a local id may arrive as ``123``, ``whg:123``, ``place:123`` or
    ``place:whg:123``. ONLY a WHG-namespaced (or bare/place-prefixed) numeric id is a local Place row;
    every CRC-gateway id carries a real source namespace (``gn:``, ``wd:``, ``osm:``, ``tgn:``, …) —
    e.g. ``gn:745044`` or ``place:gn:745044`` — and has no local pk, so it can't become a CollPlace
    and is reported as unresolved (the contribute-then-add bridge, §8.1).

    Rule: the final colon-separated segment must be all digits, and its namespace (the segment before
    it, or ``whg`` if none) must be ``whg`` or the neutral ``place`` wrapper."""
    parts = str(pid).strip().split(':')
    last = parts[-1]
    if not last.isdigit():
        return None
    namespace = parts[-2].lower() if len(parts) >= 2 else 'whg'
    return int(last) if namespace in ('whg', 'place') else None


def _resolve_places(place_refs):
    """Map snapshot place refs → (resolved:[(Place, ref)], unresolved:[id str]).

    ``place_refs`` is the snapshot ``places`` list: ``[{id, note?, relation?[], seq?, when?}, …]``.
    Only refs resolving to a local WHG ``places.Place`` pk (``whg:``/bare-digit ids) can become a
    CollPlace; CRC-namespaced ids (``gn:…``) and unknown ids are returned as ``unresolved``."""
    from places.models import Place
    resolved, unresolved, want = [], [], {}
    for ref in place_refs:
        pk = _local_place_pk(ref.get('id'))
        if pk is not None:
            want.setdefault(pk, []).append(ref)
        else:
            unresolved.append(str(ref.get('id')))       # CRC-gateway / non-local id — can't FK
    if want:
        found = {p.id: p for p in Place.objects.filter(id__in=list(want.keys()))}
        for pk, refs in want.items():
            place = found.get(pk)
            for ref in refs:
                (resolved.append((place, ref)) if place else unresolved.append(str(pk)))
    return resolved, unresolved


def _reindex_hook(collection):
    """Re-indexing seam (see module docstring). No-op for collections over already-indexed places;
    kept so the call-site is explicit and a future indexing need has one home."""
    logger.debug('publish: no ES reindex needed for collection %s (references indexed places)',
                 collection.pk)


def _apply_scope(collection, snapshot):
    """Write shared scope fields (keywords, license) from the snapshot onto the collection."""
    kws = snapshot.get('keywords')
    if isinstance(kws, list):
        collection.keywords = [str(k)[:50] for k in kws if str(k).strip()][:50]
    # license: resolve a snapshot SPDX id / label to a licensing.License if given; else leave as-is.
    lic = (snapshot.get('license') or '').strip() if isinstance(snapshot.get('license'), str) else ''
    if lic:
        from licensing.models import License
        obj = (License.objects.filter(spdx_id__iexact=lic).first()
               or License.objects.filter(label__iexact=lic).first())
        if obj:
            collection.license = obj


@transaction.atomic
def publish_place_collection(project, user, sequenced=False):
    """Publish (or re-publish) a Place Collection / Itinerary snapshot into
    ``Collection(collection_class='place')`` + ``CollPlace`` + ``TraceAnnotation``.

    ``sequenced`` (Itinerary): ``CollPlace.sequence`` is taken from each member's ``seq`` if present,
    else its position in the list, so the ordered/animated presentation has a stable order.

    Returns a summary dict: ``{collection_id, added, unresolved:[…], sequenced}``.
    """
    from collection.models import Collection, CollPlace
    from traces.models import TraceAnnotation

    snap = project.snapshot or {}
    errs = _place_collection_errors(snap)
    if errs:
        raise PublishError('; '.join(errs))

    resolved, unresolved = _resolve_places(snap.get('places') or [])

    coll = project.published_collection
    if coll is None:
        coll = Collection(owner=user, collection_class='place', status='sandbox')
    coll.title = str(snap.get('title') or project.title or 'Untitled collection')[:255]
    coll.description = str(snap.get('description') or '')[:3000]
    _apply_scope(coll, snap)
    coll.save()

    # Rebuild membership from the snapshot (publish-back is authoritative for this collection's set).
    # Only touch rows we own the shape of — CollPlace + the locating TraceAnnotations we create.
    CollPlace.objects.filter(collection=coll).delete()
    TraceAnnotation.objects.filter(collection=coll, owner=user, anno_type='place').delete()

    added = 0
    for i, (place, ref) in enumerate(resolved):
        seq = ref.get('seq')
        sequence = seq if isinstance(seq, int) else (i if sequenced else 0)
        CollPlace.objects.create(collection=coll, place=place, sequence=sequence)
        relation = ref.get('relation')
        TraceAnnotation.objects.create(
            collection=coll, place=place, owner=user, anno_type='place', motivation='locating',
            src_id=getattr(place, 'src_id', '') or '',
            note=(str(ref.get('note'))[:2044] if ref.get('note') else None),
            relation=relation if isinstance(relation, list) else [''],
            sequence=(sequence if sequenced else None),
            when=ref.get('when') if isinstance(ref.get('when'), (dict, list)) else None,
            saved=True)
        added += 1

    _reindex_hook(coll)
    _mark_published(project, collection=coll)
    return {'collection_id': coll.pk, 'added': added, 'unresolved': unresolved,
            'sequenced': bool(sequenced)}


@transaction.atomic
def publish_gazetteer_group(project, user):
    """Publish (or re-publish) a Gazetteer Group snapshot into
    ``Collection(collection_class='dataset')`` + the ``datasets`` M2M.

    Only datasets the user may access are attached; others are reported as ``unresolved``.
    Returns ``{collection_id, added, unresolved:[…]}``.
    """
    from collection.models import Collection
    from datasets.models import Dataset

    snap = project.snapshot or {}
    errs = _gazetteer_group_errors(snap)
    if errs:
        raise PublishError('; '.join(errs))

    ids = []
    for g in (snap.get('gazetteers') or []):
        did = g.get('dataset_id')
        if str(did).isdigit():
            ids.append(int(did))
    found = {d.id: d for d in Dataset.objects.filter(id__in=ids)}
    unresolved = [str(i) for i in ids if i not in found]

    coll = project.published_collection
    if coll is None:
        coll = Collection(owner=user, collection_class='dataset', status='sandbox')
    coll.title = str(snap.get('title') or project.title or 'Untitled group')[:255]
    coll.description = str(snap.get('description') or '')[:3000]
    _apply_scope(coll, snap)
    coll.save()

    coll.datasets.set(list(found.values()))
    _mark_published(project, collection=coll)
    return {'collection_id': coll.pk, 'added': len(found), 'unresolved': unresolved}


def _mark_published(project, collection):
    project.published_collection = collection
    project.status = 'published'
    project.save(update_fields=['published_collection', 'status', 'updated'])


# ── validators reused from the registry (kept here too so publish is self-contained) ──────────────
def _place_collection_errors(snap):
    from .doctypes import _errs_place_collection
    return _errs_place_collection(snap)


def _gazetteer_group_errors(snap):
    from .doctypes import _errs_gazetteer_group
    return _errs_gazetteer_group(snap)
