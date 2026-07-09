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
import json
import logging

from django.db import transaction
from django.utils import timezone

logger = logging.getLogger(__name__)


class PublishError(Exception):
    """Raised for a caller/data error that should surface to the client as a 400."""


class ConflictError(Exception):
    """Raised when the published collection changed under a checked-out working copy (optimistic-lock
    failure, plan §6). The endpoint surfaces this as a 409 so the client can reload rather than clobber."""


def _check_no_conflict(project, collection):
    """Optimistic-lock guard for publish-BACK: if this project was checked out from ``collection``
    (``base_version`` recorded at checkout), refuse to overwrite when the collection's current content
    hash no longer matches — i.e. someone else edited it meanwhile."""
    if not project.base_version:
        return
    from .checkout import collection_state_hash
    if collection_state_hash(collection) != project.base_version:
        raise ConflictError(
            'This collection was changed since you started editing it. Reload the latest version '
            '(re-open it from its page) and re-apply your changes before publishing.')


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
    if coll is not None:
        _check_no_conflict(project, coll)                       # optimistic-lock (publish-back)
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
    if coll is not None:
        _check_no_conflict(project, coll)                       # optimistic-lock (publish-back)
    if coll is None:
        coll = Collection(owner=user, collection_class='dataset', status='sandbox')
    coll.title = str(snap.get('title') or project.title or 'Untitled group')[:255]
    coll.description = str(snap.get('description') or '')[:3000]
    _apply_scope(coll, snap)
    coll.save()

    coll.datasets.set(list(found.values()))
    _mark_published(project, collection=coll)
    return {'collection_id': coll.pk, 'added': len(found), 'unresolved': unresolved}


# ── record-level correction (plan §6.1) ────────────────────────────────────────
def _reindex_place(place):
    """Targeted single-doc re-index of one place in the pub ES index — only if it's actually indexed
    there (idx_pub). Replicates make_bulk_doc's ``searchy`` enrichment (toponyms + title) so search-by-
    name keeps working. Best-effort: the database is authoritative; on ES failure we log and carry on
    (a later reindex fixes the doc) rather than losing the correction."""
    if not getattr(place, 'idx_pub', False):
        return False
    try:
        from django.conf import settings
        from elastic.es_utils import makeDoc
        doc = makeDoc(place)
        doc['whg_id'] = ''
        searchy = set(doc.get('searchy') or [])
        searchy.update(n['toponym'] for n in doc.get('names', []) if n.get('toponym'))
        if doc.get('title'):
            searchy.add(doc['title'])
        doc['searchy'] = sorted(searchy)
        settings.ES_CONN.index(index=settings.ES_PUB, id=str(place.id), body=json.dumps(doc), refresh=True)
        return True
    except Exception as e:  # noqa: BLE001
        logger.warning('record reindex failed for place %s: %s', place.id, e)
        return False


def apply_record_fields(place, snap):
    """Apply a full-LPF record snapshot's editable fields to ``place`` — place-level fields
    (title/ccodes/dates) + REBUILD of the sub-tables (names/types/links/descriptions) + the coordinate
    (point-editable places only). Returns the list of changed field-groups. Each sub-row's ``jsonb``
    mirrors the accession pipeline's shape, so ``makeDoc`` re-indexes the rebuilt rows unchanged.
    Rebuild is safe because check-out captured every sub-row and the editor round-trips them all
    (removing a row = intentional delete). Depictions/relations/periods are left untouched.

    The CALLER owns the optimistic-lock check, ``refresh_from_db``, re-indexing, and bookkeeping — so
    this same core serves both single-record publish-back and per-record dataset delta publish-back."""
    from django.contrib.gis.geos import Point
    from places.models import Place, PlaceName, PlaceType, PlaceLink, PlaceDescription, PlaceGeom

    changed = []

    def rebuild(Model, rows):
        """Delete this place's rows for a sub-table and recreate from the snapshot. bulk_create bypasses
        save() (so PlaceGeom/PlaceLink's task_id/reviewer guard never fires) — and we never set task_id."""
        Model.objects.filter(place=place).delete()
        if rows:
            Model.objects.bulk_create(rows)

    src = place.src_id or ''

    # ── Place-level fields ─────────────────────────────────────────────────────
    upd = {}
    new_title = str(snap.get('title') or '').strip()
    if new_title:
        upd['title'] = new_title[:255]
        if new_title != place.title:
            changed.append('name')
    if isinstance(snap.get('ccodes'), list):
        cc = [str(c).upper()[:2] for c in snap['ccodes'] if str(c).strip()]
        if cc != (place.ccodes or []):
            changed.append('ccodes')
        upd['ccodes'] = cc
    d = snap.get('dates') or {}
    s, e = d.get('start'), d.get('end')
    if s not in (None, '') or e not in (None, ''):
        try:
            s = int(s) if s not in (None, '') else None
            e = int(e) if e not in (None, '') else None
        except (TypeError, ValueError):
            s = e = None
        if s is not None or e is not None:
            lo, hi = (s if s is not None else e), (e if e is not None else s)
            upd['minmax'] = [lo, hi]
            upd['timespans'] = [[lo, hi]]
            changed.append('dates')
    if upd:
        Place.objects.filter(pk=place.pk).update(**upd)

    # ── Sub-tables: rebuild from the snapshot (round-tripped raw jsonb + edited keys) ──
    names = snap.get('names')
    if isinstance(names, list):
        rows = []
        for n in names:
            top = str(n.get('toponym') or '').strip()
            if not top:
                continue
            jb = dict(n.get('_raw') or {}); jb['toponym'] = top
            if n.get('lang'):
                jb['lang'] = n['lang']
            rows.append(PlaceName(place=place, src_id=src, toponym=top[:2044], jsonb=jb))
        rebuild(PlaceName, rows)
        changed.append('names')

    types = snap.get('types')
    if isinstance(types, list):
        rows = []
        for t in types:
            label, ident = str(t.get('label') or '').strip(), str(t.get('identifier') or '').strip()
            if not (label or ident):
                continue
            jb = dict(t.get('_raw') or {})
            if label:
                jb['label'] = label
            if ident:
                jb['identifier'] = ident
            fc = (t.get('fclass') or '').strip()[:1] or None
            rows.append(PlaceType(place=place, src_id=src, fclass=fc, aat_id=t.get('aat_id'), jsonb=jb))
        rebuild(PlaceType, rows)
        changed.append('types')

    links = snap.get('links')
    if isinstance(links, list):
        rows = []
        seen = set()
        for l in links:
            ident = str(l.get('identifier') or '').strip().rstrip('/')
            if not ident or ident in seen:
                continue
            seen.add(ident)
            rows.append(PlaceLink(place=place, src_id=src,
                                  jsonb={'type': str(l.get('type') or 'closeMatch'), 'identifier': ident}))
        rebuild(PlaceLink, rows)
        changed.append('links')

    descriptions = snap.get('descriptions')
    if isinstance(descriptions, list):
        rows = []
        for dd in descriptions:
            val = str(dd.get('value') or '').strip()
            if not val:
                continue
            jb = dict(dd.get('_raw') or {}); jb['value'] = val
            if dd.get('lang'):
                jb['lang'] = dd['lang']
            rows.append(PlaceDescription(place=place, src_id=src, jsonb=jb))
        rebuild(PlaceDescription, rows)
        changed.append('descriptions')

    # ── Geometry — full single/multi draw editing (plan-record-suggestions §8) ──
    # When the editor sends a drawable geometry (``geometry_editable``), REBUILD the place's geom rows
    # from the drawn geometry — but only if it actually changed, so unchanged reads preserve any
    # temporal/provenance metadata on the original rows. A snapshot without ``geometry`` falls back to
    # the legacy point path (keeps in-flight working copies working).
    if 'geometry' in snap and snap.get('geometry_editable'):
        from .checkout import _place_geometry, _norm_geom
        new_geom = snap.get('geometry')
        cur_geom, _ = _place_geometry(place)
        if _norm_geom(new_geom) != _norm_geom(cur_geom):
            PlaceGeom.objects.filter(place=place).delete()
            if new_geom and new_geom.get('type') and new_geom.get('type') != 'GeometryCollection':
                try:
                    from django.contrib.gis.geos import GEOSGeometry
                    g = GEOSGeometry(json.dumps(new_geom), srid=4326)
                    PlaceGeom.objects.create(place=place, geom=g, jsonb=new_geom, src_id=src)
                except Exception as e:  # noqa: BLE001 — bad geometry: leave rows cleared, log
                    logger.warning('record %s: could not build geometry %s: %s', place.pk, new_geom, e)
            changed.append('geometry')
    elif snap.get('point_editable') and snap.get('lng') is not None and snap.get('lat') is not None:
        try:
            lng, lat = float(snap['lng']), float(snap['lat'])
        except (TypeError, ValueError):
            lng = lat = None
        if lng is not None and -180 <= lng <= 180 and -90 <= lat <= 90:
            geoms = list(PlaceGeom.objects.filter(place=place))
            if len(geoms) == 1 and isinstance(geoms[0].jsonb, dict) and geoms[0].jsonb.get('type') == 'Point':
                jb = dict(geoms[0].jsonb); jb['coordinates'] = [lng, lat]
                PlaceGeom.objects.filter(pk=geoms[0].pk).update(geom=Point(lng, lat, srid=4326), jsonb=jb)
                changed.append('coordinate')
            elif not geoms:
                PlaceGeom.objects.create(place=place, geom=Point(lng, lat, srid=4326),
                                         jsonb={'type': 'Point', 'coordinates': [lng, lat]}, src_id=src)
                changed.append('coordinate')

    return changed


@transaction.atomic
def publish_place_record(project, user):
    """Publish-back a full-LPF single-record correction (plan §6.1): apply the edited fields to the
    ``places.Place`` (via ``apply_record_fields``) and re-index that one record. Guarded by the
    record-level optimistic hash. Returns ``{record_id, changed:[…], reindexed, dataset}``."""
    from places.models import Place
    from .checkout import record_state_hash

    snap = project.snapshot or {}
    place = Place.objects.filter(pk=snap.get('record_id')).select_related('dataset').first()
    if not place:
        raise PublishError('that place record no longer exists')
    if project.base_version and record_state_hash(place) != project.base_version:
        raise ConflictError('This record was changed since you started editing it. Reload it from its '
                            'page and re-apply your correction.')

    changed = apply_record_fields(place, snap)

    place.refresh_from_db()
    reindexed = _reindex_place(place)
    project.source_published_id = str(place.id)
    project.base_version = record_state_hash(place)
    project.status = 'published'
    project.save(update_fields=['source_published_id', 'base_version', 'status', 'updated'])
    return {'record_id': place.id, 'changed': sorted(set(changed)), 'reindexed': reindexed,
            'dataset': place.dataset.label}


def _mark_published(project, collection):
    from .checkout import collection_state_hash
    project.published_collection = collection
    project.status = 'published'
    # New baseline: the state we just wrote. Lets the same working copy re-publish repeatedly without
    # a false conflict, while still catching a THIRD party editing the collection between publishes.
    project.source_published_id = str(collection.pk)
    project.base_version = collection_state_hash(collection)
    project.save(update_fields=['published_collection', 'status', 'source_published_id',
                                'base_version', 'updated'])


# ── validators reused from the registry (kept here too so publish is self-contained) ──────────────
def _place_collection_errors(snap):
    from .doctypes import _errs_place_collection
    return _errs_place_collection(snap)


def _gazetteer_group_errors(snap):
    from .doctypes import _errs_gazetteer_group
    return _errs_gazetteer_group(snap)
