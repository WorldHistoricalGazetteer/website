"""What happens to a location added after upload (place#350) when its dataset or its Link
changes state (place#343: "the adopted location goes back with the Link").

An ADOPTED location (``datasets.location_adoption.adopt``) exists because of one Link, a
"Same place" statement about one suggestion, and carries a source's coordinates under that
source's terms. A PLACED location is the owner's own data, with no Link and no source.

The rules, per event:

* **Dataset made private** (v3 form, admin) **or withdrawn** (``lifecycle.withdraw``): its Links
  go ``pending`` (``api.attestations.deactivate_for_dataset``) and leave every public view; the
  adopted location goes with them, by the same mechanism that hides the rest of the dataset:
  every surface that shows a record (Record page, ``/entity``, ``/api/place``, ``/api/geojson``,
  downloads) is gated on dataset visibility (place#310), and the dataset leaves the indexes.
  The row is KEPT on the record, unchanged, and still shown to the dataset's own circle. Placed
  locations: the same.
* **Re-published** (``public`` back to True by any route, or ``lifecycle.publish``): the Links
  go back to ``active`` and the location comes back with them, EXCEPT any adopted location that
  the rule no longer allows (its Link no longer stands, or its source's terms changed while the
  dataset was private): those are retracted first (``enforce``, called from
  ``datasets.signals.restore_links_when_public``), so publication never re-exposes them.
* **The Link stops standing** (``withdrawn`` by its author, the Links API, the feed's Undo or a
  dataset deletion; ``rejected`` by an editor; ``superseded``): the adopted location is
  RETRACTED: copied, field by field, into ``RetractedLocation`` and its ``PlaceGeom`` row taken
  off the record, so that nothing reading ``place.geoms`` can show, download or index it. The
  download cache is cleared and, for a public dataset, its records are re-indexed after
  commit. Not retracted while another standing "Same place" Link on the same suggestion
  remains (the match still stands). Driven by the ``post_save`` of ``ContributorAttestation``
  (``datasets/signals.py``), the same hook ``api/signals.py`` uses to forward Links, so every
  route that saves a Link is covered; a bulk ``update()`` is not, which ``audit`` and the QC
  report catch.
* **The feed's Undo with "keep the location"**: the owner's explicit choice (place#350: Undo
  "offers to remove its adopted location, never silently keeps it"). The location stays, marked
  ``provenance.kept`` (who, when, which Link), and is no longer tied to the Link; it is still
  labelled as adopted from its source and still gated on the source's terms.
* **The Link comes back** (the same person marks "Same place" again, which reactivates the same
  row): the retracted location is NOT restored on its own. Adopting is a separate act, never a
  side effect of "Same place" (place#350 constraint 4); the feed offers it again. Staff can put
  the archived row back with ``restore`` (``manage.py location_lifecycle --restore <id>``), which
  refuses unless the Link stands again, the source's terms still allow it and the record has no
  location.
* **Dataset deleted**: its records and every row on them, adopted and placed, are deleted with
  it (the Links are withdrawn first and kept, ``withdraw_for_dataset``); the archive rows go too.
* **Placed locations** never depend on a Link and are never retracted here.
"""
import logging

from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from datasets.location_adoption import ADOPTED, PLACED, provenance, registry_entries, source_reason

logger = logging.getLogger(__name__)

STANDING = ("active", "pending")
REASON_TERMS = "source terms"
REASON_LINK = "link no longer stands"


class RestoreRefused(Exception):
    pass


# ---------------------------------------------------------------------------
# Reading the rule
# ---------------------------------------------------------------------------

def _adopted_qs():
    from places.models import PlaceGeom
    return PlaceGeom.objects.filter(task_id__startswith="gen:", jsonb__provenance__kind=ADOPTED)


def _standing(att_pks, cand_pks):
    """``(attestation pks, candidate pks)`` among those given that have a standing "Same place"
    Link. One query."""
    from api.models import ContributorAttestation
    att_pks, cand_pks = {p for p in att_pks if p}, {p for p in cand_pks if p}
    if not att_pks and not cand_pks:
        return set(), set()
    rows = (ContributorAttestation.objects.filter(status__in=STANDING).exclude(relation_type="distinct")
            .filter(Q(pk__in=att_pks) | Q(candidate_id__in=cand_pks)).values_list("pk", "candidate_id"))
    a, c = set(), set()
    for pk, cand in rows:
        a.add(pk)
        if cand:
            c.add(cand)
    return a, c


def _why_not(prov, standing_atts, standing_cands, entries):
    """None when an adopted row may stay on its record, else the reason it may not."""
    ns = str(prov.get("source") or "")
    if source_reason(ns, entries.get(ns)) is not None:
        return REASON_TERMS
    if prov.get("kept"):
        return None   # the owner kept it when undoing the match: no longer tied to the Link
    if prov.get("attestation") in standing_atts or (prov.get("candidate") and prov.get("candidate") in standing_cands):
        return None
    return REASON_LINK


def audit(ds=None, rows=None):
    """``[(PlaceGeom, reason)]``: the adopted rows of ``ds`` (or the ``rows`` given) that the rule
    says may not be on their record. A constant number of queries however many rows."""
    rows = list(rows if rows is not None else _adopted_qs().filter(place__dataset=ds.label))
    provs = [(g, provenance(g)) for g in rows]
    provs = [(g, p) for g, p in provs if p and p.get("kind") == ADOPTED]
    if not provs:
        return []
    atts, cands = _standing({p.get("attestation") for _, p in provs}, {p.get("candidate") for _, p in provs})
    entries = registry_entries({p.get("source") for _, p in provs})
    out = []
    for g, p in provs:
        why = _why_not(p, atts, cands, entries)
        if why:
            out.append((g, why))
    return out


# ---------------------------------------------------------------------------
# Retraction and restoration
# ---------------------------------------------------------------------------

def _after_change(datasets, collections=()):
    """After commit, never raising into the caller: the download caches of each dataset in
    ``datasets`` (``{dataset pk: (public, place pks)}``) cleared, its map data (and that of the
    ``collections`` showing its records) dropped and rebuilt at once, and, for a public dataset,
    the changed records re-indexed: ``pub`` by ``datasets.tasks.refresh_index_after_location_change``,
    the gateway by ``datasets.location_index.location_changed``."""
    def run():
        from api.download_file import FileCache
        from datasets.location_index import location_changed
        from datasets.tasks import refresh_index_after_location_change
        from utils.mapdata import delete_mapdata_cache
        for ds_pk, (public, place_pks) in datasets.items():
            try:
                for ft in ("lpf", "tsv"):
                    FileCache.delete_cache("dataset", ds_pk, filetype=ft)
            except Exception:  # noqa: BLE001 - a stale cache is rebuilt later
                logger.warning("download cache of dataset %s not cleared", ds_pk, exc_info=True)
            if public:
                try:
                    refresh_index_after_location_change.delay(ds_pk, sorted(place_pks))
                except Exception:  # noqa: BLE001 - the index sweep and the QC report catch it
                    logger.warning("re-index of dataset %s not queued", ds_pk, exc_info=True)
                try:
                    location_changed(ds_pk)   # the gateway index: debounced re-ingest (place#350)
                except Exception:  # noqa: BLE001
                    logger.warning("gateway re-index of dataset %s not queued", ds_pk, exc_info=True)
        for category, pk in [("datasets", d) for d in datasets] + [("collections", c) for c in collections]:
            try:
                delete_mapdata_cache.delay(category, pk, refresh=True)
            except Exception:  # noqa: BLE001
                logger.warning("map data of %s %s not refreshed", category, pk, exc_info=True)
    transaction.on_commit(run)


def retract(rows, reason, by=None, attestation=None):
    """Take adopted rows off their records into ``RetractedLocation``. Idempotent (one archive
    row per geometry; a row already gone is skipped) and bulk: a constant number of queries,
    whatever the number of rows (the per-row map-data signals of ``PlaceGeom`` are replaced by
    one refresh per dataset and collection, ``_after_change``). Placed rows and the
    contributor's own rows are never touched. Returns the number retracted."""
    from api.models import ContributorAttestation
    from collection.models import CollDataset, CollPlace
    from datasets.models_matching import RetractedLocation
    from places.models import Place, PlaceGeom
    rows = [g for g in rows if (provenance(g) or {}).get("kind") == ADOPTED]
    if not rows:
        return 0
    with transaction.atomic():
        live = set(PlaceGeom.objects.select_for_update().filter(pk__in=[g.pk for g in rows])
                   .values_list("pk", flat=True))
        rows = [g for g in rows if g.pk in live]
        if not rows:
            return 0
        place_pks = {g.place_id for g in rows}
        info = {pk: (ds_pk, public) for pk, ds_pk, public in
                Place.objects.filter(pk__in=place_pks).values_list("pk", "dataset__id", "dataset__public")}
        att_of = {g.pk: (attestation.pk if attestation is not None else (provenance(g) or {}).get("attestation"))
                  for g in rows}
        known = set(ContributorAttestation.objects.filter(pk__in={a for a in att_of.values() if a})
                    .values_list("pk", flat=True))
        now = timezone.now()
        RetractedLocation.objects.bulk_create([
            RetractedLocation(
                place_id=g.place_id, dataset_id=info[g.place_id][0], geom_pk=g.pk,
                attestation_id=att_of[g.pk] if att_of[g.pk] in known else None,
                src_id=g.src_id or "", jsonb=g.jsonb or {}, geom_wkt=g.geom.wkt if g.geom else "",
                geom_src=g.geom_src_id or "", task_id=g.task_id or "", reviewer_id=g.reviewer_id,
                created=g.create_date, reason=reason[:40], retracted_at=now,
                retracted_by=by if getattr(by, "pk", None) else None)
            for g in rows], ignore_conflicts=True)
        # One DELETE; nothing references a PlaceGeom, so no cascade is skipped.
        doomed = PlaceGeom.objects.filter(pk__in=[g.pk for g in rows])
        doomed._raw_delete(doomed.db)
        touched = {}
        for g in rows:
            ds_pk, public = info[g.place_id]
            touched.setdefault(ds_pk, (bool(public), set()))[1].add(g.place_id)
        collections = (set(CollPlace.objects.filter(place_id__in=place_pks).values_list("collection_id", flat=True))
                       | set(CollDataset.objects.filter(dataset_id__in=list(touched))
                             .values_list("collection_id", flat=True)))
        _after_change(touched, sorted(collections))
    logger.info("retracted %s adopted location(s) (%s)", len(rows), reason)
    return len(rows)


def _attestation_place_pks(att):
    pks = {p for p in (att.place_a_pk, att.place_b_pk) if p}
    if not pks and att.candidate_id:
        from datasets.models_matching import MatchCandidate
        pks = set(MatchCandidate.objects.filter(pk=att.candidate_id).values_list("place_id", flat=True))
    return pks


def on_link_saved(att):
    """The ``post_save`` hook: a Link that no longer stands takes back the locations adopted
    through it. No query at all for a Link that stands (publication flips thousands)."""
    if att.status in STANDING or att.relation_type == "distinct":
        return 0
    place_pks = _attestation_place_pks(att)
    if not place_pks:
        return 0
    rows = [g for g in _adopted_qs().filter(place_id__in=place_pks)
            if not (provenance(g) or {}).get("kept")
            and ((provenance(g) or {}).get("attestation") == att.pk
                 or (att.candidate_id and (provenance(g) or {}).get("candidate") == att.candidate_id))]
    if not rows:
        return 0
    atts, cands = _standing({(provenance(g) or {}).get("attestation") for g in rows},
                            {(provenance(g) or {}).get("candidate") for g in rows})
    rows = [g for g in rows if (provenance(g) or {}).get("attestation") not in atts
            and (provenance(g) or {}).get("candidate") not in cands]
    return retract(rows, f"link {att.status}", by=att.withdrawn_by if att.status == "withdrawn" else None,
                   attestation=att)


def enforce(ds, by=None, dry_run=False):
    """Retract every adopted row of ``ds`` the rule does not allow (at publication, and from the
    management command). Returns ``[(PlaceGeom, reason)]`` found."""
    found = audit(ds)
    if not dry_run:
        for reason in {why for _, why in found}:
            retract([g for g, why in found if why == reason], reason, by=by)
    return found


def keep_after_undo(user, rows):
    """The feed's "Undo, keep the location": the owner's explicit choice, recorded on each row
    (who, when, and the Link it was adopted through), so the Link's withdrawal leaves it."""
    from places.models import PlaceGeom
    today = timezone.now().date().isoformat()
    for g in rows:
        j = dict(g.jsonb or {})
        prov = dict(j.get("provenance") or {})
        prov["kept"] = {"by": user.pk, "date": today, "link": prov.get("attestation")}
        j["provenance"] = prov
        PlaceGeom.objects.filter(pk=g.pk).update(jsonb=j)


def restore(retracted, by=None):
    """Put a retracted row back on its record, exactly as it was (a new ``PlaceGeom`` pk).
    Refused unless the Link stands again, the source's terms still allow it, the dataset is not
    declared sensitive and the record has no location now. Once only."""
    from django.contrib.gis.geos import GEOSGeometry
    from datasets.location_adoption import is_sensitive
    from datasets.models_matching import RetractedLocation
    from places.models import PlaceGeom
    with transaction.atomic():
        r = RetractedLocation.objects.select_for_update().select_related("dataset").get(pk=retracted.pk)
        if r.restored_at:
            raise RestoreRefused("already restored")
        if is_sensitive(r.dataset):
            raise RestoreRefused("the dataset is declared sensitive")
        if PlaceGeom.objects.filter(place_id=r.place_id).exists():
            raise RestoreRefused("the record has a location")
        prov = (r.jsonb or {}).get("provenance") or {}
        atts, cands = _standing({prov.get("attestation")}, {prov.get("candidate")})
        entries = registry_entries({prov.get("source")})
        why = _why_not(prov, atts, cands, entries)
        if why:
            raise RestoreRefused(why)
        g = PlaceGeom.objects.create(place_id=r.place_id, src_id=r.src_id, jsonb=r.jsonb,
                                     geom=GEOSGeometry(r.geom_wkt, srid=4326) if r.geom_wkt else None,
                                     geom_src_id=r.geom_src or None, task_id=r.task_id or None,
                                     reviewer_id=r.reviewer_id or getattr(by, "pk", None))
        if r.created:
            PlaceGeom.objects.filter(pk=g.pk).update(create_date=r.created)
        RetractedLocation.objects.filter(pk=r.pk).update(restored_at=timezone.now(), restored_geom_pk=g.pk)
        _after_change({r.dataset_id: (bool(r.dataset.public), {r.place_id})})  # map data: PlaceGeom's own signal
    return g


# ---------------------------------------------------------------------------
# Counts for the QC report
# ---------------------------------------------------------------------------

def counts(ds):
    """Adopted (per source), placed, kept-after-undo and retracted counts for ``ds``, plus the
    rows the rule says should not be there. A constant number of queries."""
    from datasets.models_matching import RetractedLocation
    from places.models import PlaceGeom
    rows = list(PlaceGeom.objects.filter(place__dataset=ds.label,
                                         jsonb__provenance__kind__in=(ADOPTED, PLACED))
                .only("pk", "place_id", "jsonb", "task_id"))
    by_source, placed, kept = {}, 0, 0
    for g in rows:
        p = provenance(g) or {}
        if p.get("kind") == ADOPTED:
            ns = str(p.get("source") or "")
            by_source[ns] = by_source.get(ns, 0) + 1
            kept += 1 if p.get("kept") else 0
        else:
            placed += 1
    problems = audit(rows=[g for g in rows if (provenance(g) or {}).get("kind") == ADOPTED])
    retracted = RetractedLocation.objects.filter(dataset=ds, restored_at__isnull=True).count()
    return {"rows": rows, "by_source": by_source, "placed": placed, "kept": kept,
            "retracted": retracted, "problems": problems}
