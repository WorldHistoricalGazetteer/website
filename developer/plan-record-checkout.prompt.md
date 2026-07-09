# Plan — Record-level gazetteer check-out ("Correct this record")

**Status:** IMPLEMENTING (2026-07-09). Realises plan-collaborativeCollections **§6.1** — the
record-level / partial check-out path so a contributor can fix a single field in a single published
place **without round-tripping the whole gazetteer**.

**Relates to:** plan-collaborativeCollections §6/§6.1, place#111, place#112 ("per-record check-out",
in that issue's title). Extends the whole-item check-out already shipped for Place Collections +
Gazetteer Groups.

---

## The use case

Contributors routinely want to make a **small correction to one published record** — a typo in a
name, a wrong coordinate, a bad date. Whole-dataset check-out is infeasible for large gazetteers
(hundreds of thousands of records won't fit in the browser; a full re-upload + full re-index to fix
one cell is wasteful and risky). So: **check out ONE place, edit it, publish just that record back,
re-index just that record.**

## Scope (this increment — deliberately minimal because it mutates live, indexed data)

- **One record at a time.** Check out a single `places.Place`, not a subset/whole dataset. (Filtered
  subset / bulk correction is a later extension.)
- **Editable fields = the two safest, most common corrections:**
  - **Primary name** — `Place.title` (+ the matching `PlaceName.toponym`/`jsonb.toponym`).
  - **Coordinate** — the representative **point**, for places whose geometry is a single point (or
    none → create one). Places with **complex geometry** (polygon / multipart) show the point
    read-only and disable coordinate editing (edit those in the dataset editor). This bounds the
    geometry-mutation surface.
- **Dates deferred.** `attestation_year` / `timespans` / `PlaceWhen` editing is a documented
  fast-follow — the temporal model is richer and riskier.

## Data model (verified)

- `makeDoc(place)` (elastic/es_utils.py) builds the ES doc: `title = place.title`; `names` from
  `PlaceName` (+ title appended); `geoms` from `PlaceGeom.jsonb` (GeoJSON, whitelisting
  `{type,coordinates,geometries,bbox}`). So a correction shows in search iff we update `Place.title`
  (name) and/or `PlaceGeom.geom`+`.jsonb` (coordinate), then re-index.
- Single-doc re-index: `settings.ES_CONN.index(index=settings.ES_PUB, id=str(place.id),
  body=json.dumps(makeDoc(place)))` — only when `place.idx_pub` (the place is actually in the pub index).

## Flow

1. **"Correct this record"** button on the place detail page (`/places/<pk>/detail`), gated on
   **beta** AND **dataset edit rights** (`Dataset.can_edit`: owner / `DatasetUser` collaborator /
   staff) — mirrors the Collection check-out gate.
2. `POST /reconciliation/checkout/place/<pk>/` → `checkout_place_record(place)` serialises the record
   into a lightweight snapshot `{record_id, dataset_label, title, names[], lng, lat, point_editable,
   ccodes}`; a new team-owned `WorkbenchProject(doc_type='place_record')` is stamped with
   `source_published_id=place.id` + `base_version` (a content hash of the record's editable state).
3. The **`wb-place-record`** editor (a small form) opens on `?project=<uuid>`.
4. **Publish** → `publish_place_record(project, user)`: optimistic guard (recompute record hash vs
   `base_version` → 409 on mismatch); apply the name/coordinate delta to `Place` + `PlaceName` +
   `PlaceGeom`; **targeted re-index** of that one ES doc; new baseline hash stored.

## Safety

- Hard gate: beta + `Dataset.can_edit` on the owning dataset (button visibility AND endpoint auth,
  one shared predicate).
- Optimistic content-hash guard at **record** granularity — refuse to clobber a concurrently-changed
  record.
- Minimal editable surface (name + point). Complex-geometry places can't have their coordinate
  mangled here.
- DB write is authoritative; re-index is best-effort (logged on failure — the DB stays correct and a
  later reindex fixes the doc). All in one transaction for the DB mutation.

## Files

**New:** `checkout.checkout_place_record`, `publish.publish_place_record`; `whg/webpack/js/wb-place-record.js`;
`main/templates/main/wb_place_record.html`; a `wb-place-record` webpack entry; view `wb_place_record_view`.
**Edited:** `workbench/models.py` (+`place_record` doc-type, migration); `workbench/doctypes.py`
(registry, `enabled=False` — created only via check-out); `workbench/views.py`+`urls.py`
(`checkout/place/<pk>/`); `datasets/models.py` (`Dataset.can_edit`); `places/views.py` +
`places/templates/places/place_detail.html` (gated button).

## Later extensions (not this increment)

Date/temporal editing; multi-field LPF (types, links, descriptions); **filtered-subset / bulk**
correction; streaming/chunked transfer + `navigator.storage.estimate()` capacity guards for genuine
large-file moves (§6.1); whole legacy-Gazetteer check-out with full re-accession.
