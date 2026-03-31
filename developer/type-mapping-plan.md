# Plan: Unified Type Mapping — AAT ↔ GeoNames ↔ Wikidata

## Context

WHG indexes place records from multiple authority sources (GeoNames, Wikidata)
and from user-contributed datasets.  These sources type places using three
different vocabularies:

| Source | Typing vocabulary | Granularity |
|--------|-------------------|-------------|
| **AAT** | ~4 000 place-type concepts (hierarchical) | Fine — the canonical WHG vocabulary |
| **GeoNames** | 9 feature classes, ~680 feature codes | Medium (codes) / coarse (classes) |
| **Wikidata** | Open-ended Q-items as `P31` (instance of) values | Extremely fine but noisy |

The goal is a set of mapping tables that allow **any** of the three vocabularies
to be translated to the others, enabling:

- Automatic AAT classification of contributed data typed with Wikidata Q-ids
  or GeoNames feature codes.
- Augmentation of indexed GeoNames and Wikidata authority records in the
  `places` ES index with consistent AAT types.
- Query expansion at the CRC Gateway: a search filtered by an AAT type can
  also match records that only carry a GeoNames fcode or Wikidata Q-id.

---

## Phase 1 — Models and bootstrap data (Django side)

### 1a. New models in `placetypes`

```
GeoNamesMapping
    aat_id        FK → Type
    gn_fcode      CharField       (e.g. "PPL", "ADM1")
    gn_fclass     CharField       (derived, e.g. "P", "A")
    confidence    enum             exact | broad | inferred
    source        enum             manual | auto_label | auto_wikidata
    reviewed      BooleanField

WikidataMapping
    aat_id        FK → Type
    wd_qid        CharField       (e.g. "Q515" = city)
    wd_label      CharField       (cached English label)
    confidence    enum             exact | broad | inferred
    source        enum             manual | auto_p1566 | auto_p1014 | auto_label
    reviewed      BooleanField
```

Both tables are many-to-many by nature (one AAT type may map to several
GeoNames codes or Wikidata items and vice versa).

### 1b. Automated bootstrap heuristics

**AAT ↔ GeoNames**

1. **Label matching** — Compare AAT `term` against GeoNames
   `featureCodes_en.txt` descriptions.  Normalise plurals, strip
   parenthetical qualifiers, apply fuzzy / token-overlap scoring.
2. **Wikidata bridging** — Query Wikidata SPARQL for items that carry
   *both* `P1566` (GeoNames ID) and `P1014` (AAT ID).  Where a
   Wikidata item links a GeoNames entity to an AAT concept, infer
   a fcode ↔ AAT mapping from the GeoNames entity's feature code.
3. **Hierarchy propagation** — If an AAT leaf has no direct match,
   inherit the most-specific fcode(s) of its nearest mapped ancestor.

**AAT ↔ Wikidata**

1. **Direct P1014 links** — ~1 200 Wikidata items carry a `P1014`
   (Getty AAT ID) property.  Bulk-fetch via SPARQL and create exact
   mappings.
2. **P279 (subclass-of) walk** — For Wikidata type Q-ids commonly
   seen in WHG data (e.g. `Q515` = city, `Q3957` = town, `Q532` =
   village), walk `P279` upward until a Q-id with a `P1014` link is
   found.  Record these with `confidence = broad`.
3. **Label matching** — For unmapped high-frequency Wikidata types,
   fuzzy-match the English label against AAT terms.  Flag as
   `confidence = inferred` for manual review.
4. **Frequency-prioritised** — Only Wikidata Q-ids actually
   occurring in the WHG `places` index (or submitted datasets) need
   to be mapped.  A management command can scan ES or recent
   ingestions to discover the working set.

### 1c. Management commands

| Command | Purpose |
|---------|---------|
| `sync_geonames_mapping` | Re-run GeoNames heuristics, preserve manual overrides |
| `sync_wikidata_mapping` | Re-run Wikidata heuristics, preserve manual overrides |
| `report_unmapped_types` | List Wikidata Q-ids / GeoNames fcodes seen in data but not yet mapped |

### 1d. Admin curation UI

A simple Django-admin list-filter view per mapping table where editors
can review, confirm, override, or reject auto-assigned mappings.  Filter
by `reviewed=False` and sort by confidence to prioritise work.

---

## Phase 2 — Ingestion-time type augmentation

### 2a. Contributed datasets

During LP/TSV ingestion, if a record's `types` field contains Wikidata Q-ids
or GeoNames fcodes but no AAT identifiers:

1. Look up the Q-id / fcode in the mapping tables.
2. Assign the highest-confidence AAT type(s) found.
3. Log cases where no mapping exists → feeds `report_unmapped_types`.

### 2b. Authority-source records (GeoNames, Wikidata in `places`)

A one-off (and periodically repeatable) bulk-update job that:

1. Queries the `places` ES index for records sourced from GeoNames or
   Wikidata that lack AAT type annotations.
2. For each, looks up the record's native type (fcode or P31 Q-id)
   in the mapping tables.
3. Writes the mapped AAT type identifier(s) into the record's `types`
   array in ES.

This ensures that AAT-based type filtering in the search UI covers
*all* indexed records, not only those that were originally AAT-typed.

---

## Phase 3 — Replicate to CRC Elasticsearch / Gateway

### 3a. ES indices on the CRC cluster

**`aat_types` index**

```json
{
  "aat_id": 300008389,
  "term": "cities",
  "parent_id": 300008347,
  "path": "300264550.300008346.300008347.300008389",
  "depth": 3,
  "fclasses": ["P"],
  "gn_fcodes": ["PPL", "PPLA", "PPLA2", "PPLA3", "PPLA4", "PPLC"],
  "wd_qids": ["Q515", "Q1549591"],
  "ancestors": [300264550, 300008346, 300008347]
}
```

**`type_mappings` index** (optional, for reverse lookups)

```json
{ "gn_fcode": "PPLA", "aat_ids": [300008389, 300008347] }
{ "wd_qid": "Q515",   "aat_ids": [300008389] }
```

### 3b. Sync command

`push_types_to_es` — bulk-indexes the full `Type` table plus both
mapping tables into the CRC ES cluster.  Run after each `sync_aat_types`,
`sync_geonames_mapping`, or `sync_wikidata_mapping`.

### 3c. Gateway-side query expansion

When a search request arrives with `fclasses=aat:300008347`:

1. Look up `aat:300008347` in the local `aat_types` index.
2. Retrieve all descendant `aat_id`s (via `path` prefix query) and
   their `gn_fcodes` and `wd_qids`.
3. Build an ES bool query that matches records by *any* of:
   - AAT identifier in the record's `types` array, **or**
   - GeoNames fcode in the record's `fcode` field, **or**
   - Wikidata Q-id in the record's `types` array.

This eliminates the round-trip to Django for type expansion and ensures
records typed *only* in GeoNames or Wikidata vocabulary still match
AAT-based filters.

### 3d. Cache invalidation

Use the ES index-alias swap pattern: push to a timestamped index, then
atomically swap the alias, so the Gateway always reads a consistent
snapshot.

---

## Phase 4 — UI enhancements

1. Extend tree-widget node tooltips to show mapped GeoNames codes and
   Wikidata items (e.g. "cities → PPL, PPLA, PPLC; Q515").
2. Allow search-results faceting by GeoNames feature code or Wikidata
   type as well as by AAT type.
3. Show mapping coverage stats in the admin dashboard.

---

## Estimated effort

| Phase | Scope | Effort |
|-------|-------|--------|
| 1a | Models | 1 day |
| 1b | Automated heuristics (both vocabularies) | 3–4 days |
| 1c | Management commands | 1 day |
| 1d | Admin curation UI | 1 day |
| 2a | Ingestion-time augmentation | 2 days |
| 2b | Bulk authority-record augmentation | 1–2 days |
| 3  | ES indices + Gateway expansion | 3–4 days |
| 4  | UI enhancements | 1–2 days |
