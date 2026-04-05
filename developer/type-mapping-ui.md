# WHG Type Mapping UI — Build Specification

> **Audience:** Coding agents (Claude/Copilot) working on the WHG Django
> codebase (`whg3` repository).
>
> **Date:** April 2026
>
> **Goal:** Build a Django-based admin UI allowing any logged-in users
> to map place types from **GeoNames**, **Wikidata**, **OSM**, and
> **OHM** vocabularies to **Getty AAT** (Art & Architecture Thesaurus)
> concepts.  All data is read from and written to the production
> **Elasticsearch `types` index** via the CRC gateway — **no Django
> models or database tables** are used for mapping storage, but a record
> should be kept in the Django DB of each mapping decision and which
> user made it.  For simplicity, the most recent mapping is considered
> authoritative for the ES index.
>
> **Status:** Draft.

---

## 1. Context & Design Rationale

WHG is ontology-agnostic: any contributor may use any type vocabulary.
However AAT is the **platform default** — it drives the search filter
type-tree widget (v3.5 Search UI) and the planned reconciliation
pipeline.  The indexing repository maintains a mapping between each
authority's native type vocabulary and AAT.

Previously, automated scripts (`aat_mapper.py`) and static lookup
tables in the indexing repo handled the bulk of mapping.  A CLI
human-in-the-loop (HITL) review tool was prototyped and has now been
removed.  The new approach replaces it with a web UI in the Django
app, so that mapping can be done collaboratively by any authenticated
user.

### What already exists

| Component | Location | Notes |
|-----------|----------|-------|
| **ES `types` index** | Production ES (via CRC gateway) | ~5,800 AAT concepts with `term`, `note`, `fclasses`, `path`, `ancestors`, `depth`, cross-vocab fields (`gn_fcodes`, `wd_qids`, `osm_tags`, `ohm_tags`, `pleiades_types`) |
| **ES `types` schema** | `indexing/schemas/types.json` | See §2.1 |
| **`placetypes` Django app** | `whg3/placetypes/` | Existing app with `Type` model (PostgreSQL mirror), tree-widget views, `aat_utils.py`. The mapping UI should be added here. |
| **Type-tree widget** | `placetypes/views.py` + `placetypes/urls.py` | JSON endpoints `/types/tree/`, `/types/tree/<aat_id>/`, `/types/tree/search/` |
| **CRC gateway client** | `api/crc_client.py` | Pattern for talking to the gateway (requests-based, sync, fail-safe) |
| **Gateway URL** | `settings.CRC_GATEWAY_URL` = `http://index.whgazetteer.org:9200` | Direct ES access with `http_auth=('elastic', ELASTIC_PASSWORD)` |
| **ES connection** | `settings.ES_CONN` | `elasticsearch8.Elasticsearch` instance already configured in `local_settings.py` |
| **Base template** | `main/templates/main/base_webpack.html` | All pages extend this; uses webpack for JS bundling |

### What must be built

A new set of views + templates in the `placetypes` app providing:

1. A **dashboard page** with four tabs — GeoNames, Wikidata, OSM, OHM
   — listing source types that need AAT mappings, sortable by record
   count.
2. An **inline mapping interface** where an authenticated user can search the
   AAT type tree and assign a mapping to a source type.
3. **API endpoints** for reading/writing mappings to the ES `types`
   index (AJAX, no page reloads).

### Reference data files

The indexing repository (`indexing/typesystem/data/`) contains
pre-built JSON vocabularies with labels, descriptions, and usage
counts.  Copies of these files are already present in the Django
repository at `placetypes/data/`:

| File | Source | Entries | Size | Description |
|------|--------|---------|------|-------------|
| `osm.json` | `scripts/fetch_osm_taginfo.py` (TagInfo API) | ~3,300 tag values across 17 tag keys | 556 KB | OSM tag values with counts, wiki descriptions, grouped by tag key (`place`, `natural`, `water`, etc.) |
| `ohm.json` | `scripts/fetch_osm_taginfo.py` (OHM Overpass) | ~900 tag values across 22 tag keys | 96 KB | Same structure as OSM; OHM-specific tag value distributions |
| `pleiades.json` | `build_pleiades_types.py` (Pleiades API) | 220 active + 9 deprecated | 92 KB | Pleiades vocabulary with AAT `same_as` URIs already populated |

**GeoNames** and **Wikidata** data files are built dynamically from
the ES `places` index (by `build_geonames_types.py` and
`build_wikidata_types.py`).  They are not currently present because
they require CRC ES access to build.  For the mapping UI, counts for
these vocabularies should be fetched live from ES via aggregation
queries (see §5.1 and §5.2).  Alternatively, if the files are built
on the CRC and copied to the Django repo, they can be used as a static
fallback.

**Refreshing data files:**  Run the build scripts in the indexing
repository on the CRC VM (where ES is accessible), then re-copy the
resulting JSON files to `whg3/placetypes/data/`.  The build scripts
should **not** be moved to the Django repo — they depend on the
indexing repo's `typesystem.es_client` and CRC infrastructure.

Each JSON file uses one of two structures:

**Tag-keyed (OSM/OHM):**
```json
{
  "place": {
    "tier": "tier1_current",
    "total_features": 9280034,
    "values": [
      {
        "value": "hamlet",
        "count": 2087218,
        "description": "A smaller rural community...",
        "in_wiki": true
      }, ...
    ]
  },
  "natural": { ... },
  ...
}
```

**Flat list (Wikidata/Pleiades):**
```json
{
  "namespace": "wikidata",
  "total_distinct_types": 4500,
  "values": [
    {"value": "Q515", "label": "city", "description": "large settlement", "count": 50000},
    ...
  ]
}
```

---

## 2. Elasticsearch `types` Index

### 2.1 Schema (key fields)

```
aat_id          integer     — AAT numeric ID (e.g. 300008347)
parent_id       integer     — parent AAT ID
term            text        — preferred English label ("inhabited places")
  .keyword      keyword     — exact match sub-field
  .folded       text        — lowercase + asciifolding sub-field
term_full       keyword     — full qualified term
note            text        — AAT scope note (English)
labels          object      — multilingual labels (disabled/not indexed)
notes           object      — multilingual notes (disabled/not indexed)
path            keyword     — materialized path ("300264550.300008346.300008347")
ancestors       integer[]   — ancestor AAT IDs
depth           integer     — tree depth (0 = root)
fclasses        keyword[]   — GeoNames-style feature class letters (A,P,S,H,T,L,R,U)
is_place_type   boolean     — true if this is a valid WHG place type
gn_fcodes       keyword[]   — GeoNames feature codes mapped here (e.g. "P.PPL", "A.ADM1")
wd_qids         keyword[]   — Wikidata Q-items mapped here (e.g. "Q515", "Q3957")
osm_tags        keyword[]   — OSM tag strings mapped here (e.g. "place=city")
ohm_tags        keyword[]   — OHM tag strings mapped here
pleiades_types  keyword[]   — Pleiades type identifiers mapped here
indexed_at      date        — last indexed timestamp
```

**Document `_id` format:** `aat:{aat_id}` (e.g. `aat:300008347`).

### 2.2 Cross-vocabulary mapping fields

The mapping UI reads and writes the following array fields on AAT type
documents.  Each field is a `keyword[]` array listing all
source-vocabulary identifiers that map to that AAT concept.

| ES field | Vocabulary | Identifier format | Example values |
|----------|-----------|-------------------|----------------|
| `gn_fcodes` | GeoNames | `{fclass}.{fcode}` | `"P.PPL"`, `"A.ADM1"`, `"H.STM"` |
| `wd_qids` | Wikidata | `Q{number}` | `"Q515"`, `"Q3957"` |
| `osm_tags` | OpenStreetMap | `{tag_key}={value}` | `"place=city"`, `"natural=peak"` |
| `ohm_tags` | OpenHistoricalMap | `{tag_key}={value}` | `"place=city"`, `"historic=castle"` |

**Example:** AAT concept `300008347` ("inhabited places") currently has:

```json
{
  "gn_fcodes": ["P.PPL", "P.PPLA", "P.PPLA2", "P.PPLA3", "P.PPLA4",
                "P.PPLA5", "P.PPLC", "P.PPLL", "P.PPLR", "P.PPLS", "P.PPLX"],
  "wd_qids": ["Q486972"],
  "osm_tags": ["place=hamlet", "place=locality", "place=village"],
  "ohm_tags": ["place=village"]
}
```

To **add** a mapping: append the source identifier to the array and
issue an ES partial `_update` (script or doc merge).

To **remove** a mapping: remove the identifier from the array and
issue an ES partial `_update`.

### 2.3 Accessing ES from Django

The Django app already has a configured `ES_CONN`
in `settings.ES_CONN`.  Check whether v8 or v9 is used, and use this directly for reads and
writes to the `types` index.  The gateway at `CRC_GATEWAY_URL` is
the same host:port — both route to the same ES backend.

**Important:** All ES operations should use `settings.ES_CONN`
(the pre-configured client with auth), not construct a new client.

---

## 3. Source Vocabularies to Map

### 3.1 GeoNames Feature Codes

GeoNames uses a two-level classification: **feature class** (single
letter: A, H, L, P, R, S, T, U, V) → **feature code** (e.g. PPL,
PPLA, ADM1, STM).

The full list of ~680 feature codes is published at:
`https://download.geonames.org/export/dump/featureCodes_en.txt`

Each code has a short name and description.  The mapping UI should
show the code, name, and description alongside a count of how many
places in the ES `places` index use that code.

**Source label format in ES `types` index:**  `{fclass}.{fcode}`
(e.g. `P.PPL`, `A.ADM1`, `H.STM`).  These are stored in the
`gn_fcodes` array field on the target AAT document.

### 3.2 Wikidata P31 (instance-of) Q-items

Wikidata places use the P31 ("instance of") property to declare
their type.  The ~11M Wikidata records in WHG reference ~4,500
distinct Q-items as types.

Each Q-item has a label (e.g. Q515 = "city", Q3957 = "town").
Labels should be fetched from ES or the Wikidata API.

**Source identifier format in ES `types` index:** `Q{number}`
(e.g. `Q515`).  These are stored in the `wd_qids` array field on
the target AAT document.

### 3.3 OSM Tags

OpenStreetMap uses a `key=value` tag schema.  WHG indexes places from
17 tag keys (see `osm.json`), each containing up to 200 distinct
values — ~3,300 tag values in total.

The reference data file `placetypes/data/osm.json` provides values
with counts and wiki descriptions, grouped by tag key.  The tag keys
are:

| Tier | Tag keys |
|------|----------|
| Tier 1 (current in WHG) | `place`, `natural`, `water`, `waterway`, `historic`, `landuse` |
| Tier 2 (high value) | `amenity`, `tourism`, `leisure`, `man_made`, `boundary`, `military` |
| Tier 3 (medium value) | `aeroway`, `railway`, `geological`, `power` |
| Building | `building` (allowlisted subtypes only) |

Many OSM values are generic/uninformative (`yes`, `no`, `other`,
`fixme`, etc.) — the UI should visually de-emphasise these and
sort them below meaningful values.

**Source label format in ES `types` index:** `{tag_key}={value}`
(e.g. `place=city`, `natural=peak`, `historic=castle`).  These are
stored in the `osm_tags` array field on the target AAT document.

**Pre-existing curated mappings:** The indexing repository's
`aat_mapper.py` contains ~210 hand-curated `OSM_OHM_STATIC_MAPPINGS`
covering all the major OSM tag values.  These have already been
merged into the ES `types` index via `merge_mappings.py`.  The
mapping UI should show these as existing mappings and allow users to
review, change, or confirm them.

### 3.4 OHM Tags

OpenHistoricalMap uses the same tag schema as OSM.  WHG indexes
~800K OHM places.  The reference data file `placetypes/data/ohm.json`
has ~900 tag values across 22 tag keys.

OHM has excellent temporal coverage (`start_date`/`end_date`) and
many historic-specific tag values (`historic=castle`,
`historic=monastery`, etc.) that overlap with OSM's `historic=*` key.

**Source label format:** Same as OSM: `{tag_key}={value}`.  Stored in
the `ohm_tags` array field on the target AAT document.

Because OHM and OSM share the same tag schema, many mappings will be
identical.  The UI should support a **"copy from OSM"** action that
copies all OSM mappings to OHM for tag values that appear in both
vocabularies.  Users can then adjust individual OHM mappings as
needed.

---

## 4. UI Specification

### 4.1 Access control

- The mapping UI must be accessible to **any authenticated user**
  (`user.is_authenticated`).
- Use `@login_required` from `django.contrib.auth.decorators`.
- URL prefix: `/types/mapping/` (within the existing `placetypes`
  app URL namespace).

### 4.2 Page: Mapping Dashboard

**URL:** `/types/mapping/`

**Template:** `placetypes/templates/placetypes/mapping_dashboard.html`

**Layout:**

Four-tab interface (Bootstrap 5 tabs, matching existing WHG styling):

- **Tab 1: GeoNames** — table of all GeoNames feature codes
- **Tab 2: Wikidata** — table of all Wikidata P31 Q-items
- **Tab 3: OSM** — table of all OSM tag values, grouped by tag key
- **Tab 4: OHM** — table of all OHM tag values, grouped by tag key

Each tab contains a table with the following columns:

| Column | Description |
|--------|-------------|
| Source ID | The feature code (e.g. `P.PPL`), Q-item (e.g. `Q515`), or tag string (e.g. `place=city`) |
| Tag Key | *(OSM/OHM only)* The tag key (e.g. `place`, `natural`, `historic`) — used for grouping/filtering |
| Label | Human-readable name (e.g. "populated place" / "city" / "hamlet") |
| Description | Short description text (from GeoNames or TagInfo wiki) |
| Count | Number of WHG places using this type |
| AAT Mapping | Current mapping (if any) — shows `aat:{id} {term}`, or "unmapped" badge |
| Actions | "Map" / "Change" / "Remove" buttons |

**OSM/OHM tab extras:**
- A **tag key filter** dropdown above the table (e.g. show only
  `place=*` values, or only `historic=*` values)
- A **"Copy OSM→OHM"** button on the OHM tab that copies all OSM
  mappings to matching OHM tag values

**Table features:**
- Sortable by any column (client-side, using a lightweight lib or
  vanilla JS)
- Filterable: text search box above the table
- Pagination or virtual scrolling for large lists
- Visual distinction: mapped rows have a subtle green background;
  unmapped rows have a yellow/amber "unmapped" badge
- Count column shows the number of places in ES that use this type
  (fetched via aggregation)

### 4.3 Mapping Dialog (inline or modal)

When the user clicks "Map" or "Change" on a row:

1. A **modal dialog** or inline expansion opens.
2. The dialog shows:
   - The source type being mapped (ID + label + description)
   - A **search box** that queries the ES `types` index for AAT
     concepts (same search logic as the existing type-tree widget)
   - Search results displayed as a list of AAT concepts with:
     - AAT ID, preferred term, scope note snippet, fclass badges
   - A "Browse tree" option that renders the existing type-tree
     widget for navigation
3. The user selects an AAT concept and clicks "Save Mapping".
4. An AJAX POST updates the ES `types` index (adds the source
   identifier to the `gn_fcodes` or `wd_qids` array on the target
   AAT document).
5. The table row updates to reflect the new mapping without a full
   page reload.

### 4.4 Removing a mapping

"Remove" button on a mapped row → confirmation dialog → AJAX DELETE
→ removes the source identifier from the array field on the AAT doc.

---

## 5. Backend Implementation

### 5.1 Data loading: GeoNames feature codes

Fetch the GeoNames feature code list once and cache it.  Options:

**Option A (recommended):** Query the ES `places` index for a terms
aggregation on `types.sourceLabel` filtered to namespace `gn`.  This
gives codes + counts in a single query.  Supplement with labels from
the static GeoNames feature codes file.

**Option B:** Load the feature codes file from the indexing repo's
`typesystem/data/geonames.json` (but this file lives in the other
repo, so either copy it or fetch via API).

For labels, use the GeoNames `featureCodes_en.txt` file:
```
http://download.geonames.org/export/dump/featureCodes_en.txt
```

Cache the result in Django's cache framework (1-hour TTL).

### 5.2 Data loading: Wikidata Q-items

Query the ES `places` index for a terms aggregation on
`types.identifier` filtered to namespace `wd`.  This gives Q-items
+ counts.  Supplement with labels by querying the Wikidata API or
from the type data file.

### 5.3 Data loading: OSM tag values

Load from the reference data file `placetypes/data/osm.json`.  This
file is grouped by tag key, each containing a `values` array with
`value`, `count`, `description`, and `in_wiki` fields.

Build the source identifier for each value as `{tag_key}={value}`
(e.g. `place=city`).

```python
def get_osm_types():
    data_path = Path(__file__).parent / "data" / "osm.json"
    with open(data_path) as f:
        data = json.load(f)

    _, _, osm_map, _ = get_current_mappings()
    items = []
    for tag_key, tag_data in data.items():
        if not isinstance(tag_data, dict):
            continue
        for entry in tag_data.get("values", []):
            source_id = f"{tag_key}={entry['value']}"
            items.append({
                "source_id": source_id,
                "tag_key": tag_key,
                "label": entry.get("value", ""),
                "description": entry.get("description", ""),
                "count": entry.get("count", 0),
                "in_wiki": entry.get("in_wiki", False),
                "mapping": osm_map.get(source_id),
            })
    return items
```

### 5.4 Data loading: OHM tag values

Identical logic to OSM but using `placetypes/data/ohm.json` and
looking up mappings from the `ohm_tags` field.

### 5.5 Loading current AAT mappings

Query the ES `types` index for all documents that have non-empty
cross-vocabulary fields.  Build four reverse lookups:

```python
# source_id → {aat_id, aat_term}
gn_mappings = {}  # e.g. {"P.PPL": {"aat_id": 300008347, "aat_term": "inhabited places"}}
wd_mappings = {}  # e.g. {"Q515": {"aat_id": 300008389, "aat_term": "cities"}}
osm_mappings = {} # e.g. {"place=city": {"aat_id": 300008389, "aat_term": "cities"}}
ohm_mappings = {} # e.g. {"place=city": {"aat_id": 300008389, "aat_term": "cities"}}
```

Use an ES search with `exists` filter:

```python
es = settings.ES_CONN
resp = es.search(
    index="types",
    query={"bool": {"should": [
        {"exists": {"field": "gn_fcodes"}},
        {"exists": {"field": "wd_qids"}},
        {"exists": {"field": "osm_tags"}},
        {"exists": {"field": "ohm_tags"}},
    ], "minimum_should_match": 1}},
    source=["aat_id", "term", "gn_fcodes", "wd_qids", "osm_tags", "ohm_tags"],
    size=10000,
)
```

### 5.6 AAT concept search endpoint

**URL:** `GET /types/mapping/api/search/?q=...`

Queries the ES `types` index using the same combined bool/should
strategy as the existing type-tree search.  Returns JSON:

```json
[
  {
    "aat_id": 300008347,
    "term": "inhabited places",
    "note": "Places where people live or have lived...",
    "fclasses": ["P"],
    "path": "300264550.300008346.300008347"
  },
  ...
]
```

**Implementation:** Reuse the search logic from `placetypes/aat_utils.py`
or query ES directly with a multi-match on `term.folded` + `term.keyword`
+ `note`.

### 5.7 Save mapping endpoint

**URL:** `POST /types/mapping/api/save/`

**Request body (JSON):**

```json
{
  "source_vocab": "geonames",       // "geonames", "wikidata", "osm", or "ohm"
  "source_id": "P.PPL",             // feature code, Q-item, or tag string
  "aat_id": 300008347                // target AAT concept ID
}
```

**Implementation:**

1. Determine the ES field name from the vocabulary:
   - `"geonames"` → `gn_fcodes`
   - `"wikidata"` → `wd_qids`
   - `"osm"` → `osm_tags`
   - `"ohm"` → `ohm_tags`
2. Check if `source_id` is already mapped to a *different* AAT concept.
   If so, remove it from the old concept first.
3. Add `source_id` to the target AAT document's array field using an
   ES update script:

```python
es = settings.ES_CONN
VOCAB_FIELD_MAP = {
    "geonames": "gn_fcodes",
    "wikidata": "wd_qids",
    "osm": "osm_tags",
    "ohm": "ohm_tags",
}
field = VOCAB_FIELD_MAP[source_vocab]

# Remove from old concept (if re-mapping)
if old_aat_id:
    es.update(
        index="types",
        id=f"aat:{old_aat_id}",
        script={
            "source": f"ctx._source.{field}.remove(ctx._source.{field}.indexOf(params.val))",
            "params": {"val": source_id}
        }
    )

# Add to new concept
es.update(
    index="types",
    id=f"aat:{aat_id}",
    script={
        "source": f"""
            if (ctx._source.{field} == null) ctx._source.{field} = [];
            if (!ctx._source.{field}.contains(params.val)) ctx._source.{field}.add(params.val);
        """,
        "params": {"val": source_id}
    }
)
```

**Response:** `200 OK` with `{"status": "ok", "aat_id": 300008347, "aat_term": "inhabited places"}`

### 5.8 Remove mapping endpoint

**URL:** `POST /types/mapping/api/remove/`

**Request body (JSON):**

```json
{
  "source_vocab": "geonames",       // "geonames", "wikidata", "osm", or "ohm"
  "source_id": "P.PPL",
  "aat_id": 300008347
}
```

**Implementation:** ES update script to remove `source_id` from the
array field on the AAT document.

**Response:** `200 OK` with `{"status": "ok"}`

### 5.9 Statistics endpoint

**URL:** `GET /types/mapping/api/stats/`

Returns mapping coverage statistics:

```json
{
  "geonames": {"total": 680, "mapped": 52, "unmapped": 628},
  "wikidata": {"total": 4500, "mapped": 180, "unmapped": 4320},
  "osm": {"total": 3317, "mapped": 210, "unmapped": 3107},
  "ohm": {"total": 906, "mapped": 85, "unmapped": 821}
}
```

### 5.10 Copy OSM→OHM endpoint

**URL:** `POST /types/mapping/api/copy-osm-to-ohm/`

Copies all OSM mappings to OHM for tag values that exist in both
vocabularies.  For each OSM source_id that is mapped to an AAT
concept, check if the same tag string exists in the OHM vocabulary.
If so, add it to the same AAT concept's `ohm_tags` array (unless
already present).

Returns `{"status": "ok", "copied": 85, "skipped": 12}`.

---

## 6. File Structure

All new files go in the existing `placetypes` app:

```
placetypes/
├── aat_config.py               # existing — AAT configuration
├── aat_utils.py                # existing — tree utilities
├── admin.py                    # existing
├── apps.py                     # existing
├── models.py                   # existing — NOT modified (no new models)
├── urls.py                     # MODIFIED — add mapping URL patterns
├── views.py                    # existing — tree widget views
├── views_mapping.py            # NEW — mapping dashboard + API views
├── mapping_utils.py            # NEW — ES query helpers for mappings
├── data/                       # reference data files (already present)
│   ├── osm.json                # from indexing/typesystem/data/
│   ├── ohm.json                # from indexing/typesystem/data/
│   └── pleiades.json           # from indexing/typesystem/data/
├── templates/
│   └── placetypes/
│       ├── mapping_dashboard.html   # NEW — main mapping page
│       └── mapping_modal.html       # NEW — AAT search/select modal (partial)
├── static/
│   └── placetypes/
│       ├── mapping.js               # NEW — JS for the mapping UI
│       └── mapping.css              # NEW — styles for the mapping UI
└── migrations/                 # NO new migrations needed
```

---

## 7. URL Routing

Add to `placetypes/urls.py`:

```python
# placetypes/urls.py
from django.urls import path
from . import views
from . import views_mapping

app_name = 'placetypes'

urlpatterns = [
    # Existing type-tree endpoints
    path('tree/', views.type_tree, name='type-tree-roots'),
    path('tree/search/', views.type_tree_search, name='type-tree-search'),
    path('tree/<int:aat_id>/', views.type_tree, name='type-tree-children'),

    # NEW: Mapping UI
    path('mapping/', views_mapping.mapping_dashboard, name='mapping-dashboard'),

    # NEW: Mapping API (AJAX)
    path('mapping/api/geonames/', views_mapping.api_geonames_types, name='mapping-api-geonames'),
    path('mapping/api/wikidata/', views_mapping.api_wikidata_types, name='mapping-api-wikidata'),
    path('mapping/api/osm/', views_mapping.api_osm_types, name='mapping-api-osm'),
    path('mapping/api/ohm/', views_mapping.api_ohm_types, name='mapping-api-ohm'),
    path('mapping/api/search/', views_mapping.api_aat_search, name='mapping-api-search'),
    path('mapping/api/save/', views_mapping.api_save_mapping, name='mapping-api-save'),
    path('mapping/api/remove/', views_mapping.api_remove_mapping, name='mapping-api-remove'),
    path('mapping/api/copy-osm-to-ohm/', views_mapping.api_copy_osm_to_ohm, name='mapping-api-copy-osm-ohm'),
    path('mapping/api/stats/', views_mapping.api_mapping_stats, name='mapping-api-stats'),
]
```

The app is already mounted at `/types/` in `whg/urls.py`:
```python
path('types/', include('placetypes.urls')),
```

So the mapping dashboard will be at `/types/mapping/`.

---

## 8. Views Implementation Guide

### 8.1 `views_mapping.py` — View Functions

```python
# placetypes/views_mapping.py
"""
Type mapping UI — allows authenticated users to map GeoNames and Wikidata
types to AAT concepts via the ES types index.
"""

import json
import logging

from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_GET, require_POST

from .mapping_utils import (
    get_geonames_types,
    get_wikidata_types,
    get_osm_types,
    get_ohm_types,
    get_current_mappings,
    search_aat_types,
    save_mapping,
    remove_mapping,
    copy_osm_to_ohm,
    get_mapping_stats,
)

logger = logging.getLogger(__name__)


@login_required
def mapping_dashboard(request):
    """Main mapping dashboard page."""
    stats = get_mapping_stats()
    return render(request, 'placetypes/mapping_dashboard.html', {
        'stats': stats,
    })


@login_required
@require_GET
def api_geonames_types(request):
    """Return all GeoNames feature codes with current mappings."""
    types = get_geonames_types()
    return JsonResponse(types, safe=False)


@login_required
@require_GET
def api_wikidata_types(request):
    """Return all Wikidata P31 Q-items with current mappings."""
    types = get_wikidata_types()
    return JsonResponse(types, safe=False)


@login_required
@require_GET
def api_osm_types(request):
    """Return all OSM tag values with current mappings."""
    tag_key = request.GET.get('tag_key')  # optional filter
    types = get_osm_types(tag_key_filter=tag_key)
    return JsonResponse(types, safe=False)


@login_required
@require_GET
def api_ohm_types(request):
    """Return all OHM tag values with current mappings."""
    tag_key = request.GET.get('tag_key')
    types = get_ohm_types(tag_key_filter=tag_key)
    return JsonResponse(types, safe=False)


@login_required
@require_GET
def api_aat_search(request):
    """Search AAT concepts in the types index."""
    q = request.GET.get('q', '').strip()
    if not q or len(q) < 2:
        return JsonResponse([], safe=False)
    results = search_aat_types(q)
    return JsonResponse(results, safe=False)


@login_required
@require_POST
def api_save_mapping(request):
    """Save a type → AAT mapping to the ES types index."""
    try:
        data = json.loads(request.body)
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON'}, status=400)

    source_vocab = data.get('source_vocab')
    source_id = data.get('source_id')
    aat_id = data.get('aat_id')

    if not all([source_vocab, source_id, aat_id]):
        return JsonResponse({'error': 'Missing required fields'}, status=400)
    if source_vocab not in ('geonames', 'wikidata', 'osm', 'ohm'):
        return JsonResponse({'error': 'Invalid source_vocab'}, status=400)

    try:
        result = save_mapping(source_vocab, source_id, int(aat_id))
        return JsonResponse(result)
    except Exception as e:
        logger.exception("Error saving mapping")
        return JsonResponse({'error': str(e)}, status=500)


@login_required
@require_POST
def api_remove_mapping(request):
    """Remove a type → AAT mapping from the ES types index."""
    try:
        data = json.loads(request.body)
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON'}, status=400)

    source_vocab = data.get('source_vocab')
    source_id = data.get('source_id')
    aat_id = data.get('aat_id')

    if not all([source_vocab, source_id, aat_id]):
        return JsonResponse({'error': 'Missing required fields'}, status=400)

    try:
        result = remove_mapping(source_vocab, source_id, int(aat_id))
        return JsonResponse(result)
    except Exception as e:
        logger.exception("Error removing mapping")
        return JsonResponse({'error': str(e)}, status=500)


@login_required
@require_GET
def api_mapping_stats(request):
    """Return mapping coverage statistics."""
    stats = get_mapping_stats()
    return JsonResponse(stats)


@login_required
@require_POST
def api_copy_osm_to_ohm(request):
    """Copy all OSM mappings to OHM for overlapping tag values."""
    try:
        result = copy_osm_to_ohm()
        return JsonResponse(result)
    except Exception as e:
        logger.exception("Error copying OSM→OHM mappings")
        return JsonResponse({'error': str(e)}, status=500)
```

### 8.2 `mapping_utils.py` — ES Query Helpers

This module contains all Elasticsearch interaction logic.  Key
functions:

#### `get_geonames_types()`

1. Query ES `places` index with a nested terms aggregation on
   `types.sourceLabel` where `types.label` is a GeoNames feature
   class letter (A, H, L, P, R, S, T, U, V).  Or alternatively,
   filter by namespace prefix on `place_id` (`gn:*`) and aggregate
   `types.sourceLabel`.  This gives `{fcode: count}` pairs.
2. Supplement with labels from the GeoNames `featureCodes_en.txt`
   file (download and cache locally, or embed the ~680 lines as a
   static data dict).
3. Load current AAT mappings by querying the `types` index for docs
   with non-empty `gn_fcodes` and building a reverse dict.
4. Return a list of dicts:

```python
[
    {
        "source_id": "P.PPL",
        "label": "populated place",
        "description": "a city, town, village, or other ...",
        "fclass": "P",
        "count": 5_432_100,
        "mapping": {"aat_id": 300008347, "aat_term": "inhabited places"} | None
    },
    ...
]
```

#### `get_wikidata_types()`

1. Query ES `places` index for a nested terms aggregation on
   `types.identifier` filtered to namespace `wd` (i.e.
   `place_id` starts with `wd:`).  Gives `{qid: count}` pairs.
2. For labels: either batch-query the Wikidata API, or use
   `types.sourceLabel` from ES, or store a cached label file.
3. Load current AAT mappings from `types` index `wd_qids` fields.
4. Return same shape as GeoNames but with Q-items.

#### `get_current_mappings()`

Query the `types` index for all docs with any cross-vocab fields
populated.  Build four reverse dicts:

```python
def get_current_mappings():
    es = settings.ES_CONN
    gn_map = {}   # "P.PPL" → {"aat_id": 300008347, "aat_term": "..."}
    wd_map = {}   # "Q515" → {"aat_id": 300008389, "aat_term": "..."}
    osm_map = {}  # "place=city" → {"aat_id": 300008389, "aat_term": "..."}
    ohm_map = {}  # "place=city" → {"aat_id": 300008389, "aat_term": "..."}

    resp = es.search(
        index="types",
        query={"bool": {"should": [
            {"exists": {"field": "gn_fcodes"}},
            {"exists": {"field": "wd_qids"}},
            {"exists": {"field": "osm_tags"}},
            {"exists": {"field": "ohm_tags"}},
        ], "minimum_should_match": 1}},
        source=["aat_id", "term", "gn_fcodes", "wd_qids", "osm_tags", "ohm_tags"],
        size=10000,
    )
    for hit in resp["hits"]["hits"]:
        src = hit["_source"]
        aat_id = src["aat_id"]
        aat_term = src.get("term", "")
        info = {"aat_id": aat_id, "aat_term": aat_term}
        for fc in src.get("gn_fcodes", []):
            gn_map[fc] = info
        for qid in src.get("wd_qids", []):
            wd_map[qid] = info
        for tag in src.get("osm_tags", []):
            osm_map[tag] = info
        for tag in src.get("ohm_tags", []):
            ohm_map[tag] = info

    return gn_map, wd_map, osm_map, ohm_map
```

#### `search_aat_types(query, limit=20)`

Query the `types` index with boosted bool/should:

```python
def search_aat_types(query, limit=20):
    es = settings.ES_CONN
    clean = query.replace("_", " ")

    resp = es.search(
        index="types",
        size=limit,
        query={
            "bool": {
                "must": [{"term": {"is_place_type": True}}],
                "should": [
                    {"term": {"term.keyword": {"value": clean, "boost": 30}}},
                    {"match_phrase_prefix": {"term.folded": {"query": clean, "boost": 10}}},
                    {"match": {"term.folded": {"query": clean, "operator": "and", "boost": 5}}},
                    {"multi_match": {
                        "query": clean,
                        "fields": ["term.folded^3", "note"],
                        "type": "most_fields",
                        "boost": 1,
                    }},
                ],
                "minimum_should_match": 1,
            }
        },
        source=["aat_id", "term", "note", "fclasses", "path"],
    )

    results = []
    for hit in resp["hits"]["hits"]:
        src = hit["_source"]
        results.append({
            "aat_id": src["aat_id"],
            "term": src.get("term", ""),
            "note": (src.get("note") or "")[:200],
            "fclasses": src.get("fclasses", []),
            "path": src.get("path", ""),
            "score": hit["_score"],
        })
    return results
```

#### `save_mapping(source_vocab, source_id, aat_id)`

```python
VOCAB_FIELD_MAP = {
    "geonames": "gn_fcodes",
    "wikidata": "wd_qids",
    "osm": "osm_tags",
    "ohm": "ohm_tags",
}

def save_mapping(source_vocab, source_id, aat_id):
    es = settings.ES_CONN
    field = VOCAB_FIELD_MAP[source_vocab]

    # 1. Find and remove from any existing AAT concept
    old_resp = es.search(
        index="types",
        query={"term": {field: source_id}},
        source=["aat_id"],
        size=10,
    )
    for hit in old_resp["hits"]["hits"]:
        old_aat_id = hit["_source"]["aat_id"]
        if old_aat_id != aat_id:
            es.update(
                index="types",
                id=hit["_id"],
                script={
                    "source": f"""
                        if (ctx._source.{field} != null) {{
                            ctx._source.{field}.remove(
                                ctx._source.{field}.indexOf(params.val)
                            );
                        }}
                    """,
                    "params": {"val": source_id},
                },
            )

    # 2. Add to the target AAT concept
    es.update(
        index="types",
        id=f"aat:{aat_id}",
        script={
            "source": f"""
                if (ctx._source.{field} == null) ctx._source.{field} = [];
                if (!ctx._source.{field}.contains(params.val)) {{
                    ctx._source.{field}.add(params.val);
                }}
            """,
            "params": {"val": source_id},
        },
    )

    # 3. Fetch the updated doc to return the term
    doc = es.get(index="types", id=f"aat:{aat_id}", source=["aat_id", "term"])
    return {
        "status": "ok",
        "aat_id": aat_id,
        "aat_term": doc["_source"].get("term", ""),
    }
```

#### `remove_mapping(source_vocab, source_id, aat_id)`

```python
def remove_mapping(source_vocab, source_id, aat_id):
    es = settings.ES_CONN
    field = VOCAB_FIELD_MAP[source_vocab]

    es.update(
        index="types",
        id=f"aat:{aat_id}",
        script={
            "source": f"""
                if (ctx._source.{field} != null) {{
                    ctx._source.{field}.remove(
                        ctx._source.{field}.indexOf(params.val)
                    );
                }}
            """,
            "params": {"val": source_id},
        },
    )
    return {"status": "ok"}
```

---

## 9. Template Guide

### 9.1 `mapping_dashboard.html`

Extends `main/base_webpack.html`.  Uses Bootstrap 5 (already loaded
in the base template).

**Key sections:**
- Page title + breadcrumbs
- Summary stats cards (total mapped / unmapped for each vocab)
- Bootstrap tabs: "GeoNames" | "Wikidata" | "OSM" | "OHM"
- Each tab contains a DataTable (or simple HTML table with JS sorting)
- Search/filter input above the table
- OSM/OHM tabs also have a tag-key dropdown filter
- Rows loaded via AJAX from the API endpoints (not server-rendered,
  to avoid blocking on ES queries)

**Modal for AAT selection:**
- A Bootstrap 5 modal (`#aatSearchModal`)
- Contains a search input, results list, and the existing type-tree
  widget (lazy-loaded children)
- "Select" button confirms the choice and triggers the save API call

### 9.2 JavaScript (`mapping.js`)

- On page load: fetch stats, then fetch the active tab's data via
  AJAX
- Tab switch: fetch that vocabulary's data if not already loaded
- Table rendering: build HTML rows from JSON, with sort/filter
- "Map" / "Change" click → open modal, populate search
- Modal search: debounced input → AJAX to `/types/mapping/api/search/`
  → render results
- "Select" in modal → AJAX POST to `/types/mapping/api/save/`
  → update row in place
- "Remove" click → confirm → AJAX POST to `/types/mapping/api/remove/`
  → update row in place
- Use `fetch()` with the Django CSRF token (read from cookie or
  `{% csrf_token %}` meta tag)

### 9.3 CSS (`mapping.css`)

- `.mapping-row--mapped { background: rgba(40, 167, 69, 0.05); }`
- `.mapping-row--unmapped .badge { background: #ffc107; }`
- `.aat-result { cursor: pointer; padding: 8px; border-bottom: 1px solid #eee; }`
- `.aat-result:hover { background: #f0f0f0; }`
- Scope note text styled with `font-size: 0.85em; color: #666;`

---

## 10. Navigation Integration

Add a link to the mapping dashboard in appropriate places:

1. **Admin sidebar or dashboard:** Add a card/link on the admin
   dashboard (`dashboard_admin.html`) pointing to `/types/mapping/`.
2. **Placetypes section:** If there's a placetypes admin page, add
   a prominent link.
3. **Authenticated-user visibility:** Wrap the link in
   `{% if user.is_authenticated %}...{% endif %}`.

---

## 11. Testing Checklist

- [ ] Dashboard loads with GeoNames tab active
- [ ] Switching to Wikidata tab loads Q-items
- [ ] Switching to OSM tab loads tag values grouped by tag key
- [ ] Switching to OHM tab loads OHM tag values
- [ ] OSM/OHM tag key dropdown filter works
- [ ] Table sorting works on all columns
- [ ] Text filter narrows the table
- [ ] "Map" opens modal with AAT search
- [ ] Searching "city" returns AAT candidates including "cities"
- [ ] Selecting a candidate and saving updates ES correctly
- [ ] Row visually updates to show the new mapping
- [ ] "Change" re-opens the modal for an already-mapped row
- [ ] "Remove" removes the mapping from ES
- [ ] "Copy OSM→OHM" button copies mappings for shared tag values
- [ ] Non-authenticated users get redirected to login
- [ ] Page works when ES is temporarily unreachable (graceful error)

---

## 12. Future Extensions (not in scope now)

- **Pleiades mapping** — using `pleiades_types` field (most already
  have AAT `same_as` URIs from the Pleiades vocabulary, so a mapping
  UI is lower priority; the data file `pleiades.json` is already
  copied to the Django repo for reference)
- **Bulk import** — upload a CSV of `source_id,aat_id` pairs
- **Audit log** — record who mapped what and when (would require a
  Django model or ES metadata field)
- **Gateway API for mappings** — expose a read-only endpoint on the
  CRC gateway so the indexing pipeline can consume the mappings
  directly from ES
- **Auto-suggest** — when mapping OSM/OHM types, auto-suggest the
  same AAT concept used for matching GeoNames codes (based on shared
  fclass)
- **Data file refresh** — a management command that fetches updated
  data files from the indexing repo or rebuilds them from ES

