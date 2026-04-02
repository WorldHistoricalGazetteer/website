# CRC Gateway: Filter System Backend Specification

> **Audience:** Coding agents and developers working on the Pitt CRC
> ES/Gateway VM codebase (FastAPI, Elasticsearch, Python, Slurm).
>
> **Companion document:** `filtering-plan.md` describes the front-end
> implementation.  This document specifies the backend: Elasticsearch
> indexes, gateway API endpoints, request/response contracts, and data
> ingestion pipelines.
>
> **Date:** April 2026

---

## 1. System Overview

The CRC Gateway is a FastAPI application running on the University of
Pittsburgh CRC VM.  It sits in front of an Elasticsearch 9.x cluster
and provides a JSON API consumed exclusively by the WHG Django
application on DigitalOcean.

**Network:** Only the DO server can reach the gateway.  All requests
carry a Bearer token (`CRC_GATEWAY_API_KEY`) in the `Authorization`
header.

**Existing endpoints:** The gateway already serves `POST /api/search`,
`GET /api/suggest`, and `POST /api/reconcile` for the main places
index.  This specification adds the endpoints and indexes needed for the
search filter system.

---

## 2. Elasticsearch Indexes

### 2.1 `places` / `toponyms` (existing)

Already documented.  Mapping lives in `es_mappings_whg.json`.
Key fields used by the filter system:

| Field | ES Type | Notes |
|-------|---------|-------|
| `title` | `keyword` (normalised) | Primary toponym |
| `searchy` | `keyword` (normalised) | Lowercased variant list |
| `names.toponym` | `keyword` (normalised) | All name variants |
| `ccodes` | `keyword` | ISO-3166 country codes |
| `types.identifier` | `keyword` | AAT identifiers (e.g. `aat:300008347`) |
| `timespans` | `integer_range` | `{gte, lte}` year ranges |
| `geoms.location` | `geo_shape` | Place geometries |
| `fclasses` | `keyword` | Legacy GeoNames feature classes |
| `whg_id` | `long` | WHG cluster identifier |

No changes are required to this index.

---

### 2.2 `periodo_periods` (new)

Holds the full PeriodO dataset (~30 000 periods) with resolved
geometries, enabling text search + spatial intersection + temporal
range filtering.

#### 2.2.1 Document Schema

```json
{
  "id":                       "p0trgkv-25sx8",
  "chrononym":                "Iron Age",
  "chrononyms": [
    { "label": "Iron Age",      "lang": "en" },
    { "label": "Edad del Hierro", "lang": "es" }
  ],
  "authority_id":             "p0trgkv",
  "authority_label":          "Wikipedia (English)",
  "spatial_description":      "Eastern Mediterranean",
  "start_year":               -1200,
  "stop_year":                -550,
  "start_year_earliest":      -1200,
  "start_year_latest":        -1200,
  "stop_year_earliest":       -550,
  "stop_year_latest":         -550,
  "ccodes":                   ["GR", "TR", "SY"],
  "geometry":                 { "type": "MultiPolygon", "coordinates": [...] },
  "bbox":                     [-10.5, 30.0, 45.0, 45.0],
  "periodo_url":              "https://n2t.net/ark:/99152/p0trgkv-25sx8"
}
```

#### 2.2.2 Mapping

```json
{
  "settings": {
    "number_of_shards": 1,
    "analysis": {
      "analyzer": {
        "label_ngram": {
          "type": "custom",
          "tokenizer": "standard",
          "filter": ["lowercase", "asciifolding", "edge_ngram_filter"]
        },
        "label_search": {
          "type": "custom",
          "tokenizer": "standard",
          "filter": ["lowercase", "asciifolding"]
        }
      },
      "filter": {
        "edge_ngram_filter": {
          "type": "edge_ngram",
          "min_gram": 2,
          "max_gram": 20
        }
      }
    }
  },
  "mappings": {
    "properties": {
      "id":                     { "type": "keyword" },
      "chrononym":              {
        "type": "text",
        "analyzer": "label_ngram",
        "search_analyzer": "label_search",
        "fields": { "keyword": { "type": "keyword" } }
      },
      "chrononyms": {
        "properties": {
          "label": {
            "type": "text",
            "analyzer": "label_ngram",
            "search_analyzer": "label_search"
          },
          "lang": { "type": "keyword" }
        }
      },
      "authority_id":           { "type": "keyword" },
      "authority_label":        { "type": "text", "fields": { "keyword": { "type": "keyword" } } },
      "spatial_description":    { "type": "text", "fields": { "keyword": { "type": "keyword" } } },
      "start_year":             { "type": "integer" },
      "stop_year":              { "type": "integer" },
      "start_year_earliest":    { "type": "integer" },
      "start_year_latest":      { "type": "integer" },
      "stop_year_earliest":     { "type": "integer" },
      "stop_year_latest":       { "type": "integer" },
      "ccodes":                 { "type": "keyword" },
      "geometry":               { "type": "geo_shape", "ignore_malformed": true },
      "bbox":                   { "type": "geo_point" },
      "periodo_url":            { "type": "keyword" }
    }
  }
}
```

The `label_ngram` analyzer supports prefix typeahead on the `chrononym`
and `chrononyms.label` fields.  The `geometry` field enables
`geo_shape` `intersects` queries against the user's viewport.

#### 2.2.3 Temporal Year Handling

PeriodO temporal bounds can be:
- A single `year` integer (e.g. `{"year": -1200}`).
- A range string (e.g. `"0040-0050"`), split into `earliestYear`
  and `latestYear`.
- An `in` object with `earliestYear` and `latestYear`.
- An `in` object with only `year`.

The ingestion pipeline must normalise all forms into the four
integer fields: `start_year_earliest`, `start_year_latest`,
`stop_year_earliest`, `stop_year_latest`.  The coarse `start_year`
and `stop_year` fields use the most inclusive interpretation:
`start_year = start_year_earliest`, `stop_year = stop_year_latest`.

---

### 2.3 `territories` (new)

A single index holding polity/territory records from three datasets,
distinguished by a `dataset` keyword field.  This avoids maintaining
three separate mappings while allowing independent update cycles via
`delete_by_query` on the `dataset` field.

#### 2.3.1 Document Schema

```json
{
  "id":            "cliopatria:roman_empire_117",
  "dataset":       "cliopatria",
  "label":         "Roman Empire",
  "alt_labels":    ["Imperium Romanum"],
  "start_year":    -27,
  "stop_year":     476,
  "ccodes":        ["IT", "FR", "ES", "GB", "TR", "EG"],
  "geometry":      { "type": "MultiPolygon", "coordinates": [...] },
  "bbox":          [-10.0, 25.0, 45.0, 55.0],
  "source_url":    "https://..."
}
```

#### 2.3.2 Datasets

| `dataset` value | Source | Description |
|-----------------|--------|-------------|
| `cliopatria` | Cliopatria project | Historical polities and empires |
| `dplace` | D-PLACE database | Cultural and linguistic regions |
| `nativeland` | Native-Land.ca | Indigenous territories, languages, treaties |

#### 2.3.3 Mapping

```json
{
  "settings": {
    "number_of_shards": 1,
    "analysis": {
      "analyzer": {
        "label_ngram": { "...same as periodo_periods..." },
        "label_search": { "...same as periodo_periods..." }
      },
      "filter": {
        "edge_ngram_filter": { "...same as periodo_periods..." }
      }
    }
  },
  "mappings": {
    "properties": {
      "id":           { "type": "keyword" },
      "dataset":      { "type": "keyword" },
      "label":        {
        "type": "text",
        "analyzer": "label_ngram",
        "search_analyzer": "label_search",
        "fields": { "keyword": { "type": "keyword" } }
      },
      "alt_labels":   {
        "type": "text",
        "analyzer": "label_ngram",
        "search_analyzer": "label_search"
      },
      "start_year":   { "type": "integer" },
      "stop_year":    { "type": "integer" },
      "ccodes":       { "type": "keyword" },
      "geometry":     { "type": "geo_shape", "ignore_malformed": true },
      "bbox":         { "type": "geo_point" },
      "source_url":   { "type": "keyword" }
    }
  }
}
```

---

### 2.4 `osm_admin_polygons` (new, dedicated index)

Administrative boundary polygons from OpenStreetMap, used by the
Region selector (Tab 1 in the filter panel) and by the main search
endpoint for `filter_geometry` spatial intersection.  Regions are
categorised into four admin tiers: country, region/state,
district/county, municipality.

#### Why a dedicated index is required

The existing `osm:` entities in the `places` index **cannot** serve
this purpose.  The `places` index contains OSM *named places* —
nodes tagged with `place=city`, `place=town`, etc. — indexed as
point geometries for toponym search.  It does **not** contain
administrative boundary relations (`boundary=administrative`), which
are the polygon geometries needed for spatial filtering.  These are
fundamentally different data: a point labelled "France" in the
`places` index is not the same as the polygon boundary of France.

A dedicated index is also necessary for **performance**:

- **Small index, fast queries.**  The `places` index contains ~24M
  documents optimised for full-text toponym search with complex
  scoring.  Admin boundaries number ~300K documents with simple
  label-prefix + geo_shape queries.  Keeping them separate avoids
  polluting the `places` inverted indexes and segment caches with
  unrelated data.
- **Appropriate shard sizing.**  Admin polygons include very large
  `geo_shape` fields (country boundaries can be several MB of
  coordinates).  A single-shard index with `geo_shape` optimised
  settings is far more efficient than embedding these in the
  multi-shard `places` index.
- **Independent update cycle.**  OSM admin boundaries update on a
  different cadence from place ingestion.  A dedicated index allows
  delete-and-recreate without touching the main search index.
- **Geometry retrieval.**  The `GET /api/geometry` endpoint (§4.5)
  and the `filter_geometry` reference in `POST /api/search` (§4.1)
  both need to fetch full polygon geometries by ID.  A small,
  dedicated index makes these lookups sub-millisecond.

#### What this index supersedes

This index replaces several PostgreSQL-based systems in the Django
codebase:

| Superseded component | Current role |
|---------------------|--------------|
| `areas.models.Country` (`countries` table) | Country polygons (`mpoly` MultiPolygonField) used for ccodes lookup and spatial filtering |
| `regions.models.Region` (`regions_region` table) | UN M49 regions with `geom` MultiPolygonField, hierarchical parent/child structure |
| `regions.models.RegionLabel` | Multilingual labels for Region entries |
| `media/data/regions_countries.json` | Static JSON export of regions and countries for dropdown population |
| `utils/regions_countries.py` | Utility that reads the static JSON file |
| `regions/management/commands/export_regions_json.py` | Command to regenerate the static JSON |
| `main/management/commands/populate_regions_and_countries.py` | Command to populate regions/countries in the DB |

These components should be removed after this specification is
implemented (see §9 for the full redundancy list).

#### 2.4.1 Document Schema

```json
{
  "id":            "osm:relation/62149",
  "osm_id":        62149,
  "admin_level":   2,
  "tier":          "country",
  "name":          "France",
  "name_en":       "France",
  "alt_names":     ["République française", "Francia"],
  "iso_alpha2":    "FR",
  "parent_name":   "Europe",
  "parent_id":     null,
  "geometry":      { "type": "MultiPolygon", "coordinates": [...] },
  "bbox":          [-5.14, 41.33, 9.56, 51.09],
  "centroid":      [2.35, 46.86]
}
```

#### 2.4.2 Admin Tier Mapping

| Tier | OSM `admin_level` values |
|------|-------------------------|
| `country` | 2 |
| `region` | 3, 4 |
| `district` | 5, 6 |
| `municipality` | 7, 8 |

#### 2.4.3 Mapping

```json
{
  "settings": {
    "number_of_shards": 1,
    "analysis": {
      "analyzer": {
        "label_ngram": { "...same as above..." },
        "label_search": { "...same as above..." }
      },
      "filter": {
        "edge_ngram_filter": { "...same as above..." }
      }
    }
  },
  "mappings": {
    "properties": {
      "id":           { "type": "keyword" },
      "osm_id":       { "type": "long" },
      "admin_level":  { "type": "integer" },
      "tier":         { "type": "keyword" },
      "name":         {
        "type": "text",
        "analyzer": "label_ngram",
        "search_analyzer": "label_search",
        "fields": { "keyword": { "type": "keyword" } }
      },
      "name_en":      {
        "type": "text",
        "analyzer": "label_ngram",
        "search_analyzer": "label_search"
      },
      "alt_names":    {
        "type": "text",
        "analyzer": "label_ngram",
        "search_analyzer": "label_search"
      },
      "iso_alpha2":   { "type": "keyword" },
      "parent_name":  { "type": "keyword" },
      "parent_id":    { "type": "keyword" },
      "geometry":     { "type": "geo_shape", "ignore_malformed": true },
      "bbox":         { "type": "geo_point" },
      "centroid":     { "type": "geo_point" }
    }
  }
}
```

---

## 3. Gateway API Endpoints

All endpoints require `Authorization: Bearer <CRC_GATEWAY_API_KEY>`.
All request/response bodies are `application/json`.

### 3.1 `POST /api/search` (existing — enhanced)

The existing search endpoint.  Two additions:

1. Accept an optional `authorities` array to filter by source
   namespace (e.g. `["gn", "wd", "tgn"]`).  When absent, all
   namespaces are searched.
2. Accept an optional `filter_geometry` object for spatial filtering
   with a referenced geometry from another index (see §4.1).

### 3.2 `POST /api/periods/search` (new)

Search the `periodo_periods` index.  Supports label typeahead with
viewport spatial filtering.

### 3.3 `POST /api/territories/search` (new)

Search the `territories` index.  Supports label typeahead filtered
by `dataset` and viewport bbox.

### 3.4 `POST /api/regions/search` (new)

Search the `osm_admin_polygons` index.  Supports label typeahead filtered
by admin `tier` and viewport bbox.

### 3.5 `GET /api/geometry/{index}/{id}` (new)

Retrieve the full GeoJSON geometry for a single document from any
of the spatial indexes.  Used by the front-end to preview a
selected region, period, or territory on the context map.

**Allowed indexes:** `periodo_periods`, `territories`, `osm_admin_polygons`.
The `places` index is NOT exposed via this endpoint.

---

## 4. Request / Response Contracts

### 4.1 `POST /api/search` (enhanced)

#### Request

```json
{
  "query":              "Constantinople",
  "mode":               "fuzzy",
  "size":               100,
  "ccodes":             ["TR", "GR"],
  "bounds":             { "type": "Polygon", "coordinates": [...] },
  "start_year":         -500,
  "end_year":           1500,
  "undated":            false,
  "exact":              false,
  "geom":               "full",
  "authorities":        ["gn", "wd", "tgn", "pl", "iv", "whg"],
  "fclasses":           ["aat:300008347", "aat:300008389"],
  "filter_geometry": {
    "index":            "osm_admin_polygons",
    "id":               "osm:relation/62149"
  }
}
```

| Field | Type | Required | Default | Notes |
|-------|------|----------|---------|-------|
| `query` | string | No* | — | Toponym search text. *Required unless `fclasses` + spatial/temporal filters are sufficient |
| `mode` | string | No | `"fuzzy"` | `"fuzzy"`, `"exact"`, `"prefix"` |
| `size` | int | No | 100 | Max results |
| `ccodes` | string[] | No | — | ISO country code filter |
| `bounds` | GeoJSON | No | — | Viewport bounding box as GeoJSON geometry |
| `start_year` | int | No | — | Temporal filter start |
| `end_year` | int | No | — | Temporal filter end |
| `undated` | bool | No | false | Include records with no temporal data |
| `exact` | bool | No | false | Require exact spelling match |
| `geom` | string | No | `"full"` | `"full"` or `"repr_point"` |
| `authorities` | string[] | No | all | Filter to specific source namespaces |
| `fclasses` | string[] | No | — | AAT type identifier filter |
| `filter_geometry` | object | No | — | Reference to a geometry in another index for `geo_shape intersects` |

When `filter_geometry` is provided, the gateway fetches the geometry
from the referenced index/id and applies it as a `geo_shape intersects`
filter on `geoms.location`, intersected with `bounds` if also present.

#### Response

No change from the existing response shape:

```json
{
  "hits": [
    {
      "place_id":    "gn:745044",
      "title":       "İstanbul",
      "names":       [{"label": "Constantinople", "lang": "en"}, ...],
      "ccodes":      ["TR"],
      "types":       [{"label": "populated place", "identifier": "aat:300008347"}],
      "timespans":   [{"gte": 330, "lte": 1453}],
      "geometries":  [{"type": "Point", "coordinates": [28.97, 41.01]}],
      "repr_point":  [28.97, 41.01],
      "score":       87.5,
      "children":    [],
      "whg_id":      null,
      "dataset":     "",
      "links":       [{"identifier": "wd:Q406", "type": "closeMatch"}],
      "descriptions": [{"value": "...", "lang": "en"}],
      "depictions":  [],
      "relations":   [],
      "fclasses":    ["P"],
      "src_id":      "745044",
      "uri":         "https://www.geonames.org/745044"
    }
  ],
  "total":     1234,
  "max_score": 87.5,
  "facets": {
    "types":     [{"key": "aat:300008347", "label": "populated place", "count": 800}],
    "countries": [{"key": "TR", "count": 150}]
  }
}
```

---

### 4.2 `POST /api/periods/search`

#### Request

```json
{
  "query":    "Iron Age",
  "bbox":     [-10.5, 30.0, 45.0, 60.0],
  "size":     30,
  "authority_id": null
}
```

| Field | Type | Required | Default | Notes |
|-------|------|----------|---------|-------|
| `query` | string | Yes | — | Label prefix (min 2 chars) |
| `bbox` | number[4] | No | — | `[west, south, east, north]` viewport filter |
| `size` | int | No | 30 | Max results |
| `authority_id` | string | No | — | Restrict to a single PeriodO authority |

The ES query logic:

1. `multi_match` on `chrononym` + `chrononyms.label` (using the
   `label_search` analyzer).
2. If `bbox` is provided: `geo_shape` `intersects` filter on
   `geometry` using an `envelope` built from the bbox.
3. If `authority_id` is provided: `term` filter on `authority_id`.
4. Sort by `_score` descending, then `authority_id`.

#### Response

```json
{
  "results": [
    {
      "id":                    "p0trgkv-25sx8",
      "label":                 "Iron Age",
      "start_year":            -1200,
      "stop_year":             -550,
      "spatial_description":   "Eastern Mediterranean",
      "authority_id":          "p0trgkv",
      "authority_label":       "Wikipedia (English)",
      "ccodes":                ["GR", "TR", "SY"],
      "has_geometry":          true,
      "periodo_url":           "https://n2t.net/ark:/99152/p0trgkv-25sx8"
    }
  ],
  "total": 42,
  "authorities": [
    { "id": "p0trgkv", "label": "Wikipedia (English)", "count": 15 },
    { "id": "p0d7x3p", "label": "ARIADNE", "count": 8 }
  ]
}
```

The `authorities` array lists distinct authorities from the results
with counts, enabling the front-end's authority sub-filter without
a second round-trip.  Geometry is NOT included in search results —
only `has_geometry: true|false`.  The front-end fetches the full
geometry via `GET /api/geometry/periodo_periods/{id}` only when the
user selects a specific period.

---

### 4.3 `POST /api/territories/search`

#### Request

```json
{
  "query":    "Roman",
  "dataset":  "cliopatria",
  "bbox":     [-10.5, 30.0, 45.0, 60.0],
  "size":     20
}
```

| Field | Type | Required | Default | Notes |
|-------|------|----------|---------|-------|
| `query` | string | Yes | — | Label prefix (min 2 chars) |
| `dataset` | string | Yes | — | `"cliopatria"`, `"dplace"`, or `"nativeland"` |
| `bbox` | number[4] | No | — | Viewport filter |
| `size` | int | No | 20 | Max results |

The ES query logic:

1. `multi_match` on `label` + `alt_labels`.
2. `term` filter on `dataset`.
3. If `bbox`: `geo_shape` `intersects` on `geometry`.
4. Sort by `_score` descending.

#### Response

```json
{
  "results": [
    {
      "id":            "cliopatria:roman_empire_117",
      "label":         "Roman Empire",
      "start_year":    -27,
      "stop_year":     476,
      "has_geometry":  true
    }
  ],
  "total": 5
}
```

---

### 4.4 `POST /api/regions/search`

#### Request

```json
{
  "query":  "Fra",
  "tier":   "country",
  "bbox":   [-10.5, 30.0, 45.0, 60.0],
  "size":   10
}
```

| Field | Type | Required | Default | Notes |
|-------|------|----------|---------|-------|
| `query` | string | Yes | — | Label prefix (min 2 chars) |
| `tier` | string | No | `"country"` | `"country"`, `"region"`, `"district"`, `"municipality"` |
| `bbox` | number[4] | No | — | Viewport filter |
| `size` | int | No | 10 | Max results |

The ES query logic:

1. `multi_match` on `name` + `name_en` + `alt_names`.
2. If `tier`: `term` filter on `tier`.
3. If `bbox`: `geo_shape` `intersects` on `geometry`.
4. Sort by `_score` descending.

#### Response

```json
{
  "results": [
    {
      "id":          "osm:relation/62149",
      "label":       "France",
      "tier":        "country",
      "iso_alpha2":  "FR",
      "parent_name": "Europe",
      "has_geometry": true
    }
  ],
  "total": 1
}
```

---

### 4.5 `GET /api/geometry/{index}/{id}`

#### Path Parameters

| Parameter | Notes |
|-----------|-------|
| `index` | One of: `periodo_periods`, `territories`, `osm_admin_polygons` |
| `id` | Document `_id` in the index |

#### Response

```json
{
  "id":       "osm:relation/62149",
  "index":    "osm_admin_polygons",
  "geometry": {
    "type": "MultiPolygon",
    "coordinates": [...]
  }
}
```

The geometry is returned as-is from ES (no simplification).  The
front-end may apply client-side simplification for rendering.

Returns 404 if the document does not exist or has no geometry.
Returns 400 if the index is not in the allowed list.

---

## 5. Data Ingestion Pipelines

Ingestion runs on a Pitt CRC login node.  Each pipeline is a Python
script submitted via a Slurm `.sbatch` job.  Scripts use the
`elasticsearch` Python client's `bulk` helpers.

### 5.1 PeriodO Pipeline

**Source files:**
- `https://n2t.net/ark:/99152/p0dataset.json` — the full PeriodO
  JSON-LD dataset (~60 MB).
- `https://github.com/periodo/periodo-places` — GeoJSON gazetteer
  files providing resolved geometries for the spatial coverage URIs
  referenced by PeriodO periods.

**Steps:**

1. **Download** `p0dataset.json` (cache locally; skip if unchanged).
2. **Parse authorities and periods.**  For each period, extract:
   - `id`, `chrononym` (the `label` field), `authority` FK.
   - `spatialCoverageDescription`.
   - `localizedLabels` → `chrononyms[]` array.
   - `start`/`stop` temporal bounds → normalise to four integer
     fields (see §2.2.3).
3. **Collect spatial coverage URIs** from all periods.
4. **Download and parse gazetteer files** from `periodo-places`.
   Match features to spatial URIs by `feature.id`.
5. **Clean geometries** per the rules in §6.
6. **Compute per-period aggregates:**
   - Union all spatial coverage geometries → `geometry`.
   - Compute bounding box → `bbox`.
   - Intersect with a countries GeoJSON to derive `ccodes`.
7. **Bulk-index** into `periodo_periods` (delete-and-recreate).

**Slurm job:**

```bash
#!/bin/bash
#SBATCH --job-name=ingest-periodo
#SBATCH --time=02:00:00
#SBATCH --mem=16G
#SBATCH --cpus-per-task=4

source /path/to/venv/bin/activate
python ingest_periodo.py --es-url $ES_URL --es-api-key $ES_API_KEY
```

### 5.2 Territory Pipeline

Three sub-pipelines, one per dataset.  Each produces documents
conforming to the §2.3.1 schema and indexes into the `territories`
index with the appropriate `dataset` value.

**General steps per dataset:**

1. **Download** source data.
2. **Parse** into the common schema: `id`, `label`, `alt_labels`,
   `dataset`, `start_year`, `stop_year`, `geometry`, `ccodes`.
3. **Clean geometries** per §6.
4. **Delete** existing documents for this dataset:
   `POST /territories/_delete_by_query {"query":{"term":{"dataset":"<value>"}}}`.
5. **Bulk-index** new documents.

**Dataset-specific notes:**

- **Cliopatria:** Download from the Cliopatria project.  Records have
  temporal extents and polygon geometries.
- **D-PLACE:** Download from the D-PLACE database.  Cultural regions;
  some may lack temporal extents (set `start_year`/`stop_year` to
  `null`).
- **NativeLand:** Download from the Native-Land.ca API.  Indigenous
  territories; temporal extents vary.

### 5.3 OSM Regions Pipeline

**Source:** OpenStreetMap administrative boundary extracts (GeoJSON),
typically from a pre-processed global admin boundaries dataset (e.g.
GADM, geoBoundaries, or direct OSM Overpass exports).

**Steps:**

1. **Download** boundary GeoJSONs for admin levels 2–8.
2. **Parse** each feature.  Extract:
   - OSM relation ID → `id` (`osm:relation/{id}`).
   - `admin_level` → map to `tier` per §2.4.2.
   - `name`, `name:en` → `name`, `name_en`.
   - Alternative names (`name:*` tags) → `alt_names`.
   - ISO 3166-1 alpha-2 code (for countries) → `iso_alpha2`.
   - Parent relationship from the boundary hierarchy → `parent_id`,
     `parent_name`.
3. **Clean geometries** per §6.
4. **Compute** centroid → `centroid`.
5. **Compute** bounding box → `bbox`.
6. **Delete and recreate** the `osm_admin_polygons` index.
7. **Bulk-index** all documents.

---

## 6. Geometry Cleaning Rules

These rules are mandatory for all ingestion pipelines.  They are
derived from extensive debugging of the PeriodO gazetteer geometries
in the WHG Django codebase (`update_from_gazetteers.py`,
`validate_gazetteers.py`, `diagnose_geometry.py`).

### Rule 1: Repair self-intersections with `buffer(0)`

Apply `geometry.buffer(0)` to every `Polygon`.  If the result is
still invalid, reject the geometry and log a warning.

```python
from shapely.geometry import shape
from shapely.validation import make_valid

geom = shape(geojson_geometry)
if not geom.is_valid:
    geom = geom.buffer(0)
    if not geom.is_valid:
        geom = make_valid(geom)  # Shapely 2.x fallback
    if not geom.is_valid:
        log.warning(f"Rejected invalid geometry for {feature_id}")
        continue
```

### Rule 2: Extract polygons from GeometryCollections

Recursively walk `GeometryCollection` sub-geometries.  Keep only
`Polygon` and `MultiPolygon` components.  Discard `Point`,
`LineString`, `MultiPoint`, and `MultiLineString` — these are
artefacts in the source data and have no meaning as spatial coverage.

```python
from shapely.geometry import Polygon, MultiPolygon, GeometryCollection

def extract_polygons(geom):
    """Yield all Polygon sub-geometries from any geometry type."""
    if isinstance(geom, Polygon):
        if geom.is_valid or geom.buffer(0).is_valid:
            yield geom.buffer(0) if not geom.is_valid else geom
    elif isinstance(geom, MultiPolygon):
        for poly in geom.geoms:
            yield from extract_polygons(poly)
    elif isinstance(geom, GeometryCollection):
        for sub in geom.geoms:
            yield from extract_polygons(sub)
    # Points and LineStrings are silently discarded.
```

### Rule 3: Merge overlapping polygons via `unary_union`

After extracting all polygons, merge them into a single geometry
using `shapely.ops.unary_union`.  This dissolves overlapping regions
into a clean `MultiPolygon`.

```python
from shapely.ops import unary_union

polygons = list(extract_polygons(raw_geom))
if not polygons:
    return None
merged = unary_union(polygons)
```

### Rule 4: Reject remaining GeometryCollections

If `unary_union` returns a `GeometryCollection` (e.g., because a
polygon union produced a mix of polygon and line artefacts), reject
the entire geometry.  Do NOT store `GeometryCollection` in ES — the
`geo_shape` field handles only homogeneous types cleanly.

```python
if merged.geom_type == "GeometryCollection":
    log.warning(f"Union returned GeometryCollection for {feature_id}; rejected")
    return None
```

### Rule 5: Normalise to MultiPolygon

Always store the final geometry as `MultiPolygon` for consistency.
Wrap a single `Polygon` result in `MultiPolygon`.

```python
if merged.geom_type == "Polygon":
    merged = MultiPolygon([merged])
elif merged.geom_type != "MultiPolygon":
    log.warning(f"Unexpected geometry type {merged.geom_type}; rejected")
    return None
```

### Rule 6: Validate coordinates

Ensure all coordinates are within valid WGS-84 bounds:
longitude ∈ [−180, 180], latitude ∈ [−90, 90].  Clamp or reject
out-of-bounds coordinates.

### Rule 7: Compute bounding box

For every document with a geometry, compute the bounding box as
`[west, south, east, north]` (i.e. `[minx, miny, maxx, maxy]` from
Shapely's `.bounds` property).

---

## 7. Matching Authority Codes

The `authorities` filter uses short namespace codes.  The gateway
must map these to the appropriate namespace prefix in `place_id` or
a dedicated `namespace` field:

| Code | Full Namespace | Description |
|------|---------------|-------------|
| `gn` | `gn:` | GeoNames |
| `wd` | `wd:` | Wikidata |
| `tgn` | `tgn:` | Getty TGN |
| `pl` | `pl:` | Pleiades |
| `iv` | `iv:` | IndexVillaris |
| `whg` | `whg:` | WHG contributed datasets |
| `osm` | `osm:` | OpenStreetMap |
| `gb` | `gb1900:` | GB1900 |

When the `authorities` array is present in a search request, the
gateway should apply a `terms` filter on the namespace prefix.

---

## 8. Error Handling

All endpoints must return structured JSON errors:

```json
{
  "error": "Invalid bbox: expected 4-element array",
  "status": 400
}
```

HTTP status codes:
- `400` — Invalid request (malformed JSON, missing required fields).
- `401` — Missing or invalid Bearer token.
- `404` — Document not found (geometry endpoint).
- `500` — Internal server error (log full traceback server-side).

The WHG Django client (`crc_client.py`) handles all errors gracefully:
on any non-2xx response, it logs a warning and returns an empty result
set to the browser.

---

## 9. Redundant Django Code

The following Django code is superseded by the CRC ES pipelines
described in §5.  It should be removed from the `whg3` codebase
after this specification is implemented.

### 9.1 PeriodO (superseded by `periodo_periods` ES index, §5.1)

| File | Purpose |
|------|---------|
| `periods/models.py` | Django ORM models: `SpatialEntity`, `GazetteerTracker`, `Chrononym`, `Authority`, `Period`, `TemporalBound` |
| `periods/management/commands/update_periodo.py` | Ingests `p0dataset.json` into Django DB |
| `periods/management/commands/update_from_gazetteers.py` | Downloads and processes gazetteer geometries from GitHub |
| `periods/management/commands/diagnose_geometry.py` | Diagnostic tool for geometry storage issues |
| `periods/management/commands/validate_gazetteers.py` | Read-only validation of gazetteer geometries |
| `periods/migrations/*` | All Django migrations for the periods app |

The geometry cleaning logic in `update_from_gazetteers.py` has been
codified as the rules in §6 above.  The temporal bound parsing logic
in `update_periodo.py` has been codified in §2.2.3.

### 9.2 Admin Polygons (superseded by `osm_admin_polygons` ES index, §5.3)

The PostgreSQL-based country and region polygon system is fully
replaced by the `osm_admin_polygons` Elasticsearch index.  All
spatial filtering, region selection, and country-code lookup moves
to ES.

| File / Component | Purpose |
|------------------|---------|
| `areas/models.py` → `Country` class | Country polygons (`mpoly` MultiPolygonField) + bbox; used for ccodes lookup in ingestion and spatial filtering in reconciliation |
| `regions/models.py` → `Region` class | UN M49 hierarchy (global → region → sub-region → country) with `geom` and `hull` MultiPolygonFields |
| `regions/models.py` → `RegionLabel` class | Multilingual labels for `Region` entries |
| `regions/management/commands/export_regions_json.py` | Exports regions and countries to `regions_countries.json` |
| `regions/data/*` | Source data for region/country loading |
| `main/management/commands/populate_regions_and_countries.py` | Populates `Region` and `Country` tables from source data |
| `utils/regions_countries.py` | Reads the static `regions_countries.json` file for dropdown population |
| `media/data/regions_countries.json` | Static JSON snapshot of regions/countries for front-end dropdowns |
| `areas/forms.py` → `AreaModelForm` | Study area form using `GEOSGeometry` — may be retained if user-drawn areas remain in PostgreSQL |

**Note on `Area` model:** The `Area` model (user-created study areas
for reconciliation) is a separate concern from admin polygons.  It
may be retained in PostgreSQL if user-drawn areas are still needed
for reconciliation workflows, but the *country* and *predefined
region* entries currently stored as `Area` records with
`type='country'` and `type='predefined'` are superseded.

**Note on `Country` usage in ingestion:** The `Country.mpoly` field
is currently used by `ingestion/transformers.py` (the `isocodes()`
function) to determine country codes from point geometries during
place ingestion.  This must be migrated to use the
`osm_admin_polygons` index (a `geo_shape` `intersects` query against
tier=country documents) before the `Country` model can be removed.

---

## 10. Implementation Priority

1. **`periodo_periods` index + ingestion + search endpoint** — this
   is the most complex new index and directly enables Tab 2 (Period
   selector).
2. **`territories` index + ingestion + search endpoint** — enables
   Tab 3 (Territory selector).
3. **`osm_admin_polygons` index + ingestion + search endpoint** —
   enables Tab 1 region selector and replaces the PostgreSQL
   country/region polygon system (the dateline slider already works
   without backend changes).
4. **`GET /api/geometry/{index}/{id}`** — trivial endpoint needed by
   all three selectors for map preview.
5. **Enhanced `POST /api/search`** — add `authorities` filter and
   `filter_geometry` support to the existing endpoint.
6. **Migrate `isocodes()` in `ingestion/transformers.py`** — replace
   the `Country.mpoly` point-in-polygon lookup with an ES query
   against `osm_admin_polygons` (tier=country).
7. **Remove redundant Django code** (§9) — only after all above
   steps are verified working.
