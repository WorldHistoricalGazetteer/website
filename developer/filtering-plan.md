# WHG v3.5 Search Filters: Full Specification

> **Audience:** Human developers and coding agents working on the WHG v3.5
> codebase (Django / Elasticsearch / JavaScript / MapLibre).
>
> **Stack:** Django on DigitalOcean; Elasticsearch behind a FastAPI
> gateway on the Pitt CRC VM; MapLibre with the WHG tileserver
> (OSM data, NaturalEarth colouring, DEM hillshading).
>
> **Status:** Implementation-ready design.

---

## 1. Design Principles

The filters panel constrains the main WHG toponym and place-type
search.  It is explicitly **not a GIS tool**.  It provides simple,
predictable constraints to support search.

- **One temporal authority at a time.**  The user chooses whether time
  is controlled manually, derived from a PeriodO period, or derived
  from a polity.  These are mutually exclusive.
- **Viewport as primary spatial constraint.**  The context map's
  visible extent is always the spatial filter baseline.  A selected
  geometry (OSM region, PeriodO coverage, polity boundary) is
  intersected with the viewport, never used alone.
- **No client-side geometry processing.**  No polygon drawing, no
  unions, no buffers, no exclusions.  Geometry is fetched for preview
  and passed by reference for search.
- **Manual search triggering.**  Filter changes mark the search as
  dirty.  The main `places` query fires only when the user presses
  Search.  Lightweight queries against small indices (PeriodO,
  polities) may be reactive.

---

## 2. Panel Layout

### 2.1 Three-Column Structure

| Column | Width | Content |
|--------|-------|---------|
| Authorities | 1x | Checkbox list of data sources |
| Place Types | 1x | AAT hierarchy tree widget |
| Time & Space | 2x | Tabbed interface with persistent context map |

Columns are equal height with vertical scrolling within each.  The
Time & Space column is double-width to accommodate the context map.

### 2.2 Time & Space Tabs

The Time & Space column contains a **persistent context map** (always
visible, regardless of active tab) and a tabbed control area beneath
or alongside it:

| Tab | Label | Purpose |
|-----|-------|---------|
| 1 | Timespan + Map | Manual temporal slider; OSM region selector |
| 2 | Periods | PeriodO period selector (temporal authority from period) |
| 3 | Polities | Polity selector (temporal authority from polity) |

Tab 1 is the default.

---

## 3. Authorities Filter

### 3.1 Purpose

Restrict search results to selected data sources.

### 3.2 UI

A checkbox list.  Default: all selected except OSM.

```
[x] GeoNames
[x] Wikidata
[x] TGN
[ ] OpenStreetMap
[x] WHG datasets
```

Each item shows the authority name and an optional result count.

### 3.3 Behaviour

- Multi-select with OR logic across selected authorities.
- No selection produces no results.

### 3.4 ES Query Fragment

```json
{ "terms": { "authority": [...] } }
```

---

## 4. Place Types Filter

### 4.1 Source

Getty AAT hierarchy (existing tree widget, documented separately in
the WHG Type System specification).

### 4.2 Behaviour

- Multi-select tree with OR logic across selected nodes.
- Selecting a node includes all its descendants.
- Post-retrieval consanguinity banding applies as described in the
  Type System specification.

---

## 5. Context Map

The context map is the **primary spatial interaction surface** and is
**always visible** regardless of the active tab.  It serves three
roles:

1. Defines the **viewport bounding box** (always active as the
   baseline spatial constraint).
2. Displays **selected geometries** (OSM region, PeriodO coverage,
   polity boundary) as preview overlays.
3. Constrains **available PeriodO periods and polities** offered in
   their respective selectors.

### 5.1 Visual Treatment

WHG serves its own tiles from a tileserver built on OSM data with
NaturalEarth colouring and DEM hillshading.  No external tile
dependencies are used.

- **Desaturated basemap.**  Apply CSS `filter: saturate(0.3)
  brightness(1.05)` (or similar) to the tile layer container.  This
  is cheap to implement and reversible.  A dedicated low-saturation
  tileserver style can be developed later if label legibility suffers
  under desaturation.
- **Prominent labels and features.**  The basemap should retain
  place-name labels, country boundaries, major rivers, and terrain
  shading.  These help users orient themselves when selecting a
  region.  Test label legibility at the chosen saturation value and
  adjust accordingly.

> **Coding-agent note:** Do not introduce dependencies on external
> tile providers (Mapbox, Stamen, CartoDB, etc.).  The map uses the
> WHG tileserver exclusively.

### 5.2 Geometry Preview

When an entity with geometry is selected in any tab, its geometry is
displayed on the context map as an overlay:

- Semi-transparent fill with a distinct border.
- No complex masking, composition, or inverted masks.
- Only one overlay is active at a time (since only one
  temporal/spatial authority is active).

### 5.3 Interaction

- **Pan and zoom** to set the viewport.  The current map extent
  defines `viewport_bbox`, which is always applied as the baseline
  spatial constraint.
- Panning or zooming updates the PeriodO and polity selector results
  (reactively, since these are lightweight queries).
- No drawing tools.

### 5.4 Geometry Fetching

When the user selects an entity (OSM region, PeriodO period, polity)
that has a geometry, the browser fetches it from the CRC FastAPI for
map preview:

```
GET /geometry/{index}/{id}
```

The FastAPI looks up the geometry in the relevant ES index (`places`
for OSM regions, `periodo_periods` for periods, the polity index for
polities) and returns GeoJSON.  For complex polygons, the FastAPI may
return a simplified version (`shapely.simplify(tolerance,
preserve_topology=True)`) to keep the preview responsive.

The browser draws the returned geometry on the map but does not send
it back to the server.  When the search fires, the entity is
referenced by ID only (section 11).

> **Coding-agent note:** Cache fetched geometries on the FastAPI with
> a short TTL so that the search request does not redundantly re-fetch
> geometries that were just served as previews.

---

## 6. Core Rule: Temporal Authority

> **Only one source of temporal authority is active at a time.**

| Active tab | Temporal source | Temporal control |
|------------|----------------|------------------|
| Timespan + Map | User-controlled slider | Manual |
| Periods | Derived from selected PeriodO period | Locked |
| Polities | Derived from selected polity | Locked |

Switching tabs clears incompatible state (section 12).

---

## 7. Tab 1: Timespan + Map

This is the default tab.  The user controls time manually and may
optionally select a named spatial region.

### 7.1 Temporal Slider

- Fully user-controlled.
- Defines `start_year` and `stop_year` using the same convention as
  PeriodO: negative integers for BCE, positive for CE.
- Always active in this tab.

### 7.2 OSM Region Selector

A type-ahead search for named administrative regions sourced
primarily from OpenStreetMap data in the `places` ES index.

#### Why OSM as the primary source

- Comprehensive administrative boundaries worldwide (countries,
  states/provinces, counties, municipalities).
- Polygon geometries, not just centroids.
- Consistent admin-level hierarchy providing natural granularity
  control.
- A single namespace, avoiding reconciliation of overlapping entities
  from GeoNames, Wikidata, and TGN.

#### Controls

- **Admin tier selector** (normalised levels: country, region/state,
  district/county, municipality).
- **Type-ahead input:** the user types a region name; after a
  debounced delay, the widget queries the `places` index filtered to
  administrative/regional types and the selected admin tier.
- Results show the region name with parent context (e.g.
  "Lincolnshire, England, United Kingdom").
- Results are ranked by specificity relative to the current viewport
  and textual match quality.

#### Behaviour

- **Single region only.**  Selecting a region loads its geometry onto
  the context map and adds it to the spatial filter.
- Selecting a region does **not** affect available PeriodO or polity
  options (those depend on the viewport only, not the selected
  region).
- The selected region appears as a dismissible chip.  Dismissing it
  removes the geometry overlay and reverts to viewport-only spatial
  filtering.

#### ES query for the selector

> **Coding-agent note:** Query the `places` index with a `bool`
> query combining:
> - `match` or `match_phrase_prefix` on the name field.
> - `terms` filter on type restricting to administrative/regional AAT
>   types (or their GeoNames equivalents: `ADM1`, `ADM2`, `PCLI`,
>   etc., and OSM admin boundary types).
> - `geo_bounding_box` filter restricting to entities intersecting
>   the current viewport.
>
> Prerequisite: OSM administrative boundary geometries must be
> indexed as `geo_shape` fields in the `places` index.

### 7.3 Effective Geometry

No region selected:

```
viewport_bbox
```

Region selected:

```
viewport_bbox ∩ osm_region
```

---

## 8. Tab 2: Periods

Selecting this tab activates PeriodO as the temporal authority.  The
user selects a single period definition; time is derived from it.

### 8.1 PeriodO Background

PeriodO (`https://perio.do`) is a public-domain gazetteer of
scholarly definitions of historical, art-historical, and
archaeological periods.

- **Flat structure.** No `skos:broader`/`skos:narrower` hierarchy.
  Each period is an independent assertion.
- **Multiple competing definitions.** "Bronze Age" has hundreds of
  entries, each scoped to a different region and date range by a
  different published authority.
- **Rich metadata.** Each definition carries a label, temporal extent
  (start/stop years), spatial coverage (textual description and
  gazetteer links), and bibliographic authority source.
- **Persistent URIs.** Each period has an ARK identifier resolvable
  to JSON-LD.

PeriodO data is available as a single JSON download from
`https://data.perio.do/dataset/`.

### 8.2 PeriodO ES Index

Period definitions are stored in a dedicated ES index
(`periodo_periods`) on the CRC staging instance.

#### Index schema

```json
{
  "uri":           "http://n2t.net/ark:/99152/p05krdxmkzt",
  "label":         "Dark Age",
  "alt_labels":    ["Dark Ages"],
  "start_year":    -1100,
  "stop_year":     -750,
  "spatial_description": "Greece",
  "spatial_uris":  ["http://www.geonames.org/390903/"],
  "spatial_geo":   {
    "type": "envelope",
    "coordinates": [[19.37, 41.75], [29.65, 34.8]]
  },
  "authority_label": "Davis and Alcock 1998",
  "authority_uri":   "http://n2t.net/ark:/99152/p05krdxm"
}
```

The `spatial_geo` field is a `geo_shape` enabling ES spatial queries.
It is pre-computed at index-build time by resolving the
`dcterms:spatial` URIs (GeoNames, Wikidata, Pleiades) to bounding
boxes using cached gazetteer data from the WHG `places` index.

> **Coding-agent note:** Where spatial URIs cannot be resolved, set
> `spatial_geo` to null.  These records appear in label-only searches
> but are excluded from spatial filtering.  Where multiple spatial
> URIs resolve to different regions, compute the bounding box of
> their union.

#### Index build pipeline

1. Fetch the current PeriodO dataset from
   `https://data.perio.do/dataset/`.
2. Parse the JSON-LD.  Each authority (concept scheme) contains one
   or more period definitions (concepts).
3. Extract label, temporal extent, spatial coverage, and authority
   metadata for each period.
4. Resolve spatial URIs to geometries, using the WHG `places` index
   as a cache where possible.
5. Bulk-index into `periodo_periods` on the CRC staging ES instance.

Re-run monthly.  Use the ES index-alias swap pattern for
zero-downtime updates.

### 8.3 Period Selector

#### Filtering (reactive)

The period selector re-queries `periodo_periods` reactively as the
user types or as the viewport changes.  These are cheap queries
against a small index.

1. The user types a label fragment (e.g. "Iron Age").  After a
   debounced delay (200--300ms), the widget queries with:
   - `match` or `match_phrase_prefix` on `label` and `alt_labels`.
   - `geo_shape` intersects filter on `spatial_geo` using the current
     `viewport_bbox`, if `spatial_geo` exists.
2. Results are sorted by:
   - Exact label match first.
   - Alphabetically by label.
3. When results span multiple regions, group by
   `spatial_description` (e.g. Aegean and Levantine definitions of
   "Iron Age" cluster separately).

#### Results display

Each result row shows:

- Period label.
- Compact year range (e.g. "1100--750 BCE").
- Spatial description (e.g. "Greece").
- Authority label, displayed smaller / de-emphasised.

#### Authority sub-filter

A collapsible "Filter by authority" panel lists the distinct
authorities in the current filtered results with counts.  Selecting
an authority restricts results to that authority's periodisation.

### 8.4 Behaviour

- **Single selection only.**
- Selecting a period:
  - Sets `start_year` and `stop_year` from the period's temporal
    extent.  The temporal slider is hidden or disabled.
  - If `spatial_geo` exists, fetches and previews the geometry on the
    context map.
- Deselecting reverts to no temporal constraint and removes the
  geometry overlay.

### 8.5 Effective Geometry

No spatial geometry on selected period:

```
viewport_bbox
```

Spatial geometry exists:

```
viewport_bbox ∩ period_geometry
```

---

## 9. Tab 3: Polities

Selecting this tab activates a polity dataset as the temporal
authority.  The user selects a single polity; time and space are
derived from it.

### 9.1 Source

Cliopatria or equivalent polity dataset, stored in a dedicated ES
index on the CRC staging instance.

> **Coding-agent note:** The polity index schema and build pipeline
> are not specified here.  The index must at minimum contain: a
> unique identifier, a label, temporal extent (`start_year`,
> `stop_year`), and a `geo_shape` geometry field.

### 9.2 Polity Selector

#### Filtering (reactive)

1. The user types a polity name.  After a debounced delay, the
   widget queries the polity index with:
   - `match` or `match_phrase_prefix` on the label field.
   - `geo_shape` intersects filter using the current `viewport_bbox`.
   - Optional temporal relevance filter.
2. Results are sorted by textual match quality.

### 9.3 Behaviour

- **Single selection only.**
- Selecting a polity:
  - Sets `start_year` and `stop_year` from the polity's temporal
    validity.  The temporal slider is hidden or disabled.
  - Fetches and previews the polity geometry on the context map.
    Polity geometries are treated as authoritative and are always
    previewed.
- Deselecting reverts to no temporal constraint and removes the
  geometry overlay.

### 9.4 Effective Geometry

```
viewport_bbox ∩ polity_geometry
```

---

## 10. Tab Switching Rules

Switching tabs clears the state of the tab being left, since only
one temporal authority may be active.

| From | To | Action |
|------|----|--------|
| Timespan + Map | Periods | Clear selected OSM region and manual time range |
| Timespan + Map | Polities | Clear selected OSM region and manual time range |
| Periods | Timespan + Map | Clear selected period |
| Polities | Timespan + Map | Clear selected polity |
| Periods | Polities | Clear selected period |
| Polities | Periods | Clear selected polity |

The viewport persists across tab switches.  The Authorities and Place
Types filters are independent of the active tab and persist always.

---

## 11. Search Execution

### 11.1 Manual Triggering

The main `places` search is **never triggered automatically**.
Changes to any filter dimension mark the search state as **dirty**.
The user must press a **Search button** to execute the query.

The Search button is persistently visible in the filter panel.
When the filter state is dirty (filters have changed since the last
search), the button is visually emphasised.  If results are
displayed and filters have since changed, the button reads "Update
results" to indicate staleness.

> **Coding-agent note:** Track dirty state with a boolean flag
> toggled to `true` on any filter change and to `false` when search
> results are received.  The PeriodO and polity selectors re-query
> reactively on viewport changes (cheap, small indices); the main
> `places` search fires only on explicit user action.

### 11.2 Query Construction

When the user presses Search, the browser sends a filter descriptor
to the CRC FastAPI.  The descriptor contains IDs and parameters, not
geometries:

```json
{
  "authorities": ["geonames", "wikidata", "tgn", "whg"],
  "place_types": [300008347, 300008389],
  "mode": "timespan",

  "spatial": {
    "bbox": [min_lon, min_lat, max_lon, max_lat],
    "geometry_ref": {
      "index": "places",
      "id": "osm:relation/123456"
    }
  },

  "temporal": {
    "start_year": -1200,
    "stop_year": -700,
    "source": "manual"
  }
}
```

Or for a PeriodO period:

```json
{
  "authorities": ["geonames", "wikidata", "tgn", "whg"],
  "place_types": [300008347],
  "mode": "period",

  "spatial": {
    "bbox": [min_lon, min_lat, max_lon, max_lat],
    "geometry_ref": {
      "index": "periodo_periods",
      "id": "http://n2t.net/ark:/99152/p05krdxmkzt"
    }
  },

  "temporal": {
    "start_year": -1100,
    "stop_year": -750,
    "source": "period"
  }
}
```

The `geometry_ref` field is null when no named region, period, or
polity is selected (viewport-only spatial filtering).

### 11.3 FastAPI Processing

The CRC FastAPI receives the descriptor and:

1. If `geometry_ref` is present, fetches the referenced geometry from
   the specified ES index by ID.
2. Constructs the effective spatial filter:
   - `geo_bounding_box` from `bbox` (always applied, fast).
   - If geometry was fetched: `geo_shape` intersects filter using
     the geometry, intersected with the bbox.
3. Constructs the temporal filter as a numeric range on the place's
   temporal fields.
4. Constructs the authority and type filters from the descriptor.
5. Runs the composed `bool` query against the `places` index.
6. Applies post-retrieval type consanguinity banding (per the Type
   System specification).
7. Returns search results to the browser.

> **Coding-agent note:** Use `geo_bounding_box` as the primary
> spatial filter (fast, works on all records).  Apply `geo_shape`
> only when a named geometry is present.  This two-stage approach
> keeps the common case (viewport-only) cheap.

---

## 12. Filter State Model

The browser maintains a single state object:

```json
{
  "authorities": ["geonames", "wikidata", "tgn", "whg"],
  "place_types": [300008347, 300008389],

  "mode": "timespan | period | polity",

  "spatial": {
    "bbox": [min_lon, min_lat, max_lon, max_lat],
    "region_id": null,
    "period_id": null,
    "polity_id": null,
    "geometry_source": "osm | period | polity | none",
    "preview_geo": null
  },

  "temporal": {
    "start_year": -1200,
    "stop_year": -700,
    "source": "manual | period | polity"
  },

  "dirty": true
}
```

Only one of `region_id`, `period_id`, `polity_id` may be non-null at
a time (enforced by the tab-switching rules).  `preview_geo` holds
the geometry fetched from the FastAPI for map display; it is never
sent back in the search request.

---

## 13. Dataset Metadata: Associating Periods with Datasets

The PeriodO period selector (section 8.3) is also mounted in the
dataset metadata input/edit form.  When a contributor creates or
edits a dataset, they can search for and select one or more PeriodO
period definitions describing the dataset's temporal scope.

- `viewport_bbox` is derived from the dataset's spatial coverage (if
  already entered).
- Pre-selected periods are populated from any PeriodO URIs already
  stored against the dataset.
- Selected URIs are written to the dataset metadata.

In the dataset metadata context (unlike the search context), multiple
period selection is permitted, since a dataset may span several
distinct periods.

Over time, as datasets accumulate PeriodO associations, direct
period-based search becomes viable.

---

## 14. Migration Path to v4

- The `periodo_periods` ES index migrates to a collection in the
  ArangoDB `indexing` database.
- Spatial filtering uses ArangoDB's geo-spatial index capabilities.
- The polity dataset becomes a collection in the same database.
- Type consanguinity computation becomes native AQL graph traversal
  rather than application-layer post-processing.
- The tab-based temporal authority model carries over unchanged.

---

## 15. Performance Considerations

- Use `geo_bounding_box` as the primary spatial filter (applied to
  all records, fast).
- Use `geo_shape` only when a named geometry exists.
- Reactive PeriodO and polity queries hit small dedicated indices and
  are cheap.
- The main `places` search fires only on manual trigger, avoiding
  unnecessary load during filter composition.
- No geometry composition, buffering, or client-side geometry
  processing.

---

## 16. Explicit Non-Goals

- No polygon drawing.
- No geometry unions, exclusions, or buffers.
- No multi-period or multi-polity selection (in search context).
- No mixed temporal authority.
- No client-side geometry processing.
- No external tile provider dependencies.

---

## 17. Estimated Effort

| Phase | Scope | Effort |
|-------|-------|--------|
| 2 | Panel layout (three-column, tabbed Time & Space) | 2 days |
| 5 | Context map (CSS desaturation, preview overlays) | 1--2 days |
| 7 | Tab 1: temporal slider + OSM region selector | 3--4 days |
| 8.2 | PeriodO ES index build pipeline | 1--2 days |
| 8.3 | Tab 2: PeriodO period selector | 3--4 days |
| 9 | Tab 3: polity selector | 2--3 days |
| 10--12 | Tab switching, state management, search triggering | 2--3 days |
| 11 | FastAPI search endpoint | 2--3 days |
| 13 | Dataset metadata PeriodO integration | 1 day |

**Prerequisites:**

- OSM administrative boundary geometries indexed as `geo_shape`
  fields in the `places` index.
- Shapely available in the CRC FastAPI environment for geometry
  simplification during preview serving.
- Polity dataset indexed in ES (schema and pipeline to be specified
  separately).