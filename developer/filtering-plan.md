# WHG Integrated Filter Panel: Space, Time, and Period

> **Audience:** Human developers and coding agents working on the WHG v3.5
> codebase (Django / Elasticsearch / JavaScript).
>
> **Status:** Design document and implementation plan.

---

## 1. Overview

The WHG search interface requires three interconnected filtering
dimensions: **where** (spatial region), **when** (temporal range), and
**what period** (PeriodO definitions).  These are currently served by
separate, loosely coupled UI elements: a map with a time slider, a
deficient country/region selector drawing from a fixed Django table,
and a bare text box for period names.

This document specifies a unified filter panel in which the three
dimensions inform one another:

- Selecting a spatial region constrains which PeriodO periods are
  offered and which portion of the time slider is highlighted.
- Adjusting the temporal range constrains which PeriodO periods are
  offered.
- Selecting a PeriodO period can refine both the spatial overlay and
  the temporal range, since each period definition carries its own
  spatial coverage and date extent.

The panel uses a **context map** that is visually distinct from the
main search-results map, and a **spatial selector** backed by the ES
`places` index rather than a static Django table.

---

## 2. The Context Map

The filter panel includes a small map whose purpose is orientation
and region selection, not result display.  It should be styled to
make this distinction clear.

### 2.1 Visual Treatment

WHG serves its own tiles from a tileserver built on OSM data with
NaturalEarth colouring and DEM hillshading.  No external tile
dependencies are used.

- **Desaturated basemap.**  In the first instance, apply a CSS
  `filter: saturate(0.3) brightness(1.05)` (or similar) to the
  context map's tile layer container.  This is cheap to implement
  and reversible.  A dedicated low-saturation tileserver style can
  be developed later if the CSS approach proves insufficient (e.g.
  if label legibility suffers under desaturation).
- **Prominent labels and features.**  The basemap should show more
  place-name labels, country boundaries, major rivers, and terrain
  shading than the results map.  These contextual features help users
  orient themselves when selecting a region they may know by name but
  not by precise coordinates.  If the CSS desaturation approach is
  used, the existing tile style already carries these features; the
  concern is whether labels remain legible at reduced saturation.
  Test and adjust the saturation value accordingly.
- **Selected-region overlay.**  When a spatial filter is active, the
  selected region is drawn as a semi-transparent polygon fill with a
  distinct border.  Everything outside the region is dimmed (inverted
  mask).

> **Coding-agent note:** The map uses the WHG tileserver exclusively.
> Do not introduce dependencies on external tile providers (Mapbox,
> Stamen, CartoDB, etc.).

### 2.2 Interaction

- **Pan and zoom** to set the viewport.  The viewport bbox can itself
  serve as a loose spatial filter ("search within the visible area")
  when no named region is selected.
- **Draw a polygon or bounding box.**  The current implementation
  supports user-drawn polygons on the map; this must be preserved.
  The drawing tools should offer both freehand/vertex polygon drawing
  and a rectangular bbox mode.  Drawn geometries are added to the
  composite spatial filter (see section 3.5).
- **Click a region polygon** (if pre-loaded, e.g. country boundaries
  at low zoom) to select it and add it to the composite spatial
  filter.

---

## 3. Spatial Selector

The current country/region selector draws from a fixed list in the
Django database.  This is replaced by a type-ahead search against
the ES `places` index, which already contains administrative and
regional entities from multiple authority sources, many with
geometries or bounding boxes.

### 3.1 Why OSM as the Primary Spatial-Filter Source

The `places` index contains spatial entities from GeoNames, Wikidata,
TGN, and OpenStreetMap.  Of these, OSM offers the most practical
single-authority basis for spatial filtering:

- **Comprehensive administrative boundaries** worldwide, from
  countries (`admin_level=2`) down through states/provinces
  (`admin_level=4`), counties/districts (`admin_level=6`), and
  municipalities (`admin_level=8`).
- **Actual polygon geometries**, not just centroids.  Most GeoNames
  records carry only point coordinates; OSM boundary relations carry
  full polygons.
- **Consistent hierarchical admin levels**, providing a natural
  granularity control: the selector can offer coarser or finer
  regions depending on the map zoom level.
- **A single namespace**, avoiding the reconciliation burden of
  merging overlapping administrative entities from GeoNames, Wikidata,
  and TGN.

The full range of authorities remains available as a secondary
source for cases where OSM coverage is weak, notably for historical
regions (e.g. "Mesopotamia", "Magna Graecia") that exist in
Wikidata or TGN but not in OSM.  These can be offered alongside OSM
results, distinguished by a source label.

### 3.2 Spatial Selector Behaviour

1. The user types a region name into a search input (e.g.
   "Lincolnshire", "Anatolia", "Tuscany").
2. After a debounced delay, the widget queries the `places` index for
   records matching the text, filtered to administrative/regional AAT
   types and to records that carry a geometry or bounding box.
3. Results are displayed as a dropdown, each showing the region name,
   its parent context (e.g. "Lincolnshire, England, United Kingdom"),
   and its administrative level or type.
4. Results are ranked by:
   - Specificity relative to the current map viewport (regions that
     fit within or closely match the viewport rank higher).
   - Administrative level (prefer mid-level regions over very large
     or very small ones unless the user's query is unambiguous).
   - Textual match quality.
5. Selecting a result loads its geometry onto the context map as the
   active spatial filter.  The map zooms to fit the selected region.
6. If the selected entity has only a bounding box (no polygon), use
   the bbox as a rectangular spatial filter.
7. The active spatial filter is shown as a dismissible chip/tag below
   the search input.

### 3.3 ES Query Construction

> **Coding-agent note:** The query should target the `places` index
> with a `bool` query combining:
> - A `match` or `match_phrase_prefix` on the name field for the
>   user's text input.
> - A `terms` filter on the type field restricting to
>   administrative/regional AAT types (or their mapped GeoNames
>   equivalents: `ADM1`, `ADM2`, `ADM3`, `PCLI`, `PCLF`, etc., and
>   OSM admin boundary types).
> - A `geo_bounding_box` or `geo_shape` filter restricting to
>   entities that intersect the current map viewport, if the viewport
>   is tighter than the world view.
> - Optionally a `terms` filter on the source/authority field to
>   prefer OSM results, with a secondary `should` clause to include
>   non-OSM results at lower score.
>
> Geometry storage: the `places` index must store geometries in a
> format queryable by ES `geo_shape` queries.  For OSM admin
> boundaries this means indexing the boundary polygon as a
> `geo_shape` field.  If the current index schema does not include
> geometries for OSM records, this is a prerequisite task.

### 3.4 Fallback

If the user's query matches no indexed region, permit free-text entry
as a label-only filter (no polygon; spatial filtering falls back to
the map viewport bbox).

### 3.5 Composite Spatial Filters

The spatial filter is not limited to a single polygon.  The user may
build a composite geometry from multiple sources: one or more
ES-selected named regions, one or more hand-drawn polygons or
bounding boxes, and (optionally) spatial coverage inherited from a
selected PeriodO period.  The filter panel must support combining
these into a single effective query geometry.

#### Operations

**Union (additive).**  The default composition mode.  Each new
geometry (whether selected from ES or drawn on the map) is added to
the existing filter.  The effective query geometry is the union of
all component polygons.  This is the common case: "Lincolnshire and
East Riding of Yorkshire" or "this drawn area plus that named
region."

**Buffer.**  Any component polygon can be expanded by a specified
distance (in kilometres).  This is useful when the user's area of
interest extends somewhat beyond a named administrative boundary.
The buffer is applied per-component before the union is computed.

**Exclusion.**  A component polygon can be subtracted from the
composite geometry.  This handles "the Peloponnese except Laconia"
or "this drawn area minus that named region."  Excluded regions are
drawn on the map with a distinct hatch or crosshatch fill.

#### UI

Each component geometry appears as a dismissible chip in the spatial
filter summary, labelled with its source ("Lincolnshire", "drawn
polygon", "5 km buffer").  Each chip has a toggle for
inclusion/exclusion mode and an optional buffer control.

The context map shows all component polygons simultaneously: included
regions in the standard semi-transparent fill, excluded regions in
hatch fill, buffers as a lighter outer band.

#### Architecture: Server-Side Composition via the CRC FastAPI

The WHG ES instance runs on the Pitt CRC VM behind a **FastAPI**
gateway.  Geometry composition and buffering happen there, not on
the Django/DigitalOcean server or in the browser.

**Geometry preview flow (interactive, as the user builds the filter):**

When the user selects a named region or PeriodO period, the browser
requests its geometry from the FastAPI so it can be drawn on the
context map immediately.  The user needs to see what they are
selecting.  This is a lightweight by-ID fetch:

```
GET /geometry/osm:relation/123456
GET /geometry/periodo:ark:/99152/p05krdxmkzt
```

The FastAPI looks up the geometry in the `places` or
`periodo_periods` ES index and returns it as GeoJSON.  The browser
draws it on the map as a preview overlay.  For very complex polygons,
the FastAPI may return a simplified version (e.g.
`shapely.simplify(tolerance=0.01)`) to keep the preview responsive.

The browser holds these preview geometries in memory for map display
but does not send them back to the server.

**Search flow (when the user executes the search):**

The browser sends a compact filter descriptor.  Named geometries are
referenced by ID; only user-drawn polygons are sent as GeoJSON:

```json
{
  "spatial_components": [
    {
      "ref_type": "osm",
      "ref_id":   "osm:relation/123456",
      "mode":     "include",
      "buffer_km": 5
    },
    {
      "ref_type": "periodo",
      "ref_id":   "http://n2t.net/ark:/99152/p05krdxmkzt",
      "mode":     "include",
      "buffer_km": 0
    },
    {
      "ref_type": "drawn",
      "geo":      { "type": "Polygon", "coordinates": [...] },
      "mode":     "exclude",
      "buffer_km": 0
    }
  ],
  "temporal": { "start_year": -1200, "stop_year": -700 },
  "periods":  ["http://n2t.net/ark:/99152/p05krdxmkzt"],
  "type_filter": { ... },
  "text_query": "..."
}
```

The FastAPI then:

1. Re-fetches the full-resolution polygon geometries for each `osm`
   and `periodo` component by ID from ES.  (These may be more
   detailed than the simplified previews sent to the browser.)
2. Applies per-component buffers using Shapely
   (`geometry.buffer(...)` with geodesic projection).
3. Computes the composite geometry: union of all `"include"`
   components, then difference of all `"exclude"` components.
4. Runs the `places` search query using the composite geometry as a
   `geo_shape` filter, combined with temporal, type, and text filters.
5. Returns the search results and the final composite geometry (as
   GeoJSON) to the browser, so the context map can update its overlay
   to show the actual buffered/composed region used for filtering.

**Why this split works:**

- **Preview is immediate.** Each component geometry is fetched
  individually as the user selects it, so the map updates
  incrementally.  Simplification keeps previews responsive.
- **Search payload is small.** Named-region polygons (especially OSM
  admin boundaries with thousands of vertices) never transit from
  browser to server; only their IDs do.
- **Geometry quality.** Shapely on the CRC FastAPI handles complex
  polygon operations, geodesic buffers, and topology repair more
  robustly than any browser-side library.
- **No client-side geometry library.** Turf.js is not needed in the
  frontend build.  The browser only needs to draw GeoJSON on the map,
  which MapLibre handles natively.
- **Single search round-trip.** Geometry resolution, composition,
  buffering, and the ES search all happen within the FastAPI in one
  request.

> **Coding-agent note:** The geometry preview endpoint and the search
> endpoint are both on the CRC FastAPI (Pitt VM), not the
> Django/DigitalOcean server.  Use Shapely for all geometry
> operations.  Project to an appropriate equal-area CRS (e.g. an
> auto-UTM zone derived from the geometry centroid) before computing
> buffers, then project back to WGS84 for the ES query and for the
> GeoJSON returned to the browser.
>
> For preview simplification, use `shapely.simplify(tolerance,
> preserve_topology=True)` with a tolerance that keeps the polygon
> recognisable at the current map zoom level.  A fixed tolerance of
> ~0.01 degrees is a reasonable starting point; a zoom-adaptive
> tolerance is a refinement for later.
>
> Cache component geometries in memory on the FastAPI for the
> duration of a session or with a short TTL, so that the search
> request does not re-fetch geometries that were just served as
> previews.

---

## 4. Temporal Selector

The existing time slider is retained with minor refinements.

- The slider defines a date range (`start_year`, `stop_year`) using
  the same year convention as PeriodO: negative integers for BCE,
  positive for CE.
- When a PeriodO period is selected (section 5), the slider
  optionally snaps to or highlights the period's temporal extent,
  giving the user a visual cue of the time range they have selected.
- The slider range should be contextual: if the spatial filter is set
  to a region with a well-understood archaeological/historical range,
  the slider's default extent could be adjusted accordingly.  (This
  is a refinement, not a launch requirement.)

---

## 5. PeriodO Period Selector

### 5.1 Background

PeriodO (`https://perio.do`) is a public-domain gazetteer of
scholarly definitions of historical periods.  Key characteristics:

- **Flat structure.** No `skos:broader`/`skos:narrower` hierarchy.
  Each period is an independent assertion.
- **Multiple competing definitions.** "Bronze Age" has hundreds of
  entries, each scoped to a different region and date range by a
  different authority.
- **Rich metadata.** Each definition carries a label, temporal extent
  (start/stop years), spatial coverage (textual description and
  gazetteer links), and bibliographic authority source.
- **Persistent URIs.** Each period has an ARK identifier resolvable
  to JSON-LD.

PeriodO data is available as a single JSON download from
`https://data.perio.do/dataset/`.

### 5.2 PeriodO ES Index

Rather than a static JSON file or Django model, PeriodO period
definitions are stored in a dedicated ES index (`periodo_periods`) on
the CRC staging instance.  This enables the period selector to query
with the same spatial and temporal filtering patterns used elsewhere
in WHG.

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
boxes or polygons using cached gazetteer data from the WHG `places`
index.

> **Coding-agent note:** Where a period's spatial URIs cannot be
> resolved, set `spatial_geo` to null.  These records should appear in
> label-only searches but be excluded from spatial filtering.  Where
> multiple spatial URIs resolve to different regions, compute the
> bounding box of their union.

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

Re-run monthly.  Use the ES index-alias swap pattern for zero-downtime
updates.

### 5.3 Period Selector Behaviour

The period selector is a reusable component mounted in both the search
interface and the dataset metadata form.

#### Inputs

| Parameter | Type | Description |
|-----------|------|-------------|
| `spatialContext` | geo_shape or bbox or null | From the spatial selector or map viewport. |
| `temporalContext` | `{start: int, stop: int}` or null | From the time slider. |
| `initialSelection` | array of URIs | Pre-selected periods (for edit mode). |
| `multiSelect` | boolean | Default true. |

#### Output

| Event | Payload |
|-------|---------|
| `onChange` | Array of `{uri, label, start_year, stop_year, spatial_description}` for selected periods. |

#### Filtering

1. The user types a label fragment (e.g. "Iron Age").  After a
   debounced delay, the widget queries the `periodo_periods` ES index
   with:
   - A `match` or `match_phrase_prefix` on `label` and `alt_labels`.
   - A `geo_shape` intersects filter on `spatial_geo` using
     `spatialContext`, if provided.
   - A numeric range filter: `start_year <= temporalContext.stop AND
     stop_year >= temporalContext.start`, if provided.
2. Results are sorted by:
   - Exact label match first.
   - Temporal proximity to the context midpoint (if temporal context
     is active).
   - Alphabetically by label.
3. If no spatial or temporal context is active, results are grouped
   by `spatial_description` so that, e.g., Aegean and Levantine
   definitions of "Iron Age" cluster separately.

#### Results display

Each result row shows:

- The period label.
- A compact year range (e.g. "1100--750 BCE").
- A small inline horizontal bar representing the temporal extent,
  drawn to a common scale across all visible results.  If a temporal
  context is active, the context range is indicated as a shaded
  background band on the same scale.
- The spatial description (e.g. "Greece", "Near East and Greece").
- The authority label, displayed smaller / de-emphasised.

#### Selection

- Clicking a result toggles its selection state.
- Selected periods appear as dismissible chips above the search input.
- In single-select mode, selecting a new period deselects the
  previous one.

#### Authority filter

A collapsible "Filter by authority" panel lists the distinct
authorities in the current filtered results with counts.  Selecting
an authority restricts results to that authority's periodisation.

### 5.4 Bidirectional Context Flow

When a PeriodO period is selected:

- Its temporal extent is offered as a suggested range for the time
  slider (the user can accept or adjust).
- Its spatial coverage is offered as a suggested region for the
  spatial selector (the user can accept or adjust).
- If the user accepts both, the filter panel is fully constrained by
  the selected period definition.

This bidirectional flow means the user can approach the filter from
any direction: "I know the region, help me find the right period" or
"I know the period, show me where and when it applies."

---

## 6. Integration: How the Three Dimensions Compose

### 6.1 Filter State Model

The filter panel maintains a single client-side state object:

```
{
  spatial: {
    components: [
      {
        id:         "abc123",
        ref_type:   "osm" | "drawn" | "periodo",
        ref_id:     "osm:relation/123456" | null,
        label:      "Lincolnshire" | "drawn polygon" | null,
        mode:       "include" | "exclude",
        buffer_km:  0,
        preview_geo: <GeoJSON geometry>   // simplified geometry
                                          // fetched from FastAPI
                                          // for map display only;
                                          // null until fetched
      }
    ],
    composite_geo: <GeoJSON geometry>,    // returned by FastAPI
                                          // after a search;
                                          // null until first search
    viewport_fallback: true | false       // use map viewport if
                                          // no components present
  },
  temporal: {
    start_year: -1200,
    stop_year:  -700,
    source: "slider" | "periodo" | null
  },
  periods: [
    { uri, label, start_year, stop_year, spatial_description }
  ]
}
```

**Two kinds of geometry live in this state:**

- `preview_geo` on each component is a (possibly simplified) geometry
  fetched from the CRC FastAPI at selection time, used to draw the
  component on the context map.  It is never sent back to the server.
- `composite_geo` is the final buffered/composed geometry returned by
  the FastAPI after a search.  It replaces the individual preview
  overlays on the context map to show the actual filter region.

**When the search fires**, the browser serialises the components as a
compact descriptor (section 3.5): `ref_type`, `ref_id`, `mode`, and
`buffer_km` for named components; `ref_type`, `geo`, `mode`, and
`buffer_km` for drawn components.  The FastAPI resolves, buffers,
composes, queries, and returns both results and `composite_geo`.

Any change to one dimension triggers re-filtering of the **period
selector only** (lightweight queries against the small
`periodo_periods` index):

- Changing `spatial` (adding, removing, or modifying a component)
  re-queries `periodo_periods` with the new spatial constraint.
  (For PeriodO filtering, the individual `preview_geo` values are
  sufficient to construct the spatial query; the full composite is
  not needed until the main search fires.)
- Changing `temporal` re-queries `periodo_periods` with the new
  temporal constraint.
- Selecting a period offers to add its spatial coverage as a new
  `"periodo"`-sourced component and to update `temporal` from the
  period's own extent.

**The main `places` search is never triggered automatically.**
Changes to any filter dimension mark the search state as **dirty**
(i.e. the displayed results no longer reflect the current filters).
The user must explicitly press a **Search** button to execute the
query.  This avoids unnecessary load on ES and the FastAPI geometry
composition pipeline while the user is still assembling a
potentially complex multi-component filter.

The Search button should be persistently visible in the filter panel
and visually emphasised (e.g. highlighted or pulsing) when the
filter state is dirty.  If the user has not yet run any search, the
button reads "Search"; if results are displayed but the filters have
since changed, the button reads "Update results" or similar, making
it clear that the current results are stale.

> **Coding-agent note:** Track dirty state with a simple boolean flag
> toggled to `true` on any filter change and to `false` when search
> results are received.  The PeriodO period selector re-queries
> reactively on spatial/temporal changes (these are cheap, hitting a
> small index); the main `places` search fires only on explicit user
> action.

### 6.2 Feeding Into the Main Search

When the user presses the Search button, the browser sends the
filter descriptor to the CRC FastAPI.  The FastAPI composes the
spatial filter, runs the `places` search, and returns results plus
the composite geometry:

- **Spatial:** the FastAPI constructs and applies the `geo_shape`
  filter internally.  The browser never sends full polygon
  geometries for named components.
- **Temporal:** a numeric range filter on the place's temporal
  fields, if the places index records temporal coverage.
- **Periods:** currently, most WHG place records do not carry PeriodO
  URIs.  For v3.5, the period filter operates indirectly via its
  temporal extent (translated to a date range) and spatial coverage.
  When contributors begin associating datasets with PeriodO URIs
  (section 7), direct period-URI matching becomes possible.

### 6.3 Context Map Updates

When any filter dimension changes, the context map updates:

- All spatial components are drawn simultaneously: included regions
  in semi-transparent fill, excluded regions in hatch fill, buffer
  bands as a lighter outer ring.
- Each component is clickable for selection/editing/removal.
- If a PeriodO period is selected and its spatial coverage has not
  been added as a component, the period's region can be shown as a
  secondary dashed outline for comparison, with a prompt to add it.

---

## 7. Dataset Metadata: Associating Periods with Datasets

The period selector component (section 5.3) is also mounted in the
dataset metadata input/edit form.  When a contributor creates or
edits a dataset, they can search for and select PeriodO period
definitions that describe the dataset's temporal scope.

- `spatialContext` is derived from the dataset's spatial coverage
  (if already entered).
- `temporalContext` is derived from any temporal fields already
  entered.
- `initialSelection` is populated from any PeriodO URIs already
  stored against the dataset.
- `onChange` writes the selected URIs to the dataset metadata.

Over time, as datasets accumulate PeriodO associations, direct
period-based search becomes viable: "find all datasets (and their
places) associated with this period definition."

---

## 8. Migration Path to v4

- The `periodo_periods` ES index migrates to a collection in the
  ArangoDB `indexing` database.
- Spatial filtering uses ArangoDB's geo-spatial index capabilities.
- The bidirectional context flow between spatial, temporal, and period
  filters maps naturally to graph traversal when PeriodO periods,
  places, and regions are all nodes in the same graph.
- The OSM-sourced spatial filter polygons become a dedicated
  collection, queryable by geo-spatial intersection.

---

## 9. Estimated Effort

| Phase | Scope | Effort |
|-------|-------|--------|
| 2 | Context map (CSS desaturation, drawing tools, interaction) | 2 days |
| 3.1--3.3 | Spatial selector (ES-backed, OSM primary) | 3--4 days |
| 3.5 | Composite spatial filters (FastAPI geometry endpoints, Shapely composition) | 3--4 days |
| 5.2 | PeriodO ES index build pipeline | 1--2 days |
| 5.3 | Period selector widget | 3--4 days |
| 5.4 + 6 | Bidirectional context flow and filter composition | 2--3 days |
| 7 | Dataset metadata integration | 1 day |

**Prerequisites:**

- OSM administrative boundary geometries must be indexed as
  `geo_shape` fields in the `places` index.  If not currently
  present, this is an additional prerequisite task (estimated 2--3
  days depending on the current OSM ingestion pipeline).
- Shapely and pyproj must be available in the CRC FastAPI environment
  for server-side geometry composition and buffering.