# WHG v3.5 Search Filters: Implementation Record

> **Audience:** Human developers and coding agents working on the WHG v3.5
> codebase (Django / Elasticsearch / JavaScript / MapLibre).
>
> **Stack:** Django on DigitalOcean; Elasticsearch behind a FastAPI
> gateway on the Pitt CRC VM; MapLibre with the WHG tileserver
> (OSM data, NaturalEarth colouring, DEM hillshading).
>
> **Status:** Front-end implemented; backend search integration pending.
>
> This document describes the **actually-implemented** front-end
> filter interface as of April 2026.  It supersedes the earlier
> design-phase specification and serves as the authoritative reference
> for connecting the backend.

---

## 1. Design Principles (unchanged from original plan)

The filters panel constrains the main WHG toponym and place-type
search.  It is explicitly **not a GIS tool**.

- **One temporal authority at a time.**  The user chooses whether time
  is controlled manually (dateline slider), derived from a PeriodO
  period, or derived from a territory dataset.  These are mutually
  exclusive — enforced by tab switching.
- **Viewport as primary spatial constraint.**  The context map's
  visible extent is always the spatial filter baseline.  A selected
  geometry (OSM region, PeriodO coverage, territory boundary) is
  intersected with the viewport, never used alone.
- **No client-side geometry processing.**  No polygon drawing, no
  unions, no buffers, no exclusions.
- **Search triggered from the main search bar.**  There is no
  in-panel Search button.  The main search bar's search icon (and
  Enter key) triggers the query.  Filter changes are reflected in a
  badge count on the Filters toggle button.  Filter-only searches
  (no toponym) are permitted when at least one place type is
  selected together with a temporal or spatial constraint.

---

## 2. Panel Layout

### 2.1 Three-Column Grid

The filters live in a collapsible `#search_filters` panel beneath
the search bar.  The panel uses a CSS grid (`.filters-grid`):

| Column | CSS class | Width | Content |
|--------|-----------|-------|---------|
| 1 | `.filter-col--authorities` | `auto` (narrow, max 190 px) | Checkbox list of data sources |
| 2 | `.filter-col--timespace` | `2fr` (double-width) | Tabbed time/space interface + context map |
| 3 | `.filter-col--types` | `1fr` | AAT place-type tree widget |

Grid definition:
```css
grid-template-columns: auto 2fr 1fr;
```

Responsive breakpoints stack columns at `< 992 px` (two-column) and
`< 576 px` (single-column), with Time & Space always spanning the
full width when stacked.

### 2.2 Time & Space Internal Layout

The Time & Space panel (`.timespace-inner`) is itself a two-column
flex layout:

- **Left** (`.timespace-controls`, `flex: 1 1 0`): tab bar + tab
  content.
- **Right** (`.timespace-map`, `width: 45%`): persistent context map.

The context map is always visible regardless of active tab.

### 2.3 Time & Space Tabs

| Tab | ID | Icon | Label | Purpose |
|-----|----|------|-------|---------|
| 1 | `tab-timespan` / `pane-timespan` | `fa-map-marker-alt` | Region | Manual dateline slider + OSM region type-ahead |
| 2 | `tab-periods` / `pane-periods` | `fa-hourglass-half` | Period | PeriodO period selector (temporal authority from period) |
| 3 | `tab-polities` / `pane-polities` | `fa-flag` | Territory | Territory/polity dataset selector (Cliopatria, D-PLACE, NativeLand) |

Tab 1 (Region) is the default active tab.

---

## 3. Data Sources Filter

### 3.1 Implementation

File: `search.html`, lines within `.filter-col--authorities`.

A vertical list of `<label>` elements, each wrapping a checkbox
(`.authority-cb`) and a descriptive label.  Hover `title` attributes
explain each source.

### 3.2 Sources Listed

| Value | Label | Default | Notes |
|-------|-------|---------|-------|
| `gn` | GeoNames | ✔ checked | ~12M features from national mapping agencies |
| `wd` | Wikidata | ✔ checked | ~8M places, multilingual labels, structured data |
| `tgn` | TGN | ✔ checked | ~3M Getty scholarly place name records |
| `pl` | Pleiades | ✔ checked | ~37K ancient/classical places |
| `iv` | IndexVillaris | ✔ checked | ~24K 17th-c. English/Welsh place names |
| `whg` | WHG datasets | ✔ checked | Contributed historical gazetteers |
| `osm` | OpenStreetMap | ✘ unchecked | Hover explains noise (buildings, bus stops, etc.) |
| `gb` | GB1900 | ✘ unchecked | ~800K OS labels 1888–1914, very noisy |

### 3.3 Behaviour

- Multi-select with OR logic.  Selecting none produces no results.
- On change, the checked values are written to
  `filterState.authorities` and the active-filters badge updates.
- On "Clear search", defaults are restored (all checked except OSM
  and GB1900).

### 3.4 Clustering Toggle

Below the authority checkboxes, a titled "Clustering" section
contains a Bootstrap-style on/off switch (default: on) and an
`(i)` info icon.

| State | Behaviour |
|-------|-----------|
| **On** (default) | Linked place records from different sources are grouped together as a single result, showing a link count badge.  Searches both the `whg` (clustered) and `pub` (unclustered) indices. |
| **Off** | Every individual place record is returned separately, with no grouping.  Searches only the `pub` index. |

Hovering the `(i)` icon reveals a tooltip: _"When enabled, linked
place records from different sources are grouped together as a
single result, showing the number of linked attestations.  When
disabled, every individual place record is returned separately."_

The clustering state is passed to the backend as `cluster: true|false`
in the search payload and stored in `filterState.clustering`.
The toggle is reset to "on" on "Clear search".

### 3.5 Planned ES Query Fragment

```json
{ "terms": { "authority": ["gn", "wd", "tgn", "pl", "iv", "whg"] } }
```

---

## 4. Place Types Filter

### 4.1 Implementation

File: `search.html`, `.filter-col--types`.  Widget class:
`TypeTreeWidget` (`typeTreeWidget.js`), mounted in
`#aat_type_tree`.

The panel header includes a count badge (`#tree_selection_badge`)
and a "clear all" link (`#tree_clear`).

### 4.2 Behaviour

- Multi-select checkbox tree built from the Getty AAT place-type
  hierarchy.
- Selecting a node includes all descendants.
- Selected identifiers are written to `filterState.place_types`.
- The tree container scrolls independently (max-height 340 px on
  desktop, 200 px on mobile).
- An inline search box (sticky at top of the scrollable container)
  filters tree nodes by label match.

### 4.3 Hierarchical Post-Filtering Note

An `(i)` icon on the panel title line shows a tooltip on hover:
_"Select types to filter results. Selecting a parent category
includes all its children. Results matching your selected types
exactly are ranked highest; broader matches appear below."_

This describes the planned consanguinity banding behaviour where
exact-type matches are ranked above broader ancestor matches in
the result list.

---

## 5. Context Map

### 5.1 Implementation

File: `contextMap.js` — singleton class `ContextMap`.
HTML container: `#context_map` inside `#context_map_wrap`.

Initialisation parameters:
```js
{
    container: 'context_map',
    maxZoom: 14,
    style: ['WHG'],       // WHG tileserver only — no external tiles
    globeControl: true,    // Toggle control between globe and flat
    globeMode: true,       // Defaults to globe projection
    navigationControl: true,
    fullscreenControl: false,
    downloadMapControl: false,
    drawingControl: false,
    temporalControl: false,
}
```

### 5.2 Visual Treatment

- **Desaturated basemap** via CSS:
  `#context_map .maplibregl-canvas { filter: saturate(0.3) brightness(1.05); }`
- Square aspect ratio (`aspect-ratio: 1`) in a border-radius
  container.
- Globe view by default; a toggle control allows switching to flat
  projection.

### 5.3 Overlay Management

A single GeoJSON source (`filter-overlay`) with fill and line
layers.  Only one overlay is displayed at a time:

- `setOverlay(geojson)` — displays a geometry preview (region,
  period coverage, or territory boundary).
- `clearOverlay()` — empties the source.
- Fill: `#4a90d9` at 15% opacity.  Stroke: `#2563eb`, 2 px, 70%.

### 5.4 Viewport Tracking

On every `moveend` event, the bounding box is written to
`filterState.spatial.bbox` as `[west, south, east, north]`.
External listeners can subscribe via `onViewportChange(callback)`.

### 5.5 Zoom Gate

Selector inputs (region, period, territory search boxes) are
**disabled** until the context map has been zoomed beyond level 2.
While disabled, the inputs show the placeholder _"Zoom the map
first to constrain your search area"_.  Once the threshold is
crossed, inputs are permanently enabled for the session (re-engaging
only on full clear).

### 5.6 Globe Auto-Rotation

When the filter panel first opens, the globe rotates slowly
**westward** (matching Earth's apparent rotation as seen from above
the north pole) at 6°/s — approximately one full revolution per
minute.  This avoids favouring any particular hemisphere.

The rotation stops immediately and permanently on any user
interaction: mouse click/drag, touch, scroll wheel, or any map
control.  It also stops if the filter panel is collapsed.  Once
stopped it does not auto-restart.

---

## 6. Tab 1: Region (Timespan + Map)

### 6.1 Structure

The Region tab (`#pane-timespan`) contains two sub-sections, each
with a small-caps subtitle:

1. **Time** — the dateline slider with a three-state mode toggle.
2. **Space** — an informational note and the OSM region type-ahead.

A horizontal divider (bottom border on `#dateline_container`)
separates the Time and Space sections.

### 6.2 Dateline Temporal Slider

The existing `Dateline` widget (`dateline.js`) is instantiated with
these parameters:

```js
{
    fromValue: 800,
    toValue: 1800,
    minValue: -2000,
    maxValue: 2100,
    open: true,
    includeUndated: null,  // managed externally
    epochs: null,
    automate: null,
}
```

The widget is always expanded (collapse button hidden via CSS).  The
help icon and built-in undated checkbox are also hidden.

#### Colour overrides

The slider's default red/orange palette is overridden to match the
search panel's blue-grey scheme:

| Element | Colour |
|---------|--------|
| Slider track | `#b0c4d8` |
| Slider thumbs | `#4c79a6` (SVG replaced) |
| Thumb hover glow | `rgba(76,121,166,0.4)` |
| Year buttons | `#4c79a6` text, `#f8f9fa` bg, `#b0c4d8` border |
| Tooltip | `#4c79a6` bg |
| Tick labels | `#555` |
| Range highlight | `#4c79a6` |

#### Three-State Mode Toggle

A `.temporal-mode-toggle` button group above the slider controls
temporal filtering:

| Mode | `data-temporal-mode` | Behaviour |
|------|---------------------|-----------|
| **Off** (default) | `off` | No temporal filter.  Slider is greyed out (`opacity: 0.35`, `pointer-events: none`). |
| **Year range** | `range` | Slider is active.  Results restricted to places attested within the selected range. |
| **Range + undated** | `undated` | As above, but also includes places with no temporal attestation. |

Toggle buttons use the shared `.temporal-mode-toggle .btn` styling:
inactive is `#f8f9fa` bg; active is `#e8f0fe` bg with `#1a56db`
text and `#a0b8e8` border.

### 6.3 Region Selector

Widget: `RegionSelector` class (`regionSelector.js`), mounted in
`#region_selector_container`.

#### Info note

An `(i)` icon on the "Space" subtitle line shows a tooltip on
hover: _"Search for a named region below, or leave empty to use
the current map extent as a spatial filter."_

#### Unified tier toggle

A single `.region-tier-toggle` button group offers eight options
(with `flex-wrap` for two-row layout):

| Tier | `data-tier` | Behaviour |
|------|-------------|-----------|
| **Off** (default) | `off` | No spatial region constraint. Search input hidden. |
| **Map bounds** | `mapbounds` | Use the current viewport as an explicit spatial filter. Search input hidden. Sets `geometry_source = 'mapbounds'`. |
| **Continental** | `continental` | UN M49 continental regions (6). Shows the 5 closest suggestions on the context map. Search input filters client-side. |
| **Sub-Continental** | `subcontinental` | UN M49 subregions (22) merged with former intermediary regions (2) = 24 total. Same suggestion behaviour. Search input filters client-side. |
| **Country** | `country` | OSM admin level 2. Type-ahead backend search. |
| **State** | `region` | OSM admin levels 3–4. Type-ahead backend search. |
| **District / County** | `district` | OSM admin levels 5–6. Type-ahead backend search. |
| **Municipality** | `municipality` | OSM admin levels 7–8. Type-ahead backend search. |

The toggle follows the same visual model as the temporal mode
toggle.  **All buttons except "Off" are disabled until the context
map passes the zoom gate (§5.5).**  Disabled buttons show reduced
opacity.

#### Zoom gate on tier buttons

All tier buttons carry the class `.zoom-gated-tier` and the HTML
`disabled` attribute.  When `regionSelector.enableTiers()` is called
(from `enableSelectorInputs()` in `search.js`), all tier buttons are
enabled and the search input placeholder is restored.
`regionSelector.disableTiers()` re-engages the gate on full clear.

#### UN region suggestions

When the user selects "Continental" or "Sub-Continental", the widget
draws the 5 entities whose representative points are closest to the
current map centre **and within the map bounds** as suggestion
markers on the context map (red circles with labels).  The context
map's `setSuggestions(featureCollection)` method manages the marker
layer.  Clicking a suggestion marker dispatches a `suggestion-click`
CustomEvent, which the RegionSelector listens for and adds the
region to the chip list.

#### Multi-selection

- **Multiple regions** can be selected (even across tiers).
- Selecting a region renders a dismissible `.filter-chip` below
  the input and calls `filterState.addToList('spatial.region_id', item)`.
- Items are objects: `{id, label, source, tier}`.
- For UN regions: `id = 'un:<M49_code>'`, `source = 'un_geoscheme'`.
- For OSM regions: `id = '<osm_id>'`, `source = 'osm'`.
- Dismissing a chip removes it from the list via
  `filterState.removeFromList('spatial.region_id', id)`.
- **Switching tiers** (except "Off" and "Map bounds") does NOT
  clear existing selections.  "Off" and "Map bounds" clear all
  selections.
- A **"clear all"** link (`#space_clear`) next to the "Space"
  subtitle clears all selections via `regionSelector.clearAll()`.

#### Clear All link

The "Space" header in the Region tab now includes a "clear all"
link (`#space_clear`), styled identically to the existing "clear
all" links on other sections.  Clicking it calls
`regionSelector.clearAll()`, which resets the tier toggle to "Off",
clears all selected regions, and removes map suggestions.

The UN geoscheme data (including representative points for each
region) is compiled into the `regionSelector.js` module as a
static constant (`UN_GEOSCHEME`), not fetched from the server.
The former "Intermediary" tier (Sub-Saharan Africa, Latin America
and the Caribbean) has been merged into the sub-continental tier.

---

## 7. Tab 2: Period

### 7.1 Implementation

Widget: `PeriodSelector` class (`periodSelector.js`), mounted in
`#period_selector_container`.

The tab header includes a PeriodO external link icon.

### 7.2 Type-ahead Input

- Debounced (250 ms) text search.
- Disabled until zoom gate passes (§5.5).
- Re-queries reactively when the viewport changes (small-index
  query, cheap).
- **Backend stub:** currently shows a "Backend not yet connected"
  placeholder.  When connected, will POST to `periodo_periods`
  index with label match + `geo_shape` intersects on viewport bbox.

### 7.3 Results Display

Results are grouped by `spatial_description`.  Each result row
shows:
- Period label (bold).
- Compact year range (e.g. "1100 BCE – 750 BCE").
- Spatial description.
- Authority label (italic, de-emphasised).

### 7.4 Authority Sub-filter

A collapsible section lists distinct authorities from the current
results with counts.  Clicking an authority restricts results to
that authority's periodisation.  A "clear" link removes the
authority filter.

### 7.5 Selection

- **Multiple periods** can be selected.
- Selecting a period:
  - Appends to `filterState.spatial.period_id` via `addToList()`.
  - Recomputes the **union temporal range** (min start, max stop)
    across all selected periods.
  - Sets `filterState.temporal.source = 'period'`.
  - If `spatial_geo` exists, previews it on the context map and
    flies to fit.
- Renders yellow-toned `.filter-chip--period` chips (one per
  selected period).
- Dismissing a chip removes that period via `removeFromList()` and
  recomputes the union temporal range.  If no periods remain, the
  temporal range reverts to defaults.

---

## 8. Tab 3: Territory

### 8.1 Implementation

Widget: `PolitySelector` class (`politySelector.js`), mounted in
`#polity_selector_container`.

### 8.2 Dataset Toggle

A `.polity-dataset-toggle` button group offers **exclusive**
switching between three territory datasets:

| Value | Label | Description |
|-------|-------|-------------|
| `cliopatria` | Cliopatria | Historical polities and empires |
| `dplace` | D-PLACE | Cultural and linguistic regions |
| `nativeland` | NativeLand | Indigenous territories, languages, and treaties |

Default: Cliopatria.  Switching datasets clears the search
results but does **not** clear existing territory selections,
allowing cross-dataset multi-select.  Only one dataset may be
searched at a time, but selections from different datasets
accumulate.

### 8.3 Type-ahead Input

- Debounced (250 ms) text search.
- Disabled until zoom gate passes (§5.5).
- Re-queries reactively on viewport change.
- **Backend stub:** currently shows a "Backend not yet connected"
  placeholder.  When connected, will POST to the active dataset's
  ES index with label match + geo_shape intersects on viewport bbox.

### 8.4 Selection

- **Multiple territories** can be selected (even across datasets).
- Selecting a territory:
  - Appends to `filterState.spatial.polity_id` via `addToList()`.
  - Recomputes the **union temporal range** (min start, max stop)
    across all selected territories.
  - Sets `filterState.temporal.source = 'polity'`.
  - Previews the territory geometry on the context map and flies
    to fit.
- Renders green-toned `.filter-chip--polity` chips (one per
  selected territory).
- Dismissing a chip removes that territory via `removeFromList()`
  and recomputes the union temporal range.  If no territories
  remain, the temporal range reverts to defaults.

---

## 9. Tab Switching Rules

Switching tabs clears the state of the tab being left, since only
one temporal authority may be active.  Implemented in `search.js`
via the `shown.bs.tab` event on `#timespaceTab`.

| From | To | Actions Taken |
|------|----|---------------|
| Region | Period | Clear selected OSM region, reset dateline to defaults, reset temporal mode toggle to "Off" |
| Region | Territory | Same as above |
| Period | Region | Clear selected period |
| Period | Territory | Clear selected period |
| Territory | Region | Clear selected territory, reset polity dataset to Cliopatria |
| Territory | Period | Same as above |

The viewport persists across tab switches.  Data Sources and Place
Types filters are independent and persist always.

State clearing is handled by `filterState.clearTabState(mode)`,
which resets the relevant `spatial.*` and `temporal.*` fields.
The context map overlay is also cleared.

---

## 10. Filter State Model

Maintained by `filterState.js` as a singleton `FilterState`
instance.

### 10.1 State Shape

```js
{
    authorities: ['gn', 'wd', 'tgn', 'pl', 'iv', 'whg'],
    place_types: [],                    // AAT identifier arrays
    clustering: true,                   // Group linked records (default on)

    mode: 'timespan',                   // 'timespan' | 'period' | 'polity'

    spatial: {
        bbox: null,                     // [west, south, east, north]
        region_id: [],                  // Array of {id, label, source, tier} objects (Tab 1, multi-select)
        period_id: [],                  // Array of {id, label} objects — PeriodO URIs (Tab 2, multi-select)
        polity_id: [],                  // Array of {id, label} objects — Territory IDs (Tab 3, multi-select)
        geometry_source: 'none',        // 'osm' | 'un_geoscheme' | 'mapbounds' | 'period' | 'polity' | 'none'
        preview_geo: null,              // GeoJSON for map display only
    },

    temporal: {
        start_year: -2000,
        stop_year: 2100,
        source: 'manual',              // 'manual' | 'period' | 'polity'
    },

    dirty: false,
}
```

### 10.2 Key Methods

| Method | Purpose |
|--------|---------|
| `get(key?)` | Read full state or a dot-path (e.g. `'spatial.bbox'`) |
| `set(key, value)` | Write a dot-path key; marks dirty (except bbox changes) |
| `addToList(key, item)` | Append an item to an array-valued key (e.g. `spatial.region_id`); deduplicates by `item.id` |
| `removeFromList(key, id)` | Remove an item from an array-valued key by its `id` |
| `clearTabState(mode)` | Reset spatial/temporal fields for the given tab (arrays reset to `[]`) |
| `toSearchPayload()` | Build the search descriptor with `geometry_refs` array (not GeoJSON) |
| `reset()` | Restore all defaults |
| `subscribe(fn)` | Observer pattern; returns unsubscribe function |

### 10.3 Dirty Tracking

Any `set()` call (except `spatial.bbox`) marks `dirty = true`.
Viewport panning does not mark dirty because it is not an explicit
user filter action.  `markClean()` is called after search results
are received.

---

## 11. Search Execution

### 11.1 Trigger

Search is triggered **exclusively** from the main search bar:
- Clicking the search button (`#initiate_search`).
- Pressing Enter in the search input (`#search_input`).

There is no in-panel Search button.  The Filters toggle button
shows a badge with the count of active non-default filter
dimensions.

Changing filters does **not** automatically re-run the search.
The user must click the search button (or press Enter) again to
apply updated filters to the current or a new query.  This allows
filters to be composed at leisure before re-querying.

### 11.2 Exact Match Toggle

A toggle button (`#exact_match_toggle`) sits in the search bar
between the search button and the Filters button.  It shows a
crosshairs icon and the label "Exact".

| State | Appearance | Behaviour |
|-------|-----------|-----------|
| **Off** (default) | Pale neutral grey background (`#eef0f2`), muted text (`#777`) | Phonetic/fuzzy matching — includes similar-sounding names (e.g. "Krakow" finds "Cracow", "Kraków") |
| **On** | Muted teal-blue background (`#6b8ea4`), white text | Exact spelling match only |

The state is passed to the backend as `exact: true|false` in the
search payload.  The toggle is reset on "Clear search".

### 11.3 Filter-Only Searches

A search without a toponym is permitted when:
- At least one place type is selected in the tree, **and**
- At least one of: temporal mode is not "off", a period is
  selected, a territory is selected, or a region is selected.

### 11.4 Query Construction (`gatherOptions()`)

The `gatherOptions()` function assembles:

```js
{
    qstr: '<toponym>',
    idx: '<es_index>',
    fclasses: '<comma-separated AAT IDs>',  // Legacy — retained for backward compatibility
    types: [<AAT IDs>],
    temporal: true|false,           // whether temporal mode is active
    start: <from_year>,
    end: <to_year>,
    undated: true|false,            // whether to include undated records
    exact: true|false,              // whether to require exact spelling match
    cluster: true|false,            // whether to group linked records
    // Legacy-compatible empty fields
    bounds: { type: 'GeometryCollection', geometries: [] },
    regions: [],
    countries: [],
    userareas: [],
    spatial: 'none',
    // New filter state (for when backend is updated)
    filter_state: <full filterState object>,
}
```

The `filter_state` field carries the full state including mode,
spatial references, and temporal parameters.  The backend can
migrate to reading this field when ready.

### 11.5 Planned Search Payload (via `toSearchPayload()`)

When the backend is updated to accept the new format:

```json
{
  "authorities": ["gn", "wd", "tgn", "pl", "iv", "whg"],
  "place_types": [300008347, 300008389],
  "mode": "timespan",
  "spatial": {
    "bbox": [-10.5, 35.2, 45.0, 60.1],
    "geometry_refs": [
      { "index": "osm_admin_polygons", "id": "un:150" },
      { "index": "osm_admin_polygons", "id": "osm:relation/62149" }
    ]
  },
  "temporal": {
    "start_year": -1200,
    "stop_year": -700,
    "source": "manual"
  }
}
```

Geometries are always referenced by index + ID — never sent as
GeoJSON.  The FastAPI fetches the geometries server-side.
Multiple references are sent as an array; the backend applies a
`bool/should` query with `geo_shape intersects` for each ref
(union semantics: a place matching any of the referenced
geometries is included).

---

## 12. Active Filters Badge

The badge on the Filters toggle button (`#active_filters_badge`)
counts active non-default filter dimensions:

1. Place types selected (any).
2. One or more periods selected (array length > 0).
3. One or more territories selected (array length > 0).
4. One or more regions selected (array length > 0).
5. Temporal mode is not "off".
6. Context map has been zoomed (viewport differs from default).
7. Authority selection differs from defaults.

The count is recalculated on every filter state change.

---

## 13. Clear Behaviour

### 13.1 "Clear all" on Time & Space (`#timespace_clear`)

- Resets temporal mode to "Off".
- Resets dateline to 800–1800.
- Clears all three selector widgets (region, period, territory).
- Clears context map overlay and resets viewport.
- Re-engages the zoom gate.
- Calls `filterState.clearTabState()` for all three modes.
- Switches back to the Region tab.

### 13.2 "Clear all" on Place Types (`#tree_clear`)

- Unchecks all tree nodes.
- Resets the selection badge.
- Clears `filterState.place_types`.

### 13.3 "Clear search" (main bar `#clear_search`)

Full reset: clears the toponym, all filters (types, time, space,
authorities), both maps, and all stored state.  Restores default
authority checkboxes.  Returns to the landing/initial view.

---

## 14. Landing State

Before any search, the page shows a centred landing block
(`#landing`) with the WHG logo and explanatory text:

> _"Enter a place name above to search, or open **Filters** to
> refine by data source, place type, time range, region, historical
> period, or named territory.  You can also search by filters
> alone — select at least one place type together with a temporal
> or spatial constraint."_

This block is hidden once results are displayed.

---

## 15. Results Display

After search, the landing block hides and `#content_area` appears
with a two-column layout:

- **Left** (`col-lg-7`): Results map (`#results_map`) showing
  result geometries as point/polygon features.
- **Right** (`col-lg-5`): Scrollable results list
  (`#search_results`).

Each result card shows: title, link count, dataset badge,
description, name variants, types (with AAT links), country codes,
chronology, external links, source ID/URI, depictions, and
relations.  All long lists use a "more or less" toggle-truncate.

Clicking a result zooms the results map; clicking a point on the
results map scrolls to and flashes the corresponding result card.

The filters panel collapses when results arrive.

---

## 16. Guided Tour

### 16.1 Implementation

File: `filtersTour.js` — uses the `driver.js` library (npm
dependency) to walk users through the filter interface.

### 16.2 Tour Steps (11 steps)

| Step | Target | Title | Focus |
|------|--------|-------|-------|
| 1 | `#filters_panel` | Search Filters | Overview of three-panel layout |
| 2 | `.filter-col--authorities` | Data Sources | Explains gazetteer sources, noisy defaults, and clustering toggle |
| 3 | `.filter-col--timespace` | Time & Space | Introduces the context map and tabbed controls |
| 4 | `#tab-timespan` | Region Tab | Dateline slider modes + region selector |
| 5 | `#tab-periods` | Period Tab | PeriodO period search |
| 6 | `#tab-polities` | Territory Tab | Cliopatria / D-PLACE / NativeLand |
| 7 | `#context_map_wrap` | Context Map | Viewport as spatial filter, zoom gate |
| 8 | `.filter-col--types` | Place Types | AAT tree, hierarchical post-filtering |
| 9 | `#exact_match_toggle` | Exact Match | Phonetic vs exact spelling toggle |
| 10 | `#initiate_search` | Search Button | Must click to apply filters; re-search after changes |

### 16.3 Trigger Behaviour

- **First open:** The tour starts automatically (after a 400 ms
  delay) the first time the filters panel is opened.  Completion
  or dismissal sets `localStorage['whg_filters_tour_seen']`.
- **Subsequent opens:** The tour does not auto-trigger.
- **Manual trigger:** A "Take a tour" link in the landing text
  block (`#start_tour_link`) opens the filters panel if needed
  and starts the tour.  This link is always available.

### 16.4 Visual Styling

Driver.js popover styles are overridden in `search.css` to match
the WHG palette:
- Title: `#993333` small-caps (matches `.categories`).
- Description: `#444`, 0.82 rem.
- Next button: `#993333` background.
- Progress text: `#888`, 0.7 rem.

---

## 17. File Inventory

| File | Role |
|------|------|
| `search/templates/search/search.html` | Django template — search bar, three-column filter panel, results area |
| `whg/webpack/js/search.js` | Main orchestration — wires all widgets, handles search lifecycle |
| `whg/webpack/css/search.css` | All filter panel, result, and map styling |
| `whg/webpack/js/filterState.js` | Singleton state model with observer pattern |
| `whg/webpack/js/contextMap.js` | Context map manager (MapLibre, overlays, bbox tracking) |
| `whg/webpack/js/regionSelector.js` | Tab 1 region type-ahead (backend stub) |
| `whg/webpack/js/periodSelector.js` | Tab 2 PeriodO period selector (backend stub) |
| `whg/webpack/js/politySelector.js` | Tab 3 territory/polity selector (backend stub) |
| `whg/webpack/js/filtersTour.js` | Guided tour of the filter interface (driver.js) |
| `whg/webpack/js/typeTreeWidget.js` | AAT place-type tree widget |
| `whg/webpack/js/dateline.js` | Dateline temporal slider widget |

---

## 18. Backend Specification

The backend — ES indexes, gateway API endpoints, request/response
contracts, data ingestion pipelines, and geometry cleaning rules —
is specified in the companion document **`specification.md`** in
this directory.  That document is the authoritative reference for
the CRC Gateway VM codebase.

---

## 19. Migration Path to v4

- The `periodo_periods` ES index migrates to an ArangoDB collection.
- Territory datasets become ArangoDB collections.
- Spatial filtering uses ArangoDB geo-spatial index capabilities.
- Type consanguinity computation becomes native AQL graph traversal.
- The tab-based temporal authority model carries over unchanged.
- The filter state model and panel layout carry over unchanged.

