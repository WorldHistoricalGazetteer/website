# WHG Explorer Redesign — Specification

## Elevator Pitch

The World Historical Gazetteer transforms from **a search engine for historical place names** into **an interactive atlas of the world through time** — a spatio-temporal explorer where the map is the primary interface, and toponymic search is a secondary action enriched by the exploration context the user has built up. For DH researchers, WHG becomes the scholarly equivalent of Google Earth for history: you explore the governance, geography, and cultural territories of the world across millennia first, and search for specific places when you're ready. This repositions WHG as a mature, historically-grounded educational resource rather than a simple directory of toponyms.

---

## Architecture Overview

### Current State

The search page (`/search/`) is structured as:
- A **search bar** at top (`#search_bar`) — toponym input + filter toggle
- A **collapsible three-column filters panel** (`#search_filters`) containing:
  - Column 1: Data sources (authority checkboxes) + clustering toggle
  - Column 2: Time & Space — three **mutually exclusive tabs** (Region | Period | Territory), each with its own spatial selector; the temporal scrubber (Dateline) lives inside Tab 1 only
  - Column 3: Place types (AAT type tree)
- A **landing block** (`#landing`) shown before search — logo, description text
- A **content area** (`#content_area`) — results map + results list, shown after search

Two MapLibre instances exist:
1. `contextMap` — small globe in the filters panel, for spatial constraint selection
2. `resultsMap` — large map in the content area, for displaying search results

Filter state is managed by a singleton `FilterState` class (`filterState.js`) with a `mode` field (`'timespan' | 'period' | 'polity'`) that enforces mutual exclusion between the three tabs. Switching tabs clears temporal state.

### Problems with Current State

1. **Temporal scrubber is tab-locked.** The Dateline widget is inside the Region tab only. Switching to Period or Territory clears it. Users cannot apply temporal constraints when browsing periods or territories — the opposite of what's needed.
2. **Tabs enforce false mutual exclusion.** A user cannot select a PeriodO period AND an admin boundary simultaneously. The `filterState.mode` concept forces a single active spatial authority.
3. **Explorer potential is buried.** The boundary tiles, temporal slider, and admin hierarchy already form a powerful atlas, but they're hidden inside a collapsible panel subordinate to a search bar.
4. **Filter panels clutter the explorer.** Data source checkboxes and AAT type trees are search-time concerns that have no role during spatial exploration.
5. **Two maps are redundant.** The context map (globe) and results map (flat) serve different modes but could be unified.

---

## Envisioned Layout: Hero Map with Overlaid Controls

### Landing State (Explorer Mode)

The page loads with the **map filling the viewport** (the "hero" layout — a web design pattern where a single large visual element dominates the page edge-to-edge, with controls overlaid rather than sitting in separate columns). No search bar dominates; the map *is* the page. A slowly spinning globe invites interaction.

```
┌──────────────────────────────────────────────────────────────────────┐
│  WHG logo (small, top-left)              [Search…] (floating input) │
│                                                                      │
│                         ┌──────────────────┐                         │
│                         │                  │                         │
│                         │   HERO MAP       │                         │
│                         │   (globe/flat)   │                         │
│                         │                  │                         │
│                         │                  │                         │
│                         └──────────────────┘                         │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────────┐ │
│  │ [Off|Range|Range+undated]  ◄══════════════════════════════►    │ │
│  │ Temporal scrubber — always visible, overlaid on bottom of map  │ │
│  └─────────────────────────────────────────────────────────────────┘ │
│                                                                      │
│  ┌──────────────┐  ┌──────────────────────────┐                      │
│  │ Layer Sources │  │ Admin tier (zoom-adaptive │                     │
│  │ [expandable]  │  │ with manual override)     │                     │
│  └──────────────┘  └──────────────────────────┘                      │
└──────────────────────────────────────────────────────────────────────┘
```

Key elements overlaid on the map:

1. **Floating search input** (top-right or top-centre) — a compact text field that serves dual purposes (see below). Includes a mode indicator.
2. **Temporal scrubber** (bottom edge) — the Dateline widget, always visible, always active. The Off/Range/Range+undated mode toggle sits beside it. This constrains everything: boundaries shown on the map, period searches, territory searches, and toponymic searches.
3. **Layer Sources palette** (bottom-left, expandable) — replaces both the Modern/Historical namespace toggle AND the authority checkboxes. In Explorer mode, this controls which boundary/territory layers are visible on the map: `OSM (modern) ✓ | OHM (historical) ✓ | PeriodO ☐ | Cliopatria ☐ | D-PLACE ☐ | NativeLand ☐ | ...`. Each is a toggle.
4. **Admin tier control** (bottom, near scrubber) — the existing tier buttons (Continental → Ward), but defaulting to zoom-adaptive behaviour. A small override lets the user pin a specific level.

### Dual-Mode Search Input

The floating search input has **two modes**, switchable via a toggle or segmented control immediately beside it:

| Mode | Label | What it searches | Behaviour |
|------|-------|------------------|-----------|
| **Areas** | 🔍 Search for areas | Boundary/region/territory names from the active layer sources | Queries the `/search/boundaries/` endpoint (and future period/territory endpoints) filtered by the active layer sources, temporal range, and visible admin level. Selecting a result highlights the polygon on the map and optionally zooms to it. Multiple selections accumulate as chips. |
| **Toponyms** | 🔍 Search selected areas for toponyms | Place name records from the `places` ES index | Standard toponym search, but automatically constrained to any spatial selections (boundary polygons) and temporal range established in Explorer mode. Opens the results panel (map + list) and makes the Data Sources modal and Categories modal accessible. |

The mode toggle sits directly on the input (e.g., a segmented `[Areas | Toponyms]` pill to the left of the text field, or an icon toggle). **Areas** is the default on landing.

**Critically:** the temporal scrubber constrains both modes. Selecting "Historical" + OHM in the Layer Sources, scrubbing to 1400–1600, and typing "Ottoman" in Areas mode shows OHM boundary polygons named "Ottoman" from that era. Switching to Toponyms mode and typing "Constantinople" then searches for that toponym within the selected Ottoman boundary and the 1400–1600 temporal range.

### Transition to Search Mode (Toponyms)

When the user switches the input to Toponyms mode, or types into it with Toponyms mode active:

1. The current Explorer state is **preserved as search context** — selected boundary polygons remain as semi-transparent overlays, temporal range persists.
2. The map transitions from full-bleed to a **split view**: map (left) + results list (right), or the results list slides up as a bottom sheet on mobile.
3. Two **modal/off-canvas panels** become accessible via buttons near the search input:
   - **Data Sources** — the authority checkboxes (GeoNames, TGN, Wikidata, etc.) + clustering toggle. Currently the `.filter-col--authorities` column.
   - **Categories** — the AAT type tree. Currently the `.filter-col--types` column.
4. Active spatial constraints from Explorer mode appear as **summary chips** below the search input (e.g., "Ottoman Empire (OHM, level 2)" ✕ | "1400–1600" ✕).

### Returning to Explorer Mode

Clearing search results or switching the input back to Areas mode returns to the hero map layout. The map expands back to full viewport. Spatial selections persist.

---

## File-by-File Implementation Plan

### 1. Template: `search/templates/search/search.html`

**Current:** Bootstrap container layout with `#search_bar`, collapsible `#search_filters` (3-column grid), `#landing`, `#content_area` (map + results).

**Changes:**
- Remove the `<main class="container container-md">` wrapper. The map must be full-bleed (edge-to-edge), not constrained to a Bootstrap container.
- Remove the three-column `.filters-grid` entirely. Remove `#search_filters` as a collapsible panel.
- Remove the `#landing` block (logo + description text). The hero map replaces it.
- Replace with a structure like:

```html
<div id="explorer" class="explorer-layout">
  <!-- Hero map: full viewport -->
  <div id="hero_map"></div>

  <!-- Floating search input + mode toggle -->
  <div id="floating_search">
    <div class="search-mode-toggle">[Areas | Toponyms]</div>
    <input id="search_input" type="text" placeholder="Search for areas…" />
    <button id="initiate_search">…</button>
    <button id="clear_search">…</button>
    <button id="exact_match_toggle">…</button>
    <!-- Toponym-mode-only buttons -->
    <button id="open_sources_modal" class="toponym-mode-only">Data Sources</button>
    <button id="open_categories_modal" class="toponym-mode-only">Categories</button>
  </div>

  <!-- Spatial selection chips -->
  <div id="selection_chips"></div>

  <!-- Temporal scrubber overlay (always visible) -->
  <div id="temporal_overlay">
    <div class="temporal-mode-toggle">[Off | Range | Range+undated]</div>
    <div id="dateline"></div>
  </div>

  <!-- Layer Sources expandable palette -->
  <div id="layer_sources_palette">…</div>

  <!-- Admin tier control -->
  <div id="admin_tier_control">…</div>

  <!-- Results panel (hidden in Explorer mode, shown in Toponym mode) -->
  <div id="results_panel" class="results-hidden">
    <div id="result_container">
      <div id="search_results"></div>
    </div>
  </div>

  <!-- Data Sources modal (Bootstrap offcanvas or modal) -->
  <div id="sources_modal" class="offcanvas offcanvas-start">…checkboxes…</div>

  <!-- Categories modal (Bootstrap offcanvas or modal) -->
  <div id="categories_modal" class="offcanvas offcanvas-end">…AAT tree…</div>
</div>
```

- The `{% block header %}{% endblock %}` (currently empty) should remain empty — no site navbar overlapping the hero map, or the navbar should overlay transparently.

### 2. CSS: `whg/webpack/css/search.css`

**Current:** 1237 lines. Three-column `.filters-grid`, `#context_map_wrap` with `aspect-ratio: 1`, `#timespaceTab` styles, region-tier-toggle styles, authority column styles, etc.

**Changes:**
- Replace `.filters-grid` and all column styles with a hero-map layout:
  ```css
  .explorer-layout { position: relative; width: 100vw; height: 100vh; }
  #hero_map { position: absolute; inset: 0; }
  ```
- `#floating_search` — positioned `absolute` or `fixed`, top-centre, with `z-index` above the map. Glassmorphism or drop-shadow for readability over the map.
- `#temporal_overlay` — positioned at the bottom edge of the map, full width, semi-transparent background. The Dateline widget already has CSS variables for theming (`--slider-background`, `--range-color`, etc.).
- `#layer_sources_palette` — positioned bottom-left, collapsed by default with an expand button. When expanded, shows a compact grid of toggle switches.
- `#admin_tier_control` — positioned near the temporal overlay or as a compact dropdown.
- `#results_panel` — slides in from the right (or bottom on mobile) when Toponym mode produces results. The map shrinks to accommodate it (CSS transition on `#hero_map` width).
- Remove all `#context_map_wrap`, `.timespace-*`, `#timespaceTab`, `.filter-col--*` styles.
- Retain and adapt: `.temporal-mode-toggle`, `.region-tier-toggle`, `.filter-chip`, `.region-dropdown`, `.region-result` styles.
- Add responsive rules: on mobile, the temporal overlay stacks vertically; the results panel becomes a bottom sheet.

### 3. JavaScript: `whg/webpack/js/search.js` (949 lines → major rewrite)

**Current:** Initialises two MapLibre maps (`contextMap` and `resultsMap`), three selector widgets (`RegionSelector`, `PeriodSelector`, `PolitySelector`), `Dateline`, `TypeTreeWidget`, authority checkboxes, tab switching logic, zoom gate, search execution, result rendering.

**Changes:**

- **Remove dual-map architecture.** Remove the `resultsMap` instance and the `resultsMapParams` configuration (lines 39–48). The hero map handles both exploration and result display.
- **Remove tab switching logic.** Delete the `#timespaceTab` event listener (lines 384–424) and the `previousMode` state. Delete `filterState.clearTabState()` calls.
- **Add search mode state.** New module-level variable: `let searchMode = 'areas';` (values: `'areas'` | `'toponyms'`). Wire the mode toggle to switch this. When switching to `'toponyms'`, show the results panel, enable the Data Sources and Categories modal buttons. When switching to `'areas'`, hide the results panel, close any modals.
- **Dateline initialisation** (lines 273–287): Move out of the `Promise.all` chain. It should initialise immediately and be always visible. Remove the `open: true` option (it's overlaid, not collapsible).
- **Temporal mode toggle** (lines 290–300): Move wiring to operate on the always-visible `#temporal_overlay` element instead of inside a tab pane.
- **RegionSelector init** (line 303): Still instantiated, but mounted differently — its tier buttons go into `#admin_tier_control`, its search input integrates into the floating search input in Areas mode, its chips go into `#selection_chips`.
- **PeriodSelector and PolitySelector** (lines 304–305): These no longer have their own tabs. Instead, they become alternative search backends for the Areas mode input when corresponding layer sources are active. When the user enables "PeriodO" in the Layer Sources palette and types in Areas mode, the search queries the `periodo_periods` index. When "Cliopatria" is enabled, it queries the territories index. The input field's placeholder text updates based on active sources.
- **Authority checkboxes** (lines 352–358): Move wiring to the offcanvas `#sources_modal`. Same logic, different DOM location.
- **Type tree** (lines 173–193): Move wiring to the offcanvas `#categories_modal`.
- **`gatherOptions()`** (lines 912–948): Update to always include temporal state (regardless of former "mode"), and to send spatial constraints from whatever selections exist (regions, periods, territories — no longer mutually exclusive).
- **`renderResults()`** (lines 647–851): Update to render into the `#results_panel` instead of `#content_area`. Use the hero map's source/layers instead of `resultsMap`.
- **`clearResults()`** (lines 588–645): Update to return to Explorer mode (hero map full-bleed, clear result layers).
- **Globe spin logic** (lines 537–551): Start spin on page load (not on filter panel open). Stop on first user map interaction.
- **Zoom gate** (lines 427–479): Still needed — admin tier buttons should be gated behind zoom. But since the hero map is always visible (not hidden in a collapsible panel), the gate triggers naturally as the user zooms in.

### 4. JavaScript: `whg/webpack/js/filterState.js` (234 lines)

**Current:** `DEFAULT_STATE` has `mode: 'timespan'` field and `clearTabState()` method enforcing mutual exclusion.

**Changes:**
- **Remove `mode` field.** The concepts of `'timespan'`, `'period'`, and `'polity'` as exclusive modes disappear. Replace with:
  ```javascript
  search_mode: 'areas',  // 'areas' | 'toponyms'
  ```
- **Remove `clearTabState()` method.** No tabs, no tab switching.
- **Spatial state becomes additive:**
  ```javascript
  spatial: {
      bbox: null,
      selections: [],  // Array of {id, label, type, source, geometry, admin_level?, namespace?}
                        // type: 'boundary' | 'period' | 'territory'
                        // source: 'osm' | 'ohm' | 'periodo' | 'cliopatria' | 'dplace' | 'nativeland'
      geometry_source: 'none',
      preview_geo: null,
  },
  ```
  This replaces the separate `region_id`, `period_id`, `polity_id` arrays. Selections from any source coexist.
- **`toSearchPayload()`** (lines 174–210): Rebuild to iterate `spatial.selections` and produce geometry refs grouped by source type, rather than switching on `mode`.
- **Temporal state** is unchanged — it already works independently of mode. Just remove the `source: 'manual' | 'period' | 'polity'` field since the temporal scrubber is always user-controlled (periods and territories may *suggest* a temporal range, but the scrubber is the authority).

### 5. JavaScript: `whg/webpack/js/contextMap.js` (759 lines → becomes hero map)

**Current:** Wraps a single MapLibre instance in `#context_map`, manages boundary layers, overlay source, suggestion markers, viewport tracking.

**Changes:**
- **Rename to `heroMap.js`** (or keep the name but change the semantics). Mount into `#hero_map` instead of `#context_map`.
- **Add result display layers.** Import the source/layer setup currently in the `resultsMap` initialisation (lines 133–134 of `search.js`): `newSource('places')` + `newLayerset('places', null, 'plain')`. These layers are hidden in Explorer mode, shown in Toponym mode.
- **Add layer source management.** New methods:
  - `setActiveSources(sources: string[])` — shows/hides boundary layers based on which sources are toggled in the Layer Sources palette. Currently only `osm` and `ohm` namespaces exist; extend to show/hide PeriodO, Cliopatria, D-PLACE, NativeLand layers when their tile sources are added.
  - `showResultFeatures(geojson)` — sets data on the `places` source (currently done via `resultsMap.getSource('places').setData(...)` in search.js).
  - `clearResultFeatures()` — clears result layers.
- **Boundary filter method** (`showBoundaries(filters)`): Unchanged in logic, but now operates on the hero map.
- **Globe spin:** Start on page load. The existing `_wireSpinStop()` and `spinWasStopped` logic is sufficient.
- **Remove the aspect-ratio constraint.** The map is now full-viewport, not a square in a panel. The `#context_map_wrap { aspect-ratio: 1 }` CSS goes away.

### 6. JavaScript: `whg/webpack/js/regionSelector.js`

**Current:** Renders tier buttons, namespace toggle, search input, dropdown, and chips into `#region_selector_container`.

**Changes:**
- **Split rendering.** Instead of rendering everything into a single container, the component should accept multiple mount points:
  - Tier buttons → `#admin_tier_control` (overlaid on map)
  - Search input + dropdown → integrated into `#floating_search` when Areas mode is active and an admin tier is selected
  - Chips → `#selection_chips` (overlaid on map)
  - Namespace toggle → absorbed into the Layer Sources palette
- **Remove the namespace toggle from this component.** The Modern/Historical concept becomes part of the generalised Layer Sources palette. The `_currentNamespace` state is replaced by observing which sources are active in the palette.
- **Accept external namespace/source state.** Instead of managing `_currentNamespace` internally, read the active sources from `filterState` or accept them as a parameter.
- **Zoom-adaptive tier default.** Add an `autoTier` mode where the active tier follows map zoom level:
  - z < 3: Continental (level 0)
  - z 3–4: Sub-Continental (level 1)
  - z 4–5: Country (level 2)
  - z 5–7: State (level 3)
  - z 7–9: Province (level 4)
  - z 9+: finer levels

  The user can override by clicking a specific tier button, which disables auto-follow until reset.

### 7. JavaScript: `whg/webpack/js/periodSelector.js` (328 lines)

**Current:** Self-contained widget mounted in `#period_selector_container` with its own search input, dropdown, authority sub-filter, and chips.

**Changes:**
- **Remove standalone UI.** This is no longer a separate tab with its own input. Instead, it becomes a **search backend** that the floating search input delegates to when PeriodO is an active layer source and the mode is Areas.
- **Export a search function** instead of rendering a widget:
  ```javascript
  export async function searchPeriods(query, options) → results[]
  ```
- **Selection behaviour:** When a PeriodO period is selected from the dropdown, it adds to `filterState.spatial.selections` with `type: 'period'` and `source: 'periodo'`. Its geometry (if any) is shown on the hero map overlay. Its temporal range is suggested to the scrubber (but the user can adjust).
- **Chips** are rendered in the shared `#selection_chips` area.

### 8. JavaScript: `whg/webpack/js/politySelector.js` (257 lines)

**Current:** Self-contained widget with dataset toggle (Cliopatria | D-PLACE | NativeLand), search input, dropdown, chips.

**Changes:**
- Same refactor as PeriodSelector — becomes a search backend, not a standalone widget.
- **Export a search function:**
  ```javascript
  export async function searchPolities(query, dataset, options) → results[]
  ```
- The dataset toggle (Cliopatria | D-PLACE | NativeLand) is absorbed into the **Layer Sources palette** — each is a separate toggleable source.
- When the user enables Cliopatria in the palette and types in Areas mode, the search input queries the Cliopatria territory index.
- Selections add to `filterState.spatial.selections` with `type: 'territory'` and `source: 'cliopatria' | 'dplace' | 'nativeland'`.

### 9. New JavaScript: `whg/webpack/js/layerSourcesPalette.js` (new file)

A new component that manages the expandable Layer Sources control overlaid on the map.

**Responsibilities:**
- Renders toggle switches for each available spatial data source: OSM (modern boundaries), OHM (historical boundaries), PeriodO (period extents), Cliopatria (historical polities), D-PLACE (cultural regions), NativeLand (indigenous territories).
- Persists active sources in `filterState` (new key: `active_sources: ['osm', 'ohm']` default).
- Calls `heroMap.setActiveSources(sources)` when toggles change.
- Informs the search input routing logic which backends to query in Areas mode.

### 10. New JavaScript: `whg/webpack/js/areaSearchRouter.js` (new file)

A new module that routes Areas-mode search queries to the correct backend(s) based on active layer sources.

**Logic:**
```
if OSM or OHM active → query /search/boundaries/ (existing endpoint) filtered by namespace
if PeriodO active    → query periodo_periods ES index (existing periodSelector search logic)
if Cliopatria active → query territories ES index with dataset=cliopatria
if D-PLACE active    → query territories ES index with dataset=dplace
if NativeLand active → query territories ES index with dataset=nativeland
```

Results from all active sources are merged and presented in a unified dropdown, grouped by source. Each result shows its source as a badge.

### 11. JavaScript: `whg/webpack/js/typeTreeWidget.js` (unchanged internally)

No changes to the widget itself. It is mounted into the `#categories_modal` offcanvas instead of the `.filter-col--types` column. Its `onchange` callback still updates `filterState.place_types`.

### 12. JavaScript: `whg/webpack/js/dateline.js` (unchanged internally)

No changes to the widget itself. It is mounted into `#temporal_overlay` instead of `#dateline_container` inside a tab pane.

### 13. Backend: `search/views.py` — `SearchPageView`

**Current:** Renders `search/search.html` with context data (`dropdown_data`, `index_places`, `index_toponyms`, `es_whg`, user areas).

**Changes:**
- `dropdown_data` — still needed for result rendering (country code lookups). Keep.
- `index_places`, `index_toponyms` — currently used in the `#landing` description text. Still useful for the Data Sources modal or an "about this index" info panel. Keep.
- Consider adding context for available layer sources (list of active boundary tile URLs, dataset metadata for Cliopatria/D-PLACE/NativeLand) so the frontend doesn't hardcode them.

### 14. Backend: `search/views_crc.py` — `SearchView`

**Current:** Receives POST from `gatherOptions()`, forwards to CRC gateway.

**Changes:**
- Accept the new payload format from the updated `gatherOptions()`: spatial constraints as a list of selections (not mode-dependent), temporal always included.
- The CRC gateway query builder needs to handle multiple simultaneous spatial geometries (union them for containment filtering).

### 15. Backend: `search/views.py` — `BoundarySearchView`

**Current:** Searches the ES `boundaries` index by name, admin_level, namespace.

**Changes:**
- Extend to accept an optional `temporal_start` and `temporal_end` parameter, filtering boundaries to those whose `start_date`/`end_date` temporal properties overlap the requested range. This is essential for the OHM historical boundaries use case (e.g., searching for "Ottoman" in 1400–1600 should not return the modern Republic of Turkey).

### 16. URL routing: `whg/urls.py`

**Changes:**
- Consider making `/search/` the home page (replace the current `Home30a` view at `/`). The Explorer IS the landing experience. The old home page content (carousel of featured datasets, announcements) could move to a `/about/` or `/discover/` page, or be integrated as an overlay/panel on the Explorer.
- Alternatively, keep `/` as a lightweight landing page that has a prominent "Explore" CTA linking to `/search/`.

### 17. Webpack config: `webpack.config.js`

**Changes:**
- No new entry points needed — `search` entry point already covers all the JS.
- New files (`layerSourcesPalette.js`, `areaSearchRouter.js`) are imported by `search.js` and bundled automatically.
- If `heroMap.js` replaces `contextMap.js`, update the import in `search.js`.

---

## FilterState Schema (New)

```javascript
const DEFAULT_STATE = {
    // Search mode
    search_mode: 'areas',  // 'areas' | 'toponyms'

    // Active layer sources (for Explorer)
    active_sources: ['osm', 'ohm'],  // from: osm, ohm, periodo, cliopatria, dplace, nativeland

    // Toponym search authorities (for Search mode)
    authorities: ['gn', 'iv', 'ohm', 'pl', 'tgn', 'tm', 'wd', 'whg'],
    place_types: [],          // AAT identifiers from type tree
    clustering: true,

    // Spatial selections (additive, from any source)
    spatial: {
        bbox: null,
        selections: [],       // [{id, label, type, source, geometry, admin_level?, namespace?}]
        geometry_source: 'none',
        preview_geo: null,
    },

    // Temporal (always active, shared across all modes)
    temporal: {
        start_year: -2000,
        stop_year: 2100,
        mode: 'off',          // 'off' | 'range' | 'undated'
    },

    dirty: false,
};
```

---

## Interaction Flow Summary

```
1. PAGE LOAD
   → Hero map fills viewport, globe spinning
   → Temporal scrubber visible at bottom (mode: Off)
   → Layer Sources palette (collapsed): OSM ✓, OHM ✓
   → Floating input: [Areas ● | Toponyms ○] "Search for areas…"
   → Admin tier: zoom-adaptive (auto)

2. USER ZOOMS INTO EUROPE
   → Map shows country-level boundaries (auto-tier: Country)
   → User clicks France → chip appears: "France (OSM, level 2) ✕"

3. USER ENABLES TEMPORAL RANGE
   → Clicks "Range" on temporal toggle
   → Drags scrubber to 1200–1500
   → (If OHM boundaries have temporal properties, non-matching boundaries fade)

4. USER SWITCHES TO TOPONYM SEARCH
   → Clicks [Toponyms] on the mode toggle
   → Input placeholder: "Search within France, 1200–1500…"
   → Results panel slides in from right (map shrinks to 60% width)
   → [Data Sources] and [Categories] buttons appear
   → User types "Avignon" → results appear, points plotted on map

5. USER OPENS CATEGORIES MODAL
   → Off-canvas slides in with AAT type tree
   → Selects "religious centers" → re-search with type filter

6. USER CLEARS SEARCH
   → Results panel slides away, map returns to full-bleed
   → France selection chip and temporal range persist
   → Back to Explorer mode, Areas input active
```

---

## Vector Tile Generation: New Spatial Sources

The existing boundary tiles are served from a single `.mbtiles` file containing OSM and OHM admin boundaries, with a single source-layer named `boundaries`. Each feature carries a packed integer `id` encoding namespace + relation_id (see `utils/boundary_id.py` and `whg/webpack/js/boundaryId.js`). The 4-bit namespace code currently uses 3 of 16 available slots (`osm=1, ohm=2, m49=3`), leaving room for 13 more.

New `.mbtiles` need to be generated on CRC and pushed to the tileserver for PeriodO, Cliopatria, D-PLACE, and NativeLand.

### Recommendation: Separate `.mbtiles` Files, One per Source

**Use separate `.mbtiles` files rather than a single combined file.** Reasons:

1. **Independent update cycles.** Each dataset has its own release cadence. Cliopatria may update quarterly; NativeLand is community-driven and irregular; PeriodO publishes new authorities over time. Separate files mean regenerating only the one that changed, not rebuilding everything.

2. **Clean layer toggle semantics.** The Layer Sources palette toggles each source independently. With separate tile sources in the MapLibre style, toggling a source is a simple `setLayoutProperty(layerId, 'visibility', ...)` on its layers — no filter expressions needed to show/hide subsets within a shared source. This is cheaper and avoids edge cases where filters interact with the packed-ID feature-state mechanism.

3. **Distinct source-layer schemas.** The datasets have meaningfully different property schemas:
   - **OSM/OHM boundaries:** `name`, `admin_level` (integer 0–10), `namespace`, `start_date`/`end_date` (OHM only)
   - **PeriodO:** `label`, `spatial_description`, `authority`, `start_year`, `stop_year` — no `admin_level`
   - **Cliopatria/D-PLACE/NativeLand:** `name`, `dataset`, `start_year`, `end_year`, polity-specific metadata — no `admin_level`

   A single source-layer would force a union schema with many null fields, complicating both the tile generation and the client-side filter/paint expressions.

4. **Feature-state isolation.** Each MapLibre source maintains its own feature-state namespace. Separate sources mean hover/selection state for boundary polygons can't accidentally collide with hover/selection state for period extents.

5. **File size control.** The OSM/OHM boundary tileset is already substantial (~hundreds of MB). Combining all sources would produce a monolith that's slow to transfer and hard to debug. Separate files keep each manageable.

### Tile File Layout

| Source | `.mbtiles` file | Source-layer name | Namespace code | Feature ID scheme |
|--------|-----------------|-------------------|----------------|-------------------|
| OSM + OHM admin boundaries | `boundaries.mbtiles` (existing) | `boundaries` | `osm=1, ohm=2, m49=3` | Packed: `namespace(4 bits) + relation_id(49 bits)` |
| PeriodO period extents | `periodo.mbtiles` (new) | `periods` | `periodo=4` | Packed: `4 << 49 \| periodo_internal_id` |
| Cliopatria polities | `cliopatria.mbtiles` (new) | `polities` | `cliopatria=5` | Packed: `5 << 49 \| polity_id` |
| D-PLACE regions | `dplace.mbtiles` (new) | `regions` | `dplace=6` | Packed: `6 << 49 \| region_id` |
| NativeLand territories | `nativeland.mbtiles` (new) | `territories` | `nativeland=7` | Packed: `7 << 49 \| territory_id` |

### Required Code Changes for Tile Support

1. **`utils/boundary_id.py`** — extend `NAMESPACE_CODES`:
   ```python
   NAMESPACE_CODES = {
       "osm": 1, "ohm": 2, "m49": 3,
       "periodo": 4, "cliopatria": 5, "dplace": 6, "nativeland": 7,
   }
   ```

2. **`whg/webpack/js/boundaryId.js`** — mirror the new codes:
   ```javascript
   const NAMESPACE_CODES = {
       osm: 1, ohm: 2, m49: 3,
       periodo: 4, cliopatria: 5, dplace: 6, nativeland: 7,
   };
   ```

3. **Tileserver style configuration** — add new sources to the `whg-context` style JSON, each pointing to its `.mbtiles` URL. Add corresponding fill/line layers (initially hidden, toggled by the Layer Sources palette).

4. **`heroMap.js` (née `contextMap.js`)** — the new `setActiveSources(sources)` method shows/hides layer groups by source. Each source's layers are discovered at load time the same way `_initBoundaryLayers()` currently discovers the OSM/OHM layers.

5. **CRC tile generation scripts** — new scripts (or extensions to existing ones) to extract GeoJSON from the PeriodO, Cliopatria, D-PLACE, and NativeLand ES indexes, assign packed feature IDs using the extended `encode_feature_id()`, and run `tippecanoe` to produce `.mbtiles`. Generating and pushing these to the tileserver is straightforward and follows the same pattern as the existing boundary tile pipeline.

### Feature Properties per Source

Each new tileset should include at minimum:

- **PeriodO:** `label`, `authority_label`, `spatial_description`, `start_year`, `stop_year`, `periodo_id`
- **Cliopatria:** `name`, `start_year`, `end_year`, `polity_id`, `type` (empire/kingdom/etc)
- **D-PLACE:** `name`, `language_family`, `region_id`
- **NativeLand:** `name`, `category` (territory/language/treaty), `territory_id`

All should include `start_year` and `end_year` (or `start_date`/`end_date`) to enable temporal filtering on the map — the Dateline scrubber should be able to fade or hide features outside the active range, using data-driven paint expressions like `['interpolate', ..., ['get', 'start_year'], ...]`.

---

## AAT Styles and Periods Facet: Type Tree Augmentation

### Background

The current type tree is built from three AAT entry points (see `placetypes/aat_config.py`):

| AAT ID | Label | Facet |
|--------|-------|-------|
| `300264550` | Built Environment | Objects Facet |
| `300182722` | geographic regions | Associated Concepts Facet |
| `300232420` | sovereign states | Agents Facet |

These cover *what a place is* (settlement, temple, river) but not *when a place belongs to* in cultural-historical terms. AAT's **Styles and Periods Facet** (`aat:300264088`) contains ~14,500 concepts for historical periods, cultural movements, and chronological designations — e.g., "Iron Age" (`300019279`), "Medieval" (`300020756`), "Ottoman" (`300021625`), "Edo (Japanese period)" (`300018476`).

Adding this facet to the type tree would allow users to filter toponymic search results by cultural-historical period using the same AAT mechanism that currently filters by place type — but with period concepts rather than feature-type concepts.

### What Needs to Happen

1. **Add entry point to `placetypes/aat_config.py`:**
   ```python
   AAT_ENTRY_POINTS = [
       300264550,   # Built Environment (hierarchy name)
       300182722,   # geographic regions (Associated Concepts Facet)
       300232420,   # sovereign states (Agents Facet)
       300264088,   # Styles and Periods Facet (NEW)
   ]
   ```
   Then re-run `python manage.py sync_aat_types` to walk the Styles and Periods hierarchy and populate the `Type` table with ~14,500 new concepts.

2. **Assign a new fclass code (or a separate flag) for period concepts.** The current `AAT_FCLASS_MAP` assigns GeoNames-style single-letter codes (`A`, `P`, `S`, `H`, `L`, `T`, `R`, `U`) to spatial feature types. Period concepts are categorically different — they're temporal, not spatial. Options:
   - Add a new fclass letter (e.g., `D` for "Date/period") and add `300264088: 'D'` to `AAT_FCLASS_MAP`
   - Add a boolean field `is_period_type` to the `Type` model (paralleling `is_place_type`)
   - Use a separate category field altogether

   **Recommendation:** add `is_period_type = BooleanField(default=False)` to the `Type` model. This keeps it clean — period concepts are not feature classes, and overloading fclass would confuse the existing GeoNames-aligned mapping system.

3. **Extend the type tree widget to show a separate "Periods" section.** In the Categories modal (Toponyms mode), the tree would have two top-level sections:
   - **Place Types** (existing) — Built Environment, geographic regions, sovereign states
   - **Historical Periods** (new) — Styles and Periods Facet

   Selected period AAT identifiers would be sent alongside place-type identifiers but handled separately in the search query (matching against temporal attestation metadata rather than type fields).

4. **Exclude subtrees that aren't historical periods.** The Styles and Periods Facet includes art styles ("Impressionist"), design movements ("Art Deco"), and other non-temporal concepts. We'd need exclusion rules similar to `AAT_EXCLUDED_SUBTREES` — e.g., keep "periods by geography" and "periods by general era" but exclude "styles by visual quality" and "design movement names". This requires manual review of the AAT hierarchy under `300264088`.

### PeriodO Linkage: Investigation Required

PeriodO is the primary source for period definitions with spatial and temporal extents. Each PeriodO period has:
- A label (e.g., "Iron Age")
- An authority (who defined it)
- A spatial description (where it applies)
- A start/end year range (when it covers)
- Sometimes a URI to an external authority — **including AAT identifiers**

The critical question is: **how reliably do PeriodO periods link to AAT Styles and Periods concepts?**

Investigation points:

1. **Coverage:** What fraction of PeriodO periods carry an AAT URI? Is the linkage systematic or sporadic? If most PeriodO periods lack AAT links, the tree filter won't be useful for narrowing period-based spatial searches.

2. **Granularity mismatch:** AAT periods tend to be broad and culturally defined ("Iron Age", "Medieval"), while PeriodO periods can be extremely specific and geographically scoped ("Early Iron Age in the Southern Levant, per Smith 2003"). A single AAT concept might map to dozens of PeriodO periods across different authorities and geographies. This is potentially *useful* (select "Iron Age" in the AAT tree → find all PeriodO "Iron Age" variants) but needs verification that the linkage actually works this way.

3. **Label alignment:** Even without formal URI links, do PeriodO period labels reliably match AAT preferred terms? Could we build fuzzy linkage (matching "Iron Age" labels across both vocabularies) as a fallback?

4. **Bidirectional utility:** In Explorer mode (Areas search), selecting an AAT period concept could highlight all PeriodO period extents that link to it. In Toponyms mode, selecting an AAT period could constrain the temporal range of the search. These are different use cases that depend on different linkage properties.

5. **Authority-level vs. period-level linkage:** PeriodO organises periods under authorities. Some authorities may systematically use AAT identifiers while others don't. We may need to investigate per-authority.

### Action Items

- [ ] Query the PeriodO dataset (via their SPARQL endpoint or JSON-LD dump) to count how many periods carry `skos:closeMatch` or `skos:exactMatch` links to AAT URIs
- [ ] Assess label overlap: extract all AAT Styles and Periods preferred terms and all PeriodO period labels, measure intersection
- [ ] Review the AAT subtree under `300264088` to identify which children are genuinely "historical periods" vs. art/design styles, and draft exclusion rules
- [ ] Decide whether period-type filtering belongs in the Categories modal (Toponyms mode) only, or also has a role in Explorer mode (e.g., "show me all PeriodO extents tagged as Iron Age")

---

## Migration Notes

- This is a **major frontend rewrite** affecting `search.html`, `search.css`, `search.js`, `filterState.js`, `contextMap.js`, `regionSelector.js`, `periodSelector.js`, `politySelector.js`.
- Backend changes are minimal: extend `BoundarySearchView` with temporal filtering; update `SearchView` payload handling; optionally change URL routing.
- The `TypeTreeWidget`, `Dateline`, `CountryParents`, and `whg_maplibre` modules are **unchanged internally** — only their mount points and integration wiring change.
- The `#search_filters` collapsible panel, `#timespaceTab` tabs, and three-column `.filters-grid` are **entirely removed**.
- Existing permalink/SEO URLs (`/search/<toponym>`) continue to work — they would pre-populate the toponym input and auto-switch to Toponyms mode.



