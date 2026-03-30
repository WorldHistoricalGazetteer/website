# WHG Search System — Architecture Reference

> **Purpose:** This document describes how the current WHG search page (`/search/`) works end-to-end — from the browser UI through JavaScript payload construction, Django view handling, Elasticsearch query building, result rendering, and the PeriodO (chrononym) integration. It is intended as a briefing for designing a replacement that leverages the new CRC Elasticsearch instance (with `places`, `toponyms`, and `clusters` indices) and a gateway API.

---

## 1. Page load & context

| URL | Django view | Template |
|---|---|---|
| `GET /search/` | `SearchPageView` (TemplateView) | `search/templates/search/search.html` |
| `GET /search/<toponym>` | same, with toponym kwarg | same |

The view's `get_context_data()` provides:

| Context variable | Source | Purpose |
|---|---|---|
| `es_whg` | `settings.ES_WHG` (e.g. `whg3dev`) | Index name passed to JS |
| `adv_filters` | Hard-coded list of 7 feature-class tuples | Renders checkboxes |
| `dropdown_data` | `get_regions_countries()` | Populates the spatial-filter Select2 (regions + countries) |
| `has_areas` / `user_areas` | User's saved `Area` objects (types `ccodes`, `copied`, `drawn`) | Enables the "Custom" spatial filter option |
| `search_params` | `request.session['search_params']` | Not actively used by the template |
| `toponym` | URL kwarg (optional) | Pre-fills Schema.org metadata for SEO |

Template injects these into the global JS scope:

```html
<script>
  const dropdown_data = {{ dropdown_data|safe }};
  var eswhg = "{{ es_whg|escapejs }}";
  const has_areas = {{ has_areas|yesno:"true,false" }};
  const user_areas = {{ user_areas|safe }};
  const adv_filters = {{ adv_filters|safe }};
</script>
```

Scripts loaded (deferred, in order):
1. `whg_maplibre.bundle.js` — MapLibre GL wrapper
2. `search.bundle.js` (ES module) — main search logic

---

## 2. UI inputs & filters

### 2.1 Text input

- **Element:** `#search_input` — free-text place name.
- **Typeahead:** On each keystroke (debounced), `GET /search/suggestions/?q=…` returns up to 20 unique titles from ES using `SearchViewV3.build_search_query()` with just the `qstr` param.
- **Submit:** Pressing Enter or clicking `#initiate_search` calls `initiateSearch()`.

### 2.2 Feature-class checkboxes

- **Container:** `#adv_checkboxes`
- **Values:** `A` (Administrative), `P` (Cities/towns), `S` (Sites/buildings), `R` (Roads/routes), `L` (Regions/landscape), `T` (Terrestrial landforms), `H` (Water bodies). All checked by default.
- **Behaviour:** Unchecking all returns zero results (empty `fclasses` string triggers early exit in the backend).

### 2.3 Temporal control (Dateline)

A custom slider widget (`whg/webpack/js/dateline.js`) rendered as a MapLibre GL control.

| Property | Default | Description |
|---|---|---|
| `fromValue` | 800 | Start of selected range |
| `toValue` | 1800 | End of selected range |
| `minValue` | -2000 | Slider minimum |
| `maxValue` | 2100 | Slider maximum |
| `open` | `false` | Whether temporal filtering is active |
| `includeUndated` | `true` | Whether to include records with no timespans |

- **When closed** (`open: false`): temporal params are excluded from the search; `temporal: false`, `start: ''`, `end: ''`.
- **When open** (`open: true`): `temporal: true`, `start` and `end` are integer years from the slider.
- **`onChange`:** calls `initiateSearch()` (throttled to 300ms) on every slider drag.

### 2.4 Period filter (PeriodO / chrononym)

- **Element:** `#chrononym_input` — typeahead for period names.
- **Suggestion source:** `GET /suggest/entity?limit=60&type=period&mode=nosort&prefix=…`  
  This hits `api/reconcile.py :: SuggestEntityView`, which queries the `Chrononym` Django model using trigram similarity. Returns `{ result: [{ id, name, description }, …] }`.
- **On selection:**
  1. JS fetches `GET /entity/<period_id>/api` → `api/views_entity.py :: EntityFeatureView` → returns an LPF GeoJSON Feature with `when.timespans` and `geometry`.
  2. `deriveOuterBounds(period)` extracts the min start / max end years from `period.when.timespans[].start.in` and `period.when.timespans[].end.in`.
  3. `dateline.reconfigure(outerStart, outerEnd, outerStart, outerEnd, true)` — sets the slider range to the period bounds **and opens the temporal control** (`open = true`).
  4. `draw.deleteAll()` then `draw.add(period.geometry)` — adds the period's spatial geometry (typically multi-polygon covering the region where the period applies) to the MapLibre Draw layer.
  5. Both changes (temporal open + draw geometry) feed into the next `gatherOptions()` → `initiateSearch()` call automatically.

**Assessment:** The period filter IS properly wired — it feeds both temporal range and spatial bounds into the standard search pipeline. However, it is worth noting that the PeriodO geometry ends up in the `bounds` field (the Draw layer's GeometryCollection), not in a separate field. The period's temporal bounds become the slider range.

### 2.5 Spatial filter

Three-part UI in `#spatial_selector`:

| Element | Role |
|---|---|
| `#categorySelector` | Dropdown: `None`, `Country`, `Custom` (user areas, if any) |
| `#entrySelector` | Select2 multi-select, populated dynamically based on category |
| `#clearButton` | Resets spatial filter |

Data flow depending on category:

| Category | `#entrySelector` populated with | Payload fields set |
|---|---|---|
| None | — | `countries: []`, `regions: []`, `userareas: []` |
| Country | Countries from `dropdown_data` (id = ISO2 code, text = name) | `countries: [selected codes]` |
| Custom | User's saved Area objects | `userareas: [area IDs]` |

When countries or regions are selected, their geometries are also drawn on the map via the `countryCache` GeoJSON system.

> **Note:** The `regions` option is commented out in the template (`<!-- <option value="regions">Region</option> -->`). When it was active, selecting a region would expand to its constituent country codes. The `regions` param is gathered in JS but **never processed** by the backend — only `countries` (the derived ccodes) are used in the ES query.

### 2.6 Drawing control

MapLibre GL Draw allows freehand polygon drawing on the map. Drawn geometries are included in the `bounds` GeometryCollection and used as `geo_shape` intersection filters.

### 2.7 Result-facet filters (post-search, client-side)

After results are returned, two accordion sections appear:

- **Place Types** (`#type_checkboxes`) — dynamically built from result types.
- **Countries** (`#country_checkboxes`) — dynamically built from result ccodes.

These filter the already-returned results **client-side only** — no new ES query is made. Toggling checkboxes shows/hides result cards and updates the map source.

---

## 3. Search request

### 3.1 Payload construction (`gatherOptions()`)

```javascript
// whg/webpack/js/search.js, lines 1097–1127
function gatherOptions() {
    return {
        qstr:      $('#search_input').val(),
        idx:       eswhg,                                      // e.g. "whg3dev"
        fclasses:  checkedFclasses.join(','),                   // e.g. "A,P,S,R,L,T,H"
        temporal:  window.dateline.open,                        // boolean
        start:     window.dateline.open ? dateline.fromValue : '',
        end:       window.dateline.open ? dateline.toValue : '',
        undated:   window.dateline.open ? dateline.includeUndated : true,
        bounds:    { type: 'GeometryCollection',
                     geometries: draw.getAll().features.map(f => f.geometry) },
        regions:   [...],    // region IDs (if any)
        countries: [...],    // ISO2 country codes (if any)
        userareas: [...],    // Area model IDs (if any)
        spatial:   $('#categorySelector').val(),                 // "none" | "countries" | "userareas"
    };
}
```

### 3.2 AJAX call (`initiateSearch()`)

```javascript
$.ajax({
    type: 'POST',
    url: '/search/index/',
    data: JSON.stringify(options),
    contentType: 'application/json',
    headers: { 'X-CSRFToken': csrfToken },
    success: function(data) { renderResults(data); }
});
```

### 3.3 Example payload

```json
{
    "qstr": "coventry",
    "idx": "whg3dev",
    "fclasses": "A,P,S,R,L,T,H",
    "temporal": false,
    "start": "",
    "end": "",
    "undated": true,
    "bounds": {
        "type": "GeometryCollection",
        "geometries": []
    },
    "regions": [],
    "countries": [],
    "userareas": [],
    "spatial": null
}
```

---

## 4. Django backend processing

### 4.1 View: `SearchViewV3` (`search/views.py`, line 239)

**URL:** `POST /search/index/`

```
request body (JSON)
       ↓
    json.loads()
       ↓
    merged into request.POST via QueryDict.update()
       ↓
    handle_request() extracts individual params
       ↓
    build_search_query() constructs ES query dict
       ↓
    suggester() executes ES search across [idx, 'pub']
       ↓
    suggestionItem() normalises each hit
       ↓
    JsonResponse({ parameters, suggestions })
```

### 4.2 Parameter extraction (`handle_request()`)

All values are read from `request.POST.get(key)` — after the JSON body is merged in via `QueryDict.update()`.

| Param | Type (as received) | Notes |
|---|---|---|
| `qstr` | string | The search term |
| `idx` | string | ES index name, falls back to `settings.ES_WHG` |
| `fclasses` | string | Comma-separated, e.g. `"A,P,S,R,L,T,H"` |
| `temporal` | mixed | `true`/`false` from JSON, but `QueryDict.update()` may stringify it |
| `start` | string | Integer year as string, or `""` |
| `end` | string | Integer year as string, or `""` |
| `undated` | mixed | Boolean from JSON |
| `bounds` | mixed | Object from JSON (`QueryDict.update()` **flattens lists** — see bug note below) |
| `countries` | mixed | Array of ISO2 codes or empty array |
| `userareas` | mixed | Array of Area IDs or empty array |
| `regions` | mixed | **Not used** in query building |
| `spatial` | string | **Not used** in query building (pass-through) |
| `mode` | string | **Not used** in `SearchViewV3` (present for potential future use) |

> **⚠ Known issue:** `request.POST.update(json_data)` uses Django's `QueryDict.update()`, which stores list values as their **last element** rather than preserving arrays. For example, `countries: ["GB", "FR"]` becomes `countries: "FR"` in `request.POST`. This works for the `terms` ES filter by accident (it accepts both a string and an array), but could silently drop values for multi-valued `userareas`. The `bounds` object survives because `QueryDict.update()` stores the entire dict as a single value.

### 4.3 ES query construction (`build_search_query()`)

```python
STANDARD_FIELDS = [
    "names.toponym.text^3",       # Primary, flexible matching
    "names.toponym^1.5",          # Exact keyword match
    "names.toponym.edge_ngram",   # Substring/suggestion matching
]
```

The constructed query:

```json
{
  "size": 100,
  "query": {
    "bool": {
      "must": [
        {
          "multi_match": {
            "query": "<qstr>",
            "fields": ["names.toponym.text^3", "names.toponym^1.5", "names.toponym.edge_ngram"],
            "fuzziness": "AUTO"
          }
        }
      ],
      "filter": [
        { "exists": { "field": "whg_id" } }
        // + conditional filters below
      ]
    }
  }
}
```

#### Conditional filters appended to `filter[]`:

**1. Feature classes** (if `fclasses` is non-empty):
```json
{ "terms": { "fclasses": ["A", "P", "S", "R", "L", "T", "H", "X"] } }
```
Note: `"X"` (Other/Unknown) is always appended.

**2. Temporal** (if `temporal` is truthy):
```json
{ "range": { "timespans": { "gte": "<start>", "lte": "<end>" } } }
```
If `undated` is also truthy, this is wrapped in a `bool.should` that also matches documents where `timespans` does not exist:
```json
{
  "bool": {
    "should": [
      { "range": { "timespans": { "gte": "<start>", "lte": "<end>" } } },
      { "bool": { "must_not": { "exists": { "field": "timespans" } } } }
    ]
  }
}
```

**3. Country codes** (if `countries` is non-empty):
```json
{ "terms": { "ccodes": ["GB", "FR"] } }
```

**4. Geometry filters** (from `bounds` and/or `userareas`):

Each geometry from `bounds.geometries[]` becomes:
```json
{
  "geo_shape": {
    "geoms.location": {
      "shape": { "type": "Polygon", "coordinates": [...] },
      "relation": "intersects"
    }
  }
}
```

Each `userarea` ID is looked up in the `Area` Django model to get its GeoJSON, then the same `geo_shape` filter is created.

All geometry filters are combined with `bool.should` + `minimum_should_match: 1` (match at least one area).

### 4.4 Query execution (`suggester()`)

```python
def suggester(q, indices):
    # indices = [settings.ES_WHG, 'pub'] → e.g. ['whg3dev', 'pub']
    res = es.search(index='whg3dev,pub', body=q)
    # Each hit → { _id, _index, linkcount, hit: _source, timespans }
    # Sorted by linkcount descending (more linked = more prominent)
```

Both the union index (`whg`/`whg3dev`) and the `pub` index are searched simultaneously. This means results may include both linked (union) and unlinked (pub-only) places.

### 4.5 Result normalisation (`suggestionItem()`)

Each ES hit is transformed into:

```python
{
    "whg_id":    int,          # union index ID
    "pid":       int,          # place_id
    "index":     str,          # which index the hit came from
    "children":  list[int],    # deduplicated child place IDs
    "linkcount": int,          # number of linked records
    "title":     str,          # primary title
    "variants":  list[str],    # from searchy[] minus the title
    "ccodes":    list[str],    # country codes
    "fclasses":  list[str],    # feature classes
    "types":     list[str],    # AAT type labels
    "geom":      list[dict],   # geometry objects with coordinates + pid
    "timespans": list[list],   # [[start, end], ...] pairs
}
```

### 4.6 `linkcount` and the legacy index model

`linkcount` is computed as `len(set(hit._source.children))` — the number of unique child `place_id` values linked under a parent record in the legacy union index (`whg`). In the current system, when multiple dataset records for the same real-world place are reconciled, they are merged into a single parent document with a `whg_id`; the constituent records become `children`. `linkcount` thus represents "how many independent attestations/dataset records agree this is the same place."

Results are **sorted by `linkcount` descending** in Python after retrieval — places with more cross-dataset attestations appear first. This is also used in the UI: results with `linkcount > 1` show "N linked records" with a chain icon; results with `linkcount == 0` (from the `pub` index) show a broken-chain icon indicating they haven't been reconciled yet.

> **⚠ Redundant in the new system:** The CRC Elasticsearch instance uses a `clusters` index instead of the parent-children linking model. In that system, related place records are grouped into clusters rather than merged into parent documents with child arrays. The concept of `linkcount` (counting `children`) does not apply. The new system will need an equivalent prominence/relevance signal — likely the cluster size or an ES `_score`-based ranking — but the mechanism will be different.

---

## 5. Response & rendering

### 5.1 Response shape

```json
{
  "parameters": { /* echo of input params */ },
  "suggestions": [
    {
      "whg_id": 81655,
      "pid": 6337289,
      "index": "whg3dev",
      "children": [6337289, 6337282],
      "linkcount": 2,
      "title": "Coventry",
      "variants": ["Coventrie", "Coventre"],
      "ccodes": ["GB"],
      "fclasses": ["P"],
      "types": ["inhabited place"],
      "geom": [{ "type": "Point", "coordinates": [-1.51, 52.41], "properties": { "pid": 6337289 } }],
      "timespans": [[1200, 1900]]
    },
    ...
  ]
}
```

### 5.2 Client-side rendering (`renderResults()`)

1. `geomsGeoJSON(data.suggestions)` converts the suggestions array into a GeoJSON FeatureCollection (utility function).
2. Each suggestion becomes an HTML card in `#search_results` showing: title, linkcount, variants, types, country codes, feature classes, chronology timespans, and a "Place Details" button linking to `/places/<id>/portal/` or `/places/<id>/detail`.
3. The FeatureCollection is set as the MapLibre `places` source data.
4. `buildResultFilters()` creates dynamic type and country checkboxes for client-side post-filtering.
5. Results are stored in `localStorage` under key `last_search` for page-reload restoration.
6. Results sorted by `linkcount` descending — places with more linked records appear first.

---

## 6. ES index structure (current)

### Indices searched

| Index | Name (dev) | Content |
|---|---|---|
| Union (WHG) | `whg3dev` | Linked/reconciled places with `whg_id` |
| Published | `pub_dev` | All published dataset places |

### Key mapped fields (`es_mappings_whg.json`)

| Field | Type | Notes |
|---|---|---|
| `title` | keyword (normalised) | Primary place name |
| `searchy` | keyword (normalised) | All searchable name variants |
| `names.toponym` | keyword (normalised) | Individual toponym entries |
| `timespans` | integer_range | `{ "gte": year, "lte": year }` |
| `fclasses` | keyword | GeoNames feature classes |
| `ccodes` | keyword | ISO 3166-1 alpha-2 country codes |
| `geoms.location` | geo_shape | Place geometries |
| `whg_id` | long | Union index identifier |
| `place_id` | long | Source place ID |
| `children` | long | Linked child place IDs |
| `types[].label` | keyword | AAT type labels |

> **⚠ Note:** `STANDARD_FIELDS` references sub-fields `names.toponym.text` and `names.toponym.edge_ngram` that are **not present** in the mapping JSON file in the repo. These appear to be added dynamically or via an alternative mapping deployment on the live ES instance. The repo mapping defines `names.toponym` only as `keyword` with a `standard` normalizer. This discrepancy should be resolved when designing the new system.

---

## 7. Summary of data flow

```
┌─────────────────────────────────────────────────────────┐
│                    BROWSER (search.js)                   │
│                                                         │
│  #search_input ─┐                                       │
│  #adv_checkboxes ─┤                                     │
│  dateline slider ─┤  gatherOptions()  ─→  JSON payload  │
│  #chrononym_input ─┤  (temporal + spatial from PeriodO)  │
│  #categorySelector ─┤                                   │
│  #entrySelector ────┤                                   │
│  MapLibre Draw ─────┘                                   │
│                                                         │
│         │  POST /search/index/                          │
│         ▼                                               │
├─────────────────────────────────────────────────────────┤
│                 DJANGO (SearchViewV3)                    │
│                                                         │
│  json.loads(body) → request.POST.update()               │
│  handle_request() → extract params                      │
│  build_search_query() → ES bool query                   │
│  suggester() → es.search(index='whg3dev,pub_dev')       │
│  suggestionItem() → normalise hits                      │
│                                                         │
│         │  JsonResponse                                 │
│         ▼                                               │
├─────────────────────────────────────────────────────────┤
│                 ELASTICSEARCH (current)                  │
│                                                         │
│  Indices: whg3dev, pub_dev                              │
│  Query: bool { must: [multi_match], filter: [...] }     │
│  Fields: names.toponym.text^3, names.toponym^1.5,       │
│          names.toponym.edge_ngram                        │
│  Size: 100                                              │
│  Results sorted by linkcount (post-query, in Python)    │
│                                                         │
├─────────────────────────────────────────────────────────┤
│                    BROWSER (rendering)                   │
│                                                         │
│  renderResults() → HTML cards + map update              │
│  buildResultFilters() → type/country checkboxes         │
│  localStorage('last_search') → persist for reload       │
└─────────────────────────────────────────────────────────┘
```

---

## 8. Concerns & opportunities for the new system

### 8.1 Network constraint

The CRC Gateway (FastAPI on the Pitt CRC ES instance) accepts connections **only from the DigitalOcean server running Django**. The browser cannot call the gateway directly. This means Django must always act as a proxy layer between the browser and the CRC gateway:

```
Browser  ──POST──►  Django (DO server)  ──POST──►  CRC Gateway (Pitt)
                                                          │
                                                    Elasticsearch
                                                    (places, toponyms, clusters)
```

The design goal is therefore to make the Django proxy layer as **thin** as possible — ideally just forwarding a clean JSON payload to the gateway and returning the response, rather than building ES queries itself.

### 8.2 Pre-resolved data: user areas & PeriodO

Two filter types currently require Django DB lookups — but both are actually **already resolved client-side** before the search call:

| Filter | How it's pre-resolved | What arrives in the payload |
|---|---|---|
| **User areas** | `user_areas` (GeoJSON features) are injected into the page context at load time. When selected, their geometries are added to the MapLibre Draw layer. | `bounds.geometries[]` already contains the actual geometry — the Area model ID in `userareas[]` is redundant. |
| **PeriodO periods** | On chrononym selection, JS fetches `/entity/<id>/api`, extracts temporal bounds → dateline slider, spatial geometry → Draw layer. | By the time `gatherOptions()` runs, the period's temporal range is in `start`/`end` and its geometry is in `bounds.geometries[]`. No period ID is sent. |

**Implication:** The search payload already contains all the resolved filter data (geometries as coordinates, temporal bounds as integers, country codes as strings). Django does **not** need to perform any DB lookups to service the search request. The proxy layer can pass the payload straight through to the gateway.

The only Django-dependent calls are the **pre-search** lookups (page context for user areas, `/suggest/entity` for chrononym typeahead, `/entity/<id>/api` for period details). These remain on Django but happen *before* the search, not during it.

### 8.3 Efficiency issues in the current architecture

1. **Django rebuilds what the browser already knows:** The browser sends a fully-specified payload (query string, feature classes, temporal range, geometries, country codes). Django then re-parses and re-structures this into an ES query. A thin proxy to the gateway could skip this entirely.

2. **`QueryDict.update()` bug:** Merging JSON into Django's `QueryDict` flattens arrays. For example, `countries: ["GB", "FR"]` becomes `countries: "FR"`. This is fragile and has caused subtle issues with multi-valued params.

3. **No pagination:** `"size": 100` is hard-coded. No scroll/search-after mechanism.

4. **Two-index search:** Both `whg` and `pub` are searched in one call. Results from `pub` (unlinked) are mixed with `whg` (linked). The new CRC indices (`places`, `toponyms`, `clusters`) may allow a more structured approach.

5. **Post-query sorting only:** Results are sorted by `linkcount` in Python after retrieval. ES `_score` is used for matching but not for final ordering.

6. **Result facets are client-side only:** Type and country filtering after search doesn't update ES — it just hides/shows DOM elements. The new system could use ES aggregations for proper faceted search.

### 8.4 PeriodO integration notes

The period filter works by fetching a PeriodO entity (via the WHG `Chrononym` model → LPF Feature), then injecting its temporal bounds into the dateline slider and its spatial geometry into the draw layer. This is elegant but means:

- Period geometry is mixed with user-drawn geometry in the `bounds` field — no way to distinguish them on the backend.
- The temporal range is set to the period's outer bounds — the user can then narrow it with the slider.
- The PeriodO entity lookup requires two separate HTTP calls (`/suggest/entity` + `/entity/<id>/api`) — but these happen **before** the search, not during it.

### 8.5 Proposed responsibility split

| Responsibility | Current | Proposed |
|---|---|---|
| Page context (user areas, dropdown data) | Django `SearchPageView` | **Django** (unchanged) |
| Chrononym typeahead | Django `/suggest/entity` | **Django** (unchanged) |
| Period entity fetch | Django `/entity/<id>/api` | **Django** (unchanged) |
| **Search payload construction** | **Browser** `gatherOptions()` | **Browser** (unchanged or cleaned up) |
| Search payload → ES query building | Django `build_search_query()` | **CRC Gateway** |
| ES query execution | Django `suggester()` → ES | **CRC Gateway** → ES |
| Result normalisation | Django `suggestionItem()` | **CRC Gateway** |
| Faceted aggregations | Not implemented | **CRC Gateway** (new) |
| **Thin proxy** (browser ↔ gateway) | N/A | **Django** (new, minimal) |
| Result rendering | Browser `renderResults()` | **Browser** (unchanged or redesigned) |

The Django search view becomes a simple proxy:

```python
# Proposed thin proxy (sketch)
class SearchProxyView(View):
    def post(self, request):
        payload = json.loads(request.body)
        resp = requests.post(settings.CRC_GATEWAY_URL + '/search', json=payload)
        return JsonResponse(resp.json(), safe=False)
```

All query-building intelligence moves to the gateway. The browser payload format may need minor adjustments to match the gateway's expected input, but the data it already sends (qstr, fclasses, temporal bounds, geometries, country codes) is everything the gateway needs.

### 8.6 CRC Gateway configuration in Django

The CRC Gateway connection is configured in `whg/local_settings.py`:

```python
CRC_GATEWAY_URL = 'https://index.whgazetteer.org'  # FastAPI gateway on Pitt CRC
CRC_GATEWAY_API_KEY = ''                            # Optional Bearer token
CRC_GATEWAY_TIMEOUT = 10                            # Seconds
```

Access is **gated by the selected UI version** (beta mode). The check is in `api/crc_client.py :: _is_enabled_for_request()`:

1. `CRC_GATEWAY_URL` must be configured (non-empty) in settings.
2. The user must be authenticated.
3. The user's session version (`request.session['whg_version']`) must be **≥ 3.5** (i.e. the user has switched to the beta version via the header badge).

If any condition fails, the gateway client returns `[]` and the system falls back to the legacy ES indices on DigitalOcean only.

Currently the gateway client (`crc_client.py`) is used only for **reconciliation** (`crc_reconcile_search()` and `crc_suggest_search()`), not for the public search page. It adapts CRC gateway responses into the same shape as legacy ES hits so that existing `make_candidate()` logic works unchanged. A similar thin-proxy approach would be needed for the search page if it is to use the CRC gateway.

> **Note:** Because the gateway is only enabled in beta mode (version ≥ 3.5), it can be developed and tested without affecting production users on the stable version. Setting `WHG_BETA_VERSION=` (empty) in `.env` disables the version switcher entirely, making the gateway unreachable regardless of settings.

