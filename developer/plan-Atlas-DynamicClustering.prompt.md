# Master Plan: WHG v3.5 Platform Rebuild

> **Companion specification.** This is the front-end specification, paired with the backend execution plan at `/home/stephen/PycharmProjects/indexing/developer/plan-ingestionRebuild.execution.md`. The two documents reference each other; Appendix E summarises the implications of this plan for the indexing/ingestion side, and §10.2 follows the backend plan's principle that DO PostgreSQL remains the canonical store of all contributor gazetteers.

## 0. Introduction

This master plan consolidates three separately-evolved planning streams into a single coherent specification for WHG's transition from v3.2 to a redesigned platform:

1. **Atlas as the Default Site UI** (formerly `plan-AtlasUI-modifications.prompt.md`) — the prototype Atlas UI becomes the platform's default interface, with terminology unified around "Gazetteers" replacing the v3.2 distinctions between Authorities, Datasets, and Collections.

2. **Dynamic Clustering UI** (formerly `plan-dynamicClusteringUI.prompt.md`) — the search interface gains client-side dynamic clustering driven by a user-controlled similarity threshold, with cluster cards, facet-emphasis controls, and phonetic comparison affordances. The clustering machinery is implemented in JavaScript over a clustering-ready payload returned by the CRC gateway.

3. **Unified Contribution Workflow** (the latest planning stream, originally `plan-dynamicClustering-latest.prompt.md`) — what was three separate workflows in v3.2 (reconciliation, accessioning, ordinary search) collapses into one interaction model. Contributors use the same UI as researchers, with visibility scoping isolating their pending work and a binary editorial gate before publication.

These themes share infrastructure (the search/Atlas page, the gateway response shape, the H3 spatial layer, the contributor_attestations store) and audiences (researchers, contributors, and editors all use the same interface, with different scopes and affordances applied at appropriate moments). They are presented here as one document because the implementation work for any of the three depends on, and informs, the other two.

### 0.1 Status of related plans

This master plan **supersedes** (deprecates):

- `plan-AtlasUI-modifications.DEPRECATED.md` — retained for history; do not update.
- `plan-dynamicClusteringUI.DEPRECATED.md` — retained for history; do not update.

This master plan **complements and informs** (but does not yet update):

- `plan-dynamicClustering.prompt.md` (architectural plan in the indexing repo) — covers the offline similarity graph pipeline, ES schema changes, and CRC gateway endpoints. **Pending update** to reflect this master plan; the implications for that update are summarised in Appendix E.
- `plan-ingestionRebuild.prompt.md` (partially implemented per `plan-ingestionRebuild.execution.md`) — the dataset ingestion pipeline. **Pending update** to align with the unified contribution workflow specified in Parts VI–X; the implications for that update are summarised in Appendix E.

### 0.2 How to read this plan

The plan is organised in three movements that mirror the user-facing flow:

- **Parts I–V** specify the platform's general (researcher-facing) UI: the unification of terminology, the search response payload, the client-side clustering algorithm, the Atlas/search page's UI affordances, and the API surface that supports them.
- **Parts VI–X** specify the contribution workflow as a layered extension of that general UI: the contributor's narrative, the visibility-scoping mechanism, contributor-specific UI affordances, the editorial review process, and retention/migration policy.
- **Appendices A–E** cover implementation phases, performance characteristics, failure modes, dependencies, and the implications for the architectural and ingestion plans noted above.

---

## Part I — Atlas as the Default Site UI

The prototype Atlas UI was originally a focused interface for spatial exploration. Experience with the prototype has shown that its affordances generalise: the same map-plus-results interaction model is well suited to ordinary search, to the contributor's reconciliation work, and to the editor's review. This part specifies the consequent reframing: Atlas becomes the default UI for the platform, and the v3.2 distinction between "Authorities", "Datasets", and "Collections" — three different objects with three different interfaces — collapses into a single concept, **Gazetteers**, with one interface.

### 1.1 Terminology unification: Gazetteers

The platform's data is organised in named collections of place records, each with its own provenance, license, and curatorial intent. In v3.2 these collections were partitioned into three classes — Authorities (large external sources like GeoNames, Wikidata, OSM), Datasets (curator-uploaded contributions), and Collections (sets of places assembled for teaching or research) — each with its own UI and management workflow. The redesigned platform recognises that the technical and curatorial distinctions among these classes are softer than v3.2's UI suggested: WHG Datasets are now indexed as separately-namespaced Authorities, Collections are best understood as views over the underlying records, and any class can be a unit of search filtering, exploration, or contribution.

All such named collections are referred to henceforth as **Gazetteers**. The underlying technical distinctions (which authority a record originates from, how the records were ingested) are preserved in metadata and provenance display, but are not surfaced as fundamental classifications in the UI.

### 1.2 Renamed UI elements

In `atlas.html` and associated CSS and JS:

- The "Sources" button is reconceptualised and labelled **Gazetteers**.
- The "Data Sources" offcanvas is renamed **Gazetteers**, and references to "sources" in its constituent element `id`s are altered to maintain clarity.
- The "Toponyms" button is relabelled **Places**.

### 1.3 Relocating the clustering controls

The clustering controls (specified in detail in Part IV) are relocated from the (former) Sources offcanvas to appear at the **top of the Results panel** when it is shown. The Results panel is the natural home for clustering controls because clustering operates on the result set and its effects are immediately visible there.

### 1.4 The Gazetteers offcanvas

The renamed Gazetteers offcanvas is populated from an extended `/suggest` API which provides a unified list of available gazetteers, including:

- What were previously called Authorities (GeoNames, Wikidata, OSM, OHM, TGN, Pleiades, etc.).
- The individual WHG-curator Datasets, now indexed as separately-namespaced Authorities.
- Collections.

The Gazetteers list serves two distinct functions, the selection of which is articulated to the user by **tabbing** (`Filter` | `Explore`, or similar) which switches between checkboxes (filter) and radio buttons (explore), and changes the labelling of an action button:

- In **Filter** mode, multiple selected gazetteers serve as a filter for subsequent searches: results are restricted to records originating from any of the selected gazetteers.
- In **Explore** mode, a single selected gazetteer makes the Atlas UI a "Gazetteer Explorer" — taking over the role currently served by separate pages for Dataset Browse and the two types of Collection Browse. The user sees the selected gazetteer's records as the primary content, with the rest of the platform's affordances (clustering, comparison, propose-link/distinct) operating in that scope.

#### 1.4.1 Coverage filtering on the Gazetteers list

Selection of gazetteers that fall entirely outside any active `Area` filter should be disabled, and the filtered-out gazetteers hidden from the list by a default-hidden toggle. Similar filtering applies for **temporality**: a gazetteer whose temporal extent does not intersect an active period filter is disabled and hidden by default.

For coverage filtering to work efficiently, **condensed H3 coverage and a temporal-extent summary should be precomputed for every gazetteer at indexing time**. The precomputed coverage is small (a compacted H3 cell set per gazetteer plus a `[start_year, end_year]` tuple) and supports cheap intersection tests against the active Area/Period filters in the browser. Implications for the ingestion plan are noted in Appendix E.

#### 1.4.2 "My gazetteers" toggle

A toggle filter on the Gazetteers list allows logged-in users to show (and subsequently explore) only their own gazetteers. This is the principal entry point to the contributor's working scope — a contributor with one or more pending datasets sees them in the Gazetteers list and selects them to enter the working view (Part VI). The "My Gazetteers" placeholder list groups contributor-owned entries into three sections: **Published** (publicly visible, immutable except via supersession), **Private** (visible only to the owner; never submitted for review — see §10.3 retention policy for `private_permanent`), and **Pending** (drafts and submitted-under-review datasets).

#### 1.4.3 Curatorial fields on the gazetteer registry

The `GazetteerRegistryEntry` model (Django app `api`, defined in `api/models.py`) carries three **admin-only curatorial fields** alongside the inventory-derived fields populated by the ingestion pipeline. They are **deliberately not** part of the inventory push payload (see Appendix E.2) so staff curation survives every re-push:

- **`core`** (boolean, indexed). Pre-selects the gazetteer in the offcanvas Filter mode by default and renders a small "core" badge next to its name. Seeded `True` for **GeoNames, Wikidata, TGN**; everything else seeds `False`. Replaces the previously hardcoded default-checked attributes in the standard list.
- **`tileset_polygon_only`** (boolean). Marks gazetteers whose tilesets are generated for polygons only (currently OSM and OHM). The Atlas JS disables these entries in **Explore** mode (the Explorer view depends on tileset rendering) with an explanatory tooltip; in **Filter** mode they remain selectable because filtering does not depend on tilesets. Editable in admin to allow staff override of the pipeline-set value.
- **`gazetteer_type`** (CharField, choices `standard | itinerary | network`). Sketch-only field that backs the type-pill filter inside the Specialist Gazetteers expansion (§1.4.4). "Standard" encompasses what are currently Datasets, Place Collections, and Dataset Collections without embedded sequence/network metadata; "Itinerary" and "Network" are placeholders for future sequence- and network-aware gazetteer types.

A computed `is_global` property (`@property` returning `h3_coverage == "global"`) is exposed read-only in admin for display alongside the `h3_coverage` JSON field; it is not a separate persisted field — `h3_coverage` remains the single source of truth, holding either the literal string `"global"` or a list of compacted H3 cell IDs.

A staff-only Django admin (`api/admin.py::GazetteerRegistryEntryAdmin`) exposes the registry with **only the three curatorial fields editable** (plus `tileset_polygon_only` for staff override); all inventory-derived fields are read-only. `list_editable` on `core`, `tileset_polygon_only`, and `gazetteer_type` enables in-place editing from the changelist.

#### 1.4.4 Specialist Gazetteers expansion

The row formerly labelled "WHG datasets" is **relabelled "Specialist Gazetteers"** and behaves as an inline-expandable container rather than a leaf selection. Clicking the row reveals a tinted card beneath it containing:

- A **search box** that filters the list by case-insensitive substring match on the dataset name.
- An **Explore-only type-pill filter** (Standard | Itinerary | Network) implemented as a multi-select btn-group. **Standard** is active by default; the user can add Itinerary/Network or de-select Standard. When all pills are off, the convention is "show all" (matching the page's existing area/region selection convention).
- A **searchable list** of WHG-namespaced datasets, populated lazily from a JSON script tag emitted by the Django view from `GazetteerRegistryEntry.objects.filter(entry_class='dataset', namespace='whg')`. List rows carry `data-gazetteer-type="…"` so the type-pill filter can hide non-matching entries. Children render as checkboxes in Filter mode and radios in Explore mode, matching the parent input semantic.

The **parent row** uses **tri-state semantics** mirrored into `filterState`:

- **Unchecked** → no Specialist Gazetteers contribute to the search.
- **Fully checked** → the compact alias `whg` is sent (covers all WHG datasets); ticking the parent ticks every visible child.
- **Indeterminate** (some children checked) → the explicit list of selected child IDs is sent in place of the bare `whg` alias.

The Specialist Gazetteers parent row is **hardcoded in the template** (not seeded as a `GazetteerRegistryEntry` row) because it is a presentational grouping over WHG-namespaced datasets, not a true authority entry; treating it as a registry row would conflict with the inventory pipeline's own use of the `whg` namespace for individual datasets. The template's loop skips any DB row with `id="whg"` defensively.

### 1.5 Main navigation rationalisation

With the Atlas UI as the default and Gazetteers absorbing several previously-distinct concepts, the main site navigation bar simplifies:

- **Search** is removed. In its current `dev`-server form it was a prototype which led to development of the Atlas UI; the Atlas UI replaces it.
- **Atlas** remains as the primary entry point.
- **Gazetteers** is added as a sibling top-level link sitting immediately to the right of Atlas. It is a deep link into the Atlas page that pre-selects "Places" mode, opens the Gazetteers offcanvas, and switches it into Explore tab. The link is implemented as `{% url 'atlas-page' %}?panel=gazetteers&gmode=explore`; the Atlas JS reads `URLSearchParams(location.search)` on load and triggers native `.click()` on the existing Places-mode button, the offcanvas trigger (`#open_gazetteers_modal`), and (if `gmode=explore`) the Explore tab button — so all wired side-effects (the `.active` class update, Bootstrap's `data-bs-target` offcanvas open, and `setGazetteerMode`) fire exactly as if the user had clicked them by hand.
- **Workbench** remains for now, although the contribution pipeline will probably need to be restructured in subsequent development. The Workbench could be moved entirely to the Documentation site, where it would be more easily edited and maintained.
- **Teaching** remains but will incorporate new material from OME once the integration is active. References to "Place Collection" and "Collection Groups" are reframed as "Gazetteers". Some of the content could be moved to the Documentation site. The link might be better served as a dropdown rather than a single page.
- **Data** is removed. Most of its functionality is provided by the Gazetteers list (i.e. "My Data", "Published Datasets", and "Published Collections"). Functions that move to other options include "Admin Dashboard", "API", and "Volunteering".
- Wherever documentation is linked, it should be loaded into a modal panel where practical, to save the user leaving the site.

### 1.6 Welcome content absorbing the landing page

The content of the `.atlas-welcome-title` element can absorb the function of the current main site landing page, making that landing page redundant. Users arriving at the platform see the Atlas UI directly with welcome content in place of an immediate query, and engage by searching, by exploring a featured gazetteer, or by following a link from the Teaching or Workbench sections.

---

## Part II — Search Response Payload Format

The CRC gateway (`POST /api/search`) returns a **clustering-ready response** containing:

- A flat list of search hits, each carrying compact clustering signals (H3 cell, temporal range, AAT type info, Symphonym phonetic embedding, baseline cluster ID).
- An `edges` array forming a **local similarity subgraph** — pairwise similarity edges between result-set members, each with a composite score and per-facet signal breakdown.
- Aggregations for type and country facets (unchanged from current).
- Response-level metadata including `toponym_stoplist` (generic name tokens used by the synthetic-edge stoplist guard, §3.9) and `clustering_params` (calibrated defaults for `θ_bridge`, `θ_query`, `θ_synth`, `θ_synth_structural`, `τ_name`, `τ_link`, default facet weights).

The gateway also supports an optional `cluster_threshold` parameter for server-side fallback clustering (for non-JS consumers like OpenRefine — see §5.1).

The Django thin proxy (`api/crc_client.py`) must pass all of these additions through unchanged. The browser is the consumer that drives the client-side clustering specified in Part III.

### Architecture overview

```
Browser (whg3 Atlas page)
  ├── Sends search request → Django thin proxy → CRC Gateway → ES
  ├── Receives: hits[] + edges[] + aggregations + clustering_params + toponym_stoplist
  ├── clustering.js: Union-Find on edges, threshold from slider
  ├── Slider/weight changes → instant re-cluster (no round-trip)
  └── Renders: cluster cards (collapsed/expanded) + map markers

Django thin proxy (api/crc_client.py)
  ├── Forwards search requests to CRC Gateway
  ├── Passes through new fields (edges, phon_emb, etc.)
  └── Passes through cluster_threshold when set
```

### 2.1 Per-result compact payload

For each hit, the gateway returns (in addition to existing fields):

```json
{
  "place_id": "gn:2988507",
  "score": 87.2,
  "title": "Paris",
  "namespace": "gn",
  "repr_point": [2.3522, 48.8566],
  "h3": "871ea6d75ffffff",
  "h3_cover": ["871ea6d75ffffff", "851ea6d7fffffff"],
  "temporal_range": [-500, 2026],
  "aat_ids": [300008347],
  "aat_depths": [6],
  "baseline_cluster_id": "c_abc123",
  "query_match": {
    "name": "Paris",
    "score": 0.93,
    "phon_emb": "<base64-encoded 128-byte int8 vector>"
  },
  "names": [...],
  "ccodes": ["FR"],
  "types": [...],
  "geometries": [...]
}
```

**Key new fields:**

| Field | Type | Purpose |
|-------|------|---------|
| `h3` | string | H3 cell ID at resolution 7 for the representative point. Two results sharing an `h3` value are spatially proximate (~1.2 km hex). Used as the primary clustering spatial signal. |
| `h3_cover` | string[] | Compacted H3 cell IDs covering the full geometry (multi-resolution). Used for spatial bucketing via cover intersection at r5 (§3.9) to catch places whose centroids differ across authorities. For point-only geometries, equals `[h3]`. |
| `temporal_range` | `[int, int]` or `null` | Flattened temporal extent `[start_year, end_year]` across all timespans. Null if the place has no temporal data. For display only — temporal similarity is precomputed in edge signals. |
| `aat_ids` | `int[]` | AAT concept IDs from the place's type mappings. For display (type labels, tooltips). |
| `aat_depths` | `int[]` | AAT hierarchy depths, parallel to `aat_ids`. |
| `baseline_cluster_id` | `string` or `null` | Precomputed high-confidence cluster ID (θ ≈ 0.9). Results sharing a `baseline_cluster_id` are near-certain co-referents. Used to bootstrap the Union-Find before applying the user's threshold. |
| `query_match` | object | Discovery-time match signal. Contains `name` (the matched toponym string), `score` (normalised 0–1 discovery score — how well the query matched this toponym), and `phon_emb` (Symphonym 128-d int8 embedding, base64-encoded, 172 chars). Used for query-conditioned clustering (§3.8) and phonetic re-scoring (§3.7). |

The response also includes a top-level `query_emb` field: the Symphonym 128-d int8 embedding of the original query string (base64-encoded). This eliminates the need for a separate `GET /api/embed` call for the initial query — the client uses it directly for phonetic comparison (§3.7). For variant names the user types later, `/api/embed` is still called.

### 2.2 Edges array

Alongside the hits:

```json
{
  "edges": [
    {"a": "gn:2988507", "b": "wd:Q90", "score": 0.95,
     "s": {"n": 0.98, "sp": 0.92, "t": 0.85, "ty": 1.0, "l": 1.0}},
    {"a": "gn:2988507", "b": "osm:n12345", "score": 0.87,
     "s": {"n": 0.90, "sp": 0.95, "t": null, "ty": 0.78, "l": 0.0}},
    ...
  ]
}
```

Each edge carries:

| Field | Type | Description |
|-------|------|-------------|
| `a`, `b` | string | The two `place_id` values connected by this edge. |
| `score` | float (0–1) | Precomputed composite similarity score (weighted combination of all facets). |
| `s.n` | float (0–1) | Toponym similarity (name match). |
| `s.sp` | float (0–1) | Spatial proximity (geographic distance). |
| `s.t` | float (0–1) or `null` | Temporal overlap (null if either place lacks timespans). |
| `s.ty` | float (0–1) | Type similarity (AAT Wu-Palmer). |
| `s.l` | float (0–1) | Authority link overlap (shared cross-authority IDs). |

Only edges between result-set members are included (the gateway prunes the graph to the result set).

### 2.3 Payload size budget

- ~500 results × ~500 bytes ≈ 250 KB (hits including query_match at ~200 bytes each)
- ~2000 edges × ~120 bytes ≈ 240 KB (edges with signal breakdown)
- query_emb: 172 bytes (negligible)
- Total: ~490 KB before gzip, ~110–160 KB compressed — within budget.

**Hard cap:** `max_edges = 4000`. The gateway enforces this limit on the `edges` array, selecting edges by highest composite score globally. Without a cap, worst-case scenarios (dense urban + high K + symmetrisation: 500 × 50 / 2 = 12,500 edges ≈ 1.5–2 MB) degrade both transfer and client parsing time. The cap keeps payload under ~750 KB pre-compression in all cases.

For result sets > 500, the gateway caps edges to top-scoring pairs and/or restricts clustering to the top N results.

---

## Part III — Client-Side Clustering Algorithm

### 3.1 Edge scores and facet-weight scaling

Each edge arrives from the server with a precomputed composite score that already incorporates all facets (toponym, spatial, temporal, type, links). The client's primary operation is **thresholding**: keep or discard edges based on the user's slider position θ.

For richer control, the server decomposes the composite score into per-facet **signal components** on each edge (the `s` object). The client can reweight on the fly:

```
S = w_n·s.n + w_sp·s.sp + w_t·s.t + w_ty·s.ty + w_l·s.l
```

with UI-controlled emphasis sliders (e.g. "prioritise spatial proximity" or "prioritise name similarity"). This turns the system from simple threshold clustering into a **semantic lensing system** — the user can shift what "similar" means, not just how strict the cutoff is.

Default weights (from the offline calibration pipeline):

| Facet | Weight | Description |
|-------|--------|-------------|
| `w_n` | 0.30 | Toponym match |
| `w_sp` | 0.25 | Spatial proximity |
| `w_t` | 0.10 | Temporal overlap |
| `w_ty` | 0.10 | Type match |
| `w_l` | 0.25 | Authority links |

Temporal and type facets are weighted lower because many records lack temporal data and type mappings are incomplete; links are weighted higher because authority assertions are the strongest identity signal.

**Null-facet handling.** When a signal component is `null` (e.g. `s.t = null` because one or both places lack timespans), the client **renormalises weights dynamically**: redistribute the null facet's weight proportionally among the non-null facets. For example, if `s.t = null` and the user's weights are `[0.30, 0.25, 0.10, 0.10, 0.25]`, the effective weights become `[0.30, 0.25, 0, 0.10, 0.25] / 0.90 = [0.333, 0.278, 0, 0.111, 0.278]`. This ensures records lacking temporal data are not penalised (treated as 0) or artificially boosted — they are simply scored on the available evidence. The same renormalisation rule is used in the offline pipeline for consistency.

**Known tradeoff: missing data scores higher than noisy data.** Redistribution means two places with perfect name/spatial/link but *no* temporal data score higher than two with perfect name/spatial/link but *slightly mismatched* temporal data. We accept this because: (1) treating null as 0 is strictly worse — ~40% of records lack temporal data and would be systematically under-clustered; (2) the temporal weight is only 0.10, so the maximum scoring advantage from missing data is small (~0.10); (3) the issue diminishes as temporal coverage improves.

**Int8 cosine similarity.** Symphonym embeddings are unit vectors quantized to int8 range [-128, 127]. The client computes `dot(a, b) / (norm(a) × norm(b))` using `Int8Array` arithmetic. Server-side and client-side similarity values are consistent because both use the same quantized vectors.

**Score invariance.** The composite `score` on each edge equals the weighted sum of signal components after null renormalisation under the default (calibrated) weights: `score == Σ (w_i / Σ_nonnull w_j) × s_i`. The client can reconstruct the server's composite score exactly from the `s` breakdown under default weights. This is tested in the offline pipeline and guaranteed as an API invariant — any discrepancy between server and client scores at default weights indicates a bug. When the user adjusts facet weights, the recomputed score intentionally diverges from the precomputed `score` field; only the signal components `s.*` are used.

This approach keeps all expensive similarity computation server-side (in the offline pipeline), while giving the client cheap, instant re-weighting with no server round-trip. The client never recomputes spatial distances, temporal overlaps, or AAT LCA depths — it only applies weight coefficients to precomputed normalised scores.

### 3.2 Comparison pruning

Only compare pairs that have a precomputed edge. This avoids O(n²) explosion:

- The server already prunes to the local subgraph (edges between surviving results).
- Additional client-side blocking: same H3 cell, or shared authority link, or same baseline cluster.
- For ~500 results with ~2000 edges, clustering is O(n) — trivially fast.

### 3.3 Union-Find with threshold

```
// Phase 1 — precomputed edges
for each edge (a, b, signals):
    S = reweight(signals, weights)    // with null-facet renormalisation (§3.1)

    // Rule 1 — standard: edge exceeds user threshold
    if S >= θ:
        union(a, b)

    // Rule 2 — query bridge: relax threshold for query-relevant pairs (§3.8)
    elif S >= θ_bridge
         AND min(query_score[a], query_score[b]) >= θ_query
         AND (signals.n >= τ_name OR signals.l >= τ_link):   // name/link guard
        union(a, b)

// Phase 2a — phonetic synthetic edges for edgeless pairs (§3.9)
θ_synth_eff = max(θ_synth, θ)    // never below calibrated floor or user threshold
for each spatial bucket (results sharing h3 centroid OR h3_cover intersection at r5):
    for each pair (a, b) in bucket where find(a) ≠ find(b) AND no precomputed edge:
        if NOT both_high_frequency(name[a], name[b]):  // stoplist guard (§3.9)
            if types_overlap(a, b):   // at least one shared type (§3.9)
                sim = cosine(phon_emb[a], phon_emb[b])
                if sim >= θ_synth_eff:
                    union(a, b)

// Phase 2b — structural synthetic edges (§3.9)
for each spatial bucket (same as 2a):
    for each pair (a, b) in bucket where find(a) ≠ find(b) AND no precomputed edge:
        if (ccode_overlap(a, b) OR shared_namespace(a, b) OR shared_baseline(a, b)):
            union at θ_synth_structural (≈ 0.7)

// Phase 3 — post-processing: split oversized clusters (§3.6)
for each component C where |C| > N_max:
    split C by tightening threshold within the component
```

Properties:
- Edge iteration order does not affect the result — Union-Find is applied over all qualifying edges in a single pass (no sorting required). Complexity: O(E·α(n)) ≈ O(E).
- Rule 2 has a **name/link guard** (`signals.n >= τ_name OR signals.l >= τ_link`, where `τ_name ≈ 0.5`, `τ_link ≈ 0.8`) that prevents the bridge from firing on weak edges between places matching generic query terms (e.g. "San", "New", "Central"). Without this guard, two places both matching a common query fragment would merge on a sub-threshold edge with no substantive name or link alignment.
- Rule 2 and Phases 2a/2b can cause **non-monotonic behaviour**: lowering θ may merge clusters that were separate at higher θ due to the bridge and synthetic thresholds. In practice this is rare (bridge fires on <5% of edges, synthetic passes on edgeless pairs only). Tying `θ_synth_eff = max(θ_synth, θ)` limits the effect: at high θ, synthetic edges require even higher phonetic similarity, preserving monotonic feel in the common case.
- Union-Find is near-linear and runs in <10 ms for 500 nodes.
- The query-bridge rule (Rule 2) ensures query-relevant pairs cluster even when their precomputed toponym signal `s.n` is low — see §3.8 for the full rationale.
- The phonetic synthetic-edge pass (Phase 2a) closes the "missing edge" gap for pairs that share spatial proximity, phonetic similarity, and type overlap but were never candidates in the offline pipeline — see §3.9.
- The structural synthetic-edge pass (Phase 2b) catches same-place records across authorities where phonetics fail (cross-lingual exonyms, sparse names) — see §3.9.
- Oversized clusters are split as a post-processing step (Phase 3), not blocked during union — see §3.6.

### 3.4 Baseline cluster bootstrapping

Before applying the user threshold, initialize the Union-Find with baseline clusters (if present): for all results sharing a `baseline_cluster_id`, union them. This provides instant grouping for obvious matches (e.g. GeoNames + Wikidata for the same city) before the user even touches the slider.

**θ = 1.0 bypass.** When the user sets θ = 1.0 ("no grouping — flat list"), baseline bootstrapping is **skipped entirely**. The Union-Find starts with every result in its own singleton component, and since no edge can have a reweighted score ≥ 1.0, no unions occur. This guarantees a truly flat result list identical to unclustered behaviour. At any θ < 1.0, baseline bootstrapping runs normally.

**Safety:** baseline clusters are **link-dominated**: constructed offline using only authority link signals (`s.l`) and very high toponym signals (`s.n ≥ 0.95`), not the full composite score. This ensures they remain valid regardless of how the user tunes facet weights — a user who prioritises spatial proximity over name similarity will not find that baseline clusters contradict their semantic intent. Bootstrapping cannot merge two *different* baseline clusters — it only unions results within the same cluster ID. Subsequent edges from Phase 1 may *expand* a baseline cluster by merging additional results into it, but only if those edges pass the user's threshold θ.

### 3.5 Cluster display

Each cluster gets:
- **Representative**: highest-scoring hit (or preferred-authority heuristic).
- **Aggregated metadata**: all names across members, all authorities, temporal span union, types union.
- **Expandable**: user can expand a cluster to see individual member records.

### 3.6 Cluster-size limiting (post-processing)

Union-Find can produce "mega-clusters" at low θ in dense urban regions (e.g. every "Paris" record in one group). Rather than blocking merges during the union pass (which introduces order-dependent results and breaks transitivity), oversized clusters are **split as a post-processing step** after the Union-Find completes:

1. For each connected component with more than `N_max` members (e.g. 50):
2. Extract the subgraph of edges within the component.
3. Tighten the threshold iteratively: raise θ within this component until it fragments into sub-clusters all ≤ N_max, or until θ reaches 0.95 (at which point accept the large cluster as genuinely co-referent).
4. Hard-link edges (authority sameAs, `s.l ≈ 1.0`) are never cut during splitting — they act as unbreakable bonds within the component.

This preserves transitivity: if A~B and B~C both pass the user's threshold, they are always in the same component. Splitting only tightens the threshold *within* oversized components, producing deterministic and order-independent results.

### 3.7 Client-side phonetic re-scoring

Each hit carries a `query_match.phon_emb` field: the Symphonym 128-d int8 embedding for the place's best-matching toponym. The response also includes `query_emb` — the embedding of the original query string. The client can use these to let the user type an alternative name variant and instantly see how phonetically close it is to every result — without a server round-trip.

**Flow:**

1. User types a variant in a "Compare name" input (e.g. "Parigi").
2. The client calls `GET /api/embed?name=Parigi` on the gateway (via the Django proxy), which returns the Symphonym int8 embedding for the new variant (fast — single model inference, ~5 ms). For the *initial* query, `query_emb` from the response is used directly (no extra call needed).
3. The client computes cosine similarity between the variant embedding and each hit's `query_match.phon_emb` in JavaScript. Int8 dot product on 128 dimensions is trivially fast (~0.01 ms per pair). Embeddings are pre-normalised unit vectors quantized to int8; compute `dot(a, b) / (norm(a) × norm(b))` for the similarity value.
4. Results are re-ranked or highlighted by phonetic proximity to the user's variant.

This enables cross-script and cross-transliteration name comparison directly in the browser — a researcher can type a name in Arabic script and see which Latin-script results are phonetically closest, or compare a medieval spelling variant against modern authority records.

The `query_match.phon_emb` vectors also serve a structural role in clustering: they enable **synthetic phonetic edges** for result pairs that lack a precomputed edge — see §3.9.

### 3.8 Query-conditioned clustering

Precomputed edges encode **query-independent** similarity (`place ↔ place`). But effective search-result clustering requires **query-conditioned** grouping (`query → place → place`). Consider: a user searches for "Big Apple". One result (Wikidata's New York City) matches via that alias. Other NYC records from GeoNames, OSM, etc. may also appear in the result set — matched via "New York" through different discovery paths or neighbor expansion — but they matched the query on a different name variant.

**Primary defence (offline, in the indexing pipeline):** The toponym facet signal `s.n` on each precomputed edge is computed as the **maximum Symphonym cosine similarity across ALL cross-name pairs** of the two places — not just the single toponym that triggered blocking. This means places sharing "New York" produce a high `s.n` even when the query matched on "Big Apple". In most cases, this is sufficient: the standard threshold rule (§3.3 Rule 1) handles alias cases because the graph already captures the full alias overlap.

**Safety net (client-side):** The query-bridge rule in §3.3 (Rule 2) catches residual edge cases where the max-pairwise toponym score is still coincidentally low but both places are clearly query-relevant. Each hit's `query_match.score` indicates how strongly it matched the original query. The bridge rule relaxes the edge threshold when both endpoints strongly match the query:

- `θ_bridge = θ × 0.6` (or a configurable floor, e.g. 0.3) — minimum edge quality for bridging.
- `θ_query = 0.7` — minimum query-match score.
- `τ_name ≈ 0.5` — minimum toponym signal for name-based bridge qualification.
- `τ_link ≈ 0.8` — minimum link signal for link-based bridge qualification.

The bridge fires only when `signals.n >= τ_name OR signals.l >= τ_link` (in addition to the score and query conditions). This **name/link guard** prevents the bridge from becoming a semantic shortcut on weak edges between places matching generic query terms ("San", "New", "Central"). Without it, any two results matching a common query fragment could merge on a sub-threshold edge with no substantive identity signal.

**Why `min()` not `max()`:** both endpoints must strongly match the query for the bridge to fire. Using `max()` would let a single strong match pull in weakly-related neighbors indiscriminately. Using `min()` ensures both places are relevant to what the user searched for.

**Why a precomputed edge is required:** merging two places purely because both match the query (without any precomputed edge) is dangerous — "London" would merge London-UK with London-Ohio. The bridge rule only relaxes the *threshold* on existing edges; it does not create edges from nothing.

### 3.9 Synthetic edges (edgeless pairs)

The precomputed graph will inevitably miss some co-referent pairs — rare aliases, missing language variants, or places that fell outside the offline blocking thresholds. Two complementary synthetic passes close this gap at query time.

#### 3.9.1 Synthetic Rule A — Phonetic (§3.3 Phase 2a)

The `query_match.phon_emb` vectors enable **phonetic synthetic edges** between result pairs that have no precomputed edge.

After the main Union-Find pass (§3.3 Phase 1), the client runs Phase 2a over spatial buckets:

1. Group results by spatial proximity: same `h3` centroid (r7 ≈ 1.2 km) **or** `h3_cover` intersection at coarse resolution (r5 ≈ 8 km). Centroid equality catches point-vs-point matches; cover intersection catches cases where the same place has different centroids across authorities (e.g. Paris polygon vs Paris point, linear features, boundary geometries). The server provides both `h3` (centroid) and `h3_cover` (multi-valued) per hit.
2. Within each bucket, for every pair (a, b) not already in the same component and with no precomputed edge:
   - **Stoplist guard:** skip if both `query_match.name` values are in a **high-frequency toponym stoplist** (e.g. "Central", "Station", "Market", "Church", "School", "Main", "Park", "New", "San", "Saint"). Without this guard, high phonetic similarity + same H3 + same type (e.g. "building") produces catastrophic merging in OSM-heavy urban regions. The stoplist is included in the server response metadata.
   - **Type constraint:** at least one type must overlap (shared `aat_id`, or both lacking type data). If both places have typed records, require at least one shared AAT ancestor; if either is untyped, allow the comparison (untyped records are common and should not be excluded).
   - Compute `cosine(phon_emb[a], phon_emb[b])`.
3. If the similarity exceeds `θ_synth_eff = max(θ_synth, θ)`, union them.

**Why `max(θ_synth, θ)`:** this ties the synthetic threshold to the user's main slider. At high θ (conservative grouping), synthetic edges require even higher phonetic similarity, preserving near-monotonic behaviour. At low θ (aggressive grouping), the calibrated floor `θ_synth` (e.g. 0.85) prevents garbage merges.

#### 3.9.2 Synthetic Rule B — Structural (§3.3 Phase 2b)

A second synthetic pass catches same-place records across authorities where phonetics fail entirely — cross-lingual exonyms with weak phonetic alignment, sparse single-attestation records, or type-misaligned authorities (settlement vs admin unit). This pass uses **shared structural identifiers** rather than phonetic similarity:

Within the same spatial buckets:

```
for each pair (a, b) in bucket where find(a) ≠ find(b) AND no precomputed edge:
    if (ccode_overlap(a, b) OR shared_namespace(a, b) OR shared_baseline(a, b)):
        union at θ_synth_structural (≈ 0.7)
```

Rationale:
- **Country code overlap** catches same-place records from different authorities sharing a country (cheap, high precision when combined with spatial proximity).
- **Shared authority namespace** catches records from the same source that bypassed blocking (rare but diagnostic).
- **Shared `baseline_cluster_id`** propagates high-confidence offline groupings to pairs that lost connecting edges during result-set pruning.

This is very cheap (set intersections, no embedding computation), high precision, and catches the dominant failure mode — edge incompleteness for cross-lingual or sparse records.

#### 3.9.3 Cost analysis

**Phonetic pass:** H3 bucketing reduces comparisons from O(n²) to O(Σ |bucket|²). Typical result sets have most buckets containing 1–5 results; dense urban areas might have ~20. With 500 results across ~200 buckets, total comparisons are a few hundred — each a ~0.01 ms int8 dot product plus a cheap type-set intersection. Total cost: <1 ms.

**Structural pass:** same bucket structure, set intersections only — even cheaper.

**Why spatial gating is essential:** without it, phonetic similarity would merge geographically distant places (e.g. "Springfield" in Illinois vs Massachusetts).

**Why centroid-only is insufficient:** large polygons can have different centroids across authorities. Using `h3_cover` intersection at r5 catches these without requiring exact centroid alignment.

All parameters (`θ_bridge`, `θ_query`, `θ_synth`, `θ_synth_structural`, `τ_name`, `τ_link`, and the toponym stoplist) are calibrated by the offline pipeline and included in the server response as defaults — the client does not hard-code them.

---

## Part IV — Atlas Page UI Affordances

The Atlas page (the platform's default UI per Part I) has its template in `search/templates/search/search.html` (or its successor) with JS in `whg/webpack/js/search.js`. It requires significant changes to support dynamic clustering.

### 4.1 Remove: "Group linked records" toggle

The "Group linked records" checkbox in the Gazetteers panel must be removed. In its place, the panel displays a static note under the "Clustering" header explaining that clustering of linked records is managed using a similarity threshold control within the returned search results (the slider relocated per §1.3).

**Status:** Pending on the Atlas page itself. The toggle still exists in `search/templates/search/atlas.html` (lines 297–300, `id="atlas_clustering_toggle"`) and is still wired up in `whg/webpack/js/atlas.js` (lines 307 and 969). It must be removed from these files as part of this work.

The equivalent toggle has already been removed from the legacy prototype `search/templates/search/search.html` and `whg/webpack/js/search.js`, but those changes are now moot: §1.5 retires the legacy `Search` page entirely, so the toggle removal needs to happen on the Atlas page that is becoming the default UI.

**Files affected (pending):**
- `search/templates/search/atlas.html` — remove the `#atlas_clustering_toggle` checkbox + label; replace with the same informational note used elsewhere about results-panel clustering.
- `whg/webpack/js/atlas.js` — remove the `atlas_clustering_toggle` change-event handler and the read in `gatherOptions()`-equivalent code; retain a `clusterResults = true` constant only if still consumed by request building, and otherwise drop the dead code.
- `api/crc_client.py` (Django thin proxy) — stop forwarding `group_by_cluster` to the gateway (also pending; see §5.2).

### 4.2 Add: Similarity threshold slider (top of Results panel)

A continuous slider (θ ∈ [0,1]) at the **top of the Results panel** (per §1.3) controls clustering sensitivity. Position it prominently above the result list, with a label such as "Group similar places" and a tooltip explaining the behaviour.

| Slider position | Effect |
|----------------|--------|
| θ = 1.0 (rightmost) | No grouping — flat list identical to current behaviour |
| θ = 0.8 (default) | Conservative grouping — high-confidence co-referents only |
| θ = 0.5 | Moderate grouping — phonetically similar + spatially proximate |
| θ = 0.0 (leftmost) | Aggressive grouping — all connected results merged (subject to cluster-size limiting) |

**Behaviour:**
- Moving the slider triggers client-side re-clustering (§3.3) with no server round-trip.
- Debounce at ~100 ms to avoid flicker during drag.
- The result list re-renders with clustered/unclustered grouping.
- The map updates: clustered places share a marker group or are connected by visual links.
- Persist the slider position in `sessionStorage` so it survives page navigation.

### 4.3 Add: Facet emphasis controls (optional, collapsible)

Below the threshold slider, an expandable "Similarity tuning" section exposes per-facet weight sliders:

| Slider | Default | Controls |
|--------|---------|----------|
| Name similarity | 0.30 | w_n — toponym match weight |
| Spatial proximity | 0.25 | w_sp — geographic distance weight |
| Temporal overlap | 0.10 | w_t — timespan overlap weight |
| Type match | 0.10 | w_ty — AAT type similarity weight |
| Authority links | 0.25 | w_l — shared cross-authority ID weight |

Weights are normalised to sum to 1.0 in real time. Moving any slider re-triggers the Union-Find pass with the new weight vector. A "Reset to defaults" button restores the calibrated defaults from the server response.

This section is collapsed by default for casual users and expanded for power users / researchers.

### 4.4 Add: Phonetic comparison input

A small input field in the Results panel labelled "Compare name variant" or similar. When the user types a name:

1. Debounce at 300 ms.
2. Call `GET /api/embed?name=<input>` to obtain the Symphonym embedding. (For the initial search query, `query_emb` from the response is already available — no call needed.)
3. Compute cosine similarity against each result's `query_match.phon_emb`.
4. Display a phonetic proximity indicator (e.g. colour-coded badge or numeric score) next to each result.
5. Optionally re-sort results by phonetic proximity to the typed variant.

This is particularly valuable for researchers working with historical or non-Latin-script name variants.

### 4.5 Add: Cluster expansion/collapse UI

When clustering is active (θ < 1.0), the result list displays **cluster cards** instead of individual place cards:

- **Collapsed state** (default): shows the representative place (highest-scoring or preferred-authority member), a count badge ("3 sources"), and the aggregated name list.
- **Expanded state**: clicking the cluster card expands it to show all member places as sub-cards, each with its own authority badge, names, and metadata.
- **Map interaction**: clicking a cluster card zooms to the bounding box of all member `repr_point` coordinates (not full geometries — those are external and not loaded by default). Expanded members are shown as individual markers at their `repr_point`; collapsed clusters show a single marker at the representative's `repr_point`. Dense clusters (≥5 members within a small area) may optionally display a convex hull outline on hover, computed from member `repr_point` coordinates.

### 4.6 Update: Result-facet filters (post-search)

The existing client-side facet filters (Place Types checkboxes, Countries checkboxes) continue to work as before, but now operate on the **clustered** result set:

- A cluster is visible if **any** of its members passes the facet filter.
- The facet counts reflect unique clusters, not individual places (when clustering is active).
- Toggling a facet filter does not re-trigger clustering — it only shows/hides clusters in the already-computed grouping.

### 4.7 Update: Feature-class checkboxes → Type facets

The legacy feature-class checkboxes (`A`, `P`, `S`, etc.) in `#adv_checkboxes` are already marked for replacement (see `developer/search-system-architecture.md` §2.2 in the `indexing` repo). This plan accelerates that: replace them with the server-side type aggregation facets returned in the search response. The type facets use AAT identifiers and hierarchical labels from the `types` index, not GeoNames feature classes.

### 4.8 Update: Gazetteers panel (formerly Data Sources)

Per §1.2 and §1.4, the Data Sources panel is renamed and reconceptualised as the **Gazetteers** offcanvas. The renaming itself, the dual Filter|Explore mode, the unified gazetteers list (Authorities + Datasets + Collections), the H3/temporality coverage filtering, the curatorial fields layered on top of the registry, and the Specialist Gazetteers expansion are specified in §1.4.

**Delivered ahead of the extended `/suggest` API:**

- ✅ **Renamed** the offcanvas: `id="sources_offcanvas"` → `id="gazetteers_offcanvas"`, title "Data Sources" → "Gazetteers", icon `fa-book-atlas`. Trigger button updated to match.
- ✅ **Removed** the "Group linked records" toggle (§4.1) and replaced it with an informational note pointing to the Results-panel slider.
- ✅ **Server-driven render of the standard list.** `AtlasPageView.get_context_data` queries `GazetteerRegistryEntry.objects.filter(entry_class='authority')` and the template loops over the result, replacing the previously hardcoded checkbox list. Default-checked state is driven by the new `core` field (only GeoNames, Wikidata, TGN); a small "core" badge renders next to the name. Each row carries `data-tileset-polygon-only="0|1"` and `data-gazetteer-type="…"` data attributes for client-side gating.
- ✅ **Specialist Gazetteers expansion** (§1.4.4) — relabel of "WHG datasets", inline expansion containing a search box, an Explore-only Standard|Itinerary|Network type-pill filter, and a lazily-rendered list of WHG-namespaced datasets. Tri-state parent emits the compact `whg` alias when fully checked and the explicit list of child IDs when indeterminate. The parent row is hardcoded in the template (UI grouping, not a registry entry); the inventory loop skips any DB row with `id="whg"` defensively.
- ✅ **Polygon-only tileset gating.** `applyTilesetGating(mode)` runs on every `setGazetteerMode` transition and on initial load: in Explore mode it disables the inputs of any row carrying `data-tileset-polygon-only="1"` (currently OSM, OHM), greys the label via the `.disabled` modifier (with `pointer-events: auto` so the explanatory tooltip still fires), unchecks any previously selected polygon-only entry, and clears `exploreSelection` if it pointed at a now-disabled row. In Filter mode the inputs are restored.
- ✅ **Filter|Explore tabbing UI** wired (§1.4). Switching modes saves the current selection cache, swaps every authority input between `type="checkbox"` and `type="radio"` (with shared `name="gazetteer_explore"`), and restores the saved selection for the destination mode.
- ✅ **"My gazetteers" toggle** (§1.4.2) sketched with three placeholder sections (Published, Private, Pending).
- ✅ **Curatorial admin** — `api/admin.py::GazetteerRegistryEntryAdmin` registered (staff-only); curatorial fields editable, inventory-derived fields read-only.
- ✅ **Inventory-push protection** — `api/views_indexing.py::GazetteerInventoryView._upsert_one` carries an inline comment naming `core`, `tileset_polygon_only`, `gazetteer_type` as admin-managed and intentionally omitted from `defaults`. `update_or_create(defaults=…)` only writes the keys present in `defaults`, so omission is sufficient to preserve curation across re-pushes (Appendix E.2).

**Pending backend integration:**

- **Extended `/suggest` API** (§5.3) — the gazetteer inventory currently rendered server-side from `GazetteerRegistryEntry` will be supplemented by the `/suggest` endpoint to drive coverage filtering, owner-based filtering for "My Gazetteers", and the per-gazetteer count indicators.
- **Per-gazetteer count indicators.** Each gazetteer entry contains an empty `<span class="gazetteer-count" data-namespace="…">` slot ready to be populated from result-set aggregations.
- **Coverage filtering** against `h3_coverage` and `temporal_extent` (§1.4.1).
- **Real per-user My Gazetteers list** (§1.4.2) replacing the placeholder data once `/suggest` returns `owner_user_id` and `status`.
- **Specialist type-pill backend wiring.** The `gazetteer_type` field on `GazetteerRegistryEntry` is currently sketch-only; the type-pill filter hides DOM rows by `data-gazetteer-type` but no backend semantics are attached. Itinerary and Network are forward-looking placeholders.

**Spatial-source changes already completed:**

- D-PLACE removed from the Territory tab's polity dataset toggle (`politySelector.js`, `POLITY_DATASETS` now contains only `cliopatria` and `nativeland`). ✅ DONE
- D-PLACE removed from the Atlas page's available spatial sources (`search/views.py`, `available_sources` no longer lists `dplace`). ✅ DONE
- D-PLACE added to the renamed Gazetteers offcanvas in `atlas.html` (unchecked by default). ✅ DONE
- "OSM/OHM (Miscellaneous)" added to the Region tab's namespace toggle (`regionSelector.js`, `NAMESPACE_OPTIONS` includes `osm_misc`). ✅ DONE — covers non-standard `boundary=` tags (aboriginal_lands, barony, civil, civil_parish, climatic_zone, geographic, histori*, indigenous_administration, native_reservation, parish, political, region, etc.). Backend tile/query support pending.

### 4.9 JavaScript implementation

The client-side clustering logic (Union-Find, edge reweighting, threshold application) should be implemented as a self-contained ES module (e.g. `whg/webpack/js/clustering.js`) with no external dependencies:

- `class UnionFind` — standard disjoint-set with path compression and union by rank.
- `function clusterResults(hits, edges, theta, weights, queryScores, params)` — returns a `Map<clusterId, ClusterGroup>`. At θ = 1.0, returns all singletons immediately (no baseline bootstrapping, no edge processing). Otherwise runs Phase 0 (baseline bootstrap), Phase 1 (precomputed edges with Rules 1–2), Phase 2a (phonetic synthetic edges), and Phase 2b (structural synthetic edges).
- `function reweightEdge(edge, weights)` — computes the weighted sum from signal components with null-facet renormalisation.
- `function queryBridgeThreshold(theta)` — computes `θ_bridge` from the user's main threshold (e.g. `θ × 0.6`, floored at 0.3).
- `function phoneticSyntheticPass(uf, hits, params)` — H3-bucketed phonetic comparison for edgeless pairs with stoplist guard and type-overlap check (§3.9.1).
- `function structuralSyntheticPass(uf, hits, params)` — H3-bucketed structural comparison (ccode overlap, shared namespace, shared baseline) for edgeless pairs (§3.9.2).
- `function typesOverlap(a, b)` — returns `true` if the two hits share at least one AAT ancestor or if either is untyped. Used as a gate for phonetic synthetic edges.
- `function isStoplistName(name, stoplist)` — returns `true` if the name is in the high-frequency toponym stoplist. Used as a guard for phonetic synthetic edges.
- `function spatialBucket(hits)` — groups hits by H3 centroid equality or `h3_cover` intersection at r5. Returns buckets for synthetic edge passes.
- `function cosineSimilarity(a, b)` — int8 dot product for phonetic re-scoring and synthetic edges.
- `function decodePhonEmb(base64)` — decode base64-encoded int8 embedding to `Int8Array`.

**Lazy embedding decode.** To reduce memory pressure on mobile devices, `phon_emb` values should remain as base64 strings until needed. Decode to `Int8Array` only when: (a) the synthetic edge pass runs, or (b) the user activates phonetic re-scoring. This avoids allocating ~500 `Int8Array` objects (~64 KB) on every search — negligible on desktop but meaningful on memory-constrained mobile browsers.

This module is imported by `search.js` and called on every slider change. It should be pure (no DOM manipulation) — it returns data structures that the rendering layer consumes.

---

## Part V — Django API Changes

### 5.1 OpenRefine / Reconciliation API documentation

The WHG reconciliation service is used by OpenRefine users who cannot perform client-side clustering. The `POST /api/reconcile` endpoint must support the new `cluster_threshold` parameter:

**New parameter: `cluster_threshold`** (`float | null`, default `null`)

When set to a value between 0.0 and 1.0, the server performs Union-Find clustering on the result subgraph and returns grouped results. When `null` (default), results are returned as a flat list (backward-compatible with existing OpenRefine workflows).

Example request body:
```json
{
  "query": "Paris",
  "mode": "fuzzy",
  "cluster_threshold": 0.85
}
```

Example grouped response (additional to the flat `hits` list):
```json
{
  "clusters": [
    {
      "cluster_id": "c_abc123",
      "representative": { "place_id": "gn:2988507", "title": "Paris", ... },
      "members": [
        { "place_id": "gn:2988507", ... },
        { "place_id": "wd:Q90", ... },
        { "place_id": "osm:n12345", ... }
      ],
      "score": 0.95
    },
    ...
  ],
  "hits": [ ... ]
}
```

The flat `hits` list is always present for backward compatibility. The `clusters` list is populated only when `cluster_threshold` is set.

**Removed parameter: `group_by_cluster`** (`bool`)

The previous boolean toggle is removed. Users should migrate to `cluster_threshold` which provides the same functionality (use `cluster_threshold: 0.85` as equivalent to the old `group_by_cluster: true`) with the additional ability to control sensitivity.

### 5.2 Django thin proxy changes (`api/crc_client.py`)

The Django thin proxy forwards search and reconciliation requests to the CRC gateway. Required changes:

1. **Stop sending `group_by_cluster`** — this parameter is removed from the gateway API.
2. **Pass through `cluster_threshold`** — when the Django search form or API consumer sets `cluster_threshold`, forward it to the gateway.
3. **Pass through new response fields** — the proxy must not strip `edges`, `phon_emb`, `h3`, `h3_cover`, `temporal_range`, `baseline_cluster_id`, `query_emb`, or other new fields from the gateway response. In particular, pass through the response-level metadata: `toponym_stoplist` (generic name tokens for synthetic edge filtering), and `clustering_params` (calibrated defaults for `θ_bridge`, `θ_query`, `θ_synth`, `θ_synth_structural`, `τ_name`, `τ_link`, default facet weights).
4. **Pass through scope tokens** — when the request originates from an authenticated contributor with pending datasets, attach the contributor's pending `dataset_ids` to the request as scope tokens (Part VII). The gateway uses these to scope-filter the discovery query and the edges array.

### 5.3 `/suggest` API extension for the Gazetteers list

The Gazetteers offcanvas (§1.4) is populated from an extended `/suggest` API. This endpoint returns the unified list of gazetteers (Authorities, Datasets, and Collections) that the authenticated user can see, with the metadata required for the offcanvas's coverage and ownership filters:

- `id`, `name`, `description`, `namespace` (or equivalent identifier) — basic identification.
- `class` — one of `authority`, `dataset`, `collection` (preserved for provenance display, not surfaced as a partition in the UI).
- `owner_user_id` — for the "My gazetteers" toggle (§1.4.2). Public/system gazetteers have a sentinel value (e.g. `null` or a system user id).
- `record_count` — for display in the list.
- `h3_coverage` — the precomputed compacted H3 cell set for spatial-coverage filtering (§1.4.1). May be cached client-side and reused across queries.
- `temporal_extent` — `[start_year, end_year]` for temporal-coverage filtering.
- `status` — for contributors' pending datasets, one of `draft`, `submitted`, `rejected`, `published`. Public gazetteers always report `published`.

The endpoint accepts an optional authenticated context (so a logged-in contributor sees their pending gazetteers) and an optional area/period filter to support server-side pre-filtering for very large gazetteer counts; for typical counts the client filters in JS using the returned `h3_coverage` and `temporal_extent`.

This endpoint replaces or supplements the current authority-list and dataset-browse endpoints. Implications for the architectural plan are noted in Appendix E.

---

## Part VI — The Contributor's Workflow

### 6.1 Bringing a dataset to WHG

A historian, an archaeological project, a library digital-humanities team, or any other party with a structured collection of place data can contribute it to WHG. The platform's value to the contributor is twofold: their data joins a corpus where it can be discovered, cited, and built upon by other researchers, and their data benefits from WHG's clustering machinery, which surfaces co-references between their records and the existing corpus that the contributor might not have known about.

The contribution begins with an upload. The contributor provides their dataset in one of the supported formats (Linked Places Format, CSV with appropriate column conventions, or whatever ingestion paths the platform accepts), along with dataset-level metadata describing what the dataset is, what citation accompanies it, what license terms apply, and who the contributors are. The platform validates the upload, normalises the records into its internal schema, and admits them to the contributor's working scope as a pending dataset.

From this moment, the contributor sees their records on the platform but no other user does. The records appear in search results when the contributor searches, are subject to the same dynamic clustering as public records, and can be linked, split, or annotated using the same propose-link and propose-distinct affordances available to all users. To other researchers using the platform, the contributor's records are invisible: the public corpus continues as if the dataset did not yet exist.

### 6.2 Reconciling the dataset

The contributor's principal task during this private period is reconciliation: examining each of their records in the platform's view, seeing how the platform has clustered it (if at all) with existing public records, and making the assertions necessary to bring the dataset into proper relationship with the corpus.

For many records, reconciliation is automatic. Where the contributor's record carries an authority link (a Wikidata QID, a TGN URI, a GeoNames ID), or shares a sufficiently distinctive name and spatial footprint with an existing record, the platform's automatic clustering groups them together without requiring contributor action. The contributor sees the cluster on the search page, sees their record listed alongside the public records, and moves on. The substantial majority of records in a typical dataset fall into this category.

For some records, reconciliation requires judgement. The platform may have clustered the contributor's record with a public record that the contributor recognises as a different place — perhaps two villages share a name, and the platform has chosen the wrong one — in which case the contributor uses the propose-distinct affordance to assert separation. Conversely, the platform may have left the contributor's record unclustered when it should have been grouped with a public record carrying a different name, and the contributor uses the propose-link affordance to assert co-reference. In a few cases the platform may produce a cluster that needs only minor adjustment (one member is correct, another is not), which the contributor handles with a propose-distinct on the wrongly-included record.

Throughout this process, the contributor uses all of the search page's standard affordances. The threshold slider lets them see how the platform clusters at different settings, useful when they are trying to understand whether a borderline case has been correctly resolved. Phase 2 expansion is particularly valuable: a contributor's record carrying a name not shared with any public record can be tested by expanding from spatially or temporally adjacent records, surfacing candidates the discovery step would have missed. Cluster enrichment provides additional context (Wikipedia summaries, Wikidata properties) that helps the contributor decide whether two records refer to the same place.

The contributor's assertions during this period are recorded in the platform but scoped to their working set: visible to the contributor, visible to staff editors performing review, invisible to other researchers. The realtime-effect property that ordinary contributor assertions enjoy (the assertion takes effect on the next query for everyone) is qualified during the pending period: the assertion takes effect immediately for the contributor and editors, but not for other users, until the dataset is published.

### 6.3 The preview view

At any point during reconciliation, the contributor can switch from the working view to the **preview view**. The working view shows the contributor's dataset as it currently is: pending records, pending assertions, the contributor's unfinished reconciliation reflected in cluster groupings that incorporate their assertions in real time. The preview view shows the same records as they will appear to other users *once published*: the same pending assertions are now treated as active, the records are treated as public, and the contributor sees what a researcher arriving at WHG after publication would see when they search for places represented in the dataset.

The toggle between working and preview is the contributor's principal self-check tool. It answers the question "does my work produce the result I intend?" — in working view they see what they have done; in preview view they see what the platform will do for others on the basis of that work. Discrepancies between the two views typically indicate residual reconciliation work: a cluster that looks right in working view but wrong in preview view often reflects a contributor assertion that was made but not yet committed in the contributor's mental model, and the discrepancy prompts review.

The preview view does not change the dataset's status. The contributor remains in the pending scope; toggling to preview is purely a display affordance. They can switch back and forth freely, can make assertions while in either view, and can submit the dataset for review from either.

### 6.4 Submitting for editorial review

When the contributor judges that they have completed reconciliation — every record examined, every cluster reviewed, every assertion made — they submit the dataset for editorial review. The submission is a deliberate action, not an automatic transition: the contributor decides when the work is ready, and pressing the Submit affordance freezes the dataset for editorial consideration.

While the dataset is under editorial review, the contributor can still see it in their working view but cannot make further assertions or modifications without first withdrawing the submission. This freeze ensures that the editor reviews a stable state and that the dataset the editor accepts is exactly the dataset the contributor intended to submit. Withdrawal is unilateral on the contributor's part: they can pull the dataset back from review at any time, make further changes, and resubmit when ready.

The editor reviews the dataset using the same UI the contributor used, with their scope set to include the pending dataset and editorial controls visible. They see the contributor's records, the contributor's assertions, the resulting cluster structure, and the same preview view the contributor has. They can examine the dataset's content, the quality of its assertions, the reasonableness of its co-references against the public corpus.

The editor's decision is binary: accept or reject. Acceptance publishes the dataset wholesale: every record becomes public, every contributor assertion transitions from pending to active and is forwarded to the public hard-links store, and the dataset becomes visible to all users on the next query they issue. Rejection returns the dataset to the contributor for revision, accompanied by editor notes explaining the issues that need addressing. There is no partial acceptance: a dataset is either ready for publication or it is not, and the editor's role is to make that determination.

The binary acceptance model has two design rationales. First, it simplifies the workflow's state machine: a dataset is in one of a small number of states (draft, submitted, accepted, rejected) without intermediate "partially accepted" states that would complicate both the contributor's mental model and the editor's review. Second, it places appropriate pressure on the contributor to do thorough work before submission: knowing that submission is a commitment rather than the start of a back-and-forth, the contributor is incentivised to use the preview view conscientiously, examine borderline cases carefully, and address issues during the private period rather than relying on editorial filtering. The cost is that an editor finding a single problematic assertion in an otherwise excellent dataset must reject and ask for revision rather than fixing the issue and accepting the rest, but this cost is usually small (the contributor's revision is typically focused and fast) and is more than offset by the simplicity of the model.

### 6.5 After publication

On editorial acceptance, the contributor's name appears in the platform's provenance for any cluster their dataset's assertions affected. Other researchers searching for places in the contributed dataset see the new records alongside the public records they have always seen, with appropriate provenance indicators showing that the records are recent and identifying the contributor. The contributor can continue to refine their assertions after publication, but those refinements are now ordinary contributor assertions with realtime public effect, not pending assertions awaiting editorial gate. The dataset has joined the corpus and is treated like any other contribution to it.

A contributor who wishes to retire a published dataset, or to make substantial revisions that would amount to republication, works with staff to do so; the platform preserves the citation history and the original published state for any external resources that may have referenced the dataset, and revisions are versioned rather than overwriting silently.

---

## Part VII — Visibility Scoping

The contribution workflow rests on a visibility-scoping mechanism that determines which records and assertions are included in any given request. This part specifies how that mechanism works at each stage of the request pipeline.

### 7.1 The scope concept

Every request to the search infrastructure carries an implicit or explicit **scope**, indicating which datasets and which assertions are visible. The default scope for an unauthenticated user, or for an authenticated user with no pending datasets, is the **public scope**: only published records and active assertions are visible.

Authenticated contributors with pending datasets have a scope that includes the public corpus *and* their pending datasets and pending assertions. They see the world as it would appear if their pending work were already published, which is the working view. The preview view applies a similar scope but with their own pending content treated as if it were published — the same records and assertions, but considered through the lens of "what others will see" rather than "what I am currently working on."

Staff editors have a configurable scope. By default it is the public scope, matching what an ordinary researcher sees. When reviewing a specific pending dataset, the editor sets their scope to include that dataset, gaining the same view of the platform that the contributor has. They can return to the public scope at any time without leaving the review session, useful for comparing the dataset's effect on clustering against the unaltered public state.

### 7.2 Scope filtering through the request pipeline

Scope is threaded through every stage of the request pipeline that touches potentially-private data:

**Discovery** filters the toponyms index to records that are scope-visible. A pending dataset's records carry a `dataset_status: pending` field and a `dataset_id` field; the discovery query includes a filter clause that admits only records whose status is `published` or whose `dataset_id` is in the set of pending datasets included in the current scope. This filter applies at the Symphonym-search level, so off-scope records are never retrieved at all rather than being retrieved and filtered after the fact.

**Hard-link expansion** consults two stores: the public Pitt SQLite (active assertions) and the DO PostgreSQL `contributor_attestations` table for pending assertions. Active assertions are included in expansion regardless of scope. Pending assertions are included only if their associated dataset is in the current scope. The two stores are queried together at the Django side, with results merged before the gateway expansion request is issued, or queried sequentially with appropriate filtering.

**Phase 2 expansion** filters the toponym union to scope-visible records. A contributor in working scope generates a union including names from their pending records as well as from public records; an off-scope user generating Phase 2 expansion for the same query produces a union from public records only. The expansion search itself is scope-filtered identically to the original discovery search, so off-scope candidates are never returned.

**Pair scoring** operates over the scope-filtered hits. Where pending assertions affect clustering for an in-scope contributor, the scoring pass treats them with the same weights as active assertions; the Union-Find pass treats them with the same unconditional-union and hard-split semantics. The contributor sees the platform's clustering apparatus operating over their work as it will operate in production once published.

**The cluster representation returned to the client** is similarly scope-filtered. Cluster members are only those visible in the current scope; cluster provenance includes pending assertions only if they are in scope; the per-hit `via_hard_link` flag's relations array filters out off-scope assertions. An off-scope user who happens to receive a response that touches a pending dataset's cluster (for instance, because a public record's hard-link expansion would have pulled in a pending record that is invisible to them) sees no trace of the pending content: the public record clusters as it would absent the pending dataset entirely, with no indication that off-scope content exists.

### 7.3 The preview view as a scope variant

The preview view, available to contributors and editors during review, is implemented as a small variation on the working scope. Where the working view treats pending content as pending (visible to the in-scope user but flagged as pending for clustering purposes), the preview view treats the same pending content *as if it were published*: assertions take their full active weight, records are presented without "pending" indicators, the cluster representation is what other users will see post-publication.

The contributor toggles between working and preview using a control on the search page (described in §8.4 below). The toggle changes the request scope on the next query; cached cluster state from the previous view is discarded and the request is re-issued with the new scope flag. The visual difference between the two views is typically subtle but meaningful: a propose-link assertion the contributor has just made shows up immediately in working view (the contributor's pending state has changed), and in preview view the assertion is treated as having taken effect on the public corpus, allowing the contributor to confirm that their assertion produces the cluster they intended.

### 7.4 Implementation notes

The scope filter at the discovery level is a small ES filter clause appended to every Symphonym/BM25 search. The Django request handler attaches the contributor's pending dataset_ids to the request based on authentication, and the gateway includes them as scope tokens in the ES query. The performance cost is negligible: ES filter clauses on indexed keyword fields are handled efficiently, and the typical contributor has a small number of pending datasets (often just one).

The scope filter on hard-link assertions requires the Django side to query DO PostgreSQL for any pending assertions involving the result set, in addition to the gateway's lookup against Pitt SQLite for active assertions. The Django side merges both into the hard-link payload sent forward into the clustering pass. This adds a small DO-side query per request involving a contributor in pending scope, which is modest overhead given that ordinary public-scope queries do not pay this cost (the Django side simply skips the pending lookup for users not associated with any pending dataset).

The pending-assertions table on DO PostgreSQL extends the existing `contributor_attestations` schema with two additional fields: `dataset_id` (which pending dataset the assertion is associated with, NULL for assertions made during ordinary research outside of any dataset context) and a `status` value of `pending` (in addition to `active`, `revoked`, and `superseded`). On dataset publication, all `status = 'pending'` assertions for the published dataset transition to `status = 'active'` in a single transaction, and are forwarded to Pitt SQLite as a batch.

Scope leakage is the principal correctness concern. Every code path that returns content to a client must apply scope filtering, and the testing strategy must include explicit verification that off-scope content does not leak. The simplest verification is a test suite that issues representative queries from off-scope users and asserts that no pending content appears in any field of the response. Adding such tests as part of the implementation, and running them on every change to the request pipeline, gives reasonable assurance against regression.

---

## Part VIII — Contributor UI Affordances

The contributor uses the same Atlas page as ordinary researchers, with additional affordances for managing their pending dataset. This part specifies those affordances. The "My gazetteers" toggle on the Gazetteers offcanvas (§1.4.2) is the principal entry point: selecting a pending dataset there puts the contributor into the working scope for that dataset.

### 8.1 Dataset management panel

A "My datasets" panel (reachable via the Gazetteers offcanvas's "My gazetteers" filter and from a top-level link when an authenticated contributor has any datasets, pending or published) shows the contributor's datasets in a list. Each entry shows:

- The dataset name and a brief description.
- The dataset status: draft (newly uploaded, never submitted), submitted (under editor review), rejected (returned for revision, with editor notes), or published.
- The record count and the count of records the contributor has examined (per the lightweight definition of "examined" used elsewhere: any record the contributor has interacted with, either by making an assertion or by clicking through to its detail).
- The count of pending assertions: contributor links and contributor distincts associated with the dataset.
- The last-modified date, used to compute the retention deadline (§10.1).

Selecting a dataset puts the contributor into the working scope for that dataset, returning them to the Atlas page with their dataset visible. They can then proceed with reconciliation work.

### 8.2 Dataset upload and metadata

Uploading a new dataset uses an existing pathway broadly consistent with v3.2: the contributor uploads their data file (LPF, CSV, or whichever format they prefer among the platform's supported ingestion formats), provides dataset metadata (name, description, citation, license, contributor identification), and submits. The platform validates and normalises, reports any validation errors back to the contributor for correction, and admits the dataset to the contributor's working scope on successful processing.

Dataset metadata is collected once at upload and can be edited subsequently from the dataset management panel. Most metadata fields are not visible to other users until publication; some (for instance, the citation) become part of the platform's record permanently and cannot be changed without staff intervention.

The validation step at upload is purely format-level: the file parses correctly, the records have required fields, the geographic coordinates are within valid ranges, the place_ids are well-formed. Semantic validation — whether the records describe places sensibly, whether their assertions are accurate — is the contributor's task during reconciliation and the editor's task during review.

### 8.3 Reconciliation progress indicators

Within the Atlas page, when the contributor is operating in the working scope for a specific dataset, the page shows reconciliation progress indicators that help the contributor navigate their task:

- A persistent banner or sidebar element indicating the current scope ("Working on dataset: My Welsh Place Names; X of Y records examined").
- A "show unexamined records" filter that restricts the search to records in the dataset that the contributor has not yet examined, useful for systematic walk-through.
- A "show records with pending assertions" filter that restricts the search to records the contributor has linked or distinguished, useful for review before submission.
- A "show only my dataset" toggle, restricting all searches to the contributor's records (useful when the contributor wants to inspect their own work without the public corpus interleaving). This is functionally equivalent to switching the Gazetteers offcanvas to Explore mode with the dataset selected.

The "examined" status is updated as the contributor interacts with records. Clicking to expand a cluster that contains the record marks the record as examined (the contributor has at least seen it in context). Making an assertion involving the record marks it examined a fortiori. The status is informational rather than load-bearing: the editor's review is not gated on whether all records have been marked examined, but the contributor's submission affordance shows a warning if many records remain unexamined ("X of Y records have not been examined; submit anyway?") to reduce the risk of inadvertent under-curation.

### 8.4 Working/preview toggle

A small toggle, prominently placed on the Atlas page header when the contributor is in working scope, switches between working view and preview view. The toggle is labelled clearly ("Working view / Preview as published") and is unambiguous about what each mode shows.

In working view, pending records are flagged with a subtle visual indicator (a small "pending" badge, or a coloured border) so the contributor can distinguish them from public records. Clusters whose membership depends on pending assertions are similarly flagged. The contributor sees their work in progress.

In preview view, pending records are flagged differently (or not at all, depending on design preference) — they appear as the public records they will become — and clusters are formed as they will be once the dataset is published. Pending assertions are shown without their pending indicator. The contributor sees the future state.

The toggle is one of the affordances most distinctive to the contribution workflow, and its design deserves attention. A contributor who never uses the preview view risks submitting work whose published behaviour they have not actually verified; a contributor who uses it conscientiously can iterate confidently. The platform should encourage preview use without making it intrusive: perhaps a brief tutorial on first dataset upload, perhaps a gentle reminder before submission ("Have you reviewed your dataset in preview view?"), perhaps an inline tip near the toggle on first encounter.

### 8.5 Submission affordance

When the contributor judges their reconciliation complete, a "Submit for review" affordance accessible from the dataset management panel and from the Atlas page header initiates the submission workflow. The affordance opens a confirmation dialog summarising:

- The number of records in the dataset.
- The number of pending contributor assertions that will be transitioned to active on acceptance.
- The number of records remaining unexamined (with a soft warning if this count is non-zero, but not a hard block).
- An optional submission note field, where the contributor can leave context for the editor (a summary of the dataset's scope, any unusual decisions, any specific points the contributor would like the editor to verify).

Confirming the submission freezes the dataset: pending assertions can no longer be added or revoked, records cannot be modified, the dataset metadata cannot be edited (with limited exceptions for typos and similar minor corrections). The contributor sees the dataset's status update to "submitted" and receives confirmation.

If the contributor needs to make further changes before the editor reviews, they can withdraw the submission via a "Withdraw from review" affordance on the dataset management panel. Withdrawal returns the dataset to draft state, unfreezing the assertions and records, and the contributor can resubmit when ready. The editor is notified of the withdrawal so they do not begin reviewing a dataset that the contributor is actively revising.

### 8.6 Post-rejection revision

If the editor rejects the dataset, the contributor sees the dataset's status change to "rejected" and receives the editor's notes via the platform's notification system (and optionally email, depending on the contributor's preferences). The dataset returns to working state — the contributor can again add or revoke assertions, modify records (within the limits of what the platform permits), and address the editor's feedback.

The editor's notes are preserved on the dataset's history page so the contributor can refer back to them throughout their revision. When the contributor is ready to resubmit, they use the same Submit for review affordance, possibly with a revision note describing how they addressed the editor's feedback. The editor reviewing the resubmitted dataset can see the rejection-and-resubmission history and the contributor's revision notes, providing context for their decision on the second pass.

There is no explicit cap on the number of submission rounds. A dataset that goes back and forth several times eventually either converges to acceptance or is withdrawn by the contributor or expires under the retention policy.

---

## Part IX — Editorial Review

The staff editor's role in the contribution workflow is to make the binary accept/reject decision on submitted datasets. This part specifies the editor's interface, their evaluation criteria insofar as they affect the platform's design, and the mechanics of the acceptance and rejection actions.

### 9.1 Editor's review interface

When an editor opens a submitted dataset for review, the platform sets their scope to include the dataset and presents them with the Atlas page in preview view by default. The editor sees the dataset as it will appear once published: the contributor's pending records as public records, the contributor's pending assertions as active, the resulting cluster structure as it will be in production. Toggling to working view shows the editor what the contributor sees during their reconciliation work.

In addition to the standard Atlas page affordances, editorial controls appear:

- A dataset summary panel showing record counts, assertion counts, contributor identification, dataset metadata, and submission notes if the contributor provided any.
- An assertion history view showing the full timeline of contributor activity on the dataset: when each assertion was made, when any were revoked, any prior submission and rejection cycles. This view is read-only during review.
- The Accept and Reject affordances themselves, prominently placed but with confirmation dialogs to avoid inadvertent decisions.

The editor uses the Atlas page exactly as the contributor did: searching for representative places in the dataset, examining clusters, comparing the contributor's assertions against the editor's own knowledge or against external authorities. They can switch to working view to see exactly what the contributor was looking at when making their assertions, useful for understanding why a particular assertion was made.

The editor can also issue queries that go beyond the dataset's content, comparing the dataset against neighbouring public records, against records from other recent contributions, or against any other context the editor judges relevant. The dataset is part of the editor's working view but does not constrain what they can see.

### 9.2 Editorial criteria

The platform does not enforce specific editorial criteria — those are matters for WHG's editorial policy and team practice — but the design accommodates several kinds of review activity that any editorial process is likely to involve:

- **Verification of assertions**: spot-checking contributor links and distincts against the same evidence the platform would use, looking for assertions that are clearly wrong or unsupported.
- **Coverage assessment**: ensuring the contributor has examined a reasonable fraction of their dataset and not just the easy cases.
- **Quality of justification**: reviewing the contributor's justification text on assertions where it is provided, looking for signals that the contributor understood what they were doing.
- **Consistency with authority data**: where the contributor's assertions disagree with authority assertions, evaluating whether the disagreement is principled (the contributor knows something the authorities do not) or accidental (the contributor missed an authority assertion that contradicts theirs).
- **Dataset-level appropriateness**: the dataset itself is appropriate for inclusion in WHG (within scope, properly licensed, ethically sourced).

The editor's review may involve consulting other staff, external experts, or the contributor for clarification. The platform supports this through the editor's notification and messaging mechanisms, which allow back-channel communication during review without interrupting the formal accept/reject decision.

### 9.3 The acceptance action

Acceptance is a single deliberate action with substantial effect. The editor confirms the acceptance through a dialog that summarises what is about to happen:

- The dataset transitions to status "published".
- All `status = 'pending'` contributor assertions for the dataset transition to `status = 'active'` in DO PostgreSQL.
- The newly-active assertions are forwarded to Pitt SQLite as a batch, becoming part of the public hard-links overlay.
- The dataset's records become visible to all users on the next query they issue.
- The contributor is notified of acceptance.
- The dataset's publication date and the accepting editor's identity are recorded in the dataset's history.

The transitions are performed as a single transaction at the DO PostgreSQL level: either all assertions transition successfully, or none do, with rollback on any failure. The forwarding to Pitt is done as a follow-up action; if it fails, the transitions on DO are still durable, and a reconciliation pass (per the architectural plan's §17g) will eventually bring Pitt into sync with DO. The user-facing consequence of acceptance is therefore robust: even a transient Pitt-side failure does not roll back the publication; it just delays the public effect briefly while reconciliation completes.

After acceptance, the dataset is part of the public corpus. Further contributor assertions about its records are made through the ordinary research workflow (with realtime public effect), not through the contribution workflow. The contribution workflow's review gate has done its work.

### 9.4 The rejection action

Rejection returns the dataset to the contributor for revision. The editor's rejection action requires editor notes — a free-text field where the editor explains the issues they have found and what the contributor should address. The notes are not optional: a rejection without notes is not a useful rejection, and the platform enforces this at the affordance level.

On rejection:

- The dataset transitions to status "rejected".
- The contributor's pending assertions remain pending (they are not revoked; they remain associated with the dataset and will become active on subsequent acceptance).
- The dataset's records remain pending, visible to the contributor and editors but not to other users.
- The editor's notes are attached to the dataset's history.
- The contributor is notified, with the editor's notes included in the notification.

The contributor returns to the dataset, addresses the issues, and resubmits when ready. The editor's notes are preserved on the dataset's history page and visible to the contributor throughout their revision, and to the next editor reviewing the resubmission.

### 9.5 Editorial workload management

A small set of editorial features support practical workload management:

- An editorial queue showing all submitted datasets awaiting review, with metadata to help editors prioritise (submission date, dataset size, contributor history).
- An assignment mechanism so multiple editors can work without duplicating effort on the same dataset.
- A handoff mechanism so an editor who begins review can pass it to a colleague (with notes) if their judgement turns out to be insufficient for the dataset's content.
- A cross-reference to the platform's contributor history so editors can see whether the contributor has had datasets accepted or rejected previously, useful context for evaluating the present submission.

These are workflow conveniences rather than structural design points, and they should be implemented to suit the editorial team's practice rather than imposed by the platform's design.

---

## Part X — Retention, Migration, and Open Questions

### 10.1 Retention policy for pending datasets

A pending dataset that sits without contributor edits for one year is deleted. "Without contributor edits" means: no new assertions made, no existing assertions revoked or revised, no records modified, no submission attempt, no withdrawal. A dataset that the contributor visits but does not modify still counts as inactive; passive viewing does not extend the retention window.

The retention timer starts from the date of last modification, not the date of upload. Active reconciliation work continually resets the timer. A dataset that the contributor works on regularly never approaches the deletion threshold; a dataset that the contributor uploads, makes a few assertions on, and then abandons does.

At the eleven-month mark, the platform sends a notification to the contributor: "Your dataset 'X' has been inactive for eleven months. It will be deleted on [date] unless you resume work or download a copy. To preserve the dataset, log in and either continue your reconciliation, submit it for review, or export the data." The notification is sent through the platform's notification system and (if the contributor has opted in) by email. A second notification is sent at the twelve-month deletion date itself, confirming the deletion has occurred.

Deletion is total: records are removed from the toponyms index, pending assertions are deleted from DO PostgreSQL, dataset metadata is deleted, and the dataset is removed from the contributor's dataset management panel. The contributor receives no recoverable copy from the platform after deletion; their original upload (which they presumably retain) is the only source of the dataset's content. A retention policy of this severity is appropriate for pending content (which the platform never publicly committed to retaining anyway), but the contributor should be unambiguously warned in advance.

The retention policy applies only to pending datasets. Submitted datasets under editor review are not subject to it (the timer pauses while the dataset is in submitted state); rejected datasets resume the timer from the rejection date, since the ball is again in the contributor's court. Published datasets are not subject to retention at all; their permanence is the point of contribution.

### 10.2 v3.2 legacy reconciliation flagging

Earlier drafts of this section described a one-time **migration** of v3.2 accessioned datasets and their reconciliation links into a new store. That framing has been retired. Per the backend execution plan (`plan-ingestionRebuild.execution.md`, items 8 and 11; Batch 13b), **the Django-side DO PostgreSQL database remains the canonical source of truth for every contributor gazetteer — both legacy v3.2 accessions and any future contributions.** There is no payload migration: the indexing pipeline pulls contributor Datasets and Collections from DO on every run via `authorities/whg-places.py` and treats them as `whg`-namespaced gazetteers (one numerical sub-namespace per Dataset/Collection, `whg:<dataset_id>:<entity_id>`), like any other authority.

The only one-time legacy work is a narrow data-update on DO PostgreSQL itself:

- **Schema extension.** Add `legacy_v3_2 BOOLEAN DEFAULT false` to `contributor_attestations`.
- **Flag historical reconciliation links.** Update every existing v3.2-era reconciliation link in `contributor_attestations` to carry `legacy_v3_2 = true`.
- **Propagate the flag through the harvest.** The Pitt-side hard-link harvest (`contributor_replay.py`) reads `legacy_v3_2` from DO and appends a `legacy_v3_2` suffix to `source_id` (`contributor:<user_id>:legacy_v3_2`) so downstream consumers can filter on legacy provenance.
- **Preserve metadata in place.** Dataset description, citation, license, contributor identification, and accession history remain in their existing DO tables. They are not re-mapped onto the new submission/review history (legacy accessioning was a different process; representing it as a submission-and-acceptance under the new model would misrepresent it).

The `legacy_v3_2` flag distinguishes legacy reconciliation links from new contributor assertions made through the redesigned workflow. This matters for two reasons. First, legacy assertions may have been made under quality controls different from the new workflow's editorial review (for instance, a v3.2 reconciliation that was confirmed by the contributor only, without editorial verification), and downstream consumers may want to know this. Second, legacy assertions sometimes lack justification text or other metadata that the new workflow collects, and tools handling assertion provenance need to gracefully accommodate the absence.

In-progress v3.2 reconciliations are handled by the same DO-as-source-of-truth principle. Their `contributor_attestations` rows already carry whatever `status` is appropriate (`pending`, `active`); the new workflow simply continues to use those rows. Contributors with in-progress work see their datasets in the new dataset management panel and can resume reconciliation in the new UI without any data being moved.

There is consequently no migration script with a backlog of edge cases to triage. Any v3.2 records that are absent or malformed in DO are absent or malformed *now*, and are addressed by ordinary DO-side data-quality work rather than by a one-shot migration step.

### 10.3 Datasets intended for permanent privacy

A subtle use case that the v3.2 model arguably underserves is the personal working dataset: a researcher's collection of place data that they want to use within WHG's tools (for clustering, for enrichment, for exploration) but do not intend to publish. Such datasets exist (a researcher building a private working bibliography of place references in a manuscript they are studying, say), and forcing them through a publication-or-deletion pipeline is awkward.

The redesigned model can support this case cleanly with a small extension. A pending dataset acquires a `retention: pending` flag by default, indicating it is subject to the one-year retention policy. The contributor can change this to `retention: private_permanent`, removing the dataset from the retention-deletion sweep. Datasets so marked remain in pending scope indefinitely, visible only to the contributor, never published, never reviewed, and never deleted by automated retention policy. The contributor can use the platform's tools on their dataset as they like, and can still publish later (transitioning the dataset to `retention: pending` and then submitting for review) if they change their mind.

Private-permanent datasets impose a slightly different cost profile on the platform than pending-publication datasets: they accumulate rather than being collected in batches and then either published or deleted. A storage cap on private-permanent content per contributor is reasonable to prevent abuse (the platform is a research infrastructure, not unlimited cloud storage), with caps adjustable on request for legitimate use cases. This is operational policy rather than design: the design accommodates the use case; specific limits are calibrated by the WHG team to suit their resource budget.

### 10.4 Open questions

A few aspects of the design merit further discussion before implementation.

**Coverage of the "examined" indicator.** The lightweight definition of examined ("any record the contributor has interacted with") is convenient but may underrepresent the contributor's actual review effort, particularly if much of the contributor's work happens in the preview view rather than through explicit click-throughs. A complementary "explicitly reviewed" flag, contributor-set, would let conscientious contributors mark records as having received specific attention; this is offered as an option without making it a requirement.

**Editorial workflow for high-volume contributors.** A contributor who submits many datasets — perhaps a digital humanities centre running many small projects — places more demand on the editorial team than a one-off contributor. Whether the platform should support a "trusted contributor" status that streamlines or skips editorial review for established contributors is a policy question with implications for both editorial workload and quality control. The platform's design accommodates this (a per-contributor flag toggling the editorial gate) but the policy decision is for the WHG team.

**Reconciliation API contributions.** Programmatic reconciliation via the OpenRefine path produces contributor assertions just as interactive use does, but the OpenRefine workflow does not have the same gentle introduction to the platform's expectations as a contributor walking through the upload UI for the first time. A user reconciling thousands of records via OpenRefine could inadvertently produce many low-quality assertions if they have not understood the platform's evidence model. The design might offer a "reconciliation API contributions enter as pending and are subject to editorial review before becoming active" mode for users who prefer that workflow, distinct from an "expert mode" where reconciliation API contributions go active immediately. This is an open question both in terms of design (how does the user choose their mode?) and policy (which is the default?).

**Cross-dataset references during reconciliation.** A contributor uploading dataset B may want to reference records in their previously-uploaded dataset A. If A is published, this is straightforward (A's records are in the public corpus and behave like any other public records). If A is pending, the contributor can include both in their working scope and the records cross-reference cleanly. But this introduces a small complexity in the editorial review of B: the editor reviewing B may need to consider B's assertions against A's records, which means the editor's scope for reviewing B must include A. The platform supports this (the editor's scope is configurable to include any pending dataset), but the workflow conventions for this cross-dataset case are worth specifying. The simplest convention is that submitting B requires A to be either published or also submitted for review, so the editor reviewing B can do so against a stable A; this is a small constraint and easy to enforce.

**Versioning of published datasets.** A contributor may want to update a previously-published dataset with corrections or additions. Whether updates flow through the contribution workflow as new submissions (with the updates treated as a fresh dataset that supersedes the previous version), or as in-place modifications to the published dataset (with appropriate versioning of the record state), is a design question with implications for citation stability and the platform's relationship with downstream consumers that reference its content. The architectural plan does not currently specify versioning semantics for published datasets; this is worth resolving as part of the contribution workflow's specification rather than deferring.

---

## Conclusion

The redesigned platform specified above absorbs three previously-separate concerns — the Atlas-as-default UI reframing, dynamic clustering, and the contribution workflow — into one coherent interaction model. The contributor uploads their dataset, reconciles it interactively against the public corpus using the same affordances that ordinary researchers use in their search workflows, sees the platform's clustering machinery operating over their work in real time, switches between working and preview views to confirm their work produces the result they intend, and submits to a staff editor whose review is grounded in the same UI and the same evidence the contributor saw. On editorial acceptance the dataset joins the corpus; on rejection the contributor revises and resubmits; on inactivity the dataset is preserved for a year and then deleted with adequate warning.

The design's principal advantage over the v3.2 model is its conceptual unity: contribution, reconciliation, and ordinary research are facets of one interaction model rather than three distinct workflows; "Authorities", "Datasets", and "Collections" are facets of one concept (Gazetteers) rather than three; clustering is a continuous control rather than a binary toggle. The contributor learns one interface; the editor reviews using one interface; the researcher uses the same interface for ordinary work. The platform's hard-link store, its clustering machinery, and its provenance display all serve the unified model coherently.

The work to implement this lies in the Atlas terminology and navigation reframing (Part I), the gateway response shape and client-side clustering algorithm (Parts II–III), the Atlas page UI affordances for the general user (Part IV), the API surface (Part V), the visibility-scoping mechanism (Part VII), the contributor-specific UI affordances on top of the Atlas page (Part VIII), the editorial review interface (Part IX), and the retention and migration logic (Part X). Each is bounded and tractable; together they produce a platform that is more capable, more comprehensible, and more inviting to potential contributors than what WHG currently offers.

---

## Appendix A — Implementation Phases

The work is organised into nine phases that respect the dependency order between front-end changes and the backend changes specified in `plan-dynamicClustering.prompt.md` and `plan-ingestionRebuild.execution.md`. Phases 1 and 2 can begin in parallel; phases 3 and 4 depend on phase 2 delivering the new gateway payload and `/suggest` extension; phases 6–7 depend on phase 5's scope-filter foundation; phases 8 and 9 are largely independent and can run in parallel with the later phases.

### Phase 1 — UI foundation: Atlas reframing ✅ DONE

No backend dependency. All edits in `search/templates/search/atlas.html`, `whg/webpack/js/atlas.js`, the main-nav templates, and a few spatial-source registries.

1. ✅ DONE — Renamed the offcanvas: `id="sources_offcanvas"` → `id="gazetteers_offcanvas"` and `id="sources_offcanvas_label"` → `id="gazetteers_offcanvas_label"` in `atlas.html`; offcanvas title "Data Sources" → "Gazetteers"; trigger button `#open_sources_modal` → `#open_gazetteers_modal` with updated `data-bs-target`. Selectors updated in `atlas.js`, `atlasTour.js`, and `atlas.css` (§1.2, §1.4, §4.8).
2. ✅ DONE — Relabelled the "Sources" button → "Gazetteers" and the "Toponyms" mode button → "Places" in `atlas.html`; updated welcome-text references and the tour popover copy in `atlasTour.js`. Tooltips updated to convey the new dual function: the **Gazetteers** control button reads "Filter or Explore Gazetteers"; the **Places** mode button reads "Search for Place Names or Gazetteers within selected Areas". The Gazetteers icon is `fa-book-atlas` (replacing the earlier `fa-database`) on both the control button and the offcanvas title — propagated to the tour popover too. Internal identifiers (`data-search-mode="toponyms"`, `toponym-only-btn` CSS class) preserved as private API (§1.2).
3. ✅ DONE — Removed the `#atlas_clustering_toggle` "Group linked records" checkbox from `atlas.html` and its handlers from `atlas.js` (the change-event listener and the reset). Replaced the offcanvas content with an informational note pointing to the forthcoming Results-panel slider (§1.3, §4.1).
4. ✅ DONE — Added D-PLACE to the renamed Gazetteers offcanvas in `atlas.html` (unchecked by default) — moved here because D-PLACE contains only point data and is unsuitable as a spatial constraint (§4.8).
5. ✅ DONE — Rationalised the main navigation in `main/templates/main/base_webpack.html`:
   - Removed Search and Workbench.
   - Moved "Admin Dashboard" into the rightmost (user) dropdown, gated on `is_whg_admin`.
   - Moved "API" and "Volunteering" links into the "About" dropdown; deleted the "Data" dropdown entirely along with My Data, Published Datasets, and Published Collections (these will return as gazetteers under the "My gazetteers" toggle in Phase 4).
   - Removed the "New in v{APP_VERSION}" link.
   - Teaching remains a top-level nav link that navigates to `{% url 'teaching' %}`. (An earlier sketch routed Teaching through the existing `data-whg-modal` mechanism so it would load in an overlay; that change was reverted because the existing Teaching page is not yet authored to render cleanly inside a modal. Revisit when the Teaching page is rebuilt — see Phase 10.)

**Already completed in Phase 1's scope (record):**
- D-PLACE removed from `politySelector.js` (`POLITY_DATASETS` no longer lists `dplace`). ✅
- D-PLACE removed from `search/views.py` `available_sources`. ✅
- "OSM/OHM (Miscellaneous)" added to `regionSelector.js` (`NAMESPACE_OPTIONS` includes `osm_misc`); backend tile/query support pending. ✅

### Phase 2 — Backend integration surface

Depends on the gateway changes in `plan-dynamicClustering.prompt.md` (Appendix E.1) and the per-gazetteer pre-computation in `plan-ingestionRebuild.execution.md` (Appendix E.2).

1. Update the Django thin proxy `api/crc_client.py`:
   - Stop forwarding `group_by_cluster` to the gateway (§5.2 item 1).
   - Pass through `cluster_threshold` from request to gateway (§5.2 item 2).
   - Pass through new gateway response fields without stripping: `edges`, `h3`, `h3_cover`, `temporal_range`, `aat_ids`, `aat_depths`, `baseline_cluster_id`, `query_match` (with `phon_emb`), top-level `query_emb`, `clustering_params`, `toponym_stoplist` (§5.2 item 3).
2. Surface `/api/embed` (debounced) to the browser via the Django proxy for variant-name lookups (§4.4); the initial query's embedding is consumed directly from the response's `query_emb` field with no extra call.
3. Implement the extended `/suggest` API for the unified Gazetteers list (§5.3): `id`, `name`, `description`, `namespace`, `class`, `owner_user_id`, `record_count`, `h3_coverage`, `temporal_extent`, `status`. Coordinate with the ingestion plan's per-gazetteer H3 coverage and temporal-extent pre-computation (Appendix E.2).
4. Update the OpenRefine reconciliation contract (§5.1): document `cluster_threshold` and the new `clusters[]` shape; document removal of `group_by_cluster`.

### Phase 3 — Client-side dynamic clustering

Depends on Phase 2.

1. Implement `whg/webpack/js/clustering.js` as a self-contained, DOM-free module (§4.9):
   - `class UnionFind` (path compression, union by rank).
   - `clusterResults(hits, edges, theta, weights, queryScores, params)` covering Phase 0 (baseline bootstrap §3.4), Phase 1 (precomputed edges with Rules 1–2 §3.3, §3.8), Phase 2a (phonetic synthetic edges §3.9.1), Phase 2b (structural synthetic edges §3.9.2), Phase 3 (post-processing cluster-size split §3.6).
   - `reweightEdge`, `queryBridgeThreshold`, `phoneticSyntheticPass`, `structuralSyntheticPass`, `typesOverlap`, `isStoplistName`, `spatialBucket`, `cosineSimilarity`, `decodePhonEmb`.
   - Honour the θ = 1.0 bypass: short-circuit to all-singletons (§3.4).
   - Apply lazy `phon_emb` decode (§4.9).
2. Add the threshold slider at the top of the Results panel with ~100 ms debounced re-clustering and `sessionStorage` persistence (§4.2, §1.3).
3. Add the collapsible facet-emphasis section ("Similarity tuning") with per-facet weight sliders normalised to sum to 1.0; "Reset to defaults" reads from `clustering_params` in the response (§4.3).
4. Add the phonetic comparison input ("Compare name variant") with 300 ms debounced `/api/embed` calls and per-result proximity badges; use `query_emb` for the initial query (§4.4).
5. Render cluster cards: representative + count badge + aggregated names; expansion to member sub-cards; map cluster-bbox zoom over `repr_point`s; optional convex hull on hover for dense clusters (§4.5).
6. Update result-facet filters to operate over clustered results (cluster visible if any member passes; counts reflect unique clusters) (§4.6).
7. Replace the legacy feature-class checkboxes (`A`, `P`, `S`, …) with server-side AAT type-aggregation facets (§4.7).
8. Calibrate defaults from the server-provided `clustering_params` (`θ_bridge`, `θ_query`, `θ_synth`, `θ_synth_structural`, `τ_name`, `τ_link`, default weights); the client must not hard-code them (§3.9, §4.3).

### Phase 4 — Gazetteers offcanvas extension

The full implementation depends on Phase 2 item 3 (`/suggest` extension) and the per-gazetteer pre-computation called for in `plan-ingestionRebuild.execution.md`. The UI scaffolding has been **sketched** in `atlas.html` and `atlas.js` ahead of that backend work so colleagues can preview the intended interaction model; everything dependent on missing data is rendered disabled or inert and labelled accordingly.

The **standard authority list** is no longer hardcoded — it is rendered from `GazetteerRegistryEntry` rows (`entry_class='authority'`) populated by the inventory pipeline (or by the `0003_gazetteer_curatorial_fields` data-migration fallback that seeds the ten currently-known authorities so the page renders before the first inventory push lands). The migration uses `atomic = False` because applying it to an already-populated table fires PostgreSQL trigger events that block in-transaction `CREATE INDEX` for the new `core` field.

#### Sketched in atlas.html / atlas.js / atlas.css (record)

These pieces are visible on the Atlas page now but are not load-bearing — selecting them produces no functional change beyond the UI state described:

- **Filter | Explore mode toggle.** A `btn-group` at the top of the Gazetteers offcanvas with two buttons (Filter / Explore) wired to `setGazetteerMode(mode)` in `atlas.js`. Switching modes flips a `data-mode` attribute on the offcanvas body, swaps the help-text paragraph, shows/hides Explore-mode-only controls, and **physically swaps every authority input between `type="checkbox"` (Filter) and `type="radio"` with a shared `name="gazetteer_explore"` (Explore)** so the affordance matches the mode. **Selection state is persisted per mode** via `filterSelections` (Set of namespaces) and `exploreSelection` (single namespace) — flipping tabs saves the current mode's selection and restores the other mode's last-recorded selection rather than dropping it on the input-type swap. The type-swap and selection-restore are run as **two separate passes** (swap-and-clear, then apply saved selection) to avoid browser quirks where setting `.checked` in the same iteration that mutates `.type` can be silently dropped. The custom CSS gives both mode-toggle buttons a faint outline always; the active one gets a faint fill rather than a strong filled-button look.
- **Coverage-filter card** (`.gazetteer-card`). A tinted card beneath the help text with two enabled switches — "Hide gazetteers outside Area filter" and "Hide gazetteers outside Period filter". The switches operate; toggling either one on reveals an in-card stub note ("Coverage filtering is not yet implemented — the switch will activate once gazetteer-level `h3_coverage` and `temporal_extent` arrive via the extended `/suggest` API"). Toggling both off hides the note again.
- **My Gazetteers card** (Explore mode only). A second card matching the coverage-filter card's styling, holding a single switch for **My Gazetteers**. Hidden by default and revealed only when the user clicks the **Explore** tab. When on, it hides the standard gazetteer list and shows a placeholder list grouped into **Published** (`Welsh Place Names`, `Roman Britain Sites` as samples) and **Pending** (`Medieval Pilgrimage Routes` (draft), `Hanseatic Ports` (submitted) as samples). An italic note above the placeholder list flags it as illustrative; the real list will be populated from the extended `/suggest` API once `owner_user_id` and `status` are delivered. <br>**Sketch-period note**: the `user.is_authenticated` template guard around the toggle and placeholder list has been temporarily removed so colleagues without accounts can review the intent. The guard will be reintroduced when this is wired to real per-user data. (Documenting comments inside the template avoid using literal Django tag delimiters around `if`/`endif` because Django parses tags even inside HTML comments.)
- **Explore-mode capabilities note.** A small left-bordered note that appears only in Explore mode, explaining what signed-in users can do during exploration: add **Attestations** to any place when exploring any gazetteer; when exploring **their own** gazetteer, additionally add places, edit data and metadata, and adjust clustering — equivalent to the reconciliation/accessioning workflow in v3.2.
- **Per-gazetteer count placeholders.** Each gazetteer entry contains an empty `<span class="gazetteer-count" data-namespace="…">` slot adjacent to the label, ready to be populated from result-set aggregations once §4.8 is wired.
- **"Core" badge and default selection.** Backed by the new `core` field on `GazetteerRegistryEntry` (§1.4.3). The template's loop emits `checked` on the input only when `g.core`; a small "core" badge renders next to the label. Default-selected set is GeoNames, Wikidata, TGN. Replaces the previously hardcoded `checked` attributes.
- **Polygon-only tileset gating.** Backed by the new `tileset_polygon_only` field. `applyTilesetGating(mode)` (in `atlas.js`) disables OSM and OHM (and any other row carrying `data-tileset-polygon-only="1"`) in Explore mode with an explanatory tooltip; Filter mode keeps them selectable. Re-runs on every `setGazetteerMode` transition and on initial load. CSS class `.authority-item.disabled` provides the greyed-out treatment with `pointer-events: auto` so the tooltip still fires.
- **Specialist Gazetteers expansion.** The "WHG datasets" row is **relabelled** "Specialist Gazetteers" and behaves as an inline-expandable container (§1.4.4). Lazy-rendered children come from the `specialist_gazetteers` JSON script tag (a serialisation of `GazetteerRegistryEntry.objects.filter(entry_class='dataset', namespace='whg')`). Tri-state parent emits `whg` when fully checked and the explicit list of child IDs when indeterminate. Search box performs case-insensitive substring filtering. Standard|Itinerary|Network type-pill filter (Explore-only, **Standard active by default**) hides children by `data-gazetteer-type`; the convention is "all pills off → show all".
- **"Private" section in My Gazetteers placeholder list.** Three sections now: **Published**, **Private** (new — visible only to owner, never submitted for review), **Pending**.
- **Continue button.** A primary "← Continue" button at the bottom of the offcanvas-body, always visible across Filter / Explore / My Gazetteers states. The leftward arrow + left alignment signals "return to the map UI". Closes the offcanvas via Bootstrap's `data-bs-dismiss="offcanvas"`; current selections have already been mirrored into `filterState` by the change handlers, so no extra wiring is needed.
- **No explicit "open" action**. Selecting a gazetteer in Explore mode applies the filter via the existing change handler; the user dismisses the offcanvas with **Continue**, the close button, or by clicking the map.
- **Curatorial admin and inventory-push protection.** `api/admin.py::GazetteerRegistryEntryAdmin` registered (staff-only) with only `core`, `tileset_polygon_only`, `gazetteer_type` editable. `api/views_indexing.py::GazetteerInventoryView._upsert_one` carries an inline comment naming these fields as admin-managed and intentionally omitted from `defaults`, so re-pushes preserve curation.
- **Sitewide navbar "Gazetteers" entry** (§1.5). New `<li class="nav-item">` to the right of Atlas pointing at `?panel=gazetteers&gmode=explore`; the URL handler in `atlas.js` triggers native `.click()` on the existing Places button, the offcanvas trigger, and the Explore mode tab so all wired side-effects fire as if the user clicked manually.

The previous offcanvas-internal "Clustering" section (with its informational note pointing to the Results-panel slider) has been removed; the slider lives in the Results panel and does not need a placeholder header here.

#### Pending backend integration

When the backend pieces above land, the sketch is replaced/extended as follows:

1. Populate the **Specialist Gazetteers** child list and any future per-user "My Gazetteers" entries from the extended `/suggest` payload — supplementing (not replacing) the standard list, which is already server-driven from `GazetteerRegistryEntry` (§1.4, §1.4.4).
2. Promote the Filter | Explore tabbing from a sketch to a real interaction: in Filter mode the existing live-apply behaviour continues; in Explore mode, the radio-style single-select feeds a single-gazetteer Explorer entry point that loads the gazetteer's records as the primary content (§1.4).
3. Implement coverage filtering against `h3_coverage` and `temporal_extent`: gazetteers whose coverage does not intersect the active Area filter (or whose extent does not intersect the active period filter) are disabled and (default-hidden) hidden; a toggle reveals hidden entries (§1.4.1).
4. Replace the placeholder My Gazetteers list (Published / Private / Pending sections) with the real per-user list returned by `/suggest`, preserving the section grouping and the entry-point semantics into the contributor working scope (§1.4.2).
5. Wire the per-gazetteer count indicators from the result-set facet aggregations, reflecting the current (possibly clustered) result set (§4.8).
6. Attach backend semantics to the `gazetteer_type` field — the type-pill filter currently hides DOM rows by `data-gazetteer-type` but does not propagate the selection into the search request. Define how Itinerary and Network gazetteers differ from Standard at the search/discovery level, then either filter at the request layer or surface a different rendering for each type in Explorer view (§1.4.4).

### Phase 5 — Visibility scoping foundation

Depends on backend schema work in `plan-ingestionRebuild.execution.md`.

1. Extend the DO PostgreSQL `contributor_attestations` schema with `dataset_id` and `status: pending` (in addition to existing `active`, `revoked`, `superseded`) (§7.4).
2. Thread scope tokens (a contributor's pending `dataset_id`s) from Django request through `api/crc_client.py` to the gateway (§5.2 item 4, §7.4).
3. Apply scope filter clauses at every pipeline stage that touches potentially-private data:
   - Discovery filter on `dataset_status: published OR dataset_id ∈ scope` (§7.2).
   - Hard-link expansion: parallel lookup of pending assertions in DO PostgreSQL alongside the gateway's Pitt SQLite lookup, merged with scope filtering (§7.2).
   - Phase 2 expansion uses the same scope filter (§7.2).
   - Pair scoring treats in-scope pending assertions with the same weights as active assertions (§7.2).
   - Cluster representation: members and provenance filtered by scope (§7.2).
4. Implement the preview-view scope variant (pending content treated as if published) (§7.3).
5. Build a scope-leakage test suite that issues queries from off-scope users and asserts no pending content appears in any response field; run on every pipeline change (§7.4).

### Phase 6 — Contributor UI affordances

Depends on Phases 4 and 5.

1. Build the "My datasets" panel: name, description, status, examined-count, pending-assertion count, last-modified date / retention deadline (§8.1).
2. Selecting a dataset puts the contributor into the working scope for that dataset, returning to the Atlas page with the dataset visible (§8.1).
3. Add reconciliation progress indicators on the Atlas page when in working scope: scope banner, "show unexamined", "show with pending assertions", "show only my dataset" (Explore-mode shortcut) (§8.3).
4. Implement the Working / Preview-as-published toggle in the Atlas header (§8.4); cached cluster state from the prior view is discarded on toggle and the request is re-issued.
5. Implement the submission affordance: confirmation dialog summarising counts; optional submission note; soft warning on unexamined records; freeze semantics (§8.5).
6. Implement withdrawal during submitted state (§8.5) and post-rejection revision with editor-notes display on the dataset history page (§8.6).

### Phase 7 — Editorial review

Depends on Phase 6.

1. Build the editorial review interface: scope set to include the submitted dataset; preview-view by default; toggle to working view; dataset summary panel; read-only assertion history; Accept and Reject affordances (§9.1).
2. Implement Acceptance: single transaction on DO PostgreSQL flipping all `pending` → `active` for the dataset; batch-forward to Pitt SQLite as a follow-up (failure-tolerant — DO transitions are durable; Pitt sync is reconciled per the architectural plan's §17g) (§9.3).
3. Implement Rejection: required editor notes; pending assertions remain `pending`; records remain pending; contributor notified; notes persisted on the dataset history (§9.4).
4. Add editorial workload tools: review queue, assignment, handoff with notes, contributor-history cross-reference (§9.5) — calibrated to the editorial team's practice rather than designed in the platform.

### Phase 8 — Retention, legacy flagging, private-permanent

Depends on backend work in `plan-ingestionRebuild.execution.md` Batches 13b and 14a; runs in parallel with Phases 3–7.

1. **v3.2 legacy reconciliation flagging (no payload migration)** — DO PostgreSQL remains canonical for all contributor gazetteers (§10.2):
   - DO-side schema change: add `legacy_v3_2 BOOLEAN DEFAULT false` to `contributor_attestations`.
   - Data update: set `legacy_v3_2 = true` on every existing v3.2-era reconciliation link.
   - Pitt-side: `contributor_replay.py` reads the flag and appends a `:legacy_v3_2` suffix to `source_id` so downstream consumers can filter on legacy provenance.
   - Backend tracking: `plan-ingestionRebuild.execution.md` Batch 13b.
2. **Retention sweep for pending datasets** (§10.1, backend Batch 14a):
   - Scheduled job deletes pending datasets unmodified for one year; eleven-month notification to contributor; twelve-month deletion confirmation.
   - Exclusions: `submitted` (timer pauses), `rejected` (timer resumes from rejection date), `private_permanent` (excluded entirely), `published` (not subject to retention).
3. **Private-permanent datasets** (§10.3): `retention: private_permanent` flag on pending datasets; per-contributor storage cap (operational policy, adjustable on request); upgrade path to `retention: pending` then submission for review.

### Phase 9 — Cleanup

Independent of the rest; can run as soon as each prerequisite has shipped.

1. Remove `cluster_id` / `cluster_size` from the old search-hit rendering (the new cluster representation comes from the client-side Union-Find).
2. Remove `group_by_cluster` from the Django proxy and any downstream code that still consumes it.
3. Retire the legacy `search/templates/search/search.html` prototype page (§1.5) — once Phase 1 lands and Atlas is the default, this page has no remaining users.
4. Write the OpenRefine migration guide: `group_by_cluster` → `cluster_threshold` (§5.1).

### Phase 10 — Further considerations

Items deferred from earlier phases for separate scoping; to be revisited once the core platform is in place.

1. **Landing-page absorption into the Atlas welcome panel** (deferred from Phase 1, §1.6). Migrate the landing-page content into the `.atlas-welcome-title` / `.atlas-welcome-text` elements on the Atlas page so users arriving at the platform see Atlas directly with welcome content in place of an immediate query; retire the old landing page once parity is established. Deferred because it interacts with marketing copy, SEO, and any external links pointing at the current landing URL — all of which want their own review pass.
2. **Reconsider routing the Teaching link through the modal mechanism.** A Phase 1.5 sketch wired the Teaching nav item to `data-whg-modal="{% url 'teaching' %}"` so it would load in a Bootstrap overlay rather than navigate. The change was reverted because the existing Teaching page is a full-document template and does not render cleanly inside a modal. Worth re-evaluating when the Teaching page is rebuilt — if its content can be authored as a self-contained fragment (or split into a dedicated modal endpoint), the in-place modal entry-point is consistent with the §1.5 direction of keeping users on the Atlas page wherever practical.
3. Atlas full-screen mode.

### Open policy questions (resolve alongside Phase 6 and 7)

The following questions are flagged in §10.4 as design-adjacent but policy-driven; they should be resolved before the editorial workflow ships rather than deferred:

- "Trusted contributor" status that bypasses or streamlines editorial review (§10.4).
- Default mode for OpenRefine / programmatic reconciliation contributions: pending-with-review vs immediate-active (§10.4).
- Cross-dataset references during reconciliation: convention requiring dataset A to be published or also-submitted before dataset B can submit while referencing A (§10.4).
- Versioning of published datasets: in-place modification vs supersession by a fresh submission, with citation-stability implications (§10.4).

---

## Appendix B — Performance Characteristics

| Phase | Latency | Notes |
|-------|---------|-------|
| Server: full search pipeline | ~300 ms | Discovery + filtering + enrichment + neighbor expansion |
| Client: initial Union-Find clustering | <10 ms | ~500 nodes, ~2000 edges |
| Client: slider re-clustering | <5 ms | Re-apply threshold, no server round-trip |
| Client: phonetic embed request | ~50 ms | `GET /api/embed` round-trip |
| Client: phonetic re-scoring (all hits) | <5 ms | 500 × int8 dot product |
| Server: `/suggest` (Gazetteers list) | ~50 ms | Cached aggressively client-side; recomputed on dataset publish/upload |
| Server: scope-filtered search (contributor in working scope) | ~310 ms | +~10 ms over public scope for the DO pending-assertions lookup |

Total perceived latency: **~300 ms server + instant client interaction**.

---

## Appendix C — Failure Modes and Mitigations

### C.1 No server-side pagination for clusterable results

Client-side clustering requires the full result set and its edge subgraph in a single payload — traditional server-side pagination is fundamentally incompatible because co-referent places split across pages could never be clustered together (e.g. "Paris" from GeoNames on page 1, "Paris" from Wikidata on page 2).

The existing gateway returns all results in one response (`SearchRequest.size`, max 500) with no `page`/`offset` parameter. The clustering design preserves this: the entire clustering window (up to 500 results + up to 4000 edges) is delivered in a single payload. Any "pagination" is purely **client-side display pagination** — the browser holds all results and edges in memory, clusters them, and uses virtual scrolling or page controls to render subsets of the already-clustered list.

For queries producing more than 500 matches, the gateway returns the top 500 by discovery score. Matches beyond 500 are not clusterable but are summarised in the response metadata (total hit count, facet aggregations). If a user needs to explore beyond the clustering window, they should refine the query (add filters, narrow spatial bounds) rather than paginate.

If future requirements demand clustering over larger result sets, the server-side fallback path (`cluster_threshold` parameter) can cluster on the gateway and return pre-grouped results for any number of hits.

### C.2 Dense urban datasets (OSM-heavy)

OSM contributes many spatially proximate records with similar names (e.g. "Pharmacy" × 50 in a city). Mitigations:
- The offline pipeline's spatial proximity thresholds already limit candidate pairs.
- Type similarity separates "pharmacy" from "city" even when spatially co-located.
- The synthetic-edge stoplist guard (§3.9.1) prevents catastrophic merging on generic toponyms.

### C.3 Weak type/temporal data → over-merging

Many records (especially GeoNames) lack temporal data and have generic types. Mitigations:
- Baseline clusters at θ = 0.9 only merge near-certain matches.
- The default UI slider position should start high (e.g. 0.8), encouraging conservative grouping.
- Hard links (authority sameAs) bypass the threshold entirely.

### C.4 Edge incompleteness → under-clustering

The dominant recall failure mode: the offline graph misses edges for cross-lingual exonyms, sparse-name records, or type-misaligned authorities. Lowering θ cannot fix missing edges. Mitigations:
- Phonetic synthetic edges (§3.9.1) catch spatially co-located pairs with phonetic similarity.
- **Structural synthetic edges (§3.9.2)** catch pairs where phonetics fail entirely but structural signals (shared country code, namespace, baseline cluster) confirm co-reference. This is the cheapest and most reliable catch for the dominant failure mode.
- The query-bridge rule (§3.3 Rule 2) relaxes thresholds for query-relevant pairs.

### C.5 Scope leakage in the contribution workflow

Pending content visible to off-scope users would be a serious correctness failure. Mitigations:
- Scope is threaded through every stage of the request pipeline (§7.2).
- A test suite issuing representative queries from off-scope users asserts that no pending content appears in any field of the response (§7.4).
- Tests run on every change to the request pipeline.

---

## Appendix D — Dependencies

- **h3-js** (optional): Client-side H3 library if needed for spatial blocking in-browser. In practice the server already provides H3 cell IDs per hit, so this is only needed if the client performs H3-based spatial comparisons beyond simple string equality checks.
- **No other new client-side dependencies.** The clustering module (Union-Find, cosine similarity, base64 decoding) is implemented in vanilla JavaScript with no external libraries.

---

## Appendix E — Implications for Backend Plans

This master plan does not directly modify `plan-dynamicClustering.prompt.md` (the architectural plan in the indexing repo) or `plan-ingestionRebuild.prompt.md` (the ingestion plan, partially implemented). The following notes summarise what each of those plans needs to absorb in due course.

### E.1 For `plan-dynamicClustering.prompt.md` (architectural plan)

- **Gateway response additions** — the `clustering_params` and `toponym_stoplist` response-level metadata (Part II) must be added to the gateway response; the calibration source for these fields must be specified.
- **Per-hit fields** — `h3`, `h3_cover`, `temporal_range`, `aat_ids`, `aat_depths`, `baseline_cluster_id`, `query_match` (with `phon_emb`), and the top-level `query_emb` must be produced by the offline pipeline and emitted in the gateway response (§2.1).
- **Edges array** — the `edges` array with composite `score` and per-facet `s` breakdown must be produced and emitted, with a `max_edges = 4000` cap and globally top-K selection (§2.2, §2.3).
- **Toponym facet computation** — `s.n` must be computed as the maximum Symphonym cosine across all cross-name pairs of the two places (§3.8 "Primary defence"), not just the blocking-trigger toponym.
- **Null-facet handling** — the offline pipeline must use the same dynamic renormalisation rule as the client (§3.1) so that score invariance under default weights holds.
- **Scope filter at the discovery level** — the discovery query must accept scope tokens (a set of pending `dataset_id`s admitted in addition to `dataset_status: published`) and apply them as a filter clause (§7.2, §7.4).
- **Hard-link expansion source** — the gateway's hard-link expansion against Pitt SQLite must be augmented at the Django side with a parallel lookup against DO PostgreSQL for pending assertions associated with the request's scope (§7.2).
- **Cluster representation scope filtering** — the cluster representation returned to the client must filter members and provenance by scope; an off-scope public record whose hard-link expansion would have pulled in a pending record must cluster as if the pending content did not exist (§7.2).
- **Server-side clustering fallback** — the `cluster_threshold` parameter must run Union-Find on the gateway and return pre-grouped results (§5.1). The implementation can reuse the same Union-Find logic as the client.
- **Removal of `group_by_cluster`** — the previous boolean toggle is removed from the gateway API (§5.1, §5.2).

### E.2 For `plan-ingestionRebuild.prompt.md` (ingestion plan)

- **Per-gazetteer H3 coverage** — the ingestion pipeline must compute and store a compacted H3 cell set for every gazetteer (Authority, WHG Dataset, or Collection) at indexing time, supporting cheap intersection tests with the user's Area filter on the client (§1.4.1, §5.3). The coverage is recomputed on dataset publish and on substantial amendment.
- **Per-gazetteer temporal extent** — similarly, an `[start_year, end_year]` summary per gazetteer, recomputed on the same triggers (§1.4.1, §5.3).
- **Dataset status field** — every place record carries `dataset_status` (`published`, `pending`, etc.) and `dataset_id` (§7.2, §7.4). The ingestion pipeline initialises these on upload, transitions them on publication (§9.3), and removes them on retention deletion (§10.1).
- **Pending-dataset isolation** — pending records are admitted to the same indices as public records but with `dataset_status: pending`; the discovery filter (above) is what makes them invisible to off-scope users. Ingestion should not create separate per-contributor indices.
- **`contributor_attestations` schema extension** — `dataset_id` and `status: pending` fields must be added to the table on DO PostgreSQL (§7.4). The ingestion pipeline writes pending assertions as the contributor reconciles, and the publication transaction flips them to `status: active` (§9.3).
- **`/suggest` source data** — the unified Gazetteers list (§5.3) is sourced from the same metadata the ingestion pipeline maintains: name, description, namespace, owner, record count, status, plus the H3 coverage and temporal extent specified above.
- **Curatorial fields are admin-managed and must never be in the inventory push.** The `GazetteerRegistryEntry` model carries three curatorial fields (`core`, `tileset_polygon_only`, `gazetteer_type`) that are set by staff via Django admin (§1.4.3) and **must never** appear in the inventory-push payload from `processing/push_gazetteer_inventory.py`. The Django side enforces this by omitting them from the `defaults` dict in `api/views_indexing.py::GazetteerInventoryView._upsert_one`; `update_or_create(defaults=…)` only writes the keys present in `defaults`, so omission is sufficient — but anyone widening that dict in future could silently blow away staff curation. An inline comment in `_upsert_one` names the protected fields, and a unit test asserting curatorial-field preservation across re-pushes is recommended.
- **Legacy v3.2 reconciliation flagging (no migration)** — DO PostgreSQL remains the canonical store for every contributor gazetteer (legacy and new). The ingestion pipeline pulls these from DO via `authorities/whg-places.py` on every run as `whg`-namespaced gazetteers, with no payload migration. The only one-time legacy work is the DO-side schema extension that adds `legacy_v3_2 BOOLEAN DEFAULT false` to `contributor_attestations` and the data-update that sets `legacy_v3_2 = true` on every existing v3.2-era reconciliation link; the Pitt-side hard-link harvest then propagates a `:legacy_v3_2` suffix on `source_id` for downstream filterability (§10.2). This corresponds to Batch 13b in `plan-ingestionRebuild.execution.md`.
- **Retention sweep** — the ingestion pipeline (or a partner job) implements the one-year retention sweep on pending datasets (§10.1), with eleven-month notification, twelve-month deletion, and appropriate exclusions for `submitted` and `private_permanent` datasets.
- **Format-only validation at upload** — the ingestion pipeline performs format-level validation (parsing, required fields, coordinate ranges, well-formed place_ids) but not semantic validation (§8.2). Semantic validation is the contributor's task during reconciliation and the editor's task during review.
