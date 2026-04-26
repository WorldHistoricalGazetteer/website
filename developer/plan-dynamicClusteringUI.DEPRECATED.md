> **DEPRECATED — 2026-04-26.** This document has been absorbed into [`plan-masterPlan.prompt.md`](plan-Atlas-DynamicClustering.prompt.md) (Parts II–V and Appendices A–D) and is retained only for historical reference. Do not update this file; update the master plan instead.

# Plan: Dynamic Clustering — Front-End UI & Django API Changes (whg3 Project)

## Introduction

This plan specifies the front-end and Django-side changes required to support **dynamic query-time clustering** in the WHG search interface. It is a companion to the backend plan (`plan-dynamicClustering.prompt.md` in the `indexing` repository), which covers the offline similarity graph pipeline, ES schema changes, and CRC gateway endpoint modifications.

### What the backend provides

The CRC gateway (`POST /api/search`) returns a **clustering-ready response** containing:

- A flat list of search hits, each carrying compact clustering signals (H3 cell, temporal range, AAT type info, Symphonym phonetic embedding, baseline cluster ID).
- An `edges` array forming a **local similarity subgraph** — pairwise similarity edges between result-set members, each with a composite score and per-facet signal breakdown.
- Aggregations for type and country facets (unchanged from current).

The gateway also supports an optional `cluster_threshold` parameter for server-side fallback clustering (for non-JS consumers like OpenRefine).

### What the front-end must do

1. Receive the clustering-ready payload.
2. Perform client-side Union-Find clustering using a user-controlled similarity threshold (θ slider).
3. Allow the user to adjust the threshold and per-facet emphasis weights interactively, with instant re-clustering (no server round-trip).
4. Display results as expandable cluster cards when grouping is active.
5. Support phonetic name comparison via the gateway's Symphonym embedding endpoint (`GET /api/embed`).

### Architecture overview

```
Browser (whg3 search page)
  ├── Sends search request → Django thin proxy → CRC Gateway → ES
  ├── Receives: hits[] + edges[] + aggregations
  ├── clustering.js: Union-Find on edges, threshold from slider
  ├── Slider/weight changes → instant re-cluster (no round-trip)
  └── Renders: cluster cards (collapsed/expanded) + map markers

Django thin proxy (api/crc_client.py)
  ├── Forwards search requests to CRC Gateway
  ├── Passes through new fields (edges, phon_emb, etc.)
  └── Passes through cluster_threshold when set
```

---

## 1. Search Response Payload Format

The CRC gateway returns the following additions to the existing search response. The Django thin proxy must pass these through unchanged.

### 1a. Per-result compact payload

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
| `h3_cover` | string[] | Compacted H3 cell IDs covering the full geometry (multi-resolution). Used for spatial bucketing via cover intersection at r5 (§2i) to catch places whose centroids differ across authorities. For point-only geometries, equals `[h3]`. |
| `temporal_range` | `[int, int]` or `null` | Flattened temporal extent `[start_year, end_year]` across all timespans. Null if the place has no temporal data. For display only — temporal similarity is precomputed in edge signals. |
| `aat_ids` | `int[]` | AAT concept IDs from the place's type mappings. For display (type labels, tooltips). |
| `aat_depths` | `int[]` | AAT hierarchy depths, parallel to `aat_ids`. |
| `baseline_cluster_id` | `string` or `null` | Precomputed high-confidence cluster ID (θ ≈ 0.9). Results sharing a `baseline_cluster_id` are near-certain co-referents. Used to bootstrap the Union-Find before applying the user's threshold. |
| `query_match` | object | Discovery-time match signal. Contains `name` (the matched toponym string), `score` (normalised 0–1 discovery score — how well the query matched this toponym), and `phon_emb` (Symphonym 128-d int8 embedding, base64-encoded, 172 chars). Used for query-conditioned clustering (§2h) and phonetic re-scoring (§2g). |

The response also includes a top-level `query_emb` field: the Symphonym 128-d int8 embedding of the original query string (base64-encoded). This eliminates the need for a separate `GET /api/embed` call for the initial query — the client uses it directly for phonetic comparison (§2g). For variant names the user types later, `/api/embed` is still called.

### 1b. Edges array

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

### 1c. Payload size budget

- ~500 results × ~500 bytes ≈ 250 KB (hits including query_match at ~200 bytes each)
- ~2000 edges × ~120 bytes ≈ 240 KB (edges with signal breakdown)
- query_emb: 172 bytes (negligible)
- Total: ~490 KB before gzip, ~110–160 KB compressed — within budget.

**Hard cap:** `max_edges = 4000`. The gateway enforces this limit on the `edges` array, selecting edges by highest composite score globally. Without a cap, worst-case scenarios (dense urban + high K + symmetrisation: 500 × 50 / 2 = 12,500 edges ≈ 1.5–2 MB) degrade both transfer and client parsing time. The cap keeps payload under ~750 KB pre-compression in all cases.

For result sets > 500, the gateway caps edges to top-scoring pairs and/or restricts clustering to the top N results.

---

## 2. Client-Side Clustering Algorithm

### 2a. Edge scores and facet-weight scaling

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

### 2b. Comparison pruning

Only compare pairs that have a precomputed edge. This avoids O(n²) explosion:

- The server already prunes to the local subgraph (edges between surviving results).
- Additional client-side blocking: same H3 cell, or shared authority link, or same baseline cluster.
- For ~500 results with ~2000 edges, clustering is O(n) — trivially fast.

### 2c. Union-Find with threshold

```
// Phase 1 — precomputed edges
for each edge (a, b, signals):
    S = reweight(signals, weights)    // with null-facet renormalisation (§2a)

    // Rule 1 — standard: edge exceeds user threshold
    if S >= θ:
        union(a, b)

    // Rule 2 — query bridge: relax threshold for query-relevant pairs (§2h)
    elif S >= θ_bridge
         AND min(query_score[a], query_score[b]) >= θ_query
         AND (signals.n >= τ_name OR signals.l >= τ_link):   // name/link guard
        union(a, b)

// Phase 2a — phonetic synthetic edges for edgeless pairs (§2i)
θ_synth_eff = max(θ_synth, θ)    // never below calibrated floor or user threshold
for each spatial bucket (results sharing h3 centroid OR h3_cover intersection at r5):
    for each pair (a, b) in bucket where find(a) ≠ find(b) AND no precomputed edge:
        if NOT both_high_frequency(name[a], name[b]):  // stoplist guard (§2i)
            if types_overlap(a, b):   // at least one shared type (§2i)
                sim = cosine(phon_emb[a], phon_emb[b])
                if sim >= θ_synth_eff:
                    union(a, b)

// Phase 2b — structural synthetic edges (§2i)
for each spatial bucket (same as 2a):
    for each pair (a, b) in bucket where find(a) ≠ find(b) AND no precomputed edge:
        if (ccode_overlap(a, b) OR shared_namespace(a, b) OR shared_baseline(a, b)):
            union at θ_synth_structural (≈ 0.7)

// Phase 3 — post-processing: split oversized clusters (§2f)
for each component C where |C| > N_max:
    split C by tightening threshold within the component
```

Properties:
- Edge iteration order does not affect the result — Union-Find is applied over all qualifying edges in a single pass (no sorting required). Complexity: O(E·α(n)) ≈ O(E).
- Rule 2 has a **name/link guard** (`signals.n >= τ_name OR signals.l >= τ_link`, where `τ_name ≈ 0.5`, `τ_link ≈ 0.8`) that prevents the bridge from firing on weak edges between places matching generic query terms (e.g. "San", "New", "Central"). Without this guard, two places both matching a common query fragment would merge on a sub-threshold edge with no substantive name or link alignment.
- Rule 2 and Phases 2a/2b can cause **non-monotonic behaviour**: lowering θ may merge clusters that were separate at higher θ due to the bridge and synthetic thresholds. In practice this is rare (bridge fires on <5% of edges, synthetic passes on edgeless pairs only). Tying `θ_synth_eff = max(θ_synth, θ)` limits the effect: at high θ, synthetic edges require even higher phonetic similarity, preserving monotonic feel in the common case.
- Union-Find is near-linear and runs in <10 ms for 500 nodes.
- The query-bridge rule (Rule 2) ensures query-relevant pairs cluster even when their precomputed toponym signal `s.n` is low — see §2h for the full rationale.
- The phonetic synthetic-edge pass (Phase 2a) closes the "missing edge" gap for pairs that share spatial proximity, phonetic similarity, and type overlap but were never candidates in the offline pipeline — see §2i.
- The structural synthetic-edge pass (Phase 2b) catches same-place records across authorities where phonetics fail (cross-lingual exonyms, sparse names) — see §2i.
- Oversized clusters are split as a post-processing step (Phase 3), not blocked during union — see §2f.

### 2d. Baseline cluster bootstrapping

Before applying the user threshold, initialize the Union-Find with baseline clusters (if present): for all results sharing a `baseline_cluster_id`, union them. This provides instant grouping for obvious matches (e.g. GeoNames + Wikidata for the same city) before the user even touches the slider.

**θ = 1.0 bypass.** When the user sets θ = 1.0 ("no grouping — flat list"), baseline bootstrapping is **skipped entirely**. The Union-Find starts with every result in its own singleton component, and since no edge can have a reweighted score ≥ 1.0, no unions occur. This guarantees a truly flat result list identical to unclustered behaviour. At any θ < 1.0, baseline bootstrapping runs normally.

**Safety:** baseline clusters are **link-dominated**: constructed offline using only authority link signals (`s.l`) and very high toponym signals (`s.n ≥ 0.95`), not the full composite score. This ensures they remain valid regardless of how the user tunes facet weights — a user who prioritises spatial proximity over name similarity will not find that baseline clusters contradict their semantic intent. Bootstrapping cannot merge two *different* baseline clusters — it only unions results within the same cluster ID. Subsequent edges from Phase 1 may *expand* a baseline cluster by merging additional results into it, but only if those edges pass the user's threshold θ.

### 2e. Cluster display

Each cluster gets:
- **Representative**: highest-scoring hit (or preferred-authority heuristic).
- **Aggregated metadata**: all names across members, all authorities, temporal span union, types union.
- **Expandable**: user can expand a cluster to see individual member records.

### 2f. Cluster-size limiting (post-processing)

Union-Find can produce "mega-clusters" at low θ in dense urban regions (e.g. every "Paris" record in one group). Rather than blocking merges during the union pass (which introduces order-dependent results and breaks transitivity), oversized clusters are **split as a post-processing step** after the Union-Find completes:

1. For each connected component with more than `N_max` members (e.g. 50):
2. Extract the subgraph of edges within the component.
3. Tighten the threshold iteratively: raise θ within this component until it fragments into sub-clusters all ≤ N_max, or until θ reaches 0.95 (at which point accept the large cluster as genuinely co-referent).
4. Hard-link edges (authority sameAs, `s.l ≈ 1.0`) are never cut during splitting — they act as unbreakable bonds within the component.

This preserves transitivity: if A~B and B~C both pass the user's threshold, they are always in the same component. Splitting only tightens the threshold *within* oversized components, producing deterministic and order-independent results.

### 2g. Client-side phonetic re-scoring

Each hit carries a `query_match.phon_emb` field: the Symphonym 128-d int8 embedding for the place's best-matching toponym. The response also includes `query_emb` — the embedding of the original query string. The client can use these to let the user type an alternative name variant and instantly see how phonetically close it is to every result — without a server round-trip.

**Flow:**

1. User types a variant in a "Compare name" input (e.g. "Parigi").
2. The client calls `GET /api/embed?name=Parigi` on the gateway (via the Django proxy), which returns the Symphonym int8 embedding for the new variant (fast — single model inference, ~5 ms). For the *initial* query, `query_emb` from the response is used directly (no extra call needed).
3. The client computes cosine similarity between the variant embedding and each hit's `query_match.phon_emb` in JavaScript. Int8 dot product on 128 dimensions is trivially fast (~0.01 ms per pair). Embeddings are pre-normalised unit vectors quantized to int8; compute `dot(a, b) / (norm(a) × norm(b))` for the similarity value.
4. Results are re-ranked or highlighted by phonetic proximity to the user's variant.

This enables cross-script and cross-transliteration name comparison directly in the browser — a researcher can type a name in Arabic script and see which Latin-script results are phonetically closest, or compare a medieval spelling variant against modern authority records.

The `query_match.phon_emb` vectors also serve a structural role in clustering: they enable **synthetic phonetic edges** for result pairs that lack a precomputed edge — see §2i.

### 2h. Query-conditioned clustering

Precomputed edges encode **query-independent** similarity (`place ↔ place`). But effective search-result clustering requires **query-conditioned** grouping (`query → place → place`). Consider: a user searches for "Big Apple". One result (Wikidata's New York City) matches via that alias. Other NYC records from GeoNames, OSM, etc. may also appear in the result set — matched via "New York" through different discovery paths or neighbor expansion — but they matched the query on a different name variant.

**Primary defence (offline, in the indexing pipeline):** The toponym facet signal `s.n` on each precomputed edge is computed as the **maximum Symphonym cosine similarity across ALL cross-name pairs** of the two places — not just the single toponym that triggered blocking. This means places sharing "New York" produce a high `s.n` even when the query matched on "Big Apple". In most cases, this is sufficient: the standard threshold rule (§2c Rule 1) handles alias cases because the graph already captures the full alias overlap.

**Safety net (client-side):** The query-bridge rule in §2c (Rule 2) catches residual edge cases where the max-pairwise toponym score is still coincidentally low but both places are clearly query-relevant. Each hit's `query_match.score` indicates how strongly it matched the original query. The bridge rule relaxes the edge threshold when both endpoints strongly match the query:

- `θ_bridge = θ × 0.6` (or a configurable floor, e.g. 0.3) — minimum edge quality for bridging.
- `θ_query = 0.7` — minimum query-match score.
- `τ_name ≈ 0.5` — minimum toponym signal for name-based bridge qualification.
- `τ_link ≈ 0.8` — minimum link signal for link-based bridge qualification.

The bridge fires only when `signals.n >= τ_name OR signals.l >= τ_link` (in addition to the score and query conditions). This **name/link guard** prevents the bridge from becoming a semantic shortcut on weak edges between places matching generic query terms ("San", "New", "Central"). Without it, any two results matching a common query fragment could merge on a sub-threshold edge with no substantive identity signal.

**Why `min()` not `max()`:** both endpoints must strongly match the query for the bridge to fire. Using `max()` would let a single strong match pull in weakly-related neighbors indiscriminately. Using `min()` ensures both places are relevant to what the user searched for.

**Why a precomputed edge is required:** merging two places purely because both match the query (without any precomputed edge) is dangerous — "London" would merge London-UK with London-Ohio. The bridge rule only relaxes the *threshold* on existing edges; it does not create edges from nothing.

### 2i. Synthetic edges (edgeless pairs)

The precomputed graph will inevitably miss some co-referent pairs — rare aliases, missing language variants, or places that fell outside the offline blocking thresholds. Two complementary synthetic passes close this gap at query time.

#### Synthetic Rule A — Phonetic (§2c Phase 2a)

The `query_match.phon_emb` vectors enable **phonetic synthetic edges** between result pairs that have no precomputed edge.

After the main Union-Find pass (§2c Phase 1), the client runs Phase 2a over spatial buckets:

1. Group results by spatial proximity: same `h3` centroid (r7 ≈ 1.2 km) **or** `h3_cover` intersection at coarse resolution (r5 ≈ 8 km). Centroid equality catches point-vs-point matches; cover intersection catches cases where the same place has different centroids across authorities (e.g. Paris polygon vs Paris point, linear features, boundary geometries). The server provides both `h3` (centroid) and `h3_cover` (multi-valued) per hit.
2. Within each bucket, for every pair (a, b) not already in the same component and with no precomputed edge:
   - **Stoplist guard:** skip if both `query_match.name` values are in a **high-frequency toponym stoplist** (e.g. "Central", "Station", "Market", "Church", "School", "Main", "Park", "New", "San", "Saint"). Without this guard, high phonetic similarity + same H3 + same type (e.g. "building") produces catastrophic merging in OSM-heavy urban regions. The stoplist is included in the server response metadata.
   - **Type constraint:** at least one type must overlap (shared `aat_id`, or both lacking type data). If both places have typed records, require at least one shared AAT ancestor; if either is untyped, allow the comparison (untyped records are common and should not be excluded).
   - Compute `cosine(phon_emb[a], phon_emb[b])`.
3. If the similarity exceeds `θ_synth_eff = max(θ_synth, θ)`, union them.

**Why `max(θ_synth, θ)`:** this ties the synthetic threshold to the user's main slider. At high θ (conservative grouping), synthetic edges require even higher phonetic similarity, preserving near-monotonic behaviour. At low θ (aggressive grouping), the calibrated floor `θ_synth` (e.g. 0.85) prevents garbage merges.

#### Synthetic Rule B — Structural (§2c Phase 2b)

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

#### Cost analysis

**Phonetic pass:** H3 bucketing reduces comparisons from O(n²) to O(Σ |bucket|²). Typical result sets have most buckets containing 1–5 results; dense urban areas might have ~20. With 500 results across ~200 buckets, total comparisons are a few hundred — each a ~0.01 ms int8 dot product plus a cheap type-set intersection. Total cost: <1 ms.

**Structural pass:** same bucket structure, set intersections only — even cheaper.

**Why spatial gating is essential:** without it, phonetic similarity would merge geographically distant places (e.g. "Springfield" in Illinois vs Massachusetts).

**Why centroid-only is insufficient:** large polygons can have different centroids across authorities. Using `h3_cover` intersection at r5 catches these without requiring exact centroid alignment.

All parameters (`θ_bridge`, `θ_query`, `θ_synth`, `θ_synth_structural`, `τ_name`, `τ_link`, and the toponym stoplist) are calibrated by the offline pipeline and included in the server response as defaults — the client does not hard-code them.

---

## 3. Front-End UI Changes (Django Search Page)

The search page (`/search/`, template `search/templates/search/search.html`, JS in `whg/webpack/js/search.js`) requires significant changes to support dynamic clustering.

### 3a. Remove: "Group linked records" toggle ✅ DONE

The "Group linked records" checkbox in the **Data Sources** panel has been removed. In its place, the panel now displays a static note under the "clustering" header explaining that clustering of linked records will be managed using a similarity threshold control within the returned search results.

**Changes made:**
- `search/templates/search/search.html` — removed the checkbox element and its label from the Data Sources panel; replaced with an informational note about the forthcoming results-panel clustering control.
- `whg/webpack/js/search.js` — removed the `$('#clustering_toggle').on('change', ...)` event handler; `clusterResults` variable is retained but always `true` (no longer user-togglable from the filters panel).
- The `cluster: clusterResults` parameter in `gatherOptions()` is retained for backward compatibility until the gateway is updated.

**Files affected:**
- `search/templates/search/search.html`
- `whg/webpack/js/search.js`
- `api/crc_client.py` (Django thin proxy) — stop forwarding `group_by_cluster` to the gateway (pending).

### 3b. Add: Similarity threshold slider

A continuous slider (θ ∈ [0,1]) in the results panel controls clustering sensitivity. Position it prominently above the result list, with a label such as "Group similar places" and a tooltip explaining the behaviour.

| Slider position | Effect |
|----------------|--------|
| θ = 1.0 (rightmost) | No grouping — flat list identical to current behaviour |
| θ = 0.8 (default) | Conservative grouping — high-confidence co-referents only |
| θ = 0.5 | Moderate grouping — phonetically similar + spatially proximate |
| θ = 0.0 (leftmost) | Aggressive grouping — all connected results merged (subject to cluster-size limiting) |

**Behaviour:**
- Moving the slider triggers client-side re-clustering (§2c) with no server round-trip.
- Debounce at ~100 ms to avoid flicker during drag.
- The result list re-renders with clustered/unclustered grouping.
- The map updates: clustered places share a marker group or are connected by visual links.
- Persist the slider position in `sessionStorage` so it survives page navigation.

### 3c. Add: Facet emphasis controls (optional, collapsible)

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

### 3d. Add: Phonetic comparison input

A small input field in the results panel labelled "Compare name variant" or similar. When the user types a name:

1. Debounce at 300 ms.
2. Call `GET /api/embed?name=<input>` to obtain the Symphonym embedding. (For the initial search query, `query_emb` from the response is already available — no call needed.)
3. Compute cosine similarity against each result's `query_match.phon_emb`.
4. Display a phonetic proximity indicator (e.g. colour-coded badge or numeric score) next to each result.
5. Optionally re-sort results by phonetic proximity to the typed variant.

This is particularly valuable for researchers working with historical or non-Latin-script name variants.

### 3e. Add: Cluster expansion/collapse UI

When clustering is active (θ < 1.0), the result list displays **cluster cards** instead of individual place cards:

- **Collapsed state** (default): shows the representative place (highest-scoring or preferred-authority member), a count badge ("3 sources"), and the aggregated name list.
- **Expanded state**: clicking the cluster card expands it to show all member places as sub-cards, each with its own authority badge, names, and metadata.
- **Map interaction**: clicking a cluster card zooms to the bounding box of all member `repr_point` coordinates (not full geometries — those are external and not loaded by default). Expanded members are shown as individual markers at their `repr_point`; collapsed clusters show a single marker at the representative's `repr_point`. Dense clusters (≥5 members within a small area) may optionally display a convex hull outline on hover, computed from member `repr_point` coordinates.

### 3f. Update: Result-facet filters (post-search)

The existing client-side facet filters (Place Types checkboxes, Countries checkboxes) continue to work as before, but now operate on the **clustered** result set:

- A cluster is visible if **any** of its members passes the facet filter.
- The facet counts reflect unique clusters, not individual places (when clustering is active).
- Toggling a facet filter does not re-trigger clustering — it only shows/hides clusters in the already-computed grouping.

### 3g. Update: Feature-class checkboxes → Type facets

The legacy feature-class checkboxes (`A`, `P`, `S`, etc.) in `#adv_checkboxes` are already marked for replacement (see `developer/search-system-architecture.md` §2.2 in the `indexing` repo). This plan accelerates that: replace them with the server-side type aggregation facets returned in the search response. The type facets use AAT identifiers and hierarchical labels from the `types` index, not GeoNames feature classes.

### 3h. Update: Data Sources panel ✅ PARTIALLY DONE

The existing Data Sources panel lists the authority namespaces available for filtering (GeoNames, Wikidata, OSM, etc.). Changes:

- **Remove** the "Group linked records" toggle (§3a). ✅ DONE — replaced with an informational note about the forthcoming results-panel clustering control.
- **Retain** the namespace inclusion/exclusion checkboxes — these feed `namespaces` / `exclude_namespaces` on the search request and remain useful.
- **Add** D-PLACE as a data source checkbox (unchecked by default). ✅ DONE — D-PLACE was moved here from the Regions/Territories panel because it contains only point data and is not useful as a spatial constraint.
- **Add** a small indicator per namespace showing the count of results from that source in the current (possibly clustered) result set. (Pending.)

**Additional spatial changes:**
- D-PLACE removed from the Territory tab's polity dataset toggle (`politySelector.js`). ✅ DONE
- D-PLACE removed from the Atlas page's available spatial sources (`search/views.py`). ✅ DONE
- "OSM/OHM (Miscellaneous)" added to the Region tab's namespace toggle (`regionSelector.js`). ✅ DONE — covers non-standard `boundary=` tags (aboriginal_lands, barony, civil, civil_parish, climatic_zone, geographic, histori*, indigenous_administration, native_reservation, parish, political, region, etc.). Backend tile/query support pending.

### 3i. JavaScript implementation

The client-side clustering logic (Union-Find, edge reweighting, threshold application) should be implemented as a self-contained ES module (e.g. `whg/webpack/js/clustering.js`) with no external dependencies:

- `class UnionFind` — standard disjoint-set with path compression and union by rank.
- `function clusterResults(hits, edges, theta, weights, queryScores, params)` — returns a `Map<clusterId, ClusterGroup>`. At θ = 1.0, returns all singletons immediately (no baseline bootstrapping, no edge processing). Otherwise runs Phase 0 (baseline bootstrap), Phase 1 (precomputed edges with Rules 1–2), Phase 2a (phonetic synthetic edges), and Phase 2b (structural synthetic edges).
- `function reweightEdge(edge, weights)` — computes the weighted sum from signal components with null-facet renormalisation.
- `function queryBridgeThreshold(theta)` — computes `θ_bridge` from the user's main threshold (e.g. `θ × 0.6`, floored at 0.3).
- `function phoneticSyntheticPass(uf, hits, params)` — H3-bucketed phonetic comparison for edgeless pairs with stoplist guard and type-overlap check (§2i Rule A).
- `function structuralSyntheticPass(uf, hits, params)` — H3-bucketed structural comparison (ccode overlap, shared namespace, shared baseline) for edgeless pairs (§2i Rule B).
- `function typesOverlap(a, b)` — returns `true` if the two hits share at least one AAT ancestor or if either is untyped. Used as a gate for phonetic synthetic edges.
- `function isStoplistName(name, stoplist)` — returns `true` if the name is in the high-frequency toponym stoplist. Used as a guard for phonetic synthetic edges.
- `function spatialBucket(hits)` — groups hits by H3 centroid equality or `h3_cover` intersection at r5. Returns buckets for synthetic edge passes.
- `function cosineSimilarity(a, b)` — int8 dot product for phonetic re-scoring and synthetic edges.
- `function decodePhonEmb(base64)` — decode base64-encoded int8 embedding to `Int8Array`.

**Lazy embedding decode.** To reduce memory pressure on mobile devices, `phon_emb` values should remain as base64 strings until needed. Decode to `Int8Array` only when: (a) the synthetic edge pass runs, or (b) the user activates phonetic re-scoring. This avoids allocating ~500 `Int8Array` objects (~64 KB) on every search — negligible on desktop but meaningful on memory-constrained mobile browsers.

This module is imported by `search.js` and called on every slider change. It should be pure (no DOM manipulation) — it returns data structures that the rendering layer consumes.

---

## 4. Django API Changes

### 4a. OpenRefine / Reconciliation API documentation

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

### 4b. Django thin proxy changes (`api/crc_client.py`)

The Django thin proxy forwards search and reconciliation requests to the CRC gateway. Required changes:

1. **Stop sending `group_by_cluster`** — this parameter is removed from the gateway API.
2. **Pass through `cluster_threshold`** — when the Django search form or API consumer sets `cluster_threshold`, forward it to the gateway.
3. **Pass through new response fields** — the proxy must not strip `edges`, `phon_emb`, `h3`, `h3_cover`, `temporal_range`, `baseline_cluster_id`, `query_emb`, or other new fields from the gateway response. In particular, pass through the response-level metadata: `toponym_stoplist` (generic name tokens for synthetic edge filtering), and `clustering_params` (calibrated defaults for `θ_bridge`, `θ_query`, `θ_synth`, `θ_synth_structural`, `τ_name`, `τ_link`, default facet weights).

---

## 5. Implementation Phases

### Phase D — Client-side implementation

1. ~~Remove the "Group linked records" toggle from the Data Sources panel.~~ ✅ DONE — replaced with clustering info note; D-PLACE added to Data Sources; D-PLACE removed from Territory tab; OSM/OHM (Misc.) added to Region tab namespace toggle.
2. Implement `clustering.js` module: Union-Find, edge reweighting, threshold application, query-bridge rule (§2h), `cosineSimilarity()`, `decodePhonEmb()`.
3. Implement threshold slider (§3b) with debounced re-clustering.
4. Implement facet emphasis controls (§3c), collapsed by default.
5. Implement phonetic comparison input (§3d) with `/api/embed` integration and `query_emb` shortcut.
6. Bootstrap with baseline clusters (§2d).
7. Add cluster expansion/collapse UI (§3e).
8. Update result-facet filters to operate over clustered results (§3f).
9. Replace feature-class checkboxes with type facets (§3g).
10. Tune default weights, threshold, and query-bridge parameters using calibrated defaults from the server.

### Phase E (partial) — Cleanup

1. Remove `cluster_id` / `cluster_size` from the old search hit rendering.
2. Remove `group_by_cluster` from the Django proxy and all downstream code.
3. Write OpenRefine migration guide: `group_by_cluster` → `cluster_threshold` (see §4a above).

---

## 6. Dependencies

- **h3-js** (optional): Client-side H3 library if needed for spatial blocking in-browser. In practice the server already provides H3 cell IDs per hit, so this is only needed if the client performs H3-based spatial comparisons beyond simple string equality checks.
- **No other new client-side dependencies.** The clustering module (Union-Find, cosine similarity, base64 decoding) is implemented in vanilla JavaScript with no external libraries.

---

## 7. Performance Characteristics

| Phase | Latency | Notes |
|-------|---------|-------|
| Server: full search pipeline | ~300 ms | Discovery + filtering + enrichment + neighbor expansion |
| Client: initial Union-Find clustering | <10 ms | ~500 nodes, ~2000 edges |
| Client: slider re-clustering | <5 ms | Re-apply threshold, no server round-trip |
| Client: phonetic embed request | ~50 ms | `GET /api/embed` round-trip |
| Client: phonetic re-scoring (all hits) | <5 ms | 500 × int8 dot product |

Total perceived latency: **~300 ms server + instant client interaction**.

---

## 8. Failure Modes and Mitigations

### 8a. No server-side pagination for clusterable results

Client-side clustering requires the full result set and its edge subgraph in a single payload — traditional server-side pagination is fundamentally incompatible because co-referent places split across pages could never be clustered together (e.g. "Paris" from GeoNames on page 1, "Paris" from Wikidata on page 2).

The existing gateway returns all results in one response (`SearchRequest.size`, max 500) with no `page`/`offset` parameter. The clustering design preserves this: the entire clustering window (up to 500 results + up to 4000 edges) is delivered in a single payload. Any "pagination" is purely **client-side display pagination** — the browser holds all results and edges in memory, clusters them, and uses virtual scrolling or page controls to render subsets of the already-clustered list.

For queries producing more than 500 matches, the gateway returns the top 500 by discovery score. Matches beyond 500 are not clusterable but are summarised in the response metadata (total hit count, facet aggregations). If a user needs to explore beyond the clustering window, they should refine the query (add filters, narrow spatial bounds) rather than paginate.

If future requirements demand clustering over larger result sets, the server-side fallback path (`cluster_threshold` parameter) can cluster on the gateway and return pre-grouped results for any number of hits.

### 8b. Dense urban datasets (OSM-heavy)

OSM contributes many spatially proximate records with similar names (e.g. "Pharmacy" × 50 in a city). Mitigations:
- The offline pipeline's spatial proximity thresholds already limit candidate pairs.
- Type similarity separates "pharmacy" from "city" even when spatially co-located.

### 8c. Weak type/temporal data → over-merging

Many records (especially GeoNames) lack temporal data and have generic types. Mitigations:
- Baseline clusters at θ = 0.9 only merge near-certain matches.
- The default UI slider position should start high (e.g. 0.8), encouraging conservative grouping.
- Hard links (authority sameAs) bypass the threshold entirely.

### 8d. Edge incompleteness → under-clustering

The dominant recall failure mode: the offline graph misses edges for cross-lingual exonyms, sparse-name records, or type-misaligned authorities. Lowering θ cannot fix missing edges. Mitigations:
- Phonetic synthetic edges (§2i Rule A) catch spatially co-located pairs with phonetic similarity.
- **Structural synthetic edges (§2i Rule B)** catch pairs where phonetics fail entirely but structural signals (shared country code, namespace, baseline cluster) confirm co-reference. This is the cheapest and most reliable catch for the dominant failure mode.
- The query-bridge rule (§2c Rule 2) relaxes thresholds for query-relevant pairs.

