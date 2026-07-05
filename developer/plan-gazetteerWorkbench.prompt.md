# Plan — Browser-based, local-first Gazetteer Workbench

> **Status:** DRAFT — updated 2026-07-03 with team-meeting outcomes; still awaiting fuller
> colleague sign-off of the proposal
> ([`WorldHistoricalGazetteer/place#111`](https://github.com/WorldHistoricalGazetteer/place/issues/111)).
> **Owner:** Stephen Gadd (WHG Technical Director)
> **Created:** 2026-07-02
> **Related:** collaborative/group editing is tracked separately in
> [`WorldHistoricalGazetteer/place#112`](https://github.com/WorldHistoricalGazetteer/place/issues/112)
> and qualifies the local-first invariant below (see §2d, Phase 11).
> **Scope:** dual-repo — front-end in **`whg3`** (Django + webpack; this plan lives here in
> `whg3/developer/`), backend adapter in **`indexing`** (`gateway/`), where the reconciliation
> service and the Symphonym model (`hf/`) — the two backend touch-points — live.

---

## 0. Purpose & framing

Deliver a single-page **Gazetteer Workbench** in the WHG website that carries a tabular place dataset
through the full pre-submission lifecycle **entirely in the browser**:

> **Import → Clean → Normalise → Reconcile → Review → Enrich → Validate → Export or Submit**

The dataset never leaves the browser except as (a) minimal, deduplicated reconciliation queries to
WHG's existing API, and (b) an explicit user-initiated submission of selected rows. Server stays
**stateless w.r.t. user datasets**.

Read the consolidated issue #111 for the full rationale, feature list and caveats. This plan is the
build order.

**Design for n=1 as well as n=50k (2026-07-03 meeting).** Although the driving examples are large
spreadsheets, the UI must be **good for the single-place submission use case** — a user contributing
*one* place. Import, mapping, review, enrichment and the contribute flow should all feel natural and
un-bureaucratic at a single row, not only in bulk. Treat "add/reconcile/submit one place" as a
first-class, low-friction path through the same machinery, and check it explicitly in MVP acceptance.

### Positioning (what this is *not*, and what it could grow into)

- **Not a replacement for WHG's curated "authority" ingestion pipeline.** Large / complex /
  authoritative sources stay with the per-source scripts in the `indexing` repo (`authorities/*` +
  `processing/ingest_all_authorities.py` / `index_namespace.py`), already used for `alc`, `chgis`,
  `hgis`, GeoNames, TGN, Wikidata, etc. The Workbench serves the long tail; the curated pipeline
  serves the big/authoritative head. The UI should *route* users to the curated route past some
  size/complexity threshold (see §6) rather than pretend to handle 200k rows in a tab.
- **Could double as a curation tool for existing gazetteers** — strong overlap with authoring. To
  contain complexity, **build shared, composable modules consumed by two thin front-ends** (authoring
  Workbench + curation surface, plus a tiny single-record correction widget) rather than one
  monolithic UI. Factor for this reuse from day one even if curation ships later. See §2c and Phase 12.

### Design invariants (do not violate)

1. **No server-side persistence of user data** *(for the solo flow)*. Only queries and (opt-in) final
   submissions leave. **Exception — collaboration (#112):** group editing deliberately relaxes this,
   introducing an explicit **pre-upload / shared working copy** so teammates can check out the same
   gazetteer. That is opt-in and scoped to the collaborative case; the default solo flow stays
   strictly local-first. See §2d and Phase 11.
2. **Resumability is sacred.** A researcher may spend days on a 50k-row project. State must survive
   reloads, tab crashes, and — via a downloadable project file — browser eviction.
3. **Deduplicate before the network.** Identical query-constraint tuples reconcile once.
4. **Reuse the existing backend.** WHG already exposes a **standard OpenRefine Reconciliation Service**
   (`whg3/api` — `/reconcile`, `/suggest/*`, `/reconcile/properties`, `/entity/{id}/preview`) over the
   CRC gateway. Consume it; prefer thin additions over new pipelines.
5. **Progressive enhancement.** Heavy optional capabilities (in-browser Symphonym, map, multi-source)
   load lazily and never block the MVP path.
6. **Factor for reuse.** Core capabilities are modules, not page code, so a future curation surface can
   reuse them without a rewrite (see §2c). No monolith.

---

## 1. Current state of the world (verified 2026-07-02)

### 1a. Backend that already exists

**Standard OpenRefine Reconciliation Service** — WHG Django `api` app (`whg3/api/`). This is the
public contract OpenRefine itself uses; the Workbench should consume it directly.

| Endpoint | File | Relevance |
|---|---|---|
| `GET/POST /reconcile` | `api/reconcile.py` (`ReconciliationView`, manifest at `:72`) | **Service manifest** (`versions:["0.2"]`, `identifierSpace`, `schemaSpace`, `defaultTypes`=Place/Period, `view`, `preview`, `suggest`, `extend`) **and** batched `queries` + `extend` in one view. Forwards to CRC gateway via `crc_client` (`crc_reconcile_search`, `crc_extend`). |
| `GET /reconcile/properties` | `api/reconcile.py` (`ExtendProposeView`) | Propose-properties for the extend flow. |
| `GET /suggest/entity`, `/suggest/property` | `api/reconcile.py` | OpenRefine suggest services. |
| `/entity/{id}/preview`, `/entity/{id}/api` | `api/views_entity.py` | HTML preview snippet + entity feature JSON. |

Users get a **preconfigured, tokenised `/reconcile` URL from their Profile page** to paste into
OpenRefine's *"Add Standard Service"* dialog. Docs: <https://docs.whgazetteer.org/content/technical/apis.html>.

**Underlying search/enrichment backend** — WHG API gateway (`indexing/gateway/`), which the Django
layer forwards to. Useful if the Workbench ever needs richer hints than the standard contract exposes:

| Endpoint | File | Relevance |
|---|---|---|
| `POST /api/reconcile` | `gateway/reconcile.py` | Accepts `query`, `mode`, `ccodes`, `fclasses`, `types`, `bounds`, `contained_in`/`containment`/`relation`, `start_year`/`end_year`, `namespaces`/`exclude_namespaces`, `group_by_cluster`, `size`. Returns `hits[]`, optional `clusters[]`, `max_score`, `total`. |
| `POST /api/places` | `gateway/places.py` | Fetch full records by ID → write-back/enrichment. |
| `POST /api/extend` | `gateway/extend.py` | OpenRefine data-extension response format. |
| `GET /api/suggest`, `GET /api/search/phonetic`, `POST /api/search` | `gateway/search.py`, `app.py` | Typeahead + search. |

**No new reconciliation backend is required.** The Workbench targets the existing standard service.
The only likely backend additions are conveniences (see §4): an authenticated proxy for *other*
sources (Wikidata/GeoNames, to dodge browser CORS) and, later, the submission path.

### 1b. Front-end stack (`whg3`)

Django + **Webpack 5** (`webpack.config.js`), **vanilla JS + jQuery** ecosystem: `maplibre-gl`,
`@turf/turf`, `bootstrap`, `select2`, `jquery-ui`, `d3`, `lodash`, `FileSaver`, `moment`. Bundles emit
to `static/webpack/*.bundle.js` and are included by Django templates. **No React/Vue.** The Workbench
should be a **new webpack entrypoint** (e.g. `workbench.bundle.js`) unless we deliberately adopt a
framework (see §9 decision).

### 1c. Reusable technique — in-browser Symphonym (from `GOTW`)

Proven in the `gazetteer-of-the-world` repo (`WHG-LESSONS.md` §2b):

- `process/export_symphonym_onnx.py` exports the Symphonym v7 **Student** `UniversalEncoder` to an
  **int8 ONNX (~8 MB)**; runs in-browser via **onnxruntime-web** (wasm). Parity verified
  `cos(int8,fp32)=0.9997`.
- `process/build_symphonym_index.py` precomputes an **int8 128-d corpus embedding matrix**; the
  browser embeds the query and ranks by `dot(query_fp32, corpus_int8)`.
- Preprocessing ported exactly from `hf/inference.py`: char-vocab tokenisation, Unicode-range script
  detection, `navigator.language` conditioning. Vocabs shipped as JSON (`char_vocab.json`,
  `script_vocab.json`, `lang_vocab.json`).
- Static-hosting tricks (only relevant if we ship a static index): SQLite/FTS5 over HTTP range
  requests; serve range-read binaries as `.png` to dodge CDN gzip; self-host all libs.

**Use in the Workbench:** client-side fuzzy/phonetic **blocking** and **same-name clustering** of the
user's *own* rows (group the CP40 "Newton"s; propose near-duplicates) before any network call — cheap
retrieval in the browser, authoritative reconciliation on the server. Optional, lazily loaded.

### 1d. Prior art — Locolligo (`docuracy/Locolligo`)

Stephen's earlier hand-coded, client-side reconciliation/preparation tool (jQuery SPA on GitHub Pages;
JSONata, PapaParse, proj4+geodesy, Fuse.js, MapLibre, shp2geojson+JSZip, Citation.js). Mine it for
**techniques and config schemas, not code** (it is a jQuery-era 154 KB monolith). Borrowable ideas,
by priority:

- **JSONata as a transform substrate (architectural).** Every format conversion (KML→GeoJSON-T,
  lp.csv→LPF, …) is an *editable JSONata expression* in `templates/mappings.json`, so new
  import/export formats are **config, not code**. Consider adopting an expression language as the
  substrate for both the ROLE-mapping and LPF export, generalising the Phase 9 cleaning pipeline.
- **Coordinate-format detection over a wide range** (proj4 + geodesy `osgridref.js`): decimal
  lat/lon, DMS, **OS National Grid** and **Irish Grid** *string* refs (e.g. `SK690965`), UTM — with
  the detected format shown and user-overridable when ambiguous. *(Being built now — see §Phase 8 /
  the coordinate-mapping step; our own driver `Places.json` uses `NationalGridRef`.)*
- **Auto-confirm with per-source score thresholds** (`libraryMappings.json`: `autoConfirm`,
  `maxScore`, distance + name similarity): auto-accept high-confidence matches so reviewers see only
  genuine ambiguity. *(Folds into Phase 4; being built now.)*
- **Bbox enrichment from non-gazetteer sources** — nearby Wikipedia, Geograph photos, PAS finds,
  OSM roads, Wikidata heritage (parameterised SPARQL). A distinct *enrich-a-located-place* mode
  beyond name→ID reconciliation. New roadmap capability.
- **Shapefile (.zip) + KML import** (shp2geojson+JSZip; fast-xml-parser) — cheap import-coverage wins
  for historical GIS data.
- **Dataset-level provenance/citation capture** — schema.org `Dataset` (ORCID creators, licence,
  temporal/spatial coverage) + Citation.js / `CITATION.cff`; dovetails with the WHG citations/licensing
  work. (Their template defaults to CC-BY 4.0.)
- **`polylabel`** representative points (pole-of-inaccessibility, better than centroid) — relevant to
  WHG's `repr_point` and the `geometries.hull`/`repr_point` fallback issue.
- **NER geocoding of free-text/prose** input (Google NL API) — a non-tabular ingestion mode our
  tabular-only design has not anticipated (large; needs an NER backend).
- **`w3id.org` PID minting** — a concrete, resolvable mechanism for the roadmap's placeholder minting.

Already covered by our plan (Locolligo just confirms): candidate review, LPF/LP-TSV, CRS reprojection,
fuzzy matching, multi-source recon, type-vocabulary mapping. Skip: its 22 hard-coded UK-specific
sources and Peripleo/GitHub-Pages publishing (WHG has its own publication path).

---

## 2. Architecture

```
whg3 (Django: api app + webpack)                whg3 api → indexing/gateway         Elasticsearch
┌──────────────────────────────────────┐        ┌───────────────────────────┐       ┌───────────┐
│  Workbench SPA (workbench.bundle.js)  │        │  STANDARD OpenRefine svc  │       │ places    │
│                                       │        │  GET/POST /reconcile      │──────▶│ toponyms  │
│  ┌─ UI thread ─────────────────────┐  │        │  /suggest/{entity,prop}   │       │ clusters  │
│  │ virtualised table, review panel,│  │        │  /reconcile/properties    │       └───────────┘
│  │ map (maplibre), column editor   │  │        │  /entity/{id}/preview     │  (api forwards to
│  └─────────────────────────────────┘  │        │  ── forwards to gateway ──│   CRC gateway:
│  ┌─ Web Worker(s) ─────────────────┐  │  HTTPS │  /api/reconcile|places|.. │   crc_reconcile_search,
│  │ parser, transform pipeline,     │──┼───────▶│  [NEW proxy] Wikidata/GN  │   crc_extend)
│  │ dedup, recon queue+throttle,    │  │  (tokenised WHG session)  (CORS/keys)     │
│  │ optional Symphonym ONNX/KNN     │  │        └───────────────────────────┘
│  └─────────────────────────────────┘  │
│  ┌─ Storage ───────────────────────┐  │
│  │ IndexedDB (Dexie): rows,        │  │
│  │ decisions, cache, pipeline;     │  │
│  │ OPFS: large blobs; .whgproj     │  │
│  │ export/import for backup        │  │
│  └─────────────────────────────────┘  │
└───────────────────────────────────────┘
```

### 2a. Data model (IndexedDB via Dexie)

- `project` — id, name, createdAt, schemaVersion, storagePersisted flag, source-format, column map.
- `columns` — ordered column defs: `{id, name, role (name|county|type|lat|lon|x|y|crs|date|other|
  derived), crs? (EPSG when x/y are in a projected/national CRS), derivedTemplate?, sourceIndex}`.
- `rows` — `{rowId, cells{colId→value}, dedupKey, state (Pending|Queued|Processing|Completed|Failed|
  Skipped|NoMatch), decision{place_id, label, score, authorityIds{}, matchedAt}, notes}`.
- `queries` (dedup cache) — `{dedupKey, normalisedQuery, candidates[], fetchedAt}` keyed by hashed
  constraint tuple; many rows → one query row.
- `pipeline` — ordered transform steps (replayable recipe), stored not applied destructively.
- `enrichment` — pulled-back authority attributes keyed by place_id (shared across rows).

`dedupKey = hash(normalise(name) + '|' + county + '|' + type + '|' + coarseGeoBucket)`.

### 2b. State machine (per row) — crash-safe

`Pending → Queued → Processing → Completed | Failed`; user overlay states `Skipped`, `NoMatch`,
`Accepted(place_id)`. **On app boot, any row in `Processing` reverts to `Pending`.** The recon worker
writes state transitions inside IndexedDB transactions so a mid-flight crash can't strand a batch.

### 2c. Shared-module architecture (authoring *and* curation from one core)

The authoring Workbench and a future curation surface differ mostly at the **edges** (where rows come
from, and what "contribute" produces), not in the middle. Build the middle once, as framework-agnostic
modules, and compose them into thin front-ends:

| Module | Responsibility | Reused by |
|---|---|---|
| `store` | Dexie/IndexedDB + OPFS; project model; `.whgproj` I/O | both |
| `io` | parsers/serialisers: CSV/TSV/XLSX/JSON/**LPF**/LP-TSV/GeoJSON…; CRS reprojection (proj4js) | both |
| `schema` | auto-detection + column-mapping | both |
| `transform` | replayable, non-destructive pipeline | both |
| `recon` | worker: dedup, throttle, backoff, cache; standard `/reconcile` client | both |
| `symphonym` | in-browser ONNX embed + local KNN (blocking/clustering) | both |
| `review` | virtualised table + keyboard candidate review | both |
| `map` | maplibre candidate plotting / bbox / proximity | both |
| `contribute` | selective submission of chosen/changed rows (+ diff/patch) | both |
| **front-ends** | (a) **Workbench** page; (b) **curation** page; (c) **single-record correction widget** | — |

**Sources differ, core is shared:**
- *Authoring* loads from a **local file**; rows have **no `place_id`** yet → reconcile → contribute a
  *new* gazetteer.
- *Curation (bulk)* loads an **existing WHG/authority gazetteer** (via LPF download); rows arrive with
  `place_id`s → edit/re-reconcile → `contribute` computes a **diff** and submits only changed rows as
  a **patch/correction**, not a fresh dataset.
- *Curation (single record)* is the tiny **correction widget** embedded on a place page — reuses
  `recon` + `review` + `contribute` for one row, feeding WHG's existing correction / veracity model.

### 2d. Collaboration model (group editing — #112)

The default flow keeps everything in one browser. **Collaborative editing (#112)** requires a
**shared server-side working copy** the group edits against — a deliberate, opt-in departure from
invariant #1, because teammates cannot reach each other's IndexedDB. Design sketch (details to be
worked out in #112):

- **Groups own gazetteers.** A gazetteer can be owned by a group (project/corporate/collaboration),
  with a simple role model (owner / editor / viewer). Members share editing/correction rights.
- **Pre-upload / check-out.** To collaborate, a user makes an explicit **pre-upload (shared
  submission)** of the working gazetteer so other members can **check out the same gazetteer** and
  edit it. This shared working store is **distinct from the published index** and from the solo
  local-first store.
- **Per-record granularity.** Model collaboration at the **per-record** level (check out / edit
  individual records) rather than whole-gazetteer locks, so members work different records in
  parallel.
- **Conflict resolution on push.** Each record carries a **version/revision**; on **push**, detect
  conflicts (two members editing or reconciling the same record differently) via optimistic
  concurrency and present conflicting versions for resolution. Needs a concrete merge model (§6).
- **Module impact.** `store` gains a sync/remote-working-copy backend behind its interface; `recon`,
  `review`, `transform`, `contribute` should be usable against either the local or the shared store
  without change. Factor `store` so the shared backend slots in without rewriting the front-ends.

---

## 3. Phased delivery

Each phase is independently demoable. MVP = Phases 1–5. Everything after is enhancement.

### Phase 1 — Skeleton, storage, import (MVP)
- New Django view/URL + template + `workbench.bundle.js` webpack entry; auth-gated (WHG login).
- Dexie schema (§2a); `navigator.storage.persist()` request on project open; **"Clear my data"**.
- File import in a Web Worker: CSV/TSV (streamed), XLSX (SheetJS), JSON. Progress UI for large files.
- **Automatic schema detection** (regex/synonym dictionary for name, county, feature type, lat/lon or
  projected x/y, CRS/EPSG hints, date) → pre-filled **column-mapping** UI the user confirms/edits;
  where coordinates are non-WGS84, prompt for / detect the CRS and reproject in-browser (proj4js).
- Virtualised table (render only visible rows) — evaluate a light virtualiser or hand-roll windowing.
- **Privacy statement** banner: what stays local, what is sent.
- **Acceptance:** import a 50k-row CSV, map columns, reload the page, data still there.

### Phase 2 — Project backup file (MVP; elevated per caveats)
- Export/import a `.whgproj` (ZIP: manifest JSON + rows/decisions/pipeline; large blobs from OPFS).
- Versioned schema with forward-compatible loader.
- **Acceptance:** export, "Clear my data", re-import → identical project state.

### Phase 3 — Reconciliation engine (MVP)
- Recon Web Worker: build **standard OpenRefine `/reconcile` batched `queries`** from the column map +
  hints (name → `query`; county / feature type / coords / date → `properties`), using the user's
  tokenised session. Fall back to the gateway's richer `/api/reconcile` only if a hint can't be
  expressed in the standard `queries` shape (see Phase 6).
- **Dedup/pre-aggregation:** compute `dedupKey` per row; reconcile unique keys once; fan candidates
  back to all rows sharing the key.
- Queue with **client-side throttle**, concurrency cap, **exponential backoff on 429/5xx**; cache in
  `queries` store. Uses the **logged-in WHG session** (no dataset upload).
- Pause/resume; reconciled/total counter; recover cleanly on refocus (background-throttle mitigation).
- **Acceptance:** reconcile the Lewis England set (~16k rows, heavy name repetition) with query count
  ≪ row count; pause, reload, resume to completion.

### Phase 4 — Candidate review & disambiguation (MVP)
- Review panel: ranked candidates (label, type, country, coords, authority IDs) per row.
- **Keyboard-first** (OpenRefine-style: number keys accept candidate N, keys for skip/no-match/next).
- **accept / reject / skip / no-match**; **bulk-accept ≥ threshold**; filter by state.
- Contextual disambiguation using the row's county/type/coords/date (already passed as hints).
- **Acceptance:** review 1,000 rows by keyboard without touching the mouse.

### Phase 5 — Enrichment, derived columns, export (MVP)
- Write-back: chosen authority ID(s), matched label, score, status per row.
- Pull-back attributes via the **standard `extend` flow** (`POST /reconcile` with `extend`, properties
  from `/reconcile/properties`; `/entity/{id}/preview` for previews) — coords, alt-names, parent ADM,
  QID, GeoNames id — as columns; store in `enrichment`, shared across rows with the same match. (The
  Django layer forwards extend to the gateway's `/api/extend`/`/api/places`.)
- **Add/edit columns**, incl. **URL/template columns** (`{col}`/`{authority.qid}` interpolation) and
  free-text notes.
- Export: **CSV, JSON, LPF (first-class), LP-TSV**; all client-side (FileSaver).
- **LPF import** too (round-trips as an interchange format).
- **"Contribute to WHG" enjoinder (MVP).** A reasonably-prominent, friendly (non-nagging)
  call-to-action inviting the user to give their finished, reconciled gazetteer back to WHG — because
  WHG grows through contributed gazetteers, contribution should be the visible, natural endpoint of the
  workflow. Surface it e.g. at export time and on the progress panel once a threshold of rows is
  matched, always reinforcing that it is **opt-in and selective** (only chosen rows leave). The *full
  submission pipeline* is Phase 10; the *encouragement* to contribute ships in the MVP (here it can
  simply route to the existing WHG dataset-contribution flow with the exported LPF).
- **Acceptance:** produce a Lewis export with a working British-History-Online VCH URL column and a
  valid LPF file that re-imports; a visible, dismissible "contribute this to WHG" prompt appears.

### Phase 6 — Consume the existing standard OpenRefine service (mostly client-side)
The standard OpenRefine Reconciliation Service **already exists** (`whg3/api/reconcile.py`; see §1a, §4).
This phase is about *using* it well, not building it:
- Point the recon worker at the standard `/reconcile` (batched `queries`) + `extend` + `/suggest/*`
  contract, using the user's tokenised session — so the Workbench and OpenRefine are interchangeable.
- Confirm the manifest's `properties`/hint vocabulary covers the Workbench's needs (county, feature
  type, coords, dates). Only if a needed hint isn't expressible in the standard `queries` shape do we
  fall back to the gateway's richer `/api/reconcile` — note any such gap here rather than assuming one.
- No new manifest work is expected; see §4 for the small, optional backend items.

### Phase 7 — In-browser Symphonym blocking/clustering (enhancement)
- Ship the int8 ONNX encoder + vocabs (reuse GOTW `export_symphonym_onnx.py`); onnxruntime-web in the
  worker; port preprocessing from `hf/inference.py` exactly.
- Embed the user's rows locally → cosine-KNN to **cluster same-name/near-duplicate rows** and to
  **pre-block** before reconciliation (reconcile cluster representatives, propagate to members).
- Lazy, opt-in (~30 MB cached download); never on the MVP critical path.
- **Acceptance:** CP40 "Newton" variants cluster together offline; reconciling representatives covers
  the cluster.

### Phase 8 — Map-assisted disambiguation (enhancement)
- maplibre-gl panel: plot candidate `repr_point`s; click-to-choose; draw a bbox → feed `bounds`;
  rank by proximity to the row's own coordinates (reprojected to WGS84 from whatever CRS they arrived in).

### Phase 9 — Cleaning pipeline & audit (enhancement)
- **Replayable, non-destructive** transform recipe (trim, whitespace, expand `St.`/saints, strip
  parentheticals, split/join, find/replace, case). Undo/redo; decision audit trail.

### Phase 10 — Multi-source + submission (enhancement)
- Multi-source recon (WHG + Wikidata + GeoNames), per-source per-row selection. **CORS:** route
  external sources through a **WHG authenticated proxy** (new gateway endpoint) or require CORS-safe
  APIs; user-supplied keys kept transient/local only.
- **Direct submission:** contribute finished LPF/LP-TSV (selected rows) to WHG; **placeholder minting**
  for unmatched rows. Wire to the existing dataset-contribution path in `whg3`.

### Phase 11 — Incremental re-import, offline, collaboration (enhancement)
- "Update Dataset": re-upload revised file, match on user-selected primary key/index, preserve matched
  rows, append/patch the rest.
- Service worker for offline review of already-fetched candidates.
- **Group collaboration (#112).** The full model in §2d: groups own gazetteers with shared
  editing/correction rights; **pre-upload → check-out** a shared working copy; **per-record** editing;
  **conflict resolution on push** (per-record revisions, optimistic concurrency, human resolution of
  conflicts). This is larger than a "shared session" and has its own issue
  ([#112](https://github.com/WorldHistoricalGazetteer/place/issues/112)); it should be designed
  alongside this plan because it qualifies the local-first invariant (§2d). Needs the shared
  server-side working store and a state-sync/merge decision (§6).

### Phase 12 — Curation surface for existing gazetteers (enhancement; separate front-end)
Reuses the §2c shared modules — this is a *front-end*, not a second app. Two modes:
- **Single-record correction widget.** Inline "suggest a correction" on a place page (georeference,
  better match, typo, type fix); reuses `recon`+`review`+`contribute` for one row; submits to WHG's
  existing **correction / veracity** model. Low-effort, high-value; could ship before the bulk mode.
- **Bulk curation of one gazetteer.** Load an existing WHG/authority gazetteer via **LPF download**;
  rows arrive **with `place_id`s**; edit / re-reconcile / enrich in bulk; `contribute` computes a
  **diff** and submits only changed rows as a **patch**, not a fresh dataset.
- **Depends on:** the shared modules (Phases 1–5), selective contribution (Phase 10), and a
  server-side **gazetteer-export-to-LPF** + **correction/patch intake** contract (see §4, §6).
- **Guardrail:** honour the size/complexity threshold — steer bulk curation of huge authority
  gazetteers toward the curated pipeline rather than a 200k-row browser session.

---

## 4. Backend: reuse the existing standard service; only small, optional additions

**The standard OpenRefine Reconciliation Service already exists** in `whg3/api/reconcile.py`
(`ReconciliationView`): `GET/POST /reconcile` (manifest + batched `queries` + `extend`),
`/reconcile/properties`, `/suggest/entity`, `/suggest/property`, `/entity/{id}/preview`,
`/entity/{id}/api`. It forwards to the CRC gateway via `api/crc_client.py`. **Do not rebuild it.**

The Workbench consumes this contract directly (Phase 3/5/6). Backend work is limited to:

- **Multi-source proxy (Phase 10, optional).** For reconciling against *other* standard services
  (Wikidata / GeoNames) from the browser, add an authenticated WHG proxy to sidestep CORS and keep
  user-supplied keys out of any datastore. Could live in `whg3/api` (alongside `crc_client`) or the
  gateway.
- **Submission path (Phase 10, optional).** Wire "submit selected rows as LPF/LP-TSV" onto the
  existing `whg3` dataset-contribution flow (validation → indexing). Likely no new endpoint, just
  reuse.
- **Curation contract (Phase 12, optional).** Two server capabilities the curation surface needs:
  (a) **export an existing gazetteer to LPF** for download into the browser (may already exist in
  `whg3` — verify before building), and (b) a **correction/patch intake** that accepts a diff of
  changed rows for an existing gazetteer, mapping onto WHG's existing **correction / veracity** model
  (cf. the `indexing` authority scripts that already ingest corrections, e.g. `authorities/alcedo-places.py`).
  Single-record corrections reuse the same intake for one row.
- **Conformance/gap check (Phase 6).** Confirm the manifest's `properties`/type vocabulary expresses
  every hint the Workbench needs (county, feature type, coords, dates). If — and only if — a needed
  hint isn't expressible in the standard `queries` shape, consider extending `api/reconcile.py`'s
  query→gateway mapping (the gateway's `/api/reconcile` already accepts `fclasses`/`types`/`bounds`/
  years), and add a small conformance test. Treat this as a verification task, not assumed work.

### 4a. Onboarding & activity monitoring (server-side; 2026-07-03 meeting)

Because reconciliation runs against the authenticated `/reconcile` service, WHG can observe when a
user *starts and how they progress* through a reconciliation **from API hits alone** — no dataset
upload needed. This gives us a light, privacy-respecting engagement layer (only query metadata is
observed, never the user's rows):

- **Welcome / onboarding email on first reconciliation.** Detect a user's *first* reconciliation
  activity (the first authenticated `/reconcile` hits in a session/project) and send a welcome email
  with orientation and an offer of help. Trigger off **monitored API hits**, not a client ping.
- **Feed Palak's Gazetteer Submission Tracker** *(to be built).* Surface active reconciliation users
  in the tracker, including **which part of the world they are working on** — deducible from the
  spatial footprint of their queries (`ccodes` / `bounds` / coordinate hints already present in recon
  requests).
- **Snag / give-up detection → proactive help.** From the activity stream, detect users who **stall
  or appear to give up** — e.g. a run that goes quiet part-way, a burst of no-match/rejected results,
  or reconciliation that stops well short of the session's row count — and **offer help by email**.
- **Where it lives.** This is backend/admin work (Django `api` + whatever store backs the tracker),
  *not* part of the Workbench SPA. It reuses existing request logging/analytics where possible.
  Scope, thresholds, and email cadence (avoid nagging) are open (§6). Coordinate with Palak on the
  tracker's data model so the Workbench emits/labels activity in a shape it can consume.

---

## 5. Cross-cutting concerns

- **Auth:** reuse WHG V3 session cookie/token; gateway must accept the logged-in user's credentials;
  no dataset ever persisted server-side. Confirm rate-limit policy per authenticated user.
- **Storage eviction:** `navigator.storage.persist()` + estimate quota (`navigator.storage.estimate()`)
  + surface a "backup your project" nudge when quota is tight. Downloadable `.whgproj` is the safety net.
- **Background throttling:** visible warning during big runs; queue recovers on `visibilitychange`.
- **Performance targets:** import 100k rows without freezing the UI (worker + streaming); table stays
  smooth via windowing; recon throughput bounded by API limits, not the client.
- **Security:** no third-party keys synced anywhere; sanitise derived-column templates (no HTML/script
  injection into the table); CSP for the worker + wasm (onnxruntime-web needs `wasm-unsafe-eval`).

---

## 6. Open decisions (resolve during/after colleague discussion)

1. **Framework vs vanilla.** whg3 is jQuery/webpack. A 100k-row, keyboard-driven, virtualised,
   multi-panel app is at the edge of comfort for vanilla JS. Options: (a) stay vanilla + a small
   virtualiser; (b) add a scoped framework (Preact/Svelte/Lit) *for this bundle only*. **Recommend
   (b) Svelte or Lit** for maintainability, scoped to `workbench.bundle.js`. Needs sign-off.
2. **Where the UI code lives.** Front-end in `whg3` (this plan lives in `whg3/developer/`); the only
   backend touch-points are in `indexing` (gateway + Symphonym `hf/`).
3. **Recon rate limits & auth for high-volume users.** Define per-user throttle; confirm the gateway
   won't reject a legitimate 16k-row (deduped) run.
4. **Symphonym asset hosting.** Where do the ONNX + vocabs live (WHG static, a Release artifact)?
   Versioning vs the server model.
5. **Submission format & path.** Exact LPF profile and how "submit selected rows" maps onto the
   existing whg3 dataset-contribution flow (validation, indexing).
6. **CRS support scope.** How many coordinate reference systems to support out of the box via proj4js
   (WGS84 + common national/projected grids: OSGB, ITM, Lambert, UTM, State Plane, …) vs. letting the
   user paste an arbitrary EPSG code; and how far to auto-detect CRS from the file vs. always prompt.
7. **Workbench-vs-curated-pipeline threshold.** The row-count / geometry-richness / relation-density
   point past which the UI steers a user to the curated authority-ingestion route (`indexing` repo)
   instead of a browser session. Needed so the two routes read as complementary, not competing.
8. **Curation scope & timing.** Is the curation surface (Phase 12) in the first release or a
   fast-follow? Either way, confirm the §2c module boundaries now so nothing needs a rewrite. Also:
   does `whg3` already expose gazetteer-export-to-LPF and a correction/patch intake, or are these new?
9. **Collaboration model (#112).** Group model (reuse Django `Group`s vs a Team/Project model with
   roles); where the shared working copy lives vs the local store and the published index; check-out
   granularity (per-record vs per-column vs subset; optimistic vs hard locks); the conflict/merge
   model and what a "record revision" is. Design with #112, not after it — it qualifies invariant #1.
10. **Onboarding/monitoring thresholds (§4a).** What counts as "started", "stalled", or "gave up";
    email cadence to stay helpful not naggy; and the tracker's data contract with Palak.
11. **Documentation.** Update the public/technical docs in due course to cover the Workbench, the
    onboarding/monitoring behaviour (§4a), and collaboration (#112) — noted here so it isn't
    forgotten; do it as features land, not before.

---

## 7. First implementation steps (once approved)

1. Scaffold the webpack entry + Django route/template + auth gate (Phase 1 skeleton).
2. Stand up Dexie schema + persistence request + "Clear my data" + privacy banner.
3. Worker-based CSV/XLSX/JSON import + auto schema detection + column mapping + virtualised table.
4. `.whgproj` export/import (Phase 2) — lock the resumability guarantee early.
5. Recon worker with dedup + throttle + backoff against the **existing standard `/reconcile`** service
   (Phase 3), using the user's tokenised session.
6. Verify the standard service's hint/property vocabulary covers the Workbench's needs (Phase 6/§4);
   only extend the backend if a genuine gap is found.

---

## 8. References

- Proposal: `WorldHistoricalGazetteer/place#111` (consolidated body; 2026-07-03 meeting notes in
  its comments).
- Collaboration/group editing: `WorldHistoricalGazetteer/place#112`.
- **Standard OpenRefine service (existing):** `whg3/api/reconcile.py`, `api/crc_client.py`,
  `api/urls_root.py`; user docs at <https://docs.whgazetteer.org/content/technical/apis.html>.
- Backend: `indexing/gateway/reconcile.py`, `places.py`, `extend.py`, `es_helpers.py`, `spatial.py`.
- **Curated authority pipeline** (the *other* contribution route): `indexing/authorities/*` (per-source
  scripts, e.g. `alcedo-places.py` incl. corrections/veracity), `processing/ingest_all_authorities.py`,
  `processing/index_namespace.py`.
- In-browser Symphonym technique: `GOTW` repo `WHG-LESSONS.md` §2, `process/export_symphonym_onnx.py`,
  `process/build_symphonym_index.py`; reference preprocessing in `indexing/hf/inference.py`.
- Front-end stack: `whg3/webpack.config.js`, `whg3/package.json`, `whg3/static/webpack/`.
- Interop target: OpenRefine Reconciliation Service API / W3C Reconciliation CG spec.
