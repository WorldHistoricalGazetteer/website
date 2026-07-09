# Plan — Collaborative Collections (browser-first Workbench doc-types)

**Status:** REVIEWED (Stephen, 2026-07-09). Ready to implement in a new session.
**Relates to:** place#111 (Gazetteer Workbench / Map your Data), place#112 (collaborative
editing), place#100 (PLATO — Place Attestation Ontology), place#80 (v4 data model), place#54
(Itineraries `seq`). Supersedes the three pathways on `/workbench/` and, together with Map your
Data (MyD), forms the unified **Collaborative Workbench** targeted for **v3.3**.

> **Note for a coding agent:** `place#NNN` and `place discussion #NNN` refer to issues and
> discussions in the GitHub repo **`WorldHistoricalGazetteer/place`** (e.g. place#111 =
> `https://github.com/WorldHistoricalGazetteer/place/issues/111`). `website` PRs/issues (e.g.
> PR #522) live in `WorldHistoricalGazetteer/website` (this repo, `whg3`).

```{admonition} Cross-cutting requirement — staff/beta gating (non-negotiable)
Everything built under this plan MUST be gated to **staff and invited beta testers**
(`user.can_access_beta` / `is_staff`), exactly as Map your Data is, until v3.3 is deliberately
released to the public. This covers the unified `/workbench/` entry, every doc-type editor and
its endpoints, the "New…" picker, check-out/publish-back, and any new nav links — nothing may
appear or be reachable for regular visitors or logged-in non-beta users. See §2.5. (Published
*outputs* — a Place Collection someone publishes — follow the **existing** publication visibility
rules, unchanged; it is the authoring *tool* and its routes that stay gated.)
```

---

## 0. Decisions locked (this planning session, 2026-07-09)

| # | Decision | Choice |
|---|----------|--------|
| 1 | App architecture | **One Workbench, many doc-types.** Generalise the `workbench` app; MyD reconciliation is doc-type #1. |
| 2 | Editing published data | **Check-out → edit → publish-back.** Server stays authoritative; the Workbench is a collaborative staging editor. |
| 3 | v3.3 scope | **Place Collections + Gazetteer Groups + Itineraries.** **Routes and Networks deferred** — placeholders/sign-posting only (arriving with v4). |
| 3a | Gating (cross-cutting) | **Everything staff/beta-gated** (`can_access_beta`/`is_staff`, 404 to others) until v3.3 is deliberately released. The tool + its routes are gated; published *outputs* follow existing visibility. See §2.5. |
| 3b | Small corrections + large files | Record-level/partial **check-out** + **delta publish-back** so a single-field fix never requires moving a whole gazetteer; streaming/chunked/resumable tooling + capacity guards for genuine large-file moves. See §6.1. |
| 4 | Terminology | Legacy **Dataset → "Gazetteer"**; **Dataset Collection → "Gazetteer Group"**; **Place Collection kept**. **UI display-layer relabel only** in v3.3 (DB values, model names, URLs unchanged). |
| 5 | Publish target | **Into existing models + public pages** (extend `collection.Collection`, existing `/collections/…` browse & URLs). |
| 6 | Teaching groups | The Workbench **Team** model subsumes both `collection.Collaborator` (per-collection roles) **and** `collection.CollectionGroup` (teaching classes). Both become redundant as separate tools; plan their migration/retirement. |

**Guiding principle:** same *feel* as MyD — same page shell, panes, Bootstrap styling, local-first
IndexedDB persistence, lazy-loaded modules, "nothing leaves your browser until you choose", and the
same collaboration substrate (Teams + Yjs/Hocuspocus). Share modules and CSS wherever it fits;
generalise rather than fork.

---

## 1. Vision — what this replaces and extends

`/workbench/` (`main/templates/main/workbench_3col.html`) today offers three server-driven pathways:

1. **Datasets (upload structured place data)** → superseded by **Map your Data** (browser-first).
2. **Dataset Collections (combine multiple datasets)** → **Gazetteer Groups** doc-type.
3. **Place Collections (curate places for teaching/story)** → **Place Collections** doc-type
   (+ **Itineraries** as the sequenced variant).

The new tool keeps the *outcomes* (published collections that appear on the existing public pages)
but replaces the multi-step, server-round-tripping **builder forms** with a single local-first,
collaborative, browser editor — the same one users already meet in MyD. When both ship, the
`/workbench/` page is rewritten to point at the unified Workbench, and the old builder forms are
retired (see §12).

**Forward requirement (drove decision #2):** the Workbench must in due course be the editor for
**already-published** items of *every* kind — gazetteers (legacy datasets), collections, and later
routes/itineraries — not just new drafts. The check-out/publish-back model (§6) is the architectural
answer and must be designed in from the start even where build is phased.

### 1.1 Rationale, and reassurance for the team

Colleagues have recently invested real effort documenting the legacy `/workbench/` pathways and the
Collection/CollectionGroup builders. That work is **not wasted**, and these changes are evolution,
not repudiation. To be explicit for the team:

- **Why change at all.** The legacy pathways are multi-step, server-round-tripping *forms*: upload →
  wait → reconcile → accession, across separate pages, with no local draft, no real-time
  collaboration, and no way to edit something once published without starting over. Map your Data has
  shown that the *same outcomes* are far easier when the whole flow is local-first and in one place.
  Extending that to Collections is the natural next step, not a second system.
- **Nothing breaks for users or their data.** Published collections keep their models, URLs, and
  public pages (decision #5). Old builder URLs **redirect** into the new flows (§12). No data is
  deleted until migrated and verified; every migration is reversible.
- **The documentation migrates, it isn't discarded.** The legacy guides stay live and accurate for as
  long as the legacy flows exist; new guidance is added alongside and only *replaces* a legacy guide
  when its pathway is actually retired. (Their recent write-ups become the basis of the transition and
  redirect copy.)
- **It's phased and gated.** This ships behind the staff/beta gate (§2.5) and rolls out in stages
  (§13), so the team sees and shapes it well before any public switch — and before any outreach or
  teaching materials need updating.
- **One coordinated release, one documentation refresh.** Shipping Collections *with* MyD means the
  Workbench docs and workflow are refreshed **once**, not piecemeal — the specific ask behind the
  "What we're building" / versioning-visibility conversation.

Framing to use with colleagues: *"Same destinations, one much better vehicle — and we drive everyone
there together, with signposts, not a detour."*

---

## 2. Terminology

**v3.3 = display-layer relabel only.** Introduce one centralised label map (template tags + JS
constant) and route all *user-facing* strings through it. **No** DB value, model-name, URL, or API
field changes yet (those are a separate, later migration — see §11 and §14).

| Concept (code / DB — unchanged) | v3.3 user-facing label |
|---|---|
| legacy `datasets.Dataset` | **Gazetteer** |
| `Collection` where `collection_class='dataset'` | **Gazetteer Group** |
| `Collection` where `collection_class='place'` | **Place Collection** (unchanged) |
| sequenced Place Collection (`seq` set) | **Itinerary** |
| (future) route entity | **Route** |

*Assessment of "Place Collection":* keep it. It's clear, place-centric, and already public-facing;
renaming it would churn URLs/docs for no clarity gain. "Group" for the gazetteer aggregation is the
right disambiguator — it avoids a second overloaded "Collection".

---

## 2.5 Access & gating (cross-cutting — applies to every part of this plan)

Restating the header requirement as an implementation checklist, because it is easy to leak a new
feature into the public site by accident:

- **Every** new route/view is decorated `@login_required` + the beta gate (`user.can_access_beta`,
  and `is_staff` where a staff tool), returning **404** (not 403) to non-beta users so the feature's
  existence isn't disclosed — the same pattern as `reconciliation_view`.
- **Every** new nav entry, button, or link (the `/workbench/` rewrite, the "New…" doc-type picker,
  "Edit in Workbench" affordances on published items, dashboard entry points) is wrapped in
  `{% if user.can_access_beta %}` so it never renders for regular visitors/users.
- **New client bundles/chunks** are only referenced from beta-gated templates; a public page must not
  pull a Workbench chunk.
- **Endpoints**, not just pages: the collaboration, checkout, publish-back, and doc-type CRUD APIs are
  all gated (a determined non-beta user hitting the URL directly gets 404).
- **What is NOT gated:** the *published artefacts*. When a user publishes a Place Collection/Gazetteer
  Group, the resulting public page follows the **existing** publication-visibility rules unchanged —
  those pages are already public by design. The gate is on the *authoring tool and its routes*, not on
  what someone chooses to publish through it.
- **Test the gate:** each new endpoint gets a "non-beta → 404" test (as `workbench/tests.py` already
  does for MyD), so gating can't silently regress.
- **Release switch:** public exposure is a single, deliberate later step (drop the gate on the
  Workbench entry + link) — never an incidental side-effect of shipping a sub-feature.

---

## 3. Architecture — one Workbench, many doc-types

### 3.1 Generalise `workbench.WorkbenchProject`

Add `doc_type` to the existing model (place#112 substrate):

```
doc_type ∈ { 'reconciliation',   # Map your Data (existing behaviour)
             'gazetteer_group',   # was Dataset Collection
             'place_collection',
             'itinerary',         # sequenced place_collection (see §4.3)
             'route',             # PLACEHOLDER — creation gated OFF in v3.3 (see §4.4)
             'network' }          # PLACEHOLDER — creation gated OFF in v3.3 (see §4.4)
```

Everything else the project already has stays: `team` (owner/collaborators), JSON `snapshot`,
`version`, `ProjectSnapshot` history, share tokens, `ProjectYDoc` (realtime). The **snapshot schema
varies by `doc_type`** — a discriminated union — but the *envelope* (id, team, version, doc_type,
title, updated) is shared.

### 3.2 Doc-type registry (the extensibility seam)

A single registry (`workbench/doctypes.py`) declares, per doc-type:

- **`snapshot_schema`** — JSON shape + an Ajv/jsonschema validator (client + server share it, as MyD
  already does for LPF).
- **`editor`** — which client editor module mounts (a lazy webpack chunk).
- **`publish_target`** — the server function that writes the snapshot into the canonical model(s) and
  triggers indexing (§9).
- **`checkout_loader`** — the server function that materialises an existing published item into a
  snapshot for editing (§6).
- **`label`** — display name via the §2 map.

This mirrors how MyD already lazy-loads per-feature chunks; adding a doc-type = one registry entry +
one editor chunk + one publish/checkout pair. Route ships as a registry entry with `enabled=False`.

### 3.3 Shared-vs-new inventory (reuse map)

| Layer | Reuse from MyD | New for Collections |
|---|---|---|
| Page shell / panes / CSS | `reconciliation.css`, pane structure, header (Documentation + Take-a-tour buttons) | doc-type-specific pane bodies |
| Local persistence | IndexedDB project store, `.whgproj` save/load, autosave, "Clear my data" | snapshot schema per type |
| Collaboration | Teams, `ProjectYDoc`, Hocuspocus, share links, presence (place#112) | — (unchanged) |
| Place-adding | reconcile engine, WHG search, NER extractor, map draw, containment | "add places" panel wiring |
| Map | MapLibre map pane, draw controls | route/itinerary path rendering |
| Export | LPF / CSV / JSON serialisers, citation & CRediT builder | collection/itinerary serialisers |
| Analytics | Plausible funnel events (`MyD:` → generalise to `WB:`) | per-doc-type events |

---

## 4. Doc-types in detail

### 4.1 Place Collection
A **curated set of places** with narrative/annotations (teaching, storytelling, thematic).
- **Snapshot:** `{ title, description, image, places:[{id, note, relation[], seq?, when?}], keywords, license, citation }`.
- **Add places by:** WHG search/reconcile (reuse MyD), NER-from-text (reuse), map draw/click, paste a
  list → reconcile, or **import an existing Place Collection** (check-out, §6).
- **Editor panes:** (1) Collection metadata; (2) Places table (add/search/annotate/reorder);
  (3) Map (preview + spatial add); (4) Narrative/annotations (relations, notes, `when`); (5) Scope
  (keywords, license, citation — reuse MyD citation+CRediT); (6) Collaborate & share; (7) Publish.
- **Publishes into:** `Collection(collection_class='place')` + `CollPlace` (place, `sequence`) +
  `TraceAnnotation` (relation/note/when). All already exist.

### 4.2 Gazetteer Group  *(was Dataset Collection)*
An **aggregation of published gazetteers** for comparison/analysis.
- **Snapshot:** `{ title, description, gazetteers:[{dataset_id}], keywords, license, citation, display opts }`.
- **Add gazetteers by:** searching/selecting published gazetteers the user can access.
- **Editor panes:** metadata; member gazetteers (add/remove/order); combined map preview; scope;
  collaborate; publish.
- **Publishes into:** `Collection(collection_class='dataset')` + `datasets` M2M. Existing.

### 4.3 Itinerary  *(sequenced Place Collection — place#54)*
place#54: *"Following reconciliation, a Place Collection would be automatically generated with `seq`
annotations."* So an Itinerary is **not a new storage model** — it is a **mode of Place Collection**
where sequence is meaningful.
- **Model as:** `doc_type='itinerary'` sharing the Place-Collection snapshot, with `seq` required and
  a **route-line preview** connecting the ordered places (client-side polyline; not a stored Route).
- **Editor delta over Place Collection:** ordered list with drag-reorder; per-leg annotations (via
  `TraceAnnotation`); "generate from a reconciled MyD project" entry point (an MyD dataset with row
  order → an itinerary), realising place#54.
- **Publishes into:** `Collection(collection_class='place')` with `CollPlace.sequence` populated +
  leg `TraceAnnotation`s. Existing. (An itinerary *is* a Place Collection to the public site; the
  sequence drives the ordered/animated presentation.)

### 4.4 Route & Network  *(PLACEHOLDERS — arriving with v4)*
PLATO place#100 §6: *"A route is not merely a collection of places. It is a referential entity with
its own attestations."* The same discussion lists **Networks** as a Thing type **alongside Routes**
(Settlements · Regions · Polities · **Routes** · **Networks** · Physical features · …).

- A **Route** is a first-class Thing: name, dates, attestations, a (typically linear) geometry, and
  ordered references to constituent places.
- A **Network** is likewise a first-class, attested Thing, but **topological rather than linear** — a
  set of nodes (places) and edges (relations) with its own identity and attestations (e.g. a trade
  network, a monastic or correspondence network). A route is, in effect, a degenerate (linear) network.

**Assessment (why Network is a v4 placeholder, not an Itinerary variant — resolves the open question
in Stephen's #1):** an Itinerary is *composition* — an ordered set of existing places, no new entity,
no new architecture (it's a sequenced Place Collection, §4.3). A Network, like a Route, is a **new
referential entity with its own attestations and an edge/topology model** the current places/index
pipeline can't represent. So Network aligns with **Route**, not Itinerary, and needs the same v4
graph-model work (place#80). It therefore gets a placeholder, **not** a v3.3 build.

**v3.3 deliverable for BOTH = signposting, not implementation:**
- Registry entries `route` and `network`, both `enabled=False`.
- Disabled **"Route"** and **"Network"** tiles in the "New…" doc-type picker, each with a **"Coming
  with v4"** badge + tooltip linking to the v4/PLATO explanation.
- A short **`developer/plan-routes-networks-v4.prompt.md`** stub capturing the model options assessed
  here (new first-class `Route`/`Network` entities vs place/Thing-as-graph-node; edge/topology
  representation) so the v4 decisions are teed up, not lost. Cross-references place#80 + place#100.
- **Data placeholders:** reserve `doc_type ∈ {'route','network'}` and nullable `published_route_id` /
  `published_network_id` on `WorkbenchProject`, so no migration is needed when v4 lands.
- Keep the existing one-line mentions on the `/development/` "What we're building" page.

---

## 5. Backend data-model changes

1. **`workbench.WorkbenchProject`**
   - `+ doc_type` (CharField, choices, default `'reconciliation'` for back-compat).
   - `+ published_target` generic pointer(s): reuse/extend the existing `published_dataset_id`
     concept to also record a published `Collection` id (and a reserved `published_route_id`).
   - Snapshot validation dispatches on `doc_type` (registry, §3.2).
2. **`collection.Collection`** — *no schema change required to publish into it*; extend server-side
   `publish_*` functions. Confirm `status`/`sandbox` handling and re-index hooks fire on publish-back.
3. **Teaching/collaboration convergence (decision #6):**
   - `collection.Collaborator` (per-collection roles) → **superseded** by `TeamMember`. Keep the table
     read-only for legacy collections; new collaboration goes through Teams.
   - `collection.CollectionGroup` (teaching classes) → **superseded** by Teams. A class becomes a
     **Team**; students are members; submitted collections become **team-owned Workbench projects**
     (or published Place Collections owned by the Team). Plan a **migration** that creates a Team per
     existing CollectionGroup and maps memberships; retire the CollectionGroup builder UI. Keep the
     data until migrated. (See §12.)
4. **Route & Network placeholders:** `doc_type ∈ {'route','network'}` reserved + nullable
   `published_route_id` / `published_network_id` columns; no Route/Network models yet (v4, §4.4).

No destructive migrations in v3.3 — additive columns + label map only.

---

## 6. Check-out → edit → publish-back (the published-data editing model)

The pillar that lets the Workbench edit *already-published* items without abandoning local-first.

**Flow:**
1. **Check-out.** From a published item (a Gazetteer, a Collection), "Edit in Workbench" calls the
   doc-type's `checkout_loader`, which serialises the canonical server record → a Workbench snapshot,
   creates a `WorkbenchProject` (team-owned) marked with `source_published_id` + `base_version`
   (the server version/hash at checkout), and hands it to the browser editor.
2. **Edit** locally + collaboratively, exactly like a new draft (IndexedDB, offline, Yjs).
3. **Publish-back.** Serialise snapshot → canonical model via `publish_target`, **guarded by an
   optimistic check** against `base_version` (did the server copy change under us?). On conflict,
   surface a diff/merge (reuse MyD's three-way `merge_snapshots` where the shapes allow) rather than
   clobbering. On success: write-through + **re-index** (ES/gateway) + bump server version + snapshot
   the new baseline.

**Authority:** the published server record remains the single source of truth; the Workbench project
is an explicit, versioned working copy. This preserves "nothing changes upstream until you publish".

**Phasing (within v3.3):** build new/draft authoring first (all doc-types), then enable check-out for
**Place Collections** (self-owned data, lowest risk), then Gazetteer Groups, then legacy Gazetteers
(highest blast radius — needs the re-index path proven). Ship check-out behind a capability flag so
draft authoring can release even if published-editing needs another iteration.

### 6.1 Small corrections & large files (a first-class use case that must not require moving everything)

Contributors have long asked to make **small corrections to published material** — often a single
field in a single record (a fixed date, a corrected coordinate, a typo in a name). The check-out
model enables this, but a naïve implementation would force a **full round-trip of the entire
gazetteer** (WHG → MyD/browser → WHG) just to change one cell — infeasible for large datasets (tens
or hundreds of thousands of records won't fit comfortably in IndexedDB, and a full re-upload +
full re-index to fix one field is wasteful and risky). The plan must therefore include:

- **Record-level / partial check-out.** Fetch only what's being edited — a single record, a filtered
  subset, or a bounded page — not the whole dataset. A "correct this record" affordance on a published
  place opens the Workbench scoped to that record (and its immediate context), with a lightweight
  snapshot. Full-dataset check-out remains available but is the exception, not the default.
- **Delta / patch publish-back.** Send **only changed records** (a diff against the checkout baseline),
  not the whole file. The server applies the patch, validates just the changed records, and does a
  **targeted re-index** of the affected records — not a full dataset re-accession. This keeps a
  one-field fix cheap and fast, and is what makes routine small corrections actually usable.
- **Robust large-file tooling for when a full dataset *is* moved** (bulk edits, re-reconciliation):
  - **Streaming, chunked, resumable transfer** both ways (avoid a single giant JSON payload;
    resume on flaky connections). Reuse/extend the existing validation ingest which already streams
    features with `ijson` in batches.
  - **Capacity awareness in the browser:** check `navigator.storage.estimate()` before a large
    check-out; warn/paginate rather than silently failing when IndexedDB can't hold it; prefer
    partial check-out above a size threshold.
  - **Server-side batching + progress:** reuse the Celery batched-insert + Redis progress pattern from
    the current accessioning pipeline for large publish-backs; show progress in the Workbench.
  - **Backpressure & limits:** explicit size caps with clear messaging (mirrors MyD's existing caps),
    and a "this dataset is too large to edit wholesale — edit records or a subset instead" guardrail.
- **Integrity:** every partial/delta publish-back is still guarded by the optimistic `base_version`
  check (§6) at record granularity, so concurrent edits to *other* records don't block a correction,
  but a conflicting edit to the *same* record surfaces a merge rather than clobbering.

**Design note for implementation:** treat record-level correction as the *primary* published-editing
path and full-dataset check-out as the heavyweight fallback — most real demand is "fix one thing",
and designing for that first keeps the common case cheap and the rare case merely possible.

---

## 7. Collaboration (reuse place#112 wholesale)

No new collaboration machinery. Team-owned projects, `ProjectYDoc` + Hocuspocus realtime, share
links, presence, three-way merge fallback — all already built for MyD collaboration. Collections
projects are just more `WorkbenchProject`s of a different `doc_type` under the same Team. This is
also *why* `Collaborator`/`CollectionGroup` become redundant (§5.3): one collaboration model for
everything.

---

## 8. Adding places (shared toolkit) — a deliberate conceptual shift

A Place Collection / Itinerary needs places; reuse MyD's engines rather than a new picker:
- **Search & reconcile** — the `/reconcile` + WHG search path (with the new gazetteer-assisted,
  collocationally-disambiguated NER matcher available as a bulk "paste text → add located places").
- **NER from text** — the shipped extractor becomes an *input method* for collections too.
- **Map draw/click** — add a place by clicking the map (reverse-lookup to a WHG place) or drawing.
- **From an existing item** — import/duplicate a published Collection (via check-out).
Each yields a place reference `{id, repr_point, ccodes}` — the shape the NER matcher already returns.

### 8.1 The conceptual shift — flag and assessment

**This is a genuine rethink of how Place Collections are built, and it closes much of the gap between
*curating a Place Collection from already-indexed places* and *crafting a new Gazetteer*.** The
legacy Place Collection Builder is a search-and-add form over the existing index; MyD's toolkit
(search + reconcile + NER + map + import) makes "assemble a set of places" and "author place data"
feel like the *same activity* in the *same tool*. That convergence is desirable — but it needs one
clear line held, or the two concepts blur into incoherence:

- **Place Collection = references to places that EXIST in WHG.** Every member resolves to an indexed
  place id. This keeps a Place Collection a *curation* (interpretation, pedagogy, narrative) over the
  shared index — its historical meaning, preserved.
- **Gazetteer = authoring NEW place records.** Creating places WHG doesn't yet hold is the Gazetteer
  (MyD → contribute) path, not the collection path.
- **Where they meet (the useful part):** when a user adds a place that *isn't* in WHG (drawn on the
  map, or an NER name with no gazetteer match), the tool doesn't dead-end — it offers **"contribute
  this as a gazetteer record first, then add it to your collection"**, i.e. it routes them through the
  Gazetteer path and back. So the collection stays reference-based, but the *experience* of "I found a
  place that should exist" is smooth rather than a wall. (Sensible default in v3.3: allow only indexed
  places in a Place Collection; surface the contribute-then-add bridge for unmatched additions —
  building the bridge itself can be a fast-follow.)

**Assessment: the rethink makes good sense** provided the "references vs authoring" line above is
explicit in the UI and data model. The risk to avoid is silently letting Place Collections accrue
free-floating, unreconciled "places" that aren't in the index — that would fork place identity and
undermine reconciliation. Holding the line (collection members are always resolved ids) keeps the
convergence a UX win rather than a data-model regression, and it lines up cleanly with the v4 view
where *everything is a Thing*: a collection references Things; a gazetteer/route/network *is* a set of
Things. Worth a short **explicit design note in the implementation session** confirming the
member-must-be-indexed rule and the contribute-then-add bridge.

---

## 9. Publishing & public pages (decision #5)

Publish-back writes into the **existing** `Collection` model, so the current public experience is
preserved with **no new browse code**:
- Place Collection / Itinerary → `/collections/<id>/browse_pl` (existing).
- Gazetteer Group → existing dataset-collection browse.
- URLs, citation (`citation_csl`), carousels, feature-collection endpoints — unchanged.
- Re-index on publish so search/index reflect the new/edited item.
Public labels shift via the §2 map (e.g. "Dataset Collection" → "Gazetteer Group") without changing
the underlying routes.

---

## 10. UI/UX — the "feel"

- **Entry:** the unified Workbench landing (rewritten `/workbench/`) shows a **"New…" doc-type
  picker** (Place Collection · Gazetteer Group · Itinerary · Map your Data · Route[disabled]) plus
  **"Your projects"** and **"Edit published…"** (check-out). Same header pattern as MyD, incl. the
  **Documentation** and **Take a tour** buttons.
- **Editor:** the MyD numbered-pane layout, re-used shell + Bootstrap styling. Panes differ by
  doc-type (§4) but the chrome, autosave badge, collaborate/share, backup/clear, and privacy banner
  are identical.
- **Tours & docs:** one tour per doc-type (reuse `recon-tour` framework); a documentation button on
  each (mirroring MyD's) pointing at the relevant guide. **Documentation location (v3.3):** because
  the whole Workbench is forthcoming/beta-gated, its guides live in the **Development Roadmap → "v3.3:
  Collaborative Workbench"** docs section (`content/v3-3/…`), *not* in Guides & Tutorials — the same
  move already applied to `map-your-data.md` (now `content/v3-3/map-your-data.html`). Write a full
  `content/v3-3/collections.md` guide there. It must be authored so a **simple move** into
  `content/guides/` (updating the toctree + the in-app doc links) is all that's needed when v3.3
  ships — keep cross-refs relative and avoid roadmap-only framing in the body.
- **Consistency:** reuse the citation & CRediT builder, scope/keywords/license controls, validation
  gating, and export panel verbatim.

---

## 11. Terminology relabel — implementation

- **Server:** a `main/labels.py` map + a template tag `{% label 'gazetteer_group' %}`; replace
  hard-coded "Dataset"/"Dataset Collection" user-facing strings across templates (inventory from the
  grep list: `workbench_3col.html`, `dashboard_user.html`, `build*.html`, `builder.html`,
  `people_overview.html`, `base_webpack.html`, choices display labels).
- **Client:** a JS `LABELS` constant (single source, mirrors server) for Workbench UI strings.
- **Do NOT** change `COLLECTIONCLASSES` DB values, model names, URLs, or API fields in v3.3.
- Schedule the **deep rename** (tables/routes/API/docs, with redirects + back-compat) as its own
  planned migration post-v3.3 (§14).

---

## 12. Migration & retirement

1. **`/workbench/` pathways page** — rewrite to launch the unified Workbench; remove the three
   builder-form pathways once the doc-types cover them. Keep deep links working (redirect old builder
   URLs to the Workbench "New …" flows).
2. **Legacy builder forms** (Place Collection Builder, Dataset Collection Builder) — retire after the
   Workbench editors reach parity; redirect.
3. **`CollectionGroup` (teaching)** — migrate each group → a Team; memberships → `TeamMember`;
   retire the CollectionGroup builder + teaching-portal "Place Collection Groups" tool. Update
   `/teaching/` copy to describe collaborative Teams instead. **Interrogate before executing** — this
   touches instructors' live classes.
4. **`Collaborator`** — freeze for legacy; new collaboration via Teams only.
Nothing is deleted until migrated and verified; all reversible.

---

## 13. Phasing within the v3.3 coordinated release

- **P1 — Foundation:** `doc_type` + registry + doc-type picker + shared editor shell; Place
  Collection authoring (new/draft) end-to-end → publish into `Collection`. Label map.
- **P2 — Breadth:** Gazetteer Group authoring; Itinerary mode (sequencing + leg annotations +
  MyD→itinerary entry point).
- **P3 — Published editing:** check-out/publish-back for Place Collections, then Gazetteer Groups,
  then legacy Gazetteers (behind a capability flag; re-index proven).
- **P4 — Convergence:** CollectionGroup→Team migration; `/workbench/` + `/teaching/` rewrites;
  builder-form retirement; Route + Network placeholders + `plan-routes-networks-v4` stub.
- **Docs/roadmap:** `content/v3-3/collections.md` (in the v3.3 Roadmap section; ready to move to
  `content/guides/` on release); update `/development/` cards; changelog.

MyD ships in the same coordinated release so the old Workbench documentation/workflow retires in one
step (the motivation behind bundling — addresses Ruth's versioning-visibility concern).

---

## 14. Open questions / v4 forward-hooks (flag before/at implementation)

- **Route & Network models (v4):** first-class `Route`/`Network` entities vs place/Thing-as-graph-node,
  and how to represent a network's edge/topology — deferred; teed up in `plan-routes-networks-v4`.
  Aligns with place#80 graph model + PLATO place#100 (which lists Routes and Networks as Thing types).
- **Deep terminology rename:** timing + redirect/back-compat strategy for renaming DB/URL/API to
  Gazetteer/Gazetteer Group (post-v3.3).
- **Itinerary as its own public presentation** (animated/sequenced) vs plain ordered Place
  Collection — how much bespoke rendering, if any.
- **CollectionGroup migration UX** for instructors mid-term — needs Stephen's steer (§12.3).
- **Access/permissions** for check-out of legacy Gazetteers owned by others (who may edit what).

---

## 15. Files (indicative)

**New:** `workbench/doctypes.py`; `workbench/checkout.py`; `workbench/publish.py`;
`whg/webpack/js/wb-*.js` editor chunks (`wb-place-collection.js`, `wb-gazetteer-group.js`,
`wb-itinerary.js`); `main/labels.py` + label templatetag; `content/v3-3/collections.md` (docs repo,
in the v3.3 Roadmap section); `developer/plan-routes-networks-v4.prompt.md`.
**Edited:** `workbench/models.py` (`doc_type`, published pointers, migration);
`workbench/views.py`/`urls.py` (project create by doc_type, checkout/publish endpoints);
`whg/webpack/js/reconciliation.js` → factor shared shell into a `wb-shell` module; templates for the
label map + `/workbench/` rewrite; `collection/*` publish/checkout adapters; `webpack.config.js`
(new entries/chunks).
**Migrations:** additive `doc_type` + pointer columns; CollectionGroup→Team data migration (P4).

---

## 16. Testing & rollout

- Backend: doc-type registry validation; publish-back into `Collection` (+ re-index); checkout
  round-trip (published → snapshot → edit → publish-back preserves/updates correctly); optimistic
  conflict on concurrent server change; CollectionGroup→Team migration idempotency. (Throwaway
  PostGIS pattern as used for `workbench`/`validation` tests.)
- Client: `node --check` + build; live smoke of each doc-type via Claude-in-Chrome; collaboration
  two-profile check (reuse place#112 verification).
- Rollout: same as MyD — commit source+bundles, `deploy prod restart` (+`--migrate` for the columns,
  +`--celery` if publish/index task code changes); beta-gated behind `can_access_beta`; verify live.
