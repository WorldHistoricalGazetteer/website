# Audit: every public promise attached to "v4"

2026-09-28. Prompted by SG's proposal to renumber the platform formerly called v3.5 as **v4**, now
that the ArangoDB re-architecture is retired (place#301). The audit finds every place where "v4"
promises something, and proposes for each whether the new v4 delivers it, retargets it, or drops it.
Nothing has been changed yet.

## 0. The premise needs one decision first: which release is 4.0?

"v3.5" is used as an umbrella for the platform, but the site schedules the beta work as a
**sequence** of releases:

| Card on `/development/` (`main/views.py`) | Target |
|---|---|
| Map your Data; Collaborative Workbench; Browser-first Collections; Suggest a correction (beta) | 3.3 |
| Citations & contributor credit (dev) | 3.3 |
| Submission tracker, GRACE (dev); Open Metadata Exchange (horizon) | 3.4 |
| Atlas (beta); Standardised place types, Getty AAT (dev) | 3.5 |
| Lesson plans on ISKME's platform (horizon) | 3.6 |
| "WHG v4: a graph data model" (horizon) | 4.0 |

Other version facts:
- `VERSION` on both `staging` and `main` reads **3.2** (last changed 2025-10-29).
- The latest GitHub release is **v3.2.1 "Citable release"** (2026-07-29).
- Publishing a GitHub release rewrites `VERSION` (`.github/workflows/update-version-on-release.yml`),
  and a release is also what archives a new version to Zenodo. **Tagging `v4.0.0` is a public,
  citable act.**
- The site's own release note already plans the beta modules as "a single coordinated release"
  (`main/views.py`, `release_note`, currently "targeted for v3.3").

**Proposal.** 4.0 is that coordinated public release: Map your Data, Workbench, Collections,
Suggest a correction, Citations and Atlas, plus PLATO import/export. Everything now targeted 3.4–3.6
becomes 4.x, or joins 4.0 if it is ready. There is no 3.3–3.6 release. Until the tag, the site says
"4.0 beta".

## 1. What "v4" currently promises, by substance

| # | Promise | Where it is made | Proposal |
|---|---|---|---|
| A | **A graph / attestation data model (PLATO)** | `/development/` card "WHG v4: a graph data model"; docs `v4/data-model/*`; discussions #80 and #100 | **Deliver in 4.0** in the form now decided: PLATO as a first-class input/output format, with the attestation model held in Postgres. Reword the card from "a re-architecture" to "the data model, delivered on the existing platform". |
| B | **Routes and Networks as first-class entities** | Workbench tiles "Coming with v4"; Collections card; docs `v3-3/collections.md`; place#111 | **Retarget to "a later 4.x release"**, following SG's ruling that they may be built if the infrastructure allows. Do not leave "Coming with v4" live on a 4.0 site. |
| C | **The curated place entity (PLATO `Thing`, now `SpatialEntity`) and persistent, citable identifiers** | `/development/` card "Stable links…" ("genuinely persistent identifiers are part of the v4 work") and the v4 card ("identifiers that are stable enough to cite"); place#172, #170; discussion #100 | **Delivered** (SG decision 2): `SpatialEntity` identifiers exist via w3id.org, in the API and the beta modules. At release, reword the cards to say so. |
| D | **Re-architecture onto ArangoDB** | docs `v4/architecture/database.md` (addendum now in place); `v4/data-model/implementation.md` ("Implementation in ArangoDB", 819 lines); 7 other data-model pages that mention ArangoDB | **Drop.** Covered by place#301. `implementation.md` is the largest single item. |
| E | **Migration to Pitt Kubernetes** | docs `v4/architecture/kubernetes.md`, `technical-administration.md`, `service-configuration.md`, `v4/system-architecture.md`; docs `technical/repositories.md:15-18` ("WHG PLACE (v4)… forthcoming Version 4"); place#62 | **Drop / archive.** The `place` repo's own description already says "Abandoned Pitt/Kubernetes migration config". |
| F | **A launch date** | docs `v4.md:3` "expected to launch by mid-2026" | **Replace** with the real target once 4.0 is dated. The stated date has passed. |
| G | **Suggestions as the "seed" of v4 attestations** | `/development/` card "Suggest a correction…" ("a first, small step towards the attestation model planned for v4"); code comments | **Reword.** In 4.0 it *is* part of the delivered model, not a step towards it. |

## 2. Inventory by surface

### 2a. Visible on the live site (whg3)

| Location | Current text | Proposed |
|---|---|---|
| `main/templates/main/workbench_new.html:74` | badge "Coming with v4" (Route, Network tiles) | "Planned" (or "Coming in a later release") |
| `main/templates/main/workbench_new.html:65` | tooltip "…arriving with WHG's v4 data model" | "…a first-class historical entity, planned for a later release" |
| `main/views.py:914` (Collections card) | "(Routes and Networks arrive with the v4 model.)" | "(Routes and Networks are planned for a later release.)" |
| `main/views.py:920` (Suggest card) | "a first, small step towards the attestation model planned for v4" | "part of WHG's attestation model: every change is a reviewed, sourced claim" |
| `main/views.py:1022` (Stable links card, shipped 3.2) | "genuinely persistent identifiers are part of the v4 work" | per decision C: "…are planned for v4.x" or "…arrive in 4.0" |
| `main/views.py:1071-1075` (v4 card) | "A re-architecture around a graph model… also where places get identifiers…" | rewrite per A and C; stage becomes beta/dev, not horizon |
| `main/views.py` card versions | 3.3 / 3.4 / 3.5 / 3.6 | renumber per §0 |
| `main/views.py` card text | "Targeted for v3.4" (OME, GRACE); "expected after v3.5" (lesson plans) | renumber |
| `main/views.py` `release_note` | "targeted for v3.3" | "targeted for 4.0" |
| `VERSION` | 3.2 | set by the `v4.0.0` release (workflow), not by hand |

### 2b. Code comments only (not user-visible, low priority)

- `main/labels.py:34`
- `main/views.py:528`
- `workbench/doctypes.py:9,184`
- `workbench/models.py:33-34,140,145-146,193-195`
- `workbench/suggestions.py:8,11`
- `workbench/views.py:114`

These say "v4 placeholder" for Route/Network, and "v4 attestations". They are accurate as history, and "PLACEHOLDER (planned)" would outlive the renumbering. There is no migration impact: the reserved values stay.

### 2c. Documentation (docs.whgazetteer.org)

| Location | Issue | Proposed |
|---|---|---|
| `roadmap.md` | toctree: "v3.3: Collaborative Workbench", "Atlas", "v3.5: Toponym Phonetics", "V4: Graph Datamodel" | restructure: "v4.0" (Workbench, Collections, Atlas, Phonetics, PLATO I/O), then "later 4.x" |
| `v4.md` | "Blueprint for WHG Version 4, expected to launch by mid-2026" | becomes the v4 landing page for the *real* v4; the blueprint material moves under a "2025 design" heading |
| `v4/user-guide/*` (~5,000 lines, last revised 2025-10-19) | describes a product with **no Atlas, Map your Data or Workbench**; `route-tutorial.md` (706 lines) teaches a feature that does not exist | **the biggest item.** Archive as "v4 design (2025), not current". The real v4 user guide grows from `v3-3/*`, `atlas.md` and `phonetics.md` |
| `v4/data-model/*` | the model: still valid, but several pages name ArangoDB, and `implementation.md` is entirely ArangoDB | keep the model; retire `implementation.md`; add PLATO as the formal expression (none of these pages mentions PLATO) |
| `v4/architecture/*` | ArangoDB (addendum added) and Kubernetes | archive, except database.md with its addendum |
| `v4/teaching-resources.md` | check against the lesson-plans (ISKME) timeline | review |
| `v3-3/collections.md:30,43` | "Coming with v4" for Route/Network | match 2a |
| `technical/repositories.md:15-18` | "WHG PLACE (v4)… forthcoming Version 4" | the `place` repo is the issue tracker and archived K8s config, not v4 |
| `staff/atlas-v3-5.md` | name only | rename when convenient |
| folder names `v3-3/`, `v4/` | URLs are linked from the site (e.g. `_COLLECTIONS_DOC` → `content/v3-3/collections.html`) | keep the paths (links in the wild); change titles, not URLs |

### 2d. GitHub

| Location | Current | Proposed |
|---|---|---|
| `website` repo description | "Version 3 beta" | "World Historical Gazetteer platform (v4)" at release |
| `place` repo description | "Abandoned Pitt/Kubernetes migration config…" | it is also *the* issue tracker for all of WHG, so say so |
| `website` milestones | "v3 alpha", "v3 beta", "v3 beta+" (all empty) | close; add "4.0" if milestones are wanted |
| place#172 (loci) | "v4 is hoped to begin within a year, possibly longer" | comment: v4 is now the platform release; `Thing` target per decision C |
| place#170 (Wikidata) | "the v4 data model reintroduces a place entity" | same as #172 |
| place#111 (Workbench) | "Routes & Networks… Coming with v4" | comment: retargeted per B |
| place#62 (tilesets) | "Kubernetes-based (v4) codebase" | close or retitle; K8s abandoned |
| place#25, #32 | "eventual v4 Assertion / PLACE graph" | still true of the model; a one-line note suffices |
| place#96 | links to the v4 data model docs | fine |
| discussions #80, #98, #100, #168 | define v4 as the model | historical record; pin one explanatory comment on #80 pointing at place#301 and this renumbering |
| Releases | v3.2.1 latest; next is v4.0.0 | the release notes must say what 4.0 is *and is not* (B, C), because this is the Zenodo-archived statement |

### 2e. Outside WHG's control

- **PLATO README**, "Relationship to the WHG v4 data model": "PLATO formalises the data model developed
  for WHG v4". It stays true under the renumbering, because the model is what v4 now carries.
- **LPF discussion #53** refers to PLATO and WHG, not to v4 as a release. Nothing to change.
- **Talks, grant reports and slides** that described v4 as the graph re-architecture with PIDs: SG to
  judge. The safe line is "v4 delivers the v4 data model on the existing platform; Routes, Networks
  and [C] follow in 4.x".

## 3. SG decisions (2026-09-28)

1. **4.0 = the coordinated beta-modules release.** There are no 3.3–3.6 releases.
2. **C is already met.** PLATO replaced `Thing` with **`SpatialEntity`** (PLATO 0.3.0). The
   identifiers of these entities already exist via **w3id.org**, and they are surfaced in the API
   and in the beta modules. So the promise is *delivered*, and every mention of `Thing` becomes
   `SpatialEntity`.
3. **The v4 user guide starts again** from the v3.3, Atlas and phonetics docs, and the whole
   "Development Roadmap" section is restructured:
   - **v4.0:** Collaborative Workbench, Atlas, Phonetics.
   - **v4.1:** OER, if it is not ready in time for v4.0.
4. **Everything changes only at the release.** The beta modules are v4 features in substance, but
   nothing needs to say so before launch. **v4 launch: early 2027**, after months of beta testing
   with a hired tester. Doc changes are therefore prepared on a branch and merged at the release.

## 4. GitHub audit: issues and discussions across the WHG organisation (2026-09-28)

**Scope.**
- 21 repos. Issues are enabled on 16 and discussions on 3.
- All 389 threads were fetched, open and closed, with every comment and reply. No thread hit a
  fetch limit.
- Scanned for: v4, ArangoDB/AQL, Vespa, Kubernetes/Helm, PLATO `Thing`, version targets 3.3–3.6,
  and past-due dates.

**Result.**
- 59 threads matched, 34 of them open.
- Discarded as false positives: #141 ("3.5–5 days"), #214 ("3.5 kB"), #246 ("3.3 PB"),
  #249 ("3.4×").
- The 25 matching **closed** threads are the historical record. **No action.**
- Only `place` has open matches.

### 4a. Closed 2026-09-28: the superseded 2024–25 Vespa / Kubernetes / Neo4j plan

This is housekeeping, and it does not mention v4. Close as *not planned*, with one line each
pointing at the `place` README banner (Kubernetes abandoned, Vespa superseded by ES) and at
place#301.

| # | Title | Last touched |
|---|---|---|
| 2 | Update/delete tileset on Dataset/Collection edit ("…on the Kubernetes tileserver pod") | 2025-01-01 |
| 5 | Vespa API | 2025-01-08 |
| 6 | Embedding API (superseded by Symphonym in ES) | 2025-01-07 |
| 11 | Ingestion Pipeline (Vespa staging; its "Contribution Formats" proposal is overtaken by PLATO/LPF I/O) | 2025-02-25 |
| 15 | Neo4j Graph Store (path-finding; see SG's ruling on networks in place#301) | 2025-02-15 |
| 26 | Kubernetes/Vespa Migration Plan | 2025-10-03 |
| 29 | Exodatasets & Vespa Indexes | 2025-02-25 |
| 55 | Vespa Query does not support `namespace` filtering | 2025-03-10 |
| 57 | Fix `_locate_by_bbox` (vespa/…/processor.py) | 2025-03-19 |
| 72 | Services & Ingress Management (K8s/Contour) | 2025-07-25 |
| 79 | Migration to ArgoCD for Continuous Deployment (Helm) | 2025-10-03 |
| 62 | Dataset Tileset Deprecation ("…removed from Kubernetes-based (v4) codebase") | 2025-05-03. **Close, unless tileset deprecation is still wanted in tileboss; SG to say.** |

### 4b. Delivered: #10 and #27 closed 2026-09-28

For #27, the `metrics_` tables were dropped on staging and main, with the leftover migration rows, content
types and permissions. Backups are in `/mnt/salvage/whg-backups/metrics-backup-*`.

| # | Title | Evidence |
|---|---|---|
| 10 | Reconciliation Service API | Shipped. Release v3.2 is titled "ORCID, Reconciliation API, and PeriodO Filters" |
| 27 | Consider Plausible Analytics | Plausible is self-hosted and live, with the analytics dashboard shipped. **Check first:** the last comment leaves "remove `metrics_` tables" and "change PLAUSIBLE_BASE_URL" unticked |

### 4c. At the v4 release: retarget or reword (open, current work)

| # | Stale reference | At release |
|---|---|---|
| 111 Workbench | "Routes & Networks… 'Coming with v4'"; "public v3.3 release" | "4.0 release"; Routes/Networks in a later release |
| 137 i18n | "v3.3 Atlas release" | Atlas is 4.0; i18n has no target release (as the roadmap card already says) |
| 142 AAT mapping UI; 286 contributed fclasses | "in Atlas (v3.5?)" | Atlas is 4.0 |
| 170 Wikidata push | "v4 data model reintroduces… PLATO's `Thing`"; "before v4" | `SpatialEntity`; identifiers exist via w3id.org, so **reassess whether #170 can proceed now** rather than waiting on v4 |
| 172 Locus identifiers | "v4's PLATO `Thing`"; "when v4 delivers `Thing`"; "v4 is hoped to begin within a year"; "#170 largely waits on v4" | `SpatialEntity`; w3id identifiers exist; drop the timeline |
| 25, 32 | "eventual v4 PLACE graph"; "minimal v4 Assertion" | one line: the model is PLATO, delivered in 4.0 as I/O |
| 93, 109 | "dual store for v3.5"; "transition to v3.5 indices" | historical names for the new index. Leave, or add a one-line gloss |
| 96 | links to the v4 data-model docs | none |

### 4d. At the v4 release: open discussions

| # | Stale reference | At release |
|---|---|---|
| 80 v4 Data Model | "Outline of a Version 4 Release planned for 2026" | pinned comment: v4 is the platform release (early 2027); the model is PLATO; the store is per place#301 |
| 98 Rethinking Contribution… | "a lightweight vertex in ArangoDB"; `Thing` | one comment: store per #301; `Thing` → `SpatialEntity` |
| 100 PLATO – Clarifying Questions | "§8 Elasticsearch vs Graph DB… Arango absorbs graph"; `Thing` ×10 | same comment. §8's open question is answered by #301 |
| 168 five kinds of "region" | "the v4 model and the Atlas" | still true; none |

### 4e. Other surfaces found on the way

- **`place` README.** It correctly says Kubernetes is abandoned and Vespa superseded, but it points
  at "the planned **v4** (graph data model)". At release: v4 is the platform; the model is PLATO.
- **The v4 docs use `Thing`/`Things` 388 times in 16 files.** Rename to `SpatialEntity` in the docs
  restart. The site code has no PLATO-sense `Thing`.
- **Repos with no hits:** `website`, `documentation`, `indexing`, `deployment`, `tileboss`, `.github`
  and `gazetteer-of-the-world` READMEs, and every other repo's threads.

### 4f. `place` Discussions: pinning (found after the scan, 2026-09-28). DONE: #98 and #100 closed as outdated, #80 closed as resolved (SG), each with a comment. Unpinning #80/#81/#98/#100 must be done in the web UI (the GraphQL API has no unpin-discussion mutation).

The keyword scan could not see this. Of the 9 discussions, **4 are pinned: #80, #81, #98 and #100**.
Pinned discussions are the first thing a visitor sees.

| # | State | Last post | Problem | Proposal |
|---|---|---|---|---|
| 81 Toponymic-Linguistics API | closed (2025) | 2025-02-21 | Vespa era, **closed but still pinned** | unpin now |
| 98 Rethinking Contribution… | open | 2026-02-14, no replies | ArangoDB vertices; `Thing`; overtaken by PLATO's own repository | comment (store per #301; `Thing` → `SpatialEntity`; PLATO discussion continues in its own repo), close as outdated, unpin |
| 100 PLATO – Clarifying Questions | open | 2026-02-25, no replies | `Thing` ×10; "Arango absorbs graph"; its §8 is answered by #301 | same as #98 |
| 80 v4 Data Model | open | 2026-08-02 | "Version 4 Release planned for 2026" | keep pinned; comment at the v4 release (§4d) |
| 168 five kinds of "region" | open | 2026-08-02 | current | none |
| 20, 21, 22, 23 | closed | 2024–25 | Vespa-era record | none |

## 5. Docs rework: published 2026-09-28 as a DRAFT (SG)

- **On `main` and live:**
  - `c2182ca`, the rework.
  - `433cf1f`, which holds back the release-only wording. The new guide is the **"Draft v4 User
    Guide"**, alongside Guides & Tutorials (which it does not replace). The beta notices, the "Coming
    with v4" wording and the Atlas "What's coming" list stay. The Beta Testing Plan stays live and
    links to the draft.
- **At the v4 release:** the local branch `v4-release` is `main` plus `3b0b478`, a revert of
  `433cf1f` that applies the release wording. Rebase it onto `main` at release, resolve every
  `% TODO(release)` marker (`git grep 'TODO(release)'`), and merge.
- **Local build:** 293 warnings against 300 before, none new.
