# Plan: reassess the move to ArangoDB for WHG v4

Recorded 2026-09-28; revised the same day with SG's answers (see "Decisions from SG").
Status: **EXECUTED 2026-09-28; tracked as place#301.** Recommendation: stay on PostgreSQL/PostGIS + ES, with PLATO RDF
for interchange and a generated SPARQL endpoint. Do not open an ArangoDB licence negotiation. Results
are in "Findings so far" below; the addendum is drafted into documentation `database.md`
(documentation `1b5df8f`). Tracking: place#301. Harness, controls and raw results: `/mnt/salvage/whg-bench/harness/`. Licence sources:
`/mnt/salvage/whg-bench/licences/`. Databases: `/mnt/salvage/whg-bench/{pg,oxi,arango,arango-scale}`.
Brief received from peer session whg3-6a (a starting point to verify, not findings).

## The question, and how it gets decided

`documentation/content/v4/architecture/database.md` (1,344 lines, last revised
2025-10-19, 60cff82) recommends ArangoDB on six grounds: attestations as first-class
graph nodes, native GeoJSON, vector similarity for phonetic matching, temporal queries,
a single integrated system for a solo team, and scale (500 GB–1 TB, 73M+ nodes). It
names one critical dependency, licensing, and its next step is a licensing discussion.
It set a decision target of Q2 2025.

**Decision rule, stated before any measurement.** The switch carries the burden of
proof, because the alternative is already running. The case for ArangoDB (or any graph
store) reopens if, and only if, step 2 finds at least one **decisive** traversal query
(see 2a: networks, routes and itineraries are excluded) that the existing stack cannot
answer within **5 s** on realistic data, *and* the candidate store answers it within 5 s.
If no such query exists, the recommendation is to stay on Elasticsearch +
PostgreSQL/PostGIS, with PLATO RDF as the interchange format.

## Decisions from SG (2026-09-28)

1. **The ArangoDB licensing conversation never happened.** The assessment's single
   critical dependency was never resolved, so by the document's own terms ArangoDB has
   never been established as viable. Step 1 is therefore a reading of the licence, not a
   search for an outcome.
2. **Network / route / itinerary traversal (Q5) MIGHT be implemented if the
   infrastructure allows, but must NOT determine the choice of infrastructure.** It is
   measured for information only and carries no weight in the decision rule.
3. **Latency budget: up to 5 s**, on the premise that these are one-off browser
   queries, not chains of API calls. If a query turns out to be needed inside an API
   or reconciliation path, it needs its own tighter budget, so re-ask then.
4. **Corpora live on the salvage drive**: `/mnt/salvage/whg-plato-corpora/`.
5. **`same_as` is marginal in v3.5.** Identity comes mainly from soft clustering, not
   from the pre-v3.5 reconciliation `same_as` attestations. See 2a, Q2.
6. **The v4 place page does not need multi-hop identity outside a search** (SG: not
   built yet, but probably not). A comparable place page is envisioned for **v3.5**,
   which means on the existing stack. **Q2 leaves the decisive set.** Whatever identity
   that v3.5 page does show will be served by ES + Postgres, and that becomes evidence
   here rather than a requirement against it.

## What has already been verified (2026-09-28)

- The document, its date, the six grounds and the single critical dependency are as the
  brief describes (TL;DR, lines 1–24; lines 14, 1294, 1341).
- `deployment/secrets/V4-SERVER-UPGRADE.md` line 3 ("allow for switchover to
  ArangoDB") and §8.5 ("Reserve storage/RAM for future ArangoDB deployment") are as
  quoted. ArangoDB would be a **fourth** datastore beside ES, Postgres and Redis.
- The RDF option was **not wholly absent** from the v4 docs, which corrects the brief.
  `data-model/rdf-representation.md:696-698` plans "Store data internally in ArangoDB"
  plus "Support SPARQL endpoint". A triplestore was contemplated as an *extra* layer on
  top of ArangoDB (a fifth service), never as an *alternative* to it. The database
  assessment itself does not evaluate it.
- The store has already leaked into the model: `rdf-representation.md:496` tells
  contributors to split heterogeneous geometries into one attestation per type
  *because ArangoDB lacks GeometryCollection*. PostGIS has no such limit.
- The assessment's own next steps 4 (benchmark vector search) and 5 (validate
  multi-hop graph queries) have no recorded outcome either.
- Corpora copied out of the peer's expiring scratchpad and now held at
  **`/mnt/salvage/whg-plato-corpora/`** (2.5 GB, `SHA256SUMS` verified after the move,
  `README.txt`). The corpora are:
  Pleiades, Vision of Ireland, Vision of Britain, each as PLATO JSON + N-Triples, and
  the UKDS VoB source zips (EULA: local use only). **Discrepancy to settle:** Pleiades
  exists as 4,676,894 triples (JS serialiser, `pl.nt`, matches the pleiades README) and
  3,935,401 (plato CLI, `pl-plato-cli.nt`, the brief's "3.9M"). Both serialise the same
  42,361 places, so ~740k triples differ between the two tools.

## Step 1 — Licensing facts (do first; everything else is downstream)

1. ~~Did the licensing conversation happen?~~ **No** (SG, 2026-09-28). The critical
   dependency is open. Whether to open that conversation now is itself an output of
   this reassessment, not a precondition for it.
2. Read the **ArangoDB Community License text itself** (the PDF), not the 2024 blog
   posts. Record verbatim: the 100 GB per-cluster / three-cluster limit; whether it is
   conditioned on *commercial* use; how "commercial" is defined; whether a free public
   service funded by Pitt and grants falls inside it; and what the BSL 1.1 change date
   means for the version we would run.
3. State the non-commercial position explicitly either way. If the licence text is
   ambiguous on a Pitt-funded free service, that ambiguity *is* the finding. It is not
   something to resolve by our own reading of it; it goes to Pitt counsel or to the vendor
   in writing.
4. Size the gap: against the document's own 500 GB–1 TB projection, a binding 100 GB
   cap is five- to tenfold short. Also re-derive the projection itself (step 5).
5. Note the vendor change (arangodb.com now redirects to arango.ai, positioned for
   "agentic AI"), and check release cadence and the Community Edition's status on the
   current site. This bears on sustainability, not on fitness.

## Step 2 — The traversal test (Stephen's first requirement)

The one thing a graph store clearly buys is ad-hoc multi-hop traversal and path
queries. Write the queries down concretely **before** measuring anything, so the test
answers a question somebody actually has.

### 2a. Fix the query set

Candidates, with the depth each really has. The brief calls most of these shallow, and
two of them are not:

| # | Query | Depth | Source in v4 docs |
|---|---|---|---|
| Q1 | Containment chain: all ancestors / all descendants of a place, time-scoped | bounded, variable (~2–10) | "Historical Dynamism", `contained_in` |
| Q2 | Identity: the places that are the same as X, at a user-chosen confidence | *was* unbounded closure; **now per-result-set** (see below) | usecases.md:22, 36, 375 |
| Q3 | Provenance chain: from a claim back to all sources, through `derived_from` / citation, including PLATO #9 retraction/supersession | variable, usually shallow | usecases.md:184, 460; database.md:1167 |
| Q4 | Succession chain: `succeeds` over time | variable, shallow | usecases.md:259 |
| Q5 | Shortest path between two places over a Network/Route Thing, filtered by timespan | deep, path-finding. **NOT DECISIVE (SG)** | usecases.md:435, 128–137 |
| Q6 | The assessment's own showcase: Thing → Attestation → Name → Authority → Period, with spatial + vector filter | fixed 4 hops, i.e. a join | database.md:1010, 1324 |

**Q2 has largely been answered by production already.** The v4 design wanted to
discard precomputed cluster membership and make identity "a graph traversal query ...
[allowing] different confidence thresholds at query time" (indexing
`CLUSTERING_GUIDE.md` §4.2; `CLUSTERS.md` §2.4). Since 2026-07-12 WHG does exactly
that without a graph store. The static `clusters` index was retired, and the gateway
ships result-set hard-link edges, per-hit signals and `clustering_params`. The
browser (`clustering.js`) runs Union-Find at a user-adjustable θ over at most
500 hits (indexing `developer/search-system-architecture.md`). Identity is therefore
local to a candidate set, not a global transitive closure over 20M nodes, and it is
the design most specifically attributed to ArangoDB. What remains to test for Q2 is
narrow: **"given a place, which records across all sources are linked to it through
hard links, beyond one hop?"**. That query is needed only if the place page shows
multi-hop identity outside a search. The last global run gives its scale: 20.58M
nodes, 16.81M edges and 7.31M components, a mean of about 2.8 per component (retired
run, 2026-03-25). **Get the maximum component size**, because one giant component is
the only way this query becomes expensive. (Side note, not for this plan:
`CLUSTERING_GUIDE.md` §3–4 still describes the retired index and has no "superseded"
banner, unlike `CLUSTERS.md`.)

**Decisive set:** Q1-with-time (containment), Q3 (provenance with retraction), Q4
(succession) and Q6 (the assessment's own 4-hop showcase). **Q2 was dropped (SG
decision 6), and Q5 is measured for information only.** The largest-group question
above is still worth one ES aggregation, because the v3.5 place page may want it. Every decisive query is bounded and fairly
shallow, which is the shape relational joins and recursive CTEs serve well. The test
exists to confirm this at scale, not to assume it.

For each query record: the exact question in words, the user workflow that asks it,
the expected result size, and pass/fail against the **5 s** budget (a one-off browser
query, measured end to end from the gateway, cold and warm).

### 2b. Data

- Real corpora: Pleiades (42k places; has connections: 14,995), VoI (2,925 places,
  3.4M triples, dense containment), VoB (GBHD, containment and succession).
- The existing platform's real links: `same_as`/closeMatch edges from the `places`
  index, to get the true identity-component size distribution for Q2. **Read-only:** dev
  shares the prod ES indexes, so run scroll exports off-peak with a small page size,
  and never write.
- Q5 (information only) has no real network corpus. If it is run at all, use one
  modest synthetic network labelled as such. Do not spend effort scaling it.
- Scale-up: replicate the corpora to the 73M-node figure (step 5 re-derives it) to test
  whether an answer survives at scale, not only on 42k places.

### 2c. Run each query in each arm

- **Arm A, existing stack.** PostgreSQL recursive CTEs over an attestation-as-row
  schema loaded from PLATO JSON (local Postgres, not prod). ES for the parts ES does now
  (name/phonetic candidate retrieval), followed by Postgres expansion. Try Postgres with
  and without a precomputed closure table for Q2. Try Apache AGE only if the plain CTE
  fails its budget.
- **Arm B, RDF triplestore.** Same queries in SPARQL 1.1 with property paths (`+`, `*`).
  See step 3.
- **Arm C, ArangoDB CE**, locally, under 100 GB, AQL traversal / K_SHORTEST_PATHS, for
  a fair comparison. Confirm in step 1 that local evaluation use is permitted.

### 2d. Make the test able to fail

- A **known-answer case** for every query, planted in the data: a containment chain of
  known length, a `same_as` component of known membership, a retraction that must drop
  a source, a network with a known shortest path. Every arm must return exactly the
  planted answer before any of its timings count.
- A **negative control** for each: a planted near-miss (a broken link in the chain, an
  expired timespan) that must *not* be returned.
- Warm vs cold cache reported separately; p50/p95 over ≥50 runs; hardware recorded;
  row counts printed beside every timing (the denominator).

## Step 3 — The RDF triplestore option (Stephen's second requirement)

Evaluate it on its merits, and if it is rejected, reject it in writing for stated
reasons.

1. **Why it is a serious candidate:** PLATO *is* RDF and the corpora already exist as
   N-Triples with 0-difference round trips, so there is no impedance mismatch.
   Attestations are first-class resources in RDF already (rdf-representation.md:131).
   SPARQL property paths express Q1–Q4 directly. GeoSPARQL covers geometry, and the
   docs already plan a SPARQL endpoint.
2. **Candidates:** QLever (Apache 2.0; built for billions of triples; fast path queries;
   GeoSPARQL partial), Oxigraph (MIT/Apache; embedded or server; simpler; scale
   limits to measure), Apache Jena Fuseki + TDB2 (Apache 2.0; GeoSPARQL module),
   GraphDB Free and Virtuoso Open Source (check licence and GeoSPARQL). For each,
   record: licence, GeoSPARQL support, full-text support, write/update model, and ops
   burden.
3. **Load all three corpora** and time the load; run the step-2 queries; record the
   triples-to-disk ratio.
4. **Scale estimate.** Observed triples per place vary by more than tenfold:
   Pleiades ~93–110 per place, VoI ~1,170 per place. Extrapolating to 47M+ records gives
   somewhere between ~4.5 billion and far more, depending on attestation density. Settle
   the per-source density before quoting any figure, and test the chosen store at a
   replicated scale, not by extrapolation.
5. **What a triplestore does not do:** faceted multilingual search and phonetic kNN
   over 47M names. The likely shape is therefore ES (search) + triplestore (graph and
   provenance) + Postgres (app state), which replaces the planned ArangoDB rather than
   consolidating anything. State this plainly.
6. Say what the RDF option needs that PLATO doesn't yet give: update and retraction
   semantics in the store, named graphs per contribution for licensing and withdrawal,
   and per-source licence enforcement (cf. place#269's 451s).

## Step 4 — Re-test the other five grounds, briefly and against today's facts

- **Attestations as first-class nodes:** a logical-model claim. Show it holds in
  Postgres rows, ES nested docs and RDF resources alike (PLATO round trips are the
  evidence).
- **GeoJSON:** PostGIS plus pre-generated tiles in production; note the ArangoDB
  GeometryCollection constraint that has already bent the model.
- **Vector / phonetic:** Symphonym runs in ES in production. Record the vector count and
  latency from the live system rather than assuming them.
- **Temporal:** check what v4 temporal queries need beyond range filters (ES and
  Postgres both do range filters well).
- **Single integrated system:** only true if ArangoSearch replaces ES for 47M-record
  faceted multilingual phonetic search, which the assessment never evaluated. Either
  evaluate that replacement or drop the claim.

## Step 5 — Cost of a switch

- Re-derive the 500 GB–1 TB / 73M-node projection from the real corpora's density
  (step 3.4), since the licence gap and the hardware plan both hang on it.
- Migration: 47M+ records, reindexing, dual-running, and cut-over for the API, the
  reconciliation service, Atlas and tiles.
- A second (or third) query language, and what happens to existing ES and Postgres code.
- A fourth service on the V4 server: RAM, backup, upgrade and monitoring, for a team
  of one. Cost the V4-SERVER-UPGRADE §8.5 reservation.
- Lock-in under BSL versus Apache/Postgres/W3C standards.

## Step 6 — Deliverable

- A dated **addendum** to `database.md`, not a rewrite. The 2025 document was sound on
  its evidence and stays as the record. The addendum records the licence finding, the
  traversal results with their denominators, the triplestore verdict, and a
  recommendation under the decision rule above.
- Correct `rdf-representation.md:131, 496, 696` if the recommendation changes the
  storage premise, and the V4-SERVER-UPGRADE §8.5 reservation to match.
- File tracking on the `place` repo.

## Open questions for Stephen

None outstanding.

## Findings so far (2026-09-28)

### Step 1: licensing (primary texts in the scratchpad `licences/`, all verified by grep)

- **The 100 GB cap is enforced by the binary.** Stock `arangodb:latest` (3.12.12, 2026-09-24)
  reports `/_admin/license` → `diskUsage.bytesLimit: 107374182400`, with countdowns to
  `secondsUntilReadOnly` and `secondsUntilShutDown`. Measured, not read.
- **The Community License text has no commercial test.** §i grants use "only for your internal
  business purposes in a dataset that is less than 100GB aggregated across the cluster". §2(g)
  forbids use "with any dataset … that is in the aggregate 100GB or more". There is no definition of
  commercial, and no academic, non-profit or evaluation exception. The text is identical to the
  2024-06-16 Wayback copy. §4 also lets the vendor terminate "at any time for any reason".
- **The non-profit exemption was withdrawn.** The Feb 2024 post, as archived 2024-05-07, said "This
  explicitly does not apply to non-profit organizations" and "a maximum of three clusters". The
  live post is marked "Updated 3/28/25 for accuracy", and both passages are gone. The brief's "100 GB,
  three clusters, commercial only" framing is that withdrawn wording.
- **The escape routes are poor.** 3.11.x (the last Apache-2.0 line) went end-of-life on 2025-05-30,
  and the last public CE binary is 3.11.14. A community build of 3.12 from the BSL source is not a
  maintained path (every CMake preset sets `USE_ENTERPRISE: On`). That is inferred from the code,
  not tested.
- **Conclusion.** At the assessment's own 500 GB–1 TB, the Community Edition is unusable whatever
  "commercial" means. ArangoDB therefore means an Enterprise contract. The licensing conversation
  never happened, and it would now be a price negotiation, not a licence clarification.
- **Other store licences.** GraphDB Free forbids publishing evaluation results without permission
  (Art. 15.3), limits use to two concurrent queries, and caps at about 2 billion RDF nodes, so it is
  excluded. QLever (Apache-2.0), Oxigraph (Apache/MIT) and Jena (Apache-2.0) are unencumbered.
  Virtuoso OSE is GPLv2.

### Step 1 corollary: the assessment's own numbers do not reconcile

- It sizes "28M Things" and then computes "73M × 9.5KB". Its per-Thing model (10 nodes and ~25 edges
  per Thing) implies about 280M nodes and 700M edges for 28M Things. Production already has 47M
  places.
- **The planned V4 server is 8 vCPU / 32 GB RAM / 320 GB SSD** (V4-SERVER-UPGRADE line 7), shared
  with 14+ containers and ES. The assessment itself says ArangoDB needs 128 GB RAM (64 GB minimum)
  and 260–360 GB of storage. The ArangoDB the assessment recommends does not fit the server that
  was reserved for it.

### Step 2: traversal test, corpus scale (all real PLATO data plus 33 planted control entities)

- **Setup.** One canonical table set loaded identically into Postgres 15/PostGIS (4 CPU / 8 GB cap),
  Oxigraph 0.5.11 (PLATO's own N-Triples, 9.28M triples), and ArangoDB 3.12.12 (attestations as
  vertices, as the assessment models them).
- **Every arm passes all 11 planted known-answer checks**, including time-scoped containment,
  PLATO-parity retraction (cross-checked against plato-tools' own `resolveWithdrawn`), provenance
  through `derived_from`, the 4-hop showcase, and shortest path.
- **The checks can fail.** 5/5 deliberate defects are caught in Postgres. The unfiltered SPARQL
  property path and AQL without PRUNE both return the near-misses.
- **The arms agree on every real query.**

| Real query (p50 / p95, ms) | Postgres | Oxigraph | ArangoDB |
|---|---|---|---|
| Ancestors, cyclic Pleiades chain (6) | 1.2 / 1.5 | 1.1 / 2.0 | 6.1 / 7.3 |
| Ancestors, deepest real chain, VoI 10 levels @1900 | 1.1 / 1.5 | 83 / 101 | 15 / 27 |
| Descendants of IRELAND @1900 (2,918) | 16 / 18 | 2,188 / 2,499 | 301 / 339 |
| Predecessors, longest real succession (3) | 1.0 / 1.3 | 1.5 / 2.8 | 5.8 / 6.7 |
| Sources of the most-cited attestation (72) | 1.2 / 1.6 | 1.7 / 2.0 | 7.6 / 9.3 |
| Showcase, 50 real toponyms in central Italy (30) | 4.7 / 5.3 | 11 / 12 | 21 / 26 |
| *Info only:* shortest path, Pleiades network, 8 hops | 28 / 30 | 1,257 / 1,321 | 239 / 256 |

- **Real data is shallow and dirty.** The deepest real containment ancestry is 10 levels and the
  longest real succession chain is 3. Pleiades has genuine containment cycles (4 places) and a
  succession self-loop (place 1001909 "succeeds" itself). Any store must survive cycles, and all
  three do.
- **PLATO RDF cannot be walked with a filtered property path.** A relation is an attestation
  resource, so relation-type and time filters cannot be applied per step. The single-query form
  returns every near-miss. Every filtered walk in SPARQL therefore takes one query per level from
  the client, which is why Oxigraph's descendants query takes 2.2 s.
- **PLATO RDF quirk.** `plato:periodo_uri` is an `xsd:anyURI` literal, not an IRI, so it must be
  matched with `STR()`.
- **Pleiades serialisations.** `pl.nt` is an earlier conversion (404,836 property triples, no
  `citation_function`). `pl-plato-cli.nt` matches the current JSON, so there is no round-trip loss.
- **Implementation matters more than the store.** A naive path-enumerating Postgres CTE took 3.26 s
  for the shortest path. A visited-set BFS takes 28 ms.

### Step 2 at scale: Postgres at 47M places (81 GB)

- **Scale.** 47.0M places, 244.8M attestations, 57.9M relations, 244.8M citations and 15.0M names.
  The admin hierarchy runs 250 → 5k → 50k → 500k → 5M → 41.4M leaves, skewed so the biggest
  level-4 unit has 242,301 children. 20% of units change parent in 1851. Sources carry
  `derived_from` chains, and 1 in 2,000 names carries a 1–5-deep retraction chain.
- **Capped like the V4 server slice:** 4 CPU and 8 GB for Postgres.
- **All 11 planted controls pass inside the 47M database, before and after timing.**
- **Cold** means the first run after a restart with the DB files evicted from the page cache.

| 47M query | rows | warm p50 | cold |
|---|---|---|---|
| Ancestors of a leaf under the biggest unit @1800 / @1900 | 5 | 0.8 / 0.5 ms | 6.2 / 0.7 ms |
| First 100 children of a 242,301-child unit | 100 | 0.9 ms | 1.8 ms |
| All descendants of a typical level-3 unit | 59 | 0.9 ms | 19 ms |
| All descendants of the heaviest level-3 unit | 469,208 | 1.07 s | 1.34 s |
| Count of the same | 469,208 | 0.98 s | 1.06 s |
| **All descendants of the heaviest country** | **6,602,879** | **20.2 s** | **20.9 s** |
| Predecessors, 9-long succession | 9 | 0.2 ms | 99 ms |
| Sources through a `derived_from` chain | 3 | 0.6 ms | 21 ms in isolation (see note) |
| Retraction status, 5-deep chain | 1 | 0.1 ms | 8.3 ms |
| In-force attestations of one place (place page) | 7 | 0.5 ms | 5.2 ms |
| Showcase: 50 candidate names + 1° bbox + period | 1 | 3.2 ms | 301 ms |
| *Info only:* shortest path, 10 hops | 10 | 3.9 ms | 15 ms |

- **Note on the sources row.** In the benchmark sequence this query took 2.18 s cold, straight
  after the 20 s country scan. Reproduced alone, cold, it takes 21 ms (10.7 ms planning, 2.9 ms
  execution, 12 page reads). The plan is two index lookups. The one-off is recorded as an
  unexplained outlier.
- **The one miss is enumerating 6.6M rows.** Every bounded query is under 0.3 s even cold.
  **Pre-committed reading, fixed before the ArangoDB number was known:** the country-scale
  enumeration counts against Postgres only if ArangoDB answers it within 5 s. If both miss, it does
  not discriminate between stores, and the answer is architectural for either one: a precomputed
  per-unit, per-epoch descendant count, paged children lists, and map tiles.

### Step 2 at scale: ArangoDB on the same hierarchy

- **Setup.** The heaviest country's entire subtree (6,605,392 places and 7,927,555 containment
  attestations, all timed variants) was loaded into ArangoDB 3.12.12. That is everything the
  descendant traversal touches, with the other ~40M places left out, which favours ArangoDB.
- **Resources.** 4 CPU and 8 GB, the same as Postgres, with
  `ARANGODB_OVERRIDE_DETECTED_TOTAL_MEMORY=8G`. Without that variable ArangoDB sized itself from
  the host's 31 GB (a 20 GB per-query limit inside a 6 GB container), and the first run thrashed.
  Collections were compacted and the server was idle before timing.
- **Caveat.** ArangoDB recommends `vm.max_map_count=1536000`. The host is at 65530, and this was
  not changed (a host-wide kernel setting).
- **Ladder: descendants @1900.** Every row agrees with Postgres exactly unless marked otherwise.

| descendants | Postgres | ArangoDB, attestation vertices (the assessment's model) | ArangoDB, direct timed edges |
|---|---|---|---|
| 1,255 | 10 ms | 364 ms | 32 ms |
| 5,922 | 23 ms | 1.7 s | 189 ms |
| 18,453 | 53 ms | **8.0 s** | 1.5 s |
| 99,239 | 220 ms | **110 s** | **49 s** |
| 469,208 | 1.07 s | not finished after 3,205 s (aborted) | not run |
| 6,602,879 | 20.2 s | not run | not run |

- **ArangoDB's cost per row rises with size:** 290 → 1,104 µs/row in the vertex model and 26 →
  498 µs/row with edges. Postgres stays at about 2–8 µs/row. In the model the assessment
  recommends, ArangoDB fails the 5 s budget at about 18k descendants. Postgres passes it past
  469k.
- **AQL trap, caught by cross-checking against Postgres.** With filters on *edges* and
  `uniqueVertices: 'global'`, AQL marks a vertex visited when it is first reached through an
  edge that fails the filter, and it silently drops the valid route. That gave 1,123 instead of
  1,255. `uniqueVertices: 'path'` is correct. The vertex model avoids the trap because the filter
  sits on the attestation vertex.
- **Verdict under the decision rule.** No decisive query is answered by ArangoDB within 5 s that
  Postgres misses. The single Postgres miss (6.6M-row enumeration) is missed far worse by ArangoDB.
  **The case does not reopen.**

### Step 3: triplestore, decided without an at-scale run

- At corpus scale Oxigraph passes every control and agrees with the other arms on real data.
- But every *filtered* walk over PLATO RDF needs one SPARQL query per level from the client
  (see Step 2, corpus). That already costs 2.2 s for 2,918 descendants, against 16 ms in Postgres.
- An at-scale QLever run was not done. The decision rule requires it only if Postgres fails a
  discriminating query, and it did not.
- **Rejected as the primary store, for these reasons:**
  1. It has no way to filter a walk per step, a structural consequence of attestations being
     resources.
  2. It would still need ES for search, so it adds a service.
  3. Update and retraction semantics would have to be built on top.
- **Kept as a publication layer.** PLATO RDF round-trips losslessly (0 differences), so a SPARQL
  endpoint can be *generated* from the store of record, as `rdf-representation.md:698` already
  intends. QLever (Apache-2.0, billions of triples) or Jena (Apache-2.0, GeoSPARQL 1.0) are
  suitable. GraphDB Free is excluded by its licence.

### Step 4: other grounds

- **Vector / phonetic:** not measured live. On 2026-09-28 the CRC gateway timed out from this
  machine despite VPN routes, and the public site returns 403 to scripts. Production Symphonym in
  ES is the standing evidence.
- **Attestations as first-class:** demonstrated. The same attestation model ran correctly as
  Postgres rows, RDF resources and ArangoDB vertices, with identical answers on all real queries.
- **GeoJSON:** PostGIS handled every corpus geometry except 2 malformed rings of 38,654 (Pleiades
  source data). ArangoDB's lack of GeometryCollection has already bent the model
  (`rdf-representation.md:496`).
- **Single integrated system:** not true of the plan as deployed (see "already verified").

### Step 5: cost, from measured numbers

- **Storage.** Postgres holds the 47M-place attestation model in 81 GB. The assessment
  estimated 400–500 GB for Postgres. The synthetic rows are leaner than real ones (no notes or
  full geometries), so real size will be larger. The estimate still looks several-fold high.
- **Server fit.** The planned V4 server (32 GB / 320 GB) fits an 81–200 GB Postgres. It does not
  fit the 128 GB RAM / 260–360 GB that the assessment says ArangoDB needs.
- **Licence cost.** At WHG's size ArangoDB means an Enterprise contract. Its price is unknown,
  because the licensing conversation never happened.

### Step 6: deliverable

A dated addendum has been drafted into `documentation/content/v4/architecture/database.md`
(committed as documentation `1b5df8f`). Follow-on doc corrections are tracked in place#301.


## Tracking issue: filed as place#301 (2026-09-28)

**Title:** v4 store: retire the ArangoDB plan; PostgreSQL/PostGIS + ES, PLATO RDF for publication

**Body:** The Oct 2025 assessment (`content/v4/architecture/database.md`) made ArangoDB conditional on
licensing, and that conversation never happened. A 2026-09-28 reassessment (addendum in the same
file) found:
- The Community License caps data at 100 GB, enforced in software, with no non-commercial
  exemption.
- On WHG traversal queries ArangoDB missed a 5 s budget that PostgreSQL met at 47M places.
- A triplestore suits publication, not storage.

Follow-ups:
- [ ] Review and merge the addendum.
- [ ] `data-model/rdf-representation.md:131, 496, 696-698`: drop "internal ArangoDB" and the
      GeometryCollection split guidance.
- [ ] `deployment/secrets/V4-SERVER-UPGRADE.md` line 3 and §8.5: remove the ArangoDB reservation.
- [ ] indexing `CLUSTERING_GUIDE.md` §4.2: the "V4 (ArangoDB)" migration path is moot, and §3–4
      describe the retired index.
- [ ] Design the per-unit, per-epoch descendant count, plus paged children lists, for large regions.
