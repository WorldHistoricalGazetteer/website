# Plan (stub) — Routes & Networks as first-class Things (v4)

**Status:** PLACEHOLDER / not started. Teed up by the Collaborative Collections work
(`developer/plan-collaborativeCollections.prompt.md` §4.4), which ships `route` and `network` as
**reserved, disabled** Workbench doc-types so no migration is needed when v4 lands.

**Relates to:** place#100 (PLATO — Place Attestation Ontology; lists **Routes** and **Networks** as
Thing types alongside Settlements · Regions · Polities · Physical features), place#80 (v4 graph data
model), place#54 (Itineraries `seq`), place#111/#112 (Workbench). See the parent plan for the full
doc-type architecture this slots into.

> `place#NNN` = issues/discussions in `WorldHistoricalGazetteer/place`. `website`/`whg3` PRs live in
> `WorldHistoricalGazetteer/website`.

---

## Why this is deferred to v4 (the assessment already made)

An **Itinerary** is *composition*: an ordered set of **existing** places — no new entity, no new
storage, no topology. It is a sequenced Place Collection (parent plan §4.3) and ships in v3.3.

A **Route** and a **Network** are *not* compositions. PLATO place#100 §6: *"A route is not merely a
collection of places. It is a referential entity with its own attestations."* Both are **first-class,
attested Things** the current places/index pipeline cannot represent:

- **Route** — a (typically linear) Thing: name, dates, attestations, a geometry, and **ordered**
  references to constituent places/segments.
- **Network** — a **topological** Thing: nodes (places/Things) + **edges** (typed relations) with its
  own identity and attestations (trade network, monastic/correspondence network, …). A route is, in
  effect, a degenerate (linear) network.

Both need the v4 graph model (place#80). Hence: **placeholders in v3.3, real build in v4.**

## What already exists as of the v3.3 foundation (do not rebuild)

- `WorkbenchProject.doc_type ∈ {…, 'route', 'network'}` reserved (workbench/models.py).
- Nullable `WorkbenchProject.published_route_id` / `published_network_id` pointer columns (migration
  `workbench/migrations/0003_*`) — so publishing a v4 Route/Network needs no schema change here.
- Registry entries `route` + `network` with `enabled=False` (workbench/doctypes.py) — creation is
  gated OFF; the "New…" picker shows them as disabled "Coming with v4" tiles.
- Terminology labels `route` / `network` (main/labels.py + wb-labels.js).

## Model options to assess (the v4 decision, not yet made)

1. **New first-class `Route` / `Network` Django models** vs **Thing-as-graph-node** (place#80): does a
   Route/Network become its own table, or a node/subtype in the unified Thing graph?
2. **Edge / topology representation** for Networks: adjacency table (`(network, from_thing, to_thing,
   relation, attestation)`) vs a graph store vs LPF-style relations. Routes reuse the same edge model
   with an ordering constraint (linear ⇒ path).
3. **Attestations** attaching to the Route/Network itself and to each edge (PLATO), not just to member
   places.
4. **Geometry:** stored line for a Route vs derived from member places; Network has no single geometry
   (render edges).
5. **Publishing & public presentation:** new browse pages vs extending Collection browse; how a
   sequenced Itinerary's animated presentation (parent plan §14 open question) relates to a Route's.
6. **Editor:** a Workbench doc-type editor (map draw for the route line; a node/edge canvas for the
   network) once the model exists — the doc-type registry seam is ready for it.

## Next step when v4 starts

Flip the two registry entries to `enabled=True`, add the editors + publish/checkout pairs, and
resolve the model options above against the place#80 graph model. Nothing in v3.3 blocks it.
