# WHG Unified Type System: Architecture, Mapping, and Search

> **Audience:** Human developers and coding agents working on the WHG v3.5
> codebase.  The search model described here is designed to migrate
> cleanly to the ArangoDB graph architecture planned for v4.
>
> **Status:** Design document and implementation plan.

---

## 1. Why AAT as the Primary Type Vocabulary

WHG v3.5 adopts the Getty Art & Architecture Thesaurus (AAT) as its
canonical place-typing vocabulary, replacing the coarse GeoNames
feature-class system used in earlier versions.

**What AAT provides that GeoNames feature codes do not:**

- Dereferenceable URIs (`http://vocab.getty.edu/aat/{id}`).
- A maintained hierarchical thesaurus with explicit broader/narrower
  relationships (SKOS).
- Poly-hierarchy: a concept may have multiple broader terms through
  different facet paths.
- Wide adoption across the cultural heritage and digital humanities
  linked-data ecosystem.

GeoNames feature codes are a flat two-tier code list (9 classes, ~680
codes) without inferential structure or URI-based identity.

**The attestation-centric v4 data model will permit polyvocal typing:**
a GeoNames-sourced attestation carries its feature code, a
Wikidata-sourced attestation carries its Q-class, and contributed
datasets carry whatever vocabulary the scholar assigned.  AAT serves as
the preferred harmonisation vocabulary, not the sole permitted one.
The mapping infrastructure built here for v3.5 will feed directly into
that model.

### 1.1 AAT Bulk Data

Full N-Triples dumps are available from the Getty:

| File | URL | Contents |
|------|-----|----------|
| `full.zip` | `http://aatdownloads.getty.edu/VocabData/full.zip` | All statements including inferred triples |
| `explicit.zip` | `http://aatdownloads.getty.edu/VocabData/explicit.zip` | Only explicitly asserted triples |

Consult the "Export Files" section of the semantic representation
documentation at `http://vocab.getty.edu/doc` before use.  Data is
released under the Open Data Commons Attribution License (ODC-By) 1.0.

For WHG purposes, `full.zip` is preferred unless a local reasoner is
available to materialise inferred triples from `explicit.zip`.

---

## 2. The Mapping Problem

WHG indexes place records from multiple authority sources and from
user-contributed datasets.  These sources type places using three
different vocabularies:

| Source | Typing vocabulary | Granularity |
|--------|-------------------|-------------|
| **AAT** | ~4 000 place-type concepts (hierarchical) | Fine: the canonical WHG vocabulary |
| **GeoNames** | 9 feature classes, ~680 feature codes | Medium (codes) / coarse (classes) |
| **Wikidata** | Open-ended Q-items as `P31` values | Extremely fine but noisy |

The existing WHG `places` index on the Pitt CRC staging ES instance
already contains ingested records from **TGN**, **GeoNames**, and
**Wikidata**, together with their assertions of mutual identity
(coreferences).  TGN records carry editorially assigned AAT place-type
identifiers, and many are coreferenced with GeoNames and Wikidata
entities.  The `places` index therefore already holds all the raw
material needed to derive cross-vocabulary mappings: if a TGN record
typed as AAT `300008389` ("cities") is coreferenced with a GeoNames
entity whose feature code is `PPLA`, that pairing constitutes a
curator-verified mapping from `PPLA` to `aat:300008389`.

### 2.1 Coverage Expectations

- **Strong coverage:** GeoNames classes P (populated places) and A
  (administrative divisions) map naturally to AAT.  Parts of S
  (spots/buildings/farms) also find counterparts, since AAT is strong
  on building and structure types.
- **Weak coverage:** Natural-feature classes H (hydrographic), T
  (landforms), U (undersea), and V (vegetation) contain many codes for
  which AAT has no corresponding concept or only very coarse-grained
  matches.
- **Unmapped places:** Many places will lack any type assignment.
  Where possible, assign a default broad AAT type during the
  post-processing augmentation step (e.g. "inhabited places" for
  GeoNames P-class sources, "administrative divisions" for A-class).

---

## 3. Deriving Cross-Vocabulary Mappings

All mapping derivation is performed on the **Pitt CRC cluster**,
orchestrated with bash, Python, and Slurm.  The input is the existing
`places` index on the CRC staging ES instance.  The outputs are the
`aat_types` and `type_mappings` ES indices described in section 5, plus
a set of review-ready mapping files (JSON or TSV) for manual
inspection.

> **Coding-agent note:** Specifics of CRC job submission, ES
> connection configuration, and Slurm resource allocation should be
> inferred from existing examples of working with the staging ES in
> the WHG CRC environment.

### 3.1 Exploiting Coreferences in the `places` Index

The `places` index contains records from TGN, GeoNames, and Wikidata,
linked by identity assertions.  The mapping strategy is:

1. **Identify coreferenced clusters.** Query the `places` index for
   groups of records (one TGN, one or more GeoNames, one or more
   Wikidata) that assert identity with one another.
2. **Extract type tuples.** For each cluster, extract the TGN record's
   AAT type(s), the GeoNames record's feature code, and the Wikidata
   record's `P31` Q-id(s).  Each cluster yields one or more
   `(aat_id, gn_fcode)` and/or `(aat_id, wd_qid)` pairings.
3. **Aggregate across all clusters.** Collect all pairings, counting
   the number of independent clusters attesting each.  High-frequency
   pairings (attested by many independent places) are high-confidence
   mappings.

### 3.2 Bootstrap Passes

Execute in the order listed.  Later passes should not overwrite
higher-confidence mappings from earlier passes.

#### Pass 0: TGN-Bridged Mappings (both vocabularies)

This is the primary and highest-confidence source.

1. Query the `places` index for all coreferenced clusters that include
   a TGN record carrying an AAT type.
2. For each cluster, extract `(aat_id, gn_fcode)` and
   `(aat_id, wd_qid)` pairings as described in 3.1.
3. Aggregate and threshold: retain only pairings attested by at least
   *n* independent clusters (suggested: n >= 3) to filter noise from
   miscatalogued entries.
4. Assign `confidence=exact, source=tgn_bridge`.

Output: two mapping tables (AAT-to-GeoNames, AAT-to-Wikidata) with
attestation counts.

#### Pass 1: Wikidata P1014 Links

~1 200 Wikidata items carry a `P1014` (Getty AAT ID) property.  For
Wikidata Q-ids occurring in the `places` index that were not already
mapped in Pass 0:

1. Query Wikidata SPARQL for items with `P1014` values.
2. Create `(aat_id, wd_qid)` mappings.
3. Assign `confidence=exact, source=wikidata_p1014`.

#### Pass 2: Wikidata P279 Walk

For high-frequency Wikidata type Q-ids still unmapped after Passes 0
and 1:

1. Walk `P279` (subclass-of) upward from the unmapped Q-id until a
   Q-id with a `P1014` link or an existing mapping is found.
2. Record the mapping with the hop count.
3. Assign `confidence=broad, source=wikidata_p279`.

#### Pass 3: Label Matching (both vocabularies)

For GeoNames fcodes and Wikidata Q-ids still unmapped:

1. **GeoNames:** Compare AAT `term` values against GeoNames
   `featureCodes_en.txt` descriptions.  Normalise plurals, strip
   parenthetical qualifiers, apply fuzzy / token-overlap scoring.
2. **Wikidata:** Fuzzy-match the English label of unmapped Q-ids
   against AAT terms.
3. Assign `confidence=inferred, source=label_match`.  Flag for manual
   review.

#### Pass 4: Hierarchy Propagation

For AAT leaf types that have no direct GeoNames mapping after earlier
passes:

1. Inherit the most-specific fcode(s) of the nearest mapped ancestor
   in the AAT hierarchy.
2. Assign `confidence=broad, source=hierarchy_propagation`.

### 3.3 Review and Promotion

Each pass produces a JSON or TSV mapping file with attestation counts,
confidence levels, and source labels.  These files are reviewed
manually before being loaded into the `aat_types` and `type_mappings`
ES indices.  The review priority is:

1. `confidence=inferred` mappings from label matching (Pass 3).
2. Low-attestation-count mappings from TGN bridging (Pass 0).
3. `confidence=broad` mappings from hierarchy propagation (Pass 4).

High-confidence, high-attestation mappings from Passes 0 and 1 can
be promoted without individual review.

### 3.4 Reporting

A reporting script scans the `places` index and the current mapping
tables to identify:

- Wikidata Q-ids and GeoNames fcodes present in the data but not yet
  mapped to any AAT type.
- AAT types with no GeoNames or Wikidata mappings (coverage gaps).
- Distribution of confidence levels across the mapping tables.

---

## 4. Post-Processing Type Augmentation

The authority sources (TGN, GeoNames, Wikidata) have already been
ingested into the `places` index.  AAT type assignment is therefore a
post-processing step, run on the CRC staging instance and promoted to
production after verification.

### 4.1 Authority-Source Records

A bulk-update job:

1. Query the `places` index for records sourced from GeoNames or
   Wikidata that lack AAT type annotations.
2. For each, look up the record's native type (fcode or `P31` Q-id)
   in the mapping tables.
3. Write the mapped AAT type identifier(s) into the record's `types`
   array in ES.

### 4.2 Contributed Datasets

For contributed datasets already in the index whose `types` field
contains Wikidata Q-ids or GeoNames fcodes but no AAT identifiers:

1. Look up the Q-id / fcode in the mapping tables.
2. Assign the highest-confidence AAT type(s) found.
3. Log cases where no mapping exists (feeds the reporting script).

For future ingestions, the same lookup should be applied at ingest
time so that new records arrive with AAT types already assigned.

> **Coding-agent note:** The `places` index stores only the directly
> assigned AAT type identifier(s) for each record.  Do *not* add
> ancestor chains, related types, or any other hierarchy-derived data
> to the `places` index.  All hierarchical reasoning is performed
> post-retrieval (see section 6).

---

## 5. ES Index Structures

### 5.1 The `places` Index (existing, minimal changes)

Each place record in the `places` index retains its existing schema.
The only change is that the `types` array is progressively populated
with mapped AAT identifiers via the post-processing augmentation in
section 4.  No ancestor chains, depth values, or hierarchy-derived
fields are added to `places`.

### 5.2 The `aat_types` Index (new)

A dedicated index holding the AAT type hierarchy and cross-vocabulary
mappings.  This is the lookup structure used for query expansion and
post-retrieval consanguinity computation.

```json
{
  "aat_id": 300008389,
  "term": "cities",
  "parent_id": 300008347,
  "path": "300264550.300008346.300008347.300008389",
  "depth": 3,
  "ancestors": [300264550, 300008346, 300008347],
  "gn_fcodes": ["PPL", "PPLA", "PPLA2", "PPLA3", "PPLA4", "PPLC"],
  "wd_qids": ["Q515", "Q1549591"]
}
```

The `path` field (dot-delimited ancestor chain from root to leaf)
enables prefix queries to retrieve all descendants of a given type.
The `ancestors` array enables fast lookup of all ancestors for a given
type.  Together they support the consanguinity computation described
in section 6.

### 5.3 `type_mappings` Index (optional, for reverse lookups)

```json
{ "gn_fcode": "PPLA", "aat_ids": [300008389, 300008347] }
{ "wd_qid": "Q515",   "aat_ids": [300008389] }
```

### 5.4 Sync and Cache Invalidation

A sync script bulk-indexes the AAT hierarchy plus the derived mapping
tables into the CRC staging ES instance.  Run after each mapping
derivation cycle.

Use the ES index-alias swap pattern: push to a timestamped index, then
atomically swap the alias, so search always reads a consistent
snapshot.  Once verified on the staging instance, replicate to
production.

---

## 6. Type Search Architecture

### 6.1 Design Principle: Lean Places, Smart Post-Processing

The `places` index is not bloated with hierarchy-derived fields.  Each
place stores only its directly assigned AAT type identifier(s).  All
hierarchical reasoning (narrower-term expansion, broader-term
inclusion, sibling detection) is performed in two stages:

1. **Broad retrieval** from the `places` index, casting a wide net.
2. **Post-retrieval banding and ranking** by computing consanguinity
   between each result's assigned type and the user's query type,
   using the `aat_types` index as the hierarchy lookup.

This keeps the `places` index lean and schema-stable, concentrates
type-hierarchy logic in a single post-processing layer, and avoids
re-indexing millions of place records whenever the AAT hierarchy or
mappings change.

### 6.2 The Two Retrieval Problems

#### The narrower-term problem

A search for "villages" should find "nucleated villages", "fishing
villages", and all other subtypes.  At query time, look up the query
type in the `aat_types` index and retrieve all descendant `aat_id`s
(via a `path` prefix query).  Also retrieve their mapped `gn_fcodes`
and `wd_qids`.  Use the resulting set of identifiers to query the
`places` index.

#### The broader-term problem

A place typed only as "inhabited places" *might* be a village but
might equally be a city.  The subsumption relationship runs the wrong
way, and no hierarchical traversal can resolve ambiguity that reflects
genuine ignorance in the source data.  This is handled by
post-retrieval consanguinity banding (section 6.3).

### 6.3 Post-Retrieval Consanguinity Model

After retrieving candidate places from the `places` index (using a
broad query that encompasses exact, narrower, broader, and sibling
types), each result is assigned to a tier by comparing its assigned
AAT type against the user's query type using the `aat_types` index.

**Tier 1 -- Exact and narrower matches.**  The result's assigned type
is equal to the query type, or the query type appears in the result
type's `ancestors` array (i.e. the result type is subsumed by the
query type).  These are certain hits.  Consanguinity distance = 0.
*Example: searching for "villages" returns everything typed as
villages, nucleated villages, fishing villages, etc.*

**Tier 2 -- Broader-type matches.**  The result's assigned type is an
ancestor of the query type (i.e. the result type appears in the query
type's `ancestors` array).  These are places that *could* be what the
user wants but are typed at insufficient granularity.  Consanguinity
distance = number of edges from the query type up to the result's
assigned type.
*Example: a place typed only as "inhabited places" appears here when
searching for "villages", with distance proportional to how many
hierarchy levels separate the two.*

**Tier 3 -- Sibling and lateral matches.**  The result's type shares a
common ancestor with the query type but is neither broader nor
narrower (co-hyponyms).  Consanguinity distance = sum of edges from
each type up to their nearest common ancestor.
*Example: searching for "villages" surfaces "hamlets" and "towns" as
sibling categories under "inhabited places".*

#### Computing consanguinity

Given a query type Q and a result's assigned type R:

1. Fetch Q's `ancestors` array and R's `ancestors` array from the
   `aat_types` index.
2. If Q == R, or Q appears in R's `ancestors`: **Tier 1**, distance 0.
3. If R appears in Q's `ancestors`: **Tier 2**, distance = position of
   R in Q's `ancestors` array (counting from the leaf).
4. Otherwise, find the nearest common ancestor (the first shared
   element, scanning both `ancestors` arrays from the leaf end).
   **Tier 3**, distance = (steps from Q to NCA) + (steps from R to
   NCA).
5. If no common ancestor is found within the distance threshold:
   **unranked / excluded**.

> **Coding-agent note:** The consanguinity computation runs in the
> application layer (Django view or Gateway service), not inside ES.
> For each search, pre-fetch the query type's ancestor chain once from
> `aat_types`, then iterate over the result set.  For result types
> encountered more than once, cache their ancestor lookups for the
> duration of the request.  The `aat_types` index is small (~4 000
> documents); aggressive caching (or loading the full hierarchy into
> memory at startup) is feasible and recommended.

#### Exploiting poly-hierarchy

AAT is poly-hierarchical: some concepts have multiple broader terms
through different facet paths.  Where the `aat_types` index records
multiple paths (multiple entries in `ancestors`), compute shortest-path
distance through *any* path for richer lateral connections.

#### Distance threshold

A maximum consanguinity distance (e.g. 4 or 5 edges) prevents absurd
lateral matches.  This is more principled than hand-curated root
families and less likely to create blind spots.  Results beyond the
threshold are excluded from Tier 3 entirely.

### 6.4 User-Facing Search Modes

Type-based search within a spatial region offers two modes:

#### "Only show" (hard constraint)

Excludes everything outside the specified type.  Post-retrieval
banding discards results outside the selected tiers.  The result set
shrinks.

*"Only show villages in Lincolnshire."*

Under this mode, offer a secondary choice:

- **Strict:** Tier 1 only (exact and narrower matches).
- **Inclusive:** Tiers 1 and 2 (also include broader-typed places that
  *could* be the requested type).

#### "Prioritise" (soft ranking)

Retains the full result set.  Post-retrieval banding assigns each
result a tier and consanguinity distance; the UI groups or sorts
results by tier, with distance as a secondary sort within each tier.

*"Show me places in Lincolnshire, with villages ranked first."*

### 6.5 Query Construction

When a search request arrives with a type parameter:

1. **Expand the query type.** Look up the query type in the
   `aat_types` index.  Retrieve all descendant `aat_id`s (via `path`
   prefix query) and their associated `gn_fcodes` and `wd_qids`.  Also
   retrieve ancestor `aat_id`s and sibling `aat_id`s (children of
   ancestors) up to the distance threshold, plus *their* mapped
   `gn_fcodes` and `wd_qids`.
2. **Broad retrieval.** Build an ES `bool` query matching records in
   `places` by *any* of:
   - AAT identifier in the record's `types` array, **or**
   - GeoNames fcode in the record's `fcode` field, **or**
   - Wikidata Q-id in the record's `types` array.

   Combine with any spatial, temporal, or textual filters the user has
   specified.  In "Only show / Strict" mode, the expansion set can be
   limited to Tier 1 types only, avoiding unnecessary retrieval.
3. **Post-retrieval banding.** For each result, look up its assigned
   type in the `aat_types` index and compute consanguinity against the
   query type (section 6.3).  Assign tier and distance.
4. **Filter or rank.** In "Only show" mode, discard results outside
   the selected tiers.  In "Prioritise" mode, sort by tier then by
   distance within tier.

> **Coding-agent note:** Step 1 (expansion) and step 3 (banding) both
> read from the `aat_types` index but serve different purposes.
> Expansion determines *what to retrieve* from `places`; banding
> determines *how to present* what was retrieved.  Keep them as
> separate, clearly named functions.

---

## 7. UI Enhancements

1. Offer "Only show" / "Prioritise" toggle in the search interface,
   with "Strict" / "Inclusive" sub-option under "Only show".
2. Display tier labels alongside results so users understand why each
   result appeared (e.g. "exact match", "broader type", "related
   type").
3. Allow search-results faceting by AAT type.
4. Show mapping coverage stats in the admin dashboard.

---

## 8. Migration Path to v4

The v4 graph model (ArangoDB) replaces the ES-based type indices with
native graph traversal.  The mapping tables and bootstrap heuristics
built here carry over directly; the main architectural changes are:

- **Consanguinity becomes a graph query.** Shortest-path and
  common-ancestor calculations that run in the application layer for
  v3.5 become native AQL traversal operations in ArangoDB, likely
  faster and certainly more elegant.
- **Polyvocal typing at the attestation level.** Each attestation
  carries its own type in its original vocabulary.  The AAT mapping
  becomes a property of the attestation rather than a synthetic
  annotation on the canonical place.
- **The `aat_types` ES index is replaced by the AAT subgraph** within
  the ArangoDB `indexing` database, traversable in real time.

The post-retrieval banding model translates directly: retrieve
candidate places, then traverse the AAT subgraph to compute tier and
distance for each result.

---

## 9. Estimated Effort (v3.5 Implementation)

| Phase | Scope | Effort |
|-------|-------|--------|
| 3.1--3.2 | Coreference extraction and mapping derivation (CRC) | 3--4 days |
| 3.3 | Manual review of inferred/low-confidence mappings | 1--2 days |
| 3.4 | Reporting scripts | 1 day |
| 4 | Post-processing type augmentation | 2 days |
| 5 | ES `aat_types` index and sync | 2--3 days |
| 6 | Post-retrieval consanguinity engine | 3--4 days |
| 7 | UI enhancements | 1--2 days |