# Proposal — Wikipedia-link enrichment in the Gazetteer Workbench

**Status:** proposal only (no code yet). Requested 2026-07-06. Prior art: Locolligo's Wikipedia
enrichment. Belongs in the Workbench's **Enrichment / Export** pane.

## Goal
For places the reviewer has matched, offer to add a **Wikipedia article link** as an enrichment
column (and, optionally, into the LPF export). The natural key is the Wikidata QID of a chosen match.

## What the codebase actually exposes (investigated 2026-07-06)
1. **The `wd`/`wdgn` Elasticsearch index already stores Wikidata `sitelinks {lang, title}`**
   (`elastic/mappings/es_mappings_wd.json:49-54`) — the raw material for a Wikipedia URL.
2. **But every WHG-facing surface strips them.** Reconcile candidates (`make_candidate`,
   `api/reconcile_helpers.py:204-232`) return only `id, name, score, match, alt_names, description,
   has_geom, type` — no links. `GET /entity/{id}/api` (`api/views_entity.py`) emits a top-level LPF
   `links` array, but it carries only **authority closeMatch URIs** (`{type, identifier}`), never
   Wikipedia or sitelinks. The CRC gateway adapter drops `links` before `make_candidate`
   (`api/crc_client.py:462-469`).
3. **The Wikidata QID is recoverable from a candidate id.** IDs are `place:<namespace>:<local>`, e.g.
   **`place:wd:Q16202`** → split on `:` → namespace `wd`, QID `Q16202` (`api/crc_client.py:246`,
   `reconcile_helpers.get_namespace`). Legacy WHG ids are bare integers (`place:12345`) and carry no
   QID directly.
4. **Nothing in the repo fetches Wikipedia/sitelinks today.** The only Wikidata-API caller
   (`placetypes/mapping_utils.py:246-289`) requests `props=labels|descriptions` only.

**Conclusion:** Wikipedia links are not available through any current WHG API. Two routes.

## Option A — client-side via the public Wikidata API (recommended for the Workbench)
For each accepted candidate whose id is **Wikidata-namespaced** (`place:wd:Q…`), derive the QID and
call Wikidata directly from the browser:

```
https://www.wikidata.org/w/api.php?action=wbgetentities
    &ids=Q16202|Q90|…            (deduped QIDs, ≤50 per call)
    &props=sitelinks
    &sitefilter=enwiki           (+ the user's preferred-language wiki, if any)
    &format=json&origin=*        (origin=* → Wikidata returns CORS headers)
```
- Parse `entities.Q…​.sitelinks.enwiki.title` → build `https://en.wikipedia.org/wiki/<encoded title>`.
- Batch (≤50 QIDs/call), dedupe across accepted matches (multi-select), cache by QID in IndexedDB.
- **Enrichment pane:** a new opt-in checkbox "Wikipedia links (via Wikidata)"; new export columns
  `wikidata_qid`, `wikipedia_lang`, `wikipedia_title`, `wikipedia_url`. Show the link in the review card.
- Fits the Workbench's local-first design: no WHG server, index, or gateway change; ships entirely
  in the front-end where the rest of the tool lives.

**Caveats (A)**
- Works **only for Wikidata matches**. GeoNames/TGN/OSM/Pleiades matches have no QID in the candidate
  payload → no Wikipedia unless the reviewer also picked a `wd` candidate (multi-select helps here).
- Makes **outbound calls to wikidata.org** — the one place the Workbench reaches beyond WHG (like
  reconcile reaches WHG). Must be clearly opt-in and documented (privacy note: only QIDs are sent).
- Not every QID has an `enwiki` sitelink; offer a language-fallback list (preferred → en → any).
- External rate limits / offline: degrade gracefully, cache aggressively.
- Attribution: we store only a title/URL (fine); note Wikipedia content is CC-BY-SA.

## Option B — server-side (surface the sitelinks WHG already indexes)
Because the `wd` index already holds `sitelinks`, WHG could expose them centrally:
- Extend `make_candidate` (`reconcile_helpers.py:204-232`) and/or `/entity/{id}/api`
  (`api/views_entity.py`) to read `_source.sitelinks` for `wd`-index hits and emit a `wikipedia`/
  `sitelinks` field; stop the gateway dropping them (`crc_client.py:462-469`).
- Then the Workbench (and the main WHG UI, and non-`wd` matches that carry a `wd` closeMatch) just
  consume it — no external calls.
- **Downsides:** cross-cutting change to the reconciliation contract + CRC gateway + api layer, and it
  depends on the live index actually being **populated** with sitelinks (the mapping exists, but no
  ingestion code in *this* repo writes them — the `wd` index is built externally, so confirm first).

## Recommendation
Ship **Option A** in the Workbench now (self-contained, local-first, no cross-team dependency). Track
**Option B** as the longer-term "do it once, centrally" improvement that also benefits the main site.

## Open questions
1. Confirm the live `wd`/`wdgn` index actually populates `sitelinks` (mapping present; ingestion external).
2. Preferred-language policy — default `enwiki`, or follow the map's language switcher?
3. LPF relation type for a Wikipedia URL in `links` — `closeMatch` is wrong; likely `seeAlso` /
   `primaryTopicOf`. Decide before writing it into exports.
