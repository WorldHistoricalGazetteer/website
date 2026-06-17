# WHG Korea Demo — Planning Ideas

> Prepared for the planning session with Ali and Palak ahead of the Korea trip.
> Goal: agree what to show, how to show it, and what short prep work would make the
> demonstrations land well.

## Framing for the meeting

The strongest narrative for Korea is **"WHG at scale, end to end"**: 47M+ places now
searchable, a working pipeline that turns a 7-volume 1856 print gazetteer into linked
data, a standards-compliant Reconciliation API for external tools, a reimagined
map-first explorer, and an honest rights/attribution model underpinning all of it.

Suggested approach: agree on **one headline demo** (the Atlas explorer or GOTW are the
strongest candidates) and use the rest as supporting acts.

---

## 1. Gazetteer of the World (GOTW) — the showpiece

**Live, server-less explorer:** https://worldhistoricalgazetteer.github.io/gazetteer-of-the-world/

- **What to show:** zoom from density heatmap → population-scaled circles; click a place
  for its WHG match, type, population, and one-click deep link to the HathiTrust scan;
  open the reader modal scrolling across volume boundaries; run a cross-script phonetic
  search (e.g. "Moscow" → "Moskva").
- **The scale line:** ~90k headwords → ~116k places → ~95.6k matched to modern WHG ids,
  all from an 1856 7-volume print, served from GitHub Pages with **no backend**.
- **Why it matters to WHG:** it's a reference implementation of authority-gazetteer
  ingestion. Self-hosted OCR (Surya) + self-hosted LLM extraction (Llama-3.3-70B +
  critic/repair) on Pitt CRC — ~$84 if it had run on commercial APIs, free on the
  allocation. The `WHG-LESSONS.md` patterns (range-requested PMTiles/FTS, in-browser
  Symphonym ONNX, GitHub-Issues curation loop) feed directly into WHG's own explorer
  rebuild.
- **Discussion points:**
  - Is GOTW positioned as a *demonstrator* or a *product*?
  - Be candid that OCR/parse stages are bespoke per source — the kit generalises, the
    segmentation rules don't.
  - ~16.4k unmatched places need a human-in-the-loop georeferencer (possible
    collaboration ask for Korea).
  - The 27.5k-toponym concordance from Vol. VII is a genuinely unique WHG asset worth
    flagging.

## 2. The indexing upscale — the foundation

- **The headline number:** ~47M+ place records now indexed on the Pitt CRC Elasticsearch
  gateway (GeoNames ~12M, Wikidata ~8M, OSM ~6M, TGN ~3M, GB1900 ~2.5M, Pleiades, OHM,
  LoC…).
- **What to show:** the breadth of a single search across all authorities; the separate
  `toponyms` index with phonetic embeddings powering cross-script/fuzzy matching.
- **Discussion points:** this is the substrate everything else (Atlas search,
  Reconciliation API, GOTW matching) sits on. Decide how much infrastructure detail (CRC
  gateway, beta cohort routing) is audience-appropriate vs. just showing the result.

## 3. Reconciliation API — for the data-tools audience

- **Standard:** W3C Reconciliation Service API v0.2 — works with **OpenRefine** out of
  the box (a compelling live demo if anyone in Korea uses it).
- **What to show:** point OpenRefine at `/reconcile`, reconcile a column of place names
  against 47M+ records; namespace filtering (`namespaces=gn,tgn`); data extension (pull
  population/coordinates for matched ids); the newer **period reconciliation** via
  chrononyms.
- **Known rough edge:** per-result attribution is not yet wired into responses (the
  `attribution_for()` helper exists in `api/attribution.py` but isn't attached) — see
  coding task #1 below.

## 4. Atlas features (dev-only) — the future of the platform

These are visible only on `dev`. Best grouped by user journey:

- **Map-first Explorer / hero map:** full-viewport map, floating dual-mode search (Areas
  vs. Toponyms), persistent temporal scrubber for year-range filtering, layer-source
  palette, globe↔flat projection. **Most visually striking dev feature** — strong
  headline candidate.
- **AAT place-type tree** (`/types/tree/`): interactive hierarchical type navigation
  (1,130+ Getty AAT concepts) with cross-vocabulary mappings; searchable,
  accent-insensitive.
- **Gazetteers/authorities panel:** filter results by source, see provenance of each
  contributing gazetteer.
- **Leads tracker** (`/leads/suggest/`): public "suggest a dataset" form + admin triage
  board replacing Trello — a good *community-contribution* story for Korea.
- **Supporting polish:** accent-insensitive search, keyboard nav, map-label language
  preference (i18n), CRC beta search routing.
- **Discussion point:** what's stable enough to demo live on `dev` vs. what should be a
  screen-recording? The explorer is mid-development; agree on a safe demo path before
  Korea.

## 5. Licensing / attribution overhaul — the trust layer

- **The story:** WHG was previously asserting conflicting licences in 9+ places and
  hard-coding CC-BY-NC. The new model is a single, truthful, **per-source** rights
  system: SPDX-anchored `License` model, NISO **CRediT** contributor roles with ORCID,
  and a WHG overlay licence asserted *alongside* (never instead of) each source's rights.
- **What to show:** dataset/collection pages now showing the correct source licence
  badge; CRediT contributor roles exportable to CSL/DataCite.
- **Audience fit:** more of a *talking point than a flashy demo* — frame it as "why WHG
  data is safe to build on," which matters for institutional/Korean partners.

---

## Short coding tasks to support the demos

Ranked by demo impact ÷ effort:

1. **Wire per-result attribution into the Reconciliation API** (highest payoff). The
   `attribution_for()` / `attribution_block()` helpers already exist in
   `api/attribution.py`; they just aren't attached to reconciliation POST responses.
   Doing this makes the OpenRefine demo show source + licence per candidate — a clean,
   complete story. *Also the explicitly identified high-priority remaining piece.*
2. **A scripted "demo deck" of reconciliation calls** — a small runnable
   notebook/script (place query, batch of 50, namespace-filtered, period query,
   data-extension) so Ali/Palak can show the API live without typing JSON by hand.
3. **GOTW preset deep-links** — curate a handful of `#entry=` shareable links (a
   striking place, a cross-script search, a table-rich entry) so the live explorer demo
   is reliable and rehearsed.
4. **A safe demo route for the Atlas explorer on `dev`** — identify/smooth any rough
   edges on the specific click-path to be demoed, so it doesn't break live.
5. **A one-page "feature cheat sheet"** (URLs, talking points, fallback screenshots) for
   Ali and Palak to carry.

---

## Two decisions worth making in the meeting

1. Which single feature is the **headline**?
2. Which demos run **live** vs. **recorded** (especially anything on `dev`)?
