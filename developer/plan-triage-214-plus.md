# Triage Plan — `place` issues #214 and above

**Written** 2026-09-21 by the `whg3-6d` session, jointly with the `indexing-79` session.
**Scope** every `WorldHistoricalGazetteer/place` issue numbered #214 or higher: 72 issues, of which
42 were open when this pass began.
**Mandate** assess and sort only. No fixes were attempted. Issues answered by code that has since
shipped were closed with evidence.

> **This file is the durable index of that pass.** It is not a substitute for the issues: every issue
> in the range carries far more measurement than is repeated here, and several carry corrections to
> their own bodies. This file says **who owns what, in what order, and which orders are not
> negotiable**.

---

## 1. How to read the labels

Two new label families were created on the `place` repo. They are the machine-readable half of this
file; if the two disagree, the labels are canonical because they are edited in place.

| label | meaning |
|---|---|
| `repo:website` | handled in `whg3`/`website` — Django, the reconciliation/entity API, browser JS |
| `repo:indexing` | handled in `indexing` — ingest, the CRC gateway, tilesets, Elasticsearch |
| `unit:*` | the work unit it belongs to (§4). An issue has exactly one. |
| `needs-decision` | blocked on a decision by SG, not on engineering |
| `blocked-by-order` | must not be actioned before a prerequisite named in its own comments |

**Both `repo:` labels means both, and means the two halves must land together** — not that either
side may proceed alone. Where only one half is buildable now, that is said explicitly below.

⚠️ **An unlabelled issue in this range is an oversight, not a "no owner".** Every open issue ≥214 was
given at least one `repo:` label in this pass, including #236, whose *decision* is SG's but whose
*work* is entirely indexing's. An unowned issue is how a costing exercise becomes a permanent open
tab.

---

## 2. Closed in this pass — six issues, all on evidence

| # | why it closed | evidence |
|---|---|---|
| **#262** | gateway-error reporting fixed and on prod | the `and spatially_scoped` clause is gone on `main`; verified in the prod container with the gateway mocked to fail; **an external consumer's own cache turned up 10 pre-fix degraded responses** dated 21 min before the deploy |
| **#282** | `encodeScript` now refuses a vocab with no `OTHER` | `27f3463ed`; prod bundle byte-identical to `origin/main`'s |
| **#283** | versioned + hash-checked assets, live | `5cdbbbbae`; all four prod assets hash to the manifest **and** to the model's own `provenance.json` |
| **#284** | `PY_IS_ALPHA` repinned to 14.0.0, header retracted in place | `0e6f51c83` |
| **#257** | `panphon_embedding` gone from the mapping; v7 index deleted | live cluster, `indexing-79` |
| **#250** | `is_script_mismatch` fixed *and* the index rebuilt after it | indexing `2f093c4` (6 Sep) + v8 build (14 Sep); `tgn` now carries 649,953 Latn-family docs against 0 |

**The unlock for four of those was a single measurement.** #282/#283/#284/#285 each ended with *"production
deploy not yet run"*. Prod `asset_version` is `aa70a1891` — the v8 bundle rebuild on `main` — so the
deploy they were all waiting on **had been run and nobody had gone back to look**. Three of the four
closed on that alone.

⚠️ **Two caveats carried into the closing comments verbatim, and worth keeping:**

* **#257 closed on the mechanism, not the magnitude.** The field and the old index are gone; free
  space was **not** re-measured, and a `df` read just after a large delete is a known way to get a
  stale answer. **The ~55 GB is not claimed as recovered.** If that figure matters — and on the volume
  that took production read-only in #255, it does — it gets measured under #255.
* **#250 likewise.** Its 1,166,572 counts *source rows*; the store holds *deduplicated* toponyms. The
  two can never match and putting them side by side would invent a shortfall.

### Held open deliberately, pending one runtime check each

Neither of these is a code question. Both are "is the running process the one we pushed", which is a
different question from "is the fix merged", and this project has been caught by that distinction
before.

* **#261** (IoGuard inner waits) — fix `b0e0179` is pushed. Wanted: the **rider rate** since 10 Sep
  (fail-closed scope reports carrying the "hard-link store is currently unresponsive" rider vs all
  scope-not-applied reports). The issue's baseline is 3060/10042, 1234/2833, 32/160. A collapse is
  evidence from behaviour; the tree is not.
* **#285** (ship the v8 encoder) — the whg3 half is verified on prod. Wanted: `/api/health` →
  `stores.symphonym.client_vectors` showing `accepted > 0` and `last_client_model: "v8"`.
  ⚠️ **That counter is per-worker — poll repeatedly.** A single `accepted: 0, last_client_model: null`
  is an ordinary reading on a working system. Closing on one poll would be wrong in either direction.

---

## 3. Priority — what is doing damage now

Urgency here means *live harm at a measured rate*, not age and not difficulty.

### P0 — actively costing someone something today

| # | owner | why now |
|---|---|---|
| **#274** | website | one `POST /reconcile` holds **one of four** gunicorn workers for its whole fan-out — up to 70 s at the advertised batch of 50. Gateway slowness becomes **site-wide 503s**. This is the only issue in the range whose blast radius is the entire platform. |
| **#267** | indexing | `fclasses` + any spatial constraint returns **0**. The one issue with a **named external project blocked on it** (EDOPS needs P/S-classed records). `sev:major` added. |
| **#272** | website | `/entity/` answers **404** on a gateway timeout, so "does not exist" and "we could not ask" are byte-identical — and the failure rate rises with load, so a client sampling metadata records absences **biased toward whenever it was going fastest**. 28 of 718 in a busy period. |
| **#275** | website | queries above 50 are discarded behind a **200**. Silent input loss is the defect class hardest to attribute afterwards. |

**#274, #275 and #268 are three facts about one number** (`batch_size = 50`): it is silently truncated
above, it pins a worker for ~70 s at its maximum, and nothing rate-limits the fan-out it authorises.
Fix them as one change or the fix for each will contradict the others.

### P1 — wrong answers at a measured rate

* **#273** — 16.6% of *confident* accepted matches were not places (lakes, airfields, a sewage works).
  **It is a ranking failure, not coverage**: in 5 of 6 probed rows the correctly-typed settlement was
  already in the pool and lost to an identically-named non-settlement. Three gaps, all needed.
* **#277** — `ukhc` holds Yorkshire as one 15,753 km² polygon with no ridings, so containment tests
  over **a sixth of England's historic parishes pass vacuously**. `sev:major` added. ⚠️ The "index the
  riding names as variants" option on #204 **makes this worse, not better** — the string would resolve
  and the test would still run against all 15,753 km². Only the riding split is a fix.
* **#216** — cataloguing placeholders (`Oknoname NNNNNN`) returned as top name-match candidates.
* **#256 → #246** — see §5. The hazard, not the defect, is what makes this P1.

### P2 — correctness infrastructure

#214, #217, #218, #259, #260, #265, #266, #269, #270, #276, #249, #258, #255, #263.

### P3 — programme work, properly scoped and not urgent

#219 (deferred, no consumer), #236, #240, #245, #251, #252, #253, #271.

### Quick wins — disproportionate return, hours not days

Worth doing out of priority order precisely because they are cheap:

| # | owner | the change |
|---|---|---|
| **#279** | website | two entries in one regex. It currently **costs a paragraph of workaround instructions on somebody else's public page** (the CCEd download panel tells volunteers to set diocese and archdeaconry by hand). |
| **#263** | website | set the `django.template` logger to `INFO`. ⚠️ Confirmed **not yet done** — no such logger constraint exists on `staging` or `main`. Severity is genuinely low (the records are DEBUG, so they reach neither GlitchTip nor the repo), but it is one line. |
| **#260** | website | `has_geom` is already computed and already sent to the browser (`api/crc_client.py:788`) and `grep has_geom whg/webpack/js/*.js` returns nothing. Warn with it. |
| **#278** | website | emit no relation when `parent_id` is absent, so a contributor is not asked for an identifier they do not have. |
| **#266** | indexing | filed **UNVERIFIED** by code reading alone. Settle it. It should not sit unmeasured while #265/#276/#277 are worked around it. |

---

## 4. The work units

Grouped by what is best done in one pass, which is not the same as grouped by subsystem.

### `unit:api-contract` — an API that tells the truth and survives load
**#272 · #274 · #275 · #268** — all `repo:website`. (#262 closed.)

One theme: *every failure mode here is silent*. A 404 that means "we could not ask", a 200 that
dropped half the batch, an outage indistinguishable from slowness because we never emit a 429.

Order: **#274 with #275 and #268 together** (one number, three consequences), then **#272**, which is
the same defect as the just-closed #262 on a different endpoint — #262 is the precedent and the
reference implementation.

⚠️ **The design constraint on #268, from its own body:** a naive request-rate limiter throttles
exactly the batching we publicly ask for and leaves the wasteful shape untouched. A well-batched
client is 2 req/min and 100 gateway queries/min; an unbatched one is 20 and 20. **Limit queries, not
requests.** The daily allowance already rewards batching correctly and should be left alone.

### `unit:candidate-quality` — what a candidate carries, and what its score means
**#273** (both) · **#216** (both) · **#214** (both) · **#218** (website) · **#217** (website) ·
**#219** (both, deferred)

Order: **#217 first.** It is documentation, it costs nothing, and #214's and #219's bodies both
establish that the ambiguity signals a caller needs are *already in the response* — the reporter who
triggered this whole cluster failed because nothing told him the query model (`contained_in` by
resolved id, not commas in the string). Fixing docs first also stops #219 being justified on a false
premise.

Then **#273**, which needs all three of: fetch `types` on the reconcile path, declare it on
`CandidateHit`, and project the AAT cross-vocabulary mapping that is currently 0% outside `tgn`. Two
of those three are indexing's.

⚠️ **On #214 and #216, reuse rather than redesign.** `isAutoConfirmed()`
(`whg/webpack/js/reconciliation.js:5313`) has implemented candidate-ambiguity detection since #184 and
has been tested in anger against real user data, including two refinements worth keeping: an *inexact*
name is no rival to an exact one however the scores tie, and same-place duplicates across sources are
deduplication, not ambiguity. An API caller cannot reach a line of it. **Port it; do not reinvent it.**

⚠️ **#218 is latent, not live** — prod has 0 `embargoed` rows. It would leak the first time the
mechanism is used for its stated purpose, which is the worst possible moment to find out.

### `unit:containment` — scope semantics, co-reference, and container data
**#266 → #265 · #276 · #277** · **#259 + #270** · **#269** · **#260**
All `repo:indexing` except **#260** (`repo:website`).

Order: **#266 first**, because if the H3 terms cap truncates in practice then #265's and #277's
measurements were taken through a lossy pre-filter and mean something different.

**#259 and #270 are one problem from two ends** and must be scoped together: TGN has zero usable
co-reference edges (#259) *and* the Wikidata extractor reads one identifier property, P1566, so every
`wd` edge points at GeoNames and P1667 (TGN) is ignored (#270). Either side alone would have covered
the gap; both are missing, which is why it was invisible.

**SG's decision of 2026-09-10 stands and constrains the fix:** harvest TGN co-reference as
`exactMatch` **only**. Explicitly *not* widening `_GEOM_LENDING_RELATIONS` to `closeMatch`, and
explicitly *not* asserting `sameAs` we do not have to satisfy a downstream filter. **Coverage is the
thing being traded away and it is the right trade** — an absent scope fails closed and is visible;
a wrong scope answers confidently. ⚠️ Measure the yield before assuming it closes #259.

**#260 is the cheap half of #259 and should not wait for it.** Whichever way #259 goes, some records
will always lack a usable boundary, and a user should learn that when they pick one rather than two
stages later.

### `unit:symphonym` — the browser encoder
**#285** only. (#282/#283/#284 closed.) `repo:website` + `repo:indexing`.

Both conditions in #285's body are met on the whg3 side: the v8 ONNX ships and `query_vector_model` is
sent (`api/crc_client.py:360-362`), in the same deploy. Closure waits only on the outside check (§2).

### `unit:toponym-quality` — ingest discards and re-ingest ordering
**#256 → #246** (hard gate, §5) · **#249** · **#258**. All `repo:indexing`. (#250 closed.)

**#249 is a classification campaign, not a defect**, and should not be treated as one issue to close.
Its own scope decision separates three mechanisms that **pull in opposite directions**: Mechanisms 1
and 2 are *real names that cannot be found* (recall), the third is *non-names that can be found*
(precision). ⚠️ A single "clean the names" pass would push one of them the wrong way, and the third has
the asymmetric downside — its remedy *removes* data, so a real toponym misclassified as junk is
deleted on a judgement call. **The bar for action is higher there than anywhere else in the issue.**

✅ The protective signal is already staged and is strong: a Wikidata `sameAs` is a **10–100× protective
signal** (18.67% baseline vs 0.16% on all-digit strings), so a safe filter can be built without a
heuristic over the string — which is the method that has misfired repeatedly in this codebase.

**#258 is not a defect** — nothing the TGN extractor produces is wrong. It is worth doing for one
reason: `gvp:historicFlag` is an **uncontaminated label**, asserted by Getty editors upstream of
everything we build, worth 195,458 historic↔modern pairs. Nearly every other label we have is produced
by the machinery being measured.

### `unit:phonetics` — rules, review, lexicon
**#251** (indexing) · **#252** (website) · **#253** (both) · **#245** (indexing)

⚠️ **Read #251's ceiling before scoping any tranche of rule work.** All rule work ever tops out at
**69.53%** of the 72.7M rows. `no_lang` is **25.51%** and needs *language identification* — worth ~26
points against all rule work's 1.1, twenty times the return, and a different project. Label those
numbers ceilings, not yields.

**#252 gates a release**: v8-beta trains on `proposed` values, full release after contributor
corrections arrive through the UI. It is on `staging` and running on dev at `/phonetics/`; #254 (the
image rebuild) is resolved, so the `panphon==0.22.0` blocker is gone. Still beta-gated and requires
`signed_off`, which is enforced rather than remembered.

⚠️ **#253 must share #252's register vocabulary**, not a parallel list that happens to overlap —
otherwise the rule set and the lexicon can disagree about Myanmar in different words and nobody can
query for the disagreement. And the per-place assertion wants **its own table**, not a third value in
the per-rule enum.

**#245** was reopened by SG on 10 Sep. Its closing premise — that the material was unavailable — is
false; the Wade-Giles tail is in a Getty release we already hold. Now that #250 is fixed those forms
are *in the index*, which is necessary but not sufficient: reaching 1856 spellings from them is still
open.

### `unit:contributor-ux`
**#260** (also containment) · **#278** · **#279** · **#240** · **#271**. All `repo:website`.
**#240** and **#271** carry `needs-decision`.

**#240** — adopting a source geometry can relicense the contributor's own dataset. Wikidata (CC0) and
GeoNames (CC-BY) are safe **by accident of which two were implemented first**; most of the others are
not, and share-alike exposure is 21.9M records. This needs a licensing ruling before code.

**#271** — half-shipped on `staging` (`09d05c77b`). SG's own note is the blocker: a PID must resolve to
something readable while retrieval is gated for licensing and anti-bot reasons, which tangles with
**#268**. ⚠️ **Sequence #268 first** — otherwise #271 ships a public identifier surface with no rate
limiting behind it, which is the combination its own comment warns against.

### `unit:platform-safety`
**#255** (indexing) · **#263** (website). (#257, #261 resolved/pending §2.)

**#255 is half done.** `5e66a43` added the `CHECKPOINT` before the promoting copy. The half that
remains is the durable one: **no stage asserts free space.** What exists is a check in the *submission
wrapper* (`developer/sbatch-templates/reextract-02-rebuild.sbatch:78-81`), so a run submitted any other
way is unprotected. Given the failure mode was *a rebuild taking production read-only*, that belongs at
the stage, not with whoever types the command. ⚠️ And the `CHECKPOINT` lesson generalises: it is a
property of how this repo uses DuckDB, not of one script — but `symphonym_cache.duckdb` was **measured
and is not affected**, so do not assume.

### Unassigned to a unit
**#236** — MapTiler independence. `repo:indexing` + `needs-decision`. A costing exercise with a
decision already recorded ("independence is a goal, all three recommendations adopted"). The honest
split is in the issue: `whg-portal`/`WHG` are achievable and need only `transportation`; the `OSM`
style is unlikely; `satellite` is never. Its water-rebuild half is already delivered at 100.0%
retention.

---

## 5. 🛑 The one ordering constraint that must not be broken

> **#256 must be resolved before #246 is actioned. Same work unit, that order, no exceptions.**

#246 asks for date ingestion to be fixed and the corrected data re-tiled for `osm` and `tgn`. Acting
on it means re-ingesting OSM/OHM. But `osm-places.py:305` and `ohm-places.py:313` still build **every**
way as a LineString, area-tagged or not. The 10.76M way polygons in production today were put there by
an in-place augmentation pass in July (place#145), **not by a handler fix**, so they do not survive a
re-ingest.

**A re-ingest performed to satisfy #246 therefore returns every closed area-way to a LineString:**
`has_geom: true` with no usable polygon, `h3_cover` collapsed to one centroid hex, and
`containment=exact` degrading to a `repr_point` test — **with nothing raised.**

⚠️ **#246 already carries two warnings about a "finished" fix silently not being finished — retiling,
and the staged snapshot. Neither would catch this**, because the tileset and the staged snapshot would
both be freshly and *consistently* wrong. This is a third warning of the same kind running in the
opposite direction: here the act of repairing is what breaks something else.

Two acceptable orders: fix the handler first (port `processing/osm_way_area_geometry.py`, a working
reference), or re-run `processing.osm_way_area_geometry` afterwards **as a mandatory step of #246**,
not as a follow-up. ⚠️ The eligibility gates are source-specific — `osm` 7 tag keys, `ohm` 13; using
the OSM set for OHM silently drops ~57k ways.

Recorded as a comment on #246 and labelled `blocked-by-order`.

---

## 6. What "already shipped" means, per side

Stated so that a future reader can date any claim in this file.

| | line | note |
|---|---|---|
| `whg3` prod | `asset_version` = **`aa70a1891`** | `main` HEAD was `01e74571f` (two test-only commits ahead) |
| `whg3` dev | branch `staging`, `c59bf2130` | |
| `indexing` | **`origin/main` = `de22578`** | ⚠️ a cron watchdog on `pitt` pulls `origin/main` and restarts the gateway, so **pushed ≈ deployed** with nobody deciding to deploy |

⚠️ **That watchdog is why §2 holds #261 open.** On the gateway side a merge reaches production without
a deploy step, which is convenient and also means *nothing records the moment it happened*. "It is
pushed" is a claim about a tree; "the rider rate collapsed" is a claim about the running process. For a
runtime behaviour, take the second.

---

## 7. Method notes for whoever runs the next pass

* **Check whether the deploy happened before believing an issue's last comment.** Four issues sat open
  solely because each said "production deploy not yet run" and none had been revisited. One
  `asset_version` read closed three of them.
* **Verify a data fix by reading the data back, not the code** — and know the false negatives. On #250,
  `lang:*-Latn*` returns a confident **0 corpus-wide** because the tag is stored split into `lang` +
  `lang_variant`, and the separator is sometimes `_`. Either would have supported "still broken".
* **Verify a build-step fix against the build date.** A fixed script with an unrebuilt index changes
  nothing. #250 closed because `2f093c4` (6 Sep) precedes the v8 build (14 Sep).
* **Do not quote a magnitude across a change of denominator.** Source rows vs deduplicated toponyms
  (#250); a `df` read after a delete (#257); exposure vs realised counts (#265, where a 3× ratio
  compared an upper bound against a realised count and bounded nothing).
