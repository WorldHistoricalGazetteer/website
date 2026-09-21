# Triage Plan — `place` issues #214 and above

**Written** 2026-09-21 by the `whg3-6d` session, jointly with the `indexing-79` session.
**Scope** every `WorldHistoricalGazetteer/place` issue numbered #214 or higher: 72 issues, of which
42 were open when this pass began. **Seven closed; 35 remain open.**
**Revised** later the same day, after the `indexing` session settled #261, #266 and #285.
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

## 2. Closed in this pass — seven issues, all on evidence

| # | why it closed | evidence |
|---|---|---|
| **#262** | gateway-error reporting fixed and on prod | the `and spatially_scoped` clause is gone on `main`; verified in the prod container with the gateway mocked to fail; **an external consumer's own cache turned up 10 pre-fix degraded responses** dated 21 min before the deploy |
| **#282** | `encodeScript` now refuses a vocab with no `OTHER` | `27f3463ed`; prod bundle byte-identical to `origin/main`'s |
| **#283** | versioned + hash-checked assets, live | `5cdbbbbae`; all four prod assets hash to the manifest **and** to the model's own `provenance.json` |
| **#284** | `PY_IS_ALPHA` repinned to 14.0.0, header retracted in place | `0e6f51c83` |
| **#257** | `panphon_embedding` gone from the mapping; v7 index deleted | live cluster, `indexing-79` |
| **#250** | `is_script_mismatch` fixed *and* the index rebuilt after it | indexing `2f093c4` (6 Sep) + v8 build (14 Sep); `tgn` now carries 649,953 Latn-family docs against 0 |
| **#261** | the fix is **in the running gateway process** | deployed tree `a08313d` is a descendant of `b0e0179`; watchdog reflog at 11:28:02, process start 11:28:04 — pull-then-restart, two seconds apart |

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

### The two runtime checks — both settled, and one of them taught us something

Both were "is the running process the one we pushed", which is a different question from "is the fix
merged". Both were answered by the `indexing` session; they landed in opposite directions.

**#261 — CLOSED, but not by the check this Plan first proposed.** 🛑 **That check was uncomputable, and
would have produced a confident wrong answer.** The rider rate (fail-closed scope reports carrying the
"hard-link store is currently unresponsive" rider) cannot be computed at all, for two independent
reasons:

1. **the rider string is never logged** — `grep -c` across all six gateway logs returns `0` six times.
   It is a response-body field assembled in `build_scope_info` (`spatial.py:1180`), not a log line. This
   issue's own 3060/10042 table came from somewhere else, most likely a consumer-side response capture;
2. **retention does not reach back before the deploy** — logs start 15 Sep 02:56, the fix deployed
   10 Sep. There is no "before" side.

⚠️ **So a low rider count since 10 Sep would have been largely an artefact of the log rotation, reported
as the fix working.** A check whose subject is absent cannot fail. It closed instead on direct evidence:
deployed tree `a08313d` is a descendant of `b0e0179`, the watchdog reflog fast-forwarded at 11:28:02 and
the process started at 11:28:04, and the derived-timeout code is on disk in that tree. Supporting: 4
breaker opens in six days, all four within one three-second window and all reading `thread(s) still
stuck` — a genuine hang, i.e. the guard working, not the spurious contention trip #261 described.

**#285 — STAYS OPEN, and this is the more interesting outcome.** The acceptance criterion as written
*is* satisfied: `accepted=1, discarded=0, last_client_model="v8"`, stable across 15 polls.
⚠️ The per-worker caveat was load-bearing — the view flaps between `1/"v8"` and `0/null` depending on
which worker answers, so one poll would have misled **in either direction**.

🛑 **But `accepted` is 1 against 569 `POST /api/reconcile` over 5 d 18 h, and that one is most plausibly
our own test.** Two different claims, and only the first is established: *a v8 vector can be accepted
and the generation handshake is correct* (✅), versus *the offload is operating on real traffic* (❌).
The second is what the issue's own title is about.

🛑 **The gate is a CONDITION, not a date — deliberately.** An earlier draft of this file said "re-poll in
3–4 days", and that was a promise with nothing to execute it: no session that wrote it will exist then,
and a discrete intention arms nothing. So the trigger is stated on the issue instead, as a precondition
of closing it:

> **Before closing #285, poll `/api/health` → `stores.symphonym.client_vectors` (repeatedly — per-worker).
> If `service.accepted` is still 1, do not close it.**

That makes the issue **self-gating**: whoever next touches #285 runs the check, whenever that is, and no
timer has to have been set by anyone. ✅ The check needs no context from this pass — it is one request
against the gateway's health endpoint — which is a virtue, not a shortcoming: anybody can run it.

If `accepted` is still 1 the offload is off in practice, and the follow-on is a **whg3** question — why
the browser is not producing vectors on real runs — which is a different defect and should be filed as
one rather than dragged into #285.

---

## 3. Priority — what is doing damage now

Urgency here means *live harm at a measured rate*, not age and not difficulty.

### P0 — actively costing someone something today

| # | owner | why now |
|---|---|---|
| **#274** | website | one `POST /reconcile` holds **one of four** gunicorn workers for its whole fan-out — up to 70 s at the advertised batch of 50. Gateway slowness becomes **site-wide 503s**. This is the only issue in the range whose blast radius is the entire platform. |
| **#267** | indexing | `fclasses` returns **0 unconditionally** — ⚠️ the spatial constraint in the original title is a **red herring**, retitled 2026-09-21. `types.label` is mapped `text` with no `.keyword`, and `es_helpers.py:1179-1183` runs a `terms` query against it, so the documented uppercase letters have never matched. The one issue with a **named external project blocked on it** (EDOPS needs P/S-classed records). `sev:major` added. |
| **#272** | website | `/entity/` answers **404** on a gateway timeout, so "does not exist" and "we could not ask" are byte-identical — and the failure rate rises with load, so a client sampling metadata records absences **biased toward whenever it was going fastest**. 28 of 718 in a busy period. |
| **#275** | website | queries above 50 are discarded behind a **200**. Silent input loss is the defect class hardest to attribute afterwards. |

**#274, #275 and #268 are three facts about one number** (`batch_size = 50`): it is silently truncated
above, it pins a worker for ~70 s at its maximum, and nothing rate-limits the fan-out it authorises.
Fix them as one change or the fix for each will contradict the others.

🛑 **#267's P0 commitment is the one-line unblock only, and the Plan must not be read as promising more.**
There are two fixes and they are not the same size:

1. **lowercase the `fclasses` input** before building the `terms` clause — genuinely a one-liner, correct
   for the documented contract (GeoNames classes are single letters and survive the standard analyser as
   single lowercase tokens). **This is what unblocks EDOPS.** ⚠️ It works by relying on analyser
   behaviour and is correct **only for single-token labels** — a `terms` query can never match
   `historic county` whatever the case — so it fixes `fclasses`, not "filtering on `types.label`";
2. **add a `keyword` subfield to `types.label`** — a mapping change, and ⚠️ **adding a subfield does not
   populate it for existing documents**, so it needs a reindex or `_update_by_query` over **51.2M docs**,
   plus a decision on whether top-level `fclasses` should exist at all (it is absent from the live mapping
   and `exists: fclasses` is **0 for `gn`** as well as `whg` — corpus-wide, not a `whg` gap).

**(2) is a separate item, not P0.** Stated here because a Plan that ranks (1) at P0 and leaves (2) implicit
is a Plan promising a quick win with a 51.2M-document reindex behind it.

### P1 — wrong answers at a measured rate

* **#273** — 16.6% of *confident* accepted matches were not places (lakes, airfields, a sewage works).
  **It is a ranking failure, not coverage**: in 5 of 6 probed rows the correctly-typed settlement was
  already in the pool and lost to an identically-named non-settlement. Three gaps, all needed.
* **#277** — `ukhc` holds Yorkshire as one 15,753 km² polygon with no ridings, so containment tests
  over **a sixth of England's historic parishes pass vacuously**. `sev:major` added. ⚠️ The "index the
  riding names as variants" option on #204 **makes this worse, not better** — the string would resolve
  and the test would still run against all 15,753 km². Only the riding split is a fix.
* **#266** — **promoted from P2 on 2026-09-21**, because it was confirmed and its trigger turned out
  not to be what the title said. A multi-container `contained_in` **silently drops the H3 ancestor
  clause at 4 containers** — while the term count is still under the cap, so the only number anyone
  would check looks fine — and truncates arbitrarily at 6. Retitled; `bug` + `sev:major` added.
* **#216** — cataloguing placeholders (`Oknoname NNNNNN`) returned as top name-match candidates.
* **#256 → #246** — see §5. The hazard, not the defect, is what makes this P1.

### P2 — correctness infrastructure

#214, #217, #218, #259, #260, #265, #269, #270, #276, #249, #258, #255, #263.
(#266 moved up to P1; #261 closed.)

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
| **#266** | indexing | ~~settle the UNVERIFIED question~~ — **done 2026-09-21, and it is real, so this is no longer a quick win.** It has moved to P1. ✅ What it *did* hand us for free is an acceptance test needing no oracle and no live index: **the term count is non-monotonic in region size** (3 containers → 3,620 terms, 4 containers → 3,399, while the region grows). Any test asserting monotonicity fails today and passes after a fix. |

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

Order: **#266 first — confirmed, and the conditional is now discharged.** When this Plan was first
written the justification read *"if the cap truncates in practice, then #265's and #277's measurements
were taken through a lossy pre-filter"*. **It does truncate. So they were** — whenever those measurements
used a multi-container scope. 🛑 **Ask whoever took them how many containers they used**; at 4 the ancestor
clause is already gone.

 Measured 2026-09-21 with
the shipped `_es_h3_terms` imported directly. ⚠️ **The trigger is a multi-container `contained_in`, not
a large region** — no single container reaches the cap, because `_pick_polyfill_resolution` scales cell
size to the bbox (Kazakhstan 1,312 terms, 33% of cap; Russia *fewer* cells than Kazakhstan). Anyone who
tests this with one big country will conclude it is not real. The issue has been retitled for that
reason.

🛑 **Two defects, and the first is earlier and quieter than the issue described.** At **4** containers
the ancestor clause is dropped entirely while the count is still *under* the cap (3,399 of 4,000), so
nothing looks wrong; the ancestor clause is the one that "catches large candidates spanning the region",
so it vanishes exactly when the region is largest. At **6+** the cover is cut to an arbitrary 4,000 of a
`set` — hash order, no spatial logic, not stable across runs. **Neither sets `approximate`**, so it is
invisible in `scope`: the same class as place#262, a partial answer presenting as a whole one.

⚠️ **This changes #276's requirements, so #276 must not be built first.** #276 asks the scope report to
state the container's size. A scope that has silently lost its ancestor clause is precisely a scope whose
reported size would be a **lie** — it would report the area asked for while querying a recall clause that
no longer covers spanning candidates. Fixing #266 first, or #276 adds a confident number on top of a
silent degradation.

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

⚠️ **Read #251's ceiling before scoping any tranche of rule work — using the CORRECTED figures.** The
69.53% / 25.51% pair that circulated on that issue was measured on the **pre-top-up denominator of
72,703,552**; the corpus is **73,479,069**, and `developer/plan-symphonym-v8.md` §16.1 supersedes them
explicitly. Current set:

| lever | points | what it costs |
|---|---|---|
| **lift the `ceb`/`war`/`min`/`vo`/`mul` quarantine** | **4.643** | a **policy decision. No code.** |
| language identification (`no_lang`, 25.24%) | ~25 | a different research project |
| **all remaining rule work — this is #251** | **1.188** (67.771% achieved → 68.959% ceiling) | linguistics, per script |

🛑 **The quarantine lever is the correction that matters**, and it was missing from the two-term
comparison everyone was quoting: **3,411,436 names, 3.9× everything all rule work could ever deliver, and
it needs no engineering at all.** A Plan that told the next reader to weigh a 1.2-point project against a
26-point research programme while omitting a 4.6-point lever sitting behind an unasked decision would be
omitting the single most actionable fact in the area.

⚠️ **The achieved baseline FELL while the work succeeded** — 68.43% → 67.771% — because the added rows are
overwhelmingly unroutable romanisations, so a larger denominator lowers the share while raising the count.
Quote both terms or a reader takes it for damage.

✅ **All three are ceilings, not yields.** The source's own split: the measured lines stop at the rule-work
ceiling; the language-identification and un-quarantine lines *additionally* assume every such row would
then route, which nobody has measured (a newly identified language may have no Epitran mode at all).

⚠️ **So the two large numbers carry an extra assumption the 1.188 does not — but 1.188 is still a ceiling,
just a far more tractable one.** Its inputs are measured (866,948 `no_route` rows); "all of them would
route once the rules exist" is the assumption. **Keep the word "ceiling" on it**, or it becomes the one
apparent *yield* in a table of ceilings, which is precisely how 69.53% hardened into fact.

🛑 **And do not attach the quality argument to this number.** Rule work is the only one of the three levers
that improves the *transcriptions* rather than the count of rows that have one, and that is a real point in
its favour — but **1.188 does not measure it.** 1.188 is a coverage share, like the other two; the quality
benefit is a separate axis nobody has quantified. Make the argument on its own terms or the next reader
cites 1.188 as evidence of a quality gain it says nothing about.

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
**#255** (indexing) · **#263** (website). (#257 and #261 closed — see §2.)

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
