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
| **#250** | `is_script_mismatch` fixed *and* the index rebuilt after it | indexing `2f093c4` (6 Sep) + v8 build (14 Sep); **`tgn`** now carries 649,953 Latn-family docs against 0 — namespace-scoped, NOT a whole-index figure (`Latn` 379,161 + `Latn-pinyin-x-notone` 270,531 + wadegile/hanyu tails). ⚠️ Quoted without its scope on 21 Sep it was unreproducible to a peer measuring whole-index `lang`×`script` totals (769,921 across 8 tags) and rightly rejected |
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

## 2a. Progress — 2026-09-21, end of day

**Closed: 50 of the 80 issues numbered #214+. Open: 30.**

Everything the whg3 side fixed in this pass is **now on production**. `main` and `staging` no longer
diverge in source at all — the only differences are the webpack bundles (rebuilt per branch) and
`server-admin/test-baseline.txt`, which is **staging-only and must stay that way**
([[reference_smoke_test_baseline_guard]]: it is branch-scoped and refuses cross-branch compares).

| promoted as | issues | on prod |
|---|---|---|
| `9256b4104` | #272 gateway-vs-miss, #275 oversized batch → 400, #274 gthread workers, #268 rate limiter | ✅ |
| `089258558` | #279 diocese/archdeaconry columns, #278 `parent_name` relation, #263 environ logging, #260 container warning | ✅ |
| `bd74a8e9c` + `49ca8e3e7` | #217 query model + `error` key, #240 match references, #271 citable identifier | ✅ |
| `f1d00f4df` | #214 two incomparable score scales in `/suggest/entity` | ✅ |
| `12c58d4ad` | #218 embargo enforced on reconciliation | ✅ |
| `c048de566` | #273 candidate type in the MyD review card | ✅ |
| `f89292a7f` | #269 attribution on the LPF path, 451 for a non-redistributable source | ✅ |

Gateway/indexing side: #267 (`fclasses` lowercase, indexing `2cb28d0`) and #266 (coarsen instead of
truncate, indexing `7fa4325`) are live on the gateway. #256 has shipped.

### Open with the fix already live — the issue is the *unverified claim*, not the code

These four are deliberately still open, and closing them needs a **measurement**, not a commit:

- **#274** — the worker model changed and one request can no longer monopolise a worker. The
  site-wide 503 behaviour under a slow gateway has **not** been reproduced or re-measured.
- **#273** — the display shipped. The 16.6%-of-confident-matches figure that motivated it has not
  been re-measured against the new card.
- **#240** — the references shipped; the licensing question about *adopting* a source geometry is
  untouched and is the part that matters.
- **#276 / #277** — #277 needs no ingest (ridings exist in `vob_cty`/`vob_rc` with polygons and
  working scope queries — it is a *selection* defect), which makes **#276 the user-facing fix**.

### Still not established, and not claimed

**#269 closed a live exposure, not a latent one.** Three registry rows carry `redistributable = False`
on production — `kain_par`, `nl`, `chgis` — so the entity API had been serving all three. That is the
opposite of #218, where the embargo mechanism had 0 rows and the leak was genuinely latent. Worth
keeping the distinction: "the mechanism was never used" and "the mechanism was never enforced" look
identical in a diff and are not the same finding.

🛑 **#269 does not settle the `kain_par` licensing question.** The public tileset still serves the full
polygons behind nothing but an `Origin` check, so the boundary set remains reassemblable. The per-record
API was the smaller half.

### #245 remeasured 2026-09-21 — 18.0% → 24.0%, and it settles less than it looks like

**24 of 100** sampled `ccode=CN` places, against **18 of 100** on 5 Sep. Same 100 places, not a fresh
sample (CN sample hash `4106109371657cda`, reproduced from two independently written selections;
probe and DB sha256-identical to the cluster copy). Instrument: `process/probe_reachability.py` in the
**GOTW** repo — *not* indexing — n=100/country, seed 7, MATCH_KM=25, exact then phonetic, size 20.

⚠️ **The denominator is not what the issue's prose suggests.** It is the share of sampled Chinese places
whose 1856 printed coordinate has *some* returned candidate within 25 km. It is **not** a share of indexed
places and **not** "Latin-script reachability" — I briefed it that way and was wrong. Controls in the same
invocation: **IN 50→50, RU 25→25, GB 76→75.** Three flat while CN moves +6 is the evidence that the
instrument did not drift; that control profile, not the interval, is the argument.

🛑 **+6.0 points is inside the noise of two unpaired n=100 samples**, and no paired interval can be
computed because the 5 Sep run recorded only the aggregate, so discordant pairs are unrecoverable and
McNemar is impossible. Fixed going forward: per-place outcomes are now kept
(`/vast/ishi/gotw/data/cn_paired_20260921.json`). **Quote the +6 only with that caveat attached.**

**Confounded, deliberately unattributed.** Between the two runs the served index also took Symphonym v8
(15 Sep), the CJK-IPA re-extract, and gateway fixes #267/#266/#273 — and the probe runs in `phonetic`
mode, so v8 is a more plausible mover than #250. Reported as a confounded before/after; no attribution
claimed. #245 stays **open**: 24% is still the finding, not a fix.

The run's own negative controls are worth copying: 0 gateway errors in 200 queries, 103 non-empty pools
against 97 empty, and a nonsense query at 0 before and after — because `_post` swallows failures into
`{"_error": …}`, so a **dead gateway would have reported a clean 0.0%**. The docstring's cautionary
example is an "after" of 24% from a fresh sample proving nothing, and 24% is what came back.

### Method note — promoting from a SHARED working tree

Three Claude sessions occupied `/home/stephen/Documents/GitHub/whg3` during this promotion, so
branch-switching in the primary checkout was unsafe ([[feedback_shared_working_tree]]). What worked:

1. `git diff --stat origin/main origin/staging -- . ':(exclude)static/webpack'` to see the **real**
   payload. 90 bundle files differed and **no webpack source did** — so no rebuild was needed, and
   main's own bundles had to be left alone. Diffing without excluding bundles makes a 3-file promotion
   look like a 93-file one.
2. A `git worktree` on `main`, files copied in explicitly, `git add <paths>` — never `-A`, never `-u`.
3. ⚠️ **A worktree is not a working environment.** Three needed files are untracked and had to be
   symlinked in: `whg/local_settings.py`, `.env/.env` (+`.pitt`) and — the one that cost time —
   **`whg/local_settings_autocontext.py`**, which `settings.py` imports *first* inside a
   `try/except ImportError: pass`. Its absence silently dropped `CELERY_BROKER_URL`, and the
   resulting `AttributeError` during app loading looked exactly like a defect on `main`.
   **What settled it in one command was running the same test in the primary checkout, where it
   passed.** A failure that appears only in the new environment is evidence about the environment.
   Beware the swallowing `except` generally: it converts a missing settings file into a missing
   setting, far from the cause.

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
| **#266** | indexing | ~~settle the UNVERIFIED question~~ — **done 2026-09-21, and it is real**, so no longer a quick win; moved to P1, then **fixed the same day**. 🛑 **The "assert monotonicity" acceptance test recorded here earlier was WRONG — see §4's containment entry.** Use coverage + ancestor-closure instead. |

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
`set` — hash order, no spatial logic, not stable across runs.

✅ **FIXED 2026-09-21** (indexing, awaiting push): when the term set would exceed the cap, **coarsen
instead of truncating** — lift the finest resolution group into its parents and rebuild until it fits. A
parent contains all its children, so coarsening cannot lose coverage; it only widens. Widening is free
because `h3_terms` is **only a prefilter**: one branch of a `should`, with `hit_matches` re-testing every
survivor against the true `region.cover_by_res`. Too wide costs a little work and changes no answer; too
narrow drops places silently. Measured 26 ms on a 24,571-cell cover, 72 ms on a 67,951-cell one.

## 🛑 The acceptance test recorded in an earlier version of this file was WRONG

That version said: *"the term count is non-monotonic in region size … any test asserting monotonicity
fails today and passes after a fix."* **The first half is true; the second is false, and a test built on
it would reject the correct fix.** On the fix, 3 containers → 3,620 terms and 4 → 2,180: still
non-monotonic, and legitimately so, because coarsening *reduces* the term count while *increasing* what
it matches — one parent stands in for up to seven children. **Monotonicity was a property of the broken
implementation's accident, not of correctness.**

✅ **Use these two invariants instead.** Both offline, no live index, no oracle, no threshold:

1. **Coverage** — every region cell is represented in the term set, by itself or by an ancestor.
2. **Ancestor-closure** — if a term is present, so is every one of its ancestors.

| containers | OLD uncovered cells | OLD terms lacking ancestors |
|---|---|---|
| 3 | 0 | 0 |
| 4 | 0 | **3,399** ← silent onset, count still under cap |
| 5 | 0 | 3,834 |
| 6 | 187 | 4,000 ← visible truncation begins |
| 12 | 1,201 | 4,000 |

With the fix: **0 and 0 at every step.** ⚠️ **Closure is the invariant that matters** — it fires at 4
containers where coverage still reads clean and the count is under the cap. Coverage alone would have
missed exactly the case the original issue was blind to.

### ⚠️ And there is now nothing for #276 to report here

An earlier version of this file said #266 changes #276's requirements because a scope that lost its
ancestor clause would report a size that was a lie. **That was true of the old behaviour and is not true
of the fix.** Coarsening degrades only the prefilter and the refine step restores precision, so the
answer is **not** approximate and setting `scope.approximate=true` would now be wrong. Fixing the
mechanism dissolved the silence problem rather than requiring a new field. #276 is no longer gated on
#266; it is an ordinary enhancement again.

⚠️ Narrower than first stated, in fairness: the bbox branch of the `should` still admitted anything whose
`repr_point` lay inside the region's bounding box, so what the old truncation *uniquely* lost was
candidates whose repr_point falls **outside** the bbox but whose geometry reaches in — large candidates.
A real silent false negative, but not "the region stopped being filtered".

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

🛑 **And when you make it on its own terms, do not reach for a residue figure either.** I did, on #251, one
paragraph after writing the warning above — offering `mya-Mymr` 16.6% → 98.7% and `sin-Sinh` 71.7% → 100.0%
as quality evidence. **Both are residue** ("did a rule fire?"), and the usability figures sit in a comment on
that same issue: `mya-Mymr` is **62.7%**, not 98.7%. A 36-point gap, withdrawn.

⚠️ **Read that as a property of this topic, not as one session's slip — it is the third instance of the same
substitution in one day.** Coverage→quality, then residue→usability, in consecutive paragraphs. Every figure
involved was transcribed correctly; what failed to travel was **which population it was measured over and
which question it answers.**

✅ **So the rule for this area: quote a PanPhon-usability figure with its denominator, or make the argument
with no number at all.** The strongest version needs none — a wrong shipped value is worse than an absent
one, because it is confidently wrong and nothing downstream can tell. `ှ → ʰ` parsed cleanly, passed every
lint, and asserted aspiration where Burmese has devoicing. **Coverage share cannot express that: a row with
a wrong transcription counts as covered.**

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

> ~~**#256 must be resolved before #246 is actioned.**~~ ✅ **GATE CLEARED 2026-09-21** — #256 is fixed
> (indexing `0cb0ac1`), the `blocked-by-order` label is removed, and Stephen has approved the cost, so
> #246 may proceed. The account below is kept because it is why the gate existed and what it prevented.

### 🛑 And a second blocker nobody knew about: #246 would have crashed in its first minute

While fixing #256 the indexing session found that **the OSM/OHM ingest could not run at all.** Both
handlers called `.with_locations(idx='flex_mem')`; **pyosmium 4.x renamed that parameter to `storage`**,
measured on the CRC `whg` env (4.2.0) and on 4.3.1:

```
idx= keyword  ->  TypeError: unexpected keyword argument 'idx'
```

`apply_file` raised on its **first statement**. Every other call site in the repo passes it positionally
and was unaffected; only the two authority scripts used the keyword. Fixed in the same commit.

⚠️ **The re-ingest this Plan spent a paragraph sequencing had never been possible**, and nobody knew
because the July augmentation pass (place#145) put the polygons in without the ingest, so nothing
exercised the path for months. **An unexercised code path carries no evidence of working** — a more
general lesson than the rename, and a fifth member of the family in
`developer/postmortem-ingestion-faults.md`.

✅ Loud rather than silent, so the better failure mode. But note the shape: #246's own two standing
warnings are about a *finished* fix silently not being finished. This was the fix being unable to start,
which neither warning could have caught.

### The cost that was approved

The planet ingest is now **two passes over the 92 GB PBF** plus osmium's area assembly, where it was one.
One pass cannot work: osmium emits areas only *after* every way, and the emit-both-and-let-the-later-row-win
alternative fails because `write_staged_place_doc` **appends** — both rows survive into `places.jsonl`, so
the winning geometry would depend on row ordering through four downstream stages. The reference pass
already does two passes in production, which is what made the cost known rather than speculative.

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
| `indexing` | `origin/main` = **`2cb28d0`** (was `de22578`) | see the correction below — a push does **not** deploy |

### 🛑 CORRECTED 2026-09-21: pushing does NOT deploy the gateway

An earlier version of this file said a cron watchdog pulls `origin/main` and restarts the gateway, so
**"pushed ≈ deployed"**. **That is wrong, and it is the kind of wrong that manufactures false closes.**

`gateway_watchdog.sh` is a **liveness** watchdog: it probes `/openapi.json` and acts only on a non-200.
**A healthy gateway is never restarted.** A push therefore *arms* the next restart; it does not cause
one. Measured on #267 — after the push landed, the deployed tree **stayed at `a08313d`** and the process
kept its 5 d 19 h uptime. Several pushes had gone by over those five days without deploying.

Deploying is an explicit act: `gaz_request.sh gateway-restart`, which pulls then restarts.

⚠️ **And a successful restart is not evidence that new code is running.** `do_restart()` treats a failed
pull as **non-fatal** and restarts on the old code while still reporting `EXIT: 0`. So neither the push
nor the exit code can be read as a deploy.

✅ **Check ancestry and timing instead** — the shape used on #261 and #267: deployed `HEAD`,
`git merge-base --is-ancestor <fix> HEAD`, the reflog entry dating the pull, and the process start time
against it. On #267: reflog pull at `07:26:03`, PID 778070 started `07:26:06`, three seconds later.

**#261's close is unaffected**, and for a reason worth keeping: it was closed on exactly this direct
evidence rather than on the "pushed ≈ deployed" inference. Had it been closed on the inference, it would
now be a false close.

---

## 7. Method notes for whoever runs the next pass

* **An observation drawn from the cases that succeeded is selection, not measurement.** 🛑 Caught by
  `gotw-3d` on itself, on the #245 remeasurement. Two of the 24 reached Chinese places matched
  administrative `-fu` titles (`Shaou-Wu-Fu → Shaowu Fu`, `Hoo-Choo-Fu → Huzhou Fu`), which looks like
  evidence that TGN's romanisation material rather than v8 phonetics did the work. It is not: the
  `-fu` headwords among the **76 that did NOT reach** were never counted, and without that base rate
  the observation has no content at all. Small n was the lesser problem. The general form — *pull a
  pattern out of the winners, never look at the losers* — will pass any review that only checks
  whether the cited cases are real, because they are.
* **A hint placed among measured figures is read as a measured figure.** The same aside originally sat
  at the end of a comment whose whole argument was that numbers must be quoted with their weight
  attached. Placement is an argument nobody can disagree with, so it survives review that prose would
  not. Moving it to its own comment, with `n=2` in the first line, is the fix; so is contradicting a
  retracted claim in writing rather than deleting it, because the next reader only ever sees the page.

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
* 🛑 **Two sessions can both be right about the same field name and disagree completely — check which
  LAYER each measured.** On #294, whg3's `/reconcile` returns
  `namespaces_searched: ['gn','iv','osm','wd','whg']` while the gateway's own meta returns `[]` for the
  identical query: the gateway's field means *the explicit positive scope*, and whg3 **constructs its own**
  from `whg` plus the gateway's `namespaces` (present-in-results). Each session measured its own side and
  reported a flat contradiction.
  ⚠️ **The fix depended on resolving it.** Under the wrong reading — a list of consulted sources with `gb`
  quietly missing — the obvious remedy is to add `gb` to `namespaces_searched`, which would break the
  "queried but matched nothing" signal that field carries for #157's attribution. Under the right one it is
  a **missing field**, and the fix is additive. ✅ *A correct-sounding fix aimed at a misdescribed defect is
  the expensive kind.*

* ✅ **Cross-checking works when each side re-MEASURES rather than re-reads.** Same pass: one session
  overstated `gb` as unreachable (it is reachable two ways — explicit positive scope, or
  `exclude_namespaces: []`), the other reported a wrong value for `namespaces_searched`. Both corrections
  held, and **neither session was checking its own claim.** Note the proposed *explanation* for the second
  error was also wrong — "you misread the adjacent `namespaces` field" — and could be falsified in one
  line, because whg3's response has no `namespaces` key at all. **Verify the correction, not just the
  claim.**

* 🛑 **`_update_by_query` DOES re-run an index's `default_pipeline` — settled 2026-09-21, and it was
  recorded as unverified here for most of the day.** Measured on a throwaway index with a pipeline that
  *increments a counter*, so the answer was a number: `pipeline_runs` 1 → 2 with no `?pipeline=` given, and
  3 with one (the control, proving the counter was not stuck). **So an in-place update to `places` is not a
  read-only act** — every document touched goes back through `extract_namespace`, which carries #249's
  `<2 characters` rule, and the write reports a full count and zero errors while discarding names.
  ⚠️ **The control was the load-bearing part:** a probe showing "the name is still null afterwards" would
  have proved nothing, because it was already null. This is what blocked #246's last item and #286's AAT
  projection, and it rules out the cheap route for both.

* 🛑 **A verification that reads the wrong field reports failure on correct data — and looks exactly like
  the known bug.** On #246's retile, the first scan found 0 of 241,602 features with a real start year,
  matching the 7 August failure signature precisely. Two causes: `limit 6000` on an mbtiles is a
  *contiguous geographic slice* in rowid order, not a sample (globally scattered dated places can be missed
  entirely — `order by random()` changed the answer); and `doc_temporal_bounds` is unbounded whenever any
  timespan lacks an outer edge, so `start = -9999` is **correct by design** for nearly every `tgn` place.
  The dates are in `start_def`/`end_def`. ✅ **Any retile check that reads `start` alone will fail a correct
  `tgn` tileset**, which is the next trap after "read the tileset, not the index".

* 🛑 **A shipped API change with no documentation change is a distinct, recurring failure — and it has
  its own name now.** Named by `documentation-6f` on #288 after **three instances surfaced in one day**:

  | instance | live | documented |
  |---|---|---|
  | #262's `gateway` presence-means-failure key | 8 Sep | 21 Sep |
  | five candidate fields (`confidence`, `repr_point`, `ccodes`, `place_types`, `wikipedia`) | long before | 21 Sep |
  | #288's `source_year` | not yet | **to be done in the same pass** |

  ⚠️ **The middle one is the worst of the three**, because `confidence` is the field a caller is *told* to
  threshold on and the table listed nine of fourteen fields — so the omission read as "these are all the
  fields", not as "some are missing". An undocumented field is not a gap a reader can see.

  ✅ **The rule: a contract change and its documentation ship in the same pass.** Since 2026-09-21 there is
  a dedicated session in the `documentation` repo — so the practical form is *tell it, in the same breath
  as the commit*. The cost of not doing so is not a missing paragraph; it is an integrator building against
  a shape that was never true, which is how #217 came to exist.

* 🛑 **Match the strength of a claim to the strength of the action it justifies.** Withholding, deferring,
  asking and flagging all run on uncertainty alone and need no diagnosis; a diagnosis is only owed when
  something will be *changed* on the strength of it. `documentation-6f` withheld the `temporal_extent`
  column (right) and justified it as *"these values are wrong"* (not established — `kain_par [1851, 1851]`
  is a correct snapshot year, and PeriodO's −4,567,998,050 ≈ 4.568 Ga is plausibly a real geological
  bound). Its own conclusion is the transferable one: **the cheapest fix is not measuring harder before
  withholding, it is not upgrading "I can't interpret this" into "this is broken."**

* 🛑 **The dominant failure in this pass was not arithmetic — it was provenance failing to travel with a
  number.** Three separate substitutions, all with correctly transcribed figures: a superseded ceiling
  (69.53%, corrected two sections below where it was stated, in the same document); coverage share used as
  quality evidence; and residue used as usability (98.7% where the usable figure was 62.7%). ⚠️ **In every
  case the arithmetic was right and the subject was wrong**, and in two of the three the correction was
  already written down somewhere the quoter had read. **Before quoting a figure, state which population it
  was measured over and which question it answers.** If you cannot, you do not have the figure yet.
  ⚠️ **And check you are quoting the denominator that answers the question asked.** `gn_fcodes` on
  "309 of 58,996 concepts" is 0.5% of the **vocabulary** and potentially most of the **records**, because
  those 309 are the head of the distribution. Quoted the first way it makes the work look pointless;
  quoted the second it makes it obviously worth shipping partial. Same numerator, opposite conclusion.
