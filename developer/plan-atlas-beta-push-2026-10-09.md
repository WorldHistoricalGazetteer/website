# Atlas beta push: 10 hours, 2026-10-09

**Tracking issue:** [place#316](https://github.com/WorldHistoricalGazetteer/place/issues/316). Progress is tracked there in checkboxes; this file is the reasoning behind it. Follow-ups: place#317 and place#318.

**Goal.** Make the Atlas, including the Gazetteers panel, a solid and honest beta-test target for
Crystal, with a test script she can follow and a snag route that reaches us.

**Scope.** Map your Data is out of scope; it is moving to PLATO Tools (place#314, plato-tools#28/#29).

**Budget.** About 20% of the regular allowance and 40% of the Fable allowance (corrected
2026-10-09; first written as "Claude Code Max").

**Basis.** Three read-only surveys run today: the open and recently closed place issues, a code
audit of the Atlas on staging at 481e07cdc, and the indexing repo.

## What we start from

- **The Atlas is functionally broad, and prod already has it.** Atlas source is identical on
  `main` and `staging`, so Crystal can start on prod now. Every fix below then arrives as a drop.
- **Nothing tests the Atlas automatically.** No Django tests, no JS tests and no Playwright. The only
  check is `smoke_test.sh`, which looks at the HTTP status of `/atlas/`.
- **There is no beta plumbing for the Atlas.**
  - The snag form has no Atlas feature area (`_beta_snag_form.html:60-73`).
  - "Report a snag" lives only in the Workbench BETA menu (`base_webpack.html:266`).
  - Project #12 is all Workbench.
  - `beta-testing.md` has no Atlas checklist.
- **Most of what she would hit is dishonest failure, not a crash:**
  - A gateway timeout in the portal reads as "not found" (`search/views.py:567`, the place#272
    pattern).
  - Search shows one generic message for every failure.
  - Name search for `po`, `clio` and `nl` silently does nothing. The router keys are `periodo`,
    `cliopatria` and `nativeland`, but the registry ids are `po`, `clio` and `nl`.
  - Init, basemap, coverage and AAT failures appear only in the console.
- **The data defects she will see first** are fixable without a reindex:
  - 2.72M `wd` places titled with a bare QID (#290).
  - Every `whg:*` registry row claims global H3 coverage (indexing#2;
    `push_gazetteer_inventory.py:548` omits `per_dataset_h3`).
  - Snapshot years shown as coverage (#288).
  - The `gb` exclusion is never disclosed (#294).
  - Intermittent bare AAT ids.
- **Not in this push, because each needs a retile or reindex:** #166 tile channels, #246 osm dates,
  #305 type URIs and #286 whg fclasses. #216, placeholder titles, waits for a ruling on the
  pattern list.

## Lanes (parallel subagents)

Each lane gets the survey's file:line findings in its brief, so it does not re-explore.

The whg3 lanes work in **git worktrees on their own branches** (`isolation: worktree`), because A
and B both touch `atlas.js`. I land them onto `staging` by explicit SHA.

**Agents do not build bundles.** I run one `npm run build:prod` per drop, in the main checkout
where `.env/.env` and `node_modules` live.

### Lane A: whg3 failure honesty (Opus, about 3 h)

1. **Portal errors.** `atlas_place` and `atlas_boundaries` return 503/504 on gateway
   timeout or connection failure, and 404 only for a real miss, reusing `raise_if_gateway_failed`.
   The client says "temporarily unavailable" and raises the gateway banner.
2. **Search errors.** A 403 says beta access is needed. A 5xx or network failure says
   "unavailable" and raises the banner. Add an AbortController timeout on the client.
3. **One `atlasNotice()` toast helper** for the console-only failures:
   - init (`atlas.js:1479`)
   - basemap swap (`heroMap.js:321`, `heroMap.js:1843`)
   - gazetteer layer load (`heroMap.js:987`)
   - coverage load (`atlas.js:423`)
   - AAT vocabulary (`aatVocab.js:102`)
   - `/atlas/status/` failure
4. **Area name search for `po`, `clio` and `nl`.** Fix the id mismatch, then either wire a real
   search or show an inline "name search not available for this source" hint, following Q4.
5. **Tidy-ups.** The `filterState.js:193` FIXME (dead `osm_admin_polygons` branch), the stale
   comment at `areaSearchRouter.js:9`, and `#boundary_level_select` defaulting to `local` on a cold
   load.
6. **Tests.** New `search/tests_atlas.py`:
   - the beta gates on each atlas endpoint
   - timeout versus not-found, with `crc_client` mocked
   - each check must be able to fail (see `a-check-that-cannot-fail.md`)
   - run with `--settings=whg.settings_localtest`

### Lane B: whg3 tester surface (Sonnet, about 2.5 h)

1. **Snag plumbing.**
   - Add an "Atlas" option and an "Atlas: Gazetteers panel" option to the snag form.
   - Put a "Report a snag" entry in the BETA area wherever `/atlas/` is reachable.
   - Prefill `?page=` with the Atlas state (mode, panel, query) so a report carries its context.
2. **Licence/attribution footer on cluster cards** (`atlas.js:1291-1342`), reusing the portal's
   badge (`atlas.js:2503`). This closes `project_followup_atlas_popup_licensing`.
3. **Keyboard access:**
   - `role="button"`, `tabindex` and Enter/Space on `.cluster-head`, `.cluster-member` and `.result`
   - `aria-label` on the icon buttons (`atlas.html:103-110`, `194`, `228`, `382`) and on
     `.gaz-meta-toggle`
4. **Placeholders.** Itinerary/Network pills (`atlas.html:500-503`), Attest (`atlas.js:996`) and
   the Viewport button (`atlas.html:117`) get the Q4 treatment. Rename the "pending Phase 2" note
   at `atlas.html:427`.
5. **Explore deep links.** Make `?gazetteer=<ns>` work for `osm`, `ohm`, `osm_misc`, `po`, `clio`
   and `nl`.
6. **Mobile pass at 375 px and 768 px.**
   - breakpoints for the time slider, the cluster and weight sliders, and the basemap menu
   - the welcome offset
   - a mobile-safe tour, or skip the tour below 768 px

### Lane C: the `indexing` repo (Opus, about 4 h, one dedicated agent)

Work in `/home/stephen/PycharmProjects/indexing`.

Install `fastapi` and `pydantic` into `indexing/.venv` so the search, reconcile and hit-model tests
actually run; they currently SKIP silently. Run tests module-qualified, never with
`discover -s tests` on the host.

1. **#290: QID titles.** A read-time fallback in the gateway's hit assembly for `search.py`,
   `places.py` and `reconcile.py`: when the title matches `^Q\d+$`, use the preferred toponym
   (`en`, else the first). Declare the field on the response models.
   - The ingest fix stays open on #290.
   - Memory: derived fields belong at ingest, but fixing the reader is allowed.
2. **AAT label robustness.** Load the roughly 5.8k-doc `types` index into a worker cache at
   startup. Retry once. Log the difference between "lookup failed" and "no label".
3. **indexing#2: per-dataset H3.** Fix the full-run path at `push_gazetteer_inventory.py:548`,
   then do a `--namespace whg` push to dev and then prod.
   - Verify with `jsonb_array_length`, not the ORM, which runs out of memory.
   - The coverage filter then works for contributed datasets, and the registry stops being about
     200 MB.
4. **#288: temporal_extent.** Add a coverage-range override per authority, or omit the extent,
   for snapshot sources (`un`, `nl`, `kain_par`). Re-push.
5. **#294.** Add `namespaces_excluded` to the search meta and the response models. Lane A shows it
   as a one-line note under results.
6. **Check registry names live** (`/api/sources/`) and add `dataset_name` for any acronym
   stragglers.
7. **Deploy.** Push to origin/main, then **one batched** `gaz_run.sh gateway-restart` per drop.
   Verify `openapi.json` and a live `POST /api/search`.
   - A restart is a prod change, because dev and prod share the gateway.
   - Correct `reference_gateway_restart.md`: the path is now `/vast/ishi/elastic`.

### Lane D: tester documentation and board (Sonnet, about 1.5 h, starts first)

1. **Checklist N, "Atlas & Gazetteers panel"**, in the docs repo
   (`content/v3-3/beta-testing.md`), built from the draft "Atlas: Exploring Places" guide page.
   - One block per panel, with steps, expected results, **known issues (do not report)** and how
     to report.
   - Known issues include: low-zoom gaps for polygon gazetteers (#166), OHM square holes, sparse
     dates, `osm`/`tgn` dated "2025", repeated region labels, and 451s for `kain_par`/`nl`/`chgis`
     popups.
   - The rule: no `kain_par` or `vob_*` in screenshots.
2. **Project #12.** Add items N1 to N8, one per Atlas area.
3. **Draft a short note to Crystal** (for Stephen to send): where to start, the checklist link,
   the snag route, and what is still moving.
4. Then join Lane A's test work: an optional headless Playwright smoke test that waits on the
   page's own readiness flag (`playwright-maplibre-headless-testing.md`). This runs only if
   budget remains after drop 1.

### Me (orchestrator)

- Verify Crystal's `role = beta_tester` on prod.
- Land lanes by SHA, build once per drop, then deploy and verify on dev.
- Promote to `main` by explicit-SHA diff, then rebuild the bundles on main.
  - Check pending migrations first; none are expected.
- Deploy prod with `--collectstatic`, then do a live check in a real, frontmost browser.
- Triage Crystal's first snags with `snag-diagnostics.sh`.
- Correct the stale memory notes the audit found.

## Timeline

| Hour | What happens |
|---|---|
| 0–0.5 | Interview answers in. Check Crystal's access. Brief all four lanes. |
| 0.5–2 | **D delivers checklist N plus the board.** Crystal can start on prod, since the Atlas there matches staging. A, B and C build. |
| 2–5 | A, B and C continue. C pushes its first registry fix to dev. |
| **5–6** | **Drop 1:** land A, B and C. One build, verify on dev, promote, deploy prod, one gateway restart, re-push the prod registry. Update checklist N's known issues. |
| 6–9 | Second wave: Crystal's first snags (reserved time), Lane B items 5 and 6, the Playwright smoke test, leftovers. |
| **9–10** | **Drop 2**, then a final live check and the memory and issue housekeeping. Close or comment on #290 (reader fixed, ingest open), indexing#2, #288 and #294. |

## Budget discipline

- **Model routing.**
  - Fable 5.1 is for the hardest reasoning and for long agentic work. It costs about 2.5 times
    Opus per token.
  - So Fable gets work where depth pays: catching a bug before prod, or a cross-repo build that
    needs judgement throughout.
  - Mechanical edits stay cheap.
  - Lanes A–D were launched before this correction (A and C on Opus, B and D on Sonnet) and are
    left to finish.

  | Work | Model | Why |
  |---|---|---|
  | **Adversarial review of each lane's commits before they land** (correctness, prod safety, whether each test can actually fail) | Fable | One review per drop. The highest-leverage use, because a bug caught here never reaches Crystal or prod. |
  | **Second-wave build: `/api/geometry/<place_id>`** in the gateway (sqlite geom store), plus the Atlas consuming it for area selection instead of stitched tile fragments | Fable | The most valuable deferred item: cross-repo, a 3–4 h agentic run, design judgement throughout. Runs only if drop 1 is clean. |
  | **Crystal's snags:** diagnosing each from the report, GlitchTip and the code | Fable | Root-causing behavioural reports with nothing thrown is reasoning-heavy. |
  | **Headless Atlas smoke test** (Playwright, waiting on the page's own readiness flag) | Fable | Known-hard: it needs software GL and a harness proven able to fail. |
  | Mechanical follow-ups (CSS, aria, copy, docs and known-issue updates) | Sonnet | Cheap, and nothing to reason about. |
  | Landing, building, deploying, verifying | this session | Already holds the context. |

  Fable briefs state the goal, the constraints and the done-criteria rather than step lists. The
  Fable guidance is that over-prescriptive prompts lower its output quality.
- **Briefs.** Each brief carries file:line targets, so no lane re-surveys.
- **Builds.** One build per drop, never one per lane.
- **Restarts.** One gateway restart per drop.
- **Cutoff.** If spend reaches about 75% of either allowance before drop 2, I cut the second
  wave back to snag triage only.

## Explicitly not in this push

- **Need a retile or reindex:** #166, #246, #305, #286, and the ccodes residual tail.
- **Waits for a ruling:** #216.
- **Needs v4 or design work:** #306/#123 (cluster acceptance), #137 (i18n), #17 (IIIF basemaps,
  which now overlaps PLATO Chora).
- **MyD issues** (#152, #115, #300, #240, #146) are candidates for re-scoping to PLATO Tools.
  That is housekeeping and can come later.

## Decisions (Stephen, 2026-10-09 interview)

- **Q1 Test site:** prod, beta-gated. Crystal starts now; fixes arrive in drops.
- **Q2 Prod operations:** authorised in-session. One batched gateway restart per drop, plus a
  prod registry re-push (dev first, then verify).
- **Q3 Unbuilt controls:** label them "planned" and keep them visible but disabled. Name search for
  po/clio/nl shows an inline "not yet available for this source" hint.
- **Q4 Non-beta users at /atlas/:** keep the page open, and replace the 403 failures with a clear
  "beta feature, request access" message.
