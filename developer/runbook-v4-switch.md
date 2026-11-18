# Runbook: switching whgazetteer.org from v3 to v4 (DRAFT)

**Status: DRAFT, 2026-11-18. Not reviewed by Stephen (Technical Director). Nothing here has been run.**
Compiled from the code at `b6eab4c9d` (this branch's base), the `developer/` plans, the session memory
notes, and the documentation repo (`origin/main` `32efe31`). Every row names its evidence. **UNVERIFIED**
marks anything not checked against live state or code. Numbers are quoted only where a note recorded
them, with that note's date. Re-measure them before acting.

The legacy JSON API cutover has its own runbook, which this one does **not** repeat:
docs repo `content/technical/apis.md`, sections **"At the v4 switch"** and **"Cutover runbook (WHG staff)"**
(published at https://docs.whgazetteer.org/content/technical/apis.html). Step C4 below links to it.

Evidence key: `file:line` = this repo. `mem:<name>` = `~/.claude/projects/-home-stephen-Documents-GitHub-whg3/memory/<name>.md`.
`docs:<path>` = `/home/stephen/PycharmProjects/documentation`, `origin/main`.

---

## 1. Scope: what "the switch" means

### 1.1 What the evidence defines

| # | Statement | Evidence |
|---|---|---|
| S1 | v4.0 is the coordinated release of the beta modules: Workbench (Map your Data, Collections, Suggest a correction), Citations, Atlas, Phonetics. There are no 3.3–3.6 releases. OER may be v4.1. | `developer/audit-v4-promises.md:116-129`; mem:project_v4_renumbering |
| S2 | Launch target: "early 2027", stated as "a direction, not a fixed date". | `main/views.py:1212-1216` (release_note); `audit-v4-promises.md:128`; docs:`content/v4.md:3` (TODO) |
| S3 | **Nothing public changes until the release.** Doc changes are prepared ahead and merged at the release. | `audit-v4-promises.md:127-129`; mem:project_v4_renumbering |
| S4 | One code switch exists for the v4 public surface: `V4_LANDING_ENABLED`. It turns on portal redirects, the public Record page, anonymous `/entity/place:<pid>/` for public records, public legacy-locus dereference, Record-page Links, and linking for all signed-in users. | `whg/settings.py:61-64`; `places/v4_landing.py:1-31,52-55,154-169`; `api/views_entity.py:484-503`; `places/locus_registry.py:91-94` |
| S5 | One setting governs the legacy v3 JSON endpoints: `LEGACY_API_MODE` (`deprecated` / `compat` / `gone`). Its runbook is in the docs. | `whg/settings.py:66-73`; `api/legacy_compat.py:1-55`; docs:`content/technical/apis.md` "At the v4 switch" |
| S6 | The version shown on the site is `APP_VERSION`, read from `VERSION`. A published GitHub release rewrites `VERSION` on `main` (workflow) and is also what archives a version to Zenodo. Tagging `v4.0.0` is a public, citable act. | `whg/settings.py:55-60`; `.github/workflows/update-version-on-release.yml`; `audit-v4-promises.md:22-27` |
| S7 | `VERSION` currently reads `3.2`. (The project `CLAUDE.md` says "currently `3.5-beta`": that is stale.) | `VERSION` (read 2026-11-18) |

### 1.2 What the switch does NOT cover (found in code: needs a decision, see §9)

`V4_LANDING_ENABLED` does **not** open the headline 4.0 modules. They are gated on
`User.can_access_beta` (staff, superuser or `beta_tester` role, `users/models.py:199-204`) and **no
release flag** reaches those gates. Opening any of them needs a code change.

| Surface | Gate | Evidence |
|---|---|---|
| Atlas data endpoints (search, place, geometry, boundaries, locus resolve) | `can_access_beta` → 403 JSON / 404 | `search/views.py:556,729,852,912,1012,1106`; "Atlas itself is still beta-gated" `api/crc_client.py:53-54` |
| Atlas / Gazetteers / Workbench nav menus, snag + suggestion forms | `{% if user.can_access_beta %}` | `main/templates/main/base_webpack.html:158,227,251` |
| Built-in Map your Data (`/reconciliation/`), Workbench pages, suggestions review | `can_access_beta` | `main/views.py:591-945` (reconciliation_view, workbench_home, wb_* views, suggestions_review) |
| Workbench API, including PLATO Tools' Bearer-token path | `can_access_beta` → 403 `beta_required` | `workbench/access.py:148-152,171` |
| Collections write paths | `can_access_beta and coll.can_edit(u)` | `collection/views.py:1189,1596` |
| v4 dataset lifecycle (submit / review queue) | `can_access_beta` (+ editor for review) | `datasets/views_lifecycle.py:39,193` |
| Recipe loci (`/entity/locus:XXXX-XXXX-XXXX`), minting | beta-only even with the switch ON | `api/views_locus.py:7-16,342-360`; `search/views_locus.py:45` |
| Phonetics review UI | `PHONETICS_PUBLIC` **and** signed-off terms | `whg/settings.py:127-131`; `phonetics/visibility.py:19-30` |
| `/development/` (beta status) page | staff only (`is_staff`) | `main/views.py:1189-1192`; mem:project_py312_django52_upgrade (2026-11-18) |
| GRACE | `can_access_beta` | `grace/views.py:34-40` |

### 1.3 Open questions the evidence does not settle

Listed in full in §9. In short: the date; which beta modules open at 4.0 and how; whether v3 pages
(portal, `/search/`, dataset pages) are redirected or retired; the `VERSION` value and its "4.0 beta"
interim label; `compat` vs `gone` for the legacy API; whether `/entity/place:<pid>/api` opens to anonymous
callers; when redirects become 301.

---

## 2. Inventory: flags and settings that gate v4 or beta behaviour

| Setting / gate | Default | Prod today | Where set | Effect at the switch | Evidence |
|---|---|---|---|---|---|
| `V4_LANDING_ENABLED` | off | **OFF** (unset). Dev: ON | env (`'1'`) or `local_settings` | THE release switch (§1.1 S4) | `whg/settings.py:61-64`; mem:project_landing_pages_325, mem:project_v4_state_2026_11_16 |
| `PORTAL_REDIRECT_STATUS` (code constant, not a setting) | `302` | 302 | `places/v4_landing.py:49` | Becomes `301` "AT the release", by a commit. A cached 301 outlives a rollback and loops (see R2) | `places/v4_landing.py:41-49` |
| `LEGACY_API_MODE` | `deprecated` | deprecated (UNVERIFIED live) | env or `local_settings` | `compat` or `gone` | `whg/settings.py:66-70`; docs apis.md runbook |
| `LEGACY_API_INAPP_PASSTHROUGH` | `True` | True | env (`0` = off) or `local_settings` | Off after a clean `origin=in-app` log | `whg/settings.py:71-73`; `api/legacy_compat.py:30-39` |
| `LEGACY_API_MODE_OVERRIDES`, `LEGACY_API_COMPAT_ANONYMOUS`, `LEGACY_API_COMPAT_ANON_RATE` | `{}`, False, 30 | defaults (UNVERIFIED) | `local_settings` only | per-endpoint retirement | `api/legacy_compat.py:23-28`; docs apis.md |
| `User.can_access_beta` | — | — | `users/models.py:199-204` | Not touched by any flag (§1.2) | |
| `linking_open_to(user)` | beta only | beta only | derived | All signed-in users when switch ON | `places/v4_landing.py:162-169` |
| `record_page_open_to(user)` | beta only | beta only | derived | Everyone when switch ON | `places/v4_landing.py:154-159` |
| `PHONETICS_PUBLIC` | False | False (UNVERIFIED) | env / `local_settings` | Independent of the switch; also needs signed-off terms | `whg/settings.py:127-131` |
| `EDOP_ENABLED` | True | UNVERIFIED | `local_settings` | EDOPS on Record/Locus pages; off-switch only | `api/edop_client.py:153-154` |
| `CRC_GATEWAY_URL`, `CRC_REINGEST_URL` (+ `REINGEST_DATASET_TOKEN`) | empty | set on prod | prod `env_template.py` | Required for Record concordance, Atlas, publish-time indexing | `whg/settings.py:75-83`; mem:reference_prod_env_lives_in_env_template |
| `RECON_WORKER_CONTEXT` | — | set (dedicated `celery_recon`) | prod `env_template.py` | Feed candidate generation runs on its own queue | mem:project_v4_state_2026_11_16 |
| `WORKBENCH_CORS_ORIGINS` | pelagios.org, plato.pelagios.org | default (UNVERIFIED) | env | PLATO Tools origin | `whg/settings.py:704` |
| `CONTRIBUTOR_ATTESTATION_DAILY_CAP`, `EDITORIAL_REVIEW_SLA_DAYS` | 200/day, 10 working days | not overridden (2026-10-31) | settings | Matter once linking opens to everyone | mem:project_v4_reconciliation_plan_334 |
| Locus JSON dereference rate | `(30, 60)` | code | `api/views_locus.py:86` | "re-check throttles under real traffic" at release | mem:project_landing_pages_325 (2026-10-31) |
| `EntityResolveAnonThrottle` | `60/min` (+ a `600/min` class) | code | `api/views_entity.py:461-481` | Anonymous Record traffic goes through these | |
| Template meta `beta-user`, `user-role` | — | — | `base_webpack.html:48-49` | Drives `beta-diag.js` diagnostics | |
| Removed: `LOCUS_MINTING_ENABLED` | — | gone (2026-10-22) | — | Do not set; it does nothing | `places/v4_landing.py:1-2`; mem:project_landing_pages_325 |

---

## 3. Inventory: code that still says v3 / legacy / "until v4"

| Area | What | At / after the switch | Evidence |
|---|---|---|---|
| Portal | `/places/portal/<pid>/`, `/places/<pid>/` → Record URI; `/places/<whg_id>/portal/` → `/entity/locus:<whg_id>/` if in `LegacyUnionRecord`; multi-pid portal keeps serving with a "legacy" banner | Automatic with the switch | `places/v4_landing.py:66-94`; `places/urls.py:19-29` |
| Record page | `/atlas/place/<pk>/` is the "preview"; canonical becomes `/entity/place:<pk>/` | Automatic | `places/templates/places/atlas_record.html:15,223`; `places/v4_landing.py:261-266` |
| Legacy JSON API | `/api/db/`, `/api/index/`, `/api/spatial/`, `/api/place/…`, `/api/geojson/` carry `Deprecation: @1794873600` (2026-11-17), no `Sunset` | Per docs runbook (C4) | `api/urls.py:17-26`; `api/deprecation.py:2,35,104` |
| Identifiers inside legacy payloads | `uri`/`geometry_url` = `https://whgazetteer.org/api/place/<id>`; default dataset `uri_base` = `…/api/db/?id=` | Unchanged by the switch; decide before `gone` | docs apis.md runbook step 4 |
| Legacy ES union index `whg` | Being retired; `whg_id`s are in public circulation (Wikidata P13061) | Snapshot must be complete first | `places/management/commands/snapshot_legacy_clusters.py:1-17`; mem:project_wikidata_roundtrip_170 |
| Workbench Route / Network tiles | badge "Coming with v4"; tooltip "arriving with WHG's v4 data model" | Reword to "later release" (old v4 promise) | `main/templates/main/workbench_new.html:65,74`; `main/views.py:621`; `audit-v4-promises.md:52-63` |
| `/development/` page | "WHG v4.0 … expected in early 2027"; MyD "moving to PLATO Tools" | Rewrite at release; decide whether to ungate | `main/views.py:980-1000,1189-1216` |
| Navbar | "beta" pills; "Beta testing plan" → `docs …/v3-3/beta-testing.html`; version badge and "New in v{APP_VERSION}" link to the GitHub release tag | Remove pills for opened modules; the release link needs a `v<VERSION>` tag to exist | `base_webpack.html:172-205,227-290,382-384` |
| Locus page | tooltip "Minting … arrives with the v4 release" | Reword if minting opens | `search/templates/search/locus.html:202` |
| Code comments "v4 placeholder" | Route/Network | Leave (history) | `audit-v4-promises.md:65-74` |
| `atlasRecord.js` "attest" wording | Noted 2026-10-30 as needing a bundle rebuild at release | UNVERIFIED: today's grep finds "Attested date range" labels only, not a user-facing "attest" verb | mem:project_v4_reconciliation_plan_334 |
| Sitemap | `ToponymSitemap` points at v3 `/search/<toponym>`; static list includes `workbench` | Decide: `/atlas/<toponym>`? Add Record pages? | `sitemap/views.py:13-49` |
| Map your Data built-in | Beta-only; Plausible `MyD: *` events come only from it | Retire, keep beta, or open (decision) | `main/views.py:1229-1260`; `whg/webpack/js/reconciliation.js` |
| Authority downloads in Atlas | Disabled until PLATO export (place#312) | Not a switch item unless decided | mem:project_authority_downloads_wait_for_plato; `search/views.py:198` |
| `developer/deployment-protocol.md` | Says "NEVER merge staging into main", promote by cherry-pick | **Superseded 2026-11-17**: promotion is `git push origin staging:main` (fast-forward) | mem:project_v4_state_2026_11_16 ("staging/main joined"); mem:reference_whg_promotion_is_cherry_pick |

---

## 4. Preconditions checklist

Owner column: placeholders until Stephen assigns. "SG" = Stephen. **Every box must be ticked or
explicitly waived by SG before C1.**

### 4.1 Decisions (all from §9)

- [ ] D1 Release date set, and announced internally. Owner: SG
- [ ] D2 Which beta modules open at 4.0, and how (flag vs gate removal); code for it merged and on dev. Owner: SG / `<dev>`
- [ ] D3 Fate of v3 pages decided (portal multi-pid, `/search/`, v3 dataset/collection pages, `/reconciliation/`). Owner: SG
- [ ] D4 Legacy API target mode at the switch (`compat` or `gone`) and identifier-URL redirect policy. Owner: SG
- [ ] D5 `/entity/place:<pid>/api` anonymous: stays closed (current code) or opens (licensing, #271, place#349). Owner: SG
- [ ] D6 When `PORTAL_REDIRECT_STATUS` becomes 301 (at C2, or after a soak). Owner: SG
- [ ] D7 Release tag name (`v4.0.0`?) and whether the site shows "4.0 beta" before it. Owner: SG

### 4.2 Code and data state

| ✓ | Check | How | Evidence | Owner |
|---|---|---|---|---|
| [ ] | `main == staging`, prod checkout = `origin/main` | `git rev-parse origin/main origin/staging`; `deploy.sh prod status` | mem:project_v4_state_2026_11_16 | `<dev>` |
| [ ] | v4 landing test suites pass locally on the release SHA (never on prod) | `python3 manage.py test places.tests_v4_landing search.tests_locus_mint search.tests_locus_redaction api.tests_attestations accounts.tests_link_name places.tests_concordance --settings=whg.settings_localtest` (browser tests need `WHG_BROWSER_TESTS=1`) | test files listed in §2 grep; mem:reference_local_django_tests; mem:reference_never_run_suite_on_prod | `<dev>` |
| [ ] | Full suite vs `server-admin/test-baseline.txt`: no new failures | `server-admin/smoke_test.sh dev --deep --ssh whg --suite` (dev only; refused on prod) | `server-admin/smoke_test.sh:23-29,79-93` | `<dev>` |
| [ ] | The 9 pre-existing browser failures are fixed or waived (several are Record/Atlas pages that go public) | see note | mem:project_py312_django52_upgrade (2026-11-18: `search.tests_atlas_popup_contributed_browser` ×5, `places.tests_record_layout_browser` ×2, …) | `<dev>` |
| [ ] | `manage.py check` clean (includes `api.E101`–`E103` for legacy settings) | `docker exec web_whgazetteer-org_main python manage.py check` | docs apis.md runbook | `<dev>` |
| [ ] | Legacy union snapshot complete before anything retires the `whg` index | `LegacyUnionRecord.objects.count()` vs the index doc count; re-run `snapshot_legacy_clusters` (idempotent upsert) if short | `snapshot_legacy_clusters.py:1-17`; a 2,101,316-row snapshot was recorded in mem:project_wikidata_roundtrip_170 (date not re-checked) | `<dev>` |
| [ ] | Adopted-location rule holds | `manage.py location_lifecycle --dry-run` → "0 adopted location(s) to retract" | `datasets/management/commands/location_lifecycle.py:1-13` (prod dry run 0/0 on 2026-11-18 per mem:project_py312_django52_upgrade) | `<dev>` |
| [ ] | Datasets stuck in `accessioning` counted and a decision taken (Q8 hazard; delete view misses them) | read-only SQL `select count(*) from datasets where ds_status='accessioning'` inside `BEGIN READ ONLY` (table `datasets`, `datasets/models.py:642`) | 24 on prod 2026-10-29 per mem:project_v4_reconciliation_plan_334; MEMORY.md index | `<dev>` |
| [ ] | Publish-time indexing healthy: no `index_pending_since` older than a sweep, no `index_failure_reason` | read-only SQL; `sweep_index_pending` runs every 15 min | mem:project_v4_reconciliation_plan_334 | `<dev>` |
| [ ] | Legacy `legacy_v3_2` Links survive the next batch-overlay rebuild (`contributor_replay` part B) | indexing session | mem:project_v4_state_2026_11_16 (WATCH) | indexing owner |
| [ ] | Gateway on Pitt is up and on the expected branch; GlobalProtect up for any Pitt action | `ssh pitt '/vast/ishi/elastic/scripts/gaz_run.sh …'`; `ip route \| grep -c gpd0` | mem:project_py312_django52_upgrade (2026-11-18 indexing deploy) | indexing owner |
| [ ] | Beta has been exercised end to end: at least one real dataset through the v4 flow (lifecycle → review → publish → index) | prod read-only SQL on lifecycle events | **As of 2026-11-16: 0 lifecycle events, all 440 datasets NULL `lifecycle_state`** (mem:project_v4_state_2026_11_16) | SG |
| [ ] | Bulk confirm stays OFF (calibration had too few negatives) | — | mem:project_v4_reconciliation_plan_334 (#339) | SG |
| [ ] | Throttles reviewed for anonymous Record/locus traffic (`EntityResolveAnonThrottle` 60/min, locus JSON 30/60 s, no project-wide DRF throttle) | read the classes; decide | `api/views_entity.py:461-481`; `api/views_locus.py:86`; mem:reference_w3id_whg_resolver | `<dev>` |
| [ ] | Backup taken the same day; `pg_stat_activity` quiet before any migration | per prod-upgrade precedent | mem:project_py312_django52_upgrade (backup + 0 active queries before 2026-11-18 deploy); MEMORY.md "Check pg_stat_activity" | `<ops>` |

### 4.3 Docs, comms, external

| ✓ | Item | Evidence | Owner |
|---|---|---|---|
| [ ] | Docs release branch ready. **The local branch `v4-release` is 63 commits behind docs `origin/main`, last commit 2026-09-30, not on the remote.** Either rebase it or redo the release wording on a fresh branch | `git -C ~/PycharmProjects/documentation rev-list --count v4-release..origin/main` = 63 (2026-11-18); `ls-remote` shows no remote branch; mem:project_v4_renumbering | docs owner |
| [ ] | 21 `% TODO(release)` markers on docs `origin/main` resolved (11 files: `v4.md`, `v4/data-model/{contributions,rdf-representation}.md`, `v4/guide/{corrections,formats,getting-started,identifiers,loci,record-pages,routes-and-networks,sounds-alike}.md`) | `git grep -n 'TODO(release)' origin/main` | docs owner |
| [ ] | Never edit pages carrying the "Archived: the 2025 v4 design" banner | mem:reference_documentation_conventions | docs owner |
| [ ] | Draft v4 User Guide promoted from "draft"; Beta Testing Plan link (`v3-3/beta-testing.html`) retired or kept | `audit-v4-promises.md:223-233`; `base_webpack.html:284` | docs owner |
| [ ] | Release notes say what 4.0 is **and is not** (Routes/Networks later; Zenodo-archived statement) | `audit-v4-promises.md:105` | SG |
| [ ] | GitHub: `website` repo description ("Version 3 beta"), milestones, place#111/#137/#142/#170/#172/#286 comments | `audit-v4-promises.md:91-105,178-189` | SG |
| [ ] | Blog / announcements post drafted (UNVERIFIED: no draft found in this repo; `announcements-list` is in the sitemap) | `sitemap/views.py:20-30` | SG |
| [ ] | External v4 consumers told: PLATO Tools (needs beta + token today), MCP server (uses only `/reconcile`, `/entity/place:<id>/api`, `/api/sources/`, `/api/attribution/`), EDOPS | `workbench/access.py:148-152`; `mcp/whg_mcp/client.py:4-38`; mem:project_landing_pages_325 | SG |
| [ ] | Plausible: decide goals/events for the newly public pages (Record page, feed). Today the only custom events are `MyD: *` (built-in MyD), `Atlas: share`, and server-side API calls | `main/views.py:1236-1253`; `whg/webpack/js/atlas.js:2085`; `api/middleware.py:65` | `<analytics>` |

---

## 5. Cutover steps (ordered)

Prod host: `ssh whg` (NYC1 droplet since the place#335 migration; mem:project_158_rulings_2026_10_10,
2026-11-17). Deploy tool: `~/sites/whgazetteer-org/server-admin/deploy.sh` (mem:reference_deploy_script).
Container names below are those used in `developer/deployment-protocol.md:170-183`; confirm with
`deploy.sh prod status` first (UNVERIFIED on the new host).

**Env-var trap (applies to C2, C4, C5).** Prod `.env/.env` is regenerated from `~/sites/env_template.py`
(outside the repo; prod block `'whgazetteer-org'`) on every deploy, so edits to `.env` are lost. A plain
`restart` does **not** re-read env vars. Back up `env_template.py`, edit the prod block, then
`deploy.sh prod restart --image=<current tag>`, which recreates web, celery workers, beat and flower only.
Verify with `docker exec <web> printenv <NAME>`. (mem:reference_prod_env_lives_in_env_template;
`server-admin/deploy.sh:95-103`.) Current tag was `1.1.0` on 2026-11-18 (mem:project_py312_django52_upgrade):
**read it from `env_template.py` on the day**.

| Step | Action | Exact command / setting | Rollback | Evidence |
|---|---|---|---|---|
| **C0** | Freeze: announce a window; stop promotions; confirm §4 | — | — | — |
| **C1** | Ship release code (any D2/D3 gate changes, copy rewrites from §3, `PORTAL_REDIRECT_STATUS` only if D6 = "at C2") on staging → dev → main | `npm run build:prod` on main if JS changed; `git push origin staging:main` (FF); `deploy.sh prod restart` (+`--celery` if task code, +`--migrate` if schema; for a destructive migration: `deploy.sh prod pull` → `docker exec web_whgazetteer-org_main ./manage.py migrate` → `deploy.sh prod restart`) | Reset `main` to the previous SHA (protected branch: force-push needs the protection rule edited) and `deploy.sh prod restart` | `deployment-protocol.md:29-40,84-88`; mem:reference_deploy_script; mem:project_v4_state_2026_11_16 |
| **C2** | Flip the release switch | In `~/sites/env_template.py`, prod block: `V4_LANDING_ENABLED: '1'` (the setting accepts exactly `'1'` from env). Then `deploy.sh prod restart --image=<current tag>`; `docker exec web_whgazetteer-org_main printenv V4_LANDING_ENABLED` → `1` | Remove the key (or set `''`), same redeploy. **Safe only while redirects are 302** (R2) | `whg/settings.py:64`; mem:project_landing_pages_325 (dev set the same way) |
| **C3** | Verify (§6) | — | → C2 rollback | — |
| **C4** | Legacy API: follow the docs runbook ("Cutover runbook (WHG staff)", steps 1–6): count external callers from the `api.deprecation` log, set `LEGACY_API_MODE=compat` via `env_template.py` + `--image=` redeploy, test with/without token, then `LEGACY_API_INAPP_PASSTHROUGH=0` after a clean `origin=in-app` log | as in the docs | `LEGACY_API_MODE=deprecated`, same redeploy | docs:`content/technical/apis.md` |
| **C5** | Phonetics (only if decided): `PHONETICS_PUBLIC=true` | env_template + `--image=` redeploy; the gate also requires terms `signed_off` | unset | `whg/settings.py:127-131` |
| **C6** | Publish the docs release (merge the release branch; the docs Pages workflow publishes on push) | — | revert the merge commit | mem:project_v4_renumbering; mem:project_landing_pages_325 ("docs pushed (live; Pages workflow)") |
| **C7** | Publish the GitHub release `v4.0.0` (D7). The workflow commits `VERSION` (and the LICENSE year) to `main` | GitHub UI / `gh release create` | A published release + Zenodo DOI is **not reversible** | `.github/workflows/update-version-on-release.yml` |
| **C8** | Pull the workflow's `VERSION` commit to prod, and fast-forward `staging` to `main` (else the next `git push origin staging:main` is not a fast-forward: inference) | `git fetch; git push origin origin/main:staging` (or merge locally); `deploy.sh prod restart` | — | inference from the FF promotion regime |
| **C9** | Check Zenodo actually archived it (the webhook can 500 silently; the API 403s scripts: check in a browser) | Settings → Webhooks → Recent deliveries; resolve the concept DOI | redeliver `published` (repo owner's call) | mem:reference_zenodo_release_webhook |
| **C10** | (D6) Make redirects permanent: `PORTAL_REDIRECT_STATUS = 301`, commit, promote, `deploy.sh prod restart` | `places/v4_landing.py:49` | **Effectively none once browsers cached 301s.** Only do this after the switch is final | `places/v4_landing.py:41-49` |
| **C11** | Announce: blog, site announcement, GitHub repo description, pinned discussion comment (#80 is closed) | — | — | `audit-v4-promises.md:91-105,209` |

---

## 6. Verification

Expected results come from the code; none has been observed on prod with the switch on. Dev has had the
switch ON since 2026-10-21 (mem:project_landing_pages_325), so rehearse every check on
`dev.whgazetteer.org` first. nginx returns 403 to a bare curl: send a browser-like User-Agent
(mem:reference_deploy_script). `<pub>` = a public, non-embargoed record pk; `<priv>` = a private one;
`<wid>` = a `1NNNNNNN` whg_id present in `LegacyUnionRecord`.

| # | Request (anonymous unless stated) | Expected | Evidence |
|---|---|---|---|
| V1 | `server-admin/smoke_test.sh prod` (and with `--cookie` as a beta and a non-beta user) | exit 0. **It does not cover any v4 URL below** | `server-admin/smoke_test.sh:128-130` |
| V2 | `GET /places/portal/<pub>/` | `302`, `Location: /entity/place:<pub>/` (query string kept) | `places/v4_landing.py:136-150` |
| V3 | `GET /places/<pub>/` | `302` to the same | `places/v4_landing.py:72-74` |
| V4 | `GET /entity/place:<pub>/`, `Accept: text/html` | `200`, the Record page, `Vary: Accept`, canonical = entity URI | `places/v4_landing.py:239-266` |
| V5 | same, `Accept: application/ld+json` | `200`, `@type: "Record"`, `lpf_access: "signed-in session or API token"` | `places/v4_landing.py:205-236` |
| V6 | `GET /entity/place:<priv>/` and a missing pk | `404`, identical | `api/views_entity.py:484-493` |
| V7 | `GET /entity/place:<pub>/api` | still `401` (not opened; D5) | `places/v4_landing.py:23-27` |
| V8 | `GET /places/<wid>/portal/` | `302` to `/entity/locus:<wid>/`, which renders for anonymous | `places/v4_landing.py:117-133`; `api/views_locus.py:345-348` |
| V9 | `GET /places/portal/<encoded_ids>/` | `200`, legacy page with a "legacy" banner | `places/v4_landing.py:102-114` |
| V10 | `GET /entity/locus:<recipe PID>/` | HTML: redirect to login; JSON: `401` "beta-only until the v4 release" (unchanged unless D2 covers loci) | `api/views_locus.py:349-357` |
| V11 | w3id: `curl -sI -H 'Accept: text/html' https://w3id.org/whg/id/place:<pub>` | `303` → `/entity/place:<pub>/`, which now answers anonymous `200` | mem:reference_w3id_whg_resolver |
| V12 | Signed-in **non-beta** user: Record page shows the Link sheet; `POST /api/links` no longer `403 linking_not_open` | as stated | `places/v4_landing.py:162-169`; `api/views_indexing.py:326-330` |
| V13 | Signed-in non-beta user: `/atlas/` data calls | still `403` "beta access required" unless D2 changed it. Check whether the now-public Record page links into Atlas and dead-ends (UNVERIFIED) | `search/views.py:556-557` |
| V14 | Record concordance for anonymous visitors | only openly redistributable namespaces looked up; others show "Not looked up for signed-out visitors" | `api/crc_client.py:47-54`; `whg/webpack/js/atlasRecord.js:136-143` |
| V15 | Logs: zero 5xx after the deploy; `api.deprecation` log lines show the expected `mode=` | `docker logs web_whgazetteer-org_main` | mem:project_py312_django52_upgrade (precedent) |
| V16 | MCP still answers (`https://mcp.whgazetteer.org/`) | tools work; it uses no legacy endpoint | `mcp/whg_mcp/server.py:1-2` |
| V17 | Version badge and "New in v…" link resolve after C8 | `v4.0.0` (or D7's value); the link's release tag exists | `base_webpack.html:172-205,382-384` |

---

## 7. Rollback summary

| ID | Scope | How | Limit |
|---|---|---|---|
| R1 | Code (C1) | Reset `main`, redeploy | protected branch; migrations may not reverse cleanly: read each one first |
| R2 | Release switch (C2) | unset `V4_LANDING_ENABLED`, `--image=<current>` redeploy | **Only while `PORTAL_REDIRECT_STATUS = 302`.** After C10 a rollback makes `/entity/place:<pk>/` 302 back to the portal, which cached 301s send forward again: a redirect loop (`places/v4_landing.py:41-49`) |
| R3 | Legacy API (C4) | `LEGACY_API_MODE=deprecated` | per docs |
| R4 | Phonetics (C5) | unset `PHONETICS_PUBLIC` | — |
| R5 | Docs (C6) | revert merge | external links already followed |
| R6 | Release / Zenodo (C7, C9) | none | a DOI is permanent |
| R7 | Linking opened to all (C2) | switch off returns linking to beta only. **Links already written by non-beta users stay** (UNVERIFIED how they then display) | `places/v4_landing.py:162-169` |

---

## 8. Post-switch cleanup

| Item | Action | Evidence |
|---|---|---|
| Legacy API retirement | `gone` (or per-endpoint overrides), then delete the legacy views and `api/legacy_compat.py` paths once logs are quiet | docs apis.md steps 5–6 |
| Remove the switch | Once final: delete `V4_LANDING_ENABLED`, `v4_landing_enabled()` branches, the `legacy_view` wrappers and the off-path tests (`@override_settings(V4_LANDING_ENABLED=False)` in the test modules that pin the off path) | `whg/settings.py:61-64`; §2 grep |
| Beta plumbing | Remove `nav-beta-pill`s, `beta-user` meta and snag forms for opened modules; decide whether the `beta_tester` role stays for future betas | `base_webpack.html:48-49,227-290`; `users/models.py:199-204` |
| Retire the legacy `whg` / `pub` ES paths | After the snapshot (§4.2) and once nothing reads `ES_WHG` (e.g. `api/views.py:559,644-648`) | `snapshot_legacy_clusters.py`; mem:reference_dev_shares_prod_es_indexes (dev + prod share indexes: a retirement hits both) |
| v3 pages | Per D3: portal templates, `/search/`, `/reconciliation/` (built-in MyD) | `places/v4_landing.py:66-94`; `main/views.py:591` |
| Sitemap | Point at v4 pages per D3 | `sitemap/views.py` |
| Deprecated docs and promises | Workbench "Coming with v4" copy; `/development/` rewrite; `deployment-protocol.md` cherry-pick section | §3 |
| Analytics | Retire the `MyD: *` funnel panel if built-in MyD is retired | `main/views.py:1229-1260` |
| Memory / CLAUDE.md | Update "currently `3.5-beta`" and the v4 notes | §1.1 S7 |

---

## 9. Unknowns and decisions needed from Stephen (Technical Director)

Ordered by how much of the runbook they block.

1. **Which 4.0 modules become public, and by what mechanism.** The switch opens only Record / Locus-legacy / Linking. Atlas, Gazetteers, Workbench, Collections writes, the v4 dataset lifecycle, recipe loci and minting stay behind `can_access_beta` with no flag (§1.2). Build a second release flag, fold them into `V4_LANDING_ENABLED`, or remove the gates in the release commit?
2. **Release date** (S2: "early 2027, a direction"). Everything in §4 hangs off it.
3. **Readiness bar for a flow nobody has used.** As of 2026-11-16 prod had 0 lifecycle events and no v4-published dataset; the beta testers are the pilot (mem:project_v4_state_2026_11_16). What has to have happened in beta before C2?
4. **v3 pages:** are the multi-pid portal, `/search/` (and the sitemap that points at it), v3 dataset/collection pages and built-in Map your Data redirected, labelled legacy or removed? Q8 ("retire v3 pages at the switch") was ruled "stays at Stage 5" (mem:project_v4_reconciliation_plan_334), which is not defined further in the evidence found.
5. **Legacy API:** `compat` or straight to `gone` at the switch, and whether the `…/api/place/<id>` and `…/api/db/?id=` URLs inside published payloads/`uri_base` get permanent redirects (docs runbook step 4).
6. **Anonymous data access:** does `/entity/place:<pid>/api` (LPF) open to anonymous callers at 4.0? It is a licensing decision (#271, place#349 still open; kain_par question open). Affects the w3id JSON arm (V11) and MCP's anonymous tier.
7. **Version naming:** release tag (`v4.0.0`?), whether the site shows "4.0 beta" before it (proposed, not implemented: `audit-v4-promises.md:33-34`), and correcting the stale "3.5-beta" note.
8. **301 timing** (C10): at the flip, or after a soak? Until then rollback is safe; after it, it is not.
9. **Docs release branch:** rebase the 2-month-old local `v4-release` (63 commits behind) or redo the release wording fresh; who resolves the 21 `TODO(release)` markers, several of which need the live site (e.g. confirming the minting control, record-page URL, figures).
10. **External clients after the switch:** PLATO Tools currently needs a beta-enrolled account and token (`workbench/access.py:148-152`). Does that remain? Does the MCP server expose v4 data (it was held to v3 public data pending this decision, mem:project_py312_django52_upgrade)? What Plausible goals measure the launch?

Also open, lower priority: whether opening linking to all signed-in users needs a lower `CONTRIBUTOR_ATTESTATION_DAILY_CAP` or SLA review; whether minting is opened at 4.0 (2026-10-31 ruling: "minting control shown but disabled until the v4 release", mem:project_landing_pages_325); the 9 pre-existing failing browser tests on pages that go public; Karl draft "unsent" (mem:project_landing_pages_325; mem:project_v4_state_2026_11_16 says "Karl: do not contact"); retirement date of the `whg` ES index; where the blog post lives.
