# Map your Data — usage analytics (Plausible, Tier A)

> **2026-11-18: Map your Data is moving to [PLATO Tools](https://plato.pelagios.org/tools/)**
> ([guide](https://plato.pelagios.org/guide/tools.html#map-your-data)), which keeps users' files on
> their own computer and looks places up in WHG with a beta account's API token. WHG's built-in
> version (`/reconciliation/`, beta-gated) stays available to beta testers for now; no retirement date
> is set. Everything below describes the **in-WHG version only**. See "What the staff page shows" at
> the end for what changed on `/dashboard_admin/analytics/` and why.

Anonymous, privacy-first instrumentation of the reconciliation workbench so we can see **where users
drop off or get stuck** — without ever touching the data they bring. (Palak's monitoring idea; the
approach agreed with the Technical Director.)

## Principles

- **Cookieless, no user id.** Uses the site's existing self-hosted Plausible (script already in
  `main/templates/main/base_webpack.html`; `window.plausible(...)`). No new tracker, no consent banner
  beyond what the site already has.
- **Never any dataset contents.** No cell values, toponyms, filenames, coordinates, or dates are sent.
- **Coarse props only.** Row counts are bucketed (`bucketCount()` → `1-10`, `11-50`, `51-200`,
  `201-1000`, `1001-5000`, `5000+`) so a count can't fingerprint a dataset.
- **Fail-safe.** `track()` swallows all errors — analytics can never break the tool.
- Implemented in `whg/webpack/js/reconciliation.js` (`track`, `trackOnce`, `bucketCount`). One-shot
  funnel events dedupe per page-load and reset on each fresh import.

## Events

| Event | When | Props |
|---|---|---|
| `MyD: import` | a dataset is parsed | `source` (sample/file), `format` (csv/json), `rows` (bucket), `cols` |
| `MyD: resume` | a `.whgproj` backup is restored | `rows` (bucket) |
| `MyD: reconcile` | first reconcile run of a dataset | `columns` (count in the chain) |
| `MyD: export` | a file is exported | `format` (csv/json/lptsv/lpf), `rows` (bucket) |
| `MyD: contribute` | LPF submitted to WHG validation | `rows` (bucket) |
| `MyD: contribute blocked` | user clicks **Re-check** and LPF still fails schema | `errors` (bucket) |
| `MyD: reconcile result` | each column pass completes | `column`, `matched` (bucket), `nomatch` (bucket) — *did reconciliation actually find candidates* |
| `MyD: scope applied` | Scope modal Apply with an active scope | `where`/`when`/`what`/`period` (yes/no) — which facets people use |
| `MyD: place type assigned` | first AAT type assigned to a row | — |
| `MyD: tour` | "Take a tour" clicked | — |
| `MyD: extract column` | places extracted from a free-text column | `rows`, `places` (buckets), `cols`, `scoped` |
| `MyD: team save` / `shared` / `shared open` / `conflict` | collaboration actions | see `reconciliation.js` |

## Reading it

The drop-off between `import → reconcile → export`/`contribute` is the core "where do people give up"
signal — build a **Plausible funnel** from those events. `MyD: contribute blocked` flags people who
reached the finish line but couldn't submit (usually missing place types / geometry).

## Possible follow-ups

- A `MyD: review confirmed` stage between reconcile and export for a finer funnel.
- A `MyD: reconcile no-matches` friction event (needs per-column match-count plumbing).
- Add a one-line mention to the site privacy policy (anonymous aggregate usage stats; data stays in
  the browser) — confirm wording with the DPO.

## What the staff page shows (changed 2026-11-18)

The funnel panel on `/dashboard_admin/analytics/` (`main.views.plausible_analyser_view`, constants
`MYD_FUNNEL` / `MYD_OTHER` / `MYD_PCT_MIN_BASE`) was **kept and relabelled** "Map your Data — in-WHG
version (beta, being moved to PLATO Tools)", with a note linking PLATO Tools. Reasons, from read-only
Stats API queries on prod that day:

- It still measures something real but small. Last 6 months (all MyD activity began July 2026):
  `MyD: import` 18 visitors / 96 events, `reconcile` 17, `reconcile result` 17, `export` 6,
  `contribute` 1; `/reconciliation/` 33 visitors / 321 pageviews over 28,817 site visitors. Monthly
  importing visitors 6 / 7 / 5 (Jul / Aug / Sep), none in October, one on 17 November.
  Last 30 days: one visitor at each of import, reconcile, result, export (of 7,715 site visitors).
- Only 21 active accounts (beta testers + staff) can open the page, and Plausible visitors are re-counted
  daily, so multi-day "visitors" are not people. The panel now states both.
- Step percentages are hidden below 20 importing visitors (`MYD_PCT_MIN_BASE`): the default 30-day view
  was showing "100%" conversions computed from one visitor. Each step now reads "N of M importing
  visitors".
- `MyD: extract column` fired but was not listed; it now is.

**PLATO Tools is not measured on that page, deliberately.** Checked 2026-11-18: the PLATO Tools page and
bundle load no analytics script, and `plato.pelagios.org` is not a site this Plausible instance serves to
our key (401). WHG has no links to PLATO Tools, so outbound-link clicks (the tracker's `outbound-links`
variant is on) to it were 0 in 30 days and 6 months. Inbound referrals arrive as source `pelagios.org`
(10 visitors / 30 d, none landing on `/reconciliation/`), indistinguishable from the main Pelagios site.
PLATO Tools calls `https://whgazetteer.org/reconcile` with the user's token, but the `API_Call` event
(`api/middleware.py`) records no origin and `APIToken.last_used` / `UserAPIProfile.total_count` count
every client, so neither can be attributed to PLATO Tools. The sound way to measure the hand-off is to
add the request's `Origin` header (allow-listed values only, e.g. `plato.pelagios.org`, else `other`)
as an `API_Call` prop; that only helps data collected after it ships.
