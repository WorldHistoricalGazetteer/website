# Map your Data — usage analytics (Plausible, Tier A)

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

## Reading it

The drop-off between `import → reconcile → export`/`contribute` is the core "where do people give up"
signal — build a **Plausible funnel** from those events. `MyD: contribute blocked` flags people who
reached the finish line but couldn't submit (usually missing place types / geometry).

## Possible follow-ups

- A `MyD: review confirmed` stage between reconcile and export for a finer funnel.
- A `MyD: reconcile no-matches` friction event (needs per-column match-count plumbing).
- Add a one-line mention to the site privacy policy (anonymous aggregate usage stats; data stays in
  the browser) — confirm wording with the DPO.
