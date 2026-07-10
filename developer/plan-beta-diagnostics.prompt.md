# Plan — Beta-testing diagnostics ("what caused the reported problem?")

**Status:** REVIEWED (Stephen, 2026-07-10) — decisions locked. Companion to the snag list (place#115).
The goal is **diagnosis**, not marketing analytics: when a beta tester logs a snag, we want to
reconstruct what they did and what the system did. Plausible (aggregate, privacy-first) can't do that;
it's kept for coarse funnel signal only.

## Decisions (Stephen, 2026-07-10)
- **Diagnostic-first + Plausible aggregate.** Primary channel = GlitchTip (Sentry) enriched for beta +
  a per-session correlation id + the Workbench's own `ProjectSnapshot`/`RecordSuggestion` audit trail
  for replay. Plausible = a few coarse funnel events only.
- **Cohort notice, on by default** for `can_access_beta` testers (a one-time dismissible notice; no
  per-session friction).
- **Full Phase 1 in one go:** correlation id + snag prefill + Sentry beta enrichment + action
  breadcrumbs.

## Design
**Correlation id.** A short per-browser-session id (`sessionStorage`, e.g. `wb-1a2b3c4d`) minted for
beta users. It is: (a) a GlitchTip tag `beta_session` on every client event; (b) sent as an
`X-WHG-Beta-Session` header on every Workbench/API call; (c) read server-side and set as the same
GlitchTip tag, so client and server errors for one tester join up; (d) embedded in the snag report the
tester files, so a snag → the exact session's events + logs + snapshots.

**Client (`beta-diag.js`, init'd from `base.js` after `window.Sentry` is created; no-op for non-beta):**
- enrich GlitchTip: `setTag('beta')`, `setTag('beta_session')`, `setTag('user_role')` (staff|beta);
  user id already set by base.js.
- a **global `fetch` wrapper** (string-URL, same-origin `/reconciliation/`, `/workbench/`, `/api/place`
  only): stamps the session header, drops a breadcrumb `{method, path, status}`, fires a coarse
  Plausible event inferred from the path (`wb_checkout` / `wb_publish` / `wb_suggest_submit` /
  `wb_suggest_review`) on success and `wb_error` on 5xx. This covers **every** call site — current and
  future — without editing each editor.
- `window.WHGDiag = { session, role, breadcrumb(), event() }` for any page script that wants to add
  semantic breadcrumbs later (no-op object for non-beta).
- **snag prefill:** intercept the dropdown's *Report a snag* link → copy a ready-made snag block
  (title stub, page URL, role, session id, timestamp, browser) to the clipboard, toast, then open
  place#115 so the tester pastes it into a new checkbox.
- **consent notice:** one-time dismissible banner (localStorage `whg-beta-diag-ack`) linking to the
  privacy policy; informational (the cohort is opted-in by being testers).

**Server (`whg/middleware.SentryBetaContextMiddleware`, after AuthenticationMiddleware):** for
`can_access_beta` requests, `sentry_sdk.set_user` + `set_tag('beta')` + `set_tag('user_role')` +
`set_tag('beta_session', <header>)`. Best-effort, never raises.

**Replay asset (no new capture needed):** most Workbench snags are reproducible server-side from a
project UUID via `ProjectSnapshot` (immutable version history) + permanent `RecordSuggestion`. So we do
NOT push input payloads (geometry/snapshots) into any tracker — they're already stored safely.

## Guardrails
All detailed capture gated on `can_access_beta`. Per-tester detail lives in the diagnostic channel
(GlitchTip, consented cohort), never in the aggregate channel (Plausible). No raw PII / free text in
analytics. Honours the pending DPO/privacy-policy work.

## Files
**New:** `whg/webpack/js/beta-diag.js`, `whg/middleware.py`, this plan.
**Edited:** `whg/webpack/js/base.js` (import+call), `main/templates/main/base_webpack.html` (meta tags
`beta-user`/`user-role` + `id` on the snag link), `whg/settings.py` (MIDDLEWARE).

## Later (Phase 2, not now)
Semantic UI-action breadcrumbs (geometry draw, pane opens); a server-side beta action-log model if
breadcrumbs prove insufficient; server-side consent acknowledgement (auditable) instead of localStorage;
a richer beta funnel in Plausible with Palak.
