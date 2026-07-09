# Plan — Community record corrections ("Suggestions", attestation-seed)

**Status:** REVIEWED (Stephen, 2026-07-09) — decisions locked (§1). Lets **any beta/staff user suggest a
correction to any published gazetteer record**, routed to the dataset owner + staff for review. A
lightweight, forward-compatible precursor to v4 **Attestations** (PLATO graph, place#100) — deliberately
scoped to *error-correction*, not parallel/competing claims.

Builds directly on the shipped full-LPF record editor + record check-out + `apply_record_fields` +
per-record optimistic lock (plan-collaborativeCollections §6.1, plan-dataset-checkout). The editor and
apply-path already exist; this is a **review workflow wrapper** around them.

---

## 1. Decisions (locked, Stephen 2026-07-09)

| # | Decision | Choice |
|---|----------|--------|
| 1a | Scope vs v4 | **Error-correction now, attestation-seed.** Fix genuine mistakes only; store each suggestion as a permanent, provenance-bearing record so the corpus seeds v4 attestations. Do NOT model parallel/competing claims in v3. |
| 1b | Who may suggest | **Beta-testers + staff initially** (matches current beta gate). Widen to all logged-in users once the review loop is proven. |
| 1c | Review routing | **Owner opt-in + staff backstop.** Suggestions always reach staff (backstop, incl. orphaned datasets); the dataset owner is looped in (queue + digest) only if they opted in (`Dataset.accept_suggestions`). No per-item email spam. |
| 1d | Placement | **All three surfaces:** public place detail (`/places/<id>/detail`), portal source-box (`/places/<id>/portal/`), and dataset-places detail pane (`/datasets/<id>/places`). Each gets a "Suggest a correction" affordance + a pending-suggestions inset. |
| 1e | Pending inset visibility | Content visible to **owner / staff / the proposer**; the general public sees only a subtle "N correction(s) proposed" marker, never unvetted content. |

## 2. Model — permanence & provenance (the attestation seed)

New `workbench.RecordSuggestion` (kept forever; status transitions, never hard-deleted — this is the
v4-attestation seed corpus):

```
place            FK places.Place (related_name='suggestions', CASCADE)
dataset          FK datasets.Dataset (denormalised for queue/opt-in/inset queries)
proposer         FK User (SET_NULL)
proposed_snapshot JSON   # full-LPF record snapshot (checkout.record_snapshot shape)
base_version     char(64) # record_state_hash at checkout — optimistic lock at accept
changed_fields   JSON     # ['name','coordinate',…] computed at submit (queue/diff/inset)
rationale        text     # optional proposer note ("GeoNames coord is in the sea")
status           pending | accepted | rejected | superseded | withdrawn
created          auto
reviewed_by      FK User (SET_NULL) ; reviewed_at ; review_note ; applied_changed JSON
```

`Dataset.accept_suggestions` BooleanField(default=False) — owner opt-in to the review loop (1c).

## 3. Flow (reuses the shipped editor + apply-path)

1. **Suggest** — a beta user hits "Suggest a correction" on a record → the existing
   `checkout/place/<id>/` creates a personal `WorkbenchProject(place_record)` working copy (now allowed
   for non-owners). The record editor loads it; because the user lacks edit rights, its primary action is
   **"Submit suggestion"** (owners still get **"Publish correction"** — direct apply, unchanged).
2. **Submit** — `POST suggestions/` {project_id, rationale?} → diff the proposed snapshot vs the current
   record, create a `RecordSuggestion(status='pending', changed_fields=…)`, then delete the throwaway
   working-copy project. Nothing touches the published record.
3. **Review** — a queue page (`/workbench/suggestions/`) lists pending suggestions: staff see all;
   an owner sees those on their datasets. Side-by-side "current → proposed" diff, one-click Accept/Reject.
4. **Accept** — `apply_suggestion`: re-check `record_state_hash(place) == suggestion.base_version` (else
   → `superseded`, ask proposer to redo), then `publish.apply_record_fields(place, proposed_snapshot)` +
   `_reindex_place` (the SAME proven path), set `accepted` + `applied_changed`. **Reject** → `rejected` +
   note. Both notify the proposer (on-site; email later).

## 4. Endpoints (workbench/, beta-gated)

- `POST checkout/place/<id>/` — widen: any beta user (drop the `can_edit` requirement); `project_detail`
  GET returns `can_apply = dataset.can_edit(user)` so the editor picks Publish vs Submit-suggestion.
- `POST suggestions/` — submit {project_id, rationale?}. Beta. Any user.
- `GET  suggestions/` — the reviewer's queue data (staff: all pending; owner: their datasets').
- `POST suggestions/<id>/review/` — {decision: accept|reject, note?}. Auth: staff OR dataset owner.
- `GET  suggestions/for-place/<pid>/` — inset data: `{count, items?}` (items only for owner/staff/proposer, §1e).
- `POST suggestions/<id>/withdraw/` — proposer withdraws their own pending suggestion.

## 5. Surfaces (1d)

- **place_detail.html** (static) — button (beta, non-owner → "Suggest a correction"; owner keeps
  "Correct this record") + inset (`for-place` fetch).
- **portal.js `.source-box`** — per-box "Suggest a correction" (each box already carries
  `data-place-id` + dataset id) + pending marker; needs a `canSuggest` beta flag in the portal context.
- **ds_places.html detail pane** (JS-rendered) — same button + inset when a place is selected.

Shared tiny client (`wb-suggest.js`): opens the checkout→editor flow and renders the inset; imported by
all three surfaces so the affordance is identical.

## 6. Burden controls (1c) — non-negotiable

- Suggesters gated to beta/staff (1b). Submit requires a non-empty diff (no no-op suggestions).
- Owner is queued/notified only on opt-in; staff are the backstop so nothing is lost or forced onto
  absent owners. Digest (not per-item) email is a follow-up; queue + nav badge first.
- Optional later: rate-limit per user/day; auto-expire stale pending suggestions.

## 7. Reuse map

| Need | Reuse |
|---|---|
| Compose a correction | shipped record editor (`wb-place-record` / `wb-record-fields`) + `checkout_place_record` |
| Apply on accept | `publish.apply_record_fields` + `publish._reindex_place` |
| Optimistic lock | `checkout.record_state_hash` (base_version) |
| Diff current↔proposed | `checkout.record_snapshot` + field compare |
| Notify | on-site badge/queue now; whgmail digest later |

## 8. Follow-ups (explicit)

- **Geometry drawing in the correction UI (Stephen, 2026-07-09):** replace the plain lng/lat Coordinate
  inputs in the record editor (`wb-record-fields.js`) with a **MapLibre draw utility** supporting single
  AND multi- geometries (points, lines, polygons — full LPF geometry), mirroring the Map-your-Data draw
  UI. Applies to BOTH the direct record editor and this suggestions flow (so a suggester can fix a
  polygon, not just a point). Backend already round-trips arbitrary geometry per place; only the editor
  is point-only today. Sizeable front-end task — its own phase.
- Widen suggesters beta→all-logged-in (1b) once the loop is proven.
- Email digest to opted-in owners; rate-limiting; auto-expiry.
- v4: migrate the `RecordSuggestion` corpus into PLATO Attestations (provenance already captured).
