# Workbench collaboration — Phase 2 (Yjs + Hocuspocus real-time co-editing)

**Status:** scoping only. Phases 0–1 (share read-only + Team-owned `WorkbenchProject` with
async optimistic-lock sync) are **built and deployed** in the `workbench` Django app + `recon-sync.js`.
Phase 2 replaces the optimistic-lock stopgap with a CRDT for live, presence-aware co-editing. It
**requires a devops decision** (a new Node service + `wss` endpoint) before implementation — that
decision is the purpose of this doc.

Design origin: `place#112` comment
https://github.com/WorldHistoricalGazetteer/place/issues/112#issuecomment-4912961683 (§3, §9).

---

## What Phase 1 already gives us (build on, don't rebuild)

- `workbench.WorkbenchProject` — server-held `snapshot` (the whole browser `project` object) +
  monotonic `version`; `ProjectSnapshot` history; `Team`/`TeamMember` (owner/editor/viewer).
- Auth + membership gate (`can_access_beta` + `TeamMember.role_for`).
- Client sync metadata on the `project` object (`serverId`, `serverVersion`, `role`, `teamId`, …),
  stripped from the shared snapshot (`SYNC_KEYS` in `reconciliation.js`).
- A **stub endpoint** `POST /reconciliation/projects/<id>/collab-token/` (currently `501`) — the
  intended mint point for the Hocuspocus auth token.

## Phase 2 target architecture

- **CRDT:** Yjs. Map the `project` object to shared types:
  - `rows` → `Y.Array<Y.Map>` (per-row map keyed by column) — makes concurrent row/cell edits
    granular (the coarse structural merge in Phase 1 goes away).
  - `columns` → `Y.Array<Y.Map>`; `decisions` / `matches` / `geom` / `rowTypes` / `scope` /
    `submissionTypes` → `Y.Map`.
  - A one-time **migration** from the current flat JSON snapshot into the Yjs doc on first
    real-time open (seed the doc from `WorkbenchProject.snapshot`).
- **Offline / local-first:** `y-indexeddb` provider (keeps the existing resume-on-reload invariant;
  merges on reconnect). Solo projects stay IndexedDB-only and never open a socket.
- **Sync server:** **Hocuspocus** (Node WS server for Yjs).
  - `onAuthenticate`: client presents the short-lived token from `collab-token/`; Hocuspocus
    validates it and the user's `TeamMember` role for `documentName = <project uuid>` → read-only
    (viewer) vs read-write.
  - **Persistence:** Hocuspocus database extension → store the ydoc update stream/snapshot in
    **Postgres** (reuse the existing DB). Periodically also write a flattened JSON back to
    `WorkbenchProject.snapshot` (+ bump `version`, write a `ProjectSnapshot`) so Phase-0 share,
    export/publish, and non-realtime clients keep working off the same store.
  - **Presence:** Yjs **awareness** API → live cursors / "X is editing row 412" + a member avatar
    strip. New UI in `reconciliation.js` (small; no framework).
- **Client bundle:** `yjs` + `y-indexeddb` + `@hocuspocus/provider` ≈ tens of KB; lazy-load the
  real-time chunk only when a project is opened collaboratively (keeps the initial bundle small,
  matching the existing `recon-*` lazy-chunk pattern).

## The devops decision (blockers before coding)

1. **New service:** add one `docker-compose` service (node/hocuspocus) to the prod stack behind the
   host nginx. Needs a `wss://…/collab` route (subpath or a `collab.` vhost) — see the prod host
   topology memo. Single node is fine initially; document scaling later.
2. **Token minting:** implement `collab-token/` to return a signed, short-TTL token (JWT or a
   server-stored nonce) carrying `{user_id, project_id, role, exp}`; Hocuspocus verifies it (shared
   secret in env, same pattern as other WHG service creds).
3. **ydoc persistence choice:** Hocuspocus Postgres extension vs a small custom store. Decide the
   flatten-back cadence to `WorkbenchProject.snapshot` (on debounce / on last-client-disconnect).
4. **Encryption at rest** for embargoed/sensitive shared copies (open question from §9) — decide
   whether the ydoc/snapshot columns need `encrypted_model_fields` (already a project dependency).
5. **Node in the deploy model:** the WHG deploy is `git reset --hard` with no build step; decide how
   the Hocuspocus service is built/shipped (its own image, prebuilt, committed `node_modules`, or a
   build stage) — the same asset-hosting constraint that shaped the Symphonym decision.

## Migration / coexistence

- Keep the Phase-1 REST push as the fallback for clients without the real-time chunk and for the
  flatten-back path — do **not** remove it. Phase 2 is additive: a project opened in a Yjs-capable
  client uses the socket; everything else continues to use `PUT /projects/<id>/`.
- Conflict UI (`recon-conflict-modal`) becomes dead code for real-time sessions but stays for the
  REST fallback.

## Rough effort

Medium-large: Hocuspocus service + auth + Yjs doc mapping + presence UI + Postgres persistence +
flatten-back. Gated entirely on the devops decision above.
