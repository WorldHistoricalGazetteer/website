// recon-sync.js — network layer for collaborative Workbench projects (place#112, Phase 0/1).
//
// Pure fetch calls against the Django API mounted under /reconciliation/ (see workbench/urls.py).
// Session-authenticated + CSRF (mirrors the /reconcile call in reconciliation.js). No DOM, no
// project state — the orchestration (debounced push, conflict UI, read-only mode) lives in
// reconciliation.js which owns the `project` object.

const BASE = '/reconciliation';

function csrf() {
  const input = document.querySelector('input[name=csrfmiddlewaretoken]');
  if (input && input.value) return input.value;
  const m = document.cookie.match(/(?:^|;\s*)csrftoken=([^;]+)/);
  return m ? decodeURIComponent(m[1]) : '';
}

// Low-level request. Returns { ok, status, data }. Never throws on HTTP status (callers branch on
// status — e.g. 409 conflict is an expected, meaningful response); only network failures reject.
async function req(method, path, body) {
  const opts = {
    method,
    credentials: 'same-origin',
    headers: { 'X-CSRFToken': csrf() },
  };
  if (body !== undefined) {
    opts.headers['Content-Type'] = 'application/json';
    opts.body = JSON.stringify(body);
  }
  const res = await fetch(BASE + path, opts);
  let data = null;
  try { data = await res.json(); } catch (_) { /* empty/non-JSON body */ }
  return { ok: res.ok, status: res.status, data };
}

// ── projects ────────────────────────────────────────────────────────────────
export const listProjects = () => req('GET', '/projects/');
export const createProject = (snapshot, title, team) =>
  req('POST', '/projects/', { snapshot, title, team });
export const fetchProject = (id) => req('GET', `/projects/${id}/`);
export const deleteProject = (id) => req('DELETE', `/projects/${id}/`);

// PUT a snapshot built on `baseVersion`. Resolves to { status, data }:
//   200 { status:'ok'|'merged', version, snapshot? }   — accepted (adopt merged snapshot if present)
//   409 { status:'conflict', version, conflicts, merged }  — resolve + re-push at `version`
//   409 { status:'stale', version, snapshot }              — ancestor gone; reload server copy
export const pushSnapshot = (id, snapshot, baseVersion) =>
  req('PUT', `/projects/${id}/`, { snapshot, base_version: baseVersion });

// Mint a short-lived JWT for the Hocuspocus real-time service (Phase 2). 200 {token,document,role},
// or 501 (realtime not configured) / 403 (not a member) → caller stays on the Phase-1 REST sync.
export const collabToken = (id) => req('POST', `/projects/${id}/collab-token/`);

// Fetch a shared Google Sheet as CSV via the server-side proxy (avoids browser CORS).
export const importGSheet = (url) => req('POST', '/gsheet/', { url });

// ── sharing (Phase 0) ─────────────────────────────────────────────────────────
export const shareProject = (id) => req('POST', `/projects/${id}/share/`);
export const unshareProject = (id) => req('DELETE', `/projects/${id}/share/`);
export const fetchShared = (token) => req('GET', `/shared/${token}/`);

// ── teams ───────────────────────────────────────────────────────────────────
export const listTeams = () => req('GET', '/teams/');
export const createTeam = (title, description) => req('POST', '/teams/', { title, description });
export const listMembers = (teamId) => req('GET', `/teams/${teamId}/members/`);
export const addMember = (teamId, identifier, role) =>
  req('POST', `/teams/${teamId}/members/`, { identifier, role });
export const removeMember = (teamId, uid) => req('DELETE', `/teams/${teamId}/members/${uid}/`);
