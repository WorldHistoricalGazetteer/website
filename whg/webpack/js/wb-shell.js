// wb-shell.js — shared primitives for Collaborative Workbench doc-type editors.
//
// The plan (plan-collaborativeCollections §3.3) calls for one shell shared by every doc-type editor:
// local-first IndexedDB persistence, an autosave/"saved" badge, CSRF, and small DOM helpers. This is
// a fresh, focused implementation the new editors build on (starting with wb-place-collection.js).
// The large existing Map-your-Data tool (reconciliation.js) can migrate onto this module later; it is
// deliberately dependency-light and framework-free so that migration is mechanical.
//
// What lives here: the plumbing that is identical across doc-types. What does NOT: the panes/controls
// specific to a doc-type (those live in the doc-type editor + its template).

import { createProject, fetchProject, pushSnapshot, publishProject } from './recon-sync.js';

// ── DOM + text helpers ────────────────────────────────────────────────────────
export const el = (id) => document.getElementById(id);
export const esc = (s) => String(s == null ? '' : s)
  .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
  .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
export const truncate = (s, n) => { s = String(s || ''); return s.length > n ? s.slice(0, n - 1) + '…' : s; };

// Great-circle distance in km between two [lng, lat] points. Used to score suggestions by
// colocation with already-selected members (a collection's places tend to cluster geographically).
export function haversineKm(a, b) {
  if (!a || !b || a[0] == null || b[0] == null) return Infinity;
  const R = 6371, rad = Math.PI / 180;
  const dLat = (b[1] - a[1]) * rad, dLon = (b[0] - a[0]) * rad;
  const s = Math.sin(dLat / 2) ** 2 + Math.cos(a[1] * rad) * Math.cos(b[1] * rad) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.min(1, Math.sqrt(s)));
}

// Mean [lng, lat] of a list of [lng, lat] points (ignoring nulls); null if none.
export function centroid(points) {
  const pts = (points || []).filter((p) => p && p[0] != null && p[1] != null);
  if (!pts.length) return null;
  return [pts.reduce((s, p) => s + p[0], 0) / pts.length, pts.reduce((s, p) => s + p[1], 0) / pts.length];
}

export function debounce(fn, ms) {
  let t = null;
  const wrapped = (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), ms); };
  wrapped.flush = (...args) => { clearTimeout(t); fn(...args); };
  return wrapped;
}

// ── accordion (sole-open panes) ────────────────────────────────────────────────
// Mirrors the Map-your-Data pane accordion (reconciliation.js openPane): panes use the shared
// `.recon-pane` / `.recon-pane-toggle[data-pane]` markup from reconciliation.css; opening one adds
// `.recon-collapsed` to every other. Coloured step-number badges come from per-pane CSS in the
// editor template.
export function openPane(id) {
  document.querySelectorAll('.recon-pane').forEach((p) => p.classList.toggle('recon-collapsed', p.id !== id));
}

export function mountAccordion(openId) {
  document.querySelectorAll('.recon-pane-toggle').forEach((btn) => btn.addEventListener('click', () => {
    const p = btn.closest('.recon-pane');
    if (!p) return;
    if (p.classList.contains('recon-collapsed')) openPane(p.id);   // sole-open
    else p.classList.add('recon-collapsed');                       // click again to collapse
  }));
  if (openId) openPane(openId);
}

// Copy text to the clipboard; resolves true/false. Falls back to a hidden textarea+execCommand when
// the async Clipboard API is unavailable (older browsers / insecure context).
export async function copyToClipboard(text) {
  try {
    if (navigator.clipboard && window.isSecureContext) { await navigator.clipboard.writeText(text); return true; }
  } catch (_) { /* fall through */ }
  try {
    const ta = document.createElement('textarea');
    ta.value = text; ta.style.position = 'fixed'; ta.style.opacity = '0';
    document.body.appendChild(ta); ta.select();
    const ok = document.execCommand('copy'); document.body.removeChild(ta); return ok;
  } catch (_) { return false; }
}

export function csrf() {
  const input = document.querySelector('input[name=csrfmiddlewaretoken]');
  if (input && input.value) return input.value;
  const m = document.cookie.match(/(?:^|;\s*)csrftoken=([^;]+)/);
  return m ? decodeURIComponent(m[1]) : '';
}

// ── local-first project store (IndexedDB, one current draft per browser) ───────
// Mirrors the Map-your-Data store contract (single "current" project per DB) so behaviour and mental
// model match. A doc-type editor picks its own dbName so drafts of different types don't collide.
export function openStore(dbName, storeName = 'project', key = 'current') {
  const open = () => new Promise((resolve, reject) => {
    const req = indexedDB.open(dbName, 1);
    req.onupgradeneeded = () => { if (!req.result.objectStoreNames.contains(storeName)) req.result.createObjectStore(storeName); };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
  const tx = async (mode, fn) => {
    const db = await open();
    return new Promise((resolve, reject) => {
      const t = db.transaction(storeName, mode);
      const store = t.objectStore(storeName);
      const r = fn(store);
      t.oncomplete = () => resolve(r && r.result);
      t.onerror = () => reject(t.error);
    });
  };
  return {
    load: () => tx('readonly', (s) => s.get(key)),
    save: (project) => tx('readwrite', (s) => s.put(project, key)),
    clear: () => tx('readwrite', (s) => s.delete(key)),
  };
}

// ── saved/saving/error status badge ────────────────────────────────────────────
export function statusBadge(node) {
  if (!node) return { saved() {}, saving() {}, error() {}, offline() {} };
  const set = (cls, txt) => { node.className = 'wb-status ' + cls; node.textContent = txt; };
  return {
    saving() { set('wb-status--saving', 'Saving…'); },
    saved() { set('wb-status--saved', 'Saved in your browser'); },
    synced() { set('wb-status--saved', 'Saved to your account'); },
    error(msg) { set('wb-status--error', msg || 'Save failed'); },
    offline() { set('wb-status--saved', 'Saved locally'); },
  };
}

// ── server bridge (optional; local-first works with none of this) ──────────────
// A thin wrapper over recon-sync giving editors a uniform create/push/publish surface that carries
// doc_type and tracks the server id + version for optimistic-lock pushes.
export function serverBridge(docType) {
  const state = { id: null, version: 0 };
  return {
    get id() { return state.id; },
    setServer(id, version) { state.id = id; state.version = version || 0; },
    async create(snapshot, title, team) {
      const r = await createProject(snapshot, title, team, docType);
      if (r.ok && r.data && r.data.id) { state.id = r.data.id; state.version = r.data.version || 1; }
      return r;
    },
    async push(snapshot) {
      if (!state.id) return { ok: false, status: 0, data: { error: 'not saved to account yet' } };
      const r = await pushSnapshot(state.id, snapshot, state.version);
      if (r.ok && r.data && typeof r.data.version === 'number') state.version = r.data.version;
      return r;
    },
    async load(id) {
      const r = await fetchProject(id);
      if (r.ok && r.data) { state.id = id; state.version = r.data.version || 0; }
      return r;
    },
    publish() { return publishProject(state.id); },
  };
}
