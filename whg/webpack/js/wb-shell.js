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

export function debounce(fn, ms) {
  let t = null;
  const wrapped = (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), ms); };
  wrapped.flush = (...args) => { clearTimeout(t); fn(...args); };
  return wrapped;
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
