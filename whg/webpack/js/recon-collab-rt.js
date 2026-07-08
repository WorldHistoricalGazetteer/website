// recon-collab-rt.js — real-time collaboration (Workbench Phase 2, place#112).
//
// Models the WHOLE workbench `project` as a Yjs CRDT document so a team edits it together, live and
// conflict-free: raw table cells, column roles, reconciliation matches, review decisions, drawn
// geometry, row place-types and dataset-wide settings all merge per-field. Adds presence (who's here
// + which cell they're on) and offline persistence (y-indexeddb), merging on reconnect.
//
// Architecture: the plain `project` object in reconciliation.js stays the UI's working copy (so the
// 250 KB of rendering code is untouched). This module bidirectionally *mirrors* it to a Yjs doc:
//   • mirror(project)   — reconcile the Yjs doc to match the local project (local edits → peers)
//   • readProject()     — reconstruct a plain project from the Yjs doc (peers → local)
// Only runs for team (non-personal) server projects; solo/personal projects never load this chunk.
//
// Doc shape:  rows = Y.Array<Y.Map>  (a Y.Map per row, keyed by column index "0","1",… → cell text)
//             columns = Y.Array<Y.Map>  (a Y.Map per column: name/role/child/…)
//             decisions|matches|geom|rowTypes = Y.Map  (keyed overlays, per key = JSON value)
//             meta = Y.Map  (all remaining scalar/whole-value fields: scope, coordFormat, total, …)

import * as Y from 'yjs';
import { HocuspocusProvider } from '@hocuspocus/provider';
import { IndexeddbPersistence } from 'y-indexeddb';

// Keyed-overlay collections mapped to their own top-level Y.Map.
const KEYED = ['matches', 'decisions', 'geom', 'rowTypes'];
// Fields NOT stored in `meta` (they have dedicated shared types, or are per-client sync metadata).
const META_EXCLUDE = new Set([
  'rows', 'columns', 'matches', 'decisions', 'geom', 'rowTypes', 'id',
  'serverId', 'serverVersion', 'role', 'teamId', 'teamTitle', 'teamPersonal', 'sharedToken', 'sharedUrl',
]);

let provider = null;
let idb = null;
let ydoc = null;
let yRows = null, yCols = null, yMeta = null;
const yKeyed = {};
let cbs = {};
let _remoteTimer = null;
let _dirty = new Set(); // which top-level sections changed since the last remote flush

export function isConnected() { return !!(provider && provider.status === 'connected'); }
export function isEmpty() {
  return !ydoc || (yRows.length === 0 && yCols.length === 0 && yMeta.size === 0);
}

// opts: { serverId, token, wsUrl, user:{name,color}, offline, onStatus, onSynced, onRemote(project), onPresence(states) }
export function connect(opts) {
  disconnect();
  cbs = opts || {};
  ydoc = new Y.Doc();
  yRows = ydoc.getArray('rows');
  yCols = ydoc.getArray('columns');
  yMeta = ydoc.getMap('meta');
  KEYED.forEach((k) => { yKeyed[k] = ydoc.getMap(k); });

  if (opts.offline !== false) {
    try { idb = new IndexeddbPersistence('wb-' + opts.serverId, ydoc); } catch (_) { /* private mode */ }
  }

  provider = new HocuspocusProvider({
    url: opts.wsUrl,
    name: opts.serverId,
    token: opts.token,
    document: ydoc,
    onStatus: ({ status }) => { if (opts.onStatus) opts.onStatus(status); },
    onAuthenticationFailed: () => { if (opts.onStatus) opts.onStatus('unauthorized'); },
    onSynced: () => { if (opts.onSynced) opts.onSynced(); },
  });

  // Presence (awareness): advertise who we are; notify on any change.
  if (opts.user) provider.awareness.setLocalStateField('user', opts.user);
  provider.awareness.on('change', () => { if (cbs.onPresence) cbs.onPresence(publicStates()); });

  // Remote changes → debounced project rebuild, tagging which section changed so the client can
  // repaint only the affected panes (ignore our own local transactions).
  const mark = (section) => (events, tx) => { if (tx && tx.local) return; _dirty.add(section); scheduleRemote(); };
  yRows.observeDeep(mark('rows'));
  yCols.observeDeep(mark('columns'));
  yMeta.observe((e, tx) => { if (tx && tx.local) return; _dirty.add('meta'); scheduleRemote(); });
  KEYED.forEach((k) => yKeyed[k].observeDeep(mark(k)));

  return { provider, ydoc };
}

function scheduleRemote() {
  clearTimeout(_remoteTimer);
  _remoteTimer = setTimeout(() => {
    const sections = Array.from(_dirty);
    _dirty.clear();
    if (cbs.onRemote) cbs.onRemote(readProject(), sections);
  }, 160);
}

// ── local project → Yjs (one transaction so observers see a single, self-tagged change) ──────────
export function mirror(project) {
  if (!ydoc || !project) return;
  ydoc.transact(() => {
    // meta (everything scalar/whole-value)
    for (const k of Object.keys(project)) {
      if (META_EXCLUDE.has(k)) continue;
      if (!jsonEq(yMeta.get(k), project[k])) yMeta.set(k, project[k]);
    }
    for (const k of Array.from(yMeta.keys())) if (!(k in project) || META_EXCLUDE.has(k)) yMeta.delete(k);

    reconcileObjArray(yCols, project.columns || []);
    reconcileRows(yRows, project.rows || []);

    for (const kk of KEYED) {
      const ymap = yKeyed[kk];
      const src = project[kk] || {};
      for (const [key, val] of Object.entries(src)) if (!jsonEq(ymap.get(key), val)) ymap.set(key, val);
      for (const key of Array.from(ymap.keys())) if (!(key in src)) ymap.delete(key);
    }
  }, 'local');
}

// ── Yjs → plain project ──────────────────────────────────────────────────────────────────────────
export function readProject() {
  const p = {};
  yMeta.forEach((v, k) => { p[k] = v; });
  p.columns = yCols.toArray().map((m) => (m instanceof Y.Map ? mapToObj(m) : m));
  const ncols = p.columns.length;
  p.rows = yRows.toArray().map((m) => {
    if (!(m instanceof Y.Map)) return Array.isArray(m) ? m : [];
    let max = ncols - 1;
    m.forEach((v, k) => { const j = Number(k); if (j > max) max = j; });
    const arr = [];
    for (let j = 0; j <= max; j++) { const v = m.get(String(j)); arr.push(v == null ? '' : v); }
    return arr;
  });
  p.total = p.rows.length;
  for (const kk of KEYED) { p[kk] = {}; yKeyed[kk].forEach((v, k) => { p[kk][k] = v; }); }
  return p;
}

// ── reconcilers ────────────────────────────────────────────────────────────────────────────────
function rowMap(row) { const m = new Y.Map(); for (let j = 0; j < row.length; j++) m.set(String(j), cell(row[j])); return m; }
function objMap(obj) { const m = new Y.Map(); for (const [k, v] of Object.entries(obj)) m.set(k, v); return m; }

// Reconcile in place by index: append ONLY for genuinely new indices (i >= length), otherwise update
// the existing Y.Map. Never insert at an occupied index (that would shift + duplicate).
function reconcileRows(yarr, rows) {
  while (yarr.length > rows.length) yarr.delete(yarr.length - 1, 1);
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i] || [];
    if (i >= yarr.length) { yarr.push([rowMap(row)]); continue; }
    const ym = yarr.get(i);
    if (!(ym instanceof Y.Map)) { yarr.delete(i, 1); yarr.insert(i, [rowMap(row)]); continue; }
    for (let j = 0; j < row.length; j++) { const cv = cell(row[j]); if (ym.get(String(j)) !== cv) ym.set(String(j), cv); }
    for (const key of Array.from(ym.keys())) if (Number(key) >= row.length) ym.delete(key);
  }
}

function reconcileObjArray(yarr, objs) {
  while (yarr.length > objs.length) yarr.delete(yarr.length - 1, 1);
  for (let i = 0; i < objs.length; i++) {
    const obj = objs[i] || {};
    if (i >= yarr.length) { yarr.push([objMap(obj)]); continue; }
    const ym = yarr.get(i);
    if (!(ym instanceof Y.Map)) { yarr.delete(i, 1); yarr.insert(i, [objMap(obj)]); continue; }
    for (const [k, v] of Object.entries(obj)) if (!jsonEq(ym.get(k), v)) ym.set(k, v);
    for (const k of Array.from(ym.keys())) if (!(k in obj)) ym.delete(k);
  }
}

function mapToObj(m) { const o = {}; m.forEach((v, k) => { o[k] = v; }); return o; }
function cell(v) { return String(v == null ? '' : v); }
function jsonEq(a, b) { return JSON.stringify(a) === JSON.stringify(b); }

// ── presence ─────────────────────────────────────────────────────────────────────────────────────
export function setCursor(cursor) {
  if (provider) provider.awareness.setLocalStateField('cursor', cursor || null);
}
function publicStates() {
  if (!provider) return [];
  const me = provider.awareness.clientID;
  const out = [];
  provider.awareness.getStates().forEach((state, id) => {
    if (id === me || !state || !state.user) return;
    out.push({ id, user: state.user, cursor: state.cursor || null });
  });
  return out;
}

export function disconnect() {
  clearTimeout(_remoteTimer);
  _dirty.clear();
  if (provider) { try { provider.destroy(); } catch (_) { /* ignore */ } }
  if (idb) { try { idb.destroy(); } catch (_) { /* ignore */ } }
  provider = idb = ydoc = yRows = yCols = yMeta = null;
  cbs = {};
}
