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
// Doc shape:  rows = Y.Array<Y.Map>  (a Y.Map per row, keyed by column index "0","1",… → the cell's
//                    JSON value as it is: a number stays a number, null stays null; only an ABSENT
//                    cell reads back as '')
//             columns = Y.Array<Y.Map>  (a Y.Map per column: name/role/child/…)
//             decisions|matches|geom|rowTypes = Y.Map  (keyed overlays, per key = JSON value)
//             meta = Y.Map  (all remaining fields: scope, coordFormat, total, notes, flags, …); an
//                    object-valued field is a NESTED Y.Map keyed like the object, so two people
//                    annotating different rows both keep their note; scalars and arrays are whole.
// MUST match hocuspocus/server.js (projectToDoc/docToProject), which seeds and flattens the same doc.

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

// opts: { serverId, token, wsUrl, user:{name,color}, offline, onStatus, onSynced, onRemote(project), onPresence(states),
//         onVersion(version) — the project's REST version after each server store, so a client that
//         drops back to REST pushes with a current base_version }
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
    onStateless: ({ payload }) => {
      let msg = null;
      try { msg = JSON.parse(payload); } catch (_) { return; }
      if (msg && msg.type === 'version' && Number.isInteger(msg.version) && cbs.onVersion) cbs.onVersion(msg.version);
    },
  });

  // Presence (awareness): advertise who we are; notify on any change.
  if (opts.user) provider.awareness.setLocalStateField('user', opts.user);
  provider.awareness.on('change', () => { if (cbs.onPresence) cbs.onPresence(publicStates()); });

  // Remote changes → debounced project rebuild, tagging which section changed so the client can
  // repaint only the affected panes (ignore our own local transactions).
  const mark = (section) => (events, tx) => { if (tx && tx.local) return; _dirty.add(section); scheduleRemote(); };
  yRows.observeDeep(mark('rows'));
  yCols.observeDeep(mark('columns'));
  yMeta.observeDeep(mark('meta')); // deep: an object-valued field is a nested map
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
    // meta: an object-valued field is reconciled key by key inside its nested map (only the keys
    // this client changed are written, so a peer's concurrent key survives); anything else whole.
    for (const k of Object.keys(project)) {
      if (META_EXCLUDE.has(k)) continue;
      const v = project[k];
      const cur = yMeta.get(k);
      if (isPlainObject(v)) {
        if (cur instanceof Y.Map) reconcileMap(cur, v);
        else yMeta.set(k, objMap(v)); // first write, or a whole value left by an older client
      } else if (!jsonEq(cur, v)) {
        yMeta.set(k, jsonValue(v));
      }
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
  yMeta.forEach((v, k) => { p[k] = fromY(v); });
  p.columns = yCols.toArray().map((m) => (m instanceof Y.Map ? mapToObj(m) : m));
  const ncols = p.columns.length;
  p.rows = yRows.toArray().map((m) => {
    if (!(m instanceof Y.Map)) return Array.isArray(m) ? m : [];
    let max = ncols - 1;
    m.forEach((v, k) => { const j = Number(k); if (j > max) max = j; });
    const arr = [];
    for (let j = 0; j <= max; j++) { const v = m.get(String(j)); arr.push(v === undefined ? '' : fromY(v)); }
    return arr;
  });
  p.total = p.rows.length;
  for (const kk of KEYED) { p[kk] = {}; yKeyed[kk].forEach((v, k) => { p[kk][k] = fromY(v); }); }
  return p;
}

// ── reconcilers ────────────────────────────────────────────────────────────────────────────────
function rowMap(row) { const m = new Y.Map(); for (let j = 0; j < row.length; j++) m.set(String(j), cell(row[j])); return m; }
function objMap(obj) { const m = new Y.Map(); for (const [k, v] of Object.entries(obj)) m.set(k, jsonValue(v)); return m; }
// Bring a nested meta map to `obj`, touching only the keys that differ.
function reconcileMap(ym, obj) {
  for (const [k, v] of Object.entries(obj)) if (!jsonEq(ym.get(k), v)) ym.set(k, jsonValue(v));
  for (const k of Array.from(ym.keys())) if (!(k in obj)) ym.delete(k);
}

// Reconcile in place by index: append ONLY for genuinely new indices (i >= length), otherwise update
// the existing Y.Map. Never insert at an occupied index (that would shift + duplicate).
function reconcileRows(yarr, rows) {
  while (yarr.length > rows.length) yarr.delete(yarr.length - 1, 1);
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i] || [];
    if (i >= yarr.length) { yarr.push([rowMap(row)]); continue; }
    const ym = yarr.get(i);
    if (!(ym instanceof Y.Map)) { yarr.delete(i, 1); yarr.insert(i, [rowMap(row)]); continue; }
    for (let j = 0; j < row.length; j++) { const cv = cell(row[j]); if (!jsonEq(ym.get(String(j)), cv)) ym.set(String(j), cv); }
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

function mapToObj(m) { const o = {}; m.forEach((v, k) => { o[k] = fromY(v); }); return o; }
// A cell is stored as the JSON value it is (place#314: no stringification, so a number or null
// survives a live session). JSON has no undefined, which becomes null.
function cell(v) { return jsonValue(v); }
function jsonValue(v) { return v === undefined ? null : v; }
function isPlainObject(v) { return v !== null && typeof v === 'object' && !Array.isArray(v); }
function fromY(v) { return v instanceof Y.Map ? v.toJSON() : v; }
function jsonEq(a, b) { return JSON.stringify(a) === JSON.stringify(b); }

// ── presence ─────────────────────────────────────────────────────────────────────────────────────
export function setCursor(cursor) {
  if (provider) provider.awareness.setLocalStateField('cursor', cursor || null);
}
// Advisory activity broadcast over awareness (place#112): e.g. {type:'reconciling', column:'parish'}.
// Peers use it to soft-lock the Reconcile button so two members don't kick off the same heavy,
// gateway-hammering pass at once. Advisory only — awareness is best-effort and clears if a client
// drops, so it can never deadlock. Pass null to clear.
export function setActivity(activity) {
  if (provider) provider.awareness.setLocalStateField('activity', activity || null);
}
// Ephemeral team chat (place#154): messages ride the awareness channel — never persisted, delivered only
// to members connected right now. `msg` holds the latest sent message ({id,text,ts,from,context}); peers
// show each new id once. `typing` is a timestamp for the "…is typing" hint. Both clear when the client
// drops, so nothing is stored anywhere.
export function sendMessage(msg) {
  if (provider) provider.awareness.setLocalStateField('msg', msg || null);
}
export function setTyping(on) {
  if (provider) provider.awareness.setLocalStateField('typing', on ? Date.now() : null);
}
function publicStates() {
  if (!provider) return [];
  const me = provider.awareness.clientID;
  const out = [];
  provider.awareness.getStates().forEach((state, id) => {
    if (id === me || !state || !state.user) return;
    out.push({ id, user: state.user, cursor: state.cursor || null, activity: state.activity || null,
               msg: state.msg || null, typing: state.typing || null });
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
