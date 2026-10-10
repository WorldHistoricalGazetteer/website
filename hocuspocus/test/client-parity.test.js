// The browser mapping (whg/webpack/js/recon-collab-rt.js) and the server mapping (server.js) MUST
// agree on the Yjs document's shape: the server seeds a document from the REST snapshot and the
// client reads it; the client mirrors its edits and the server flattens them back. This loads the
// REAL client source (ESM imports replaced by the same `yjs` the server uses, the provider and
// IndexedDB stubbed) and runs each direction, so a change to either file alone fails here.
//
// Each test names how it would fail: a cell type lost in either direction, a `notes` key lost when
// two clients annotate different rows, a version message not reaching the client, a mirror that
// rewrites unchanged typed cells.

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const Y = require('yjs');

const S = require('../server.js');

const SRC = path.join(__dirname, '..', '..', 'whg', 'webpack', 'js', 'recon-collab-rt.js');

// A fresh, independent instance of the client module each call (its state is module-level).
function loadClient() {
  const raw = fs.readFileSync(SRC, 'utf8');
  const names = [...raw.matchAll(/^export (?:function|const) (\w+)/mg)].map((m) => m[1]);
  assert.ok(names.includes('connect') && names.includes('mirror') && names.includes('readProject'));
  const src = raw
    .replace(/^import .*$/mg, '')
    .replace(/^export (function|const) /mg, '$1 ');
  const providers = [];
  class FakeProvider {
    constructor(opts) {
      this.opts = opts;
      this.status = 'connected';
      this.awareness = { clientID: 1, setLocalStateField() {}, on() {}, getStates() { return new Map(); } };
      providers.push(this);
    }
    destroy() {}
  }
  class FakeIdb { destroy() {} }
  const factory = new Function('Y', 'HocuspocusProvider', 'IndexeddbPersistence', `${src}\nreturn { ${names.join(', ')} };`);
  const mod = factory(Y, FakeProvider, FakeIdb);
  return { mod, providers };
}

const DOC = '7d2f1c4e-1b1a-4c3d-9e8f-0a1b2c3d4e5f';

// A Y.Map leaking out of either mapping would make deepEqual walk the whole document: fail fast.
function assertPlain(obj, keys, where) {
  for (const k of keys) {
    assert.ok(!(obj[k] instanceof Y.Map) && typeof obj[k] === 'object', `${where}: ${k} did not come back as a plain object`);
  }
}
const META_OBJECTS = ['notes', 'flags', 'scope', 'excludedRows', 'citation'];

const PROJECT = {
  fileName: 'x.csv', title: 'Markets', total: 3, coordFormat: 'dd',
  columns: [{ name: 'Place', role: 'title' }, { name: 'Lat', role: 'lat' }, { name: 'Pop' }],
  rows: [['Richmond', 51.4, 1200], ['York', null, true], ['Leeds']],
  decisions: { '0:1': { status: 'accepted' } }, matches: { 0: [{ id: 'a' }] }, geom: {}, rowTypes: {},
  scope: { cc: ['GB'], when: null }, notes: { 0: 'first note' }, flags: { '1:0': true },
  rowFilters: [{ col: 0, q: 'R' }], excludedRows: { 2: true }, citation: { contributors: [] },
};
// The snapshot the server seeds from and flattens to carries `total` for the row count.
const expected = (p) => ({ ...p, rows: p.rows.map((r) => { const o = r.slice(); while (o.length < 3) o.push(''); return o; }), total: p.rows.length });

function connectTo(mod, ydocState) {
  const { ydoc } = mod.connect({ serverId: DOC, token: 't', wsUrl: 'ws://x', offline: false });
  if (ydocState) Y.applyUpdate(ydoc, ydocState);
  return ydoc;
}

test('server seed → client readProject: types and nested meta survive', () => {
  const { mod } = loadClient();
  const seeded = S.projectToDoc(PROJECT);
  connectTo(mod, Y.encodeStateAsUpdate(seeded));
  const read = mod.readProject();
  assertPlain(read, META_OBJECTS, 'readProject');
  assert.deepEqual(read, expected(PROJECT));
  assert.equal(typeof read.rows[0][1], 'number');
  assert.equal(read.rows[1][1], null);
  assert.equal(read.rows[1][2], true);
  assert.deepEqual(read.notes, { 0: 'first note' });     // a nested map read back as an object
  assert.deepEqual(read.rowFilters, [{ col: 0, q: 'R' }]); // an array, whole
});

test('client mirror → server docToProject: the same, and object-valued meta is a nested map', () => {
  const { mod } = loadClient();
  const ydoc = connectTo(mod, null);
  mod.mirror(PROJECT);
  assert.ok(ydoc.getMap('meta').get('notes') instanceof Y.Map, 'notes stored whole by the client');
  assert.ok(ydoc.getMap('meta').get('excludedRows') instanceof Y.Map);
  assert.ok(!(ydoc.getMap('meta').get('rowFilters') instanceof Y.Map));
  const flat = S.docToProject(ydoc);
  assertPlain(flat, META_OBJECTS, 'docToProject');
  assert.deepEqual(flat, expected(PROJECT));
  assert.equal(typeof flat.rows[0][2], 'number');
  // ...and the server's own flattened form round-trips through a fresh seed unchanged
  assert.equal(S.canonical(S.docToProject(S.projectToDoc(flat))), S.canonical(flat));
});

test('two clients annotating different rows both keep their note; a scalar still wins whole', () => {
  const a = loadClient();
  const b = loadClient();
  const seed = Y.encodeStateAsUpdate(S.projectToDoc(PROJECT));
  const ya = connectTo(a.mod, seed);
  const yb = connectTo(b.mod, seed);
  a.mod.mirror({ ...PROJECT, notes: { 0: 'first note', 1: 'alice on York' } });
  b.mod.mirror({ ...PROJECT, notes: { 0: 'bob rewrote the first' }, title: 'Fairs' });
  Y.applyUpdate(ya, Y.encodeStateAsUpdate(yb));
  Y.applyUpdate(yb, Y.encodeStateAsUpdate(ya));
  const ra = a.mod.readProject();
  const rb = b.mod.readProject();
  assert.deepEqual(ra.notes, { 0: 'bob rewrote the first', 1: 'alice on York' });
  assert.deepEqual(rb.notes, ra.notes);
  assert.equal(ra.title, 'Fairs');
  const flat = S.docToProject(ya);                      // and the server flattens the merged map
  assertPlain(flat, ['notes'], 'docToProject');
  assert.deepEqual(flat.notes, ra.notes);
});

test('mirroring an unchanged project with typed cells writes nothing', () => {
  const { mod } = loadClient();
  // seeded from the padded form: a short row is padded with '' on read and written back once, so
  // only a full-width project can show whether typed cells are compared as values
  const ydoc = connectTo(mod, Y.encodeStateAsUpdate(S.projectToDoc(expected(PROJECT))));
  let updates = 0;
  ydoc.on('update', () => { updates += 1; });
  mod.mirror(expected(PROJECT));
  assert.equal(updates, 0, 'an unchanged project was rewritten (a type compared as text?)');
  mod.mirror({ ...expected(PROJECT), rows: [['Richmond', 51.5, 1200], ['York', null, true], ['Leeds', '', '']] });
  assert.equal(updates, 1);
  assert.equal(ydoc.getArray('rows').get(0).get('1'), 51.5);
});

test('the server’s version message reaches the client as onVersion', () => {
  const { mod, providers } = loadClient();
  const seen = [];
  mod.connect({ serverId: DOC, token: 't', wsUrl: 'ws://x', offline: false, onVersion: (v) => seen.push(v) });
  const p = providers[0];
  assert.equal(typeof p.opts.onStateless, 'function');
  p.opts.onStateless({ payload: S.versionMessage(9) });
  p.opts.onStateless({ payload: 'not json' });
  p.opts.onStateless({ payload: JSON.stringify({ type: 'other', version: 10 }) });
  p.opts.onStateless({ payload: JSON.stringify({ type: 'version', version: '11' }) });
  assert.deepEqual(seen, [9]);
});
