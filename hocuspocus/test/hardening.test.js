// Hardening of the Workbench collaboration service (place#314). Run: `npm test` in hocuspocus/
// (Node 18+, no database, no socket: the pieces take their dependencies as arguments).
//
// Every test here names the way it would fail: a token for another document or audience accepted,
// a removed member still connecting, a foreign origin upgraded, an opaque doc_type materialised,
// a budget that never trips.

const test = require('node:test');
const assert = require('node:assert/strict');
const jwt = require('jsonwebtoken');
const Y = require('yjs');

const S = require('../server.js');

const SECRET = 'test-secret';
const DOC = '7d2f1c4e-1b1a-4c3d-9e8f-0a1b2c3d4e5f';
const OTHER_DOC = '00000000-1111-4222-8333-444444444444';
const NOW = 1_800_000_000;

// `at` is the mint time: the pure verify tests pin it to NOW and pass their own clock; the hook
// tests leave it at the real clock, because onAuthenticate uses Date.now().
function mint(overrides = {}, secret = SECRET, at = Math.floor(Date.now() / 1000)) {
  const payload = {
    iss: S.JWT_ISSUER, aud: S.JWT_AUDIENCE, sub: '42', name: 'Alice', project_id: DOC,
    doc_type: 'reconciliation', role: 'editor', jti: 'x', iat: at, nbf: at, exp: at + 120,
    ...overrides,
  };
  for (const k of Object.keys(overrides)) if (overrides[k] === undefined) delete payload[k];
  return jwt.sign(payload, secret, { algorithm: 'HS256' }); // keeps the explicit iat
}
const atNow = (overrides = {}, secret = SECRET) => mint(overrides, secret, NOW);

const OWN = { host: 'whgazetteer.org', 'x-forwarded-proto': 'https', origin: 'https://whgazetteer.org' };

// ── token ────────────────────────────────────────────────────────────────────
test('a well-formed token for this document verifies', () => {
  const p = S.verifyCollabToken(atNow(), SECRET, DOC, NOW + 10);
  assert.equal(p.sub, '42');
});

test('a token is refused when: wrong secret, wrong audience, wrong issuer, no audience', () => {
  assert.throws(() => S.verifyCollabToken(atNow({}, 'other'), SECRET, DOC, NOW), /invalid/);
  assert.throws(() => S.verifyCollabToken(atNow({ aud: 'something-else' }), SECRET, DOC, NOW), /audience/);
  assert.throws(() => S.verifyCollabToken(atNow({ iss: 'someone' }), SECRET, DOC, NOW), /issuer/);
  assert.throws(() => S.verifyCollabToken(atNow({ aud: undefined }), SECRET, DOC, NOW), /audience/);
  assert.throws(() => S.verifyCollabToken(atNow({ iss: undefined }), SECRET, DOC, NOW), /issuer/);
});

test('an expired, not-yet-valid, never-expiring or long-lived token is refused', () => {
  assert.throws(() => S.verifyCollabToken(atNow(), SECRET, DOC, NOW + 121), /expired/);
  assert.throws(() => S.verifyCollabToken(atNow({ nbf: NOW + 60 }), SECRET, DOC, NOW), /active/);
  assert.throws(() => S.verifyCollabToken(atNow({ exp: undefined }), SECRET, DOC, NOW), /exp/);
  assert.throws(() => S.verifyCollabToken(atNow({ exp: NOW + 3600 }), SECRET, DOC, NOW), /lifetime/);
});

test('a token for another document, or without a subject, is refused', () => {
  assert.throws(() => S.verifyCollabToken(atNow(), SECRET, OTHER_DOC, NOW), /mismatch/);
  assert.throws(() => S.verifyCollabToken(atNow({ project_id: 'not-a-uuid' }), SECRET, 'not-a-uuid', NOW), /document/);
  assert.throws(() => S.verifyCollabToken(atNow({ sub: undefined }), SECRET, DOC, NOW), /subject/);
  assert.throws(() => S.verifyCollabToken(atNow({ sub: 'alice' }), SECRET, DOC, NOW), /subject/);
});

test('the none algorithm and an RS-signed token are refused', () => {
  const header = Buffer.from(JSON.stringify({ alg: 'none', typ: 'JWT' })).toString('base64url');
  const body = Buffer.from(JSON.stringify({ iss: S.JWT_ISSUER, aud: S.JWT_AUDIENCE, sub: '1',
    project_id: DOC, iat: NOW, exp: NOW + 60 })).toString('base64url');
  assert.throws(() => S.verifyCollabToken(`${header}.${body}.`, SECRET, DOC, NOW), /invalid/);
});

// ── origin ───────────────────────────────────────────────────────────────────
test('the site’s own origin is allowed; another origin, or none, is not', () => {
  assert.equal(S.originAllowed(OWN, []), true);
  assert.equal(S.originAllowed({ ...OWN, origin: 'https://evil.example' }, []), false);
  assert.equal(S.originAllowed({ host: 'whgazetteer.org', 'x-forwarded-proto': 'https' }, []), false);
  // scheme matters: an http page on the same host is not this site behind TLS
  assert.equal(S.originAllowed({ ...OWN, origin: 'http://whgazetteer.org' }, []), false);
  // dev behind the same nginx pattern
  assert.equal(S.originAllowed({ host: 'dev.whgazetteer.org', 'x-forwarded-proto': 'https',
    origin: 'https://dev.whgazetteer.org' }, []), true);
});

test('an allow-listed extra origin is allowed, case-insensitively, and only that one', () => {
  const allowed = ['https://pelagios.org'];
  assert.equal(S.originAllowed({ ...OWN, origin: 'https://pelagios.org' }, allowed), true);
  assert.equal(S.originAllowed({ ...OWN, origin: 'HTTPS://Pelagios.org' }, allowed), true);
  assert.equal(S.originAllowed({ ...OWN, origin: 'https://pelagios.org.evil.example' }, allowed), false);
  assert.equal(S.originAllowed({ ...OWN, origin: 'https://sub.pelagios.org' }, allowed), false);
});

test('the upgrade hook writes a 403, destroys the socket and rejects with nothing', async () => {
  const hooks = S.buildHooks({ config: S.makeConfig({ HOCUSPOCUS_SECRET: SECRET }), query: async () => ({ rows: [] }) });
  const written = [];
  const socket = { write: (s) => written.push(s), destroyed: false, destroy() { this.destroyed = true; } };
  let rejectedWith = 'not rejected';
  try {
    await hooks.onUpgrade({ request: { headers: { ...OWN, origin: 'https://evil.example' } }, socket });
  } catch (e) { rejectedWith = e; }
  assert.equal(rejectedWith, undefined); // falsy: Hocuspocus stops the default handler, no rethrow
  assert.match(written[0], /^HTTP\/1\.1 403/);
  assert.equal(socket.destroyed, true);
  // ...and a good origin passes through untouched
  const ok = { write: () => assert.fail('wrote'), destroy: () => assert.fail('destroyed') };
  await hooks.onUpgrade({ request: { headers: OWN }, socket: ok });
});

// ── authorisation on connect ─────────────────────────────────────────────────
function dbWith(rows) {
  const calls = [];
  return { calls, query: async (sql, params) => { calls.push({ sql, params }); return { rows }; } };
}

test('membership, role and doc_type come from the database, not the token', async () => {
  const db = dbWith([{ doc_type: 'reconciliation', role: 'viewer', is_active: true }]);
  const hooks = S.buildHooks({ config: S.makeConfig({ HOCUSPOCUS_SECRET: SECRET }), query: db.query });
  const connection = { readOnly: false };
  const ctx = await hooks.onAuthenticate({ documentName: DOC, token: mint({ role: 'owner' }),
    connection, requestHeaders: OWN });
  assert.equal(connection.readOnly, true);         // the token said owner; the database says viewer
  assert.equal(ctx.user.role, 'viewer');
  assert.deepEqual(db.calls[0].params, [DOC, 42]); // looked up by (document, subject)
  assert.match(db.calls[0].sql, /workbench_team_member/);
});

test('a user removed from the team since the mint is refused', async () => {
  const hooks = S.buildHooks({ config: S.makeConfig({ HOCUSPOCUS_SECRET: SECRET }), query: dbWith([]).query });
  await assert.rejects(hooks.onAuthenticate({ documentName: DOC, token: mint(), connection: {},
    requestHeaders: OWN }), /not a member/);
});

test('an opaque doc_type never gets a live connection, whatever the token claims', async () => {
  const hooks = S.buildHooks({ config: S.makeConfig({ HOCUSPOCUS_SECRET: SECRET }),
    query: dbWith([{ doc_type: 'plato', role: 'owner', is_active: true }]).query });
  await assert.rejects(hooks.onAuthenticate({ documentName: DOC, token: mint({ doc_type: 'reconciliation' }),
    connection: {}, requestHeaders: OWN }), /live editing is disabled for doc_type plato/);
  assert.equal(S.DEFAULT_LIVE_DOC_TYPES.includes('plato'), false);
});

test('authentication also refuses a foreign origin and a bad token', async () => {
  const hooks = S.buildHooks({ config: S.makeConfig({ HOCUSPOCUS_SECRET: SECRET }),
    query: dbWith([{ doc_type: 'reconciliation', role: 'editor', is_active: true }]).query });
  await assert.rejects(hooks.onAuthenticate({ documentName: DOC, token: mint(), connection: {},
    requestHeaders: { ...OWN, origin: 'https://evil.example' } }), /origin/);
  await assert.rejects(hooks.onAuthenticate({ documentName: DOC, token: mint({}, 'other'), connection: {},
    requestHeaders: OWN }), /invalid/);
  // the positive control for the two above
  const ctx = await hooks.onAuthenticate({ documentName: DOC, token: mint(), connection: {}, requestHeaders: OWN });
  assert.equal(ctx.user.role, 'editor');
});

// ── budgets ──────────────────────────────────────────────────────────────────
test('the message and byte budgets trip, per connection, and reset after a minute', () => {
  let t = 0;
  const c = new S.WindowCounter({ messageRate: 3, byteRate: 1000, docCheckEvery: 1e9 }, () => t);
  assert.equal(c.charge('a', 10), null);
  assert.equal(c.charge('a', 10), null);
  assert.equal(c.charge('a', 10), null);
  assert.equal(c.charge('a', 10), 'message rate exceeded');
  assert.equal(c.charge('b', 10), null);                 // another connection has its own budget
  assert.equal(c.charge('b', 2000), 'byte rate exceeded');
  t = 60_001;
  assert.equal(c.charge('a', 10), null);                 // new window
  c.forget('a');
  assert.equal(c.state.has('a'), false);
});

test('a zero rate means unlimited', () => {
  const c = new S.WindowCounter({ messageRate: 0, byteRate: 0, docCheckEvery: 1e9 }, () => 0);
  for (let i = 0; i < 10_000; i++) assert.equal(c.charge('a', 1e6), null);
});

test('beforeHandleMessage closes with a policy code when over budget, and measures the document', async () => {
  const config = { ...S.makeConfig({ HOCUSPOCUS_SECRET: SECRET }), messageRate: 2, maxDocBytes: 100, docCheckEvery: 1 };
  const sizes = [50, 500];
  const hooks = S.buildHooks({ config, query: async () => ({ rows: [] }), encodedSize: () => sizes.shift() });
  const doc = new Y.Doc();
  await hooks.beforeHandleMessage({ socketId: 's', update: new Uint8Array(10), document: doc }); // size 50: fine
  try {
    await hooks.beforeHandleMessage({ socketId: 's', update: new Uint8Array(10), document: doc }); // size 500
    assert.fail('did not throw');
  } catch (e) {
    assert.equal(e.code, S.CLOSE_POLICY);
    assert.match(e.reason, /document too large/);
  }
  await assert.rejects(hooks.beforeHandleMessage({ socketId: 's', update: new Uint8Array(1), document: doc }),
    (e) => e.code === S.CLOSE_POLICY && /message rate/.test(e.reason));
  await hooks.onDisconnect({ socketId: 's' });
  // after a disconnect the budget is gone
  const hooks2 = S.buildHooks({ config: { ...config, docCheckEvery: 1e9 }, query: async () => ({ rows: [] }) });
  await hooks2.beforeHandleMessage({ socketId: 's', update: new Uint8Array(1), document: doc });
});

test('the real document measurement sees growth', async () => {
  const config = { ...S.makeConfig({ HOCUSPOCUS_SECRET: SECRET }), maxDocBytes: 2000, docCheckEvery: 1 };
  const hooks = S.buildHooks({ config, query: async () => ({ rows: [] }) });
  const small = S.projectToDoc({ rows: [['a']], columns: [{ name: 'x' }] });
  await hooks.beforeHandleMessage({ socketId: 's', update: new Uint8Array(1), document: small });
  const big = S.projectToDoc({ rows: Array.from({ length: 400 }, () => ['abcdefghij']), columns: [{ name: 'x' }] });
  await assert.rejects(hooks.beforeHandleMessage({ socketId: 't', update: new Uint8Array(1), document: big }),
    /document too large/);
});

// ── config ───────────────────────────────────────────────────────────────────
test('configuration defaults need nothing in the environment, and the env overrides them', () => {
  const d = S.makeConfig({ HOCUSPOCUS_SECRET: 's' });
  assert.equal(d.port, 8010);
  assert.deepEqual(d.allowedOrigins, []);
  assert.deepEqual(d.liveDocTypes, S.DEFAULT_LIVE_DOC_TYPES);
  assert.equal(d.maxMessageBytes, 1024 * 1024);
  assert.equal(d.maxDocBytes, 8 * 1024 * 1024);
  assert.equal(d.messageRate, 600);
  const o = S.makeConfig({ HOCUSPOCUS_SECRET: 's', HOCUSPOCUS_PORT: '8011',
    HOCUSPOCUS_ALLOWED_ORIGINS: 'https://pelagios.org, https://other.example',
    HOCUSPOCUS_LIVE_DOC_TYPES: 'reconciliation', HOCUSPOCUS_MESSAGE_RATE: '0' });
  assert.equal(o.port, 8011);
  assert.deepEqual(o.allowedOrigins, ['https://pelagios.org', 'https://other.example']);
  assert.deepEqual(o.liveDocTypes, ['reconciliation']);
  assert.equal(o.messageRate, 0);
});

// ── the mapping ──────────────────────────────────────────────────────────────
test('projectToDoc/docToProject round-trip a Map-your-Data snapshot', () => {
  const snap = { fileName: 'x.csv', columns: [{ name: 'Place' }, { name: 'Lat' }], rows: [['Richmond', '51.4'], ['York', null]],
    decisions: { '0:1': { status: 'accepted' } }, matches: {}, geom: {}, rowTypes: {}, scope: { cc: ['GB'] } };
  const back = S.docToProject(S.projectToDoc(snap));
  assert.equal(back.fileName, 'x.csv');
  assert.deepEqual(back.rows, [['Richmond', '51.4'], ['York', null]]);
  assert.deepEqual(back.decisions, snap.decisions);
  assert.deepEqual(back.scope, snap.scope);
  assert.equal(back.total, 2);
});

// ── follow-ups (place#314 "remaining") ───────────────────────────────────────
test('cells keep their types: a number, a boolean, null and an object come back as themselves; only an absent cell is ""', () => {
  const rows = [['Richmond', 51.4, true, null, { a: 1 }], ['York']];
  const back = S.docToProject(S.projectToDoc({ columns: [{ name: 'a' }, { name: 'b' }, { name: 'c' }, { name: 'd' }, { name: 'e' }], rows }));
  assert.deepEqual(back.rows[0], rows[0]);
  assert.equal(typeof back.rows[0][1], 'number');
  assert.equal(back.rows[0][3], null);
  assert.deepEqual(back.rows[1], ['York', '', '', '', '']); // padding for absent cells, as before
  // the persisted JSON (what flattenBack writes) is the input, not a stringified copy
  assert.equal(S.canonical(back.rows[0]), S.canonical(rows[0]));
});

test('an object-valued meta field is merged by key: two editors annotating different rows both keep their note', () => {
  const seed = S.projectToDoc({ columns: [{ name: 'x' }], rows: [['a'], ['b']], notes: { 0: 'first' }, scope: { cc: ['GB'] },
    rowFilters: [{ col: 0, q: 'a' }], title: 'T' });
  const state = Y.encodeStateAsUpdate(seed);
  const alice = new Y.Doc(); Y.applyUpdate(alice, state);
  const bob = new Y.Doc(); Y.applyUpdate(bob, state);
  // nested maps, not whole values
  assert.ok(alice.getMap('meta').get('notes') instanceof Y.Map);
  assert.ok(!(alice.getMap('meta').get('rowFilters') instanceof Y.Map)); // an array stays whole
  assert.equal(alice.getMap('meta').get('title'), 'T');
  alice.getMap('meta').get('notes').set('1', 'from alice');
  bob.getMap('meta').get('notes').set('0', 'bob rewrote the first');
  Y.applyUpdate(alice, Y.encodeStateAsUpdate(bob));
  Y.applyUpdate(bob, Y.encodeStateAsUpdate(alice));
  const a = S.docToProject(alice);
  const b = S.docToProject(bob);
  assert.deepEqual(a.notes, { 0: 'bob rewrote the first', 1: 'from alice' });
  assert.deepEqual(b.notes, a.notes);
  assert.deepEqual(a.scope, { cc: ['GB'] });
  assert.deepEqual(a.rowFilters, [{ col: 0, q: 'a' }]);
  // a whole-value meta written by an OLD client (plain object) still reads back as the object
  alice.getMap('meta').set('flags', { '0:1': true });
  assert.deepEqual(S.docToProject(alice).flags, { '0:1': true });
});

test('canonical JSON ignores key order (jsonb reorders keys) and nothing else', () => {
  assert.equal(S.canonical({ b: [1, { d: null, c: 'x' }], a: 1 }), S.canonical({ a: 1, b: [1, { c: 'x', d: null }] }));
  assert.notEqual(S.canonical({ a: 1 }), S.canonical({ a: '1' }));
  assert.notEqual(S.canonical([1, 2]), S.canonical([2, 1]));
});

// A fake pool that holds one project row and records the writes.
function fakePool(project) {
  const writes = [];
  return {
    writes,
    row: project,
    async query(sql, params) {
      writes.push({ sql, params });
      if (/SELECT snapshot, version FROM workbench_project/.test(sql)) {
        return { rows: project ? [{ snapshot: JSON.parse(JSON.stringify(project.snapshot)), version: project.version }] : [] };
      }
      if (/UPDATE workbench_project/.test(sql)) {
        if (!project) return { rows: [] };
        project.version += 1;
        project.snapshot = JSON.parse(params[1]);
        return { rows: [{ version: project.version }] };
      }
      return { rows: [] };
    },
  };
}

test('a store whose flattened snapshot equals the stored one bumps nothing; a changed one bumps once and tells the clients', async () => {
  const snapshot = { columns: [{ name: 'x' }], rows: [['a', 1]], notes: { 0: 'n' }, matches: {}, decisions: {}, geom: {}, rowTypes: {}, total: 1 };
  // the stored copy with keys in jsonb's order, so only canonical comparison can see it is the same
  const stored = { total: 1, rows: [['a', 1]], notes: { 0: 'n' }, rowTypes: {}, matches: {}, geom: {}, decisions: {}, columns: [{ name: 'x' }] };
  const pool = fakePool({ snapshot: stored, version: 7 });
  const db = S.buildDatabase(pool, S.makeConfig({ HOCUSPOCUS_SECRET: SECRET }));
  const sent = [];
  const document = { broadcastStateless: (p) => sent.push(p) };
  const doc = S.projectToDoc(snapshot);
  await db.configuration.store({ documentName: DOC, state: Y.encodeStateAsUpdate(doc), document });
  assert.equal(pool.row.version, 7, 'an unchanged snapshot bumped the version');
  assert.equal(pool.writes.filter((w) => /INSERT INTO workbench_project_snapshot/.test(w.sql)).length, 0);
  assert.deepEqual(sent.map((s) => JSON.parse(s)), [{ type: S.VERSION_MESSAGE, version: 7 }]);
  // now a real change
  doc.getMap('meta').get('notes').set('0', 'edited');
  await db.configuration.store({ documentName: DOC, state: Y.encodeStateAsUpdate(doc), document });
  assert.equal(pool.row.version, 8);
  assert.deepEqual(pool.row.snapshot.notes, { 0: 'edited' });
  const hist = pool.writes.filter((w) => /INSERT INTO workbench_project_snapshot/.test(w.sql));
  assert.equal(hist.length, 1);
  assert.equal(hist[0].params[1], 8);
  assert.deepEqual(JSON.parse(sent[1]), { type: S.VERSION_MESSAGE, version: 8 });
  // the ydoc state itself is still written every time
  assert.equal(pool.writes.filter((w) => /INSERT INTO workbench_ydoc/.test(w.sql)).length, 2);
});

test('an inactive account is refused on connect, and the query asks the users table', async () => {
  const db = dbWith([{ doc_type: 'reconciliation', role: 'editor', is_active: false }]);
  const hooks = S.buildHooks({ config: S.makeConfig({ HOCUSPOCUS_SECRET: SECRET }), query: db.query });
  await assert.rejects(hooks.onAuthenticate({ documentName: DOC, token: mint(), connection: {}, requestHeaders: OWN, socketId: 's' }),
    /inactive/);
  assert.match(db.calls[0].sql, /is_active/);
  assert.match(db.calls[0].sql, /auth_users/);
  // a row without the column (an old query shape) is not treated as active either
  const hooks2 = S.buildHooks({ config: S.makeConfig({ HOCUSPOCUS_SECRET: SECRET }),
    query: dbWith([{ doc_type: 'reconciliation', role: 'editor' }]).query });
  await assert.rejects(hooks2.onAuthenticate({ documentName: DOC, token: mint(), connection: {}, requestHeaders: OWN, socketId: 's' }),
    /inactive/);
});

test('a long-lived connection is re-authorised: deactivated or removed → closed with the policy code; demoted → read-only', async () => {
  let t = 0;
  let rows = [{ doc_type: 'reconciliation', role: 'editor', is_active: true }];
  const calls = [];
  const query = async (sql, params) => { calls.push(params); return { rows }; };
  const config = { ...S.makeConfig({ HOCUSPOCUS_SECRET: SECRET }), recheckSeconds: 60, docCheckEvery: 1e9 };
  const tracker = new S.ConnectionTracker(config.maxConnectionsPerUser, () => t);
  const hooks = S.buildHooks({ config, query, tracker });
  const connection = { readOnly: false };
  const context = await hooks.onAuthenticate({ documentName: DOC, token: mint(), connection, requestHeaders: OWN, socketId: 's' });
  assert.equal(calls.length, 1);
  const msg = () => hooks.beforeHandleMessage({ socketId: 's', documentName: DOC, update: new Uint8Array(1), document: new Y.Doc(), connection, context });
  t = 59_000; await msg();
  assert.equal(calls.length, 1, 're-checked before the interval');
  t = 60_000; rows = [{ doc_type: 'reconciliation', role: 'viewer', is_active: true }];
  await msg();
  assert.equal(calls.length, 2, 'not re-checked at the interval');
  assert.deepEqual(calls[1], [DOC, 42]);          // by the connection's user, no token involved
  assert.equal(connection.readOnly, true);         // demoted in mid-session
  assert.equal(context.user.role, 'viewer');
  t = 120_000; rows = [{ doc_type: 'reconciliation', role: 'owner', is_active: true }];
  await msg();
  assert.equal(connection.readOnly, false);        // and promoted back
  t = 180_000; rows = [{ doc_type: 'reconciliation', role: 'owner', is_active: false }];
  await assert.rejects(msg(), (e) => e.code === S.CLOSE_POLICY && /inactive/.test(e.reason));
  t = 240_000; rows = [];
  await assert.rejects(msg(), (e) => e.code === S.CLOSE_POLICY && /not a member/.test(e.reason));
  // recheckSeconds 0 = never
  rows = [{ doc_type: 'reconciliation', role: 'editor', is_active: true }];
  const never = S.buildHooks({ config: { ...config, recheckSeconds: 0 }, query, tracker: new S.ConnectionTracker(0, () => t) });
  await never.onAuthenticate({ documentName: DOC, token: mint(), connection: {}, requestHeaders: OWN, socketId: 'n' });
  const before = calls.length;
  t = 1e9;
  await never.beforeHandleMessage({ socketId: 'n', documentName: DOC, update: new Uint8Array(1), document: new Y.Doc(), connection: {}, context: {} });
  assert.equal(calls.length, before);
});

test('a user gets at most N live connections; a disconnect frees a slot; another user is unaffected; 0 = unlimited', async () => {
  const config = { ...S.makeConfig({ HOCUSPOCUS_SECRET: SECRET, HOCUSPOCUS_MAX_CONNECTIONS_PER_USER: '2' }) };
  assert.equal(config.maxConnectionsPerUser, 2);
  const query = async () => ({ rows: [{ doc_type: 'reconciliation', role: 'editor', is_active: true }] });
  const hooks = S.buildHooks({ config, query });
  const auth = (socketId, doc = DOC, sub = '42') => hooks.onAuthenticate({ documentName: doc, token: mint({ sub, project_id: doc }),
    connection: {}, requestHeaders: OWN, socketId });
  await auth('a');
  await auth('b');
  await assert.rejects(auth('c'), /too many live connections/);
  await auth('d', DOC, '43');                      // another user has their own budget
  await auth('a');                                  // the same (socket, document) again is not a new slot
  await hooks.onDisconnect({ socketId: 'a', documentName: DOC });
  await auth('c');                                  // freed
  // a second document on the same socket is another connection
  await assert.rejects(auth('b', OTHER_DOC), /too many/);
  const unlimited = S.buildHooks({ config: { ...config, maxConnectionsPerUser: 0 }, query });
  for (let i = 0; i < 50; i++) {
    await unlimited.onAuthenticate({ documentName: DOC, token: mint(), connection: {}, requestHeaders: OWN, socketId: `s${i}` });
  }
  // a refused authentication (not a member) never occupies a slot
  const strict = S.buildHooks({ config: { ...config, maxConnectionsPerUser: 1 }, query: async () => ({ rows: [] }) });
  await assert.rejects(strict.onAuthenticate({ documentName: DOC, token: mint(), connection: {}, requestHeaders: OWN, socketId: 'x' }), /not a member/);
  const tracker = new S.ConnectionTracker(1);
  assert.equal(tracker.count('42'), 0);
});
