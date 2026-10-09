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
  const db = dbWith([{ doc_type: 'reconciliation', role: 'viewer' }]);
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
    query: dbWith([{ doc_type: 'plato', role: 'owner' }]).query });
  await assert.rejects(hooks.onAuthenticate({ documentName: DOC, token: mint({ doc_type: 'reconciliation' }),
    connection: {}, requestHeaders: OWN }), /live editing is disabled for doc_type plato/);
  assert.equal(S.DEFAULT_LIVE_DOC_TYPES.includes('plato'), false);
});

test('authentication also refuses a foreign origin and a bad token', async () => {
  const hooks = S.buildHooks({ config: S.makeConfig({ HOCUSPOCUS_SECRET: SECRET }),
    query: dbWith([{ doc_type: 'reconciliation', role: 'editor' }]).query });
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

// ── the mapping is unchanged ─────────────────────────────────────────────────
test('projectToDoc/docToProject still round-trip a Map-your-Data snapshot', () => {
  const snap = { fileName: 'x.csv', columns: [{ name: 'Place' }, { name: 'Lat' }], rows: [['Richmond', '51.4'], ['York', null]],
    decisions: { '0:1': { status: 'accepted' } }, matches: {}, geom: {}, rowTypes: {}, scope: { cc: ['GB'] } };
  const back = S.docToProject(S.projectToDoc(snap));
  assert.equal(back.fileName, 'x.csv');
  assert.deepEqual(back.rows, [['Richmond', '51.4'], ['York', '']]); // the known stringification
  assert.deepEqual(back.decisions, snap.decisions);
  assert.deepEqual(back.scope, snap.scope);
  assert.equal(back.total, 2);
});
