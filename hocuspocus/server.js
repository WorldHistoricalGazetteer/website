// Hocuspocus real-time collaboration server for the WHG Workbench (place#112, Phase 2).
//
// Yjs WebSocket sync for Team-owned WorkbenchProjects. Django (the `workbench` app) mints a
// short-lived JWT (POST /reconciliation/projects/<id>/collab-token/); this service verifies it with
// the shared HOCUSPOCUS_SECRET and enforces the role (viewer → read-only). Document state persists
// to Postgres (table `workbench_ydoc`, schema owned by Django) and is flattened back into
// `workbench_project.snapshot` so the REST/snapshot path stays authoritative for non-realtime
// clients, export and publishing.
//
// Hardening (place#314), each a separately testable piece below:
//   * the token must carry `iss`, `aud`, `exp`, `sub` and `project_id`; the service verifies all of
//     them, so a token minted for another purpose, or for another document, is useless here;
//   * the token is only a ticket: on connect the service re-reads membership, role and doc_type
//     from the database, so a user removed from the team since the mint (≤120 s ago) is refused,
//     and the role used is the database's, not the token's;
//   * the websocket upgrade is refused unless the request's Origin is this site's own origin (from
//     the Host header nginx forwards) or one in HOCUSPOCUS_ALLOWED_ORIGINS — a page on another
//     origin cannot even open the socket, let alone present a token;
//   * doc_types with live editing off (the opaque `plato` type) never get a connection, whatever
//     the token says;
//   * per-message size (ws maxPayload), per-connection message and byte rates, and a cap on the
//     encoded document all close the connection with a policy-violation code.
//
// Follow-ups (place#314, the "remaining" list), each testable below too:
//   * the Yjs mapping keeps cell types (a number, boolean or null round-trips as itself; only an
//     ABSENT cell reads back as '') and stores an object-valued `meta` field as a nested Y.Map, so
//     two editors annotating different keys of `notes` or `flags` both keep their edit;
//   * a store whose flattened snapshot equals the stored one bumps nothing, and every store tells
//     the live clients the resulting version (a stateless message), so a client that drops back to
//     REST pushes with a current base_version;
//   * `is_active` is part of the authorisation query, and the whole authorisation is re-run every
//     HOCUSPOCUS_RECHECK_SECONDS of activity on a connection: a user deactivated or removed in
//     mid-session is closed, a demoted one becomes read-only;
//   * at most HOCUSPOCUS_MAX_CONNECTIONS_PER_USER live connections per user.
//
// All limits have defaults; nothing new is required in the environment. The file is a module as
// well as a script (`require.main === module` starts the server), so `node --test test/` can
// exercise the pieces without a database or a socket.

const { Server } = require('@hocuspocus/server');
const { Database } = require('@hocuspocus/extension-database');
const { Pool } = require('pg');
const jwt = require('jsonwebtoken');
const Y = require('yjs');

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

// Must match workbench/views.py collab_token.
const JWT_ISSUER = 'whg-workbench';
const JWT_AUDIENCE = 'whg-hocuspocus';
const JWT_MAX_TTL_SECONDS = 300; // a token claiming to live longer than this is refused outright

// Must match the registry in workbench/doctypes.py (`live=True`).
const DEFAULT_LIVE_DOC_TYPES = ['reconciliation', 'place_collection', 'itinerary', 'gazetteer_group',
  'place_record', 'dataset_edit'];

const CLOSE_POLICY = 1008; // RFC 6455 "policy violation"

function intEnv(env, name, fallback) {
  const v = parseInt(env[name] || '', 10);
  return Number.isFinite(v) && v >= 0 ? v : fallback;
}

function listEnv(env, name, fallback) {
  const raw = (env[name] || '').split(',').map((s) => s.trim()).filter(Boolean);
  return raw.length ? raw : fallback;
}

function makeConfig(env = process.env) {
  return {
    secret: env.HOCUSPOCUS_SECRET || '',
    port: intEnv(env, 'HOCUSPOCUS_PORT', 8010),
    allowedOrigins: listEnv(env, 'HOCUSPOCUS_ALLOWED_ORIGINS', []),
    liveDocTypes: listEnv(env, 'HOCUSPOCUS_LIVE_DOC_TYPES', DEFAULT_LIVE_DOC_TYPES),
    maxMessageBytes: intEnv(env, 'HOCUSPOCUS_MAX_MESSAGE_BYTES', 1024 * 1024),      // one ws frame
    maxDocBytes: intEnv(env, 'HOCUSPOCUS_MAX_DOC_BYTES', 8 * 1024 * 1024),          // encoded Y state
    messageRate: intEnv(env, 'HOCUSPOCUS_MESSAGE_RATE', 600),                       // per connection per minute
    byteRate: intEnv(env, 'HOCUSPOCUS_BYTE_RATE', 8 * 1024 * 1024),                 // per connection per minute
    docCheckEvery: intEnv(env, 'HOCUSPOCUS_DOC_CHECK_BYTES', 256 * 1024),           // re-measure the doc after this much input
    maxConnectionsPerUser: intEnv(env, 'HOCUSPOCUS_MAX_CONNECTIONS_PER_USER', 8),   // 0 = unlimited
    recheckSeconds: intEnv(env, 'HOCUSPOCUS_RECHECK_SECONDS', 60),                  // re-authorise an active connection this often; 0 = never
  };
}

// ── origin ────────────────────────────────────────────────────────────────────
// The site's own origin, as nginx presents it: `Host $host` and `X-Forwarded-Proto $scheme`.
function ownOrigin(headers) {
  const host = headers.host || headers.Host;
  if (!host) return null;
  const proto = (headers['x-forwarded-proto'] || 'https').split(',')[0].trim();
  return `${proto}://${host}`.toLowerCase();
}

// A browser always sends Origin on a WebSocket handshake; its absence is a non-browser client,
// which has no business here (the only client is the Workbench page).
function originAllowed(headers, allowedOrigins = []) {
  const origin = (headers.origin || headers.Origin || '').trim().toLowerCase();
  if (!origin) return false;
  const own = ownOrigin(headers);
  if (own && origin === own) return true;
  return allowedOrigins.map((o) => o.toLowerCase()).includes(origin);
}

// ── token ──────────────────────────────────────────────────────────────────────
// Verify the Django-minted ticket. Returns the payload or throws an Error naming the reason.
function verifyCollabToken(token, secret, documentName, now = Math.floor(Date.now() / 1000)) {
  if (!secret) throw new Error('no secret configured');
  let payload;
  try {
    payload = jwt.verify(token, secret, {
      algorithms: ['HS256'],
      issuer: JWT_ISSUER,
      audience: JWT_AUDIENCE,
      clockTimestamp: now,
    });
  } catch (err) {
    throw new Error(`invalid or expired collab token (${err.message})`);
  }
  if (typeof payload !== 'object' || payload === null) throw new Error('malformed token');
  if (!Number.isFinite(payload.exp) || !Number.isFinite(payload.iat)) throw new Error('token has no exp/iat');
  if (payload.exp - payload.iat > JWT_MAX_TTL_SECONDS) throw new Error('token lifetime too long');
  if (!payload.sub || !/^\d+$/.test(String(payload.sub))) throw new Error('token has no subject');
  if (payload.project_id !== documentName) throw new Error('token/document mismatch');
  if (!UUID_RE.test(documentName)) throw new Error('bad document name');
  return payload;
}

// ── authorisation (re-checked on connect, and again periodically) ──────────────
// `query(sql, params)` is the pg pool's query (injected for tests). Returns {role, docType}.
// The account must be active (`auth_users.is_active`): Django refuses the mint for an inactive
// account, but a token lives 120 s and a connection lives for hours, so the service checks too.
async function authorise(query, documentName, userId, liveDocTypes) {
  const r = await query(
    `SELECT p.doc_type AS doc_type, tm.role AS role, u.is_active AS is_active
       FROM workbench_project p
       JOIN workbench_team_member tm ON tm.team_id = p.team_id
       JOIN auth_users u ON u.id = tm.user_id
      WHERE p.id = $1 AND tm.user_id = $2`,
    [documentName, parseInt(String(userId), 10)]
  );
  const row = r.rows && r.rows[0];
  if (!row) throw new Error('not a member of this project’s team');
  if (row.is_active !== true) throw new Error('account is inactive');
  if (!liveDocTypes.includes(row.doc_type)) throw new Error(`live editing is disabled for doc_type ${row.doc_type}`);
  if (!['owner', 'editor', 'viewer'].includes(row.role)) throw new Error('unknown role');
  return { role: row.role, docType: row.doc_type };
}

// ── live connections per user ──────────────────────────────────────────────────
// One entry per (socket, document): Hocuspocus multiplexes documents over a socket and runs
// onAuthenticate/onDisconnect once per document. Remembers who the connection belongs to and
// when it was last authorised, so beforeHandleMessage can re-check without a token.
class ConnectionTracker {
  constructor(maxPerUser, now = Date.now) {
    this.maxPerUser = maxPerUser;
    this.now = now;
    this.byKey = new Map();   // key → {userId, checkedAt}
    this.byUser = new Map();  // userId → Set<key>
  }

  static key(socketId, documentName) { return `${socketId}|${documentName}`; }

  count(userId) { const s = this.byUser.get(String(userId)); return s ? s.size : 0; }

  // Register a connection; false (and nothing registered) when the user is at the cap.
  open(key, userId) {
    const uid = String(userId);
    if (this.byKey.has(key)) return true; // the same (socket, document) authenticating again
    if (this.maxPerUser && this.count(uid) >= this.maxPerUser) return false;
    this.byKey.set(key, { userId: uid, checkedAt: this.now() });
    if (!this.byUser.has(uid)) this.byUser.set(uid, new Set());
    this.byUser.get(uid).add(key);
    return true;
  }

  get(key) { return this.byKey.get(key) || null; }

  // True once `everyMs` has passed since the last authorisation of this connection; resets the clock.
  dueRecheck(key, everyMs) {
    const e = this.byKey.get(key);
    if (!e || !everyMs) return false;
    const t = this.now();
    if (t - e.checkedAt < everyMs) return false;
    e.checkedAt = t;
    return true;
  }

  close(key) {
    const e = this.byKey.get(key);
    if (!e) return;
    this.byKey.delete(key);
    const s = this.byUser.get(e.userId);
    if (s) { s.delete(key); if (!s.size) this.byUser.delete(e.userId); }
  }
}

// ── per-connection budgets ─────────────────────────────────────────────────────
class WindowCounter {
  constructor(config, now = Date.now) {
    this.config = config;
    this.now = now;
    this.state = new Map(); // socketId → {windowStart, messages, bytes, sinceDocCheck}
  }

  // Charge one message of `bytes`. Returns null when within budget, else the reason string.
  charge(socketId, bytes) {
    const t = this.now();
    let s = this.state.get(socketId);
    if (!s || t - s.windowStart >= 60000) {
      s = { windowStart: t, messages: 0, bytes: 0, sinceDocCheck: s ? s.sinceDocCheck : 0 };
      this.state.set(socketId, s);
    }
    s.messages += 1;
    s.bytes += bytes;
    s.sinceDocCheck += bytes;
    if (this.config.messageRate && s.messages > this.config.messageRate) return 'message rate exceeded';
    if (this.config.byteRate && s.bytes > this.config.byteRate) return 'byte rate exceeded';
    return null;
  }

  // True when enough input has arrived since the last document measurement.
  dueDocCheck(socketId) {
    const s = this.state.get(socketId);
    if (!s) return false;
    if (s.sinceDocCheck < this.config.docCheckEvery) return false;
    s.sinceDocCheck = 0;
    return true;
  }

  forget(socketId) {
    this.state.delete(socketId);
  }
}

function policyError(reason) {
  const e = new Error(reason);
  e.code = CLOSE_POLICY;
  e.reason = reason;
  return e;
}

// ── hooks ──────────────────────────────────────────────────────────────────────
// `deps`: {config, query, counter?, tracker?, encodedSize?}. Returned hooks are what Server.configure gets.
function buildHooks(deps) {
  const { config, query } = deps;
  const counter = deps.counter || new WindowCounter(config);
  const tracker = deps.tracker || new ConnectionTracker(config.maxConnectionsPerUser);
  const encodedSize = deps.encodedSize || ((doc) => Y.encodeStateAsUpdate(doc).byteLength);
  const recheckMs = (config.recheckSeconds || 0) * 1000;

  return {
    // HTTP level: a disallowed Origin never becomes a websocket. Rejecting with no error stops
    // Hocuspocus's default upgrade without an unhandled rejection (its catch ignores a falsy one).
    async onUpgrade({ request, socket }) {
      if (originAllowed(request.headers, config.allowedOrigins)) return;
      console.warn('[hocuspocus] refused upgrade from origin', request.headers.origin || '(none)');
      try {
        socket.write('HTTP/1.1 403 Forbidden\r\nConnection: close\r\nContent-Length: 0\r\n\r\n');
      } finally {
        socket.destroy();
      }
      throw undefined; // eslint-disable-line no-throw-literal
    },

    // The client connects with documentName = project uuid and the Django-minted JWT as its token.
    async onAuthenticate({ documentName, token, connection, requestHeaders, socketId }) {
      if (!originAllowed(requestHeaders || {}, config.allowedOrigins)) throw new Error('origin not allowed');
      const payload = verifyCollabToken(token, config.secret, documentName);
      const { role, docType } = await authorise(query, documentName, payload.sub, config.liveDocTypes);
      // Counted only once authorised, so a refused attempt never occupies a slot.
      if (!tracker.open(ConnectionTracker.key(socketId, documentName), payload.sub)) {
        throw new Error(`too many live connections for this user (limit ${config.maxConnectionsPerUser})`);
      }
      if (role === 'viewer') connection.readOnly = true; // the DATABASE role, not the token's
      return { user: { id: payload.sub, name: payload.name || '', role, docType } };
    },

    async beforeHandleMessage({ socketId, documentName, update, document, connection, context }) {
      const bytes = update ? update.byteLength : 0;
      const over = counter.charge(socketId, bytes);
      if (over) throw policyError(over);
      if (counter.dueDocCheck(socketId) && document && config.maxDocBytes) {
        const size = encodedSize(document);
        if (size > config.maxDocBytes) throw policyError(`document too large (${size} > ${config.maxDocBytes} bytes)`);
      }
      // Re-authorise a long-lived connection: the token was a ticket for the handshake only.
      const key = ConnectionTracker.key(socketId, documentName);
      if (tracker.dueRecheck(key, recheckMs)) {
        const who = tracker.get(key);
        let role;
        try {
          ({ role } = await authorise(query, documentName, who.userId, config.liveDocTypes));
        } catch (err) {
          throw policyError(`no longer authorised: ${err.message}`);
        }
        if (connection) connection.readOnly = role === 'viewer';
        if (context && context.user) context.user.role = role;
      }
    },

    async onDisconnect({ socketId, documentName }) {
      counter.forget(socketId);
      tracker.close(ConnectionTracker.key(socketId, documentName));
    },
  };
}

// ── document mapping ───────────────────────────────────────────────────────────
// MUST match whg/webpack/js/recon-collab-rt.js exactly:
//   rows = Y.Array<Y.Map> (a Y.Map per row, keyed by column index "0","1",… → the cell's JSON value,
//          as given: a number stays a number, null stays null; only an ABSENT cell reads back as '')
//   columns = Y.Array<Y.Map>; decisions|matches|geom|rowTypes = Y.Map
//   meta = Y.Map (everything else); an object-valued field (notes, flags, excludedRows, scope, …)
//          is a nested Y.Map keyed like the object, so concurrent edits to different keys both
//          survive; a scalar or an array is stored whole.
const KEYED = ['matches', 'decisions', 'geom', 'rowTypes'];
const META_EXCLUDE = new Set([
  'rows', 'columns', 'matches', 'decisions', 'geom', 'rowTypes', 'id',
  'serverId', 'serverVersion', 'role', 'teamId', 'teamTitle', 'teamPersonal', 'sharedToken', 'sharedUrl',
]);

const isPlainObject = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);
// JSON has no undefined: a cell or field that is undefined is stored as null.
const jsonValue = (v) => (v === undefined ? null : v);

function objToMap(obj) {
  const m = new Y.Map();
  for (const [k, v] of Object.entries(obj)) m.set(k, jsonValue(v));
  return m;
}

// A value read from a Y.Map: a nested Y.Map comes back as the plain object it stands for.
function fromY(v) {
  return v instanceof Y.Map ? v.toJSON() : v;
}

function projectToDoc(snapshot) {
  const doc = new Y.Doc();
  const s = snapshot || {};
  doc.transact(() => {
    const meta = doc.getMap('meta');
    for (const k of Object.keys(s)) {
      if (META_EXCLUDE.has(k)) continue;
      meta.set(k, isPlainObject(s[k]) ? objToMap(s[k]) : jsonValue(s[k]));
    }
    const yCols = doc.getArray('columns');
    (s.columns || []).forEach((col) => yCols.push([objToMap(col || {})]));
    const yRows = doc.getArray('rows');
    (s.rows || []).forEach((row) => {
      const m = new Y.Map();
      (row || []).forEach((v, j) => m.set(String(j), jsonValue(v)));
      yRows.push([m]);
    });
    for (const kk of KEYED) {
      const ym = doc.getMap(kk);
      for (const [key, val] of Object.entries(s[kk] || {})) ym.set(key, jsonValue(val));
    }
  });
  return doc;
}

function docToProject(doc) {
  const p = {};
  doc.getMap('meta').forEach((v, k) => { p[k] = fromY(v); });
  p.columns = doc.getArray('columns').toArray().map((m) => {
    const o = {};
    if (m instanceof Y.Map) m.forEach((v, k) => { o[k] = fromY(v); });
    return o;
  });
  const ncols = p.columns.length;
  p.rows = doc.getArray('rows').toArray().map((m) => {
    if (!(m instanceof Y.Map)) return [];
    let max = ncols - 1;
    m.forEach((v, k) => { const j = Number(k); if (j > max) max = j; });
    const arr = [];
    for (let j = 0; j <= max; j++) { const v = m.get(String(j)); arr.push(v === undefined ? '' : fromY(v)); }
    return arr;
  });
  p.total = p.rows.length;
  for (const kk of KEYED) { p[kk] = {}; doc.getMap(kk).forEach((v, k) => { p[kk][k] = fromY(v); }); }
  return p;
}

// JSON with keys sorted at every level: Postgres's jsonb reorders keys, so this is the only way to
// compare a snapshot read back from the database with one just flattened.
function canonical(v) {
  if (Array.isArray(v)) return `[${v.map(canonical).join(',')}]`;
  if (isPlainObject(v)) return `{${Object.keys(v).sort().map((k) => `${JSON.stringify(k)}:${canonical(v[k])}`).join(',')}}`;
  return JSON.stringify(v === undefined ? null : v);
}

// Tells every live client of the document the version its last store produced.
const VERSION_MESSAGE = 'version';
function versionMessage(version) {
  return JSON.stringify({ type: VERSION_MESSAGE, version });
}

// ── persistence ────────────────────────────────────────────────────────────────
function buildDatabase(pool, config) {
  // Seed a fresh Yjs update from the project's stored snapshot (first-time open).
  async function seedFromSnapshot(documentName) {
    const r = await pool.query('SELECT snapshot, doc_type FROM workbench_project WHERE id = $1', [documentName]);
    if (!r.rows[0]) return null;
    if (!config.liveDocTypes.includes(r.rows[0].doc_type)) return null; // never materialise an opaque doc
    const snapshot = r.rows[0].snapshot || {};
    return Buffer.from(Y.encodeStateAsUpdate(projectToDoc(snapshot)));
  }

  // Flatten the whole live Yjs doc back into the canonical snapshot + write a history row.
  // Returns the project's version afterwards (null if the project is gone). A store whose
  // flattened snapshot equals what is stored (a reconnect, an edit undone before the debounce,
  // an identical concurrent edit) bumps nothing: every bump makes every REST client stale and
  // costs one of the 25 history rows that make their merge possible.
  async function flattenBack(documentName, state) {
    const doc = new Y.Doc();
    Y.applyUpdate(doc, state);
    const snapshot = docToProject(doc);
    const cur = await pool.query('SELECT snapshot, version FROM workbench_project WHERE id = $1', [documentName]);
    if (!cur.rows[0]) return null;
    if (canonical(cur.rows[0].snapshot) === canonical(snapshot)) return cur.rows[0].version;
    const upd = await pool.query(
      `UPDATE workbench_project
         SET snapshot = $2::jsonb, version = version + 1, updated = now()
       WHERE id = $1
       RETURNING version`,
      [documentName, JSON.stringify(snapshot)]
    );
    if (!upd.rows[0]) return null;
    await pool.query(
      `INSERT INTO workbench_project_snapshot (project_id, version, snapshot, created)
       VALUES ($1, $2, $3::jsonb, now())
       ON CONFLICT (project_id, version) DO NOTHING`,
      [documentName, upd.rows[0].version, JSON.stringify(snapshot)]
    );
    return upd.rows[0].version;
  }

  return new Database({
    fetch: async ({ documentName }) => {
      if (!UUID_RE.test(documentName)) return null;
      try {
        const r = await pool.query('SELECT state FROM workbench_ydoc WHERE project_id = $1', [documentName]);
        if (r.rows[0] && r.rows[0].state) return r.rows[0].state; // Buffer (bytea)
        return await seedFromSnapshot(documentName);
      } catch (err) {
        console.error('[hocuspocus] fetch failed', documentName, err.message);
        return null;
      }
    },
    store: async ({ documentName, state, document }) => {
      if (!UUID_RE.test(documentName)) return;
      const buf = Buffer.from(state);
      if (config.maxDocBytes && buf.byteLength > config.maxDocBytes) {
        // The connection that grew it past the cap has been closed by beforeHandleMessage; the
        // last state within the cap stays on disk.
        console.error('[hocuspocus] refusing to store oversize document', documentName, buf.byteLength);
        return;
      }
      try {
        await pool.query(
          `INSERT INTO workbench_ydoc (project_id, state, updated) VALUES ($1, $2, now())
           ON CONFLICT (project_id) DO UPDATE SET state = EXCLUDED.state, updated = now()`,
          [documentName, buf]
        );
        const version = await flattenBack(documentName, buf);
        // Live clients learn the version so that one dropping back to REST is not stale.
        if (version != null && document && typeof document.broadcastStateless === 'function') {
          document.broadcastStateless(versionMessage(version));
        }
      } catch (err) {
        console.error('[hocuspocus] store failed', documentName, err.message);
      }
    },
  });
}

// ── main ───────────────────────────────────────────────────────────────────────
function main() {
  const config = makeConfig(process.env);
  if (!config.secret) {
    console.error('[hocuspocus] HOCUSPOCUS_SECRET is not set — refusing to start.');
    process.exit(1);
  }

  const pool = new Pool({
    host: process.env.DB_HOST,
    port: parseInt(process.env.DB_PORT_INTERNAL || '5432', 10),
    database: process.env.DB_NAME,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    max: 8,
  });

  const hooks = buildHooks({ config, query: (sql, params) => pool.query(sql, params) });

  const server = Server.configure({
    name: 'whg-workbench',
    port: config.port,
    address: '0.0.0.0',
    extensions: [buildDatabase(pool, config)],
    ...hooks,
  });

  // The third argument reaches `new ws.WebSocketServer({noServer: true, ...})`: one frame larger
  // than maxPayload closes the socket with 1009 before any hook sees it.
  server.listen(null, null, { maxPayload: config.maxMessageBytes }).then(() => {
    console.log(`[hocuspocus] listening on ${config.port}; origins: own host`
      + (config.allowedOrigins.length ? ` + ${config.allowedOrigins.join(', ')}` : '')
      + `; live doc_types: ${config.liveDocTypes.join(', ')}`
      + `; per-user cap ${config.maxConnectionsPerUser || 'none'}; re-check every ${config.recheckSeconds || 'never'} s`);
  });

  process.on('SIGTERM', () => { server.destroy(); pool.end(); process.exit(0); });
  process.on('SIGINT', () => { server.destroy(); pool.end(); process.exit(0); });
}

module.exports = {
  JWT_ISSUER, JWT_AUDIENCE, JWT_MAX_TTL_SECONDS, DEFAULT_LIVE_DOC_TYPES, CLOSE_POLICY, VERSION_MESSAGE,
  makeConfig, ownOrigin, originAllowed, verifyCollabToken, authorise, WindowCounter, ConnectionTracker,
  buildHooks, projectToDoc, docToProject, canonical, versionMessage, buildDatabase,
};

if (require.main === module) main();
