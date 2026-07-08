// Hocuspocus real-time collaboration server for the WHG Workbench (place#112, Phase 2).
//
// Yjs WebSocket sync for Team-owned WorkbenchProjects. Django (the `workbench` app) mints a
// short-lived JWT (POST /reconciliation/projects/<id>/collab-token/); this service verifies it with
// the shared HOCUSPOCUS_SECRET and enforces the role (viewer → read-only). Document state persists
// to Postgres (table `workbench_ydoc`, schema owned by Django) and is flattened back into
// `workbench_project.snapshot` so the REST/snapshot path stays authoritative for non-realtime
// clients, export and publishing.
//
// Pass-1 spike scope: syncs only the `decisions` map (proves token→auth→sync→persist→flatten).
// Pass 2 widens the mapping to rows/matches/scope/… and adds presence.

const { Server } = require('@hocuspocus/server');
const { Database } = require('@hocuspocus/extension-database');
const { Pool } = require('pg');
const jwt = require('jsonwebtoken');
const Y = require('yjs');

const SECRET = process.env.HOCUSPOCUS_SECRET || '';
const PORT = parseInt(process.env.HOCUSPOCUS_PORT || '8010', 10);
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

if (!SECRET) {
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

// Doc mapping — MUST match whg/webpack/js/recon-collab-rt.js exactly:
//   rows = Y.Array<Y.Map> (a Y.Map per row, keyed by column index "0","1",…)
//   columns = Y.Array<Y.Map>; decisions|matches|geom|rowTypes = Y.Map; meta = Y.Map (everything else)
const KEYED = ['matches', 'decisions', 'geom', 'rowTypes'];
const META_EXCLUDE = new Set([
  'rows', 'columns', 'matches', 'decisions', 'geom', 'rowTypes', 'id',
  'serverId', 'serverVersion', 'role', 'teamId', 'teamTitle', 'teamPersonal', 'sharedToken', 'sharedUrl',
]);

function projectToDoc(snapshot) {
  const doc = new Y.Doc();
  const s = snapshot || {};
  doc.transact(() => {
    const meta = doc.getMap('meta');
    for (const k of Object.keys(s)) if (!META_EXCLUDE.has(k)) meta.set(k, s[k]);
    const yCols = doc.getArray('columns');
    (s.columns || []).forEach((col) => {
      const m = new Y.Map();
      for (const [k, v] of Object.entries(col || {})) m.set(k, v);
      yCols.push([m]);
    });
    const yRows = doc.getArray('rows');
    (s.rows || []).forEach((row) => {
      const m = new Y.Map();
      (row || []).forEach((v, j) => m.set(String(j), String(v == null ? '' : v)));
      yRows.push([m]);
    });
    for (const kk of KEYED) {
      const ym = doc.getMap(kk);
      for (const [key, val] of Object.entries(s[kk] || {})) ym.set(key, val);
    }
  });
  return doc;
}

function docToProject(doc) {
  const p = {};
  doc.getMap('meta').forEach((v, k) => { p[k] = v; });
  p.columns = doc.getArray('columns').toArray().map((m) => {
    const o = {};
    if (m instanceof Y.Map) m.forEach((v, k) => { o[k] = v; });
    return o;
  });
  const ncols = p.columns.length;
  p.rows = doc.getArray('rows').toArray().map((m) => {
    if (!(m instanceof Y.Map)) return [];
    let max = ncols - 1;
    m.forEach((v, k) => { const j = Number(k); if (j > max) max = j; });
    const arr = [];
    for (let j = 0; j <= max; j++) { const v = m.get(String(j)); arr.push(v == null ? '' : v); }
    return arr;
  });
  p.total = p.rows.length;
  for (const kk of KEYED) { p[kk] = {}; doc.getMap(kk).forEach((v, k) => { p[kk][k] = v; }); }
  return p;
}

// Seed a fresh Yjs update from the project's stored snapshot (first-time open).
async function seedFromSnapshot(documentName) {
  const r = await pool.query('SELECT snapshot FROM workbench_project WHERE id = $1', [documentName]);
  const snapshot = (r.rows[0] && r.rows[0].snapshot) || {};
  return Buffer.from(Y.encodeStateAsUpdate(projectToDoc(snapshot)));
}

// Flatten the whole live Yjs doc back into the canonical snapshot + write a history row.
async function flattenBack(documentName, state) {
  const doc = new Y.Doc();
  Y.applyUpdate(doc, state);
  const snapshot = docToProject(doc);
  const upd = await pool.query(
    `UPDATE workbench_project
       SET snapshot = $2::jsonb, version = version + 1, updated = now()
     WHERE id = $1
     RETURNING version`,
    [documentName, JSON.stringify(snapshot)]
  );
  if (upd.rows[0]) {
    await pool.query(
      `INSERT INTO workbench_project_snapshot (project_id, version, snapshot, created)
       VALUES ($1, $2, $3::jsonb, now())
       ON CONFLICT (project_id, version) DO NOTHING`,
      [documentName, upd.rows[0].version, JSON.stringify(snapshot)]
    );
  }
}

const database = new Database({
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
  store: async ({ documentName, state }) => {
    if (!UUID_RE.test(documentName)) return;
    const buf = Buffer.from(state);
    try {
      await pool.query(
        `INSERT INTO workbench_ydoc (project_id, state, updated) VALUES ($1, $2, now())
         ON CONFLICT (project_id) DO UPDATE SET state = EXCLUDED.state, updated = now()`,
        [documentName, buf]
      );
      await flattenBack(documentName, buf);
    } catch (err) {
      console.error('[hocuspocus] store failed', documentName, err.message);
    }
  },
});

const server = Server.configure({
  name: 'whg-workbench',
  port: PORT,
  address: '0.0.0.0',
  extensions: [database],

  // The client connects with documentName = project uuid and the Django-minted JWT as its token.
  async onAuthenticate({ documentName, token, connection }) {
    let payload;
    try {
      payload = jwt.verify(token, SECRET, { algorithms: ['HS256'] });
    } catch (err) {
      throw new Error('invalid or expired collab token');
    }
    if (payload.project_id !== documentName) throw new Error('token/document mismatch');
    if (payload.role === 'viewer') connection.readOnly = true; // viewers cannot write
    return { user: { id: payload.sub, name: payload.name, role: payload.role } };
  },
});

server.listen().then(() => {
  console.log(`[hocuspocus] listening on ${PORT}`);
});

process.on('SIGTERM', () => { server.destroy(); pool.end(); process.exit(0); });
process.on('SIGINT', () => { server.destroy(); pool.end(); process.exit(0); });
