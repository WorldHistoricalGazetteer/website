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

// Build a seed Yjs update from the project's stored snapshot.decisions (first-time open).
async function seedFromSnapshot(documentName) {
  const r = await pool.query('SELECT snapshot FROM workbench_project WHERE id = $1', [documentName]);
  const decisions = (r.rows[0] && r.rows[0].snapshot && r.rows[0].snapshot.decisions) || {};
  const doc = new Y.Doc();
  const map = doc.getMap('decisions');
  for (const [k, v] of Object.entries(decisions)) map.set(k, v);
  return Buffer.from(Y.encodeStateAsUpdate(doc));
}

// Flatten the live Yjs doc's `decisions` map back into the canonical snapshot + write a history row.
async function flattenBack(documentName, state) {
  const doc = new Y.Doc();
  Y.applyUpdate(doc, state);
  const decisions = doc.getMap('decisions').toJSON();
  const upd = await pool.query(
    `UPDATE workbench_project
       SET snapshot = jsonb_set(coalesce(snapshot, '{}'::jsonb), '{decisions}', $2::jsonb, true),
           version = version + 1,
           updated = now()
     WHERE id = $1
     RETURNING version, snapshot`,
    [documentName, JSON.stringify(decisions)]
  );
  if (upd.rows[0]) {
    await pool.query(
      `INSERT INTO workbench_project_snapshot (project_id, version, snapshot, created)
       VALUES ($1, $2, $3, now())
       ON CONFLICT (project_id, version) DO NOTHING`,
      [documentName, upd.rows[0].version, upd.rows[0].snapshot]
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
