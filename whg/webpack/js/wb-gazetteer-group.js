// wb-gazetteer-group.js — Collaborative Workbench editor for a Gazetteer Group (doc-type #4).
// plan-collaborativeCollections §4.2: an aggregation of published gazetteers (datasets) for
// comparison/analysis. Was "Dataset Collection"; publishes into Collection(collection_class='dataset')
// + the datasets M2M via the beta-gated publish endpoint. Members are DATASETS (not places), so this
// editor uses the shared shell (wb-shell) but its own member logic + the dataset-search endpoint. It
// reuses the shared wb_collection_editor.html DOM (same element ids).

import '../css/reconciliation.css';
import { el, esc, truncate, debounce, statusBadge, openStore, serverBridge, mountAccordion, haversineKm, centroid } from './wb-shell.js';
import { searchDatasets } from './recon-sync.js';
import { mountCollab } from './wb-collab.js';

// eslint-disable-next-line camelcase, no-undef
__webpack_public_path__ = '/static/webpack/';

const DB_NAME = 'whg-wb-gazetteer-group';
const store = openStore(DB_NAME);
const bridge = serverBridge('gazetteer_group');
let status = null;

// Members: {dataset_id, title}. Snapshot matches doctypes._errs_gazetteer_group / publish_gazetteer_group.
let project = { title: '', description: '', keywords: [], gazetteers: [] };

const snapshot = () => ({
  title: project.title.trim() || 'Untitled group',
  description: project.description || '',
  keywords: project.keywords || [],
  gazetteers: project.gazetteers.map((g) => ({ dataset_id: g.dataset_id, title: g.title })),
});

const saveLocal = debounce(async () => {
  try { await store.save(project); status.saved(); } catch (e) { status.error('Could not save locally'); }
  if (bridge.id) pushServer();
}, 600);
const pushServer = debounce(async () => {
  const r = await bridge.push(snapshot());
  if (r.ok) status.synced();
  else if (r.status === 409) status.error('Edited elsewhere — reload to merge');
  else status.error('Could not save to your account');
}, 1500);
const touched = () => { status.saving(); saveLocal(); };

// ── metadata ────────────────────────────────────────────────────────────────
function bindMeta() {
  el('wb-title').addEventListener('input', (e) => { project.title = e.target.value; touched(); });
  el('wb-desc').addEventListener('input', (e) => { project.description = e.target.value; touched(); });
  el('wb-keywords').addEventListener('input', (e) => {
    project.keywords = e.target.value.split(',').map((k) => k.trim()).filter(Boolean); touched();
  });
}
function fillMeta() {
  el('wb-title').value = project.title || '';
  el('wb-desc').value = project.description || '';
  el('wb-keywords').value = (project.keywords || []).join(', ');
}

// ── search + add gazetteers ───────────────────────────────────────────────────
async function search() {
  const box = el('wb-search-results');
  const q = (el('wb-search').value || '').trim();
  if (!q) { box.innerHTML = ''; return; }
  box.innerHTML = '<span class="text-muted small"><i class="fas fa-spinner fa-spin me-1"></i>searching…</span>';
  const r = await searchDatasets(q);
  if (!r.ok) { box.innerHTML = `<span class="text-danger small">Search failed.</span>`; return; }
  const results = (r.data && r.data.datasets) || [];
  if (!results.length) { box.innerHTML = '<span class="text-muted small">No published gazetteers found.</span>'; return; }
  // Colocation: rank suggestions by proximity of their bbox centroid to the centroid of already-added
  // gazetteers (coarse, but useful for regional groupings). Datasets without a bbox fall to the end.
  const anchor = centroid(project.gazetteers.map((g) => (g.centroid || null)));
  if (anchor) results.sort((a, b) => haversineKm(a.centroid, anchor) - haversineKm(b.centroid, anchor));
  const note = anchor ? '<div class="text-muted small mb-1"><i class="fas fa-location-crosshairs me-1"></i>ranked by nearness to your other gazetteers</div>' : '';
  box.innerHTML = note + results.map((d, i) => `<button type="button" class="btn btn-sm btn-outline-secondary text-start d-block w-100 mb-1 wb-hit" data-i="${i}" title="${esc(d.description || '')}">
      <span class="fw-semibold">${esc(truncate(d.title, 52))}</span>
      ${d.description ? `<span class="text-muted small ms-1">${esc(truncate(d.description, 40))}</span>` : ''}
    </button>`).join('');
  box.querySelectorAll('.wb-hit').forEach((b) => b.addEventListener('click', () => {
    const d = results[+b.dataset.i];
    addGazetteer({ dataset_id: d.id, title: d.title, tip: d.description || '', centroid: d.centroid || null });
    el('wb-search').value = ''; box.innerHTML = '';
  }));
}

function addGazetteer(g) {
  if (project.gazetteers.some((x) => x.dataset_id === g.dataset_id)) return;
  project.gazetteers.push({ dataset_id: g.dataset_id, title: g.title, tip: g.tip || '',
                            centroid: g.centroid || null });
  render(); touched();
}
function move(i, d) {
  const j = i + d;
  if (j < 0 || j >= project.gazetteers.length) return;
  [project.gazetteers[i], project.gazetteers[j]] = [project.gazetteers[j], project.gazetteers[i]];
  render(); touched();
}
function render() {
  const list = el('wb-places');
  el('wb-places-count').textContent = project.gazetteers.length;
  if (!project.gazetteers.length) { list.innerHTML = '<li class="text-muted small list-group-item">No gazetteers yet — search above to add some.</li>'; return; }
  list.innerHTML = project.gazetteers.map((g, i) => `
    <li class="list-group-item d-flex align-items-center gap-2" data-i="${i}">
      <span class="badge bg-secondary">${i + 1}</span>
      <span class="flex-grow-1 fw-semibold" title="${esc(g.tip || '')}">${esc(g.title || ('dataset ' + g.dataset_id))}</span>
      <span class="btn-group btn-group-sm" role="group">
        <button type="button" class="btn btn-outline-secondary wb-up" title="Move up">↑</button>
        <button type="button" class="btn btn-outline-secondary wb-down" title="Move down">↓</button>
        <button type="button" class="btn btn-outline-danger wb-del" title="Remove">✕</button>
      </span>
    </li>`).join('');
  list.querySelectorAll('li').forEach((li) => {
    const i = +li.dataset.i;
    li.querySelector('.wb-up').addEventListener('click', () => move(i, -1));
    li.querySelector('.wb-down').addEventListener('click', () => move(i, 1));
    li.querySelector('.wb-del').addEventListener('click', () => { project.gazetteers.splice(i, 1); render(); touched(); });
  });
}

// ── save + publish ─────────────────────────────────────────────────────────────
async function saveToAccount() {
  status.saving();
  const r = bridge.id ? await bridge.push(snapshot()) : await bridge.create(snapshot(), project.title, null);
  if (r.ok) { status.synced(); flash(el('wb-save-result'), 'Saved to your account.', 'success'); }
  else { status.error(); flash(el('wb-save-result'), (r.data && r.data.error) || 'Save failed.', 'danger'); }
}
async function publish() {
  const out = el('wb-publish-result');
  if (!project.gazetteers.length) { flash(out, 'Add at least one gazetteer before publishing.', 'warning'); return; }
  flash(out, 'Publishing…', 'secondary');
  if (!bridge.id) {
    const c = await bridge.create(snapshot(), project.title, null);
    if (!c.ok) { flash(out, (c.data && c.data.error) || 'Could not save before publishing.', 'danger'); return; }
  } else { await bridge.push(snapshot()); }
  const r = await bridge.publish();
  if (!r.ok) { flash(out, (r.data && r.data.error) || 'Publishing failed.', 'danger'); return; }
  const d = r.data;
  const link = `<a href="/collections/${d.collection_id}/browse_ds" target="_blank" rel="noopener">View your published group →</a>`;
  const warn = (d.unresolved && d.unresolved.length)
    ? `<div class="small text-warning mt-1">${d.unresolved.length} gazetteer(s) could not be added. Added ${d.added}.</div>` : '';
  out.innerHTML = `<div class="alert alert-success py-2 mb-0">Published a group of ${d.added} gazetteer(s). ${link}${warn}</div>`;
  status.synced();
}
function flash(node, msg, kind) { if (node) node.innerHTML = msg ? `<div class="alert alert-${kind} py-2 mb-0">${esc(msg)}</div>` : ''; }

async function clearDraft() {
  await store.clear();
  project = { title: '', description: '', keywords: [], gazetteers: [] };
  fillMeta(); render(); status.saved();
  el('wb-publish-result').innerHTML = ''; el('wb-save-result').innerHTML = '';
}

// ── boot ────────────────────────────────────────────────────────────────────────
async function init() {
  status = statusBadge(el('wb-status'));
  bindMeta();
  mountAccordion('wb-pane-about');
  mountCollab({ bridge, getSnapshot: snapshot, getTitle: () => project.title.trim() || 'Untitled group',
                container: el('wb-collab-body'), onSaved: () => status.synced() });
  el('wb-search-btn').addEventListener('click', search);
  el('wb-search').addEventListener('keydown', (e) => { if (e.key === 'Enter') { e.preventDefault(); search(); } });
  el('wb-search').addEventListener('input', debounce(search, 300));   // typeahead
  el('wb-save-btn').addEventListener('click', saveToAccount);
  el('wb-publish-btn').addEventListener('click', publish);
  el('wb-clear-btn').addEventListener('click', () => { if (confirm('Clear this draft from your browser? This cannot be undone.')) clearDraft(); });

  const pid = new URLSearchParams(location.search).get('project');
  if (pid) {
    const r = await bridge.load(pid);
    if (r.ok && r.data && r.data.snapshot) {
      const s = r.data.snapshot;
      project = { title: s.title || '', description: s.description || '', keywords: s.keywords || [], gazetteers: (s.gazetteers || []).map((g) => ({ dataset_id: g.dataset_id, title: g.title || ('dataset ' + g.dataset_id) })) };
    }
  } else {
    const local = await store.load();
    if (local) project = Object.assign(project, local);
  }
  fillMeta(); render(); status.saved();
}

if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
else init();
