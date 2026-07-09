// wb-dataset.js — gazetteer records-correction shell (plan-dataset-checkout §3/§4).
//
// Two modes off the query string:
//   ?dataset=<id>   fresh: a capacity chooser (navigator.storage.estimate vs the dataset's estimated
//                   size) → whole-dataset-that-fits, or a name/country/paged subset. Both POST to the
//                   staff-gated checkout endpoint, which returns a new dataset_edit project.
//   ?project=<uuid> resume: the checked-out record list. Expand a record to edit every LPF field
//                   (shared wb-record-fields editor); edits mark it dirty. "Publish changes" pushes
//                   the snapshot and applies the DELTA of dirty records server-side (per-record
//                   optimistic-locked + reindexed), never a full re-accession.

import '../css/reconciliation.css';
import { el, esc, truncate, debounce, csrf, statusBadge, serverBridge } from './wb-shell.js';
import { renderRecordFields } from './wb-record-fields.js';

// eslint-disable-next-line camelcase, no-undef
__webpack_public_path__ = '/static/webpack/';

const SAFETY_BYTES = 50 * 1024 * 1024;   // keep this much browser quota free
const bridge = serverBridge('dataset_edit');
let status = null;
let snap = null;
let pid = null;

const pushSoon = debounce(async () => {
  const r = await bridge.push(snap);
  if (r.ok) status.synced();
  else if (r.status === 409) status.error('Edited elsewhere — reload');
  else status.error('Could not save');
}, 800);
const touched = () => { status.saving(); pushSoon(); };
function result(kind, html) { const o = el('wb-ds-result'); if (o) o.innerHTML = html ? `<div class="alert alert-${kind} py-2 mb-0">${html}</div>` : ''; }

// ── capacity chooser (fresh ?dataset=) ─────────────────────────────────────────
function fmtBytes(n) { return n > 1e9 ? (n / 1e9).toFixed(1) + ' GB' : n > 1e6 ? Math.round(n / 1e6) + ' MB' : Math.round(n / 1e3) + ' KB'; }

async function checkoutDataset(dsId, body, btn) {
  if (btn) { btn.disabled = true; btn.innerHTML = '<i class="fas fa-spinner fa-spin me-1"></i>Checking out…'; }
  try {
    const res = await fetch(`/reconciliation/checkout/dataset/${dsId}/`, {
      method: 'POST', credentials: 'same-origin',
      headers: { 'X-CSRFToken': csrf(), 'Content-Type': 'application/json' }, body: JSON.stringify(body || {}) });
    const d = await res.json();
    if (res.ok && d.id) { window.location.href = `/workbench/dataset/?project=${d.id}`; return; }
    result('warning', esc(d.error || 'Could not check out those records.'));
  } catch (e) { result('danger', 'Could not reach the Workbench — please try again.'); }
  if (btn) { btn.disabled = false; btn.textContent = 'Check out'; }
}

async function runChooser(dsId) {
  el('wb-ds-chooser').style.display = '';
  el('wb-ds-where').innerHTML = 'Choose which records to check out for editing.';
  const box = el('wb-ds-chooser-body');
  let info;
  try {
    const res = await fetch(`/reconciliation/checkout/dataset/${dsId}/info/`, { credentials: 'same-origin' });
    info = await res.json();
    if (!res.ok) { box.innerHTML = `<div class="text-danger small">${esc(info.error || 'Could not read this gazetteer.')}</div>`; return; }
  } catch (e) { box.innerHTML = '<div class="text-danger small">Could not reach the Workbench.</div>'; return; }

  el('wb-ds-where').innerHTML = `Gazetteer <strong>${esc(info.title)}</strong> — <strong>${info.numrows.toLocaleString()}</strong> records.`;

  // Browser capacity (dynamic, per §1b). If the API is missing, we don't block — the server cap applies.
  let avail = Infinity;
  try { if (navigator.storage && navigator.storage.estimate) { const q = await navigator.storage.estimate(); avail = Math.max(0, (q.quota || 0) - (q.usage || 0) - SAFETY_BYTES); } } catch (e) { /* ignore */ }
  const wholeFits = info.fits_whole && info.est_total_bytes < avail;

  const ccOpts = ['<option value="">Any country</option>'].concat((info.ccodes || []).map((c) => `<option value="${esc(c)}">${esc(c)}</option>`)).join('');
  box.innerHTML = `
    <div class="mb-3">
      <div class="fw-semibold mb-1">Whole gazetteer</div>
      ${wholeFits
        ? `<button id="wb-ds-all" class="btn btn-primary btn-sm">Edit all ${info.numrows.toLocaleString()} records</button>
           <div class="form-text">About ${fmtBytes(info.est_total_bytes)} — fits in this browser.</div>`
        : `<div class="text-muted small"><i class="fas fa-circle-info me-1"></i>${info.numrows > info.max_records
              ? `Too many records (${info.numrows.toLocaleString()}) to edit wholesale — check out a subset below.`
              : `About ${fmtBytes(info.est_total_bytes)} — larger than this browser's free storage. Check out a subset below.`}</div>`}
    </div>
    <div>
      <div class="fw-semibold mb-1">A subset</div>
      <div class="row g-2 align-items-end">
        <div class="col-12 col-sm"><label class="form-label small mb-1">Name contains</label>
          <input id="wb-ds-q" class="form-control form-control-sm" placeholder="e.g. Roma" autocomplete="off"></div>
        <div class="col-6 col-sm-auto"><label class="form-label small mb-1">Country</label>
          <select id="wb-ds-cc" class="form-select form-select-sm">${ccOpts}</select></div>
        <div class="col-6 col-sm-auto"><label class="form-label small mb-1">Max records</label>
          <input id="wb-ds-limit" class="form-control form-control-sm" type="number" min="1" max="${info.max_records}" value="${Math.min(500, info.max_records)}" style="max-width:7rem;"></div>
        <div class="col-12 col-sm-auto"><button id="wb-ds-sub" class="btn btn-outline-primary btn-sm">Check out</button></div>
      </div>
      <div class="form-text">Edit up to ${info.max_records.toLocaleString()} records at a time.</div>
    </div>`;

  const all = el('wb-ds-all');
  if (all) all.addEventListener('click', () => checkoutDataset(dsId, {}, all));
  el('wb-ds-sub').addEventListener('click', (e) => checkoutDataset(dsId, {
    q: el('wb-ds-q').value.trim(), ccode: el('wb-ds-cc').value,
    limit: parseInt(el('wb-ds-limit').value, 10) || undefined }, e.target));
}

// ── resume: record list (?project=) ────────────────────────────────────────────
function dirtyCount() { return (snap.records || []).filter((r) => r._dirty).length; }
function refreshCount() {
  const n = dirtyCount();
  el('wb-ds-count').textContent = `${snap.loaded} record${snap.loaded !== 1 ? 's' : ''} loaded` +
    (snap.total_matched > snap.loaded ? ` of ${snap.total_matched} matching` : '') +
    (n ? ` · ${n} changed` : '');
  const pub = el('wb-ds-pub');
  pub.disabled = n === 0;
  pub.textContent = n ? `Publish ${n} change${n !== 1 ? 's' : ''}` : 'Publish changes';
}

function rowSub(rec) {
  const bits = [`#${rec.record_id}`];
  if ((rec.ccodes || []).length) bits.push(rec.ccodes.join(', '));
  if (rec.geometry_editable === false) bits.push('complex geometry');
  return bits.join(' · ');
}

function markDirty(rec, row) {
  if (!rec._dirty) {
    rec._dirty = true;
    const badge = row.querySelector('.wb-ds-dirty');
    if (badge) badge.classList.remove('d-none');
  }
  refreshCount();
  touched();
}

function renderList() {
  const list = el('wb-ds-list');
  const records = snap.records || [];
  if (!records.length) { list.innerHTML = '<p class="text-muted">No records were checked out.</p>'; return; }
  list.innerHTML = records.map((rec, i) => `
    <div class="wb-ds-row" data-i="${i}" data-title="${esc((rec.title || '').toLowerCase())} ${rec.record_id}">
      <div class="wb-ds-head">
        <i class="fas fa-chevron-right wb-ds-caret"></i>
        <span class="wb-ds-title text-truncate flex-grow-1">${esc(rec.title || '(untitled)')}
          <span class="badge bg-warning text-dark wb-ds-dirty ms-1 ${rec._dirty ? '' : 'd-none'}">changed</span></span>
        <span class="wb-ds-sub">${esc(rowSub(rec))}</span>
      </div>
      <div class="wb-ds-body"></div>
    </div>`).join('');

  list.querySelectorAll('.wb-ds-row').forEach((row) => {
    const i = +row.dataset.i;
    const rec = records[i];
    const head = row.querySelector('.wb-ds-head');
    const body = row.querySelector('.wb-ds-body');
    head.addEventListener('click', () => {
      const opening = !row.classList.contains('open');
      row.classList.toggle('open', opening);
      row.querySelector('.wb-ds-caret').className = `fas fa-chevron-${opening ? 'down' : 'right'} wb-ds-caret`;
      if (opening && !body.dataset.mounted) {
        body.dataset.mounted = '1';
        renderRecordFields(body, rec, { onChange: () => markDirty(rec, row) });
      }
    });
  });
  refreshCount();
}

async function publish() {
  const n = dirtyCount();
  if (!n) return;
  const pub = el('wb-ds-pub');
  pub.disabled = true; pub.innerHTML = '<i class="fas fa-spinner fa-spin me-1"></i>Publishing…';
  await bridge.push(snap);
  const r = await bridge.publish();
  if (r.status === 409) { result('warning', esc((r.data && r.data.error) || 'This gazetteer changed since you started editing.')); refreshCount(); return; }
  if (!r.ok) { result('danger', esc((r.data && r.data.error) || 'Could not publish the corrections.')); refreshCount(); return; }
  const d = r.data;
  let msg = `Published <strong>${d.changed_records}</strong> record${d.changed_records !== 1 ? 's' : ''}` +
    (d.reindexed ? ` · ${d.reindexed} reindexed in search` : '') + '.';
  if (d.conflicts && d.conflicts.length) {
    msg += ` <span class="text-danger">${d.conflicts.length} skipped (changed since checkout):</span> ` +
      d.conflicts.map((c) => `#${c.record_id}`).join(', ') + ' — reload to get the latest and re-apply.';
    result('warning', msg);
  } else {
    result('success', msg);
  }
  // Reload the server copy so cleared dirty flags + refreshed per-record baselines are reflected.
  const rl = await bridge.load(pid);
  if (rl.ok && rl.data && rl.data.snapshot) { snap = rl.data.snapshot; renderList(); }
  status.synced();
}

function bindSearch() {
  const search = el('wb-ds-search');
  if (!search) return;
  search.addEventListener('input', () => {
    const q = search.value.trim().toLowerCase();
    el('wb-ds-list').querySelectorAll('.wb-ds-row').forEach((row) => {
      row.style.display = (!q || row.dataset.title.indexOf(q) > -1) ? '' : 'none';
    });
  });
}

async function init() {
  status = statusBadge(el('wb-status'));
  const params = new URLSearchParams(location.search);
  pid = params.get('project');
  const dsId = params.get('dataset');

  if (pid) {
    const r = await bridge.load(pid);
    if (!r.ok || !r.data || !r.data.snapshot || !r.data.snapshot.dataset_id) {
      el('wb-ds-where').innerHTML = '<span class="text-danger">Could not load these records for editing.</span>'; return;
    }
    snap = r.data.snapshot;
    el('wb-ds-where').innerHTML = `Correcting records in gazetteer <strong>${esc(snap.dataset_title || snap.dataset_label || '')}</strong>. ` +
      'Expand a record to edit it; changes are saved to your account and published together.';
    el('wb-ds-toolbar').style.display = '';
    el('wb-ds-pub').addEventListener('click', publish);
    bindSearch();
    renderList();
    status.saved();
  } else if (dsId) {
    runChooser(dsId);
  } else {
    el('wb-ds-where').innerHTML = 'Open a gazetteer’s <em>Edit records in Workbench</em> button to correct its records here.';
  }
}

if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
else init();
