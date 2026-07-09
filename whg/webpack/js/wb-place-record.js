// wb-place-record.js — full-LPF single-record correction editor (plan §6.1).
//
// Reached ONLY via record-level check-out (?project=<uuid>). Edits every common LPF field of one
// place — primary name, also-known-as names, place types, coordinate, dates, authority links, and
// descriptions — plus per-record re-reconciliation. Pushes the whole snapshot to the server working
// copy; on Publish the server rebuilds that place's sub-tables and re-indexes just that record.
// Unexposed jsonb (citations, per-item when) is round-tripped via each item's `_raw`.

import '../css/reconciliation.css';
import { el, esc, truncate, debounce, csrf, statusBadge, serverBridge } from './wb-shell.js';

// eslint-disable-next-line camelcase, no-undef
__webpack_public_path__ = '/static/webpack/';

const RECON_ENDPOINT = '/reconcile';
const bridge = serverBridge('place_record');
let status = null;
let snap = null;

const pushSoon = debounce(async () => {
  const r = await bridge.push(snap);
  if (r.ok) status.synced();
  else if (r.status === 409) status.error('Edited elsewhere — reload');
  else status.error('Could not save');
}, 700);
const touched = () => { status.saving(); pushSoon(); };
function flash(kind, msg) { const o = el('wb-rec-result'); if (o) o.innerHTML = msg ? `<div class="alert alert-${kind} py-2 mb-0">${msg}</div>` : ''; }

// Generic repeatable-list editor. fields: [{k, placeholder, width}]. factory() → a new item.
function listEditor(container, arr, fields, factory) {
  function render() {
    container.innerHTML = arr.map((_, i) => `<div class="wb-item" data-i="${i}">${
      fields.map((f) => `<input class="form-control form-control-sm wb-f" data-k="${f.k}" placeholder="${esc(f.placeholder)}" ${f.width ? `style="max-width:${f.width}"` : ''} value="${esc(arr[i][f.k] == null ? '' : arr[i][f.k])}">`).join('')
    }<button type="button" class="btn btn-sm btn-outline-danger btn-rm" title="Remove">✕</button></div>`).join('')
      || '<div class="text-muted small mb-1">None.</div>';
    container.querySelectorAll('.wb-item').forEach((row) => {
      const i = +row.dataset.i;
      row.querySelectorAll('.wb-f').forEach((inp) => inp.addEventListener('input', (e) => { arr[i][e.target.dataset.k] = e.target.value; touched(); }));
      row.querySelector('.btn-rm').addEventListener('click', () => { arr.splice(i, 1); render(); touched(); });
    });
  }
  render();
  return { add() { arr.push(factory()); render(); touched(); }, render };
}

function renderCoord() {
  const box = el('wb-rec-coord');
  if (snap.point_editable) {
    box.innerHTML = `<div class="input-group input-group-sm" style="max-width:26rem;">
        <span class="input-group-text">lng</span><input id="wb-rec-lng" class="form-control" type="number" step="any" value="${snap.lng != null ? snap.lng : ''}">
        <span class="input-group-text">lat</span><input id="wb-rec-lat" class="form-control" type="number" step="any" value="${snap.lat != null ? snap.lat : ''}"></div>
      <div class="form-text">Decimal degrees (WGS-84).${snap.lng == null ? ' No coordinate yet — add one.' : ''}</div>`;
    el('wb-rec-lng').addEventListener('input', (e) => { snap.lng = e.target.value === '' ? null : parseFloat(e.target.value); touched(); });
    el('wb-rec-lat').addEventListener('input', (e) => { snap.lat = e.target.value === '' ? null : parseFloat(e.target.value); touched(); });
  } else {
    box.innerHTML = `<div class="text-muted small"><i class="fas fa-circle-info me-1"></i>This place has complex geometry
      ${snap.lng != null ? `(near ${(+snap.lng).toFixed(3)}, ${(+snap.lat).toFixed(3)})` : ''} — edit its geometry in the dataset editor.</div>`;
  }
}

let linksEd = null;
async function reReconcile() {
  const box = el('wb-rec-recon-results');
  const q = (snap.title || (snap.names[0] && snap.names[0].toponym) || '').trim();
  if (!q) { box.innerHTML = '<span class="text-muted small">Give the record a name first.</span>'; return; }
  box.innerHTML = '<span class="text-muted small"><i class="fas fa-spinner fa-spin me-1"></i>searching WHG &amp; authorities…</span>';
  let data;
  try {
    const res = await fetch(RECON_ENDPOINT, { method: 'POST', credentials: 'same-origin',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'X-CSRFToken': csrf() },
      body: 'queries=' + encodeURIComponent(JSON.stringify({ q0: { query: q, type: 'place', limit: 8 } })) });
    data = await res.json();
  } catch (e) { box.innerHTML = '<span class="text-danger small">Search failed.</span>'; return; }
  const results = (data.q0 && data.q0.result) || [];
  if (!results.length) { box.innerHTML = '<span class="text-muted small">No matches found.</span>'; return; }
  box.innerHTML = '<div class="small text-muted mb-1">Click a match to add it as a link:</div>' + results.map((c, i) =>
    `<button type="button" class="btn btn-sm btn-outline-secondary text-start d-block w-100 mb-1 wb-recon-hit" data-i="${i}">
       ${esc(truncate(c.name, 48))} <span class="text-muted small ms-1">${esc(c.id)}</span>
       ${c.description ? `<span class="text-muted small ms-1">${esc(truncate(c.description, 30))}</span>` : ''}</button>`).join('');
  box.querySelectorAll('.wb-recon-hit').forEach((b) => b.addEventListener('click', () => {
    const c = results[+b.dataset.i];
    if (!snap.links.some((l) => l.identifier === c.id)) { snap.links.push({ type: 'closeMatch', identifier: c.id }); linksEd.render(); touched(); }
    if (c.repr_point && (snap.lng == null) && snap.point_editable) { snap.lng = c.repr_point[0]; snap.lat = c.repr_point[1]; renderCoord(); }
    b.disabled = true; b.classList.add('active');
  }));
}

async function publish() {
  await bridge.push(snap);
  flash('secondary', 'Publishing…');
  const r = await bridge.publish();
  if (r.status === 409) { flash('warning', esc((r.data && r.data.error) || 'This record changed since you started editing.')); return; }
  if (!r.ok) { flash('danger', esc((r.data && r.data.error) || 'Could not publish the correction.')); return; }
  const d = r.data;
  const what = (d.changed && d.changed.length) ? d.changed.join(', ') : 'no changes';
  flash('success', `Correction published (${esc(what)}).${d.reindexed ? ' Search index updated.' : ''} <a href="/places/${d.record_id}/detail">View the record →</a>`);
  status.synced();
}

async function init() {
  status = statusBadge(el('wb-status'));
  const pid = new URLSearchParams(location.search).get('project');
  if (!pid) { flash('warning', 'Open a record with “Correct this record” from its page to edit it here.'); return; }
  const r = await bridge.load(pid);
  if (!r.ok || !r.data || !r.data.snapshot || !r.data.snapshot.record_id) { flash('danger', 'Could not load that record for editing.'); return; }
  snap = r.data.snapshot;
  // Ensure arrays exist (older snapshots may lack them).
  ['names', 'types', 'links', 'descriptions'].forEach((k) => { if (!Array.isArray(snap[k])) snap[k] = []; });
  if (!snap.dates) snap.dates = {};

  el('wb-rec-where').innerHTML = `Correcting record <strong>#${snap.record_id}</strong> in gazetteer <strong>${esc(snap.dataset_label || '')}</strong>`;
  el('wb-rec-title').value = snap.title || '';
  el('wb-rec-ccodes').value = (snap.ccodes || []).join(', ');
  el('wb-rec-start').value = snap.dates.start != null ? snap.dates.start : '';
  el('wb-rec-end').value = snap.dates.end != null ? snap.dates.end : '';
  renderCoord();

  const namesEd = listEditor(el('wb-rec-names'), snap.names, [{ k: 'toponym', placeholder: 'name' }, { k: 'lang', placeholder: 'lang', width: '6rem' }], () => ({ toponym: '', lang: '', _raw: {} }));
  const typesEd = listEditor(el('wb-rec-types'), snap.types, [{ k: 'label', placeholder: 'type label' }, { k: 'identifier', placeholder: 'aat:… (optional)', width: '12rem' }], () => ({ label: '', identifier: '', _raw: {} }));
  linksEd = listEditor(el('wb-rec-links'), snap.links, [{ k: 'type', placeholder: 'closeMatch', width: '9rem' }, { k: 'identifier', placeholder: 'wd:Q… / gn:… / URL' }], () => ({ type: 'closeMatch', identifier: '' }));
  const descrEd = listEditor(el('wb-rec-descriptions'), snap.descriptions, [{ k: 'value', placeholder: 'description' }, { k: 'lang', placeholder: 'lang', width: '6rem' }], () => ({ value: '', lang: '', _raw: {} }));

  el('wb-rec-title').addEventListener('input', (e) => { snap.title = e.target.value; touched(); });
  el('wb-rec-ccodes').addEventListener('input', (e) => { snap.ccodes = e.target.value.split(',').map((c) => c.trim()).filter(Boolean); touched(); });
  el('wb-rec-start').addEventListener('input', (e) => { snap.dates.start = e.target.value === '' ? null : parseInt(e.target.value, 10); touched(); });
  el('wb-rec-end').addEventListener('input', (e) => { snap.dates.end = e.target.value === '' ? null : parseInt(e.target.value, 10); touched(); });
  el('wb-rec-names-add').addEventListener('click', namesEd.add);
  el('wb-rec-types-add').addEventListener('click', typesEd.add);
  el('wb-rec-links-add').addEventListener('click', linksEd.add);
  el('wb-rec-descr-add').addEventListener('click', descrEd.add);
  el('wb-rec-recon').addEventListener('click', reReconcile);
  el('wb-rec-pub').addEventListener('click', publish);
  status.saved();
}

if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
else init();
