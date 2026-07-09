// wb-place-record.js — Collaborative Workbench editor for a single-record correction (plan §6.1).
//
// Reached ONLY via record-level check-out ("Correct this record" on a place page → a checked-out
// WorkbenchProject(doc_type='place_record') → this editor at ?project=<uuid>). It edits the two safest,
// most common corrections — the primary name and, for point-geometry places, the coordinate — pushes
// them to the server working copy, and on Publish applies the delta to the live place and re-indexes
// just that record. Guarded by a record-level optimistic lock (409 → "changed since you started").

import '../css/reconciliation.css';
import { el, esc, debounce, statusBadge, serverBridge } from './wb-shell.js';

// eslint-disable-next-line camelcase, no-undef
__webpack_public_path__ = '/static/webpack/';

const bridge = serverBridge('place_record');
let status = null;
let snap = null;   // the whole checked-out snapshot (record_id, title, lng, lat, point_editable, …)

const pushSoon = debounce(async () => {
  const r = await bridge.push(snap);
  if (r.ok) status.synced();
  else if (r.status === 409) status.error('Edited elsewhere — reload');
  else status.error('Could not save');
}, 700);
const touched = () => { status.saving(); pushSoon(); };

function flash(kind, msg) {
  const out = el('wb-rec-result');
  if (out) out.innerHTML = msg ? `<div class="alert alert-${kind} py-2 mb-0">${msg}</div>` : '';
}

function render() {
  el('wb-rec-where').innerHTML = `Correcting record <strong>#${snap.record_id}</strong> in gazetteer <strong>${esc(snap.dataset_label || '')}</strong>`;
  el('wb-rec-title').value = snap.title || '';
  const coordBox = el('wb-rec-coord');
  if (snap.point_editable) {
    coordBox.innerHTML = `
      <label class="form-label small mb-1">Coordinate</label>
      <div class="input-group input-group-sm" style="max-width:26rem;">
        <span class="input-group-text">lng</span>
        <input id="wb-rec-lng" class="form-control" type="number" step="any" value="${snap.lng != null ? snap.lng : ''}">
        <span class="input-group-text">lat</span>
        <input id="wb-rec-lat" class="form-control" type="number" step="any" value="${snap.lat != null ? snap.lat : ''}">
      </div>
      <div class="form-text">${snap.lng == null ? 'This place has no coordinate yet — add one.' : 'Decimal degrees (WGS-84).'}</div>`;
    el('wb-rec-lng').addEventListener('input', (e) => { snap.lng = e.target.value === '' ? null : parseFloat(e.target.value); touched(); });
    el('wb-rec-lat').addEventListener('input', (e) => { snap.lat = e.target.value === '' ? null : parseFloat(e.target.value); touched(); });
  } else {
    coordBox.innerHTML = `<label class="form-label small mb-1">Coordinate</label>
      <div class="text-muted small"><i class="fas fa-circle-info me-1"></i>This place has complex geometry
      ${snap.lng != null ? `(near ${(+snap.lng).toFixed(3)}, ${(+snap.lat).toFixed(3)})` : ''} — edit its
      geometry in the dataset editor. You can still correct its name here.</div>`;
  }
  el('wb-rec-pub').textContent = snap.idx_pub ? 'Publish correction (updates search)' : 'Publish correction';
}

async function publish() {
  await bridge.push(snap);                    // flush latest edits first
  flash('secondary', 'Publishing…');
  const r = await bridge.publish();
  if (r.status === 409) { flash('warning', esc((r.data && r.data.error) || 'This record changed since you started editing.')); return; }
  if (!r.ok) { flash('danger', esc((r.data && r.data.error) || 'Could not publish the correction.')); return; }
  const d = r.data;
  const what = (d.changed && d.changed.length) ? d.changed.join(' + ') : 'no changes';
  const idx = d.reindexed ? ' Search index updated.' : '';
  const link = `<a href="/places/${d.record_id}/detail">View the record →</a>`;
  flash('success', `Correction published (${esc(what)}).${idx} ${link}`);
  status.synced();
}

async function init() {
  status = statusBadge(el('wb-status'));
  const pid = new URLSearchParams(location.search).get('project');
  if (!pid) { flash('warning', 'Open a record with “Correct this record” from its page to edit it here.'); return; }
  const r = await bridge.load(pid);
  if (!r.ok || !r.data || !r.data.snapshot || !r.data.snapshot.record_id) {
    flash('danger', 'Could not load that record for editing.'); return;
  }
  snap = r.data.snapshot;
  render();
  el('wb-rec-title').addEventListener('input', (e) => { snap.title = e.target.value; touched(); });
  el('wb-rec-pub').addEventListener('click', publish);
  status.saved();
}

if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
else init();
