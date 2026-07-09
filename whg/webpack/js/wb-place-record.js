// wb-place-record.js — full-LPF single-record correction editor (plan §6.1).
//
// Reached ONLY via record-level check-out (?project=<uuid>). Edits every common LPF field of one
// place via the shared field editor (wb-record-fields.renderRecordFields), then pushes the whole
// snapshot to the server working copy; on Publish the server rebuilds that place's sub-tables and
// re-indexes just that record. Unexposed jsonb (citations, per-item when) round-trips via each
// item's `_raw`.

import '../css/reconciliation.css';
import { el, esc, debounce, statusBadge, serverBridge } from './wb-shell.js';
import { renderRecordFields } from './wb-record-fields.js';

// eslint-disable-next-line camelcase, no-undef
__webpack_public_path__ = '/static/webpack/';

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

  el('wb-rec-where').innerHTML = `Correcting record <strong>#${snap.record_id}</strong> in gazetteer <strong>${esc(snap.dataset_label || '')}</strong>`;
  renderRecordFields(el('wb-rec-form'), snap, { onChange: touched });
  el('wb-rec-pub').addEventListener('click', publish);
  status.saved();
}

if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
else init();
