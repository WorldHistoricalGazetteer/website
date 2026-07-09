// wb-place-record.js — full-LPF single-record editor (plan §6.1) + community-suggestion mode
// (plan-record-suggestions §3).
//
// Reached via record check-out (?project=<uuid>), which is open to any beta user. If the user has edit
// rights on the record's gazetteer (`can_apply`), the action is **Publish correction** (direct apply,
// as before). Otherwise it's **Submit suggestion** — the same edits, routed to the owner/staff for
// review instead of applied. Editing is identical either way (shared wb-record-fields editor).

import '../css/reconciliation.css';
import { el, esc, debounce, csrf, statusBadge, serverBridge } from './wb-shell.js';
import { renderRecordFields } from './wb-record-fields.js';

// eslint-disable-next-line camelcase, no-undef
__webpack_public_path__ = '/static/webpack/';

const bridge = serverBridge('place_record');
let status = null;
let snap = null;
let pid = null;
let canApply = true;

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

async function submitSuggestion() {
  await bridge.push(snap);                                   // ensure the server copy is current
  flash('secondary', 'Submitting your suggestion…');
  const rationale = (el('wb-rec-rationale') && el('wb-rec-rationale').value || '').trim();
  let res, data;
  try {
    res = await fetch('/reconciliation/suggestions/', {
      method: 'POST', credentials: 'same-origin',
      headers: { 'X-CSRFToken': csrf(), 'Content-Type': 'application/json' },
      body: JSON.stringify({ project_id: pid, rationale }) });
    data = await res.json();
  } catch (e) { flash('danger', 'Could not reach the review service — please try again.'); return; }
  if (!res.ok) { flash('warning', esc((data && data.error) || 'Could not submit your suggestion.')); return; }
  const fields = (data.changed_fields || []).join(', ') || 'your changes';
  flash('success', `Thank you — your suggested correction (${esc(fields)}) has been sent to the gazetteer’s ` +
    'editors and WHG staff for review. <a href="/places/' + snap.record_id + '/detail">Back to the record →</a>');
  const act = el('wb-rec-actions'); if (act) act.querySelectorAll('button,textarea').forEach((n) => { n.disabled = true; });
  status.synced();
}

function renderActions() {
  const box = el('wb-rec-actions');
  if (canApply) {
    box.innerHTML = '<button id="wb-rec-pub" class="btn btn-success" type="button">Publish correction</button>';
    el('wb-rec-pub').addEventListener('click', publish);
  } else {
    box.innerHTML = `
      <label class="form-label small mb-1" for="wb-rec-rationale">Why this correction? <span class="text-muted">(optional — helps the reviewer)</span></label>
      <textarea id="wb-rec-rationale" class="form-control form-control-sm mb-2" rows="2" maxlength="4000"
        placeholder="e.g. The coordinate is in the sea; Wikidata has the correct location."></textarea>
      <button id="wb-rec-suggest" class="btn btn-primary" type="button"><i class="fas fa-paper-plane me-1"></i>Submit suggestion</button>`;
    el('wb-rec-suggest').addEventListener('click', submitSuggestion);
  }
}

async function init() {
  status = statusBadge(el('wb-status'));
  pid = new URLSearchParams(location.search).get('project');
  if (!pid) { flash('warning', 'Open a record with “Correct this record” or “Suggest a correction” from its page to edit it here.'); return; }
  const r = await bridge.load(pid);
  if (!r.ok || !r.data || !r.data.snapshot || !r.data.snapshot.record_id) { flash('danger', 'Could not load that record for editing.'); return; }
  snap = r.data.snapshot;
  canApply = r.data.can_apply !== false;

  const verb = canApply ? 'Correcting' : 'Suggesting a correction to';
  el('wb-rec-where').innerHTML = `${verb} record <strong>#${snap.record_id}</strong> in gazetteer <strong>${esc(snap.dataset_label || '')}</strong>`;
  const h = document.querySelector('#wb-rec-heading');
  if (h && !canApply) h.textContent = 'Suggest a correction';
  const note = el('wb-rec-note');
  if (note) note.innerHTML = canApply
    ? 'Publishing applies your changes to this single record and re-indexes it in WHG search — the rest of the gazetteer is untouched. Guarded so you can’t overwrite a change someone else made meanwhile. (Depictions, relations, and named periods aren’t editable here yet and are preserved as-is.)'
    : 'You don’t own this gazetteer, so your changes are sent as a <strong>suggestion</strong> for its editors and WHG staff to review — nothing is changed until they accept it. (Depictions, relations, and named periods aren’t editable here yet.)';

  renderRecordFields(el('wb-rec-form'), snap, { onChange: touched });
  renderActions();
  status.saved();
}

if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
else init();
