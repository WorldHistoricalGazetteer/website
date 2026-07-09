// wb-record-fields.js — the full-LPF field editor for ONE place record, rendered into a caller-supplied
// container. Shared by the single-record editor (wb-place-record.js) and the dataset shell
// (wb-dataset.js, which mounts one per checked-out record). Everything is scoped to `container` (no
// global element ids), so many records can be edited on one page without collisions.
//
// `rec` is the full-LPF record snapshot (checkout.record_snapshot): mutated in place as the user edits.
// `opts.onChange()` fires after every edit (host debounces + persists). Unexposed jsonb (citations,
// per-item `when`) rides along on each item's `_raw` and is round-tripped untouched.

import { esc, truncate, csrf } from './wb-shell.js';

const RECON_ENDPOINT = '/reconcile';

// Generic repeatable-list editor over a sub-container. fields: [{k, placeholder, width}].
function listEditor(box, arr, fields, factory, onChange) {
  function render() {
    box.innerHTML = arr.map((_, i) => `<div class="wb-item" data-i="${i}">${
      fields.map((f) => `<input class="form-control form-control-sm wb-f" data-k="${f.k}" placeholder="${esc(f.placeholder)}" ${f.width ? `style="max-width:${f.width}"` : ''} value="${esc(arr[i][f.k] == null ? '' : arr[i][f.k])}">`).join('')
    }<button type="button" class="btn btn-sm btn-outline-danger btn-rm" title="Remove">✕</button></div>`).join('')
      || '<div class="text-muted small mb-1">None.</div>';
    box.querySelectorAll('.wb-item').forEach((row) => {
      const i = +row.dataset.i;
      row.querySelectorAll('.wb-f').forEach((inp) => inp.addEventListener('input', (e) => { arr[i][e.target.dataset.k] = e.target.value; onChange(); }));
      row.querySelector('.btn-rm').addEventListener('click', () => { arr.splice(i, 1); render(); onChange(); });
    });
  }
  render();
  return { add() { arr.push(factory()); render(); onChange(); }, render };
}

const SECTIONS = `
  <div class="wb-rf-grid">
    <div class="wb-rf-field"><label class="form-label small mb-1">Primary name (title)</label>
      <input class="form-control form-control-sm wb-rf-title" maxlength="255"></div>
    <div class="wb-rf-field"><label class="form-label small mb-1">Country codes <span class="text-muted">(ISO-2, comma-separated)</span></label>
      <input class="form-control form-control-sm wb-rf-ccodes" placeholder="GB, FR"></div>
  </div>
  <div class="wb-rf-block"><div class="wb-rf-h">Also-known-as (names)</div>
    <div class="wb-rf-names"></div>
    <button type="button" class="btn btn-sm btn-outline-secondary wb-rf-add" data-list="names"><i class="fas fa-plus me-1"></i>Add a name</button></div>
  <div class="wb-rf-block"><div class="wb-rf-h">Place types</div>
    <div class="wb-rf-types"></div>
    <button type="button" class="btn btn-sm btn-outline-secondary wb-rf-add" data-list="types"><i class="fas fa-plus me-1"></i>Add a type</button></div>
  <div class="wb-rf-block"><div class="wb-rf-h">Coordinate</div><div class="wb-rf-coord"></div></div>
  <div class="wb-rf-block"><div class="wb-rf-h">Dates</div>
    <div class="input-group input-group-sm" style="max-width:26rem;">
      <span class="input-group-text">start (year)</span><input class="form-control wb-rf-start" type="number" step="1">
      <span class="input-group-text">end</span><input class="form-control wb-rf-end" type="number" step="1"></div>
    <div class="form-text">Negative years are BCE (e.g. -500).</div></div>
  <div class="wb-rf-block"><div class="wb-rf-h">Authority links</div>
    <div class="wb-rf-links"></div>
    <div class="d-flex gap-2 mt-1">
      <button type="button" class="btn btn-sm btn-outline-secondary wb-rf-add" data-list="links"><i class="fas fa-plus me-1"></i>Add a link</button>
      <button type="button" class="btn btn-sm btn-outline-primary wb-rf-recon"><i class="fas fa-wand-magic-sparkles me-1"></i>Re-reconcile this record</button></div>
    <div class="wb-rf-recon-results mt-2"></div></div>
  <div class="wb-rf-block"><div class="wb-rf-h">Descriptions</div>
    <div class="wb-rf-descriptions"></div>
    <button type="button" class="btn btn-sm btn-outline-secondary wb-rf-add" data-list="descriptions"><i class="fas fa-plus me-1"></i>Add a description</button></div>`;

export function renderRecordFields(container, rec, opts = {}) {
  const onChange = opts.onChange || (() => {});
  ['names', 'types', 'links', 'descriptions'].forEach((k) => { if (!Array.isArray(rec[k])) rec[k] = []; });
  if (!rec.dates) rec.dates = {};

  container.innerHTML = SECTIONS;
  const $ = (sel) => container.querySelector(sel);

  $('.wb-rf-title').value = rec.title || '';
  $('.wb-rf-ccodes').value = (rec.ccodes || []).join(', ');
  $('.wb-rf-start').value = rec.dates.start != null ? rec.dates.start : '';
  $('.wb-rf-end').value = rec.dates.end != null ? rec.dates.end : '';

  $('.wb-rf-title').addEventListener('input', (e) => { rec.title = e.target.value; onChange(); });
  $('.wb-rf-ccodes').addEventListener('input', (e) => { rec.ccodes = e.target.value.split(',').map((c) => c.trim()).filter(Boolean); onChange(); });
  $('.wb-rf-start').addEventListener('input', (e) => { rec.dates.start = e.target.value === '' ? null : parseInt(e.target.value, 10); onChange(); });
  $('.wb-rf-end').addEventListener('input', (e) => { rec.dates.end = e.target.value === '' ? null : parseInt(e.target.value, 10); onChange(); });

  const namesEd = listEditor($('.wb-rf-names'), rec.names, [{ k: 'toponym', placeholder: 'name' }, { k: 'lang', placeholder: 'lang', width: '6rem' }], () => ({ toponym: '', lang: '', _raw: {} }), onChange);
  const typesEd = listEditor($('.wb-rf-types'), rec.types, [{ k: 'label', placeholder: 'type label' }, { k: 'identifier', placeholder: 'aat:… (optional)', width: '12rem' }], () => ({ label: '', identifier: '', _raw: {} }), onChange);
  const linksEd = listEditor($('.wb-rf-links'), rec.links, [{ k: 'type', placeholder: 'closeMatch', width: '9rem' }, { k: 'identifier', placeholder: 'wd:Q… / gn:… / URL' }], () => ({ type: 'closeMatch', identifier: '' }), onChange);
  const descrEd = listEditor($('.wb-rf-descriptions'), rec.descriptions, [{ k: 'value', placeholder: 'description' }, { k: 'lang', placeholder: 'lang', width: '6rem' }], () => ({ value: '', lang: '', _raw: {} }), onChange);
  const editors = { names: namesEd, types: typesEd, links: linksEd, descriptions: descrEd };
  container.querySelectorAll('.wb-rf-add').forEach((b) => b.addEventListener('click', () => editors[b.dataset.list].add()));

  function renderCoord() {
    const box = $('.wb-rf-coord');
    if (rec.point_editable) {
      box.innerHTML = `<div class="input-group input-group-sm" style="max-width:26rem;">
          <span class="input-group-text">lng</span><input class="form-control wb-rf-lng" type="number" step="any" value="${rec.lng != null ? rec.lng : ''}">
          <span class="input-group-text">lat</span><input class="form-control wb-rf-lat" type="number" step="any" value="${rec.lat != null ? rec.lat : ''}"></div>
        <div class="form-text">Decimal degrees (WGS-84).${rec.lng == null ? ' No coordinate yet — add one.' : ''}</div>`;
      box.querySelector('.wb-rf-lng').addEventListener('input', (e) => { rec.lng = e.target.value === '' ? null : parseFloat(e.target.value); onChange(); });
      box.querySelector('.wb-rf-lat').addEventListener('input', (e) => { rec.lat = e.target.value === '' ? null : parseFloat(e.target.value); onChange(); });
    } else {
      box.innerHTML = `<div class="text-muted small"><i class="fas fa-circle-info me-1"></i>This place has complex geometry
        ${rec.lng != null ? `(near ${(+rec.lng).toFixed(3)}, ${(+rec.lat).toFixed(3)})` : ''} — edit its geometry in the dataset editor.</div>`;
    }
  }
  renderCoord();

  async function reReconcile() {
    const box = $('.wb-rf-recon-results');
    const q = (rec.title || (rec.names[0] && rec.names[0].toponym) || '').trim();
    if (!q) { box.innerHTML = '<span class="text-muted small">Give the record a name first.</span>'; return; }
    box.innerHTML = '<span class="text-muted small"><i class="fas fa-spinner fa-spin me-1"></i>searching WHG &amp; authorities…</span>';
    let data;
    try {
      const res = await fetch(opts.reconEndpoint || RECON_ENDPOINT, {
        method: 'POST', credentials: 'same-origin',
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
      if (!rec.links.some((l) => l.identifier === c.id)) { rec.links.push({ type: 'closeMatch', identifier: c.id }); linksEd.render(); onChange(); }
      if (c.repr_point && (rec.lng == null) && rec.point_editable) { rec.lng = c.repr_point[0]; rec.lat = c.repr_point[1]; renderCoord(); onChange(); }
      b.disabled = true; b.classList.add('active');
    }));
  }
  $('.wb-rf-recon').addEventListener('click', reReconcile);
}
