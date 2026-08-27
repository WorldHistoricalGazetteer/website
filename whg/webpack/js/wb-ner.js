// wb-ner.js — shared "add places from a block of text" (NER) input for Workbench editors.
//
// plan-collaborativeCollections §8: the place-name extractor is an INPUT METHOD for collections, not
// a Map-your-Data-only feature. This module packages the flow — paste text / upload a file / import a
// Google Doc → the language model on WHG's own server (the one step that leaves the browser; the UI says so)
// → located place matches — behind one mount() call so Place Collection + Itinerary can offer it, and
// Map your Data can adopt it later. It reuses the existing /ner + /gdoc endpoints (recon-sync) and the
// docx/pdf text extractor (recon-textextract, lazy).
//
// The extraction returns entities each optionally carrying a gazetteer `match` (id + coords, from the
// server's geo-disambiguation). Located entities can be added straight to the collection; unlocated
// ones are shown so the user can search them by hand.

import { el, esc, truncate } from './wb-shell.js';
import { ner as nerCall, importGDoc } from './recon-sync.js';

let TextExtract = null;
const loadTextExtract = async () => (TextExtract || (TextExtract = await import(/* webpackChunkName: "recon-textextract" */ './recon-textextract.js')));

async function readFile(file) {
  const name = file.name || '';
  if (/\.docx$/i.test(name)) { const m = await loadTextExtract(); return m.extractDocx(await file.arrayBuffer()); }
  if (/\.pdf$/i.test(name)) { const m = await loadTextExtract(); return m.extractPdf(await file.arrayBuffer()); }
  const text = await file.text();
  if (/\.html?$/i.test(name)) {
    try { return new DOMParser().parseFromString(text, 'text/html').body.textContent || ''; } catch (_) { return text; }
  }
  return text;
}

const isLocated = (e) => e && e.match && e.match.id && e.match.lng != null && e.match.lat != null;

// mountNer({ container, onAdd }): render the extractor UI into `container`. onAdd({id, title, lnglat})
// is called for each located place the user adds.
export function mountNer({ container, onAdd }) {
  if (!container) return;
  let entities = [];

  container.innerHTML = `
    <p class="text-muted small mb-2"><i class="fas fa-triangle-exclamation me-1"></i>Your text is sent to
      WHG's server to detect place names (the one step that leaves your browser). It is not stored.</p>
    <textarea id="wb-ner-text" class="form-control mb-2" rows="4" placeholder="Paste a paragraph, itinerary, or narrative…"></textarea>
    <div class="d-flex flex-wrap gap-2 align-items-center mb-2">
      <button id="wb-ner-btn" class="btn btn-sm btn-primary" type="button">Find places in this text</button>
      <label class="btn btn-sm btn-outline-secondary mb-0">
        <i class="fas fa-file-arrow-up me-1"></i>Upload a file<input id="wb-ner-file" type="file" accept=".txt,.md,.html,.htm,.docx,.pdf" hidden>
      </label>
      <div class="input-group input-group-sm" style="max-width:22rem;">
        <input id="wb-ner-gdoc" class="form-control" placeholder="…or a shared Google Doc link">
        <button id="wb-ner-gdoc-btn" class="btn btn-outline-secondary" type="button">Load</button>
      </div>
    </div>
    <div id="wb-ner-msg" class="small mb-1"></div>
    <div id="wb-ner-results"></div>`;

  const setMsg = (h) => { const m = el('wb-ner-msg'); if (m) m.innerHTML = h; };

  function renderResults() {
    const box = el('wb-ner-results');
    if (!box) return;
    if (!entities.length) { box.innerHTML = ''; return; }
    const located = entities.filter(isLocated);
    const head = located.length
      ? `<div class="d-flex align-items-center mb-1"><span class="small text-success me-2">${located.length} located</span>
           <button id="wb-ner-addall" class="btn btn-sm btn-outline-success" type="button">Add all located places</button></div>`
      : '<div class="small text-warning mb-1">No places could be located against WHG — try searching them by name above.</div>';
    box.innerHTML = head + '<ul class="list-group">' + entities.map((e, i) => {
      const loc = isLocated(e);
      const m = e.match || {};
      return `<li class="list-group-item d-flex align-items-center gap-2 py-1" data-i="${i}" title="${esc(e.context || '')}">
        <span class="flex-grow-1">${esc(truncate(e.name, 40))}
          ${e.count ? `<span class="text-muted small">· ${e.count}×</span>` : ''}
          ${loc ? `<span class="text-success small ms-1">→ ${esc(truncate(m.title || e.name, 30))}${m.ambiguous ? ' <span class="badge bg-warning text-dark">ambiguous</span>' : ''}</span>`
                : '<span class="text-muted small ms-1">not located</span>'}</span>
        ${loc ? '<button type="button" class="btn btn-sm btn-outline-primary wb-ner-add">Add</button>' : ''}
      </li>`;
    }).join('') + '</ul>';
    if (located.length) el('wb-ner-addall').addEventListener('click', () => { located.forEach(addOne); done(located.length); });
    box.querySelectorAll('.wb-ner-add').forEach((b) => b.addEventListener('click', () => {
      const e = entities[+b.closest('li').dataset.i]; addOne(e); b.disabled = true; b.textContent = 'Added';
    }));
  }

  function addOne(e) {
    if (!isLocated(e)) return;
    onAdd({ id: e.match.id, title: e.match.title || e.name, lnglat: [e.match.lng, e.match.lat] });
  }
  function done(n) { setMsg(`<span class="text-success">Added ${n} place${n === 1 ? '' : 's'} to your collection.</span>`); }

  async function find() {
    const text = (el('wb-ner-text').value || '').trim();
    if (!text) { setMsg('<span class="text-muted">Paste or load some text first.</span>'); return; }
    const btn = el('wb-ner-btn'); btn.disabled = true;
    setMsg('<span class="text-muted"><i class="fas fa-spinner fa-spin me-1"></i>Finding place names…</span>');
    try {
      const res = await nerCall(text);
      if (res.status !== 200 || !res.data || !Array.isArray(res.data.entities)) {
        setMsg(`<span class="text-danger">${esc((res.data && res.data.error) || 'Extraction failed — please try again.')}</span>`); return;
      }
      entities = res.data.entities;
      if (!entities.length) { setMsg('<span class="text-warning">No place names were found in that text.</span>'); renderResults(); return; }
      const n = res.data.reconciled || entities.filter(isLocated).length;
      setMsg(`<span class="text-muted">Found ${entities.length}, located ${n} against WHG.</span>`);
      renderResults();
    } catch (err) {
      setMsg('<span class="text-danger">Extraction failed — check your connection and try again.</span>');
    } finally { btn.disabled = false; }
  }

  el('wb-ner-btn').addEventListener('click', find);
  el('wb-ner-file').addEventListener('change', async (ev) => {
    const f = ev.target.files && ev.target.files[0]; if (!f) return;
    setMsg('<span class="text-muted"><i class="fas fa-spinner fa-spin me-1"></i>Reading file…</span>');
    try { el('wb-ner-text').value = await readFile(f); setMsg(''); }
    catch (_) { setMsg('<span class="text-danger">Could not read that file.</span>'); }
    ev.target.value = '';
  });
  el('wb-ner-gdoc-btn').addEventListener('click', async () => {
    const url = (el('wb-ner-gdoc').value || '').trim();
    if (!url) { setMsg('<span class="text-muted">Paste a Google Doc link first.</span>'); return; }
    setMsg('<span class="text-muted"><i class="fas fa-spinner fa-spin me-1"></i>Fetching the document…</span>');
    const res = await importGDoc(url);
    if (res.status !== 200 || !res.data || res.data.text == null) {
      setMsg(`<span class="text-danger">${esc((res.data && res.data.error) || 'Could not fetch that document.')}</span>`); return;
    }
    el('wb-ner-text').value = res.data.text; el('wb-ner-gdoc').value = '';
    setMsg('<span class="text-success">Loaded — now click “Find places in this text”.</span>');
  });
}
