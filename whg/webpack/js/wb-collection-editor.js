// wb-collection-editor.js — shared editor core for place-based Workbench collections.
//
// Both the Place Collection (plan §4.1) and the Itinerary (§4.3 — a sequenced Place Collection) are
// the same activity: curate WHG places with notes, then publish into collection.Collection. This
// module is that shared core; wb-place-collection.js and wb-itinerary.js are thin entries that call
// mountCollectionEditor() with a small config. "Generalise rather than fork" (plan §3).
//
// The only real difference is emphasis: an Itinerary's ORDER is meaningful (it's a journey), so its
// config sets sequenced=true (drives copy + the ordinal styling). The backend derives "sequenced"
// from the project's doc_type, so publishing needs nothing special here beyond sending doc_type.

import { el, esc, truncate, debounce, csrf, openStore, statusBadge, serverBridge } from './wb-shell.js';

const RECON_ENDPOINT = '/reconcile';
const SEARCH_LIMIT = 8;

// Is a reconciliation candidate id a WHG-indexed (publishable) place? Mirrors publish._local_place_pk:
// whg:/place:/bare-digit ⇒ local; any real source namespace (gn:, wd:, …) ⇒ external.
const isWhg = (id) => {
  const parts = String(id || '').split(':');
  const ns = parts.length >= 2 ? parts[parts.length - 2].toLowerCase() : 'whg';
  return /^\d+$/.test(parts[parts.length - 1]) && (ns === 'whg' || ns === 'place');
};

export function mountCollectionEditor(cfg) {
  // cfg: { docType, dbName, browse(id)->url, sequenced, memberWord ('place'|'stop') }
  const store = openStore(cfg.dbName);
  const bridge = serverBridge(cfg.docType);
  const memberWord = cfg.memberWord || 'place';
  let status = null;
  let project = { title: '', description: '', keywords: [], places: [] };

  const snapshot = () => ({
    title: project.title.trim() || 'Untitled',
    description: project.description || '',
    keywords: project.keywords || [],
    places: project.places.map((p, i) => ({ id: p.id, title: p.title, note: p.note || undefined, seq: i })),
  });

  // ── persistence ────────────────────────────────────────────────────────────
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

  // ── metadata ─────────────────────────────────────────────────────────────
  function bindMeta() {
    el('wb-title').addEventListener('input', (e) => { project.title = e.target.value; touched(); });
    el('wb-desc').addEventListener('input', (e) => { project.description = e.target.value; touched(); });
    el('wb-keywords').addEventListener('input', (e) => {
      project.keywords = e.target.value.split(',').map((k) => k.trim()).filter(Boolean);
      touched();
    });
  }
  function fillMeta() {
    el('wb-title').value = project.title || '';
    el('wb-desc').value = project.description || '';
    el('wb-keywords').value = (project.keywords || []).join(', ');
  }

  // ── search + add ───────────────────────────────────────────────────────────
  async function search() {
    const box = el('wb-search-results');
    const q = (el('wb-search').value || '').trim();
    if (!q) { box.innerHTML = ''; return; }
    box.innerHTML = '<span class="text-muted small"><i class="fas fa-spinner fa-spin me-1"></i>searching…</span>';
    let data;
    try {
      const res = await fetch(RECON_ENDPOINT, {
        method: 'POST', credentials: 'same-origin',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'X-CSRFToken': csrf() },
        body: 'queries=' + encodeURIComponent(JSON.stringify({ q0: { query: q, type: 'place', limit: SEARCH_LIMIT } })),
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      data = await res.json();
    } catch (err) { box.innerHTML = `<span class="text-danger small">Search failed: ${esc(err.message)}</span>`; return; }
    const results = (data.q0 && data.q0.result) || [];
    if (!results.length) { box.innerHTML = '<span class="text-muted small">No places found in WHG.</span>'; return; }
    box.innerHTML = results.map((c, i) => `<button type="button" class="btn btn-sm btn-outline-secondary text-start d-block w-100 mb-1 wb-hit" data-i="${i}">
        ${esc(truncate(c.name, 48))}
        ${isWhg(c.id) ? '' : '<span class="badge bg-warning text-dark ms-1" title="Not a WHG-indexed place — cannot be published to a collection yet">external</span>'}
        ${c.description ? `<span class="text-muted small ms-1">${esc(truncate(c.description, 34))}</span>` : ''}
      </button>`).join('');
    box.querySelectorAll('.wb-hit').forEach((b) => b.addEventListener('click', () => {
      const c = results[+b.dataset.i];
      addPlace({ id: c.id, title: c.name });
      el('wb-search').value = ''; box.innerHTML = '';
    }));
  }

  function addPlace(p) {
    if (project.places.some((x) => x.id === p.id)) return;
    project.places.push({ id: p.id, title: p.title, note: '' });
    renderPlaces(); touched();
  }

  function move(i, d) {
    const j = i + d;
    if (j < 0 || j >= project.places.length) return;
    [project.places[i], project.places[j]] = [project.places[j], project.places[i]];
    renderPlaces(); touched();
  }

  function renderPlaces() {
    const list = el('wb-places');
    el('wb-places-count').textContent = project.places.length;
    if (!project.places.length) {
      list.innerHTML = `<li class="text-muted small list-group-item">No ${memberWord}s yet — search above to add some.</li>`;
      return;
    }
    // Itinerary ordinals get a directional tint so the journey order reads clearly.
    const badgeClass = cfg.sequenced ? 'bg-primary' : 'bg-secondary';
    list.innerHTML = project.places.map((p, i) => `
      <li class="list-group-item d-flex align-items-start gap-2" data-i="${i}">
        <span class="badge ${badgeClass} mt-1">${i + 1}</span>
        <span class="flex-grow-1">
          <span class="fw-semibold">${esc(p.title || p.id)}</span>
          ${isWhg(p.id) ? '' : '<span class="badge bg-warning text-dark ms-1">external</span>'}
          <input class="form-control form-control-sm mt-1 wb-note" placeholder="note (optional)" value="${esc(p.note || '')}">
        </span>
        <span class="btn-group btn-group-sm" role="group">
          <button type="button" class="btn btn-outline-secondary wb-up" title="Move up">↑</button>
          <button type="button" class="btn btn-outline-secondary wb-down" title="Move down">↓</button>
          <button type="button" class="btn btn-outline-danger wb-del" title="Remove">✕</button>
        </span>
      </li>`).join('');
    list.querySelectorAll('li').forEach((li) => {
      const i = +li.dataset.i;
      li.querySelector('.wb-note').addEventListener('input', (e) => { project.places[i].note = e.target.value; touched(); });
      li.querySelector('.wb-up').addEventListener('click', () => move(i, -1));
      li.querySelector('.wb-down').addEventListener('click', () => move(i, 1));
      li.querySelector('.wb-del').addEventListener('click', () => { project.places.splice(i, 1); renderPlaces(); touched(); });
    });
  }

  // ── save + publish ───────────────────────────────────────────────────────────
  async function saveToAccount() {
    status.saving();
    const r = bridge.id ? await bridge.push(snapshot()) : await bridge.create(snapshot(), project.title, null);
    if (r.ok) { status.synced(); flash(el('wb-save-result'), 'Saved to your account.', 'success'); }
    else { status.error(); flash(el('wb-save-result'), (r.data && r.data.error) || 'Save failed.', 'danger'); }
  }

  async function publish() {
    const out = el('wb-publish-result');
    if (!project.places.length) { flash(out, `Add at least one ${memberWord} before publishing.`, 'warning'); return; }
    flash(out, 'Publishing…', 'secondary');
    if (!bridge.id) {
      const c = await bridge.create(snapshot(), project.title, null);
      if (!c.ok) { flash(out, (c.data && c.data.error) || 'Could not save before publishing.', 'danger'); return; }
    } else {
      await bridge.push(snapshot());
    }
    const r = await bridge.publish();
    if (!r.ok) { flash(out, (r.data && r.data.error) || 'Publishing failed.', 'danger'); return; }
    const d = r.data;
    const link = `<a href="${cfg.browse(d.collection_id)}" target="_blank" rel="noopener">View your published ${cfg.sequenced ? 'itinerary' : 'collection'} →</a>`;
    const warn = (d.unresolved && d.unresolved.length)
      ? `<div class="small text-warning mt-1">${d.unresolved.length} ${memberWord}(s) could not be added because they aren’t WHG-indexed yet
          (contribute them as gazetteer records first). Added ${d.added}.</div>` : '';
    out.innerHTML = `<div class="alert alert-success py-2 mb-0">Published ${d.added} ${memberWord}(s). ${link}${warn}</div>`;
    status.synced();
  }

  function flash(node, msg, kind) {
    if (!node) return;
    node.innerHTML = msg ? `<div class="alert alert-${kind} py-2 mb-0">${esc(msg)}</div>` : '';
  }

  async function clearDraft() {
    await store.clear();
    project = { title: '', description: '', keywords: [], places: [] };
    fillMeta(); renderPlaces(); status.saved();
    el('wb-publish-result').innerHTML = ''; el('wb-save-result').innerHTML = '';
  }

  // ── boot ─────────────────────────────────────────────────────────────────────
  async function init() {
    status = statusBadge(el('wb-status'));
    bindMeta();
    el('wb-search-btn').addEventListener('click', search);
    el('wb-search').addEventListener('keydown', (e) => { if (e.key === 'Enter') { e.preventDefault(); search(); } });
    el('wb-save-btn').addEventListener('click', saveToAccount);
    el('wb-publish-btn').addEventListener('click', publish);
    el('wb-clear-btn').addEventListener('click', () => { if (confirm('Clear this draft from your browser? This cannot be undone.')) clearDraft(); });

    const pid = new URLSearchParams(location.search).get('project');
    if (pid) {
      const r = await bridge.load(pid);
      if (r.ok && r.data && r.data.snapshot) {
        const s = r.data.snapshot;
        project = { title: s.title || '', description: s.description || '', keywords: s.keywords || [], places: (s.places || []).map((p) => ({ id: p.id, title: p.title || p.id, note: p.note || '' })) };
      }
    } else {
      const local = await store.load();
      if (local) project = Object.assign(project, local);
    }
    fillMeta(); renderPlaces();
    status.saved();
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
}
