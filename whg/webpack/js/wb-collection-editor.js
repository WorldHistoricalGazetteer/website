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

import { el, esc, truncate, debounce, csrf, openStore, statusBadge, serverBridge, mountAccordion, haversineKm, centroid, WB_COLORS } from './wb-shell.js';
import { mountCollab } from './wb-collab.js';
import { mountNer } from './wb-ner.js';

const RECON_ENDPOINT = '/reconcile';
const SEARCH_LIMIT = 8;

// MapLibre + the numbered-marker/full map are a heavy lazy chunk (same as Map your Data): only paid
// for when a place with coordinates is actually shown. recon-map self-loads the maplibre shims.
let ReconMap = null;
const loadReconMap = async () => (ReconMap || (ReconMap = await import(/* webpackChunkName: "recon-map" */ './recon-map.js')));
const colorDot = (i) => `<span class="badge me-1" style="background:${WB_COLORS[i % WB_COLORS.length]};color:#fff;">${i + 1}</span>`;

// A hover tooltip for a reconciliation candidate: its description plus any alternative names.
function candidateTip(c) {
  const bits = [];
  if (c.description) bits.push(c.description);
  if (c.alt_names && c.alt_names.length) bits.push('Also known as: ' + c.alt_names.slice(0, 12).join(', '));
  return bits.join('\n');
}

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
    // Colocation: once the collection has located members, rank suggestions by proximity to their
    // centroid (a collection's places tend to cluster). Candidates without coordinates fall to the end.
    const anchor = centroid(project.places.map((p) => (p.lng != null ? [p.lng, p.lat] : null)));
    if (anchor) results.sort((a, b) => haversineKm(a.repr_point, anchor) - haversineKm(b.repr_point, anchor));
    const note = anchor ? '<div class="text-muted small mb-1"><i class="fas fa-location-crosshairs me-1"></i>ranked by nearness to your other places</div>' : '';
    box.innerHTML = note + results.map((c, i) => {
      const km = anchor && c.repr_point ? haversineKm(c.repr_point, anchor) : null;
      const dist = km != null ? `<span class="text-muted small ms-1">· ${km < 1 ? '<1' : Math.round(km)} km</span>` : '';
      return `<button type="button" class="btn btn-sm btn-outline-secondary text-start d-block w-100 mb-1 wb-hit" data-i="${i}" title="${esc(candidateTip(c))}">
        ${c.repr_point ? colorDot(i) : ''}${esc(truncate(c.name, 44))}
        ${isWhg(c.id) ? '' : '<span class="badge bg-warning text-dark ms-1" title="Not a WHG-indexed place — cannot be published to a collection yet">external</span>'}
        ${c.description ? `<span class="text-muted small ms-1">${esc(truncate(c.description, 34))}</span>` : ''}${dist}
      </button>`;
    }).join('');
    box.querySelectorAll('.wb-hit').forEach((b) => b.addEventListener('click', () => addCandidate(results, +b.dataset.i, box)));
    renderCandMap(results);
  }

  function addCandidate(results, i, box) {
    const c = results[i];
    addPlace({ id: c.id, title: c.name, tip: candidateTip(c), lnglat: c.repr_point });
    el('wb-search').value = ''; if (box) box.innerHTML = '';
    hideCandMap();
  }

  // Numbered-marker map of the current search candidates — matches the list numbers/colours (as in
  // Map your Data). Clicking a pin adds that place. Only candidates with coordinates are shown.
  async function renderCandMap(results) {
    const container = el('wb-cand-map');
    if (!container) return;
    const pts = results.map((c, i) => (c.repr_point ? { ci: i, lon: c.repr_point[0], lat: c.repr_point[1], name: c.name } : null)).filter(Boolean);
    if (!pts.length) { hideCandMap(); return; }
    container.classList.remove('d-none');
    const mod = await loadReconMap();
    mod.renderReviewMap(container, pts, null, { onAccept: (ci) => addCandidate(results, ci, el('wb-search-results')) });
    // The container just un-hid (display:none → real size); MapLibre must be resized AFTER layout
    // settles or it paints blank on first show. Nudge across a few frames to cover layout + tile load.
    kickResize(mod.resizeReviewMap);
  }
  function hideCandMap() { const c = el('wb-cand-map'); if (c) c.classList.add('d-none'); }

  // Fire a map resize now, next frame, and a couple of short delays later — covers the case where the
  // map's container only gains its real size a tick after becoming visible (accordion/hidden → shown).
  function kickResize(fn) {
    if (typeof fn !== 'function') return;
    const go = () => { try { fn(); } catch (_) { /* map torn down */ } };
    requestAnimationFrame(go);
    setTimeout(go, 200);
    setTimeout(go, 600);
  }

  // Assembled-collection map (its own step) — all located members as a clustered/labelled map.
  async function renderFullMap() {
    const container = el('wb-full-map'); if (!container) return;
    const located = project.places.filter((p) => p.lng != null && p.lat != null);
    const note = el('wb-full-map-note');
    if (note) note.textContent = located.length
      ? `${located.length} of ${project.places.length} ${memberWord}s mapped.`
      : `No mapped ${memberWord}s yet — add some in step 2.`;
    const fc = { type: 'FeatureCollection', features: located.map((p, i) => ({
      type: 'Feature', geometry: { type: 'Point', coordinates: [p.lng, p.lat] },
      properties: { title: `${i + 1}. ${p.title || p.id}`, match: true } })) };
    const mod = await loadReconMap();
    mod.renderFullMap(container, fc);
    kickResize(mod.resizeFullMap);
  }

  function addPlace(p) {
    if (project.places.some((x) => x.id === p.id)) return;
    const ll = p.lnglat && p.lnglat[0] != null ? p.lnglat : null;
    project.places.push({ id: p.id, title: p.title, note: '', tip: p.tip || '',
                          lng: ll ? ll[0] : null, lat: ll ? ll[1] : null });
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
    list.innerHTML = project.places.map((p, i) => `
      <li class="list-group-item d-flex align-items-start gap-2" data-i="${i}">
        <span class="badge mt-1" style="background:${WB_COLORS[i % WB_COLORS.length]};color:#fff;">${i + 1}</span>
        <span class="flex-grow-1">
          <span class="fw-semibold" title="${esc(p.tip || '')}">${esc(p.title || p.id)}</span>
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
    if (r.status === 409) { flash(out, (r.data && r.data.error) || 'This collection changed since you started editing.', 'warning'); return; }
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
    mountAccordion('wb-pane-about', (id) => { if (id === 'wb-pane-map') renderFullMap(); });
    mountCollab({ bridge, getSnapshot: snapshot, getTitle: () => project.title.trim() || 'Untitled',
                  container: el('wb-collab-body'), onSaved: () => status.synced() });
    if (el('wb-ner-body')) mountNer({ container: el('wb-ner-body'), onAdd: (p) => addPlace(p) });
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
        project = { title: s.title || '', description: s.description || '', keywords: s.keywords || [],
                    places: (s.places || []).map((p) => ({ id: p.id, title: p.title || p.id, note: p.note || '',
                      tip: p.tip || '', lng: p.lng != null ? p.lng : null, lat: p.lat != null ? p.lat : null })) };
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
