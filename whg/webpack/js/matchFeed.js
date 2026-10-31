// The contributor feed, "Link your records" (place#334 4c/4d/4i, Phase 2: place#338).
//
// One record at a time: its suggestions, best first, each with the sentence saying why it was
// suggested and what disagrees. Decisions go to /datasets/<id>/matches/review/, which writes Links
// through the same rules as the Link sheet. Words on screen: "Link", "Same place", "Not the same
// place", "None of these fits", "Not sure yet". No bulk confirm in this phase.
//
// "Which one, if any?" (the ambiguous tier, 4c): one decision per record, so nothing is linked until
// a suggestion is chosen (no default to the first), and "Not the same place" there offers, as a
// second step, to record it as a Link others can see. Elsewhere it only sets the suggestion aside.
// "Not sure yet" moves a record to the Later tab, which is always reachable and never expires.
//
// The map (matchFeedMap.js) is a lazy chunk: if it cannot load, the feed works without it.
//
// window.__whgMatchFeed.ready is the page's readiness flag (browser tests wait on it, not on timers);
// window.__whgMatchFeed.map.renders moves each time the map panel is redrawn.
import '../css/matchFeed.css';
import { constraintChipsHTML, bindConstraintChips, esc } from './constraintChips';

const root = document.getElementById('match_feed');
const S = {
  tab: root.dataset.tab, records: [], idx: 0, offset: 0, next: null, selected: new Set(),
  undo: [], relaxed: new Set(), searchResults: null, searchChips: [], distinctAsk: null,
};
const NOTES = {
  confident: 'The suggestion and your record agree on the name and at least one other thing. Check before you confirm: a perfect name can still be a lake or a station named after the place.',
  ambiguous: 'Which one, if any? More than one place fits, or only the name agrees. Choose the one that is the same place (1–9), or say None of these fits.',
  weak: 'Weak suggestions: optional. Looking is up to you.',
  none: 'Nothing usable was found. That is normal; search by hand if you know a spelling, or leave it.',
  later: 'Records you marked "Not sure yet". They stay here until you decide; nothing here expires.',
  unqueried: 'The search service did not answer for these records yet. They will be tried again; this is not "nothing found".',
};
window.__whgMatchFeed = { ready: false, tab: S.tab, saves: 0, map: { state: 'idle', renders: 0, basemap: null } };

function csrf() {
  const m = document.querySelector('meta[name="csrf-token"]');
  return m ? m.content : '';
}

async function getJSON(url) {
  const r = await fetch(url, { credentials: 'same-origin', headers: { Accept: 'application/json' } });
  return { ok: r.ok, status: r.status, data: await r.json().catch(() => ({})) };
}

async function postJSON(url, body) {
  const r = await fetch(url, {
    method: 'POST', credentials: 'same-origin',
    headers: { 'Content-Type': 'application/json', 'X-CSRFToken': csrf(), Accept: 'application/json' },
    body: JSON.stringify(body),
  });
  return { ok: r.ok, status: r.status, data: await r.json().catch(() => ({})) };
}

function announce(text) {
  const live = document.getElementById('mf_live');
  if (live) live.textContent = text;
}

function updateCounts(counts) {
  if (!counts) return;
  document.querySelectorAll('[data-count-for]').forEach((b) => {
    const n = counts[b.dataset.countFor];
    if (n !== undefined) b.textContent = String(n);
  });
}

function years(mm) {
  if (!Array.isArray(mm) || mm.length < 2 || (mm[0] == null && mm[1] == null)) return '';
  return `${mm[0] ?? ''}–${mm[1] ?? ''}`;
}

function nsLabel(ns) {
  return { gn: 'GeoNames', wd: 'Wikidata', tgn: 'Getty TGN', osm: 'OpenStreetMap', ohm: 'OpenHistoricalMap',
    whg: 'WHG contribution', pl: 'Pleiades' }[ns] || ns;
}

function candHTML(c, i, opts) {
  const card = c.card || {};
  const sel = S.selected.has(i);
  const cls = ['mf-cand', sel ? 'mf-cand--selected' : '', c.status === 'accepted' ? 'mf-cand--accepted' : '',
    c.status === 'rejected' ? 'mf-cand--rejected' : ''].join(' ');
  const facts = [
    `<span class="badge text-bg-light border">${esc(nsLabel(card.namespace || c.namespace))}</span>`,
    card.types && card.types.length ? `<span>${esc(card.types.join(', '))}</span>` : '',
    c.km != null ? `<span>${c.km < 1 ? 'under 1 km' : `${Math.round(c.km)} km`}</span>` : '',
    years(card.temporal_range) ? `<span>${esc(years(card.temporal_range))}</span>` : '',
    card.ccodes && card.ccodes.length ? `<span>${esc(card.ccodes.join(', '))}</span>` : '',
    c.location_withheld ? '<span>location not available to you</span>'
      : (c.location === null ? '<span>no location</span>' : ''),
  ].filter(Boolean).join(' · ');
  const other = (card.names || []).map((n) => n.label).filter((l) => l && l !== card.title).slice(0, 3);
  const why = (c.explanation && c.explanation.sentence) || '';
  const warn = c.demoted
    ? '<div class="small mf-warn"><i class="fas fa-triangle-exclamation me-1"></i>This looks like a feature named after a place (a building, lake, station…), not the place itself.</div>' : '';
  const done = c.status === 'accepted' ? '<span class="badge bg-success ms-1">linked</span>'
    : c.status === 'rejected' ? '<span class="badge bg-secondary ms-1">not this one</span>' : '';
  const actions = opts.readOnly ? '' : `
      <div class="mt-1 d-flex flex-wrap gap-1">
        <button type="button" class="btn btn-sm btn-success" data-act="link-one" data-i="${i}"
                aria-label="Same place: ${esc(card.title || c.title)}">Same place</button>
        ${opts.hand ? '' : `<button type="button" class="btn btn-sm btn-outline-secondary" data-act="reject-one" data-i="${i}"
                aria-label="Not the same place: ${esc(card.title || c.title)}">Not the same place</button>`}
      </div>`;
  return `<li class="${cls}" data-i="${i}" data-candidate="${esc(c.candidate_id)}">
      <div><span class="mf-key">${i + 1}</span><span class="fw-semibold">${esc(card.title || c.title)}</span>
        <span class="text-secondary small ms-1">${esc(c.candidate_id)}</span>${done}</div>
      <div class="small text-secondary">${facts}</div>
      ${other.length ? `<div class="small text-secondary">also: ${other.map(esc).join(', ')}</div>` : ''}
      ${why ? `<div class="mf-why">${esc(why)}</div>` : ''}
      ${warn}${actions}
      ${!opts.hand && S.distinctAsk === i ? `<div class="mt-1 small alert alert-secondary py-1 mb-0 mf-distinct-ask" role="group" aria-label="Also record it">
        Also record that these are not the same place? It becomes a Link others can see.
        <button type="button" class="btn btn-sm btn-outline-dark ms-1" data-act="distinct-yes" data-i="${i}">Record it</button>
        <button type="button" class="btn btn-sm btn-link" data-act="distinct-no">No thanks</button></div>` : ''}
    </li>`;
}

function current() { return S.records[S.idx] || null; }

function renderQueue() {
  const q = document.getElementById('mf_queue');
  q.innerHTML = S.records.map((r, i) => `<li class="list-group-item list-group-item-action${i === S.idx ? ' active' : ''}"
      data-qi="${i}" aria-current="${i === S.idx ? 'true' : 'false'}">${esc(r.title)}
      ${r.state !== 'unreviewed' && r.state !== 'later' ? `<span class="badge bg-light text-dark ms-1">${esc(r.state === 'linked' ? 'linked' : 'done')}</span>` : ''}</li>`).join('')
    || '<li class="list-group-item text-secondary">Nothing in this tab.</li>';
  document.getElementById('mf_more').hidden = S.next == null;
}

function renderRecord() {
  const box = document.getElementById('mf_record');
  const r = current();
  S.searchResults = null;
  if (!r) { box.innerHTML = '<p class="text-secondary" id="mf_empty">Nothing to review in this tab.</p>'; renderMap(); return; }
  const cands = r.candidates || [];
  const tierBadge = r.tier === 'unqueried'
    ? `<span class="badge bg-warning text-dark ms-1">not yet checked${r.last_failure ? ` (${esc(r.last_failure)})` : ''}</span>` : '';
  box.innerHTML = `
    <div class="d-flex flex-wrap justify-content-between align-items-baseline gap-2">
      <h2 class="h5 mb-1" id="mf_record_title">${esc(r.title)}${tierBadge}</h2>
      <div class="small text-secondary">${esc(r.src_id)}${years(r.minmax) ? ` · ${esc(years(r.minmax))}` : ''}${r.has_geometry ? '' : ' · no location'}</div>
    </div>
    <ol class="list-unstyled mt-2 mb-2" id="mf_cands">${cands.map((c, i) => candHTML(c, i, {})).join('')
      || '<li class="text-secondary">No suggestions for this record.</li>'}</ol>
    <details class="small mb-2" id="mf_advanced"><summary>Advanced</summary>
      <div class="form-check form-check-inline mt-1">
        <input class="form-check-input" type="radio" name="mf_relation" id="mf_rel_close" value="closeMatch" checked>
        <label class="form-check-label" for="mf_rel_close">Same place</label></div>
      <div class="form-check form-check-inline">
        <input class="form-check-input" type="radio" name="mf_relation" id="mf_rel_exact" value="exactMatch">
        <label class="form-check-label" for="mf_rel_exact">Same place, exactly</label></div>
    </details>
    ${mustChoose(r) ? '<p class="small mb-1 mf-ambiguous-hint" id="mf_choose_hint">Which one, if any? Choose a suggestion first (1–9), or say None of these fits.</p>' : ''}
    <div class="d-flex flex-wrap gap-2 align-items-center">
      <button type="button" class="btn btn-sm btn-primary" data-act="link" ${cands.length && !mustChoose(r) ? '' : 'disabled'}>Same place <kbd>Enter</kbd></button>
      <button type="button" class="btn btn-sm btn-outline-warning" data-act="none">None of these fits <kbd>n</kbd></button>
      <button type="button" class="btn btn-sm btn-outline-secondary" data-act="later">Not sure yet <kbd>l</kbd></button>
      <button type="button" class="btn btn-sm btn-outline-secondary" data-act="undo" ${S.undo.length ? '' : 'disabled'}>Undo <kbd>u</kbd></button>
      <span class="ms-auto"></span>
      <button type="button" class="btn btn-sm btn-outline-secondary" data-act="prev" aria-label="Previous record">&larr;</button>
      <button type="button" class="btn btn-sm btn-outline-secondary" data-act="next" aria-label="Next record">&rarr;</button>
    </div>
    <input type="text" class="form-control form-control-sm mt-2" id="mf_note" maxlength="2000"
           placeholder="Note (optional): why you decided" aria-label="Note">
    <div class="mt-3" id="mf_search">
      <div class="input-group input-group-sm">
        <input type="text" class="form-control" id="mf_search_q" value="${esc(r.title)}" aria-label="Search for another name">
        <button type="button" class="btn btn-outline-secondary" data-act="search">Search <kbd>s</kbd></button>
      </div>
      <div id="mf_search_cx"></div>
      <ol class="list-unstyled mt-2" id="mf_search_results"></ol>
    </div>`;
  renderMap();
}

// ── The map panel ─────────────────────────────────────────────────────────────────────────────
// What is placed: the record's point, every suggestion that has one (the chosen one(s) stand out:
// the selection, else the first, which is what Enter would link outside "Which one, if any?").
// Nothing is dropped silently: suggestions without a point are counted in the caption.
let feedMap = null;
let mapLoading = null;
let mapFailed = false;

function mapView(r) {
  const cands = r.candidates || [];
  const focus = S.selected.size ? S.selected : new Set(cands.length ? [0] : []);
  const points = [];
  const missing = [];
  const withheld = [];
  cands.forEach((c, i) => {
    if (Array.isArray(c.location)) points.push({ i, title: (c.card && c.card.title) || c.title, location: c.location, focus: focus.has(i), status: c.status });
    else if (c.location_withheld) withheld.push(i + 1);
    else missing.push(i + 1);
  });
  return { record: Array.isArray(r.location) ? { title: r.title, location: r.location } : null, points, missing, withheld };
}

function mapCaption(r, v) {
  const parts = [];
  if (!v.record) parts.push('<strong>Your record has no location</strong>; only its suggestions are shown.');
  else parts.push('★ your record');
  if (v.points.length) parts.push('numbers: suggestions, as in the list; the one you chose (else the first) is larger, the others grey');
  if (v.missing.length) parts.push(`not on the map (no location): ${v.missing.join(', ')}`);
  if (v.withheld.length) parts.push(`not on the map (location not available to you): ${v.withheld.join(', ')}`);
  if (window.__whgMatchFeed.map.basemap === 'blank') parts.push('the background map could not be loaded');
  return parts.join(' · ');
}

function mapDone(state, v) {
  const m = window.__whgMatchFeed.map;
  m.state = state;
  m.record = v && v.record ? v.record.location : null;
  m.points = v ? v.points.map((p) => ({ i: p.i, focus: p.focus, location: p.location })) : [];
  m.renders += 1;
}

function showMapFailed() {
  mapFailed = true;
  const el = (id) => document.getElementById(id);
  el('mf_map').hidden = true;
  el('mf_map_none').hidden = true;
  el('mf_map_failed').hidden = false;
  el('mf_map_caption').textContent = '';
  mapDone('unavailable', null);
}

function ensureMap() {
  if (feedMap) return Promise.resolve(feedMap);
  if (!mapLoading) {
    mapLoading = import(/* webpackChunkName: "match-feed-map" */ './matchFeedMap.js').then((mod) => {
      feedMap = mod.createFeedMap(document.getElementById('mf_map'), {
        onPick: (i) => {
          if (S.selected.has(i)) S.selected.delete(i); else S.selected.add(i);
          renderRecord();
        },
        onBasemap: (state) => {
          window.__whgMatchFeed.map.basemap = state;
          if (state === 'blank') renderMap();
        },
      });
      window.__whgMatchFeed.map.instance = feedMap.map;
      return feedMap;
    });
  }
  return mapLoading;
}

function renderMap() {
  const panel = document.getElementById('mf_map_panel');
  if (!panel) return;
  const r = current();
  if (!r) { panel.hidden = true; mapDone('no-record', null); return; }
  panel.hidden = false;
  if (mapFailed) { mapDone('unavailable', null); return; }
  const v = mapView(r);
  const box = document.getElementById('mf_map');
  const none = document.getElementById('mf_map_none');
  const caption = document.getElementById('mf_map_caption');
  document.getElementById('mf_map_failed').hidden = true;
  if (!v.record && !v.points.length) {
    // A clear "no location" state, never an empty box.
    box.hidden = true;
    none.hidden = false;
    document.getElementById('mf_map_none_text').textContent = (r.candidates || []).length
      ? 'No location to show: this record has no location, and none of its suggestions has one either.'
      : 'No location to show: this record has no location.';
    caption.textContent = '';
    mapDone('no-location', v);
    return;
  }
  none.hidden = true;
  box.hidden = false;
  caption.innerHTML = mapCaption(r, v);
  const place = r.place;
  ensureMap().then((fm) => {
    const now = current();
    if (!now || now.place !== place) return;   // the person has moved on; that render will draw
    const latest = mapView(now);
    fm.update(latest);
    mapDone('ok', latest);
  }).catch((err) => {
    console.warn('The feed map could not be shown:', err);
    showMapFailed();
  });
}

function render() { renderQueue(); renderRecord(); window.__whgMatchFeed.record = current() ? current().place : null; }

async function load(tab, append) {
  if (!append) { S.offset = 0; S.records = []; S.idx = 0; S.distinctAsk = null; }
  S.tab = tab;
  window.__whgMatchFeed.tab = tab;
  document.querySelectorAll('.mf-tabs .nav-link').forEach((b) => {
    const on = b.dataset.tab === tab;
    b.classList.toggle('active', on);
    b.setAttribute('aria-selected', on ? 'true' : 'false');
  });
  document.getElementById('mf_tab_note').textContent = NOTES[tab] || '';
  const res = await getJSON(`${root.dataset.candidatesUrl}?tier=${encodeURIComponent(tab)}&offset=${S.offset}`);
  if (!res.ok) { document.getElementById('mf_record').innerHTML = `<p class="text-danger">Could not load records (${res.status}).</p>`; return; }
  S.records = S.records.concat(res.data.records || []);
  S.next = res.data.next;
  S.offset = res.data.next || S.offset;
  updateCounts(res.data.counts);
  const want = Number(root.dataset.record || 0);
  if (want && !append) {
    const i = S.records.findIndex((r) => r.place === want);
    if (i >= 0) S.idx = i;
    root.dataset.record = '';
  }
  S.selected.clear();
  render();
}

function relation() {
  const el = document.querySelector('input[name="mf_relation"]:checked');
  return el ? el.value : 'closeMatch';
}

function noteText() { const n = document.getElementById('mf_note'); return n ? n.value : ''; }

function remaining() {
  return S.records.filter((r) => r.state === 'unreviewed' || (S.tab === 'later' && r.state === 'later')).length;
}

async function review(body, after) {
  const r = current();
  if (!r) return;
  const res = await postJSON(root.dataset.reviewUrl, { place: r.place, note: noteText(), ...body });
  if (!res.ok) {
    announce(`Not saved: ${res.data.error || res.status}`);
    const box = document.getElementById('mf_record');
    box.insertAdjacentHTML('afterbegin', `<div class="alert alert-danger py-1 small" role="alert">Not saved: ${esc(res.data.error || String(res.status))}</div>`);
    return;
  }
  window.__whgMatchFeed.saves += 1;
  updateCounts(res.data.counts);
  S.undo.push(r.place);
  r.state = res.data.state;
  if (after) after(r);
  announce(body.action === 'later' ? `Moved to Later. ${Math.max(0, remaining())} to go in this tab.`
    : `Saved. ${Math.max(0, remaining())} to go in this tab.`);
  if (body.action !== 'reject') move(1); else render();
}

// "Which one, if any?": the record's own tier decides (it may be shown in the Later tab).
function mustChoose(r) { return !!r && r.tier === 'ambiguous' && S.selected.size === 0; }

function selectedCandidates(r) {
  const idx = S.selected.size ? [...S.selected] : (r.candidates.length && r.tier !== 'ambiguous' ? [0] : []);
  return idx.map((i) => r.candidates[i]).filter(Boolean);
}

function linkSelected() {
  const r = current();
  if (!r) return;
  if (mustChoose(r)) { announce('Which one, if any? Choose a suggestion first (1–9), or say None of these fits.'); return; }
  const cs = selectedCandidates(r);
  if (!cs.length) return;
  review({ action: 'link', candidates: cs.map((c) => c.id), relation: relation() },
    () => cs.forEach((c) => { c.status = 'accepted'; }));
}

// "Not the same place" sets the suggestion aside (never a statement others see). In the
// "Which one, if any?" tier the plan offers a second step: also record it as a Link.
function rejectOne(i) {
  const r = current();
  const c = r && r.candidates[i];
  if (!c) return;
  review({ action: 'reject', candidates: [c.id] }, () => {
    c.status = 'rejected';
    S.distinctAsk = r.tier === 'ambiguous' ? i : null;
  });
}

function recordDistinct(i) {
  const r = current();
  const c = r && r.candidates[i];
  S.distinctAsk = null;
  if (!c) { render(); return; }
  review({ action: 'reject', candidates: [c.id], distinct: true }, () => { c.status = 'rejected'; });
}

function move(d) {
  if (!S.records.length) return;
  S.idx = Math.max(0, Math.min(S.records.length - 1, S.idx + d));
  S.selected.clear();
  S.distinctAsk = null;
  render();
  const t = document.getElementById('mf_record_title');
  if (t) { t.tabIndex = -1; t.focus(); }
}

async function undo() {
  const place = S.undo.pop();
  if (place == null) return;
  const res = await postJSON(root.dataset.reviewUrl, { place, action: 'reopen' });
  if (!res.ok) { announce('Undo failed.'); return; }
  updateCounts(res.data.counts);
  const i = S.records.findIndex((r) => r.place === place);
  if (i >= 0) {
    S.records[i].state = 'unreviewed';
    (S.records[i].candidates || []).forEach((c) => { c.status = 'suggested'; });
    S.idx = i;
  }
  S.distinctAsk = null;
  announce('Undone.');
  render();
}

async function search() {
  const r = current();
  if (!r) return;
  const q = document.getElementById('mf_search_q').value;
  const url = `${root.dataset.searchUrl}?place=${r.place}&q=${encodeURIComponent(q)}&relax=${encodeURIComponent([...S.relaxed].join(','))}`;
  const out = document.getElementById('mf_search_results');
  out.innerHTML = '<li class="text-secondary small">Searching…</li>';
  const res = await getJSON(url);
  const cx = document.getElementById('mf_search_cx');
  S.searchChips = res.data.constraints || [];
  cx.innerHTML = constraintChipsHTML(S.searchChips, S.relaxed);
  if (!res.ok) { out.innerHTML = `<li class="text-danger small">Search failed (${res.status}).</li>`; return; }
  if (!res.data.answered) {
    out.innerHTML = '<li class="text-warning-emphasis small" id="mf_search_silent">The search service did not answer. Try again in a moment: this does not mean nothing exists.</li>';
    return;
  }
  S.searchResults = res.data.results || [];
  out.innerHTML = S.searchResults.map((c, i) => candHTML({ ...c, id: null, status: 'suggested' }, i, { hand: true }).replace(/data-act="link-one"/g, 'data-act="link-hand"'))
    .join('') || '<li class="text-secondary small">Nothing found for that name.</li>';
}

root.addEventListener('click', (e) => {
  const tabBtn = e.target.closest('.mf-tabs [data-tab]');
  if (tabBtn) { load(tabBtn.dataset.tab); return; }
  const qi = e.target.closest('[data-qi]');
  if (qi) { S.idx = Number(qi.dataset.qi); S.selected.clear(); S.distinctAsk = null; render(); return; }
  const b = e.target.closest('[data-act]');
  if (!b) {
    const li = e.target.closest('#mf_cands .mf-cand');
    if (li) { const i = Number(li.dataset.i); if (S.selected.has(i)) S.selected.delete(i); else S.selected.add(i); renderRecord(); }
    return;
  }
  const act = b.dataset.act;
  if (act === 'link') linkSelected();
  else if (act === 'link-one') { S.selected = new Set([Number(b.dataset.i)]); linkSelected(); }
  else if (act === 'reject-one') rejectOne(Number(b.dataset.i));
  else if (act === 'distinct-yes') recordDistinct(Number(b.dataset.i));
  else if (act === 'distinct-no') { S.distinctAsk = null; renderRecord(); }
  else if (act === 'link-hand') {
    const c = S.searchResults && S.searchResults[Number(b.dataset.i)];
    if (c) review({ action: 'link', target: c.candidate_id, relation: relation() });
  } else if (act === 'none') review({ action: 'none' });
  else if (act === 'later') review({ action: 'later' });
  else if (act === 'undo') undo();
  else if (act === 'prev') move(-1);
  else if (act === 'next') move(1);
  else if (act === 'search') search();
  else if (act === 'regenerate') {
    postJSON(root.dataset.regenerateUrl, {}).then((res) => {
      const box = document.getElementById('mf_no_generation');
      if (box) box.textContent = res.ok ? 'Suggestions are being prepared. Come back in a while.' : `Could not start: ${res.data.error || res.status}`;
    });
  }
});

document.getElementById('mf_more').addEventListener('click', () => load(S.tab, true));

bindConstraintChips(root, () => S.searchChips, S.relaxed, () => search());

document.addEventListener('keydown', (e) => {
  const tag = (e.target && e.target.tagName) || '';
  if (/INPUT|TEXTAREA|SELECT/.test(tag) || e.metaKey || e.ctrlKey || e.altKey) {
    if (e.key === 'Enter' && e.target.id === 'mf_search_q') { e.preventDefault(); search(); }
    return;
  }
  // Enter or Space on a focused button is that button's own click; never also a shortcut.
  if ((e.key === 'Enter' || e.key === ' ') && /^(BUTTON|A|SUMMARY)$/.test(tag)) return;
  const r = current();
  if (/^[1-9]$/.test(e.key) && r) {
    const i = Number(e.key) - 1;
    if (i < r.candidates.length) {
      if (S.selected.has(i)) S.selected.delete(i); else S.selected.add(i);
      renderRecord();
    }
  } else if (e.key === 'Enter') { e.preventDefault(); linkSelected(); }
  else if (e.key === 'x' && S.selected.size === 1) rejectOne([...S.selected][0]);
  else if (e.key === 'n') review({ action: 'none' });
  else if (e.key === 'l') review({ action: 'later' });
  else if (e.key === 'u') undo();
  else if (e.key === 's') { e.preventDefault(); const q = document.getElementById('mf_search_q'); if (q) q.focus(); }
  else if (e.key === 'ArrowRight') move(1);
  else if (e.key === 'ArrowLeft') move(-1);
  else return;
});

load(S.tab).then(() => { window.__whgMatchFeed.ready = true; });
