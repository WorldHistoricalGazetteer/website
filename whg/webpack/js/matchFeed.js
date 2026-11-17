// The contributor feed, "Link your records" (place#334 4c/4d/4i, Phase 2: place#338; redesign 2026-11).
//
// One record at a time. Each suggestion is one compact row with three states: "Same place",
// "Not the same" and "Can't tell". Nothing starts decided, except in "Likely the same place",
// where the first suggestion starts as the same place (shown as provisional: Enter confirms it).
// Marking a row "Same place" decides NOTHING about the other rows: choosing one place is not a
// judgement of the rest, so they stay "Not marked" and are left as they were. Only what the person
// marks is ever written (each decision is sent with how it arose: explicit | default). One summary
// line says exactly what Save will write.
// Suggestions that are one place in two sources (the server's ``group``) sit together and each
// keeps its own decision and its own Link: nothing is merged.
//
// Decisions go to /datasets/<id>/matches/review/ (``decide``), which writes Links through the same
// rules as the Link sheet. Words on screen: "Link", "Same place", "Not the same place", "None of
// these fits", "Not sure yet". No bulk confirm.
//
// "Which one, if any?" (the ambiguous tier): nothing is linked until a row is marked.
// The map (matchFeedMap.js) is a lazy chunk: if it cannot load, the feed works without it. Clicking
// a map marker only moves the focus to its row; it never records a decision.
//
// window.__whgMatchFeed.ready is the page's readiness flag (browser tests wait on it, not on timers);
// .map.renders moves each time the map panel is redrawn; .regions says which regions are drawn
// (record, rows, map, bar) so a guided tour can wait on state, not on timers.
import '../css/matchFeed.css';
import { startMatchTour, hasSeenMatchTour } from './matchFeedTour';
import { constraintChipsHTML, bindConstraintChips, esc } from './constraintChips';

const root = document.getElementById('match_feed');
const S = {
  tab: root.dataset.tab, records: [], idx: 0, offset: 0, next: null,
  dec: new Map(),            // row index -> { d: 'same'|'not'|'unsure', src: 'explicit'|'prefilled'|'default' }
  focus: null,               // the row with the focus (roving tabindex, map highlight)
  undo: [], relaxed: new Set(), searchResults: null, handSel: null, searchChips: [],
  shortcuts: store('local', 'mf.shortcuts') === 'on',   // off until the person switches them on
  publish: store('session', 'mf.publish') === '1',
  note: '', relation: 'closeMatch',   // kept across redraws; both reset when the record changes
};
const NOTES = {
  confident: 'The suggestion and your record agree on the name and at least one other thing. Check before you confirm: a perfect name can still be a lake or a station named after the place.',
  ambiguous: 'More than one place fits, or only the name agrees. Mark the one that is the same place, or say None of these fits.',
  weak: 'Weak suggestions: optional. Looking is up to you.',
  none: 'Nothing usable was found. That is normal; search by hand if you know a spelling, or leave it.',
  later: 'Records you marked "Not sure yet". They stay here until you decide; nothing here expires.',
  unqueried: 'The search service has not answered for these records yet. They will be tried again; this is not "nothing found".',
};
const FAILURES = {
  timeout: 'the search service was too slow to answer', unreachable: 'the search service could not be reached',
  disabled: 'suggestions were switched off for this run', error: 'the search service reported an error',
};
window.__whgMatchFeed = {
  ready: false, tab: S.tab, saves: 0, map: { state: 'idle', renders: 0, basemap: null, hand: [] },
  regions: { record: false, rows: false, map: false, bar: false },
};

function store(kind, key, value) {
  try {
    const s = kind === 'local' ? window.localStorage : window.sessionStorage;
    if (value === undefined) return s.getItem(key);
    s.setItem(key, value);
  } catch (_) { /* private window, blocked storage: the page works without it */ }
  return null;
}

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

function updateProgress(p) {
  const el = document.getElementById('mf_progress');
  if (!el || !p) return;
  el.textContent = `${p.reviewed} / ${p.records} reviewed · ${p.links} match${p.links === 1 ? '' : 'es'}`;
}

function years(mm) {
  if (!Array.isArray(mm) || mm.length < 2 || (mm[0] == null && mm[1] == null)) return '';
  return `${mm[0] ?? ''}–${mm[1] ?? ''}`;
}

function nsLabel(ns) {
  return { gn: 'GeoNames', wd: 'Wikidata', tgn: 'Getty TGN', osm: 'OpenStreetMap', ohm: 'OpenHistoricalMap',
    whg: 'WHG contribution', pl: 'Pleiades' }[ns] || ns;
}

// ── Plain words for feature codes (a historian should not have to read "PPLA3") ──────────────────
const GN_KIND = {
  PPL: 'populated place', PPLA: 'seat of a first-order division', PPLA2: 'seat of a second-order division',
  PPLA3: 'seat of a third-order division', PPLA4: 'seat of a fourth-order division', PPLC: 'capital',
  PPLG: 'seat of government', PPLH: 'historical populated place', PPLQ: 'abandoned populated place',
  PPLW: 'destroyed populated place', PPLX: 'section of a populated place', PPLL: 'populated locality',
  ADM1: 'first-order division', ADM2: 'second-order division', ADM3: 'third-order division',
  ADM4: 'fourth-order division', ADMD: 'administrative division', PCLI: 'country', PCLH: 'historical country',
  HSTS: 'historical site', RUIN: 'ruin', CSTL: 'castle', CH: 'church', MSTY: 'monastery', TMPL: 'temple',
  ISL: 'island', MT: 'mountain', RGN: 'region', STM: 'stream', LK: 'lake', AIRP: 'airport', RSTN: 'railway station',
  BLDG: 'building', PRK: 'park', FRT: 'fort', ANS: 'archaeological site', CMTY: 'cemetery', MUS: 'museum',
};
const WD_KIND = {
  Q515: 'city', Q486972: 'human settlement', Q532: 'village', Q3957: 'town', Q5119: 'capital city',
  Q1549591: 'big city', Q1093829: 'city of the United States', Q839954: 'archaeological site',
  Q2977: 'cathedral', Q6256: 'country', Q35657: 'US state', Q5393308: 'Buddhist temple',
  Q10864048: 'first-level administrative division',
};
// The first code we can put in words, or ''. The raw codes stay in a tooltip, never in the way.
function kindOf(card) {
  for (const raw of (card && card.types) || []) {
    const t = String(raw);
    const code = t.includes('.') ? t.split('.').pop() : t;
    if (GN_KIND[code]) return GN_KIND[code];
    if (WD_KIND[t]) return WD_KIND[t];
    const osm = t.match(/^(?:place|historic|amenity|building)=(.+)$/);
    if (osm) return osm[1].replace(/_/g, ' ');
  }
  return '';
}

function distText(km) {
  if (km == null) return '';
  return km < 1 ? 'under 1 km away' : `${Math.round(km).toLocaleString('en')} km away`;
}

// The "why" from the explanation's pieces, in words: what agrees, what does not. Distance is shown
// on its own and a code we cannot put in words is left out (its sentence would be a raw code).
function whyText(c) {
  const pieces = (c.explanation && c.explanation.pieces) || [];
  if (!pieces.length) return (c.explanation && c.explanation.sentence) || '';
  const say = (p) => {
    if (p.kind === 'distance' || p.kind === 'country') return '';
    if (p.kind === 'type') return kindOf({ types: [p.text] }) || '';
    return p.text;
  };
  const yes = pieces.filter((p) => p.agrees === true).map(say).filter(Boolean);
  const no = pieces.filter((p) => p.agrees === false).map(say).filter(Boolean);
  const out = [];
  if (yes.length) out.push(`Agrees: ${yes.join(', ')}.`);
  if (no.length) out.push(`Does not agree: ${no.join(', ')}.`);
  return out.join(' ');
}

function warnText(c) {
  if (!c.demoted) return '';
  const k = kindOf(c.card);
  return k ? `Probably not the place itself: this looks like a ${k} named after it.`
    : 'Probably not the place itself: this looks like a building, region or other feature named after it.';
}

function current() { return S.records[S.idx] || null; }
function settled(c) { return c.status === 'accepted' || c.status === 'rejected'; }
function isDecided(r) { return S.tab === 'later' ? r.state !== 'later' : r.state !== 'unreviewed'; }

// ── Decisions: the three states, and what choosing one place does to the others ───────────────────
// Nothing is ever filled in for the person: a row is decided only by their own mark (or the
// provisional default on a "Likely the same place" record, which Enter confirms). Kept as a hook so
// every place that changes a decision still goes through one function.
function recompute() { /* deliberately empty */ }

function resetDecisions(r) {
  S.dec.clear();
  S.focus = null;
  const first = r && r.candidates[0];
  if (r && r.tier === 'confident' && r.state === 'unreviewed' && first && !settled(first)) {
    S.dec.set(0, { d: 'same', src: 'default' });
    recompute(r);
  }
}

function setRow(i, d) {
  const r = current();
  const c = r && r.candidates[i];
  if (!c || settled(c)) return;
  const had = S.dec.get(i);
  if (had && had.d === d && had.src !== 'prefilled') S.dec.delete(i);   // the same press again takes it back
  else {
    S.dec.set(i, { d, src: 'explicit' });
    if (d === 'same') for (const [j, v] of [...S.dec]) if (v.src === 'default') S.dec.delete(j);
  }
  recompute(r);
  S.focus = i;
  renderRecord({ keepSearch: true, refit: false });
  focusRow(i);
}

function setGroup(g, d) {
  const r = current();
  if (!r) return;
  r.candidates.forEach((c, i) => { if (c.group === g && !settled(c)) S.dec.set(i, { d, src: 'explicit' }); });
  if (d === 'same') for (const [j, v] of [...S.dec]) if (v.src === 'default') S.dec.delete(j);
  recompute(r);
  renderRecord({ keepSearch: true, refit: false });
}

function tally(r) {
  const t = { same: [], not: [], unsure: [], open: [] };
  r.candidates.forEach((c, i) => {
    if (settled(c)) return;
    const v = S.dec.get(i);
    (v ? t[v.d] : t.open).push(i);
  });
  return t;
}

function nums(list) { return list.map((i) => i + 1).join(', '); }

function summaryHTML(r, t) {
  if (!r.candidates.length) return '';
  const part = (label, list, cls) => (list.length ? `<span class="mf-sum mf-sum--${cls}"><b>${label}:</b> ${nums(list)}</span>` : '');
  const parts = [part('Same place', t.same, 'same'), part('Not the same', t.not, 'not'),
    part("Can't tell", t.unsure, 'unsure'), part('Not marked', t.open, 'open')].filter(Boolean);
  const dflt = [...S.dec.values()].some((v) => v.src === 'default');
  const hint = dflt ? 'The first suggestion is set as the same place: change it if it is not.'
    : (t.same.length && t.open.length) ? 'Suggestions you did not mark are left as they are.' : '';
  return `${parts.join(' ') || '<span class="text-secondary">Nothing marked yet.</span>'}${hint ? `<div class="small text-secondary">${hint}</div>` : ''}`;
}

// ── Rendering ────────────────────────────────────────────────────────────────────────────────────
function rowHTML(c, i, r) {
  const card = c.card || {};
  const v = S.dec.get(i);
  const done = settled(c);
  const title = card.title || c.title;
  const kind = kindOf(card);
  const rawTypes = (card.types || []).join(', ');
  const other = (card.names || []).map((n) => n.label).filter((l) => l && l !== title).slice(0, 4);
  const where = [card.ccodes && card.ccodes.length ? esc(card.ccodes.join(', ')) : '',
    c.km != null ? `<b class="mf-dist">${esc(distText(c.km))}</b>` : '',
    years(card.temporal_range) ? esc(years(card.temporal_range)) : '',
    c.location_withheld ? 'location not available to you' : (c.location === null ? 'no location' : '')].filter(Boolean).join(' · ');
  const warn = warnText(c);
  const why = whyText(c);
  const state = done ? (c.status === 'accepted' ? 'matched' : 'set aside') : (v ? `${v.d}${v.src === 'prefilled' ? ' (filled in)' : ''}` : 'not marked');
  const seg = done
    ? `<span class="badge ${c.status === 'accepted' ? 'bg-success' : 'bg-secondary'}">${c.status === 'accepted' ? 'matched' : 'not this one'}</span>`
    : `<div class="mf-seg" role="group" aria-label="Decision for ${i + 1}, ${esc(title)}" data-tour="row-state">
        ${[['same', 'Same place'], ['not', 'Not the same'], ['unsure', "Can't tell"]].map(([d, label]) =>
    `<button type="button" class="mf-seg-btn mf-seg-btn--${d}${v && v.d === d ? ` is-on${v.src === 'prefilled' ? ' is-prefilled' : ''}` : ''}"
        data-act="set" data-i="${i}" data-d="${d}" aria-pressed="${v && v.d === d ? 'true' : 'false'}" tabindex="${S.shortcuts ? -1 : 0}"
        aria-label="${esc(label)}: ${esc(title)}">${esc(label)}</button>`).join('')}
      </div>`;
  const cls = ['mf-cand', 'mf-row', v ? `mf-row--${v.d}` : '', v && v.src === 'prefilled' ? 'mf-row--prefilled' : '',
    done ? `mf-cand--${c.status}` : '', S.focus === i ? 'is-focus' : ''].filter(Boolean).join(' ');
  return `<li class="${cls}" data-i="${i}" data-candidate="${esc(c.candidate_id)}" data-group="${c.group == null ? '' : c.group}"
      tabindex="${S.focus === i || (S.focus == null && i === 0) ? 0 : -1}" role="group"
      aria-label="Suggestion ${i + 1}: ${esc(title)}, ${esc(nsLabel(card.namespace || c.namespace))}${c.km != null ? `, ${esc(distText(c.km))}` : ''}. ${esc(state)}.">
      <div class="mf-row-main">
        <span class="mf-key">${i + 1}</span>
        <div class="mf-row-text">
          <div class="mf-row-title"><span class="fw-semibold" dir="auto">${esc(title)}</span>
            <span class="badge text-bg-light border ms-1">${esc(nsLabel(card.namespace || c.namespace))}</span>
            ${kind ? `<span class="mf-kind"${rawTypes ? ` title="${esc(rawTypes)}"` : ''}>${esc(kind)}</span>` : ''}</div>
          <div class="mf-row-facts small text-secondary">${where}</div>
          ${warn ? `<div class="small mf-warn"><i class="fas fa-triangle-exclamation me-1" aria-hidden="true"></i>${esc(warn)}</div>` : ''}
          <details class="small mf-more"><summary>Details</summary>
            ${other.length ? `<div class="text-secondary" dir="auto">also: ${other.map(esc).join(', ')}</div>` : ''}
            <div class="mf-why">${esc(why)}</div>
            <div class="text-secondary">${esc(c.candidate_id)}${rawTypes ? ` · ${esc(rawTypes)}` : ''}</div>
          </details>
        </div>
        ${seg}
      </div>
    </li>`;
}

// Rows in display order: a place described by more than one source sits together under one heading.
function rowsHTML(r) {
  const cs = r.candidates;
  const seen = new Set();
  const out = [];
  cs.forEach((c, i) => {
    if (c.group == null) { out.push(rowHTML(c, i, r)); return; }
    if (seen.has(c.group)) return;
    seen.add(c.group);
    const members = cs.map((m, j) => [m, j]).filter(([m]) => m.group === c.group);
    const open = members.filter(([m]) => !settled(m));
    out.push(`<li class="mf-group" data-group="${c.group}" role="group" aria-label="The same place in ${members.length} sources">
      <div class="mf-group-head small"><i class="fas fa-link me-1" aria-hidden="true"></i>
        Probably one place, described by ${members.length} sources
        ${open.length ? `<button type="button" class="btn btn-sm btn-outline-success ms-2 py-0" data-act="group" data-g="${c.group}" data-d="same">All same place</button>` : ''}</div>
      <ol class="list-unstyled mb-0">${members.map(([m, j]) => rowHTML(m, j, r)).join('')}</ol></li>`);
  });
  return out.join('');
}

function displayOrder() {
  const order = S.records.map((_, i) => i);
  return order.filter((i) => !isDecided(S.records[i])).concat(order.filter((i) => isDecided(S.records[i])));
}

function renderQueue() {
  const q = document.getElementById('mf_queue');
  q.innerHTML = displayOrder().map((i) => {
    const r = S.records[i];
    const done = isDecided(r);
    return `<li><button type="button" class="list-group-item list-group-item-action${i === S.idx ? ' active' : ''}${done ? ' mf-queue--done' : ''}"
      data-qi="${i}" aria-current="${i === S.idx ? 'true' : 'false'}" dir="auto">${esc(r.title)}
      ${done ? `<span class="badge bg-light text-dark ms-1">${esc(r.state === 'linked' ? 'matched' : 'done')}</span>` : ''}</button></li>`;
  }).join('') || '<li class="list-group-item text-secondary">Nothing in this tab.</li>';
  document.getElementById('mf_more').hidden = S.next == null;
  // keep the current record in view inside the list's own scroll box (never scrolls the page)
  const act = q.querySelector('.active');
  if (act) {
    const top = act.offsetTop - q.offsetTop;
    if (top < q.scrollTop) q.scrollTop = top;
    else if (top + act.offsetHeight > q.scrollTop + q.clientHeight) q.scrollTop = top + act.offsetHeight - q.clientHeight;
  }
}

function recordCardHTML(r) {
  const meta = [r.names && r.names.length ? `also: <span dir="auto">${r.names.map(esc).join(', ')}</span>` : '',
    r.types && r.types.length ? esc(r.types.join(', ')) : '', r.ccodes && r.ccodes.length ? esc(r.ccodes.join(', ')) : '',
    years(r.minmax) ? esc(years(r.minmax)) : '', r.has_geometry ? '' : 'no location'].filter(Boolean).join(' · ');
  const tierBadge = r.tier === 'unqueried' ? '<span class="badge bg-warning text-dark ms-1">waiting for the search service</span>' : '';
  const why = r.tier === 'unqueried'
    ? `<p class="small mb-0 mt-2" id="mf_waiting">Nothing was found yet because ${esc(FAILURES[r.last_failure] || 'the search service has not answered')}. It will be tried again; this is not "nothing found".</p>` : '';
  const later = r.state === 'later'
    ? `<p class="small mb-0 mt-2 mf-later">You marked this "Not sure yet"${r.decided_at ? ` on ${esc(new Date(r.decided_at).toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' }))}` : ''}.${r.note ? ` Your note: <em dir="auto">${esc(r.note)}</em>` : ''}</p>` : '';
  return `<div class="mf-record-card" data-tour="record">
      <div class="d-flex flex-wrap justify-content-between align-items-baseline gap-2">
        <h2 class="h5 mb-0" id="mf_record_title" dir="auto">${esc(r.title)}${tierBadge}</h2>
        <span class="small text-secondary d-flex align-items-center gap-1">${esc(r.src_id)}
          <button type="button" class="btn btn-sm btn-outline-secondary py-0 ms-2" data-act="prev" aria-label="Previous record">&larr;</button>
          <button type="button" class="btn btn-sm btn-outline-secondary py-0" data-act="next" aria-label="Next record">&rarr;</button></span>
      </div>
      ${meta ? `<div class="small text-secondary mt-1">${meta}</div>` : ''}${why}${later}
    </div>`;
}

function barHTML(r, t) {
  const has = r.candidates.length > 0;
  const waiting = r.tier === 'unqueried';
  const canSave = t.same.length + t.not.length > 0;
  return `<div class="mf-bar" id="mf_bar" data-tour="bar">
      ${has ? `<div class="mf-summary" id="mf_summary" data-tour="summary">${summaryHTML(r, t)}</div>` : ''}
      <div class="d-flex flex-wrap gap-2 align-items-center">
        ${has ? `<button type="button" class="btn btn-sm btn-primary" data-act="save" data-tour="save" ${canSave ? '' : 'disabled'}>Save decision <kbd>Enter</kbd></button>
        <button type="button" class="btn btn-sm btn-outline-warning" data-act="none" data-tour="none">None of these fits <kbd>n</kbd></button>` : ''}
        ${waiting ? '' : '<button type="button" class="btn btn-sm btn-outline-secondary" data-act="later" data-tour="later">Not sure yet <kbd>l</kbd></button>'}
        <button type="button" class="btn btn-sm btn-outline-secondary" data-act="undo" ${S.undo.length ? '' : 'disabled'}>Undo <kbd>u</kbd></button>
      </div>
      <input type="text" class="form-control form-control-sm mt-2" id="mf_note" maxlength="2000" dir="auto"
             value="${esc(S.note)}" placeholder="Note (optional): why you decided" aria-label="Note">
      ${has ? `<details class="small mt-2" id="mf_advanced"><summary>More options</summary>
        <div class="form-check mt-1">
          <input class="form-check-input" type="checkbox" id="mf_publish" ${S.publish ? 'checked' : ''}>
          <label class="form-check-label" for="mf_publish">Also share the "Not the same" decisions I marked myself as statements others can see (never the ones the page filled in)</label></div>
        <div class="mt-1">How closely do the record and the place match?</div>
        <div class="form-check"><input class="form-check-input" type="radio" name="mf_relation" id="mf_rel_close" value="closeMatch" ${S.relation === 'exactMatch' ? '' : 'checked'}>
          <label class="form-check-label" for="mf_rel_close">Same place (the usual choice: they are about the same place)</label></div>
        <div class="form-check"><input class="form-check-input" type="radio" name="mf_relation" id="mf_rel_exact" value="exactMatch" ${S.relation === 'exactMatch' ? 'checked' : ''}>
          <label class="form-check-label" for="mf_rel_exact">Exactly the same place (same place, same extent: use sparingly)</label></div>
      </details>` : ''}
    </div>`;
}

function searchHTML(r) {
  return `<div class="mt-3" id="mf_search" data-tour="search">
      <label class="small text-secondary" for="mf_search_q">Search for another name <kbd>/</kbd></label>
      <div class="input-group input-group-sm">
        <input type="text" class="form-control" id="mf_search_q" value="${esc(r.title)}" dir="auto">
        <button type="button" class="btn btn-outline-secondary" data-act="search">Search</button>
      </div>
      <div id="mf_search_cx"></div>
      <ol class="list-unstyled mt-2" id="mf_search_results"></ol>
    </div>`;
}

function renderRecord(opts = {}) {
  const box = document.getElementById('mf_record');
  const r = current();
  const keepSearch = opts.keepSearch;
  if (!keepSearch) { S.searchResults = null; S.handSel = null; }
  window.__whgMatchFeed.regions = { record: false, rows: false, map: false, bar: false };
  if (!r) {
    box.innerHTML = '<p class="text-secondary" id="mf_empty" data-tour="record">Nothing to review in this tab.</p>';
    renderMap(true);
    window.__whgMatchFeed.regions.record = true;
    return;
  }
  const searchBefore = keepSearch ? document.getElementById('mf_search') : null;
  const t = tally(r);
  const has = r.candidates.length > 0;
  const rows = has ? `<ol class="list-unstyled mt-2 mb-2 mf-rows" id="mf_cands" data-tour="candidates" aria-label="Suggestions for ${esc(r.title)}">${rowsHTML(r)}</ol>`
    : (r.tier === 'unqueried' ? '' : '<p class="text-secondary mt-2" id="mf_nosugg" data-tour="candidates">No suggestions for this record.</p>');
  // With nothing suggested, the search comes first: it is the way forward.
  const search = searchHTML(r);
  box.innerHTML = `${recordCardHTML(r)}${has ? '' : search}${rows}${barHTML(r, t)}${has ? search : ''}`;
  if (searchBefore) {
    const nowBox = document.getElementById('mf_search');
    if (nowBox) nowBox.replaceWith(searchBefore);
  }
  window.__whgMatchFeed.regions.record = true;
  window.__whgMatchFeed.regions.rows = true;
  window.__whgMatchFeed.regions.bar = true;
  renderMap(opts.refit !== false);
}

function focusRow(i) {
  const li = document.querySelector(`#mf_cands .mf-row[data-i="${i}"]`);
  if (!li) return;
  S.focus = i;
  document.querySelectorAll('#mf_cands .mf-row').forEach((x) => {
    const on = Number(x.dataset.i) === i;
    x.classList.toggle('is-focus', on);
    x.tabIndex = on ? 0 : -1;
  });
  li.focus({ preventScroll: false });
  highlight(i);
}

// ── The map panel ─────────────────────────────────────────────────────────────────────────────
// What is placed: the record's point and every suggestion that has one, all equal until a row has
// the focus or a decision. Nothing is dropped silently: suggestions with no point are named in the
// caption, and those off the visible map are named on its edge.
let feedMap = null;
let mapLoading = null;
let mapFailed = false;

function mapView(r) {
  const cands = r.candidates || [];
  const handFocus = S.handSel !== null && S.handSel !== undefined;
  const points = [];
  const missing = [];
  const withheld = [];
  cands.forEach((c, i) => {
    if (Array.isArray(c.location)) {
      const v = S.dec.get(i);
      points.push({
        i, title: (c.card && c.card.title) || c.title, location: c.location, km: c.km,
        focus: !handFocus && S.focus === i, status: c.status, state: v ? v.d : null,
        provisional: !!(v && v.src === 'prefilled'), group: c.group == null ? null : c.group,
      });
    } else if (c.location_withheld) withheld.push(i + 1);
    else missing.push(i + 1);
  });
  const hand = [];
  const handMissing = [];
  (S.searchResults || []).forEach((c, i) => {
    if (Array.isArray(c.location)) hand.push({ i, title: (c.card && c.card.title) || c.title, location: c.location, focus: S.handSel === i });
    else handMissing.push(`s${i + 1}`);
  });
  return { record: Array.isArray(r.location) ? { title: r.title, location: r.location } : null, points, missing, withheld, hand, handMissing };
}

function mapCaption(r, v) {
  const parts = [];
  if (!v.record) parts.push('<strong>Your record has no location</strong>; only its suggestions are shown.');
  else parts.push('★ your record');
  if (v.points.length) parts.push('numbers: suggestions, as in the list (green: same place, struck through: not the same)');
  if (v.missing.length) parts.push(`not on the map (no location): ${v.missing.join(', ')}`);
  if (v.withheld.length) parts.push(`not on the map (location not available to you): ${v.withheld.join(', ')}`);
  if (v.hand.length) parts.push('s1, s2…: your own search results (click one to see it here)');
  if (v.handMissing.length) parts.push(`search results not on the map (no location): ${v.handMissing.join(', ')}`);
  if (window.__whgMatchFeed.map.basemap === 'blank') parts.push('the background map could not be loaded');
  return parts.join(' · ');
}

function mapDone(state, v) {
  const m = window.__whgMatchFeed.map;
  m.state = state;
  m.record = v && v.record ? v.record.location : null;
  m.points = v ? v.points.map((p) => ({ i: p.i, focus: p.focus, state: p.state, location: p.location })) : [];
  m.hand = v ? v.hand.map((p) => ({ i: p.i, focus: p.focus, location: p.location })) : [];
  m.renders += 1;
  window.__whgMatchFeed.regions.map = state === 'ok' || state === 'no-location' || state === 'unavailable';
}

function showMapFailed() {
  mapFailed = true;
  const el = (id) => document.getElementById(id);
  el('mf_map').hidden = true;
  el('mf_map_tools').hidden = true;
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
        // A marker only moves the focus to its row: it never records a decision.
        onPick: (i) => { S.focus = i; focusRow(i); const li = document.querySelector(`#mf_cands .mf-row[data-i="${i}"]`); if (li) li.scrollIntoView({ block: 'nearest', behavior: 'smooth' }); },
        onPickHand: (i) => pickHand(i),
        onBasemap: (state) => {
          window.__whgMatchFeed.map.basemap = state;
          if (state === 'blank') renderMap(false);
        },
      });
      window.__whgMatchFeed.map.instance = feedMap.map;
      return feedMap;
    });
  }
  return mapLoading;
}

function renderMap(refit) {
  const panel = document.getElementById('mf_map_panel');
  if (!panel) return;
  const r = current();
  if (!r) { panel.hidden = true; mapDone('no-record', null); return; }
  panel.hidden = false;
  if (mapFailed) { mapDone('unavailable', null); return; }
  const v = mapView(r);
  const box = document.getElementById('mf_map');
  const none = document.getElementById('mf_map_none');
  const tools = document.getElementById('mf_map_tools');
  const caption = document.getElementById('mf_map_caption');
  document.getElementById('mf_map_failed').hidden = true;
  if (!v.record && !v.points.length && !v.hand.length) {
    // A clear "no location" state, never an empty box.
    box.hidden = true;
    tools.hidden = true;
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
  tools.hidden = false;
  caption.innerHTML = mapCaption(r, v);
  const place = r.place;
  ensureMap().then((fm) => {
    const now = current();
    if (!now || now.place !== place) return;   // the person has moved on; that render will draw
    const latest = mapView(now);
    fm.update(latest, { refit });
    mapDone('ok', latest);
  }).catch((err) => {
    console.warn('The feed map could not be shown:', err);
    showMapFailed();
  });
}

function highlight(i) {
  if (feedMap) feedMap.highlight(i);
}

function render() { renderQueue(); resetDecisions(current()); renderRecord(); window.__whgMatchFeed.record = current() ? current().place : null; }

async function load(tab, append) {
  if (!append) { S.offset = 0; S.records = []; S.idx = 0; S.note = ''; S.relation = 'closeMatch'; }
  S.tab = tab;
  window.__whgMatchFeed.tab = tab;
  document.querySelectorAll('.mf-tabs .nav-link').forEach((b) => {
    const on = b.dataset.tab === tab;
    b.classList.toggle('active', on);
    b.setAttribute('aria-selected', on ? 'true' : 'false');
    b.tabIndex = on ? 0 : -1;
    if (on) document.getElementById('mf_panel').setAttribute('aria-labelledby', b.id);
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
  if (append) { renderQueue(); return; }   // more records for the list: never redraw (and so reset) the record being decided
  render();
}

function relation() {
  const el = document.querySelector('input[name="mf_relation"]:checked');
  return el ? el.value : 'closeMatch';
}

function noteText() { const n = document.getElementById('mf_note'); return n ? n.value : ''; }

function remaining() { return S.records.filter((r) => !isDecided(r)).length; }

// The record to show after a decision: the next one still to decide after the one in view (then
// from the top). Chosen before the decided record moves to the end of the queue.
function nextTarget(order, pos) {
  const rest = order.slice(pos + 1).concat(order.slice(0, pos));
  return rest.find((i) => !isDecided(S.records[i]));
}

async function review(body, after, { stay } = {}) {
  const r = current();
  if (!r) return;
  const order0 = displayOrder();
  const pos0 = order0.indexOf(S.idx);
  const res = await postJSON(root.dataset.reviewUrl, { place: r.place, note: noteText(), ...body });
  if (!res.ok) {
    announce(`Not saved: ${res.data.error || res.status}`);
    const box = document.getElementById('mf_record');
    box.insertAdjacentHTML('afterbegin', `<div class="alert alert-danger py-1 small" role="alert">Not saved: ${esc(res.data.error || String(res.status))}</div>`);
    return;
  }
  window.__whgMatchFeed.saves += 1;
  updateCounts(res.data.counts);
  updateProgress(res.data.progress);
  S.undo.push(r.place);
  r.state = res.data.state;
  if (after) after(r);
  announce(body.action === 'later' ? `Moved to Later. ${Math.max(0, remaining())} to go in this tab.`
    : `Saved. ${Math.max(0, remaining())} to go in this tab.`);
  S.note = '';
  if (stay && !isDecided(r)) { S.dec.clear(); renderQueue(); renderRecord(); return; }
  const to = nextTarget(order0, pos0);
  go(to === undefined ? S.idx : to);
}

function go(i) {
  S.idx = Math.max(0, Math.min(S.records.length - 1, i));
  S.note = '';
  S.relation = 'closeMatch';
  render();
  const t = document.getElementById('mf_record_title');
  if (t) { t.tabIndex = -1; t.focus(); }
}

function saveDecision() {
  const r = current();
  if (!r || !r.candidates.length) return;
  const t = tally(r);
  if (!t.same.length && !t.not.length) {
    announce(r.tier === 'ambiguous'
      ? 'Which one, if any? Mark the place that is the same, or say None of these fits.'
      : 'Nothing is marked yet. Mark a suggestion as the same place, or say None of these fits.');
    return;
  }
  const decisions = [...S.dec].filter(([i]) => !settled(r.candidates[i]))
    .map(([i, v]) => ({ candidate: r.candidates[i].id, decision: v.d, source: v.src }));
  const explicitNot = [...S.dec].some(([, v]) => v.d === 'not' && v.src === 'explicit');
  const keep = new Map(S.dec);
  review({ action: 'decide', decisions, relation: relation(), distinct: S.publish && explicitNot },
    () => keep.forEach((v, i) => {
      if (v.d === 'same') r.candidates[i].status = 'accepted';
      else if (v.d === 'not') r.candidates[i].status = 'rejected';
    }), { stay: true });
}

async function undo() {
  const place = S.undo.pop();
  if (place == null) return;
  const res = await postJSON(root.dataset.reviewUrl, { place, action: 'reopen' });
  if (!res.ok) { announce('Undo failed.'); return; }
  updateCounts(res.data.counts);
  updateProgress(res.data.progress);
  const i = S.records.findIndex((r) => r.place === place);
  if (i >= 0) {
    S.records[i].state = 'unreviewed';
    (S.records[i].candidates || []).forEach((c) => { c.status = 'suggested'; });
    S.idx = i;
  }
  announce('Undone.');
  render();
}

// ── Hand search (unchanged in behaviour) ──────────────────────────────────────────────────────────
function handHTML(c, i) {
  const card = c.card || {};
  const kind = kindOf(card);
  const where = [card.ccodes && card.ccodes.length ? esc(card.ccodes.join(', ')) : '',
    c.km != null ? `<b class="mf-dist">${esc(distText(c.km))}</b>` : '',
    c.location === null ? 'no location' : ''].filter(Boolean).join(' · ');
  return `<li class="mf-cand mf-hand${S.handSel === i ? ' mf-cand--selected' : ''}" data-i="${i}" data-candidate="${esc(c.candidate_id)}">
      <div class="mf-row-main"><span class="mf-key">s${i + 1}</span>
        <div class="mf-row-text">
          <div class="mf-row-title"><span class="fw-semibold" dir="auto">${esc(card.title || c.title)}</span>
            <span class="badge text-bg-light border ms-1">${esc(nsLabel(card.namespace || c.namespace))}</span>
            ${kind ? `<span class="mf-kind">${esc(kind)}</span>` : ''}</div>
          <div class="mf-row-facts small text-secondary">${where}</div>
          <div class="mf-why small">${esc(whyText(c))}</div>
        </div>
        <button type="button" class="btn btn-sm btn-success" data-act="link-hand" data-i="${i}"
                aria-label="Same place: ${esc(card.title || c.title)}">Same place</button>
      </div></li>`;
}

async function search() {
  const r = current();
  if (!r) return;
  const q = document.getElementById('mf_search_q').value;
  const url = `${root.dataset.searchUrl}?place=${r.place}&q=${encodeURIComponent(q)}&relax=${encodeURIComponent([...S.relaxed].join(','))}`;
  const out = document.getElementById('mf_search_results');
  out.innerHTML = '<li class="text-secondary small">Searching…</li>';
  const res = await getJSON(url);
  if (current() !== r) return;   // the person moved on while the search ran
  const cx = document.getElementById('mf_search_cx');
  S.searchChips = res.data.constraints || [];
  cx.innerHTML = constraintChipsHTML(S.searchChips, S.relaxed);
  if (!res.ok) { out.innerHTML = `<li class="text-danger small">Search failed (${res.status}).</li>`; return; }
  if (!res.data.answered) {
    out.innerHTML = '<li class="text-warning-emphasis small" id="mf_search_silent">The search service did not answer. Try again in a moment: this does not mean nothing exists.</li>';
    return;
  }
  S.searchResults = res.data.results || [];
  S.handSel = null;
  out.innerHTML = S.searchResults.map((c, i) => handHTML(c, i)).join('')
    || '<li class="text-secondary small">Nothing found for that name.</li>';
  try { renderMap(false); } catch (err) { console.warn('The feed map could not show the search results:', err); }
}

function pickHand(i) {
  if (!S.searchResults || !S.searchResults[i]) return;
  S.handSel = S.handSel === i ? null : i;
  document.querySelectorAll('#mf_search_results .mf-hand').forEach((li) => {
    li.classList.toggle('mf-cand--selected', Number(li.dataset.i) === S.handSel);
  });
  try { renderMap(true); } catch (err) { console.warn('The feed map could not be updated:', err); }
}

// ── Events ───────────────────────────────────────────────────────────────────────────────────────
root.addEventListener('click', (e) => {
  const tabBtn = e.target.closest('.mf-tabs [data-tab]');
  if (tabBtn) { load(tabBtn.dataset.tab); return; }
  const qi = e.target.closest('[data-qi]');
  if (qi) { go(Number(qi.dataset.qi)); return; }
  const b = e.target.closest('[data-act]');
  if (!b) {
    const hl = e.target.closest('#mf_search_results .mf-hand');
    if (hl) { pickHand(Number(hl.dataset.i)); return; }
    const row = e.target.closest('#mf_cands .mf-row');
    if (row && !e.target.closest('details')) { S.focus = Number(row.dataset.i); focusRow(S.focus); }
    return;
  }
  const act = b.dataset.act;
  if (act === 'set') setRow(Number(b.dataset.i), b.dataset.d);
  else if (act === 'group') setGroup(Number(b.dataset.g), b.dataset.d);
  else if (act === 'save') saveDecision();
  else if (act === 'link-hand') {
    const c = S.searchResults && S.searchResults[Number(b.dataset.i)];
    if (c) { S.handSel = Number(b.dataset.i); review({ action: 'link', target: c.candidate_id, relation: relation() }); }
  } else if (act === 'none') review({ action: 'none' });
  else if (act === 'later') review({ action: 'later' });
  else if (act === 'tour') startMatchTour();
  else if (act === 'undo') undo();
  else if (act === 'prev') move(-1);
  else if (act === 'next') move(1);
  else if (act === 'search') search();
  else if (act === 'fit-all') { if (feedMap) feedMap.fitAll(); }
  else if (act === 'fit-near') { if (feedMap) feedMap.fitNear(); }
  else if (act === 'regenerate') {
    postJSON(root.dataset.regenerateUrl, {}).then((res) => {
      const box = document.getElementById('mf_no_generation');
      if (box) box.textContent = res.ok ? 'Suggestions are being prepared. Come back in a while.' : `Could not start: ${res.data.error || res.status}`;
    });
  }
});

root.addEventListener('input', (e) => { if (e.target && e.target.id === 'mf_note') S.note = e.target.value; });

root.addEventListener('change', (e) => {
  if (e.target && e.target.name === 'mf_relation') S.relation = e.target.value;
  if (e.target && e.target.id === 'mf_publish') { S.publish = e.target.checked; store('session', 'mf.publish', S.publish ? '1' : '0'); }
  if (e.target && e.target.id === 'mf_shortcuts') {
    S.shortcuts = e.target.checked;
    store('local', 'mf.shortcuts', S.shortcuts ? 'on' : 'off');
    syncShortcutUI();
    renderRecord({ keepSearch: true, refit: false });
  }
});

// A row under the pointer or with the focus is highlighted on the map.
root.addEventListener('mouseover', (e) => { const row = e.target.closest('#mf_cands .mf-row'); if (row) highlight(Number(row.dataset.i)); });
root.addEventListener('mouseout', (e) => { if (e.target.closest('#mf_cands .mf-row')) highlight(S.focus); });
root.addEventListener('focusin', (e) => { const row = e.target.closest('#mf_cands .mf-row'); if (row) { S.focus = Number(row.dataset.i); highlight(S.focus); } });

function move(d) {
  const order = displayOrder();
  if (!order.length) return;
  const pos = order.indexOf(S.idx);
  // at the end of what is loaded, fetch the next page rather than stopping
  if (d > 0 && pos === order.length - 1 && S.next != null) { load(S.tab, true).then(() => move(1)); return; }
  go(order[Math.max(0, Math.min(order.length - 1, pos + d))]);
}

document.getElementById('mf_more').addEventListener('click', () => load(S.tab, true));

bindConstraintChips(root, () => S.searchChips, S.relaxed, () => search());

// The shortcut switch lives in the key legend; its state is remembered on this device.
function syncShortcutUI() {
  const box = document.getElementById('mf_shortcuts');
  if (box) box.checked = S.shortcuts;
  const list = document.getElementById('mf_keylist');
  if (list) list.hidden = !S.shortcuts;
  root.classList.toggle('mf-keys-on', S.shortcuts);   // key hints on the buttons show only then
}
syncShortcutUI();

document.addEventListener('keydown', (e) => {
  if (window.__whgMatchFeed.touring) return;   // the tour owns the keyboard while it runs
  const tag = (e.target && e.target.tagName) || '';
  const inField = /INPUT|TEXTAREA|SELECT/.test(tag);
  if (inField || e.metaKey || e.ctrlKey || e.altKey) {
    if (e.key === 'Enter' && e.target.id === 'mf_search_q') { e.preventDefault(); search(); }
    return;
  }
  // The tab strip is its own keyboard region: arrows, Home and End move between the tabs.
  if (e.target && e.target.closest && e.target.closest('.mf-tabs') && /^(ArrowLeft|ArrowRight|Home|End)$/.test(e.key)) {
    const tabs = [...document.querySelectorAll('.mf-tabs [data-tab]')];
    const at = tabs.indexOf(e.target.closest('[data-tab]'));
    const to = e.key === 'Home' ? 0 : e.key === 'End' ? tabs.length - 1
      : (at + (e.key === 'ArrowRight' ? 1 : -1) + tabs.length) % tabs.length;
    e.preventDefault();
    tabs[to].focus();
    load(tabs[to].dataset.tab);
    return;
  }
  // The map is its own keyboard region: its arrow keys pan it, never change the record.
  if (e.target && e.target.closest && e.target.closest('#mf_map')) return;
  // Enter or Space on a focused button is that button's own click; never also a shortcut.
  if ((e.key === 'Enter' || e.key === ' ') && /^(BUTTON|A|SUMMARY)$/.test(tag)) return;
  const r = current();
  const onRow = !!(e.target && e.target.closest && e.target.closest('#mf_cands .mf-row'));
  // All keys below are shortcuts: they are off until the person switches them on.
  if (!S.shortcuts) return;
  if (onRow && r) {
    const i = Number(e.target.closest('.mf-row').dataset.i);
    if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
      e.preventDefault();
      const next = Math.max(0, Math.min(r.candidates.length - 1, i + (e.key === 'ArrowDown' ? 1 : -1)));
      focusRow(next);
      return;
    }
    if (e.key === 's') { e.preventDefault(); setRow(i, 'same'); return; }
    if (e.key === 'x') { e.preventDefault(); setRow(i, 'not'); return; }
    if (e.key === '?' || e.key === 'c') { e.preventDefault(); setRow(i, 'unsure'); return; }
  }
  if (/^[1-9]$/.test(e.key) && r) {
    const i = Number(e.key) - 1;
    if (i < r.candidates.length) focusRow(i);
  } else if (e.key === 'Enter') { e.preventDefault(); saveDecision(); }
  else if (e.key === 'n') { if (r && r.candidates.length) review({ action: 'none' }); }
  else if (e.key === 'l') { if (r && r.tier !== 'unqueried') review({ action: 'later' }); }
  else if (e.key === 'u') undo();
  else if (e.key === '/') { e.preventDefault(); const q = document.getElementById('mf_search_q'); if (q) q.focus(); }
  else if (e.key === 'ArrowRight') move(1);
  else if (e.key === 'ArrowLeft') move(-1);
  else return;
});

// The note above the tabs: dismissed once, remembered on this device.
(function intro() {
  const box = document.getElementById('mf_intro');
  if (!box) return;
  if (store('local', 'mf.intro') !== 'hidden') box.hidden = false;
  document.getElementById('mf_intro_close').addEventListener('click', () => { box.hidden = true; store('local', 'mf.intro', 'hidden'); });
}());

load(S.tab).then(() => {
  window.__whgMatchFeed.ready = true;
  // First visit only, and only when there is something on screen for the tour to point at.
  if (!hasSeenMatchTour() && document.querySelector('#mf_cands .mf-row')) startMatchTour();
});
