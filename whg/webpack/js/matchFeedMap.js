// matchFeedMap.js: the small map in the contributor feed, "Link your records" (place#334 4c/4i,
// place#338; redesign 2026-11). Lazy-loaded by matchFeed.js, so the feed works without it (a failed
// chunk, no WebGL).
//
// One map for the page. It shows the record's own point (a star) and every suggestion that has a
// location, numbered as in the list. All suggestions look alike until one has the focus (the row
// under the pointer or with the keyboard focus) or a decision: green = same place, struck through =
// not the same, dashed = filled in by the page, not yet confirmed. Markers are DOM elements, like
// the review map in recon-map.js, so they are placed whether or not the basemap loads, and they
// carry their number and shape (colour never carries meaning alone). A marker only reports a click
// (onPick); it never decides anything.
//
// The camera shows the record and the suggestions NEARBY (as many, nearest first, as keep a
// regional zoom). Suggestions outside the view are never silently dropped: each direction gets an
// edge chip naming them and how far away they are ("Fit all" is one click). Markers that would
// overlap are merged into one badge ("1·2"); clicking it zooms to them, or, when they sit on the
// same spot, steps through them.
//
// Basemap: the same tileboss style the review map uses (process.env.TILEBOSS); no other network
// calls. If the style cannot be fetched, the map falls back to a plain background and says so.

import './recon-maplibre-global.js'; // MUST be first: publishes window.maplibregl (whg_maplibre needs the global)
import 'maplibre-gl/dist/maplibre-gl.css';
import './whg_maplibre.js'; // side effect: window.whg_maplibre = (wrapped) maplibregl

const BLANK_STYLE = {
  version: 8, metadata: {}, sources: {},
  layers: [{ id: 'mf-blank', type: 'background', paint: { 'background-color': '#e9eef1' } }],
};
const STYLE_GRACE_MS = 10000;
const MIN_REGIONAL_ZOOM = 5;   // "nearby" keeps at least this zoom (about a region, not a continent)
const MERGE_PX = 22;           // markers closer than this on screen become one badge
const SAME_SPOT_DEG = 0.0005;  // members within about 50 m: zooming cannot separate them

const ML = () => window.whg_maplibre;
const ARROWS = ['→', '↘', '↓', '↙', '←', '↖', '↑', '↗'];
const WORDS = ['east', 'south-east', 'south', 'south-west', 'west', 'north-west', 'north', 'north-east'];

// container: the map's element. onPick(i): a suggestion's marker was clicked (focus its row).
// onBasemap(state): 'ok' | 'blank' when the basemap could not be loaded.
// Returns { map, update(view, {refit}), highlight(i), fitAll(), fitNear(), resize() } or throws when
// no map can be made (no WebGL).
export function createFeedMap(container, { onPick, onPickHand, onBasemap } = {}) {
  const M = ML();
  const map = new M.Map({
    container,
    style: process.env.TILEBOSS ? ['whg-portal'] : BLANK_STYLE,
    center: [0, 20], zoom: 1, minZoom: 0, maxZoom: 17,
    attributionControl: { position: 'bottom-right', autoClose: true, customAttribution: '&copy; World Historical Gazetteer & contributors' },
    navigationControl: { position: 'top-right', showZoom: true, showCompass: false, visualizePitch: false },
  });
  // handleMapCreationFailure returns a stub with no camera when WebGL is unavailable.
  if (!map || typeof map.fitBounds !== 'function' || typeof map.project !== 'function') {
    throw new Error('map could not be created');
  }

  let loaded = false;
  let fellBack = false;
  const fallBack = () => {
    if (loaded || fellBack) return;
    fellBack = true;
    try { map.setStyle(BLANK_STYLE); } catch (_) { /* the markers do not need a style */ }
    if (onBasemap) onBasemap('blank');
  };
  map.once('load', () => { loaded = true; if (onBasemap && !fellBack) onBasemap('ok'); });
  // A style that cannot be fetched never loads (no sourceId: it is the style itself, not a tile).
  map.on('error', (e) => {
    if (loaded || !e || e.sourceId || e.tile) return;
    if (e.error && (e.error.name === 'AbortError' || /abort/i.test(e.error.message || ''))) return;
    fallBack();
  });
  const grace = setTimeout(fallBack, STYLE_GRACE_MS);
  map.once('load', () => clearTimeout(grace));

  // The edge chips sit over the map, outside MapLibre's own canvas.
  const chipLayer = document.createElement('div');
  chipLayer.className = 'mf-map-chips';
  container.appendChild(chipLayer);

  let view = null;
  let handMarkers = [];
  let recordMarker = null;
  let pins = [];            // { i, location, km, title, el, mk, focus... } one per suggestion with a point
  let badges = [];          // merged-badge markers for the current zoom
  let lit = null;           // the suggestion whose row has the focus
  let lastFit = null;

  const bounds = (pts) => pts.reduce((acc, p) => acc.extend(p), new M.LngLatBounds());
  const same = (pts) => pts.every((p) => p[0] === pts[0][0] && p[1] === pts[0][1]);

  function jump(target) {
    if (!target) return;
    map.jumpTo(target);
    lastFit = target;
  }

  function cameraFor(pts, maxZoom = 12) {
    if (!pts.length) return null;
    if (same(pts)) return { center: pts[0], zoom: 10 };
    return map.cameraForBounds(bounds(pts), { padding: 36, maxZoom });
  }

  function allPoints() {
    const pts = pins.map((p) => p.location);
    (view.hand || []).forEach((h) => pts.push(h.location));
    if (view.record && view.record.location) pts.unshift(view.record.location);
    return pts;
  }

  function fitAll() {
    if (!view) return;
    const cam = cameraFor(allPoints());
    if (cam) { jump(cam); relayout(); }
  }

  // The record (plus anything that must stay in view) and as many of the nearest suggestions and
  // search results as keep a regional zoom.
  function fitNear(must = []) {
    if (!view) return;
    const anchor = view.record && view.record.location ? view.record.location : (pins[0] && pins[0].location) || (must[0]);
    if (!anchor) return;
    const d = (p) => (p[0] - anchor[0]) ** 2 + (p[1] - anchor[1]) ** 2;
    const sorted = [...pins.map((p) => p.location), ...(view.hand || []).map((h) => h.location)].sort((a, b) => d(a) - d(b));
    let set = [anchor, ...must];
    let cam = cameraFor(set) || { center: anchor, zoom: 10 };
    for (const p of sorted) {
      const test = [...set, p];
      if (same(test)) { set = test; continue; }
      const c = map.cameraForBounds(bounds(test), { padding: 36, maxZoom: 12 });
      if (c && c.zoom >= MIN_REGIONAL_ZOOM) { set = test; cam = c; } else break;
    }
    jump(cam);
    relayout();
  }

  // ── Markers ────────────────────────────────────────────────────────────────────────────────────
  function clearMarkers() {
    pins.forEach((p) => p.mk.remove());
    badges.forEach((b) => b.remove());
    handMarkers.forEach((m) => m.remove());
    if (recordMarker) recordMarker.remove();
    pins = []; badges = []; handMarkers = []; recordMarker = null;
  }

  function pinClass(p) {
    return ['mf-map-pin', 'mf-map-pin--cand',
      p.state ? `mf-map-pin--${p.state}` : '', p.provisional ? 'mf-map-pin--prov' : '',
      p.status === 'accepted' ? 'mf-map-pin--linked' : '', p.status === 'rejected' ? 'mf-map-pin--declined' : '',
      lit === p.i ? 'mf-map-pin--lit' : ''].filter(Boolean).join(' ');
  }

  function addPin(p) {
    const el = document.createElement('button');
    el.type = 'button';
    el.className = pinClass(p);
    const dot = document.createElement('span');
    dot.className = 'mf-map-marker';
    dot.textContent = String(p.i + 1);
    el.appendChild(dot);
    el.dataset.role = p.state === 'same' ? 'same' : (lit === p.i ? 'focus' : 'other');
    el.dataset.anchor = 'bottom';
    el.dataset.i = String(p.i);
    el.dataset.lng = String(p.location[0]);
    el.dataset.lat = String(p.location[1]);
    el.setAttribute('aria-label', `Suggestion ${p.i + 1}: ${p.title}. Show its row.`);
    el.title = `${p.i + 1}. ${p.title}`;
    el.addEventListener('click', (e) => { e.stopPropagation(); if (onPick) onPick(p.i); });
    const mk = new M.Marker({ element: el, anchor: 'bottom' }).setLngLat(p.location).addTo(map);
    pins.push({ ...p, el, mk });
  }

  function addHand(h) {
    const el = document.createElement('button');
    el.type = 'button';
    el.className = `mf-map-pin mf-map-pin--hand ${h.focus ? 'mf-map-pin--lit' : 'mf-map-pin--cand'}`;
    const dot = document.createElement('span');
    dot.className = 'mf-map-marker';
    dot.textContent = `s${h.i + 1}`;
    el.appendChild(dot);
    el.dataset.role = h.focus ? 'hand-focus' : 'hand';
    el.dataset.anchor = 'bottom';
    el.dataset.i = String(h.i);
    el.dataset.lng = String(h.location[0]);
    el.dataset.lat = String(h.location[1]);
    el.setAttribute('aria-label', `Search result s${h.i + 1}: ${h.title}`);
    el.title = `s${h.i + 1}. ${h.title}`;
    el.addEventListener('click', (e) => { e.stopPropagation(); if (onPickHand) onPickHand(h.i); });
    handMarkers.push(new M.Marker({ element: el, anchor: 'bottom' }).setLngLat(h.location).addTo(map));
  }

  // Markers that would overlap at this zoom become one badge; the rest stay as they are.
  function layoutBadges() {
    badges.forEach((b) => b.remove());
    badges = [];
    if (!pins.length) return;
    const xy = pins.map((p) => map.project(p.location));
    const used = new Set();
    pins.forEach((a, ia) => {
      if (used.has(ia)) return;
      const group = [ia];
      pins.forEach((_b, ib) => {
        if (ib <= ia || used.has(ib)) return;
        if (Math.hypot(xy[ia].x - xy[ib].x, xy[ia].y - xy[ib].y) <= MERGE_PX) group.push(ib);
      });
      group.forEach((g) => used.add(g));
      pins[ia].el.style.display = '';
      if (group.length === 1) return;
      group.forEach((g) => { pins[g].el.style.display = 'none'; });
      const members = group.map((g) => pins[g]);
      const el = document.createElement('button');
      el.type = 'button';
      const states = new Set(members.map((m) => m.state));
      el.className = ['mf-map-pin', 'mf-map-pin--cand', 'mf-map-pin--badge', states.has('same') ? 'mf-map-pin--same' : '',
        members.some((m) => lit === m.i) ? 'mf-map-pin--lit' : ''].filter(Boolean).join(' ');
      const dot = document.createElement('span');
      dot.className = 'mf-map-marker';
      const labels = members.slice(0, 3).map((m) => m.i + 1);
      dot.textContent = labels.join('·') + (members.length > 3 ? `+${members.length - 3}` : '');
      el.appendChild(dot);
      el.dataset.role = 'badge';
      el.dataset.members = members.map((m) => m.i + 1).join(',');
      el.dataset.lng = String(members[0].location[0]);
      el.dataset.lat = String(members[0].location[1]);
      el.setAttribute('aria-label', `Suggestions ${members.map((m) => `${m.i + 1} ${m.title}`).join(', ')} are close together. Show them.`);
      el.title = members.map((m) => `${m.i + 1}. ${m.title}`).join('\n');
      let turn = 0;
      el.addEventListener('click', (e) => {
        e.stopPropagation();
        const pts = members.map((m) => m.location);
        const spread = Math.max(...pts.map((p) => Math.abs(p[0] - pts[0][0])), ...pts.map((p) => Math.abs(p[1] - pts[0][1])));
        if (spread > SAME_SPOT_DEG && map.getZoom() < 16) {
          map.fitBounds(bounds(pts), { padding: 70, maxZoom: 17, duration: 300 });
        } else if (onPick) {       // the same spot: step through them, row by row
          onPick(members[turn % members.length].i);
          turn += 1;
        }
      });
      badges.push(new M.Marker({ element: el, anchor: 'bottom' }).setLngLat(members[0].location).addTo(map));
    });
  }

  // One chip per direction for the suggestions outside the view: their numbers and how far away.
  function layoutChips() {
    chipLayer.textContent = '';
    if (!pins.length && !(view && view.hand && view.hand.length)) return;
    const b = map.getBounds();
    const w = container.clientWidth;
    const h = container.clientHeight;
    if (!w || !h) return;
    const buckets = new Map();
    const items = [...pins.map((p) => ({ label: String(p.i + 1), location: p.location, km: p.km })),
      ...((view && view.hand) || []).map((h) => ({ label: `s${h.i + 1}`, location: h.location, km: null }))];
    items.forEach((p) => {
      if (b.contains(p.location)) return;
      const pt = map.project(p.location);
      const dx = pt.x - w / 2;
      const dy = pt.y - h / 2;
      const k = ((Math.round(Math.atan2(dy, dx) / (Math.PI / 4)) % 8) + 8) % 8;
      const e = buckets.get(k) || { nums: [], kms: [], dx: 0, dy: 0 };
      e.nums.push(p.label);
      if (p.km != null) e.kms.push(p.km);
      e.dx += dx; e.dy += dy;
      buckets.set(k, e);
    });
    buckets.forEach((e, k) => {
      const dx = e.dx / e.nums.length;
      const dy = e.dy / e.nums.length;
      const scale = Math.min((w / 2 - 6) / (Math.abs(dx) || 1), (h / 2 - 14) / (Math.abs(dy) || 1));
      const lo = e.kms.length ? Math.round(Math.min(...e.kms)) : null;
      const hi = e.kms.length ? Math.round(Math.max(...e.kms)) : null;
      const dist = lo == null ? '' : (lo === hi ? `${lo.toLocaleString('en')} km` : `${lo.toLocaleString('en')}–${hi.toLocaleString('en')} km`);
      const chip = document.createElement('button');
      chip.type = 'button';
      chip.className = 'mf-map-chip';
      chip.dataset.dir = WORDS[k];
      chip.dataset.members = e.nums.join(',');
      chip.style.left = `${Math.round(w / 2 + dx * scale)}px`;
      chip.style.top = `${Math.round(h / 2 + dy * scale)}px`;
      // Anchored to the side it points at, so the text stays inside the map.
      chip.style.transform = `translate(${dx > w * 0.15 ? '-100%' : (dx < -w * 0.15 ? '0' : '-50%')}, -50%)`;
      chip.textContent = `${ARROWS[k]} ${e.nums.join(', ')}${dist ? ` · ${dist}` : ''}`;
      chip.setAttribute('aria-label', `Suggestion${e.nums.length > 1 ? 's' : ''} ${e.nums.join(', ')} ${e.nums.length > 1 ? 'are' : 'is'} off the map to the ${WORDS[k]}${dist ? `, ${dist} away` : ''}. Fit all.`);
      chip.addEventListener('click', (ev) => { ev.stopPropagation(); fitAll(); });
      chipLayer.appendChild(chip);
    });
  }

  function relayout() {
    if (!view) return;
    layoutBadges();
    layoutChips();
  }

  // view: { record: {title, location:[lon,lat]} | null,
  //         points: [{i, title, location, km, focus, status, state, provisional, group}],
  //         hand:   [{i, title, location, focus}] }  (the person's own search results)
  function update(next, opts = {}) {
    view = next;
    clearMarkers();
    const f = next.points.find((p) => p.focus);   // the page's focus is the truth; hover only adds to it
    lit = f ? f.i : null;
    (next.hand || []).filter((h) => !h.focus).forEach(addHand);
    next.points.forEach(addPin);
    (next.hand || []).filter((h) => h.focus).forEach(addHand);
    if (next.record && next.record.location) {
      const el = document.createElement('div');
      el.className = 'mf-map-marker mf-map-marker--record';
      el.textContent = '★';
      el.dataset.role = 'record';
      el.dataset.anchor = 'center';
      el.dataset.lng = String(next.record.location[0]);
      el.dataset.lat = String(next.record.location[1]);
      el.setAttribute('role', 'img');
      el.setAttribute('aria-label', `Your record: ${next.record.title}`);
      el.title = `Your record: ${next.record.title}`;
      recordMarker = new M.Marker({ element: el, anchor: 'center' }).setLngLat(next.record.location).addTo(map);
    }
    map.resize();
    if (opts.refit !== false) {
      const focusHand = (next.hand || []).find((h) => h.focus);
      fitNear(focusHand ? [focusHand.location] : []);
    }
    relayout();
  }

  // The row with the focus is lit on the map (and its badge, if it is merged into one).
  function highlight(i) {
    lit = typeof i === 'number' ? i : null;
    pins.forEach((p) => p.el.classList.toggle('mf-map-pin--lit', lit === p.i));
    badges.forEach((b) => {
      const el = b.getElement();
      const ms = (el.dataset.members || '').split(',').map(Number);
      el.classList.toggle('mf-map-pin--lit', lit != null && ms.includes(lit + 1));
    });
  }

  map.on('zoomend', relayout);
  map.on('moveend', layoutChips);

  if (typeof ResizeObserver !== 'undefined') {
    const ro = new ResizeObserver(() => { map.resize(); if (lastFit) map.jumpTo(lastFit); relayout(); });
    ro.observe(container);
  }

  return { map, update, highlight, fitAll, fitNear, resize: () => map.resize() };
}
