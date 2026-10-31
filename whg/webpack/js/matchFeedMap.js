// matchFeedMap.js: the small map in the contributor feed, "Link your records" (place#334 4c/4i,
// place#338). Lazy-loaded by matchFeed.js, so the feed works without it (a failed chunk, no WebGL).
//
// One map for the page. It shows the record's own point (a star), the suggestion(s) in focus
// (numbered, as in the list) and the other suggestions muted. Markers are DOM elements, like the
// review map in recon-map.js, so they are placed whether or not the basemap loads, and they carry
// their number and shape (colour never carries meaning alone).
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

const ML = () => window.whg_maplibre;

// container: the map's element. onPick(i): a suggestion's marker was clicked.
// onBasemap(state): 'ok' | 'blank' when the basemap could not be loaded.
// Returns { map, update(view), resize() } or throws when no map can be made (no WebGL).
export function createFeedMap(container, { onPick, onBasemap } = {}) {
  const M = ML();
  const map = new M.Map({
    container,
    style: process.env.TILEBOSS ? ['whg-portal'] : BLANK_STYLE,
    center: [0, 20], zoom: 1, minZoom: 0, maxZoom: 16,
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

  let markers = [];
  let lastFit = null;

  function marker(el, lngLat, anchor = 'center') {
    const mk = new M.Marker({ element: el, anchor }).setLngLat(lngLat).addTo(map);
    markers.push(mk);
    return mk;
  }

  function fit(primary, all) {
    const b = (pts) => pts.reduce((acc, p) => acc.extend(p), new M.LngLatBounds());
    const pad = 36;
    const one = (pts) => pts.length === 1 || pts.every((p) => p[0] === pts[0][0] && p[1] === pts[0][1]);
    let target = null;
    if (all.length && !one(all)) {
      const cam = map.cameraForBounds(b(all), { padding: pad, maxZoom: 12 });
      // Same-named suggestions can lie on other continents; fitting them all would shrink the
      // record and its chosen suggestion to one pixel. Keep the focus legible instead (the
      // others are still on the map, a pan away, and the caption counts them).
      if (cam && cam.zoom >= 3) target = cam;
    }
    if (!target && primary.length && !one(primary)) target = map.cameraForBounds(b(primary), { padding: pad, maxZoom: 12 });
    if (!target) {
      const c = (primary[0] || all[0]);
      if (c) target = { center: c, zoom: 10 };
    }
    if (target) {
      map.jumpTo(target);
      lastFit = target;
    }
  }

  // view: { record: {title, location:[lon,lat]} | null,
  //         points: [{i, title, location:[lon,lat], focus:bool, status}] }
  function update(view) {
    markers.forEach((m) => m.remove());
    markers = [];
    const primary = [];
    const all = [];
    // Muted first, so the focused markers and the record sit on top.
    const pts = [...view.points].sort((a, b) => Number(a.focus) - Number(b.focus));
    pts.forEach((p) => {
      // A pin standing ON its point (anchor bottom), so it never hides the record's star there.
      const el = document.createElement('button');
      el.type = 'button';
      el.className = `mf-map-pin ${p.focus ? 'mf-map-pin--focus' : 'mf-map-pin--muted'}`
        + (p.status === 'accepted' ? ' mf-map-pin--linked' : '')
        + (p.status === 'rejected' ? ' mf-map-pin--declined' : '');
      const dot = document.createElement('span');
      dot.className = 'mf-map-marker';
      dot.textContent = String(p.i + 1);
      el.appendChild(dot);
      el.dataset.role = p.focus ? 'focus' : 'other';
      el.dataset.anchor = 'bottom';
      el.dataset.i = String(p.i);
      el.dataset.lng = String(p.location[0]);
      el.dataset.lat = String(p.location[1]);
      el.setAttribute('aria-label', `Suggestion ${p.i + 1}: ${p.title}`);
      el.title = `${p.i + 1}. ${p.title}`;
      el.addEventListener('click', (e) => { e.stopPropagation(); if (onPick) onPick(p.i); });
      marker(el, p.location, 'bottom');
      all.push(p.location);
      if (p.focus) primary.push(p.location);
    });
    if (view.record && view.record.location) {
      const el = document.createElement('div');
      el.className = 'mf-map-marker mf-map-marker--record';
      el.textContent = '★';
      el.dataset.role = 'record';
      el.dataset.anchor = 'center';
      el.dataset.lng = String(view.record.location[0]);
      el.dataset.lat = String(view.record.location[1]);
      el.setAttribute('role', 'img');
      el.setAttribute('aria-label', `Your record: ${view.record.title}`);
      el.title = `Your record: ${view.record.title}`;
      marker(el, view.record.location);
      primary.unshift(view.record.location);
      all.unshift(view.record.location);
    }
    map.resize();
    fit(primary, all);
  }

  if (typeof ResizeObserver !== 'undefined') {
    const ro = new ResizeObserver(() => { map.resize(); if (lastFit) map.jumpTo(lastFit); });
    ro.observe(container);
  }

  return { map, update, resize: () => map.resize() };
}
