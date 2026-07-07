// recon-map.js
// MapLibre map for the Phase 4 candidate-review pane. Uses the project's `whg_maplibre` wrapper
// (imported for its side effect — it sets `window.whg_maplibre`) so the panel gets the same rich
// portal basemap, the layer/style switcher, and the terrain toggle as the place-portal pages.
// Numbered + coloured markers match the candidate list; hover shows the source namespace (by name)
// and alternate toponyms; a ★ marks the row's own coordinate. Lazy-loaded (heavy).
//
// It also hosts a lightweight geometry picker (point / line / polygon) so the reviewer can clone a
// match's location into the dataset or draw a new one, and it persists the layer/basemap choice.

import './recon-maplibre-global.js'; // MUST be first: publishes window.maplibregl (whg_maplibre needs the global)
import 'maplibre-gl/dist/maplibre-gl.css';
import './whg_maplibre.js'; // side effect: window.whg_maplibre = (wrapped) maplibregl

// Marker colours, aligned with the candidate-number badges in reconciliation.js.
const COLORS = ['#1565c0', '#c2410c', '#2e7d32', '#6a1b9a', '#00838f', '#b26a00', '#455a64', '#c2185b', '#5d4037'];
const GEOM_COLOR = '#c2410c';
const LAYER_LS_KEY = 'whg-recon-maplayers';   // persisted basemap/style + hillshade choice
const DEFAULT_STYLES = ['whg-portal', 'Satellite'];

let map = null;
let markers = [];
let hoverPopup = null;
let ro = null; // ResizeObserver — keeps MapLibre sized when the accordion pane expands
let lastBounds = null;
let clickBound = false;

// Geometry-picker state.
let onGeomCb = null;              // callback(geometry|null) fired when the override changes
let currentGeom = null;          // committed override geometry for the current place (GeoJSON geometry)
let draw = { mode: null, verts: [], parts: [] }; // in-progress drawing (parts → Multi* geometries)

const ML = () => window.whg_maplibre; // the wrapped MapLibre (Map/Marker/Popup/LngLatBounds)
const esc = (v) => String(v == null ? '' : v).replace(/[&<>"']/g, (c) =>
  ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

function popupHTML(p) {
  return `<div class="recon-pop">
    <div class="recon-pop-head">
      <span class="recon-pop-name">${esc(p.name)}</span>
      ${p.namespace ? `<span class="recon-pop-ns">${esc(p.namespace)}</span>` : ''}
    </div>
    ${p.score != null ? `<div class="recon-pop-score">score ${esc(p.score)}</div>` : ''}
    ${p.altNames && p.altNames.length
      ? `<div class="recon-pop-alt"><span class="recon-pop-alt-label">also known as:</span> ${p.altNames.slice(0, 15).map(esc).join(', ')}${p.altNames.length > 15 ? '…' : ''}</div>`
      : ''}
  </div>`;
}

// ── Layer/basemap persistence (#2) ──────────────────────────────────────────
function readLayerPrefs() { try { return JSON.parse(localStorage.getItem(LAYER_LS_KEY) || 'null') || {}; } catch (_) { return {}; } }
function writeLayerPrefs(p) { try { localStorage.setItem(LAYER_LS_KEY, JSON.stringify(p)); } catch (_) { /* ignore */ } }
// Put the remembered style first so the wrapper shows it by default (styles[0] is the checked one).
function orderedStyles() {
  const pref = readLayerPrefs().style;
  const arr = DEFAULT_STYLES.slice();
  if (pref && arr.includes(pref)) return [pref].concat(arr.filter((s) => s !== pref));
  return arr;
}
// The wrapper builds the style switcher (radios name="style") and #hillshadeCheckbox inside the map
// container once the map loads. Persist any change; re-apply the stored hillshade toggle.
function hookLayerPersistence(container) {
  if (container.dataset.reconLayerHooked) return; // bind once per container (delegated; survives map recreation)
  container.dataset.reconLayerHooked = '1';
  const save = () => {
    const styleRadio = container.querySelector('input[name="style"]:checked');
    const hill = container.querySelector('#hillshadeCheckbox');
    const prefs = readLayerPrefs();
    if (styleRadio) prefs.style = styleRadio.dataset.value || styleRadio.value;
    if (hill) prefs.hillshade = hill.checked;
    writeLayerPrefs(prefs);
  };
  // Delegate so it works regardless of when the control DOM is built; a label click sets the radio
  // programmatically (no 'change' event), so also save on any click within the style list.
  container.addEventListener('change', (e) => { if (e.target.matches('input[name="style"], #hillshadeCheckbox')) save(); });
  container.addEventListener('click', (e) => { if (e.target.closest('#mapStyleList')) setTimeout(save, 0); });
  const hill = container.querySelector('#hillshadeCheckbox');
  if (hill && readLayerPrefs().hillshade && !hill.checked) {
    hill.checked = true;
    hill.dispatchEvent(new Event('change', { bubbles: true })); // let the wrapper add the hillshade layer
  }
}

// ── Geometry picker (#1) — supports point/line/polygon and their Multi- variants ─────────────
// Re-selecting the same shape button adds another *part* to the existing geometry, promoting it to
// MultiPoint / MultiLineString / MultiPolygon. `draw.parts` holds the parts committed so far in the
// current session; each part's coordinates match the single-geometry shape for that base kind.
function emptyFC() { return { type: 'FeatureCollection', features: [] }; }
// Flatten any geometry's coordinates to a list of [lng,lat] pairs (for fitting bounds).
function allCoords(g) {
  if (!g || !g.coordinates) return [];
  const out = [];
  (function walk(c) {
    if (Array.isArray(c) && typeof c[0] === 'number') out.push(c);
    else if (Array.isArray(c)) c.forEach(walk);
  }(g.coordinates));
  return out;
}
function baseOf(type) {
  if (type === 'Point' || type === 'MultiPoint') return 'point';
  if (type === 'LineString' || type === 'MultiLineString') return 'line';
  if (type === 'Polygon' || type === 'MultiPolygon') return 'polygon';
  return null;
}
// Break a committed geometry into an array of single-part coordinates (for that base kind).
function toParts(g) {
  if (!g) return [];
  switch (g.type) {
    case 'Point': case 'LineString': case 'Polygon': return [g.coordinates];
    case 'MultiPoint': case 'MultiLineString': case 'MultiPolygon': return g.coordinates.slice();
    default: return [];
  }
}
// Build a geometry of base `kind` from single-part coordinates — single or Multi by count.
function fromParts(kind, parts) {
  if (!parts.length) return null;
  const multi = parts.length > 1;
  if (kind === 'point') return { type: multi ? 'MultiPoint' : 'Point', coordinates: multi ? parts : parts[0] };
  if (kind === 'line') return { type: multi ? 'MultiLineString' : 'LineString', coordinates: multi ? parts : parts[0] };
  if (kind === 'polygon') return { type: multi ? 'MultiPolygon' : 'Polygon', coordinates: multi ? parts : parts[0] };
  return null;
}
// The in-progress part's coordinates from the vertices collected so far (line/polygon only).
function draftPart() {
  const v = draw.verts;
  if (draw.mode === 'line') return v.length >= 2 ? v.slice() : null;
  if (draw.mode === 'polygon') return v.length >= 3 ? [v.concat([v[0]])] : null;
  return null;
}
// What to render: the parts committed this drawing session (or the standing geometry when idle).
function committedPreview() { return draw.mode ? fromParts(draw.mode, draw.parts) : currentGeom; }
function ensureGeomLayers() {
  if (!map || !map.isStyleLoaded || !map.isStyleLoaded() || map.getSource('recon-geom')) return;
  try {
    // Filters must be consistent modern expressions and match Multi* variants too.
    map.addSource('recon-geom', { type: 'geojson', data: emptyFC() });
    map.addLayer({ id: 'recon-geom-fill', type: 'fill', source: 'recon-geom', filter: ['match', ['geometry-type'], ['Polygon', 'MultiPolygon'], true, false], paint: { 'fill-color': GEOM_COLOR, 'fill-opacity': 0.15 } });
    map.addLayer({ id: 'recon-geom-line', type: 'line', source: 'recon-geom', filter: ['match', ['geometry-type'], ['LineString', 'MultiLineString', 'Polygon', 'MultiPolygon'], true, false], paint: { 'line-color': GEOM_COLOR, 'line-width': 2 } });
    map.addLayer({ id: 'recon-geom-vtx', type: 'circle', source: 'recon-geom', filter: ['all', ['match', ['geometry-type'], ['Point', 'MultiPoint'], true, false], ['==', ['get', 'v'], 1]], paint: { 'circle-radius': 5, 'circle-color': '#fff', 'circle-stroke-color': GEOM_COLOR, 'circle-stroke-width': 2 } });
    map.addLayer({ id: 'recon-geom-pt', type: 'circle', source: 'recon-geom', filter: ['all', ['match', ['geometry-type'], ['Point', 'MultiPoint'], true, false], ['!=', ['get', 'v'], 1]], paint: { 'circle-radius': 7, 'circle-color': GEOM_COLOR, 'circle-stroke-color': '#fff', 'circle-stroke-width': 2 } });
  } catch (_) { /* style not ready yet; the next styledata/load will re-add */ }
}
function redrawGeom() {
  if (!map || !map.getSource('recon-geom')) return;
  const feats = [];
  const g = committedPreview();
  if (g) feats.push({ type: 'Feature', geometry: g, properties: {} });
  // In-progress part of a line/polygon: the connecting segment plus vertex handles.
  if (draw.mode && draw.mode !== 'point') {
    if (draw.verts.length >= 2) feats.push({ type: 'Feature', geometry: { type: 'LineString', coordinates: draw.verts }, properties: {} });
    draw.verts.forEach((pt) => feats.push({ type: 'Feature', geometry: { type: 'Point', coordinates: pt }, properties: { v: 1 } }));
  }
  map.getSource('recon-geom').setData({ type: 'FeatureCollection', features: feats });
}
function commitGeom(g) {
  draw = { mode: null, verts: [], parts: [] };
  currentGeom = g;
  if (map) map.getCanvas().style.cursor = '';
  redrawGeom();
  if (onGeomCb) onGeomCb(g);
}
function onMapClick(e) {
  if (!draw.mode) return;
  const pt = [e.lngLat.lng, e.lngLat.lat];
  if (draw.mode === 'point') { draw.parts.push(pt); commitGeom(fromParts('point', draw.parts)); return; }
  draw.verts.push(pt);
  redrawGeom();
}
// Public picker API, called from the review card. Re-selecting the same base shape seeds the existing
// geometry's parts, so the next drawing appends a part (→ Multi*); a different shape starts fresh.
export function startDraw(kind) {
  if (!map) return;
  const seed = (currentGeom && baseOf(currentGeom.type) === kind) ? toParts(currentGeom) : [];
  draw = { mode: kind, verts: [], parts: seed };
  map.getCanvas().style.cursor = 'crosshair';
  redrawGeom();
}
export function finishDraw() {
  if (!draw.mode) return;
  if (draw.mode !== 'point') { const dp = draftPart(); if (dp) draw.parts.push(dp); }
  commitGeom(fromParts(draw.mode, draw.parts));
}
export function clearGeom() { commitGeom(null); }
// Set an override without going through drawing (e.g. cloned from a match), without firing onGeom.
export function setOverride(g) { draw = { mode: null, verts: [], parts: [] }; currentGeom = g || null; redrawGeom(); }

function ensureMap(container) {
  const M = ML();
  if (map && map.getContainer() === container) return map;
  if (map) { try { map.remove(); } catch (_) { /* ignore */ } map = null; markers = []; hoverPopup = null; clickBound = false; }
  map = new M.Map({
    container,
    // 2+ style codes → the layer/style switcher auto-appears; terrainControl adds the terrain toggle.
    style: process.env.TILEBOSS ? orderedStyles() : { version: 8, sources: {}, layers: [] },
    center: [0, 20], zoom: 1, maxZoom: 17,
    terrainControl: !!process.env.TILEBOSS,
    fullscreenControl: true,
  });
  hoverPopup = new M.Popup({ closeButton: false, closeOnClick: false, offset: 16, className: 'recon-map-popup' });

  // The map is created inside a collapsed accordion pane (ancestor display:none → container 0×0).
  // When the pane expands the container gains its real size; MapLibre must be told to resize or it
  // stays blank. A ResizeObserver catches every such transition (pane expand, fullscreen, window).
  if (ro) { try { ro.disconnect(); } catch (_) { /* ignore */ } }
  if (typeof ResizeObserver !== 'undefined') {
    ro = new ResizeObserver((entries) => {
      for (const e of entries) {
        const r = e.contentRect;
        if (map && r.width > 0 && r.height > 0) { map.resize(); refit(); }
      }
    });
    ro.observe(container);
  }
  // Switching basemap style wipes custom sources/layers — re-add the geometry layers each time.
  map.on('styledata', () => { ensureGeomLayers(); redrawGeom(); });
  return map;
}

function refit() {
  if (map && lastBounds && !lastBounds.isEmpty()) {
    map.fitBounds(lastBounds, { padding: 48, maxZoom: 10, duration: 0 });
  }
}

// Called from the workbench when the review pane is expanded, as a belt-and-braces companion to
// the ResizeObserver (some browsers coalesce the observer callback a frame late).
export function resizeReviewMap() {
  if (map) { map.resize(); refit(); }
}

// Highlight the marker for candidate `ci` (called on list hover; null clears). Also raises it.
export function setMarkerHover(ci) {
  Object.keys(markerByCi).forEach((k) => {
    const el = markerByCi[k];
    const on = String(ci) === k;
    el.classList.toggle('recon-map-marker--hover', on);
    if (on) el.style.zIndex = '3';
    else el.style.removeProperty('z-index');
  });
}

// points: [{ci, lon, lat, name, namespace, altNames, score}]; rowPoint: {lon,lat}|null
// opts: { onAccept(ci), onGeom(geometry|null), override: <GeoJSON geometry>|null,
//         selected: [ci,…] accepted candidates, onHover(ci|null): reflect map-hover back to the list }
let markerByCi = {};
export function renderReviewMap(container, points, rowPoint, opts) {
  opts = opts || {};
  const onAccept = opts.onAccept || (() => {});
  const onHover = opts.onHover || (() => {});
  const selected = new Set(opts.selected || []);
  const M = ML();
  const m = ensureMap(container);
  onGeomCb = opts.onGeom || null;
  currentGeom = opts.override || null;
  draw = { mode: null, verts: [], parts: [] };
  markers.forEach((mk) => mk.remove());
  markers = [];
  markerByCi = {};
  const bounds = new M.LngLatBounds();

  points.forEach((p) => {
    const el = document.createElement('div');
    el.className = 'recon-map-marker' + (selected.has(p.ci) ? ' recon-map-marker--selected' : '');
    el.style.background = COLORS[p.ci % COLORS.length];
    el.style.cursor = 'pointer';
    el.textContent = String(p.ci + 1);
    el.addEventListener('click', () => onAccept(p.ci));
    el.addEventListener('mouseenter', () => { hoverPopup.setLngLat([p.lon, p.lat]).setHTML(popupHTML(p)).addTo(m); setMarkerHover(p.ci); onHover(p.ci); });
    el.addEventListener('mouseleave', () => { hoverPopup.remove(); setMarkerHover(null); onHover(null); });
    markerByCi[p.ci] = el;
    markers.push(new M.Marker({ element: el }).setLngLat([p.lon, p.lat]).addTo(m));
    bounds.extend([p.lon, p.lat]);
  });
  if (rowPoint) {
    const el = document.createElement('div');
    el.className = 'recon-map-marker recon-map-marker--row';
    el.textContent = '★';
    el.title = 'Your coordinate for this place';
    markers.push(new M.Marker({ element: el }).setLngLat([rowPoint.lon, rowPoint.lat]).addTo(m));
    bounds.extend([rowPoint.lon, rowPoint.lat]);
  }
  allCoords(currentGeom).forEach((pt) => bounds.extend(pt));

  lastBounds = bounds;
  const onReady = () => {
    m.resize(); // container may have gained size since creation (accordion expand)
    ensureGeomLayers();
    redrawGeom();
    if (!clickBound) { m.on('click', onMapClick); clickBound = true; }
    hookLayerPersistence(container);
    if (!bounds.isEmpty()) m.fitBounds(bounds, { padding: 48, maxZoom: 10, duration: 0 });
  };
  if (m.loaded()) onReady(); else m.once('load', onReady);
}

export function destroyReviewMap() {
  if (ro) { try { ro.disconnect(); } catch (_) { /* ignore */ } ro = null; }
  if (map) { try { map.remove(); } catch (_) { /* ignore */ } map = null; markers = []; hoverPopup = null; lastBounds = null; clickBound = false; }
}

// ── Full-dataset map (pane 5) ────────────────────────────────────────────────
// Plots EVERY located row as one GeoJSON source rendered by a GPU circle layer (NOT DOM markers), with
// clustering — so it scales to tens of thousands of points client-side without choking the DOM. A
// heatmap layer takes over at low zoom for a density read on very large sets. Local-first: no tiles/server.
let fullMap = null, fullPopup = null, fullRo = null, fullData = null;
function fullPopupHTML(p) {
  return `<div class="recon-fullpop">
    <div class="recon-fullpop-title">${esc(p.title || '')}</div>
    ${p.admin ? `<div class="text-muted small">${esc(p.admin)}</div>` : ''}
    ${p.date ? `<div class="small"><i class="fas fa-calendar-days me-1 text-secondary"></i>${esc(p.date)}</div>` : ''}
    ${p.match ? `<div class="small"><i class="fas fa-check text-success me-1"></i>${esc(p.match)}${p.score ? ` <span class="text-muted">(${esc(p.score)})</span>` : ''}</div>` : ''}
    ${p.coord ? `<div class="small text-muted">${esc(p.coord)}</div>` : ''}
  </div>`;
}
function ensureFullLayers() {
  if (!fullMap || fullMap.getSource('recon-full')) return; // idempotent; retried on load/styledata/idle
  try {
    fullMap.addSource('recon-full', { type: 'geojson', data: fullData || { type: 'FeatureCollection', features: [] }, cluster: true, clusterMaxZoom: 12, clusterRadius: 46 });
    // Density heatmap — visible only when zoomed out (a fast read on large datasets).
    fullMap.addLayer({ id: 'recon-full-heat', type: 'heatmap', source: 'recon-full', maxzoom: 6,
      paint: { 'heatmap-radius': 14, 'heatmap-opacity': ['interpolate', ['linear'], ['zoom'], 0, 0.7, 6, 0] } });
    fullMap.addLayer({ id: 'recon-full-clusters', type: 'circle', source: 'recon-full', filter: ['has', 'point_count'],
      paint: { 'circle-color': '#1565c0', 'circle-opacity': 0.8, 'circle-stroke-color': '#fff', 'circle-stroke-width': 1.5,
        'circle-radius': ['step', ['get', 'point_count'], 15, 50, 20, 500, 27, 5000, 34] } });
    fullMap.addLayer({ id: 'recon-full-count', type: 'symbol', source: 'recon-full', filter: ['has', 'point_count'],
      layout: { 'text-field': ['get', 'point_count_abbreviated'], 'text-size': 12 }, paint: { 'text-color': '#fff' } });
    fullMap.addLayer({ id: 'recon-full-pt', type: 'circle', source: 'recon-full', filter: ['!', ['has', 'point_count']], minzoom: 4,
      paint: { 'circle-color': '#c2410c', 'circle-radius': 6, 'circle-stroke-color': '#fff', 'circle-stroke-width': 1.5 } });
    // Place-name label beside each unclustered point (just the name — full detail is in the hover popup).
    // text-optional + no overlap keeps it uncluttered: labels that would collide are dropped, not stacked.
    fullMap.addLayer({ id: 'recon-full-label', type: 'symbol', source: 'recon-full', filter: ['!', ['has', 'point_count']], minzoom: 6,
      layout: { 'text-field': ['get', 'title'], 'text-size': 11, 'text-offset': [0, 1.1], 'text-anchor': 'top',
        'text-optional': true, 'text-allow-overlap': false, 'symbol-sort-key': ['-', 0, ['coalesce', ['to-number', ['get', 'score'], 0], 0]] },
      paint: { 'text-color': '#333', 'text-halo-color': '#fff', 'text-halo-width': 1.4 } });
    fullMap.on('click', 'recon-full-clusters', (e) => {
      const f = e.features[0];
      fullMap.getSource('recon-full').getClusterExpansionZoom(f.properties.cluster_id, (err, z) => { if (!err) fullMap.easeTo({ center: f.geometry.coordinates, zoom: z }); });
    });
    // Point details on hover (not click): mousemove keeps the popup on the point under the cursor
    // even as you slide between nearby points; mouseleave hides it when the cursor leaves the layer.
    fullMap.on('mousemove', 'recon-full-pt', (e) => {
      if (!e.features.length) return;
      const f = e.features[0];
      fullMap.getCanvas().style.cursor = 'pointer';
      fullPopup.setLngLat(f.geometry.coordinates).setHTML(fullPopupHTML(f.properties)).addTo(fullMap);
    });
    fullMap.on('mouseleave', 'recon-full-pt', () => { fullMap.getCanvas().style.cursor = ''; fullPopup.remove(); });
    fullMap.on('mouseenter', 'recon-full-clusters', () => { fullMap.getCanvas().style.cursor = 'pointer'; });
    fullMap.on('mouseleave', 'recon-full-clusters', () => { fullMap.getCanvas().style.cursor = ''; });
  } catch (err) { console.error('[recon] full-map layers failed', err); }
}
// geojson: FeatureCollection of Point features with { title, admin, date, match, score, coord } properties.
export function renderFullMap(container, geojson) {
  const M = ML();
  fullData = geojson || { type: 'FeatureCollection', features: [] };
  if (!fullMap || fullMap.getContainer() !== container) {
    if (fullMap) { try { fullMap.remove(); } catch (_) { /* ignore */ } }
    fullMap = new M.Map({
      container,
      style: process.env.TILEBOSS ? ['whg-portal', 'Satellite'] : { version: 8, sources: {}, layers: [] },
      center: [0, 20], zoom: 1, maxZoom: 17, terrainControl: !!process.env.TILEBOSS, fullscreenControl: true,
    });
    fullPopup = new M.Popup({ closeButton: false, closeOnClick: false, maxWidth: '300px', className: 'recon-map-popup recon-map-popup--hover' });
    if (fullRo) { try { fullRo.disconnect(); } catch (_) { /* ignore */ } }
    if (typeof ResizeObserver !== 'undefined') { fullRo = new ResizeObserver((es) => { for (const e of es) if (e.contentRect.width > 0 && e.contentRect.height > 0) fullMap.resize(); }); fullRo.observe(container); }
    const reapply = () => { ensureFullLayers(); const s = fullMap.getSource('recon-full'); if (s) s.setData(fullData); };
    fullMap.on('styledata', reapply);
    fullMap.on('idle', reapply); // fires once the style is fully loaded — reliable point to add the layers
  }
  const apply = () => {
    fullMap.resize();
    ensureFullLayers();
    const s = fullMap.getSource('recon-full'); if (s) s.setData(fullData);
    const b = new M.LngLatBounds();
    fullData.features.forEach((f) => b.extend(f.geometry.coordinates));
    if (!b.isEmpty()) fullMap.fitBounds(b, { padding: 40, maxZoom: 9, duration: 0 });
  };
  if (fullMap.loaded()) apply(); else fullMap.once('load', apply);
}
export function resizeFullMap() { if (fullMap) fullMap.resize(); }

// ── Scope map (dataset-wide region picker) ────────────────────────────────────
// A standalone map for the Scope modal: draw one bounding polygon to constrain reconciliation to inside
// it. Kept fully independent of the review map (its own singletons) so the two never clash. Reuses the
// module's geometry helpers (emptyFC / allCoords) and marker colour.
let scopeMap = null, scopeRo = null, scopeCb = null;
let scopeGeom = null;   // committed polygon (GeoJSON Polygon | null)
let scopeVerts = [];    // in-progress vertices
let scopeDrawing = false;

function scopePolygonFrom(verts) {
  return verts.length >= 3 ? { type: 'Polygon', coordinates: [verts.concat([verts[0]])] } : null;
}
function ensureScopeLayers() {
  if (!scopeMap || !scopeMap.isStyleLoaded || !scopeMap.isStyleLoaded() || scopeMap.getSource('scope-geom')) return;
  try {
    scopeMap.addSource('scope-geom', { type: 'geojson', data: emptyFC() });
    scopeMap.addLayer({ id: 'scope-fill', type: 'fill', source: 'scope-geom', filter: ['match', ['geometry-type'], ['Polygon', 'MultiPolygon'], true, false], paint: { 'fill-color': GEOM_COLOR, 'fill-opacity': 0.15 } });
    scopeMap.addLayer({ id: 'scope-line', type: 'line', source: 'scope-geom', filter: ['match', ['geometry-type'], ['LineString', 'MultiLineString', 'Polygon', 'MultiPolygon'], true, false], paint: { 'line-color': GEOM_COLOR, 'line-width': 2 } });
    scopeMap.addLayer({ id: 'scope-vtx', type: 'circle', source: 'scope-geom', filter: ['==', ['geometry-type'], 'Point'], paint: { 'circle-radius': 5, 'circle-color': '#fff', 'circle-stroke-color': GEOM_COLOR, 'circle-stroke-width': 2 } });
  } catch (_) { /* style not ready; retried on styledata */ }
}
function redrawScope() {
  if (!scopeMap || !scopeMap.getSource('scope-geom')) return;
  const feats = [];
  const g = scopeDrawing ? scopePolygonFrom(scopeVerts) : scopeGeom;
  if (g) feats.push({ type: 'Feature', geometry: g, properties: {} });
  if (scopeDrawing) {
    if (scopeVerts.length >= 2) feats.push({ type: 'Feature', geometry: { type: 'LineString', coordinates: scopeVerts }, properties: {} });
    scopeVerts.forEach((pt) => feats.push({ type: 'Feature', geometry: { type: 'Point', coordinates: pt }, properties: {} }));
  }
  scopeMap.getSource('scope-geom').setData({ type: 'FeatureCollection', features: feats });
}
function onScopeClick(e) {
  if (!scopeDrawing) return;
  scopeVerts.push([e.lngLat.lng, e.lngLat.lat]);
  redrawScope();
}
export function scopeDraw() {
  if (!scopeMap) return;
  scopeDrawing = true; scopeVerts = [];
  scopeMap.getCanvas().style.cursor = 'crosshair';
  ensureScopeLayers(); // the source may not have been added yet if drawing starts very early
  redrawScope();
}
export function scopeFinish() {
  if (!scopeDrawing) return;
  const poly = scopePolygonFrom(scopeVerts);
  if (poly) scopeGeom = poly; // fewer than 3 vertices → keep the previous geometry
  scopeDrawing = false; scopeVerts = [];
  if (scopeMap) scopeMap.getCanvas().style.cursor = '';
  redrawScope();
  if (scopeCb) scopeCb(scopeGeom);
}
export function scopeClear() {
  scopeGeom = null; scopeDrawing = false; scopeVerts = [];
  if (scopeMap) scopeMap.getCanvas().style.cursor = '';
  redrawScope();
  if (scopeCb) scopeCb(null);
}
// container: the map div; existingGeom: a previously-committed polygon to show; onChange(geometry|null).
export function renderScopeMap(container, existingGeom, onChange) {
  const M = ML();
  scopeCb = onChange || null;
  scopeGeom = existingGeom || null;
  scopeDrawing = false; scopeVerts = [];
  if (!scopeMap || scopeMap.getContainer() !== container) {
    if (scopeMap) { try { scopeMap.remove(); } catch (_) { /* ignore */ } }
    scopeMap = new M.Map({
      container,
      style: process.env.TILEBOSS ? ['whg-portal', 'Satellite'] : { version: 8, sources: {}, layers: [] },
      center: [0, 20], zoom: 1, maxZoom: 17, terrainControl: !!process.env.TILEBOSS, fullscreenControl: true,
    });
    if (scopeRo) { try { scopeRo.disconnect(); } catch (_) { /* ignore */ } }
    if (typeof ResizeObserver !== 'undefined') { scopeRo = new ResizeObserver((es) => { for (const e of es) if (e.contentRect.width > 0 && e.contentRect.height > 0) scopeMap.resize(); }); scopeRo.observe(container); }
    // Re-add the geometry source/layers whenever the style settles. The whg-portal basemap + terrain
    // trigger late style reloads that wipe custom sources; 'styledata' alone can miss the final one, so
    // (as the full-dataset map does) also re-apply on 'idle' — the reliable "everything is loaded" signal.
    scopeMap.on('styledata', () => { ensureScopeLayers(); redrawScope(); });
    scopeMap.on('idle', () => { ensureScopeLayers(); redrawScope(); });
    scopeMap.on('click', onScopeClick);
  }
  const apply = () => {
    scopeMap.resize(); // container was likely 0×0 while the modal was hidden
    ensureScopeLayers();
    redrawScope();
    if (scopeGeom) { const b = new M.LngLatBounds(); allCoords(scopeGeom).forEach((pt) => b.extend(pt)); if (!b.isEmpty()) scopeMap.fitBounds(b, { padding: 40, maxZoom: 9, duration: 0 }); }
  };
  if (scopeMap.loaded()) apply(); else scopeMap.once('load', apply);
}
