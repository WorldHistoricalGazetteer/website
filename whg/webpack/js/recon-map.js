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
let draw = { mode: null, verts: [] }; // in-progress drawing

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

// ── Geometry picker (#1) ────────────────────────────────────────────────────
function emptyFC() { return { type: 'FeatureCollection', features: [] }; }
function collectVertices(g) {
  if (!g) return [];
  if (g.type === 'Point') return [g.coordinates];
  if (g.type === 'LineString') return g.coordinates;
  if (g.type === 'Polygon') return g.coordinates[0] || [];
  return [];
}
function draftGeom() {
  const v = draw.verts;
  if (!v.length) return null;
  if (draw.mode === 'point') return { type: 'Point', coordinates: v[0] };
  if (draw.mode === 'line') return v.length >= 2 ? { type: 'LineString', coordinates: v } : null;
  if (draw.mode === 'polygon') return v.length >= 3 ? { type: 'Polygon', coordinates: [v.concat([v[0]])] } : { type: 'LineString', coordinates: v };
  return null;
}
function ensureGeomLayers() {
  if (!map || !map.isStyleLoaded || !map.isStyleLoaded() || map.getSource('recon-geom')) return;
  try {
    map.addSource('recon-geom', { type: 'geojson', data: emptyFC() });
    map.addLayer({ id: 'recon-geom-fill', type: 'fill', source: 'recon-geom', filter: ['==', '$type', 'Polygon'], paint: { 'fill-color': GEOM_COLOR, 'fill-opacity': 0.15 } });
    map.addLayer({ id: 'recon-geom-line', type: 'line', source: 'recon-geom', filter: ['in', '$type', 'LineString', 'Polygon'], paint: { 'line-color': GEOM_COLOR, 'line-width': 2 } });
    map.addLayer({ id: 'recon-geom-vtx', type: 'circle', source: 'recon-geom', filter: ['all', ['==', '$type', 'Point'], ['==', ['get', 'v'], 1]], paint: { 'circle-radius': 5, 'circle-color': '#fff', 'circle-stroke-color': GEOM_COLOR, 'circle-stroke-width': 2 } });
    map.addLayer({ id: 'recon-geom-pt', type: 'circle', source: 'recon-geom', filter: ['all', ['==', '$type', 'Point'], ['!=', ['get', 'v'], 1]], paint: { 'circle-radius': 7, 'circle-color': GEOM_COLOR, 'circle-stroke-color': '#fff', 'circle-stroke-width': 2 } });
  } catch (_) { /* style not ready yet; the next styledata/load will re-add */ }
}
function redrawGeom() {
  if (!map || !map.getSource('recon-geom')) return;
  const feats = [];
  const g = draw.mode ? draftGeom() : currentGeom;
  if (g) feats.push({ type: 'Feature', geometry: g, properties: {} });
  // Vertex handles while drawing a line/polygon.
  if (draw.mode && draw.mode !== 'point') draw.verts.forEach((pt) => feats.push({ type: 'Feature', geometry: { type: 'Point', coordinates: pt }, properties: { v: 1 } }));
  map.getSource('recon-geom').setData({ type: 'FeatureCollection', features: feats });
}
function commitGeom(g) {
  draw = { mode: null, verts: [] };
  currentGeom = g;
  if (map) map.getCanvas().style.cursor = '';
  redrawGeom();
  if (onGeomCb) onGeomCb(g);
}
function onMapClick(e) {
  if (!draw.mode) return;
  const pt = [e.lngLat.lng, e.lngLat.lat];
  if (draw.mode === 'point') { draw.verts = [pt]; commitGeom({ type: 'Point', coordinates: pt }); return; }
  draw.verts.push(pt);
  redrawGeom();
}
// Public picker API, called from the review card.
export function startDraw(kind) {
  if (!map) return;
  draw = { mode: kind, verts: [] };
  currentGeom = null;
  map.getCanvas().style.cursor = 'crosshair';
  redrawGeom();
}
export function finishDraw() { commitGeom(draftGeom()); }
export function clearGeom() { commitGeom(null); }
// Set an override without going through drawing (e.g. cloned from a match), without firing onGeom.
export function setOverride(g) { draw = { mode: null, verts: [] }; currentGeom = g || null; redrawGeom(); }

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

// points: [{ci, lon, lat, name, namespace, altNames, score}]; rowPoint: {lon,lat}|null
// opts: { onAccept(ci), onGeom(geometry|null), override: <GeoJSON geometry>|null }
export function renderReviewMap(container, points, rowPoint, opts) {
  opts = opts || {};
  const onAccept = opts.onAccept || (() => {});
  const M = ML();
  const m = ensureMap(container);
  onGeomCb = opts.onGeom || null;
  currentGeom = opts.override || null;
  draw = { mode: null, verts: [] };
  markers.forEach((mk) => mk.remove());
  markers = [];
  const bounds = new M.LngLatBounds();

  points.forEach((p) => {
    const el = document.createElement('div');
    el.className = 'recon-map-marker';
    el.style.background = COLORS[p.ci % COLORS.length];
    el.style.cursor = 'pointer';
    el.textContent = String(p.ci + 1);
    el.addEventListener('click', () => onAccept(p.ci));
    el.addEventListener('mouseenter', () => hoverPopup.setLngLat([p.lon, p.lat]).setHTML(popupHTML(p)).addTo(m));
    el.addEventListener('mouseleave', () => hoverPopup.remove());
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
  collectVertices(currentGeom).forEach((pt) => bounds.extend(pt));

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
