// recon-map.js
// MapLibre map for the Phase 4 candidate-review pane. Uses the project's `whg_maplibre` wrapper
// (imported for its side effect — it sets `window.whg_maplibre`) so the panel gets the same rich
// portal basemap, the layer/style switcher, and the terrain toggle as the place-portal pages.
// Numbered + coloured markers match the candidate list; hover shows the source namespace (by name)
// and alternate toponyms; a ★ marks the row's own coordinate. Lazy-loaded (heavy).

import 'maplibre-gl/dist/maplibre-gl.css';
import './whg_maplibre.js'; // side effect: window.whg_maplibre = (wrapped) maplibregl

// Marker colours, aligned with the candidate-number badges in reconciliation.js.
const COLORS = ['#1565c0', '#c2410c', '#2e7d32', '#6a1b9a', '#00838f', '#b26a00', '#455a64', '#c2185b', '#5d4037'];

let map = null;
let markers = [];
let hoverPopup = null;
let ro = null; // ResizeObserver — keeps MapLibre sized when the accordion pane expands

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

function ensureMap(container) {
  const M = ML();
  if (map && map.getContainer() === container) return map;
  if (map) { try { map.remove(); } catch (_) { /* ignore */ } map = null; markers = []; hoverPopup = null; }
  map = new M.Map({
    container,
    // 2+ style codes → the layer/style switcher auto-appears; terrainControl adds the terrain toggle.
    style: process.env.TILEBOSS ? ['whg-portal', 'Satellite'] : { version: 8, sources: {}, layers: [] },
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
  return map;
}

let lastBounds = null; // remembered so a resize can re-fit once the pane becomes visible
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

// points: [{ci, lon, lat, name, namespace, altNames, score}]; rowPoint: {lon,lat}|null; onAccept: (ci)=>void
export function renderReviewMap(container, points, rowPoint, onAccept) {
  const M = ML();
  const m = ensureMap(container);
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

  lastBounds = bounds;
  const fit = () => {
    m.resize(); // container may have gained size since creation (accordion expand)
    if (!bounds.isEmpty()) m.fitBounds(bounds, { padding: 48, maxZoom: 10, duration: 0 });
  };
  if (m.loaded()) fit(); else m.once('load', fit);
}

export function destroyReviewMap() {
  if (ro) { try { ro.disconnect(); } catch (_) { /* ignore */ } ro = null; }
  if (map) { try { map.remove(); } catch (_) { /* ignore */ } map = null; markers = []; hoverPopup = null; lastBounds = null; }
}
