// recon-map.js
// Small MapLibre map for the Phase 4 candidate-review pane: plots numbered + coloured markers for the
// reconciliation candidates (matching the candidate list), plus a star for the row's own coordinate.
// Lazy-loaded (maplibre-gl is large) — imported only when a review card has geolocatable candidates.

import maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';

// Marker colours, aligned with the candidate-number badges in reconciliation.js.
const COLORS = ['#1565c0', '#c2410c', '#2e7d32', '#6a1b9a', '#00838f', '#b26a00', '#455a64', '#c2185b', '#5d4037'];

let map = null;
let markers = [];

function ensureMap(container) {
  if (map && map.getContainer() === container) return map;
  if (map) { map.remove(); map = null; markers = []; }
  const style = process.env.TILEBOSS
    ? `${process.env.TILEBOSS}/styles/whg-basic-light/style.json`
    : { version: 8, sources: {}, layers: [] }; // blank fallback — markers still work
  map = new maplibregl.Map({
    container, style, attributionControl: false, dragRotate: false, center: [0, 20], zoom: 1,
  });
  map.addControl(new maplibregl.NavigationControl({ showCompass: false }), 'top-right');
  return map;
}

function marker(cls, bg, label, title, onClick) {
  const e = document.createElement('div');
  e.className = cls;
  if (bg) e.style.background = bg;
  e.textContent = label;
  if (title) e.title = title;
  if (onClick) e.addEventListener('click', onClick);
  return e;
}

// points: [{ci, lon, lat, name}]; rowPoint: {lon,lat}|null; onAccept: (ci) => void
export function renderReviewMap(container, points, rowPoint, onAccept) {
  const m = ensureMap(container);
  markers.forEach((mk) => mk.remove());
  markers = [];
  const bounds = new maplibregl.LngLatBounds();

  points.forEach((p) => {
    const el = marker('recon-map-marker', COLORS[p.ci % COLORS.length], String(p.ci + 1), p.name, () => onAccept(p.ci));
    markers.push(new maplibregl.Marker({ element: el }).setLngLat([p.lon, p.lat]).addTo(m));
    bounds.extend([p.lon, p.lat]);
  });
  if (rowPoint) {
    const el = marker('recon-map-marker recon-map-marker--row', null, '★', 'Your coordinate for this place');
    markers.push(new maplibregl.Marker({ element: el }).setLngLat([rowPoint.lon, rowPoint.lat]).addTo(m));
    bounds.extend([rowPoint.lon, rowPoint.lat]);
  }

  const fit = () => { if (!bounds.isEmpty()) m.fitBounds(bounds, { padding: 42, maxZoom: 9, duration: 0 }); };
  if (m.loaded()) fit(); else m.once('load', fit);
  m.resize();
}

export function destroyReviewMap() { if (map) { map.remove(); map = null; markers = []; } }
