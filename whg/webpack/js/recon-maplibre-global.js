// recon-maplibre-global.js
// whg_maplibre.js references a *global* `maplibregl` (it wraps it: `window.whg_maplibre = maplibregl`).
// On normal WHG pages that global is supplied by base.js loading maplibre-gl from a CDN, asynchronously.
// The standalone reconciliation page doesn't wait for that, so importing whg_maplibre.js there throws
// `ReferenceError: maplibregl is not defined` and the whole review-map chunk fails to load.
//
// This shim bundles maplibre-gl locally and publishes it as `window.maplibregl`. It MUST be imported
// before whg_maplibre.js — ES module evaluation runs imports in source order to completion, so this
// module's side effect is guaranteed to have set the global before whg_maplibre.js is evaluated.
import maplibregl from 'maplibre-gl';

// Force-set: deterministic and self-contained (local-first), independent of the CDN load in base.js.
// The reconciliation page has no other map, so overriding the global here is safe.
if (typeof window !== 'undefined') window.maplibregl = maplibregl;

export default maplibregl;
