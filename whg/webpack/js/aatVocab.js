// /whg/webpack/js/aatVocab.js
/**
 * Shared AAT-vocabulary cache + IndexedDB helpers, extracted from atlas.js so
 * Map-your-Data and the Workbench can reuse the same version-gated cache
 * (place#134). One IndexedDB store — DB ``whg-atlas`` / object store
 * ``registry`` — backs BOTH the Atlas coverage maps (key ``coverage``) and the
 * AAT place-type vocabulary (key ``aat_vocab``), so a concept's label +
 * scope-note is fetched at most once per registry version and shared across
 * every page that renders type chips or the type-tree widget.
 *
 * Version source (client-side, no per-page wiring needed): the base template
 * emits ``<meta name="registry-version">`` on every page via the
 * ``whg.context_processors.registry_version`` context processor. The Atlas page
 * additionally sets a ``registry_version`` JS global (kept as a fallback). When
 * no version can be resolved the vocab is simply fetched fresh each time —
 * correct, just uncached.
 */

import { setAatVocab } from './gazetteerInteraction.js';

const IDB_NAME = 'whg-atlas';
const IDB_STORE = 'registry';

export function idbOpen() {
    return new Promise((resolve, reject) => {
        let req;
        try { req = indexedDB.open(IDB_NAME, 1); } catch (e) { return reject(e); }
        req.onupgradeneeded = () => { try { req.result.createObjectStore(IDB_STORE); } catch (e) { /* */ } };
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error);
    });
}
export function idbGet(key) {
    return idbOpen().then(db => new Promise((res, rej) => {
        const r = db.transaction(IDB_STORE, 'readonly').objectStore(IDB_STORE).get(key);
        r.onsuccess = () => res(r.result); r.onerror = () => rej(r.error);
    }));
}
export function idbPut(key, val) {
    return idbOpen().then(db => new Promise((res, rej) => {
        const tx = db.transaction(IDB_STORE, 'readwrite');
        tx.objectStore(IDB_STORE).put(val, key);
        tx.oncomplete = () => res(); tx.onerror = () => rej(tx.error);
    }));
}

/** The authority-registry change-stamp the caches are keyed on. Prefer the base
 *  template's ``<meta name="registry-version">``, then the Atlas page's
 *  ``registry_version`` JS global, then null (→ always fetch, never cache). */
export function registryVersion() {
    if (typeof document !== 'undefined') {
        const m = document.querySelector('meta[name="registry-version"]');
        if (m && m.content) return m.content;
    }
    // Atlas sets `const registry_version` in the page (a lexical global, not on
    // window) — reachable via the scope chain; guarded so an undefined binding
    // doesn't throw.
    try { if (typeof registry_version !== 'undefined' && registry_version) return registry_version; } catch (e) { /* not defined here */ }
    return null;
}

// De-duplicate concurrent/repeat loads: many modules on one page may call
// loadAatVocab(); they all await the same fetch + cache write.
let _loadPromise = null;

/**
 * Populate the module-local AAT vocab (via setAatVocab) from IndexedDB when the
 * cached version matches, else fetch ``/types/vocab/`` and re-cache. Idempotent
 * per page. Resolves to the ``byId`` map.
 *
 * @param {Object} [opts]
 * @param {string} [opts.version] - override the resolved registry version
 * @returns {Promise<Object>} the ``{ "aat:<id>": {label, desc} }`` map
 */
export function loadAatVocab(opts = {}) {
    if (_loadPromise) return _loadPromise;
    const version = (opts && opts.version != null) ? opts.version : registryVersion();
    const use = (byId) => { if (byId && Object.keys(byId).length) setAatVocab(byId); };
    _loadPromise = (async () => {
        try {
            if (version) {
                const cached = await idbGet('aat_vocab');
                if (cached && cached.version === version && cached.byId) { use(cached.byId); return cached.byId; }
            }
            // WHY THIS GATE IS SAFE, AND WHAT WOULD MAKE IT UNSAFE. The version stamped below is the
            // CURRENT one, applied to whatever this fetch returns — so if the response could come from
            // an HTTP cache, stale bytes would be written under the new version and would then satisfy
            // the gate until the NEXT bump. A version bump whose purpose was to deliver new data would
            // deliver old data, once, permanently, and silently: the store is only rewritten when the
            // gate misses. Measured 2026-09-05: /types/vocab/ carries no Cache-Control, no ETag and no
            // Last-Modified (it does return Content-Length and Vary, so that is a real absence, not a
            // failed request) — a browser has nothing to compute heuristic freshness from, so no such
            // window exists. Put any freshness header on that endpoint, or front it with a CDN, and it
            // does: fetch with `cache: 'no-cache'` here in the same change.
            const data = await fetch('/types/vocab/', { credentials: 'same-origin' }).then(r => r.json());
            use(data.byId);
            if (version && data.byId) {
                try { await idbPut('aat_vocab', { version, byId: data.byId }); } catch (e) { /* best-effort cache */ }
            }
            return data.byId || {};
        } catch (e) {
            console.warn('AAT vocab load failed (type tooltips/labels fall back to ids only)', e);
            return {};
        }
    })();
    return _loadPromise;
}

// Re-export the AAT display helpers so consumers have a single import surface.
export { setAatVocab, aatUrl, aatTooltip, aatTooltipHtml, aatLabel } from './gazetteerInteraction.js';
