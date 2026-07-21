// /whg/webpack/js/atlas.js
// WHG Atlas — Hero Map Explorer + Toponymic Search
//
// New entry point for /atlas/ page. Reuses existing modules
// (filterState, TypeTreeWidget, etc.) but wires them
// into the hero-map layout instead of the three-column search layout.

import { errorModal } from './error-modal.js';
import throttle from 'lodash/throttle';
import debounce from 'lodash/debounce';
import { geomsGeoJSON } from './utilities';
import CountryParents from './countryParents';
import TypeTreeWidget from './typeTreeWidget';
import filterState from './filterState';
import heroMap from './heroMap';
import LayerSourcesPalette from './layerSourcesPalette';
import AreaSearchRouter from './areaSearchRouter';
import { startAtlasTour, hasSeenAtlasTour } from './atlasTour.js';
import { polygonToCells, latLngToCell, cellToParent } from 'h3-js';
import { clusterHits, suggestTheta } from './clustering.js';
import PlaceList from './atlasPlaceList.js';
import { setWebTemplates, setAatVocab } from './gazetteerInteraction.js';
import './toggle-truncate.js';
import '../css/typeahead.css';
import '../css/atlas.css';

/* ═══════════════════════════════════════════════════════════════════
   State
   ═══════════════════════════════════════════════════════════════════ */

let results = null;
let searchDisabled = false;
let enteringPortal = false;
let typeTree = null;
let layerPalette = null;
let areaRouter = null;
let temporalMode = 'off';       // 'off' | 'range' | 'undated'
let searchMode = 'areas';       // 'areas' | 'toponyms'
let exactMatch = false;
let useViewport = false;       // viewport-constraint toggle (non-globe only)
let clusterResults = true;
let selectedRegions = [];        // Array of {id, label, admin_level, namespace, geometry}
let areaSearchResults = [];      // Current area search dropdown results
let areaDropdownIndex = -1;

/* ── Gazetteers offcanvas mode-state caches ──
   Keep Filter and Explore selections separately so switching tabs restores the
   last-recorded selection instead of dropping it on each input-type swap. */
const filterSelections = new Set();   // namespaces ticked in Filter mode (multi-select)
let exploreSelection = null;          // single namespace selected in Explore mode

/* ── Temporal range state ── */
let temporalFrom = 800;
let temporalTo = 1800;
const TEMPORAL_MIN = -2000;
const TEMPORAL_MAX = 2100;

/* Default merge sensitivity (θ_query). Bumped from the calibration default 0.55
   to a more disambiguating 0.65: empirically (spread-vs-θ sweep on ambiguous
   names) the max intra-cluster geographic spread collapses from ~30 km to ~4 km
   between 0.55 and 0.65 — tightening loose same-name merges — while 0.70 starts
   over-splitting genuine multi-source places into pairs. */
const ATLAS_DEFAULT_THETA = 0.65;

/* ═══════════════════════════════════════════════════════════════════
   Init: Load data dependencies
   ═══════════════════════════════════════════════════════════════════ */

const countryParents = new CountryParents();
await countryParents.dataLoaded;

/* ── Display-name helpers for cluster cards ──
   Gazetteer (namespace) names come from the gazetteer registry (/api/sources/,
   loaded once, non-blocking); a static map covers the common authorities before
   the registry resolves, with an uppercase fallback for anything unknown.
   Country names come from ``window.ccode_hash`` (loaded above via CountryParents). */
const NS_NAMES = {
    gn: 'GeoNames', wd: 'Wikidata', tgn: 'Getty TGN', osm: 'OpenStreetMap',
    ohm: 'OpenHistoricalMap', pl: 'Pleiades', pleiades: 'Pleiades',
    whg: 'World Historical Gazetteer', chgis: 'CHGIS', hgis: 'HGIS de las Indias',
    alc: 'Alcedo', gb: 'GB1900', gb1900: 'GB1900', ukhc: 'UK Historic Counties',
};
const _nsNames = {};
fetch('/api/sources/', { credentials: 'same-origin', headers: { Accept: 'application/json' } })
    .then(r => (r.ok ? r.json() : null))
    .then(d => {
        const webMap = {};
        ((d && d.sources) || []).forEach(s => {
            if (s.namespace && s.name) _nsNames[s.namespace] = s.name;
            if (s.namespace && s.web_item) webMap[s.namespace] = s.web_item;
        });
        // Feed per-source "view at source" templates to the popup renderer (place#121).
        setWebTemplates(webMap);
    })
    .catch(() => { /* fall back to the static NS_NAMES / built-in web templates */ });

function nsLabel(ns) {
    const k = String(ns || '').toLowerCase();
    // The gazetteer registry (/api/sources/) is the source of truth for names;
    // NS_NAMES is only a fallback before it resolves / for namespaces it lacks.
    // Short or acronym names (e.g. tgn→"TGN") are fixed in the authority metadata
    // on the indexing side, not patched here.
    return _nsNames[k] || NS_NAMES[k] || k.toUpperCase();
}
function ccLabel(cc) {
    const e = (window.ccode_hash || {})[cc];
    return (e && (e.gnlabel || e.tgnlabel)) || cc;
}
// Distinct toponyms attested by one place record. Each gateway ``names[]`` entry
// packs surface forms into a comma-joined ``label``; split, trim, de-dupe (ci).
function memberToponyms(m) {
    const seen = new Set();
    const out = [];
    (m.names || []).forEach(n => String(n.label || '').split(',').forEach(t => {
        const s = t.trim();
        const k = s.toLowerCase();
        if (s && !seen.has(k)) { seen.add(k); out.push(s); }
    }));
    return out;
}
// Toponym variants for a cluster (common to all) or a member (extras), rendered
// as plain inline comma-separated text (chips waste space). A single entry just
// replicates the headline title, so it's omitted. A toggle that reveals only one
// or two names wastes a click, so show up to five inline; collapse only when at
// least three would be hidden (first three inline + "<n> more toponyms" toggle).
function toponymsList(toponyms, extraClass) {
    if (!toponyms || toponyms.length <= 1) return '';
    const esc = t => escapeHtml(t);
    const cls = `toponyms-list${extraClass ? ' ' + extraClass : ''}`;
    if (toponyms.length <= 5) {
        return `<div class="${cls}"><span class="toponym-names">${toponyms.map(esc).join(', ')}</span></div>`;
    }
    const shown = toponyms.slice(0, 3).map(esc).join(', ');
    const rest = toponyms.slice(3);
    return `<div class="${cls}">`
        + `<span class="toponym-names">${shown}</span>`
        + `<details class="toponyms-more"><summary>${rest.length} more toponyms</summary>`
        + `<span class="toponym-names">, ${rest.map(esc).join(', ')}</span></details>`
        + `</div>`;
}
// Distinct type labels for one place record. Prefers AAT-resolved labels
// (normalised, cross-source) via the result-set facet map (aat_id → friendly
// label). Places whose source type has NO AAT mapping (custom types — place#122)
// carry no aat_ids, so fall back to the source's own ``types[].sourceLabel`` so
// their types are still surfaced (the popup already does this).
function placeTypeLabels(m, aatLabels) {
    const seen = new Set();
    const out = [];
    const add = (label) => {
        if (label == null || label === '') return;
        const s = String(label);
        const k = s.toLowerCase();
        if (!seen.has(k)) { seen.add(k); out.push(s); }
    };
    (m.aat_ids || []).forEach(id => add((aatLabels && aatLabels[id]) || null));
    if (!out.length) {
        (m.types || []).forEach(t => {
            if (t && typeof t === 'object') add(t.sourceLabel || t.label || t.identifier);
        });
    }
    return out;
}
// Back-compat alias — cluster cards call memberTypes(m, aatLabels).
function memberTypes(m, aatLabels) {
    return placeTypeLabels(m, aatLabels);
}
// Given a per-member array of display-string lists, split into the entries
// common to ALL members (intersection, case-insensitive) and each member's
// remaining (extra) entries. Preserves the first-seen surface form.
function splitCommonExtra(perMember) {
    const sets = perMember.map(arr => new Set(arr.map(s => s.toLowerCase())));
    const commonKeys = sets.length
        ? new Set([...sets[0]].filter(k => sets.every(s => s.has(k))))
        : new Set();
    const common = (perMember[0] || []).filter(s => commonKeys.has(s.toLowerCase()));
    const extras = perMember.map(arr => arr.filter(s => !commonKeys.has(s.toLowerCase())));
    return { common, extras };
}
// Inline chip row (namespaces / countries / types). Each chip may carry a hover title.
function chipRow(cls, items) {
    if (!items.length) return '';
    return `<div class="${cls}">`
        + items.map(it => `<span class="${it.chip}"${it.title ? ` title="${escapeHtml(it.title)}"` : ''}>${escapeHtml(it.text)}</span>`).join('')
        + `</div>`;
}
// Temporal formatting: negative years → BCE; a [start,end] range collapses to a
// single year when equal. Returns '' for undated (null/malformed) ranges.
function formatYear(y) {
    if (y == null) return '';
    return y < 0 ? `${-y} BCE` : `${y}`;
}
function formatRange(tr) {
    if (!Array.isArray(tr) || tr.length !== 2) return '';
    const [s, e] = tr;
    if (s == null && e == null) return '';
    if (s === e) return formatYear(s);
    return `${formatYear(s)}–${formatYear(e)}`; // en-dash
}
// The cluster's overall attested span: earliest start → latest end across the
// members that carry a temporal_range. Null when none are dated.
function clusterTemporalSpan(members) {
    const starts = [], ends = [];
    for (const m of members) {
        const tr = m.temporal_range;
        if (Array.isArray(tr) && tr.length === 2) {
            if (tr[0] != null) starts.push(tr[0]);
            if (tr[1] != null) ends.push(tr[1]);
        }
    }
    if (!starts.length && !ends.length) return null;
    return [starts.length ? Math.min(...starts) : null, ends.length ? Math.max(...ends) : null];
}

/* ═══════════════════════════════════════════════════════════════════
   Custom temporal range control
   ═══════════════════════════════════════════════════════════════════ */

let temporalRangeChanged = throttle(() => {
    filterState.set('temporal.start_year', temporalFrom);
    filterState.set('temporal.stop_year', temporalTo);
}, 300);

function fillTemporalSlider() {
    const fromSlider = document.getElementById('temporal_from_slider');
    const toSlider = document.getElementById('temporal_to_slider');
    if (!fromSlider || !toSlider) return;
    const range = TEMPORAL_MAX - TEMPORAL_MIN;
    const fromPct = ((temporalFrom - TEMPORAL_MIN) / range) * 100;
    const toPct = ((temporalTo - TEMPORAL_MIN) / range) * 100;
    toSlider.style.background = `linear-gradient(to right,
        #b0bec5 0%, #b0bec5 ${fromPct}%,
        #546e7a ${fromPct}%, #546e7a ${toPct}%,
        #b0bec5 ${toPct}%, #b0bec5 100%)`;

    // Keep the drag-band aligned with the fill, inset ~8px (half a thumb) from
    // each handle so it only covers the MIDDLE — the handles stay independently
    // grabbable. Hide it when the range is too narrow to leave a middle.
    const band = document.getElementById('temporal_range_band');
    if (band) {
        const THUMB = 8;
        const wrap = document.querySelector('.temporal-slider-wrap');
        const px = wrap ? wrap.getBoundingClientRect().width : 200;
        const midPx = ((toPct - fromPct) / 100) * px - 2 * THUMB;
        if (midPx > 6) {
            band.style.left = `calc(${fromPct}% + ${THUMB}px)`;
            band.style.width = `calc(${toPct - fromPct}% - ${2 * THUMB}px)`;
            band.style.display = 'block';
        } else {
            band.style.display = 'none';
        }
    }
}

function updateTemporalLabels() {
    const fromLabel = document.getElementById('temporal_from_label');
    const toLabel = document.getElementById('temporal_to_label');
    if (fromLabel) fromLabel.textContent = temporalFrom;
    if (toLabel) toLabel.textContent = temporalTo;
}

// Client-side mirror of the gateway temporal filter — interval overlap on the
// hit's `temporal_range` ([start,end] or null=undated); undated hits pass only
// in +Undated mode. Lets the range control live-refilter the LOADED results
// instantly, ahead of the debounced authoritative server re-search.
function temporalHitPasses(h) {
    if (temporalMode === 'off') return true;
    const tr = h.temporal_range;
    if (!Array.isArray(tr) || tr.length !== 2) return temporalMode === 'undated';
    const s = tr[0] == null ? -Infinity : tr[0];
    const e = tr[1] == null ? Infinity : tr[1];
    return s <= temporalTo && e >= temporalFrom;
}

// Live temporal interaction: throttled instant re-filter/re-cluster of the loaded
// set + a debounced authoritative re-query of the whole index within the range.
const throttledTemporalRender = throttle(() => { if (gatewayData) renderClusters(); }, 100);
const debouncedTemporalResearch = debounce(() => {
    const input = document.getElementById('atlas_search_input');
    if (gatewayData && input && input.value.trim()) initiateToponymSearch({ preserveFacets: true });
}, 550);
function applyTemporalLive() {
    throttledTemporalRender();     // instant preview on what's already loaded
    debouncedTemporalResearch();   // authoritative top-N in-range from the index
    applyGazetteerCoverageFilter(); // re-filter the Gazetteers list if its Date Range switch is on
}

// ── Gazetteers coverage maps (temporal + coarse H3) ──
// Fetched from /atlas/registry/coverage/ and cached in IndexedDB, keyed by the
// page's `registry_version`, so the ~200 KB coarse-H3 payload is downloaded once
// per registry change instead of inlined on every Atlas page load.
let coverageTemporal = {};   // namespace → [earliest, latest]
let coverageH3 = {};         // namespace → "global" | [res-2 cells]

const IDB_NAME = 'whg-atlas';
const IDB_STORE = 'registry';
function idbOpen() {
    return new Promise((resolve, reject) => {
        let req;
        try { req = indexedDB.open(IDB_NAME, 1); } catch (e) { return reject(e); }
        req.onupgradeneeded = () => { try { req.result.createObjectStore(IDB_STORE); } catch (e) { /* */ } };
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error);
    });
}
function idbGet(key) {
    return idbOpen().then(db => new Promise((res, rej) => {
        const r = db.transaction(IDB_STORE, 'readonly').objectStore(IDB_STORE).get(key);
        r.onsuccess = () => res(r.result); r.onerror = () => rej(r.error);
    }));
}
function idbPut(key, val) {
    return idbOpen().then(db => new Promise((res, rej) => {
        const tx = db.transaction(IDB_STORE, 'readwrite');
        tx.objectStore(IDB_STORE).put(val, key);
        tx.oncomplete = () => res(); tx.onerror = () => rej(tx.error);
    }));
}

// Populate coverageTemporal/coverageH3 — from IndexedDB when the cached version
// matches the page's registry_version (no network), else fetch + re-cache.
async function loadRegistryCoverage() {
    const version = (typeof registry_version !== 'undefined') ? registry_version : null;
    const useMaps = (t, h) => { coverageTemporal = t || {}; coverageH3 = h || {}; applyGazetteerCoverageFilter(); };
    try {
        if (version) {
            const cached = await idbGet('coverage');
            if (cached && cached.version === version) { useMaps(cached.temporal, cached.h3); return; }
        }
        const data = await fetch('/atlas/registry/coverage/', { credentials: 'same-origin' }).then(r => r.json());
        useMaps(data.temporal, data.h3);
        if (data.version) { try { await idbPut('coverage', { version: data.version, temporal: data.temporal, h3: data.h3 }); } catch (e) { /* best-effort cache */ } }
    } catch (e) {
        console.warn('Atlas: registry coverage load failed (coverage filters will keep all gazetteers visible)', e);
    }
}

// Prefetch the AAT place-type vocabulary (id → label + scope note) into the same
// IndexedDB store (key 'aat_vocab'), version-gated by the page's
// aat_vocab_version, and hand it to the popup renderer so type chips get an
// aat:<id> + description tooltip (place#122). Reusable for any AAT-label need.
async function loadAatVocab() {
    // Cache-keyed by registry_version (the vocab tracks the indexed corpus).
    const version = (typeof registry_version !== 'undefined') ? registry_version : null;
    const use = (byId) => { if (byId && Object.keys(byId).length) setAatVocab(byId); };
    try {
        if (version) {
            const cached = await idbGet('aat_vocab');
            if (cached && cached.version === version && cached.byId) { use(cached.byId); return; }
        }
        const data = await fetch('/types/vocab/', { credentials: 'same-origin' }).then(r => r.json());
        use(data.byId);
        if (version && data.byId) {
            try { await idbPut('aat_vocab', { version, byId: data.byId }); } catch (e) { /* best-effort */ }
        }
    } catch (e) {
        console.warn('Atlas: AAT vocab load failed (type tooltips fall back to ids only)', e);
    }
}

// ── Gazetteers coverage filtering (client-side) ──
function temporalCoverageOverlaps(extent, from, to) {
    if (!Array.isArray(extent) || extent.length < 2) return true;   // unknown → keep visible
    const e0 = extent[0], e1 = extent[1];
    if (e0 == null && e1 == null) return true;                       // fully undated → keep visible
    const lo = (e0 == null) ? -Infinity : e0;
    const hi = (e1 == null) ? Infinity : e1;
    return lo <= to && hi >= from;                                   // interval overlap
}
// Res-2 H3 cells the selected area(s) touch — matches the registry's
// `h3_coverage_coarse` resolution. polygonToCells covers big areas; the per-
// vertex cells catch areas smaller than a (huge, ~86,000 km²) res-2 hex.
const COVERAGE_H3_RES = 2;
function computeAreaH3Cells() {
    const cells = new Set();
    // Area geometries arrive as Polygon, MultiPolygon, OR GeometryCollection
    // (the boundary sources wrap selections in a GeometryCollection) — recurse.
    const addGeom = (geom) => {
        if (!geom) return;
        if (geom.type === 'GeometryCollection') { (geom.geometries || []).forEach(addGeom); return; }
        const polys = geom.type === 'MultiPolygon' ? geom.coordinates
            : (geom.type === 'Polygon' ? [geom.coordinates] : []);
        for (const polyCoords of polys) {
            try { polygonToCells(polyCoords, COVERAGE_H3_RES, true).forEach(c => cells.add(c)); } catch (e) { /* */ }
            for (const pt of (polyCoords[0] || [])) {
                try { cells.add(latLngToCell(pt[1], pt[0], COVERAGE_H3_RES)); } catch (e) { /* */ }
            }
        }
    };
    for (const r of selectedRegions) addGeom(r && r.geometry);
    return cells;
}

function applyGazetteerCoverageFilter() {
    const temporalMap = coverageTemporal;
    const h3Map = coverageH3;
    const periodSw = document.getElementById('gazetteer_filter_period');
    const periodOn = periodSw && periodSw.checked && !periodSw.disabled;
    const areaSw = document.getElementById('gazetteer_filter_area');
    const areaOn = areaSw && areaSw.checked && !areaSw.disabled;
    const areaCells = areaOn ? computeAreaH3Cells() : null;
    document.querySelectorAll('#gazetteers_offcanvas .authority-item[data-namespace]').forEach(item => {
        const ns = item.dataset.namespace;
        let hide = false;
        if (periodOn && !temporalCoverageOverlaps(temporalMap[ns], temporalFrom, temporalTo)) {
            hide = true;
        }
        if (!hide && areaOn && areaCells) {
            const cov = h3Map[ns];
            if (cov === 'global') { /* global coverage always overlaps */ }
            else if (Array.isArray(cov) && cov.length) {
                // The coarse set is COMPACTED (cells at res 0–2). A res-2 area
                // cell overlaps if it, or either coarser ancestor (res-1/res-0),
                // is in the set.
                const covSet = new Set(cov);
                let overlap = false;
                for (const c of areaCells) {
                    if (covSet.has(c) || covSet.has(cellToParent(c, 1)) || covSet.has(cellToParent(c, 0))) {
                        overlap = true; break;
                    }
                }
                if (!overlap) hide = true;
            } /* else: no coarse coverage → unknown → keep visible */
        }
        item.classList.toggle('coverage-hidden', hide);
    });
}

function wireTemporalControl() {
    const fromSlider = document.getElementById('temporal_from_slider');
    const toSlider = document.getElementById('temporal_to_slider');
    if (!fromSlider || !toSlider) return;

    fromSlider.addEventListener('input', () => {
        temporalFrom = parseInt(fromSlider.value);
        if (temporalFrom >= temporalTo) {
            temporalTo = temporalFrom;
            toSlider.value = temporalTo;
        }
        updateTemporalLabels();
        fillTemporalSlider();
        temporalRangeChanged();
        applyTemporalLive();
    });

    toSlider.addEventListener('input', () => {
        temporalTo = parseInt(toSlider.value);
        if (temporalTo <= temporalFrom) {
            temporalFrom = temporalTo;
            fromSlider.value = temporalFrom;
        }
        updateTemporalLabels();
        fillTemporalSlider();
        temporalRangeChanged();
        applyTemporalLive();
    });

    // Temporal mode toggle
    document.querySelectorAll('#temporal_control .temporal-mode-toggle .btn').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('#temporal_control .temporal-mode-toggle .btn')
                .forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            temporalMode = btn.dataset.temporalMode;
            const tc = document.getElementById('temporal_control');
            tc.classList.toggle('temporal-off', temporalMode === 'off');
            updateGazetteerPeriodSwitch();
            applyTemporalLive();
        });
    });
    updateGazetteerPeriodSwitch();

    // Drag the whole range band (shift both handles, preserving span).
    const wrap = document.querySelector('.temporal-slider-wrap');
    const band = document.getElementById('temporal_range_band');
    if (wrap && band) {
        let dragging = false, startX = 0, startFrom = 0, span = 0;
        band.addEventListener('pointerdown', (e) => {
            dragging = true;
            startX = e.clientX;
            startFrom = temporalFrom;
            span = temporalTo - temporalFrom;
            band.classList.add('dragging');
            try { band.setPointerCapture(e.pointerId); } catch (_) { /* noop */ }
            e.preventDefault();
        });
        band.addEventListener('pointermove', (e) => {
            if (!dragging) return;
            const px = wrap.getBoundingClientRect().width || 1;
            const deltaYears = Math.round(((e.clientX - startX) / px) * (TEMPORAL_MAX - TEMPORAL_MIN));
            let newFrom = startFrom + deltaYears;
            // Clamp so the span stays within bounds (both handles move together).
            newFrom = Math.max(TEMPORAL_MIN, Math.min(newFrom, TEMPORAL_MAX - span));
            temporalFrom = newFrom;
            temporalTo = newFrom + span;
            fromSlider.value = temporalFrom;
            toSlider.value = temporalTo;
            updateTemporalLabels();
            fillTemporalSlider();
            temporalRangeChanged();
            applyTemporalLive();
        });
        const endDrag = (e) => {
            if (!dragging) return;
            dragging = false;
            band.classList.remove('dragging');
            try { band.releasePointerCapture(e.pointerId); } catch (_) { /* noop */ }
        };
        band.addEventListener('pointerup', endDrag);
        band.addEventListener('pointercancel', endDrag);
    }

    // Initial fill
    fillTemporalSlider();
}

function resetTemporalControl() {
    temporalFrom = 800;
    temporalTo = 1800;
    temporalMode = 'off';
    const fromSlider = document.getElementById('temporal_from_slider');
    const toSlider = document.getElementById('temporal_to_slider');
    if (fromSlider) fromSlider.value = temporalFrom;
    if (toSlider) toSlider.value = temporalTo;
    updateTemporalLabels();
    fillTemporalSlider();
    document.querySelectorAll('#temporal_control .temporal-mode-toggle .btn')
        .forEach(b => b.classList.remove('active'));
    const offBtn = document.querySelector('#temporal_control .temporal-mode-toggle .btn[data-temporal-mode="off"]');
    if (offBtn) offBtn.classList.add('active');
    document.getElementById('temporal_control')?.classList.add('temporal-off');
    updateGazetteerPeriodSwitch();
}

// The Gazetteers "Hide gazetteers outside Date Range filter" switch only makes
// sense while a Date Range filter is active — disable + clear it otherwise.
function updateGazetteerPeriodSwitch() {
    setGazetteerCoverageSwitch('gazetteer_filter_period', temporalMode !== 'off');
}

// Likewise, the "Hide gazetteers outside Area filter" switch only makes sense
// once one or more areas have been selected.
function updateGazetteerAreaSwitch() {
    setGazetteerCoverageSwitch('gazetteer_filter_area', selectedRegions.length > 0);
}

// Wrapper tooltips explaining why a coverage switch is disabled (a disabled
// input can't fire hover, so the tooltip lives on the enclosing .form-check).
const COVERAGE_TOOLTIPS = {
    gazetteer_filter_area: {
        off: 'Select one or more areas (in Areas mode) first — then this hides gazetteers whose coverage doesn’t include the selected area.',
        on: 'Hides gazetteers whose coverage doesn’t overlap the selected area(s).',
    },
    gazetteer_filter_period: {
        off: 'Turn on the Date Range filter first — then this hides gazetteers whose dates don’t overlap it.',
        on: 'Hides gazetteers whose date coverage doesn’t overlap the Date Range.',
    },
};

// Enable/disable a coverage-filter switch, clearing it (and its stub note) when
// disabled, dimming its label, and updating its wrapper tooltip.
function setGazetteerCoverageSwitch(id, active) {
    const sw = document.getElementById(id);
    if (!sw) return;
    sw.disabled = !active;
    if (!active && sw.checked) {
        sw.checked = false;
        sw.dispatchEvent(new Event('change', { bubbles: true })); // hides the stub note if now all-off
    }
    const wrap = sw.closest('.form-check');
    if (wrap) {
        const label = wrap.querySelector('.form-check-label');
        if (label) label.classList.toggle('text-muted', !active);
        const tips = COVERAGE_TOOLTIPS[id];
        if (tips) {
            const text = active ? tips.on : tips.off;
            wrap.setAttribute('data-bs-title', text);
            try {
                if (window.bootstrap) {
                    const tt = window.bootstrap.Tooltip.getOrCreateInstance(wrap, { trigger: 'hover' });
                    tt.setContent({ '.tooltip-inner': text });
                }
            } catch (e) { /* bootstrap not ready — initAtlasTooltips picks it up */ }
        }
    }
}

/* ═══════════════════════════════════════════════════════════════════
   Map init
   ═══════════════════════════════════════════════════════════════════ */

function waitMapLoad() {
    return heroMap.init().then(() => {
        // Wire click on result features
        heroMap.map.on('click', function (e) {
            const features = heroMap.map.queryRenderedFeatures(e.point);
            if (features.length > 0 && features[0].layer.id.startsWith('places_')) {
                const idx = features[0].id;
                if (clusterFC && clusterFC.features[idx]) {
                    // Gateway/cluster mode: highlight the marker + scroll to /
                    // highlight the cluster card that contains this hit.
                    const pid = clusterFC.features[idx].properties.pid;
                    highlightHits([idx]);
                    const $card = $(`#atlas_search_results .cluster-card[data-pids~="${pid}"]`);
                    $('#atlas_search_results .cluster-card').removeClass('cluster-highlight');
                    $card.addClass('cluster-highlight');
                    if ($card.length) $card[0].scrollIntoView({ behavior: 'smooth', block: 'nearest' });
                } else {
                    $('#atlas_search_results .result').eq(idx)
                        .attr('data-map-clicked', 'true').click();
                }
            }
        });
        heroMap.map.on('mousemove', function (e) {
            const features = heroMap.map.queryRenderedFeatures(e.point);
            if (features.length > 0 && features[0].layer.id.startsWith('places_')) {
                heroMap.map.getCanvas().style.cursor = 'pointer';
            }
        });
    });
}

function waitDocumentReady() {
    return new Promise((resolve) => $(document).ready(() => resolve()));
}

// ── Welcome panel ──────────────────────────────────────────────────────────
// Persisted opt-out (mirrors atlasTour.js's TOUR_SEEN_KEY): once the user
// clicks "Don't show this again" the panel never returns, and the first-visit
// auto-tour is suppressed too (the tour stays relaunchable via the bottom-left
// button). The panel is shown immediately in the template, so its controls are
// wired on document-ready — NOT behind the map-load gate below — otherwise a
// slow/failed map load would trap the user behind an undismissable panel.
const WELCOME_DISMISSED_KEY = 'whg_atlas_welcome_dismissed';
function isWelcomeDismissed() {
    try { return localStorage.getItem(WELCOME_DISMISSED_KEY) === 'true'; }
    catch (e) { return false; }
}
// Shared so the map-drag handler (added after map load) can also fade the panel.
let fadeOutWelcome = () => {};

function setupWelcomePanel() {
    const welcomePanel = document.getElementById('atlas_welcome');
    if (!welcomePanel) return;
    if (isWelcomeDismissed()) {
        // Opted out: hide the welcome, but show a lightweight "please wait"
        // placeholder so a returning user still sees something while the map
        // loads. It auto-hides once the map is ready (map-gated .then() below);
        // a fallback timeout clears it if the map load never signals.
        welcomePanel.style.display = 'none';
        const loading = document.getElementById('atlas_loading');
        if (loading) {
            loading.style.display = '';
            setTimeout(() => { loading.style.display = 'none'; }, 20000);
        }
        return;
    }
    fadeOutWelcome = () => {
        welcomePanel.classList.add('atlas-welcome-hidden');
        welcomePanel.addEventListener('transitionend', () => {
            welcomePanel.style.display = 'none';
        }, { once: true });
    };

    // Dismiss button (this visit only)
    const dismissBtn = document.getElementById('atlas_welcome_dismiss');
    if (dismissBtn) dismissBtn.addEventListener('click', fadeOutWelcome);

    // "Don't show this again" — persist the opt-out, then fade out.
    const dontShowBtn = document.getElementById('atlas_welcome_dontshow');
    if (dontShowBtn) {
        dontShowBtn.addEventListener('click', () => {
            try { localStorage.setItem(WELCOME_DISMISSED_KEY, 'true'); } catch (e) { /* private mode */ }
            fadeOutWelcome();
        });
    }

    // Fade when any (already-present) control receives focus/click.
    const controlSelectors = [
        '#atlas_search_input',
        '.search-mode-toggle .btn',
        '.atlas-control-buttons .btn',
        '#temporal_control',
        '#atlas_initiate_search',
        '#atlas_exact_match',
        '#atlas_clear_search',
        '.maplibregl-ctrl-zoom-in',
        '.maplibregl-ctrl-zoom-out',
        '.maplibregl-ctrl-globe',
    ];
    controlSelectors.forEach(sel => {
        document.querySelectorAll(sel).forEach(el => {
            el.addEventListener('focus', fadeOutWelcome, { once: true });
            el.addEventListener('click', fadeOutWelcome, { once: true });
        });
    });

    // Tour link inside welcome panel
    const tourLink = document.getElementById('atlas_start_tour_link');
    if (tourLink) {
        tourLink.addEventListener('click', (e) => {
            e.preventDefault();
            fadeOutWelcome();
            setTimeout(() => startAtlasTour(), 400);
        });
    }
}
waitDocumentReady().then(setupWelcomePanel);
// Load the gazetteer coverage maps (IndexedDB-cached, version-gated) — decoupled
// from the map, so the coverage filters work even if the map is slow/unavailable.
waitDocumentReady().then(loadRegistryCoverage);
waitDocumentReady().then(loadAatVocab);

/* ═══════════════════════════════════════════════════════════════════
   DOM wiring — runs after map + DOM ready
   ═══════════════════════════════════════════════════════════════════ */

Promise.all([
    waitMapLoad(),
    waitDocumentReady(),
    Promise.all(select2_CDN_fallbacks.map(loadResource)),
]).then(() => {

    // Map is ready — clear the "please wait" placeholder (opted-out users).
    const atlasLoadingEl = document.getElementById('atlas_loading');
    if (atlasLoadingEl) atlasLoadingEl.style.display = 'none';

    // ── Start globe spin on load ──
    heroMap.startSpin();

    // ── Initialise Layer Sources palette (inside offcanvas) ──
    layerPalette = new LayerSourcesPalette(
        '#layer_sources_panel', null,
        typeof available_sources !== 'undefined' ? available_sources : []
    );

    // ── Initialise Area Search Router ──
    areaRouter = new AreaSearchRouter(layerPalette);

    // ── Initialise custom temporal range control ──
    wireTemporalControl();

    // ── Wire boundary click events ──
    document.addEventListener('boundary-click', (e) => {
        if (searchMode !== 'areas') return;
        const detail = e.detail;
        if (!detail || !detail.geometry) return;
        addRegionSelection({
            id: `boundary:${detail.namespace || 'osm'}:${detail.id || detail.name}`,
            label: detail.name || 'Unnamed',
            admin_level: detail.admin_level,
            namespace: detail.namespace || 'osm',
            geometry: detail.geometry,
        });
    });

    // ── Search mode toggle ──
    document.querySelectorAll('.search-mode-toggle .btn').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('.search-mode-toggle .btn')
                .forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            switchSearchMode(btn.dataset.searchMode);
        });
    });

    // ── Wire search input ──
    const searchInput = document.getElementById('atlas_search_input');
    const debouncedAreaSearch = debounce(() => performAreaSearch(), 250);

    searchInput.addEventListener('input', () => {
        if (searchMode === 'areas') {
            debouncedAreaSearch();
        }
    });

    searchInput.addEventListener('keydown', (e) => {
        if (searchMode === 'areas') {
            handleAreaDropdownKeydown(e);
        }
    });

    searchInput.addEventListener('keyup', (e) => {
        if (e.key === 'Enter' && searchMode === 'toponyms') {
            initiateToponymSearch();
        }
    });

    // ── Wire buttons ──
    document.getElementById('atlas_initiate_search').addEventListener('click', () => {
        if (searchMode === 'toponyms') initiateToponymSearch();
        else performAreaSearch();
    });

    document.getElementById('atlas_clear_search').addEventListener('click', () => {
        clearAll();
    });

    document.getElementById('atlas_close_results').addEventListener('click', () => {
        hideResultsPanel();
        switchSearchMode('areas');
    });

    // Relocate Regions/Gazetteers/Categories into the fixed panel as views.
    initPanelViews();

    // BETA: Gazetteer → Explore "Place Lists" panel (place#125). Inject the
    // atlas.js collaborators it needs (search-option builder, portal opener,
    // panel switcher, CSRF token) so the module stays free of a circular import.
    PlaceList.configure({
        getBaseOptions: gatherToponymOptions,
        openPortal: openAtlasPortal,
        showPanelView: showPanelView,
        getCsrf: () => csrfToken,
    });

    // ── BETA: cluster merge-sensitivity (θ) slider — re-clusters live ──
    const thetaSlider = document.getElementById('atlas_cluster_theta');
    if (thetaSlider) {
        const thetaVal = document.getElementById('atlas_cluster_theta_val');
        // Seed the control to the Atlas default so the displayed value matches.
        thetaSlider.value = clusterThetaOverride;
        if (thetaVal) thetaVal.textContent = clusterThetaOverride.toFixed(2);
        const applyTheta = () => {
            clusterThetaOverride = parseFloat(thetaSlider.value);
            if (thetaVal) thetaVal.textContent = clusterThetaOverride.toFixed(2);
            // Manual drag → the user owns θ now; stop auto-fitting + drop the badge.
            thetaUserSet = true;
            thetaAutoFitted = false;
            const autoBadge = document.getElementById('atlas_cluster_theta_auto');
            if (autoBadge) autoBadge.style.display = 'none';
            if (gatewayData) renderClusters();
        };
        thetaSlider.addEventListener('input', debounce(applyTheta, 120));
    }

    // ── BETA: per-signal weight sliders — re-cluster live ──
    const applyWeights = debounce(() => { if (gatewayData) renderClusters(); }, 120);
    document.querySelectorAll('.weight-slider').forEach(sl => {
        sl.addEventListener('input', () => {
            const f = sl.dataset.facet;
            weightOverrides[f] = parseFloat(sl.value);
            const rd = document.querySelector(`.weight-val[data-facet="${f}"]`);
            if (rd) rd.textContent = parseFloat(sl.value).toFixed(2);
            applyWeights();
        });
    });
    // ── BETA: same-gazetteer repulsion — re-cluster live ──
    const nsPenaltySlider = document.getElementById('atlas_ns_penalty');
    if (nsPenaltySlider) {
        nsPenaltySlider.addEventListener('input', () => {
            sameNsPenaltyOverride = parseFloat(nsPenaltySlider.value);
            const rd = document.getElementById('atlas_ns_penalty_val');
            if (rd) rd.textContent = sameNsPenaltyOverride.toFixed(2);
            applyWeights();
        });
    }

    // ── Bootstrap tooltips: initialise every static trigger in the Atlas controls
    //    so they all render as consistent styled tooltips (not native title tips).
    //    Bootstrap loads via a deferred CDN script, so it may not be ready at init
    //    time — try now, then on window.load, then poll briefly until it appears. ──
    const initAtlasTooltips = () => {
        const bs = window.bootstrap;
        if (!bs || !bs.Tooltip) return false;
        document.querySelectorAll('#floating_search [data-bs-toggle="tooltip"], .gazetteer-coverage-filters [data-bs-toggle="tooltip"]').forEach(el => {
            // Move the native `title` into a BS data attr and drop it, so the
            // browser's own tooltip doesn't ALSO pop up alongside the styled one.
            const t = el.getAttribute('title');
            if (t != null) {
                if (!el.getAttribute('data-bs-title')) el.setAttribute('data-bs-title', t);
                el.removeAttribute('title');
            }
            bs.Tooltip.getOrCreateInstance(el, { trigger: 'hover' });
        });
        return true;
    };
    if (!initAtlasTooltips()) {
        window.addEventListener('load', initAtlasTooltips, { once: true });
        let tries = 0;
        const ttTimer = setInterval(() => { if (initAtlasTooltips() || ++tries > 20) clearInterval(ttTimer); }, 150);
    }

    // ── Tooltip on/off preference (for experienced users) — persisted; hides
    //    every rendered tooltip on this page via a body class. ──
    const TT_OFF_KEY = 'whg_atlas_tooltips_off';
    const ttToggle = document.getElementById('atlas_tooltips_toggle');
    const applyTooltipPref = () => {
        const off = localStorage.getItem(TT_OFF_KEY) === '1';
        document.body.classList.toggle('atlas-tooltips-off', off);
        if (ttToggle) ttToggle.checked = !off;
    };
    if (ttToggle) {
        ttToggle.addEventListener('change', () => {
            const off = !ttToggle.checked;
            localStorage.setItem(TT_OFF_KEY, off ? '1' : '0');
            document.body.classList.toggle('atlas-tooltips-off', off);
        });
    }
    applyTooltipPref();

    // ── Wire exact match toggle ──
    document.getElementById('atlas_exact_match').addEventListener('click', function () {
        exactMatch = !exactMatch;
        this.classList.toggle('active', exactMatch);
        this.setAttribute('aria-pressed', exactMatch);
    });

    // ── Wire viewport constraint toggle ──
    const viewportBtn = document.getElementById('atlas_viewport_btn');
    const viewportWrap = document.getElementById('atlas_viewport_wrap');
    if (viewportBtn) {
        viewportBtn.addEventListener('click', function () {
            if (this.disabled) return;
            useViewport = !useViewport;
            this.classList.toggle('active', useViewport);
            setViewportFrame(useViewport);
            updateViewportTooltip();

            // Hide boundaries when viewport mode is active; restore when deactivated
            if (useViewport) {
                heroMap.hideBoundaries();
                // Viewport & Regions are mutually-exclusive spatial constraints —
                // turning viewport on closes the Regions panel + clears its button.
                showResultsView();
            } else if (layerPalette) {
                layerPalette.refreshBoundaries();
            }
        });
    }
    // Click on the wrapper when the button is disabled → switch to flat map
    if (viewportWrap) {
        viewportWrap.addEventListener('click', function () {
            if (viewportBtn && viewportBtn.disabled && heroMap.isGlobeMode()) {
                heroMap.map.setProjection({ type: 'mercator' });
                // The projection-change listener will enable the button;
                // give it a moment then activate viewport constraint.
                setTimeout(() => {
                    if (viewportBtn && !viewportBtn.disabled) {
                        viewportBtn.click();
                    }
                }, 350);
            }
        });
    }

    // ── Listen for projection changes to enable/disable viewport button ──
    heroMap.onProjectionChange((isGlobe) => {
        updateViewportButtonState(isGlobe);
    });
    // Set initial state after a short delay (projection may need time to settle)
    setTimeout(() => updateViewportButtonState(heroMap.isGlobeMode()), 500);

    // ── Initialise the Filter-mode selection cache from the default-checked HTML state. ──
    document.querySelectorAll('#gazetteers_offcanvas .standard-gazetteers-list .authority-cb').forEach(cb => {
        if (cb.checked) filterSelections.add(cb.value);
    });

    // ── Wire authority checkboxes ──
    // Clustering is now controlled by the similarity-threshold slider at the top of
    // the Results panel (see Master Plan §1.3, §4.1, §4.2). The legacy
    // #atlas_clustering_toggle checkbox has been removed from atlas.html; clusterResults
    // remains as a constant `true` until the new slider replaces this signal end-to-end.
    //
    // Use event delegation so that authority controls in either the standard or the
    // My-Gazetteers placeholder list fire the same handler. Inputs are swapped between
    // type=checkbox (Filter mode) and type=radio (Explore mode) by setGazetteerMode;
    // the per-mode selection caches are kept in sync so flipping tabs restores state.
    document.querySelector('#gazetteers_offcanvas').addEventListener('change', (e) => {
        if (!e.target.classList.contains('authority-cb')) return;
        const offcanvasBody = document.querySelector('#gazetteers_offcanvas .offcanvas-body');
        const mode = (offcanvasBody && offcanvasBody.dataset.mode) || 'filter';

        if (e.target.closest('.specialist-list')) {
            // Specialist child change → recount parent tri-state.
            recountSpecialistTriState();
        } else if (e.target.closest('.specialist-gazetteers-parent')) {
            // Parent toggled directly (input or label) → propagate to children.
            applyParentToggleToChildren(e.target.checked);
        }
        emitGazetteerSelection(mode);
    });

    // ── Gazetteers offcanvas: Filter | Explore mode toggle (Master Plan §1.4) ──
    // Sketch only — backend support for the unified /suggest list and the Explorer view
    // arrives in Phases 2 and 4.
    document.querySelectorAll('#gazetteers_offcanvas .gazetteer-mode-toggle .btn').forEach(btn => {
        btn.addEventListener('click', () => setGazetteerMode(btn.dataset.gazetteerMode));
    });

    // ── Gazetteers offcanvas: My-Gazetteers toggle (Explore mode only) ──
    // Swaps between the standard gazetteer list and the (placeholder) per-user list
    // grouped into Published / Pending. Real data arrives via /suggest in Phase 2.
    const mineToggle = document.getElementById('gazetteer_mine_toggle');
    if (mineToggle) {
        mineToggle.addEventListener('change', () => updateGazetteerListVisibility());
    }

    // ── Gazetteers coverage filters ──
    // Both switches are now FUNCTIONAL, client-side: Date Range = temporal
    // overlap (registry temporal_extent); Area = res-2 H3 intersection with the
    // selected area(s) (registry h3_coverage_coarse + h3-js).
    document.querySelectorAll('#gazetteers_offcanvas .gazetteer-stub-switch').forEach(sw => {
        sw.addEventListener('change', () => applyGazetteerCoverageFilter());
    });

    // ── Specialist Gazetteers: parent row click reveals the inline dropdown. ──
    // Lazy-render only; the actual tri-state propagation to children is handled
    // by the offcanvas change handler so that clicking either the input OR the
    // label text fires it.
    document.querySelectorAll('#gazetteers_offcanvas .specialist-gazetteers-parent').forEach(label => {
        label.addEventListener('click', () => {
            const expansion = document.querySelector('#gazetteers_offcanvas .specialist-gazetteers-expansion');
            if (!expansion) return;
            if (expansion.classList.contains('d-none')) {
                renderSpecialistList();
                expansion.classList.remove('d-none');
            }
        });
    });

    // ── Tileset gating for ``no_explore`` gazetteers (OSM, OHM in Explore mode). ──
    applyTilesetGating(document.querySelector('#gazetteers_offcanvas .offcanvas-body')?.dataset.mode || 'filter');

    // ── Initialise type tree in categories offcanvas ──
    typeTree = new TypeTreeWidget('#atlas_type_tree', {
        onchange: () => {
            const ids = typeTree.getSelectedIdentifiers();
            filterState.set('place_types', ids);
            updateTreeBadge();
        },
    });
    typeTree.init();

    document.getElementById('atlas_tree_clear').addEventListener('click', (e) => {
        e.preventDefault();
        if (typeTree) {
            typeTree.clearAll();
            filterState.set('place_types', []);
            updateTreeBadge();
        }
    });

    // ── Portal link handler ──
    $(document).on('click', '.portal-link', function (e) {
        enteringPortal = true;
        e.preventDefault();
        const id = $(this).data('whg-id') || $(this).data('pid');
        const path = $(this).data('whg-id') ? 'portal/' : 'detail';
        window.location.href = `/places/${id}/${path}`;
    });

    // ── BETA cluster panel → map sync ──
    // Click a cluster card head → highlight + fit all its members' markers.
    $(document).on('click', '#atlas_search_results .cluster-head', function () {
        const $card = $(this).closest('.cluster-card');
        const pids = String($card.data('pids') || '').split(' ').filter(Boolean);
        if (!pids.length) return;
        highlightHits(pidsToIndices(pids), { fit: true });
        $('#atlas_search_results .cluster-card').removeClass('cluster-highlight');
        $card.addClass('cluster-highlight');
    });
    // Click a single member → highlight + fit just that marker.
    $(document).on('click', '#atlas_search_results .cluster-member', function (e) {
        e.stopPropagation();
        const pid = $(this).data('pid');
        if (pid == null) return;
        highlightHits(pidsToIndices([String(pid)]), { fit: true });
        $('#atlas_search_results .cluster-card').removeClass('cluster-highlight');
        $(this).closest('.cluster-card').addClass('cluster-highlight');
    });

    // ── BETA dynamic portal: open place detail (from cluster cards/members and
    //    from the portal's own live-cluster links). ──
    $(document).on('click', '.atlas-portal-open', function (e) {
        e.preventDefault();
        e.stopPropagation();
        openAtlasPortal($(this).data('portal-pid'));
    });

    // ── BETA AAT type facet: toggle a hierarchical type filter, re-search ──
    $(document).on('click', '#atlas_type_facets .type-facet-chip-custom', function (e) {
        // Custom (non-AAT) source type — toggle an exact `types` identifier filter.
        e.stopPropagation();
        const id = $(this).data('type-id');
        if (id == null || id === '') return;
        const key = String(id);
        const i = selectedTypes.indexOf(key);
        if (i >= 0) selectedTypes.splice(i, 1); else selectedTypes.push(key);
        initiateToponymSearch({ preserveFacets: true });
    });
    $(document).on('click', '#atlas_type_facets .type-facet-chip:not(.type-facet-chip-custom)', function () {
        const id = parseInt($(this).data('aat-id'), 10);
        if (Number.isNaN(id)) return;
        const i = selectedAatTypes.indexOf(id);
        if (i >= 0) selectedAatTypes.splice(i, 1); else selectedAatTypes.push(id);
        initiateToponymSearch({ preserveFacets: true });
    });
    $(document).on('click', '#atlas_type_facets .type-facet-clear', function () {
        selectedAatTypes = [];
        selectedTypes = [];
        initiateToponymSearch({ preserveFacets: true });
    });

    // ── Result item click handler ──
    $(document).on('click', '#atlas_search_results .result', function () {
        const $el = $(this);
        const index = $el.index('#atlas_search_results .result');

        heroMap.map.removeFeatureState({ source: 'places' });
        heroMap.map.setFeatureState({ source: 'places', id: index }, { highlight: true });

        const fc = heroMap.map.getSource('places')._data?.geojson;

        if ($el.attr('data-map-clicked') === 'true') {
            $el.removeAttr('data-map-clicked');
            scrollToResult($el);
        } else if (fc?.features?.length > index) {
            heroMap.map.fitViewport(bbox(fc.features[index]), 6); // clamp: keep context
        }

        $('#atlas_search_results .result').removeClass('selected');
        $el.addClass('selected');
    });

    // ── Handle pre-populated toponym from URL ──
    if (typeof atlas_toponym !== 'undefined' && atlas_toponym) {
        searchInput.value = atlas_toponym;
        switchSearchMode('toponyms');
        setTimeout(() => initiateToponymSearch(), 300);
    }

    // ── Handle Gazetteers deep link from sitewide navbar ──
    // /atlas/?panel=gazetteers&gmode=explore opens the Atlas in Places mode
    // with the Gazetteers offcanvas in view and the requested Filter|Explore
    // tab pre-selected. We trigger native click() on the existing buttons so
    // that all wired side-effects (.active class updates, etc.) fire exactly
    // as if the user had clicked them by hand.
    {
        const params = new URLSearchParams(location.search);
        // A ?gazetteer=<ns> deep link implies Places → Explore on that gazetteer
        // (the Place List's shareable URL), even without an explicit panel/gmode.
        const wantGazetteer = params.get('gazetteer');
        if (params.get('panel') === 'gazetteers' || wantGazetteer) {
            const placesBtn = document.querySelector(
                '.search-mode-toggle .btn[data-search-mode="toponyms"]'
            );
            if (placesBtn) placesBtn.click();
            // The toponym-only-btn group (#open_gazetteers_modal, #open_categories_modal)
            // is unhidden by switchSearchMode; clicking the trigger opens the offcanvas
            // via Bootstrap's data-bs-target wiring.
            setTimeout(() => {
                const trigger = document.getElementById('open_gazetteers_modal');
                if (trigger) trigger.click();
                const gmode = (params.get('gmode') === 'explore' || wantGazetteer) ? 'explore' : 'filter';
                if (gmode === 'explore') {
                    const exploreBtn = document.querySelector(
                        '#gazetteers_offcanvas .gazetteer-mode-toggle .btn[data-gazetteer-mode="explore"]'
                    );
                    if (exploreBtn) exploreBtn.click();
                    // Pre-select the requested gazetteer radio → opens its Place
                    // List. On a slow cold load the authority list / radio swap can
                    // lag, so poll (≤2s) until the (enabled) radio exists, then tick
                    // it and fire change to run the normal selection flow LAST.
                    if (wantGazetteer) {
                        const sel = `#gazetteers_offcanvas .authority-cb[value="${wantGazetteer.replace(/"/g, '\\"')}"]`;
                        let tries = 0;
                        const pick = () => {
                            const radio = document.querySelector(sel);
                            if (radio && !radio.disabled && radio.type === 'radio') {
                                radio.checked = true;
                                radio.dispatchEvent(new Event('change', { bubbles: true }));
                            } else if (++tries < 20) {
                                setTimeout(pick, 100);
                            }
                        };
                        pick();
                    }
                }
            }, 50);
        }
    }

    // ── Welcome panel: fade on map interaction ──
    // The panel's own controls (dismiss, "don't show again", control-focus
    // fade, tour link) are wired on document-ready in setupWelcomePanel(),
    // independent of map load; here we add the map-drag fade now the map exists.
    heroMap.map.on('mousedown', fadeOutWelcome);
    heroMap.map.on('touchstart', fadeOutWelcome);

    // ── Tour relaunch button (bottom-left) ──
    const tourBtn = document.getElementById('atlas_tour_btn');
    if (tourBtn) {
        tourBtn.addEventListener('click', () => startAtlasTour());
    }

    // ── Auto-start tour on first visit ──
    if (!hasSeenAtlasTour() && !isWelcomeDismissed() && !(typeof atlas_toponym !== 'undefined' && atlas_toponym)) {
        // Delay slightly to let the map finish rendering
        setTimeout(() => {
            const wp = document.getElementById('atlas_welcome');
            if (wp) {
                wp.classList.add('atlas-welcome-hidden');
                wp.addEventListener('transitionend', () => {
                    wp.style.display = 'none';
                }, { once: true });
            }
            startAtlasTour();
        }, 1500);
    }

    // ── Close area dropdown on outside click ──
    document.addEventListener('click', (e) => {
        const dropdown = document.querySelector('.atlas-region-dropdown');
        if (dropdown && !dropdown.contains(e.target) &&
            e.target.id !== 'atlas_search_input') {
            closeAreaDropdown();
        }
    });

}).catch(error => console.error('Atlas init error:', error));

/* ═══════════════════════════════════════════════════════════════════
   Helper functions
   ═══════════════════════════════════════════════════════════════════ */

function updateTreeBadge() {
    if (!typeTree) return;
    const count = typeTree.selectionCount();
    const $badge = $('#atlas_tree_badge');
    if (count > 0) $badge.text(count + ' selected').show();
    else $badge.hide();
}

function switchSearchMode(mode) {
    searchMode = mode;
    // Mode tint on the floating search bar (Areas = indigo, Places = burgundy).
    const fs = document.getElementById('floating_search');
    if (fs) { fs.classList.remove('mode-areas', 'mode-toponyms'); fs.classList.add('mode-' + mode); }
    // A settings view (Regions/Gazetteers/Categories) is mode-specific, so leave
    // it on a mode switch — otherwise it lingers under the wrong mode's controls.
    showResultsView();
    const input = document.getElementById('atlas_search_input');
    const toponymBtns = document.querySelectorAll('.toponym-only-btn');
    const areasBtns = document.querySelectorAll('.areas-only-btn');

    if (mode === 'areas') {
        input.placeholder = buildAreasPlaceholder();
        toponymBtns.forEach(btn => btn.style.display = 'none');
        areasBtns.forEach(btn => btn.style.display = '');
        hideResultsPanel();
        heroMap.clearResultFeatures();

        // Ensure whg-context style is active for area search
        heroMap.ensureContextStyle();
    } else {
        const chipLabels = selectedRegions.map(r => r.label).join(', ');
        if (useViewport && !heroMap.isGlobeMode()) {
            input.placeholder = 'Search within viewport…';
        } else if (chipLabels) {
            input.placeholder = `Search within ${chipLabels}…`;
        } else {
            input.placeholder = 'Search for place names…';
        }
        toponymBtns.forEach(btn => btn.style.display = '');
        areasBtns.forEach(btn => btn.style.display = 'none');
    }
    input.value = '';
    closeAreaDropdown();
}

function buildAreasPlaceholder() {
    if (layerPalette && layerPalette.getAdminLevel() !== null) {
        return `Search for regions…`;
    }
    return 'Search for regions…';
}

/**
 * Switch the Gazetteers offcanvas between Filter and Explore mode (Master Plan §1.4).
 *
 * Sketch only. This updates the visible UI (active tab, mode-specific help copy,
 * Explore-mode-only controls) and physically swaps every authority input between
 * type=checkbox (Filter mode, multi-select) and type=radio (Explore mode,
 * single-select via shared `name` attribute). Selection state is preserved per mode
 * via filterSelections / exploreSelection so flipping tabs restores the previously
 * recorded selection rather than dropping it on the type swap.
 */
function setGazetteerMode(mode) {
    if (mode !== 'filter' && mode !== 'explore') return;
    const offcanvas = document.getElementById('gazetteers_offcanvas');
    if (!offcanvas) return;

    const body = offcanvas.querySelector('.offcanvas-body');
    const previousMode = (body && body.dataset.mode) || 'filter';
    if (mode === previousMode) return;

    // ── Save current selection into the cache for the previous mode. ──
    const inputs = offcanvas.querySelectorAll('.authority-cb');
    const currentChecked = Array.from(inputs).filter(i => i.checked).map(i => i.value);
    if (previousMode === 'filter') {
        filterSelections.clear();
        currentChecked.forEach(v => filterSelections.add(v));
    } else {
        exploreSelection = currentChecked[0] || null;
    }

    if (body) body.dataset.mode = mode;

    offcanvas.querySelectorAll('.gazetteer-mode-toggle .btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.gazetteerMode === mode);
    });

    offcanvas.querySelectorAll('.gazetteer-mode-help').forEach(p => {
        p.classList.toggle('d-none', p.dataset.mode !== mode);
    });

    offcanvas.querySelectorAll('[data-mode-visible]').forEach(el => {
        el.classList.toggle('d-none', el.dataset.modeVisible !== mode);
    });

    // ── Swap input type to match the mode, then apply that mode's saved selection. ──
    // Two-pass to avoid browser quirks where setting `.checked` in the same iteration
    // that mutates `.type` can be silently dropped (the type change can reset state).
    if (mode === 'explore') {
        // Pass 1: convert every input to a radio in a shared name group, all unchecked.
        inputs.forEach(input => {
            input.type = 'radio';
            input.name = 'gazetteer_explore';
            input.checked = false;
        });
        // Pass 2: tick the one matching the saved Explore-mode selection (if any).
        if (exploreSelection !== null) {
            const target = Array.from(inputs).find(i => i.value === exploreSelection);
            if (target) target.checked = true;
        }
    } else {
        // Pass 1: convert every input to a checkbox, all unchecked.
        inputs.forEach(input => {
            input.type = 'checkbox';
            input.removeAttribute('name');
            input.checked = false;
        });
        // Pass 2: tick everything that was in the Filter-mode selection cache.
        inputs.forEach(input => {
            if (filterSelections.has(input.value)) input.checked = true;
        });
    }

    // The My-Gazetteers toggle is Explore-mode-only; reset it when leaving Explore.
    if (mode === 'filter') {
        const mineToggle = document.getElementById('gazetteer_mine_toggle');
        if (mineToggle) mineToggle.checked = false;
    }
    updateGazetteerListVisibility();

    // Gazetteers flagged ``no_explore`` (e.g. OSM/OHM) are disabled in Explore
    // mode and re-enabled in Filter mode.
    applyTilesetGating(mode);

    // Re-apply type-pill filter visibility & state if the Specialist expansion
    // is open (the [data-mode-visible] loop above has already toggled the row's
    // visibility, but the underlying child filtering must re-run).
    applyTypePillFilter();

    // Mirror the resulting selection into filterState.
    emitGazetteerSelection(mode);
}

/**
 * Show the standard gazetteer list or the (placeholder) My-Gazetteers list, depending
 * on the My-Gazetteers toggle. Only meaningful in Explore mode for authenticated users.
 */
function updateGazetteerListVisibility() {
    const offcanvas = document.getElementById('gazetteers_offcanvas');
    if (!offcanvas) return;
    const standardList = offcanvas.querySelector('.standard-gazetteers-list');
    const myList = offcanvas.querySelector('.my-gazetteers-list');
    if (!standardList) return;

    const mineToggle = document.getElementById('gazetteer_mine_toggle');
    const showMine = !!(mineToggle && mineToggle.checked && myList);

    standardList.classList.toggle('d-none', showMine);
    if (myList) myList.classList.toggle('d-none', !showMine);
}

/* ── Explore-mode gating for ``no_explore`` gazetteers ──
   Disables the input and visually greys the row in Explore mode; restores it in
   Filter mode. ``no_explore`` is set on the GazetteerRegistryEntry for sources
   whose tilesets are not browsable in Explore mode (currently OSM and OHM).
   The original tooltip is preserved on a data-bs-title-original attribute so it
   can be swapped back when leaving Explore. */
function applyTilesetGating(mode) {
    const labels = document.querySelectorAll(
        '#gazetteers_offcanvas .authority-item[data-no-explore="1"]'
    );
    const disabled = (mode === 'explore');
    labels.forEach(label => {
        const input = label.querySelector('.authority-cb');
        if (input) {
            input.disabled = disabled;
            if (disabled) input.checked = false;
        }
        label.classList.toggle('disabled', disabled);
        if (!label.dataset.bsTitleOriginal) {
            label.dataset.bsTitleOriginal = label.getAttribute('data-bs-title') || '';
        }
        const newTitle = disabled
            ? 'This gazetteer is not available in Explore mode'
            : label.dataset.bsTitleOriginal;
        label.setAttribute('data-bs-title', newTitle);
        label.setAttribute('data-bs-original-title', newTitle);
        try {
            const tt = window.bootstrap && window.bootstrap.Tooltip.getInstance(label);
            if (tt) tt.setContent({ '.tooltip-inner': newTitle });
        } catch (e) { /* no tooltip yet — picked up on next init */ }
    });
    // Clear a stale Explore selection that now points at a disabled row.
    if (disabled && exploreSelection) {
        const sel = document.querySelector(
            `#gazetteers_offcanvas .authority-cb[value="${CSS.escape(exploreSelection)}"]`
        );
        if (sel && sel.disabled) exploreSelection = null;
    }
}

/* ── Specialist Gazetteers — lazy render of children, tri-state, search, type pills ──
   The expansion is hidden by default and rendered on first parent-row interaction.
   Children are rendered with .authority-cb so the existing offcanvas change
   delegation routes them through recountSpecialistTriState. */
let _specialistRendered = false;

function getSpecialistData() {
    const tag = document.getElementById('specialist_gazetteers_data');
    if (!tag) return [];
    try {
        return JSON.parse(tag.textContent) || [];
    } catch (e) {
        return [];
    }
}

function renderSpecialistList() {
    if (_specialistRendered) return;
    const list = document.querySelector('#gazetteers_offcanvas .specialist-list');
    if (!list) return;
    const data = getSpecialistData();
    if (!data.length) {
        list.innerHTML = '<p class="small text-muted fst-italic mb-0">No specialist gazetteers registered yet.</p>';
        _specialistRendered = true;
        return;
    }
    const offcanvasBody = document.querySelector('#gazetteers_offcanvas .offcanvas-body');
    const mode = (offcanvasBody && offcanvasBody.dataset.mode) || 'filter';
    const inputType = mode === 'explore' ? 'radio' : 'checkbox';
    const nameAttr = mode === 'explore' ? ' name="gazetteer_explore"' : '';
    const html = data.map(g => {
        const desc = (g.description || g.name || '').replace(/"/g, '&quot;');
        const type = g.gazetteer_type || 'standard';
        return `
            <label class="authority-item form-check"
                   data-bs-toggle="tooltip"
                   data-bs-title="${desc}"
                   data-gazetteer-type="${type}"
                   data-specialist-id="${g.id}">
                <input class="form-check-input authority-cb" type="${inputType}"${nameAttr}
                       value="${g.id}">
                <span class="form-check-label">${g.name}</span>
            </label>
        `;
    }).join('');
    list.innerHTML = html;
    _specialistRendered = true;

    // Wire the search input (once).
    const search = document.querySelector('#gazetteers_offcanvas .specialist-search');
    if (search && !search.dataset.wired) {
        search.dataset.wired = '1';
        search.addEventListener('input', () => {
            const q = search.value.trim().toLowerCase();
            list.querySelectorAll('label.authority-item').forEach(l => {
                const text = (l.textContent || '').toLowerCase();
                l.classList.toggle('d-none', q && !text.includes(q));
            });
            applyTypePillFilter();
            recountSpecialistTriState();
        });
    }

    // Wire the type-pill filter (once).
    document.querySelectorAll('#gazetteers_offcanvas .gazetteer-type-pill-filter [data-gazetteer-type-pill]').forEach(btn => {
        if (btn.dataset.wired) return;
        btn.dataset.wired = '1';
        btn.addEventListener('click', () => {
            btn.classList.toggle('active');
            applyTypePillFilter();
            recountSpecialistTriState();
        });
    });

    applyTypePillFilter();
    recountSpecialistTriState();
}

function applyParentToggleToChildren(checkedAll) {
    const children = document.querySelectorAll(
        '#gazetteers_offcanvas .specialist-list .authority-cb'
    );
    children.forEach(cb => {
        // Only toggle visible (non-d-none, non-disabled) children.
        const label = cb.closest('label');
        if (label && label.classList.contains('d-none')) return;
        cb.checked = !!checkedAll;
    });
    recountSpecialistTriState();
}

function recountSpecialistTriState() {
    const parentInput = document.querySelector(
        '#gazetteers_offcanvas .specialist-gazetteers-parent .authority-cb'
    );
    if (!parentInput) return;
    // Count *all* children regardless of search/pill visibility so that the
    // 'whg' alias is only emitted when literally every Specialist Gazetteer
    // is selected, never when filtering merely hides some rows.
    const allChildren = Array.from(document.querySelectorAll(
        '#gazetteers_offcanvas .specialist-list .authority-cb'
    ));
    if (!allChildren.length) {
        parentInput.indeterminate = false;
        return;
    }
    const checkedCount = allChildren.filter(cb => cb.checked).length;
    if (checkedCount === 0) {
        parentInput.checked = false;
        parentInput.indeterminate = false;
    } else if (checkedCount === allChildren.length) {
        parentInput.checked = true;
        parentInput.indeterminate = false;
    } else {
        parentInput.checked = false;
        parentInput.indeterminate = true;
    }
}

/* ── Type-pill filter (Explore-only sketch) ──
   Hides Specialist children whose data-gazetteer-type is not in the active pill
   set. Convention: when *all* pills are off, fall back to "show all" (matches
   the page's existing area/region selection convention). */
function applyTypePillFilter() {
    const filterRow = document.querySelector('#gazetteers_offcanvas .gazetteer-type-pill-filter');
    if (!filterRow) return;
    const list = document.querySelector('#gazetteers_offcanvas .specialist-list');
    if (!list) return;
    // If filter row is hidden (Filter mode), force show all.
    const hidden = filterRow.classList.contains('d-none');
    const activePills = hidden
        ? null
        : Array.from(filterRow.querySelectorAll('[data-gazetteer-type-pill].active'))
            .map(b => b.dataset.gazetteerTypePill);
    const showAll = !activePills || activePills.length === 0;
    list.querySelectorAll('label.authority-item').forEach(label => {
        if (showAll) {
            // Restore visibility unless the search-box has hidden this row.
            // (We can't distinguish source-of-hide; safe default: show.)
            label.classList.remove('d-none');
            return;
        }
        const t = label.dataset.gazetteerType || 'standard';
        label.classList.toggle('d-none', !activePills.includes(t));
    });
}

// Reflect the current Explore-mode gazetteer selection in the URL as
// ``?gazetteer=<ns>`` so the view is shareable/bookmarkable and survives a
// reload (a cold load of such a URL opens Places → Explore on that gazetteer;
// see the deep-link handler on init). replaceState avoids polluting history as
// the user tries different gazetteers.
function updateExploreUrl(ns) {
    try {
        const url = new URL(window.location.href);
        if (ns) url.searchParams.set('gazetteer', ns);
        else url.searchParams.delete('gazetteer');
        window.history.replaceState(null, '', url);
    } catch (e) { /* URL API unavailable */ }
}

/* ── Mirror the active gazetteer selection into filterState ──
   Tri-state: parent fully checked → 'whg' (compact alias for "all WHG datasets");
   parent indeterminate → explicit list of child specialist ids; parent unchecked
   → no Specialist contribution. Standard authority selections are always sent
   verbatim. */
function emitGazetteerSelection(mode) {
    const offcanvas = document.getElementById('gazetteers_offcanvas');
    if (!offcanvas) return;
    // Standard list values only — never include specialist children here, since
    // the parent's tri-state below decides whether to expand to explicit ids.
    const standardChecked = Array.from(
        offcanvas.querySelectorAll('.standard-gazetteers-list > .authority-item .authority-cb:checked')
    ).map(el => el.value);

    const parentInput = offcanvas.querySelector(
        '.specialist-gazetteers-parent .authority-cb'
    );
    let composed = standardChecked.slice();
    if (parentInput && parentInput.indeterminate) {
        // Drop the bare 'whg' alias (the parent isn't fully checked) and
        // substitute the explicit list of selected child specialist ids.
        composed = composed.filter(v => v !== 'whg');
        const childIds = Array.from(offcanvas.querySelectorAll(
            '.specialist-list .authority-cb:checked'
        )).map(el => el.value);
        composed = composed.concat(childIds);
    }
    if (mode === 'filter') {
        filterSelections.clear();
        composed.forEach(v => filterSelections.add(v));
        // Leaving Explore for Filter: dismiss the Place List if it's on screen and
        // drop the shareable ?gazetteer= param.
        PlaceList.close();
        updateExploreUrl(null);
    } else {
        exploreSelection = composed[0] || null;
        // Mirror the Explore selection onto the map: load the gazetteer's
        // tileset and remove any others. Specialist gazetteers (e.g. ``whg:892``)
        // resolve to their tileserver name (``whg-892``) inside setActiveSource.
        if (layerPalette && exploreSelection) {
            layerPalette.setActiveSource(exploreSelection);
            // Open the browsable Place List for the selected gazetteer alongside
            // the map tileset (place#125) — the friendly name comes from the
            // ticked authority row's label span (excludes the "core" badge).
            let label = exploreSelection;
            const el = offcanvas.querySelector(`.authority-cb[value="${exploreSelection.replace(/"/g, '\\"')}"]`);
            const item = el && el.closest('.authority-item');
            const nameEl = item && item.querySelector('.form-check-label');
            if (nameEl) label = nameEl.textContent.trim();
            else if (item) label = item.textContent.trim();
            PlaceList.open(exploreSelection, label);
            updateExploreUrl(exploreSelection);
        } else {
            PlaceList.close();
            updateExploreUrl(null);
        }
    }
    filterState.set('authorities', composed);
}

/* ── Viewport constraint helpers ── */

// Show/hide the on-map edge-marker frame that signals Viewport mode is active.
function setViewportFrame(on) {
    const frame = document.getElementById('atlas_viewport_frame');
    if (frame) frame.classList.toggle('active', on);
}

function updateViewportButtonState(isGlobe) {
    const btn = document.getElementById('atlas_viewport_btn');
    if (!btn) return;
    if (isGlobe) {
        // Disable viewport in globe mode
        btn.disabled = true;
        btn.style.pointerEvents = 'none';
        if (useViewport) {
            useViewport = false;
            btn.classList.remove('active');
            setViewportFrame(false);
            // Restore boundaries when viewport is auto-disabled
            if (layerPalette) layerPalette.refreshBoundaries();
        }
    } else {
        btn.disabled = false;
        btn.style.pointerEvents = '';
    }
    updateViewportTooltip();
}

function updateViewportTooltip() {
    const btn = document.getElementById('atlas_viewport_btn');
    const wrap = document.getElementById('atlas_viewport_wrap');
    if (!btn || !wrap) return;
    let text;
    if (btn.disabled) {
        text = 'Switch to flat map projection to enable viewport constraint';
    } else if (btn.classList.contains('active')) {
        text = 'Viewport constraint active — search will be limited to the visible map area. Click to disable.';
    } else {
        text = 'Constrain search to the current map viewport (clears any selected areas)';
    }
    // Use data-bs-title (not `title`) so no native tooltip pops up beside the BS one.
    wrap.setAttribute('data-bs-title', text);
    wrap.removeAttribute('title');
    // Update Bootstrap tooltip if initialised on the wrapper
    try {
        const tt = window.bootstrap && window.bootstrap.Tooltip.getInstance(wrap);
        if (tt) tt.setContent({'.tooltip-inner': text});
    } catch (e) { /* */ }
}


/* ── Area search (Explorer mode) ── */

async function performAreaSearch() {
    const query = document.getElementById('atlas_search_input').value.trim();
    if (query.length < 2) {
        closeAreaDropdown();
        return;
    }

    const results = await areaRouter.search(query, {
        adminLevel: layerPalette ? layerPalette.getAdminLevel() : null,
        namespace: layerPalette ? layerPalette.getNamespace() : 'osm',
    });

    areaSearchResults = results;
    areaDropdownIndex = -1;

    if (results.length === 0) {
        renderAreaDropdown([{
            _stub: true,
            label: 'No matching areas found',
            sublabel: 'Try a different name or adjust your admin level',
        }]);
    } else {
        renderAreaDropdown(results);
    }
}

function renderAreaDropdown(items) {
    // Remove existing dropdown
    closeAreaDropdown();

    const dropdown = document.createElement('div');
    dropdown.className = 'atlas-region-dropdown';

    dropdown.innerHTML = items.map((item, i) => `
        <div class="region-result ${item._stub ? 'region-result--stub' : ''}" data-index="${i}">
            <div class="region-result-label">${item.label || ''}</div>
            ${item.sublabel ? `<div class="region-result-sublabel">${item.sublabel}</div>` : ''}
            ${item.source_type ? `<span class="badge bg-secondary" style="font-size:0.6rem">${item.source || ''}</span>` : ''}
        </div>
    `).join('');

    // Mount directly beneath the search input box (not the whole floating panel,
    // which would drop it below the temporal control).
    const anchor = document.querySelector('#floating_search .search-input-group')
        || document.getElementById('floating_search');
    anchor.appendChild(dropdown);

    // Wire clicks
    dropdown.querySelectorAll('.region-result:not(.region-result--stub)').forEach(el => {
        el.addEventListener('mousedown', (e) => {
            e.preventDefault();
            selectAreaResult(parseInt(el.dataset.index));
        });
    });
}

function closeAreaDropdown() {
    document.querySelectorAll('.atlas-region-dropdown').forEach(el => el.remove());
    areaDropdownIndex = -1;
}

function handleAreaDropdownKeydown(e) {
    const items = document.querySelectorAll('.atlas-region-dropdown .region-result:not(.region-result--stub)');
    if (!items.length) return;

    if (e.key === 'ArrowDown') {
        e.preventDefault();
        areaDropdownIndex = Math.min(areaDropdownIndex + 1, items.length - 1);
        highlightAreaDropdown(items);
    } else if (e.key === 'ArrowUp') {
        e.preventDefault();
        areaDropdownIndex = Math.max(areaDropdownIndex - 1, 0);
        highlightAreaDropdown(items);
    } else if (e.key === 'Enter' && areaDropdownIndex >= 0) {
        e.preventDefault();
        selectAreaResult(areaDropdownIndex);
    } else if (e.key === 'Escape') {
        closeAreaDropdown();
    }
}

function highlightAreaDropdown(items) {
    items.forEach((el, i) => {
        el.classList.toggle('region-result--active', i === areaDropdownIndex);
    });
}

function selectAreaResult(index) {
    const item = areaSearchResults[index];
    if (!item) return;

    if (item._fromIndex && !item.geometry && item.bounds) {
        // Zoom to bounds so user can click polygon on map
        const [west, south, east, north] = item.bounds;
        try {
            heroMap.map.fitBounds([[west, south], [east, north]], {
                padding: 40, maxZoom: 8,
            });
        } catch (e) { /* */ }
        document.getElementById('atlas_search_input').value = '';
        closeAreaDropdown();
        return;
    }

    if (item.geometry) {
        addRegionSelection({
            id: item.id,
            label: item.label,
            admin_level: item.admin_level,
            namespace: item.namespace || 'osm',
            geometry: item.geometry,
        });
    } else if (item.bounds) {
        // Zoom to bounds so user can click on map
        const [west, south, east, north] = item.bounds;
        heroMap.map.fitBounds([[west, south], [east, north]], { padding: 40, maxZoom: 8 });
    }

    document.getElementById('atlas_search_input').value = '';
    closeAreaDropdown();
}

/* ── Region selections ── */

function addRegionSelection(item) {
    if (selectedRegions.some(r => r.id === item.id)) return;
    selectedRegions.push(item);

    // Update overlay on map
    updateSelectionOverlay();
    if (item.geometry) {
        heroMap.fitTo({ type: 'Feature', geometry: item.geometry, properties: {} });
    }

    renderSelectionChips();
}

function removeRegionSelection(id) {
    selectedRegions = selectedRegions.filter(r => r.id !== id);
    if (selectedRegions.length === 0) {
        heroMap.clearOverlay();
    } else {
        updateSelectionOverlay();
    }
    renderSelectionChips();
}

function updateSelectionOverlay() {
    const features = selectedRegions
        .filter(r => r.geometry)
        .map(r => ({
            type: 'Feature',
            geometry: r.geometry,
            properties: { id: r.id, label: r.label },
        }));
    if (features.length > 0) {
        heroMap.setOverlay({ type: 'FeatureCollection', features });
    } else {
        heroMap.clearOverlay();
    }
}

function renderSelectionChips() {
    updateGazetteerAreaSwitch();   // keep the coverage "Area" switch in sync with the selection
    applyGazetteerCoverageFilter(); // re-filter the gazetteer list if the Area switch is on
    const container = document.getElementById('selection_chips');
    if (selectedRegions.length === 0) {
        container.innerHTML = '';
        return;
    }
    container.innerHTML = selectedRegions.map(item => `
        <span class="filter-chip" data-region-id="${item.id}">
            <i class="fas fa-vector-square me-1"></i>
            ${escapeHtml(item.label)}
            ${item.admin_level != null ? `<span class="filter-chip-meta">(level ${item.admin_level})</span>` : ''}
            <button type="button" class="filter-chip-dismiss" aria-label="Remove" data-dismiss-id="${item.id}">
                <i class="fas fa-times"></i>
            </button>
        </span>
    `).join(' ');

    container.querySelectorAll('.filter-chip-dismiss').forEach(btn => {
        btn.addEventListener('click', () => removeRegionSelection(btn.dataset.dismissId));
    });
}

/* ── Toponym search ── */

function initiateToponymSearch(opts = {}) {
    if (searchDisabled) return;
    // A fresh query clears the AAT type-facet selection; a facet toggle
    // (preserveFacets) re-searches with the same query + updated facet filter.
    // A fresh query also re-enables θ auto-fit; a preserved re-search (facets /
    // temporal) keeps the user's manual θ if they set one.
    if (!opts.preserveFacets) { selectedAatTypes = []; selectedTypes = []; thetaUserSet = false; thetaNeedsFit = true; }
    const input = document.getElementById('atlas_search_input');
    const qstr = input.value.trim();

    if (!qstr && !(typeTree && typeTree.selectionCount() > 0) && !selectedAatTypes.length) {
        console.log('Atlas: need a search term or type filter');
        return;
    }

    // Starting a search leaves Explore browsing behind — dismiss any open
    // gazetteer place popup so it doesn't linger over the new results.
    try { heroMap.closePlacePopup(); } catch (e) { /* map not ready */ }

    const options = gatherToponymOptions(qstr);
    console.log('Atlas: initiating toponym search', options);

    showResultsPanel();
    // A fresh search returns focus to the results view — otherwise an open
    // Gazetteers/Categories/Regions panel would obscure the new results.
    showResultsView();
    const resultsDiv = document.getElementById('atlas_search_results');
    resultsDiv.innerHTML = '<div class="p-3 text-center"><i class="fas fa-spinner fa-spin"></i> Searching…</div>';

    // BETA: route through the CRC gateway (/atlas/search/) for client-side
    // clustering. Non-beta users stay on the legacy Django /search/index/ path.
    if (isBetaUser()) {
        initiateGatewaySearch(options);
        return;
    }

    $.ajax({
        type: 'POST',
        url: '/search/index/',
        data: JSON.stringify(options),
        contentType: 'application/json',
        headers: { 'X-CSRFToken': csrfToken },
        success: function (data) {
            console.log('Atlas: search completed', data);
            renderToponymResults(data);
        },
        error: function (error) {
            console.error('Atlas: search error', error);
            resultsDiv.innerHTML = '<div class="p-3 text-danger">Search failed. Please try again.</div>';
        },
    });
}

// ── BETA: gateway-routed clustered search ───────────────────────────────────
// The gateway ships the clustering fuel (edges[], per-hit fields,
// clustering_params); clustering.js clusters it in the browser and we show
// cluster cards. The θ slider re-clusters the cached response live (no refetch).
let gatewayData = null;            // cached last gateway response
let clusterThetaOverride = ATLAS_DEFAULT_THETA;   // θ slider value (seeded to the Atlas default)
let thetaUserSet = false;          // true once the user drags θ → stop auto-seeding until a fresh query
let thetaAutoFitted = false;       // whether the current θ was auto-fitted to the result set
let thetaNeedsFit = true;          // re-fit θ only on a FRESH query; preserve it across reloads of a result set
let weightOverrides = {};          // per-facet weight slider overrides (merged over params.weights)
const ATLAS_DEFAULT_NS_PENALTY = 0.15;  // mirrors clustering.js DEFAULT_PARAMS.same_ns_penalty
let sameNsPenaltyOverride = ATLAS_DEFAULT_NS_PENALTY; // same-gazetteer repulsion strength (slider)
let clusterFC = null;              // last plotted FeatureCollection (feature.id = hit index)
let clusterPidToIndex = null;      // place_id → feature/hit index (for panel↔map sync)
let lastClusters = [];             // last clusterHits() clusters (for the portal's live context)
let selectedAatTypes = [];         // AAT concept ids selected in the type facet (hierarchical filter)
let selectedTypes = [];            // custom (non-AAT) source type identifiers selected in the facet (place#122)

// Render the result-driven AAT type facet from the gateway's facets.aat_types
// ([{aat_id,label,count}]). Clicking a chip toggles a hierarchical aat_types
// filter and re-runs the search (preserving the query + facet selection).
function renderTypeFacets(facets) {
    const el = document.getElementById('atlas_type_facets');
    if (!el) return;
    const types = (facets && facets.aat_types) || [];
    // Custom (non-AAT) source types, from the gateway's dedicated facet (place#122).
    const customTypes = (facets && facets.custom_types) || [];
    const anySelected = selectedAatTypes.length || selectedTypes.length;
    if (!types.length && !customTypes.length && !anySelected) { el.style.display = 'none'; el.innerHTML = ''; return; }
    el.style.display = '';
    let h = '<span class="type-facets-label">Types:</span>';
    types.forEach(t => {
        const sel = selectedAatTypes.includes(t.aat_id);
        h += `<button class="type-facet-chip${sel ? ' selected' : ''}" data-aat-id="${t.aat_id}" `
            + `title="${escapeHtml(t.label || '')} (aat:${t.aat_id})${sel ? ' — selected' : ''}">`
            + `${escapeHtml(t.label || String(t.aat_id))} <span class="type-facet-count">${t.count}</span></button>`;
    });
    // Custom source types — a distinct chip style, filtered via the exact-source
    // `types` identifier rather than the hierarchical AAT path.
    customTypes.forEach(t => {
        const sel = selectedTypes.includes(t.identifier);
        const label = t.label || t.identifier;
        h += `<button class="type-facet-chip type-facet-chip-custom${sel ? ' selected' : ''}" data-type-id="${escapeHtml(t.identifier)}" `
            + `title="Custom type ${escapeHtml(t.identifier)}${sel ? ' — selected' : ''}">`
            + `${escapeHtml(label)} <span class="type-facet-count">${t.count}</span></button>`;
    });
    // A "clear" affordance for any selected types not present in the current facet list.
    if (anySelected) {
        h += `<button class="type-facet-clear" title="Clear type filter"><i class="fas fa-times"></i> clear</button>`;
    }
    el.innerHTML = h;
}

// ── Dynamic Atlas portal ────────────────────────────────────────────────────
// Resolve one place on demand (/atlas/place/) and show it with its LIVE cluster
// context (the other members of its current client-side cluster) — no fixed
// cluster_id, so it reflects the current θ/weights.
function findClusterFor(pid) {
    return (lastClusters || []).find(c => c.memberIds && c.memberIds.includes(pid));
}

function openAtlasPortal(pid) {
    if (!pid) return;
    if (clusterPidToIndex && clusterPidToIndex.has(pid)) {
        highlightHits([clusterPidToIndex.get(pid)], { fit: true });
    }
    const body = document.getElementById('atlas_portal_body');
    document.getElementById('atlas_portal_title').textContent = 'Place';
    body.innerHTML = '<div class="p-3 text-center"><i class="fas fa-spinner fa-spin"></i> Loading…</div>';
    window.bootstrap.Modal.getOrCreateInstance(document.getElementById('atlas_portal_modal')).show();
    $.ajax({
        url: '/atlas/place/?id=' + encodeURIComponent(pid),
        success: (place) => renderPortal(place, pid),
        error: (err) => {
            body.innerHTML = `<div class="p-3 text-danger">Could not load this place${err.status === 404 ? ' (not found)' : ''}.</div>`;
        },
    });
}

function renderPortal(place, pid) {
    document.getElementById('atlas_portal_title').textContent = place.title || pid;
    const attr = place.attribution || {};
    let h = `<p class="portal-source"><strong>Source:</strong> ${escapeHtml(attr.name || place.namespace || '')}`;
    if (attr.rights_holder) h += ` — ${escapeHtml(attr.rights_holder)}`;
    if (attr.license__spdx_id) h += ` <span class="portal-license">(${escapeHtml(attr.license__spdx_id)})</span>`;
    h += ` <span class="text-muted small">${escapeHtml(pid)}</span></p>`;

    const names = (place.names || []).map(n => escapeHtml(n.label || n.toponym)).filter(Boolean);
    if (names.length) h += `<p><strong>Names:</strong> ${names.join(', ')}</p>`;
    const types = (place.types || []).map(t => escapeHtml(t.label || t.sourceLabel || t.identifier)).filter(Boolean);
    if (types.length) h += `<p><strong>Types:</strong> ${types.join(', ')}</p>`;
    if ((place.ccodes || []).length) h += `<p><strong>Countries:</strong> ${place.ccodes.map(escapeHtml).join(', ')}</p>`;
    const ts = place.timespans || [];
    if (ts.length) {
        h += `<p><strong>Chronology:</strong> ${ts.map(s => `${s.start ?? s[0] ?? '?'}–${s.end ?? s[1] ?? '?'}`).join(', ')}</p>`;
    }
    const links = place.links || [];
    if (links.length) {
        h += `<p class="mb-1"><strong>Links:</strong></p><ul class="portal-links">`;
        links.forEach(l => {
            const id = l.identifier || l.uri || '';
            h += `<li><span class="text-muted">${escapeHtml(l.type || 'seeAlso')}:</span> `
                + `<a href="${escapeHtml(id)}" target="_blank" rel="noopener">${escapeHtml(id)}</a></li>`;
        });
        h += `</ul>`;
    }

    // Live cluster context (dynamic — reflects current θ/weights, no stored id).
    const cl = findClusterFor(pid);
    const others = cl ? cl.members.filter(m => m.place_id !== pid) : [];
    if (others.length) {
        h += `<hr><p class="portal-cluster-note"><i class="fas fa-layer-group me-1"></i>`
            + `<strong>Currently grouped with ${others.length} other place${others.length !== 1 ? 's' : ''}</strong> `
            + `in your search — the live grouping at the current merge settings:</p><ul class="portal-cluster-members">`;
        others.forEach(m => {
            h += `<li><a href="#" class="atlas-portal-open" data-portal-pid="${escapeHtml(m.place_id)}">`
                + `${escapeHtml(m.title || m.place_id)}</a> <span class="text-muted small">${escapeHtml(m.namespace || '')}</span></li>`;
        });
        h += `</ul>`;
    } else {
        h += `<hr><p class="text-muted small">Not grouped with any other place at the current merge settings.</p>`;
    }
    document.getElementById('atlas_portal_body').innerHTML = h;
}

// Fallback weights when the gateway ships no clustering_params (mirrors
// clustering.js DEFAULT_PARAMS / indexing clustering_params.json).
const DEFAULT_CLUSTER_WEIGHTS = { name: 0.35, spatial: 0.20, temporal: 0.15, type: 0.15, link: 0.15 };

// Seed the θ + weight sliders from the shipped clustering_params (called once
// per search, before the user starts tuning). Resets any prior overrides.
function seedClusterControls(clusteringParams) {
    weightOverrides = {};
    const wts = Object.assign({}, DEFAULT_CLUSTER_WEIGHTS, (clusteringParams && clusteringParams.weights) || {});
    // θ: auto-fit a best-guess disambiguating value to THIS result set (the
    // natural valley in the pair-score distribution), unless the user has taken
    // manual control. Falls back to the Atlas default on small/degenerate sets.
    // Auto-fit θ only on a fresh query; a reload of the same result set (facet
    // toggle, temporal re-search) keeps the current θ. Manual θ always kept.
    if (!thetaUserSet && thetaNeedsFit) {
        const sug = suggestTheta({
            hits: (gatewayData && gatewayData.hits) || [],
            edges: (gatewayData && gatewayData.edges) || [],
            params: clusteringParams,
            stoplist: (gatewayData && gatewayData.toponym_stoplist) || [],
            fallback: ATLAS_DEFAULT_THETA,
        });
        clusterThetaOverride = sug.theta;
        thetaAutoFitted = sug.reason === 'gap';   // only badge a genuine fit, not the fallback
        thetaNeedsFit = false;
    }
    const thetaSlider = document.getElementById('atlas_cluster_theta');
    if (thetaSlider) {
        thetaSlider.value = clusterThetaOverride;
        const tv = document.getElementById('atlas_cluster_theta_val');
        if (tv) tv.textContent = clusterThetaOverride.toFixed(2);
        const autoBadge = document.getElementById('atlas_cluster_theta_auto');
        if (autoBadge) autoBadge.style.display = thetaAutoFitted ? '' : 'none';
    }
    document.querySelectorAll('.weight-slider').forEach(sl => {
        const f = sl.dataset.facet;
        if (wts[f] != null) {
            sl.value = wts[f];
            const rd = document.querySelector(`.weight-val[data-facet="${f}"]`);
            if (rd) rd.textContent = Number(wts[f]).toFixed(2);
        }
    });
    // Same-gazetteer repulsion: reset to the shipped/default strength each search.
    sameNsPenaltyOverride = (clusteringParams && clusteringParams.same_ns_penalty != null)
        ? clusteringParams.same_ns_penalty : ATLAS_DEFAULT_NS_PENALTY;
    const nsSlider = document.getElementById('atlas_ns_penalty');
    if (nsSlider) {
        nsSlider.value = sameNsPenaltyOverride;
        const rd = document.getElementById('atlas_ns_penalty_val');
        if (rd) rd.textContent = sameNsPenaltyOverride.toFixed(2);
    }
}

function clearMapHighlight() {
    try { heroMap.map.removeFeatureState({ source: 'places' }); } catch (e) { /* map not ready */ }
}

// Highlight the given hit indices on the map (+ optionally fit to them).
function highlightHits(indices, { fit = false } = {}) {
    if (!clusterFC) return;
    clearMapHighlight();
    indices.forEach(i => {
        try { heroMap.map.setFeatureState({ source: 'places', id: i }, { highlight: true }); } catch (e) { /* */ }
    });
    if (fit) {
        const feats = indices.map(i => clusterFC.features[i]).filter(Boolean);
        if (feats.length) {
            // Clamp the zoom so clicking a cluster keeps geographic context (a
            // single point would otherwise zoom right in). The panel no longer
            // overlays the map, so the old right:400 padding is gone.
            // fitViewport's 2nd arg is a NUMERIC maxZoom (not an options object);
            // clamp so clicking a cluster keeps geographic context.
            heroMap.map.fitViewport(bbox({ type: 'FeatureCollection', features: feats }), 6);
        }
    }
}

function pidsToIndices(pids) {
    if (!clusterPidToIndex) return [];
    return pids.map(p => clusterPidToIndex.get(p)).filter(i => i != null);
}

function isBetaUser() {
    const m = document.querySelector('meta[name="beta-user"]');
    return !!(m && m.content === '1');
}

function initiateGatewaySearch(options) {
    const resultsDiv = document.getElementById('atlas_search_results');
    $.ajax({
        type: 'POST',
        url: '/atlas/search/',
        data: JSON.stringify(options),
        contentType: 'application/json',
        headers: { 'X-CSRFToken': csrfToken },
        success: (data) => {
            gatewayData = data;
            seedClusterControls(data.clustering_params);
            renderClusters();
        },
        error: (err) => {
            console.error('Atlas: gateway search error', err);
            resultsDiv.innerHTML = '<div class="p-3 text-danger">Search failed. Please try again.</div>';
        },
    });
}

function hitsToFeatureCollection(hits, assignments) {
    const features = [];
    hits.forEach((h, i) => {
        let geometry = null;
        if (Array.isArray(h.geometries) && h.geometries.length) {
            geometry = h.geometries.length === 1
                ? h.geometries[0]
                : { type: 'GeometryCollection', geometries: h.geometries };
        } else if (Array.isArray(h.repr_point) && h.repr_point.length >= 2) {
            geometry = { type: 'Point', coordinates: [h.repr_point[0], h.repr_point[1]] };
        }
        if (!geometry) return;
        features.push({
            type: 'Feature',
            // Contiguous id == array position. Hits with no geometry are skipped,
            // so the ORIGINAL hit index `i` would leave gaps and break every
            // `clusterFC.features[id]` array lookup (card↔marker↔zoom sync).
            id: features.length,
            geometry,
            properties: {
                pid: h.place_id,
                title: h.title,
                namespace: h.namespace,
                cluster: assignments ? assignments.get(h.place_id) : null,
            },
        });
    });
    return { type: 'FeatureCollection', features };
}

function renderClusters() {
    if (!gatewayData) return;
    // Apply the temporal filter client-side too, so dragging the date control
    // re-filters the loaded results live (idempotent when the server already
    // filtered — those hits all pass).
    const hits = (gatewayData.hits || []).filter(temporalHitPasses);
    const $resultsDiv = $('#atlas_search_results');
    $resultsDiv.empty();

    document.getElementById('atlas_no_results').style.display = hits.length === 0 ? 'block' : 'none';
    const controls = document.getElementById('atlas_cluster_controls');
    if (controls) controls.style.display = hits.length ? '' : 'none';
    const weightControls = document.getElementById('atlas_weight_controls');
    if (weightControls) weightControls.style.display = hits.length ? '' : 'none';

    const { clusters, assignments, params, debug } = clusterHits({
        hits,
        edges: gatewayData.edges || [],
        params: gatewayData.clustering_params || undefined,
        theta: clusterThetaOverride == null ? undefined : clusterThetaOverride,
        weights: weightOverrides,
        sameNsPenalty: sameNsPenaltyOverride == null ? undefined : sameNsPenaltyOverride,
        stoplist: gatewayData.toponym_stoplist || [],
    });
    lastClusters = clusters;
    renderTypeFacets(gatewayData.facets);
    console.log('Atlas cluster', debug, params);

    // aat_id → friendly label, from the result-set facets (used for member types).
    const aatLabels = {};
    ((gatewayData.facets && gatewayData.facets.aat_types) || []).forEach(f => { aatLabels[f.aat_id] = f.label; });

    const countEl = document.getElementById('atlas_results_count');
    countEl.textContent = `${hits.length} place${hits.length !== 1 ? 's' : ''} · `
        + `${clusters.length} cluster${clusters.length !== 1 ? 's' : ''}`;

    clusters.forEach((cluster, ci) => {
        const members = cluster.members;
        const rep = members[0] || {};
        const multi = members.length > 1;
        const ccodes = [...new Set(members.flatMap(m => m.ccodes || []).filter(Boolean))];
        // Toponyms and AAT types shared by ALL members (cluster level) vs each
        // member's additional variants (member level).
        const { common: commonTop, extras: extraTop } = splitCommonExtra(members.map(memberToponyms));
        const { common: commonTypes, extras: extraTypes } = splitCommonExtra(members.map(m => memberTypes(m, aatLabels)));
        // Space-separated pids so the map-click handler can find this card via
        // an attribute selector ([data-pids~="<pid>"]).
        const pids = members.map(m => m.place_id).join(' ');
        let html = `<div class="cluster-card${multi ? ' cluster-multi' : ''}" data-cluster="${ci}" data-pids="${escapeHtml(pids)}">`;
        html += `<div class="cluster-head">
            <span class="cluster-title">${escapeHtml(rep.title || '(untitled)')}</span>`;
        if (multi) {
            html += `<span class="cluster-badge" title="places merged into this cluster">`
                + `${members.length}<i class="fas fa-layer-group ms-1"></i></span>`;
        } else {
            // Single-place cluster: the "open details" affordance lives on the head.
            html += `<button class="atlas-portal-open btn btn-sm btn-link p-0" data-portal-pid="${escapeHtml(rep.place_id)}" title="Open place details"><i class="fas fa-circle-info"></i></button>`;
        }
        html += `</div>`;
        // Cluster-level facets: countries, AAT types common to all members, and
        // the overall attested date span. (Namespaces are shown per member below.)
        html += chipRow('cluster-countries', ccodes.map(c => ({ chip: 'cc-chip', text: c, title: ccLabel(c) })));
        html += chipRow('cluster-types', commonTypes.map(t => ({ chip: 'type-chip', text: t })));
        const clusterSpan = clusterTemporalSpan(members);
        if (clusterSpan) {
            html += chipRow('cluster-temporal', [{ chip: 'temporal-chip', text: formatRange(clusterSpan), title: 'Attested date span (earliest–latest across members)' }]);
        }
        // Toponyms common to ALL members (or, for a single-place cluster, simply all of them).
        html += toponymsList(commonTop);
        if (multi) {
            html += `<div class="cluster-members">`;
            members.forEach((m, mi) => {
                const mRange = formatRange(m.temporal_range);
                html += `<div class="cluster-member-wrap">`
                    + `<div class="cluster-member" data-pid="${escapeHtml(m.place_id)}">`
                    + `<span class="member-title">${escapeHtml(m.title || m.place_id)}</span>`
                    + (mRange ? `<span class="member-temporal" title="Attested date range">${escapeHtml(mRange)}</span>` : '')
                    + `<span class="member-ns" title="${escapeHtml(nsLabel(m.namespace))}">${escapeHtml(m.namespace || '')}</span>`
                    + `<button class="atlas-portal-open btn btn-sm btn-link p-0 ms-1" data-portal-pid="${escapeHtml(m.place_id)}" title="Open place details"><i class="fas fa-circle-info"></i></button>`
                    + `</div>`
                    + chipRow('member-types', (extraTypes[mi] || []).map(t => ({ chip: 'type-chip', text: t })))
                    + toponymsList(extraTop[mi] || [], 'member-toponyms')
                    + `</div>`;
            });
            html += `</div>`;
        }
        html += `</div>`;
        $resultsDiv.append(html);
    });

    // Plot on the hero map + cache the pid→index map for panel↔map sync.
    const fc = hitsToFeatureCollection(hits, assignments);
    clusterFC = fc;
    // Derive pid→index from the plotted features (contiguous ids), NOT from the
    // raw hits — so it stays aligned when no-geometry hits are dropped.
    clusterPidToIndex = new Map(fc.features.map(f => [f.properties.pid, f.id]));
    heroMap.showResultFeatures(fc);
    if (fc.features.length > 0) {
        heroMap.map.fitViewport(bbox(fc), 9); // numeric maxZoom; avoid street-level on a tight result set
    }
}

function gatherToponymOptions(qstr) {
    const treeIds = typeTree ? typeTree.getSelectedIdentifiers() : [];

    // Build spatial constraint: viewport takes precedence over area selections
    let bounds;
    let spatialMode;

    if (useViewport && !heroMap.isGlobeMode()) {
        // Viewport constraint: use current map viewport as a bounding polygon
        const bb = heroMap.getBBox(); // [west, south, east, north]
        if (bb) {
            bounds = {
                type: 'Polygon',
                coordinates: [[
                    [bb[0], bb[1]],
                    [bb[2], bb[1]],
                    [bb[2], bb[3]],
                    [bb[0], bb[3]],
                    [bb[0], bb[1]],
                ]],
            };
            spatialMode = 'region';
        } else {
            bounds = { type: 'GeometryCollection', geometries: [] };
            spatialMode = 'none';
        }
    } else {
        // Area-selection constraint
        const regionGeometries = selectedRegions
            .filter(r => r.geometry)
            .map(r => r.geometry);
        bounds = regionGeometries.length > 0
            ? { type: 'GeometryCollection', geometries: regionGeometries }
            : { type: 'GeometryCollection', geometries: [] };
        spatialMode = regionGeometries.length > 0 ? 'region' : 'none';
    }

    return {
        qstr: qstr,
        idx: eswhg,
        fclasses: treeIds.join(','),
        // `types` = exact source-vocabulary identifiers. Preserve the AAT tree's
        // contribution and append custom (non-AAT) source-type facet selections
        // (place#122); the result-facet AAT chips drive `aat_types` separately.
        types: treeIds.concat(selectedTypes),
        temporal: temporalMode !== 'off',
        start: temporalFrom,
        end: temporalTo,
        undated: temporalMode === 'undated',
        exact: exactMatch,
        cluster: clusterResults,
        bounds: bounds,
        regions: [],
        countries: [],
        userareas: [],
        spatial: spatialMode,
        aat_types: selectedAatTypes.slice(),
    };
}

function renderToponymResults(data) {
    const $resultsDiv = $('#atlas_search_results');
    $resultsDiv.empty();

    let featureCollection = data.features ? data : geomsGeoJSON(data['suggestions']);
    results = featureCollection.features;

    const countEl = document.getElementById('atlas_results_count');
    countEl.textContent = `${results.length} result${results.length !== 1 ? 's' : ''}`;

    const noResultsEl = document.getElementById('atlas_no_results');
    noResultsEl.style.display = results.length === 0 ? 'block' : 'none';

    results.forEach((feature, index) => {
        let r = feature.properties;
        const count = parseInt(r.linkcount) + 1;
        const pid = r.pid;
        const whg_id = r.whg_id;
        const children = r.children;
        const encodedChildren = encodeURIComponent(children.join(','));
        let resultIdx = count > 1 ? 'whg' : 'pub';

        let html = `<div class="result ${resultIdx}-result">
            <span>
                <span class="red-head">${r.title}</span>
                <span class="float-end small">
                    ${resultIdx === 'pub'
                        ? '<i class="fas fa-chain-broken" title="Unlinked"></i>'
                        : (count > 1 ? `${count} linked <i class="fas fa-link"></i>` : '')}
                    <button class="btn btn-primary btn-sm m-1 portal-link"
                            data-whg-id="${whg_id}" data-pid="${pid}"
                            data-children="${encodedChildren}">Details</button>
                </span>
            </span>`;

        if (r.dataset) {
            html += `<span class="result-dataset-badge">${r.dataset}</span>`;
        }

        if (r.descriptions && r.descriptions.length > 0) {
            html += `<p class="result-description more-or-less">${r.descriptions[0].value}</p>`;
        }

        if (r.names && r.names.length > 0) {
            const nameItems = r.names
                .filter(n => n.toponym && n.toponym !== r.title)
                .map(n => n.toponym);
            if (nameItems.length > 0) {
                html += `<p class="more-or-less">Names (${nameItems.length}): ${nameItems.join(', ')}</p>`;
            }
        } else if (r.variants && r.variants.length > 0) {
            html += `<p class="more-or-less">Variants (${r.variants.length}): ${r.variants.join(', ')}</p>`;
        }

        if (r.types && r.types.length > 0) {
            html += `<p>Type(s): ${r.types.join(', ')}</p>`;
        }

        if (r.ccodes && r.ccodes.length > 0 && !(r.ccodes.length === 1 && r.ccodes[0] === '')) {
            html += `<p>Countries: ${r.ccodes.map(c => {
                const country = dropdown_data[1].children.find(ch => ch.id === c);
                return `<span title="${country ? country.text : ''}">${c}</span>`;
            }).join(', ')}</p>`;
        }

        if (r.timespans && r.timespans.length > 0) {
            r.timespans.sort((a, b) => a[0] - b[0]);
            html += `<p>Chronology: ${r.timespans.map(s => `${s[0]}–${s[1]}`).join(', ')}</p>`;
        }

        html += `</div>`;
        $resultsDiv.append(html);
    });

    $resultsDiv.find('.more-or-less').toggleTruncate();

    // Show results on the hero map
    heroMap.showResultFeatures(featureCollection);

    // Zoom to results
    if (results.length > 0) {
        heroMap.map.fitViewport(bbox(featureCollection), 9); // numeric maxZoom
    }
}

/* ── Results panel show/hide ── */

// ── Fixed-panel views ──
// Relocate the Regions/Gazetteers/Categories offcanvas into the results panel as
// swappable views (Replace + back). IDs and inner content are preserved so all
// existing wiring (queried by id) keeps working; only the display changes.
function initPanelViews() {
    const panel = document.getElementById('atlas_results_panel');
    if (!panel) return;
    ['layers_offcanvas', 'gazetteers_offcanvas', 'categories_offcanvas'].forEach(id => {
        const el = document.getElementById(id);
        if (!el) return;
        el.classList.remove('offcanvas', 'offcanvas-end', 'atlas-offcanvas', 'show');
        el.classList.add('atlas-panel-view');
        el.removeAttribute('tabindex');
        // Drop the offcanvas × and add a labelled "Back to results" control that
        // only shows when a result set is loaded (see showPanelView).
        const header = el.querySelector('.offcanvas-header');
        if (header) {
            header.querySelectorAll('[data-bs-dismiss="offcanvas"]').forEach(b => b.remove());
            const back = document.createElement('button');
            back.type = 'button';
            back.className = 'btn btn-sm atlas-view-back';
            back.title = 'Back to results';
            back.innerHTML = '<i class="fas fa-arrow-left me-1"></i>Results';
            back.style.display = 'none';
            back.addEventListener('click', showResultsView);
            header.insertBefore(back, header.firstChild);
        }
        panel.appendChild(el);   // move into the fixed panel (keeps listeners)
    });
    // Toolbar buttons toggle their view (click again → back to results).
    document.querySelectorAll('[data-panel-view]').forEach(btn => {
        btn.addEventListener('click', () => {
            const view = document.getElementById(btn.dataset.panelView);
            if (view && view.classList.contains('active')) showResultsView();
            else showPanelView(btn.dataset.panelView);
        });
    });
}

function showPanelView(id) {
    const panel = document.getElementById('atlas_results_panel');
    if (!panel) return;
    // Opening the Regions panel deactivates Viewport (mutually-exclusive spatial
    // constraints — Viewport hides the boundaries Regions needs).
    if (id === 'layers_offcanvas' && useViewport) {
        useViewport = false;
        const vb = document.getElementById('atlas_viewport_btn');
        if (vb) vb.classList.remove('active');
        setViewportFrame(false);
        if (typeof updateViewportTooltip === 'function') updateViewportTooltip();
        if (layerPalette) layerPalette.refreshBoundaries();
    }
    panel.querySelectorAll('.atlas-panel-view').forEach(v => v.classList.remove('active'));
    const view = document.getElementById(id);
    // Highlight the toolbar button for the active view (none matches the results
    // view, so returning to results de-highlights all of them).
    document.querySelectorAll('[data-panel-view]').forEach(btn => {
        btn.classList.toggle('active', !!view && btn.dataset.panelView === id);
    });
    if (!view) return;
    view.classList.add('active');
    // "Back to results" is only meaningful once a result set exists.
    const back = view.querySelector('.atlas-view-back');
    if (back) back.style.display = gatewayData ? '' : 'none';
}

function showResultsView() {
    showPanelView('atlas_results_view');
}

// The panel is a permanent column; "show" just swaps the idle instructions out
// for results, "hide" (clear/close) swaps them back and resets the controls.
function showResultsPanel() {
    const dp = document.getElementById('atlas_default_panel');
    if (dp) dp.style.display = 'none';
    const hdr = document.querySelector('#atlas_results_panel .results-panel-header');
    if (hdr) hdr.style.display = '';   // header (with Clear ×) only while showing results
}

function hideResultsPanel() {
    const dp = document.getElementById('atlas_default_panel');
    if (dp) dp.style.display = '';
    const hdr = document.querySelector('#atlas_results_panel .results-panel-header');
    if (hdr) hdr.style.display = 'none';   // no Clear button on the idle instructions
    heroMap.clearResultFeatures();
    document.getElementById('atlas_search_results').innerHTML = '';
    document.getElementById('atlas_no_results').style.display = 'none';
    const count = document.getElementById('atlas_results_count');
    if (count) count.textContent = '';
    ['atlas_cluster_controls', 'atlas_weight_controls', 'atlas_type_facets'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.style.display = 'none';
    });
    gatewayData = null;
}

/* ── Clear all ── */

function clearAll() {
    searchDisabled = true;
    const input = document.getElementById('atlas_search_input');
    input.value = '';

    // Clear selections
    selectedRegions = [];
    renderSelectionChips();
    heroMap.clearOverlay();
    heroMap.clearSuggestions();
    heroMap.clearResultFeatures();

    // Reset temporal
    resetTemporalControl();

    // Reset admin level (via layer palette)
    if (layerPalette) layerPalette.resetAdminLevel();

    // Reset type tree
    if (typeTree) {
        typeTree.clearAll();
        filterState.set('place_types', []);
        updateTreeBadge();
    }

    // Reset exact match
    exactMatch = false;
    const emBtn = document.getElementById('atlas_exact_match');
    if (emBtn) { emBtn.classList.remove('active'); emBtn.setAttribute('aria-pressed', 'false'); }

    // Reset viewport constraint
    useViewport = false;
    const vpBtn = document.getElementById('atlas_viewport_btn');
    if (vpBtn) { vpBtn.classList.remove('active'); }
    setViewportFrame(false);
    updateViewportTooltip();

    // Reset clustering (always-on default; slider TBD per Master Plan §4.2)
    clusterResults = true;

    // Reset gazetteers (formerly authorities) — return to Filter mode + standard list,
    // restore default checkbox selection, and reset both per-mode caches.
    setGazetteerMode('filter');
    const defaults = ['gn', 'iv', 'ohm', 'pl', 'tgn', 'tm', 'wd', 'whg'];
    filterSelections.clear();
    defaults.forEach(v => filterSelections.add(v));
    exploreSelection = null;
    document.querySelectorAll('#gazetteers_offcanvas .authority-cb').forEach(cb => {
        cb.checked = filterSelections.has(cb.value);
    });
    // Reset the coverage switches + re-apply (clears any coverage filtering).
    document.querySelectorAll('#gazetteers_offcanvas .gazetteer-stub-switch').forEach(sw => {
        sw.checked = false;
    });
    applyGazetteerCoverageFilter();

    // Hide results
    hideResultsPanel();

    // Reset search mode to Areas
    switchSearchMode('areas');
    document.querySelectorAll('.search-mode-toggle .btn').forEach(b => b.classList.remove('active'));
    const areasBtn = document.querySelector('.search-mode-toggle .btn[data-search-mode="areas"]');
    if (areasBtn) areasBtn.classList.add('active');

    // Reset map
    if (heroMap.map) heroMap.map.reset();

    filterState.reset();
    searchDisabled = false;
    closeAreaDropdown();
}

/* ── Scroll helper ── */

function scrollToResult($elem) {
    const $container = $('#atlas_result_container');
    if (!$container.length) return;
    const containerTop = $container.offset().top;
    const containerScrollTop = $container.scrollTop();
    const containerHeight = $container.innerHeight();
    const elemTop = $elem.offset().top;
    const elemHeight = $elem.outerHeight(true);
    const target = Math.round(containerScrollTop + (elemTop - containerTop) - (containerHeight / 2) + (elemHeight / 2));
    $container.stop(true).animate({ scrollTop: target }, 400, function () {
        $elem.addClass('flash-border');
        setTimeout(() => $elem.removeClass('flash-border'), 3000);
    });
}

/* ── Utilities ── */

function escapeHtml(s) {
    return String(s)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}


