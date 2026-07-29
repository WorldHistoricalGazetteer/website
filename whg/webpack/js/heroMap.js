// /whg/webpack/js/heroMap.js
/**
 * Hero map for the Atlas page — wraps a MapLibre instance in #hero_map.
 *
 * This is similar to contextMap.js but:
 * - Mounts into #hero_map (full-viewport) instead of #context_map
 * - Includes result display layers (places source + layerset)
 * - Adds layer source management (setActiveSources)
 * - Starts globe spin on init
 *
 * Does NOT import or modify the existing contextMap singleton.
 */

import filterState from './filterState';
import GazetteerInteraction from './gazetteerInteraction';

const OVERLAY_SOURCE = 'filter-overlay';
const OVERLAY_FILL = 'filter-overlay-fill';
const OVERLAY_LINE = 'filter-overlay-line';

const SUGGESTION_SOURCE = 'suggestion-markers';
const SUGGESTION_CIRCLES = 'suggestion-circles';
const SUGGESTION_LABELS = 'suggestion-labels';

// Boundary source-layer names in the whg-context style. Each is also
// the source ID for its tileset. See contextMap.js for full schema notes.
// Source ids in the base style that carry boundary geometry. After the
// tileset rename, OSM and OHM admin boundaries live under their bare
// namespace ids (``osm``, ``ohm``); the inner source-layer name in each
// mbtiles equals the source id, so this list also indexes by that name.
const BOUNDARY_SOURCE_LAYERS = ['osm', 'ohm', 'osm_misc', 'po', 'clio', 'nl'];

// Persistent user preference: when set, the map opens in Mercator and the
// gazetteer-bounds rule never auto-switches into globe. Toggled on/off by
// the globe control on the Atlas map.
const GLOBE_DISABLED_KEY = 'whg.globe_disabled';

function readGlobeDisabled() {
    try { return localStorage.getItem(GLOBE_DISABLED_KEY) === '1'; }
    catch (e) { return false; }
}

function writeGlobeDisabled(disabled) {
    try {
        if (disabled) localStorage.setItem(GLOBE_DISABLED_KEY, '1');
        else localStorage.removeItem(GLOBE_DISABLED_KEY);
    } catch (e) { /* localStorage unavailable */ }
}

// Debug flag — enable by adding ``?debug`` to the URL or setting
// ``localStorage['whg.debug'] = '1'``. When on, the MapLibre map is exposed as
// ``window.heroMapInstance`` for console/automation access (queryRenderedFeatures,
// project, etc.). Off by default so nothing leaks the map into the global scope.
function isDebugEnabled() {
    try { if (localStorage.getItem('whg.debug') === '1') return true; } catch (e) { /* */ }
    try { return /[?&]debug\b/.test(window.location.search); } catch (e) { /* */ }
    return false;
}

// Layer IDs use a neutral ``_atlas_overlay_`` prefix rather than
// ``_atlas_context_``: the global error handler in whg_maplibre.js
// classifies any error message containing the substring "context" as a
// WebGL-context fatality, which silently shows a fatal modal whenever
// MapLibre reports a layer/expression error mentioning the layer id.
//
// Country boundaries provide just enough geographic anchoring without
// competing with the GeoNames-sourced settlement overlay. Sub-country
// admin levels are deliberately left out — the place markers do that
// job better. Both line and label layers source from the ``un`` tileset
// (see BORDER_OVERLAY_TILESET below) which is country-only.
const CONTEXT_LINE_LAYER = '_atlas_overlay_country_line';
const CONTEXT_LABEL_LAYER = '_atlas_overlay_country_label';
const CONTEXT_PLACE_CIRCLE_LAYER = '_atlas_overlay_place_circle';
const CONTEXT_PLACE_LABEL_LAYER = '_atlas_overlay_place_label';
const CONTEXT_CAPITAL_CIRCLE_LAYER = '_atlas_overlay_capital_circle';
const CONTEXT_CAPITAL_LABEL_LAYER = '_atlas_overlay_capital_label';

// Settlement-context overlay. We re-use the GeoNames tileset because it
// carries population and feature-code metadata for every populated place
// on Earth, indexed by the same pipeline that builds the gazetteer
// tilesets. Pulling tiles via a separately-named source means the
// settlement overlay renders even when the user's selected gazetteer
// isn't GeoNames; when they pick GeoNames we accept the small
// duplication (browser tile cache absorbs it).
const PLACE_OVERLAY_SOURCE = '_atlas_overlay_settlements';
const PLACE_OVERLAY_TILESET = 'gn';

// Country-border overlay. The ``un`` tileset is a clean, zoom-stable
// set of UN-defined country boundaries — unlike the OSM admin tileset
// it doesn't suffer from tippecanoe coalesce-densest fragmentation that
// makes country lines flicker on/off as the user zooms. Loaded via a
// separately-named source for the same reason as the settlement
// overlay above.
const BORDER_OVERLAY_SOURCE = '_atlas_overlay_borders';
const BORDER_OVERLAY_TILESET = 'un';

// Capitals overlay — a synthetic, non-clustered tileset built by the
// indexing pipeline (``processing/generate_tiles.py::_CONTEXT_OVERLAY_BUCKETS``)
// containing every PPLC/PPLG/PPLCH/PPLA point from GeoNames. We use
// it instead of the main ``gn`` tileset for capital-class features
// because tippecanoe's clustering on ``gn`` drops capitals at zooms
// 0-8 (cluster-rep selection prefers dense neighbouring POIs).
// ``gn_capitals`` is generated without clustering, so every capital
// survives at every zoom from 0 upwards.
const CAPITALS_OVERLAY_SOURCE = '_atlas_overlay_capitals';
const CAPITALS_OVERLAY_TILESET = 'gn_capitals';
// Fcode set rendered by the capitals overlay. Mirrors the indexing-side
// filter — duplicating it here lets the existing ``gn`` settlement
// layer exclude these so capitals aren't double-rendered. ``PPLCH``
// (historical capital) is *not* included: it was tried earlier but
// pulled in noisy artefacts like former dioceses and pre-modern seats.
const CAPITAL_FCODES = ['PPLC', 'PPLG', 'PPLA'];

function filtersIntersect(layerFilter, requestedValues) {
    if (!requestedValues || requestedValues.length === 0) return true;
    const requested = new Set(requestedValues);
    if (!Array.isArray(layerFilter)) return true;
    if (layerFilter[0] === 'match' && Array.isArray(layerFilter[2])) {
        for (const v of layerFilter[2]) if (requested.has(v)) return true;
        return false;
    }
    if (layerFilter[0] === '==' && typeof layerFilter[2] === 'string') {
        return requested.has(layerFilter[2]);
    }
    return true;
}

/**
 * Merge the tile fragments of one boundary feature back into a single polygon.
 *
 * A polygon in a vector tileset is cut at every tile edge, and tippecanoe pads
 * each piece with a buffer, so neighbouring fragments *overlap* by a fraction
 * of a degree. MapLibre stencils that away for the tiled fill, but the selected
 * region is re-published as a GeoJSON overlay, where nothing clips it: the
 * overlap bands blend twice (the "tile overlap stripes" of place#156) and every
 * tile-clip edge is drawn by the overlay's line layer as a rule straight across
 * the region's interior. Unioning the fragments removes both, and — since the
 * same geometry is handed on as the search's spatial constraint — also stops
 * the constraint double-counting its own seams.
 *
 * Falls back to the raw fragments if turf is unavailable or the union fails,
 * which is no worse than the previous behaviour.
 *
 * @param {Array} features — one Feature per fragment, already de-duplicated
 * @returns {Object} FeatureCollection, ideally of a single merged Feature
 */
function dissolveFragments(features) {
    const fc = { type: 'FeatureCollection', features };
    // ``window.turf`` — turf is a CDN global (see base.js), not a bundled import,
    // so the bare identifier is not in this module's scope.
    const turf = window.turf;
    if (features.length < 2 || !turf || !turf.union) return fc;
    try {
        let merged = features[0];
        for (let i = 1; i < features.length; i++) {
            merged = turf.union(merged, features[i]) || merged;
        }
        if (!merged || !merged.geometry) return fc;
        return {
            type: 'FeatureCollection',
            features: [{
                type: 'Feature',
                geometry: merged.geometry,
                properties: features[0].properties || {},
            }],
        };
    } catch (e) {
        console.warn('heroMap: boundary fragment union failed, using raw fragments', e);
        return fc;
    }
}

class HeroMap {
    constructor() {
        this.map = null;
        this._viewportListeners = [];
        this._projectionChangeListeners = [];
        this._isGlobe = true;
        this._ready = false;
        this._readyPromise = null;
        this._spinning = false;
        this._spinRAF = null;
        this._spinStopped = false;
        this._layersBySource = {};
        this._fillBySource = {};
        this._boundaryLayerIds = [];
        this._originalFilters = {};
        this._originalZoomRanges = {};
        this._hoverTooltip = null;
        this._currentSource = null;
        this._currentGazetteer = null;
        // True while the shown gazetteer carries a place#140 coverage footprint
        // (a polygon gazetteer). Drives the "zoom in for detail" hint pill.
        this._hasCoverage = false;
        this._currentBasemap = 'whg-context';   // active basemap style id (see setBasemapStyle)
        this._gazetteerInteraction = null;
        this._contextLayerIds = [];
        this._settlementDiagnostic = null;
        this._hovered = null;
        this._selected = null;
        // Set true while ``applyProjectionForBounds`` is driving a
        // programmatic projection change; ``_emitProjectionChange``
        // checks this so it doesn't persist a globe-disabled flag from
        // an automatic switch.
        this._programmaticProjection = false;
        // When pinned (e.g. by the Atlas demo mode), the gazetteer-bounds
        // rule never switches to Mercator — the map stays on the globe.
        this._globePinned = false;
        // The first bounds-driven projection decision (the initial context
        // tileset load) keeps the globe so the Atlas opens in globe view
        // even for world-wide bounds; later wide bounds may still flatten.
        this._initialBoundsApplied = false;
    }

    /**
     * Initialise the map in the #hero_map container.
     * Returns a promise that resolves when the map is loaded.
     */
    init() {
        if (this._readyPromise) return this._readyPromise;

        const startInGlobe = !readGlobeDisabled();
        this._isGlobe = startInGlobe;

        this._readyPromise = new Promise((resolve) => {
            this.map = new whg_maplibre.Map({
                container: 'hero_map',
                zoom: 2.5,
                minZoom: 0.5,
                maxZoom: 22,
                maxBounds: undefined,
                style: ['whg-context'],
                fullscreenControl: false,
                downloadMapControl: false,
                drawingControl: false,
                temporalControl: false,
                navigationControl: {position: 'top-right', showZoom: true, showCompass: false, visualizePitch: false},
                globeControl: true,
                globeMode: startInGlobe,
            });

            this.map.on('load', () => {
                this._addOverlayLayers();
                this._addSuggestionLayers();
                this._addResultLayers();
                this._initBoundaryLayers();

                // Track viewport
                this.map.on('moveend', () => {
                    const bounds = this.map.getBounds();
                    const bbox = [
                        bounds.getWest(), bounds.getSouth(),
                        bounds.getEast(), bounds.getNorth(),
                    ];
                    filterState.set('spatial.bbox', bbox);
                    this._emitViewportChange(bbox);
                });

                this._wireSpinStop();
                this._wireProjectionDetection();
                // Toggle the coverage "zoom in for detail" hint as the zoom
                // crosses the footprint→boundaries threshold (place#140).
                this.map.on('zoom', () => this._updateCoverageHint());

                // Fade out the loading overlay once the map is idle
                // (globe projection has finished its initial render)
                this.map.once('idle', () => {
                    const overlay = document.getElementById('map_loading_overlay');
                    if (overlay) {
                        overlay.classList.add('fade-out');
                        overlay.addEventListener('transitionend', () => {
                            overlay.style.display = 'none';
                        }, { once: true });
                    }
                });

                this._ready = true;
                // Debug hook: expose the map for console/automation when enabled.
                if (isDebugEnabled()) {
                    try { window.heroMapInstance = this.map; } catch (e) { /* */ }
                }
                resolve(this.map);
            });
        });

        return this._readyPromise;
    }

    // ── Overlay layers (for selected region polygons) ──

    _addOverlayLayers() {
        this.map.addSource(OVERLAY_SOURCE, {
            type: 'geojson',
            data: { type: 'FeatureCollection', features: [] },
        });
        this.map.addLayer({
            id: OVERLAY_FILL, type: 'fill', source: OVERLAY_SOURCE,
            paint: { 'fill-color': '#4a90d9', 'fill-opacity': 0.15 },
        });
        this.map.addLayer({
            id: OVERLAY_LINE, type: 'line', source: OVERLAY_SOURCE,
            paint: { 'line-color': '#2563eb', 'line-width': 2, 'line-opacity': 0.7 },
        });
    }

    // ── Suggestion markers ──

    _addSuggestionLayers() {
        this.map.addSource(SUGGESTION_SOURCE, {
            type: 'geojson',
            data: { type: 'FeatureCollection', features: [] },
        });
        this.map.addLayer({
            id: SUGGESTION_CIRCLES, type: 'circle', source: SUGGESTION_SOURCE,
            paint: {
                'circle-radius': 7, 'circle-color': '#e04040',
                'circle-stroke-color': '#fff', 'circle-stroke-width': 2, 'circle-opacity': 0.85,
            },
        });
        this.map.addLayer({
            id: SUGGESTION_LABELS, type: 'symbol', source: SUGGESTION_SOURCE,
            layout: {
                'text-field': ['get', 'label'], 'text-font': ['Open Sans Semibold'],
                'text-size': 11, 'text-offset': [0, 1.4], 'text-anchor': 'top',
                'text-allow-overlap': true,
            },
            paint: { 'text-color': '#333', 'text-halo-color': '#fff', 'text-halo-width': 1.5 },
        });

        this.map.on('click', SUGGESTION_CIRCLES, (e) => {
            if (e.features && e.features.length > 0) {
                const props = e.features[0].properties;
                const detail = typeof props === 'string' ? JSON.parse(props) : props;
                document.dispatchEvent(new CustomEvent('suggestion-click', { detail }));
            }
        });
        this.map.on('mouseenter', SUGGESTION_CIRCLES, () => {
            this.map.getCanvas().style.cursor = 'pointer';
        });
        this.map.on('mouseleave', SUGGESTION_CIRCLES, () => {
            this.map.getCanvas().style.cursor = '';
        });
    }

    // ── Result display layers (for toponym search results) ──

    _addResultLayers() {
        this.map.newSource('places').newLayerset('places', null, 'plain');
        // Hide result layers initially (Explorer mode)
        try {
            const placeLayers = this.map.getStyle().layers
                .filter(l => l.source === 'places')
                .map(l => l.id);
            for (const layerId of placeLayers) {
                this.map.setLayoutProperty(layerId, 'visibility', 'none');
            }
        } catch (e) { /* layers may not exist yet */ }
    }

    /** Show result features on the map (Toponym mode). */
    showResultFeatures(geojson) {
        if (!this.map) return;
        this.map.getSource('places').setData(geojson);
        // Make result layers visible
        try {
            const placeLayers = this.map.getStyle().layers
                .filter(l => l.source === 'places')
                .map(l => l.id);
            for (const layerId of placeLayers) {
                this.map.setLayoutProperty(layerId, 'visibility', 'visible');
            }
        } catch (e) { /* ignore */ }
    }

    /** Clear result features (return to Explorer mode). */
    clearResultFeatures() {
        if (!this.map) return;
        this.map.getSource('places').setData(this.map.nullCollection());
        try {
            const placeLayers = this.map.getStyle().layers
                .filter(l => l.source === 'places')
                .map(l => l.id);
            for (const layerId of placeLayers) {
                this.map.setLayoutProperty(layerId, 'visibility', 'none');
            }
        } catch (e) { /* ignore */ }
    }

    // ── Public API mirroring contextMap ──

    getBBox() {
        if (!this.map) return null;
        const bounds = this.map.getBounds();
        return [bounds.getWest(), bounds.getSouth(), bounds.getEast(), bounds.getNorth()];
    }

    setOverlay(geojson) {
        if (!this.map) return;
        const data = geojson.type === 'Feature' ? geojson
            : geojson.type === 'FeatureCollection' ? geojson
            : { type: 'Feature', geometry: geojson, properties: {} };
        this.map.getSource(OVERLAY_SOURCE).setData(data);
    }

    clearOverlay() {
        if (!this.map) return;
        this.map.getSource(OVERLAY_SOURCE).setData({ type: 'FeatureCollection', features: [] });
    }

    setSuggestions(fc) {
        if (!this.map) return;
        try { this.map.getSource(SUGGESTION_SOURCE).setData(fc); }
        catch (e) { console.warn('heroMap.setSuggestions: source not ready', e); }
    }

    clearSuggestions() {
        if (!this.map) return;
        try {
            this.map.getSource(SUGGESTION_SOURCE).setData({ type: 'FeatureCollection', features: [] });
        } catch (e) { /* */ }
    }

    fitTo(geojson) {
        if (!this.map) return;
        try { this.map.fitViewport(bbox(geojson)); }
        catch (e) { console.warn('heroMap.fitTo: could not fit bounds', e); }
    }

    onViewportChange(callback) {
        this._viewportListeners.push(callback);
        return () => {
            this._viewportListeners = this._viewportListeners.filter(fn => fn !== callback);
        };
    }

    resize() {
        if (this.map) this.map.resize();
    }

    // ── Globe spin ──

    startSpin(degreesPerSecond = 1) {
        if (this._spinStopped || this._spinning || !this.map) return;
        this._spinning = true;
        let lastTime = performance.now();
        const tick = (now) => {
            if (!this._spinning) return;
            const dt = (now - lastTime) / 1000;
            lastTime = now;
            const center = this.map.getCenter();
            let lng = center.lng - degreesPerSecond * dt;
            if (lng < -180) lng += 360;
            this.map.jumpTo({ center: [lng, center.lat] });
            this._spinRAF = requestAnimationFrame(tick);
        };
        this._spinRAF = requestAnimationFrame(tick);
    }

    stopSpin() {
        if (!this._spinning) return;
        this._spinning = false;
        this._spinStopped = true;
        if (this._spinRAF) {
            cancelAnimationFrame(this._spinRAF);
            this._spinRAF = null;
        }
    }

    get spinWasStopped() { return this._spinStopped; }

    _wireSpinStop() {
        const canvas = this.map.getCanvas();
        const stop = () => this.stopSpin();
        canvas.addEventListener('mousedown', stop, { once: true });
        canvas.addEventListener('touchstart', stop, { once: true });
        canvas.addEventListener('wheel', stop, { once: true });
        this.map.on('zoomstart', stop);
        this.map.on('dragstart', stop);
        this.map.on('pitchstart', stop);
        this.map.on('rotatestart', stop);
    }

    // ── Boundary layers ──

    _initBoundaryLayers() {
        const style = this.map.getStyle();
        if (!style || !style.layers) return;

        this._layersBySource = {};
        this._fillBySource = {};
        for (const l of style.layers) {
            const sl = l['source-layer'];
            if (!BOUNDARY_SOURCE_LAYERS.includes(sl)) continue;
            (this._layersBySource[sl] ||= []).push(l.id);
            if (l.type === 'fill' && !this._fillBySource[sl]) {
                this._fillBySource[sl] = l.id;
            }
        }
        this._boundaryLayerIds = Object.values(this._layersBySource).flat();

        if (this._boundaryLayerIds.length === 0) {
            console.warn('heroMap: no boundary layers found in style');
            return;
        }

        this._originalFilters = {};
        this._originalZoomRanges = {};
        for (const layerId of this._boundaryLayerIds) {
            const def = style.layers.find(l => l.id === layerId);
            this._originalFilters[layerId] = def?.filter
                ? JSON.parse(JSON.stringify(def.filter)) : null;
            this._originalZoomRanges[layerId] = [def?.minzoom ?? 0, def?.maxzoom ?? 24];
        }

        for (const layerId of this._boundaryLayerIds) {
            try { this.map.setLayoutProperty(layerId, 'visibility', 'none'); } catch (e) {}
        }

        this._hoverTooltip = document.createElement('div');
        this._hoverTooltip.className = 'boundary-hover-tooltip';
        this._hoverTooltip.style.display = 'none';
        document.body.appendChild(this._hoverTooltip);

        for (const [sourceId, fillId] of Object.entries(this._fillBySource)) {
            this.map.on('mousemove', fillId, (e) => this._onBoundaryMousemove(e, sourceId));
            this.map.on('mouseleave', fillId, () => this._onBoundaryMouseleave(sourceId));
            this.map.on('click', fillId, (e) => this._onBoundaryClick(e, sourceId));
        }
    }

    _onBoundaryMousemove(e, sourceId) {
        this.map.getCanvas().style.cursor = 'pointer';
        if (!e.features || e.features.length === 0) return;
        const feature = e.features[0];
        const featureId = feature.id;
        if (featureId != null && (
            !this._hovered || this._hovered.source !== sourceId || this._hovered.id !== featureId
        )) {
            if (this._hovered) {
                try {
                    this.map.removeFeatureState(
                        { source: this._hovered.source, sourceLayer: this._hovered.source, id: this._hovered.id },
                        'hover',
                    );
                } catch (err) {}
            }
            this._hovered = { source: sourceId, id: featureId };
            try {
                this.map.setFeatureState(
                    { source: sourceId, sourceLayer: sourceId, id: featureId },
                    { hover: true },
                );
            } catch (err) {}
        }
        const name = feature.properties?.name;
        if (name && this._hoverTooltip) {
            this._hoverTooltip.textContent = name;
            this._hoverTooltip.style.display = 'block';
            this._hoverTooltip.style.left = (e.originalEvent.pageX + 12) + 'px';
            this._hoverTooltip.style.top = (e.originalEvent.pageY - 28) + 'px';
        }
    }

    _onBoundaryMouseleave(_sourceId) {
        this.map.getCanvas().style.cursor = '';
        this.clearBoundaryHover();
    }

    /**
     * Select a boundary the user found by name rather than by clicking it.
     *
     * The index carries no polygon for an OSM/OHM boundary, so a search result
     * only becomes a usable region constraint once the same feature is located
     * in the tiles — which is what this does, emitting the same
     * ``boundary-click`` event the map click path emits.
     *
     * @param {string} sourceId — boundary source-layer name (``osm``/``ohm``)
     * @param {string} placeId — e.g. ``osm:r161648``
     * @returns {boolean} whether the feature was present in the loaded tiles
     */
    selectBoundaryByPlaceId(sourceId, placeId) {
        if (!this.map || !placeId || !this._layersBySource[sourceId]) return false;
        let matches;
        try {
            matches = this.map.querySourceFeatures(sourceId, {
                sourceLayer: sourceId,
                filter: ['==', ['get', 'place_id'], placeId],
            });
        } catch (e) {
            return false;
        }
        if (!matches || matches.length === 0) return false;
        this._emitBoundarySelection(matches[0], sourceId);
        return true;
    }

    _onBoundaryClick(e, sourceId) {
        if (!e.features || e.features.length === 0) return;
        this._emitBoundarySelection(e.features[0], sourceId);
    }

    _emitBoundarySelection(feature, sourceId) {
        const props = feature.properties || {};

        this.clearBoundarySelection();
        if (feature.id != null) {
            this._selected = { source: sourceId, id: feature.id };
            try {
                this.map.setFeatureState(
                    { source: sourceId, sourceLayer: sourceId, id: feature.id },
                    { selected: true },
                );
            } catch (err) {}
        }

        const fc = this._collectBoundaryFragments(feature, sourceId);
        let geometry = feature.geometry;
        if (fc && fc.features.length > 0) {
            geometry = fc.features.length === 1
                ? fc.features[0].geometry
                : { type: 'GeometryCollection', geometries: fc.features.map(f => f.geometry) };
        }

        document.dispatchEvent(new CustomEvent('boundary-click', {
            detail: {
                id: props.place_id || '',
                place_id: props.place_id || '',
                name: props.name || '',
                boundary: props.boundary,
                namespace: props.namespace || sourceId.split('_')[0],
                geometry,
            },
        }));
    }

    _collectBoundaryFragments(feature, sourceId) {
        if (feature.id == null) return null;
        try {
            const all = this.map.querySourceFeatures(sourceId, {
                sourceLayer: sourceId,
                filter: ['==', ['id'], feature.id],
            });
            if (all.length === 0) return null;
            const seen = new Set();
            const features = [];
            for (const f of all) {
                const key = JSON.stringify(f.geometry.coordinates);
                if (seen.has(key)) continue;
                seen.add(key);
                features.push({ type: 'Feature', geometry: f.geometry, properties: f.properties || {} });
            }
            return dissolveFragments(features);
        } catch (e) {
            console.warn('heroMap._collectBoundaryFragments: query failed', e);
            return null;
        }
    }

    /**
     * Activate boundary display for one source, narrowed to the given
     * `boundary` field values. See contextMap.showBoundaries for full
     * semantics — this is the same logic on the hero map.
     *
     * The style tiers its boundary linework by zoom (country lines stop at z8,
     * every tier at z10) on the assumption that all tiers are drawn together.
     * Here only one tier is ever visible at a time, so those ceilings just make
     * the chosen boundaries vanish as you zoom in while their fill stays
     * clickable (place#156). Any layer this call switches on is therefore
     * widened to the full zoom range, and put back when it is switched off.
     *
     * @param {Object} opts
     * @param {string} opts.source — boundary source-layer name
     * @param {string[]} [opts.boundaryValues] — restrict visible features
     */
    showBoundaries(opts) {
        if (!this.map || this._boundaryLayerIds.length === 0) return;
        // Switching back to a baked-in source — drop any dynamic gazetteer first.
        this.hideGazetteer();
        const { source, boundaryValues = null } = opts || {};
        if (!source || !this._layersBySource[source]) {
            console.warn('heroMap.showBoundaries: unknown source', source);
            return;
        }

        for (const [sId, layerIds] of Object.entries(this._layersBySource)) {
            if (sId === source) continue;
            for (const layerId of layerIds) {
                try {
                    this.map.setFilter(layerId, this._originalFilters[layerId] || null);
                    this._restoreLayerZoomRange(layerId);
                    this.map.setLayoutProperty(layerId, 'visibility', 'none');
                } catch (e) {}
            }
        }

        const valueFilter = (boundaryValues && boundaryValues.length)
            ? ['match', ['get', 'boundary'], boundaryValues, true, false]
            : null;

        const fillId = this._fillBySource[source];
        for (const layerId of this._layersBySource[source]) {
            const original = this._originalFilters[layerId];
            const overlaps = filtersIntersect(original, boundaryValues);
            if (!overlaps) {
                try {
                    this.map.setFilter(layerId, original || null);
                    this._restoreLayerZoomRange(layerId);
                    this.map.setLayoutProperty(layerId, 'visibility', 'none');
                } catch (e) {}
                continue;
            }
            try {
                if (layerId === fillId && valueFilter) {
                    this.map.setFilter(layerId, valueFilter);
                } else {
                    this.map.setFilter(layerId, original || null);
                }
                this._widenLayerZoomRange(layerId);
                this.map.setLayoutProperty(layerId, 'visibility', 'visible');
            } catch (e) {}
        }

        if (fillId) {
            try {
                this.map.setPaintProperty(fillId, 'fill-color', [
                    'case',
                    ['boolean', ['feature-state', 'selected'], false], '#4a90d9',
                    ['boolean', ['feature-state', 'hover'], false], '#fbbf24',
                    'rgb(100, 140, 190)',
                ]);
                this.map.setPaintProperty(fillId, 'fill-opacity', [
                    'case',
                    ['boolean', ['feature-state', 'selected'], false], 0.25,
                    ['boolean', ['feature-state', 'hover'], false], 0.30,
                    0.12,
                ]);
                this.map.setPaintProperty(fillId, 'fill-outline-color', [
                    'case',
                    ['boolean', ['feature-state', 'selected'], false], '#2563eb',
                    'rgba(50, 80, 120, 0.35)',
                ]);
            } catch (e) {}
        }

        this._currentSource = source;
    }

    /** Let an active boundary tier draw at every zoom (see showBoundaries). */
    _widenLayerZoomRange(layerId) {
        const original = this._originalZoomRanges[layerId];
        if (!original) return;
        try { this.map.setLayerZoomRange(layerId, 0, 24); } catch (e) {}
    }

    /** Put a boundary layer's styled zoom range back as the style declared it. */
    _restoreLayerZoomRange(layerId) {
        const original = this._originalZoomRanges[layerId];
        if (!original) return;
        try { this.map.setLayerZoomRange(layerId, original[0], original[1]); } catch (e) {}
    }

    /**
     * How many distinct boundary features a source holds at the given
     * ``boundary`` values, over the tiles currently loaded. Drives the Regions
     * panel's readout and its auto-tier fallback — a tier that resolves to
     * nothing is the main reason the old fixed zoom→level mapping felt
     * unpredictable (place#156).
     *
     * Deliberately ``querySourceFeatures``, not ``queryRenderedFeatures``:
     * layer filters are applied when a tile is parsed, so a count taken right
     * after ``setFilter`` would still describe the previous tier. Querying the
     * source with an ad-hoc filter is synchronous and answers about the data
     * rather than about the current paint.
     *
     * @param {string} source — boundary source-layer name
     * @param {string[]} boundaryValues — admin levels making up the tier
     * @returns {number} distinct features found
     */
    countBoundaryFeatures(source, boundaryValues) {
        if (!this.map || !source || !this._layersBySource[source]) return 0;
        try {
            const features = this.map.querySourceFeatures(source, {
                sourceLayer: source,
                filter: (boundaryValues && boundaryValues.length)
                    ? ['match', ['get', 'boundary'], boundaryValues, true, false]
                    : null,
            });
            const seen = new Set();
            for (const f of features) {
                seen.add(f.id != null ? f.id : f.properties?.place_id);
            }
            return seen.size;
        } catch (e) {
            return 0;
        }
    }

    hideBoundaries() {
        if (!this.map || this._boundaryLayerIds.length === 0) return;
        for (const layerId of this._boundaryLayerIds) {
            try {
                this.map.setFilter(layerId, this._originalFilters[layerId] || null);
                this._restoreLayerZoomRange(layerId);
                this.map.setLayoutProperty(layerId, 'visibility', 'none');
            } catch (e) {}
        }
        for (const fillId of Object.values(this._fillBySource)) {
            try {
                this.map.setPaintProperty(fillId, 'fill-color', 'rgba(0, 0, 0, 0)');
                this.map.setPaintProperty(fillId, 'fill-outline-color', 'rgba(0, 0, 0, 0)');
            } catch (e) {}
        }
        for (const sourceId of Object.keys(this._layersBySource)) {
            try { this.map.removeFeatureState({ source: sourceId, sourceLayer: sourceId }); }
            catch (e) {}
        }
        this._hovered = null;
        this._selected = null;
        this._currentSource = null;
        this.clearBoundaryHover();
    }

    clearBoundaryHover() {
        if (!this.map) return;
        if (this._hovered) {
            try {
                this.map.removeFeatureState(
                    { source: this._hovered.source, sourceLayer: this._hovered.source, id: this._hovered.id },
                    'hover',
                );
            } catch (e) {}
            this._hovered = null;
        }
        if (this._hoverTooltip) this._hoverTooltip.style.display = 'none';
    }

    clearBoundarySelection() {
        if (!this.map) return;
        if (this._selected) {
            try {
                this.map.removeFeatureState(
                    { source: this._selected.source, sourceLayer: this._selected.source, id: this._selected.id },
                    'selected',
                );
            } catch (e) {}
            this._selected = null;
        }
    }

    searchBoundaryFeatures(query, boundaryValues = null, limit = 20) {
        if (!this.map) return [];
        const sources = this._currentSource
            ? [this._currentSource]
            : Object.keys(this._layersBySource);
        if (sources.length === 0) return [];
        const valueFilter = (boundaryValues && boundaryValues.length)
            ? ['match', ['get', 'boundary'], boundaryValues, true, false]
            : null;
        const lower = query.toLowerCase();
        const seen = new Set();
        const results = [];
        for (const sourceId of sources) {
            let features;
            try {
                features = this.map.querySourceFeatures(sourceId, {
                    sourceLayer: sourceId,
                    filter: valueFilter,
                });
            } catch (e) { continue; }
            for (const f of features) {
                const name = (f.properties?.name || '').toLowerCase();
                if (!name || !name.includes(lower)) continue;
                const key = `${f.properties.name}|${f.properties.namespace || sourceId}`;
                if (seen.has(key)) continue;
                seen.add(key);
                results.push(f);
                if (results.length >= limit) return results;
            }
        }
        return results;
    }

    /**
     * Show a dynamic gazetteer tileset — one not present in the base
     * whg-context style. Hides baked-in boundary layers and tears down
     * any previously-shown dynamic gazetteer source, then loads (if needed)
     * and shows the requested tileset via ``map.loadGazetteerStyle``.
     *
     * After the source is in place, fits the viewport to the tileset's
     * declared ``bounds`` and picks the projection: Mercator if the bounds
     * span more than 180° of longitude, globe otherwise (unless the user
     * has previously disabled globe via the map's globe control).
     *
     * @param {string} id — tileset id (e.g. ``"tm"`` or ``"whg-892"``)
     */
    async showGazetteer(id) {
        if (!this.map || !id) return;
        this.hideBoundaries();
        if (this._currentGazetteer && this._currentGazetteer !== id) {
            if (this._gazetteerInteraction) this._gazetteerInteraction.detach();
            try { this.map.eraseSource(this._currentGazetteer); } catch (e) {}
            this._currentGazetteer = null;
        }
        // Always rebuild context layers so their z-order stays correct
        // relative to the freshly-loaded gazetteer fill/line/circle.
        this._removeContextLayers();
        // Some authority gazetteers (clio/nl/po) are pre-registered as sources in
        // the base whg-context style — with their own context fill/line layers
        // that draw the raw polygons at ALL zooms. Exploring them must not reuse
        // that stale source (it has a restrictive minzoom and would let those base
        // layers undercut the low-zoom coverage footprint), so erase it first and
        // let loadGazetteerStyle add a fresh source from the gazetteer's tilejson.
        if (this.map.getSource(id)) {
            try { this.map.eraseSource(id); } catch (e) {}
        }
        let tilejson = null;
        try {
            tilejson = await this.map.loadGazetteerStyle(id);
        } catch (e) {
            console.warn('heroMap.showGazetteer: load failed', id, e);
            return;
        }
        this._currentGazetteer = id;
        this._addContextLayers();
        // Wire hover-cursor + click-popup on the gazetteer's shape layers.
        // baseId rule must match whg_maplibre.loadGazetteerStyle (line ~132):
        // single vector_layer → ``id``; multi-layer → ``${id}__${sourceLayer}``.
        const vectorLayers = (tilejson && tilejson.vector_layers && tilejson.vector_layers.length)
            ? tilejson.vector_layers
            : [{ id }];
        const baseIds = vectorLayers.map((vl) =>
            vectorLayers.length > 1 ? `${id}__${vl.id}` : id
        );
        if (!this._gazetteerInteraction) {
            this._gazetteerInteraction = new GazetteerInteraction(this.map);
        }
        this._gazetteerInteraction.attach(id, baseIds);
        this._addGazetteerLabels(id, vectorLayers, baseIds);
        // Polygon gazetteers carry a `coverage` field (place#140); points don't.
        // Drives the low-zoom "zoom in for detail" hint.
        this._hasCoverage = vectorLayers.some(
            (vl) => vl && vl.fields && Object.prototype.hasOwnProperty.call(vl.fields, 'coverage'));
        this._updateCoverageHint();
        if (tilejson && Array.isArray(tilejson.bounds) && tilejson.bounds.length === 4) {
            this.applyProjectionForBounds(tilejson.bounds);
            try { this.map.fitViewport(tilejson.bounds); } catch (e) {}
        }
    }

    /**
     * Add canonical-name labels for a dynamic gazetteer's own features (Explore
     * mode). The gazetteer tiles carry ``name`` / ``name_local`` / ``name_en``
     * per place; clustered aggregate features carry only a ``point_count``, so
     * filtering those out leaves labels on individual places. Language follows
     * the map's preferred-language setting, mirroring the context place labels.
     * Placed above the gazetteer shape layers (no ``beforeId``) so names sit on
     * top; ``eraseSource`` (called on switch/hide) removes it with the source.
     *
     * @param {string} id — the gazetteer/source id
     * @param {Array}  vectorLayers — tilejson.vector_layers (or [{id}])
     * @param {string[]} baseIds — per-vector-layer base ids (match _fill/_line/_circle)
     */
    _addGazetteerLabels(id, vectorLayers, baseIds) {
        if (!this.map) return;
        const lang = this.map.preferredLanguage;
        const textField = (!lang || lang === 'local')
            ? ['coalesce', ['get', 'name_local'], ['get', 'name'], ['get', 'name_en'], '']
            : ['coalesce', ['get', `name_${lang}`], ['get', 'name_en'], ['get', 'name_local'], ['get', 'name'], ''];
        vectorLayers.forEach((vl, i) => {
            const labelId = `${baseIds[i]}_label`;
            if (this.map.getLayer(labelId)) return;
            try {
                this.map.addLayer({
                    id: labelId,
                    type: 'symbol',
                    source: id,
                    'source-layer': vl.id,
                    minzoom: 8,   // match the point layer — no labels under the heatmap (place#133)
                    // Individual places only — skip clustered aggregate features.
                    filter: ['!', ['has', 'point_count']],
                    layout: {
                        'text-field': textField,
                        'text-font': ['Open Sans Regular'],
                        'text-size': ['interpolate', ['linear'], ['zoom'], 4, 10, 10, 13],
                        'text-anchor': 'top',
                        'text-offset': [0, 0.6],
                        'text-optional': true,
                        'text-allow-overlap': false,
                        'text-padding': 4,
                        'text-max-width': 8,
                    },
                    paint: {
                        'text-color': '#33404d',
                        'text-halo-color': 'rgba(255, 255, 255, 0.9)',
                        'text-halo-width': 1.2,
                    },
                });
            } catch (e) {
                console.warn('heroMap._addGazetteerLabels: failed', labelId, e);
            }
        });
    }

    /**
     * Pick the projection appropriate to the supplied lon/lat bounds.
     * Bounds spanning more than 180° of longitude render badly on a globe,
     * so we force Mercator; narrower bounds switch back to globe unless the
     * user has globally disabled it via the globe control.
     *
     * @param {[number, number, number, number]} bounds — [west, south, east, north]
     */
    applyProjectionForBounds(bounds) {
        if (!this.map || !Array.isArray(bounds) || bounds.length !== 4) return;
        let west = bounds[0], east = bounds[2];
        if (east < west) east += 360;       // antimeridian-spanning
        const lonSpan = east - west;
        const wantsMercator = lonSpan > 180;
        // Demo mode pins the globe; the very first bounds decision (initial
        // context load) also keeps the globe so the map opens in globe view.
        let target;
        if (this._globePinned) {
            target = 'globe';
        } else if (readGlobeDisabled()) {
            target = 'mercator';
        } else if (wantsMercator && this._initialBoundsApplied) {
            target = 'mercator';
        } else {
            target = 'globe';
        }
        this._initialBoundsApplied = true;
        // Mark the upcoming projection change as programmatic so that
        // ``_emitProjectionChange`` doesn't flip the user's stored
        // ``whg.globe_disabled`` preference. Cleared on the next idle —
        // the transition takes longer than a microtask.
        this._programmaticProjection = true;
        try { this.map.setProjection({ type: target }); } catch (e) {}
        this.map.once('idle', () => { this._programmaticProjection = false; });
        // Nudge the projection listeners so the first call doesn't lag.
        setTimeout(() => this._checkProjectionChange(), 50);
    }

    /**
     * Pin (or release) the globe projection. While pinned, the
     * gazetteer-bounds rule never switches to Mercator. Pinning also
     * snaps the map to the globe immediately. Used by the Atlas demo mode
     * so the looping presentation always shows the globe.
     *
     * @param {boolean} pinned
     */
    setGlobePinned(pinned) {
        this._globePinned = !!pinned;
        if (!this.map) return;
        if (pinned && !this.isGlobeMode()) {
            this._programmaticProjection = true;
            try { this.map.setProjection({ type: 'globe' }); } catch (e) {}
            this.map.once('idle', () => { this._programmaticProjection = false; });
            setTimeout(() => this._checkProjectionChange(), 50);
        }
    }

    /**
     * Open the gazetteer place popup for ``pid`` at ``lngLat`` — the
     * programmatic entry point used by the Place List panel (list-row clicks),
     * mirroring an on-map feature click. Reuses the GazetteerInteraction popup
     * so a click and a list selection re-target the same popup. Lazily creates
     * the interaction if a tileset click hasn't wired one yet.
     */
    openPlacePopup(pid, lngLat) {
        if (!this.map || !pid || !lngLat) return;
        if (!this._gazetteerInteraction) {
            this._gazetteerInteraction = new GazetteerInteraction(this.map);
        }
        this._gazetteerInteraction.openPopup(pid, lngLat);
    }

    /** Close the gazetteer place popup, if one is open, leaving map handlers
     *  intact. Used e.g. when a new search is initiated. */
    closePlacePopup() {
        if (this._gazetteerInteraction) this._gazetteerInteraction.closePopup();
    }

    /** Tear down the currently-shown dynamic gazetteer, if any. */
    hideGazetteer() {
        if (!this.map) return;
        if (this._gazetteerInteraction) this._gazetteerInteraction.detach();
        this._removeContextLayers();
        if (this._currentGazetteer) {
            try { this.map.eraseSource(this._currentGazetteer); } catch (e) {}
            this._currentGazetteer = null;
        }
        this._hasCoverage = false;
        this._updateCoverageHint();
    }

    /**
     * Show/hide the coverage-mode hint pill (place#140). Visible only while a
     * polygon gazetteer is shown below the z8 detail crossover — i.e. when the
     * map draws just the dissolved coverage footprint, not the individual
     * boundaries. Created lazily; clicking it zooms past the crossover.
     */
    _updateCoverageHint() {
        if (!this.map) return;
        const COVERAGE_CROSSOVER_ZOOM = 8;   // == whg_maplibre COV_MAXZOOM / POINT_MINZOOM
        const show = this._hasCoverage && this.isExploring()
            && this.map.getZoom() < COVERAGE_CROSSOVER_ZOOM;
        let el = this._coverageHintEl;
        if (!el) {
            if (!show) return;   // don't build the node until first needed
            const container = this.map.getContainer();
            el = document.createElement('div');
            el.id = 'atlas_coverage_hint';
            el.setAttribute('role', 'button');
            el.setAttribute('tabindex', '0');
            el.setAttribute('title', 'Zoom in to reveal individual places');
            el.innerHTML = '<i class="fas fa-search-plus ach-icon" aria-hidden="true"></i>'
                + '<span>Coverage view — zoom in to see individual places</span>';
            const zoomIn = () => {
                try { this.map.easeTo({ zoom: COVERAGE_CROSSOVER_ZOOM + 0.5, duration: 700 }); } catch (e) {}
            };
            el.addEventListener('click', zoomIn);
            el.addEventListener('keydown', (e) => {
                if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); zoomIn(); }
            });
            container.appendChild(el);
            this._coverageHintEl = el;
        }
        el.classList.toggle('visible', show);
    }

    /**
     * Add the context overlay beneath the selected gazetteer:
     * - country borders + low-zoom country labels from ``osm``
     * - settlement points + labels from the GeoNames tileset, tiered by
     *   the ``population`` and ``fcode`` metadata that the indexing
     *   pipeline emits onto each tile feature
     *
     * The country line renders below the gazetteer's fill so it never
     * overpowers the primary data; labels and settlement layers render
     * above the fill so they stay readable on point-bearing gazetteers.
     */
    _addContextLayers() {
        if (!this.map || !this._currentGazetteer) {
            console.debug('heroMap._addContextLayers: skipped — map or currentGazetteer missing',
                {map: !!this.map, currentGazetteer: this._currentGazetteer});
            return;
        }
        if (!this.map.getSource('osm')) {
            console.warn('heroMap._addContextLayers: skipped — no "osm" source in current style',
                {sources: Object.keys(this.map.getStyle().sources || {})});
            return;
        }
        if (this._contextLayerIds.length > 0) {
            console.debug('heroMap._addContextLayers: skipped — already added',
                this._contextLayerIds);
            return;
        }

        // All overlay layers are inserted just before the first symbol
        // layer of the base style — i.e. ABOVE the gazetteer's heatmap
        // and circle layers (also placed at this same insertion point,
        // earlier in the call) so a heatmap doesn't paint over the
        // country borders / settlement points the user is meant to see.
        const symbolBeforeId = this.map.getStyle().layers.find(l => l.type === 'symbol')?.id;
        console.debug('heroMap._addContextLayers: building',
            {gazetteer: this._currentGazetteer, symbolBeforeId});

        // Label-language fallback chain — shared by every label layer
        // (country labels, settlement labels, capital labels). The
        // user's preferred language wins; English and the tileset's
        // local-language toponym are progressive fallbacks. Hoisted to
        // function scope so the capitals block (added after the gn
        // settlement block closes) can reference ``placeTextField``
        // without throwing a ReferenceError.
        const lang = this.map.preferredLanguage;
        const placeTextField = (!lang || lang === 'local')
            ? ['coalesce', ['get', 'name_local'], ['get', 'name'], ['get', 'name_en'], '']
            : ['coalesce',
               ['get', `name_${lang}`],
               ['get', 'name_en'],
               ['get', 'name_local'],
               ['get', 'name'],
               ''];

        // Country borders — sourced from the ``un`` (UN-defined country
        // boundaries) tileset rather than ``osm``: the OSM admin tileset
        // suffers from tippecanoe ``--coalesce-densest-as-needed`` drops
        // at low zooms, which makes country borders fragment on/off as
        // the user zooms. ``un`` is a small, dedicated country-only
        // tileset that renders consistently. ``minzoom: 3`` because the
        // tileset's lowest zooms have features that don't render well at
        // very wide views.
        if (!this.map.getSource(BORDER_OVERLAY_SOURCE)) {
            try {
                this.map.addSource(BORDER_OVERLAY_SOURCE, {
                    type: 'vector',
                    url: `${process.env.TILEBOSS}/data/${BORDER_OVERLAY_TILESET}.json`,
                });
            } catch (e) { /* source may already exist after a partial teardown */ }
        }
        if (this.map.getSource(BORDER_OVERLAY_SOURCE)) {
            try {
                this.map.addLayer({
                    id: CONTEXT_LINE_LAYER,
                    type: 'line',
                    source: BORDER_OVERLAY_SOURCE,
                    'source-layer': BORDER_OVERLAY_TILESET,
                    minzoom: 3,
                    paint: {
                        'line-color': 'rgba(0, 0, 0, 0.32)',
                        'line-width': 0.6,
                        'line-opacity': 0.8,
                    },
                }, symbolBeforeId);
                this._contextLayerIds.push(CONTEXT_LINE_LAYER);
            } catch (e) {
                console.warn('heroMap._addContextLayers: country line failed', e);
            }
        }

        // Country labels are useful only at low zooms where the whole
        // country fits comfortably on screen — once zoomed past ~z6 a
        // single huge centroid label is more clutter than orientation.
        // The text-opacity step fades the label away over z5–z7.
        // Country labels reuse the function-scoped ``placeTextField``
        // declared above (same WHG ``name_<lang>``/``name_local``/
        // ``name`` fallback chain as the settlement / capital labels).
        // Country labels — sourced from the ``un`` tileset (same source
        // as the borders) for the same reason: zoom-stable rendering
        // without the OSM tippecanoe coalesce drop-outs. ``name_en`` is
        // expected to be the only fully-populated language field, but
        // the fallback chain catches ``name_<user>``/``name_local``/
        // ``name`` if any of them happen to be present.
        try {
            this.map.addLayer({
                id: CONTEXT_LABEL_LAYER,
                type: 'symbol',
                source: BORDER_OVERLAY_SOURCE,
                'source-layer': BORDER_OVERLAY_TILESET,
                minzoom: 3,
                layout: {
                    'text-field': placeTextField,
                    'text-font': ['Open Sans Regular'],
                    'text-size': [
                        'interpolate', ['linear'], ['zoom'],
                        2, 10, 5, 12,
                    ],
                    'text-allow-overlap': false,
                    'text-padding': 6,
                    'symbol-placement': 'point',
                },
                paint: {
                    'text-color': 'rgba(60, 60, 60, 0.85)',
                    'text-halo-color': 'rgba(255, 255, 255, 0.8)',
                    'text-halo-width': 1.2,
                    'text-opacity': [
                        'interpolate', ['linear'], ['zoom'],
                        3, 0.85,
                        5, 0.85,
                        7, 0.0,
                    ],
                },
            }, symbolBeforeId);
            this._contextLayerIds.push(CONTEXT_LABEL_LAYER);
        } catch (e) {
            console.warn('heroMap._addContextLayers: country label failed', e);
        }

        // Settlement context — driven by the GeoNames ``fcode`` metadata
        // emitted by the indexing pipeline. ``population`` is unreliable
        // for tiering (most GN points have no value), so we use the GN
        // feature-code hierarchy directly: capitals (PPLC/PPLG/PPLCH) →
        // first-order admin seats (PPLA) → second-order (PPLA2) →
        // third-order (PPLA3) → lower seats / generic populated places.
        // Historical / destroyed / abandoned codes are kept and slotted
        // into the appropriate tier — this is a *historical* gazetteer.
        // The source is added on first use and persists across gazetteer
        // switches so its tile cache is reused.
        if (!this.map.getSource(PLACE_OVERLAY_SOURCE)) {
            try {
                this.map.addSource(PLACE_OVERLAY_SOURCE, {
                    type: 'vector',
                    url: `${process.env.TILEBOSS}/data/${PLACE_OVERLAY_TILESET}.json`,
                });
            } catch (e) { /* source may already exist after a partial teardown */ }
        }
        if (this.map.getSource(PLACE_OVERLAY_SOURCE)) {
            // Filter: PPL-family fcodes EXCEPT the capital classes —
            // those are rendered by the dedicated ``gn_capitals``
            // overlay added below, where they survive at every zoom
            // because that tileset isn't clustered. Including them
            // here too would just waste paint cycles.
            const PPL_FCODES = [
                'PPLA2', 'PPLA3', 'PPLA4', 'PPLA5',
                'PPL', 'PPLH',
                'PPLF', 'PPLL', 'PPLR', 'PPLS', 'PPLX', 'PPLW', 'PPLQ',
            ];
            const settlementFilter = ['match', ['get', 'fcode'],
                PPL_FCODES, true, false,
            ];
            // Per-feature significance tier within the non-capital
            // family (3 = 2nd-order admin seat, 6 = catch-all). Tiers
            // 1 and 2 (capitals, 1st-order seats) live in the capitals
            // overlay so they're not represented here.
            const tierExpr = ['match', ['get', 'fcode'],
                'PPLA2', 3,
                'PPLA3', 4,
                'PPLA4', 5, 'PPLA5', 5, 'PPL', 5, 'PPLH', 5,
                6,
            ];

            // Diagnostic — re-run on each idle (i.e. after tiles for
            // the current viewport have loaded) so the counts reflect
            // what's actually visible. Without the idle listener the
            // first call fires before tiles arrive and returns an empty
            // set every time. The listener is detached in
            // ``_removeContextLayers`` so it doesn't accumulate.
            this._settlementDiagnostic = () => {
                if (!this.map || !this.map.getSource(PLACE_OVERLAY_SOURCE)) return;
                let features;
                try {
                    features = this.map.querySourceFeatures(
                        PLACE_OVERLAY_SOURCE,
                        { sourceLayer: PLACE_OVERLAY_TILESET },
                    );
                } catch (e) { return; }
                if (!features.length) return;
                const z = this.map.getZoom().toFixed(2);

                // Surface the surviving capitals (any fcode in the
                // PPLC-family) so we can see whether they exist at all
                // at this zoom — not just the few I happened to filter
                // by name. ``PPLA`` is also surfaced because in some
                // indexings the country capital is reclassified as the
                // first-order admin seat.
                const capitals = features
                    .filter(f => /^PPL(C|G|CH|A)$/.test(f.properties?.fcode || ''))
                    .map(f => ({
                        name: f.properties?.name,
                        name_local: f.properties?.name_local,
                        fcode: f.properties?.fcode,
                        place_id: f.properties?.place_id,
                        clustered: f.properties?.clustered,
                    }));

                // Anything name-matching Dublin in any case / partial form.
                const dublin = features
                    .filter(f => /dubl|baile.*?cliath/i.test(
                        (f.properties?.name || '') + ' ' + (f.properties?.name_local || '')))
                    .map(f => ({
                        name: f.properties?.name,
                        name_local: f.properties?.name_local,
                        fcode: f.properties?.fcode,
                        place_id: f.properties?.place_id,
                    }));

                const fcodeCounts = features.reduce((acc, f) => {
                    const k = f.properties?.fcode || '(none)';
                    acc[k] = (acc[k] || 0) + 1;
                    return acc;
                }, {});

                console.debug(`settlement @ z${z} — capitals/PPLA in view (${capitals.length})\n` +
                    JSON.stringify(capitals, null, 2));
                console.debug(`settlement @ z${z} — Dublin name match (${dublin.length})\n` +
                    JSON.stringify(dublin, null, 2));
                console.debug(`settlement @ z${z} — full fcode counts ` +
                    JSON.stringify(fcodeCounts));
            };
            this.map.on('idle', this._settlementDiagnostic);
            // ``placeTextField`` is the function-scoped variable
            // declared near the top of _addContextLayers — see the
            // hoisted comment block there for the fallback chain.

            try {
                this.map.addLayer({
                    id: CONTEXT_PLACE_CIRCLE_LAYER,
                    type: 'circle',
                    source: PLACE_OVERLAY_SOURCE,
                    'source-layer': PLACE_OVERLAY_TILESET,
                    minzoom: 2,
                    filter: settlementFilter,
                    paint: {
                        // Marker size shrinks as significance falls.
                        // Tiers 1 and 2 (capitals, 1st-order seats) are
                        // not represented in this layer — see the
                        // capitals overlay for those.
                        'circle-radius': [
                            'step', tierExpr,
                            1.8,        // (default: tier 3 — 2nd-order)
                            4, 1.5,     // tier 4 — 3rd-order
                            5, 1.3,     // tier 5 — lower seats + generic
                            6, 1.1,     // tier 6 — catch-all
                        ],
                        'circle-color': 'rgba(70, 70, 70, 0.7)',
                        'circle-stroke-color': 'rgba(255, 255, 255, 0.85)',
                        'circle-stroke-width': 0.7,
                        'circle-opacity': [
                            'step', ['zoom'],
                            0,
                            4, ['case', ['<=', tierExpr, 3], 0.65, 0],  // 2nd-order at z4+
                            5, ['case', ['<=', tierExpr, 4], 0.6,  0],  // + 3rd-order at z5+
                            6, ['case', ['<=', tierExpr, 5], 0.55, 0],  // + lower seats + generic at z6+
                            8, 0.55,                                    // everything left in at z8+
                        ],
                        'circle-stroke-opacity': [
                            'step', ['zoom'],
                            0,
                            4, ['case', ['<=', tierExpr, 3], 0.8,  0],
                            5, ['case', ['<=', tierExpr, 4], 0.75, 0],
                            6, ['case', ['<=', tierExpr, 5], 0.7,  0],
                            8, 0.7,
                        ],
                    },
                }, symbolBeforeId);
                this._contextLayerIds.push(CONTEXT_PLACE_CIRCLE_LAYER);
            } catch (e) {
                console.warn('heroMap._addContextLayers: place circle failed', e);
            }

            try {
                this.map.addLayer({
                    id: CONTEXT_PLACE_LABEL_LAYER,
                    type: 'symbol',
                    source: PLACE_OVERLAY_SOURCE,
                    'source-layer': PLACE_OVERLAY_TILESET,
                    minzoom: 3,
                    filter: settlementFilter,
                    layout: {
                        'text-field': placeTextField,
                        'text-font': ['Open Sans Regular'],
                        'text-size': [
                            'interpolate', ['linear'], ['zoom'],
                            3, 9, 8, 11,
                        ],
                        'text-anchor': 'top',
                        'text-offset': [0, 0.45],
                        'text-allow-overlap': false,
                        'text-padding': 4,
                    },
                    paint: {
                        'text-color': 'rgba(60, 60, 60, 0.85)',
                        'text-halo-color': 'rgba(255, 255, 255, 0.85)',
                        'text-halo-width': 1.0,
                        // Labels are stricter than the circles — each
                        // tier opens one zoom level later so the map
                        // stays legible at moderate zooms.
                        'text-opacity': [
                            'step', ['zoom'],
                            0,
                            5, ['case', ['<=', tierExpr, 3], 0.8,  0],   // 2nd-order at z5+
                            6, ['case', ['<=', tierExpr, 4], 0.75, 0],   // + 3rd-order at z6+
                            8, ['case', ['<=', tierExpr, 5], 0.7,  0],   // + lower seats + generic at z8+
                        ],
                    },
                }, symbolBeforeId);
                this._contextLayerIds.push(CONTEXT_PLACE_LABEL_LAYER);
            } catch (e) {
                console.warn('heroMap._addContextLayers: place label failed', e);
            }
        }

        // ── Capitals overlay (PPLC/PPLG/PPLCH/PPLA) ──
        // Sourced from the dedicated, non-clustered ``gn_capitals``
        // tileset. Because clustering doesn't run on this tileset every
        // capital survives at every zoom; we tier visibility by zoom
        // here purely so the world map at z2 isn't a forest of dots.
        if (!this.map.getSource(CAPITALS_OVERLAY_SOURCE)) {
            try {
                this.map.addSource(CAPITALS_OVERLAY_SOURCE, {
                    type: 'vector',
                    url: `${process.env.TILEBOSS}/data/${CAPITALS_OVERLAY_TILESET}.json`,
                });
            } catch (e) { /* may already exist after a partial teardown */ }
        }
        if (this.map.getSource(CAPITALS_OVERLAY_SOURCE)) {
            // Defensive filter — the tileset is *meant* to be pre-
            // filtered to PPLC/PPLG/PPLA by the indexing pipeline, but
            // until the regenerated mbtiles is deployed the existing
            // gn_capitals.mbtiles still includes ``PPLCH``. Filtering
            // here keeps historical-capital noise out either way.
            const capitalFilter = ['match', ['get', 'fcode'],
                CAPITAL_FCODES, true, false,
            ];
            // Size differential: PPLC/PPLG slightly larger than PPLA so
            // national capitals read more strongly than first-order
            // admin seats. *Visibility* is no longer tiered by zoom —
            // the gn_capitals tileset is small enough that every
            // feature can render at every zoom from 0 upwards, which
            // is the whole reason we built a separate non-clustered
            // tileset for it.
            const capitalSizeTier = ['match', ['get', 'fcode'],
                'PPLC', 1, 'PPLG', 1,
                'PPLA', 2,
                2,
            ];
            try {
                this.map.addLayer({
                    id: CONTEXT_CAPITAL_CIRCLE_LAYER,
                    type: 'circle',
                    source: CAPITALS_OVERLAY_SOURCE,
                    'source-layer': CAPITALS_OVERLAY_TILESET,
                    filter: capitalFilter,
                    paint: {
                        'circle-radius': [
                            'step', capitalSizeTier,
                            2.8,         // tier 1 — national capitals
                            2, 2.3,      // tier 2 — first-order admin seats
                        ],
                        'circle-color': 'rgba(60, 60, 60, 0.85)',
                        'circle-stroke-color': 'rgba(255, 255, 255, 0.95)',
                        'circle-stroke-width': 0.9,
                        'circle-opacity': 0.85,
                        'circle-stroke-opacity': 0.95,
                    },
                }, symbolBeforeId);
                this._contextLayerIds.push(CONTEXT_CAPITAL_CIRCLE_LAYER);
            } catch (e) {
                console.warn('heroMap._addContextLayers: capital circle failed', e);
            }
            try {
                this.map.addLayer({
                    id: CONTEXT_CAPITAL_LABEL_LAYER,
                    type: 'symbol',
                    source: CAPITALS_OVERLAY_SOURCE,
                    'source-layer': CAPITALS_OVERLAY_TILESET,
                    filter: capitalFilter,
                    layout: {
                        'text-field': placeTextField,
                        'text-font': ['Open Sans Semibold'],
                        'text-size': [
                            'interpolate', ['linear'], ['zoom'],
                            2, 10, 6, 12, 10, 14,
                        ],
                        'text-anchor': 'top',
                        'text-offset': [0, 0.55],
                        'text-allow-overlap': false,
                        'text-padding': 6,
                    },
                    paint: {
                        'text-color': 'rgba(20, 20, 20, 0.95)',
                        'text-halo-color': 'rgba(255, 255, 255, 0.95)',
                        'text-halo-width': 1.4,
                        'text-opacity': 0.95,
                    },
                });
                this._contextLayerIds.push(CONTEXT_CAPITAL_LABEL_LAYER);
            } catch (e) {
                console.warn('heroMap._addContextLayers: capital label failed', e);
            }
        }

        // Diagnostic snapshot — layer ids actually present in the style
        // and their visibility, so we can see whether the layers are
        // missing, shadowed, or rendering with no features.
        const allLayers = this.map.getStyle().layers;
        const overlayState = this._contextLayerIds.map(id => {
            const layer = allLayers.find(l => l.id === id);
            const idx = layer ? allLayers.indexOf(layer) : -1;
            return {
                id,
                present: !!layer,
                index: idx,
                visibility: layer?.layout?.visibility ?? '(default visible)',
                source: layer?.source,
                'source-layer': layer?.['source-layer'],
            };
        });
        console.debug('heroMap._addContextLayers: post-add state\n' +
            JSON.stringify(overlayState, null, 2));
        // Also log the next-up layers above each overlay layer in z-order,
        // so we can see whether a heatmap or fill might be obscuring them.
        const above = this._contextLayerIds.flatMap(id => {
            const idx = allLayers.findIndex(l => l.id === id);
            if (idx < 0) return [];
            return allLayers.slice(idx + 1, idx + 4).map(l => ({
                aboveOf: id, neighbour: l.id, type: l.type, visibility: l.layout?.visibility ?? 'visible'
            }));
        });
        console.debug('heroMap._addContextLayers: layers stacked above\n' +
            JSON.stringify(above, null, 2));
    }

    /** Remove the context layers. */
    _removeContextLayers() {
        if (!this.map) return;
        if (this._settlementDiagnostic) {
            try { this.map.off('idle', this._settlementDiagnostic); } catch (e) {}
            this._settlementDiagnostic = null;
        }
        for (const layerId of this._contextLayerIds) {
            try {
                if (this.map.getLayer(layerId)) this.map.removeLayer(layerId);
            } catch (e) {}
        }
        this._contextLayerIds = [];
    }

    /**
     * Set which boundary layer sources are active/visible.
     * Currently only supports osm/ohm (existing boundaries tile source).
     * Future sources (periodo, cliopatria, etc.) will add additional tile sources.
     */
    setActiveSources(sources) {
        // For now, this just controls the namespace filter applied to boundary layers
        // When additional tile sources exist, this will show/hide their layer groups
        console.log('heroMap.setActiveSources:', sources);
    }

    /**
     * Ensure the whg-context style is active (used when entering area search mode).
     * Re-initialises boundary layers if they were lost during a style switch.
     */
    ensureContextStyle() {
        if (!this.map) return;
        // Re-init the boundary layers only if they're genuinely absent from the
        // current style. Normally transformStyle carries them across a basemap
        // swap (with their live show/hide + filter state), so re-initing would
        // wrongly re-hide them and re-bind duplicate hover/click handlers.
        const present = this._boundaryLayerIds.length > 0
            && this._boundaryLayerIds.every(id => this.map.getLayer(id));
        if (!present) {
            this._boundaryLayerIds = [];
            this._initBoundaryLayers();
        }
    }

    /** The active basemap style id (e.g. 'whg-context', 'OSM'). */
    getBasemapStyle() { return this._currentBasemap; }

    /** True while a dynamic gazetteer tileset is shown (Explore mode). */
    isExploring() { return !!this._currentGazetteer; }

    /**
     * Swap the basemap to another WHG tileserver style WITHOUT losing the
     * Atlas overlays. Uses the same overlay-preserving ``transformStyle`` as
     * whg_maplibre's acmeStyleControl: every source/layer that isn't part of
     * the base style (i.e. everything the Atlas added — result/overlay/context/
     * gazetteer/boundary layers) is re-grafted onto the new basemap in one
     * frame, so nothing flashes out. Projection, camera and preferred-language
     * labels are re-applied after the new style loads.
     *
     * @param {string} styleId — a tileserver style id from ${TILEBOSS}/styles/<id>
     * @returns {Promise}
     */
    setBasemapStyle(styleId) {
        if (!this.map || !styleId || styleId === this._currentBasemap) {
            return Promise.resolve();
        }
        const url = `${process.env.TILEBOSS}/styles/${styleId}/style.json`;
        const wasGlobe = this.isGlobeMode();
        return fetch(url)
            .then(r => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); })
            .then(styleJSON => {
                this.map.setStyle(styleJSON, {
                    diff: false,   // native diff can't handle a full basemap swap
                    transformStyle: (previousStyle, nextStyle) => {
                        // Keep every source/layer the app added on top of the
                        // OLD base (those not tracked in map.baseStyle) — PLUS the
                        // Areas>Regions boundary sources/layers. Those are baked
                        // into the whg-context base style (so map.baseStyle counts
                        // them as "base"), yet they must survive a basemap swap or
                        // Areas>Regions shows no polygons on OSM/Satellite/etc.
                        // Dedupe against nextStyle so switching *back* to a basemap
                        // that already defines them doesn't create duplicate ids.
                        const isBoundary = (sourceLayerOrId) =>
                            BOUNDARY_SOURCE_LAYERS.includes(sourceLayerOrId);
                        const nextSourceIds = new Set(Object.keys(nextStyle.sources));
                        const nextLayerIds = new Set(nextStyle.layers.map(l => l.id));

                        const sources = { ...nextStyle.sources };
                        Object.keys(previousStyle.sources).forEach(k => {
                            if (nextSourceIds.has(k)) return;
                            if (!this.map.baseStyle.sources.includes(k) || isBoundary(k)) {
                                sources[k] = previousStyle.sources[k];
                            }
                        });
                        // Boundary layers carry LIVE state (show/hide, the active
                        // admin-level filter, hover/selected paint), so always take
                        // them from the previous style and drop the next style's
                        // fresh copies — an active Areas>Regions selection then
                        // survives a basemap switch in BOTH directions.
                        const prevBoundaryIds = new Set(previousStyle.layers
                            .filter(l => isBoundary(l['source-layer'])).map(l => l.id));
                        const layers = [
                            ...nextStyle.layers.filter(l => !prevBoundaryIds.has(l.id)),
                            ...previousStyle.layers.filter(l => {
                                if (isBoundary(l['source-layer'])) return true;   // carry boundaries + state
                                if (nextLayerIds.has(l.id)) return false;         // dedupe overlays
                                return !this.map.baseStyle.layers.includes(l.id); // app-added overlays
                            }),
                        ];
                        // The new style becomes the base for the next swap.
                        this.map.baseStyle.sources = Object.keys(nextStyle.sources);
                        this.map.baseStyle.layers = nextStyle.layers.map(l => l.id);
                        return { ...nextStyle, sources, layers };
                    },
                });
                this._currentBasemap = styleId;
                this.map.once('style.load', () => {
                    this._applyLanguageToBaseLabels();
                    // setStyle wipes the image atlas, so the re-grafted place#140
                    // coverage `fill-pattern` layers now point at missing sprites —
                    // re-register them or the mottle silently vanishes on swap.
                    try { this.map.ensureCoverageSprites(); } catch (e) {}
                    // A basemap can carry its own projection default — restore ours.
                    this._programmaticProjection = true;
                    try { this.map.setProjection({ type: wasGlobe ? 'globe' : 'mercator' }); } catch (e) {}
                    this.map.once('idle', () => { this._programmaticProjection = false; });
                    // The boundary layers are now carried across the swap by
                    // transformStyle WITH their live state, so we must NOT reset +
                    // rescan them here — _initBoundaryLayers would re-hide them and
                    // re-bind duplicate handlers. Only (re)build them if they are
                    // genuinely absent (e.g. a first load that raced the swap).
                    this.ensureContextStyle();
                });
            })
            .catch(e => { console.warn('heroMap.setBasemapStyle failed', styleId, e); });
    }

    /** Re-apply the map's preferred-language coalesce to the base style's
     *  OpenMapTiles label layers (mirrors whg_maplibre's on-load logic) so a
     *  freshly-swapped basemap honours the language preference. */
    _applyLanguageToBaseLabels() {
        if (!this.map) return;
        const lang = this.map.preferredLanguage || 'local';
        const style = this.map.getStyle();
        (style.layers || []).forEach(layer => {
            if (layer.layout && layer.layout['text-field'] && layer.source === 'openmaptiles') {
                const textField = lang === 'local'
                    ? ['coalesce', ['get', 'name:local'], ['get', 'name'], ['get', 'name:en']]
                    : ['coalesce', ['get', `name:${lang}`], ['get', 'name'], ['get', 'name:en']];
                try { this.map.setLayoutProperty(layer.id, 'text-field', textField); } catch (e) {}
            }
        });
    }

    // ── Projection detection ──

    /**
     * Check whether the map is currently in globe projection.
     * @returns {boolean}
     */
    isGlobeMode() {
        if (!this.map) return this._isGlobe;
        try {
            if (typeof this.map.getProjection === 'function') {
                const proj = this.map.getProjection();
                if (proj) {
                    if (proj.type === 'mercator') return false;
                    if (proj.type === 'globe') return true;
                }
            }
        } catch (e) { /* API may not exist */ }
        return this._isGlobe;
    }

    /**
     * Register a callback for projection changes (globe ↔ flat).
     * @param {function(boolean)} callback — receives `true` if globe, `false` if flat
     * @returns {function} unsubscribe function
     */
    onProjectionChange(callback) {
        this._projectionChangeListeners.push(callback);
        return () => {
            this._projectionChangeListeners =
                this._projectionChangeListeners.filter(fn => fn !== callback);
        };
    }

    /** @private Wire up globe control button click detection. */
    _wireProjectionDetection() {
        // Check on every moveend (catches programmatic changes)
        this.map.on('moveend', () => this._checkProjectionChange());
        this.map.on('idle', () => this._checkProjectionChange());

        // The persistent "disable globe globally" preference is driven by
        // ``_emitProjectionChange`` (below) — it persists whenever the
        // projection actually changes AND the change wasn't programmatic.
    }

    /** @private Check if projection changed and notify listeners. */
    _checkProjectionChange() {
        const nowGlobe = this.isGlobeMode();
        if (nowGlobe !== this._isGlobe) {
            this._isGlobe = nowGlobe;
            this._emitProjectionChange(nowGlobe);
        }
    }

    /** @private */
    _emitProjectionChange(isGlobe) {
        // Persist the preference whenever the user (not the gazetteer-
        // bounds rule) drives the change. ``_programmaticProjection`` is
        // set by ``applyProjectionForBounds`` for its window of work.
        if (!this._programmaticProjection) {
            writeGlobeDisabled(!isGlobe);
        }
        this._projectionChangeListeners.forEach(fn => {
            try { fn(isGlobe); } catch (e) { console.error('projection listener error', e); }
        });
    }

    _emitViewportChange(bbox) {
        this._viewportListeners.forEach(fn => {
            try { fn(bbox); } catch (e) { console.error('viewport listener error', e); }
        });
    }
}

const heroMap = new HeroMap();
export default heroMap;

