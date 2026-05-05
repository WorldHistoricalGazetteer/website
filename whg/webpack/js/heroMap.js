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

const OVERLAY_SOURCE = 'filter-overlay';
const OVERLAY_FILL = 'filter-overlay-fill';
const OVERLAY_LINE = 'filter-overlay-line';

const SUGGESTION_SOURCE = 'suggestion-markers';
const SUGGESTION_CIRCLES = 'suggestion-circles';
const SUGGESTION_LABELS = 'suggestion-labels';

// Boundary source-layer names in the whg-context style. Each is also
// the source ID for its tileset. See contextMap.js for full schema notes.
const BOUNDARY_SOURCE_LAYERS = ['osm_admin', 'ohm_admin', 'osm_misc', 'po', 'clio', 'nl'];

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

// Context layer (faint OSM admin boundaries shown beneath a selected
// gazetteer in Explore mode). The thresholds pick which admin levels are
// visible at each zoom; chosen to keep ~3 levels in view at any zoom so
// the user always sees *some* context without crowding the display.
const CONTEXT_ZOOM_THRESHOLDS = [
    { maxZoom: 2.5, levels: ['0', '1', '2'] },
    { maxZoom: 4.0, levels: ['1', '2', '3'] },
    { maxZoom: 6.0, levels: ['2', '3', '4'] },
    { maxZoom: 8.0, levels: ['3', '4', '5'] },
    { maxZoom: 10.0, levels: ['4', '5', '6'] },
    { maxZoom: Infinity, levels: ['5', '6', '7', '8'] },
];

function adminLevelsForZoom(zoom) {
    for (const t of CONTEXT_ZOOM_THRESHOLDS) {
        if (zoom < t.maxZoom) return t.levels;
    }
    return CONTEXT_ZOOM_THRESHOLDS[CONTEXT_ZOOM_THRESHOLDS.length - 1].levels;
}

// Layer IDs use a neutral ``_atlas_overlay_`` prefix rather than
// ``_atlas_context_``: the global error handler in whg_maplibre.js
// classifies any error message containing the substring "context" as a
// WebGL-context fatality, which silently shows a fatal modal whenever
// MapLibre reports a layer/expression error mentioning the layer id.
const CONTEXT_LINE_LAYER = '_atlas_overlay_admin_line';
const CONTEXT_LABEL_LAYER = '_atlas_overlay_admin_label';
const CONTEXT_PLACE_CIRCLE_LAYER = '_atlas_overlay_place_circle';
const CONTEXT_PLACE_LABEL_LAYER = '_atlas_overlay_place_label';

// The base style ships a stripped Natural Earth source (``whg-ne-basic``,
// exposed as ``natural_earth``) that holds only water/ice/rivers/lakes —
// no place labels. To draw town/city points we add the full
// ``natural-earth-vector`` tileset on demand, the first time context
// layers are built. The source id is namespaced so it can't clash with
// anything in the base style.
const PLACE_OVERLAY_SOURCE = '_atlas_overlay_place_source';
const PLACE_OVERLAY_TILESET = 'natural-earth-vector';

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
        this._hoverTooltip = null;
        this._currentSource = null;
        this._currentGazetteer = null;
        this._contextLayerIds = [];
        this._contextZoomListener = null;
        this._hovered = null;
        this._selected = null;
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
                maxZoom: 14,
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
        for (const layerId of this._boundaryLayerIds) {
            const def = style.layers.find(l => l.id === layerId);
            this._originalFilters[layerId] = def?.filter
                ? JSON.parse(JSON.stringify(def.filter)) : null;
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

    _onBoundaryClick(e, sourceId) {
        if (!e.features || e.features.length === 0) return;
        const feature = e.features[0];
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
            return { type: 'FeatureCollection', features };
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

    hideBoundaries() {
        if (!this.map || this._boundaryLayerIds.length === 0) return;
        for (const layerId of this._boundaryLayerIds) {
            try {
                this.map.setFilter(layerId, this._originalFilters[layerId] || null);
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
            try { this.map.eraseSource(this._currentGazetteer); } catch (e) {}
            this._currentGazetteer = null;
        }
        // Always rebuild context layers so their z-order stays correct
        // relative to the freshly-loaded gazetteer fill/line/circle.
        this._removeContextLayers();
        let tilejson = null;
        try {
            tilejson = await this.map.loadGazetteerStyle(id);
        } catch (e) {
            console.warn('heroMap.showGazetteer: load failed', id, e);
            return;
        }
        this._currentGazetteer = id;
        this._addContextLayers();
        if (tilejson && Array.isArray(tilejson.bounds) && tilejson.bounds.length === 4) {
            this.applyProjectionForBounds(tilejson.bounds);
            try { this.map.fitViewport(tilejson.bounds); } catch (e) {}
        }
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
        const target = wantsMercator
            ? 'mercator'
            : (readGlobeDisabled() ? 'mercator' : 'globe');
        try { this.map.setProjection({ type: target }); } catch (e) {}
        // Sync internal state — programmatic setProjection doesn't fire the
        // user-click path, but moveend/idle in _wireProjectionDetection will
        // catch up. Nudge it explicitly so the first listener call doesn't lag.
        setTimeout(() => this._checkProjectionChange(), 50);
    }

    /** Tear down the currently-shown dynamic gazetteer, if any. */
    hideGazetteer() {
        if (!this.map) return;
        this._removeContextLayers();
        if (this._currentGazetteer) {
            try { this.map.eraseSource(this._currentGazetteer); } catch (e) {}
            this._currentGazetteer = null;
        }
    }

    /**
     * Add a faint OSM-admin context layer beneath the selected gazetteer.
     * Driven by the existing ``osm_admin`` source from the base style — no
     * extra tileset is loaded. The line layer renders below the gazetteer's
     * fill so it never overpowers the primary data; labels render above the
     * fill so they stay readable on point-bearing gazetteers.
     *
     * The visible admin levels switch on zoom via ``adminLevelsForZoom``.
     */
    _addContextLayers() {
        if (!this.map || !this._currentGazetteer) return;
        if (!this.map.getSource('osm_admin')) return;          // base style missing source
        if (this._contextLayerIds.length > 0) return;          // already added

        const fillLayerId = `${this._currentGazetteer}_fill`;
        const fillExists = !!this.map.getLayer(fillLayerId);
        const symbolBeforeId = this.map.getStyle().layers.find(l => l.type === 'symbol')?.id;

        const initialLevels = adminLevelsForZoom(this.map.getZoom());
        const levelFilter = ['match', ['get', 'boundary'], initialLevels, true, false];

        try {
            this.map.addLayer({
                id: CONTEXT_LINE_LAYER,
                type: 'line',
                source: 'osm_admin',
                'source-layer': 'osm_admin',
                paint: {
                    'line-color': 'rgba(0, 0, 0, 0.18)',
                    'line-width': 0.6,
                    'line-opacity': 0.7,
                },
                filter: levelFilter,
            }, fillExists ? fillLayerId : symbolBeforeId);
            this._contextLayerIds.push(CONTEXT_LINE_LAYER);
        } catch (e) { /* layer may already exist after a partial teardown */ }

        try {
            this.map.addLayer({
                id: CONTEXT_LABEL_LAYER,
                type: 'symbol',
                source: 'osm_admin',
                'source-layer': 'osm_admin',
                layout: {
                    'text-field': ['coalesce', ['get', 'name'], ['get', 'name:en'], ''],
                    'text-font': ['Open Sans Regular'],
                    'text-size': 10,
                    'text-allow-overlap': false,
                    'symbol-placement': 'point',
                },
                paint: {
                    'text-color': 'rgba(60, 60, 60, 0.75)',
                    'text-halo-color': 'rgba(255, 255, 255, 0.7)',
                    'text-halo-width': 1.0,
                },
                filter: levelFilter,
            }, symbolBeforeId);
            this._contextLayerIds.push(CONTEXT_LABEL_LAYER);
        } catch (e) { /* same as above */ }

        // Settlement context — Natural Earth's place_label layer. The base
        // style's ``natural_earth`` source is a stripped subset; the full
        // tileset is added here on first use. The ``scalerank`` field gives
        // a 0–10 city-importance ordering (0 = megacity); we filter
        // generously and tier visibility through step-by-zoom radius/opacity
        // so low zooms show only the largest cities and smaller towns join
        // progressively as the user zooms in.
        if (!this.map.getSource(PLACE_OVERLAY_SOURCE)) {
            try {
                this.map.addSource(PLACE_OVERLAY_SOURCE, {
                    type: 'vector',
                    url: `${process.env.TILEBOSS}/data/${PLACE_OVERLAY_TILESET}.json`,
                });
            } catch (e) { /* source may already exist after a partial teardown */ }
        }
        if (this.map.getSource(PLACE_OVERLAY_SOURCE)) {
            try {
                this.map.addLayer({
                    id: CONTEXT_PLACE_CIRCLE_LAYER,
                    type: 'circle',
                    source: PLACE_OVERLAY_SOURCE,
                    'source-layer': 'place_label',
                    minzoom: 2,
                    filter: ['<=', ['to-number', ['get', 'scalerank']], 8],
                    paint: {
                        'circle-radius': [
                            'step', ['to-number', ['get', 'scalerank']],
                            2.4,    // scalerank 0–2: major cities
                            3, 2.0, // 3–5: cities
                            6, 1.4, // 6+: towns
                        ],
                        'circle-color': 'rgba(70, 70, 70, 0.7)',
                        'circle-stroke-color': 'rgba(255, 255, 255, 0.85)',
                        'circle-stroke-width': 0.7,
                        'circle-opacity': [
                            'step', ['zoom'],
                            0,
                            3, ['case', ['<=', ['to-number', ['get', 'scalerank']], 2], 0.7, 0],
                            5, ['case', ['<=', ['to-number', ['get', 'scalerank']], 5], 0.65, 0],
                            7, 0.6,
                        ],
                        'circle-stroke-opacity': [
                            'step', ['zoom'],
                            0,
                            3, ['case', ['<=', ['to-number', ['get', 'scalerank']], 2], 0.85, 0],
                            5, ['case', ['<=', ['to-number', ['get', 'scalerank']], 5], 0.8, 0],
                            7, 0.75,
                        ],
                    },
                }, fillExists ? fillLayerId : symbolBeforeId);
                this._contextLayerIds.push(CONTEXT_PLACE_CIRCLE_LAYER);
            } catch (e) { /* same as above */ }

            try {
                this.map.addLayer({
                    id: CONTEXT_PLACE_LABEL_LAYER,
                    type: 'symbol',
                    source: PLACE_OVERLAY_SOURCE,
                    'source-layer': 'place_label',
                    minzoom: 3,
                    filter: ['<=', ['to-number', ['get', 'scalerank']], 6],
                    layout: {
                        'text-field': ['coalesce', ['get', 'name'], ''],
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
                        'text-opacity': [
                            'step', ['zoom'],
                            0,
                            4, ['case', ['<=', ['to-number', ['get', 'scalerank']], 2], 0.85, 0],
                            6, ['case', ['<=', ['to-number', ['get', 'scalerank']], 4], 0.8, 0],
                            8, 0.75,
                        ],
                    },
                }, symbolBeforeId);
                this._contextLayerIds.push(CONTEXT_PLACE_LABEL_LAYER);
            } catch (e) { /* same as above */ }
        }

        // Keep the visible admin levels in step with zoom — but apply the
        // boundary filter only to the admin-source layers. The place
        // (Natural Earth) layers filter by ``scalerank``, set once at
        // creation and tiered via paint expressions.
        this._contextZoomListener = () => {
            if (!this.map) return;
            const levels = adminLevelsForZoom(this.map.getZoom());
            const filter = ['match', ['get', 'boundary'], levels, true, false];
            for (const layerId of [CONTEXT_LINE_LAYER, CONTEXT_LABEL_LAYER]) {
                if (this._contextLayerIds.includes(layerId)) {
                    try { this.map.setFilter(layerId, filter); } catch (e) {}
                }
            }
        };
        this.map.on('zoomend', this._contextZoomListener);
    }

    /** Remove the context layers and detach the zoom listener. */
    _removeContextLayers() {
        if (!this.map) return;
        if (this._contextZoomListener) {
            try { this.map.off('zoomend', this._contextZoomListener); } catch (e) {}
            this._contextZoomListener = null;
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
        // Re-scan boundary layers in case style was switched
        if (this._boundaryLayerIds.length === 0) {
            this._initBoundaryLayers();
        }
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

        // Also listen for the globe control button click for faster response.
        // A user-driven toggle here is the canonical signal for the persistent
        // "disable globe globally" preference: if the click leaves the map in
        // Mercator the user wants Mercator by default; in globe → globe default.
        const container = this.map.getContainer();
        container.addEventListener('click', (e) => {
            if (e.target.closest('.maplibregl-ctrl-globe')) {
                const persist = () => {
                    this._checkProjectionChange();
                    writeGlobeDisabled(!this.isGlobeMode());
                };
                setTimeout(persist, 200);
                setTimeout(persist, 500);
            }
        });
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

