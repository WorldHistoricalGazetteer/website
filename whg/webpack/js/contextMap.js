// /whg/webpack/js/contextMap.js
/**
 * Context map manager for the WHG v3.5 search filters.
 *
 * Wraps a single MapLibre instance in the #context_map container.
 * Provides overlay management for geometry previews (one at a time)
 * and exposes the viewport bounding box.
 */

import filterState from './filterState';

const OVERLAY_SOURCE = 'filter-overlay';
const OVERLAY_FILL = 'filter-overlay-fill';
const OVERLAY_LINE = 'filter-overlay-line';

const SUGGESTION_SOURCE = 'suggestion-markers';
const SUGGESTION_CIRCLES = 'suggestion-circles';
const SUGGESTION_LABELS = 'suggestion-labels';

/* ── Admin boundaries from the whg-context style ──
 * The whg-context style splits boundaries across multiple vector
 * sources, each with a same-named source-layer (post tileset rename
 * the OSM/OHM sources are bare ``osm`` / ``ohm`` rather than
 * ``osm_admin`` / ``ohm_admin``):
 *
 *   osm / ohm — OSM/OHM admin boundaries; per-tier line and label
 *     layers plus a transparent fill we use for hover/click.
 *     Feature-property `boundary` is a string admin_level "0"–"11".
 *   osm_misc — miscellaneous OSM/OHM boundary types (parishes,
 *     historical, regions, …); fill layers per category. `boundary`
 *     is the tag value (e.g. "civil_parish", "historic_county").
 *   po / clio / nl — PeriodO, Cliopatria, NativeLand polygon datasets.
 *     `boundary` is "period" / "polity" / "native".
 *
 * We discover layers per source at load time, hide them, and reveal
 * them on demand via showBoundaries({source, boundaryValues}).
 */
const BOUNDARY_SOURCE_LAYERS = ['osm', 'ohm', 'osm_misc', 'po', 'clio', 'nl'];

/**
 * Decide whether a per-layer static filter on the `boundary` field
 * intersects a list of requested values. Returns true iff the layer's
 * filter could match at least one of the requested values.
 *
 * Recognises the two filter shapes used in whg-context:
 *   ['match', ['get', 'boundary'], [v1, v2, ...], true, false]
 *   ['==',    ['get', 'boundary'], 'v1']
 * Anything else (including `null`) is conservatively treated as
 * "matches everything" so unfamiliar layers don't get hidden by
 * accident. If `requestedValues` is null/empty, the layer is treated
 * as in-scope (no narrowing requested).
 */
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

class ContextMap {
    constructor() {
        this.map = null;
        this._viewportListeners = [];
        this._ready = false;
        this._readyPromise = null;
        this._spinning = false;
        this._spinRAF = null;
        this._spinStopped = false; // Once stopped, never auto-restart
        this._layersBySource = {};     // sourceId -> [layerId, ...]
        this._fillBySource = {};       // sourceId -> fillLayerId (one per source, if any)
        this._boundaryLayerIds = [];   // flat list across all sources
        this._originalFilters = {};    // Snapshot of each boundary layer's original filter
        this._hoverTooltip = null;     // Floating tooltip element for hovered boundaries
        this._currentSource = null;    // The boundary source currently shown (e.g. 'osm')
        this._hovered = null;          // {source, id} of currently hovered feature
        this._selected = null;         // {source, id} of currently selected feature
    }

    /**
     * Initialise the map once the DOM container exists.
     * Returns a promise that resolves when the map is loaded.
     */
    init() {
        if (this._readyPromise) return this._readyPromise;

        this._readyPromise = new Promise((resolve) => {
            this.map = new whg_maplibre.Map({
                container: 'context_map',
                maxZoom: 14,
                style: ['whg-context'],
                fullscreenControl: false,
                downloadMapControl: false,
                drawingControl: false,
                temporalControl: false,
                navigationControl: true,
                globeControl: true,
                globeMode: true,
            });

            this.map.on('load', () => {
                // Add empty overlay source + layers
                this.map.addSource(OVERLAY_SOURCE, {
                    type: 'geojson',
                    data: { type: 'FeatureCollection', features: [] },
                });

                this.map.addLayer({
                    id: OVERLAY_FILL,
                    type: 'fill',
                    source: OVERLAY_SOURCE,
                    paint: {
                        'fill-color': '#4a90d9',
                        'fill-opacity': 0.15,
                    },
                });

                this.map.addLayer({
                    id: OVERLAY_LINE,
                    type: 'line',
                    source: OVERLAY_SOURCE,
                    paint: {
                        'line-color': '#2563eb',
                        'line-width': 2,
                        'line-opacity': 0.7,
                    },
                });

                // Add suggestion markers source + layers (for region selector map suggestions)
                this.map.addSource(SUGGESTION_SOURCE, {
                    type: 'geojson',
                    data: { type: 'FeatureCollection', features: [] },
                });

                this.map.addLayer({
                    id: SUGGESTION_CIRCLES,
                    type: 'circle',
                    source: SUGGESTION_SOURCE,
                    paint: {
                        'circle-radius': 7,
                        'circle-color': '#e04040',
                        'circle-stroke-color': '#fff',
                        'circle-stroke-width': 2,
                        'circle-opacity': 0.85,
                    },
                });

                this.map.addLayer({
                    id: SUGGESTION_LABELS,
                    type: 'symbol',
                    source: SUGGESTION_SOURCE,
                    layout: {
                        'text-field': ['get', 'label'],
                        'text-font': ['Open Sans Semibold'],
                        'text-size': 11,
                        'text-offset': [0, 1.4],
                        'text-anchor': 'top',
                        'text-allow-overlap': true,
                    },
                    paint: {
                        'text-color': '#333',
                        'text-halo-color': '#fff',
                        'text-halo-width': 1.5,
                    },
                });

                // Click handler on suggestion circles: dispatch custom event
                this.map.on('click', SUGGESTION_CIRCLES, (e) => {
                    if (e.features && e.features.length > 0) {
                        const props = e.features[0].properties;
                        const detail = typeof props === 'string' ? JSON.parse(props) : props;
                        document.dispatchEvent(new CustomEvent('suggestion-click', { detail }));
                    }
                });

                // Cursor change on hover over suggestions
                this.map.on('mouseenter', SUGGESTION_CIRCLES, () => {
                    this.map.getCanvas().style.cursor = 'pointer';
                });
                this.map.on('mouseleave', SUGGESTION_CIRCLES, () => {
                    this.map.getCanvas().style.cursor = '';
                });

                // ── Discover boundary layers from the whg-context style ──
                this._initBoundaryLayers();

                // Update bbox on viewport change
                this.map.on('moveend', () => {
                    const bounds = this.map.getBounds();
                    const bbox = [
                        bounds.getWest(),
                        bounds.getSouth(),
                        bounds.getEast(),
                        bounds.getNorth(),
                    ];
                    filterState.set('spatial.bbox', bbox);
                    this._emitViewportChange(bbox);
                });

                // Wire user-interaction events to stop the spin
                this._wireSpinStop();

                this._ready = true;
                resolve(this.map);
            });
        });

        return this._readyPromise;
    }

    /** Get the current viewport bounding box. */
    getBBox() {
        if (!this.map) return null;
        const bounds = this.map.getBounds();
        return [
            bounds.getWest(),
            bounds.getSouth(),
            bounds.getEast(),
            bounds.getNorth(),
        ];
    }

    /** Display a GeoJSON geometry as the overlay preview. */
    setOverlay(geojson) {
        if (!this.map) return;
        const data = geojson.type === 'Feature' ? geojson
            : geojson.type === 'FeatureCollection' ? geojson
            : { type: 'Feature', geometry: geojson, properties: {} };
        this.map.getSource(OVERLAY_SOURCE).setData(data);
    }

    /** Clear the overlay geometry. */
    clearOverlay() {
        if (!this.map) return;
        this.map.getSource(OVERLAY_SOURCE).setData({
            type: 'FeatureCollection',
            features: [],
        });
    }

    /**
     * Display suggestion point markers on the map.
     * @param {GeoJSON FeatureCollection} fc - features with `label` property
     */
    setSuggestions(fc) {
        if (!this.map) return;
        try {
            this.map.getSource(SUGGESTION_SOURCE).setData(fc);
        } catch (e) {
            console.warn('contextMap.setSuggestions: source not ready', e);
        }
    }

    /** Remove all suggestion markers from the map. */
    clearSuggestions() {
        if (!this.map) return;
        try {
            this.map.getSource(SUGGESTION_SOURCE).setData({
                type: 'FeatureCollection',
                features: [],
            });
        } catch (e) { /* source not yet added */ }
    }

    /** Fly the map to fit a GeoJSON geometry. */
    fitTo(geojson) {
        if (!this.map) return;
        try {
            this.map.fitViewport(bbox(geojson));
        } catch (e) {
            console.warn('contextMap.fitTo: could not fit bounds', e);
        }
    }

    /** Subscribe to viewport changes. */
    onViewportChange(callback) {
        this._viewportListeners.push(callback);
        return () => {
            this._viewportListeners = this._viewportListeners.filter(fn => fn !== callback);
        };
    }

    /** Resize the map (call after container size changes). */
    resize() {
        if (this.map) this.map.resize();
    }

    // --- Globe auto-rotation -------------------------------------------

    /**
     * Start a slow westward rotation of the globe (matching Earth's
     * apparent rotation as seen from above the north pole).
     * Does nothing if the spin has already been stopped by user
     * interaction, or if the map is not in globe projection.
     *
     * @param {number} [degreesPerSecond=6] — rotation speed
     */
    startSpin(degreesPerSecond = 6) {
        if (this._spinStopped || this._spinning || !this.map) return;
        this._spinning = true;

        let lastTime = performance.now();
        const tick = (now) => {
            if (!this._spinning) return;
            const dt = (now - lastTime) / 1000; // seconds elapsed
            lastTime = now;

            const center = this.map.getCenter();
            // Rotate westward (subtract); wrap longitude to [-180, 180]
            let lng = center.lng - degreesPerSecond * dt;
            if (lng < -180) lng += 360;

            // Use jumpTo to avoid triggering animated move events
            this.map.jumpTo({ center: [lng, center.lat] });

            this._spinRAF = requestAnimationFrame(tick);
        };
        this._spinRAF = requestAnimationFrame(tick);
    }

    /** Stop the globe spin. Once stopped it will not auto-restart. */
    stopSpin() {
        if (!this._spinning) return;
        this._spinning = false;
        this._spinStopped = true;
        if (this._spinRAF) {
            cancelAnimationFrame(this._spinRAF);
            this._spinRAF = null;
        }
    }

    /** @returns {boolean} Whether the globe has ever been stopped by user interaction. */
    get spinWasStopped() {
        return this._spinStopped;
    }

    /**
     * Wire DOM events on the map canvas so that any user interaction
     * immediately stops the spin. Covers mouse, touch, and wheel.
     * @private
     */
    _wireSpinStop() {
        const canvas = this.map.getCanvas();
        const stop = () => this.stopSpin();
        canvas.addEventListener('mousedown', stop, { once: true });
        canvas.addEventListener('touchstart', stop, { once: true });
        canvas.addEventListener('wheel', stop, { once: true });
        // Also stop on programmatic interactions (e.g. zoom controls)
        this.map.on('zoomstart', stop);
        this.map.on('dragstart', stop);
        this.map.on('pitchstart', stop);
        this.map.on('rotatestart', stop);
    }

    // --- Admin boundary layers -------------------------------------------

    /**
     * Discover boundary layers already present in the whg-context style
     * and wire up click/hover on the fill layer.
     *
     * ALL boundary layers (fill, line, label) are hidden initially —
     * showBoundaries() makes them visible when the user selects an
     * admin-level tier.  Each layer's original filter is saved so that
     * showBoundaries() can combine it with the new admin-level/namespace
     * constraint, and hideBoundaries() can restore it.
     *
     * Hover and selection highlighting are driven entirely by MapLibre's
     * feature-state mechanism:  showBoundaries() installs paint
     * expressions that respond to `hover` and `selected` feature-state
     * properties.  Because setFeatureState propagates to ALL tile
     * fragments sharing the same MVT feature ID (our packed boundary ID),
     * highlighting is seamless across tile boundaries with zero
     * JavaScript geometry work on each mousemove.
     *
     * A floating tooltip is also created for boundary name display.
     * @private
     */
    _initBoundaryLayers() {
        const style = this.map.getStyle();
        if (!style || !style.layers) return;

        // Group boundary layers by source (which equals source-layer in our convention).
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
            console.warn('contextMap: no boundary layers found in style');
            return;
        }

        // Snapshot original filters
        this._originalFilters = {};
        for (const layerId of this._boundaryLayerIds) {
            const def = style.layers.find(l => l.id === layerId);
            this._originalFilters[layerId] = def?.filter
                ? JSON.parse(JSON.stringify(def.filter)) : null;
        }

        // Belt-and-suspenders: hide everything initially.
        for (const layerId of this._boundaryLayerIds) {
            try { this.map.setLayoutProperty(layerId, 'visibility', 'none'); } catch (e) {}
        }

        // Hover tooltip
        this._hoverTooltip = document.createElement('div');
        this._hoverTooltip.className = 'boundary-hover-tooltip';
        this._hoverTooltip.style.display = 'none';
        document.body.appendChild(this._hoverTooltip);

        // Wire hover/click on every fill layer. Feature-state propagates to all
        // tile fragments sharing the MVT feature ID, so highlight is seamless
        // across tile boundaries. Each handler captures its source ID via closure.
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

    /**
     * Collect all loaded tile fragments for one boundary feature so the
     * boundary-click event can carry a complete geometry. Vector tiles
     * clip polygons at tile edges, so a large region is split across many
     * tiles; we re-assemble by querying all features sharing the MVT id.
     *
     * @private
     */
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
            console.warn('contextMap._collectBoundaryFragments: query failed', e);
            return null;
        }
    }

    /**
     * Activate boundary display for one source, narrowed to the given
     * `boundary` field values.
     *
     * The whg-context style holds line+label layers per tier (continental,
     * country, state, district, local) whose static filter targets a
     * specific subset of `boundary` values, and a single transparent fill
     * layer per source covering all values. We make a layer visible iff
     * its static filter intersects the caller's requested `boundaryValues`.
     * For the fill layer we also narrow the runtime filter so that hover
     * and click only respond to the selected tier.
     *
     * @param {Object} opts
     * @param {string} opts.source — boundary source-layer name
     *   ('osm' | 'ohm' | 'osm_misc' | 'po' | 'clio' | 'nl').
     * @param {string[]} [opts.boundaryValues] — list of `boundary` values
     *   to show (e.g. ['2'] for country tier). If omitted/empty, every
     *   feature in the source is shown.
     */
    showBoundaries(opts) {
        if (!this.map || this._boundaryLayerIds.length === 0) return;
        const { source, boundaryValues = null } = opts || {};
        if (!source || !this._layersBySource[source]) {
            console.warn('contextMap.showBoundaries: unknown source', source);
            return;
        }

        // First pass: hide everything from other sources.
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

        // Second pass: for each layer in the active source, decide whether
        // its static filter intersects boundaryValues. If yes, make it
        // visible (with the static filter unchanged for line/label, narrowed
        // for the fill so click/hover only fires inside the chosen tier).
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
            } catch (e) {
                console.warn(`contextMap.showBoundaries: error on layer ${layerId}`, e);
            }
        }

        // Feature-state-driven paint on the fill layer so hover/select are
        // visible without any geometry work in JS.
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

    /** Hide every boundary layer, restore static filters, clear feature-state. */
    hideBoundaries() {
        if (!this.map || this._boundaryLayerIds.length === 0) return;

        for (const layerId of this._boundaryLayerIds) {
            try {
                this.map.setFilter(layerId, this._originalFilters[layerId] || null);
                this.map.setLayoutProperty(layerId, 'visibility', 'none');
            } catch (e) {}
        }

        // Reset every fill's paint so the previously-shown source no longer
        // carries hover/select expressions referring to its feature state.
        for (const fillId of Object.values(this._fillBySource)) {
            try {
                this.map.setPaintProperty(fillId, 'fill-color', 'rgba(0, 0, 0, 0)');
                this.map.setPaintProperty(fillId, 'fill-outline-color', 'rgba(0, 0, 0, 0)');
            } catch (e) {}
        }

        // Clear feature-state across every boundary source.
        for (const sourceId of Object.keys(this._layersBySource)) {
            try {
                this.map.removeFeatureState({ source: sourceId, sourceLayer: sourceId });
            } catch (e) {}
        }
        this._hovered = null;
        this._selected = null;
        this._currentSource = null;

        this.clearBoundaryHover();
    }

    /** Clear the hover highlight (feature-state) and tooltip. */
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

    /** Clear the selection highlight (feature-state). */
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

    /**
     * Search loaded boundary tiles for features whose `name` includes
     * `query`. Restricted to the currently shown source if any (else
     * searches all known boundary sources).
     *
     * @param {string} query — substring to match against the `name` property
     * @param {string[]} [boundaryValues] — restrict to these `boundary` values
     * @param {number} [limit=20] — maximum results
     * @returns {Array} deduplicated matching features
     */
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

    // -------------------------------------------------------------------

    _emitViewportChange(bbox) {
        this._viewportListeners.forEach(fn => {
            try { fn(bbox); } catch (e) { console.error('viewport listener error', e); }
        });
    }
}

// Export singleton
const contextMap = new ContextMap();
export default contextMap;

