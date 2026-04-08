// /whg/webpack/js/contextMap.js
/**
 * Context map manager for the WHG v3.5 search filters.
 *
 * Wraps a single MapLibre instance in the #context_map container.
 * Provides overlay management for geometry previews (one at a time)
 * and exposes the viewport bounding box.
 */

import filterState from './filterState';
import { decodeBoundaryId } from './boundaryId';

const OVERLAY_SOURCE = 'filter-overlay';
const OVERLAY_FILL = 'filter-overlay-fill';
const OVERLAY_LINE = 'filter-overlay-line';

const SUGGESTION_SOURCE = 'suggestion-markers';
const SUGGESTION_CIRCLES = 'suggestion-circles';
const SUGGESTION_LABELS = 'suggestion-labels';

/* ── Admin boundaries from the whg-context style ──
 * The 'whg-context' style already includes a `boundaries` source
 * with fill and line layers.  We discover their IDs at load time
 * so we can toggle visibility, set filters, and wire click/hover.
 */
const BOUNDARIES_SOURCE_LAYER = 'boundaries';  // source-layer name inside the vector tiles

class ContextMap {
    constructor() {
        this.map = null;
        this._viewportListeners = [];
        this._ready = false;
        this._readyPromise = null;
        this._spinning = false;
        this._spinRAF = null;
        this._spinStopped = false; // Once stopped, never auto-restart
        this._boundaryLayerIds = [];   // Discovered at load time
        this._boundaryFillId = null;   // The fill layer (for click/hover)
        this._boundarySourceId = null; // Source ID for the boundary tiles
        this._originalFilters = {};    // Snapshot of each boundary layer's original filter
        this._hoverTooltip = null;     // Floating tooltip element for hovered boundaries
        this._hoveredBoundaryId = null;   // Packed integer ID of currently hovered boundary
        this._selectedBoundaryId = null;  // Packed integer ID of currently selected boundary
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

        // Collect all layers whose source-layer is 'boundaries'
        this._boundaryLayerIds = style.layers
            .filter(l => l['source-layer'] === BOUNDARIES_SOURCE_LAYER)
            .map(l => l.id);

        if (this._boundaryLayerIds.length === 0) {
            console.warn('contextMap: no boundary layers found in style');
            return;
        }

        console.log('contextMap: discovered boundary layers:', this._boundaryLayerIds);

        // Snapshot each layer's original filter (deep copy)
        this._originalFilters = {};
        for (const layerId of this._boundaryLayerIds) {
            const layerDef = style.layers.find(l => l.id === layerId);
            this._originalFilters[layerId] = layerDef?.filter
                ? JSON.parse(JSON.stringify(layerDef.filter))
                : null;
        }

        // Identify the fill layer (for click/hover + filter control)
        this._boundaryFillId = this._boundaryLayerIds.find(id => {
            const layer = this.map.getLayer(id);
            return layer && layer.type === 'fill';
        }) || this._boundaryLayerIds[0];

        // Discover the source ID from the fill layer definition
        const fillLayerDef = style.layers.find(l => l.id === this._boundaryFillId);
        this._boundarySourceId = fillLayerDef ? fillLayerDef.source : null;

        // ── Ensure ALL boundary layers are hidden initially ──
        // (The style already sets visibility: none, but this is belt-and-suspenders)
        for (const layerId of this._boundaryLayerIds) {
            try {
                this.map.setLayoutProperty(layerId, 'visibility', 'none');
            } catch (e) {
                console.warn(`contextMap: could not hide boundary layer ${layerId}`, e);
            }
        }

        // ── Add hover highlight source + layers ──
        // ── Hover + selection: driven by feature-state on the existing fill layer ──
        // MapLibre's setFeatureState propagates to ALL tile fragments sharing the
        // same MVT feature ID (our packed boundary ID), so hover/selection paint
        // is seamless across tile boundaries with zero geometry work in JS.
        // The feature-state-driven paint expressions are installed by showBoundaries().

        // ── Create floating tooltip for boundary names ──
        this._hoverTooltip = document.createElement('div');
        this._hoverTooltip.className = 'boundary-hover-tooltip';
        this._hoverTooltip.style.display = 'none';
        document.body.appendChild(this._hoverTooltip);

        // ── Hover: feature-state highlight + tooltip on mousemove over the fill layer ──
        // setFeatureState propagates to ALL tile fragments with the same packed ID,
        // so the entire boundary highlights seamlessly across tile edges.
        this._hoveredBoundaryId = null;
        this.map.on('mousemove', this._boundaryFillId, (e) => {
            this.map.getCanvas().style.cursor = 'pointer';
            if (e.features && e.features.length > 0) {
                const feature = e.features[0];
                const featureId = feature.id;

                // Only update feature-state when the hovered feature changes
                if (featureId != null && featureId !== this._hoveredBoundaryId) {
                    // Clear previous hover state
                    if (this._hoveredBoundaryId != null) {
                        try {
                            this.map.removeFeatureState(
                                { source: this._boundarySourceId, sourceLayer: BOUNDARIES_SOURCE_LAYER, id: this._hoveredBoundaryId },
                                'hover'
                            );
                        } catch (err) { /* source not ready */ }
                    }
                    this._hoveredBoundaryId = featureId;
                    try {
                        this.map.setFeatureState(
                            { source: this._boundarySourceId, sourceLayer: BOUNDARIES_SOURCE_LAYER, id: featureId },
                            { hover: true }
                        );
                    } catch (err) { /* source not ready */ }
                }

                const name = feature.properties?.name;
                if (name && this._hoverTooltip) {
                    this._hoverTooltip.textContent = name;
                    this._hoverTooltip.style.display = 'block';
                    this._hoverTooltip.style.left = (e.originalEvent.pageX + 12) + 'px';
                    this._hoverTooltip.style.top = (e.originalEvent.pageY - 28) + 'px';
                }
            }
        });
        this.map.on('mouseleave', this._boundaryFillId, () => {
            this.map.getCanvas().style.cursor = '';
            this._hoveredBoundaryId = null;
            this.clearBoundaryHover();
        });

        // ── Click: set selection feature-state, collect fragments for geometry, dispatch event ──
        this.map.on('click', this._boundaryFillId, (e) => {
            if (!e.features || e.features.length === 0) return;
            const feature = e.features[0];
            const props = feature.properties || {};

            // Clear previous selection, set new one via feature-state
            this.clearBoundarySelection();
            if (feature.id != null) {
                this._selectedBoundaryId = feature.id;
                try {
                    this.map.setFeatureState(
                        { source: this._boundarySourceId, sourceLayer: BOUNDARIES_SOURCE_LAYER, id: feature.id },
                        { selected: true }
                    );
                } catch (err) { /* source not ready */ }
            }

            // Collect all tile fragments to build a complete geometry
            // (still needed for the boundary-click event's geometry payload)
            const fc = this._collectBoundaryFragments(feature);
            let geometry = feature.geometry;
            if (fc && fc.features.length > 0) {
                // Combine into a GeometryCollection so the full extent is covered
                if (fc.features.length === 1) {
                    geometry = fc.features[0].geometry;
                } else {
                    geometry = {
                        type: 'GeometryCollection',
                        geometries: fc.features.map(f => f.geometry),
                    };
                }
            }

            // Decode the packed integer MVT feature ID
            const decoded = decodeBoundaryId(feature.id);

            document.dispatchEvent(new CustomEvent('boundary-click', {
                detail: {
                    id: `${decoded.namespace}:r${decoded.relationId}`,
                    name: props.name || '',
                    admin_level: props.admin_level,
                    namespace: decoded.namespace,
                    geometry,
                },
            }));
        });
    }

    /**
     * Collect all tile fragments for the same boundary feature.
     *
     * Vector tiles clip polygons at tile edges, so a large admin region
     * may be split across many tiles.  This queries all loaded tiles for
     * fragments sharing the same packed integer feature ID and returns
     * them as a FeatureCollection.
     *
     * Note: this is only used during click to assemble a complete
     * geometry for the `boundary-click` event payload.  Hover and
     * selection visual feedback use feature-state instead (which
     * MapLibre applies to all fragments automatically).
     *
     * @param {Object} feature — a MapLibre feature from the fill layer
     * @returns {Object|null} GeoJSON FeatureCollection, or null on failure
     * @private
     */
    _collectBoundaryFragments(feature) {
        if (feature.id == null || !this._boundarySourceId) return null;

        try {
            const allFragments = this.map.querySourceFeatures(this._boundarySourceId, {
                sourceLayer: BOUNDARIES_SOURCE_LAYER,
                filter: ['==', ['id'], feature.id],
            });

            if (allFragments.length === 0) return null;

            // Deduplicate by serialised coordinates (tiles can return
            // the same fragment from overlapping tile buffers)
            const seen = new Set();
            const features = [];
            for (const f of allFragments) {
                const key = JSON.stringify(f.geometry.coordinates);
                if (seen.has(key)) continue;
                seen.add(key);
                features.push({
                    type: 'Feature',
                    geometry: f.geometry,
                    properties: f.properties || {},
                });
            }

            return { type: 'FeatureCollection', features };
        } catch (e) {
            console.warn('contextMap._collectBoundaryFragments: query failed', e);
            return null;
        }
    }

    /**
     * Combine two MapLibre filter expressions into a single compound.
     * Flattens nested `['all', ...]` wrappers so the result is clean.
     *
     * @param {Array|null} original — the layer's style-defined filter
     * @param {Array|null} extra    — the new constraint to add
     * @returns {Array|null} merged filter expression
     * @private
     */
    _combineFilters(original, extra) {
        if (!original && !extra) return null;
        if (!original) return extra;
        if (!extra) return original;

        const origParts = (Array.isArray(original) && original[0] === 'all')
            ? original.slice(1) : [original];
        const extraParts = (Array.isArray(extra) && extra[0] === 'all')
            ? extra.slice(1) : [extra];
        return ['all', ...origParts, ...extraParts];
    }

    /**
     * Activate boundary display for the given filter.
     *
     * Shows ALL boundary layers (fill, line, label) with their original
     * style-defined filters combined with the new filter.  This ensures
     * that only layers whose admin-level range overlaps the requested
     * level will actually render features — e.g. asking for admin_level 2
     * will show features on `boundary-line-country` (original filter ==2)
     * but nothing on `boundary-line-state` (original filter >=3 && <5,
     * combined with ==2 yields an impossible condition).
     *
     * The fill layer opacity is boosted so polygons are clearly visible
     * for hover/click interaction.
     *
     * @param {Array|null} [filter] — MapLibre filter expression
     *   e.g. ['all', ['==', ['get', 'admin_level'], 2], ['==', ['get', 'namespace'], 'osm']]
     */
    showBoundaries(filter) {
        if (!this.map || this._boundaryLayerIds.length === 0) return;

        for (const layerId of this._boundaryLayerIds) {
            try {
                const original = this._originalFilters[layerId];
                const combined = this._combineFilters(original, filter);
                this.map.setFilter(layerId, combined);
                this.map.setLayoutProperty(layerId, 'visibility', 'visible');
            } catch (e) {
                console.warn(`contextMap.showBoundaries: error on layer ${layerId}`, e);
            }
        }

        // Set feature-state-driven paint on the fill layer so that
        // hover and selection highlighting work across tile boundaries
        // without any JavaScript geometry collection.
        if (this._boundaryFillId) {
            try {
                this.map.setPaintProperty(this._boundaryFillId, 'fill-color', [
                    'case',
                    ['boolean', ['feature-state', 'selected'], false], '#4a90d9',
                    ['boolean', ['feature-state', 'hover'], false], '#fbbf24',
                    'rgb(100, 140, 190)',
                ]);
                this.map.setPaintProperty(this._boundaryFillId, 'fill-opacity', [
                    'case',
                    ['boolean', ['feature-state', 'selected'], false], 0.25,
                    ['boolean', ['feature-state', 'hover'], false], 0.30,
                    0.12,
                ]);
                this.map.setPaintProperty(this._boundaryFillId, 'fill-outline-color', [
                    'case',
                    ['boolean', ['feature-state', 'selected'], false], '#2563eb',
                    'rgba(50, 80, 120, 0.35)',
                ]);
            } catch (e) { /* not critical */ }
        }
    }

    /**
     * Deactivate boundary display — hide ALL boundary layers,
     * restore their original filters, and clear any hover highlight.
     */
    hideBoundaries() {
        if (!this.map || this._boundaryLayerIds.length === 0) return;

        for (const layerId of this._boundaryLayerIds) {
            try {
                // Restore the layer's original filter
                this.map.setFilter(layerId, this._originalFilters[layerId] || null);
                this.map.setLayoutProperty(layerId, 'visibility', 'none');
            } catch (e) { /* layer may not exist yet */ }
        }

        // Clear ALL feature-state (hover + selected) from the boundary source
        if (this._boundarySourceId) {
            try {
                this.map.removeFeatureState(
                    { source: this._boundarySourceId, sourceLayer: BOUNDARIES_SOURCE_LAYER }
                );
            } catch (e) { /* source not ready */ }
        }
        this._hoveredBoundaryId = null;
        this._selectedBoundaryId = null;

        // Restore original fill paint (data-driven match expression)
        if (this._boundaryFillId) {
            try {
                this.map.setPaintProperty(this._boundaryFillId, 'fill-color', [
                    'match', ['get', 'admin_level'],
                    0, 'rgba(80, 110, 160, 0.06)',
                    1, 'rgba(90, 120, 165, 0.05)',
                    2, 'rgba(100, 130, 170, 0.05)',
                    3, 'rgba(120, 150, 185, 0.04)',
                    4, 'rgba(120, 150, 185, 0.04)',
                    'rgba(140, 160, 195, 0.03)',
                ]);
                this.map.setPaintProperty(this._boundaryFillId, 'fill-outline-color', 'rgba(0, 0, 0, 0)');
            } catch (e) { /* not critical */ }
        }

        this.clearBoundaryHover();
    }

    /** Clear the hover highlight (via feature-state) and tooltip. */
    clearBoundaryHover() {
        if (!this.map) return;
        if (this._hoveredBoundaryId != null && this._boundarySourceId) {
            try {
                this.map.removeFeatureState(
                    { source: this._boundarySourceId, sourceLayer: BOUNDARIES_SOURCE_LAYER, id: this._hoveredBoundaryId },
                    'hover'
                );
            } catch (e) { /* source not ready */ }
            this._hoveredBoundaryId = null;
        }
        if (this._hoverTooltip) {
            this._hoverTooltip.style.display = 'none';
        }
    }

    /** Clear the selection highlight (via feature-state). */
    clearBoundarySelection() {
        if (!this.map) return;
        if (this._selectedBoundaryId != null && this._boundarySourceId) {
            try {
                this.map.removeFeatureState(
                    { source: this._boundarySourceId, sourceLayer: BOUNDARIES_SOURCE_LAYER, id: this._selectedBoundaryId },
                    'selected'
                );
            } catch (e) { /* source not ready */ }
            this._selectedBoundaryId = null;
        }
    }

    /**
     * Search for boundary features by name within currently loaded tiles.
     *
     * @param {string} query — text to match against the `name` property
     * @param {number} adminLevel — exact admin_level to match
     * @param {string} [namespace] — 'osm' or 'ohm' (empty → all)
     * @param {number} [limit=20] — max results
     * @returns {Array} matching MapLibre features (deduplicated by name)
     */
    searchBoundaryFeatures(query, adminLevel, namespace, limit = 20) {
        if (!this.map || !this._boundarySourceId) return [];

        const filter = ['all', ['==', ['get', 'admin_level'], adminLevel]];
        if (namespace) {
            filter.push(['==', ['get', 'namespace'], namespace]);
        }

        let features;
        try {
            features = this.map.querySourceFeatures(this._boundarySourceId, {
                sourceLayer: BOUNDARIES_SOURCE_LAYER,
                filter: filter,
            });
        } catch (e) {
            console.warn('contextMap.searchBoundaryFeatures: query failed', e);
            return [];
        }

        const lowerQuery = query.toLowerCase();
        const seen = new Set();
        const results = [];

        for (const f of features) {
            const name = (f.properties?.name || '').toLowerCase();
            if (!name || !name.includes(lowerQuery)) continue;
            // Deduplicate by name + namespace
            const key = `${f.properties.name}|${f.properties.namespace || ''}`;
            if (seen.has(key)) continue;
            seen.add(key);
            results.push(f);
            if (results.length >= limit) break;
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

