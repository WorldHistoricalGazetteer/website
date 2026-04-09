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
import { decodeBoundaryId } from './boundaryId';

const OVERLAY_SOURCE = 'filter-overlay';
const OVERLAY_FILL = 'filter-overlay-fill';
const OVERLAY_LINE = 'filter-overlay-line';

const SUGGESTION_SOURCE = 'suggestion-markers';
const SUGGESTION_CIRCLES = 'suggestion-circles';
const SUGGESTION_LABELS = 'suggestion-labels';

const BOUNDARIES_SOURCE_LAYER = 'boundaries';

class HeroMap {
    constructor() {
        this.map = null;
        this._viewportListeners = [];
        this._ready = false;
        this._readyPromise = null;
        this._spinning = false;
        this._spinRAF = null;
        this._spinStopped = false;
        this._boundaryLayerIds = [];
        this._boundaryFillId = null;
        this._boundarySourceId = null;
        this._originalFilters = {};
        this._hoverTooltip = null;
        this._hoveredBoundaryId = null;
        this._selectedBoundaryId = null;
    }

    /**
     * Initialise the map in the #hero_map container.
     * Returns a promise that resolves when the map is loaded.
     */
    init() {
        if (this._readyPromise) return this._readyPromise;

        this._readyPromise = new Promise((resolve) => {
            this.map = new whg_maplibre.Map({
                container: 'hero_map',
                zoom: 1.5,
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

    startSpin(degreesPerSecond = 6) {
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

        this._boundaryLayerIds = style.layers
            .filter(l => l['source-layer'] === BOUNDARIES_SOURCE_LAYER)
            .map(l => l.id);

        if (this._boundaryLayerIds.length === 0) {
            console.warn('heroMap: no boundary layers found in style');
            return;
        }

        this._originalFilters = {};
        for (const layerId of this._boundaryLayerIds) {
            const layerDef = style.layers.find(l => l.id === layerId);
            this._originalFilters[layerId] = layerDef?.filter
                ? JSON.parse(JSON.stringify(layerDef.filter)) : null;
        }

        this._boundaryFillId = this._boundaryLayerIds.find(id => {
            const layer = this.map.getLayer(id);
            return layer && layer.type === 'fill';
        }) || this._boundaryLayerIds[0];

        const fillLayerDef = style.layers.find(l => l.id === this._boundaryFillId);
        this._boundarySourceId = fillLayerDef ? fillLayerDef.source : null;

        for (const layerId of this._boundaryLayerIds) {
            try { this.map.setLayoutProperty(layerId, 'visibility', 'none'); }
            catch (e) { /* */ }
        }

        // Hover tooltip
        this._hoverTooltip = document.createElement('div');
        this._hoverTooltip.className = 'boundary-hover-tooltip';
        this._hoverTooltip.style.display = 'none';
        document.body.appendChild(this._hoverTooltip);

        // Hover feature-state
        this._hoveredBoundaryId = null;
        this.map.on('mousemove', this._boundaryFillId, (e) => {
            this.map.getCanvas().style.cursor = 'pointer';
            if (e.features && e.features.length > 0) {
                const feature = e.features[0];
                const featureId = feature.id;
                if (featureId != null && featureId !== this._hoveredBoundaryId) {
                    if (this._hoveredBoundaryId != null) {
                        try {
                            this.map.removeFeatureState(
                                { source: this._boundarySourceId, sourceLayer: BOUNDARIES_SOURCE_LAYER, id: this._hoveredBoundaryId },
                                'hover'
                            );
                        } catch (err) { /* */ }
                    }
                    this._hoveredBoundaryId = featureId;
                    try {
                        this.map.setFeatureState(
                            { source: this._boundarySourceId, sourceLayer: BOUNDARIES_SOURCE_LAYER, id: featureId },
                            { hover: true }
                        );
                    } catch (err) { /* */ }
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

        // Click: selection + dispatch event
        this.map.on('click', this._boundaryFillId, (e) => {
            if (!e.features || e.features.length === 0) return;
            const feature = e.features[0];
            const props = feature.properties || {};

            this.clearBoundarySelection();
            if (feature.id != null) {
                this._selectedBoundaryId = feature.id;
                try {
                    this.map.setFeatureState(
                        { source: this._boundarySourceId, sourceLayer: BOUNDARIES_SOURCE_LAYER, id: feature.id },
                        { selected: true }
                    );
                } catch (err) { /* */ }
            }

            const fc = this._collectBoundaryFragments(feature);
            let geometry = feature.geometry;
            if (fc && fc.features.length > 0) {
                if (fc.features.length === 1) {
                    geometry = fc.features[0].geometry;
                } else {
                    geometry = {
                        type: 'GeometryCollection',
                        geometries: fc.features.map(f => f.geometry),
                    };
                }
            }

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

    _collectBoundaryFragments(feature) {
        if (feature.id == null || !this._boundarySourceId) return null;
        try {
            const allFragments = this.map.querySourceFeatures(this._boundarySourceId, {
                sourceLayer: BOUNDARIES_SOURCE_LAYER,
                filter: ['==', ['id'], feature.id],
            });
            if (allFragments.length === 0) return null;
            const seen = new Set();
            const features = [];
            for (const f of allFragments) {
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

    showBoundaries(filter) {
        if (!this.map || this._boundaryLayerIds.length === 0) return;
        for (const layerId of this._boundaryLayerIds) {
            try {
                const original = this._originalFilters[layerId];
                const combined = this._combineFilters(original, filter);
                this.map.setFilter(layerId, combined);
                this.map.setLayoutProperty(layerId, 'visibility', 'visible');
            } catch (e) { /* */ }
        }
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
            } catch (e) { /* */ }
        }
    }

    hideBoundaries() {
        if (!this.map || this._boundaryLayerIds.length === 0) return;
        for (const layerId of this._boundaryLayerIds) {
            try {
                this.map.setFilter(layerId, this._originalFilters[layerId] || null);
                this.map.setLayoutProperty(layerId, 'visibility', 'none');
            } catch (e) { /* */ }
        }
        if (this._boundarySourceId) {
            try {
                this.map.removeFeatureState(
                    { source: this._boundarySourceId, sourceLayer: BOUNDARIES_SOURCE_LAYER }
                );
            } catch (e) { /* */ }
        }
        this._hoveredBoundaryId = null;
        this._selectedBoundaryId = null;
        if (this._boundaryFillId) {
            try {
                this.map.setPaintProperty(this._boundaryFillId, 'fill-color', [
                    'match', ['get', 'admin_level'],
                    0, 'rgba(80, 110, 160, 0.06)', 1, 'rgba(90, 120, 165, 0.05)',
                    2, 'rgba(100, 130, 170, 0.05)', 3, 'rgba(120, 150, 185, 0.04)',
                    4, 'rgba(120, 150, 185, 0.04)', 'rgba(140, 160, 195, 0.03)',
                ]);
                this.map.setPaintProperty(this._boundaryFillId, 'fill-outline-color', 'rgba(0, 0, 0, 0)');
            } catch (e) { /* */ }
        }
        this.clearBoundaryHover();
    }

    clearBoundaryHover() {
        if (!this.map) return;
        if (this._hoveredBoundaryId != null && this._boundarySourceId) {
            try {
                this.map.removeFeatureState(
                    { source: this._boundarySourceId, sourceLayer: BOUNDARIES_SOURCE_LAYER, id: this._hoveredBoundaryId },
                    'hover'
                );
            } catch (e) { /* */ }
            this._hoveredBoundaryId = null;
        }
        if (this._hoverTooltip) this._hoverTooltip.style.display = 'none';
    }

    clearBoundarySelection() {
        if (!this.map) return;
        if (this._selectedBoundaryId != null && this._boundarySourceId) {
            try {
                this.map.removeFeatureState(
                    { source: this._boundarySourceId, sourceLayer: BOUNDARIES_SOURCE_LAYER, id: this._selectedBoundaryId },
                    'selected'
                );
            } catch (e) { /* */ }
            this._selectedBoundaryId = null;
        }
    }

    searchBoundaryFeatures(query, adminLevel, namespace, limit = 20) {
        if (!this.map || !this._boundarySourceId) return [];
        const filter = ['all', ['==', ['get', 'admin_level'], adminLevel]];
        if (namespace) filter.push(['==', ['get', 'namespace'], namespace]);
        let features;
        try {
            features = this.map.querySourceFeatures(this._boundarySourceId, {
                sourceLayer: BOUNDARIES_SOURCE_LAYER, filter,
            });
        } catch (e) { return []; }
        const lowerQuery = query.toLowerCase();
        const seen = new Set();
        const results = [];
        for (const f of features) {
            const name = (f.properties?.name || '').toLowerCase();
            if (!name || !name.includes(lowerQuery)) continue;
            const key = `${f.properties.name}|${f.properties.namespace || ''}`;
            if (seen.has(key)) continue;
            seen.add(key);
            results.push(f);
            if (results.length >= limit) break;
        }
        return results;
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

    _emitViewportChange(bbox) {
        this._viewportListeners.forEach(fn => {
            try { fn(bbox); } catch (e) { console.error('viewport listener error', e); }
        });
    }
}

const heroMap = new HeroMap();
export default heroMap;

