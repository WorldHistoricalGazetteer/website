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

class ContextMap {
    constructor() {
        this.map = null;
        this._viewportListeners = [];
        this._ready = false;
        this._readyPromise = null;
        this._spinning = false;
        this._spinRAF = null;
        this._spinStopped = false; // Once stopped, never auto-restart
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
                style: ['WHG'],
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

