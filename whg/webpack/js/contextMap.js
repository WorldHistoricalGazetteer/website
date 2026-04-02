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
            this.map.setCenter([lng, center.lat]);

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

