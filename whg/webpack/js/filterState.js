// /whg/webpack/js/filterState.js
/**
 * Central filter state model for the WHG v3.5 search filters.
 *
 * Holds the unified state object (per spec §12) and provides
 * an observer pattern for reactive updates. Only one temporal
 * authority may be active at a time.
 */

const DEFAULT_STATE = {
    authorities: ['gn', 'wd', 'tgn', 'pl', 'iv', 'whg'],
    place_types: [],

    mode: 'timespan', // 'timespan' | 'period' | 'polity'

    spatial: {
        bbox: null,           // [minLon, minLat, maxLon, maxLat]
        region_id: null,      // OSM region ID (Tab 1)
        period_id: null,      // PeriodO URI (Tab 2)
        polity_id: null,      // Polity ID (Tab 3)
        geometry_source: 'none', // 'osm' | 'period' | 'polity' | 'none'
        preview_geo: null,    // GeoJSON for map preview (never sent to server)
    },

    temporal: {
        start_year: -2000,
        stop_year: 2100,
        source: 'manual', // 'manual' | 'period' | 'polity'
    },

    dirty: false,
};

class FilterState {
    constructor() {
        this._state = JSON.parse(JSON.stringify(DEFAULT_STATE));
        this._listeners = [];
    }

    /** Get the full state or a nested key (dot-separated). */
    get(key) {
        if (!key) return this._state;
        return key.split('.').reduce((obj, k) => (obj != null ? obj[k] : undefined), this._state);
    }

    /** Set a value by dot-separated key. Marks state dirty and notifies. */
    set(key, value) {
        const keys = key.split('.');
        let obj = this._state;
        for (let i = 0; i < keys.length - 1; i++) {
            obj = obj[keys[i]];
        }
        obj[keys[keys.length - 1]] = value;

        // Don't mark dirty for bbox changes (viewport moves are not user filter actions)
        if (key !== 'spatial.bbox') {
            this._state.dirty = true;
        }

        this._notify(key, value);
    }

    /** Replace multiple top-level keys at once. */
    update(partial) {
        Object.keys(partial).forEach(key => {
            if (typeof partial[key] === 'object' && partial[key] !== null && !Array.isArray(partial[key])) {
                Object.assign(this._state[key], partial[key]);
            } else {
                this._state[key] = partial[key];
            }
        });
        this._state.dirty = true;
        this._notify('bulk', partial);
    }

    isDirty() {
        return this._state.dirty;
    }

    markClean() {
        this._state.dirty = false;
        this._notify('dirty', false);
    }

    /** Subscribe to state changes. Returns unsubscribe function. */
    subscribe(callback) {
        this._listeners.push(callback);
        return () => {
            this._listeners = this._listeners.filter(fn => fn !== callback);
        };
    }

    /**
     * Clear state associated with a tab being left (§10 tab switching rules).
     * The viewport persists; authorities and place_types persist.
     */
    clearTabState(leavingMode) {
        switch (leavingMode) {
            case 'timespan':
                this._state.spatial.region_id = null;
                this._state.spatial.geometry_source = 'none';
                this._state.spatial.preview_geo = null;
                this._state.temporal = {
                    start_year: -2000,
                    stop_year: 2100,
                    source: 'manual',
                };
                break;
            case 'period':
                this._state.spatial.period_id = null;
                this._state.spatial.geometry_source = 'none';
                this._state.spatial.preview_geo = null;
                this._state.temporal = {
                    start_year: -2000,
                    stop_year: 2100,
                    source: 'manual',
                };
                break;
            case 'polity':
                this._state.spatial.polity_id = null;
                this._state.spatial.geometry_source = 'none';
                this._state.spatial.preview_geo = null;
                this._state.temporal = {
                    start_year: -2000,
                    stop_year: 2100,
                    source: 'manual',
                };
                break;
        }
        this._state.dirty = true;
        this._notify('tabSwitch', leavingMode);
    }

    /**
     * Build the search payload descriptor (§11.2).
     * Geometries are referenced by ID, never sent as GeoJSON.
     */
    toSearchPayload() {
        const s = this._state;
        const payload = {
            authorities: s.authorities,
            place_types: s.place_types,
            mode: s.mode,
            spatial: {
                bbox: s.spatial.bbox,
                geometry_ref: null,
            },
            temporal: {
                start_year: s.temporal.start_year,
                stop_year: s.temporal.stop_year,
                source: s.temporal.source,
            },
        };

        // Attach geometry reference based on active mode
        if (s.mode === 'timespan' && s.spatial.region_id) {
            payload.spatial.geometry_ref = {
                index: 'places',
                id: s.spatial.region_id,
            };
        } else if (s.mode === 'period' && s.spatial.period_id) {
            payload.spatial.geometry_ref = {
                index: 'periodo_periods',
                id: s.spatial.period_id,
            };
        } else if (s.mode === 'polity' && s.spatial.polity_id) {
            payload.spatial.geometry_ref = {
                index: 'polities',
                id: s.spatial.polity_id,
            };
        }

        return payload;
    }

    /** Reset state to defaults. */
    reset() {
        this._state = JSON.parse(JSON.stringify(DEFAULT_STATE));
        this._notify('reset', null);
    }

    _notify(key, value) {
        this._listeners.forEach(fn => {
            try {
                fn(key, value, this._state);
            } catch (e) {
                console.error('FilterState listener error:', e);
            }
        });
    }
}

// Export a singleton instance
const filterState = new FilterState();
export default filterState;


