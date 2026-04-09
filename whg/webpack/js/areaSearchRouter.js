// /whg/webpack/js/areaSearchRouter.js
/**
 * Area Search Router for the Atlas page.
 *
 * Routes Areas-mode search queries to the correct backend(s) based
 * on which layer sources are active in the Layer Sources palette.
 *
 * Currently supports:
 * - OSM/OHM → /search/boundaries/ endpoint
 *
 * Future:
 * - PeriodO → periodo_periods ES index
 * - Cliopatria/D-PLACE/NativeLand → territories ES index
 */

export default class AreaSearchRouter {
    /**
     * @param {LayerSourcesPalette} palette - the layer sources palette instance
     */
    constructor(palette) {
        this._palette = palette;
    }

    /**
     * Search for areas matching the query, routing to appropriate backends.
     *
     * @param {string} query - search text (min 2 chars)
     * @param {object} options - { adminLevel, namespace, limit, temporalStart, temporalEnd }
     * @returns {Promise<Array>} merged results from all active sources
     */
    async search(query, options = {}) {
        if (!query || query.length < 2) return [];

        const activeSources = this._palette.getActiveSources();
        const promises = [];

        // OSM/OHM boundary search
        if (activeSources.includes('osm') || activeSources.includes('ohm')) {
            promises.push(this._searchBoundaries(query, options, activeSources));
        }

        // PeriodO (future)
        if (activeSources.includes('periodo')) {
            promises.push(this._searchPeriods(query, options));
        }

        // Cliopatria/D-PLACE/NativeLand (future)
        const polityDatasets = ['cliopatria', 'dplace', 'nativeland']
            .filter(d => activeSources.includes(d));
        if (polityDatasets.length > 0) {
            promises.push(this._searchPolities(query, polityDatasets, options));
        }

        const resultSets = await Promise.all(promises);
        // Flatten and return all results
        return resultSets.flat();
    }

    /**
     * Query /search/boundaries/ for OSM/OHM admin regions.
     */
    async _searchBoundaries(query, options, activeSources) {
        const params = new URLSearchParams({ q: query, limit: String(options.limit || 20) });

        if (options.adminLevel != null && options.adminLevel !== '') {
            params.set('admin_level', String(options.adminLevel));
        }

        // Build namespace filter
        const namespaces = [];
        if (activeSources.includes('osm')) namespaces.push('osm');
        if (activeSources.includes('ohm')) namespaces.push('ohm');
        if (namespaces.length > 0 && namespaces.length < 2) {
            // If both are active, don't filter by namespace
            params.set('namespace', namespaces[0]);
        }
        // For admin levels 0-1, Modern includes 'osm' and 'm49'
        if (options.adminLevel != null && options.adminLevel <= 1 && activeSources.includes('osm')) {
            params.set('namespace', 'osm,m49');
        }

        try {
            const resp = await fetch(`/search/boundaries/?${params}`);
            if (!resp.ok) return [];
            const data = await resp.json();
            return (data.results || []).map(r => ({
                id: r.id || `boundary:${r.namespace}:${r.name}`,
                label: r.name,
                sublabel: `Level ${r.admin_level} · ${(r.namespace || 'osm').toUpperCase()}`
                    + (r.ccodes && r.ccodes.length ? ` · ${r.ccodes.join(', ')}` : ''),
                source: r.namespace || 'osm',
                source_type: 'boundary',
                bounds: r.bounds,
                repr_point: r.repr_point,
                admin_level: r.admin_level,
                namespace: r.namespace || 'osm',
                geometry: null, // Must click polygon on map to get geometry
                _fromIndex: true,
            }));
        } catch (e) {
            console.warn('AreaSearchRouter: boundary search failed', e);
            return [];
        }
    }

    /**
     * Search PeriodO periods (stub — backend not yet connected).
     */
    async _searchPeriods(query, options) {
        // Future: POST to /search/periods/ or query periodo_periods ES index
        console.log('AreaSearchRouter: PeriodO search not yet connected');
        return [];
    }

    /**
     * Search polity/territory datasets (stub — backend not yet connected).
     */
    async _searchPolities(query, datasets, options) {
        // Future: POST to /search/polities/ or query territories ES index
        console.log('AreaSearchRouter: polity search not yet connected for', datasets);
        return [];
    }
}

