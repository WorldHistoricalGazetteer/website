// /whg/webpack/js/areaSearchRouter.js
/**
 * Area Search Router for the Atlas page.
 *
 * Routes Areas-mode search queries to the correct backend(s) based
 * on which layer sources are active in the Layer Sources palette.
 *
 * Source ids are the gazetteer-registry ids the palette uses
 * (search/views.py REGION_SOURCE_ORDER): osm, ohm, osm_misc, po, clio, nl.
 *
 * Currently supports:
 * - osm / ohm → /atlas/boundaries/ (gateway name search over admin boundaries)
 *
 * Not yet searchable by name (planned): po (PeriodO), clio (Cliopatria),
 * nl (Native Land), osm_misc. For these the router returns an inline hint
 * rather than an empty list, so the dropdown never says "No matching areas
 * found" about a source it never asked (Decision Q3).
 *
 * Results are an array of selectable items; non-selectable notices (hints,
 * beta-access, service failures) are appended as items with `_stub: true`.
 */

import { classifyStatus, failureText, failureHtml } from './atlasNotice.js';

// Registry ids with a name-search backend.
const NAME_SEARCH_SOURCES = ['osm', 'ohm'];
const SOURCE_LABELS = { po: 'PeriodO', clio: 'Cliopatria', nl: 'Native Land', osm_misc: 'OSM (other areas)' };

export default class AreaSearchRouter {
    /**
     * @param {LayerSourcesPalette} palette - the layer sources palette instance
     */
    constructor(palette) {
        this._palette = palette;
    }

    /**
     * Search for regions matching the query, routing to appropriate backends.
     *
     * @param {string} query - search text (min 2 chars)
     * @param {object} options - { namespace, limit, mode, temporalStart, temporalEnd }
     * @returns {Promise<Array>} merged results from all active sources
     */
    async search(query, options = {}) {
        if (!query || query.length < 2) return [];

        const activeSources = this._palette.getActiveSources();
        const promises = [];

        // OSM/OHM boundary search
        if (activeSources.some(s => NAME_SEARCH_SOURCES.includes(s))) {
            promises.push(this._searchBoundaries(query, options, activeSources));
        }

        const resultSets = await Promise.all(promises);
        const results = resultSets.flat();

        // Sources with no name search yet: say so inline (planned), instead of
        // returning nothing and letting the caller report "no matching areas".
        const unsupported = activeSources.filter(s => !NAME_SEARCH_SOURCES.includes(s));
        if (unsupported.length) {
            results.push({
                _stub: true,
                label: "Name search isn't available for this source yet (planned)",
                sublabel: unsupported.map(s => SOURCE_LABELS[s] || s).join(', ')
                    + ' — pick areas on the map instead',
            });
        }
        return results;
    }

    /**
     * Query /atlas/boundaries/ for OSM/OHM admin regions.
     *
     * Was ``/search/boundaries/``, a path with no URL pattern behind it: every
     * lookup 404'd, so the Areas box could never find anything (place#156).
     *
     * The index carries no polygon for a boundary, only a representative point,
     * so results arrive without geometry; ``selectAreaResult`` in atlas.js
     * finishes the job by matching ``place_id`` against the boundary tiles once
     * the map has flown to the hit.
     */
    async _searchBoundaries(query, options, activeSources) {
        const params = new URLSearchParams({ q: query, limit: String(options.limit || 20) });
        if (options.mode) params.set('mode', options.mode);

        // Single-namespace filter only — both active means no constraint.
        const namespaces = [];
        if (activeSources.includes('osm')) namespaces.push('osm');
        if (activeSources.includes('ohm')) namespaces.push('ohm');
        if (namespaces.length === 1) params.set('namespace', namespaces[0]);

        try {
            const resp = await fetch(`/atlas/boundaries/?${params}`, { credentials: 'same-origin' });
            if (!resp.ok) {
                // 403 = no beta access; 503/504 = the gateway did not answer.
                // Neither is "no matching areas" — return a notice instead.
                const kind = classifyStatus(resp.status);
                return [this._failureStub(kind)];
            }
            const data = await resp.json();
            return (data.results || []).map(r => ({
                id: r.place_id || `boundary:${r.namespace}:${r.name}`,
                label: r.name,
                sublabel: [
                    r.boundary != null ? `Level ${r.boundary}` : null,
                    (r.namespace || 'osm').toUpperCase(),
                    r.ccodes && r.ccodes.length ? r.ccodes.join(', ') : null,
                ].filter(Boolean).join(' · '),
                source: r.namespace || 'osm',
                source_type: 'boundary',
                repr_point: r.repr_point,
                place_id: r.place_id,
                boundary: r.boundary,
                namespace: r.namespace || 'osm',
                geometry: null,  // resolved from the tiles on selection
                _fromIndex: true,
            }));
        } catch (e) {
            console.warn('AreaSearchRouter: boundary search failed', e);
            return [this._failureStub('unavailable')];
        }
    }

    /** A non-selectable dropdown notice for a failed boundary search.
     *  `labelHtml` is trusted markup (the beta case carries a link);
     *  `kind` lets the caller react (e.g. raise the gateway banner). */
    _failureStub(kind) {
        return {
            _stub: true,
            _failure: kind,
            label: failureText(kind, 'Area search'),
            labelHtml: failureHtml(kind, 'Area search'),
        };
    }
}

