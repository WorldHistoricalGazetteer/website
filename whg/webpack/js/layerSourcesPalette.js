// /whg/webpack/js/layerSourcesPalette.js
/**
 * Layer Sources palette for the Atlas page.
 *
 * Renders toggleable source switches (OSM, OHM, PeriodO, etc.)
 * and persists state in filterState.active_sources.
 */

import heroMap from './heroMap';

export default class LayerSourcesPalette {
    /**
     * @param {string} panelSelector - CSS selector for the panel container
     * @param {string} toggleSelector - CSS selector for the toggle button
     * @param {Array} sources - [{id, label, enabled, coming_soon?}]
     */
    constructor(panelSelector, toggleSelector, sources) {
        this._panel = document.querySelector(panelSelector);
        this._toggleBtn = document.querySelector(toggleSelector);
        this._sources = sources || [];
        this._activeSources = this._sources
            .filter(s => s.enabled)
            .map(s => s.id);
        this._init();
    }

    _init() {
        // Render source toggles
        this._panel.innerHTML = this._sources.map(s => `
            <div class="layer-source-item">
                <div class="form-check form-switch">
                    <input class="form-check-input layer-source-cb" type="checkbox"
                           id="layer_src_${s.id}" value="${s.id}"
                           ${s.enabled ? 'checked' : ''}
                           ${s.coming_soon ? 'disabled' : ''}>
                    <label class="form-check-label" for="layer_src_${s.id}">
                        ${s.label}
                        ${s.coming_soon ? '<span class="coming-soon">(coming soon)</span>' : ''}
                    </label>
                </div>
            </div>
        `).join('');

        // Wire toggle button
        this._toggleBtn.addEventListener('click', () => {
            const isVisible = this._panel.style.display !== 'none';
            this._panel.style.display = isVisible ? 'none' : 'block';
        });

        // Wire source checkboxes
        this._panel.querySelectorAll('.layer-source-cb').forEach(cb => {
            cb.addEventListener('change', () => {
                this._activeSources = Array.from(
                    this._panel.querySelectorAll('.layer-source-cb:checked')
                ).map(el => el.value);
                this._onSourcesChange();
            });
        });

        // Close panel on outside click
        document.addEventListener('click', (e) => {
            if (!this._panel.contains(e.target) && !this._toggleBtn.contains(e.target)) {
                this._panel.style.display = 'none';
            }
        });
    }

    _onSourcesChange() {
        heroMap.setActiveSources(this._activeSources);
        // Dispatch custom event so other components can react
        document.dispatchEvent(new CustomEvent('layer-sources-change', {
            detail: { activeSources: this._activeSources },
        }));
    }

    /** Get currently active source IDs. */
    getActiveSources() {
        return [...this._activeSources];
    }

    /** Check if a given source is active. */
    isActive(sourceId) {
        return this._activeSources.includes(sourceId);
    }

    /**
     * Get the namespace value(s) for boundary queries.
     * Maps active sources to namespace values used by the boundary tiles.
     */
    getActiveNamespaces() {
        const nsMap = { osm: 'osm', ohm: 'ohm' };
        return this._activeSources
            .filter(s => s in nsMap)
            .map(s => nsMap[s]);
    }
}

