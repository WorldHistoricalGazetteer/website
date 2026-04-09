// /whg/webpack/js/layerSourcesPalette.js
/**
 * Layer Sources palette for the Atlas page.
 *
 * Renders toggleable source switches (OSM, OHM, PeriodO, etc.)
 * and admin-level boundary controls with zoom-based auto-switching.
 * Persists state in filterState.active_sources.
 */

import heroMap from './heroMap';

/* ── Admin tier definitions ── */
const ADMIN_TIERS = [
    { value: 'off', label: 'Off', adminLevel: null },
    { value: 'admin0', label: 'Continental', adminLevel: 0, minZoom: 0 },
    { value: 'admin1', label: 'Sub-Cont.', adminLevel: 1, minZoom: 0 },
    { value: 'admin2', label: 'Country', adminLevel: 2, minZoom: 2 },
    { value: 'admin3', label: 'State', adminLevel: 3, minZoom: 3.5 },
    { value: 'admin4', label: 'Province', adminLevel: 4, minZoom: 4.5 },
    { value: 'admin5', label: 'District', adminLevel: 5, minZoom: 5.5 },
    { value: 'admin6', label: 'County', adminLevel: 6, minZoom: 6.5 },
    { value: 'admin7', label: 'City', adminLevel: 7, minZoom: 7.5 },
    { value: 'admin8', label: 'Ward', adminLevel: 8, minZoom: 8.5 },
];

/* Zoom thresholds for auto admin-level switching */
const ZOOM_THRESHOLDS = [
    { maxZoom: 2.5, adminLevel: 2 },
    { maxZoom: 4,   adminLevel: 3 },
    { maxZoom: 5.5, adminLevel: 4 },
    { maxZoom: 7,   adminLevel: 6 },
    { maxZoom: 9,   adminLevel: 7 },
    { maxZoom: Infinity, adminLevel: 8 },
];

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
        this._currentAdminLevel = null;
        this._currentNamespace = 'osm';
        this._autoAdmin = true;
        this._boundariesVisible = false;
        this._init();
    }

    _init() {
        // Render source toggles
        let html = this._sources.map(s => `
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

        // Add admin-level section
        html += `
            <hr class="layer-panel-divider">
            <span class="layer-panel-section-label">Boundaries</span>
            <div class="admin-level-group btn-group btn-group-sm" role="group">
                ${ADMIN_TIERS.map(t => `
                    <button type="button"
                            class="btn${t.value === 'off' ? ' active' : ''}"
                            data-tier="${t.value}"
                            data-admin-level="${t.adminLevel ?? ''}"
                            title="${t.label}">
                        ${t.label}
                    </button>
                `).join('')}
            </div>
            <div class="namespace-toggle-panel btn-group btn-group-sm" role="group">
                <button type="button" class="btn active" data-ns="osm">Modern</button>
                <button type="button" class="btn" data-ns="ohm">Historical</button>
            </div>
            <div class="admin-auto-toggle">
                <div class="form-check form-switch">
                    <input class="form-check-input" type="checkbox" id="admin_auto_zoom" checked>
                    <label class="form-check-label" for="admin_auto_zoom">Auto by zoom</label>
                </div>
            </div>
        `;

        this._panel.innerHTML = html;

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

        // Wire admin tier buttons
        this._panel.querySelectorAll('.admin-level-group .btn').forEach(btn => {
            btn.addEventListener('click', () => {
                this._panel.querySelectorAll('.admin-level-group .btn')
                    .forEach(b => b.classList.remove('active', 'auto-selected'));
                btn.classList.add('active');
                const tier = btn.dataset.tier;
                if (tier === 'off') {
                    this._currentAdminLevel = null;
                    this._boundariesVisible = false;
                    heroMap.hideBoundaries();
                } else {
                    const def = ADMIN_TIERS.find(t => t.value === tier);
                    this._currentAdminLevel = def ? def.adminLevel : null;
                    this._boundariesVisible = true;
                    this._updateBoundaryFilter();
                }
                // Disable auto when user manually picks
                this._autoAdmin = false;
                const autoCheck = this._panel.querySelector('#admin_auto_zoom');
                if (autoCheck) autoCheck.checked = false;
            });
        });

        // Wire namespace toggle
        this._panel.querySelectorAll('.namespace-toggle-panel .btn').forEach(btn => {
            btn.addEventListener('click', () => {
                this._panel.querySelectorAll('.namespace-toggle-panel .btn')
                    .forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                this._currentNamespace = btn.dataset.ns;
                if (this._currentAdminLevel !== null) this._updateBoundaryFilter();
            });
        });

        // Wire auto-zoom toggle
        const autoCheck = this._panel.querySelector('#admin_auto_zoom');
        if (autoCheck) {
            autoCheck.addEventListener('change', () => {
                this._autoAdmin = autoCheck.checked;
                if (this._autoAdmin) {
                    this._applyZoomAutoLevel();
                }
            });
        }

        // Setup zoom-based auto-switching
        this._setupZoomAutoSwitch();

        // Close panel on outside click
        document.addEventListener('click', (e) => {
            if (!this._panel.contains(e.target) && !this._toggleBtn.contains(e.target)) {
                this._panel.style.display = 'none';
            }
        });
    }

    _updateBoundaryFilter() {
        if (this._currentAdminLevel === null) {
            heroMap.hideBoundaries();
            return;
        }
        const filters = ['all', ['==', ['get', 'admin_level'], this._currentAdminLevel]];
        if (this._currentNamespace === 'osm' && this._currentAdminLevel <= 1) {
            filters.push(['any',
                ['==', ['get', 'namespace'], 'osm'],
                ['==', ['get', 'namespace'], 'm49'],
            ]);
        } else {
            filters.push(['==', ['get', 'namespace'], this._currentNamespace]);
        }
        heroMap.showBoundaries(filters);
    }

    _setupZoomAutoSwitch() {
        heroMap.init().then(() => {
            heroMap.map.on('zoomend', () => {
                if (this._autoAdmin) {
                    this._applyZoomAutoLevel();
                }
            });
        });
    }

    _applyZoomAutoLevel() {
        if (!heroMap.map) return;
        const zoom = heroMap.map.getZoom();
        let targetLevel = null;
        for (const t of ZOOM_THRESHOLDS) {
            if (zoom < t.maxZoom) {
                targetLevel = t.adminLevel;
                break;
            }
        }
        if (targetLevel !== this._currentAdminLevel) {
            this._currentAdminLevel = targetLevel;
            this._boundariesVisible = targetLevel !== null;

            // Update button highlight
            this._panel.querySelectorAll('.admin-level-group .btn')
                .forEach(b => b.classList.remove('active', 'auto-selected'));
            if (targetLevel !== null) {
                const tier = ADMIN_TIERS.find(t => t.adminLevel === targetLevel);
                if (tier) {
                    const btn = this._panel.querySelector(`.admin-level-group .btn[data-tier="${tier.value}"]`);
                    if (btn) btn.classList.add('auto-selected');
                }
                this._updateBoundaryFilter();
            } else {
                const offBtn = this._panel.querySelector('.admin-level-group .btn[data-tier="off"]');
                if (offBtn) offBtn.classList.add('active');
                heroMap.hideBoundaries();
            }
        }
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

    /** Get current admin level. */
    getAdminLevel() {
        return this._currentAdminLevel;
    }

    /** Get current namespace. */
    getNamespace() {
        return this._currentNamespace;
    }

    /** Reset admin level to off. */
    resetAdminLevel() {
        this._currentAdminLevel = null;
        this._boundariesVisible = false;
        this._autoAdmin = true;
        heroMap.hideBoundaries();
        this._panel.querySelectorAll('.admin-level-group .btn')
            .forEach(b => b.classList.remove('active', 'auto-selected'));
        const offBtn = this._panel.querySelector('.admin-level-group .btn[data-tier="off"]');
        if (offBtn) offBtn.classList.add('active');
        const autoCheck = this._panel.querySelector('#admin_auto_zoom');
        if (autoCheck) autoCheck.checked = true;
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

