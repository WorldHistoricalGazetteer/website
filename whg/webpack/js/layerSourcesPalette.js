// /whg/webpack/js/layerSourcesPalette.js
/**
 * Layer Sources palette for the Atlas page.
 *
 * Renders radio-button source selector (OSM, OHM, PeriodO, etc.)
 * and a compact boundary-level dropdown (visible only when a
 * boundary source — OSM or OHM — is selected).
 *
 * Namespace (osm/ohm) is inferred from the selected source,
 * eliminating the separate Modern / Historical toggle.
 */

import heroMap from './heroMap';

/* ── Admin tier definitions ── */
const ADMIN_TIERS = [
    { value: 'off', label: 'Off',          adminLevel: null },
    { value: '0',   label: 'Continental',   adminLevel: 0 },
    { value: '1',   label: 'Sub-Cont.',     adminLevel: 1 },
    { value: '2',   label: 'Country',       adminLevel: 2 },
    { value: '3',   label: 'State',         adminLevel: 3 },
    { value: '4',   label: 'Province',      adminLevel: 4 },
    { value: '5',   label: 'District',      adminLevel: 5 },
    { value: '6',   label: 'County',        adminLevel: 6 },
    { value: '7',   label: 'City',          adminLevel: 7 },
    { value: '8',   label: 'Ward',          adminLevel: 8 },
];

/* Zoom thresholds for auto admin-level switching */
const ZOOM_THRESHOLDS = [
    { maxZoom: 1.2,      adminLevel: 0 },
    { maxZoom: 2.0,      adminLevel: 1 },
    { maxZoom: 2.5,      adminLevel: 2 },
    { maxZoom: 4,        adminLevel: 3 },
    { maxZoom: 5.5,      adminLevel: 4 },
    { maxZoom: 7,        adminLevel: 6 },
    { maxZoom: 9,        adminLevel: 7 },
    { maxZoom: Infinity, adminLevel: 8 },
];

/* Sources that support boundary display */
const BOUNDARY_SOURCES = new Set(['osm', 'ohm']);

/* Tooltip descriptions for known sources */
const SOURCE_TOOLTIPS = {
    osm:        'OpenStreetMap — modern administrative boundaries and geographic features',
    ohm:        'OpenHistoricalMap — community-contributed historical boundaries and features',
    periodo:    'PeriodO — periods, events, and temporalities',
    cliopatria: 'Cliopatria — historical political entities',
    dplace:     'D-PLACE — cross-cultural linguistic and geographic datasets',
    nativeland: 'NativeLand — indigenous territories and languages',
};

export default class LayerSourcesPalette {
    /**
     * @param {string} panelSelector  – CSS selector for the panel container
     * @param {string} _toggleSelector – (unused, kept for API compat)
     * @param {Array}  sources        – [{id, label, enabled, coming_soon?}]
     */
    constructor(panelSelector, _toggleSelector, sources) {
        this._panel = document.querySelector(panelSelector);
        this._sources = sources || [];

        // With radio buttons only one source is active at a time
        const enabledSource = this._sources.find(s => s.enabled);
        this._activeSource = enabledSource ? enabledSource.id : (this._sources[0]?.id || 'osm');
        this._activeSources = [this._activeSource];

        // Namespace is inferred from the selected source
        this._currentNamespace = this._activeSource === 'ohm' ? 'ohm' : 'osm';

        this._currentAdminLevel = null;
        this._autoAdmin = true;
        this._boundariesVisible = false;

        this._init();
    }

    /* ──────────────────────────────────────────────────────────────── */
    /*  Init                                                           */
    /* ──────────────────────────────────────────────────────────────── */

    _init() {
        // ── Source radio buttons ──
        let html = '<span class="layer-panel-section-label">Source</span>';

        this._sources.forEach(s => {
            const tooltip = SOURCE_TOOLTIPS[s.id] || s.label;
            const checked = s.id === this._activeSource ? 'checked' : '';
            const disabled = s.coming_soon ? 'disabled' : '';
            html += `
                <div class="layer-source-item">
                    <div class="form-check">
                        <input class="form-check-input layer-source-radio" type="radio"
                               name="layer_source" id="layer_src_${s.id}" value="${s.id}"
                               ${checked} ${disabled}>
                        <label class="form-check-label" for="layer_src_${s.id}"
                               data-bs-toggle="tooltip" data-bs-placement="right"
                               title="${tooltip}">
                            ${s.label}
                            ${s.coming_soon ? '<span class="coming-soon">(coming soon)</span>' : ''}
                        </label>
                    </div>
                </div>`;
        });

        // ── Boundary level section (hidden by default, shown for OSM/OHM) ──
        html += `
            <div id="boundary_level_section"
                 style="${BOUNDARY_SOURCES.has(this._activeSource) ? '' : 'display:none'}">
                <hr class="layer-panel-divider">
                <span class="layer-panel-section-label">Boundaries</span>
                <p class="layer-panel-help-text">
                    Set the administrative level of boundaries displayed on the map.
                    Click a boundary polygon to select it as a region constraint.
                </p>
                <select id="boundary_level_select" class="form-select form-select-sm">
                    ${ADMIN_TIERS.map(t =>
                        `<option value="${t.value}">${t.label}${t.adminLevel != null ? ' (' + t.adminLevel + ')' : ''}</option>`
                    ).join('')}
                </select>
                <div class="admin-auto-toggle">
                    <div class="form-check form-switch">
                        <input class="form-check-input" type="checkbox" id="admin_auto_zoom" checked>
                        <label class="form-check-label" for="admin_auto_zoom">Auto by zoom</label>
                    </div>
                </div>
                <p class="layer-panel-help-text" style="margin-top:4px;">
                    When <em>Auto by zoom</em> is on, the boundary level adjusts
                    automatically as you zoom in or out — showing countries at
                    wide zoom, provinces at mid zoom, and districts when zoomed in.
                </p>
            </div>`;

        this._panel.innerHTML = html;

        // ── Initialise Bootstrap tooltips ──
        this._panel.querySelectorAll('[data-bs-toggle="tooltip"]').forEach(el => {
            new bootstrap.Tooltip(el, { trigger: 'hover' });
        });


        // ── Wire source radio buttons ──
        this._panel.querySelectorAll('.layer-source-radio').forEach(radio => {
            radio.addEventListener('change', () => {
                if (!radio.checked) return;
                this._activeSource = radio.value;
                this._activeSources = [this._activeSource];
                this._currentNamespace = this._activeSource === 'ohm' ? 'ohm' : 'osm';

                // Show/hide boundary section
                const boundarySection = this._panel.querySelector('#boundary_level_section');
                if (boundarySection) {
                    const show = BOUNDARY_SOURCES.has(this._activeSource);
                    boundarySection.style.display = show ? '' : 'none';
                    if (!show && this._boundariesVisible) {
                        this._currentAdminLevel = null;
                        this._boundariesVisible = false;
                        heroMap.hideBoundaries();
                    } else if (show && this._autoAdmin) {
                        this._applyZoomAutoLevel();
                    }
                }

                this._onSourcesChange();
            });
        });

        // ── Wire boundary level dropdown ──
        const select = this._panel.querySelector('#boundary_level_select');
        if (select) {
            select.addEventListener('change', () => {
                const val = select.value;
                if (val === 'off') {
                    this._currentAdminLevel = null;
                    this._boundariesVisible = false;
                    heroMap.hideBoundaries();
                } else {
                    this._currentAdminLevel = parseInt(val, 10);
                    this._boundariesVisible = true;
                    this._updateBoundaryFilter();
                }
                // Disable auto when user manually picks
                this._autoAdmin = false;
                const autoCheck = this._panel.querySelector('#admin_auto_zoom');
                if (autoCheck) autoCheck.checked = false;
            });
        }

        // ── Wire auto-zoom toggle ──
        const autoCheck = this._panel.querySelector('#admin_auto_zoom');
        if (autoCheck) {
            autoCheck.addEventListener('change', () => {
                this._autoAdmin = autoCheck.checked;
                if (this._autoAdmin) {
                    this._applyZoomAutoLevel();
                }
            });
        }

        // ── Setup zoom-based auto-switching ──
        this._setupZoomAutoSwitch();
    }

    /* ──────────────────────────────────────────────────────────────── */
    /*  Boundary filter                                                */
    /* ──────────────────────────────────────────────────────────────── */

    _updateBoundaryFilter() {
        if (this._currentAdminLevel === null) {
            heroMap.hideBoundaries();
            return;
        }
        const filters = ['all', ['==', ['get', 'admin_level'], this._currentAdminLevel]];

        // For admin levels 0–1 with OSM, include m49 namespace
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

    /* ──────────────────────────────────────────────────────────────── */
    /*  Auto zoom-level switching                                      */
    /* ──────────────────────────────────────────────────────────────── */

    _setupZoomAutoSwitch() {
        heroMap.init().then(() => {
            heroMap.map.on('zoomend', () => {
                if (this._autoAdmin && BOUNDARY_SOURCES.has(this._activeSource)) {
                    this._applyZoomAutoLevel();
                }
            });
        });
    }

    _applyZoomAutoLevel() {
        if (!heroMap.map) return;
        if (!BOUNDARY_SOURCES.has(this._activeSource)) return;

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

            // Update dropdown to reflect auto-selected level
            const select = this._panel.querySelector('#boundary_level_select');
            if (select) {
                select.value = targetLevel !== null ? String(targetLevel) : 'off';
            }

            if (targetLevel !== null) {
                this._updateBoundaryFilter();
            } else {
                heroMap.hideBoundaries();
            }
        }
    }

    /* ──────────────────────────────────────────────────────────────── */
    /*  Source change callback                                         */
    /* ──────────────────────────────────────────────────────────────── */

    _onSourcesChange() {
        heroMap.setActiveSources(this._activeSources);
        document.dispatchEvent(new CustomEvent('layer-sources-change', {
            detail: { activeSources: this._activeSources },
        }));
    }

    /* ──────────────────────────────────────────────────────────────── */
    /*  Public API                                                     */
    /* ──────────────────────────────────────────────────────────────── */

    /** Get currently active source IDs (single-element array). */
    getActiveSources() {
        return [...this._activeSources];
    }

    /** Check if a given source is active. */
    isActive(sourceId) {
        return this._activeSources.includes(sourceId);
    }

    /** Get current admin level (number or null). */
    getAdminLevel() {
        return this._currentAdminLevel;
    }

    /** Get current namespace (inferred from selected source). */
    getNamespace() {
        return this._currentNamespace;
    }

    /** Reset admin level to off, re-enable auto. */
    resetAdminLevel() {
        this._currentAdminLevel = null;
        this._boundariesVisible = false;
        this._autoAdmin = true;
        heroMap.hideBoundaries();

        const select = this._panel.querySelector('#boundary_level_select');
        if (select) select.value = 'off';
        const autoCheck = this._panel.querySelector('#admin_auto_zoom');
        if (autoCheck) autoCheck.checked = true;
    }

    /** Re-apply the current boundary filter (show or hide based on current admin level). */
    refreshBoundaries() {
        this._updateBoundaryFilter();
    }

    /**
     * Get the namespace value(s) for boundary queries.
     * Maps active source to namespace used by boundary tiles.
     */
    getActiveNamespaces() {
        const nsMap = { osm: 'osm', ohm: 'ohm' };
        return this._activeSources
            .filter(s => s in nsMap)
            .map(s => nsMap[s]);
    }
}

