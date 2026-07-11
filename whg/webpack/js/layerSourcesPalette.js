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

/* Zoom thresholds for auto admin-level switching.
 * Globe projection uses lower zoom numbers — Continental features
 * need to be visible at typical globe-view zooms (~2–3), not only
 * when fully zoomed out. */
const ZOOM_THRESHOLDS = [
    { maxZoom: 2.5,      adminLevel: 0 },
    { maxZoom: 3.2,      adminLevel: 1 },
    { maxZoom: 4.0,      adminLevel: 2 },
    { maxZoom: 5.0,      adminLevel: 3 },
    { maxZoom: 6.5,      adminLevel: 4 },
    { maxZoom: 8,        adminLevel: 6 },
    { maxZoom: 10,       adminLevel: 7 },
    { maxZoom: Infinity, adminLevel: 8 },
];

/* Sources that support admin-tier filtering (the Boundary Level dropdown).
   osm_misc, po, clio, nl are boundary-bearing but untiered: their
   ``boundary`` field carries tag values or single class strings, not the
   string admin levels "0".."11" that osm/ohm use. */
const TIERED_SOURCES = new Set(['osm', 'ohm']);

/* Map a gazetteer / palette source id to the boundary tile source-layer
   in the whg-context style. After the OSM/OHM tileset rename the mapping
   is identity except for WHG-namespaced ids: ``whg:892`` becomes
   ``whg-892`` to match the tileserver naming. */
function tileSourceFor(sourceId) {
    return (sourceId || '').replace(':', '-');
}

/* Tooltip descriptions are now carried per-row by the registry-driven
   data feed (``available_sources`` in search/views.py::AtlasPageView).
   Each row provides a ``description`` string used as the tooltip text. */

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
        this._currentNamespace = this._inferNamespace(this._activeSource);

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
            const tooltip = s.description || s.label;
            const checked = s.id === this._activeSource ? 'checked' : '';
            const disabled = s.enabled === false ? 'disabled' : '';
            html += `
                <div class="layer-source-item">
                    <div class="form-check">
                        <input class="form-check-input layer-source-radio" type="radio"
                               name="layer_source" id="layer_src_${s.id}" value="${s.id}"
                               ${checked} ${disabled}>
                        <label class="form-check-label" for="layer_src_${s.id}"
                               data-bs-toggle="tooltip" data-bs-placement="right"
                               data-bs-title="${tooltip}">
                            ${s.label}
                        </label>
                    </div>
                </div>`;
        });

        // ── Boundary level section (hidden by default, shown for OSM/OHM) ──
        html += `
            <div id="boundary_level_section"
                 style="${TIERED_SOURCES.has(this._activeSource) ? '' : 'display:none'}">
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
                this._currentNamespace = this._inferNamespace(this._activeSource);

                const isTiered = TIERED_SOURCES.has(this._activeSource);
                const boundarySection = this._panel.querySelector('#boundary_level_section');
                if (boundarySection) {
                    boundarySection.style.display = isTiered ? '' : 'none';
                }

                if (isTiered) {
                    // Apply the admin-tier boundary filter — auto-pick by zoom
                    // if auto is on, otherwise reuse the current admin level.
                    if (this._autoAdmin) {
                        this._applyZoomAutoLevel();
                    } else if (this._currentAdminLevel != null) {
                        this._updateBoundaryFilter();
                    }
                } else {
                    // Untiered: show every feature in this source's tileset.
                    this._currentAdminLevel = null;
                    this._boundariesVisible = true;
                    heroMap.showBoundaries({ source: tileSourceFor(this._activeSource) });
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
    /*  Namespace inference                                             */
    /* ──────────────────────────────────────────────────────────────── */

    /** Map source id to the namespace used by boundary tiles. Identity:
     *  the layer-sources palette and the registry use the same id strings,
     *  so the namespace is just the source id. */
    _inferNamespace(sourceId) {
        return sourceId || 'osm';
    }

    /* ──────────────────────────────────────────────────────────────── */
    /*  Boundary filter                                                */
    /* ──────────────────────────────────────────────────────────────── */

    _updateBoundaryFilter() {
        if (this._currentAdminLevel === null) {
            heroMap.hideBoundaries();
            return;
        }
        // Only tiered sources reach this path (osm/ohm). Untiered sources
        // render via the radio change handler with no boundaryValues.
        heroMap.showBoundaries({
            source: tileSourceFor(this._activeSource),
            boundaryValues: [String(this._currentAdminLevel)],
        });
    }

    /* ──────────────────────────────────────────────────────────────── */
    /*  Auto zoom-level switching                                      */
    /* ──────────────────────────────────────────────────────────────── */

    _setupZoomAutoSwitch() {
        heroMap.init().then(() => {
            heroMap.map.on('zoomend', () => {
                if (this._autoAdmin && TIERED_SOURCES.has(this._activeSource)) {
                    this._applyZoomAutoLevel();
                }
            });
        });
    }

    _applyZoomAutoLevel() {
        if (!heroMap.map) return;
        if (!TIERED_SOURCES.has(this._activeSource)) return;

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

    /**
     * Programmatically select a source by id. If the id matches a registered
     * Region Source radio, ticks it and runs the same change path the user
     * would trigger by clicking. If the id has no matching radio (e.g. a
     * specialist gazetteer's tileset like ``whg-892``), unticks the radios
     * and applies the tileset directly via ``heroMap.showBoundaries``.
     *
     * Used by the Atlas Gazetteers offcanvas to mirror Explore-mode selections
     * onto the map.
     */
    setActiveSource(id) {
        if (!id) return;
        const radio = this._panel.querySelector(`.layer-source-radio[value="${id}"]`);
        if (radio) {
            if (!radio.checked) radio.checked = true;
            radio.dispatchEvent(new Event('change'));
            return;
        }
        // Untick any currently-checked Region Source radio so the UI doesn't
        // keep claiming a source the map is no longer showing.
        this._panel.querySelectorAll('.layer-source-radio').forEach(r => { r.checked = false; });
        const boundarySection = this._panel.querySelector('#boundary_level_section');
        if (boundarySection) boundarySection.style.display = 'none';
        this._activeSource = id;
        this._activeSources = [id];
        this._currentNamespace = id;
        this._currentAdminLevel = null;
        this._boundariesVisible = true;
        // Tileset isn't in the base style — load it on demand via the generic
        // gazetteer-style loader.
        heroMap.showGazetteer(tileSourceFor(id));
        this._onSourcesChange();
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
        const nsMap = { osm: 'osm', ohm: 'ohm', osm_misc: 'osm_misc' };
        return this._activeSources
            .filter(s => s in nsMap)
            .map(s => nsMap[s]);
    }
}

