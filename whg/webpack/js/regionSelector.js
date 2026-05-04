// /whg/webpack/js/regionSelector.js
/**
 * Region selector (Tab 1: Timespan + Map).
 *
 * Supports **multi-selection**: selecting a region adds it as a
 * chip/badge.  Multiple regions can be selected (even across tiers).
 * "Off" and "Map bounds" clear all selections.  Switching between
 * other tiers preserves the selection list.
 *
 * **Boundary interaction**: When an admin-level tier is active,
 * boundary polygons for that exact level are shown on the map.
 * Hovering highlights; clicking selects.  The search input queries
 * boundary features from loaded vector tiles by name.
 *
 * **Zoom gate on tier buttons**: All tier buttons except "Off" are
 * disabled until the context map is zoomed past the threshold.
 *
 * **Modern / Historical toggle**: Controls whether OSM (modern) or
 * OHM (historical) boundaries are shown.  Defaults to Modern.
 *
 * Tiers:
 *   Off              — no spatial region constraint
 *   Map bounds       — use current viewport as explicit spatial filter
 *   Continental      — admin level 0 (M49 regions + Antarctica)
 *   Sub-Continental  — admin level 1 (M49 sub-regions)
 *   Country          — admin level 2
 *   State            — admin level 3
 *   Province         — admin level 4
 *   District         — admin level 5
 *   County           — admin level 6
 *   City             — admin level 7
 *   Ward             — admin level 8
 */

import filterState from './filterState';
import contextMap from './contextMap';
import debounce from 'lodash/debounce';

/* ───────────────────────────────────────────────────────────────────
   Tier definitions
   ─────────────────────────────────────────────────────────────────── */

/** All tier options shown in the toggle button group. */
const ALL_TIERS = [
    { value: 'off',            label: 'Off' },
    { value: 'mapbounds',      label: 'Map bounds' },
    { value: 'admin0',         label: 'Continental',     adminLevel: 0 },
    { value: 'admin1',         label: 'Sub-Continental', adminLevel: 1 },
    { value: 'admin2',         label: 'Country',         adminLevel: 2 },
    { value: 'admin3',         label: 'State',           adminLevel: 3 },
    { value: 'admin4',         label: 'Province',        adminLevel: 4 },
    { value: 'admin5',         label: 'District',        adminLevel: 5 },
    { value: 'admin6',         label: 'County',          adminLevel: 6 },
    { value: 'admin7',         label: 'City',            adminLevel: 7 },
    { value: 'admin8',         label: 'Ward',            adminLevel: 8 },
];

/** Namespace options: Modern (OSM) vs Historical (OHM) vs Miscellaneous. */
const NAMESPACE_OPTIONS = [
    { value: 'osm', label: 'Modern', title: 'Modern administrative boundaries from OpenStreetMap' },
    { value: 'ohm', label: 'Historical', title: 'Historical boundaries from OpenHistoricalMap' },
    { value: 'osm_misc', label: 'Misc.', title: 'OSM/OHM miscellaneous boundary types — includes aboriginal lands, baronies, civil and political boundaries, climatic zones, geographic regions, historical and obsolete administrative divisions, indigenous territories, parishes, and other non-standard boundary classifications' },
];

/** Namespace toggle value → boundary tile source-layer (= source ID). */
const NAMESPACE_TO_SOURCE = {
    osm: 'osm_admin',
    ohm: 'ohm_admin',
    osm_misc: 'osm_misc',
};

/* ───── Helpers ───── */

/** Is the tier one of the admin-boundary tiers? */
function isAdminTier(tier) {
    return tier.startsWith('admin');
}

/** Get the admin_level number for a tier value. */
function getAdminLevel(tier) {
    const def = ALL_TIERS.find(t => t.value === tier);
    return def ? def.adminLevel : null;
}

/** Tiers that should be gated behind map zoom (everything except 'off'). */
function isZoomGatedTier(tier) {
    return tier !== 'off';
}

/** Parse a `boundary` field value to a numeric admin level, or null. */
function parseAdminLevel(boundary) {
    if (boundary == null) return null;
    const n = parseInt(String(boundary), 10);
    return Number.isFinite(n) ? n : null;
}

/* ═══════════════════════════════════════════════════════════════════ */

export default class RegionSelector {
    /**
     * @param {string|HTMLElement} container - CSS selector or element for the widget
     */
    constructor(container) {
        this.$el = typeof container === 'string' ? document.querySelector(container) : container;
        this._selectedRegions = [];   // Array of {id, label, admin_level, namespace, geometry}
        this._results = [];
        this._currentTier = 'off';
        this._currentNamespace = 'osm';  // Default to Modern (OSM)
        this._tiersEnabled = false;      // Zoom gate state
        this._nsMountPoint = null;       // External mount point for namespace toggle
        this._init();
    }

    /* ── Build DOM ── */

    _init() {
        this.$el.innerHTML = `
            <div class="region-selector">
                <div class="region-tier-toggle btn-group btn-group-sm flex-wrap mb-2" role="group" aria-label="Spatial region tier">
                    ${ALL_TIERS.map((t, i) => {
                        const isActive = i === 0;
                        const isZoomGated = isZoomGatedTier(t.value);
                        const disabled = isZoomGated;
                        const tooltip = t.adminLevel != null
                            ? `Admin level ${t.adminLevel}`
                            : t.label;
                        const classes = ['btn'];
                        if (isActive) classes.push('active');
                        if (isZoomGated) classes.push('zoom-gated-tier');
                        return `<button type="button" class="${classes.join(' ')}"
                            data-tier="${t.value}" data-bs-toggle="tooltip" title="${tooltip}"
                            ${disabled ? 'disabled' : ''}>${t.label}</button>`;
                    }).join('')}
                </div>
                <div class="region-input-wrap position-relative">
                    <input type="text" class="form-control form-control-sm region-search-input"
                           placeholder="Zoom the map first to constrain your search area" disabled
                           autocomplete="off" spellcheck="false">
                    <div class="region-dropdown"></div>
                </div>
                <div class="region-chip-area mt-1"></div>
            </div>
        `;

        this._input        = this.$el.querySelector('.region-search-input');
        this._inputWrap    = this.$el.querySelector('.region-input-wrap');
        this._dropdown     = this.$el.querySelector('.region-dropdown');
        this._chipArea     = this.$el.querySelector('.region-chip-area');

        // ── Mount namespace toggle externally (on the "space" subtitle line) ──
        this._nsMountPoint = document.getElementById('namespace_toggle_mount');
        if (this._nsMountPoint) {
            this._nsMountPoint.innerHTML = `
                <span class="namespace-toggle btn-group" role="group" aria-label="Boundary era">
                    ${NAMESPACE_OPTIONS.map(o => `
                        <button type="button" class="btn${o.value === this._currentNamespace ? ' active' : ''}"
                                data-namespace="${o.value}"
                                ${o.title ? `data-bs-toggle="tooltip" title="${o.title}"` : ''}>${o.label}</button>
                    `).join('')}
                </span>
            `;
            this._nsMountPoint.querySelectorAll('.namespace-toggle .btn').forEach(btn => {
                btn.addEventListener('click', () => {
                    this._nsMountPoint.querySelectorAll('.namespace-toggle .btn').forEach(b => b.classList.remove('active'));
                    btn.classList.add('active');
                    this._currentNamespace = btn.dataset.namespace;
                    this._updateBoundaryFilter();
                });
            });
        }

        // Wire tier toggle buttons
        this.$el.querySelectorAll('.region-tier-toggle .btn').forEach(btn => {
            btn.addEventListener('click', () => {
                if (btn.disabled) return;
                this.$el.querySelectorAll('.region-tier-toggle .btn').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                this._onTierChange(btn.dataset.tier);
            });
        });

        // Debounced search
        this._debouncedSearch = debounce(() => this._search(), 250);
        this._input.addEventListener('input', () => this._debouncedSearch());

        // Keyboard navigation
        this._input.addEventListener('keydown', (e) => this._onKeydown(e));

        // Close dropdown on outside click
        document.addEventListener('click', (e) => {
            if (!this.$el.contains(e.target)) this._closeDropdown();
        });

        // Listen for boundary-click events from the context map
        document.addEventListener('boundary-click', (e) => {
            this._onBoundaryClick(e.detail);
        });

        this._activeIndex = -1;
    }

    /* ── Tier change ── */

    _onTierChange(tier) {
        this._currentTier = tier;

        if (tier === 'off') {
            if (this._tiersEnabled) {
                this._inputWrap.style.display = 'none';
            }
            this._clearAllSelections();
            contextMap.hideBoundaries();
            filterState.set('spatial.geometry_source', 'none');
        } else if (tier === 'mapbounds') {
            if (this._tiersEnabled) {
                this._inputWrap.style.display = 'none';
            }
            this._clearAllSelections();
            contextMap.hideBoundaries();
            filterState.set('spatial.geometry_source', 'mapbounds');
        } else if (isAdminTier(tier)) {
            // Show search input and boundaries for this admin level
            this._inputWrap.style.display = '';
            this._input.placeholder = 'Search for a region\u2026';
            this._input.value = '';
            this._closeDropdown();
            this._updateBoundaryFilter();
        }
    }

    /* ── Build and apply the boundary filter for the current tier + namespace ── */

    _updateBoundaryFilter() {
        const tier = this._currentTier;
        if (!isAdminTier(tier)) {
            contextMap.hideBoundaries();
            return;
        }
        const adminLevel = getAdminLevel(tier);
        if (adminLevel == null) {
            contextMap.hideBoundaries();
            return;
        }

        // Map the namespace toggle (osm | ohm | osm_misc) to the boundary
        // tile source. For osm/ohm the tier picks a single `boundary` value
        // ("0"–"8"); the synthetic continental/sub-continental rows live
        // under the osm namespace as place_ids "osm:m49_*".
        const source = NAMESPACE_TO_SOURCE[this._currentNamespace] || 'osm_admin';
        if (source === 'osm_misc') {
            // osm_misc records carry tag-named `boundary` values, not admin
            // levels. Tier doesn't apply — show everything in the source.
            contextMap.showBoundaries({ source });
            return;
        }
        contextMap.showBoundaries({ source, boundaryValues: [String(adminLevel)] });
    }

    /* ── Handle boundary click from the context map ── */

    _onBoundaryClick(detail) {
        if (!detail || !detail.geometry) return;
        // Only handle when an admin tier is active
        if (!isAdminTier(this._currentTier)) return;

        // The new tilesets carry a string `boundary` field ("0"–"11" for
        // admin tiers); convert back to the numeric admin level the chip
        // and filterState expect.
        const adminLevel = parseAdminLevel(detail.boundary);
        const id = detail.place_id
            || `boundary:${detail.namespace || 'osm'}:${detail.id || detail.name}`;
        this._addBoundaryRegion({
            id,
            label: detail.name || 'Unnamed',
            admin_level: adminLevel,
            namespace: detail.namespace || 'osm',
            geometry: detail.geometry,
        });
    }

    /* ── Selection helpers ── */

    _clearAllSelections() {
        this._selectedRegions = [];
        this._chipArea.innerHTML = '';
        this._input.value = '';
        this._closeDropdown();
        filterState.set('spatial.region_id', []);
        filterState.set('spatial.preview_geo', null);
        contextMap.clearOverlay();
        contextMap.clearSuggestions();
        contextMap.hideBoundaries();
    }

    _addBoundaryRegion(item) {
        // Don't add duplicates
        if (this._selectedRegions.some(r => r.id === item.id)) return;

        this._selectedRegions.push(item);
        this._input.value = '';
        this._closeDropdown();

        // Update filter state
        filterState.addToList('spatial.region_id', item);
        filterState.set('spatial.geometry_source', item.namespace === 'ohm' ? 'ohm' : 'osm');

        // Re-render all chips
        this._renderChips();

        // Show the selected polygon(s) on the overlay and fly to it
        this._updateSelectionOverlay();
        if (item.geometry) {
            contextMap.fitTo({ type: 'Feature', geometry: item.geometry, properties: {} });
        }

        console.log('RegionSelector: boundary region added:', item.label,
            `(admin_level ${item.admin_level}, namespace ${item.namespace})`);
    }

    /** Combine all selected region geometries onto the overlay. */
    _updateSelectionOverlay() {
        const features = this._selectedRegions
            .filter(r => r.geometry)
            .map(r => ({
                type: 'Feature',
                geometry: r.geometry,
                properties: { id: r.id, label: r.label },
            }));

        if (features.length > 0) {
            contextMap.setOverlay({ type: 'FeatureCollection', features });
        } else {
            contextMap.clearOverlay();
        }
    }

    _removeRegion(id) {
        this._selectedRegions = this._selectedRegions.filter(r => r.id !== id);
        filterState.removeFromList('spatial.region_id', id);

        this._renderChips();

        if (this._selectedRegions.length === 0) {
            filterState.set('spatial.preview_geo', null);
            contextMap.clearOverlay();
            // Revert geometry_source — the tier stays active but no region is selected
            if (this._currentTier !== 'mapbounds') {
                filterState.set('spatial.geometry_source', 'none');
            }
        } else {
            this._updateSelectionOverlay();
        }
    }

    _selectResult(index) {
        const item = this._results[index];
        if (!item) return;

        if (item._boundary) {
            // If result came from ES index search (no geometry), zoom to bounds
            // so the user can then click the polygon on the map.
            if (item._fromIndex && !item.geometry && item.bounds) {
                const [west, south, east, north] = item.bounds;
                try {
                    contextMap.map.fitBounds([[west, south], [east, north]], {
                        padding: 40,
                        maxZoom: 8,
                    });
                } catch (e) {
                    console.warn('RegionSelector: could not zoom to bounds', e);
                }
                this._input.value = '';
                this._closeDropdown();
                return;
            }

            this._addBoundaryRegion({
                id: item.id,
                label: item.label,
                admin_level: item.properties?.admin_level || item.admin_level,
                namespace: item.properties?.namespace || item.namespace || 'osm',
                geometry: item.geometry,
            });
            return;
        }
    }

    /* ── Search / filter ── */

    async _search() {
        const query = this._input.value.trim();
        if (query.length < 2) {
            this._closeDropdown();
            return;
        }

        const tier = this._currentTier;

        if (isAdminTier(tier)) {
            const adminLevel = getAdminLevel(tier);
            const boundaryValue = String(adminLevel);

            // 1. Query the ES `places` index for boundary-flagged docs.
            try {
                const params = new URLSearchParams({
                    q: query,
                    boundary: boundaryValue,
                    limit: '20',
                });
                if (this._currentNamespace && this._currentNamespace !== 'osm_misc') {
                    params.set('namespace', this._currentNamespace);
                }
                const resp = await fetch(`/search/boundaries/?${params}`);
                if (resp.ok) {
                    const data = await resp.json();
                    if (data.results && data.results.length > 0) {
                        this._results = data.results.map(r => ({
                            id: r.id || `boundary:${r.namespace}:${r.name}`,
                            label: r.name,
                            sublabel: `Level ${r.boundary} \u00b7 ${(r.namespace || 'osm').toUpperCase()}`
                                + (r.ccodes && r.ccodes.length ? ` \u00b7 ${r.ccodes.join(', ')}` : ''),
                            bounds: r.bounds,
                            repr_point: r.repr_point,
                            boundary: r.boundary,
                            namespace: r.namespace || 'osm',
                            // No geometry returned \u2014 user clicks the polygon on
                            // the map after the search zooms to bounds.
                            geometry: null,
                            _boundary: true,
                            _fromIndex: true,
                        }));
                        this._renderDropdown(this._results);
                        return;
                    }
                }
            } catch (e) {
                console.warn('RegionSelector: ES boundary search failed, falling back to tiles', e);
            }

            // 2. Fall back to tile-based search across loaded/rendered features.
            const tileFeatures = contextMap.searchBoundaryFeatures(query, [boundaryValue]);

            if (tileFeatures.length > 0) {
                this._results = tileFeatures.map(f => ({
                    id: f.properties.place_id
                        || `boundary:${f.properties.namespace || 'osm'}:${f.properties.name}`,
                    label: f.properties.name || 'Unnamed',
                    sublabel: `Level ${f.properties.boundary || '?'} \u00b7 ${(f.properties.namespace || 'osm').toUpperCase()}`,
                    geometry: f.geometry,
                    properties: f.properties,
                    _boundary: true,
                }));
                this._renderDropdown(this._results);
                return;
            }

            // 3. No results from either source
            this._results = [];
            this._renderDropdown([{
                _stub: true,
                label: 'No matching regions found',
                sublabel: 'Try a different name or zoom/pan the map',
            }]);
            return;
        }

        // Non-admin tiers don't support search
        this._closeDropdown();
    }

    /* ── Dropdown rendering ── */

    _renderDropdown(items) {
        this._activeIndex = -1;
        if (!items.length) {
            this._closeDropdown();
            return;
        }

        this._dropdown.innerHTML = items.map((item, i) => `
            <div class="region-result ${item._stub ? 'region-result--stub' : ''}" data-index="${i}">
                <div class="region-result-label">${item.label || item.name || ''}</div>
                ${item.sublabel ? `<div class="region-result-sublabel">${item.sublabel}</div>` : ''}
            </div>
        `).join('');

        this._dropdown.style.display = 'block';

        // Click handlers
        this._dropdown.querySelectorAll('.region-result:not(.region-result--stub)').forEach(el => {
            el.addEventListener('mousedown', (e) => {
                e.preventDefault();
                this._selectResult(parseInt(el.dataset.index));
            });
        });
    }

    _closeDropdown() {
        this._dropdown.style.display = 'none';
        this._dropdown.innerHTML = '';
        this._activeIndex = -1;
    }

    /* ── Keyboard navigation ── */

    _onKeydown(e) {
        const items = this._dropdown.querySelectorAll('.region-result:not(.region-result--stub)');
        if (!items.length) return;

        if (e.key === 'ArrowDown') {
            e.preventDefault();
            this._activeIndex = Math.min(this._activeIndex + 1, items.length - 1);
            this._highlightActive(items);
        } else if (e.key === 'ArrowUp') {
            e.preventDefault();
            this._activeIndex = Math.max(this._activeIndex - 1, 0);
            this._highlightActive(items);
        } else if (e.key === 'Enter' && this._activeIndex >= 0) {
            e.preventDefault();
            this._selectResult(this._activeIndex);
        } else if (e.key === 'Escape') {
            this._closeDropdown();
        }
    }

    _highlightActive(items) {
        items.forEach((el, i) => {
            el.classList.toggle('region-result--active', i === this._activeIndex);
        });
    }

    /* ── Chips (multi-select) ── */

    _renderChips() {
        if (this._selectedRegions.length === 0) {
            this._chipArea.innerHTML = '';
            return;
        }

        this._chipArea.innerHTML = this._selectedRegions.map(item => `
            <span class="filter-chip" data-region-id="${item.id}">
                <i class="fas fa-vector-square me-1"></i>
                ${item.label}
                ${item.admin_level ? `<span class="filter-chip-meta">(level ${item.admin_level})</span>` : ''}
                <button type="button" class="filter-chip-dismiss" aria-label="Remove" data-dismiss-id="${item.id}">
                    <i class="fas fa-times"></i>
                </button>
            </span>
        `).join(' ');

        this._chipArea.querySelectorAll('.filter-chip-dismiss').forEach(btn => {
            btn.addEventListener('click', () => {
                this._removeRegion(btn.dataset.dismissId);
            });
        });
    }

    /* ── Zoom gate: enable/disable tier buttons ── */

    /**
     * Called externally (from search.js) when the context map passes
     * the zoom threshold.  Enables all zoom-gated tier buttons.
     */
    enableTiers() {
        this._tiersEnabled = true;
        this.$el.querySelectorAll('.zoom-gated-tier').forEach(btn => {
            btn.disabled = false;
        });
        // Now that tiers are enabled, hide the input if we're on off/mapbounds
        if (this._currentTier === 'off' || this._currentTier === 'mapbounds') {
            this._inputWrap.style.display = 'none';
        }
        // Restore input placeholder and enable it for active admin tiers
        if (this._input) {
            this._input.disabled = false;
            if (isAdminTier(this._currentTier)) {
                this._input.placeholder = 'Search for a region\u2026';
            }
        }
    }

    /**
     * Called externally to re-engage the zoom gate (on full clear).
     */
    disableTiers() {
        this._tiersEnabled = false;
        this.$el.querySelectorAll('.zoom-gated-tier').forEach(btn => {
            btn.disabled = true;
        });
        if (this._input) {
            this._input.placeholder = 'Zoom the map first\u2026';
            this._input.disabled = true;
        }
        // Show the input wrap so the zoom-gate placeholder is visible
        if (this._inputWrap) {
            this._inputWrap.style.display = '';
        }
    }

    /* ── clearAll (public — called from the "clear all" link) ── */

    clearAll() {
        this._clearAllSelections();
        this._currentTier = 'off';
        this._currentNamespace = 'osm';
        // Reset tier toggle to Off
        this.$el.querySelectorAll('.region-tier-toggle .btn').forEach(b => b.classList.remove('active'));
        const offBtn = this.$el.querySelector('.region-tier-toggle .btn[data-tier="off"]');
        if (offBtn) offBtn.classList.add('active');
        // Reset namespace toggle to Modern
        this._resetNamespaceToggle();
        // Hide input only if tiers are enabled (map zoomed)
        if (this._inputWrap && this._tiersEnabled) {
            this._inputWrap.style.display = 'none';
        }
        filterState.set('spatial.geometry_source', 'none');
    }

    /* ── Full clear (called externally on tab switch / clear all) ── */

    clear() {
        this._clearAllSelections();
        this._currentTier = 'off';
        this._currentNamespace = 'osm';

        // Reset tier toggle to Off
        this.$el.querySelectorAll('.region-tier-toggle .btn').forEach(b => b.classList.remove('active'));
        const offBtn = this.$el.querySelector('.region-tier-toggle .btn[data-tier="off"]');
        if (offBtn) offBtn.classList.add('active');

        // Reset namespace toggle to Modern
        this._resetNamespaceToggle();

        // Hide input only if tiers are enabled (map zoomed)
        if (this._inputWrap && this._tiersEnabled) {
            this._inputWrap.style.display = 'none';
        }

        filterState.set('spatial.region_id', []);
        filterState.set('spatial.geometry_source', 'none');
        filterState.set('spatial.preview_geo', null);
        contextMap.clearOverlay();
        contextMap.clearSuggestions();
    }

    /* ── Reset the namespace toggle to default (Modern) ── */

    _resetNamespaceToggle() {
        this._currentNamespace = 'osm';
        if (this._nsMountPoint) {
            this._nsMountPoint.querySelectorAll('.namespace-toggle .btn').forEach(b => b.classList.remove('active'));
            const modernBtn = this._nsMountPoint.querySelector('.namespace-toggle .btn[data-namespace="osm"]');
            if (modernBtn) modernBtn.classList.add('active');
        }
    }
}

