// /whg/webpack/js/regionSelector.js
/**
 * Region selector (Tab 1: Timespan + Map).
 *
 * Supports **multi-selection**: selecting a region adds it as a
 * chip/badge.  Multiple regions can be selected (even across tiers).
 * "Off" and "Map bounds" clear all selections.  Switching between
 * other tiers preserves the selection list.
 *
 * **Map suggestions**: When switching to any tier other than Off/Map
 * bounds, the 5 entities closest to the map centre (and within bounds)
 * are drawn on the map as suggestion markers.  Clicking a suggestion
 * adds it to the chip list.
 *
 * **Zoom gate on tier buttons**: All tier buttons except "Off" are
 * disabled until the context map is zoomed past the threshold.
 *
 * Tiers:
 *   Off            — no spatial region constraint
 *   Map bounds     — use current viewport as explicit spatial filter
 *   Continental    — UN M49 continental regions
 *   Sub-Continental — UN M49 subregions + intermediary merged
 *   Country        — OSM admin level 2
 *   State          — OSM admin levels 3–4
 *   District / County — OSM admin levels 5–6
 *   Municipality   — OSM admin levels 7–8
 */

import filterState from './filterState';
import contextMap from './contextMap';
import debounce from 'lodash/debounce';

/* ───────────────────────────────────────────────────────────────────
   UN Standard Country or Area Codes for Statistical Use (M49)
   https://unstats.un.org/unsd/methodology/m49/

   The former "Intermediary" tier (Sub-Saharan Africa, Latin America
   and the Caribbean) is merged into the sub-continental tier.
   Each entry includes a representative point [lng, lat] for
   proximity matching when auto-selecting the closest region.
   ─────────────────────────────────────────────────────────────────── */
const UN_GEOSCHEME = {
    continental: [
        { code: '002', label: 'Africa',     repr_point: [20, 5] },
        { code: '019', label: 'Americas',   repr_point: [-80, 10] },
        { code: '142', label: 'Asia',       repr_point: [80, 35] },
        { code: '150', label: 'Europe',     repr_point: [15, 50] },
        { code: '009', label: 'Oceania',    repr_point: [150, -15] },
        { code: '010', label: 'Antarctica', repr_point: [0, -82] },
    ],
    subcontinental: [
        // Africa
        { code: '015', label: 'Northern Africa',      parent: 'Africa',   repr_point: [15, 30] },
        { code: '014', label: 'Eastern Africa',        parent: 'Africa',   repr_point: [35, 0] },
        { code: '017', label: 'Middle Africa',         parent: 'Africa',   repr_point: [20, -2] },
        { code: '018', label: 'Southern Africa',       parent: 'Africa',   repr_point: [25, -25] },
        { code: '011', label: 'Western Africa',        parent: 'Africa',   repr_point: [-5, 10] },
        { code: '202', label: 'Sub-Saharan Africa',    parent: 'Africa',   repr_point: [25, -5] },
        // Americas
        { code: '029', label: 'Caribbean',                       parent: 'Americas', repr_point: [-70, 18] },
        { code: '013', label: 'Central America',                 parent: 'Americas', repr_point: [-87, 14] },
        { code: '005', label: 'South America',                   parent: 'Americas', repr_point: [-58, -15] },
        { code: '021', label: 'Northern America',                parent: 'Americas', repr_point: [-100, 45] },
        { code: '419', label: 'Latin America and the Caribbean', parent: 'Americas', repr_point: [-70, 0] },
        // Asia
        { code: '143', label: 'Central Asia',       parent: 'Asia', repr_point: [65, 42] },
        { code: '030', label: 'Eastern Asia',        parent: 'Asia', repr_point: [115, 35] },
        { code: '035', label: 'South-eastern Asia',  parent: 'Asia', repr_point: [110, 5] },
        { code: '034', label: 'Southern Asia',       parent: 'Asia', repr_point: [78, 22] },
        { code: '145', label: 'Western Asia',        parent: 'Asia', repr_point: [45, 30] },
        // Europe
        { code: '151', label: 'Eastern Europe',  parent: 'Europe', repr_point: [30, 52] },
        { code: '154', label: 'Northern Europe',  parent: 'Europe', repr_point: [10, 60] },
        { code: '039', label: 'Southern Europe',  parent: 'Europe', repr_point: [15, 40] },
        { code: '155', label: 'Western Europe',   parent: 'Europe', repr_point: [3, 48] },
        // Oceania
        { code: '053', label: 'Australia and New Zealand', parent: 'Oceania', repr_point: [145, -30] },
        { code: '054', label: 'Melanesia',                parent: 'Oceania', repr_point: [155, -8] },
        { code: '057', label: 'Micronesia',               parent: 'Oceania', repr_point: [155, 8] },
        { code: '061', label: 'Polynesia',                parent: 'Oceania', repr_point: [-155, -15] },
    ],
};

/** All tier options shown in the toggle button group. */
const ALL_TIERS = [
    { value: 'off',            label: 'Off' },
    { value: 'mapbounds',      label: 'Map bounds' },
    { value: 'continental',    label: 'Continental' },
    { value: 'subcontinental', label: 'Sub-Continental' },
    { value: 'country',        label: 'Country' },
    { value: 'region',         label: 'State' },
    { value: 'district',       label: 'District / County' },
    { value: 'municipality',   label: 'Municipality' },
];

/**
 * Map each OSM tier value to the max admin_level to display.
 * Used to filter the `boundaries` vector tile layer.
 */
const TIER_ADMIN_LEVELS = {
    country:      2,
    region:       4,   // admin levels 2–4
    district:     6,   // admin levels 2–6
    municipality: 8,   // admin levels 2–8
};

/** Namespace options for the source toggle (OSM vs OHM). */
const NAMESPACE_OPTIONS = [
    { value: '',    label: 'All' },
    { value: 'osm', label: 'OSM' },
    { value: 'ohm', label: 'OHM' },
];

/* ───── Helpers ───── */

/** Squared Euclidean distance between two [lng, lat] points. */
function distSq(a, b) {
    const dx = a[0] - b[0];
    const dy = a[1] - b[1];
    return dx * dx + dy * dy;
}

/**
 * Return the N UN regions whose representative points are closest to
 * lngLat and within the given map bounds (if provided).
 */
function closestRegions(regions, lngLat, bounds, n = 5) {
    let candidates = regions;
    if (bounds) {
        const [west, south, east, north] = bounds;
        candidates = regions.filter(r => {
            const [lng, lat] = r.repr_point;
            return lng >= west && lng <= east && lat >= south && lat <= north;
        });
    }
    // If no candidates within bounds, fall back to all
    if (candidates.length === 0) candidates = regions;
    return candidates
        .map(r => ({ region: r, dist: distSq(r.repr_point, lngLat) }))
        .sort((a, b) => a.dist - b.dist)
        .slice(0, n)
        .map(x => x.region);
}

/** Is the tier one of the UN predefined tiers? */
function isUnTier(tier) {
    return tier === 'continental' || tier === 'subcontinental';
}

/** Is the tier one of the OSM admin tiers? */
function isOsmTier(tier) {
    return ['country', 'region', 'district', 'municipality'].includes(tier);
}

/** Tiers that should be gated behind map zoom. */
function isZoomGatedTier(tier) {
    return tier !== 'off';
}

/* ═══════════════════════════════════════════════════════════════════ */

export default class RegionSelector {
    /**
     * @param {string|HTMLElement} container - CSS selector or element for the widget
     */
    constructor(container) {
        this.$el = typeof container === 'string' ? document.querySelector(container) : container;
        this._selectedRegions = [];   // Array of {id, label, source, tier}
        this._results = [];
        this._currentTier = 'off';
        this._currentNamespace = '';  // '' = all, 'osm', 'ohm'
        this._tiersEnabled = false;   // Zoom gate state
        this._init();
    }

    /* ── Build DOM ── */

    _init() {
        this.$el.innerHTML = `
            <div class="region-selector">
                <div class="region-tier-toggle btn-group btn-group-sm flex-wrap mb-2" role="group" aria-label="Spatial region tier">
                    ${ALL_TIERS.map((t, i) => `
                        <button type="button" class="btn${i === 0 ? ' active' : ''}${isZoomGatedTier(t.value) ? ' zoom-gated-tier' : ''}"
                                data-tier="${t.value}" data-bs-toggle="tooltip" title="${t.label}"
                                ${isZoomGatedTier(t.value) ? 'disabled' : ''}>${t.label}</button>
                    `).join('')}
                </div>
                <div class="region-namespace-toggle btn-group btn-group-sm mb-2 d-none" role="group" aria-label="Boundary source">
                    ${NAMESPACE_OPTIONS.map((o, i) => `
                        <button type="button" class="btn${i === 0 ? ' active' : ''}"
                                data-namespace="${o.value}">${o.label}</button>
                    `).join('')}
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
        this._nsToggle     = this.$el.querySelector('.region-namespace-toggle');

        // Wire tier toggle buttons
        this.$el.querySelectorAll('.region-tier-toggle .btn').forEach(btn => {
            btn.addEventListener('click', () => {
                if (btn.disabled) return;
                this.$el.querySelectorAll('.region-tier-toggle .btn').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                this._onTierChange(btn.dataset.tier);
            });
        });

        // Wire namespace toggle buttons
        this.$el.querySelectorAll('.region-namespace-toggle .btn').forEach(btn => {
            btn.addEventListener('click', () => {
                this.$el.querySelectorAll('.region-namespace-toggle .btn').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                this._currentNamespace = btn.dataset.namespace;
                this._updateBoundaryFilter();
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

        // Listen for suggestion-click events from the context map
        document.addEventListener('suggestion-click', (e) => {
            this._onSuggestionClick(e.detail);
        });

        this._activeIndex = -1;
    }

    /* ── Tier change ── */

    _onTierChange(tier) {
        this._currentTier = tier;

        if (tier === 'off') {
            // Hide the input only if tiers are enabled (map zoomed);
            // otherwise keep it visible-but-disabled with the zoom msg.
            if (this._tiersEnabled) {
                this._inputWrap.style.display = 'none';
            }
            this._nsToggle.classList.add('d-none');
            this._clearAllSelections();
            contextMap.clearSuggestions();
            contextMap.hideBoundaries();
            filterState.set('spatial.geometry_source', 'none');
        } else if (tier === 'mapbounds') {
            if (this._tiersEnabled) {
                this._inputWrap.style.display = 'none';
            }
            this._nsToggle.classList.add('d-none');
            this._clearAllSelections();
            contextMap.clearSuggestions();
            contextMap.hideBoundaries();
            filterState.set('spatial.geometry_source', 'mapbounds');
        } else if (isUnTier(tier)) {
            this._inputWrap.style.display = '';
            this._input.placeholder = tier === 'continental'
                ? 'Filter continental regions…'
                : 'Filter sub-continental regions…';
            this._input.value = '';
            this._closeDropdown();
            this._nsToggle.classList.add('d-none');
            contextMap.hideBoundaries();
            // Show suggestions on the map
            this._showSuggestions(tier);
        } else {
            // OSM/OHM tier — show boundaries + namespace toggle + type-ahead
            this._inputWrap.style.display = '';
            this._input.placeholder = 'Search for a region…';
            this._input.value = '';
            this._closeDropdown();
            this._nsToggle.classList.remove('d-none');
            contextMap.clearSuggestions();
            this._updateBoundaryFilter();
        }
    }

    /* ── Build and apply the boundary filter for the current tier + namespace ── */

    _updateBoundaryFilter() {
        const tier = this._currentTier;
        if (!isOsmTier(tier)) {
            contextMap.hideBoundaries();
            return;
        }

        const maxLevel = TIER_ADMIN_LEVELS[tier];
        const filters = ['all', ['<=', ['get', 'admin_level'], maxLevel]];

        if (this._currentNamespace) {
            filters.push(['==', ['get', 'namespace'], this._currentNamespace]);
        }

        contextMap.showBoundaries(filters);
    }

    /* ── Map suggestions ── */

    _showSuggestions(tier) {
        const map = contextMap.map;
        if (!map) return;
        const centre = map.getCenter();
        const lngLat = [centre.lng, centre.lat];
        const bounds = contextMap.getBBox();
        const regions = UN_GEOSCHEME[tier] || [];

        const closest = closestRegions(regions, lngLat, bounds, 5);

        // Build GeoJSON FeatureCollection for suggestion markers
        const features = closest.map(r => ({
            type: 'Feature',
            geometry: {
                type: 'Point',
                coordinates: r.repr_point,
            },
            properties: {
                id: `un:${r.code}`,
                label: r.label,
                code: r.code,
                tier: tier,
                source: 'un_geoscheme',
            },
        }));

        contextMap.setSuggestions({
            type: 'FeatureCollection',
            features,
        });
    }

    /* ── Handle suggestion click from the map ── */

    _onSuggestionClick(detail) {
        if (!detail || !detail.id) return;
        // Only act if we're on a UN tier
        const tier = detail.tier || this._currentTier;
        if (!isUnTier(tier)) return;

        const regions = UN_GEOSCHEME[tier] || [];
        const region = regions.find(r => r.code === detail.code);
        if (region) {
            this._addUnRegion(region, tier);
        }
    }

    /* ── Selection helpers (multi-select) ── */

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

    _addUnRegion(region, tier) {
        const id = `un:${region.code}`;
        // Don't add duplicates
        if (this._selectedRegions.some(r => r.id === id)) return;

        const item = {
            id,
            label: region.label,
            source: 'un_geoscheme',
            tier: tier,
        };
        this._selectedRegions.push(item);
        this._input.value = '';
        this._closeDropdown();

        // Update filter state
        filterState.addToList('spatial.region_id', item);
        filterState.set('spatial.geometry_source', 'un_geoscheme');

        // Re-render all chips
        this._renderChips();

        // Fetch and preview geometry (when backend is ready)
        console.log('RegionSelector: UN region added:', region.label, `(${tier}, M49 code ${region.code})`);
    }

    _addOsmRegion(item) {
        // Don't add duplicates
        if (this._selectedRegions.some(r => r.id === item.id)) return;

        this._selectedRegions.push(item);
        this._input.value = '';
        this._closeDropdown();

        filterState.addToList('spatial.region_id', item);
        filterState.set('spatial.geometry_source', 'osm');

        this._renderChips();
        this._fetchGeometry(item.id);
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
        }
    }

    _selectResult(index) {
        const item = this._results[index];
        if (!item) return;

        // UN region from client-side filtering
        if (item._un) {
            this._addUnRegion(item._un, item._tier);
            return;
        }

        // OSM region from backend
        this._addOsmRegion(item);
    }

    /* ── Search / filter ── */

    async _search() {
        const query = this._input.value.trim();
        if (query.length < 2) {
            this._closeDropdown();
            return;
        }

        const tier = this._currentTier;

        // UN tiers: client-side filtering of the fixed set
        if (isUnTier(tier)) {
            const regions = UN_GEOSCHEME[tier] || [];
            const lowerQuery = query.toLowerCase();
            const matches = regions.filter(r =>
                r.label.toLowerCase().includes(lowerQuery) ||
                (r.parent && r.parent.toLowerCase().includes(lowerQuery))
            );
            if (matches.length === 0) {
                this._results = [];
                this._renderDropdown([{
                    _stub: true,
                    label: 'No matching regions',
                }]);
            } else {
                this._results = matches.map(r => ({
                    id: `un:${r.code}`,
                    label: r.label,
                    sublabel: r.parent || '',
                    _un: r,
                    _tier: tier,
                }));
                this._renderDropdown(this._results);
            }
            return;
        }

        // OSM tiers: backend search (stub)
        // const bbox = contextMap.getBBox();  // Will be used when backend is connected
        this._results = [];
        this._renderDropdown([{
            _stub: true,
            label: `Searching "${query}" (${tier})…`,
            sublabel: 'Backend not yet connected',
        }]);
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
                <i class="fas fa-map-marker-alt me-1"></i>
                ${item.label}
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
     * the zoom threshold.  Enables all tier buttons.
     */
    enableTiers() {
        this._tiersEnabled = true;
        this.$el.querySelectorAll('.zoom-gated-tier').forEach(btn => {
            btn.disabled = false;
        });
        // Now that tiers are enabled, hide the input if we're on off/mapbounds
        // (it was kept visible only to show the zoom-gate placeholder)
        if (this._currentTier === 'off' || this._currentTier === 'mapbounds') {
            this._inputWrap.style.display = 'none';
        }
        // Restore input placeholder and enable it for active search tiers
        if (this._input) {
            this._input.disabled = false;
            if (isUnTier(this._currentTier)) {
                this._input.placeholder = this._currentTier === 'continental'
                    ? 'Filter continental regions…'
                    : 'Filter sub-continental regions…';
            } else if (isOsmTier(this._currentTier)) {
                this._input.placeholder = 'Search for a region…';
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
            this._input.placeholder = 'Zoom the map first…';
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
        this._currentNamespace = '';
        // Reset tier toggle to Off
        this.$el.querySelectorAll('.region-tier-toggle .btn').forEach(b => b.classList.remove('active'));
        const offBtn = this.$el.querySelector('.region-tier-toggle .btn[data-tier="off"]');
        if (offBtn) offBtn.classList.add('active');
        // Reset namespace toggle
        this.$el.querySelectorAll('.region-namespace-toggle .btn').forEach(b => b.classList.remove('active'));
        const allNsBtn = this.$el.querySelector('.region-namespace-toggle .btn[data-namespace=""]');
        if (allNsBtn) allNsBtn.classList.add('active');
        this._nsToggle.classList.add('d-none');
        // Hide input only if tiers are enabled (map zoomed);
        // otherwise keep it visible for the zoom-gate placeholder
        if (this._inputWrap && this._tiersEnabled) {
            this._inputWrap.style.display = 'none';
        }
        filterState.set('spatial.geometry_source', 'none');
    }

    /* ── Full clear (called externally on tab switch / clear all) ── */

    clear() {
        this._clearAllSelections();
        this._currentTier = 'off';
        this._currentNamespace = '';

        // Reset tier toggle to Off
        this.$el.querySelectorAll('.region-tier-toggle .btn').forEach(b => b.classList.remove('active'));
        const offBtn = this.$el.querySelector('.region-tier-toggle .btn[data-tier="off"]');
        if (offBtn) offBtn.classList.add('active');

        // Reset namespace toggle
        this.$el.querySelectorAll('.region-namespace-toggle .btn').forEach(b => b.classList.remove('active'));
        const allNsBtn = this.$el.querySelector('.region-namespace-toggle .btn[data-namespace=""]');
        if (allNsBtn) allNsBtn.classList.add('active');
        this._nsToggle.classList.add('d-none');

        // Hide input only if tiers are enabled (map zoomed);
        // otherwise keep it visible for the zoom-gate placeholder
        if (this._inputWrap && this._tiersEnabled) {
            this._inputWrap.style.display = 'none';
        }

        filterState.set('spatial.region_id', []);
        filterState.set('spatial.geometry_source', 'none');
        filterState.set('spatial.preview_geo', null);
        contextMap.clearOverlay();
        contextMap.clearSuggestions();
    }

    /* ── Geometry fetch (stub) ── */

    async _fetchGeometry(id) {
        // Stub: when the backend proxy is ready, fetch the geometry
        // and display it on the context map.
        console.log('RegionSelector: would fetch geometry for', id);
    }
}

