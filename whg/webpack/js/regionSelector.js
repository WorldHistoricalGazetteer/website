// /whg/webpack/js/regionSelector.js
/**
 * Region selector (Tab 1: Timespan + Map).
 *
 * Provides tier radio buttons and a debounced text input
 * for selecting spatial region constraints.
 *
 * Tiers:
 *   Off            — no spatial region constraint
 *   Map bounds     — use current viewport as explicit spatial filter
 *   Continental    — UN M49 continental regions (auto-picks closest to map centre)
 *   Sub-Continental — UN M49 subregions + intermediary merged (auto-picks closest)
 *   Country        — OSM admin level 2
 *   Region / State — OSM admin levels 3–4
 *   District / County — OSM admin levels 5–6
 *   Municipality   — OSM admin levels 7–8
 *
 * For Continental and Sub-Continental tiers, regions are compiled as
 * static data.  When the user selects one of these tiers, the region
 * whose representative point is closest to the current map centre is
 * automatically loaded in preview.  The type-ahead input filters the
 * fixed set client-side.
 *
 * For OSM tiers (Country … Municipality), the type-ahead posts to
 * the CRC FastAPI backend (stub until backend is connected).
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
    { value: 'region',         label: 'Region / State' },
    { value: 'district',       label: 'District / County' },
    { value: 'municipality',   label: 'Municipality' },
];

/* ───── Helpers ───── */

/** Squared Euclidean distance between two [lng, lat] points. */
function distSq(a, b) {
    const dx = a[0] - b[0];
    const dy = a[1] - b[1];
    return dx * dx + dy * dy;
}

/** Return the UN region whose representative point is closest to lngLat. */
function closestRegion(regions, lngLat) {
    let best = null;
    let bestDist = Infinity;
    for (const r of regions) {
        const d = distSq(r.repr_point, lngLat);
        if (d < bestDist) {
            bestDist = d;
            best = r;
        }
    }
    return best;
}

/** Is the tier one of the UN predefined tiers? */
function isUnTier(tier) {
    return tier === 'continental' || tier === 'subcontinental';
}

/** Is the tier one of the OSM admin tiers? */
function isOsmTier(tier) {
    return ['country', 'region', 'district', 'municipality'].includes(tier);
}

/* ═══════════════════════════════════════════════════════════════════ */

export default class RegionSelector {
    /**
     * @param {string|HTMLElement} container - CSS selector or element for the widget
     */
    constructor(container) {
        this.$el = typeof container === 'string' ? document.querySelector(container) : container;
        this._selectedRegion = null;
        this._results = [];
        this._currentTier = 'off';
        this._init();
    }

    /* ── Build DOM ── */

    _init() {
        this.$el.innerHTML = `
            <div class="region-selector">
                <div class="region-tier-toggle btn-group btn-group-sm flex-wrap mb-2" role="group" aria-label="Spatial region tier">
                    ${ALL_TIERS.map((t, i) => `
                        <button type="button" class="btn${i === 0 ? ' active' : ''}"
                                data-tier="${t.value}" title="${t.label}">${t.label}</button>
                    `).join('')}
                </div>
                <div class="region-input-wrap position-relative" style="display:none;">
                    <input type="text" class="form-control form-control-sm region-search-input"
                           placeholder="Search for a region…"
                           autocomplete="off" spellcheck="false">
                    <div class="region-dropdown"></div>
                </div>
                <div class="region-chip-area mt-1"></div>
            </div>
        `;

        this._input     = this.$el.querySelector('.region-search-input');
        this._inputWrap = this.$el.querySelector('.region-input-wrap');
        this._dropdown  = this.$el.querySelector('.region-dropdown');
        this._chipArea  = this.$el.querySelector('.region-chip-area');

        // Wire tier toggle buttons
        this.$el.querySelectorAll('.region-tier-toggle .btn').forEach(btn => {
            btn.addEventListener('click', () => {
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

        this._activeIndex = -1;
    }

    /* ── Tier change ── */

    _onTierChange(tier) {
        this._currentTier = tier;

        // Clear any existing selection when switching tiers
        this._clearSelection();

        if (tier === 'off') {
            this._inputWrap.style.display = 'none';
            filterState.set('spatial.geometry_source', 'none');
        } else if (tier === 'mapbounds') {
            this._inputWrap.style.display = 'none';
            filterState.set('spatial.geometry_source', 'mapbounds');
        } else if (isUnTier(tier)) {
            this._inputWrap.style.display = '';
            this._input.placeholder = tier === 'continental'
                ? 'Filter continental regions…'
                : 'Filter sub-continental regions…';
            // Auto-select the region closest to the current map centre
            this._autoSelectClosest(tier);
        } else {
            // OSM tier — type-ahead backend search
            this._inputWrap.style.display = '';
            this._input.placeholder = 'Search for a region…';
        }
    }

    /* ── Auto-select closest UN region ── */

    _autoSelectClosest(tier) {
        const map = contextMap.map;
        if (!map) return;
        const centre = map.getCenter();
        const lngLat = [centre.lng, centre.lat];
        const regions = UN_GEOSCHEME[tier] || [];
        const closest = closestRegion(regions, lngLat);
        if (closest) {
            this._selectUnRegion(closest, tier);
        }
    }

    /* ── Selection helpers ── */

    _clearSelection() {
        this._selectedRegion = null;
        this._chipArea.innerHTML = '';
        this._input.value = '';
        this._closeDropdown();
        filterState.set('spatial.region_id', null);
        filterState.set('spatial.preview_geo', null);
        contextMap.clearOverlay();
    }

    _selectUnRegion(region, tier) {
        this._selectedRegion = {
            id: `un:${region.code}`,
            label: region.label,
            source: 'un_geoscheme',
            tier: tier,
        };
        this._input.value = '';
        this._closeDropdown();

        // Update filter state
        filterState.set('spatial.region_id', `un:${region.code}`);
        filterState.set('spatial.geometry_source', 'un_geoscheme');

        // Show chip
        this._renderChip(this._selectedRegion);

        // Fetch and preview geometry (when backend is ready)
        console.log('RegionSelector: UN region selected:', region.label, `(${tier}, M49 code ${region.code})`);
    }

    _selectResult(index) {
        const item = this._results[index];
        if (!item) return;

        // UN region from client-side filtering
        if (item._un) {
            this._selectUnRegion(item._un, item._tier);
            return;
        }

        // OSM region from backend
        this._selectedRegion = item;
        this._input.value = '';
        this._closeDropdown();

        filterState.set('spatial.region_id', item.id);
        filterState.set('spatial.geometry_source', 'osm');

        this._renderChip(item);
        this._fetchGeometry(item.id);
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

    /* ── Chip ── */

    _renderChip(item) {
        this._chipArea.innerHTML = `
            <span class="filter-chip">
                <i class="fas fa-map-marker-alt me-1"></i>
                ${item.label || item.name}
                <button type="button" class="filter-chip-dismiss" aria-label="Remove">
                    <i class="fas fa-times"></i>
                </button>
            </span>
        `;
        this._chipArea.querySelector('.filter-chip-dismiss').addEventListener('click', () => {
            this._dismissChip();
        });
    }

    /**
     * Dismiss the chip but stay on the current tier.
     * Reverts to "no region selected" within the active tier.
     */
    _dismissChip() {
        this._selectedRegion = null;
        this._chipArea.innerHTML = '';
        filterState.set('spatial.region_id', null);
        filterState.set('spatial.preview_geo', null);
        contextMap.clearOverlay();

        // Revert geometry_source — the tier stays active but no region is selected
        if (this._currentTier !== 'mapbounds') {
            filterState.set('spatial.geometry_source', 'none');
        }
    }

    /* ── Full clear (called externally on tab switch / clear all) ── */

    clear() {
        this._selectedRegion = null;
        this._chipArea.innerHTML = '';
        this._input.value = '';
        this._currentTier = 'off';
        this._closeDropdown();

        // Reset tier toggle to Off
        this.$el.querySelectorAll('.region-tier-toggle .btn').forEach(b => b.classList.remove('active'));
        const offBtn = this.$el.querySelector('.region-tier-toggle .btn[data-tier="off"]');
        if (offBtn) offBtn.classList.add('active');

        // Hide input
        if (this._inputWrap) this._inputWrap.style.display = 'none';

        filterState.set('spatial.region_id', null);
        filterState.set('spatial.geometry_source', 'none');
        filterState.set('spatial.preview_geo', null);
        contextMap.clearOverlay();
    }

    /* ── Geometry fetch (stub) ── */

    async _fetchGeometry(id) {
        // Stub: when the backend proxy is ready, fetch the geometry
        // and display it on the context map.
        console.log('RegionSelector: would fetch geometry for', id);
    }
}

