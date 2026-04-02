// /whg/webpack/js/regionSelector.js
/**
 * OSM Region type-ahead selector (Tab 1: Timespan + Map).
 *
 * Provides admin-tier radio buttons and a debounced text input
 * for searching named administrative regions from OpenStreetMap
 * data in the places index.
 */

import filterState from './filterState';
import contextMap from './contextMap';
import debounce from 'lodash/debounce';

const ADMIN_TIERS = [
    { value: 'country', label: 'Country' },
    { value: 'region', label: 'Region / State' },
    { value: 'district', label: 'District / County' },
    { value: 'municipality', label: 'Municipality' },
];

export default class RegionSelector {
    /**
     * @param {string|HTMLElement} container - CSS selector or element for the widget
     */
    constructor(container) {
        this.$el = typeof container === 'string' ? document.querySelector(container) : container;
        this._selectedRegion = null;
        this._results = [];
        this._init();
    }

    _init() {
        this.$el.innerHTML = `
            <div class="region-selector">
                <div class="region-tier-toggle btn-group btn-group-sm mb-2" role="group" aria-label="Region type">
                    ${ADMIN_TIERS.map((t, i) => `
                        <button type="button" class="btn${i === 0 ? ' active' : ''}" data-tier="${t.value}" title="${t.label}">${t.label}</button>
                    `).join('')}
                </div>
                <div class="region-input-wrap position-relative">
                    <input type="text" class="form-control form-control-sm region-search-input"
                           placeholder="Search for a region…"
                           autocomplete="off" spellcheck="false">
                    <div class="region-dropdown"></div>
                </div>
                <div class="region-chip-area mt-1"></div>
            </div>
        `;

        this._input = this.$el.querySelector('.region-search-input');
        this._dropdown = this.$el.querySelector('.region-dropdown');
        this._chipArea = this.$el.querySelector('.region-chip-area');

        // Wire tier toggle buttons
        this.$el.querySelectorAll('.region-tier-toggle .btn').forEach(btn => {
            btn.addEventListener('click', () => {
                this.$el.querySelectorAll('.region-tier-toggle .btn').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
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

    _getSelectedTier() {
        const active = this.$el.querySelector('.region-tier-toggle .btn.active');
        return active ? active.dataset.tier : 'country';
    }

    async _search() {
        const query = this._input.value.trim();
        if (query.length < 2) {
            this._closeDropdown();
            return;
        }

        const tier = this._getSelectedTier();
        const bbox = contextMap.getBBox();

        // For now, build a stub result set.
        // When the backend is ready, this will POST to the CRC FastAPI proxy.
        // Stub: show a "coming soon" message until backend is wired up.
        this._results = [];
        this._renderDropdown([{
            _stub: true,
            label: `Searching "${query}" (${tier})…`,
            sublabel: 'Backend not yet connected',
        }]);
    }

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

    _selectResult(index) {
        const item = this._results[index];
        if (!item) return;

        this._selectedRegion = item;
        this._input.value = '';
        this._closeDropdown();

        // Update filter state
        filterState.set('spatial.region_id', item.id);
        filterState.set('spatial.geometry_source', 'osm');

        // Show chip
        this._renderChip(item);

        // Fetch and preview geometry (when backend is ready)
        this._fetchGeometry(item.id);
    }

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
            this.clear();
        });
    }

    clear() {
        this._selectedRegion = null;
        this._chipArea.innerHTML = '';
        this._input.value = '';
        filterState.set('spatial.region_id', null);
        filterState.set('spatial.geometry_source', 'none');
        filterState.set('spatial.preview_geo', null);
        contextMap.clearOverlay();
    }

    async _fetchGeometry(id) {
        // Stub: when the backend proxy /search/geometry/places/{id}/ is ready,
        // fetch the geometry and display it.
        // For now, just log.
        console.log('RegionSelector: would fetch geometry for', id);
    }
}

