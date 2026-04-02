// /whg/webpack/js/politySelector.js
/**
 * Polity selector (Tab 3: Polities).
 *
 * Reactive type-ahead searching the polity ES index.
 * Single selection sets both temporal and spatial constraints.
 */

import filterState from './filterState';
import contextMap from './contextMap';
import debounce from 'lodash/debounce';

const POLITY_DATASETS = [
    { value: 'cliopatria', label: 'Cliopatria', title: 'Cliopatria — historical polities and empires' },
    { value: 'dplace', label: 'D-PLACE', title: 'D-PLACE — cultural and linguistic regions' },
    { value: 'nativeland', label: 'NativeLand', title: 'NativeLand — indigenous territories, languages, and treaties' },
];

export default class PolitySelector {
    /**
     * @param {string|HTMLElement} container - CSS selector or element for the widget
     */
    constructor(container) {
        this.$el = typeof container === 'string' ? document.querySelector(container) : container;
        this._selectedPolity = null;
        this._activeDataset = POLITY_DATASETS[0].value;
        this._results = [];
        this._init();
    }

    _init() {
        this.$el.innerHTML = `
            <div class="polity-selector">
                <div class="polity-dataset-toggle btn-group btn-group-sm mb-2" role="group" aria-label="Polity dataset">
                    ${POLITY_DATASETS.map((ds, i) => `
                        <button type="button" class="btn${i === 0 ? ' active' : ''}" data-polity-dataset="${ds.value}" title="${ds.title}">${ds.label}</button>
                    `).join('')}
                </div>
                <div class="polity-input-wrap position-relative mb-2">
                    <input type="text" class="form-control form-control-sm polity-search-input"
                           placeholder="Search for a territory or polity…"
                           autocomplete="off" spellcheck="false">
                    <div class="polity-dropdown"></div>
                </div>
                <div class="polity-results-area"></div>
                <div class="polity-chip-area mt-1"></div>
                <div class="polity-info small text-muted mt-1">
                    <i class="fas fa-info-circle"></i>
                    Select a territory to set time and spatial extent.
                </div>
            </div>
        `;

        this._input = this.$el.querySelector('.polity-search-input');
        this._dropdown = this.$el.querySelector('.polity-dropdown');
        this._resultsArea = this.$el.querySelector('.polity-results-area');
        this._chipArea = this.$el.querySelector('.polity-chip-area');

        // Wire dataset toggle buttons — exclusive selection
        this.$el.querySelectorAll('.polity-dataset-toggle .btn').forEach(btn => {
            btn.addEventListener('click', () => {
                this.$el.querySelectorAll('.polity-dataset-toggle .btn').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                this._activeDataset = btn.dataset.polityDataset;
                // Clear current selection when switching datasets
                this.clear();
                filterState.set('spatial.polity_dataset', this._activeDataset);
            });
        });

        // Debounced search
        this._debouncedSearch = debounce(() => this._search(), 250);
        this._input.addEventListener('input', () => this._debouncedSearch());

        // Keyboard
        this._input.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') {
                this._resultsArea.innerHTML = '';
            }
        });

        // Close on outside click
        document.addEventListener('click', (e) => {
            if (!this.$el.contains(e.target)) {
                this._dropdown.style.display = 'none';
            }
        });

        // React to viewport changes
        contextMap.onViewportChange(() => {
            if (this._input.value.trim().length >= 2) {
                this._debouncedSearch();
            }
        });
    }

    async _search() {
        const query = this._input.value.trim();
        if (query.length < 2) {
            this._resultsArea.innerHTML = '';
            return;
        }

        const bbox = contextMap.getBBox();

        // Stub: when backend proxy /search/polities/ is ready, POST query + bbox.
        this._results = [];
        this._resultsArea.innerHTML = `
            <div class="polity-stub small text-muted p-2">
                <i class="fas fa-spinner fa-spin me-1"></i>
                Searching for "<strong>${this._escapeHtml(query)}</strong>"…
                <br><span class="smaller">Backend not yet connected.</span>
            </div>
        `;
    }

    _renderResults(results) {
        this._results = results;
        if (!results.length) {
            this._resultsArea.innerHTML = '<div class="small text-muted p-2">No territories found.</div>';
            return;
        }

        this._resultsArea.innerHTML = results.map(item => {
            const years = this._formatYears(item.start_year, item.stop_year);
            return `
                <div class="polity-result" data-id="${this._escapeHtml(item.id || '')}">
                    <div class="polity-result-label">${this._escapeHtml(item.label)}</div>
                    <div class="polity-result-years small text-muted">${years}</div>
                </div>
            `;
        }).join('');

        this._resultsArea.querySelectorAll('.polity-result').forEach(el => {
            el.addEventListener('click', () => {
                const id = el.dataset.id;
                const item = results.find(r => r.id === id);
                if (item) this._selectPolity(item);
            });
        });
    }

    _selectPolity(item) {
        this._selectedPolity = item;
        this._input.value = '';
        this._resultsArea.innerHTML = '';

        // Set temporal + spatial from polity
        filterState.set('temporal.start_year', item.start_year);
        filterState.set('temporal.stop_year', item.stop_year);
        filterState.set('temporal.source', 'polity');
        filterState.set('spatial.polity_id', item.id);
        filterState.set('spatial.geometry_source', 'polity');

        if (item.geometry) {
            filterState.set('spatial.preview_geo', item.geometry);
            contextMap.setOverlay(item.geometry);
            contextMap.fitTo(item.geometry);
        }

        this._renderChip(item);
    }

    _renderChip(item) {
        const years = this._formatYears(item.start_year, item.stop_year);
        this._chipArea.innerHTML = `
            <span class="filter-chip filter-chip--polity">
                <i class="fas fa-globe-americas me-1"></i>
                ${this._escapeHtml(item.label)}
                <span class="filter-chip-years">${years}</span>
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
        this._selectedPolity = null;
        this._chipArea.innerHTML = '';
        this._resultsArea.innerHTML = '';
        this._input.value = '';
        filterState.set('spatial.polity_id', null);
        filterState.set('spatial.geometry_source', 'none');
        filterState.set('spatial.preview_geo', null);
        filterState.set('spatial.polity_dataset', null);
        filterState.set('temporal.start_year', -2000);
        filterState.set('temporal.stop_year', 2100);
        filterState.set('temporal.source', 'manual');
        contextMap.clearOverlay();
    }

    _formatYears(start, stop) {
        const fmtYear = (y) => {
            if (y == null) return '?';
            return y < 0 ? `${Math.abs(y)} BCE` : `${y} CE`;
        };
        return `${fmtYear(start)} – ${fmtYear(stop)}`;
    }

    _escapeHtml(s) {
        return String(s)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }
}

