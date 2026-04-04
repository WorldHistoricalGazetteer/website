// /whg/webpack/js/periodSelector.js
/**
 * PeriodO period selector (Tab 2: Periods).
 *
 * Reactive type-ahead searching the `periodo_periods` ES index.
 * Results are grouped by spatial_description and show label,
 * year range, spatial description, and authority label.
 * Includes a collapsible authority sub-filter.
 *
 * Supports **multi-selection**: selecting a period adds it as a
 * chip/badge.  Multiple periods can be selected.  The temporal
 * range is the union (min start, max stop) of all selected periods.
 */

import filterState from './filterState';
import contextMap from './contextMap';
import debounce from 'lodash/debounce';

export default class PeriodSelector {
    /**
     * @param {string|HTMLElement} container - CSS selector or element for the widget
     */
    constructor(container) {
        this.$el = typeof container === 'string' ? document.querySelector(container) : container;
        this._selectedPeriods = [];   // Array of selected period objects
        this._results = [];
        this._allResults = [];
        this._authorityFilter = null;
        this._init();
    }

    _init() {
        this.$el.innerHTML = `
            <div class="period-selector">
                <div class="period-input-wrap position-relative mb-2">
                    <input type="text" class="form-control form-control-sm period-search-input"
                           placeholder="Search for a period (e.g. Iron Age)…"
                           autocomplete="off" spellcheck="false">
                    <div class="period-dropdown"></div>
                </div>
                <div class="period-authority-filter collapse" id="periodAuthorityFilter">
                    <div class="period-authority-header small text-muted mb-1">
                        <i class="fas fa-filter me-1"></i>Filter by authority
                        <button type="button" class="btn btn-link btn-sm p-0 ms-1 period-authority-clear" style="display:none">clear</button>
                    </div>
                    <div class="period-authority-list"></div>
                </div>
                <div class="period-results-area"></div>
                <div class="period-chip-area mt-1"></div>
                <div class="period-info small text-muted mt-1">
                    <i class="fas fa-info-circle"></i>
                    Select <a href="https://perio.do" target="_blank" data-bs-toggle="tooltip" title="Learn about PeriodO">PeriodO</a> periods to set time and (optionally) spatial extent. Multiple periods can be selected.
                </div>
            </div>
        `;

        this._input = this.$el.querySelector('.period-search-input');
        this._dropdown = this.$el.querySelector('.period-dropdown');
        this._resultsArea = this.$el.querySelector('.period-results-area');
        this._chipArea = this.$el.querySelector('.period-chip-area');
        this._authorityList = this.$el.querySelector('.period-authority-list');
        this._authorityClear = this.$el.querySelector('.period-authority-clear');

        // Debounced search
        this._debouncedSearch = debounce(() => this._search(), 250);
        this._input.addEventListener('input', () => this._debouncedSearch());

        // Keyboard navigation
        this._activeIndex = -1;
        this._input.addEventListener('keydown', (e) => this._onKeydown(e));

        // Close dropdown on outside click
        document.addEventListener('click', (e) => {
            if (!this.$el.contains(e.target)) this._closeDropdown();
        });

        // Authority clear
        this._authorityClear.addEventListener('click', () => {
            this._authorityFilter = null;
            this._authorityClear.style.display = 'none';
            this._renderResults(this._allResults);
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
            this._closeDropdown();
            this._resultsArea.innerHTML = '';
            return;
        }

        const bbox = contextMap.getBBox();

        // Stub: when backend proxy /search/periods/ is ready, POST query + bbox.
        // For now, show placeholder.
        this._allResults = [];
        this._results = [];
        this._authorityFilter = null;

        this._resultsArea.innerHTML = `
            <div class="period-stub small text-muted p-2">
                <i class="fas fa-spinner fa-spin me-1"></i>
                Searching for "<strong>${this._escapeHtml(query)}</strong>"…
                <br><span class="smaller">Backend not yet connected.</span>
            </div>
        `;
    }

    _renderResults(results) {
        this._results = results;
        if (!results.length) {
            this._resultsArea.innerHTML = '<div class="small text-muted p-2">No periods found.</div>';
            return;
        }

        // Group by spatial_description
        const groups = {};
        results.forEach(r => {
            const key = r.spatial_description || 'Unspecified region';
            if (!groups[key]) groups[key] = [];
            groups[key].push(r);
        });

        let html = '';
        Object.entries(groups).forEach(([region, items]) => {
            html += `<div class="period-group">
                <div class="period-group-header small text-muted">${this._escapeHtml(region)}</div>`;
            items.forEach((item, i) => {
                const yearRange = this._formatYears(item.start_year, item.stop_year);
                html += `
                    <div class="period-result" data-uri="${this._escapeHtml(item.uri || '')}">
                        <div class="period-result-label">${this._escapeHtml(item.label)}</div>
                        <div class="period-result-meta">
                            <span class="period-result-years">${yearRange}</span>
                            <span class="period-result-authority">${this._escapeHtml(item.authority_label || '')}</span>
                        </div>
                    </div>
                `;
            });
            html += '</div>';
        });

        this._resultsArea.innerHTML = html;

        // Click handlers
        this._resultsArea.querySelectorAll('.period-result').forEach(el => {
            el.addEventListener('click', () => {
                const uri = el.dataset.uri;
                const item = results.find(r => r.uri === uri);
                if (item) this._selectPeriod(item);
            });
        });

        // Build authority sub-filter
        this._buildAuthorityFilter(results);
    }

    _buildAuthorityFilter(results) {
        const counts = {};
        results.forEach(r => {
            const auth = r.authority_label || 'Unknown';
            counts[auth] = (counts[auth] || 0) + 1;
        });

        if (Object.keys(counts).length <= 1) {
            this._authorityList.innerHTML = '';
            return;
        }

        const filterEl = this.$el.querySelector('#periodAuthorityFilter');
        filterEl.classList.add('show');

        this._authorityList.innerHTML = Object.entries(counts)
            .sort((a, b) => b[1] - a[1])
            .map(([auth, count]) => `
                <div class="period-authority-item small" data-authority="${this._escapeHtml(auth)}">
                    ${this._escapeHtml(auth)} <span class="badge bg-secondary">${count}</span>
                </div>
            `).join('');

        this._authorityList.querySelectorAll('.period-authority-item').forEach(el => {
            el.addEventListener('click', () => {
                this._authorityFilter = el.dataset.authority;
                this._authorityClear.style.display = 'inline';
                const filtered = this._allResults.filter(r =>
                    (r.authority_label || 'Unknown') === this._authorityFilter
                );
                this._renderResults(filtered);
            });
        });
    }

    _selectPeriod(item) {
        // Don't add duplicates
        const uri = item.uri || item.id;
        if (this._selectedPeriods.some(p => (p.uri || p.id) === uri)) return;

        this._selectedPeriods.push(item);
        this._input.value = '';
        this._resultsArea.innerHTML = '';
        this._closeDropdown();

        // Compute union temporal range (min start, max stop)
        const starts = this._selectedPeriods.map(p => p.start_year).filter(y => y != null);
        const stops = this._selectedPeriods.map(p => p.stop_year).filter(y => y != null);
        const unionStart = starts.length > 0 ? Math.min(...starts) : -2000;
        const unionStop = stops.length > 0 ? Math.max(...stops) : 2100;

        filterState.set('temporal.start_year', unionStart);
        filterState.set('temporal.stop_year', unionStop);
        filterState.set('temporal.source', 'period');
        filterState.addToList('spatial.period_id', { id: uri, label: item.label });

        if (item.spatial_geo) {
            filterState.set('spatial.geometry_source', 'period');
            filterState.set('spatial.preview_geo', item.spatial_geo);
            contextMap.setOverlay(item.spatial_geo);
            contextMap.fitTo(item.spatial_geo);
        }

        this._renderChips();
    }

    _removePeriod(uri) {
        this._selectedPeriods = this._selectedPeriods.filter(p => (p.uri || p.id) !== uri);
        filterState.removeFromList('spatial.period_id', uri);

        if (this._selectedPeriods.length === 0) {
            // Revert to defaults
            filterState.set('temporal.start_year', -2000);
            filterState.set('temporal.stop_year', 2100);
            filterState.set('temporal.source', 'manual');
            filterState.set('spatial.geometry_source', 'none');
            filterState.set('spatial.preview_geo', null);
            contextMap.clearOverlay();
        } else {
            // Recompute union temporal range
            const starts = this._selectedPeriods.map(p => p.start_year).filter(y => y != null);
            const stops = this._selectedPeriods.map(p => p.stop_year).filter(y => y != null);
            filterState.set('temporal.start_year', starts.length > 0 ? Math.min(...starts) : -2000);
            filterState.set('temporal.stop_year', stops.length > 0 ? Math.max(...stops) : 2100);
        }

        this._renderChips();
    }

    _renderChips() {
        if (this._selectedPeriods.length === 0) {
            this._chipArea.innerHTML = '';
            return;
        }

        this._chipArea.innerHTML = this._selectedPeriods.map(item => {
            const years = this._formatYears(item.start_year, item.stop_year);
            const uri = item.uri || item.id || '';
            return `
                <span class="filter-chip filter-chip--period" data-period-uri="${this._escapeHtml(uri)}">
                    <i class="fas fa-clock me-1"></i>
                    ${this._escapeHtml(item.label)}
                    <span class="filter-chip-years">${years}</span>
                    <button type="button" class="filter-chip-dismiss" aria-label="Remove" data-dismiss-uri="${this._escapeHtml(uri)}">
                        <i class="fas fa-times"></i>
                    </button>
                </span>
            `;
        }).join(' ');

        this._chipArea.querySelectorAll('.filter-chip-dismiss').forEach(btn => {
            btn.addEventListener('click', () => {
                this._removePeriod(btn.dataset.dismissUri);
            });
        });
    }

    clear() {
        this._selectedPeriods = [];
        this._chipArea.innerHTML = '';
        this._resultsArea.innerHTML = '';
        this._input.value = '';
        this._authorityFilter = null;
        filterState.set('spatial.period_id', []);
        filterState.set('spatial.geometry_source', 'none');
        filterState.set('spatial.preview_geo', null);
        filterState.set('temporal.start_year', -2000);
        filterState.set('temporal.stop_year', 2100);
        filterState.set('temporal.source', 'manual');
        contextMap.clearOverlay();
    }

    _closeDropdown() {
        this._dropdown.style.display = 'none';
        this._dropdown.innerHTML = '';
    }

    _onKeydown(e) {
        // Minimal keyboard support for the results area
        if (e.key === 'Escape') {
            this._closeDropdown();
            this._resultsArea.innerHTML = '';
        }
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

