/**
 * placetypes/static/placetypes/mapping.js
 *
 * Client-side logic for the Type Mapping Dashboard.
 * Handles tab data loading, table rendering, sorting, filtering,
 * AAT concept search modal, and save/remove API calls.
 */
(function () {
    'use strict';

    // -----------------------------------------------------------------------
    // Configuration
    // -----------------------------------------------------------------------
    const API = {
        geonames: '/types/mapping/api/geonames/',
        wikidata: '/types/mapping/api/wikidata/',
        osm: '/types/mapping/api/osm/',
        ohm: '/types/mapping/api/ohm/',
        search: '/types/mapping/api/search/',
        save: '/types/mapping/api/save/',
        remove: '/types/mapping/api/remove/',
        stats: '/types/mapping/api/stats/',
        copyOsmToOhm: '/types/mapping/api/copy-osm-to-ohm/',
    };

    // Low-value tag values to de-emphasise
    const LOW_VALUE_VALUES = new Set([
        'yes', 'no', 'other', 'fixme', 'none', 'unknown', 'unclassified',
        'general', 'default', 'misc', 'various',
    ]);

    // CSRF token from meta tag
    function getCsrfToken() {
        const meta = document.querySelector('meta[name="csrf-token"]');
        return meta ? meta.getAttribute('content') : '';
    }

    // -----------------------------------------------------------------------
    // State
    // -----------------------------------------------------------------------
    const state = {
        data: {geonames: null, wikidata: null, osm: null, ohm: null},
        filtered: {geonames: null, wikidata: null, osm: null, ohm: null},
        sort: {geonames: {col: 'count', dir: 'desc'}, wikidata: {col: 'count', dir: 'desc'},
               osm: {col: 'count', dir: 'desc'}, ohm: {col: 'count', dir: 'desc'}},
        tagKeys: {osm: [], ohm: []},
        activeTab: 'geonames',
        // Modal state
        modalSourceVocab: null,
        modalSourceId: null,
        modalSourceLabel: null,
        selectedAatId: null,
        selectedAatTerm: null,
    };

    // -----------------------------------------------------------------------
    // Utility functions
    // -----------------------------------------------------------------------
    function fetchJSON(url, options) {
        return fetch(url, options).then(r => {
            if (!r.ok) throw new Error(`HTTP ${r.status}`);
            return r.json();
        });
    }

    function postJSON(url, body) {
        return fetchJSON(url, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': getCsrfToken(),
            },
            body: JSON.stringify(body),
        });
    }

    function formatCount(n) {
        if (n == null) return '–';
        return n.toLocaleString();
    }

    function escapeHtml(str) {
        if (!str) return '';
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }

    function debounce(fn, delay) {
        let timer;
        return function (...args) {
            clearTimeout(timer);
            timer = setTimeout(() => fn.apply(this, args), delay);
        };
    }

    // -----------------------------------------------------------------------
    // Tab ↔ vocab mapping
    // -----------------------------------------------------------------------
    const TAB_VOCAB = {
        'geonames-tab': 'geonames',
        'wikidata-tab': 'wikidata',
        'osm-tab': 'osm',
        'ohm-tab': 'ohm',
    };

    const VOCAB_CONFIG = {
        geonames: {
            tableId: 'gn-table', filterId: 'gn-filter', loadingId: 'gn-loading',
            countLabelId: 'gn-count-label',
            columns: ['source_id', 'label', 'description', 'count', 'mapping'],
            hasTagKey: false,
        },
        wikidata: {
            tableId: 'wd-table', filterId: 'wd-filter', loadingId: 'wd-loading',
            countLabelId: 'wd-count-label',
            columns: ['source_id', 'label', 'count', 'mapping'],
            hasTagKey: false,
        },
        osm: {
            tableId: 'osm-table', filterId: 'osm-filter', loadingId: 'osm-loading',
            countLabelId: 'osm-count-label', tagKeyFilterId: 'osm-tag-key-filter',
            columns: ['source_id', 'tag_key', 'label', 'description', 'count', 'mapping'],
            hasTagKey: true,
        },
        ohm: {
            tableId: 'ohm-table', filterId: 'ohm-filter', loadingId: 'ohm-loading',
            countLabelId: 'ohm-count-label', tagKeyFilterId: 'ohm-tag-key-filter',
            columns: ['source_id', 'tag_key', 'label', 'description', 'count', 'mapping'],
            hasTagKey: true,
        },
    };

    // -----------------------------------------------------------------------
    // Load stats
    // -----------------------------------------------------------------------
    function loadStats() {
        fetchJSON(API.stats).then(stats => {
            document.getElementById('stat-gn-mapped').textContent = formatCount(stats.geonames?.mapped);
            document.getElementById('stat-gn-total').textContent = formatCount(stats.geonames?.total);
            document.getElementById('stat-wd-mapped').textContent = formatCount(stats.wikidata?.mapped);
            document.getElementById('stat-wd-total').textContent = formatCount(stats.wikidata?.total);
            document.getElementById('stat-osm-mapped').textContent = formatCount(stats.osm?.mapped);
            document.getElementById('stat-osm-total').textContent = formatCount(stats.osm?.total);
            document.getElementById('stat-ohm-mapped').textContent = formatCount(stats.ohm?.mapped);
            document.getElementById('stat-ohm-total').textContent = formatCount(stats.ohm?.total);
        }).catch(err => {
            console.warn('Failed to load stats:', err);
        });
    }

    // -----------------------------------------------------------------------
    // Load vocabulary data
    // -----------------------------------------------------------------------
    function loadVocabData(vocab) {
        if (state.data[vocab] !== null) return; // already loaded

        const cfg = VOCAB_CONFIG[vocab];
        const loading = document.getElementById(cfg.loadingId);
        loading.classList.remove('d-none');

        fetchJSON(API[vocab]).then(data => {
            loading.classList.add('d-none');
            state.data[vocab] = data;
            state.filtered[vocab] = data;

            // Populate tag key filter for OSM/OHM
            if (cfg.hasTagKey) {
                const keys = [...new Set(data.map(d => d.tag_key))].sort();
                state.tagKeys[vocab] = keys;
                const sel = document.getElementById(cfg.tagKeyFilterId);
                sel.innerHTML = '<option value="">All tag keys</option>';
                keys.forEach(k => {
                    const opt = document.createElement('option');
                    opt.value = k;
                    opt.textContent = k;
                    sel.appendChild(opt);
                });
            }

            applyFilters(vocab);
            renderTable(vocab);
        }).catch(err => {
            console.error(`Failed to load ${vocab} data:`, err);
            loading.classList.remove('d-none');
            loading.innerHTML = `<span class="text-danger">Error loading data: ${escapeHtml(err.message)}</span>`;
        });
    }

    // -----------------------------------------------------------------------
    // Filtering
    // -----------------------------------------------------------------------
    function applyFilters(vocab) {
        const cfg = VOCAB_CONFIG[vocab];
        const data = state.data[vocab] || [];
        const textFilter = document.getElementById(cfg.filterId).value.toLowerCase().trim();
        const tagKeyFilter = cfg.hasTagKey
            ? document.getElementById(cfg.tagKeyFilterId).value
            : '';

        let filtered = data;

        if (tagKeyFilter) {
            filtered = filtered.filter(d => d.tag_key === tagKeyFilter);
        }
        if (textFilter) {
            filtered = filtered.filter(d => {
                const searchable = [
                    d.source_id, d.label, d.description || '',
                    d.mapping ? `${d.mapping.aat_id} ${d.mapping.aat_term}` : '',
                ].join(' ').toLowerCase();
                return searchable.includes(textFilter);
            });
        }

        state.filtered[vocab] = filtered;

        // Update count label
        const label = document.getElementById(cfg.countLabelId);
        if (filtered.length !== data.length) {
            label.textContent = `Showing ${filtered.length.toLocaleString()} of ${data.length.toLocaleString()}`;
        } else {
            label.textContent = `${data.length.toLocaleString()} types`;
        }
    }

    // -----------------------------------------------------------------------
    // Sorting
    // -----------------------------------------------------------------------
    function sortData(vocab, col) {
        const current = state.sort[vocab];
        if (current.col === col) {
            current.dir = current.dir === 'asc' ? 'desc' : 'asc';
        } else {
            current.col = col;
            current.dir = col === 'count' ? 'desc' : 'asc';
        }

        const dir = current.dir === 'asc' ? 1 : -1;

        state.filtered[vocab].sort((a, b) => {
            let va, vb;
            if (col === 'mapping') {
                va = a.mapping ? a.mapping.aat_term : '';
                vb = b.mapping ? b.mapping.aat_term : '';
            } else if (col === 'count') {
                va = a.count || 0;
                vb = b.count || 0;
            } else {
                va = (a[col] || '').toString().toLowerCase();
                vb = (b[col] || '').toString().toLowerCase();
            }
            if (va < vb) return -1 * dir;
            if (va > vb) return 1 * dir;
            return 0;
        });

        // Update header sort indicators
        const cfg = VOCAB_CONFIG[vocab];
        const table = document.getElementById(cfg.tableId);
        table.querySelectorAll('th.sortable').forEach(th => {
            th.classList.remove('sort-asc', 'sort-desc');
            if (th.dataset.col === col) {
                th.classList.add(current.dir === 'asc' ? 'sort-asc' : 'sort-desc');
            }
        });
    }

    // -----------------------------------------------------------------------
    // Table rendering
    // -----------------------------------------------------------------------
    function renderTable(vocab) {
        const cfg = VOCAB_CONFIG[vocab];
        const table = document.getElementById(cfg.tableId);
        const tbody = table.querySelector('tbody');
        const items = state.filtered[vocab] || [];

        // Virtual scroll: render max 500 items, with pagination note
        const MAX_RENDER = 500;
        const renderItems = items.slice(0, MAX_RENDER);

        const rows = renderItems.map(item => {
            const mapped = !!item.mapping;
            const isLowValue = LOW_VALUE_VALUES.has((item.label || '').toLowerCase());
            const rowClass = [
                mapped ? 'mapping-row--mapped' : 'mapping-row--unmapped',
                isLowValue ? 'mapping-row--lowvalue' : '',
            ].join(' ');

            const mappingCell = mapped
                ? `<span class="aat-badge badge bg-success bg-opacity-75">aat:${item.mapping.aat_id}</span>
                   <small class="ms-1">${escapeHtml(item.mapping.aat_term)}</small>`
                : `<span class="badge badge-unmapped bg-warning text-dark">unmapped</span>`;

            const actionBtn = mapped
                ? `<button class="btn btn-sm btn-outline-primary me-1 btn-map"
                     data-source-id="${escapeHtml(item.source_id)}"
                     data-vocab="${vocab}"
                     data-label="${escapeHtml(item.label)}"
                     data-desc="${escapeHtml(item.description || '')}">Change</button>
                   <button class="btn btn-sm btn-outline-danger btn-remove"
                     data-source-id="${escapeHtml(item.source_id)}"
                     data-vocab="${vocab}"
                     data-aat-id="${item.mapping.aat_id}"
                     data-aat-term="${escapeHtml(item.mapping.aat_term)}">Remove</button>`
                : `<button class="btn btn-sm btn-primary btn-map"
                     data-source-id="${escapeHtml(item.source_id)}"
                     data-vocab="${vocab}"
                     data-label="${escapeHtml(item.label)}"
                     data-desc="${escapeHtml(item.description || '')}">Map</button>`;

            let cells = '';
            for (const col of cfg.columns) {
                switch (col) {
                    case 'source_id':
                        cells += `<td><code>${escapeHtml(item.source_id)}</code></td>`;
                        break;
                    case 'tag_key':
                        cells += `<td><span class="badge bg-secondary bg-opacity-25 text-dark">${escapeHtml(item.tag_key)}</span></td>`;
                        break;
                    case 'label':
                        cells += `<td>${escapeHtml(item.label)}</td>`;
                        break;
                    case 'description':
                        cells += `<td class="text-muted small">${escapeHtml((item.description || '').substring(0, 120))}</td>`;
                        break;
                    case 'count':
                        cells += `<td class="count-cell">${formatCount(item.count)}</td>`;
                        break;
                    case 'mapping':
                        cells += `<td>${mappingCell}</td>`;
                        break;
                }
            }
            cells += `<td class="text-nowrap">${actionBtn}</td>`;

            return `<tr class="${rowClass}" data-source-id="${escapeHtml(item.source_id)}">${cells}</tr>`;
        });

        tbody.innerHTML = rows.join('');

        if (items.length > MAX_RENDER) {
            tbody.innerHTML += `<tr><td colspan="${cfg.columns.length + 1}" class="text-center text-muted py-2">
                Showing first ${MAX_RENDER.toLocaleString()} of ${items.length.toLocaleString()} results. Use the filter to narrow down.
            </td></tr>`;
        }

        if (items.length === 0 && state.data[vocab] !== null) {
            tbody.innerHTML = `<tr><td colspan="${cfg.columns.length + 1}" class="text-center text-muted py-3">
                No matching types found.
            </td></tr>`;
        }
    }

    // -----------------------------------------------------------------------
    // AAT Search Modal
    // -----------------------------------------------------------------------
    let aatModal = null;

    function openMappingModal(vocab, sourceId, label, desc) {
        state.modalSourceVocab = vocab;
        state.modalSourceId = sourceId;
        state.modalSourceLabel = label;
        state.selectedAatId = null;
        state.selectedAatTerm = null;

        document.getElementById('modal-source-id').textContent = sourceId;
        document.getElementById('modal-source-label').textContent = label || '';
        document.getElementById('modal-source-desc').textContent = desc || '';
        document.getElementById('aat-search-input').value = '';
        document.getElementById('aat-results-list').innerHTML = '';
        document.getElementById('aat-search-placeholder').classList.remove('d-none');
        document.getElementById('aat-no-results').classList.add('d-none');
        document.getElementById('aat-select-btn').disabled = true;

        // Pre-populate search with the label
        if (label && label.length >= 2) {
            document.getElementById('aat-search-input').value = label;
            performAatSearch(label);
        }

        if (!aatModal) {
            aatModal = new bootstrap.Modal(document.getElementById('aatSearchModal'));
        }
        aatModal.show();

        // Focus the search input after modal opens
        document.getElementById('aatSearchModal').addEventListener('shown.bs.modal', function onShown() {
            document.getElementById('aat-search-input').focus();
            document.getElementById('aatSearchModal').removeEventListener('shown.bs.modal', onShown);
        });
    }

    function performAatSearch(query) {
        if (!query || query.length < 2) {
            document.getElementById('aat-results-list').innerHTML = '';
            document.getElementById('aat-search-placeholder').classList.remove('d-none');
            document.getElementById('aat-no-results').classList.add('d-none');
            return;
        }

        document.getElementById('aat-search-placeholder').classList.add('d-none');
        document.getElementById('aat-no-results').classList.add('d-none');
        document.getElementById('aat-search-spinner').classList.remove('d-none');

        fetchJSON(`${API.search}?q=${encodeURIComponent(query)}`).then(results => {
            document.getElementById('aat-search-spinner').classList.add('d-none');
            const list = document.getElementById('aat-results-list');

            if (results.length === 0) {
                list.innerHTML = '';
                document.getElementById('aat-no-results').classList.remove('d-none');
                return;
            }

            list.innerHTML = results.map(r => {
                const fclassBadges = (r.fclasses || []).map(
                    f => `<span class="fclass-badge">${escapeHtml(f)}</span>`
                ).join('');
                return `<a href="#" class="list-group-item list-group-item-action aat-result"
                    data-aat-id="${r.aat_id}" data-aat-term="${escapeHtml(r.term)}">
                    <div class="d-flex justify-content-between align-items-start">
                        <div>
                            <strong>${escapeHtml(r.term)}</strong>
                            ${fclassBadges}
                        </div>
                        <span class="aat-id">aat:${r.aat_id}</span>
                    </div>
                    ${r.note ? `<div class="aat-note">${escapeHtml(r.note)}</div>` : ''}
                </a>`;
            }).join('');
        }).catch(err => {
            document.getElementById('aat-search-spinner').classList.add('d-none');
            document.getElementById('aat-results-list').innerHTML =
                `<div class="text-danger p-2">Search error: ${escapeHtml(err.message)}</div>`;
        });
    }

    const debouncedSearch = debounce(function (query) {
        performAatSearch(query);
    }, 300);

    // -----------------------------------------------------------------------
    // Save & Remove handlers
    // -----------------------------------------------------------------------
    function saveMapping(vocab, sourceId, aatId) {
        return postJSON(API.save, {
            source_vocab: vocab,
            source_id: sourceId,
            aat_id: aatId,
        });
    }

    function removeMapping(vocab, sourceId, aatId) {
        return postJSON(API.remove, {
            source_vocab: vocab,
            source_id: sourceId,
            aat_id: aatId,
        });
    }

    function updateRowAfterSave(vocab, sourceId, aatId, aatTerm) {
        const data = state.data[vocab];
        if (data) {
            const item = data.find(d => d.source_id === sourceId);
            if (item) {
                item.mapping = {aat_id: aatId, aat_term: aatTerm};
            }
        }
        applyFilters(vocab);
        renderTable(vocab);
        loadStats();
    }

    function updateRowAfterRemove(vocab, sourceId) {
        const data = state.data[vocab];
        if (data) {
            const item = data.find(d => d.source_id === sourceId);
            if (item) {
                item.mapping = null;
            }
        }
        applyFilters(vocab);
        renderTable(vocab);
        loadStats();
    }

    // -----------------------------------------------------------------------
    // Event binding
    // -----------------------------------------------------------------------
    function init() {
        // Load stats on page load
        loadStats();

        // Load initial tab data
        loadVocabData('geonames');

        // Tab switching
        document.querySelectorAll('#vocabTabs button[data-bs-toggle="tab"]').forEach(btn => {
            btn.addEventListener('shown.bs.tab', function () {
                const vocab = TAB_VOCAB[this.id];
                if (vocab) {
                    state.activeTab = vocab;
                    loadVocabData(vocab);
                }
            });
        });

        // Text filter inputs
        Object.keys(VOCAB_CONFIG).forEach(vocab => {
            const cfg = VOCAB_CONFIG[vocab];

            document.getElementById(cfg.filterId).addEventListener('input', debounce(function () {
                applyFilters(vocab);
                renderTable(vocab);
            }, 200));

            // Tag key filter (OSM/OHM)
            if (cfg.hasTagKey) {
                document.getElementById(cfg.tagKeyFilterId).addEventListener('change', function () {
                    applyFilters(vocab);
                    renderTable(vocab);
                });
            }

            // Column sorting
            document.getElementById(cfg.tableId).querySelectorAll('th.sortable').forEach(th => {
                th.addEventListener('click', function () {
                    sortData(vocab, this.dataset.col);
                    renderTable(vocab);
                });
            });
        });

        // Map / Change button delegation
        document.addEventListener('click', function (e) {
            const mapBtn = e.target.closest('.btn-map');
            if (mapBtn) {
                e.preventDefault();
                const vocab = mapBtn.dataset.vocab;
                const sourceId = mapBtn.dataset.sourceId;
                const label = mapBtn.dataset.label;
                const desc = mapBtn.dataset.desc;
                openMappingModal(vocab, sourceId, label, desc);
            }
        });

        // Remove button delegation
        document.addEventListener('click', function (e) {
            const rmBtn = e.target.closest('.btn-remove');
            if (rmBtn) {
                e.preventDefault();
                const vocab = rmBtn.dataset.vocab;
                const sourceId = rmBtn.dataset.sourceId;
                const aatId = parseInt(rmBtn.dataset.aatId, 10);
                const aatTerm = rmBtn.dataset.aatTerm;

                if (!confirm(`Remove mapping ${sourceId} → aat:${aatId} (${aatTerm})?`)) return;

                rmBtn.disabled = true;
                rmBtn.textContent = '…';
                removeMapping(vocab, sourceId, aatId).then(result => {
                    if (result.status === 'ok') {
                        updateRowAfterRemove(vocab, sourceId);
                    } else {
                        alert('Error: ' + (result.error || 'Unknown error'));
                    }
                }).catch(err => {
                    alert('Error removing mapping: ' + err.message);
                }).finally(() => {
                    rmBtn.disabled = false;
                    rmBtn.textContent = 'Remove';
                });
            }
        });

        // AAT search input (in modal)
        document.getElementById('aat-search-input').addEventListener('input', function () {
            debouncedSearch(this.value.trim());
        });

        // Clear search button
        document.getElementById('aat-search-clear').addEventListener('click', function () {
            document.getElementById('aat-search-input').value = '';
            document.getElementById('aat-results-list').innerHTML = '';
            document.getElementById('aat-search-placeholder').classList.remove('d-none');
            document.getElementById('aat-no-results').classList.add('d-none');
            document.getElementById('aat-select-btn').disabled = true;
            state.selectedAatId = null;
            state.selectedAatTerm = null;
        });

        // AAT result selection (in modal)
        document.getElementById('aat-results-list').addEventListener('click', function (e) {
            e.preventDefault();
            const item = e.target.closest('.aat-result');
            if (!item) return;

            // Deselect previous
            this.querySelectorAll('.aat-result.selected').forEach(el => el.classList.remove('selected'));
            item.classList.add('selected');

            state.selectedAatId = parseInt(item.dataset.aatId, 10);
            state.selectedAatTerm = item.dataset.aatTerm;
            document.getElementById('aat-select-btn').disabled = false;
        });

        // Save Mapping button (in modal)
        document.getElementById('aat-select-btn').addEventListener('click', function () {
            if (!state.selectedAatId || !state.modalSourceVocab || !state.modalSourceId) return;

            this.disabled = true;
            this.textContent = 'Saving…';

            saveMapping(state.modalSourceVocab, state.modalSourceId, state.selectedAatId)
                .then(result => {
                    if (result.status === 'ok') {
                        updateRowAfterSave(
                            state.modalSourceVocab,
                            state.modalSourceId,
                            result.aat_id,
                            result.aat_term
                        );
                        aatModal.hide();
                    } else {
                        alert('Error: ' + (result.error || 'Unknown error'));
                    }
                })
                .catch(err => {
                    alert('Error saving mapping: ' + err.message);
                })
                .finally(() => {
                    this.disabled = false;
                    this.textContent = 'Save Mapping';
                });
        });

        // Copy OSM → OHM button
        const copyBtn = document.getElementById('copy-osm-to-ohm-btn');
        if (copyBtn) {
            copyBtn.addEventListener('click', function () {
                if (!confirm('Copy all OSM tag mappings to OHM for matching tag values?\n\nThis will not overwrite existing OHM mappings.')) return;

                this.disabled = true;
                const origHtml = this.innerHTML;
                this.innerHTML = '<span class="spinner-border spinner-border-sm" role="status"></span> Copying…';

                postJSON(API.copyOsmToOhm, {}).then(result => {
                    if (result.status === 'ok') {
                        alert(`Done! Copied ${result.copied} mappings, skipped ${result.skipped}.`);
                        // Reload OHM data
                        state.data.ohm = null;
                        loadVocabData('ohm');
                        loadStats();
                    } else {
                        alert('Error: ' + (result.error || 'Unknown error'));
                    }
                }).catch(err => {
                    alert('Error: ' + err.message);
                }).finally(() => {
                    this.disabled = false;
                    this.innerHTML = origHtml;
                });
            });
        }
    }

    // -----------------------------------------------------------------------
    // Bootstrap
    // -----------------------------------------------------------------------
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();

