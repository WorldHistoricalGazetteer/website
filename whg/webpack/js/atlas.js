// /whg/webpack/js/atlas.js
// WHG Atlas — Hero Map Explorer + Toponymic Search
//
// New entry point for /atlas/ page. Reuses existing modules
// (filterState, TypeTreeWidget, etc.) but wires them
// into the hero-map layout instead of the three-column search layout.

import { errorModal } from './error-modal.js';
import throttle from 'lodash/throttle';
import debounce from 'lodash/debounce';
import { geomsGeoJSON } from './utilities';
import CountryParents from './countryParents';
import TypeTreeWidget from './typeTreeWidget';
import filterState from './filterState';
import heroMap from './heroMap';
import LayerSourcesPalette from './layerSourcesPalette';
import AreaSearchRouter from './areaSearchRouter';
import './toggle-truncate.js';
import '../css/typeahead.css';
import '../css/atlas.css';

/* ═══════════════════════════════════════════════════════════════════
   State
   ═══════════════════════════════════════════════════════════════════ */

let results = null;
let searchDisabled = false;
let enteringPortal = false;
let typeTree = null;
let layerPalette = null;
let areaRouter = null;
let temporalMode = 'off';       // 'off' | 'range' | 'undated'
let searchMode = 'areas';       // 'areas' | 'toponyms'
let exactMatch = false;
let clusterResults = true;
let selectedRegions = [];        // Array of {id, label, admin_level, namespace, geometry}
let areaSearchResults = [];      // Current area search dropdown results
let areaDropdownIndex = -1;

/* ── Temporal range state ── */
let temporalFrom = 800;
let temporalTo = 1800;
const TEMPORAL_MIN = -2000;
const TEMPORAL_MAX = 2100;

/* ═══════════════════════════════════════════════════════════════════
   Init: Load data dependencies
   ═══════════════════════════════════════════════════════════════════ */

const countryParents = new CountryParents();
await countryParents.dataLoaded;

/* ═══════════════════════════════════════════════════════════════════
   Custom temporal range control
   ═══════════════════════════════════════════════════════════════════ */

let temporalRangeChanged = throttle(() => {
    filterState.set('temporal.start_year', temporalFrom);
    filterState.set('temporal.stop_year', temporalTo);
}, 300);

function fillTemporalSlider() {
    const fromSlider = document.getElementById('temporal_from_slider');
    const toSlider = document.getElementById('temporal_to_slider');
    if (!fromSlider || !toSlider) return;
    const range = TEMPORAL_MAX - TEMPORAL_MIN;
    const fromPct = ((temporalFrom - TEMPORAL_MIN) / range) * 100;
    const toPct = ((temporalTo - TEMPORAL_MIN) / range) * 100;
    toSlider.style.background = `linear-gradient(to right,
        #b0bec5 0%, #b0bec5 ${fromPct}%,
        #546e7a ${fromPct}%, #546e7a ${toPct}%,
        #b0bec5 ${toPct}%, #b0bec5 100%)`;
}

function updateTemporalLabels() {
    const fromLabel = document.getElementById('temporal_from_label');
    const toLabel = document.getElementById('temporal_to_label');
    if (fromLabel) fromLabel.textContent = temporalFrom;
    if (toLabel) toLabel.textContent = temporalTo;
}

function wireTemporalControl() {
    const fromSlider = document.getElementById('temporal_from_slider');
    const toSlider = document.getElementById('temporal_to_slider');
    if (!fromSlider || !toSlider) return;

    fromSlider.addEventListener('input', () => {
        temporalFrom = parseInt(fromSlider.value);
        if (temporalFrom >= temporalTo) {
            temporalTo = temporalFrom;
            toSlider.value = temporalTo;
        }
        updateTemporalLabels();
        fillTemporalSlider();
        temporalRangeChanged();
    });

    toSlider.addEventListener('input', () => {
        temporalTo = parseInt(toSlider.value);
        if (temporalTo <= temporalFrom) {
            temporalFrom = temporalTo;
            fromSlider.value = temporalFrom;
        }
        updateTemporalLabels();
        fillTemporalSlider();
        temporalRangeChanged();
    });

    // Temporal mode toggle
    document.querySelectorAll('#temporal_control .temporal-mode-toggle .btn').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('#temporal_control .temporal-mode-toggle .btn')
                .forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            temporalMode = btn.dataset.temporalMode;
            const tc = document.getElementById('temporal_control');
            tc.classList.toggle('temporal-off', temporalMode === 'off');
        });
    });

    // Initial fill
    fillTemporalSlider();
}

function resetTemporalControl() {
    temporalFrom = 800;
    temporalTo = 1800;
    temporalMode = 'off';
    const fromSlider = document.getElementById('temporal_from_slider');
    const toSlider = document.getElementById('temporal_to_slider');
    if (fromSlider) fromSlider.value = temporalFrom;
    if (toSlider) toSlider.value = temporalTo;
    updateTemporalLabels();
    fillTemporalSlider();
    document.querySelectorAll('#temporal_control .temporal-mode-toggle .btn')
        .forEach(b => b.classList.remove('active'));
    const offBtn = document.querySelector('#temporal_control .temporal-mode-toggle .btn[data-temporal-mode="off"]');
    if (offBtn) offBtn.classList.add('active');
    document.getElementById('temporal_control')?.classList.add('temporal-off');
}

/* ═══════════════════════════════════════════════════════════════════
   Map init
   ═══════════════════════════════════════════════════════════════════ */

function waitMapLoad() {
    return heroMap.init().then(() => {
        // Wire click on result features
        heroMap.map.on('click', function (e) {
            const features = heroMap.map.queryRenderedFeatures(e.point);
            if (features.length > 0 && features[0].layer.id.startsWith('places_')) {
                const idx = features[0].id;
                $('#atlas_search_results .result').eq(idx)
                    .attr('data-map-clicked', 'true').click();
            }
        });
        heroMap.map.on('mousemove', function (e) {
            const features = heroMap.map.queryRenderedFeatures(e.point);
            if (features.length > 0 && features[0].layer.id.startsWith('places_')) {
                heroMap.map.getCanvas().style.cursor = 'pointer';
            }
        });
    });
}

function waitDocumentReady() {
    return new Promise((resolve) => $(document).ready(() => resolve()));
}

/* ═══════════════════════════════════════════════════════════════════
   DOM wiring — runs after map + DOM ready
   ═══════════════════════════════════════════════════════════════════ */

Promise.all([
    waitMapLoad(),
    waitDocumentReady(),
    Promise.all(select2_CDN_fallbacks.map(loadResource)),
]).then(() => {

    // ── Start globe spin on load ──
    heroMap.startSpin();

    // ── Initialise Layer Sources palette (inside offcanvas) ──
    layerPalette = new LayerSourcesPalette(
        '#layer_sources_panel', null,
        typeof available_sources !== 'undefined' ? available_sources : []
    );

    // ── Initialise Area Search Router ──
    areaRouter = new AreaSearchRouter(layerPalette);

    // ── Initialise custom temporal range control ──
    wireTemporalControl();

    // ── Wire boundary click events ──
    document.addEventListener('boundary-click', (e) => {
        if (searchMode !== 'areas') return;
        const detail = e.detail;
        if (!detail || !detail.geometry) return;
        addRegionSelection({
            id: `boundary:${detail.namespace || 'osm'}:${detail.id || detail.name}`,
            label: detail.name || 'Unnamed',
            admin_level: detail.admin_level,
            namespace: detail.namespace || 'osm',
            geometry: detail.geometry,
        });
    });

    // ── Search mode toggle ──
    document.querySelectorAll('.search-mode-toggle .btn').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('.search-mode-toggle .btn')
                .forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            switchSearchMode(btn.dataset.searchMode);
        });
    });

    // ── Wire search input ──
    const searchInput = document.getElementById('atlas_search_input');
    const debouncedAreaSearch = debounce(() => performAreaSearch(), 250);

    searchInput.addEventListener('input', () => {
        if (searchMode === 'areas') {
            debouncedAreaSearch();
        }
    });

    searchInput.addEventListener('keydown', (e) => {
        if (searchMode === 'areas') {
            handleAreaDropdownKeydown(e);
        }
    });

    searchInput.addEventListener('keyup', (e) => {
        if (e.key === 'Enter' && searchMode === 'toponyms') {
            initiateToponymSearch();
        }
    });

    // ── Wire buttons ──
    document.getElementById('atlas_initiate_search').addEventListener('click', () => {
        if (searchMode === 'toponyms') initiateToponymSearch();
        else performAreaSearch();
    });

    document.getElementById('atlas_clear_search').addEventListener('click', () => {
        clearAll();
    });

    document.getElementById('atlas_close_results').addEventListener('click', () => {
        hideResultsPanel();
        switchSearchMode('areas');
    });

    // ── Wire exact match toggle ──
    document.getElementById('atlas_exact_match').addEventListener('click', function () {
        exactMatch = !exactMatch;
        this.classList.toggle('active', exactMatch);
        this.setAttribute('aria-pressed', exactMatch);
    });

    // ── Wire clustering toggle ──
    document.getElementById('atlas_clustering_toggle').addEventListener('change', function () {
        clusterResults = this.checked;
    });

    // ── Wire authority checkboxes ──
    document.querySelectorAll('#sources_offcanvas .authority-cb').forEach(cb => {
        cb.addEventListener('change', () => {
            const checked = Array.from(
                document.querySelectorAll('#sources_offcanvas .authority-cb:checked')
            ).map(el => el.value);
            filterState.set('authorities', checked);
        });
    });

    // ── Initialise type tree in categories offcanvas ──
    typeTree = new TypeTreeWidget('#atlas_type_tree', {
        onchange: () => {
            const ids = typeTree.getSelectedIdentifiers();
            filterState.set('place_types', ids);
            updateTreeBadge();
        },
    });
    typeTree.init();

    document.getElementById('atlas_tree_clear').addEventListener('click', (e) => {
        e.preventDefault();
        if (typeTree) {
            typeTree.clearAll();
            filterState.set('place_types', []);
            updateTreeBadge();
        }
    });

    // ── Portal link handler ──
    $(document).on('click', '.portal-link', function (e) {
        enteringPortal = true;
        e.preventDefault();
        const id = $(this).data('whg-id') || $(this).data('pid');
        const path = $(this).data('whg-id') ? 'portal/' : 'detail';
        window.location.href = `/places/${id}/${path}`;
    });

    // ── Result item click handler ──
    $(document).on('click', '#atlas_search_results .result', function () {
        const $el = $(this);
        const index = $el.index('#atlas_search_results .result');

        heroMap.map.removeFeatureState({ source: 'places' });
        heroMap.map.setFeatureState({ source: 'places', id: index }, { highlight: true });

        const fc = heroMap.map.getSource('places')._data?.geojson;

        if ($el.attr('data-map-clicked') === 'true') {
            $el.removeAttr('data-map-clicked');
            scrollToResult($el);
        } else if (fc?.features?.length > index) {
            heroMap.map.fitViewport(bbox(fc.features[index]), { maxZoom: 12, padding: 60 });
        }

        $('#atlas_search_results .result').removeClass('selected');
        $el.addClass('selected');
    });

    // ── Handle pre-populated toponym from URL ──
    if (typeof atlas_toponym !== 'undefined' && atlas_toponym) {
        searchInput.value = atlas_toponym;
        switchSearchMode('toponyms');
        setTimeout(() => initiateToponymSearch(), 300);
    }

    // ── Close area dropdown on outside click ──
    document.addEventListener('click', (e) => {
        const dropdown = document.querySelector('.atlas-region-dropdown');
        if (dropdown && !dropdown.contains(e.target) &&
            e.target.id !== 'atlas_search_input') {
            closeAreaDropdown();
        }
    });

}).catch(error => console.error('Atlas init error:', error));

/* ═══════════════════════════════════════════════════════════════════
   Helper functions
   ═══════════════════════════════════════════════════════════════════ */

function updateTreeBadge() {
    if (!typeTree) return;
    const count = typeTree.selectionCount();
    const $badge = $('#atlas_tree_badge');
    if (count > 0) $badge.text(count + ' selected').show();
    else $badge.hide();
}

function switchSearchMode(mode) {
    searchMode = mode;
    const input = document.getElementById('atlas_search_input');
    const toponymBtns = document.querySelectorAll('.toponym-only-btn');

    if (mode === 'areas') {
        input.placeholder = buildAreasPlaceholder();
        toponymBtns.forEach(btn => btn.style.display = 'none');
        hideResultsPanel();
        heroMap.clearResultFeatures();

        // Ensure whg-context style is active for area search
        heroMap.ensureContextStyle();
    } else {
        const chipLabels = selectedRegions.map(r => r.label).join(', ');
        input.placeholder = chipLabels
            ? `Search within ${chipLabels}…`
            : 'Search for place names…';
        toponymBtns.forEach(btn => btn.style.display = '');
    }
    input.value = '';
    closeAreaDropdown();
}

function buildAreasPlaceholder() {
    if (layerPalette && layerPalette.getAdminLevel() !== null) {
        return `Search for areas…`;
    }
    return 'Search for areas…';
}


/* ── Area search (Explorer mode) ── */

async function performAreaSearch() {
    const query = document.getElementById('atlas_search_input').value.trim();
    if (query.length < 2) {
        closeAreaDropdown();
        return;
    }

    const results = await areaRouter.search(query, {
        adminLevel: layerPalette ? layerPalette.getAdminLevel() : null,
        namespace: layerPalette ? layerPalette.getNamespace() : 'osm',
    });

    areaSearchResults = results;
    areaDropdownIndex = -1;

    if (results.length === 0) {
        renderAreaDropdown([{
            _stub: true,
            label: 'No matching areas found',
            sublabel: 'Try a different name or adjust your admin level',
        }]);
    } else {
        renderAreaDropdown(results);
    }
}

function renderAreaDropdown(items) {
    // Remove existing dropdown
    closeAreaDropdown();

    const dropdown = document.createElement('div');
    dropdown.className = 'atlas-region-dropdown';

    dropdown.innerHTML = items.map((item, i) => `
        <div class="region-result ${item._stub ? 'region-result--stub' : ''}" data-index="${i}">
            <div class="region-result-label">${item.label || ''}</div>
            ${item.sublabel ? `<div class="region-result-sublabel">${item.sublabel}</div>` : ''}
            ${item.source_type ? `<span class="badge bg-secondary" style="font-size:0.6rem">${item.source || ''}</span>` : ''}
        </div>
    `).join('');

    // Mount below the floating search
    document.getElementById('floating_search').appendChild(dropdown);

    // Wire clicks
    dropdown.querySelectorAll('.region-result:not(.region-result--stub)').forEach(el => {
        el.addEventListener('mousedown', (e) => {
            e.preventDefault();
            selectAreaResult(parseInt(el.dataset.index));
        });
    });
}

function closeAreaDropdown() {
    document.querySelectorAll('.atlas-region-dropdown').forEach(el => el.remove());
    areaDropdownIndex = -1;
}

function handleAreaDropdownKeydown(e) {
    const items = document.querySelectorAll('.atlas-region-dropdown .region-result:not(.region-result--stub)');
    if (!items.length) return;

    if (e.key === 'ArrowDown') {
        e.preventDefault();
        areaDropdownIndex = Math.min(areaDropdownIndex + 1, items.length - 1);
        highlightAreaDropdown(items);
    } else if (e.key === 'ArrowUp') {
        e.preventDefault();
        areaDropdownIndex = Math.max(areaDropdownIndex - 1, 0);
        highlightAreaDropdown(items);
    } else if (e.key === 'Enter' && areaDropdownIndex >= 0) {
        e.preventDefault();
        selectAreaResult(areaDropdownIndex);
    } else if (e.key === 'Escape') {
        closeAreaDropdown();
    }
}

function highlightAreaDropdown(items) {
    items.forEach((el, i) => {
        el.classList.toggle('region-result--active', i === areaDropdownIndex);
    });
}

function selectAreaResult(index) {
    const item = areaSearchResults[index];
    if (!item) return;

    if (item._fromIndex && !item.geometry && item.bounds) {
        // Zoom to bounds so user can click polygon on map
        const [west, south, east, north] = item.bounds;
        try {
            heroMap.map.fitBounds([[west, south], [east, north]], {
                padding: 40, maxZoom: 8,
            });
        } catch (e) { /* */ }
        document.getElementById('atlas_search_input').value = '';
        closeAreaDropdown();
        return;
    }

    if (item.geometry) {
        addRegionSelection({
            id: item.id,
            label: item.label,
            admin_level: item.admin_level,
            namespace: item.namespace || 'osm',
            geometry: item.geometry,
        });
    } else if (item.bounds) {
        // Zoom to bounds so user can click on map
        const [west, south, east, north] = item.bounds;
        heroMap.map.fitBounds([[west, south], [east, north]], { padding: 40, maxZoom: 8 });
    }

    document.getElementById('atlas_search_input').value = '';
    closeAreaDropdown();
}

/* ── Region selections ── */

function addRegionSelection(item) {
    if (selectedRegions.some(r => r.id === item.id)) return;
    selectedRegions.push(item);

    // Update overlay on map
    updateSelectionOverlay();
    if (item.geometry) {
        heroMap.fitTo({ type: 'Feature', geometry: item.geometry, properties: {} });
    }

    renderSelectionChips();
}

function removeRegionSelection(id) {
    selectedRegions = selectedRegions.filter(r => r.id !== id);
    if (selectedRegions.length === 0) {
        heroMap.clearOverlay();
    } else {
        updateSelectionOverlay();
    }
    renderSelectionChips();
}

function updateSelectionOverlay() {
    const features = selectedRegions
        .filter(r => r.geometry)
        .map(r => ({
            type: 'Feature',
            geometry: r.geometry,
            properties: { id: r.id, label: r.label },
        }));
    if (features.length > 0) {
        heroMap.setOverlay({ type: 'FeatureCollection', features });
    } else {
        heroMap.clearOverlay();
    }
}

function renderSelectionChips() {
    const container = document.getElementById('selection_chips');
    if (selectedRegions.length === 0) {
        container.innerHTML = '';
        return;
    }
    container.innerHTML = selectedRegions.map(item => `
        <span class="filter-chip" data-region-id="${item.id}">
            <i class="fas fa-vector-square me-1"></i>
            ${escapeHtml(item.label)}
            ${item.admin_level != null ? `<span class="filter-chip-meta">(level ${item.admin_level})</span>` : ''}
            <button type="button" class="filter-chip-dismiss" aria-label="Remove" data-dismiss-id="${item.id}">
                <i class="fas fa-times"></i>
            </button>
        </span>
    `).join(' ');

    container.querySelectorAll('.filter-chip-dismiss').forEach(btn => {
        btn.addEventListener('click', () => removeRegionSelection(btn.dataset.dismissId));
    });
}

/* ── Toponym search ── */

function initiateToponymSearch() {
    if (searchDisabled) return;
    const input = document.getElementById('atlas_search_input');
    const qstr = input.value.trim();

    if (!qstr && !(typeTree && typeTree.selectionCount() > 0)) {
        console.log('Atlas: need a search term or type filter');
        return;
    }

    const options = gatherToponymOptions(qstr);
    console.log('Atlas: initiating toponym search', options);

    showResultsPanel();
    const resultsDiv = document.getElementById('atlas_search_results');
    resultsDiv.innerHTML = '<div class="p-3 text-center"><i class="fas fa-spinner fa-spin"></i> Searching…</div>';

    $.ajax({
        type: 'POST',
        url: '/search/index/',
        data: JSON.stringify(options),
        contentType: 'application/json',
        headers: { 'X-CSRFToken': csrfToken },
        success: function (data) {
            console.log('Atlas: search completed', data);
            renderToponymResults(data);
        },
        error: function (error) {
            console.error('Atlas: search error', error);
            resultsDiv.innerHTML = '<div class="p-3 text-danger">Search failed. Please try again.</div>';
        },
    });
}

function gatherToponymOptions(qstr) {
    const treeIds = typeTree ? typeTree.getSelectedIdentifiers() : [];

    // Build bounds from selected region geometries
    const regionGeometries = selectedRegions
        .filter(r => r.geometry)
        .map(r => r.geometry);

    const bounds = regionGeometries.length > 0
        ? { type: 'GeometryCollection', geometries: regionGeometries }
        : { type: 'GeometryCollection', geometries: [] };

    return {
        qstr: qstr,
        idx: eswhg,
        fclasses: treeIds.join(','),
        types: treeIds,
        temporal: temporalMode !== 'off',
        start: temporalFrom,
        end: temporalTo,
        undated: temporalMode === 'undated',
        exact: exactMatch,
        cluster: clusterResults,
        bounds: bounds,
        regions: [],
        countries: [],
        userareas: [],
        spatial: regionGeometries.length > 0 ? 'region' : 'none',
    };
}

function renderToponymResults(data) {
    const $resultsDiv = $('#atlas_search_results');
    $resultsDiv.empty();

    let featureCollection = data.features ? data : geomsGeoJSON(data['suggestions']);
    results = featureCollection.features;

    const countEl = document.getElementById('atlas_results_count');
    countEl.textContent = `${results.length} result${results.length !== 1 ? 's' : ''}`;

    const noResultsEl = document.getElementById('atlas_no_results');
    noResultsEl.style.display = results.length === 0 ? 'block' : 'none';

    results.forEach((feature, index) => {
        let r = feature.properties;
        const count = parseInt(r.linkcount) + 1;
        const pid = r.pid;
        const whg_id = r.whg_id;
        const children = r.children;
        const encodedChildren = encodeURIComponent(children.join(','));
        let resultIdx = count > 1 ? 'whg' : 'pub';

        let html = `<div class="result ${resultIdx}-result">
            <span>
                <span class="red-head">${r.title}</span>
                <span class="float-end small">
                    ${resultIdx === 'pub'
                        ? '<i class="fas fa-chain-broken" title="Unlinked"></i>'
                        : (count > 1 ? `${count} linked <i class="fas fa-link"></i>` : '')}
                    <button class="btn btn-primary btn-sm m-1 portal-link"
                            data-whg-id="${whg_id}" data-pid="${pid}"
                            data-children="${encodedChildren}">Details</button>
                </span>
            </span>`;

        if (r.dataset) {
            html += `<span class="result-dataset-badge">${r.dataset}</span>`;
        }

        if (r.descriptions && r.descriptions.length > 0) {
            html += `<p class="result-description more-or-less">${r.descriptions[0].value}</p>`;
        }

        if (r.names && r.names.length > 0) {
            const nameItems = r.names
                .filter(n => n.toponym && n.toponym !== r.title)
                .map(n => n.toponym);
            if (nameItems.length > 0) {
                html += `<p class="more-or-less">Names (${nameItems.length}): ${nameItems.join(', ')}</p>`;
            }
        } else if (r.variants && r.variants.length > 0) {
            html += `<p class="more-or-less">Variants (${r.variants.length}): ${r.variants.join(', ')}</p>`;
        }

        if (r.types && r.types.length > 0) {
            html += `<p>Type(s): ${r.types.join(', ')}</p>`;
        }

        if (r.ccodes && r.ccodes.length > 0 && !(r.ccodes.length === 1 && r.ccodes[0] === '')) {
            html += `<p>Countries: ${r.ccodes.map(c => {
                const country = dropdown_data[1].children.find(ch => ch.id === c);
                return `<span title="${country ? country.text : ''}">${c}</span>`;
            }).join(', ')}</p>`;
        }

        if (r.timespans && r.timespans.length > 0) {
            r.timespans.sort((a, b) => a[0] - b[0]);
            html += `<p>Chronology: ${r.timespans.map(s => `${s[0]}–${s[1]}`).join(', ')}</p>`;
        }

        html += `</div>`;
        $resultsDiv.append(html);
    });

    $resultsDiv.find('.more-or-less').toggleTruncate();

    // Show results on the hero map
    heroMap.showResultFeatures(featureCollection);

    // Zoom to results
    if (results.length > 0) {
        heroMap.map.fitViewport(bbox(featureCollection), { maxZoom: 12, padding: { top: 80, right: 400, bottom: 60, left: 80 } });
    }
}

/* ── Results panel show/hide ── */

function showResultsPanel() {
    const panel = document.getElementById('atlas_results_panel');
    panel.classList.remove('atlas-results-hidden');
    // Resize map to accommodate
    setTimeout(() => heroMap.resize(), 350);
}

function hideResultsPanel() {
    const panel = document.getElementById('atlas_results_panel');
    panel.classList.add('atlas-results-hidden');
    heroMap.clearResultFeatures();
    document.getElementById('atlas_search_results').innerHTML = '';
    setTimeout(() => heroMap.resize(), 350);
}

/* ── Clear all ── */

function clearAll() {
    searchDisabled = true;
    const input = document.getElementById('atlas_search_input');
    input.value = '';

    // Clear selections
    selectedRegions = [];
    renderSelectionChips();
    heroMap.clearOverlay();
    heroMap.clearSuggestions();
    heroMap.clearResultFeatures();

    // Reset temporal
    resetTemporalControl();

    // Reset admin level (via layer palette)
    if (layerPalette) layerPalette.resetAdminLevel();

    // Reset type tree
    if (typeTree) {
        typeTree.clearAll();
        filterState.set('place_types', []);
        updateTreeBadge();
    }

    // Reset exact match
    exactMatch = false;
    const emBtn = document.getElementById('atlas_exact_match');
    if (emBtn) { emBtn.classList.remove('active'); emBtn.setAttribute('aria-pressed', 'false'); }

    // Reset clustering
    clusterResults = true;
    const ct = document.getElementById('atlas_clustering_toggle');
    if (ct) ct.checked = true;

    // Reset authorities
    document.querySelectorAll('#sources_offcanvas .authority-cb').forEach(cb => {
        cb.checked = ['gn', 'iv', 'ohm', 'pl', 'tgn', 'tm', 'wd', 'whg'].includes(cb.value);
    });

    // Hide results
    hideResultsPanel();

    // Reset search mode to Areas
    switchSearchMode('areas');
    document.querySelectorAll('.search-mode-toggle .btn').forEach(b => b.classList.remove('active'));
    const areasBtn = document.querySelector('.search-mode-toggle .btn[data-search-mode="areas"]');
    if (areasBtn) areasBtn.classList.add('active');

    // Reset map
    if (heroMap.map) heroMap.map.reset();

    filterState.reset();
    searchDisabled = false;
    closeAreaDropdown();
}

/* ── Scroll helper ── */

function scrollToResult($elem) {
    const $container = $('#atlas_result_container');
    if (!$container.length) return;
    const containerTop = $container.offset().top;
    const containerScrollTop = $container.scrollTop();
    const containerHeight = $container.innerHeight();
    const elemTop = $elem.offset().top;
    const elemHeight = $elem.outerHeight(true);
    const target = Math.round(containerScrollTop + (elemTop - containerTop) - (containerHeight / 2) + (elemHeight / 2));
    $container.stop(true).animate({ scrollTop: target }, 400, function () {
        $elem.addClass('flash-border');
        setTimeout(() => $elem.removeClass('flash-border'), 3000);
    });
}

/* ── Utilities ── */

function escapeHtml(s) {
    return String(s)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}


