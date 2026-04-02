// /whg/webpack/js/search.js
// WHG v3.5 Search — rebuilt with filter state model

import { errorModal } from './error-modal.js';
import Dateline from './dateline';
import throttle from 'lodash/throttle';
import debounce from 'lodash/debounce';
import { geomsGeoJSON, initSimpleTypeahead } from './utilities';
import CountryParents from './countryParents';
import TypeTreeWidget from './typeTreeWidget';
import filterState from './filterState';
import contextMap from './contextMap';
import RegionSelector from './regionSelector';
import PeriodSelector from './periodSelector';
import PolitySelector from './politySelector';
import './toggle-truncate.js';
import { startFiltersTour, hasSeenTour } from './filtersTour';
import '../css/typeahead.css';
import '../css/dateline.css';
import '../css/search.css';

let results = null;
let searchDisabled = false;
let enteringPortal = false;
let typeTree = null;
let regionSelector = null;
let periodSelector = null;
let politySelector = null;
let temporalMode = 'off'; // 'off' | 'range' | 'undated'
let contextMapZoomed = false;
let exactMatch = false; // When true, disable phonetic/fuzzy matching

// Load window.ccode_hash and window.regions
const countryParents = new CountryParents();
await countryParents.dataLoaded;

// --- Results map: shows search result geometries only ---
let resultsMapParams = {
    container: 'results_map',
    maxZoom: 14,
    style: ['WHG', 'Satellite'],
    fullscreenControl: false,
    downloadMapControl: true,
    drawingControl: false,
    temporalControl: false,
};
let resultsMap = new whg_maplibre.Map(resultsMapParams);

// --- AAT Type Tree Widget ---
function updateTreeBadge() {
    if (!typeTree) return;
    const count = typeTree.selectionCount();
    const $badge = $('#tree_selection_badge');
    if (count > 0) {
        $badge.text(count + ' selected').show();
    } else {
        $badge.hide();
    }
    updateActiveFiltersBadge();
}

// --- Active-filters badge on the Filters toggle button ---
function updateActiveFiltersBadge() {
    let count = 0;
    if (typeTree && typeTree.selectionCount() > 0) count++;
    const state = filterState.get();
    if (state.mode === 'period' && state.spatial.period_id) count++;
    if (state.mode === 'polity' && state.spatial.polity_id) count++;
    if (state.mode === 'timespan' && state.spatial.region_id) count++;
    // Temporal filter counts when mode is not 'off'
    if (temporalMode !== 'off') count++;
    // Spatial viewport counts when the map has been zoomed in
    if (contextMapZoomed) count++;
    // Count non-default authorities
    const defaultAuth = ['gn', 'wd', 'tgn', 'pl', 'iv', 'whg'];
    const currentAuth = [...state.authorities].sort().join(',');
    if (currentAuth !== defaultAuth.sort().join(',')) count++;

    const $badge = $('#active_filters_badge');
    if (count > 0) {
        $badge.text(count).show();
    } else {
        $badge.hide();
    }
}

// --- Check whether enough filters are set to allow a no-name search ---
function hasFilterOnlySearchCriteria() {
    const hasTypes = typeTree && typeTree.selectionCount() > 0;
    if (!hasTypes) return false;

    const state = filterState.get();
    // Temporal filter is active when mode is not 'off'
    const hasTemporal = temporalMode !== 'off';
    const hasPeriod = state.mode === 'period' && state.spatial.period_id;
    const hasPolity = state.mode === 'polity' && state.spatial.polity_id;
    const hasRegion = state.mode === 'timespan' && state.spatial.region_id;

    return hasTemporal || hasPeriod || hasPolity || hasRegion;
}

// --- Dirty state → no longer drives a panel Search button ---
// Search is triggered from the main search bar only.
function updateSearchButton() {
    // No-op: the execute_search button has been removed from the panel.
    // The main search bar buttons (#initiate_search) are managed by toggleButtonState().
}

// Subscribe to filter state changes
filterState.subscribe((key, value, state) => {
    updateSearchButton();
    updateActiveFiltersBadge();
    toggleButtonState();
});

// --- Dateline temporal slider change handler ---
let dateRangeChanged = throttle(() => {
    if (window.dateline) {
        filterState.set('temporal.start_year', window.dateline.fromValue);
        filterState.set('temporal.stop_year', window.dateline.toValue);
        filterState.set('temporal.source', 'manual');
    }
    toggleButtonState();
}, 300);

// --- Context map + results map load ---
function waitMapLoad() {
    const contextReady = contextMap.init();

    const resultsReady = new Promise((resolve) => {
        resultsMap.on('load', () => {
            resultsMap.newSource('places')
                .newLayerset('places', null, 'plain');

            function getFeatureId(e) {
                const features = resultsMap.queryRenderedFeatures(e.point);
                if (features.length > 0) {
                    if (features[0].layer.id.startsWith('places_')) {
                        resultsMap.getCanvas().style.cursor = 'pointer';
                        return features[0].id;
                    }
                }
                resultsMap.getCanvas().style.cursor = 'grab';
                return null;
            }

            resultsMap.on('mousemove', function (e) {
                getFeatureId(e);
            });

            resultsMap.on('click', function (e) {
                $('.result')
                    .eq(getFeatureId(e))
                    .attr('data-map-clicked', 'true')
                    .click();
            });

            resolve();
        });
    });

    return Promise.all([contextReady, resultsReady]);
}

function waitDocumentReady() {
    return new Promise((resolve) => {
        $(document).ready(() => resolve());
    });
}

// Initialise type tree when DOM is ready
waitDocumentReady().then(() => {
    console.log('TypeTreeWidget: DOM ready, constructing widget');
    typeTree = new TypeTreeWidget('#aat_type_tree', {
        onchange: () => {
            updateTreeBadge();
            const ids = typeTree.getSelectedIdentifiers();
            filterState.set('place_types', ids);
            toggleButtonState();
        },
    });
    typeTree.init();

    $('#tree_clear').on('click', function (e) {
        e.preventDefault();
        if (typeTree) {
            typeTree.clearAll();
            updateTreeBadge();
            filterState.set('place_types', []);
        }
    });
});

Promise.all([
    waitMapLoad(),
    waitDocumentReady(),
    Promise.all(select2_CDN_fallbacks.map(loadResource))
]).then(() => {

    // --- Delegated event listener for Portal links ---
    $(document).on('click', '.portal-link', function (e) {
        enteringPortal = true;
        e.stopPropagation();
    });

    // --- Delegated event listener for Result links ---
    $(document).on('click', '.result', function (e) {
        const $clickedResult = $(this);
        const index = $clickedResult.index('.result');

        resultsMap.removeFeatureState({ source: 'places' });
        resultsMap.setFeatureState({ source: 'places', id: index }, { highlight: true });

        const featureCollection = resultsMap.getSource('places')._data?.geojson;

        if ($clickedResult.attr('data-map-clicked') === 'true') {
            $clickedResult.removeAttr('data-map-clicked');
            const $container = $('#result_container');
            const $elem = $clickedResult;
            const duration = 400;

            function startFlash(elem) {
                const existingTimer = elem.data('flashTimer');
                if (existingTimer) {
                    clearTimeout(existingTimer);
                    elem.removeData('flashTimer');
                    elem.removeClass('flash-border');
                }
                elem.addClass('flash-border');
                const t = setTimeout(() => {
                    elem.removeClass('flash-border');
                    elem.removeData('flashTimer');
                }, 3000);
                elem.data('flashTimer', t);
            }

            if ($container.length) {
                const containerTop = $container.offset().top;
                const containerScrollTop = $container.scrollTop();
                const containerHeight = $container.innerHeight();
                const elemTop = $elem.offset().top;
                const elemHeight = $elem.outerHeight(true);
                const targetScrollTop = Math.round(containerScrollTop + (elemTop - containerTop) - (containerHeight / 2) + (elemHeight / 2));
                $container.stop(true).animate({ scrollTop: targetScrollTop }, duration, function () {
                    startFlash($elem);
                });
            } else {
                const elemTop = $elem.offset().top;
                const elemHeight = $elem.outerHeight(true);
                const windowHeight = $(window).height();
                const target = Math.round(elemTop - (windowHeight / 2) + (elemHeight / 2));
                $('html, body').stop(true).animate({ scrollTop: target }, duration, function () {
                    startFlash($elem);
                });
            }
        } else if ($clickedResult.attr('data-map-initialising') === 'true') {
            $clickedResult.removeAttr('data-map-initialising');
            if (featureCollection) {
                resultsMap.fitViewport(bbox(featureCollection), defaultZoom);
            }
        } else {
            if (featureCollection?.features?.length > index) {
                resultsMap.fitViewport(bbox(featureCollection.features[index]), defaultZoom);
            }
        }

        $('.result').removeClass('selected');
        $clickedResult.addClass('selected');
    });

    // --- Initialise the Dateline temporal slider — always open in filter panel ---
    window.dateline = new Dateline({
        fromValue: 800,
        toValue: 1800,
        minValue: -2000,
        maxValue: 2100,
        open: true,
        includeUndated: null, // Managed by the temporal-mode-toggle, not dateline's built-in checkbox
        epochs: null,
        automate: null,
        onChange: dateRangeChanged,
        onClick: () => {
            toggleButtonState();
            filterState.set('temporal.source', 'manual');
        },
    });

    // --- Wire the temporal mode toggle (off / range / undated) ---
    document.querySelectorAll('.temporal-mode-toggle .btn').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('.temporal-mode-toggle .btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            temporalMode = btn.dataset.temporalMode;
            const container = document.getElementById('dateline_container');
            container.classList.toggle('temporal-off', temporalMode === 'off');
            updateActiveFiltersBadge();
            toggleButtonState();
        });
    });

    // --- Initialise the selector widgets ---
    regionSelector = new RegionSelector('#region_selector_container');
    periodSelector = new PeriodSelector('#period_selector_container');
    politySelector = new PolitySelector('#polity_selector_container');

    // --- "Clear all" for the Time & Space column ---
    $('#timespace_clear').on('click', function (e) {
        e.preventDefault();
        // Reset temporal mode to 'off'
        temporalMode = 'off';
        document.querySelectorAll('.temporal-mode-toggle .btn').forEach(b => b.classList.remove('active'));
        const offBtn = document.querySelector('.temporal-mode-toggle .btn[data-temporal-mode="off"]');
        if (offBtn) offBtn.classList.add('active');
        const dlContainer = document.getElementById('dateline_container');
        if (dlContainer) dlContainer.classList.add('temporal-off');
        if (window.dateline) window.dateline.reset(800, 1800);
        // Clear all selector widgets
        if (regionSelector) regionSelector.clear();
        if (periodSelector) periodSelector.clear();
        if (politySelector) politySelector.clear();
        // Clear context map overlay and reset viewport
        contextMap.clearOverlay();
        if (contextMap.map) contextMap.map.reset();
        // Re-engage zoom gate
        contextMapZoomed = false;
        disableSelectorInputs();
        // Reset filter state for the whole time & space section
        filterState.clearTabState('timespan');
        filterState.clearTabState('period');
        filterState.clearTabState('polity');
        // Switch back to the Region tab
        const regionTab = document.getElementById('tab-timespan');
        if (regionTab) {
            const bsTab = bootstrap.Tab.getOrCreateInstance(regionTab);
            bsTab.show();
        }
        updateActiveFiltersBadge();
        toggleButtonState();
    });

    // --- Wire authority checkboxes ---
    document.querySelectorAll('.authority-cb').forEach(cb => {
        cb.addEventListener('change', () => {
            const checked = Array.from(document.querySelectorAll('.authority-cb:checked'))
                .map(el => el.value);
            filterState.set('authorities', checked);
        });
    });

    // --- Wire exact-match toggle ---
    $('#exact_match_toggle').on('click', function () {
        exactMatch = !exactMatch;
        const $btn = $(this);
        $btn.toggleClass('active', exactMatch);
        $btn.attr('aria-pressed', exactMatch);
        $btn.attr('title', exactMatch
            ? 'Exact match: on — requiring exact spelling. Click to include phonetically similar names.'
            : 'Exact match: off — currently including phonetically similar names. Click to require exact spelling.');
    });

    // --- Tab switching: mutual exclusion of temporal authority (§10) ---
    const tabEl = document.getElementById('timespaceTab');
    let previousMode = 'timespan';

    tabEl.addEventListener('shown.bs.tab', (event) => {
        const targetId = event.target.getAttribute('data-bs-target');
        let newMode = 'timespan';
        if (targetId === '#pane-periods') newMode = 'period';
        else if (targetId === '#pane-polities') newMode = 'polity';

        // Clear state of the tab being left
        filterState.clearTabState(previousMode);

        // Clear associated UI
        if (previousMode === 'timespan') {
            if (regionSelector) regionSelector.clear();
            if (window.dateline) {
                window.dateline.reset(800, 1800);
            }
            // Reset temporal mode to 'off'
            temporalMode = 'off';
            document.querySelectorAll('.temporal-mode-toggle .btn').forEach(b => b.classList.remove('active'));
            const offBtn = document.querySelector('.temporal-mode-toggle .btn[data-temporal-mode="off"]');
            if (offBtn) offBtn.classList.add('active');
            const dlContainer = document.getElementById('dateline_container');
            if (dlContainer) dlContainer.classList.add('temporal-off');
        } else if (previousMode === 'period') {
            if (periodSelector) periodSelector.clear();
        } else if (previousMode === 'polity') {
            if (politySelector) politySelector.clear();
        }

        // Clear the context map overlay
        contextMap.clearOverlay();

        filterState.set('mode', newMode);
        previousMode = newMode;

        // Resize context map after tab transition
        setTimeout(() => contextMap.resize(), 100);
    });

    // --- Zoom-gate: disable selector inputs until context map is zoomed ---
    const ZOOM_THRESHOLD = 2; // Must zoom beyond this level to enable selectors

    function updateZoomGate() {
        if (contextMapZoomed) return; // Once enabled, stay enabled
        const map = contextMap.map;
        if (map && map.getZoom() > ZOOM_THRESHOLD) {
            contextMapZoomed = true;
            enableSelectorInputs();
            updateActiveFiltersBadge();
            toggleButtonState();
        }
    }

    function enableSelectorInputs() {
        document.querySelectorAll('.zoom-gated-input').forEach(input => {
            input.disabled = false;
            // Restore original placeholder
            if (input.dataset.originalPlaceholder) {
                input.placeholder = input.dataset.originalPlaceholder;
            }
        });
        document.querySelectorAll('.zoom-gate-notice').forEach(el => el.remove());
    }

    function disableSelectorInputs() {
        // Apply after selectors have rendered their inputs
        setTimeout(() => {
            const zoomMsg = 'Zoom the map first to constrain your search area';
            document.querySelectorAll('.region-search-input, .period-search-input, .polity-search-input').forEach(input => {
                if (!contextMapZoomed) {
                    input.dataset.originalPlaceholder = input.placeholder;
                    input.placeholder = zoomMsg;
                    input.disabled = true;
                    input.classList.add('zoom-gated-input');
                }
            });
        }, 100);
    }

    disableSelectorInputs();

    // Listen for zoom changes on the context map
    if (contextMap.map) {
        contextMap.map.on('zoomend', updateZoomGate);
    } else {
        contextMap.init().then(() => {
            contextMap.map.on('zoomend', updateZoomGate);
        });
    }

    // --- Search is triggered from the main search bar only ---
    // (The execute_search button has been removed from the panel)

    // --- Stored results restore ---
    const storedResults = localStorage.getItem('last_search');
    results = storedResults ? JSON.parse(storedResults) : results;
    $(window).on('beforeunload', function () {
        if (!enteringPortal) {
            localStorage.removeItem('last_search');
        }
    });

    if (results) {
        renderResults(results, true);
    }

    window.whgTypeahead = initSimpleTypeahead('#search_input');

    // Initialise mechanism to prevent reappearance of tooltip on `#search_input`
    const tooltipKey = 'searchTooltipHidden';
    if (localStorage.getItem(tooltipKey)) {
        $('#search_input').tooltip('disable');
    }

    $('#search_input')
        .on('focus', function () {
            $(this).tooltip('hide').tooltip('disable');
            localStorage.setItem(tooltipKey, 'true');
        })
        .on('keyup', function (e) {
            if (e.key === 'Enter' || e.which === 13) {
                e.preventDefault();
                initiateSearch();
            }
            toggleButtonState();
        });

    toggleButtonState();

    if ($('#search_input').data('locate') === true) {
        var e = $.Event('keyup');
        e.key = 'Enter';
        e.which = 13;
        $('#search_input').trigger(e);
    }

    $('#clear_search').on('click', function () {
        if (!$(this).hasClass('disabledButton')) clearResults();
    });

    $('#initiate_search').on('click', function () {
        if (!$(this).hasClass('disabledButton')) initiateSearch();
    });

    // Resize maps when the filters panel finishes its collapse/expand animation
    $('#search_filters')
        .on('shown.bs.collapse', () => {
            contextMap.resize();
            // Start the globe spinning if user hasn't interacted with it yet
            if (!contextMap.spinWasStopped) {
                contextMap.startSpin();
            }
            // Auto-trigger the guided tour on first-ever filters open
            if (!hasSeenTour()) {
                setTimeout(() => startFiltersTour(), 400);
            }
        })
        .on('hidden.bs.collapse', () => {
            contextMap.stopSpin();
            resultsMap.resize();
        });

    // "Take a Tour" link in the landing block
    $(document).on('click', '#start_tour_link', function (e) {
        e.preventDefault();
        // Ensure filters panel is open first, then start tour
        const filtersEl = document.getElementById('search_filters');
        if (!filtersEl.classList.contains('show')) {
            const bsCollapse = bootstrap.Collapse.getOrCreateInstance(filtersEl);
            $(filtersEl).one('shown.bs.collapse', () => {
                contextMap.resize();
                setTimeout(() => startFiltersTour(), 300);
            });
            bsCollapse.show();
        } else {
            startFiltersTour();
        }
    });

    $('#initiate_search, #clear_search').each(function () {
        $(this).tooltip({
            title: function () {
                return $(this).data('title').split('|')[$(this).hasClass('disabledButton') ? 1 : 0];
            }
        });
    });

}).catch(error => console.error('An error occurred:', error));

function toggleButtonState() {
    const hasText = $('#search_input').val().trim() !== '';
    const disable = !hasText && !hasFilterOnlySearchCriteria();
    $('#initiate_search, #clear_search').each(function () {
        $(this).toggleClass('disabledButton', disable);
    });
}

function clearResults() {
    searchDisabled = true;
    if (window.whgTypeahead && typeof window.whgTypeahead.closeDropdown === 'function') {
        window.whgTypeahead.closeDropdown();
    }
    $('#search_input').val('');
    if (typeTree) {
        typeTree.clearAll();
        $('#tree_selection_badge').hide();
    }
    if (window.dateline) {
        window.dateline.reset(800, 1800);
    }
    // Reset temporal mode toggle to 'off'
    temporalMode = 'off';
    document.querySelectorAll('.temporal-mode-toggle .btn').forEach(b => b.classList.remove('active'));
    const offBtn = document.querySelector('.temporal-mode-toggle .btn[data-temporal-mode="off"]');
    if (offBtn) offBtn.classList.add('active');
    const datelineContainer = document.getElementById('dateline_container');
    if (datelineContainer) datelineContainer.classList.add('temporal-off');
    resultsMap.getSource('places').setData(resultsMap.nullCollection());
    resultsMap.reset();
    contextMap.clearOverlay();
    if (contextMap.map) contextMap.map.reset();
    contextMapZoomed = false;
    if (regionSelector) regionSelector.clear();
    if (periodSelector) periodSelector.clear();
    if (politySelector) politySelector.clear();

    filterState.reset();

    // Reset exact-match toggle
    exactMatch = false;
    $('#exact_match_toggle').removeClass('active').attr('aria-pressed', 'false')
        .attr('title', 'Exact match: off — currently including phonetically similar names. Click to require exact spelling.');

    $('#search_content')
        .toggleClass('initial', true)
        .toggleClass('no-results', true);
    $('#search_results').empty();
    localStorage.removeItem('last_search');

    // Re-check default authorities
    document.querySelectorAll('.authority-cb').forEach(cb => {
        cb.checked = ['gn', 'wd', 'tgn', 'pl', 'iv', 'whg'].includes(cb.value);
    });

    searchDisabled = false;
    toggleButtonState();
    updateActiveFiltersBadge();
    updateSearchButton();
}

function renderResults(data, fromStorage = false) {

    let $resultsDiv = $('#search_results');
    $resultsDiv.empty();
    $('#search_content').toggleClass('initial', false);

    if (fromStorage) {
        $('#search_input').val(data.parameters.qstr);
        if (data.parameters.tree_selections && data.parameters.tree_selections.length > 0) {
            const treeCount = data.parameters.tree_selections.length;
            $('#tree_selection_badge').text(treeCount + ' selected').show();
        }
    }

    let featureCollection = data.features ?
        data :
        geomsGeoJSON(data['suggestions']);

    results = featureCollection.features;

    $('#search_content').toggleClass('no-results', results.length == 0);

    results.forEach((feature, index) => {
        let result = feature.properties;
        const count = parseInt(result.linkcount) + 1;
        const pid = result.pid;
        const whg_id = result.whg_id;
        const children = result.children;
        const encodedChildren = encodeURIComponent(children.join(','));

        let resultIdx = count > 1 ? 'whg' : 'pub';

        let html = `<div data-bs-toggle="tooltip" title="Click to zoom on map" class="result ${resultIdx}-result">
	<span>
	  <span class="red-head">${result.title}</span>
	  <span class="float-end small">${resultIdx === 'pub' ? '<i class="fas fa-chain-broken" data-bs-toggle="tooltip" title="This place has not yet been reconciled to any other WHG places" aria-hidden="true" style="margin-right:6px;"></i>' : (count > 1 ? `${count} linked records <i class="fas fa-link"></i>` : '')}
	        <button data-bs-toggle="tooltip" title="Click to view all details for this ${count > 1 ? 'set of linked places' : 'unlinked place'}" class="btn btn-primary btn-sm m-1 portal-link" data-whg-id="${whg_id}" data-pid="${pid}" data-children="${encodedChildren}">
                Place Details
            </button>
	  </span>
	</span>`;

        if (result.dataset) {
            html += `<span class="result-dataset-badge" data-bs-toggle="tooltip" title="Source dataset">${result.dataset}</span>`;
        }

        if (result.descriptions && result.descriptions.length > 0) {
            const desc = result.descriptions[0];
            const langTag = desc.lang ? ` <span class="result-lang">[${desc.lang}]</span>` : '';
            html += `<p class="result-description more-or-less">${desc.value}${langTag}</p>`;
        }

        if (result.names && result.names.length > 0) {
            const nameItems = result.names
                .filter(n => n.toponym && n.toponym !== result.title)
                .map(n => {
                    const lang = n.lang ? `<span class="result-lang">${n.lang}</span>` : '';
                    return `${n.toponym}${lang ? ' ' + lang : ''}`;
                });
            if (nameItems.length > 0) {
                html += `<p class="more-or-less">Names (${nameItems.length}): ${nameItems.join(', ')}</p>`;
            }
        } else if (result.variants && result.variants.length > 0) {
            result.variants.sort((a, b) => {
                const aAscii = /^[\x00-\x7F]/.test(a);
                const bAscii = /^[\x00-\x7F]/.test(b);
                if (aAscii === bAscii) return a.localeCompare(b);
                else if (aAscii && !bAscii) return -1;
                else return 1;
            });
            html += `<p class="more-or-less">Variants (${result.variants.length}): ${result.variants.join(', ')}</p>`;
        }

        if (result.types_full && result.types_full.length > 0) {
            const typeItems = result.types_full.map(t => {
                const label = t.sourceLabel || t.label || '';
                const aat = t.identifier ? ` <a href="http://vocab.getty.edu/page/${t.identifier.replace('aat:', 'aat/')}" target="_blank" class="result-aat-link" data-bs-toggle="tooltip" title="View in AAT: ${t.identifier}">${t.identifier}</a>` : '';
                return `${label}${aat}`;
            }).filter(s => s.trim());
            if (typeItems.length > 0) {
                html += `<p>Type(s): ${typeItems.join('; ')}</p>`;
            }
        } else if (result.types && result.types.length > 0) {
            html += `<p>Type(s): ${result.types.join(', ')}</p>`;
        }

        html += (result.ccodes && result.ccodes.length > 0 && !(result.ccodes.length == 1 && result.ccodes[0] == '')) ?
            `<p>Country Codes: ${result.ccodes.map(ccode => {
                const country = dropdown_data[1].children.find(child => child.id === ccode);
                const countryName = country ? country.text : '';
                return `<span class="pointer" data-bs-toggle="tooltip" title="${countryName}">${ccode}</span>`;
            }).join(', ')}</p>` :
            '';

        if (result.timespans && result.timespans.length > 0) {
            result.timespans.sort((a, b) => a[0] - b[0]);
            html += `<p>Chronology: ${result.timespans.map(span => `${span[0]}-${span[1]}`).join(', ')}</p>`;
        }

        if (result.links && result.links.length > 0) {
            const linkItems = result.links.map(lnk => {
                const id = lnk.identifier || '';
                let icon = 'fas fa-external-link-alt';
                let label = id;
                let url = id;
                if (id.startsWith('wd:') || id.startsWith('Q')) {
                    const qid = id.replace('wd:', '');
                    icon = 'fab fa-wikipedia-w';
                    label = qid;
                    url = `https://www.wikidata.org/wiki/${qid}`;
                } else if (id.startsWith('gn:') || id.match(/^\d{5,}$/)) {
                    const gnid = id.replace('gn:', '');
                    label = `GeoNames ${gnid}`;
                    url = `https://www.geonames.org/${gnid}`;
                } else if (id.startsWith('tgn:')) {
                    const tgnid = id.replace('tgn:', '');
                    label = `TGN ${tgnid}`;
                    url = `http://vocab.getty.edu/page/tgn/${tgnid}`;
                } else if (id.startsWith('dbp:')) {
                    const dbpid = id.replace('dbp:', '');
                    label = `DBpedia`;
                    url = `https://dbpedia.org/resource/${dbpid}`;
                } else if (id.startsWith('viaf:')) {
                    const viafid = id.replace('viaf:', '');
                    label = `VIAF ${viafid}`;
                    url = `https://viaf.org/viaf/${viafid}`;
                } else if (id.startsWith('loc:')) {
                    const locid = id.replace('loc:', '');
                    label = `LoC ${locid}`;
                    url = `https://id.loc.gov/authorities/${locid}`;
                } else if (id.startsWith('http')) {
                    url = id;
                    label = new URL(id).hostname;
                }
                const relType = lnk.type ? `<span class="result-link-type">${lnk.type}</span> ` : '';
                return `${relType}<a href="${url}" target="_blank" data-bs-toggle="tooltip" title="${id}"><i class="${icon}"></i> ${label}</a>`;
            });
            html += `<p class="result-links more-or-less">Links (${linkItems.length}): ${linkItems.join(', ')}</p>`;
        }

        if (result.src_id || result.uri) {
            let srcParts = [];
            if (result.src_id) srcParts.push(`Source ID: ${result.src_id}`);
            if (result.uri) srcParts.push(`<a href="${result.uri}" target="_blank" class="result-uri-link" data-bs-toggle="tooltip" title="${result.uri}">URI</a>`);
            html += `<p class="result-source-info">${srcParts.join(' · ')}</p>`;
        }

        if (result.depictions && result.depictions.length > 0) {
            html += `<div class="result-depictions">`;
            result.depictions.forEach(dep => {
                if (dep.id) {
                    const depTitle = dep.title || 'Depiction';
                    html += `<a href="${dep.id}" target="_blank" data-bs-toggle="tooltip" title="${depTitle}"><img src="${dep.id}" alt="${depTitle}" class="result-depiction-thumb" loading="lazy"/></a>`;
                }
            });
            html += `</div>`;
        }

        if (result.relations && result.relations.length > 0) {
            const relItems = result.relations.map(rel => {
                const label = rel.label || '';
                const relType = rel.relationType || '';
                return `${relType}: ${label}`;
            });
            html += `<p class="more-or-less">Relations (${relItems.length}): ${relItems.join('; ')}</p>`;
        }

        html += `</div>`;
        $resultsDiv.append(html);
    });

    $resultsDiv
        .on('mouseenter', '.portal-link', function () {
            $(this).parents('.result').tooltip('hide');
        })
        .on('click', '.portal-link', function (event) {
            event.preventDefault();
            const id = $(this).data('whg-id') || $(this).data('pid');
            const path = $(this).data('whg-id') ? 'portal/' : 'detail';
            window.location.href = `/places/${id}/${path}`;
        });

    $resultsDiv.find('.more-or-less').toggleTruncate();

    resultsMap.getSource('places').setData(featureCollection);

    if (fromStorage || results.length > 0) {
        const filtersEl = document.getElementById('search_filters');
        if (filtersEl && filtersEl.classList.contains('show')) {
            const bsCollapse = bootstrap.Collapse.getOrCreateInstance(filtersEl);
            $(filtersEl).one('hidden.bs.collapse', () => {
                resultsMap.resize();
            });
            bsCollapse.hide();
        }
        $('.result').first().attr('data-map-initialising', 'true').click();
    } else {
        resultsMap.reset();
        $('#detail').empty();
    }

    // Mark search as clean after results arrive
    filterState.markClean();
    updateSearchButton();
}

function initiateSearch() {
    if (searchDisabled) return;

    updateActiveFiltersBadge();

    const options = gatherOptions();

    if (options.qstr == '' && !hasFilterOnlySearchCriteria()) {
        console.log('Cannot search without a place name or sufficient filters.');
        return;
    }

    console.log('Initiating search...', options);
    $('#search_content').spin();

    $.ajax({
        type: 'POST',
        url: '/search/index/',
        data: JSON.stringify(options),
        contentType: 'application/json',
        headers: { 'X-CSRFToken': csrfToken },
        success: function (data) {
            let localStorageJSON;
            try {
                console.log('...search completed.', data);
                renderResults(data);
                localStorageJSON = JSON.stringify(data);
                localStorage.setItem('last_search', localStorageJSON);
            } catch (error) {
                if (error.name === 'QuotaExceededError') {
                    console.error('LocalStorage quota exceeded. Clearing space.');
                    try {
                        const deletionPrefixes = ['dataset', 'collection'];
                        for (let prefix of deletionPrefixes) {
                            for (let i = localStorage.length - 1; i >= 0; i--) {
                                let key = localStorage.key(i);
                                if (key && key.startsWith(prefix)) {
                                    localStorage.removeItem(key);
                                }
                            }
                        }
                        localStorage.setItem('last_search', localStorageJSON);
                    } catch (retryError) {
                        console.error('Failed to clear sufficient space:', retryError.message);
                    }
                } else {
                    console.error('Error:', error.message);
                }
            }
        },
        error: function (error) {
            console.error('Error:', error);
            errorModal('Sorry, something went wrong with that search.', null, error);
        },
    }).always(function () {
        $('#search_content').stopSpin();
    });
}

function gatherOptions() {
    // Gather from filter state + legacy-compatible fields
    const state = filterState.get();
    const treeIds = typeTree ? typeTree.getSelectedIdentifiers() : [];

    const options = {
        qstr: $('#search_input').val(),
        idx: eswhg,
        fclasses: treeIds.join(','),
        tree_selections: treeIds,
        temporal: temporalMode !== 'off',
        start: window.dateline ? window.dateline.fromValue : '',
        end: window.dateline ? window.dateline.toValue : '',
        undated: temporalMode === 'undated',
        exact: exactMatch,
        // Legacy-compatible empty fields (no drawing, no country/region select2)
        bounds: { type: 'GeometryCollection', geometries: [] },
        regions: [],
        countries: [],
        userareas: [],
        spatial: 'none',
        // New filter state payload for when backend is updated
        filter_state: state,
    };

    return options;
}
