// /whg/webpack/search.js

import {errorModal} from './error-modal.js';
import Dateline from './dateline';
import throttle from 'lodash/throttle';
import debounce from 'lodash/debounce';
import {geomsGeoJSON, initSimpleTypeahead,} from './utilities';
import CountryParents from './countryParents';
import {CountryCacheFeatureCollection} from './countryCache';
import TypeTreeWidget from './typeTreeWidget';
import './toggle-truncate.js';
import '../css/typeahead.css';
import '../css/dateline.css';
import '../css/search.css';

let results = null;
let draw;
let $drawControl;
let countryCache = new CountryCacheFeatureCollection();
let searchDisabled = false;
let enteringPortal = false;
let typeTree = null;

// Load window.ccode_hash and window.regions
const countryParents = new CountryParents();
await countryParents.dataLoaded;

let dateRangeChanged = throttle(() => { // Uses imported lodash function
    toggleButtonState();
    initiateSearch();
}, 300);

// --- Filter map: lives inside the filters panel, carries dateline + draw ---
let filterMapParams = {
    container: 'filter_map',
    maxZoom: 14,
    style: ['WHG'],
    fullscreenControl: false,
    downloadMapControl: false,
    drawingControl: {
        hide: false,
    },
    temporalControl: {
        fromValue: 800,
        toValue: 1800,
        minValue: -2000,
        maxValue: 2100,
        open: false,
        includeUndated: true,
        epochs: null,
        automate: null,
        onChange: dateRangeChanged,
        onClick: initiateSearch,
    },
};
let filterMap = new whg_maplibre.Map(filterMapParams);

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

// --- AAT Type Tree Widget (initialised independently of map/CDN loading) ---
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
    const chronVal = $('#chrononym_input').val();
    if (chronVal && chronVal.trim()) count++;
    const catVal = $('#categorySelector').val();
    if (catVal && catVal !== 'none') count++;
    if (window.dateline && window.dateline.open) count++;
    if (draw && draw.getAll().features.length > 0) count++;
    const $badge = $('#active_filters_badge');
    if (count > 0) {
        $badge.text(count).show();
    } else {
        $badge.hide();
    }
}


// --- Check whether enough filters are set to allow a no-name search ---
function hasFilterOnlySearchCriteria() {
    // At least one place type selected
    const hasTypes = typeTree && typeTree.selectionCount() > 0;
    if (!hasTypes) return false;

    // Plus at least one spatial or temporal constraint
    const hasTemporal = window.dateline && window.dateline.open;
    const hasChrononym = !!($('#chrononym_input').val() || '').trim();
    const hasSpatialCategory = $('#categorySelector').val() !== 'none';
    let hasSpatialEntries = false;
    try {
        const selData = $('#entrySelector').select2('data');
        hasSpatialEntries = selData && selData.length > 0;
    } catch (_) { /* Select2 not yet initialised */ }
    const hasDrawn = draw && draw.getAll().features.length > 0;

    return hasTemporal || hasChrononym || hasSpatialCategory && hasSpatialEntries || hasDrawn;
}

waitDocumentReady().then(() => {
    console.log('TypeTreeWidget: DOM ready, constructing widget');
    typeTree = new TypeTreeWidget('#aat_type_tree', {
        onchange: () => {
            updateTreeBadge();
            toggleButtonState();
            initiateSearch();
        },
    });
    typeTree.init();

    $('#tree_clear').on('click', function (e) {
        e.preventDefault();
        if (typeTree) {
            typeTree.clearAll();
            updateTreeBadge();
        }
    });
});

function waitMapLoad() {
    // Wait for BOTH maps to load
    const filterReady = new Promise((resolve) => {
        filterMap.on('load', () => {
            if (has_areas) {
                filterMap.newSource('userareas')
                    .newLayerset('userareas', 'userareas', 'userareas');
            }
            filterMap.newSource('countries')
                .newLayerset('countries', 'countries', 'countries');
            resolve();
        });
    });

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

    return Promise.all([filterReady, resultsReady]);
}

function waitDocumentReady() {
    return new Promise((resolve) => {
        $(document).ready(() => resolve());
    });
}

Promise.all([
    waitMapLoad(),
    waitDocumentReady(),
    Promise.all(select2_CDN_fallbacks.map(loadResource))
]).then(() => {

    draw = filterMap._draw;
    $drawControl = $(filterMap._drawControl);

    // Delegated event listener for Portal links
    $(document).on('click', '.portal-link', function (e) {
        enteringPortal = true;
        e.stopPropagation();

    });

    // Delegated event listener for Result links
    $(document).on('click', '.result', function (e) {
        const $clickedResult = $(this);
        const index = $clickedResult.index('.result'); // Get index of clicked card

        resultsMap.removeFeatureState({
            source: 'places',
        });
        resultsMap.setFeatureState({
            source: 'places',
            id: index,
        }, {
            highlight: true,
        });

        const featureCollection = resultsMap.getSource('places')._data?.geojson;

        if ($clickedResult.attr('data-map-clicked') === 'true') { // Scroll table
            $clickedResult.removeAttr('data-map-clicked');

            // Prefer scrolling the results container so the list (not whole page) recenters the item.
            const $container = $('#result_container');
            const $elem = $clickedResult;
            const duration = 400; // ms

            // helper to start the flash overlay after scrolling finishes
            function startFlash(elem) {
                // Simple CSS-driven flash: add class then remove after 3s. Clear any previous timer.
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
                // Compute offsets relative to the container and animate its scrollTop so the element is centered.
                const containerTop = $container.offset().top;
                const containerScrollTop = $container.scrollTop();
                const containerHeight = $container.innerHeight();

                const elemTop = $elem.offset().top;
                const elemHeight = $elem.outerHeight(true);

                const targetScrollTop = Math.round(containerScrollTop + (elemTop - containerTop) - (containerHeight / 2) + (elemHeight / 2));

                // Animate then start flash in callback so overlay is positioned correctly
                $container.stop(true).animate({scrollTop: targetScrollTop}, duration, function () {
                    startFlash($elem);
                });
            } else {
                // Fallback: animate whole page so the element is vertically centered in the viewport
                const elemTop = $elem.offset().top;
                const elemHeight = $elem.outerHeight(true);
                const windowHeight = $(window).height();
                const target = Math.round(elemTop - (windowHeight / 2) + (elemHeight / 2));
                $('html, body').stop(true).animate({scrollTop: target}, duration, function () {
                    startFlash($elem);
                });
            }
        } else if ($clickedResult.attr('data-map-initialising') === 'true') {
            $clickedResult.removeAttr('data-map-initialising');
            if (featureCollection) {
                resultsMap.fitViewport(bbox(featureCollection), defaultZoom);
            } else {
                console.warn("Cannot fit map viewport: featureCollection data is missing.");
            }
        } else {
            if (featureCollection?.features?.length > index) {
                resultsMap.fitViewport(bbox(featureCollection.features[index]), defaultZoom);
            } else {
                console.warn(`Cannot fit map viewport: Feature at index ${index} is missing or array is empty.`);
            }
        }

        $('.result').removeClass('selected');
        $clickedResult.addClass('selected');

    });

    function updateAreaMap() {

        if (has_areas) filterMap.clearSource('userareas');
        filterMap.clearSource('countries');

        var data = $('#entrySelector').select2('data');

        function fitMap(features) {
            if (!$('#search_content').hasClass('no-results')) return;
            try {
                filterMap.fitViewport(bbox(features), defaultZoom);
            } catch {
                filterMap.reset();
            }
        }

        if (data.length > 0) {
            if (!!data[0].feature) {
                const userAreas = {
                    type: 'FeatureCollection',
                    features: data.some(feature => feature.feature) ? data.map(feature => feature.feature) : [],
                }
                filterMap.getSource('userareas').setData(userAreas);
                fitMap(userAreas);
            } else {
                const selectedCountries = data.length < 1 || data.some(feature => feature.feature) ? [] :
                    (data.some(region => region.ccodes) ? [].concat(...data.map(region => region.ccodes)) : data.map(country => country.id));
                countryCache.filter(selectedCountries).then(filteredCountries => {
                    filterMap.getSource('countries').setData(filteredCountries);
                    fitMap(filteredCountries);
                });
            }
        } else if ($('#search_content').hasClass('no-results')) filterMap.reset();
    }

    const debouncedUpdates = debounce(() => { // Uses imported lodash function
        updateAreaMap();
    }, 400);

    // Spatial list-entry selector
    $('#entrySelector').prop('disabled', true).select2({
        data: [],
        width: 'element',
        height: 'element',
        placeholder: '(choose type)',
        allowClear: false,
    }).on('change', function (e) {
        if (!searchDisabled) {
            debouncedUpdates();
            initiateSearch();
        } else updateAreaMap();
    })
        .parent().tooltip({
        selector: '.select2-container',
        title: function () {
            return $(this).prev().attr('data-bs-title');
        }
    });

    $('#categorySelector').on('change', function () {
        $('#clearButton').click();
        switch ($(this).val()) {
            case 'regions':
                $('#entrySelector').prop('disabled', false).empty().select2({
                    placeholder: 'None',
                    data: dropdown_data[0].children
                });
                break;
            case 'countries':
                $('#entrySelector').prop('disabled', false).empty().select2({
                    placeholder: 'None',
                    data: dropdown_data[1].children
                });
                break;
            case 'userareas':
                $('#entrySelector').prop('disabled', false).empty().select2({
                    placeholder: 'None',
                    data: user_areas.map(feature => ({
                        id: feature.properties.id,
                        text: feature.properties.title,
                        feature: feature,
                    }))
                });
                break;
            default:
                $('#entrySelector').prop('disabled', true).empty().select2({
                    placeholder: '(choose type)',
                    data: []
                });
                break;
        }
        toggleButtonState();
    });

    $('#clearButton').on('click', function () {
        if ($('#entrySelector').val() !== null) $('#entrySelector').val(null).trigger('change');
    });

    const storedResults = localStorage.getItem('last_search'); // Includes both `.parameters` and `.suggestions` objects
    results = storedResults ? JSON.parse(storedResults) : results;
    $(window).on('beforeunload', function (event) { // Clear any search+results if not navigating away to a portal page
        if (!enteringPortal) {
            localStorage.removeItem('last_search');
        }
    });

    if (results) {
        renderResults(results, true); // Pass a `true` flag to indicate that results are from storage
    } else {
        // Initialise default temporal control
        let datelineContainer = document.createElement('div');
        datelineContainer.id = 'dateline';
        filterMap.getContainer().querySelector('.maplibregl-control-container').appendChild(datelineContainer);
        window.dateline = new Dateline(filterMapParams.temporalControl);
    }

    //$('#advanced_search').hide();

    window.whgTypeahead = initSimpleTypeahead('#search_input');


    function deriveOuterBounds(period) {
        if (!period.when || !Array.isArray(period.when.timespans) || period.when.timespans.length === 0) {
            return {outerStart: null, outerEnd: null};
        }

        let minStart = Infinity;
        let maxEnd = -Infinity;

        for (const span of period.when.timespans) {
            const start = span.start || {};
            const end = span.end || {};

            const s = start.in ?? start.earliest;
            const e = end.in ?? end.latest;

            if (s !== undefined && s !== null) {
                const val = Number(s);
                if (!isNaN(val)) minStart = Math.min(minStart, val);
            }

            if (e !== undefined && e !== null) {
                const val = Number(e);
                if (!isNaN(val)) maxEnd = Math.max(maxEnd, val);
            }
        }

        return {
            outerStart: minStart === Infinity ? null : minStart,
            outerEnd: maxEnd === -Infinity ? null : maxEnd,
        };
    }


    function initialiseChrononymSuggestions() {
        const input = document.querySelector('#chrononym_input');
        if (!input) return;

        const csrfTokenMeta = document.querySelector('meta[name="csrf-token"]');
        const csrfToken = csrfTokenMeta ? csrfTokenMeta.getAttribute('content') : null;
        const urlBase = '/suggest/entity?limit=60&type=period&mode=nosort&prefix=';

        let dropdown = null;
        let activeIndex = -1;
        let suggestions = [];
        let isSelecting = false;

        const debounce = (fn, delay) => {
            let timer;
            return (...args) => {
                clearTimeout(timer);
                timer = setTimeout(() => fn(...args), delay);
            };
        };

        function createDropdown() {
            dropdown = document.createElement('div');
            dropdown.className = 'tt-menu';
            Object.assign(dropdown.style, {
                position: 'absolute',
                zIndex: 1000,
                background: '#fff',
                border: '1px solid #ccc',
                borderRadius: '0 0 4px 4px',
                boxShadow: '0 4px 6px rgba(0,0,0,0.1)',
                maxHeight: '300px',
                overflowY: 'auto',
                boxSizing: 'border-box'
            });

            const parent = input.offsetParent || input.parentNode;
            if (parent && getComputedStyle(parent).position === 'static') {
                parent.style.position = 'relative';
            }
            parent.appendChild(dropdown);
        }

        function clearDropdown() {
            if (dropdown) dropdown.remove();
            dropdown = null;
            activeIndex = -1;
        }

        function highlightActive() {
            if (!dropdown) return;
            const nodes = dropdown.querySelectorAll('.tt-suggestion');
            nodes.forEach((n, i) => {
                n.classList.toggle('tt-cursor', i === activeIndex);
                n.style.background = i === activeIndex ? '#eee' : '#fff';
            });
        }

        function escapeHtml(s) {
            return String(s)
                .replace(/&/g, '&amp;')
                .replace(/</g, '&lt;')
                .replace(/>/g, '&gt;')
                .replace(/"/g, '&quot;')
                .replace(/'/g, '&#39;');
        }

        async function fetchChrononyms(query) {
            if (!query || query.length < 2) {
                clearDropdown();
                return;
            }

            try {
                const res = await fetch(urlBase + encodeURIComponent(query), {
                    method: 'GET',
                    headers: csrfToken ? {'X-CSRF-Token': csrfToken} : {}
                });
                if (!res.ok) throw new Error(res.statusText);
                const data = await res.json();
                // Map API response to array of suggestions (consistent with original)
                suggestions = (data.result || []).map(r => ({
                    id: r.id,
                    name: r.name,
                    description: r.description
                }));
                renderDropdown(suggestions);
            } catch (err) {
                console.warn('Chrononym fetch failed', err);
                clearDropdown();
            }
        }

        const debouncedFetch = debounce(fetchChrononyms, 200);

        function renderDropdown(items) {
            clearDropdown();
            if (!items || !items.length) return;

            createDropdown();
            const rect = input.getBoundingClientRect();
            const parentRect = input.offsetParent?.getBoundingClientRect() || {top: 0, left: 0};
            Object.assign(dropdown.style, {
                top: (rect.bottom - parentRect.top) + 'px',
                left: (rect.left - parentRect.left) + 'px',
                width: rect.width + 'px'
            });

            const list = document.createElement('div');
            list.className = 'tt-dataset tt-dataset-Chrononyms';

            items.forEach((item, i) => {
                const div = document.createElement('div');
                div.className = 'tt-suggestion';
                div.innerHTML = `
            <div>
                <strong>${escapeHtml(item.name)}</strong><br>
                <small>${item.description || ''}</small>
            </div>
        `;
                Object.assign(div.style, {padding: '6px 10px', cursor: 'pointer'});

                // Use mousedown to prevent blur issues
                div.addEventListener('mousedown', e => {
                    e.preventDefault();
                    selectSuggestion(i);
                });

                div.addEventListener('mouseenter', () => {
                    activeIndex = i;
                    highlightActive();
                });

                list.appendChild(div);
            });

            dropdown.appendChild(list);
        }

        function selectSuggestion(index) {
            if (!Array.isArray(suggestions) || index < 0 || index >= suggestions.length) return;

            const item = suggestions[index];
            isSelecting = true;

            // Clear dropdown immediately
            clearDropdown();

            // Fill input
            input.value = item.name || '';
            input.setAttribute('data-chrononym-id', item.id);
            input.focus();

            // Trigger input & Enter
            input.dispatchEvent(new Event('input', {bubbles: true}));
            input.dispatchEvent(new KeyboardEvent('keyup', {
                key: 'Enter',
                code: 'Enter',
                which: 13,
                keyCode: 13,
                bubbles: true
            }));

            setTimeout(() => {
                isSelecting = false;
            }, 100);

            // Fetch entity
            fetch(`/entity/${encodeURIComponent(item.id)}/api`, {
                method: 'GET',
                headers: csrfToken ? {'X-CSRFToken': csrfToken} : {}
            }).then(r => r.json()).then(period => {
                try {
                    const {outerStart, outerEnd} = deriveOuterBounds(period);
                    if (outerStart !== null && outerEnd !== null) {
                        dateline.reconfigure(outerStart, outerEnd, outerStart, outerEnd, true);
                    }
                    draw.deleteAll();
                    if (period.geometry) {
                        if (period.geometry.type === "GeometryCollection") {
                            period.geometry.geometries.forEach(geom => {
                                draw.add({type: "Feature", properties: period.properties || {}, geometry: geom});
                            });
                        } else {
                            draw.add(period);
                        }
                        filterMap.fitViewport(bbox(period));
                        if (window.jQuery) window.jQuery('#clear_chrononym').show();
                    } else {
                        filterMap.reset();
                    }
                } catch (err) {
                    console.error('Error processing entity period:', err);
                }
            }).catch(err => console.error('Error fetching entity:', err));
        }

        // Keyboard navigation and selection
        input.addEventListener('keydown', e => {
            if (!dropdown) {
                if (e.key === 'Enter') {
                    // If user presses enter manually, clear any chrononym id (keep previous behaviour if applicable)
                    input.removeAttribute('data-chrononym-id');
                }
                return;
            }
            const maxIndex = suggestions.length - 1;
            switch (e.key) {
                case 'ArrowDown':
                    activeIndex = Math.min(activeIndex + 1, maxIndex);
                    highlightActive();
                    e.preventDefault();
                    break;
                case 'ArrowUp':
                    activeIndex = Math.max(activeIndex - 1, 0);
                    highlightActive();
                    e.preventDefault();
                    break;
                case 'Enter':
                    if (activeIndex >= 0) {
                        e.preventDefault();
                        selectSuggestion(activeIndex);
                    }
                    break;
                case 'Escape':
                    clearDropdown();
                    break;
            }
        });

        // Input typing => debounce requests
        input.addEventListener('input', e => {
            // Check if the change was caused by a selection
            if (isSelecting) {
                return; // Exit without calling debouncedFetch
            }
            // clear stored chrononym id if user types manually
            input.removeAttribute('data-chrononym-id');
            debouncedFetch(e.target.value);
        });

        // Close dropdown on outside click
        document.addEventListener('click', e => {
            if (!dropdown || dropdown.contains(e.target) || e.target === input) return;
            clearDropdown();
        });

        // clear_chrononym behaviour
        const clearBtn = document.querySelector('#clear_chrononym');
        if (clearBtn) {
            clearBtn.addEventListener('click', () => {
                input.value = '';
                input.removeAttribute('data-chrononym-id');
                if (typeof initiateSearch === 'function') initiateSearch();
            });
        }
    }

    initialiseChrononymSuggestions();

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
            if (e.key === 'Enter' || e.which === 13) { // e.which for older browser compatibility
                e.preventDefault();
                $('#initiate_search').focus();
                initiateSearch();
            }
            // Update search button state
            toggleButtonState();
        });
    // Initialise search button state
    toggleButtonState();


    if ($('#search_input').data('locate') === true) {
        var e = $.Event('keyup');
        e.key = 'Enter';
        e.which = 13;
        $('#search_input').trigger(e);
    }

    $('#clear_search').on('click', function () { // Clear the input, results, and map
        if (!$(this).hasClass('disabledButton')) clearResults();
    });

    $('#initiate_search').on('click', function () {
        if (!$(this).hasClass('disabledButton')) initiateSearch();
    });



    filterMap.on('draw.create', initiateSearch); // draw events fail to register if not done individually
    filterMap.on('draw.delete', initiateSearch);
    filterMap.on('draw.update', initiateSearch);

    // Resize maps when the filters panel finishes its collapse/expand animation
    $('#search_filters')
        .on('shown.bs.collapse', () => {
            filterMap.resize();
        })
        .on('hidden.bs.collapse', () => {
            resultsMap.resize();
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
        $(this)
            //.prop('disabled', disable) // Cannot use this because it disables the title
            .toggleClass('disabledButton', disable)
    });
}

function clearResults() { // Reset all inputs to default values
    searchDisabled = true;
    if (window.whgTypeahead && typeof window.whgTypeahead.closeDropdown === 'function') {
        window.whgTypeahead.closeDropdown();
    }
    $('#search_input').val('');
    if (typeTree) {
        typeTree.clearAll();
        $('#tree_selection_badge').hide();
    }
    window.dateline.reset(filterMapParams.temporalControl.fromValue,
        filterMapParams.temporalControl.toValue,
        filterMapParams.temporalControl.open);
    draw.deleteAll();
    resultsMap.getSource('places').setData(resultsMap.nullCollection());
    resultsMap.reset();
    filterMap.getSource('countries').setData(filterMap.nullCollection());
    filterMap.reset();
    $('#search_content')
        .toggleClass('initial', true)
        .toggleClass('no-results', true);
    $('#search_results').empty();
    localStorage.removeItem('last_search');
    $('#clearButton').click();
    $('#chrononym_input').val('').removeAttr('data-chrononym-id');
    searchDisabled = false;
    toggleButtonState();
    updateActiveFiltersBadge();

}

function renderResults(data, fromStorage = false) {

    let $resultsDiv = $('#search_results');
    $resultsDiv.empty();
    $('#search_content').toggleClass('initial', false);

    if (fromStorage) { // Initialise by setting all inputs to retrieved values
        $('#search_mode').val(data.parameters.mode);
        $('#search_input').val(data.parameters.qstr);

        if (data.parameters.tree_selections && data.parameters.tree_selections.length > 0) {
            // Tree selections were active — show badge
            const treeCount = data.parameters.tree_selections.length;
            $('#tree_selection_badge').text(treeCount + ' selected').show();
        }

        // Initialise temporal control
        let datelineContainer = document.createElement('div');
        datelineContainer.id = 'dateline';
        filterMap.getContainer().querySelector('.maplibregl-control-container').appendChild(datelineContainer);
        window.dateline = new Dateline({
            ...filterMapParams.temporalControl,
            fromValue: data.parameters.start == '' ?
                filterMapParams.temporalControl.fromValue : data.parameters.start,
            toValue: data.parameters.end == '' ?
                filterMapParams.temporalControl.toValue : data.parameters.end,
            open: data.parameters.temporal,
            includeUndated: data.parameters.undated,
        });

        // Initialise drawing
        if (!!data.parameters.bounds && data.parameters.bounds.geometries.length >
            0) {
            data.parameters.bounds.geometries.forEach(geometry => {
                draw.add(geometry);
            });
        }

        searchDisabled = true;
        $('#categorySelector').val(data.parameters.spatial).trigger('change'); // Loads correct dataset into #entrySelector
        $('#entrySelector').val(data.parameters[data.parameters.spatial]).trigger('change');
        searchDisabled = false;

    }

    let featureCollection = data.features ?
        data :
        geomsGeoJSON(data['suggestions']); // `data` may already be a FeatureCollection

    results = featureCollection.features;

    // Update Results
    $('#search_content').toggleClass('no-results', results.length == 0); // CSS hides #search_results

    results.forEach((feature, index) => {
        let result = feature.properties;
        const count = parseInt(result.linkcount) + 1;
        const pid = result.pid;
        const whg_id = result.whg_id;
        const children = result.children;
        const encodedChildren = encodeURIComponent(children.join(','));

        let resultIdx = count > 1 ? 'whg' : 'pub';

        // --- Header row: title, link count badge, Place Details button ---
        let html = `<div data-bs-toggle="tooltip" title="Click to zoom on map" class="result ${resultIdx}-result">
	<span>
	  <span class="red-head">${result.title}</span>
	  <span class="float-end small">${resultIdx === 'pub' ? '<i class="fas fa-chain-broken" data-bs-toggle="tooltip" title="This place has not yet been reconciled to any other WHG places" aria-hidden="true" style="margin-right:6px;"></i>' : (count > 1 ? `${count} linked records <i class="fas fa-link"></i>` : '')}
	        <button data-bs-toggle="tooltip" title="Click to view all details for this ${count > 1 ? 'set of linked places' : 'unlinked place'}" class="btn btn-primary btn-sm m-1 portal-link" data-whg-id="${whg_id}" data-pid="${pid}" data-children="${encodedChildren}">
                Place Details
            </button>
	  </span>
	</span>`;

        // --- Dataset badge ---
        if (result.dataset) {
            html += `<span class="result-dataset-badge" data-bs-toggle="tooltip" title="Source dataset">${result.dataset}</span>`;
        }

        // --- Description (first available, truncated) ---
        if (result.descriptions && result.descriptions.length > 0) {
            const desc = result.descriptions[0];
            const langTag = desc.lang ? ` <span class="result-lang">[${desc.lang}]</span>` : '';
            html += `<p class="result-description more-or-less">${desc.value}${langTag}</p>`;
        }

        // --- Names with language tags ---
        if (result.names && result.names.length > 0) {
            // Group names: show toponym (lang) pairs, skip the title
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
            // Fallback to old variants display
            result.variants.sort((a, b) => {
                const aAscii = /^[\x00-\x7F]/.test(a);
                const bAscii = /^[\x00-\x7F]/.test(b);
                if (aAscii === bAscii) return a.localeCompare(b);
                else if (aAscii && !bAscii) return -1;
                else return 1;
            });
            html += `<p class="more-or-less">Variants (${result.variants.length}): ${result.variants.join(', ')}</p>`;
        }

        // --- Types with AAT identifiers ---
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

        // --- Country codes ---
        html += (result.ccodes && result.ccodes.length > 0 && !(result.ccodes.length == 1 && result.ccodes[0] == '')) ?
            `<p>Country Codes: ${result.ccodes.map(ccode => {
                const country = dropdown_data[1].children.find(child => child.id === ccode);
                const countryName = country ? country.text : '';
                return `<span class="pointer" data-bs-toggle="tooltip" title="${countryName}">${ccode}</span>`;
            }).join(', ')}</p>` :
            '';

        // --- Timespans ---
        if (result.timespans && result.timespans.length > 0) {
            result.timespans.sort((a, b) => a[0] - b[0]);
            html += `<p>Chronology: ${result.timespans.map(span => `${span[0]}-${span[1]}`).join(', ')}</p>`;
        }

        // --- External links (Wikidata, GeoNames, etc.) ---
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

        // --- Source identifier & URI ---
        if (result.src_id || result.uri) {
            let srcParts = [];
            if (result.src_id) srcParts.push(`Source ID: ${result.src_id}`);
            if (result.uri) srcParts.push(`<a href="${result.uri}" target="_blank" class="result-uri-link" data-bs-toggle="tooltip" title="${result.uri}">URI</a>`);
            html += `<p class="result-source-info">${srcParts.join(' · ')}</p>`;
        }

        // --- Depictions (thumbnail images) ---
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

        // --- Relations ---
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
        .on('mouseenter', '.portal-link', function (event) {
            $(this).parents('.result').tooltip('hide');
        })
        .on('click', '.portal-link', function (event) {
            event.preventDefault();
            const id = $(this).data('whg-id') || $(this).data('pid');
            const path = $(this).data('whg-id') ? 'portal/' : 'detail';
            window.location.href = `/places/${id}/${path}`;
        });

    $resultsDiv.find('.more-or-less')
        .toggleTruncate();

    // Update Map & Detail with first result (if any)
    resultsMap.getSource('places').setData(featureCollection);
    $drawControl.toggle(results.length > 0 || draw.getAll().features.length > 0); // Leave control to allow deletion of areas

    if (fromStorage || results.length > 0) {
        // Auto-collapse the filters panel to make room for results
        const filtersEl = document.getElementById('search_filters');
        if (filtersEl && filtersEl.classList.contains('show')) {
            const bsCollapse = bootstrap.Collapse.getOrCreateInstance(filtersEl);
            // Resize results map after the collapse animation finishes
            $(filtersEl).one('hidden.bs.collapse', () => {
                resultsMap.resize();
            });
            bsCollapse.hide();
        }
        // Highlight first result and render its detail
        $('.result').first().attr('data-map-initialising', 'true').click();
    } else {
        resultsMap.reset();
        $('#detail').empty(); // Clear the detail view
    }


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

    // AJAX POST request to SearchView() with the options (includes qstr)
    $.ajax({
        type: 'POST',
        url: '/search/index/',
        data: JSON.stringify(options),
        contentType: 'application/json',
        headers: {'X-CSRFToken': csrfToken}, // Include CSRF token in headers for Django POST requests
        success: function (data) {
            let localStorageJSON;
            try {
                console.log('...search completed.', data);
                renderResults(data);
                localStorageJSON = JSON.stringify(data);
                localStorage.setItem('last_search', localStorageJSON); // Includes both `.parameters` and `.suggestions` objects
            } catch (error) {
                if (error.name === 'QuotaExceededError') {  // Changed from error.code
                    console.error('LocalStorage quota exceeded. Clearing space.');
                    try {
                        const deletionPrefixes = ['dataset', 'collection'];  // Added const
                        for (let prefix of deletionPrefixes) {
                            for (let i = localStorage.length - 1; i >= 0; i--) {
                                let key = localStorage.key(i);
                                if (key && key.startsWith(prefix)) {  // Added null check
                                    localStorage.removeItem(key);
                                }
                            }
                        }
                        localStorage.setItem('last_search', localStorageJSON);
                    } catch (retryError) {  // Named the catch variable
                        console.error('Failed to clear sufficient space in LocalStorage. Error:', retryError.message);
                    }
                } else {
                    // Handle other errors
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

function gatherOptions() { // gather and return option values from the UI

    // --- Type identifiers from the tree widget ---
    const treeIds = typeTree ? typeTree.getSelectedIdentifiers() : [];

    const areaFilter = {
        type: 'GeometryCollection',
        geometries: draw.getAll().features.map(feature => feature.geometry),
    };

    const spatialSelections = $('#entrySelector').select2('data');

    const options = {
        qstr: $('#search_input').val(),
        idx: eswhg, // hard-coded in `search.html` template
        fclasses: treeIds.join(','),
        tree_selections: treeIds,  // persisted for restore
        temporal: window.dateline.open,
        start: window.dateline.open ? window.dateline.fromValue : '',
        end: window.dateline.open ? window.dateline.toValue : '',
        undated: window.dateline.open ? window.dateline.includeUndated : true,
        bounds: areaFilter,
        regions: spatialSelections.some(region => region.ccodes) ? spatialSelections.map(region => region.id) : [],
        countries: spatialSelections.length < 1 || spatialSelections.some(feature => feature.feature) ? [] :
            (spatialSelections.some(region => region.ccodes) ? [].concat(...spatialSelections.map(region => region.ccodes)) : spatialSelections.map(country => country.id)),
        userareas: spatialSelections.some(feature => feature.feature) ? spatialSelections.map(feature => feature.id) : [],
        spatial: $('#categorySelector').val(),
    };

    return options;
}
