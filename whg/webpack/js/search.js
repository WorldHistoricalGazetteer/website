// /whg/webpack/search.js

import {errorModal} from './error-modal.js';
import Dateline from './dateline';
import throttle from 'lodash/throttle';
import debounce from 'lodash/debounce';
import {geomsGeoJSON, initSimpleTypeahead,} from './utilities';
import CountryParents from './countryParents';
import {CountryCacheFeatureCollection} from './countryCache';
import './toggle-truncate.js';
import '../css/typeahead.css';
import '../css/dateline.css';
import '../css/search.css';

let results = null;
let checkboxStates = {};
let draw;
let $drawControl;
let countryCache = new CountryCacheFeatureCollection();
let searchDisabled = false;
let enteringPortal = false;

// Load window.ccode_hash and window.regions
const countryParents = new CountryParents();
await countryParents.dataLoaded;

let dateRangeChanged = throttle(() => { // Uses imported lodash function
    initiateSearch();
}, 300);

let mapParameters = {
    maxZoom: 14,
    style: [
        'WHG',
        'Satellite'
    ],
    fullscreenControl: false,
    downloadMapControl: true,
    drawingControl: {
        hide: false,
    },
    temporalControl: {
        fromValue: 800,
        toValue: 1800,
        minValue: -2000,
        maxValue: 2100,
        open: false,
        includeUndated: true, // null | false | true - 'false/true' determine state of select box input; 'null' excludes the button altogether
        epochs: null,
        automate: null,
        onChange: dateRangeChanged,
        onClick: initiateSearch,
    },
};
let whg_map = new whg_maplibre.Map(mapParameters);

function waitMapLoad() {
    return new Promise((resolve) => {
        whg_map.on('load', () => {
            console.log('Map loaded.');

            /* This codeblock commented-out because labels can now be switched off using the style-switcher
            const style = whg_map.getStyle();
            style.layers.forEach(layer => {
                if (layer.id.includes('label')) {
                    whg_map.setLayoutProperty(layer.id, 'visibility', 'none');
                }
            });*/

            if (has_areas) {
                whg_map.newSource('userareas') // Add empty source
                    .newLayerset('userareas', 'userareas', 'userareas');
            }

            whg_map.newSource('countries') // Add empty source
                .newLayerset('countries', 'countries', 'countries');

            whg_map.newSource('places') // Add empty source
                .newLayerset('places', null, 'plain');

            function getFeatureId(e) {
                const features = whg_map.queryRenderedFeatures(e.point);
                if (features.length > 0) {
                    if (features[0].layer.id.startsWith('places_')) { // Query only the top-most feature
                        whg_map.getCanvas().style.cursor = 'pointer';
                        return features[0].id;
                    }
                }
                whg_map.getCanvas().style.cursor = 'grab';
                return null;
            }

            whg_map.on('mousemove', function (e) {
                if (!whg_map._draw || whg_map._draw.getMode() !== 'draw_polygon') {
                    getFeatureId(e);
                }
            });

            whg_map.on('click', function (e) {
                if (!whg_map._draw || whg_map._draw.getMode() !== 'draw_polygon') {
                    $('.result')
                        .eq(getFeatureId(e))
                        .attr('data-map-clicked', 'true')
                        .click();
                }
            });

            resolve();
        });
    });
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

    draw = whg_map._draw;
    $drawControl = $(whg_map._drawControl);

    // Delegated event listener for Portal links
    $(document).on('click', '.portal-link', function (e) {
        enteringPortal = true;
        e.stopPropagation();

    });

    // Delegated event listener for Result links
    $(document).on('click', '.result', function (e) {
        const $clickedResult = $(this);
        const index = $clickedResult.index('.result'); // Get index of clicked card

        whg_map.removeFeatureState({
            source: 'places',
        });
        whg_map.setFeatureState({
            source: 'places',
            id: index,
        }, {
            highlight: true,
        });

        const featureCollection = whg_map.getSource('places')._data?.geojson;

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
                whg_map.fitViewport(bbox(featureCollection), defaultZoom);
            } else {
                console.warn("Cannot fit map viewport: featureCollection data is missing.");
            }
        } else {
            if (featureCollection?.features?.length > index) {
                whg_map.fitViewport(bbox(featureCollection.features[index]), defaultZoom);
            } else {
                console.warn(`Cannot fit map viewport: Feature at index ${index} is missing or array is empty.`);
            }
        }

        $('.result').removeClass('selected');
        $clickedResult.addClass('selected');

    });

    function updateAreaMap() {

        if (has_areas) whg_map.clearSource('userareas');
        whg_map.clearSource('countries');

        var data = $('#entrySelector').select2('data');

        function fitMap(features) {
            if (!$('#search_content').hasClass('no-results')) return;
            try {
                whg_map.fitViewport(bbox(features), defaultZoom);
            } catch {
                whg_map.reset();
            }
        }

        if (data.length > 0) {
            if (!!data[0].feature) {
                const userAreas = {
                    type: 'FeatureCollection',
                    features: data.some(feature => feature.feature) ? data.map(feature => feature.feature) : [],
                }
                whg_map.getSource('userareas').setData(userAreas);
                fitMap(userAreas);
            } else {
                const selectedCountries = data.length < 1 || data.some(feature => feature.feature) ? [] :
                    (data.some(region => region.ccodes) ? [].concat(...data.map(region => region.ccodes)) : data.map(country => country.id));
                countryCache.filter(selectedCountries).then(filteredCountries => {
                    whg_map.getSource('countries').setData(filteredCountries);
                    fitMap(filteredCountries);
                });
            }
        } else if ($('#search_content').hasClass('no-results')) whg_map.reset();
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
        document.querySelector('.maplibregl-control-container').appendChild(datelineContainer);
        window.dateline = new Dateline(mapParameters.temporalControl);
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
                        whg_map.fitViewport(bbox(period));
                        if (window.jQuery) window.jQuery('#clear_chrononym').show();
                    } else {
                        whg_map.reset();
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

    $('#check_select').on('click', () => { // Advanced Options Place Categories
        $('#adv_checkboxes input').prop('checked', true);
        initiateSearch();
    });

    $('#check_clear').on('click', () => { // Advanced Options Place Categories
        $('#adv_checkboxes input').prop('checked', false);
        initiateSearch();
    });

    $('#adv_checkboxes input').on('click', () => { // Advanced Options Place Categories
        initiateSearch();
    });

    $(document).on('click', '.check_clear, .check_select', (e) => { // Result Filters
        $(e.target).closest('.accordion-collapse')
            .find('.accordion-body input.filter-checkbox')
            .prop('checked', $(e.target).hasClass('check_select'))
            .trigger('change');
    });

    whg_map.on('draw.create', initiateSearch); // draw events fail to register if not done individually
    whg_map.on('draw.delete', initiateSearch);
    whg_map.on('draw.update', initiateSearch);

    $('#initiate_search, #clear_search').each(function () {
        $(this).tooltip({
            title: function () {
                return $(this).data('title').split('|')[$(this).hasClass('disabledButton') ? 1 : 0];
            }
        });
    });

    $('.accordion-button').each(function () { // Initialise Filter Accordions
        var accordion = $($(this).data('bs-target'));
        var accordionHeader = accordion.siblings('.accordion-header');
        var chevron = accordionHeader.find('.accordion-toggle-indicator i');
        accordionHeader.on('click', function () {
            chevron.toggleClass('fa-chevron-up fa-chevron-down');
            accordion.collapse('toggle');
        });
    });

    console.log(whg_map.getStyle());

}).catch(error => console.error('An error occurred:', error));

function toggleButtonState() {
    const disable = $('#search_input').val().trim() == '';
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
    $('#result_facets input[type="checkbox"]').prop('checked', false);
    $('#adv_checkboxes input').prop('checked', true);
    window.dateline.reset(mapParameters.temporalControl.fromValue,
        mapParameters.temporalControl.toValue,
        mapParameters.temporalControl.open);
    draw.deleteAll();
    whg_map.getSource('places').setData(whg_map.nullCollection());
    whg_map.reset();
    whg_map.getSource('countries').setData(whg_map.nullCollection());
    $('#search_content')
        .toggleClass('initial', true)
        .toggleClass('no-results', true)
        .toggleClass('no-filtered-results', false);
    $('#search_results').empty();
    localStorage.removeItem('last_search');
    $('#clearButton').click();
    searchDisabled = false;
    toggleButtonState();

}

function renderResults(data, fromStorage = false) {

    let $resultsDiv = $('#search_results');
    $resultsDiv.empty();
    $('#search_content').toggleClass('initial', false);
    $('#search_content').toggleClass('no-filtered-results', false);

    if (fromStorage) { // Initialise by setting all inputs to retrieved values
        $('#search_mode').val(data.parameters.mode);
        $('#search_input').val(data.parameters.qstr);
        $('#result_facets input[type="checkbox"]').prop('checked', false);

        if (data.parameters.fclasses) {
            const checkedOptions = data.parameters.fclasses.toLowerCase().split(',');
            $('#adv_checkboxes input').each(function (index, checkbox) {
                $(checkbox).prop('checked',
                    checkedOptions.includes($(checkbox).attr('id').split('_')[1]));
            });
        } else {
            $('#adv_checkboxes input').prop('checked', true);
        }

        // Initialise temporal control
        let datelineContainer = document.createElement('div');
        datelineContainer.id = 'dateline';
        document.querySelector('.maplibregl-control-container').appendChild(datelineContainer);
        window.dateline = new Dateline({
            ...mapParameters.temporalControl,
            fromValue: data.parameters.start == '' ?
                mapParameters.temporalControl.fromValue : data.parameters.start,
            toValue: data.parameters.end == '' ?
                mapParameters.temporalControl.toValue : data.parameters.end,
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
    $('#search_content').toggleClass('no-results', results.length == 0); // CSS hides #search_results, #result_facets

    results.forEach((feature, index) => {
        let result = feature.properties;
        const count = parseInt(result.linkcount) + 1;
        const pid = result.pid;
        const whg_id = result.whg_id;
        const children = result.children;
        const encodedChildren = encodeURIComponent(children.join(','));

        let resultIdx = count > 1 ? 'whg' : 'pub';
        // Aberaeron (in 'pub' and 'whg') will have two cards, one for each index
        let html = `<div data-bs-toggle="tooltip" title="Click to zoom on map" class="result ${resultIdx}-result">
	<span>
	  <span class="red-head">${result.title}</span>
	  <span class="float-end small">${resultIdx === 'pub' ? '<i class="fas fa-chain-broken" data-bs-toggle="tooltip" title="This place has not yet been reconciled to any other WHG places" aria-hidden="true" style="margin-right:6px;"></i>' : (count > 1 ? `${count} linked records <i class="fas fa-link"></i>` : '')}
	        <button data-bs-toggle="tooltip" title="Click to view all details for this ${count > 1 ? 'set of linked places' : 'unlinked place'}" class="btn btn-primary btn-sm m-1 portal-link" data-whg-id="${whg_id}" data-pid="${pid}" data-children="${encodedChildren}">
                Place Details
            </button>
	  </span>
	</span>`;

        if (result.variants && result.variants.length > 0) {
            // Sort variants so that strings with ASCII characters precede those with non-ASCII
            result.variants.sort((a, b) => {
                const aAscii = /^[\x00-\x7F]/.test(a);
                const bAscii = /^[\x00-\x7F]/.test(b);
                if (aAscii === bAscii) return a.localeCompare(b);
                else if (aAscii && !bAscii) return -1;
                else return 1;
            });
            html += `<p class="more-or-less">Variants (${result.variants.length}): ${result.variants.join(', ')}</p>`;
        } else {
            html += `<p>Variants: none provided</p>`;
        }

        html += (result.types && result.types.length > 0) ?
            `<p>Type(s): ${result.types.join(', ')}</p>` :
            '';
        html += (result.ccodes && result.ccodes.length > 0 && !(result.ccodes.length == 1 && result.ccodes[0] == '')) ?
            `<p>Country Codes: ${result.ccodes.map(ccode => {
                const country = dropdown_data[1].children.find(child => child.id === ccode);
                const countryName = country ? country.text : '';
                return `<span class="pointer" data-bs-toggle="tooltip" title="${countryName}">${ccode}</span>`;
            }).join(', ')}</p>` :
            '';
        html += (result.fclasses && result.fclasses.length > 0) ?
            `<p>Feature Classes: ${result.fclasses.map(fclass => {
                const facet = adv_filters.find(child => child[0] === fclass);
                const facetName = facet ? facet[1] : '';
                return `<span class="pointer" data-bs-toggle="tooltip" title="${facetName}">${fclass}</span>`;
            }).join(', ')}</p>` :
            '';

        if (result.timespans && result.timespans.length > 0) {
            result.timespans.sort((a, b) => a[0] - b[0]);
            html += `<p>Chronology: ${result.timespans.map(span => `${span[0]}-${span[1]}`).join(', ')}</p>`;
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
            // NB (place#170): this deliberately still prefers whg_id. Switching to the pid
            // form would NOT be a like-for-like canonicalisation — /places/<whg_id>/portal/
            // renders the ES union cluster (findPortalPlaces = place_id + children), while
            // /places/portal/<pid>/ renders the Postgres CloseMatch neighbours, which is
            // generally a narrower set. Changing it is a product decision, not a URL fix,
            // and it waits on Blocker 1.
            const id = $(this).data('whg-id') || $(this).data('pid');
            const path = $(this).data('whg-id') ? 'portal/' : 'detail';
            window.location.href = `/places/${id}/${path}`;
        });

    $resultsDiv.find('.more-or-less')
        .toggleTruncate();

    // Update Map & Detail with first result (if any)
    whg_map.getSource('places').setData(featureCollection);
    $drawControl.toggle(results.length > 0 || draw.getAll().features.length > 0); // Leave control to allow deletion of areas

    if (fromStorage || results.length > 0) {
        // Highlight first result and render its detail
        $('.result').first().attr('data-map-initialising', 'true').click();
    } else {
        whg_map.reset();
        $('#detail').empty(); // Clear the detail view
    }

    buildResultFilters();

}

function buildResultFilters() {

    const {
        typesSet,
        typeCounts,
    } = results.reduce(({
                            typesSet,
                            typeCounts,
                        }, feature) => {
        const result = feature.properties;
        result.types.forEach(type => {
            typesSet.add(type);
            typeCounts[type] = (typeCounts[type] || 0) + 1;
        });
        return {
            typesSet,
            typeCounts,
        };
    }, {
        typesSet: new Set(),
        typeCounts: {},
    });

    const allTypes = Array.from(typesSet).sort();

    var typesShowing = $('#headingTypes').find('.accordion-toggle-indicator i').hasClass('fa-chevron-up');
    if ((allTypes.length <= 5 && !typesShowing) ||
        (allTypes.length > 5 && typesShowing)) {
        $('#headingTypes button').click();
    }

    $('#type_checkboxes').html(allTypes.map(type => {
        const count = typeCounts[type] || 0;
        return `
	    <p><input
	      type="checkbox"
	      id="type_${type.replace(' ', '_')}"
	      value="${type}"
	      class="filter-checkbox type-checkbox"
	      checked="${checkboxStates[type] || false}"
	    />
	    <label for="type_${type}">${type == '' ? 'unspecified' : type} (${count})</label></p>`;
    }).join(''));

    const {
        countriesSet,
        countryCounts,
    } = results.reduce(({
                            countriesSet,
                            countryCounts,
                        }, feature) => {
        const result = feature.properties;
        result.ccodes.forEach(country => {
            countriesSet.add(country);
            countryCounts[country] = (countryCounts[country] || 0) + 1;
        });
        return {
            countriesSet,
            countryCounts,
        };
    }, {
        countriesSet: new Set(),
        countryCounts: {},
    });

    const allCountries = Array.from(countriesSet).sort();

    $('#headingCountries').parent().toggle(allCountries.length > 0);

    var countriesShowing = $('#headingCountries').find('.accordion-toggle-indicator i').hasClass('fa-chevron-up');
    if ((allCountries.length <= 5 && !countriesShowing) ||
        (allCountries.length > 5 && countriesShowing)) {
        $('#headingCountries button').click();
    }

    console.log('allCountries', allCountries, ccode_hash)
    $('#country_checkboxes').html(allCountries.map(country => {
        const cName = ccode_hash[country]['gnlabel'];
        const count = countryCounts[country] || 0;
        return `
	    <p><input
	      type="checkbox"
	      id="country_${country}"
	      value="${country}"
	      class="filter-checkbox country-checkbox"
	      checked="${checkboxStates[country] || false}"
	    />
	    <label for="country_${country}">${cName} (${country}; ${count})</label></p>`;
    }).join(''));

    $('#typesCount').text(`(${allTypes.length})`);
    $('#countriesCount').text(`(${allCountries.length})`);

    $('.filter-checkbox').change(function () {
        // store state
        checkboxStates[this.value] = this.checked;

        // Get all checked checkboxes
        let checkedTypes = $('.type-checkbox:checked').map(function () {
            return this.value;
        }).get();
        let checkedCountries = $('.country-checkbox:checked').map(function () {
            return this.value;
        }).get();

        const filteredResults = results.filter(feature => {
            const hasCommonType = feature.properties.types.some(
                type => checkedTypes.includes(type));
            const hasCommonCountry = allCountries.length == 0 || feature.properties.ccodes.some(
                country => checkedCountries.includes(country));
            return hasCommonType && hasCommonCountry;
        });

        whg_map.getSource('places').setData({
            type: 'FeatureCollection',
            features: filteredResults,
        });
        const $pidLinks = $('#result_container .portal-link');
        $pidLinks.each(function () {
            const show = filteredResults.some(
                feature => feature.properties.pid === $(this).data('pid'));
            $(this).closest('.result').toggle(show);
        });
        $('#search_content').toggleClass('no-filtered-results', filteredResults.length == 0);

    });

}

function initiateSearch() {

    if (searchDisabled) return;

    const options = gatherOptions();

    if (options.qstr == '') {
        console.log('Cannot search without an input place name.');
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

    const fclasses = $('#adv_checkboxes input:checked').map(function () {
        return $(this).val();
    }).get(); // .get() converts jQuery object to an array

    const areaFilter = {
        type: 'GeometryCollection',
        geometries: draw.getAll().features.map(feature => feature.geometry),
    };

    const spatialSelections = $('#entrySelector').select2('data');

    const options = {
        qstr: $('#search_input').val(),
        idx: eswhg, // hard-coded in `search.html` template
        fclasses: fclasses.join(','),
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
