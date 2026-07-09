// /whg/webpack/portal.js

import throttle from 'lodash/throttle';
import {attributionString, deepCopy, geomsGeoJSON} from './utilities';
import Historygram from './historygram';
import {popupFeatureHTML} from './getPlace.js';
import './toggle-truncate.js';
import './notes.js';

import '../css/mapAndTableMirrored.css';
import '../css/dateline.css';
import '../css/portal.css';

const payload = JSON.parse($('#payload_data').text());
$('#collection_list').data('allCollections', [...new Set(payload.flatMap(place => place.collections))]);

let mapParameters = {
    zoom: 4, // Required initially to retrieve ecoregion data from the rendered layers
    center: centroid,
    maxZoom: 17,
    style: [
        'whg-portal',
        'Satellite'
    ],
    /*    basemap: ['natural-earth-1-landcover', 'natural-earth-2-landcover', 'natural-earth-hypsometric-noshade'],
        basemap: 'natural-earth-2-landcover',*/
    fullscreenControl: true,
    terrainControl: true,
    temporalControl: true
}

let whg_map = new whg_maplibre.Map(mapParameters);

const noSources = $('<div>').html('<i>None - please adjust time slider.</i>').hide();

let featureCollection;
let relatedFeatureCollection;
let nearbyFeatureCollection;
let showingRelated = false;

let nearPlacePopup = new whg_maplibre.Popup({
    closeButton: false,
})
    .addTo(whg_map);
$(nearPlacePopup.getElement()).hide();

function waitMapLoad() {
    return new Promise((resolve) => {
        whg_map.on('load', () => {
            console.log('Map loaded.');

            whg_map
                .newSource('nearbyPlaces') // Add empty source
                .newLayerset('nearbyPlaces', 'nearbyPlaces', 'nearby-places');

            whg_map
                .newSource('places') // Add empty source
                .newLayerset('places', null, 'plain')
                .addFilter(['!=', 'outOfDateRange', true]);

            const dateRangeChanged = throttle((fromValue, toValue, includeUndated) => { // Uses imported lodash function
                filterSources(fromValue, toValue, includeUndated);
            }, 300);

            if (!!mapParameters.temporalControl) {
                new Historygram(whg_map, allts, dateRangeChanged);
            }

            // 1. Initialize empty to prevent reference errors later
            let ecoAdminFeatures = [];

            // 2. Wrap the projection and query in a Try-Catch block
            try {
                // Only attempt this if centroid looks valid
                if (centroid && Array.isArray(centroid) && centroid.length === 2) {
                    const screenPoint = whg_map.project(centroid);

                    // Ensure the projected point is actually within the canvas dimensions
                    const canvas = whg_map.getCanvas();
                    const isOnScreen = screenPoint.x >= 0 && screenPoint.x <= canvas.width &&
                        screenPoint.y >= 0 && screenPoint.y <= canvas.height;

                    if (isOnScreen) {
                        ecoAdminFeatures = whg_map.queryRenderedFeatures(screenPoint).filter(feature => {
                            return feature.source === 'ecoregions' || feature.source === 'natural_earth';
                        });
                    } else {
                        console.warn("Centroid is off-screen; skipping biome lookup.");
                    }
                }
            } catch (error) {
                console.warn("Could not query eco/admin features (Terrain not ready):", error);
            }

            // 3. Run loop on the result (safely)
            ecoAdminFeatures.forEach(feature => {
                if (feature.layer['source-layer'] === 'biomes' && !!feature.properties.label) {
                    geoData.biome.name = feature.properties.label;
                    geoData.biome.url = `https://en.wikipedia.org/wiki/${feature.properties.label.charAt(0) + feature.properties.label.slice(1).toLowerCase().replace('&', 'and').replace(' ', '_')}`;
                } else if (feature.layer['source-layer'] === 'ecoregions' && !!feature.properties.label) {
                    geoData.ecoregion.name = feature.properties.label;
                    geoData.ecoregion.url = `https://en.wikipedia.org/wiki/${feature.properties.label.charAt(0) + feature.properties.label.slice(1).toLowerCase().replace('&', 'and').replace(' ', '_')}`;
                } else if (feature.layer['source-layer'] === 'countries' || feature.layer['source-layer'] === 'states') {
                    geoData.admin.push(feature.properties['NAME'] || feature.properties['name']);
                }
            });

            whg_map
                .setZoom(mapParameters.maxZoom) // Maximum resolution for terrain elevation query
                .once('idle', function () {
                    let elevation = 0;
                    try {
                        elevation = -whg_map.queryTerrainElevation(centroid, {exaggerated: false});
                        console.log('raw elevation', elevation);
                    } catch (error) {
                        console.warn("Could not query terrain elevation:", error);
                    }
                    geoData.elevation = Math.max(0, Math.floor(elevation));
                    whg_map.getContainer().style.opacity = 1;
                    resolve();
                });

            whg_map.on('mousemove', function (e) {
                const features = whg_map.queryRenderedFeatures(e.point);

                function clearHighlights() {
                    if (whg_map.highlights.length > 0) {
                        whg_map.getCanvas().style.cursor = 'grab';
                        whg_map.clearHighlights();
                        $('.source-box').removeClass('highlight');
                    }
                }

                if (!showingRelated) {
                    if (features.length > 0) {
                        clearHighlights();
                        features.forEach(feature => {
                            if (feature.layer.id.startsWith('places_')) {
                                whg_map.highlight(feature);
                                $('.source-box').eq(feature.id).addClass('highlight');
                            }
                        });
                        features.forEach(feature => { // Check nearbyPlaces
                            if (feature.layer.id.startsWith('nearbyPlaces_')) {
                                nearPlacePopup
                                    .setLngLat(e.lngLat)
                                    .setHTML(popupFeatureHTML(feature, false)); // second parameter indicates clickability
                            }
                            $(nearPlacePopup.getElement()).show();
                        });
                        if (whg_map.highlights.length > 0) {
                            whg_map.getCanvas().style.cursor = 'pointer';
                        }

                    } else {
                        clearHighlights();
                        $(nearPlacePopup.getElement()).hide();
                    }
                }

            });

            whg_map.on('click', function () {
                if (!showingRelated && whg_map.highlights.length > 0) {
                    console.log(whg_map.highlights);
                    whg_map.fitViewport(bbox(geomsGeoJSON(whg_map.highlights)), defaultZoom);
                }
            });

        });
    });
}

function waitDocumentReady() {
    return new Promise((resolve) => {
        $(document).ready(() => {

            $('#dataset_content').spin();
            let truncation = $('h5.more-or-less').text().split(';').slice(0, 4).join(';').length;
            $('h5.more-or-less, b.more-or-less').toggleTruncate(truncation, {'ellipsis': '', 'moreText': '. . .'});

            let checked_cards = []

            document.querySelectorAll('input[name="r_anno"]').forEach(radio => {
                radio.addEventListener('click', function () {
                    // Add the data-id value of the radio input to the checked_cards array
                    const pid = $(this).data('id');
                    checked_cards = [pid];
                    console.log('checked_cards', checked_cards);
                    // Unhide the #addtocoll span
                    document.getElementById('addtocoll').style.display = 'block';
                });
            });

            // Show modal dialog when needed
            $("#addchecked").click(function () {
                $("#addtocoll_popup").modal('show');
            });

            initCollectionForm();

            resolve();
        });
    });
}

Promise.all([waitMapLoad(), waitDocumentReady()])
    .then(() => {

        payload.forEach(place => { // Ensure that years are all base-10 integers
            place.timespans = place.timespans.map(timespan => timespan.map(year => parseInt(year, 10)));
        });

        const allTimespans = payload.flatMap(place => place.timespans);
        const {
            earliestStartYear,
            latestEndYear
        } = allTimespans.reduce((acc, timespan) => {
            const [startYear, endYear] = timespan;
            return {
                earliestStartYear: Math.min(acc.earliestStartYear, startYear),
                latestEndYear: Math.max(acc.latestEndYear, endYear)
            };
        }, {
            earliestStartYear: Infinity,
            latestEndYear: -Infinity
        });

        const temporalRange = Number.isFinite(earliestStartYear) && Number.isFinite(latestEndYear) ?
            `, and a temporal extent of <b>${Math.abs(earliestStartYear)}${earliestStartYear < 0 ? 'B' : ''}CE to ${Math.abs(latestEndYear)}${latestEndYear < 0 ? 'B' : ''}CE</b>` :
            '';

        const allNameVariants = payload.flatMap(place => place.names.map(label => label.label));
        const distinctNameVariants = new Set(allNameVariants);

        const allTypes = payload
            .flatMap(place => place.types)
            .filter(type => type.label !== null)
            .map(type => type.label);
        const distinctTypes = new Set(allTypes);
        const distinctTypesArray = [...distinctTypes].sort();
        const distinctTypesText = distinctTypesArray.length > 0 ?
            `  as having type${distinctTypesArray.length > 1 ? 's' : ''} ${distinctTypesArray.map((name, index) => index < distinctTypesArray.length - 1 ? `<b>${name}</b>, ` : `<b>${name}</b>`).join('').replace(/,([^,]*)$/, `${distinctTypesArray.length == 2 ? '' : ','} and$1`)}` :
            '';

        $('#gloss').append($('<p>').addClass('mb-1 smallish').html(`
			This place is attested (so far) in the ${payload.length == 1 ? 'unlinked source' : `<b>${payload.length}</b> source${payload.length > 1 ? 's' : ''}`} listed below${distinctTypesText}, 
			with <b>${distinctNameVariants.size}</b> distinct name variant${distinctNameVariants.size > 1 ? 's' : ''}${temporalRange}.
		`));

        const elevationString = geoData.elevation !== null ?
            ` at an elevation of about <b>${geoData.elevation} metres</b>` :
            '';
        const adminString = geoData.admin.length > 0 ?
            ` within the modern boundaries of ${geoData.admin.map((name, index) => index < geoData.admin.length - 1 ? `<b>${name}</b>, ` : `<b>${name}</b>`).join('').replace(/,([^,]*)$/, `${geoData.admin.length == 2 ? '' : ','} and$1`)}, and` :
            '';
        const ecoString = geoData.ecoregion.name ?
            ` within the <a href="${geoData.ecoregion.url}" target="_blank">${geoData.ecoregion.name}</a> ecoregion${geoData.biome.name ? ` and <a href="${geoData.biome.url}" target="_blank">${geoData.biome.name}</a> biome` : ''}` :
            '';
        $('<p class="map-data">').addClass('mb-1').html(`
		    It lies${elevationString}${adminString}${ecoString}.
		    <!--<span class="asterisk" data-bs-toggle="tooltip" data-bs-title="Information in this paragraph is based on a point at the centroid of the associated source geometries.">*</span>-->
		`).insertAfter($('#gloss').find('p:first'));

        $('#gloss').append($('<span id="collectionInfo">'));

        $('#sources').find('h6').html(`${payload.length == 1 ? 'Unlinked Source' : `${payload.length} Source${payload.length > 1 ? 's' : ''}`} <span id="filterCount"></span>`);

        payload[0]['primary'] = true; // Payload arrives with Places in descending link-count order
        payload.forEach(place => { // Reverse-sort each set of timespans by end year
            place.timespans.sort((a, b) => b[1] - a[1]);
        });

        payload.sort((a, b) => { // Sort places based on the latest end year
            const endYearA = a.timespans.length > 0 ? a.timespans[0][1] : Number.NEGATIVE_INFINITY;
            const endYearB = b.timespans.length > 0 ? b.timespans[0][1] : Number.NEGATIVE_INFINITY;
            return endYearB - endYearA;
        });

        function extractCoordinates(geom) {
            const coordinates = [];

            geom.forEach(g => {
                if (g.type !== 'Point') {
                    return JSON.stringify(geom);
                } else if (g.type === 'GeometryCollection') {
                    g.geometries.forEach(subGeom => {
                        if (subGeom.type !== 'Point') {
                            return JSON.stringify(geom);
                        } else if (subGeom.coordinates) {
                            coordinates.push(subGeom.coordinates);
                        }
                    });
                } else if (g.coordinates) {
                    coordinates.push(g.coordinates);
                }
            });

            return JSON.stringify(coordinates);
        }

        payload.forEach(place => {
            const coordinates = !!place.geom ? extractCoordinates(place.geom) : null;
            const sourceHTML = `
				<div class="source-box${!!place.primary ? ' primary-place' : ''}"${payload.length == 1 ? '' : ` data-bs-toggle="tooltip" data-bs-title="${!!place.primary ? 'This is considered to be the <i>Primary Source</i>. ' : ''}Click to zoom map to features associated with this source."`}>
		            <span class="notes" ${userId == false ? '' : `data-user-id="${userId}" `}data-place-id="${place.place_id}">
		            	${place.notes.map(note => `<p title="${note.tag}" data-bs-toggle="tooltip" data-creator="${note.user}" data-note-id="${note.id}">${note.note}</p>`).join('')}
		            </span>
		            in: <a class="pop-link pop-dataset"
		                   data-id="${place.dataset.id}" 
		                   data-bs-toggle="popover"
                           ${place.dataset.show_link === false ? '' : `href="/datasets/${place.dataset.id}/places"`}
						   tabindex="0" 
						   rel="clickover">
		                ${place.dataset.title.replace('(stub) ', '').substring(0, 25)}
		            </a>
					<div class="name-variants toggle-truncate">
					    <strong class="larger">${place.title}</strong> ${place.names.map(label => label.label.trim()).join('; ')}
					</div>
		            ${coordinates ? `<div>${Array.isArray(JSON.parse(coordinates)) ? 'Coordinates' : 'Geometry'}: <a class="clip-coordinates" data-coordinates="${coordinates}" data-bs-toggle="tooltip" title="copy to clipboard"><i class="fas fa-clipboard linky"></i></a></div>` : ''}
		            ${place.types.length > 0 ? `<div>Type${place.types.length > 1 ? 's' : ''}: ${place.types.map(type => type.label).join(', ')}</div>` : ''}
	    			${place.timespans.length > 0 ? `<div>Chronology: ${place.timespans.reverse().map(timespan => timespan.join('-')).join(', ')}</div>` : ''}
	    			${(window.WHGSuggest && window.WHGSuggest.canSuggest) ? `<div class="source-suggest mt-1">${window.WHGSuggest.buttonHTML(place.place_id)} ${window.WHGSuggest.insetHTML(place.place_id)}</div>` : ''}
    			</div>
	        `;

            $('#sources').append(sourceHTML);
        });
        $('#sources .toggle-truncate').toggleTruncate();
        $('#sources').height($('#sources').height()); // Fix height to prevent change when content is hidden
        $('.notes').notes();
        // Community record corrections (plan-record-suggestions): fill the per-source pending insets.
        if (window.WHGSuggest) window.WHGSuggest.mountInsets(document.getElementById('sources'));

        // For single source (unlinked), the place_id is the primary source
        if (payload.length === 1) {
            $('#collection_form #primarySource')
                .html(`<option value="${payload[0].place_id}" selected>${payload[0].dataset.title}: ${payload[0].title}</option>`)
                .closest('.form-group');
            $('#sourceOptions').hide(); // Hide the dropdown since there's only one option
        } else {
            var sourceOptions = payload.map(function (item) {
                return `<option value="${item.place_id}"${!!item.primary ? ' selected' : ''}>${item.dataset.title}: ${item.title}</option>`;
            }).join('');
            $('#collection_form #primarySource')
                .html(sourceOptions)
                .closest('.form-group')
                .show(); // Show the dropdown for multiple sources
        }

        updateCollections();
        var chevron = $('#source_detail h6 i.fas');
        var collectionList = $('#collection_list');
        collectionList.on('show.bs.collapse hide.bs.collapse', function () {
            chevron.toggleClass('fa-chevron-down fa-chevron-up');
        });
        $('#source_detail h6').click(function () {
            collectionList.collapse('toggle');
        });

        $('#sources').append(noSources);

        if (userAuthenticated) {
            const addToCollectionButton = `
                <span id="addtocoll" class="me-1 small">
                    <span id="added_flash" class="mr-2 hidden" style="background-color: yellow; position:absolute; top:10px; right:10px;"> added! </span>
                    <button id="addchecked" class="btn btn-warning btn-sm" data-bs-toggle="modal" data-bs-target="#addtocoll_popup">
                        <i class="fas fa-plus-circle"></i> Add to a Collection
                    </button>
                </span>
            `;

            // Insert the button at the top of the #sources div
            $('#sources h6').append(addToCollectionButton);
        }

        featureCollection = geomsGeoJSON(payload);
        whg_map
            .getSource('places')
            .setData(featureCollection);
        whg_map
            .reset(false)
            .once('idle', function () {
                // Do not use fitBounds or flyTo due to padding bug in MapLibre/Maptiler
                whg_map.fitViewport(bbox(featureCollection), defaultZoom);
            });

        $('#textContent')
            .on('mousemove', '#sources:not([disabled]) .source-box', function () {
                $(this)
                    .addClass('highlight');
                const index = $(this).index() - 1;
                whg_map.setFeatureState({
                    source: 'places',
                    id: index
                }, {
                    highlight: true
                });
            })
            .on('mouseleave', '#sources:not([disabled]) .source-box', function () {
                $(this)
                    .removeClass('highlight');
                const index = $(this).index() - 1;
                whg_map.setFeatureState({
                    source: 'places',
                    id: index
                }, {
                    highlight: false
                });
                whg_map.fitViewport(bbox(featureCollection), defaultZoom);
            })
            .on('click', '#sources:not([disabled]) .source-box', function () {
                $(this)
                const index = $(this).index() - 1;
                whg_map.fitViewport(bbox(featureCollection.features[index].geometry), defaultZoom);
            })

        // Show/Hide related Collection (propagate event delegation to dynamically added elements)
        $('#collection_list').on('click', '.show-collection', function (e) {
            e.preventDefault();
            const parentItem = $(this).parent('li');
            parentItem.toggleClass('showing');
            if (parentItem.hasClass('showing')) {
                parentItem.siblings().removeClass('showing');
                $.get("/search/collgeom", { // TODO: This doesn't appear to be fetching geometries updated with new additions ######################
                        coll_id: $(this).data('id')
                    },
                    function (collgeom) {
                        relatedFeatureCollection = collgeom;
                        console.log('coll_places', relatedFeatureCollection);
                        whg_map.getSource('places').setData(relatedFeatureCollection);
                        whg_map.fitViewport(bbox(relatedFeatureCollection), defaultZoom);
                    });
                showingRelated = true;
                $('#sources, .source-box').attr('disabled', true);
            } else {
                whg_map.getSource('places').setData(featureCollection);
                whg_map.fitViewport(bbox(featureCollection), defaultZoom);
                showingRelated = false;
                $('#sources, .source-box').removeAttr('disabled');
            }
        });

        document.querySelectorAll('.toggle-link').forEach(link => {
            link.addEventListener('click', function (event) {
                toggleVariants(event, this);
            });
        });

        fetchWatershed();

        $('body').on('change', '#nearby_places, #radiusSelect', function () {
            nearbyPlaces();
        });
        $('body').on('click', '#update_nearby', function () {
            nearbyPlaces();
        });

        $(".pop-dataset").popover({
            title: 'Dataset Profile',
            content: function () {
                var placeId = $(this).data('id');
                var place = payload.find(p => p.dataset.id == placeId);
                if (place) {
                    var content = `
		                <p class='thin'><b>Title</b>: ${place.dataset.title.replace('(stub) ', '').substring(0, 25)}</p>
		                <p class='thin'><b>Description</b>: ${place.dataset.description}</p>
		                <p class='thin'><b>WHG Owner</b>: ${place.dataset.owner}</p>
		                <p class='thin'><b>Creator</b>: ${place.dataset.creator}</p>
		            `;
                    if (place.dataset.show_link === false) {
                        content += `<p class='strong-red'>This dataset is too large to be displayed.</p>`;
                    }
                    console.log(place);
                    return content;
                } else {
                    return ''; // Return empty content if place data is not found
                }
            }
        }).on('show.bs.popover', function () { // Close the tooltip on the parent div
            $(this).closest('.source-box').tooltip('hide');
        });

        new ClipboardJS('#permalinkButton', {
            text: function () {
                return window.location.href;
            }
        })
            .on('success', function (e) {
                e.clearSelection();
                const tooltip = bootstrap.Tooltip.getInstance(e.trigger);
                tooltip.setContent({
                    '.tooltip-inner': 'Permalink copied to clipboard successfully!'
                });
                console.log('tooltip', tooltip);
                setTimeout(function () { // Hide the tooltip after 2 seconds
                    tooltip.hide();
                    tooltip.setContent({
                        '.tooltip-inner': tooltip._config.title
                    }) // Restore original text
                }, 2000);
            })
            .on('error', function (e) {
                console.error('Failed to copy:', e.trigger);
            });

        new ClipboardJS('.clip-coordinates', {
            text: function (trigger) {
                return $(trigger).attr('data-coordinates');
            }
        })
            .on('success', function (e) {
                e.clearSelection();
                const tooltip = bootstrap.Tooltip.getInstance(e.trigger);
                tooltip.setContent({
                    '.tooltip-inner': 'Geometry copied to clipboard successfully!'
                });
                console.log('tooltip', tooltip);
                setTimeout(function () { // Hide the tooltip after 2 seconds
                    tooltip.hide();
                    tooltip.setContent({
                        '.tooltip-inner': tooltip._config.title
                    }) // Restore original text
                }, 2000);
            })
            .on('error', function (e) {
                console.error('Failed to copy:', e.trigger);
            });

    })
    .catch(error => console.error("An error occurred:", error))
    .finally(function () {
        $('#dataset_content').stopSpin();
    });

function initCollectionForm() {

    function resetForm() {
        $('#addtocoll_popup').modal('hide');
        $('#title_input').prop('disabled', true).removeClass('is-invalid');
        $('.form-inputs').show();
        $('#submission_alerts').empty();
        var form = $('#collection_form');
        form[0].reset();
        form.removeClass('was-validated');
    }

    $('.modal-footer .btn-secondary, .modal-header .btn-close').click(resetForm);

    $('#collection_form').submit(function (event) {
        event.preventDefault();
        event.stopPropagation();

        var form = $(this);

        if (form[0].checkValidity() === false) {
            form.addClass('was-validated');
        } else if (!$('#title_input').hasClass('is-invalid')) {
            var formData = new FormData(this);
            var selectedCollection = $('input[name="collection"]:checked');
            if (selectedCollection.val() !== "-1") {
                formData.append('title', selectedCollection.next('label').text().trim());
            }
            $.ajax({
                url: '/collections/add_collection_places/',
                method: 'POST',
                headers: {'X-CSRFToken': csrfToken}, // Include CSRF token in headers for Django POST requests
                data: formData,
                processData: false,
                contentType: false,
                success: function (response) {
                    console.log('Form submitted successfully:', response);

                    var submissionAlerts = $('#submission_alerts');
                    submissionAlerts.empty();

                    if (response.status === 'success') {
                        if (!!response.new_collection_id) {
                            var successAlert = $('<div>').addClass('alert alert-success').html(`<p><b>Collection "<em>${response.payload_received.title}</em>" was created successfully.</b></p>`);
                            submissionAlerts.append(successAlert);
                        }
                        var successAlert = $('<div>').addClass('alert alert-success').html(
                            `<p><b>Collection "<em>${response.payload_received.title}</em>" was updated:</b></p>` +
                            `<p>${response.added_places.length} place${response.added_places.length == 1 ? ' was' : 's were'} added.</p>` +
                            `<p>${response.existing_places.length} of the submitted places ${response.existing_places.length == 1 ? 'was' : 'were'} already in the Collection.</p>`
                        );
                        submissionAlerts.append(successAlert);
                        if (response.collection) {
                            updateCollections(response.collection);
                        }
                    } else {
                        var errorAlert = $('<div>').addClass('alert alert-danger').text('Error: ' + response.msg);
                        submissionAlerts.append(errorAlert);
                    }
                    $('.form-inputs').hide();

                    setTimeout(function () {
                        resetForm();
                    }, 5000);
                },
                error: function (xhr, status, error) {
                    console.error('Error submitting form:', error);
                    var submissionAlerts = $('#submission_alerts');
                    submissionAlerts.empty();

                    var errorAlert = $('<div>').addClass('alert alert-danger').text('Error: ' + error);
                    submissionAlerts.append(errorAlert);

                    setTimeout(function () {
                        submissionAlerts.fadeOut();
                    }, 5000);
                }
            });
        }
    });

    $('input[name="collection"]').change(function () {
        $('#title_input')
            .prop('disabled', !($(this).val() == '-1' && $(this).prop('checked')));
    });

    const collectionTitles = $('#my_collections input[type="radio"]').not('[value="-1"]').map(function () {
        return $(this).next('label').text().toLowerCase();
    }).get();
    $('#title_input').on('input', function () {
        $(this).toggleClass('is-invalid', collectionTitles.includes($(this).val().toLowerCase()));
    });

}

function updateCollections(addCollection = false) {

    if (addCollection !== false) {
        const allCollections = $('#collection_list').data('allCollections') || [];
        if (!allCollections.map(collection => collection.id).includes(addCollection.id)) {
            allCollections.push(addCollection);
        }
        $('#collection_list').data('allCollections');
    }

    var uniqueCollections = $('#collection_list').data('allCollections');

    if (uniqueCollections.length > 0) {
        const ul = $('<ul>').addClass('coll-list');
        uniqueCollections.forEach(collection => {
            // console.log('collection', collection);
            let listItem = '';
            // TODO: places are only ever in place collections
            if (collection.class === 'place') {
                listItem = `
					<a href="${collection.url}" target="_blank"><b>${collection.title.trim()} <i class="fas fa-external-link"></i></b>
					</a>, <br/>a collection of <sr>${collection.count}</sr> places${!!collection.owner ? ` by ${collection.owner.name}` : ''}.
					<span class="showing"><p>${collection.description}</p></span>
					[<a href="javascript:void(0);" data-id="${collection.id}" class="show-collection"><span>preview</span><span class="showing">close</span></a>]
				`;
            } else {
                listItem = `
					<a href="${collection.url}" target="_blank"><b>${title}</b>
					</a>, a collection of all <sr>${collection.count}</sr> places in datasets
				`;
            }
            ul.append($('<li>').html(listItem));
        });

        $('#collection_list')
            .find('.coll-list')
            .remove()
            .end()
            .append(ul)
            .prev('span')
            .find('h6')
            .html(`In ${uniqueCollections.length} Collection${uniqueCollections.length > 1 ? 's' : ''} <i class="fas fa-chevron-${$('#collection_list').hasClass('show') ? 'up' : 'down'}"></i>`);

        $('#collectionInfo').html(uniqueCollections.length == 0 ? '<p>It does not yet appear in any WHG collections.</p>' : '');

        $('#source_detail').show();
    } else {
        $('#source_detail').hide();
    }

}

function filterSources(fromValue, toValue, includeUndated) {
    console.log(`Filter dates: ${fromValue} - ${toValue} (includeUndated: ${includeUndated})`);

    function inDateRange(source) {
        const timespans = source.timespans;
        if (timespans.length > 0) {
            return !timespans.every(timespan => {
                return timespan[1] < fromValue || timespan[0] > toValue;
            });
        } else {
            return includeUndated;
        }
    }

    featureCollection.features.forEach((feature, index) => {
        const outOfDateRange = !inDateRange(feature.properties)
        feature.properties['outOfDateRange'] = outOfDateRange;
        $('.source-box').eq(index).toggle(!outOfDateRange);
    });
    whg_map.getSource('places').setData(featureCollection);
    let hiddenCount = $('.source-box:not(:visible)').length;
    $('#filterCount').text(hiddenCount == 0 ? '' : `(${payload.length < 2 ? '' : `${hiddenCount} `}hidden by temporal filter)`);
    noSources.toggle($('.source-box:visible').length == 0);
}

function nearbyPlaces() {
    console.log('nearbyPlaces');

    if ($('#nearby_places').prop('checked')) {
        const center = whg_map.getCenter();
        const radius = $('#radiusSelect').val();
        const lon = center.lng;
        const lat = center.lat;
        $('#update_nearby').show();

        fetch(`/api/spatial/?type=nearby&lon=${lon}&lat=${lat}&km=${radius}`)
            .then((response) => {
                if (!response.ok) {
                    throw new Error('Failed to fetch nearby places.');
                }
                return response.json(); // Parse the response JSON
            })
            .then((data) => {
                data.features.forEach((feature, index) => feature.id = index);
                nearbyFeatureCollection = data; // Set the global variable
                console.log(nearbyFeatureCollection);
                whg_map.getSource('nearbyPlaces').setData(nearbyFeatureCollection);
                $('#update_nearby span').html(`${nearbyFeatureCollection.features.length}`);
                if (!showingRelated && nearbyFeatureCollection.features.length > 0) {
                    whg_map.fitViewport(bbox(nearbyFeatureCollection), defaultZoom);
                }
            })
            .catch((error) => {
                console.error(error);
            });

    } else {
        whg_map.clearSource('nearbyPlaces');
        $('#update_nearby').hide();
    }
}

function fetchWatershed() {
    fetch(`/api/watershed/?lat=${centroid[1]}&lng=${centroid[0]}`)
        .then(response => response.json())
        .then(data => {
            if (data && !!data.type && data.type === 'FeatureCollection') {
                whg_map.addSource('watershed', {
                    type: 'geojson',
                    data: data,
                    attribution: data.attribution
                });
                whg_map.addLayer({
                    id: 'watershed-layer',
                    type: 'fill',
                    source: 'watershed',
                    paint: {
                        'fill-color': 'blue',
                        'fill-opacity': 0.3
                    },
                    layout: {
                        visibility: 'none'
                    }
                });
                whg_map.addSource('watershed-origin-marker', {
                    type: 'geojson',
                    data: {
                        type: 'FeatureCollection',
                        features: [{
                            type: 'Feature',
                            geometry: {
                                type: 'Point',
                                coordinates: centroid
                            },
                            properties: {
                                marker: 'watershed-origin'
                            }
                        }]
                    }
                });
                whg_map.addLayer({
                    id: 'watershed-origin-marker',
                    type: 'circle',
                    source: 'watershed-origin-marker',
                    paint: {
                        'circle-radius': 10,
                        'circle-color': 'blue',
                        'circle-opacity': 0.5
                    },
                    layout: {
                        visibility: 'none'
                    }
                });
                $(whg_map.styleControl._listContainer).find('#layerSwitches').before(
                    '<li class="group-item strong-red">Watershed<input type="checkbox" id="watershedCheckbox"></li>'
                );
                $('#watershedCheckbox').change(function () {
                    const isVisible = this.checked;
                    if (isVisible) {
                        whg_map.setLayoutProperty('watershed-layer', 'visibility', 'visible');
                        whg_map.setLayoutProperty('watershed-origin-marker', 'visibility', 'visible');
                    } else {
                        whg_map.setLayoutProperty('watershed-layer', 'visibility', 'none');
                        whg_map.setLayoutProperty('watershed-origin-marker', 'visibility', 'none');
                    }
                });

            }
        })
        .catch(error => {
            console.error('Error fetching watershed data:', error);
        });
}