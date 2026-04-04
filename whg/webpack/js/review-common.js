import { base_urls } from './aliases.js';

// ============================================================================
// MAP INITIALIZATION
// ============================================================================

let whg_map = null;
const mapElement = document.getElementById('map');

if (mapElement) {
    whg_map = new whg_maplibre.Map({
        container: 'map',
        maxZoom: 14,
        style: ['WHG', 'Satellite'],
        scaleControl: true,
    });
} else {
    console.warn("Map container element with id 'map' not found.");
}

export { whg_map };

let featureCollection;
export let layersets = {};

// ============================================================================
// MAP CONFIGURATION
// ============================================================================

const MAP_CONFIG = {
    reconciliation: {
        markerColours: {
            'dataset': 'green',
            'wikidata': 'orange',
            'geonames': 'blue',
        },
        shouldNumber: (ds) => ds !== 'dataset',
        shouldHideGeonames: (groupedFeatures) =>
            !!groupedFeatures['wikidata']
    },
    accession: {
        markerColours: {
            'dataset': 'green',
        },
        shouldNumber: () => false,
        shouldHideGeonames: () => false
    }
};

// ============================================================================
// FEATURE MATCHING UTILITIES
// ============================================================================

function getFeatureIdentifier(feature) {
    const props = feature.properties;
    return props.record_id || props.pid || props.hit_id;
}

function getElementIdentifier(element) {
    return $(element).data('id') || $(element).attr('id');
}

function findMatchingFeature(element) {
    const elementId = String(getElementIdentifier(element));
    return featureCollection.features.find(feature => {
        const featureId = String(getFeatureIdentifier(feature));
        return featureId === elementId;
    });
}

function findMatchingElement(feature) {
    const featureId = getFeatureIdentifier(feature);
    if (!featureId) return null;
    return $(`.hovermap[data-id="${featureId}"]`);
}

// ============================================================================
// MAP INITIALIZATION
// ============================================================================

export function initialiseMap() {
    console.log('Map loaded.');

    featureCollection = JSON.parse(featureCollectionJSON);
    console.log(featureCollection);

    const groupedFeatures = groupFeaturesByDataset();
    const config = MAP_CONFIG[page_variant] || MAP_CONFIG.accession;

    createLayersets(groupedFeatures, config);

    console.log(groupedFeatures, layersets);

    fitMapToFeatures();
    whg_map.getContainer().style.opacity = 1;
}

function groupFeaturesByDataset() {
    const grouped = {};
    featureCollection.features.forEach(feature => {
        const ds = feature.properties.dslabel;
        if (!grouped[ds]) grouped[ds] = [];
        grouped[ds].push(feature);
    });
    return grouped;
}

function createLayersets(groupedFeatures, config) {
    Object.entries(groupedFeatures).forEach(([ds, features]) => {
        const colour = config.markerColours[ds] || 'orange';
        const strokeColour = ds === 'dataset' ? 'green' : null;
        const shouldNumber = config.shouldNumber(ds);
        const scale = ds === 'dataset' ? 1.3 : 1;

        layersets[ds] = whg_map
            .newSource(ds, { type: 'FeatureCollection', features })
            .newLayerset(ds, null, 'plain', colour, strokeColour, shouldNumber, scale);

        if (ds === 'geonames' && config.shouldHideGeonames(groupedFeatures)) {
            layersets[ds].toggleVisibility(false);
        }
    });
}

function fitMapToFeatures() {
    if (featureCollection.features.length > 0) {
        whg_map.fitViewport(bbox(featureCollection), defaultZoom);
    } else {
        console.log('No features to map.');
    }
}

// ============================================================================
// MAP INTERACTION HANDLERS
// ============================================================================

function setupMapClickHandler() {
    whg_map.on('click', function(e) {
        clearAllHighlights();
        const features = whg_map.queryRenderedFeatures(e.point);

        features.forEach(feature => {
            if (isUserAddedFeature(feature)) {
                const $element = findMatchingElement(feature);
                if ($element && $element.length) {
                    highlightElement($element);
                }
            }
        });
    });
}

function setupMapHoverHandler() {
    whg_map.on('mousemove', function(e) {
        const features = whg_map.queryRenderedFeatures(e.point);

        if (features.length > 0) {
            const topFeature = features[0];

            if (isUserAddedFeature(topFeature)) {
                const $element = findMatchingElement(topFeature);
                if ($element && $element.length) {
                    whg_map.getCanvas().style.cursor = 'pointer';
                    highlightElement($element);
                    return;
                }
            }
        }

        clearMapInteraction();
    });
}

function isUserAddedFeature(feature) {
    return feature.properties &&
           !whg_map.baseStyle.layers.includes(feature.layer.id);
}

function highlightElement($element) {
    clearAllHighlights();
    $element.addClass('highlight-row')
            .closest('.review-item')
            .scrollintoview();
}

function clearMapInteraction() {
    whg_map.getCanvas().style.cursor = 'grab';
    clearAllHighlights();
}

function clearAllHighlights() {
    $('.highlight-row').removeClass('highlight-row');
}

// ============================================================================
// FEATURE STATE MANAGEMENT
// ============================================================================

function toggleHighlight(highlight, element) {
    const matchingFeature = findMatchingFeature(element);
    if (!matchingFeature) return;

    const datasetSource = matchingFeature.properties.dslabel;

    if (datasetSource && whg_map.getSource(datasetSource)) {
        whg_map.setFeatureState(
            { source: datasetSource, id: matchingFeature.id },
            { highlight }
        );
    }
}

// ============================================================================
// DOM EVENT HANDLERS
// ============================================================================

function setupGeolinkHandler() {
    $(".geolink")
        .attr('title', 'Click to zoom to this location.')
        .on('click', function() {
            const matchingFeature = findMatchingFeature(this);
            if (matchingFeature) {
                whg_map.fitViewport(bbox(matchingFeature), defaultZoom);
            }
        });
}

function setupHovermapHandler() {
    $(".hovermap").hover(
        function() { toggleHighlight(true, this); },
        function() { toggleHighlight(false, this); }
    );
}

function setupExternalLinkHandlers() {
    $('.ext').on('click', function(e) {
        e.preventDefault();
        const str = $(this).text().trim();
        const re = /(http|bnf|cerl|dbp|gn|gnd|gov|indias|loc|pl|tgn|viaf|wd|wdlocal|whg|wp):(.*?)$/;
        const match = str.match(re);
        const url = match[1] === 'http' ? str : base_urls[match[1]] + match[2];
        console.log('str, url', str, url);
        window.open(url, '_blank');
    });

    $('.ext-recon').on('click', function(e) {
        e.preventDefault();
        const id = $.trim($(this).text());
        const url = base_urls[$(this).data('auth')] + id.toString();
        console.log('id, url', id, url);
        window.open(url, '_blank');
    });
}

function setupSaveHandler() {
    $("#btn_save").click(function() {
        const current_place = $('input[name=place_id]').val();
        console.log('current place:', current_place);
        sessionStorage.setItem('reviewBegun', true);
        sessionStorage.setItem('lastPlace', current_place);
    });
}

function setupUndoHandler() {
    const lastPlace = sessionStorage.lastPlace;
    const currentPlace = $('input[name=place_id]').val();

    console.log('lastPlace:', lastPlace);
    console.log('current place:', currentPlace);

    if (lastPlace && lastPlace !== currentPlace) {
        $("#undo").removeClass('hidden-imp');
    }

    $("#undo").click(function(e) {
        e.preventDefault();
        const url = $(this).data('url').replace('999', sessionStorage.lastPlace);
        console.log('undo url:', url);
        document.location.href = url;
    });
}

function setupPassDropdownHandler() {
    $("#select_pass").change(function() {
        const z = window.location.href;
        const baseurl = z.substring(0, z.lastIndexOf('/') + 1);
        window.location.href = baseurl + $(this).val();
    });
}

function setupNoteHandlers() {
    $('.noteicon').on('click', function() {
        $(this).parents(".matchbar").find(".notefield").toggle();
    });

    $('.noteicon').hover(function() {
        console.log('hovering');
    });

    $('.notes').notes();
}

// ============================================================================
// PUBLIC API
// ============================================================================

export function addReviewListeners() {
    setupUndoHandler();

    // Update dynamic pass number display
    const z = window.location.href;
    $('#passnum_dynamic').html('<b>' + z.slice(-6) + '</b>');

    // Map interaction
    setupMapClickHandler();
    setupMapHoverHandler();

    // DOM interactions
    setupGeolinkHandler();
    setupHovermapHandler();
    setupExternalLinkHandlers();
    setupSaveHandler();
    setupPassDropdownHandler();
    setupNoteHandlers();

    // Clean up default textarea content
    $('.textarea').html('');
}