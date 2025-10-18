// /whg/webpack/js/mapAndTable.js

import '../css/mapAndTable.css';
import '../css/mapAndTableAdditional.css';

import {init_mapControls} from './mapControls';
import {initDownloadLinks, initObservers, initOverlays, initPopups, recenterMap} from './mapFunctions';
import {toggleFilters} from './mapFilters';
import {arrayColors, colorTable, deepCopy, initInfoOverlay, initUtils} from './utilities';
import {initialiseTable} from './tableFunctions';
import {init_collection_listeners} from './collections';
import SequenceArcs from './mapSequenceArcs';
import './toggle-truncate.js';
import './enlarge.js';

window.mapBounds;
window.highlightedFeatureIndex;
window.additionalLayers = []; // Keep track of added map sources and layers - required for baselayer switching

window.dateline = null;
let datelineContainer = null;
let mapSequencer = null;
let sequenceArcs;

let table;
let checked_rows;
let mapParameters;
let whg_map;

// Utility to load dataset
async function loadDataset() {
    return new Promise((resolve, reject) => {
        $('#dataset_content').spin({
            label: `Fetching data...`
        });
        $.get(mapdata_url, function (data) {
            window.datacollection = data;
            console.debug(`Dataset "${data.metadata.title}" loaded.`, data);

            $('#dataset_content').spin({
                label: `Loading ${data.metadata.num_places.toLocaleString('en-US')} places...`
            });

            loadMapParameters();
            resolve();
        }).fail(reject);
    });
}

// Update mapParameters based on dataset
function loadMapParameters() {
    const meta = window.datacollection.metadata;

    mapParameters = {
        maxZoom: 20,
        downloadMapControl: true,
        fullscreenControl: true,
        globeControl: true,
        globeMode: meta.globeMode,
        style: [
            'WHG',
            'Satellite'
        ],
    }

    if (
        meta.ds_type === 'collections' ||
        (meta.visParameters.max.tabulate !== false && meta.visParameters.min.tabulate !== false)
    ) {
        mapParameters = {
            ...mapParameters,
            temporalControl: {
                fromValue: 1550,
                toValue: 1720,
                minValue: -2000,
                maxValue: 2100,
                open: meta.ds_type !== 'collections',
                includeUndated: true,
                epochs: null,
                automate: null,
            },
            ...(meta.ds_type === 'collections' && {
                sequencerControl: true,
                controls: {sequencer: true},
            }),
        };
    }
}

// Load map once parameters are ready
async function loadMap() {
    return new Promise((resolve) => {
        whg_map = new whg_maplibre.Map(mapParameters);
        whg_map.on('load', () => {
            console.log('Map loaded.');
            resolve();
        });
    });
}

// Load datatables CDN fallbacks
async function loadCDNResources() {
    return Promise.all(datatables_CDN_fallbacks.map(loadResource));
}

// Main orchestrator
async function initialiseMapInterface() {
    try {
        await loadDataset();
        await loadCDNResources();
        await loadMap();
        completeLoading();
    } catch (error) {
        console.error('Error during initialisation:', error);
    }
}

function completeLoading() {

    initOverlays(whg_map.getContainer());
    initDownloadLinks();

    $('.thumbnail').enlarge();

    let circleColors;
    if (!!window.datacollection.metadata.relations) {
        circleColors = arrayColors(window.datacollection.metadata.relations);
        colorTable(circleColors, '.maplibregl-control-container');
    }
    if (window.datacollection.metadata.datasets?.length > 0) {
        circleColors = arrayColors(window.datacollection.metadata.datasets.map(d => d.id.toString()));
        colorTable(circleColors, '.maplibregl-control-container', window.datacollection.metadata.datasets.map(d => d.title), window.datacollection.metadata.multi_relations, window.datacollection.metadata.ds_id, whg_map);
    }

    let marker_reducer = !!window.datacollection.metadata.coordinate_density ? (window.datacollection.metadata.coordinate_density < 50 ? 1 : 50 / window.datacollection.metadata.coordinate_density) : 1
    whg_map
        .newSource(window.datacollection)
        .newLayerset(window.datacollection.metadata.ds_id, window.datacollection, null, null, null, null, marker_reducer, circleColors); // Add standard layerset (defined in `layerset.js` and prototyped in `whg_maplibre.js`)

    // Initialise Data Table
    const tableInit = initialiseTable(window.datacollection.table.features, checked_rows, whg_map);
    table = tableInit.table;
    checked_rows = tableInit.checked_rows;

    // Initialise Map Controls
    const mapControlsInit = init_mapControls(whg_map, datelineContainer, toggleFilters, mapParameters, table);
    datelineContainer = mapControlsInit.datelineContainer;
    mapSequencer = mapControlsInit.mapSequencer;
    mapParameters = mapControlsInit.mapParameters;

    window.mapBounds = window.datacollection.metadata.extent || [-180, -90, 180, 90];
    recenterMap(false, true);

    // Initialise Map Popups
    initPopups(table);

    // Initialise resize observers
    initObservers();

    // Initialise Info Box state
    initInfoOverlay();

    if (window.datacollection.metadata.ds_type === 'collections') {
        let tableOrder = null;

        function updateVisualisation(newTableOrder) {
            tableOrder = deepCopy(newTableOrder);

            if (sequenceArcs) sequenceArcs = sequenceArcs.destroy();
            const facet = table.settings()[0].aoColumns[tableOrder[0]].mData.split('.')[1];
            const order = tableOrder[1];

            if (!!window.datacollection.metadata.visParameters[facet]) {
                toggleFilters(window.datacollection.metadata.visParameters[facet]['temporal_control'] === 'filter', whg_map, table);
                dateline.toggle(window.datacollection.metadata.visParameters[facet]['temporal_control'] === 'filter');
                mapSequencer.toggle(window.datacollection.metadata.visParameters[facet]['temporal_control'] === 'player');
                const featureCollection = {type: 'FeatureCollection', features: window.datacollection.table.features}
                if (window.datacollection.metadata.visParameters[facet]['trail']) {
                    sequenceArcs = new SequenceArcs(whg_map, featureCollection, {facet: facet, order: order});
                }
            } else {
                toggleFilters(false, whg_map, table);
                dateline.toggle(false);
                mapSequencer.toggle(false);
            }
        }

        updateVisualisation(table.order()[0]); // Initialise
        $('#placetable').on('order.dt', function () { // Also fired by table.draw(), so need to track the order
            const newTableOrder = deepCopy(table.order()[0]);
            if (newTableOrder[0] !== tableOrder[0] || newTableOrder[1] !== tableOrder[1]) {
                updateVisualisation(newTableOrder);
            }
        });
    }

    initUtils(whg_map); // Tooltips, ClipboardJS, clearlines, help-matches

    init_collection_listeners(checked_rows);

    $('#dataset_content').stopSpin();

}

// Initialise the whole map/data interface
initialiseMapInterface();

export {whg_map};
