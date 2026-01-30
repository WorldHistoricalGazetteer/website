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

/**
 * Wait for GPU to finish rendering using WebGL fence synchronization
 * This is more reliable than 'idle' or 'render' events for complex datasets
 * @param {Map} map - MapLibre map instance
 * @returns {Promise} - Resolves when GPU has finished rendering
 */
function waitForGPUCompletion(map) {
    return new Promise((resolve) => {
        // First wait for the map to be idle (no more tiles loading, no transitions)
        const onIdle = () => {
            map.off('idle', onIdle);

            // Now check if GPU has finished rendering using fence sync
            try {
                const gl = map.painter.context.gl;

                // Create a sync object - this acts as a "fence" in the GPU command queue
                const sync = gl.fenceSync(gl.SYNC_GPU_COMMANDS_COMPLETE, 0);

                // Flush to ensure the fence command is sent to the GPU
                gl.flush();

                // Poll for fence completion
                function checkFence() {
                    const status = gl.clientWaitSync(sync, 0, 0);

                    if (status === gl.ALREADY_SIGNALED || status === gl.CONDITION_SATISFIED) {
                        // GPU has finished all rendering commands
                        gl.deleteSync(sync);
                        console.log('GPU rendering complete - all pixels drawn');
                        resolve();
                    } else {
                        // GPU still working, check again on next tick
                        setTimeout(checkFence, 0);
                    }
                }

                checkFence();

            } catch (error) {
                // Fallback if WebGL sync is not available (shouldn't happen in modern browsers)
                console.warn('WebGL fence sync not available, using fallback:', error);
                // Wait an additional frame to be safe
                requestAnimationFrame(() => {
                    requestAnimationFrame(resolve);
                });
            }
        };

        // If map is already idle, trigger immediately
        if (!map.isMoving() && !map.isRotating() && !map.isZooming()) {
            // Wait one frame to ensure current render is complete
            requestAnimationFrame(() => {
                map.once('idle', onIdle);
            });
        } else {
            map.once('idle', onIdle);
        }
    });
}

// Utility to load dataset
async function loadDataset() {
    return new Promise((resolve, reject) => {
        $('#dataset_content').spin({
            label: `Fetching data...`
        });
        $.get(mapdata_url, function (data) {
            window.datacollection = data;
            console.debug(`Dataset "${data.metadata.title}" loaded.`, data);

            const numPlaces = data.metadata.num_places;
            const isLargeDataset = numPlaces > 5000;

            if (isLargeDataset) {
                $('#dataset_content').spin({
                    label: `Loading ${numPlaces.toLocaleString('en-US')} places...<br><small>This is a large dataset. Rendering may take a moment.</small>`
                });
            } else {
                $('#dataset_content').spin({
                    label: `Loading ${numPlaces.toLocaleString('en-US')} places...`
                });
            }

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
                // Don't set fromValue/toValue here - let mapControls.js initialize based on has_multitemporal_geometries flag
                minValue: meta.min || -2000,
                maxValue: meta.max || 2100,
                open: meta.ds_type !== 'collections',
                includeUndated: !meta.has_multitemporal_geometries,  // Uncheck "Undated" only for multitemporal datasets
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
        const isLargeDataset = window.datacollection.metadata.num_places > 5000;

        if (isLargeDataset) {
            $('#dataset_content').spin({
                label: `Rendering map with ${window.datacollection.metadata.num_places.toLocaleString('en-US')} places...Please wait while the map is being prepared.`
            });
        }

        whg_map = new whg_maplibre.Map(mapParameters);
        whg_map.on('load', () => {
            console.log('Map loaded.');

            if (isLargeDataset) {
                $('#dataset_content').spin({
                    label: `Processing geometries...Almost ready!`
                });
            }

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
        await completeLoading();
    } catch (error) {
        console.error('Error during initialisation:', error);
        $('#dataset_content').stopSpin();
    }
}

async function completeLoading() {

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

    // Calculate initial temporal filter for mid-year only if dataset has multitemporal GeometryCollections
    let initialTemporalFilter = null;
    if (mapParameters.temporalControl && window.datacollection.metadata.has_multitemporal_geometries) {
        const meta = window.datacollection.metadata;
        const midYear = Math.floor((meta.min + meta.max) / 2);

        // Create the same filter that will be used by the temporal widget
        initialTemporalFilter = [
            'all',
            ['!=', 'max', 'null'],
            ['!=', 'min', 'null'],
            ['>=', 'max', midYear],
            ['<=', 'min', midYear],
        ];

        console.log(`Dataset has multitemporal geometries. Setting initial temporal filter to mid-year: ${midYear}`);
    } else if (mapParameters.temporalControl) {
        console.log(`Dataset has monotemporal geometries. Initial filter will show full temporal range.`);
    }

    // Create layerset with initial temporal filter
    whg_map
        .newSource(window.datacollection)
        .newLayerset(window.datacollection.metadata.ds_id, window.datacollection, null, null, null, null, marker_reducer, circleColors, initialTemporalFilter);

    // Initialise Data Table
    const tableInit = initialiseTable(window.datacollection.table.features, checked_rows, whg_map);
    table = tableInit.table;
    checked_rows = tableInit.checked_rows;

    // Initialise Map Controls
    const mapControlsInit = init_mapControls(whg_map, datelineContainer, toggleFilters, mapParameters, table);
    datelineContainer = mapControlsInit.datelineContainer;
    mapSequencer = mapControlsInit.mapSequencer;
    mapParameters = mapControlsInit.mapParameters;

    // Apply initial temporal filtering if temporal control exists
    if (mapParameters.temporalControl) {
        // For datasets, enable filtering by default
        // For collections, filtering will be controlled by updateVisualisation
        const isDataset = window.datacollection.metadata.ds_type !== 'collections';
        if (isDataset) {
            toggleFilters(true, whg_map, table);
        }
    }

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

    // Update spinner message before waiting for GPU
    const isLargeDataset = window.datacollection.metadata.num_places > 5000;
    if (isLargeDataset) {
        $('#dataset_content').spin({
            label: `Finalizing rendering...Almost there!`
        });
    }

    // Wait for GPU to finish rendering before removing spinner
    await waitForGPUCompletion(whg_map);

    console.log('Map interface fully loaded and rendered');
    $('#dataset_content').stopSpin();

}

// Initialise the whole map/data interface
initialiseMapInterface();

export {whg_map};
