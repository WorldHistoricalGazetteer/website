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
 * @param {number} timeout - Maximum time to wait in milliseconds (default: 90000 = 90 seconds)
 * @returns {Promise} - Resolves when GPU has finished rendering or timeout occurs
 */
function waitForGPUCompletion(map, timeout = 90000) {
    return new Promise((resolve) => {
        const startTime = Date.now();
        let idleTimeout;
        let fenceCheckCount = 0;
        const maxFenceChecks = 10000; // Prevent infinite loops (allow up to 10k checks for very long renders)

        // Safety timeout - always resolve after timeout period
        const safetyTimeout = setTimeout(() => {
            console.warn(`[GPU Sync] Safety timeout after ${Date.now() - startTime}ms`);
            cleanup();
            resolve();
        }, timeout);

        function cleanup() {
            clearTimeout(safetyTimeout);
            clearTimeout(idleTimeout);
        }

        // First wait for the map to be idle (no more tiles loading, no transitions)
        const onIdle = () => {
            map.off('idle', onIdle);

            // Now check if GPU has finished rendering using fence sync
            try {
                const gl = map.painter.context.gl;

                if (!gl || !gl.fenceSync) {
                    console.warn('[GPU Sync] WebGL fence sync not available - using fallback');
                    useFallback();
                    return;
                }

                // Create a sync object - this acts as a "fence" in the GPU command queue
                const sync = gl.fenceSync(gl.SYNC_GPU_COMMANDS_COMPLETE, 0);

                if (!sync) {
                    console.warn('[GPU Sync] Failed to create fence sync - using fallback');
                    useFallback();
                    return;
                }

                // Flush to ensure the fence command is sent to the GPU
                gl.flush();

                // Poll for fence completion
                function checkFence() {
                    fenceCheckCount++;

                    // Safety check to prevent infinite loops
                    if (fenceCheckCount > maxFenceChecks) {
                        console.warn(`[GPU Sync] Exceeded max fence checks (${maxFenceChecks})`);
                        try { gl.deleteSync(sync); } catch(e) {}
                        cleanup();
                        resolve();
                        return;
                    }

                    // Check if we've exceeded timeout
                    if (Date.now() - startTime > timeout) {
                        console.warn('[GPU Sync] Fence check timeout');
                        try { gl.deleteSync(sync); } catch(e) {}
                        cleanup();
                        resolve();
                        return;
                    }

                    try {
                        const status = gl.clientWaitSync(sync, 0, 0);

                        if (status === gl.ALREADY_SIGNALED || status === gl.CONDITION_SATISFIED) {
                            // GPU has finished all rendering commands
                            const totalTime = Date.now() - startTime;
                            console.log(`[GPU Sync] GPU rendering complete (${totalTime}ms, ${fenceCheckCount} checks)`);
                            gl.deleteSync(sync);
                            cleanup();
                            resolve();
                        } else if (status === gl.WAIT_FAILED) {
                            console.warn('[GPU Sync] Fence wait failed - using fallback');
                            gl.deleteSync(sync);
                            useFallback();
                        } else {
                            // GPU still working, check again on next tick
                            setTimeout(checkFence, 0);
                        }
                    } catch (error) {
                        console.warn('[GPU Sync] Error checking fence:', error);
                        try { gl.deleteSync(sync); } catch(e) {}
                        useFallback();
                    }
                }

                checkFence();

            } catch (error) {
                console.warn('Error in GPU fence sync:', error);
                useFallback();
            }
        };

        // Fallback method using map state polling
        function useFallback() {
            cleanup();

            let checkCount = 0;
            let consecutiveIdleChecks = 0; // Track how many times we've seen idle+no-tiles
            const maxChecks = 600; // Max 600 checks = ~60 seconds at 100ms intervals
            const checkInterval = 100; // Check every 100ms

            function checkMapState() {
                checkCount++;

                // Check if we've exceeded max checks or timeout
                if (checkCount >= maxChecks || Date.now() - startTime > timeout) {
                    const totalTime = Date.now() - startTime;
                    console.log(`[GPU Sync] Fallback timeout (${totalTime}ms)`);
                    resolve();
                    return;
                }

                // Check if map is truly idle and not rendering
                const isMapIdle = !map.isMoving() && !map.isRotating() && !map.isZooming();
                const isLoaded = map.loaded();
                const hasStyle = map.isStyleLoaded();

                // Also check if there are pending tiles
                let hasPendingTiles = false;
                try {
                    const style = map.getStyle();
                    if (style && style.sources) {
                        // Check if any source is still loading
                        const sources = Object.keys(style.sources);
                        for (const sourceId of sources) {
                            const source = map.getSource(sourceId);
                            if (source && source.loaded && !source.loaded()) {
                                hasPendingTiles = true;
                                break;
                            }
                        }
                    }
                } catch (e) {
                    // If we can't check sources, assume they're loaded
                }

                // Ideal condition: everything reports ready
                if (isMapIdle && isLoaded && hasStyle && !hasPendingTiles) {
                    // Map appears to be fully loaded and idle
                    requestAnimationFrame(() => {
                        const totalTime = Date.now() - startTime;
                        console.log(`[GPU Sync] Rendering complete (${totalTime}ms)`);
                        resolve();
                    });
                    return;
                }

                // Relaxed condition: map is idle with no pending tiles
                // (loaded and style may incorrectly report false with globe projection + terrain)
                if (isMapIdle && !hasPendingTiles) {
                    consecutiveIdleChecks++;

                    // If we've seen idle+no-tiles for 30 consecutive checks (3 seconds), that's good enough
                    if (consecutiveIdleChecks >= 30) {
                        requestAnimationFrame(() => {
                            const totalTime = Date.now() - startTime;
                            console.log(`[GPU Sync] Rendering complete (${totalTime}ms, relaxed mode)`);
                            if (!isLoaded || !hasStyle) {
                                console.log(`[GPU Sync] Note: map.loaded=${isLoaded}, isStyleLoaded=${hasStyle}`);
                            }
                            resolve();
                        });
                        return;
                    }
                } else {
                    // Reset counter if conditions not met
                    consecutiveIdleChecks = 0;
                }

                // Continue checking
                setTimeout(checkMapState, checkInterval);
            }

            // Start checking after a brief delay
            setTimeout(checkMapState, checkInterval);
        }

        // Additional safety: also listen for render events as backup
        let renderCount = 0;
        const onRender = () => {
            renderCount++;
            // After a few render events, if we haven't resolved yet, use fallback
            if (renderCount >= 3) {
                map.off('render', onRender);
                if (Date.now() - startTime > timeout / 2) {
                    console.warn('[GPU Sync] Multiple render events without idle - using fallback');
                    useFallback();
                }
            }
        };
        map.on('render', onRender);

        // Cleanup render listener when we resolve
        const originalResolve = resolve;
        resolve = () => {
            map.off('render', onRender);
            originalResolve();
        };

        // Start the idle detection with its own timeout
        idleTimeout = setTimeout(() => {
            console.warn('[GPU Sync] Idle event timeout - using fallback');
            // Check if map has any sources - if not, it might be empty
            const sources = map.getStyle()?.sources || {};
            const hasAnySources = Object.keys(sources).length > 0;

            if (!hasAnySources) {
                console.warn('[GPU Sync] Map has no sources - completing immediately');
                cleanup();
                resolve();
            } else {
                useFallback();
            }
        }, timeout - 1000); // Slightly less than main timeout

        // Register idle listener
        const alreadyIdle = !map.isMoving() && !map.isRotating() && !map.isZooming();

        if (alreadyIdle) {
            // Map claims to be idle, but idle event may never fire with globe projection + terrain
            // Set a shorter timeout for this case - if idle doesn't fire within 5s, use fallback
            const earlyFallbackTimeout = setTimeout(() => {
                console.warn('[GPU Sync] Map idle but event not firing - using fallback');
                map.off('idle', onIdle); // Remove the listener that will never fire
                useFallback();
            }, 5000); // 5 seconds - if idle event hasn't fired by then, it probably won't

            // Wrap onIdle to clear the early timeout if idle does fire
            const originalOnIdle = onIdle;
            const wrappedOnIdle = () => {
                clearTimeout(earlyFallbackTimeout);
                originalOnIdle();
            };

            // Register immediately - no need to wait for rAF
            map.once('idle', wrappedOnIdle);
        } else {
            // Map is actively moving/rotating/zooming - wait for idle normally
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
        $.ajax({
            url: mapdata_url,
            type: 'GET',
            dataType: 'json',
            success: function (data, textStatus, jqXHR) {
                window.datacollection = data;

                // Calculate approximate size of the mapdata
                // This is a better indicator of rendering complexity than feature count
                const dataString = JSON.stringify(data);
                const dataSizeBytes = new Blob([dataString]).size;
                const dataSizeMB = (dataSizeBytes / (1024 * 1024)).toFixed(2);

                // Store size for timeout calculation
                window.datacollection.metadata.mapdata_size_bytes = dataSizeBytes;
                window.datacollection.metadata.mapdata_size_mb = parseFloat(dataSizeMB);

                console.debug(`Dataset "${data.metadata.title}" loaded. Size: ${dataSizeMB} MB`, data);

                const numPlaces = data.metadata.num_places;
                const isLargeDataset = dataSizeBytes > 5 * 1024 * 1024; // > 5MB

                if (isLargeDataset) {
                    $('#dataset_content').spin({
                        label: `Loading ${numPlaces.toLocaleString('en-US')} places (${dataSizeMB} MB)... This is a large dataset. Rendering may take a moment.`
                    });
                } else {
                    $('#dataset_content').spin({
                        label: `Loading ${numPlaces.toLocaleString('en-US')} places...`
                    });
                }

                loadMapParameters();
                resolve();
            },
            error: reject
        });
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

    // Get mapdata size for timeout calculation (better proxy for rendering complexity than feature count)
    const dataSizeMB = window.datacollection.metadata.mapdata_size_mb || 0;
    const dataSizeBytes = window.datacollection.metadata.mapdata_size_bytes || 0;
    const numFeatures = window.datacollection.metadata.num_places;
    const isLargeDataset = dataSizeMB > 5; // > 5MB

    // Update spinner message before waiting for GPU
    if (isLargeDataset) {
        $('#dataset_content').spin({
            label: `Finalising rendering (${dataSizeMB} MB)... Almost there!`
        });
    }

    // Wait for GPU to finish rendering before removing spinner
    // Use try-finally to ensure spinner is always removed
    try {
        // Set timeout based on mapdata size (better indicator of rendering complexity)
        // Polygon datasets with many coordinates take much longer than point datasets
        // Small (<2MB): 30s, Medium (2-10MB): 60s, Large (10-30MB): 90s, Very Large (>30MB): 120s
        let timeoutMs;
        if (dataSizeMB < 2) {
            timeoutMs = 30000; // 30 seconds - simple datasets
        } else if (dataSizeMB < 10) {
            timeoutMs = 60000; // 1 minute - moderate complexity
        } else if (dataSizeMB < 30) {
            timeoutMs = 90000; // 1.5 minutes - complex polygon datasets
        } else {
            timeoutMs = 120000; // 2 minutes - very complex datasets
        }

        await waitForGPUCompletion(whg_map, timeoutMs);
    } catch (error) {
        console.error('Error during GPU completion wait:', error);
    } finally {
        // Always remove spinner, even if there was an error
        $('#dataset_content').stopSpin();
    }

}

// Initialise the whole map/data interface
initialiseMapInterface();

export {whg_map};
