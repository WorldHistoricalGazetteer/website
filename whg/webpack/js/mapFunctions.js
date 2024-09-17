import {scrollToRowByProperty} from './tableFunctions-extended';
import {clearFilters} from './tableFunctions';

import {popupFeatureHTML} from './getPlace.js';
import {mappy} from './mapAndTable';

let mapParams;

export function updatePadding() {
    const ControlsRect = mapParams.ControlsRectEl.getBoundingClientRect();
    const MapRect = mapParams.MapRectEl.getBoundingClientRect();

    // Function to validate and log padding values
    function getValidPadding(value, name) {
        // Ensure value is a number and is non-negative
        const validValue = (typeof value === 'number' && !isNaN(value) && value >= 0) ? value : 0;
        if (validValue !== value) {
            console.warn(`Invalid value for ${name}: ${value}. Defaulting to 0.`);
        }
        return validValue;
    }

    if ($('#mapOverlays').length > 0) {
        const top = getValidPadding(ControlsRect.top - MapRect.top - mapParams.ControlsRectMargin);
        const bottom = getValidPadding(MapRect.bottom - ControlsRect.bottom - mapParams.ControlsRectMargin);
        const left = getValidPadding(ControlsRect.left - MapRect.left - mapParams.ControlsRectMargin);
        const right = getValidPadding(MapRect.right - ControlsRect.right - mapParams.ControlsRectMargin);

        mappy.setPadding({
            top,
            bottom,
            left,
            right,
        });
    } else {
        mappy.setPadding({
            top: 0,
            bottom: 0,
            left: 0,
            right: 0,
        });
    }
}

function updateBounds() {
    const ControlsRect = mapParams.ControlsRectEl.getBoundingClientRect();
    const MapRect = mapParams.MapRectEl.getBoundingClientRect();
    const centerX = -mapParams.MapRectBorder + ControlsRect.left - MapRect.left +
        ControlsRect.width / 2;
    const centerY = -mapParams.MapRectBorder + ControlsRect.top - MapRect.top +
        ControlsRect.height / 2;
    const pseudoCenter = mapParams.mappy.unproject([centerX, centerY]);
    window.mapBounds = {
        'center': pseudoCenter,
        'zoom': mapParams.mappy.getZoom()
    };
    //console.log('window.mapBounds updated:', window.mapBounds);
}

// Control positioning of map, clear of overlays
export function recenterMap(duration) {
    duration = duration == 'lazy' ? 1000 : 0; // Set duration of movement
    window.blockBoundsUpdate = true;
    // mappy.showPadding = true; // Used for debugging - draws coloured lines to indicate padding
    if (window.mapBounds) {

        // If mapbounds extent is actually a point, convert it
        if (
            Array.isArray(window.mapBounds) &&
            window.mapBounds.length === 4 &&
            window.mapBounds[0] === window.mapBounds[2] &&
            window.mapBounds[1] === window.mapBounds[3]
        ) {
            window.mapBounds = {center: window.mapBounds.slice(0, 2)};
        }

        if (Array.isArray(window.mapBounds) ||
            !window.mapBounds.hasOwnProperty('center')) { // mapBounds might be a coordinate pair object returned by mappy.getBounds();
            mappy.fitBounds(window.mapBounds, {
                duration: duration,
            });
        } else { // mapBounds has been set based on a center point and zoom
            mappy.flyTo({
                ...window.mapBounds,
                duration: duration,
            });
        }
    }
    //console.log('Map recentred', window.mapBounds);
}

export function initObservers() {

    mapParams = {
        mappy: mappy,
        ControlsRectEl: $('.maplibregl-control-container').first()[0],
        MapRectEl: document.querySelector('div.maplibregl-map'),
        ControlsRectMargin: 4,
        MapRectBorder: 1,
    };

    window.blockBoundsUpdate = false;
    const resizeObserver = new ResizeObserver(function () {
        const mapOverlays = $('#mapOverlays').length > 0;
        if (mapOverlays && window.innerWidth < 576) {
            removeOverlays(mappy.getContainer());
        } else if (!mapOverlays && window.innerWidth > 575) {
            initOverlays(mappy.getContainer());
        }
        updatePadding();
        recenterMap(false);
    });

    // Recenter map whenever its viewport changes size
    resizeObserver.observe(mapParams.ControlsRectEl);
    resizeObserver.observe(mapParams.MapRectEl);
    updatePadding();

    mappy.on('zoomend', function () { // Triggered by `flyTo` and `fitBounds` - must be blocked to prevent misalignment
        if (window.blockBoundsUpdate) {
            window.blockBoundsUpdate = false;
            //console.log('blockBoundsUpdate released.');
        } else {
            updateBounds();
        }
    });
    mappy.on('dragend', function () {
        updateBounds();
    });

}

export function initOverlays(whgMap) {

    if (window.innerWidth < 576) return // Do not use map overlays on very narrow screens

    const controlContainer = document.querySelector(
        '.maplibregl-control-container');
    controlContainer.setAttribute('id', 'mapControls');
    controlContainer.classList.add('item');

    const mapOverlays = document.createElement('div');
    mapOverlays.id = 'mapOverlays';
    whgMap.appendChild(mapOverlays);

    ['left', 'right'].forEach(function (side) {
        const column = document.createElement('div');
        column.classList.add('column', side);
        mapOverlays.appendChild(column);
        const overlays = document.querySelectorAll('.overlay.' + side);
        overlays.forEach(function (overlay) {
            column.appendChild(overlay);
            overlay.classList.add('item');
        });
        if (side == 'left') column.appendChild(controlContainer);
    });
}

export function removeOverlays(whgMap) {

    const mainElement = document.querySelector('main div.row');

    const mapOverlays = document.getElementById('mapOverlays');
    const overlaysLeft = mapOverlays.querySelectorAll('.overlay.left');
    const overlaysRight = mapOverlays.querySelectorAll('.overlay.right');

    overlaysLeft.forEach(function (overlay) {
        mainElement.appendChild(overlay);
        overlay.classList.remove('item');
    });

    overlaysRight.forEach(function (overlay) {
        mainElement.appendChild(overlay);
        overlay.classList.remove('item');
    });

    mainElement.appendChild(whgMap);

    const controlContainer = document.querySelector(
        '.maplibregl-control-container');
    controlContainer.removeAttribute('id');
    controlContainer.classList.remove('item');
    whgMap.appendChild(controlContainer);

    whgMap.removeChild(mapOverlays);
}

export function initDownloadLinks() {
    // Initialise Download link listener
    $('.a-dl, .a-dl-celery').click(function (e) {
        e.preventDefault();
        console.log('a-dl click()');
        let dsid = $(this).data('id');
        let collid = $(this).data('collid');
        let format = $('input[name="format"]:checked').val() || 'lpf';
        let urly = '/dlcelery/';
        console.log('dsid:', dsid, 'collid:', collid, 'format:', format);
        $.ajax({
            type: 'POST',
            url: urly,
            data: {
                'dsid': dsid,
                'collid': collid,
                'format': format,
                'csrfmiddlewaretoken': csrfToken,
            },
            datatype: 'json',
            success: function (response) {
                let taskId = response.task_id;
                console.log('.a-dl response:', response);
                console.log('.a-dl response taskId:', taskId);

                // Start polling for task progress
                var intervalId = setInterval(function () {
                    $.get('/task_progress/' + taskId, function (data) {
                        console.log('Server response:', data);
                        $('#progress-message').text('working...');
                        if (data.state == 'PROGRESS') {
                            // Update the progress indicator
                            $('#progress-bar').css('width',
                                data.progress.current / data.progress.total * 100 + '%');
                        } else if (data.state == 'SUCCESS') {
                            // Display a completion message and stop the interval
                            $('#progress-message').text('download complete!');
                            clearInterval(intervalId);
                        }
                    }).fail(function (jqXHR, textStatus, errorThrown) {
                        console.log('GET request failed', textStatus, errorThrown);
                    });
                }, 5000);
            },
        });
    });

    // TODO: Collection download event handlers
    // $('.btn-cancel').click(function() {
    //   $('#downloadModal').modal('hide');
    // });
    let clearEl = function (el) {
        $('#progress-bar').fadeOut();
        el.html('');
    };
}

let activePopup;

export function initPopups(table) {

    function clearPopup(preserveCursor = false) {
        if (activePopup) {
            if (activePopup.featureHighlight !== false) {
                mappy.setFeatureState(activePopup.featureHighlight, {highlight: false});
            }
            activePopup.remove();
            activePopup = null;
            if (!preserveCursor) mappy.getCanvas().style.cursor = '';
        }
    }

    mappy.on('mousemove', function (e) {
        const features = mappy.queryRenderedFeatures(e.point);

        if (features.length > 0) {
            const topFeature = features[0]; // Handle only the top-most feature
            const topLayerId = topFeature.layer.id;

            /*			// Check if the top feature's layer id starts with the id of any layer in datasetLayers
                  const isTopFeatureInDatasetLayer = // TODO: Remove this when finished upgrade of layersets
                    datasetLayers.some(layer => topLayerId.startsWith(layer.id));
                  if (isTopFeatureInDatasetLayer) { // TODO: Remove this when finished upgrade of layersets
                    topFeature.sourceLayer = false;
                  }*/

            const featureInLayerset = mappy.layersets.some(
                layer => topLayerId.startsWith(layer));
            if (featureInLayerset) {
                const dataset = ds_list.find(ds => ds.ds_id === topFeature.source);
                if (dataset) {
                    const datasetFeature = dataset.features.find(
                        f => f.properties.pid === topFeature.properties.pid);
                    if (datasetFeature) {
                        topFeature.properties.title = datasetFeature.properties.title;
                        topFeature.properties.min = datasetFeature.properties.min;
                        topFeature.properties.max = datasetFeature.properties.max;
                    }
                }
            }

            if (featureInLayerset) {
                mappy.getCanvas().style.cursor = 'pointer';

                if (!activePopup || activePopup.pid !== topFeature.properties.pid) {
                    // If there is no activePopup or it's a different feature, create a new one ...
                    if (activePopup) {
                        clearPopup(true);
                    }
                    activePopup = new whg_maplibre.Popup({
                        closeButton: false,
                    }).setLngLat(e.lngLat).setHTML(popupFeatureHTML(topFeature)).addTo(mappy);
                    activePopup.pid = topFeature.properties.pid;
                    activePopup.featureHighlight = {
                        source: topFeature.source,
                        sourceLayer: topFeature.sourceLayer,
                        id: topFeature.id,
                    };
                    if (!!window.highlightedFeatureIndex &&
                        window.highlightedFeatureIndex.id ===
                        activePopup.featureHighlight.id &&
                        window.highlightedFeatureIndex.source ===
                        activePopup.featureHighlight.source) {
                        activePopup.featureHighlight = false;
                    } else {
                        mappy.setFeatureState(activePopup.featureHighlight,
                            {highlight: true});
                    }
                } else {
                    // ... otherwise just update its position
                    activePopup.setLngLat(e.lngLat);
                }

            } else {
                clearPopup();
            }
        } else {
            clearPopup();
        }
    });

    mappy.on('click', function () {
        if (activePopup && activePopup.pid) {
            let savedPID = activePopup.pid;
            clearPopup();
            table.search('').draw();
            scrollToRowByProperty(table, 'pid', savedPID);
        }
    });
}

export function listSourcesAndLayers() {
    const style = mappy.getStyle();
    const sources = style.sources;
    console.log('Sources:', Object.keys(sources));
    const layers = style.layers;
    console.log('Layers:', layers.map(layer => layer.id));
}