// /whg/webpack/js/ds_browse.js

// TODO: Much of this code is replicated in mapFunctions.js and elsewhere
// Refactoring is not considered worthwhile because the page may be made redundant by planned backend upgrade

import {getPlace, popupFeatureHTML} from './getPlace';
import '../css/ds_browse.css';
import {scrollToRowByProperty} from "./tableFunctions-extended";

let whg_map = new whg_maplibre.Map({maxZoom: 10});
let featureCollection;
let table;
let activePopup;
let highlightedFeatureId = false;

async function init() {
    await waitMapLoad();
    await waitDocumentReady();
    await Promise.all(datatables_CDN_fallbacks.map(loadResource));

    console.log(`wdtask: ${wdtask}, tgntask: ${tgntask}, whgtask: ${whgtask}, anytask: ${anytask}`);

    localStorage.setItem('filter', '99');

    const filter_col = anytask ? "<'#status_filter.col-sm-12 col-md-5'>" : "<'#nostatus.col-sm-12 col-md-5'>";

    try {
        const response = await fetch(mapdata_url);
        if (!response.ok) throw new Error(`Failed to load mapdata: ${response.status}`);
        window.datacollection = await response.json();

        featureCollection = window.datacollection.table;

        whg_map
            .newSource(window.datacollection)
            .newLayerset(window.datacollection.metadata.ds_id, window.datacollection);
        whg_map.fitViewport(window.datacollection.metadata.extent, defaultZoom);

        initPlacetable(filter_col);
        initMapInteractions(table);
        whg_map.getContainer().style.opacity = 1;

    } catch (error) {
        console.error("Error loading mapdata:", error);
    }
}

function waitMapLoad() {
    return new Promise(resolve => {
        whg_map.on('load', () => {
            console.log('Map loaded.');
            resolve();
        });
    });
}

function waitDocumentReady() {
    return new Promise(resolve => {
        $(document).ready(resolve);
    });
}

init();

function initPlacetable(filter_col) {
    $('#drftable_list').spin();

	const data = featureCollection.features.map(f => {
		const props = f.properties ?? {};

        const getReview = key => props[key] || '<i>no hits</i>';

        return {
            properties: {
                id: props.pid ?? props.id,
                src_id: props.src_id ?? '',
                title: props.title ?? '',
                ccodes: props.ccodes ?? '',
                geo: props.geo ?? '',
                review_wd: props.review_wd ?? '',
                review_tgn: props.review_tgn ?? '',
                review_whg: props.review_whg ?? '',
                revwd: getReview('review_wd'),
                revtgn: getReview('review_tgn'),
                revwhg: getReview('review_whg')
            }
        };
	});

    table = $('#placetable').DataTable({
        dom: "<'row'<'col-sm-12 col-md-3'l>" + filter_col + "<'col-sm-12 col-md-4'f>>" +
            "<'row'<'col-sm-12'tr>>" +
            "<'row'<'col-sm-12 col-md-5'i><'col-sm-12 col-md-7'p>>",
        serverSide: false,
        data: data,
        scrollY: 400,
        select: true,
        order: [[2, 'asc']],
        columns: [
            {"data": "properties.id"},
            {"data": "properties.src_id"},
            {"data": "properties.title"},
            {"data": "properties.ccodes"},
            {"data": "properties.geo"},
            {"data": "properties.review_wd"},
            {"data": "properties.review_tgn"},
            {"data": "properties.review_whg"},
            {"data": "properties.revwd", "visible": !!wdtask, "orderable": false},
            {"data": "properties.revtgn", "visible": !!tgntask, "orderable": false},
            {"data": "properties.revwhg", "visible": !!whgtask, "orderable": false}
        ],
        columnDefs: [
            {className: "browse-task-col", "targets": [8, 9, 10]},
            {orderable: false, "targets": [4, 5, 6, 7]},
            {searchable: false, "targets": [0, 1, 3, 4, 8, 9, 10]},
            {visible: false, "targets": [5, 6, 7]}
        ],
		createdRow: function(row, data, dataIndex) {
			const $row = $(row);

			$row
				.data({
					properties: data,
				});
		},
        drawCallback: () => {
            $("#status_filter").html(buildSelect());
            $("#status_select").val(localStorage.getItem('filter'));
            setRowEvents();
        },
		initComplete: () => {
			$('#drftable_list').stopSpin();
		}
    });
}

function initMapInteractions(table) {
    whg_map.on('mouseleave', () => clearPopup());

    // whg_map.on('click', () => {
    //     if (activePopup) {
    //         getPlace(activePopup.pid);
    //         clearPopup();
    //         $('.highlight-row, .selected').removeClass('highlight-row selected');
    //     }
    // });

    // whg_map.on('click', function () {
    //     if (activePopup && activePopup.id) {
    //         getPlace(activePopup.id);
    //         clearPopup();
    //         $('.highlight-row, .selected').removeClass('highlight-row selected');
    //     }
    // });

    whg_map.on('click', function () {
        if (activePopup && activePopup.pid) {
            let savedID = activePopup.pid; // Store the ID of the clicked feature before clearing the popup
            clearPopup();
            table.search('').draw();
            scrollToRowByProperty(table, 'id', savedID);
        }
    });

    whg_map.on('mousemove', e => {
        const features = whg_map.queryRenderedFeatures(e.point);
        if (!features.length) return clearPopup();

        const topFeature = features[0];
        if (whg_map.baseStyle.layers.includes(topFeature.layer.id)) return clearPopup();

        whg_map.getCanvas().style.cursor = 'pointer';

        const datasetFeature = window.datacollection.table.features.find(
            f => f.properties.id === topFeature.id);
        if (datasetFeature) {
            topFeature.properties.title = datasetFeature.properties.title;
            topFeature.properties.pid = datasetFeature.properties.pid;
            topFeature.properties.min = datasetFeature.properties.min;
            topFeature.properties.max = datasetFeature.properties.max;
        }
        else {
            console.warn('Feature not found in dataset:', topFeature.id);
        }

        if (!activePopup || activePopup.id !== topFeature.id) {
            // If there is no activePopup or it's a different feature, create a new one ...
            if (activePopup) {
                clearPopup(true);
            }
            activePopup = new whg_maplibre.Popup({
                closeButton: false,
            }).setLngLat(e.lngLat).setHTML(popupFeatureHTML(topFeature)).addTo(whg_map);
            activePopup.pid = topFeature.properties.pid;
            activePopup.featureHighlight = {
                source: topFeature.source,
                id: topFeature.id,
            };
            if (!!window.highlightedFeatureIndex &&
                window.highlightedFeatureIndex.id ===
                activePopup.featureHighlight.id &&
                window.highlightedFeatureIndex.source ===
                activePopup.featureHighlight.source) {
                activePopup.featureHighlight = false;
            } else {
                whg_map.setFeatureState(activePopup.featureHighlight,
                    {highlight: true});
            }
        } else {
            // ... otherwise just update its position
            activePopup.setLngLat(e.lngLat);
        }
    });
}

function clearPopup(preserveCursor = false) {
    if (activePopup) {
        activePopup.remove();
        activePopup = null;
        if (!preserveCursor) {
            whg_map.getCanvas().style.cursor = '';
        }
    }
}

function highlightFeature(ds_pid, features, whg_map, extent = false) {
    var featureIndex = features.findIndex(f => f.properties.pid === parseInt(ds_pid.pid));
    if (featureIndex !== -1) {
        if (window.highlightedFeatureIndex !== undefined) whg_map.setFeatureState(window.highlightedFeatureIndex, {
            highlight: false
        });
        var feature = features[featureIndex];
        const geom = feature.geometry;
        if (geom) {
            whg_map.fitViewport(extent || bbox(geom), defaultZoom);

			// highlight feature in multiple sources & layers
			window.datacollection.metadata.layers.forEach(layerName => {
				window.highlightedFeatureIndex = {
					source: `${ds_pid.ds_id}_${layerName}`,
					id: featureIndex,
				};
				whg_map.setFeatureState(window.highlightedFeatureIndex, {
					highlight: true,
				});
			});
        } else {
            console.log('Feature in clicked row has no geometry.');
        }
    } else {
        console.log(`Feature ${ds_pid.pid} not found.`);
    }
}

function setRowEvents() {
    $("#status_select").change(function (e) {
        // clear search first
        console.log('search has val:', $("#placetable_filter input").val())
        //$("#placetable_filter input").val('')
        if ($("#placetable_filter input").val() != '') {
            $('#placetable').DataTable().search('').draw()
        }
        //fnDraw()
        const val = $(this).val();
        localStorage.setItem('filter', val);
        console.log(val);
        $('#status_filter').spin();
        if (val == '99') {
            // clear search
            clearFilters();
        } else {
            clearFilters();
            filterColumn(val[0], val[1]);
        }
        $('#status_filter').stopSpin();
    })

    $("#placetable tbody tr").click(function () {
        const thisy = $(this);
        // close popup if exists
        if (typeof poppy != 'undefined') {
            poppy.remove();
        }
        // get id
        const pid = parseInt($(this)[0].cells[0].textContent);

        // highlight this row, clear others
        var selected = $(this).hasClass("highlight-row");
        $("#placetable tr").removeClass("highlight-row");

        if (!selected)
            $(this).removeClass("rowhover");
        $(this).addClass("highlight-row");

        // fetch its detail
        getPlace(pid, false, function (placedata) {
            console.log('placedata', placedata);

            const ds_pid = {
                ds: window.datacollection.metadata.id,
                pid: pid,
                ds_id: window.datacollection.metadata.ds_id
            }

            highlightFeature(ds_pid, featureCollection.features, whg_map, placedata.extent);

        });

    })

    const row = $("#drftable_list table tbody")[0].rows[0];
    const pid = parseInt(row.cells[0].textContent);
    // highlight first row, fetch detail, but don't zoomTo() it
    $("#placetable tbody").find('tr').eq(0).addClass('highlight-row');

    if (pid) {
        $('#row_detail').spin();
        getPlace(pid);
        $('#row_detail').stopSpin();
    }

}

function buildSelect() {
    var select = '<label>Review filters: <select name="status" aria-controls="placetable" id="status_select" class="datatables_length">' +
        '<option value="99" selected="selected">All</option>'

    if (wdtask) {
        select +=
            '<option disabled>-----wikidata-----</option>' +
            '<option value="50">Needs review (wd)</option>' +
            '<option value="51">Reviewed (wd)</option>' +
            '<option value="52">Deferred (wd)</option>'
    }
    if (tgntask) {
        select +=
            '<option disabled>-------tgn-------</option>' +
            '<option value="60">Needs review (tgn)</option>' +
            '<option value="61">Reviewed (tgn)</option>' +
            '<option value="62">Deferred (tgn)</option>'
    }
    if (whgtask) {
        select +=
            '<option disabled>-------whg-------</option>' +
            '<option value="70">Needs review (whg)</option>' +
            '<option value="71">Reviewed (whg)</option>' +
            '<option value="72">Deferred (whg)</option>'
    }

    select += '</select></label>'
    return select
}

function filterColumn(i, v) {
    // clear then search
    console.log('filterColumn', i, v, typeof (i))
    table
        .columns([7, 8])
        .search('')
        .columns(i)
        .search(v)
        .draw();
    $("#status_select").val(localStorage.getItem('filter'))
}

function clearFilters() {
    // clear
    table
        .columns([5, 6, 7])
        .search('')
        .draw();
    $("#status_select").val('99')
}