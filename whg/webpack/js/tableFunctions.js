import {getPlace} from './getPlace';
import {updatePadding, recenterMap} from './mapFunctions';
import {mapSequencer} from './mapControls';
import {whg_map} from './mapAndTable';
import {scrollToRowByProperty} from './tableFunctions-extended';

let table;

function highlightFirstRow() {
	let rows = $('#placetable tbody tr');
	rows.removeClass('highlight-row');
	const firstrow = rows.first();
	if (!firstrow.find('td:first').hasClass('dataTables_empty')) {
		const pid = firstrow.data('ds_pid').pid;
		const cid = firstrow.data('cid');
		// highlight first row and fetch detail, but don't fly map to it
		firstrow.addClass('highlight-row');
		getPlace(pid, cid);
	}
}

// Adjust the DataTable's page length to avoid scrolling where possible
function adjustPageLength() {
	const dsTable = document.getElementById('ds_table');
	const tableFilter = document.getElementById('placetable_filter');
	const tablePaginate = document.getElementById('placetable_paginate');
	const theadRow = document.querySelector('#placetable thead tr');
	const tbody = document.querySelector('#placetable tbody');
	const availableHeight = dsTable.clientHeight - (2 * 10 /*padding*/ ) -
		tableFilter.clientHeight - tablePaginate.clientHeight -
		theadRow.clientHeight;
	const averageRowHeight = 2 + (tbody.clientHeight /
		document.querySelectorAll('#placetable tr:not(thead tr)').length);
	let estimatedRowsPerPage = Math.floor(availableHeight / (averageRowHeight));
	// Ensure a minimum of 5 rows
	estimatedRowsPerPage = Math.max(estimatedRowsPerPage, 5);
	const DataTable = $('#placetable').DataTable();
	DataTable.page.len(estimatedRowsPerPage).draw();
}

export function resetSearch() {
	var e = $.Event('keyup');
	e.which(13);
	$('#placetable_filter input').trigger(e);
}

function filterColumn(i, v) {
	// clear then search
	table.search('').columns(i).search(v).draw();
	$('#ds_select').val(localStorage.getItem('filter'));
}

function clearFilters() {
	// clear
	table.columns([3, 5, 6, 7, 11]) // TODO: Check these values?
		.search('').draw();
	$('#ds_select').val('-1');
}

function toggleMapLayers(whg_map, val) {
	if (val !== 'all') {
		window.mapBounds = whg_map.getSource(val)._data.extent;
		recenterMap('lazy');
	}
	whg_map.getStyle().layers.forEach(layer => {
		if (whg_map.layersets.includes(layer.source)) {
			whg_map.setLayoutProperty(layer.id, 'visibility',
				(val == 'all' || val == layer.source) ? 'visible' : 'none');
		}
	});
}

export function highlightFeature(ds_pid, features, whg_map, extent = false) {
    if (!Array.isArray(features) || features.length === 0) {
        console.warn('No features provided to highlightFeature.');
        return;
    }

    // Filter features belonging to the dataset
    features = features.filter(f => f.properties?.dsid === ds_pid.ds);
    if (features.length === 0) {
        console.warn(`No features found for dataset ${ds_pid.ds}.`);
        return;
    }

    // Find the table feature by pid
    const tableFeature = features.find(
        f => String(f.properties?.pid) === String(ds_pid.pid)
    );

    if (!tableFeature) {
        console.warn(`Feature ${ds_pid.pid} not found in dataset ${ds_pid.ds}.`);
        return;
    }

    // Get the table ID (index in table.features array)
    const tableId = tableFeature.properties.id;

    // Remove highlight from previously highlighted feature
    if (window.highlightedFeatureIndex !== undefined) {
        whg_map.setFeatureState(window.highlightedFeatureIndex, { highlight: false });
    }

    // Zoom to feature (using extent if provided, otherwise compute bbox)
    try {
        whg_map.fitViewport(extent, defaultZoom);
    } catch (err) {
        console.error('Failed to fit viewport for feature', err);
        return;
    }

    // Highlight feature in all relevant layers by finding map features with matching table_id
    if (!window.datacollection?.metadata?.layers?.length) {
        console.warn('No layers defined in datacollection metadata.');
        return;
    }

    // We need to find the actual map feature(s) with this table_id
    // Query all features from all relevant sources
    let foundFeature = false;
    window.datacollection.metadata.layers.forEach(layerName => {
        const sourceId = `${ds_pid.ds_id}_${layerName}`;
        const source = whg_map.getSource(sourceId);
        if (!source) {
            console.warn(`Source ${sourceId} not found in current map style.`);
            return;
        }

        // Query all features from this source
        const mapFeatures = whg_map.querySourceFeatures(sourceId);

        // Find features with matching table_id
        mapFeatures.forEach(mapFeature => {
            if (mapFeature.properties && mapFeature.properties.table_id === tableId) {
                window.highlightedFeatureIndex = { source: sourceId, id: mapFeature.id };
                whg_map.setFeatureState(window.highlightedFeatureIndex, { highlight: true });
                foundFeature = true;
            }
        });
    });

    if (!foundFeature) {
        console.warn(`No map features found with table_id ${tableId} for pid ${ds_pid.pid}`);
    }
}

export function initialiseTable(
	features, checked_rows, whg_map) {

	// TODO: remove these artifacts of table used for review
	localStorage.setItem('filter', '99');
	var wdtask = false;
	var tgntask = false;
	var whgtask = false;

	$('#drftable_list, #detail').spin();

	const isCollection = window.datacollection.ds_type == 'collections'; // i.e. *place* collection (not *dataset* collection or dataset)

	checked_rows = [];

	/*  if (!!window.ds_list[0].datasets) { // Initialise dataset selector for Dataset Collection
	    let select = '<label>Datasets: <select id="ds_select">' +
	        '<option value="-1" data="all" selected="selected">All</option>';
	    for (let ds of window.ds_list[0].datasets) {
	      select += '<option value="' + ds.label + '" data="' + ds.id + '">' +
	          ds.title + '</option>';
	    }
	    select += '</select></label>';
	    $('#ds_filter').html(select);
	  }*/

	// Define Datatable columns based on ds_type
	let columns;
	let columnDefs;
	let order;
	let hideColumns;
	let sortColumn;

	if (!isCollection) { // Datasets

		const check_column = window.loggedin == 'true' ? {
			title: '<a href=\'#\' rel=\'tooltip\' title=\'add one or more rows to a collection\'><i class=\'fas fa-question-circle linkypop\'></i></a>',
			data: 'properties.pid',
			render: function(data, type, row) {
				return `<input type="checkbox" name="addme" class="table-chk" data-id="${data}"/>`;
			},
			visible: true,
		} : {
			data: 'properties.pid',
			visible: false,
		};

		columns = [{
				title: 'start',
				data: 'properties.min',
				render: function(data) {
					return data === 'null' ? '-' : data;
				},
			},
			{
				title: 'end',
				data: 'properties.max',
				render: function(data) {
					return data === 'null' ? '-' : data;
				},
			},
			{
				title: 'title',
				data: 'properties.title',
			},
			{
				title: 'pid',
				data: 'properties.pid',
				render: function(data, type, row) {
					//return detail page, not json
					//return `<a href="${URL_FRONT}api/db/?id=${data}" target="_blank">${data}</a>`;
					return `<a href="/places/${data}/detail" target="_blank">${data}</a>`
				},
				visible: false,
			},
			{
				title: 'geo',
				data: 'geometry',
				render: function(data, type, row) {
					if (data) {
						return `<img src="/static/images/geo_${data.type.toLowerCase().
                replace('multi', '')}.svg" width=12/>`;
					} else {
						return '<i>none</i>';
					}
				},
			},
			{
				title: 'dataset',
				data: function(row) {
					return row.properties.dslabel || null;
				},
				visible: false,
			},
			check_column,
		];

		// Determine columns to be hidden
		hideColumns = columns.slice(0,2).reduce((result, column, index) => {
			const tabulateValue = window.datacollection.metadata.visParameters[column.data.split('.')[1]]?.tabulate;
			if (tabulateValue !== undefined && tabulateValue === false) {
				result.push(index);
			}
			return result;
		}, []);
		// if (window.ds_list.length > 1) hideColumns.push(5);
	
		// Determine initial sort column
		sortColumn = columns.slice(0,2).reduce((result, column, index) => {
			const tabulateValue = window.datacollection.metadata.visParameters[column.data.split('.')[1]]?.tabulate;
			if (tabulateValue !== undefined && tabulateValue === 'initial') {
				result.push(index);
			}
			return result;
		}, []);
		sortColumn.push(0); // - in case none has been set

		columnDefs = [{
				orderable: false,
				'targets': [4, 6],
			},
			{
				searchable: false,
				'targets': [0, 1, 4, 6],
			},
			{
				visible: false,
				'targets': hideColumns,
			},
		];

		order = [
			[sortColumn[0], 'asc'],
		];
		
	} else { // Collections
		columns = [{
			title: 'seq',
			data: 'properties.seq',
		}, {
			title: 'start',
			data: 'properties.min',
			render: function(data) {
				return data === 'null' ? '-' : data;
			},
		}, {
			title: 'end',
			data: 'properties.max',
			render: function(data) {
				return data === 'null' ? '-' : data;
			},
		}, {
			title: 'title',
			data: 'properties.title',
		}, {
			title: 'country',
			data: 'properties.ccodes',
		}, {
			data: 'properties.pid',
			visible: false,
		}, ];

		console.log('Determining column inclusion and initial sorting:', window.datacollection.metadata.visParameters);
		// Determine columns to be hidden
		hideColumns = columns.slice(0,3).reduce((result, column, index) => {
			const tabulateValue = window.datacollection.metadata.visParameters[column.data.split('.')[1]]?.tabulate;
			if (tabulateValue !== undefined && tabulateValue === false) {
				result.push(index);
			}
			return result;
		}, []);
	
		// Determine initial sort column
		sortColumn = columns.slice(0,3).reduce((result, column, index) => {
			const tabulateValue = window.datacollection.metadata.visParameters[column.data.split('.')[1]]?.tabulate;
			if (tabulateValue !== undefined && tabulateValue === 'initial') {
				result.push(index);
			}
			return result;
		}, []);
		sortColumn.push(0); // - in case none has been set

		/*		columns.push({
		          title: 'sortIndex', // Used by Collection sequencer control
		          visible: false,
		          orderable: false,
		          searchable: false
		      })*/

		columnDefs = [{
			searchable: false,
			'targets': [0, 1, 2, 4],
		}, {
			visible: false,
			'targets': hideColumns,
		}, ];

		order = [
			[sortColumn[0], 'asc'],
		];
	}

	table = $('#placetable').DataTable({
		deferRender: true,
		dom: '<\'row small\'<\'col-sm-9\'f>' + '<\'col-sm-3\'>>' +
			'<\'row\'<\'col-sm-12\'tr>>' +
			'<\'row small\'<\'col-sm-12\'p>>',
		select: true,
		columns: columns,
		columnDefs: columnDefs,
		order: order,
		data: features,
		rowId: 'properties.pid',
		createdRow: function(row, data, dataIndex) {
			const $row = $(row);
			const props = data.properties;

			$row
				.attr({
					'data-min': props.min,
					'data-max': props.max,
					'dsid': props.dsid,
					'pid': props.pid
				})
				.data({
					ds_pid: {
						ds: props.dsid,
						pid: props.pid,
						ds_id: props.ds_id,
					},
					cid: props.cid,
					placedata: props,
				});

			if (!data.geometry) $row.addClass('no-geometry');
			if (props.min === 'null' || props.max === 'null') $row.addClass('no-temporal');
		},
		initComplete: function(settings, json) {
			adjustPageLength();
		},
		drawCallback: function(settings) {
			//console.log('table drawn')
			$('#drftable_list').stopSpin();
			if (checked_rows.length > 0) {
				const currentPageRows = table.rows({ page: 'current' }).nodes();
				checked_rows.forEach(id => {
					$(currentPageRows).find(`[data-id="${id}"]`).prop('checked', true);
				});
				$('#selection_status').show();
			}
			highlightFirstRow();
			if (!!mapSequencer) {
				mapSequencer.updateButtons();
			}
		},
	});

    $('#placetable_filter input')
    .attr('placeholder', 'Filter places...')
    .removeAttr('aria-controls')
    .on('keyup search', function() {
        const searchTerm = $(this).val().toLowerCase();

        if (searchTerm === '') {
            whg_map.layersetObjects.forEach(layerset => {
                layerset.setSearchFilter(null);
            });
        } else {
            // Get visible rows and extract their table_id (properties.id)
            const visibleTableIds = table.rows({ search: 'applied' }).data().toArray()
                .map(row => row.properties?.id) // Use 'id', not 'pid'
                .filter(id => id !== null && id !== undefined);

            console.log('Visible table IDs:', visibleTableIds);

            if (visibleTableIds.length === 0) {
                whg_map.layersetObjects.forEach(layerset => {
                    layerset.setSearchFilter(['==', 'table_id', -1]);
                });
            } else {
                // Filter by table_id, not pid
                const filter = ['in', 'table_id', ...visibleTableIds];
                console.log('Applying filter:', filter);

                whg_map.layersetObjects.forEach(layerset => {
                    layerset.setSearchFilter(filter);
                });
            }
        }
    });

    $('#placetable_filter label').contents().filter(function() {
        return this.nodeType === 3; // Remove text nodes (i.e., "Search:")
    }).remove();

	$('#addchecked').click(function() {
		console.log('clicked #addchecked');
		$('#addtocoll_popup').fadeIn();
	});

	$('.closer').click(function() {
		$('#addtocoll_popup').hide();
	});

	$('#ds_select').change(function(e) {

		// filter table
		let val = $(this).val();
		localStorage.setItem('filter', val);
		
		$('#dataset_content').spin();

		// filter map
		let ds_id = $(this).find(':selected').attr('data');
		const dsItem = window.datacollection.datasets.find(ds => String(ds.id) === ds_id);
		if (dsItem) {
			window.mapBounds = dsItem.extent;
		} else {
			window.mapBounds = window.datacollection.metadata.extent;
		}
		//toggleMapLayers(whg_map, ds_id); // Also recenters map on selected layer
		recenterMap('lazy');

		$('#dataset_content').stopSpin();
	});

	$('body').on('click', '.table-chk', function(e) {
		e.stopPropagation(); // Prevents row-click functionality
		const id = $(this).data('id');
		if ($(this).is(':checked')) {
			// Checkbox is checked, add the data-id to the checked_rows array
			console.log('adding', id);
			checked_rows.push(id);
		} else {
			// Checkbox is unchecked, remove the data-id from the checked_rows array
			console.log('removing', id);
			const index = checked_rows.indexOf(id);
			if (index > -1) {
				checked_rows.splice(index, 1);
			}
		}
		console.log('checked_rows', checked_rows);
		if (checked_rows.length > 0) {
			$('#selection_status').fadeIn(600);
		} else {
			$('#selection_status').fadeOut(600);
		}
		$('#sel_count').html(' ' + checked_rows.length + ' ');
	});

	// $("body").on("click", ".table-chk", function(e) {
	// 	e.stopPropagation(); // Prevents row-click functionality
	// 	console.log('adding', $(this).data('id'))
	// 	checked_rows.push($(this).data('id'))
	// 	console.log('checked_rows',checked_rows)
	// 	console.log('checked_rows count', checked_rows.length)
	// 	$("#sel_count").html(' ' + checked_rows.length + ' ')
	// 	$("#selection_status").fadeIn()
	// })

	$('body').on('click', '#placetable tbody tr', function() {
		const ds_pid = $(this).data('ds_pid');

		// highlight this row, clear others
		var selected = $(this).hasClass('highlight-row');
		$('#placetable tr').removeClass('highlight-row selected');

		if (!selected)
			$(this).removeClass('rowhover');
		$(this).addClass('highlight-row');
		
		$('#detail').empty().spin();
		$('.maplibregl-control-container').spin();

		// fetch its detail
		getPlace(
			ds_pid.pid,
			$(this).data('cid'),
			function(placedata) {
				highlightFeature(ds_pid, features, whg_map, placedata.extent);
			}
		);
		
		$('#detail, .maplibregl-control-container').stopSpin();

		if (!!mapSequencer) {
			mapSequencer.updateButtons();
			if (mapSequencer.continuePlay) {
				mapSequencer.continuePlay = false;
			} else if (mapSequencer.playing) {
				mapSequencer.stopPlayback();
			}
		}

	});

	// Custom search function to filter table based on dateline.fromValue and dateline.toValue
	$.fn.dataTable.ext.search.push(function(settings, data, dataIndex) {
		// Check if temporal filtering is enabled
		// If dateline doesn't exist or filtering is disabled, show all rows
		if (!window.dateline || !window.dateline.filteringEnabled) {
			return true;
		}

		const fromValue = window.dateline.fromValue;
		const toValue = window.dateline.toValue;
		const includeUndated = window.dateline.includeUndated;

		const rawRow = settings.aoData[dataIndex]._aData;
		const props = rawRow.properties;

		const parseVal = (val) => {
			if (val === 'null' || val === null || val === undefined || val === '') return null;
			const parsed = parseFloat(val);
			return isNaN(parsed) ? null : parsed;
		};

		const min = parseVal(props.min);
		const max = parseVal(props.max);

		const isUndated = (min === null || max === null);

		if (isUndated) {
			return includeUndated;
		}

		// Check if feature's temporal range [min, max] overlaps with selected range [fromValue, toValue]
		// Two ranges overlap if: max >= fromValue AND min <= toValue
		const passes = max >= fromValue && min <= toValue;

		return passes;

	});

	return {
		table,
		checked_rows,
	};
}

export { table };
