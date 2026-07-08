// builders-dataset.js <- used by ds_metadata.html

import '../css/builders-dataset.css';

$(function() {

	if (context_status == 'updating') {
		console.log(context_status, context_context)
		$("#ds_info").addClass('hidden')
		$("#ds_updating").removeClass('hidden')
		$("#div_file").toggleClass('border-red')
		$(".update-msg").removeClass('hidden')
	}

	window.dslabel = context_datasetLabel

	// ******************* LISTENERS *******************

	$('#volunteers').change(function() {
		var isChecked = $(this).is(':checked');
		console.log('volunteers?', isChecked)
		$.ajax({
			url: '/datasets/toggle_volunteers',
			type: 'POST',
			headers: { 'X-CSRFToken': csrfToken },
			data: {
				'is_checked': isChecked,
				'dataset_id': context_datasetId,
			},
			success: function(response) {
				if (isChecked) {
					alert('Dataset is now listed as a volunteer opportunity');
				} else {
					alert('Dataset is no longer listed as a volunteer opportunity');
				}
			}
		});
	});

	// views_dl.downloader()
	$(".a-dl-celery").click(function(e) {
		e.preventDefault();

		// Check if it's already been clicked
		if ($(this).hasClass('clicked')) {
			return; // Exit if already clicked
		}

		// Mark as clicked
		$(this).addClass('clicked');

		// Indicate it's disabled
		$(this).css({
			'cursor': 'not-allowed',
			'pointer-events': 'none', // prevents further clicks
			'color': '#777',          // changes the color to a "disabled" gray
			'text-decoration': 'none' // removes underline
		});

		// Change tooltip text
		$(this)
			.tooltip('dispose')
			.html('Download is being prepared. It will be available shortly in your \'My Data\' download section.');

		let format = $(this).data('format') || context_format
		let dsid = context_datasetId
		let urly = '/dlcelery/'
		$.ajax({
			type: 'POST',
			headers: { 'X-CSRFToken': csrfToken },
			url: urly,
			data: {
				"format": format,
				"dsid": dsid,
			},
			datatype: 'json',
			success: function(response) {
				if ($("#ds_downloads").length) {
					let task_id = response.task_id
					var progressUrl = "/celery-progress/" + task_id + "/";
					CeleryProgressBar.initProgressBar(progressUrl, {
						pollingInterval: 500,
						onResult: customResult,
					})
				}
			}
		})
	})

	$("#a_update_modal").on('click', function(e) {
		console.log('clicked update')
		if (context_format != 'delimited') {
			alert(
					'Sorry, update is available only for delimited files right now. Soon...')
		}
	})

	// show upload button after file selected
	$("#newfile").on("change", function() {
		$("#btn_upload").removeClass('hidden')
	});

	$("#btn_done").on('click', function() {
		location.reload();
	})
	$("#btn_cancel").on('click', function() {
		location.reload();
	})

	$('.vocab').on('click', function(e) {
		console.log('id', $(this).data('id'))
	});

	// TODO: refactor this unholy mess
	// also, button is hidden on 2nd click
	$(".edit-title").click(function() {
		$(".editing-title").toggleClass("hidden")
		$(".form-title").toggleClass("hidden")
		$(".btn-ds").toggleClass("hidden")
	})
	$(".edit-description").click(function() {
		$(".editing-description").toggleClass("hidden")
		$(".form-description").toggleClass("hidden")
		$(".btn-ds").toggleClass("hidden")
	})
	$(".edit-public").click(function() {
		console.log('edit-public')
		$(".editing-public").toggleClass("hidden")
		$(".form-public").toggleClass("hidden")
		$(".btn-ds").toggleClass("hidden")
	})

	// Publishing (make-public) indexes the dataset into search. If its reconciliation review isn't
	// complete, warn the editor first — the records are indexed as-is, deferred/unreviewed matches and
	// all — with a "publish anyway" option. Only intercepts when turning public ON for the first time
	// on a not-yet-complete dataset; a normal publish of a finished dataset saves without the prompt.
	var _publishConfirmed = false;
	$("#ds_form").on("submit", function(e) {
		var form = this;
		var pubBox = document.getElementById("id_public");
		if (_publishConfirmed || !pubBox || !pubBox.checked) return;      // not publishing → normal save
		if (form.getAttribute("data-was-public") === "1") return;         // already public → no change
		if (form.getAttribute("data-recon-complete") === "1") return;     // fully reconciled → no warning
		e.preventDefault();
		var rows = form.getAttribute("data-numrows") || "";
		var rowsEl = document.getElementById("publishWarnRows");
		if (rowsEl) rowsEl.textContent = rows;
		var m = document.getElementById("publishWarnModal");
		if (m && window.bootstrap && window.bootstrap.Modal) {
			window.bootstrap.Modal.getOrCreateInstance(m).show();
		} else if (window.confirm("This dataset's reconciliation review isn't complete. Publish and index all records into search anyway?")) {
			_publishConfirmed = true; form.submit();
		}
	});
	$("#confirmPublishBtn").on("click", function() {
		_publishConfirmed = true;
		var m = document.getElementById("publishWarnModal");
		if (m && window.bootstrap && window.bootstrap.Modal) {
			var inst = window.bootstrap.Modal.getInstance(m);
			if (inst) inst.hide();
		}
		document.getElementById("ds_form").submit();
	})
	$(".edit-creator").click(function() {
		$(".editing-creator").toggleClass("hidden")
		$(".form-creator").toggleClass("hidden")
		$(".btn-ds").toggleClass("hidden")
	})
	$(".edit-source").click(function() {
		$(".editing-source").toggleClass("hidden")
		$(".form-source").toggleClass("hidden")
		$(".btn-ds").toggleClass("hidden")
	})
	$(".edit-contrib").click(function() {
		$(".editing-contrib").toggleClass("hidden")
		$(".form-contrib").toggleClass("hidden")
		$(".btn-ds").toggleClass("hidden")
	})
	$(".edit-citation").click(function() {
		$(".editing-citation").toggleClass("hidden")
		$(".form-citation").toggleClass("hidden")
		$(".btn-ds").toggleClass("hidden")
	})
	$(".edit-uri_base").click(function() {
		$(".editing-uri_base").toggleClass("hidden")
		$(".form-uri_base").toggleClass("hidden")
		$(".btn-ds").toggleClass("hidden")
	})
	$(".edit-webpage").click(function() {
		$(".editing-webpage").toggleClass("hidden")
		$(".form-webpage").toggleClass("hidden")
		$(".btn-ds").toggleClass("hidden")
	})
	$(".edit-featured").click(function() {
		$(".editing-featured").toggleClass("hidden")
		$(".form-featured").toggleClass("hidden")
		$(".btn-ds").toggleClass("hidden")
	})
	$(".edit-image").click(function() {
		$(".editing-image").toggleClass("hidden")
		$(".form-image").toggleClass("hidden")
		$(".btn-ds").toggleClass("hidden")
	})
	$(".edit-pdf").click(function() {
		$(".editing-pdf").toggleClass("hidden")
		$(".form-pdf").toggleClass("hidden")
		$(".btn-ds").toggleClass("hidden")
	})

	// ******************* FUNCTIONS *******************

	let dater = function() {
		const date = new Date(Date.now());
		return date.toISOString().substring(0, 10)
	}

	let clearEl = function(el) {
		$("#progress-bar").fadeOut()
		el.html('')
	}

	function customResult(resultElement, result) {
		pass
	}

	// post-download actions
	function customResult(resultElement, result) {
		console.log('celery result', result)
		console.log('celery resultElement', resultElement)
		$("#ds_downloads").stopSpin();
		let fn = result.filename
		let link = '[ <span class="dl-save"><a href="/' + fn +
				'" title="downloaded: ' + dater() + '" download>save</a></span> ]'
		$(resultElement).append(
				$('<p>').html(link)
		);
		$(".dl-save a")[0].click()
		setTimeout(clearEl($("#celery-result")), 1000)
	}

})

	// function startDownloadSpinner() {
	// 	$("#ds_downloads").append(spinner_dl.el);
	// }

	// ******************* UPDATE functions DEPRECATED *******************
	// submit new file for comparison
	// prepares compare_data{} object, passed to ds_update if/when 'proceed' is clicked
	// $(document).on('submit', '#newfileform', function(e) {
	// 	e.preventDefault()
	// 	var formData = new FormData();
	// 	formData.append('file', $('#newfile')[0].files[0]);
	// 	formData.append('dsid', context_datasetId);
	// 	formData.append('format', context_format);
	//
	// 	$.ajax({
	// 		type: 'POST',
	// 		headers: { 'X-CSRFToken': document.querySelector('[name=csrfmiddlewaretoken]').value },
	// 		enctype: 'multipart/form-data',
	// 		url: '/datasets/compare/',
	// 		processData: false,
	// 		contentType: false,
	// 		cache: false,
	// 		data: formData,
	// 		success: function(data) {
	// 			console.log('data returned', data)
	// 			if ('failed' in data) {
	// 				errors = data['failed']['errors']
	// 				html = '<b class="text-danger">Data validation issue(s):</b> <ul class="ul-flush">'
	// 				var i = 0;
	// 				i < 9;
	// 				i++
	// 				for (var i = 0; i < errors.length; i++) {
	// 					html += '<li class="li-flush">' + errors[i] + '</li>'
	// 				}
	// 				html += '</ul><p>Please correct and try again</p>'
	// 			} else {
	// 				html = '<b>Current file</b>: <i>' + data['filename_cur'] + '</i><br/>'
	// 				html += '<b>New file</b>: <i>' + data['filename_new'] + '</i><br/>'
	// 				html += '<b>New temp file</b>: <i>' + data['tempfn'] + '</i><br/>'
	// 				html += '<b>Indexed?</b>: <i>' + (data['count_indexed'] > 0 ? "Yes" : "No") + '</i><br/>'
	// 				html += '<b>Validation result</b>: <i>' +
	// 					(data['validation_result']['errors'].length < 1 ? 'format valid' : data['validation_result']['errors']) + '</i><hr/>'
	// 				html += comparisonText(data)
	// 				$("#btn_update").removeClass('hidden')
	// 			}
	// 			$("#loadfile").addClass('hidden')
	// 			$("#results_text").html(html)
	// 			compare_data = data
	// 		}
	// 	})
	// })

	// performs ds_update()
	// $("#btn_update").on('click', function() {
	// 	console.log('compare_data', compare_data)
	// 	startUpdateSpinner()
	// 	var formData = new FormData();
	// 	formData.append('dsid', context_datasetId);
	// 	formData.append('format', context_format);
	// 	formData.append('keepg', $("#preserve_geoms").length ? $('#preserve_geoms')[0].checked : "true");
	// 	formData.append('keepl', $("#preserve_links").length ? $('#preserve_links')[0].checked : "true");
	// 	formData.append('compare_data', JSON.stringify(compare_data));
	//
	// 	$.ajax({
	// 		type: 'POST',
	// 		headers: { 'X-CSRFToken': document.querySelector('[name=csrfmiddlewaretoken]').value },
	// 		enctype: 'multipart/form-data',
	// 		url: '/datasets/update/',
	// 		processData: false,
	// 		contentType: false,
	// 		cache: false,
	// 		data: formData,
	// 		success: function(data) {
	// 			console.log('update result data', data)
	// 			$("#buttons_pre").addClass('hidden')
	// 			$("#btn_done").removeClass('hidden')
	// 			html = '<h6>update complete!</h6>'
	// 			html += updateText(data)
	// 			$("#results_text").html(html)
	// 			spinner_update.stop()
	// 		}
	// 	})
	// })

	// parse & prettify ds_update() results
	// [status,format,update_count,redo_count,new_count,deleted_count,newfile]
// 	function updateText(data) {
// 		html = 'Changes in database: <br/>' + '<ul>' +
// 			'<li>Added ' + data["new_count"] + ' rows </li>' +
// 			'<li>Deleted ' + data["deleted_count"] + ' rows </li>' +
// 			'<li>Updated ' + data["update_count"] + ' rows </li>' + '</ul>'
// 		html += 'A followup reconciliation to Wikidata task is required.'
//
// 		if (data['indexed'] == true) {
// 			html += ', as well as a reindexing, in order to account for these changes.'
// 		}
// 		return html
// 	}
//
// 	// parse & prettify ds_compare() results
// 	function comparisonText(data) {
// 		stats = data['compare_result']
// 		keepg = data['keepg'];
// 		keepl = data['keepl'];
// 		html = 'This action would perform these WHG <i>database</i> updates:<br/><ul style="padding:0;list-style: inside;">'
// 		html += '<li>Update <b>' + stats['count_replace'] + '</b> place record(s) having same source IDs </li>'
// 		html += stats['rows_add'].length > 0 ? '<li>Add ' + stats['rows_add'].length + ' record(s): (<b>' + stats['rows_add'].join(', ') + '</b>)</li>' : ''
// 		html += stats['rows_del'].length > 0 ? '<li>Remove ' + stats['rows_del'].length + ' record(s): (<b>' + stats['rows_del'].join(', ') + '</b>)</li>' : ''
// 		html += stats['cols_add'].length > 0 ? '<li>Add column(s): (<b>' + stats['cols_add'] + '</b>)</li>' : ''
// 		html += stats['cols_del'].length > 0 ? '<li>Remove column(s): (<b>' + stats['cols_del'] + '</b>)</li>' : ''
// 		if (data['count_geoms_added'] > 0) {
// 			html += '<li class="text-danger">Keep <b>' + data['count_geoms_added'] +
// 				'</b> existing place-geometry records from prior reconciliation)' +
// 				' <input type="checkbox" id="preserve_geoms" checked></li>'
// 		}
// 		if (data['count_links_added'] > 0) {
// 			html += '<li class="text-danger">Keep <b>' + data['count_links_added'] +
// 				'</b> existing place-link records from prior reconciliation)' +
// 				' <input type="checkbox" id="preserve_links" checked></li>'
// 		}
// 		html += '</ul>'
// 		html += stats['rows_add'].length > 0 ? '<b>NOTE:</b><br/>' +
// 			'The ' + stats['rows_add'].length + ' new record(s) and any being modified will have to be reconciled.' : ''
// 		html += data['count_indexed'] > 0 ? '<br/>Following that, the new and/or modified records will need re-indexing.' : ''
// 		return html
// 	}
//
// })
// 	function startUpdateSpinner() {
// 		window.spinner_update = new Spin.Spinner().spin();
// 		$("#update_spinner").append(spinner_update.el);
// 	}
//