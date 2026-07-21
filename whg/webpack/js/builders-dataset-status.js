// builders-dataset-status.js

import '../css/builders-dataset-status.css';

$(function() {

	// if (context_status == 'updating') {
	// 	console.log(context_status, context_context)
	// 	$("#ds_info").addClass('hidden')
	// 	$("#ds_updating").removeClass('hidden')
	// 	$("#div_file").toggleClass('border-red')
	// 	$(".update-msg").removeClass('hidden')
	// }

	window.dslabel = context_dsLabel
	
	// ******************* LISTENERS *******************   

	// display time info y/n
	$('#year_filter').change(function() {
			var isChecked = $(this).prop('checked');
			$.ajax({
					url: '/datasets/update_vis_parameters/',
					method: 'POST',
					headers: { 'X-CSRFToken': csrfToken },
					data: {
							'checked': isChecked,
							'ds_id': context_dsid
					},
					success: function(response) {
			console.log(response.message);
					},
					error: function(xhr, status, error) {
							// If the AJAX call fails, reset the checkbox state
							$('#year_filter').prop('checked', !isChecked);
							alert('Sorry, failed to update visualisation parameters.');
					}
			});
	});

	// display form and value if there is one
	window.volunteersText = context_volunteers_text;
	console.log('volunteersText', volunteersText);
	window.ds_status = context_status;

	if(volunteersText && volunteersText !== "None" && (ds_status === 'reconciling' || ds_status === 'accessioning')) {
		console.log('supposed to fucking show')
		$('#volunteerForm').show();
		$('#volunteerText').val(volunteersText);
	}

	$('#volunteers').change(function() {
		var isChecked = $(this).is(':checked');
		if (isChecked) {
			$('#volunteerForm').fadeIn();
				$("label[for='volunteers']").text("Volunteer request listed");
				$('#volunteerText').show();
		} else {
			// Checkbox is unchecked
			$("label[for='volunteers']").text("Removed from volunteering list");
			$('#volunteerText').val(''); // Reset textarea
			$('#volunteerForm').fadeOut();
			setTimeout(function() {
					$("label[for='volunteers']").text("Request volunteer help");
			}, 2000); // Change label text back after 2 seconds

			$.ajax({
				url: '/datasets/update_volunteers_text/', // URL of the Django view
				type: 'POST',
				headers: { 'X-CSRFToken': csrfToken },
				data: {
					'reset': 'true',
					'dataset_id': context_dsid,
				},
				success: function(response) {
					$("label[for='volunteers']").text("Removed from volunteering list");
					console.log('removed from volunteering list');
				},
				error: function(xhr, status, error) {
					console.error('Failed to reset volunteer text');
				}
			});
		}


	});

	$('#volunteerTextForm').submit(function(e) {
			e.preventDefault();
			var volunteerText = $('#volunteerText').val();
			$.ajax({
					url: '/datasets/update_volunteers_text/',
					type: 'POST',
					headers: { 'X-CSRFToken': csrfToken },
					data: {
							'volunteers_text': volunteerText,
							'dataset_id': context_dsid,
					},
					success: function(response) {
							alert('Volunteer text saved successfully');
					}
			});
	});
	// wants volumnteers y/n
	// $('#volunteers').change(function() {
	// 	var isChecked = $(this).is(':checked');
	// 	console.log('volunteers?', isChecked)
	// 	$.ajax({
	// 		url: '/datasets/toggle_volunteers',
	// 		type: 'POST',
	// 		headers: { 'X-CSRFToken': csrfToken },
	// 		data: {
	// 			'is_checked': isChecked,
	// 			'dataset_id': context_dsid,
	// 		},
	// 		success: function(response) {
	// 			if (isChecked) {
	// 				alert('Dataset is now listed as a volunteer opportunity');
	// 			} else {
	// 				alert('Dataset is no longer listed as a volunteer opportunity');
	// 			}
	// 		}
	// 	});
	// });

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
	$(".edit-web_item").click(function() {
		$(".editing-web_item").toggleClass("hidden")
		$(".form-web_item").toggleClass("hidden")
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

})

	// post-download actions
	// function customResult(resultElement, result) {
	// 	console.log('celery result', result)
	// 	console.log('celery resultElement', resultElement)
	// 	spinner_dl.stop()
	// 	fn = result.filename
	// 	link = '[ <span class="dl-save"><a href="/' + fn + '" title="downloaded: ' + dater() +
	// 		'" download>save</a></span> ]'
	// 	$(resultElement).append(
	// 		$('<p>').html(link)
	// 	);
	// 	$(".dl-save a")[0].click()
	// 	setTimeout(clearEl($("#celery-result")), 1000)
	// }
	
	// function startUpdateSpinner() {
	// 	window.spinner_update = new Spin.Spinner().spin();
	// 	$("#update_spinner").append(spinner_update.el);
	// }
	//
	// function startDownloadSpinner() {
	// 	window.spinner_dl = new Spin.Spinner().spin();
	// 	$("#ds_downloads").append(spinner_dl.el);
	// }
	
	// parse & prettify ds_update() results
	// [status,format,update_count,redo_count,new_count,deleted_count,newfile]
	// function updateText(data) {
	// 	html = 'Changes in database: <br/>' + '<ul>' +
	// 		'<li>Added ' + data["new_count"] + ' rows </li>' +
	// 		'<li>Deleted ' + data["deleted_count"] + ' rows </li>' +
	// 		'<li>Updated ' + data["update_count"] + ' rows </li>' + '</ul>'
	// 	html += 'A followup reconciliation to Wikidata task is required.'
	//
	// 	if (data['indexed'] == true) {
	// 		html += ', as well as a reindexing, in order to account for these changes.'
	// 	}
	// 	return html
	// }
