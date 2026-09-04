// builders-collection-dataset.js

import './enlarge.js';
import '../css/builders-collection-dataset.css';
import wireCollectionLicense from './collectionLicenseControl.js';

$(function() {
    wireCollectionLicense();

	let dslist = [] // holds ids of selected datasets
	
	$(".textarea").each(function(index) {
		if (["None", "null"].includes($(this).val())) {
			$(this).val('')
		}
	});

	$("#id_geojson").attr("placeholder", "generated from country codes");
	
	$('#image_selector .thumbnail').enlarge();
	
	// ******************* LISTENERS *******************
	
	$("#select_ds").change(function() {
		// show add button if ds selected
		if ($(this).val() != 0) {
			$("#a_add").removeClass('hidden')
		} else {
			$("#a_add").addClass('hidden')
		}
	})

	// remove collaborator from collection
	$(document).on('click', '#remove_collab', function(event) {
		event.preventDefault();
		console.log('remove collab')
		var uid = $(this).data('uid');
		var cid = context_objectId;

		$.ajax({
			url: '/collections/collab-remove/' + uid + '/' + cid + '/',
			type: 'POST',
			headers: { 'X-CSRFToken': csrfToken },
			success: function(response) {
				console.log('response', response)
				$('#remove_collab[data-uid="' + response.uid + '"]').parent().parent().remove();
				// Instead of setting window.location.hash directly, use history.replaceState()
				history.replaceState(null, null, '#coll_collaborators');
				$('.nav-link[data-bs-target="#coll_collaborators"]').tab('show');

			},
			error: function(response) {
				// Handle error
				console.log('error', response)
			}
		});
	});

	$('#sharing_form').submit(function(event) {
		// Stop form from submitting normally
		event.preventDefault();
		console.log('sharing form submitted via ajax')

		var formData = new FormData(this);

		$.ajax({
			url: $(this).attr('action'),
			type: $(this).attr('method'),
			data: formData,
			processData: false,
			contentType: false,
			csrfmiddlewaretoken: csrfToken,
			success: function(response) {
				console.log('response', response)
				if (response.status == 'ok') {
					var removalMarkup = '<span class="float-end me-2"><a id="remove_collab" data-uid="' +
						response.uid + '" href="/collections/collab-remove/' + response.uid +
						'/' + response.cid + '/"><i class="fas fa-times-circle linky"></i></a></span>';
					$("#collabs_list").append(
						'<li>' + response.user + removalMarkup + '</li>')
					history.replaceState(null, null, '#coll_collaborators');
					$('.nav-link[data-bs-target="#coll_collaborators"]').tab('show');
					$('input[name="username"]').val('');
				} else {
					alert(response.status)
				}
			},
			error: function(response) {
				alert('a problem occurred')
			}
		});
	});
	
	
	$("#a_linkpopup").click(function() {
		$("#linkform_popup").fadeIn()
	})
	$(".closer").click(function() {
		$(".pop").fadeOut()
	})
	$("#b_createlink").click(function() {
		create_collection_link()
	})

	$("#btn_coll_submit").click(function(e) {
		// at least two datasets selected?
		if (context_action == 'create' && dslist.length < 2) {
			e.preventDefault()
			$("#msg").html('<h6>Collections must contain at least 2 datasets!</h6>').addClass('strong-red').show()
		}
	})
	
	$('.show-chooser').click(function(event) {
        event.preventDefault();
		$(this).parent().find('.chooser').show();
	});
	
    $('#dataset_adder').click(function(event) {
        event.preventDefault();
        addDataset();
    });
    
    $('#year_filter').change(function() {
        var isChecked = $(this).prop('checked');
        $.ajax({
            url: '/collections/update_vis_parameters/',
			headers: { 'X-CSRFToken': csrfToken },
            method: 'POST',
            data: {
                'checked': isChecked,
                'coll_id': context_objectId
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
    
	$('a.confirm-remove').click(function(event) {
	    event.preventDefault();
	    confirmRemove($(this).data('dataset-id'), context_objectId, $(this));
	});    
	
	// ******************* FUNCTIONS *******************

	function create_collection_link() {
		var formData = new FormData()
		formData.append('model', 'Collection')
		formData.append('objectid', context_objectId)
		formData.append('uri', $("#l_uri").val())
		formData.append('label', $("#l_label").val())
		formData.append('link_type', $("#select_linktype").val())
		formData.append('license', '')
		$.ajax({
			type: 'POST',
			enctype: 'multipart/form-data',
			url: '{% url "create-link" %}',
			headers: { 'X-CSRFToken': csrfToken },
			processData: false,
			contentType: false,
			cache: false,
			data: formData,
			success: function(response) {
				if (response.result == 'dupe') {
					alert('That url is already linked to this collection!')
				} else if (response.result == 'bad uri') {
					alert('That url is not formed correctly!')
				} else {
					let linky = response.result
					console.log(linky)
					$("#linklist").append(linky.link_icon + ' <a href="' + linky.uri +
						'" target="_blank">' + linky.label + '</a>')
					$("#added_flash").fadeIn().delay(2000).fadeOut()
					if ($("#linklist svg").length >= 2) {
						$(".link-counter").html("(that's 3!)")
					}
				}
			}
		})
		$("#linkform_popup input").val('')
		$("#linkform_popup").hide()
	}
	
	function listDataset(d) {
		// add to coll_dscards
		dslist.push(d.id)
		$("#id_datasets [value=" + d.id + "]").attr("checked", "checked");
		var card = '<div class="ds_card" id="card_' + d.id + '">' +
			'<p class="mb-0"><a href="#"><span class="ds_title">' + d.title + '</span></a> (' + d.label + '/' + d.id + ')' +
			'<span id="ds_sequence" class="float-end"></span></p>' +
			'<div class="ds_fields">' +
			'<p class="my-1"><b>Description</b>: ' + d.description + '</p>' +
			'<p class="my-1"><b>Create date</b>: ' + d.create_date + '</p>' +
			'<p class="my-1"><b># rows</b>: ' + d.numrows +
			'<a href="javascript:{ removeDataset(' + d.id + ') }" class="float-end">' +
			'<i class="fas fa-minus-square" style="color:#336699"></i> remove</a>' +
			'</p></div></div>'
		$("#coll_dscards").append(card)
	}
	
	// initiates the add dataset step
	function addDataset() {
		let selectedOption = $("#select_ds option:selected");
		let dsStatus = selectedOption.data('status');
		let dsTitle = selectedOption.data('title');
		console.log('addDataset() selectedOption', selectedOption)
		console.log('addDataset() dsStatus', dsStatus)
		console.log('addDataset() dsTitle', dsTitle)
		// Generate the modal content based on the dataset status
		generateModalContent(dsStatus, dsTitle);
	
		// Show the modal
		$('#modal').modal('show');
	}
	
	function generateModalContent(dsStatus, dsTitle) {
		// Clear the modal content
		$('.modal-body').empty();
	
		// Generate the modal content based on the dataset status
		if (dsStatus != 'indexed') {
			$('.modal-body').append('<p>The status of ' + dsTitle + ' is currently "' + dsStatus + '". It must be indexed before adding to a collection</p>');
			$('.modal-body').append('<button type="button" class="btn btn-secondary" ' +
				'data-bs-dismiss="modal">Cancel</button>');
		} else {
			$('.modal-body').append('<p>Dataset is indexed, OK to proceed!</p>');
			$('.modal-body').append('<button type="button" class="btn btn-secondary" ' +
				'data-bs-dismiss="modal">Cancel</button>');
			$('.modal-body').append('<button id="b_continue" type="button" class="btn btn-success ms-2">Continue</button>');
		}
	
		// Add event listener to the 'continue' button
		// fetch and display card info in dslist[]
		// TODO: this should index the dataset to "builder"
		$('#b_continue').click(function() {
			let selectedOption = $("#select_ds option:selected");
			// start a spinner
			$.ajax({
				url: `/collections/add_ds/${context_objectId}/${selectedOption.val()}`,
				success: function(result) {
					if (result.status === 'success') {
						listDataset(result.dataset);
					} else if (result.status === 'already_added') {
						alert('Dataset already in the collection');
					}
				},
				error: function(xhr, status, error) {
					console.error('Error in addDataset:', error);
				}
			});
	
			// Reset select and hide message
			$("#select_ds").val(0);
			$("#msg").html('').hide();
	
			// Hide the modal
			$('#modal').modal('hide');
		});
	}
	
	function confirmRemove(dsId, collId, element) {
		console.log('dsId, collId', dsId, collId)
	
		// Get the dataset list and the index of the dataset to remove
		var datasetList = $("#coll_dscards").children().toArray();
		var datasetElement = $("#coll_dscards").find(`[data-id='${dsId}']`);
		var datasetIndex = datasetList.indexOf(datasetElement[0]);
	
	
		// Check the state of the dataset
		var hasAlignCollectionTask = datasetElement.data('has_task');
		var remainingToReview = datasetElement.data('remaining');
	
		// Determine the confirmation message based on the state of the dataset
		var confirmMessage;
		if (datasetIndex === 0 && datasetList.length > 1) {
			confirmMessage = "You can't remove the seed dataset for a collection once others are added.";
		} else if (hasAlignCollectionTask) {
			confirmMessage = "Reconciliation of this dataset to the collection is under way, these are the consequences of removing it: blah blah";
		} else if (remainingToReview === 0) {
			confirmMessage = "Reconciliation of this dataset to the collection is completed, these are the consequences of removing it: blah blah";
		} else {
			confirmMessage = "Are you sure you want to remove this dataset?";
		}
	
		// Show the confirmation dialog
		if (confirm(confirmMessage)) {
			var url = `/collections/remove_ds/${collId}/${dsId}`;
			window.location.href = url;
		} else {
			console.log("Removal canceled.");
		}
	}
	
	function removeDataset(dsid) {
		if (context_action == 'update') {
			alert(`removing ${dsid} from collection ${context_objectId}`)
			$.ajax(`/collections/remove_ds/${context_objectId}/${dsid}`,
				function(result) {
					console.log('removeDataset() result', result)
				});
		} else {
			// TODO: why would there be an else: here?
			$(`#id_datasets [value=${dsid}]`).prop("checked", false);
			let idx = dslist.indexOf(dsid)
			dslist.splice(idx, dslist.length);
			$(`#card_${dsid}`).remove()
			if (dslist.length == 0) {
				$("#msg").html('None yet...').show()
			}
		}
		// reset select
		$("#select_ds").val(0)
	}    

})
