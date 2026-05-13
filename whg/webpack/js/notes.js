// Create a Bootstrap form dialog for submitting a comment
//
// Top of JavaScript: import './notes.js';
// Initialise with: $('.notes').notes();

$.fn.notes = function() {
	
	const tagVocabulary = [
		['geom','Geometry Error'],
		['mismatch','Misplaced in Set'],
		['missing','Missing Record'],
		['typo','Typographical Error'],
		['other','Other Error'], // The last item is selected by default
	];
	
	const vocabularyTags = `
		  <label for="issue">Issue:</label>
          <ul class="no-bullet">
          	${
				tagVocabulary.map(([value, label], index) => {
				    const checked = index === tagVocabulary.length - 1 ? 'checked' : ''; // Select the last item
				    return `<li><input type="radio" name="tag" value="${value}" class="no-bullet" required id="note_tag_${value}" ${checked}><label for="note_tag_${value}">${label}</label></li>`;
				}).join('')
			}
          </ul>
        `;
        
    const trashHTML = ' <i class="fas fa-trash linky fa-xs" data-bs-toggle="tooltip" data-bs-title="Delete this note"></i>';
	
	$('body').on('submit', '#commentForm', function(event) {
		event.preventDefault();
		var formData = $(this).serialize();
		$.ajax({
			url: '/comment/',
			method: 'POST',
			headers: { 'X-CSRFToken': csrfToken },
			data: formData,
			success: function(response) {
				$('#commentModal').modal('hide');
				console.log('Comment submitted successfully.');
				$(`.notes[data-place-id="${response.comment.place_id}"]`)
				.find('.record-notes')
				.append(`<p data-bs-title="${response.comment.tag}" data-bs-toggle="tooltip" data-creator="${response.comment.user}">${response.comment.note}${trashHTML}</p>`)
				.end()
				.addClass('has-notes');
			},
			error: function(xhr, status, error) {
				alert('Sorry, an error occurred while submitting the comment.');
			}
		});
	});

    $('body').on('click', '#commentModal button[data-dismiss="modal"]', function() {
        $('#commentModal').modal('hide');
    });

    $('body').on('hidden.bs.modal', '#commentModal', function (e) {
        $(this).remove();
    });

    $('body').on('click', 'i.fa-trash', function (e) {
        e.stopPropagation();
        const confirmed = window.confirm('Are you sure you want to delete this comment?');
    
    	if (confirmed) {
	    	const noteId = $(this).closest('p').data('note-id');
	    	const trashIcon = $(this);
	    	$.ajax({
		        url: '/comment/',
		        method: 'POST',
		        headers: { 'X-CSRFToken': csrfToken },
		        data: {
		            deleteId: noteId,
		        },
		        success: function(response) {
		            console.log('Comment deleted successfully.');
		            const notes = trashIcon.closest('.notes');
		            notes.toggleClass('has-notes', notes.find('.record-notes p').length > 1);
		            trashIcon.tooltip('dispose');
		            trashIcon.closest('p').tooltip('dispose').remove();
		        },
		        error: function(xhr, status, error) {
		            alert('Sorry, an error occurred while attempting to delete the comment.');
		        }
		    });
		}
    });
	
	// Inject required CSS styles into the head of the document if not done already
	if (!$('style#comment-styles').length) {
	    const commentStyles = `
	    	.notes {
    			background-color: oldlace;
    			padding: 0.3rem;
			    border: 1px solid grey;
			    border-radius: 0.3rem;
    			margin: 0 0 0.3rem 0.3rem;
			    max-width: 50%;
			    max-height: 250px;
    			font-size: .8rem;
    			overflow-y: auto;
			}
			
			.notes:not(.has-notes) .notes-list {
				display: none;
			}
			
			.notes.logged-in p.notes-toggler span:nth-child(3) {
				display: none;
			}
			
			.notes .add-note {
			    cursor: pointer;
    			display: contents;
    			user-select: none;		
			}
			
			.notes .notes-toggler {
    			font-size: 0.7rem !important;
			    cursor: pointer;
    			display: contents;	
    			user-select: none;		
			}
			
			.notes .notes-list.hiding p.notes-toggler span:nth-child(2),
			.notes .notes-list.hiding div.record-notes
			{
				display: none;
			}
			
			.notes .notes-list:not(.hiding) p.notes-toggler span:first-of-type {
				display: none;
			}
			
			.notes .record-notes p {
			    background-color: white;
			    padding: 0.1rem 0.2rem;
			    border: 1px solid grey;
			    border-radius: 0.3rem;
    			margin: 0.1rem 0;		
			}
			
			.notes .record-notes i.fa-trash {
			    color: red;
			    float: right;
			    top: 0.6rem;
			    position: relative;
			    margin-right: 0.1rem;
			    cursor: pointer;
			}
			
			#commentModal {
			    font-family: Arial, sans-serif;
    			user-select: none;	
			}
			
			#commentModal .modal-content {
    			height: fit-content;
			    background-color: #f8f9fa;
			    border: none;
			}
			
			#commentModal .modal-header {
			    background-color: #007bff;
			    color: #fff;
			}
			
			#commentModal .modal-header .close {
			    padding: 0;
			    margin: 0;
			    background: none;
			    border: none;
			    font-size: 2rem;
			    color: white;
			    opacity: 0.5;
			}
			
			#commentModal .modal-header .close:hover {
			    opacity: 1;
			}			
			
			#commentModal .modal-title {
			    font-weight: bold;
			}
			
			#commentModal .modal-body {
			    padding: 20px;
			}
			
			#commentModal textarea {
			    width: 100%;
			    height: 100px;
			    resize: vertical;
			}
			
			#commentModal .modal-footer {
			    justify-content: flex-end;
			}
			
			#commentModal .modal-footer .btn {
			    height: 38px;
			}
			
			#commentModal .btn-primary {
			    background-color: #007bff;
			    color: #fff;
			    border: none;
			}
			
			#commentModal .btn-primary:hover {
			    background-color: #0056b3;
			}
			
			#commentModal input[name="tag"] {
			    vertical-align: middle;
			    margin-right: 0.3rem;
			    margin-top: 2px;
			}
	    `;
	
	    $('<style>')
	        .prop('type', 'text/css')
	        .prop('id', 'comment-styles')
	        .html(commentStyles)
	        .appendTo('head');
	}
		
    return this.each(function() {
	
		const placeId = $(this).data('place-id');
		const userId = ($(this).data('user-id') || '');
		const forceTag = ($(this).data('force-tag') || '').trim();
		const addText = $(this).data('add-text') || 'add note';
		const hasNotes = $(this).find('p').length > 0;
		
		const chosenTags = forceTag == '' ? vocabularyTags : `<input type="hidden" name="tag" value="${forceTag}">`;
		const deferNotice = forceTag === 'defer'
			? `<div class="alert alert-warning small py-2 mb-2" role="alert">These notes are private to you and are intended as a scratch pad while you reconcile this dataset. They will be cleared when the dataset is published, and are not included in dataset downloads.</div>`
			: '';
		
		if (!userId && !hasNotes) {
			$(this).remove();
			return;
		}
		
		$(this)
		.addClass('float-end')
		.toggleClass('has-notes', hasNotes)
		.toggleClass('logged-in', userId !== '')
		.wrapInner('<div class="record-notes"></div>')
		.find('.record-notes')
		.wrap('<div class="notes-list hiding"></div>')
		.before('<p class="notes-toggler"><span>show</span><span>hide</span> notes<span> <i class="fas fa-edit linky fa-xs"></i></span></p>')
		.end()
	    .on('click', '.notes-toggler', function() {
			$(this).parent().toggleClass('hiding');
		})
		.find(`.record-notes p[data-creator="${userId}"]`)
		.append(trashHTML)
		.end()
		.on('show.bs.tooltip', '.record-notes p', (e) => { // Prevent overlapping tooltips
			bootstrap.Tooltip.getInstance($(e.target).closest('.source-box')).hide();
		})
		.on('show.bs.tooltip', 'i.fa-trash', (e) => { // Prevent overlapping tooltips
			bootstrap.Tooltip.getInstance($(e.target).closest('p')).hide();
		})
		.filter('.logged-in')
		.prepend(`<p class="add-note" data-place-id="${placeId}" data-text="add note">${addText}${addText == '' ? '' : ' '}<i class="fas fa-edit linky fa-xs"></i></p>`)
		.on('click', '.add-note', function() {
			console.log('modalHtml', placeId);
	        var modalHtml = `
	        <div class="modal fade" id="commentModal" tabindex="-1" role="dialog" aria-labelledby="commentModalLabel" aria-hidden="true">
	          <div class="modal-dialog" role="document">
	            <div class="modal-content">
	              <div class="modal-header">
	                <h5 class="modal-title" id="commentModalLabel">New Comment: Place #${placeId}</h5>
	                <button type="button" class="close" data-dismiss="modal" aria-label="Close">
	                  <span aria-hidden="true">&times;</span>
	                </button>
	              </div>
	              <div class="modal-body">
	                ${deferNotice}
	                <form id="commentForm">
	                  ${chosenTags}
	                  <label for="commentText">Note:</label>
	                  <textarea id="commentText" name="commentText" rows="4" required></textarea>
	                  <input type="hidden" id="placeId" name="placeId" value="${placeId}">
	                </form>
	              </div>
	              <div class="modal-footer">
	                <button type="button" class="btn btn-secondary" data-dismiss="modal">Cancel</button>
	                <button type="submit" form="commentForm" class="btn btn-primary">Submit</button>
	              </div>
	            </div>
	          </div>
	        </div>
	        `;
	
			$('body').append(modalHtml);
			$('#commentModal').modal('show');
	    });
		
	});	
};