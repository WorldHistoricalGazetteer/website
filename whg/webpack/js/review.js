// /whg/webpack/js/review.js

import { whg_map, layersets, initialiseMap, addReviewListeners } from './review-common';
import './notes.js';

import '../css/review.css';


function waitMapLoad() {
    return new Promise((resolve) => {
        if (whg_map) {
            whg_map.on('load', () => {
                initialiseMap();
                resolve();
            });
        } else {
            resolve();
        }
    });
}

function waitDocumentReady() {
    return new Promise((resolve) => {
        $(document).ready(() => resolve());
    });
}

function toggleGeonamesReview() {
	$('#geonames_review').toggle();
}

Promise.all([waitMapLoad(), waitDocumentReady()])
    .then(() => {

		// pass number dropdown set on load and listener
		$( "#select_pass" ).val(passnum)
		// pass number listener
		$( "#select_pass" ).change(function() {
            $('body').spin();
			let z=window.location.href
			let baseurl=z.substring(0,z.lastIndexOf('/')+1)
			window.location.href = baseurl + $(this).val()
		});

    if ($('.wdlocal').length > 0) {
			// hide geonmaes hits (if any) and show toggle link
			if($('.gn').length > 0) {
				$('.gn').hide();
				$('.show-link').show();
			}
    } else {
			// geonames (.gn) only
			$('.gn').show();
			// hide geonames toggle link
			$('.show-link').hide();
    }

    $('#toggle_geonames_review').on('click', function(e) {
        e.preventDefault(); // prevent the default action

        // toggle visibility of .auth-match and adjacent .matchbar elements associated with 'gn'
        $('.auth-match').each(function() {
            if ($(this).find('a[data-auth="gn"]').length > 0) {
                $(this).prev('.matchbar').toggle();
                $(this).toggle();
            }
        });
        
        // toggle visibility of geonames map markers
        if (!!layersets['geonames']) layersets['geonames'].toggleVisibility();

        // change the text of the link
        $(this).text(function(i, text){
            return text === "Show GeoNames hits" ? "Hide GeoNames hits" : "Show GeoNames hits";
        })
    });

	if (already !==false) {
		alert('last record was saved by someone else, this is the next')
	}

	if (page_variant == 'accession') {
	
		let placeid = $('script').filter(function() { // TODO: Redundant code?
			return this.id.match("placeid")
		}).text();
		
		if (test == "on") {
			$("#review_nav").css("background-color", "lightsalmon");
			$("input:radio").attr("disabled", "disabled");
			$("#btn_save").hide();
			$("#defer_link").hide();
		}

		$(".pop-dataset").popover({
		    content: function () {
		        var label = $(this).data('label');
		        var details = datasetDetails[label];
		        return `
		            <p class="thin"><b>Title</b>: ${details['title']}</p>
		            <p class="thin"><b>Description</b>: ${details['description']}</p>
		            <p class="thin"><b>WHG Owner</b>: ${details['owner']}</p>
		            <p class="thin"><b>Creator</b>: ${details['creator']}</p>
		        `;
		    }
		});
		
	}
	
	if (whg_map) addReviewListeners();

    $('#btn_save').on('click', function() {
        $('body').spin();
    });

    $('#form_related a[href]').not('[data-whg-modal]').not('.ext').not('.ext-recon').on('click', function() {
        // Prevent spinner if the link is handled by AJAX/other script (like geolink) or is a JS void
        let href = $(this).attr('href');
        if (href && href !== 'javascript:void(0)') {
            $('body').spin();
        }
    });

    // Special listener for the form submission itself, in case Save is triggered by pressing Enter
    $('#form_related').on('submit', function() {
        $('body').spin();
    });
	
})
.catch(error => console.error("An error occurred:", error));
