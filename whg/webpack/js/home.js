// /whg/webpack/home.js

import '../css/home.css';
import { errorModal } from './error-modal.js';
import { initialiseCarousels } from './carousels';
import {initSimpleTypeahead} from "./utilities";

let whg_map = new whg_maplibre.Map({
    style: ['whg-basic-light'],
	navigationControl: false,
	maxZoom: 10,
});

//let idToArea = {};
let area_list = [];
let area_objs = [];

function waitMapLoad() {
	return new Promise((resolve) => {
		// `load` fires ONCE. Attach after it has already fired — which happens whenever
		// the style comes from cache — and this promise never settles, so the carousels
		// are never built and the home page silently shows an empty globe. Seen on dev
		// with a warm cache; the race is timing-dependent, so prod has been getting away
		// with it rather than being immune. See place#177.
		const onMapReady = () => {
			console.log('Map loaded.');

			const style = whg_map.getStyle();
			style.layers.forEach(layer => { // Hide map labels
				if (layer.id.includes('label')) {
					whg_map.setLayoutProperty(layer.id, 'visibility', 'none');
				}
			});
			
			$('.maplibregl-control-container')
			.wrap('<div id="controlWrapper"></div>')
			.after($('#carousel-outer-container'));

			resolve();
		};
		if (whg_map.loaded()) onMapReady(); else whg_map.on('load', onMapReady);
	});
}

function waitDocumentReady() {
	return new Promise((resolve) => {
		$(document).ready(() => resolve());
	});
}

Promise.all([
	waitMapLoad(),
	waitDocumentReady()
]).then(() => {

	initialiseCarousels(galleries, carouselMetadata, startCarousels, whg_map);

	// v3 welcome modal
	if (!localStorage.getItem('dontShowSplash')) {
		$('#welcomeModal').modal('show');
	}

	// Handle the "Don't show again" checkbox
	$('#dontShowAgain').on('change', function() {
		if ($(this).is(':checked')) {
		localStorage.setItem('dontShowSplash', 'true');
	} else {
		localStorage.removeItem('dontShowSplash');
	}
	});


	// always clear last_search (if set)
	localStorage.removeItem('last_search');

    initSimpleTypeahead('#search_input');

	// listen for search action
	$('#initiate_search').click(function() {
		initiateSearchHome();
	});

	$('#search_input').on('keyup', function(event) {
		if (event.which == 13) { // 13 is the 'Enter' key code
			event.preventDefault();
			initiateSearchHome(); // Call the initiateSearch function when 'Enter' is pressed
		}
	});

	$('#a_advanced').on('click', function(e) {
		e.preventDefault();
		clearTimeout(timer); // Stop the carousels
		if (startCarousels) $('.carousel').first().carousel('pause');
		// $('#advanced_search').slideToggle(300); // This toggles the visibility of the advanced search div
	});

	$.get('/api/area_list', function(data) {
		//console.log('initial areas for list',data)
		for (var i = 0; i < data.length; i++) {
			area_objs.push(data[i]);
			area_list.push(data[i]['title']);
		}
	});
	$('#input_area').autocomplete({
		source: area_list,
		minLength: 2,
		select: function(event, ui) {
			// build boundsobj, insert as hidden input val()
			let label = ui.item.label;
			let obj = area_objs.find(o => o.title == label);
			let areaid = obj.id;
			let boundsobj = '{"type":["' + obj.type + '"],"id":["' + areaid + '"]}';
			$('#boundsobj').val(boundsobj);
			console.log('boundsobj for view ', boundsobj);

			// fetch and render selected area
			render_area(areaid);
		},
	});
	
	let render_area = function(areaid) {
		$.ajax({
			url: '/api/area/' + areaid,
		}).done(function(data) {
			console.log('render_area() data', data);
			let geom = {
				'type': 'FeatureCollecton',
				'features': [],
			};
			geom['features'].push(data.geojson);
			console.log('render_area() geom', geom);

			// clear existing
			if (typeof arealayer !== 'undefined') {
				arealayer.removeFrom(whg_map);
			}
		});
	};

	// call views.search_new() with parameters
	// search_new() will pass these to search_new.html
	function initiateSearchHome() {
		const filters = {
			'qstr': $('#search_input').val(),
			'mode': 'exactly',
			'temporal': false,
			'undated': true,
			'idx': eswhg, // hard-coded in html template
		};

		// AJAX POST request to SearchView() with the options (includes qstr)
		$.ajax({
			type: 'POST',
			url: '/search/index/',
			data: JSON.stringify(filters),
			contentType: 'application/json',
			headers: {'X-CSRFToken': csrfToken}, // Include CSRF token in headers for Django POST requests
			success: function(data) {
				console.log('...search completed.', data);
                try {
                    localStorage.setItem('last_search', JSON.stringify(data));
                } catch (error) {
                    if (error.name === 'QuotaExceededError') {
                        console.error('LocalStorage quota exceeded. Clearing old searches.');
                        // Clear old dataset/collection data
                        for (let i = localStorage.length - 1; i >= 0; i--) {
                            let key = localStorage.key(i);
                            if (key && (key.startsWith('dataset') || key.startsWith('collection'))) {
                                localStorage.removeItem(key);
                            }
                        }
                        // Try again
                        try {
                            localStorage.setItem('last_search', JSON.stringify(data));
                        } catch (e) {
                            console.error('Failed to save search even after clearing:', e);
                        }
                    }
                }
				window.location.href = '/search';				
			},
			error: function(error) {
				console.error('Error:', error);
				errorModal('Sorry, something went wrong with that search.', null,
						error);
			},
		});
	}

	// toggle all fclass checkboxes
	$('#check_toggle').click(function(e) {
		e.preventDefault();
		var state = $('#check_toggle').text();
		if (state == 'check all') {
			$('input:checkbox').prop('checked', true);
			$('#check_toggle').text('clear all');
		} else if (state == 'clear all') {
			$('input:checkbox').prop('checked', false);
			$('#check_toggle').text('check all');
		}
	});

	// modal for images
	$('.pop').on('click', function() {
		let url = $(this).find('img').attr('src');
		let txt = $(this).find('img').attr('alt');
		let re = /(.png|.jpg|.jpeg|.gif|.tif)/g;
		let ext = url.match(re)[0];
		url = url.replace(ext, '_full' + ext);
		$('#header_text').html(txt);
		$('.imagepreview').attr('src', url);
		$('#image_modal').modal('show');
	});

	// // announcements (w/fade)
	// let currentIndex = 0;
	// const announcements = $('#announcement-container .announcement');
	// const totalAnnouncements = announcements.length;
	//
	// function rotateAnnouncements() {
	// 	const currentAnnouncement = $(announcements[currentIndex]);
	// 	const nextIndex = (currentIndex + 1) % totalAnnouncements;
	// 	const nextAnnouncement = $(announcements[nextIndex]);
	//
	// 	// Fade out the current announcement and fade in the next
	// 	currentAnnouncement.fadeOut(1000, function() {
	// 		nextAnnouncement.fadeIn(1000);
	// 	});
	//
	// 	currentIndex = nextIndex;
	// }
	//
	// // Start rotating announcements every 3 seconds, plus the fade time to ensure one fades out before the next fades in
	// setInterval(rotateAnnouncements, 5000); // Adjust time as needed

});