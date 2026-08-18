// /whg/webpack/js/carousels.js

import { fetchDataForHorse } from './carousel-mapdata';

// Motion preference for the home-page carousels (place#177). Continuous automatic
// movement is not a cosmetic detail: a beta tester reported it causing motion
// sickness after a while, and there was no way to stop it short of leaving the page.
//
// The choice is REMEMBERED. Someone who finds the movement unpleasant finds it
// unpleasant on every visit, and making them hunt for the button each time would
// answer the complaint in form only. And an unset preference follows the OS: a
// visitor who has asked their system to reduce motion has already told us.
const CAROUSEL_PAUSE_KEY = 'whg-carousel-paused';

function prefersReducedMotion() {
	try { return window.matchMedia('(prefers-reduced-motion: reduce)').matches; } catch (e) { return false; }
}
function carouselPaused() {
	try {
		const stored = localStorage.getItem(CAROUSEL_PAUSE_KEY);
		if (stored !== null) return stored === '1';
	} catch (e) { /* private mode — fall through to the OS preference */ }
	return prefersReducedMotion();
}
function storeCarouselPaused(paused) {
	try { localStorage.setItem(CAROUSEL_PAUSE_KEY, paused ? '1' : '0'); } catch (e) { /* ignore */ }
}

export function initialiseCarousels(galleries, carouselMetadata, startCarousels, whg_map) {

	var timer;
	let paused = carouselPaused();
	const v3 = galleries.length == 1;
	
	galleries.forEach(gallery => {
	    const [title, url] = gallery;
	    const type = v3 ? 'datasets' : title.toLowerCase();
	    const carouselContainer = $(
	        `<div class="carousel-container ${v3 ? 'mx-0 mb-2 mb-lg-0 h-40' : 'p-1'} home-carousel"></div>`);
	    const border = $('<div class="border p-1 h-100 d-flex flex-column"></div>'); // Added flex-column class
	    const heading = $(`<p class="p-1">${title}</p>`);
	    if (type == 'datasets') {
	        heading.addClass('ds-header');
	    } else {
	        heading.addClass('coll-header');
	    }
      const galleryLink = url == null ?
          '' : `<button class="btn btn-primary float-end small" 
          style="height: 22px; font-size: 0.8rem; padding: 0 0.5rem; background-color: cornflowerblue;" 
          onclick="location.href='${url}'">BROWSE ALL</button>`;
	    // const galleryLink = url == null ?
	    //     '' :
	    //     `<span class="float-end small"><a class="linky" href="${url}">view all</a></span>`;
	    const carousel = $(
	        `<div id="${type.toLowerCase()}Carousel" class="carousel slide carousel-fade flex-grow-1"></div>`); // Added flex-grow-1 class
	    const carouselInner = $('<div class="carousel-inner"></div>');
	    const prevButton = $(
	        `<button class="carousel-control-prev" type="button" data-bs-target="#${type}Carousel" data-bs-slide="prev">
	                                    <span class="carousel-control-prev-icon" aria-hidden="true"></span>
	                                    <span class="visually-hidden">Previous</span>
	                                </button>`);
	    const nextButton = $(
	        `<button class="carousel-control-next" type="button" data-bs-target="#${type}Carousel" data-bs-slide="next">
	                                    <span class="carousel-control-next-icon" aria-hidden="true"></span>
	                                    <span class="visually-hidden">Next</span>
	                                </button>`);
	    // One control for the whole set — the galleries advance in step, so a pause
	    // per gallery would be a lie. Added to the first heading only.
	    if (gallery === galleries[0]) {
	        heading.append(
	            `<button type="button" id="carousel-pause-toggle" class="btn btn-sm float-end me-1"
	                     style="height: 22px; font-size: 0.8rem; padding: 0 0.5rem;"
	                     aria-pressed="${paused}"
	                     title="${paused ? 'Resume the automatic slideshow' : 'Stop the slideshow moving on its own'}">
	               <i class="fas ${paused ? 'fa-play' : 'fa-pause'}" aria-hidden="true"></i>
	               <span class="visually-hidden">${paused ? 'Play slideshow' : 'Pause slideshow'}</span>
	             </button>`);
	    }
	    heading.append(galleryLink);
	    border.append(heading);
	    carousel.append(carouselInner);
	    carousel.append(prevButton);
	    carousel.append(nextButton);
	    border.append(carousel);
	    carouselContainer.append(border);
	    if (v3) {
	        $('#carousel-outer-container').replaceWith(carouselContainer);
	    } else {
	        $('#carousel-outer-container').append(carouselContainer);
	    }
	
	    // Add CSS to ensure full height of carousel items
	    carouselContainer.find('.carousel-inner').css('height', '100%');
	    carouselContainer.find('.carousel-item').css('height', '100%');
	});

    carouselMetadata.forEach(datacollection => {
        const target = $(`#${v3 ? 'dataset' : datacollection.type}sCarousel .carousel-inner`);
        const carouselItem = $(`<div class="carousel-item${target.children(
            '.carousel-item').length == 0 ? ' active' : ''} p-2"></div>`);
        const description = datacollection.description.length > 130 ?
            datacollection.description.substring(0, 130) + '...' :
            datacollection.description;
	    if (datacollection.image_file) {
	        const carouselImage = $(`<img class="carousel-image" src="${datacollection.image_file}">`);
	        carouselItem.append(carouselImage);
	    }
        carouselItem.append(`
                    <h6>
                        <a href="${datacollection.url}">${datacollection.title}</a>
                    </h6>
                    <p>${description}</p>
                `).data({
            id: datacollection.ds_or_c_id,
            type: datacollection.type,
        });
        target.append(carouselItem);
    });

    var carousels = $('.carousel');
    const carouselCount = carousels.length;
    var currentCarousel = 0;
    let delay = 10000;
    let mouseover = false;
    // `ride: 'carousel'` tells Bootstrap to start cycling the moment it initialises, so
    // pausing immediately afterwards was a race we could not win: a transition already
    // in flight finishes by restoring the cycling it captured before we intervened, and
    // the carousel would set off a moment after a page that opened showing PLAY. When
    // the standing preference is paused we therefore never ask it to ride in the first
    // place. See place#177.
    if (startCarousels) carousels.first().carousel(
        paused
            ? { interval: delay, keyboard: false }
            : { interval: delay, ride: 'carousel', keyboard: false }
    ).on('slide.bs.carousel', function() {
        if (!mouseover && !paused) {
            timer = setTimeout(function() {
				currentCarousel += 1;
				currentCarousel = currentCarousel % carouselCount;
                carousels.eq(currentCarousel).carousel('next');
            }, delay / carouselCount);
        }
    });
	// Initialise all remaining carousels
	carousels.slice(1).carousel({
	    keyboard: false, // Ignore keyboard
	});

    carousels.on('slid.bs.carousel', function() {
        // Bootstrap restarts cycling by itself in two places we do not control: at the
        // end of a slide it believed was cycling, and on mouseleave of the carousel
        // (`_maybeEnableCycle`, because `ride` is set). Neither knows about the user's
        // pause, so re-assert it whenever a slide completes.
        if (paused && startCarousels) { clearTimeout(timer); carousels.first().carousel('pause'); }
        $('.carousel-container .border').removeClass('highlight-carousel');
        fetchDataForHorse($(this).find('.carousel-item.active'), whg_map);
    }).trigger('slid.bs.carousel'); // Load first map
    $('.carousel-container').on('mouseenter', function() {
        if (startCarousels) carousels.first().carousel('pause');
        clearTimeout(timer);
        mouseover = true;
    }).on('mouseleave', function() {
        if (startCarousels && !paused) carousels.first().carousel('cycle');
        mouseover = false;
    });
    $('#carousel-pause-toggle').on('click', function() {
        paused = !paused;
        storeCarouselPaused(paused);
        clearTimeout(timer);
        if (startCarousels) {
            if (paused) {
                carousels.first().carousel('pause');
            } else {
                // Re-initialise WITH `ride` before resuming. A carousel started in the
                // paused state has no ride configured, and Bootstrap's own resume after
                // a hover (`_maybeEnableCycle`) returns early without it — so cycling
                // would have died the first time the pointer crossed the gallery.
                // Dispose + re-init is the public way to change that; poking the
                // instance's private config is not.
                carousels.first().carousel('dispose').carousel({
                    interval: delay,
                    ride: 'carousel',
                    keyboard: false,
                });
                // And move a slide AT ONCE: `cycle()` waits a full interval — ten
                // seconds here — so pressing play looked like nothing had happened,
                // which is how it came to be reported as a broken button.
                carousels.first().carousel('next');
            }
        }
        $(this).attr('aria-pressed', String(paused))
               .attr('title', paused ? 'Resume the automatic slideshow' : 'Stop the slideshow moving on its own')
               .find('i').attr('class', `fas ${paused ? 'fa-play' : 'fa-pause'}`);
        $(this).find('.visually-hidden').text(paused ? 'Play slideshow' : 'Pause slideshow');
    });

    // Cycling restarts on button click unless carousel is paused, even though mouse has not left container
    $('.carousel-control-next').on('click', function() {
        if (startCarousels) carousels.first().carousel('pause');   // stepping by hand never resumes cycling
        $($(this).data('bs-target')).carousel('next');
    });
    $('.carousel-control-prev').on('click', function() {
        if (startCarousels) carousels.first().carousel('pause');
        $($(this).data('bs-target')).carousel('prev');
    });
}
