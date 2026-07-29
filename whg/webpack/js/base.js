// base.js
import * as Sentry from '@sentry/browser';

const {browserTracingIntegration, thirdPartyErrorFilterIntegration, captureConsoleIntegration} = Sentry;
import {Spinner} from './spin.js';
import {initWHGModal} from './whg-modal.js';
import {initBetaDiag, betaInitialScope} from './beta-diag.js';
import {initializeCitationFormatters} from './citationFormatter';
import {initTooltipHygiene} from './tooltipHygiene.js';
import {wireLicenseControl, pickLicense} from './licensePicker.js';
import {base_urls} from './aliases.js';
import '../css/base.css';
import '../../static/css/styles.css'; // /whg/static/css/styles.css
import 'spin.js/spin.css';

window.defaultZoom = 8;
window.csrfToken = document.querySelector('meta[name="csrf-token"]').getAttribute('content');

if ('fonts' in document) {
    const fontFamilies = ['Raleway', 'Archivo Black'];

    const fontPromises = fontFamilies.flatMap(font => [
        document.fonts.load(`normal 1em "${font}"`),
        document.fonts.load(`bold 1em "${font}"`),
        document.fonts.load(`italic 1em "${font}"`),
        document.fonts.load(`italic bold 1em "${font}"`),
    ]);

    Promise.all(fontPromises).then(_ => {
        document.documentElement.classList.add('fonts-loaded');
    });
}

String.prototype.toUpperCaseFirst = function () {
    return this.charAt(0).toUpperCase() + this.slice(1);
};

var CDN_fallbacks = [
    {
        cdnUrl: 'https://cdnjs.cloudflare.com/ajax/libs/bootstrap/5.2.3/css/bootstrap.min.css',
        localUrl: 'bootstrap.min.css',
        position: 'head'
    },
    {
        cdnUrl: 'https://cdnjs.cloudflare.com/ajax/libs/jqueryui/1.13.2/themes/base/jquery-ui.min.css',
        localUrl: 'jquery-ui.min.css',
        position: 'head'
    },
    {
        cdnUrl: 'https://cdnjs.cloudflare.com/ajax/libs/clipboard.js/2.0.11/clipboard.min.js',
        localUrl: 'clipboard.min.js',
        position: 'head'
    },
    {
        cdnUrl: 'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css',
        localUrl: 'all.min.css',
        position: 'head'
    },
    {
        cdnUrl: 'https://cdnjs.cloudflare.com/ajax/libs/d3/4.13.0/d3.min.js',
        localUrl: 'd3.min.js',
        position: 'head'
    },
];

var jquery_fallbacks = [
    {
        cdnUrl: 'https://code.jquery.com/jquery-3.6.3.min.js',
        localUrl: 'jquery.min.js',
        position: 'head'
    },
]

var jquery_dependent_fallbacks = [
    {
        cdnUrl: 'https://code.jquery.com/ui/1.13.2/jquery-ui.min.js',
        localUrl: 'jquery-ui.min.js',
        position: 'head'
    },
    {
        cdnUrl: 'https://cdnjs.cloudflare.com/ajax/libs/jquery-scrollintoview/1.8/jquery.scrollintoview.min.js',
        localUrl: 'scrollintoview.min.js',
        position: 'head'
    },

]

var jquery_ui_dependent_fallbacks = [
    {
        cdnUrl: 'https://cdnjs.cloudflare.com/ajax/libs/bootstrap/5.2.3/js/bootstrap.bundle.min.js',
        localUrl: 'bootstrap.bundle.min.js',
        position: 'body'
    },

]

var maplibre_fallbacks = [
    {
        // PINNED to 5.3.1 (the version in package.json / built against). Do NOT
        // use @latest here: maplibre-gl 6.0.0 (a breaking major) shipped and
        // @latest now 302-redirects to it — unpkg was 503ing that resolution
        // and 6.x would break the 5.x-built app anyway, taking the Atlas map
        // down. Keep this in step with package.json's maplibre-gl version.
        cdnUrl: 'https://unpkg.com/maplibre-gl@5.3.1/dist/maplibre-gl.js',
        localUrl: 'maplibre-gl.js',
        position: 'head',
    },
    {
        cdnUrl: 'https://unpkg.com/maplibre-gl@5.3.1/dist/maplibre-gl.css',
        localUrl: 'maplibre-gl.css',
        position: 'head'
    },
    {
        cdnUrl: 'https://cdnjs.cloudflare.com/ajax/libs/Turf.js/6.5.0/turf.min.js',
        localUrl: 'turf.min.js',
        position: 'head'
    },
];

window.select2_CDN_fallbacks = [
    {
        cdnUrl: 'https://cdnjs.cloudflare.com/ajax/libs/select2/4.0.13/js/select2.full.min.js',
        localUrl: 'select2.full.js',
        position: 'head',
    },
    {
        cdnUrl: 'https://cdnjs.cloudflare.com/ajax/libs/select2/4.0.13/css/select2.min.css',
        localUrl: 'select2.css',
        position: 'head'
    },
];

window.datatables_CDN_fallbacks = [
    {
        cdnUrl: 'https://cdn.datatables.net/v/dt/dt-1.10.24/b-1.7.0/b-colvis-1.7.0/b-html5-1.7.0/cr-1.5.3/fh-3.1.8/sc-2.0.3/sp-1.2.2/sl-1.3.3/datatables.min.js',
        localUrl: 'datatables.min.js',
        position: 'head',
    },
    {
        cdnUrl: 'https://cdn.datatables.net/v/dt/dt-1.10.24/b-1.7.0/b-colvis-1.7.0/b-html5-1.7.0/cr-1.5.3/fh-3.1.8/sc-2.0.3/sp-1.2.2/sl-1.3.3/datatables.min.css',
        localUrl: 'datatables.min.css',
        position: 'head'
    },
];

window.loadResource = function (element) {
    return new Promise(function (resolve, reject) {
        var resource;
        const isCSS = element.cdnUrl.endsWith('.css');
        const parentElement = document[element.position];

        if (isCSS) {
            resource = document.createElement('link');
            resource.type = 'text/css';
            resource.rel = 'stylesheet';
        } else {
            resource = document.createElement('script');
            resource.type = 'text/javascript';
        }

        resource[isCSS ? 'href' : 'src'] = element.cdnUrl;

        if (element.integrity) resource.integrity = element.integrity;
        if (element.crossorigin) resource.crossorigin = element.crossorigin;

        var localResource = resource.cloneNode();
        // Fallbacks live under the served static root (/static/webpack/CDNfallbacks/),
        // NOT a bare /CDNfallbacks/ (which 403s — there's no nginx location for it,
        // so no CDN fallback ever actually loaded). This is what makes the local
        // copy usable when a CDN is unreachable (see the 2026-07-22 maplibre outage).
        localResource[isCSS ? 'href' : 'src'] = `/static/webpack/CDNfallbacks/${element.localUrl}`;

        resource.onload = function () {
            // console.log(`Loaded CDN resource ${element.cdnUrl}`);
            resolve();
        };

        resource.onerror = function () {
            console.log(`Failed to load CDN resource (${element.cdnUrl}), falling back to local: ${element.localUrl}`);

            localResource.onload = function () {
                resolve();
            };
            localResource.onerror = function () {
                resolve();
            };

            parentElement.insertBefore(localResource, resource);
            parentElement.removeChild(resource);

        };

        parentElement.appendChild(resource);
    });
};

if (typeof loadMaplibre !== 'undefined') {
    CDN_fallbacks = [...CDN_fallbacks, ...maplibre_fallbacks];
}

Promise.all([
    Promise.all(CDN_fallbacks.map(loadResource)),
    Promise.all(jquery_fallbacks.map(loadResource)) // Ensure that JQuery is loaded before proceeding with its dependents
        .then(function () {
            return Promise.all(jquery_dependent_fallbacks.map(loadResource));
        })
        .then(function () {
            //  Resolve name collision between jQuery UI and Bootstrap
            $.widget.bridge('uitooltip', $.ui.tooltip);
            return Promise.all(jquery_ui_dependent_fallbacks.map(loadResource));
        })
])
    .then(function () {

        const GLITCHTIP_DSN = document.querySelector('meta[name="glitchtip-dsn"]')?.content;
        const ENV_CONTEXT = document.querySelector('meta[name="env-context"]')?.content;
        const GLITCHTIP_RELEASE = document.querySelector('meta[name="glitchtip-release"]')?.content;
        const USER_ID = document.querySelector('meta[name="user-id"]')?.content;

        if (GLITCHTIP_DSN) {
            // Seed the beta tags (beta_session/user_role) at init so even errors thrown during the rest
            // of page setup are attributable to the tester's session, not just those after initBetaDiag.
            const betaScope = betaInitialScope();
            Sentry.init({
                dsn: GLITCHTIP_DSN,
                integrations: [
                    Sentry.browserTracingIntegration(),
                    Sentry.thirdPartyErrorFilterIntegration(),
                    Sentry.captureConsoleIntegration({
                        levels: ['error']  // Could include 'warn' for more verbose reporting
                    }),
                ],
                tracesSampleRate: 0.01,
                environment: ENV_CONTEXT,
                release: GLITCHTIP_RELEASE,
                initialScope: betaScope || undefined,
                ignoreErrors: [
                    // 'MapLibre runtime error: [object Object]', // Exact string
                    // /NetworkError/,                        // Regex
                    // /^Specific Error Message$/i            // Case-insensitive regex
                    /^MapLibre runtime error/,
                ],
            });

            if (USER_ID) {
                Sentry.setUser({
                    id: USER_ID
                });
            }
        }

        window.Sentry = Sentry;

        // Beta-tester diagnostics (plan-beta-diagnostics): correlate client+server errors + snags via a
        // per-session id, enrich GlitchTip, prefill the snag report. No-op for non-beta users.
        try { initBetaDiag(Sentry); } catch (e) { /* diagnostics must never break the page */ }

        // Set Bootstrap tooltip defaults
        $.extend(true, $.fn.tooltip.Constructor.Default, {
            selector: '[data-bs-toggle="tooltip"]:not([disabled]), [rel="tooltip"]:not([disabled])',
            html: true,
            trigger: 'hover',
            title: function () {
                const titleAttribute = this.getAttribute('title');
                const dataBsTitle = this.getAttribute('data-bs-title');
                // If both defaults are missing or null, return an empty string ('') instead of null/undefined.
                return dataBsTitle || titleAttribute || '';
            }
        });
        $('body').tooltip(); // Initialize Bootstrap tooltips with delegation to any dynamic content

        // Safety net: dismiss any shown tooltip on scroll (and drop orphans left
        // behind by re-rendered triggers) — Bootstrap alone relies on a
        // `mouseleave` that wheel-scrolling never fires. See tooltipHygiene.js.
        initTooltipHygiene();

        // Expose the licence picker globally. The Workbench editors import it as a
        // module, but legacy Django templates (the dataset upload form) have no
        // bundle of their own and script inline against jQuery — without this they
        // would need either a webpack entry each or a second, divergent picker.
        window.wireWHGLicenseControl = wireLicenseControl;
        window.pickWHGLicense = pickLicense;

        // Extend popover defaults
        $.extend(true, $.fn.popover.Constructor.Default, {
            html: true,
            placement: 'right',
            trigger: 'hover'
        });
        $('[data-bs-toggle="popover"]').popover();

        initWHGModal();
        initializeCitationFormatters();

        // Extend Spinner module to operate with JQuery initialisation
        (function ($) {
            $.fn.spin = function (options) {
                return this.each(function () {
                    const $this = $(this);
                    let spinner = $this.data('spinner');

                    if (!spinner) {
                        spinner = new Spinner(options);
                        $this.data('spinner', spinner);
                    }

                    // Calculate the scale based on the minimum dimension of the target element
                    const minDimension = Math.min($this.width(), $this.height());
                    const scaleFactor = minDimension / 300; // 300px corresponds to a scale of 1

                    if (!options) {
                        options = {};
                    }

                    options.scale = (options.scale || 1) * scaleFactor;

                    spinner.opts = Object.assign(spinner.opts, options);
                    spinner.spin(this);
                });
            };

            $.fn.stopSpin = function () {
                return this.each(function () {
                    const $this = $(this);
                    const spinner = $this.data('spinner');
                    if (spinner) {
                        spinner.stop();
                        $this.removeData('spinner');
                    }
                });
            };
        })(jQuery);

        document.querySelector('body').style.opacity = 1;

        if (typeof scripts !== 'undefined') {
            //console.log('Executing deferred scripts.');
            executeDeferredScripts();
        }

        if (typeof loadMaplibre !== 'undefined') {
            window.bbox = turf.bbox;
            window.buffer = turf.buffer;
            window.convex = turf.convex;
            window.flatten = turf.flatten;
            window.dissolve = turf.dissolve;
            window.combine = turf.combine;
            window.midpoint = turf.midpoint
            window.centroid = turf.centroid
            window.getType = turf.getType
            window.area = turf.area
            window.distance = turf.distance
            window.lineString = turf.lineString
            window.bezierSpline = turf.bezierSpline
        }

        window.Spinner = Spinner;

        var language = window.navigator.language.substr(0, 2);

        // Hide/show links based on the URL
        var url = window.location.pathname;
        if (url !== '/') {
            $("#links_home").addClass('d-none');
            $("#links_other").removeClass('d-none');
        }

        // Highlight active links based on the URL
        var abouts = ['/about/', '/system/', '/licensing/', '/credits/'];
        var clicked = window.location.pathname;

        if ($.inArray(clicked, abouts) > -1) {
            console.log('in abouts');
            $("#aboutDropdown").addClass('navactive');
        } else {
            $("[href='" + clicked + "']").addClass('navactive');
        }

        $(document).on('click', '.toggle-truncate-link', function (e) {
            e.preventDefault();
            const $wrapper = $(this).closest('.toggle-truncate-wrapper');
            $wrapper.toggleClass('toggle-truncate-more');
        });
    })
    .catch(function (error) {
        console.error('Error loading resources:', error);
    });