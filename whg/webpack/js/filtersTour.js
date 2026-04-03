// /whg/webpack/js/filtersTour.js
/**
 * Guided tour of the WHG Search Filters panel.
 *
 * Uses driver.js to walk users through the filter interface.
 * Auto-triggers on first-ever filter panel open; thereafter
 * available via a "Take a Tour" link.
 */

import { driver } from 'driver.js';
import 'driver.js/dist/driver.css';

const TOUR_SEEN_KEY = 'whg_filters_tour_seen';

/** The tour step definitions. */
function getTourSteps() {
    return [
        {
            element: '#filters_panel',
            popover: {
                title: 'Search Filters',
                description:
                    'This panel lets you refine your search across three dimensions: ' +
                    '<strong>Data Sources</strong>, <strong>Time & Space</strong>, and <strong>Place Types</strong>. ' +
                    'Filters work together to narrow results — you can combine them freely.',
                side: 'bottom',
                align: 'center',
            },
        },
        {
            element: '.filter-col--authorities',
            popover: {
                title: 'Data Sources',
                description:
                    'Choose which gazetteers to include in your search. ' +
                    'WHG indexes records from GeoNames, Wikidata, TGN, Pleiades, and more. ' +
                    'Sources like OpenStreetMap and GB1900 are unchecked by default because they include ' +
                    'very large numbers of minor features (buildings, bus stops, field names). ' +
                    'Hover over any source name for details.',
                side: 'right',
                align: 'start',
            },
        },
        {
            element: '.filter-col--timespace',
            popover: {
                title: 'Time & Space',
                description:
                    'Control <em>when</em> and <em>where</em> you\'re searching. ' +
                    'The small map to the right always shows the spatial extent of your search — ' +
                    'zoom and pan it to focus on a region of interest. ' +
                    'Three tabs offer different ways to define temporal and spatial constraints.',
                side: 'bottom',
                align: 'center',
            },
        },
        {
            element: '#tab-timespan',
            popover: {
                title: 'Region Tab',
                description:
                    'The default tab. Use the <strong>dateline slider</strong> to set a year range, ' +
                    'and the <strong>Space</strong> toggle to constrain by region. ' +
                    'Options include <em>Off</em>, <em>Map bounds</em> (use viewport), ' +
                    'UN <em>Continental</em> and <em>Sub-Continental</em> regions ' +
                    '(shows the 5 closest as suggestions on the map), ' +
                    'and OSM admin tiers (<em>Country</em>, <em>State</em>, <em>District/County</em>, <em>Municipality</em>). ' +
                    '<strong>Multiple regions</strong> can be selected — click suggestions on the map or pick from the dropdown. ' +
                    'Tier buttons are disabled until you zoom the map. ' +
                    'The dateline has three modes: <em>Off</em> (no time filter), ' +
                    '<em>Year range</em> (strict), and <em>Range + undated</em> ' +
                    '(includes places with no temporal attestation).',
                side: 'bottom',
                align: 'start',
            },
        },
        {
            element: '#tab-periods',
            popover: {
                title: 'Period Tab',
                description:
                    'Search for scholarly period definitions from <strong>PeriodO</strong> ' +
                    '(e.g. "Iron Age", "Bronze Age"). Selecting a period automatically sets ' +
                    'the time range and, where available, the spatial extent. ' +
                    '<strong>Multiple periods</strong> can be selected — the temporal range becomes ' +
                    'the union (widest span) of all selected periods.',
                side: 'bottom',
                align: 'center',
            },
        },
        {
            element: '#tab-polities',
            popover: {
                title: 'Territory Tab',
                description:
                    'Search historical polities (<strong>Cliopatria</strong>), cultural regions ' +
                    '(<strong>D-PLACE</strong>), or indigenous territories (<strong>NativeLand</strong>). ' +
                    'Selecting a territory sets both the time range and spatial boundary. ' +
                    '<strong>Multiple territories</strong> can be selected, even across different datasets — ' +
                    'the temporal range becomes the union of all selected territories.',
                side: 'bottom',
                align: 'end',
            },
        },
        {
            element: '#context_map_wrap',
            popover: {
                title: 'Context Map',
                description:
                    'This map defines the <strong>spatial extent</strong> of your search. ' +
                    'Zoom in to your area of interest — the map must be zoomed past the default ' +
                    'view before the region, period, and territory search inputs become active. ' +
                    'Selected regions or territories are previewed as an overlay here. ' +
                    'Toggle between globe and flat views using the control in the corner.',
                side: 'left',
                align: 'center',
            },
        },
        {
            element: '.filter-col--types',
            popover: {
                title: 'Place Types',
                description:
                    'Filter by the kind of place you\'re looking for using the Getty AAT hierarchy. ' +
                    'Select categories like "inhabited places", "archaeological sites", or "bodies of water". ' +
                    'Selecting a parent category includes all its children. ' +
                    'Use the search box at the top to find specific types quickly. ' +
                    'Results are post-filtered: broader matches are ranked below exact type matches.',
                side: 'left',
                align: 'start',
            },
        },
        {
            element: '#exact_match_toggle',
            popover: {
                title: 'Exact Match',
                description:
                    'By default, WHG searches include <strong>phonetically similar</strong> place names ' +
                    '(e.g. searching "Krakow" also finds "Cracow" and "Kraków"). ' +
                    'Click this button to toggle <strong>exact spelling</strong> mode, which requires ' +
                    'results to match your search term precisely. ' +
                    'The button turns <span style="color:#993333;font-weight:600">red</span> when exact mode is active.',
                side: 'bottom',
                align: 'center',
            },
        },
        {
            element: '#initiate_search',
            popover: {
                title: 'Search Button',
                description:
                    'Click this button (or press <strong>Enter</strong>) to run your search. ' +
                    'Changing filters does <em>not</em> automatically update results — ' +
                    'you must click here again to apply any new filter settings. ' +
                    'This lets you compose filters at your own pace before re-querying. ' +
                    'You can also search <em>without</em> a place name if you\'ve selected at least one ' +
                    'place type together with a temporal or spatial constraint.',
                side: 'bottom',
                align: 'center',
            },
        },
    ];
}

/** Build and return a driver.js instance for the tour. */
function createTourDriver() {
    return driver({
        showProgress: true,
        showButtons: ['next', 'previous', 'close'],
        steps: getTourSteps(),
        nextBtnText: 'Next →',
        prevBtnText: '← Back',
        doneBtnText: 'Done',
        progressText: '{{current}} of {{total}}',
        onDestroyed: () => {
            localStorage.setItem(TOUR_SEEN_KEY, 'true');
        },
    });
}

/** Start the tour. */
export function startFiltersTour() {
    const d = createTourDriver();
    d.drive();
}

/** Has the user already seen the tour? */
export function hasSeenTour() {
    return localStorage.getItem(TOUR_SEEN_KEY) === 'true';
}

/** Reset the tour-seen flag (for testing). */
export function resetTourFlag() {
    localStorage.removeItem(TOUR_SEEN_KEY);
}

