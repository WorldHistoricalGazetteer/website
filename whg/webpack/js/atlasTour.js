// /whg/webpack/js/atlasTour.js
/**
 * Guided tour of the WHG Atlas page.
 *
 * Uses driver.js to walk users through the Atlas interface.
 * Auto-triggers on first visit; can be relaunched via the
 * bottom-left tour button or the welcome panel link.
 */

import { driver } from 'driver.js';
import 'driver.js/dist/driver.css';

const TOUR_SEEN_KEY = 'whg_atlas_tour_seen';

/** The tour step definitions. */
function getTourSteps() {
    return [
        {
            element: '.search-mode-toggle',
            popover: {
                title: 'Search Modes',
                description:
                    'The Atlas has two modes. <strong>Areas</strong> mode lets you explore geographic ' +
                    'boundaries — countries, states, historical territories. ' +
                    '<strong>Toponyms</strong> mode searches for place names within selected areas.',
                side: 'bottom',
                align: 'start',
            },
        },
        {
            element: '.search-input-group',
            popover: {
                title: 'Search Input',
                description:
                    'Type here to search. In <strong>Areas</strong> mode, matching boundaries appear ' +
                    'as a dropdown — click one to select it. In <strong>Toponyms</strong> mode, ' +
                    'press <strong>Enter</strong> or click the search button to query place names.',
                side: 'bottom',
                align: 'center',
            },
        },
        {
            element: '#atlas_exact_match',
            popover: {
                title: 'Exact Match',
                description:
                    'Toggle between <strong>fuzzy</strong> (phonetically similar) and <strong>exact</strong> ' +
                    '(precise spelling) matching. Useful when you know the exact form of a place name.',
                side: 'bottom',
                align: 'center',
            },
        },
        {
            element: '.atlas-control-buttons',
            popover: {
                title: 'Control Panels',
                description:
                    '<strong>Layers</strong> configures map tile sources and boundary display settings. ' +
                    'In Toponyms mode, <strong>Sources</strong> filters by data source (GeoNames, Wikidata, etc.) ' +
                    'and <strong>Categories</strong> filters by place type using the AAT hierarchy.',
                side: 'bottom',
                align: 'end',
            },
        },
        {
            element: '#selection_chips',
            popover: {
                title: 'Selected Areas',
                description:
                    'Areas you\'ve selected appear here as chips. You can select <strong>multiple areas</strong> — ' +
                    'toponym searches will look within all of them. Click the × on a chip to remove it.',
                side: 'bottom',
                align: 'start',
            },
        },
        {
            element: '#temporal_control',
            popover: {
                title: 'Temporal Filter',
                description:
                    'Constrain results by time period. Choose <strong>Date Range</strong> to set a year span, ' +
                    'or <strong>+Undated</strong> to also include places without temporal information. ' +
                    'Drag the slider handles to adjust the range.',
                side: 'bottom',
                align: 'start',
            },
        },
        {
            element: '#hero_map',
            popover: {
                title: 'The Map',
                description:
                    'The full-screen map displays boundaries, search results, and overlays. ' +
                    'In Areas mode, <strong>click directly on a boundary</strong> to select it. ' +
                    'In Toponyms mode, click a result marker to highlight it in the results panel. ' +
                    'Use the controls in the corners to zoom, rotate, and switch projections.',
                side: 'top',
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
export function startAtlasTour() {
    const d = createTourDriver();
    d.drive();
}

/** Has the user already seen the tour? */
export function hasSeenAtlasTour() {
    return localStorage.getItem(TOUR_SEEN_KEY) === 'true';
}

/** Reset the tour-seen flag (for testing). */
export function resetAtlasTourFlag() {
    localStorage.removeItem(TOUR_SEEN_KEY);
}

