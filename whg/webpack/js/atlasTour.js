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
            element: '#hero_map',
            popover: {
                title: 'Welcome to the WHG Atlas',
                description:
                    'The Atlas is an <strong>interactive historical atlas</strong> of the world through time. ' +
                    'The map is your primary interface — explore geographic boundaries first, then search ' +
                    'for historical place names within those areas.<br><br>' +
                    'Zoom, pan, and rotate the globe to navigate. ' +
                    '<strong>Click on any boundary polygon</strong> to select a region. ' +
                    'Use the corner controls to switch between globe and flat projections.',
                side: 'top',
                align: 'center',
            },
        },
        {
            element: '.search-mode-toggle',
            popover: {
                title: 'Two Exploration Modes',
                description:
                    'The Atlas has two complementary modes:<br><br>' +
                    '<strong>Areas</strong> — explore geographic boundaries: modern countries (from OpenStreetMap), ' +
                    'historical territories (from OpenHistoricalMap), and more. ' +
                    'Type a name to find regions, or click directly on the map.<br><br>' +
                    '<strong>Toponyms</strong> — search the WHG index of <strong>47 million+ place records</strong> ' +
                    'from GeoNames, Wikidata, Pleiades, TGN, and other gazetteers. ' +
                    'Results are automatically constrained to any areas you\'ve selected.',
                side: 'bottom',
                align: 'start',
            },
        },
        {
            element: '.search-input-group',
            popover: {
                title: 'Search Input',
                description:
                    'Type here to search. Behaviour depends on which mode is active:<br><br>' +
                    'In <strong>Areas</strong> mode, matching boundaries appear as a dropdown as you type ' +
                    '— select one to highlight it on the map and add it as a spatial constraint. ' +
                    'Try typing "France" or "Ottoman".<br><br>' +
                    'In <strong>Toponyms</strong> mode, press <strong>Enter</strong> or click the ' +
                    '<i class="fas fa-search"></i> button to search the gazetteer index. ' +
                    'A results panel will slide in alongside the map.',
                side: 'bottom',
                align: 'center',
            },
        },
        {
            element: '#atlas_exact_match',
            popover: {
                title: 'Exact / Fuzzy Match',
                description:
                    'Toggle between matching modes for toponym searches:<br><br>' +
                    '<strong>Fuzzy</strong> (default) — finds phonetically similar names. ' +
                    'Searching "Konstantinopolis" will also find "Constantinople", "Kostantiniyye", etc.<br><br>' +
                    '<strong>Exact</strong> — precise spelling match only. ' +
                    'Use this when you know the exact form of a place name and want to avoid false positives.',
                side: 'bottom',
                align: 'center',
            },
        },
        {
            element: '.atlas-control-buttons',
            popover: {
                title: 'Control Panels',
                description:
                    'Three off-canvas panels give you fine-grained control:<br><br>' +
                    '<strong><i class="fas fa-layer-group"></i> Layers</strong> (Areas mode) — ' +
                    'configure map tile sources, choose modern or historical boundaries, and set the ' +
                    'administrative level (country, state, province, etc.).<br><br>' +
                    '<strong><i class="fas fa-database"></i> Sources</strong> (Toponyms mode) — ' +
                    'filter results by data source: GeoNames, Wikidata, Pleiades, TGN, and more.<br><br>' +
                    '<strong><i class="fas fa-sitemap"></i> Categories</strong> (Toponyms mode) — ' +
                    'filter by place type using the Getty AAT hierarchy (e.g. settlements, temples, rivers).',
                side: 'bottom',
                align: 'end',
            },
        },
        {
            element: '#selection_chips',
            popover: {
                title: 'Selected Areas',
                description:
                    'Every area you select — whether by clicking the map or choosing from the search dropdown ' +
                    '— appears here as a chip. You can <strong>select multiple areas</strong> to build up ' +
                    'a composite study region.<br><br>' +
                    'When you switch to <strong>Toponyms</strong> mode, your search is automatically constrained ' +
                    'to the union of all selected areas. Click the <strong>×</strong> on any chip to remove it.',
                side: 'bottom',
                align: 'start',
            },
        },
        {
            element: '#temporal_control',
            popover: {
                title: 'Temporal Filter',
                description:
                    'Constrain results to a specific time period. Three modes:<br><br>' +
                    '<strong>Off</strong> — no temporal filtering (default).<br>' +
                    '<strong>Date Range</strong> — only show places attested within the selected year span. ' +
                    'Drag the two slider handles to set a range, e.g. 1200–1500 CE.<br>' +
                    '<strong>+Undated</strong> — same as Date Range, but also includes places that have ' +
                    'no temporal information at all.<br><br>' +
                    'The slider covers 2000 BCE to 2100 CE.',
                side: 'bottom',
                align: 'start',
            },
        },
        {
            element: '#atlas_tour_btn',
            popover: {
                title: 'Re-take This Tour',
                description:
                    'You can re-launch this guided tour at any time by clicking this ' +
                    '<strong><i class="fas fa-map-signs"></i></strong> button in the bottom-left corner of the map. ' +
                    'The tour only auto-starts on your first visit.',
                side: 'right',
                align: 'end',
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

