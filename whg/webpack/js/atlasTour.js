// /whg/webpack/js/atlasTour.js
/**
 * Guided tour of the WHG Atlas page.
 *
 * Uses driver.js to walk users through the Atlas interface.
 * Auto-triggers on first visit; can be relaunched via the
 * bottom-left tour button or the welcome panel link.
 *
 * The tour automatically demonstrates UI controls (mode switches,
 * offcanvas panels, temporal filter) and disables user interaction
 * with page controls during the tour to prevent unpredictable state.
 */

import { driver } from 'driver.js';
import 'driver.js/dist/driver.css';

const TOUR_SEEN_KEY = 'whg_atlas_tour_seen';
const TOUR_ACTIVE_CLASS = 'atlas-tour-active';

/* ═══════════════════════════════════════════════════════════════════
   Helpers — programmatic control interaction during the tour
   ═══════════════════════════════════════════════════════════════════ */

/** Pending demo timeouts — cleared on every step transition. */
let _demoTimeouts = [];

function demoTimeout(fn, ms) {
    const id = setTimeout(fn, ms);
    _demoTimeouts.push(id);
    return id;
}

function clearDemoTimeouts() {
    _demoTimeouts.forEach(clearTimeout);
    _demoTimeouts = [];
}

/** Open a Bootstrap offcanvas programmatically (no backdrop). */
function openOffcanvas(selector) {
    const el = document.querySelector(selector);
    if (!el || typeof bootstrap === 'undefined') return;
    try {
        bootstrap.Offcanvas.getOrCreateInstance(el, { backdrop: false }).show();
    } catch (e) { /* */ }
}

/** Close a Bootstrap offcanvas. */
function closeOffcanvas(selector) {
    const el = document.querySelector(selector);
    if (!el || typeof bootstrap === 'undefined') return;
    try {
        const instance = bootstrap.Offcanvas.getInstance(el);
        if (instance) instance.hide();
    } catch (e) { /* */ }
}

/** Close ALL atlas offcanvas panels. */
function closeAllOffcanvas() {
    ['#layers_offcanvas', '#sources_offcanvas', '#categories_offcanvas']
        .forEach(closeOffcanvas);
}

/** Click a search-mode toggle button (triggers atlas.js switchSearchMode). */
function setSearchMode(mode) {
    const btn = document.querySelector(
        `.search-mode-toggle .btn[data-search-mode="${mode}"]`
    );
    if (btn) btn.click();
}

/** Only switch mode if not already in the requested mode. */
function ensureSearchMode(mode) {
    const active = document.querySelector('.search-mode-toggle .btn.active');
    if (active?.dataset?.searchMode === mode) return;
    setSearchMode(mode);
}

/** Click a temporal-mode toggle button. */
function setTemporalMode(mode) {
    const btn = document.querySelector(
        `#temporal_control .temporal-mode-toggle .btn[data-temporal-mode="${mode}"]`
    );
    if (btn) btn.click();
}

/* ═══════════════════════════════════════════════════════════════════
   Interaction blocking
   ═══════════════════════════════════════════════════════════════════ */

/**
 * Capturing-phase click blocker: prevents clicks on offcanvas panels
 * and their toggle buttons from propagating to driver.js (which would
 * otherwise interpret them as "outside clicks" and end the tour).
 */
function tourClickBlocker(e) {
    if (e.target.closest('.offcanvas')) {
        e.stopPropagation();
        e.preventDefault();
        return;
    }
    if (e.target.closest('[data-bs-toggle="offcanvas"]')) {
        e.stopPropagation();
        e.preventDefault();
        return;
    }
}

function blockInteractions() {
    document.body.classList.add(TOUR_ACTIVE_CLASS);
    document.addEventListener('click', tourClickBlocker, true);
    // Mark all offcanvas panels as inert so they cannot receive focus or
    // pointer events — this prevents accidental interaction from causing
    // the tour to cancel.
    document.querySelectorAll('.atlas-offcanvas').forEach(el => {
        el.inert = true;
    });
}

function unblockInteractions() {
    document.body.classList.remove(TOUR_ACTIVE_CLASS);
    document.removeEventListener('click', tourClickBlocker, true);
    document.querySelectorAll('.atlas-offcanvas').forEach(el => {
        el.inert = false;
    });
}

/** Full cleanup — called on tour destroy and as a safety net. */
function tourCleanup() {
    clearDemoTimeouts();
    closeAllOffcanvas();
    unblockInteractions();
    ensureSearchMode('areas');
    setTemporalMode('off');
}

/* ═══════════════════════════════════════════════════════════════════
   Tour step definitions
   ═══════════════════════════════════════════════════════════════════ */

function getTourSteps() {
    return [

        /* ── 1. Map overview ────────────────────────────────────── */
        {
            element: '#hero_map',
            popover: {
                title: 'Welcome to the WHG Atlas',
                description:
                    'The Atlas is an <strong>interactive historical atlas</strong> ' +
                    'of the world through time. The map is your primary interface — ' +
                    'explore geographic boundaries first, then search for historical ' +
                    'place names within those areas.<br><br>' +
                    'Zoom, pan, and rotate the globe to navigate. ' +
                    '<strong>Click on any boundary polygon</strong> to select a region. ' +
                    'Use the corner controls to switch between globe and flat projections.',
                side: 'top',
                align: 'center',
            },
        },

        /* ── 2. Search modes (with auto-demo) ──────────────────── */
        {
            element: '.search-mode-toggle',
            popover: {
                title: 'Two Exploration Modes',
                description:
                    'The Atlas has two complementary modes:<br><br>' +
                    '<strong>Areas</strong> — explore geographic boundaries: modern countries ' +
                    '(from OpenStreetMap), historical territories (from OpenHistoricalMap), ' +
                    'and more. Type a name to find regions, or click directly on the map.<br><br>' +
                    '<strong>Toponyms</strong> — search the WHG index of ' +
                    '<strong>47 million+ place records</strong> from GeoNames, Wikidata, ' +
                    'Pleiades, TGN, and other gazetteers. Results are automatically ' +
                    'constrained to any areas you\'ve selected.<br><br>' +
                    '<em class="text-muted">Watch the toggle switch automatically…</em>',
                side: 'bottom',
                align: 'start',
            },
            onHighlighted: () => {
                demoTimeout(() => setSearchMode('toponyms'), 800);
                demoTimeout(() => setSearchMode('areas'), 2400);
            },
            onDeselected: () => {
                clearDemoTimeouts();
                ensureSearchMode('areas');
            },
        },

        /* ── 3. Search input ────────────────────────────────────── */
        {
            element: '.search-input-group',
            popover: {
                title: 'Search Input',
                description:
                    'Type here to search. Behaviour depends on which mode is active:<br><br>' +
                    'In <strong>Areas</strong> mode, matching boundaries appear as a dropdown ' +
                    'as you type — select one to highlight it on the map and add it as a ' +
                    'spatial constraint. Try typing "France" or "Ottoman".<br><br>' +
                    'In <strong>Toponyms</strong> mode, press <strong>Enter</strong> or click ' +
                    'the <i class="fas fa-search"></i> button to search the gazetteer index. ' +
                    'A results panel will slide in alongside the map.',
                side: 'bottom',
                align: 'center',
            },
        },

        /* ── 4. Exact / fuzzy match ─────────────────────────────── */
        {
            element: '#atlas_exact_match',
            popover: {
                title: 'Exact / Fuzzy Match',
                description:
                    'Toggle between matching modes for toponym searches:<br><br>' +
                    '<strong>Fuzzy</strong> (default) — finds phonetically similar names. ' +
                    'Searching "Konstantinopolis" will also find "Constantinople", ' +
                    '"Kostantiniyye", etc.<br><br>' +
                    '<strong>Exact</strong> — precise spelling match only. ' +
                    'Use this when you know the exact form of a place name and want to ' +
                    'avoid false positives.',
                side: 'bottom',
                align: 'center',
            },
        },

        /* ── 5. Viewport + Regions pill (auto-open Regions) ─────────── */
        {
            element: '#areas_control_pill',
            popover: {
                title: 'Viewport & Regions',
                description:
                    'These two buttons work together in <strong>Areas</strong> mode:<br><br>' +
                    '<strong><i class="fas fa-crop-alt"></i> Viewport</strong> — when the map ' +
                    'is in flat (non-globe) projection, toggle this to constrain your toponym ' +
                    'search to the <strong>current map viewport</strong> instead of selected ' +
                    'areas. Disabled in globe view.<br><br>' +
                    '<strong><i class="fas fa-globe-americas"></i> Regions</strong> — opens a panel ' +
                    'where you can choose <strong>modern</strong> (OpenStreetMap) or ' +
                    '<strong>historical</strong> (OpenHistoricalMap) boundaries, and set the ' +
                    'administrative level (continent → country → state → province, etc.).<br><br>' +
                    '<em class="text-muted">The Regions panel is opening now…</em>',
                side: 'bottom',
                align: 'start',
            },
            onHighlighted: () => {
                demoTimeout(() => openOffcanvas('#layers_offcanvas'), 300);
            },
            onDeselected: () => {
                clearDemoTimeouts();
                closeOffcanvas('#layers_offcanvas');
            },
        },

        /* ── 6. Selection chips ─────────────────────────────────── */
        {
            element: '#selection_chips',
            popover: {
                title: 'Selected Areas',
                description:
                    'Every area you select — whether by clicking the map or choosing from ' +
                    'the search dropdown — appears here as a chip. You can ' +
                    '<strong>select multiple areas</strong> to build up a composite study ' +
                    'region.<br><br>' +
                    'When you switch to <strong>Toponyms</strong> mode, your search is ' +
                    'automatically constrained to the union of all selected areas. ' +
                    'Click the <strong>×</strong> on any chip to remove it.',
                side: 'bottom',
                align: 'start',
            },
        },

        /* ── 7. Temporal filter (auto-activate) ─────────────────── */
        {
            element: '#temporal_control',
            popover: {
                title: 'Temporal Filter',
                description:
                    'Constrain results to a specific time period. Three modes:<br><br>' +
                    '<strong>Off</strong> — no temporal filtering (default).<br>' +
                    '<strong>Date Range</strong> — only show places attested within the ' +
                    'selected year span. Drag the two slider handles to set a range, ' +
                    'e.g. 1200–1500 CE.<br>' +
                    '<strong>+Undated</strong> — same as Date Range, but also includes ' +
                    'places that have no temporal information at all.<br><br>' +
                    'The slider covers 2000 BCE to 2100 CE.<br><br>' +
                    '<em class="text-muted">Activating Date Range to demonstrate…</em>',
                side: 'bottom',
                align: 'start',
            },
            onHighlightStarted: () => {
                // Ensure Areas mode (in case user navigated back from step 8)
                ensureSearchMode('areas');
            },
            onHighlighted: () => {
                demoTimeout(() => setTemporalMode('range'), 300);
            },
            onDeselected: () => {
                clearDemoTimeouts();
                setTemporalMode('off');
            },
        },

        /* ── 8. Sources panel (switch to Toponyms, auto-open) ──── */
        {
            element: '#open_sources_modal',
            popover: {
                title: 'Data Sources Panel',
                description:
                    'Now switching to <strong>Toponyms</strong> mode. The ' +
                    '<strong><i class="fas fa-database"></i> Sources</strong> button ' +
                    'opens a panel where you can filter search results by data source: ' +
                    'GeoNames, Wikidata, Pleiades, TGN, OpenHistoricalMap, and more.<br><br>' +
                    'Uncheck sources to narrow your results to specific gazetteers.<br><br>' +
                    '<em class="text-muted">The Sources panel is opening now…</em>',
                side: 'bottom',
                align: 'end',
            },
            onHighlightStarted: () => {
                // Switch to Toponyms so the Sources button becomes visible
                ensureSearchMode('toponyms');
            },
            onHighlighted: () => {
                demoTimeout(() => openOffcanvas('#sources_offcanvas'), 300);
            },
            onDeselected: () => {
                clearDemoTimeouts();
                closeOffcanvas('#sources_offcanvas');
            },
        },

        /* ── 9. Categories panel (auto-open) ────────────────────── */
        {
            element: '#open_categories_modal',
            popover: {
                title: 'Place Categories Panel',
                description:
                    'The <strong><i class="fas fa-sitemap"></i> Categories</strong> panel ' +
                    'lets you filter toponym results by place type using the ' +
                    '<strong>Getty AAT hierarchy</strong> — settlements, temples, rivers, ' +
                    'fortifications, and thousands more.<br><br>' +
                    'Select categories to narrow your search to specific types of places.<br><br>' +
                    '<em class="text-muted">The Categories panel is opening now…</em>',
                side: 'bottom',
                align: 'end',
            },
            onHighlightStarted: () => {
                ensureSearchMode('toponyms');
            },
            onHighlighted: () => {
                demoTimeout(() => openOffcanvas('#categories_offcanvas'), 300);
            },
            onDeselected: () => {
                clearDemoTimeouts();
                closeOffcanvas('#categories_offcanvas');
                ensureSearchMode('areas');
            },
        },

        /* ── 10. Re-take tour button ────────────────────────────── */
        {
            element: '#atlas_tour_btn',
            popover: {
                title: 'Re-take This Tour',
                description:
                    'You can re-launch this guided tour at any time by clicking this ' +
                    '<strong><i class="fas fa-map-signs"></i></strong> button in the ' +
                    'bottom-left corner of the map.<br><br>' +
                    'The tour only auto-starts on your first visit — after that, use ' +
                    'this button whenever you need a refresher.',
                side: 'right',
                align: 'end',
            },
        },
    ];
}

/* ═══════════════════════════════════════════════════════════════════
   Driver.js instance
   ═══════════════════════════════════════════════════════════════════ */

function createTourDriver() {
    return driver({
        showProgress: true,
        showButtons: ['next', 'previous', 'close'],
        steps: getTourSteps(),
        nextBtnText: 'Next →',
        prevBtnText: '← Back',
        doneBtnText: 'Done',
        progressText: '{{current}} of {{total}}',
        // Block all user interaction with highlighted elements
        disableActiveInteraction: true,
        // Clicking the shaded overlay advances to the next step instead
        // of cancelling the tour (which was too easy to trigger accidentally
        // when offcanvas panels or the backdrop were open).
        overlayClickBehavior: 'nextStep',
        onDestroyed: () => {
            tourCleanup();
            localStorage.setItem(TOUR_SEEN_KEY, 'true');
        },
    });
}

/* ═══════════════════════════════════════════════════════════════════
   Public API
   ═══════════════════════════════════════════════════════════════════ */

/** Start the guided tour. */
export function startAtlasTour() {
    // Ensure clean state before starting
    tourCleanup();
    blockInteractions();
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

