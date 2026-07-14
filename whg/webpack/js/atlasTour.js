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
import heroMap from './heroMap';

const TOUR_SEEN_KEY = 'whg_atlas_tour_seen';
const TOUR_ACTIVE_CLASS = 'atlas-tour-active';

/* ═══════════════════════════════════════════════════════════════════
   Demo mode — auto-advancing, looping presentation for kiosk/monitor use
   ═══════════════════════════════════════════════════════════════════ */

/** Dwell time per step before auto-advancing (ms). Must comfortably
 *  exceed the longest in-step demo timeout (step 2 fires at 2400ms). */
const DEMO_DWELL_MS = 9000;

/** Static URL of the WHG logo, shown small in each tour-step footer. */
const WHG_LOGO_URL = '/static/images/whg_logo.svg';

/** Body class applied while demo mode runs — used to suppress the dev
 *  server's red viewport border for a clean presentation. */
const DEMO_MODE_CLASS = 'atlas-demo-mode';

let _demoMode = false;
let _demoPaused = false;
let _autoAdvanceTimer = null;
let _activeDriver = null;

/** Number of steps shown in the current mode. The final "Re-take this
 *  tour" step is omitted in demo mode. */
function activeStepCount() {
    const n = getTourSteps().length;
    return _demoMode ? n - 1 : n;
}

function clearAutoAdvance() {
    if (_autoAdvanceTimer) {
        clearTimeout(_autoAdvanceTimer);
        _autoAdvanceTimer = null;
    }
}

/** Advance one step, looping back to the first after the last. */
function advanceDemoStep() {
    if (!_activeDriver || !_activeDriver.isActive()) return;
    const total = activeStepCount();
    const idx = _activeDriver.getActiveIndex();
    if (typeof idx === 'number' && idx >= total - 1) {
        _activeDriver.moveTo(0);   // loop
    } else {
        _activeDriver.moveNext();
    }
}

/** Schedule the next auto-advance. Loops back to the first step after
 *  the last, so the presentation runs indefinitely. */
function scheduleAutoAdvance() {
    clearAutoAdvance();
    if (!_demoMode || _demoPaused || !_activeDriver) return;
    _autoAdvanceTimer = setTimeout(() => {
        _autoAdvanceTimer = null;
        if (!_demoMode || _demoPaused || !_activeDriver || !_activeDriver.isActive()) return;
        advanceDemoStep();
        scheduleAutoAdvance();
    }, DEMO_DWELL_MS);
}

/** Toggle the demo auto-advance between paused and running. Resuming
 *  advances to the next step immediately, then resumes the timer. */
function toggleDemoPause() {
    _demoPaused = !_demoPaused;
    updatePauseButtons();
    if (_demoPaused) {
        clearAutoAdvance();
    } else {
        advanceDemoStep();
        scheduleAutoAdvance();
    }
}

/** Reflect the paused/running state on any rendered pause buttons. */
function updatePauseButtons() {
    document.querySelectorAll('.driver-popover-demo-pause').forEach((btn) => {
        const label = _demoPaused ? 'Resume demo' : 'Pause demo';
        btn.innerHTML =
            '<i class="fas ' + (_demoPaused ? 'fa-play' : 'fa-pause') +
            '" aria-hidden="true"></i>';
        btn.setAttribute('aria-label', label);
        btn.title = label;
    });
}

/**
 * Inject the footer bar (small WHG logo + preview note) into a tour
 * popover. In demo mode it also gets a discrete pause/continue control.
 * Called for every step from ``onPopoverRender``.
 */
function injectPopoverFooter(popover) {
    if (!popover || !popover.wrapper) return;
    const bar = document.createElement('div');
    bar.className = 'driver-popover-footnote';

    const logo = document.createElement('img');
    logo.className = 'driver-popover-footnote-logo';
    logo.src = WHG_LOGO_URL;
    logo.alt = 'WHG';

    const note = document.createElement('span');
    note.className = 'driver-popover-footnote-text';
    note.textContent = 'WHG Atlas — preview of a future release';

    bar.appendChild(logo);
    bar.appendChild(note);

    if (_demoMode) {
        const pause = document.createElement('button');
        pause.type = 'button';
        pause.className = 'driver-popover-demo-pause';
        pause.addEventListener('click', (e) => {
            e.preventDefault();
            e.stopPropagation();
            toggleDemoPause();
        });
        bar.appendChild(pause);
    }

    popover.wrapper.appendChild(bar);
    if (_demoMode) updatePauseButtons();
}

/** Style the first ("Welcome to the WHG Atlas") popover as a centred,
 *  hero-style modal with a large WHG logo. */
function decorateWelcomePopover(popover) {
    if (!popover || !popover.wrapper) return;
    popover.wrapper.classList.add('atlas-welcome-popover');
    if (popover.wrapper.querySelector('.atlas-welcome-popover-logo')) return;
    const hero = document.createElement('img');
    hero.className = 'atlas-welcome-popover-logo';
    hero.src = WHG_LOGO_URL;
    hero.alt = 'World Historical Gazetteer';
    if (popover.title && popover.title.parentNode) {
        popover.title.parentNode.insertBefore(hero, popover.title);
    } else {
        popover.wrapper.insertBefore(hero, popover.wrapper.firstChild);
    }
}

/** Enter demo mode from the button on the first tour step: rebuild the
 *  tour without the final "re-take" step and start auto-advancing. */
function enterDemoMode() {
    if (_demoMode) return;
    // Tear down the current (non-demo) driver without triggering demo
    // teardown, then restart cleanly in demo mode.
    if (_activeDriver) {
        const d = _activeDriver;
        _activeDriver = null;
        try { d.destroy(); } catch (e) { /* */ }
    }
    startAtlasTour({ demo: true });
}

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
    ['#layers_offcanvas', '#gazetteers_offcanvas', '#categories_offcanvas']
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

        /* ── 1. Welcome (centred modal — no highlighted element) ──── */
        {
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

        /* ── 8. Gazetteers panel (switch to Places, auto-open) ──── */
        {
            element: '#open_gazetteers_modal',
            popover: {
                title: 'Gazetteers Panel',
                description:
                    'Now switching to <strong>Places</strong> mode. The ' +
                    '<strong><i class="fas fa-book-atlas"></i> Gazetteers</strong> button ' +
                    'opens a panel where you can filter search results by gazetteer: ' +
                    'GeoNames, Wikidata, Pleiades, TGN, OpenHistoricalMap, and more.<br><br>' +
                    'Uncheck gazetteers to narrow your results.<br><br>' +
                    '<em class="text-muted">The Gazetteers panel is opening now…</em>',
                side: 'bottom',
                align: 'end',
            },
            onHighlightStarted: () => {
                // Switch to Places so the Gazetteers button becomes visible
                ensureSearchMode('toponyms');
            },
            onHighlighted: () => {
                demoTimeout(() => openOffcanvas('#gazetteers_offcanvas'), 300);
            },
            onDeselected: () => {
                clearDemoTimeouts();
                closeOffcanvas('#gazetteers_offcanvas');
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
    // Demo mode drops the final "Re-take this tour" step — it's
    // meaningless in an auto-advancing loop.
    const steps = _demoMode ? getTourSteps().slice(0, -1) : getTourSteps();
    return driver({
        showProgress: true,
        showButtons: ['next', 'previous', 'close'],
        steps: steps,
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
        onPopoverRender: (popover, opts) => {
            // Footer bar (WHG logo + preview note, plus demo pause control)
            // on every step.
            injectPopoverFooter(popover);
            const idx = opts?.state?.activeIndex;
            // First step is a centred hero-style welcome modal.
            if (idx === 0) decorateWelcomePopover(popover);
            // "Start looping demo" button on the first step's popover
            // (the "Welcome to the WHG Atlas" card) — only when not
            // already running as a demo.
            if (idx !== 0 || _demoMode) return;
            const btn = document.createElement('button');
            btn.id = 'atlas_demo_mode_btn';
            btn.type = 'button';
            btn.className = 'driver-popover-demo-btn';
            btn.innerHTML =
                '<i class="fas fa-circle-play" aria-hidden="true"></i> ' +
                'Start looping demo';
            btn.addEventListener('click', (e) => {
                e.preventDefault();
                e.stopPropagation();
                enterDemoMode();
            });
            if (popover.description) popover.description.appendChild(btn);
        },
        onDestroyed: () => {
            clearAutoAdvance();
            tourCleanup();
            if (_demoMode) {
                _demoMode = false;
                _demoPaused = false;
                document.body.classList.remove(DEMO_MODE_CLASS);
                try { heroMap.setGlobePinned(false); } catch (e) { /* */ }
            }
            _activeDriver = null;
            localStorage.setItem(TOUR_SEEN_KEY, 'true');
        },
    });
}

/* ═══════════════════════════════════════════════════════════════════
   Public API
   ═══════════════════════════════════════════════════════════════════ */

/**
 * Start the guided tour.
 * @param {{demo?: boolean}} [options] — when `demo` is true, the tour runs
 *   in auto-advancing, looping demo mode with the globe pinned and a
 *   persistent "preview of a future release" banner.
 */
export function startAtlasTour(options = {}) {
    // Ensure clean state before starting
    clearAutoAdvance();
    tourCleanup();
    blockInteractions();
    _demoPaused = false;
    if (options.demo) {
        _demoMode = true;
        document.body.classList.add(DEMO_MODE_CLASS);
        try { heroMap.setGlobePinned(true); } catch (e) { /* map not ready */ }
    }
    const d = createTourDriver();
    _activeDriver = d;
    d.drive();
    if (_demoMode) scheduleAutoAdvance();
}

/** Start the looping demo presentation directly. */
export function startAtlasDemo() {
    startAtlasTour({ demo: true });
}

/** Has the user already seen the tour? */
export function hasSeenAtlasTour() {
    return localStorage.getItem(TOUR_SEEN_KEY) === 'true';
}

/** Reset the tour-seen flag (for testing). */
export function resetAtlasTourFlag() {
    localStorage.removeItem(TOUR_SEEN_KEY);
}

