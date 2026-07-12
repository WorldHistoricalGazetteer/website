// /whg/webpack/js/atlas.js
// WHG Atlas — Hero Map Explorer + Toponymic Search
//
// New entry point for /atlas/ page. Reuses existing modules
// (filterState, TypeTreeWidget, etc.) but wires them
// into the hero-map layout instead of the three-column search layout.

import { errorModal } from './error-modal.js';
import throttle from 'lodash/throttle';
import debounce from 'lodash/debounce';
import { geomsGeoJSON } from './utilities';
import CountryParents from './countryParents';
import TypeTreeWidget from './typeTreeWidget';
import filterState from './filterState';
import heroMap from './heroMap';
import LayerSourcesPalette from './layerSourcesPalette';
import AreaSearchRouter from './areaSearchRouter';
import { startAtlasTour, hasSeenAtlasTour } from './atlasTour.js';
import { clusterHits } from './clustering.js';
import './toggle-truncate.js';
import '../css/typeahead.css';
import '../css/atlas.css';

/* ═══════════════════════════════════════════════════════════════════
   State
   ═══════════════════════════════════════════════════════════════════ */

let results = null;
let searchDisabled = false;
let enteringPortal = false;
let typeTree = null;
let layerPalette = null;
let areaRouter = null;
let temporalMode = 'off';       // 'off' | 'range' | 'undated'
let searchMode = 'areas';       // 'areas' | 'toponyms'
let exactMatch = false;
let useViewport = false;       // viewport-constraint toggle (non-globe only)
let clusterResults = true;
let selectedRegions = [];        // Array of {id, label, admin_level, namespace, geometry}
let areaSearchResults = [];      // Current area search dropdown results
let areaDropdownIndex = -1;

/* ── Gazetteers offcanvas mode-state caches ──
   Keep Filter and Explore selections separately so switching tabs restores the
   last-recorded selection instead of dropping it on each input-type swap. */
const filterSelections = new Set();   // namespaces ticked in Filter mode (multi-select)
let exploreSelection = null;          // single namespace selected in Explore mode

/* ── Temporal range state ── */
let temporalFrom = 800;
let temporalTo = 1800;
const TEMPORAL_MIN = -2000;
const TEMPORAL_MAX = 2100;

/* ═══════════════════════════════════════════════════════════════════
   Init: Load data dependencies
   ═══════════════════════════════════════════════════════════════════ */

const countryParents = new CountryParents();
await countryParents.dataLoaded;

/* ═══════════════════════════════════════════════════════════════════
   Custom temporal range control
   ═══════════════════════════════════════════════════════════════════ */

let temporalRangeChanged = throttle(() => {
    filterState.set('temporal.start_year', temporalFrom);
    filterState.set('temporal.stop_year', temporalTo);
}, 300);

function fillTemporalSlider() {
    const fromSlider = document.getElementById('temporal_from_slider');
    const toSlider = document.getElementById('temporal_to_slider');
    if (!fromSlider || !toSlider) return;
    const range = TEMPORAL_MAX - TEMPORAL_MIN;
    const fromPct = ((temporalFrom - TEMPORAL_MIN) / range) * 100;
    const toPct = ((temporalTo - TEMPORAL_MIN) / range) * 100;
    toSlider.style.background = `linear-gradient(to right,
        #b0bec5 0%, #b0bec5 ${fromPct}%,
        #546e7a ${fromPct}%, #546e7a ${toPct}%,
        #b0bec5 ${toPct}%, #b0bec5 100%)`;
}

function updateTemporalLabels() {
    const fromLabel = document.getElementById('temporal_from_label');
    const toLabel = document.getElementById('temporal_to_label');
    if (fromLabel) fromLabel.textContent = temporalFrom;
    if (toLabel) toLabel.textContent = temporalTo;
}

function wireTemporalControl() {
    const fromSlider = document.getElementById('temporal_from_slider');
    const toSlider = document.getElementById('temporal_to_slider');
    if (!fromSlider || !toSlider) return;

    fromSlider.addEventListener('input', () => {
        temporalFrom = parseInt(fromSlider.value);
        if (temporalFrom >= temporalTo) {
            temporalTo = temporalFrom;
            toSlider.value = temporalTo;
        }
        updateTemporalLabels();
        fillTemporalSlider();
        temporalRangeChanged();
    });

    toSlider.addEventListener('input', () => {
        temporalTo = parseInt(toSlider.value);
        if (temporalTo <= temporalFrom) {
            temporalFrom = temporalTo;
            fromSlider.value = temporalFrom;
        }
        updateTemporalLabels();
        fillTemporalSlider();
        temporalRangeChanged();
    });

    // Temporal mode toggle
    document.querySelectorAll('#temporal_control .temporal-mode-toggle .btn').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('#temporal_control .temporal-mode-toggle .btn')
                .forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            temporalMode = btn.dataset.temporalMode;
            const tc = document.getElementById('temporal_control');
            tc.classList.toggle('temporal-off', temporalMode === 'off');
        });
    });

    // Initial fill
    fillTemporalSlider();
}

function resetTemporalControl() {
    temporalFrom = 800;
    temporalTo = 1800;
    temporalMode = 'off';
    const fromSlider = document.getElementById('temporal_from_slider');
    const toSlider = document.getElementById('temporal_to_slider');
    if (fromSlider) fromSlider.value = temporalFrom;
    if (toSlider) toSlider.value = temporalTo;
    updateTemporalLabels();
    fillTemporalSlider();
    document.querySelectorAll('#temporal_control .temporal-mode-toggle .btn')
        .forEach(b => b.classList.remove('active'));
    const offBtn = document.querySelector('#temporal_control .temporal-mode-toggle .btn[data-temporal-mode="off"]');
    if (offBtn) offBtn.classList.add('active');
    document.getElementById('temporal_control')?.classList.add('temporal-off');
}

/* ═══════════════════════════════════════════════════════════════════
   Map init
   ═══════════════════════════════════════════════════════════════════ */

function waitMapLoad() {
    return heroMap.init().then(() => {
        // Wire click on result features
        heroMap.map.on('click', function (e) {
            const features = heroMap.map.queryRenderedFeatures(e.point);
            if (features.length > 0 && features[0].layer.id.startsWith('places_')) {
                const idx = features[0].id;
                $('#atlas_search_results .result').eq(idx)
                    .attr('data-map-clicked', 'true').click();
            }
        });
        heroMap.map.on('mousemove', function (e) {
            const features = heroMap.map.queryRenderedFeatures(e.point);
            if (features.length > 0 && features[0].layer.id.startsWith('places_')) {
                heroMap.map.getCanvas().style.cursor = 'pointer';
            }
        });
    });
}

function waitDocumentReady() {
    return new Promise((resolve) => $(document).ready(() => resolve()));
}

// ── Welcome panel ──────────────────────────────────────────────────────────
// Persisted opt-out (mirrors atlasTour.js's TOUR_SEEN_KEY): once the user
// clicks "Don't show this again" the panel never returns, and the first-visit
// auto-tour is suppressed too (the tour stays relaunchable via the bottom-left
// button). The panel is shown immediately in the template, so its controls are
// wired on document-ready — NOT behind the map-load gate below — otherwise a
// slow/failed map load would trap the user behind an undismissable panel.
const WELCOME_DISMISSED_KEY = 'whg_atlas_welcome_dismissed';
function isWelcomeDismissed() {
    try { return localStorage.getItem(WELCOME_DISMISSED_KEY) === 'true'; }
    catch (e) { return false; }
}
// Shared so the map-drag handler (added after map load) can also fade the panel.
let fadeOutWelcome = () => {};

function setupWelcomePanel() {
    const welcomePanel = document.getElementById('atlas_welcome');
    if (!welcomePanel) return;
    if (isWelcomeDismissed()) {
        // Opted out: hide the welcome, but show a lightweight "please wait"
        // placeholder so a returning user still sees something while the map
        // loads. It auto-hides once the map is ready (map-gated .then() below);
        // a fallback timeout clears it if the map load never signals.
        welcomePanel.style.display = 'none';
        const loading = document.getElementById('atlas_loading');
        if (loading) {
            loading.style.display = '';
            setTimeout(() => { loading.style.display = 'none'; }, 20000);
        }
        return;
    }
    fadeOutWelcome = () => {
        welcomePanel.classList.add('atlas-welcome-hidden');
        welcomePanel.addEventListener('transitionend', () => {
            welcomePanel.style.display = 'none';
        }, { once: true });
    };

    // Dismiss button (this visit only)
    const dismissBtn = document.getElementById('atlas_welcome_dismiss');
    if (dismissBtn) dismissBtn.addEventListener('click', fadeOutWelcome);

    // "Don't show this again" — persist the opt-out, then fade out.
    const dontShowBtn = document.getElementById('atlas_welcome_dontshow');
    if (dontShowBtn) {
        dontShowBtn.addEventListener('click', () => {
            try { localStorage.setItem(WELCOME_DISMISSED_KEY, 'true'); } catch (e) { /* private mode */ }
            fadeOutWelcome();
        });
    }

    // Fade when any (already-present) control receives focus/click.
    const controlSelectors = [
        '#atlas_search_input',
        '.search-mode-toggle .btn',
        '.atlas-control-buttons .btn',
        '#temporal_control',
        '#atlas_initiate_search',
        '#atlas_exact_match',
        '#atlas_clear_search',
        '.maplibregl-ctrl-zoom-in',
        '.maplibregl-ctrl-zoom-out',
        '.maplibregl-ctrl-globe',
    ];
    controlSelectors.forEach(sel => {
        document.querySelectorAll(sel).forEach(el => {
            el.addEventListener('focus', fadeOutWelcome, { once: true });
            el.addEventListener('click', fadeOutWelcome, { once: true });
        });
    });

    // Tour link inside welcome panel
    const tourLink = document.getElementById('atlas_start_tour_link');
    if (tourLink) {
        tourLink.addEventListener('click', (e) => {
            e.preventDefault();
            fadeOutWelcome();
            setTimeout(() => startAtlasTour(), 400);
        });
    }
}
waitDocumentReady().then(setupWelcomePanel);

/* ═══════════════════════════════════════════════════════════════════
   DOM wiring — runs after map + DOM ready
   ═══════════════════════════════════════════════════════════════════ */

Promise.all([
    waitMapLoad(),
    waitDocumentReady(),
    Promise.all(select2_CDN_fallbacks.map(loadResource)),
]).then(() => {

    // Map is ready — clear the "please wait" placeholder (opted-out users).
    const atlasLoadingEl = document.getElementById('atlas_loading');
    if (atlasLoadingEl) atlasLoadingEl.style.display = 'none';

    // ── Start globe spin on load ──
    heroMap.startSpin();

    // ── Initialise Layer Sources palette (inside offcanvas) ──
    layerPalette = new LayerSourcesPalette(
        '#layer_sources_panel', null,
        typeof available_sources !== 'undefined' ? available_sources : []
    );

    // ── Initialise Area Search Router ──
    areaRouter = new AreaSearchRouter(layerPalette);

    // ── Initialise custom temporal range control ──
    wireTemporalControl();

    // ── Wire boundary click events ──
    document.addEventListener('boundary-click', (e) => {
        if (searchMode !== 'areas') return;
        const detail = e.detail;
        if (!detail || !detail.geometry) return;
        addRegionSelection({
            id: `boundary:${detail.namespace || 'osm'}:${detail.id || detail.name}`,
            label: detail.name || 'Unnamed',
            admin_level: detail.admin_level,
            namespace: detail.namespace || 'osm',
            geometry: detail.geometry,
        });
    });

    // ── Search mode toggle ──
    document.querySelectorAll('.search-mode-toggle .btn').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('.search-mode-toggle .btn')
                .forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            switchSearchMode(btn.dataset.searchMode);
        });
    });

    // ── Wire search input ──
    const searchInput = document.getElementById('atlas_search_input');
    const debouncedAreaSearch = debounce(() => performAreaSearch(), 250);

    searchInput.addEventListener('input', () => {
        if (searchMode === 'areas') {
            debouncedAreaSearch();
        }
    });

    searchInput.addEventListener('keydown', (e) => {
        if (searchMode === 'areas') {
            handleAreaDropdownKeydown(e);
        }
    });

    searchInput.addEventListener('keyup', (e) => {
        if (e.key === 'Enter' && searchMode === 'toponyms') {
            initiateToponymSearch();
        }
    });

    // ── Wire buttons ──
    document.getElementById('atlas_initiate_search').addEventListener('click', () => {
        if (searchMode === 'toponyms') initiateToponymSearch();
        else performAreaSearch();
    });

    document.getElementById('atlas_clear_search').addEventListener('click', () => {
        clearAll();
    });

    document.getElementById('atlas_close_results').addEventListener('click', () => {
        hideResultsPanel();
        switchSearchMode('areas');
    });

    // ── BETA: cluster merge-sensitivity (θ) slider — re-clusters live ──
    const thetaSlider = document.getElementById('atlas_cluster_theta');
    if (thetaSlider) {
        const thetaVal = document.getElementById('atlas_cluster_theta_val');
        const applyTheta = () => {
            clusterThetaOverride = parseFloat(thetaSlider.value);
            if (thetaVal) thetaVal.textContent = clusterThetaOverride.toFixed(2);
            if (gatewayData) renderClusters();
        };
        thetaSlider.addEventListener('input', debounce(applyTheta, 120));
    }

    // ── Wire exact match toggle ──
    document.getElementById('atlas_exact_match').addEventListener('click', function () {
        exactMatch = !exactMatch;
        this.classList.toggle('active', exactMatch);
        this.setAttribute('aria-pressed', exactMatch);
    });

    // ── Wire viewport constraint toggle ──
    const viewportBtn = document.getElementById('atlas_viewport_btn');
    const viewportWrap = document.getElementById('atlas_viewport_wrap');
    if (viewportBtn) {
        viewportBtn.addEventListener('click', function () {
            if (this.disabled) return;
            useViewport = !useViewport;
            this.classList.toggle('active', useViewport);
            updateViewportTooltip();

            // Hide boundaries when viewport mode is active; restore when deactivated
            if (useViewport) {
                heroMap.hideBoundaries();
            } else if (layerPalette) {
                layerPalette.refreshBoundaries();
            }
        });
    }
    // Click on the wrapper when the button is disabled → switch to flat map
    if (viewportWrap) {
        viewportWrap.addEventListener('click', function () {
            if (viewportBtn && viewportBtn.disabled && heroMap.isGlobeMode()) {
                heroMap.map.setProjection({ type: 'mercator' });
                // The projection-change listener will enable the button;
                // give it a moment then activate viewport constraint.
                setTimeout(() => {
                    if (viewportBtn && !viewportBtn.disabled) {
                        viewportBtn.click();
                    }
                }, 350);
            }
        });
    }

    // ── Listen for projection changes to enable/disable viewport button ──
    heroMap.onProjectionChange((isGlobe) => {
        updateViewportButtonState(isGlobe);
    });
    // Set initial state after a short delay (projection may need time to settle)
    setTimeout(() => updateViewportButtonState(heroMap.isGlobeMode()), 500);

    // ── Initialise the Filter-mode selection cache from the default-checked HTML state. ──
    document.querySelectorAll('#gazetteers_offcanvas .standard-gazetteers-list .authority-cb').forEach(cb => {
        if (cb.checked) filterSelections.add(cb.value);
    });

    // ── Wire authority checkboxes ──
    // Clustering is now controlled by the similarity-threshold slider at the top of
    // the Results panel (see Master Plan §1.3, §4.1, §4.2). The legacy
    // #atlas_clustering_toggle checkbox has been removed from atlas.html; clusterResults
    // remains as a constant `true` until the new slider replaces this signal end-to-end.
    //
    // Use event delegation so that authority controls in either the standard or the
    // My-Gazetteers placeholder list fire the same handler. Inputs are swapped between
    // type=checkbox (Filter mode) and type=radio (Explore mode) by setGazetteerMode;
    // the per-mode selection caches are kept in sync so flipping tabs restores state.
    document.querySelector('#gazetteers_offcanvas').addEventListener('change', (e) => {
        if (!e.target.classList.contains('authority-cb')) return;
        const offcanvasBody = document.querySelector('#gazetteers_offcanvas .offcanvas-body');
        const mode = (offcanvasBody && offcanvasBody.dataset.mode) || 'filter';

        if (e.target.closest('.specialist-list')) {
            // Specialist child change → recount parent tri-state.
            recountSpecialistTriState();
        } else if (e.target.closest('.specialist-gazetteers-parent')) {
            // Parent toggled directly (input or label) → propagate to children.
            applyParentToggleToChildren(e.target.checked);
        }
        emitGazetteerSelection(mode);
    });

    // ── Gazetteers offcanvas: Filter | Explore mode toggle (Master Plan §1.4) ──
    // Sketch only — backend support for the unified /suggest list and the Explorer view
    // arrives in Phases 2 and 4.
    document.querySelectorAll('#gazetteers_offcanvas .gazetteer-mode-toggle .btn').forEach(btn => {
        btn.addEventListener('click', () => setGazetteerMode(btn.dataset.gazetteerMode));
    });

    // ── Gazetteers offcanvas: My-Gazetteers toggle (Explore mode only) ──
    // Swaps between the standard gazetteer list and the (placeholder) per-user list
    // grouped into Published / Pending. Real data arrives via /suggest in Phase 2.
    const mineToggle = document.getElementById('gazetteer_mine_toggle');
    if (mineToggle) {
        mineToggle.addEventListener('change', () => updateGazetteerListVisibility());
    }

    // ── Gazetteers offcanvas: stub-note handler for unimplemented coverage filters. ──
    // Show the stub note when either Area or Period switch is on; hide when both off.
    document.querySelectorAll('#gazetteers_offcanvas .gazetteer-stub-switch').forEach(sw => {
        sw.addEventListener('change', () => {
            const card = sw.closest('.gazetteer-coverage-filters');
            if (!card) return;
            const note = card.querySelector('.gazetteer-stub-note');
            const anyOn = card.querySelectorAll('.gazetteer-stub-switch:checked').length > 0;
            if (note) note.classList.toggle('d-none', !anyOn);
        });
    });

    // ── Specialist Gazetteers: parent row click reveals the inline dropdown. ──
    // Lazy-render only; the actual tri-state propagation to children is handled
    // by the offcanvas change handler so that clicking either the input OR the
    // label text fires it.
    document.querySelectorAll('#gazetteers_offcanvas .specialist-gazetteers-parent').forEach(label => {
        label.addEventListener('click', () => {
            const expansion = document.querySelector('#gazetteers_offcanvas .specialist-gazetteers-expansion');
            if (!expansion) return;
            if (expansion.classList.contains('d-none')) {
                renderSpecialistList();
                expansion.classList.remove('d-none');
            }
        });
    });

    // ── Tileset gating for ``no_explore`` gazetteers (OSM, OHM in Explore mode). ──
    applyTilesetGating(document.querySelector('#gazetteers_offcanvas .offcanvas-body')?.dataset.mode || 'filter');

    // ── Initialise type tree in categories offcanvas ──
    typeTree = new TypeTreeWidget('#atlas_type_tree', {
        onchange: () => {
            const ids = typeTree.getSelectedIdentifiers();
            filterState.set('place_types', ids);
            updateTreeBadge();
        },
    });
    typeTree.init();

    document.getElementById('atlas_tree_clear').addEventListener('click', (e) => {
        e.preventDefault();
        if (typeTree) {
            typeTree.clearAll();
            filterState.set('place_types', []);
            updateTreeBadge();
        }
    });

    // ── Portal link handler ──
    $(document).on('click', '.portal-link', function (e) {
        enteringPortal = true;
        e.preventDefault();
        const id = $(this).data('whg-id') || $(this).data('pid');
        const path = $(this).data('whg-id') ? 'portal/' : 'detail';
        window.location.href = `/places/${id}/${path}`;
    });

    // ── Result item click handler ──
    $(document).on('click', '#atlas_search_results .result', function () {
        const $el = $(this);
        const index = $el.index('#atlas_search_results .result');

        heroMap.map.removeFeatureState({ source: 'places' });
        heroMap.map.setFeatureState({ source: 'places', id: index }, { highlight: true });

        const fc = heroMap.map.getSource('places')._data?.geojson;

        if ($el.attr('data-map-clicked') === 'true') {
            $el.removeAttr('data-map-clicked');
            scrollToResult($el);
        } else if (fc?.features?.length > index) {
            heroMap.map.fitViewport(bbox(fc.features[index]), { maxZoom: 12, padding: 60 });
        }

        $('#atlas_search_results .result').removeClass('selected');
        $el.addClass('selected');
    });

    // ── Handle pre-populated toponym from URL ──
    if (typeof atlas_toponym !== 'undefined' && atlas_toponym) {
        searchInput.value = atlas_toponym;
        switchSearchMode('toponyms');
        setTimeout(() => initiateToponymSearch(), 300);
    }

    // ── Handle Gazetteers deep link from sitewide navbar ──
    // /atlas/?panel=gazetteers&gmode=explore opens the Atlas in Places mode
    // with the Gazetteers offcanvas in view and the requested Filter|Explore
    // tab pre-selected. We trigger native click() on the existing buttons so
    // that all wired side-effects (.active class updates, etc.) fire exactly
    // as if the user had clicked them by hand.
    {
        const params = new URLSearchParams(location.search);
        if (params.get('panel') === 'gazetteers') {
            const placesBtn = document.querySelector(
                '.search-mode-toggle .btn[data-search-mode="toponyms"]'
            );
            if (placesBtn) placesBtn.click();
            // The toponym-only-btn group (#open_gazetteers_modal, #open_categories_modal)
            // is unhidden by switchSearchMode; clicking the trigger opens the offcanvas
            // via Bootstrap's data-bs-target wiring.
            setTimeout(() => {
                const trigger = document.getElementById('open_gazetteers_modal');
                if (trigger) trigger.click();
                const gmode = params.get('gmode') === 'explore' ? 'explore' : 'filter';
                if (gmode === 'explore') {
                    const exploreBtn = document.querySelector(
                        '#gazetteers_offcanvas .gazetteer-mode-toggle .btn[data-gazetteer-mode="explore"]'
                    );
                    if (exploreBtn) exploreBtn.click();
                }
            }, 50);
        }
    }

    // ── Welcome panel: fade on map interaction ──
    // The panel's own controls (dismiss, "don't show again", control-focus
    // fade, tour link) are wired on document-ready in setupWelcomePanel(),
    // independent of map load; here we add the map-drag fade now the map exists.
    heroMap.map.on('mousedown', fadeOutWelcome);
    heroMap.map.on('touchstart', fadeOutWelcome);

    // ── Tour relaunch button (bottom-left) ──
    const tourBtn = document.getElementById('atlas_tour_btn');
    if (tourBtn) {
        tourBtn.addEventListener('click', () => startAtlasTour());
    }

    // ── Auto-start tour on first visit ──
    if (!hasSeenAtlasTour() && !isWelcomeDismissed() && !(typeof atlas_toponym !== 'undefined' && atlas_toponym)) {
        // Delay slightly to let the map finish rendering
        setTimeout(() => {
            const wp = document.getElementById('atlas_welcome');
            if (wp) {
                wp.classList.add('atlas-welcome-hidden');
                wp.addEventListener('transitionend', () => {
                    wp.style.display = 'none';
                }, { once: true });
            }
            startAtlasTour();
        }, 1500);
    }

    // ── Close area dropdown on outside click ──
    document.addEventListener('click', (e) => {
        const dropdown = document.querySelector('.atlas-region-dropdown');
        if (dropdown && !dropdown.contains(e.target) &&
            e.target.id !== 'atlas_search_input') {
            closeAreaDropdown();
        }
    });

}).catch(error => console.error('Atlas init error:', error));

/* ═══════════════════════════════════════════════════════════════════
   Helper functions
   ═══════════════════════════════════════════════════════════════════ */

function updateTreeBadge() {
    if (!typeTree) return;
    const count = typeTree.selectionCount();
    const $badge = $('#atlas_tree_badge');
    if (count > 0) $badge.text(count + ' selected').show();
    else $badge.hide();
}

function switchSearchMode(mode) {
    searchMode = mode;
    const input = document.getElementById('atlas_search_input');
    const toponymBtns = document.querySelectorAll('.toponym-only-btn');
    const areasBtns = document.querySelectorAll('.areas-only-btn');

    if (mode === 'areas') {
        input.placeholder = buildAreasPlaceholder();
        toponymBtns.forEach(btn => btn.style.display = 'none');
        areasBtns.forEach(btn => btn.style.display = '');
        hideResultsPanel();
        heroMap.clearResultFeatures();

        // Ensure whg-context style is active for area search
        heroMap.ensureContextStyle();
    } else {
        const chipLabels = selectedRegions.map(r => r.label).join(', ');
        if (useViewport && !heroMap.isGlobeMode()) {
            input.placeholder = 'Search within viewport…';
        } else if (chipLabels) {
            input.placeholder = `Search within ${chipLabels}…`;
        } else {
            input.placeholder = 'Search for place names…';
        }
        toponymBtns.forEach(btn => btn.style.display = '');
        areasBtns.forEach(btn => btn.style.display = 'none');
    }
    input.value = '';
    closeAreaDropdown();
}

function buildAreasPlaceholder() {
    if (layerPalette && layerPalette.getAdminLevel() !== null) {
        return `Search for regions…`;
    }
    return 'Search for regions…';
}

/**
 * Switch the Gazetteers offcanvas between Filter and Explore mode (Master Plan §1.4).
 *
 * Sketch only. This updates the visible UI (active tab, mode-specific help copy,
 * Explore-mode-only controls) and physically swaps every authority input between
 * type=checkbox (Filter mode, multi-select) and type=radio (Explore mode,
 * single-select via shared `name` attribute). Selection state is preserved per mode
 * via filterSelections / exploreSelection so flipping tabs restores the previously
 * recorded selection rather than dropping it on the type swap.
 */
function setGazetteerMode(mode) {
    if (mode !== 'filter' && mode !== 'explore') return;
    const offcanvas = document.getElementById('gazetteers_offcanvas');
    if (!offcanvas) return;

    const body = offcanvas.querySelector('.offcanvas-body');
    const previousMode = (body && body.dataset.mode) || 'filter';
    if (mode === previousMode) return;

    // ── Save current selection into the cache for the previous mode. ──
    const inputs = offcanvas.querySelectorAll('.authority-cb');
    const currentChecked = Array.from(inputs).filter(i => i.checked).map(i => i.value);
    if (previousMode === 'filter') {
        filterSelections.clear();
        currentChecked.forEach(v => filterSelections.add(v));
    } else {
        exploreSelection = currentChecked[0] || null;
    }

    if (body) body.dataset.mode = mode;

    offcanvas.querySelectorAll('.gazetteer-mode-toggle .btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.gazetteerMode === mode);
    });

    offcanvas.querySelectorAll('.gazetteer-mode-help').forEach(p => {
        p.classList.toggle('d-none', p.dataset.mode !== mode);
    });

    offcanvas.querySelectorAll('[data-mode-visible]').forEach(el => {
        el.classList.toggle('d-none', el.dataset.modeVisible !== mode);
    });

    // ── Swap input type to match the mode, then apply that mode's saved selection. ──
    // Two-pass to avoid browser quirks where setting `.checked` in the same iteration
    // that mutates `.type` can be silently dropped (the type change can reset state).
    if (mode === 'explore') {
        // Pass 1: convert every input to a radio in a shared name group, all unchecked.
        inputs.forEach(input => {
            input.type = 'radio';
            input.name = 'gazetteer_explore';
            input.checked = false;
        });
        // Pass 2: tick the one matching the saved Explore-mode selection (if any).
        if (exploreSelection !== null) {
            const target = Array.from(inputs).find(i => i.value === exploreSelection);
            if (target) target.checked = true;
        }
    } else {
        // Pass 1: convert every input to a checkbox, all unchecked.
        inputs.forEach(input => {
            input.type = 'checkbox';
            input.removeAttribute('name');
            input.checked = false;
        });
        // Pass 2: tick everything that was in the Filter-mode selection cache.
        inputs.forEach(input => {
            if (filterSelections.has(input.value)) input.checked = true;
        });
    }

    // The My-Gazetteers toggle is Explore-mode-only; reset it when leaving Explore.
    if (mode === 'filter') {
        const mineToggle = document.getElementById('gazetteer_mine_toggle');
        if (mineToggle) mineToggle.checked = false;
    }
    updateGazetteerListVisibility();

    // Gazetteers flagged ``no_explore`` (e.g. OSM/OHM) are disabled in Explore
    // mode and re-enabled in Filter mode.
    applyTilesetGating(mode);

    // Re-apply type-pill filter visibility & state if the Specialist expansion
    // is open (the [data-mode-visible] loop above has already toggled the row's
    // visibility, but the underlying child filtering must re-run).
    applyTypePillFilter();

    // Mirror the resulting selection into filterState.
    emitGazetteerSelection(mode);
}

/**
 * Show the standard gazetteer list or the (placeholder) My-Gazetteers list, depending
 * on the My-Gazetteers toggle. Only meaningful in Explore mode for authenticated users.
 */
function updateGazetteerListVisibility() {
    const offcanvas = document.getElementById('gazetteers_offcanvas');
    if (!offcanvas) return;
    const standardList = offcanvas.querySelector('.standard-gazetteers-list');
    const myList = offcanvas.querySelector('.my-gazetteers-list');
    if (!standardList) return;

    const mineToggle = document.getElementById('gazetteer_mine_toggle');
    const showMine = !!(mineToggle && mineToggle.checked && myList);

    standardList.classList.toggle('d-none', showMine);
    if (myList) myList.classList.toggle('d-none', !showMine);
}

/* ── Explore-mode gating for ``no_explore`` gazetteers ──
   Disables the input and visually greys the row in Explore mode; restores it in
   Filter mode. ``no_explore`` is set on the GazetteerRegistryEntry for sources
   whose tilesets are not browsable in Explore mode (currently OSM and OHM).
   The original tooltip is preserved on a data-bs-title-original attribute so it
   can be swapped back when leaving Explore. */
function applyTilesetGating(mode) {
    const labels = document.querySelectorAll(
        '#gazetteers_offcanvas .authority-item[data-no-explore="1"]'
    );
    const disabled = (mode === 'explore');
    labels.forEach(label => {
        const input = label.querySelector('.authority-cb');
        if (input) {
            input.disabled = disabled;
            if (disabled) input.checked = false;
        }
        label.classList.toggle('disabled', disabled);
        if (!label.dataset.bsTitleOriginal) {
            label.dataset.bsTitleOriginal = label.getAttribute('data-bs-title') || '';
        }
        const newTitle = disabled
            ? 'This gazetteer is not available in Explore mode'
            : label.dataset.bsTitleOriginal;
        label.setAttribute('data-bs-title', newTitle);
        label.setAttribute('data-bs-original-title', newTitle);
        try {
            const tt = bootstrap.Tooltip.getInstance(label);
            if (tt) tt.setContent({ '.tooltip-inner': newTitle });
        } catch (e) { /* no tooltip yet — picked up on next init */ }
    });
    // Clear a stale Explore selection that now points at a disabled row.
    if (disabled && exploreSelection) {
        const sel = document.querySelector(
            `#gazetteers_offcanvas .authority-cb[value="${CSS.escape(exploreSelection)}"]`
        );
        if (sel && sel.disabled) exploreSelection = null;
    }
}

/* ── Specialist Gazetteers — lazy render of children, tri-state, search, type pills ──
   The expansion is hidden by default and rendered on first parent-row interaction.
   Children are rendered with .authority-cb so the existing offcanvas change
   delegation routes them through recountSpecialistTriState. */
let _specialistRendered = false;

function getSpecialistData() {
    const tag = document.getElementById('specialist_gazetteers_data');
    if (!tag) return [];
    try {
        return JSON.parse(tag.textContent) || [];
    } catch (e) {
        return [];
    }
}

function renderSpecialistList() {
    if (_specialistRendered) return;
    const list = document.querySelector('#gazetteers_offcanvas .specialist-list');
    if (!list) return;
    const data = getSpecialistData();
    if (!data.length) {
        list.innerHTML = '<p class="small text-muted fst-italic mb-0">No specialist gazetteers registered yet.</p>';
        _specialistRendered = true;
        return;
    }
    const offcanvasBody = document.querySelector('#gazetteers_offcanvas .offcanvas-body');
    const mode = (offcanvasBody && offcanvasBody.dataset.mode) || 'filter';
    const inputType = mode === 'explore' ? 'radio' : 'checkbox';
    const nameAttr = mode === 'explore' ? ' name="gazetteer_explore"' : '';
    const html = data.map(g => {
        const desc = (g.description || g.name || '').replace(/"/g, '&quot;');
        const type = g.gazetteer_type || 'standard';
        return `
            <label class="authority-item form-check"
                   data-bs-toggle="tooltip"
                   data-bs-title="${desc}"
                   data-gazetteer-type="${type}"
                   data-specialist-id="${g.id}">
                <input class="form-check-input authority-cb" type="${inputType}"${nameAttr}
                       value="${g.id}">
                <span class="form-check-label">${g.name}</span>
            </label>
        `;
    }).join('');
    list.innerHTML = html;
    _specialistRendered = true;

    // Wire the search input (once).
    const search = document.querySelector('#gazetteers_offcanvas .specialist-search');
    if (search && !search.dataset.wired) {
        search.dataset.wired = '1';
        search.addEventListener('input', () => {
            const q = search.value.trim().toLowerCase();
            list.querySelectorAll('label.authority-item').forEach(l => {
                const text = (l.textContent || '').toLowerCase();
                l.classList.toggle('d-none', q && !text.includes(q));
            });
            applyTypePillFilter();
            recountSpecialistTriState();
        });
    }

    // Wire the type-pill filter (once).
    document.querySelectorAll('#gazetteers_offcanvas .gazetteer-type-pill-filter [data-gazetteer-type-pill]').forEach(btn => {
        if (btn.dataset.wired) return;
        btn.dataset.wired = '1';
        btn.addEventListener('click', () => {
            btn.classList.toggle('active');
            applyTypePillFilter();
            recountSpecialistTriState();
        });
    });

    applyTypePillFilter();
    recountSpecialistTriState();
}

function applyParentToggleToChildren(checkedAll) {
    const children = document.querySelectorAll(
        '#gazetteers_offcanvas .specialist-list .authority-cb'
    );
    children.forEach(cb => {
        // Only toggle visible (non-d-none, non-disabled) children.
        const label = cb.closest('label');
        if (label && label.classList.contains('d-none')) return;
        cb.checked = !!checkedAll;
    });
    recountSpecialistTriState();
}

function recountSpecialistTriState() {
    const parentInput = document.querySelector(
        '#gazetteers_offcanvas .specialist-gazetteers-parent .authority-cb'
    );
    if (!parentInput) return;
    // Count *all* children regardless of search/pill visibility so that the
    // 'whg' alias is only emitted when literally every Specialist Gazetteer
    // is selected, never when filtering merely hides some rows.
    const allChildren = Array.from(document.querySelectorAll(
        '#gazetteers_offcanvas .specialist-list .authority-cb'
    ));
    if (!allChildren.length) {
        parentInput.indeterminate = false;
        return;
    }
    const checkedCount = allChildren.filter(cb => cb.checked).length;
    if (checkedCount === 0) {
        parentInput.checked = false;
        parentInput.indeterminate = false;
    } else if (checkedCount === allChildren.length) {
        parentInput.checked = true;
        parentInput.indeterminate = false;
    } else {
        parentInput.checked = false;
        parentInput.indeterminate = true;
    }
}

/* ── Type-pill filter (Explore-only sketch) ──
   Hides Specialist children whose data-gazetteer-type is not in the active pill
   set. Convention: when *all* pills are off, fall back to "show all" (matches
   the page's existing area/region selection convention). */
function applyTypePillFilter() {
    const filterRow = document.querySelector('#gazetteers_offcanvas .gazetteer-type-pill-filter');
    if (!filterRow) return;
    const list = document.querySelector('#gazetteers_offcanvas .specialist-list');
    if (!list) return;
    // If filter row is hidden (Filter mode), force show all.
    const hidden = filterRow.classList.contains('d-none');
    const activePills = hidden
        ? null
        : Array.from(filterRow.querySelectorAll('[data-gazetteer-type-pill].active'))
            .map(b => b.dataset.gazetteerTypePill);
    const showAll = !activePills || activePills.length === 0;
    list.querySelectorAll('label.authority-item').forEach(label => {
        if (showAll) {
            // Restore visibility unless the search-box has hidden this row.
            // (We can't distinguish source-of-hide; safe default: show.)
            label.classList.remove('d-none');
            return;
        }
        const t = label.dataset.gazetteerType || 'standard';
        label.classList.toggle('d-none', !activePills.includes(t));
    });
}

/* ── Mirror the active gazetteer selection into filterState ──
   Tri-state: parent fully checked → 'whg' (compact alias for "all WHG datasets");
   parent indeterminate → explicit list of child specialist ids; parent unchecked
   → no Specialist contribution. Standard authority selections are always sent
   verbatim. */
function emitGazetteerSelection(mode) {
    const offcanvas = document.getElementById('gazetteers_offcanvas');
    if (!offcanvas) return;
    // Standard list values only — never include specialist children here, since
    // the parent's tri-state below decides whether to expand to explicit ids.
    const standardChecked = Array.from(
        offcanvas.querySelectorAll('.standard-gazetteers-list > .authority-item .authority-cb:checked')
    ).map(el => el.value);

    const parentInput = offcanvas.querySelector(
        '.specialist-gazetteers-parent .authority-cb'
    );
    let composed = standardChecked.slice();
    if (parentInput && parentInput.indeterminate) {
        // Drop the bare 'whg' alias (the parent isn't fully checked) and
        // substitute the explicit list of selected child specialist ids.
        composed = composed.filter(v => v !== 'whg');
        const childIds = Array.from(offcanvas.querySelectorAll(
            '.specialist-list .authority-cb:checked'
        )).map(el => el.value);
        composed = composed.concat(childIds);
    }
    if (mode === 'filter') {
        filterSelections.clear();
        composed.forEach(v => filterSelections.add(v));
    } else {
        exploreSelection = composed[0] || null;
        // Mirror the Explore selection onto the map: load the gazetteer's
        // tileset and remove any others. Specialist gazetteers (e.g. ``whg:892``)
        // resolve to their tileserver name (``whg-892``) inside setActiveSource.
        if (layerPalette && exploreSelection) {
            layerPalette.setActiveSource(exploreSelection);
        }
    }
    filterState.set('authorities', composed);
}

/* ── Viewport constraint helpers ── */

function updateViewportButtonState(isGlobe) {
    const btn = document.getElementById('atlas_viewport_btn');
    if (!btn) return;
    if (isGlobe) {
        // Disable viewport in globe mode
        btn.disabled = true;
        btn.style.pointerEvents = 'none';
        if (useViewport) {
            useViewport = false;
            btn.classList.remove('active');
            // Restore boundaries when viewport is auto-disabled
            if (layerPalette) layerPalette.refreshBoundaries();
        }
    } else {
        btn.disabled = false;
        btn.style.pointerEvents = '';
    }
    updateViewportTooltip();
}

function updateViewportTooltip() {
    const btn = document.getElementById('atlas_viewport_btn');
    const wrap = document.getElementById('atlas_viewport_wrap');
    if (!btn || !wrap) return;
    let text;
    if (btn.disabled) {
        text = 'Switch to flat map projection to enable viewport constraint';
    } else if (btn.classList.contains('active')) {
        text = 'Viewport constraint active — search will be limited to the visible map area. Click to disable.';
    } else {
        text = 'Constrain search to the current map viewport (clears any selected areas)';
    }
    wrap.setAttribute('title', text);
    wrap.setAttribute('data-bs-original-title', text);
    // Update Bootstrap tooltip if initialised on the wrapper
    try {
        const tt = bootstrap.Tooltip.getInstance(wrap);
        if (tt) tt.setContent({'.tooltip-inner': text});
    } catch (e) { /* */ }
}


/* ── Area search (Explorer mode) ── */

async function performAreaSearch() {
    const query = document.getElementById('atlas_search_input').value.trim();
    if (query.length < 2) {
        closeAreaDropdown();
        return;
    }

    const results = await areaRouter.search(query, {
        adminLevel: layerPalette ? layerPalette.getAdminLevel() : null,
        namespace: layerPalette ? layerPalette.getNamespace() : 'osm',
    });

    areaSearchResults = results;
    areaDropdownIndex = -1;

    if (results.length === 0) {
        renderAreaDropdown([{
            _stub: true,
            label: 'No matching areas found',
            sublabel: 'Try a different name or adjust your admin level',
        }]);
    } else {
        renderAreaDropdown(results);
    }
}

function renderAreaDropdown(items) {
    // Remove existing dropdown
    closeAreaDropdown();

    const dropdown = document.createElement('div');
    dropdown.className = 'atlas-region-dropdown';

    dropdown.innerHTML = items.map((item, i) => `
        <div class="region-result ${item._stub ? 'region-result--stub' : ''}" data-index="${i}">
            <div class="region-result-label">${item.label || ''}</div>
            ${item.sublabel ? `<div class="region-result-sublabel">${item.sublabel}</div>` : ''}
            ${item.source_type ? `<span class="badge bg-secondary" style="font-size:0.6rem">${item.source || ''}</span>` : ''}
        </div>
    `).join('');

    // Mount below the floating search
    document.getElementById('floating_search').appendChild(dropdown);

    // Wire clicks
    dropdown.querySelectorAll('.region-result:not(.region-result--stub)').forEach(el => {
        el.addEventListener('mousedown', (e) => {
            e.preventDefault();
            selectAreaResult(parseInt(el.dataset.index));
        });
    });
}

function closeAreaDropdown() {
    document.querySelectorAll('.atlas-region-dropdown').forEach(el => el.remove());
    areaDropdownIndex = -1;
}

function handleAreaDropdownKeydown(e) {
    const items = document.querySelectorAll('.atlas-region-dropdown .region-result:not(.region-result--stub)');
    if (!items.length) return;

    if (e.key === 'ArrowDown') {
        e.preventDefault();
        areaDropdownIndex = Math.min(areaDropdownIndex + 1, items.length - 1);
        highlightAreaDropdown(items);
    } else if (e.key === 'ArrowUp') {
        e.preventDefault();
        areaDropdownIndex = Math.max(areaDropdownIndex - 1, 0);
        highlightAreaDropdown(items);
    } else if (e.key === 'Enter' && areaDropdownIndex >= 0) {
        e.preventDefault();
        selectAreaResult(areaDropdownIndex);
    } else if (e.key === 'Escape') {
        closeAreaDropdown();
    }
}

function highlightAreaDropdown(items) {
    items.forEach((el, i) => {
        el.classList.toggle('region-result--active', i === areaDropdownIndex);
    });
}

function selectAreaResult(index) {
    const item = areaSearchResults[index];
    if (!item) return;

    if (item._fromIndex && !item.geometry && item.bounds) {
        // Zoom to bounds so user can click polygon on map
        const [west, south, east, north] = item.bounds;
        try {
            heroMap.map.fitBounds([[west, south], [east, north]], {
                padding: 40, maxZoom: 8,
            });
        } catch (e) { /* */ }
        document.getElementById('atlas_search_input').value = '';
        closeAreaDropdown();
        return;
    }

    if (item.geometry) {
        addRegionSelection({
            id: item.id,
            label: item.label,
            admin_level: item.admin_level,
            namespace: item.namespace || 'osm',
            geometry: item.geometry,
        });
    } else if (item.bounds) {
        // Zoom to bounds so user can click on map
        const [west, south, east, north] = item.bounds;
        heroMap.map.fitBounds([[west, south], [east, north]], { padding: 40, maxZoom: 8 });
    }

    document.getElementById('atlas_search_input').value = '';
    closeAreaDropdown();
}

/* ── Region selections ── */

function addRegionSelection(item) {
    if (selectedRegions.some(r => r.id === item.id)) return;
    selectedRegions.push(item);

    // Update overlay on map
    updateSelectionOverlay();
    if (item.geometry) {
        heroMap.fitTo({ type: 'Feature', geometry: item.geometry, properties: {} });
    }

    renderSelectionChips();
}

function removeRegionSelection(id) {
    selectedRegions = selectedRegions.filter(r => r.id !== id);
    if (selectedRegions.length === 0) {
        heroMap.clearOverlay();
    } else {
        updateSelectionOverlay();
    }
    renderSelectionChips();
}

function updateSelectionOverlay() {
    const features = selectedRegions
        .filter(r => r.geometry)
        .map(r => ({
            type: 'Feature',
            geometry: r.geometry,
            properties: { id: r.id, label: r.label },
        }));
    if (features.length > 0) {
        heroMap.setOverlay({ type: 'FeatureCollection', features });
    } else {
        heroMap.clearOverlay();
    }
}

function renderSelectionChips() {
    const container = document.getElementById('selection_chips');
    if (selectedRegions.length === 0) {
        container.innerHTML = '';
        return;
    }
    container.innerHTML = selectedRegions.map(item => `
        <span class="filter-chip" data-region-id="${item.id}">
            <i class="fas fa-vector-square me-1"></i>
            ${escapeHtml(item.label)}
            ${item.admin_level != null ? `<span class="filter-chip-meta">(level ${item.admin_level})</span>` : ''}
            <button type="button" class="filter-chip-dismiss" aria-label="Remove" data-dismiss-id="${item.id}">
                <i class="fas fa-times"></i>
            </button>
        </span>
    `).join(' ');

    container.querySelectorAll('.filter-chip-dismiss').forEach(btn => {
        btn.addEventListener('click', () => removeRegionSelection(btn.dataset.dismissId));
    });
}

/* ── Toponym search ── */

function initiateToponymSearch() {
    if (searchDisabled) return;
    const input = document.getElementById('atlas_search_input');
    const qstr = input.value.trim();

    if (!qstr && !(typeTree && typeTree.selectionCount() > 0)) {
        console.log('Atlas: need a search term or type filter');
        return;
    }

    const options = gatherToponymOptions(qstr);
    console.log('Atlas: initiating toponym search', options);

    showResultsPanel();
    const resultsDiv = document.getElementById('atlas_search_results');
    resultsDiv.innerHTML = '<div class="p-3 text-center"><i class="fas fa-spinner fa-spin"></i> Searching…</div>';

    // BETA: route through the CRC gateway (/atlas/search/) for client-side
    // clustering. Non-beta users stay on the legacy Django /search/index/ path.
    if (isBetaUser()) {
        initiateGatewaySearch(options);
        return;
    }

    $.ajax({
        type: 'POST',
        url: '/search/index/',
        data: JSON.stringify(options),
        contentType: 'application/json',
        headers: { 'X-CSRFToken': csrfToken },
        success: function (data) {
            console.log('Atlas: search completed', data);
            renderToponymResults(data);
        },
        error: function (error) {
            console.error('Atlas: search error', error);
            resultsDiv.innerHTML = '<div class="p-3 text-danger">Search failed. Please try again.</div>';
        },
    });
}

// ── BETA: gateway-routed clustered search ───────────────────────────────────
// The gateway ships the clustering fuel (edges[], per-hit fields,
// clustering_params); clustering.js clusters it in the browser and we show
// cluster cards. The θ slider re-clusters the cached response live (no refetch).
let gatewayData = null;            // cached last gateway response
let clusterThetaOverride = null;   // θ slider value, or null → use params default

function isBetaUser() {
    const m = document.querySelector('meta[name="beta-user"]');
    return !!(m && m.content === '1');
}

function initiateGatewaySearch(options) {
    const resultsDiv = document.getElementById('atlas_search_results');
    $.ajax({
        type: 'POST',
        url: '/atlas/search/',
        data: JSON.stringify(options),
        contentType: 'application/json',
        headers: { 'X-CSRFToken': csrfToken },
        success: (data) => {
            gatewayData = data;
            clusterThetaOverride = null;
            const slider = document.getElementById('atlas_cluster_theta');
            if (slider && data.clustering_params && data.clustering_params.thresholds) {
                slider.value = data.clustering_params.thresholds.theta_query ?? slider.value;
            }
            renderClusters();
        },
        error: (err) => {
            console.error('Atlas: gateway search error', err);
            resultsDiv.innerHTML = '<div class="p-3 text-danger">Search failed. Please try again.</div>';
        },
    });
}

function hitsToFeatureCollection(hits, assignments) {
    const features = [];
    hits.forEach((h, i) => {
        let geometry = null;
        if (Array.isArray(h.geometries) && h.geometries.length) {
            geometry = h.geometries.length === 1
                ? h.geometries[0]
                : { type: 'GeometryCollection', geometries: h.geometries };
        } else if (Array.isArray(h.repr_point) && h.repr_point.length >= 2) {
            geometry = { type: 'Point', coordinates: [h.repr_point[0], h.repr_point[1]] };
        }
        if (!geometry) return;
        features.push({
            type: 'Feature',
            id: i,
            geometry,
            properties: {
                pid: h.place_id,
                title: h.title,
                namespace: h.namespace,
                cluster: assignments ? assignments.get(h.place_id) : null,
            },
        });
    });
    return { type: 'FeatureCollection', features };
}

function renderClusters() {
    if (!gatewayData) return;
    const hits = gatewayData.hits || [];
    const $resultsDiv = $('#atlas_search_results');
    $resultsDiv.empty();

    document.getElementById('atlas_no_results').style.display = hits.length === 0 ? 'block' : 'none';
    const controls = document.getElementById('atlas_cluster_controls');
    if (controls) controls.style.display = hits.length ? '' : 'none';

    const { clusters, assignments, params, debug } = clusterHits({
        hits,
        edges: gatewayData.edges || [],
        params: gatewayData.clustering_params || undefined,
        theta: clusterThetaOverride == null ? undefined : clusterThetaOverride,
    });
    console.log('Atlas cluster', debug, params);

    const countEl = document.getElementById('atlas_results_count');
    countEl.textContent = `${hits.length} place${hits.length !== 1 ? 's' : ''} · `
        + `${clusters.length} cluster${clusters.length !== 1 ? 's' : ''}`;

    clusters.forEach((cluster, ci) => {
        const members = cluster.members;
        const rep = members[0] || {};
        const multi = members.length > 1;
        const namespaces = [...new Set(members.map(m => m.namespace).filter(Boolean))];
        let html = `<div class="cluster-card${multi ? ' cluster-multi' : ''}" data-cluster="${ci}">`;
        html += `<div class="cluster-head">
            <span class="cluster-title">${escapeHtml(rep.title || '(untitled)')}</span>`;
        if (multi) {
            html += `<span class="cluster-badge" title="places merged into this cluster">`
                + `${members.length}<i class="fas fa-layer-group ms-1"></i></span>`;
        }
        html += `</div>`;
        if (namespaces.length) {
            html += `<div class="cluster-namespaces">`
                + namespaces.map(n => `<span class="ns-chip">${escapeHtml(n)}</span>`).join('')
                + `</div>`;
        }
        if (multi) {
            html += `<div class="cluster-members">`;
            members.forEach(m => {
                html += `<div class="cluster-member">`
                    + `<span class="member-title">${escapeHtml(m.title || m.place_id)}</span>`
                    + `<span class="member-ns">${escapeHtml(m.namespace || '')}</span>`
                    + `</div>`;
            });
            html += `</div>`;
        }
        html += `</div>`;
        $resultsDiv.append(html);
    });

    // Plot on the hero map
    const fc = hitsToFeatureCollection(hits, assignments);
    heroMap.showResultFeatures(fc);
    if (fc.features.length > 0) {
        heroMap.map.fitViewport(bbox(fc), {
            maxZoom: 12, padding: { top: 80, right: 400, bottom: 60, left: 80 },
        });
    }
}

function gatherToponymOptions(qstr) {
    const treeIds = typeTree ? typeTree.getSelectedIdentifiers() : [];

    // Build spatial constraint: viewport takes precedence over area selections
    let bounds;
    let spatialMode;

    if (useViewport && !heroMap.isGlobeMode()) {
        // Viewport constraint: use current map viewport as a bounding polygon
        const bb = heroMap.getBBox(); // [west, south, east, north]
        if (bb) {
            bounds = {
                type: 'Polygon',
                coordinates: [[
                    [bb[0], bb[1]],
                    [bb[2], bb[1]],
                    [bb[2], bb[3]],
                    [bb[0], bb[3]],
                    [bb[0], bb[1]],
                ]],
            };
            spatialMode = 'region';
        } else {
            bounds = { type: 'GeometryCollection', geometries: [] };
            spatialMode = 'none';
        }
    } else {
        // Area-selection constraint
        const regionGeometries = selectedRegions
            .filter(r => r.geometry)
            .map(r => r.geometry);
        bounds = regionGeometries.length > 0
            ? { type: 'GeometryCollection', geometries: regionGeometries }
            : { type: 'GeometryCollection', geometries: [] };
        spatialMode = regionGeometries.length > 0 ? 'region' : 'none';
    }

    return {
        qstr: qstr,
        idx: eswhg,
        fclasses: treeIds.join(','),
        types: treeIds,
        temporal: temporalMode !== 'off',
        start: temporalFrom,
        end: temporalTo,
        undated: temporalMode === 'undated',
        exact: exactMatch,
        cluster: clusterResults,
        bounds: bounds,
        regions: [],
        countries: [],
        userareas: [],
        spatial: spatialMode,
    };
}

function renderToponymResults(data) {
    const $resultsDiv = $('#atlas_search_results');
    $resultsDiv.empty();

    let featureCollection = data.features ? data : geomsGeoJSON(data['suggestions']);
    results = featureCollection.features;

    const countEl = document.getElementById('atlas_results_count');
    countEl.textContent = `${results.length} result${results.length !== 1 ? 's' : ''}`;

    const noResultsEl = document.getElementById('atlas_no_results');
    noResultsEl.style.display = results.length === 0 ? 'block' : 'none';

    results.forEach((feature, index) => {
        let r = feature.properties;
        const count = parseInt(r.linkcount) + 1;
        const pid = r.pid;
        const whg_id = r.whg_id;
        const children = r.children;
        const encodedChildren = encodeURIComponent(children.join(','));
        let resultIdx = count > 1 ? 'whg' : 'pub';

        let html = `<div class="result ${resultIdx}-result">
            <span>
                <span class="red-head">${r.title}</span>
                <span class="float-end small">
                    ${resultIdx === 'pub'
                        ? '<i class="fas fa-chain-broken" title="Unlinked"></i>'
                        : (count > 1 ? `${count} linked <i class="fas fa-link"></i>` : '')}
                    <button class="btn btn-primary btn-sm m-1 portal-link"
                            data-whg-id="${whg_id}" data-pid="${pid}"
                            data-children="${encodedChildren}">Details</button>
                </span>
            </span>`;

        if (r.dataset) {
            html += `<span class="result-dataset-badge">${r.dataset}</span>`;
        }

        if (r.descriptions && r.descriptions.length > 0) {
            html += `<p class="result-description more-or-less">${r.descriptions[0].value}</p>`;
        }

        if (r.names && r.names.length > 0) {
            const nameItems = r.names
                .filter(n => n.toponym && n.toponym !== r.title)
                .map(n => n.toponym);
            if (nameItems.length > 0) {
                html += `<p class="more-or-less">Names (${nameItems.length}): ${nameItems.join(', ')}</p>`;
            }
        } else if (r.variants && r.variants.length > 0) {
            html += `<p class="more-or-less">Variants (${r.variants.length}): ${r.variants.join(', ')}</p>`;
        }

        if (r.types && r.types.length > 0) {
            html += `<p>Type(s): ${r.types.join(', ')}</p>`;
        }

        if (r.ccodes && r.ccodes.length > 0 && !(r.ccodes.length === 1 && r.ccodes[0] === '')) {
            html += `<p>Countries: ${r.ccodes.map(c => {
                const country = dropdown_data[1].children.find(ch => ch.id === c);
                return `<span title="${country ? country.text : ''}">${c}</span>`;
            }).join(', ')}</p>`;
        }

        if (r.timespans && r.timespans.length > 0) {
            r.timespans.sort((a, b) => a[0] - b[0]);
            html += `<p>Chronology: ${r.timespans.map(s => `${s[0]}–${s[1]}`).join(', ')}</p>`;
        }

        html += `</div>`;
        $resultsDiv.append(html);
    });

    $resultsDiv.find('.more-or-less').toggleTruncate();

    // Show results on the hero map
    heroMap.showResultFeatures(featureCollection);

    // Zoom to results
    if (results.length > 0) {
        heroMap.map.fitViewport(bbox(featureCollection), { maxZoom: 12, padding: { top: 80, right: 400, bottom: 60, left: 80 } });
    }
}

/* ── Results panel show/hide ── */

function showResultsPanel() {
    const panel = document.getElementById('atlas_results_panel');
    panel.classList.remove('atlas-results-hidden');
    // Resize map to accommodate
    setTimeout(() => heroMap.resize(), 350);
}

function hideResultsPanel() {
    const panel = document.getElementById('atlas_results_panel');
    panel.classList.add('atlas-results-hidden');
    heroMap.clearResultFeatures();
    document.getElementById('atlas_search_results').innerHTML = '';
    setTimeout(() => heroMap.resize(), 350);
}

/* ── Clear all ── */

function clearAll() {
    searchDisabled = true;
    const input = document.getElementById('atlas_search_input');
    input.value = '';

    // Clear selections
    selectedRegions = [];
    renderSelectionChips();
    heroMap.clearOverlay();
    heroMap.clearSuggestions();
    heroMap.clearResultFeatures();

    // Reset temporal
    resetTemporalControl();

    // Reset admin level (via layer palette)
    if (layerPalette) layerPalette.resetAdminLevel();

    // Reset type tree
    if (typeTree) {
        typeTree.clearAll();
        filterState.set('place_types', []);
        updateTreeBadge();
    }

    // Reset exact match
    exactMatch = false;
    const emBtn = document.getElementById('atlas_exact_match');
    if (emBtn) { emBtn.classList.remove('active'); emBtn.setAttribute('aria-pressed', 'false'); }

    // Reset viewport constraint
    useViewport = false;
    const vpBtn = document.getElementById('atlas_viewport_btn');
    if (vpBtn) { vpBtn.classList.remove('active'); }
    updateViewportTooltip();

    // Reset clustering (always-on default; slider TBD per Master Plan §4.2)
    clusterResults = true;

    // Reset gazetteers (formerly authorities) — return to Filter mode + standard list,
    // restore default checkbox selection, and reset both per-mode caches.
    setGazetteerMode('filter');
    const defaults = ['gn', 'iv', 'ohm', 'pl', 'tgn', 'tm', 'wd', 'whg'];
    filterSelections.clear();
    defaults.forEach(v => filterSelections.add(v));
    exploreSelection = null;
    document.querySelectorAll('#gazetteers_offcanvas .authority-cb').forEach(cb => {
        cb.checked = filterSelections.has(cb.value);
    });
    // Reset stub-note and the unimplemented coverage switches.
    document.querySelectorAll('#gazetteers_offcanvas .gazetteer-stub-switch').forEach(sw => {
        sw.checked = false;
    });
    const stubNote = document.querySelector('#gazetteers_offcanvas .gazetteer-stub-note');
    if (stubNote) stubNote.classList.add('d-none');

    // Hide results
    hideResultsPanel();

    // Reset search mode to Areas
    switchSearchMode('areas');
    document.querySelectorAll('.search-mode-toggle .btn').forEach(b => b.classList.remove('active'));
    const areasBtn = document.querySelector('.search-mode-toggle .btn[data-search-mode="areas"]');
    if (areasBtn) areasBtn.classList.add('active');

    // Reset map
    if (heroMap.map) heroMap.map.reset();

    filterState.reset();
    searchDisabled = false;
    closeAreaDropdown();
}

/* ── Scroll helper ── */

function scrollToResult($elem) {
    const $container = $('#atlas_result_container');
    if (!$container.length) return;
    const containerTop = $container.offset().top;
    const containerScrollTop = $container.scrollTop();
    const containerHeight = $container.innerHeight();
    const elemTop = $elem.offset().top;
    const elemHeight = $elem.outerHeight(true);
    const target = Math.round(containerScrollTop + (elemTop - containerTop) - (containerHeight / 2) + (elemHeight / 2));
    $container.stop(true).animate({ scrollTop: target }, 400, function () {
        $elem.addClass('flash-border');
        setTimeout(() => $elem.removeClass('flash-border'), 3000);
    });
}

/* ── Utilities ── */

function escapeHtml(s) {
    return String(s)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}


