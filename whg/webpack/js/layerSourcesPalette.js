// /whg/webpack/js/layerSourcesPalette.js
/**
 * Layer Sources palette for the Atlas page.
 *
 * Renders radio-button source selector (OSM, OHM, PeriodO, etc.)
 * and a compact boundary-level dropdown (visible only when a
 * boundary source — OSM or OHM — is selected).
 *
 * Namespace (osm/ohm) is inferred from the selected source,
 * eliminating the separate Modern / Historical toggle.
 */

import heroMap from './heroMap';

/* ── Boundary tier definitions ──
 *
 * A tier is a *group* of OSM/OHM ``admin_level`` values, not a single one, and
 * the groups are exactly those the whg-context style already uses for its
 * boundary line and label layers (``*-line-continental`` = 0,1; ``-country`` =
 * 2; ``-state`` = 3,4; ``-district`` = 5,6; ``-local`` = 7..11).
 *
 * Single levels were the root of place#156's "unpredictable" filtering:
 *   • the fill layer was filtered to one level while the outlines were drawn
 *     for the whole pair, so the map showed borders that could not be clicked;
 *   • ``admin_level`` is not comparable between countries — 3 and 5 are used by
 *     only a handful of them (a z4 tile over western Europe carries 14 features
 *     at level 3 against 206 at level 4), so those two settings looked broken;
 *   • levels 9–11 carry the dense urban data in OSM and had no setting at all.
 *
 * ``autoFrom`` is the zoom at which the tier takes over when *Auto by zoom* is
 * on, chosen to match the zoom range over which the style is willing to draw
 * that tier's linework. */
const BOUNDARY_TIERS = [
    { value: 'off',         label: 'Off',                     levels: null,  autoFrom: null },
    { value: 'continental', label: 'Continent / world region', levels: ['0', '1'], autoFrom: 0 },
    { value: 'country',     label: 'Country',                  levels: ['2'], autoFrom: 2.5 },
    { value: 'state',       label: 'State / province',         levels: ['3', '4'], autoFrom: 4 },
    { value: 'district',    label: 'District / county',        levels: ['5', '6'], autoFrom: 6 },
    { value: 'local',       label: 'Municipality / locality',  levels: ['7', '8', '9', '10', '11'], autoFrom: 8 },
];

/* Selectable tiers, coarsest first — BOUNDARY_TIERS without the Off row. */
const SELECTABLE_TIERS = BOUNDARY_TIERS.filter(t => t.levels);

function tierByValue(value) {
    return BOUNDARY_TIERS.find(t => t.value === value) || null;
}

/** Coarsest-to-finest tier whose ``autoFrom`` the given zoom has reached. */
function tierForZoom(zoom) {
    let chosen = SELECTABLE_TIERS[0];
    for (const t of SELECTABLE_TIERS) {
        if (zoom >= t.autoFrom) chosen = t;
    }
    return chosen;
}

/** "4" or "3–4" or "7–11" — the admin_level values a tier covers. */
function tierLevelRange(tier) {
    if (!tier || !tier.levels) return '';
    const first = tier.levels[0];
    const last = tier.levels[tier.levels.length - 1];
    return first === last ? first : `${first}–${last}`;
}

/* Sources that support admin-tier filtering (the Boundary Level dropdown).
   osm_misc, po, clio, nl are boundary-bearing but untiered: their
   ``boundary`` field carries tag values or single class strings, not the
   string admin levels "0".."11" that osm/ohm use. */
const TIERED_SOURCES = new Set(['osm', 'ohm']);

/* Map a gazetteer / palette source id to the boundary tile source-layer
   in the whg-context style. After the OSM/OHM tileset rename the mapping
   is identity except for WHG-namespaced ids: ``whg:892`` becomes
   ``whg-892`` to match the tileserver naming. */
function tileSourceFor(sourceId) {
    return (sourceId || '').replace(':', '-');
}

/* Tooltip descriptions are now carried per-row by the registry-driven
   data feed (``available_sources`` in search/views.py::AtlasPageView).
   Each row provides a ``description`` string used as the tooltip text. */

export default class LayerSourcesPalette {
    /**
     * @param {string} panelSelector  – CSS selector for the panel container
     * @param {string} _toggleSelector – (unused, kept for API compat)
     * @param {Array}  sources        – [{id, label, enabled, coming_soon?}]
     */
    constructor(panelSelector, _toggleSelector, sources) {
        this._panel = document.querySelector(panelSelector);
        this._sources = sources || [];

        // With radio buttons only one source is active at a time
        const enabledSource = this._sources.find(s => s.enabled);
        this._activeSource = enabledSource ? enabledSource.id : (this._sources[0]?.id || 'osm');
        this._activeSources = [this._activeSource];

        // Namespace is inferred from the selected source
        this._currentNamespace = this._inferNamespace(this._activeSource);

        this._currentTier = null;      // BOUNDARY_TIERS entry, or null when off
        this._shownSource = null;      // tile source the current tier was applied to
        this._autoAdmin = true;
        this._boundariesVisible = false;
        this._probeToken = 0;          // cancels a superseded empty-tier probe

        this._init();
    }

    /* ──────────────────────────────────────────────────────────────── */
    /*  Init                                                           */
    /* ──────────────────────────────────────────────────────────────── */

    _init() {
        // ── Source radio buttons ──
        let html = '<span class="layer-panel-section-label">Source</span>';

        this._sources.forEach(s => {
            const tooltip = s.description || s.label;
            const checked = s.id === this._activeSource ? 'checked' : '';
            const disabled = s.enabled === false ? 'disabled' : '';
            html += `
                <div class="layer-source-item">
                    <div class="form-check">
                        <input class="form-check-input layer-source-radio" type="radio"
                               name="layer_source" id="layer_src_${s.id}" value="${s.id}"
                               ${checked} ${disabled}>
                        <label class="form-check-label" for="layer_src_${s.id}"
                               data-bs-toggle="tooltip" data-bs-placement="right"
                               data-bs-title="${tooltip}">
                            ${s.label}
                        </label>
                    </div>
                </div>`;
        });

        // ── Boundary level section (hidden by default, shown for OSM/OHM) ──
        html += `
            <div id="boundary_level_section"
                 style="${TIERED_SOURCES.has(this._activeSource) ? '' : 'display:none'}">
                <hr class="layer-panel-divider">
                <span class="layer-panel-section-label">Boundaries</span>
                <p class="layer-panel-help-text">
                    Set the administrative level of boundaries displayed on the map.
                    Click a boundary polygon to select it as a region constraint.
                </p>
                <select id="boundary_level_select" class="form-select form-select-sm">
                    ${BOUNDARY_TIERS.map(t =>
                        `<option value="${t.value}">${t.label}${t.levels ? ' (' + tierLevelRange(t) + ')' : ''}</option>`
                    ).join('')}
                </select>
                <div class="admin-auto-toggle">
                    <div class="form-check form-switch">
                        <input class="form-check-input" type="checkbox" id="admin_auto_zoom" checked>
                        <label class="form-check-label" for="admin_auto_zoom">Auto by zoom</label>
                    </div>
                </div>
                <p id="boundary_level_status" class="layer-panel-help-text boundary-level-status"></p>
                <p class="layer-panel-help-text" style="margin-top:4px;">
                    Each setting groups the <em>admin_level</em> values that mean the
                    same thing across countries — they are not used consistently
                    worldwide, so some levels are empty in some places.
                    When <em>Auto by zoom</em> is on the level follows the zoom, and
                    steps to the nearest level that has anything to show here.
                </p>
            </div>`;

        this._panel.innerHTML = html;

        // ── Initialise Bootstrap tooltips ──
        this._panel.querySelectorAll('[data-bs-toggle="tooltip"]').forEach(el => {
            new bootstrap.Tooltip(el, { trigger: 'hover' });
        });


        // ── Wire source radio buttons ──
        this._panel.querySelectorAll('.layer-source-radio').forEach(radio => {
            radio.addEventListener('change', () => {
                if (!radio.checked) return;
                this._activeSource = radio.value;
                this._activeSources = [this._activeSource];
                this._currentNamespace = this._inferNamespace(this._activeSource);

                const isTiered = TIERED_SOURCES.has(this._activeSource);
                const boundarySection = this._panel.querySelector('#boundary_level_section');
                if (boundarySection) {
                    boundarySection.style.display = isTiered ? '' : 'none';
                }

                if (isTiered) {
                    // Apply the boundary tier filter — auto-pick by zoom if auto
                    // is on, otherwise reuse the tier the user chose. OSM and OHM
                    // populate wildly different admin levels, so a source change
                    // has to re-probe even when the tier is unchanged.
                    if (this._autoAdmin) {
                        this._applyAutoTier();
                    } else if (this._currentTier) {
                        this._updateBoundaryFilter();
                    }
                } else {
                    // Untiered: show every feature in this source's tileset.
                    this._currentTier = null;
                    this._shownSource = tileSourceFor(this._activeSource);
                    this._boundariesVisible = true;
                    this._setStatus('');
                    heroMap.showBoundaries({ source: this._shownSource });
                }

                this._onSourcesChange();
            });
        });

        // ── Wire boundary level dropdown ──
        const select = this._panel.querySelector('#boundary_level_select');
        if (select) {
            select.addEventListener('change', () => {
                const tier = tierByValue(select.value);
                if (!tier || !tier.levels) {
                    this._currentTier = null;
                    this._boundariesVisible = false;
                    heroMap.hideBoundaries();
                    this._setStatus('');
                } else {
                    this._currentTier = tier;
                    this._boundariesVisible = true;
                    this._updateBoundaryFilter();
                }
                // Disable auto when user manually picks
                this._autoAdmin = false;
                const autoCheck = this._panel.querySelector('#admin_auto_zoom');
                if (autoCheck) autoCheck.checked = false;
            });
        }

        // ── Wire auto-zoom toggle ──
        const autoCheck = this._panel.querySelector('#admin_auto_zoom');
        if (autoCheck) {
            autoCheck.addEventListener('change', () => {
                this._autoAdmin = autoCheck.checked;
                if (this._autoAdmin) {
                    this._applyAutoTier();
                }
            });
        }

        // ── Setup zoom-based auto-switching ──
        this._setupZoomAutoSwitch();
    }

    /* ──────────────────────────────────────────────────────────────── */
    /*  Namespace inference                                             */
    /* ──────────────────────────────────────────────────────────────── */

    /** Map source id to the namespace used by boundary tiles. Identity:
     *  the layer-sources palette and the registry use the same id strings,
     *  so the namespace is just the source id. */
    _inferNamespace(sourceId) {
        return sourceId || 'osm';
    }

    /* ──────────────────────────────────────────────────────────────── */
    /*  Boundary filter                                                */
    /* ──────────────────────────────────────────────────────────────── */

    _updateBoundaryFilter() {
        if (!this._currentTier) {
            this._shownSource = null;
            heroMap.hideBoundaries();
            this._setStatus('');
            return;
        }
        // Only tiered sources reach this path (osm/ohm). Untiered sources
        // render via the radio change handler with no boundaryValues.
        this._shownSource = tileSourceFor(this._activeSource);
        heroMap.showBoundaries({
            source: this._shownSource,
            boundaryValues: this._currentTier.levels,
        });
        this._syncSelect();
        this._probeTier();
    }

    /** Reflect the active tier in the dropdown (auto changes it behind the user). */
    _syncSelect() {
        const select = this._panel.querySelector('#boundary_level_select');
        if (select) select.value = this._currentTier ? this._currentTier.value : 'off';
    }

    _setStatus(text, muted = false) {
        const el = this._panel.querySelector('#boundary_level_status');
        if (!el) return;
        el.textContent = text;
        el.classList.toggle('boundary-level-status--empty', !!muted);
    }

    /* ──────────────────────────────────────────────────────────────── */
    /*  Auto tier switching                                            */
    /* ──────────────────────────────────────────────────────────────── */

    _setupZoomAutoSwitch() {
        heroMap.init().then(() => {
            // ``moveend`` as well as ``zoomend``: which admin levels exist is a
            // property of *where* you are, not only how far in you are, so
            // panning from a country that uses level 4 to one that does not has
            // to re-pick. Both fire once per gesture, so this is not chatty.
            const reapply = () => {
                if (this._autoAdmin && TIERED_SOURCES.has(this._activeSource)) {
                    this._applyAutoTier();
                } else if (this._currentTier) {
                    this._probeTier();
                }
            };
            heroMap.map.on('zoomend', reapply);
            heroMap.map.on('moveend', reapply);
        });
    }

    /**
     * Pick and apply the tier for the current view.
     *
     * The zoom proposes a tier; the data decides. ``admin_level`` coverage is
     * wildly uneven — OHM's data sits at levels 2–4 whatever the zoom, and
     * plenty of countries skip a level entirely — so a purely zoom-driven
     * choice regularly resolved to an empty map with nothing to explain it
     * (place#156). If the proposed tier has nothing here, the nearest tier that
     * does is substituted, and the panel says so.
     *
     * While the source is already on screen the substitution is resolved
     * *before* the filter is touched, from the source data rather than from
     * what is painted, so the map never flashes through an empty tier on the
     * way to a populated one. A source being switched *to* is different: a
     * tileset with no visible layer is never fetched, so there would be
     * nothing to count and the panel would wait for ever on a source it had
     * not yet asked for. There, the zoom's tier goes on first and is corrected
     * once the tiles land.
     */
    _applyAutoTier() {
        if (!heroMap.map) return;
        if (!TIERED_SOURCES.has(this._activeSource)) return;

        const wanted = tierForZoom(heroMap.map.getZoom());
        const showFirst = tileSourceFor(this._activeSource) !== this._shownSource;
        if (showFirst) this._showTier(wanted);
        this._whenSettled(() => {
            const source = tileSourceFor(this._activeSource);
            const count = heroMap.countBoundaryFeatures(source, wanted.levels);
            if (count > 0) {
                this._showTier(wanted);
                this._setStatus(`${count} ${count === 1 ? 'region' : 'regions'} here`);
                return true;
            }
            const next = this._nextNonEmptyTier(wanted, source);
            if (next) {
                this._showTier(next);
                this._setStatus(
                    `No ${wanted.label.toLowerCase()} boundaries here — showing `
                    + `${next.label.toLowerCase()} instead `
                    + `(${heroMap.countBoundaryFeatures(source, next.levels)})`);
                return true;
            }
            // Empty at every level is what a source whose tiles have not yet
            // arrived also looks like, so ask to be called again rather than
            // announcing a blank map that is really still loading.
            return false;
        });
    }

    /** Report how much the manually-chosen tier has to show, without changing it. */
    _probeTier() {
        if (!heroMap.map || !this._currentTier) return;
        this._whenSettled(() => {
            const tier = this._currentTier;
            if (!tier) return true;
            const source = tileSourceFor(this._activeSource);
            const count = heroMap.countBoundaryFeatures(source, tier.levels);
            if (count > 0) {
                this._setStatus(`${count} ${count === 1 ? 'region' : 'regions'} here`);
                return true;
            }
            // Distinguish "this level is empty here" from "nothing has loaded".
            if (heroMap.countBoundaryFeatures(source, null) === 0) return false;
            this._setStatus(
                `No ${tier.label.toLowerCase()} boundaries here — try another level`, true);
            return true;
        });
    }

    /** Show a tier, skipping the work if it is already the one on screen. */
    _showTier(tier) {
        this._boundariesVisible = true;
        const source = tileSourceFor(this._activeSource);
        if (tier === this._currentTier && source === this._shownSource) return;
        this._currentTier = tier;
        this._shownSource = source;
        heroMap.showBoundaries({ source, boundaryValues: tier.levels });
        this._syncSelect();
    }

    /**
     * Run ``fn`` once the map has settled, cancelling any earlier pending call.
     * Tiles for a new view arrive asynchronously and an empty count taken
     * mid-flight would trigger a pointless substitution.
     *
     * Never reads on the current frame, even when the map claims to be
     * idle: immediately after ``moveend`` the tiles that are "loaded" are
     * still the *previous* view's, so a count taken then describes where the
     * user came from — which is exactly how a district-level view of Nebraska
     * came back empty and got substituted away.
     */
    _whenSettled(fn) {
        const token = ++this._probeToken;
        const map = heroMap.map;
        let finished = false;
        let waits = 0;
        const attempt = () => {
            if (finished || token !== this._probeToken) return;
            const settled = !map.isMoving() && map.areTilesLoaded();
            // ``fn`` returning false means it does not believe what it read
            // either — see _applyAutoTier, where "nothing at any level" is
            // indistinguishable from "this source's tiles have not arrived".
            if ((!settled || fn() === false) && waits++ < 10) {
                map.once('idle', attempt);
                setTimeout(attempt, 400 * waits);
                return;
            }
            finished = true;
        };
        map.once('idle', attempt);
        // ...but do not wait for ever: with nothing left to render, the 'idle'
        // subscribed above may never fire.
        setTimeout(attempt, 1200);
    }

    /**
     * Nearest tier to ``tier`` that has features here — one step coarser, then
     * one step finer, widening — or null if the source holds nothing at all at
     * this view. Nearest-first keeps the correction small and stable instead of
     * jumping to whichever level happens to be busiest.
     */
    _nextNonEmptyTier(tier, source) {
        const i = SELECTABLE_TIERS.indexOf(tier);
        for (let d = 1; d < SELECTABLE_TIERS.length; d++) {
            for (const candidate of [SELECTABLE_TIERS[i - d], SELECTABLE_TIERS[i + d]]) {
                if (!candidate) continue;
                if (heroMap.countBoundaryFeatures(source, candidate.levels) > 0) return candidate;
            }
        }
        return null;
    }

    /* ──────────────────────────────────────────────────────────────── */
    /*  Source change callback                                         */
    /* ──────────────────────────────────────────────────────────────── */

    _onSourcesChange() {
        heroMap.setActiveSources(this._activeSources);
        document.dispatchEvent(new CustomEvent('layer-sources-change', {
            detail: { activeSources: this._activeSources },
        }));
    }

    /* ──────────────────────────────────────────────────────────────── */
    /*  Public API                                                     */
    /* ──────────────────────────────────────────────────────────────── */

    /** Get currently active source IDs (single-element array). */
    getActiveSources() {
        return [...this._activeSources];
    }

    /**
     * Programmatically select a source by id, mirroring an Atlas Gazetteers
     * Explore-mode selection onto the map.
     *
     * TIERED admin sources (osm/ohm) are region-boundary layers with their own
     * admin-level UI, not explorable coverage gazetteers, so they keep the
     * radio/boundary path. Every other selection — including authority
     * gazetteers that the base whg-context style ALSO registers as a
     * region-source radio (clio, nl, po) — is loaded via the generic gazetteer
     * loader so its place#140 coverage footprint renders and the map zooms to
     * its bounds. (Previously any radio-backed id short-circuited to
     * ``showBoundaries``, so clio/nl/po never rendered as gazetteers — the map
     * neither zoomed nor changed and the prior gazetteer's markers persisted.)
     */
    setActiveSource(id) {
        if (!id) return;
        const radio = this._panel.querySelector(`.layer-source-radio[value="${id}"]`);
        if (radio && TIERED_SOURCES.has(id)) {
            if (!radio.checked) radio.checked = true;
            radio.dispatchEvent(new Event('change'));
            return;
        }
        // Untick any currently-checked Region Source radio so the UI doesn't
        // keep claiming a source the map is no longer showing.
        this._panel.querySelectorAll('.layer-source-radio').forEach(r => { r.checked = false; });
        const boundarySection = this._panel.querySelector('#boundary_level_section');
        if (boundarySection) boundarySection.style.display = 'none';
        this._activeSource = id;
        this._activeSources = [id];
        this._currentNamespace = id;
        this._currentTier = null;
        this._shownSource = null;
        this._boundariesVisible = true;
        // Tileset isn't in the base style — load it on demand via the generic
        // gazetteer-style loader.
        heroMap.showGazetteer(tileSourceFor(id));
        this._onSourcesChange();
    }

    /** Check if a given source is active. */
    isActive(sourceId) {
        return this._activeSources.includes(sourceId);
    }

    /** Admin levels the active tier covers, e.g. ``['3','4']``; null when off. */
    getBoundaryLevels() {
        return this._currentTier ? [...this._currentTier.levels] : null;
    }

    /**
     * Representative admin level of the active tier (its finest), or null.
     * Kept for callers that just want to know whether a tier is showing.
     */
    getAdminLevel() {
        if (!this._currentTier) return null;
        return parseInt(this._currentTier.levels[this._currentTier.levels.length - 1], 10);
    }

    /** Get current namespace (inferred from selected source). */
    getNamespace() {
        return this._currentNamespace;
    }

    /** Reset the boundary tier to off, re-enable auto. */
    resetAdminLevel() {
        this._currentTier = null;
        this._shownSource = null;
        this._boundariesVisible = false;
        this._autoAdmin = true;
        this._probeToken++;            // drop any probe still waiting on 'idle'
        heroMap.hideBoundaries();
        this._setStatus('');

        const select = this._panel.querySelector('#boundary_level_select');
        if (select) select.value = 'off';
        const autoCheck = this._panel.querySelector('#admin_auto_zoom');
        if (autoCheck) autoCheck.checked = true;
    }

    /** Re-apply the current boundary filter (show or hide based on current admin level). */
    refreshBoundaries() {
        this._updateBoundaryFilter();
    }

    /**
     * Get the namespace value(s) for boundary queries.
     * Maps active source to namespace used by boundary tiles.
     */
    getActiveNamespaces() {
        const nsMap = { osm: 'osm', ohm: 'ohm', osm_misc: 'osm_misc' };
        return this._activeSources
            .filter(s => s in nsMap)
            .map(s => nsMap[s]);
    }
}

