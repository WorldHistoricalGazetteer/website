// /whg/webpack/js/atlasPlaceList.js
//
// Atlas Gazetteer → Explore "Place Lists" panel (place#125).
//
// The legacy dataset browser was list + map based; Atlas Explore mode only
// exposed a gazetteer as map features, which hides every place that lacks
// geometry. This panel restores a browsable, searchable inventory in the
// right-hand results column: a virtualised (evicting) list of a single
// gazetteer's places with a typeahead filter.
//
//   • Data source: the existing POST /atlas/search/ proxy, scoped with
//     namespaces:[<gazetteer>]. Empty box → browse (gateway match-all within
//     the namespace); typing → server-side search across the WHOLE gazetteer.
//   • Pagination: the gateway takes `size` but no offset, so "load more" grows
//     `size` (100 → 200 → …) up to the ES max_result_window ceiling. The list
//     states honestly when the ceiling is hit — the typeahead narrows beyond it.
//   • Row click: a place WITH geometry zooms the map and opens its popup
//     (reusing heroMap.openPlacePopup → GazetteerInteraction); a place WITHOUT
//     geometry opens the standalone portal modal (openAtlasPortal).
//   • Close: returns to the Gazetteers (Explore) panel.
//
// Rendering is a fixed-row-height virtual list: only the rows in (and just
// around) the viewport are mounted, so DOM stays bounded no matter how large
// the gazetteer — that is the "evicting" behaviour the issue asks for.
//
// atlas.js owns the search-option builder, the portal opener and the panel
// switcher; they are injected via configure() to avoid a circular import.

import heroMap from './heroMap';
// eslint-disable-next-line no-unused-vars — reserved for future per-language titles
import { getPreferredLanguage } from './languages.js';
import { variantLabels, variantsHtml } from './toponyms.js';
import { aatTooltipHtml, aatUrl, ccName, isLoggedIn } from './gazetteerInteraction.js';
import debounce from 'lodash/debounce';

const PAGE_SIZE = 100;      // per-page fetch size (gateway caps size at 500)
const OFFSET_CAP = 10000;   // gateway offset ceiling (ES max_result_window)
const ROW_H = 74;           // px — fixed row height (enables simple virtualisation);
                            //      title + optional variants line + meta chips
const OVERSCAN = 6;         // rows mounted above/below the viewport
const VIEW_ID = 'atlas_placelists_view';

function esc(s) {
    if (s == null) return '';
    return String(s)
        .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

// Attested date range for a hit, formatted exactly as the main search results'
// temporal chips (atlas.js formatRange/formatYear): a single year, or a
// dash-joined span, with BCE marked. '' when the hit carries no dates.
function formatYear(y) {
    if (y == null) return '';
    return y < 0 ? `${-y} BCE` : `${y}`;
}
function formatRange(tr) {
    if (!Array.isArray(tr) || tr.length !== 2) return '';
    const [s, e] = tr;
    if (s == null && e == null) return '';
    if (s === e) return formatYear(s);
    return `${formatYear(s)}–${formatYear(e)}`; // en-dash
}

// Geometry a hit can be fitted to: full geometries if present, else its
// representative point. Returns null for a place with no location at all.
function hitGeometry(hit) {
    if (Array.isArray(hit.geometries) && hit.geometries.length) {
        return hit.geometries.length === 1
            ? hit.geometries[0]
            : { type: 'GeometryCollection', geometries: hit.geometries };
    }
    if (Array.isArray(hit.repr_point) && hit.repr_point.length >= 2) {
        return { type: 'Point', coordinates: [hit.repr_point[0], hit.repr_point[1]] };
    }
    return null;
}

// Type chips for a hit: AAT-resolved labels (from the result facets) when the
// place has an AAT mapping, else the source's own ``types[].sourceLabel`` so
// custom / unmapped types are still shown (place#122). Returns objects carrying
// the AAT id (when any) so the row can attach the same concept tooltip the map
// popup uses.
function hitTypeChips(hit, aatLabels) {
    const seen = new Set();
    const out = [];
    const add = (label, aat) => {
        if (label == null || label === '') return;
        const s = String(label);
        const k = s.toLowerCase();
        if (!seen.has(k)) { seen.add(k); out.push({ label: s, aat: aat || null }); }
    };
    (hit.aat_ids || []).forEach(id => add(aatLabels && aatLabels[id], id));
    if (!out.length) {
        (hit.types || []).forEach(t => {
            if (t && typeof t === 'object') add(t.sourceLabel || t.label || t.identifier, null);
        });
    }
    return out;
}

const PlaceList = {
    cfg: null,
    ns: null, label: '',
    hits: [], aatLabels: {}, total: null,
    qstr: '', ccode: '', typeVal: '',   // active Country / Type facet selections
    //  typeVal encodes the Type dropdown pick: '' | 'aat:<id>' | 'src:<identifier>'
    reqSeq: 0, loading: false, hasMore: false,
    _pendingFocus: null,   // place_id to focus once the first page has loaded (deep link)
    _pendingZoom: null,    // zoom to restore for the deep-linked place (shared view)
    els: null, wired: false, _debouncedFilter: null,

    /** Inject atlas.js collaborators once at page init.
     *  cfg = { getBaseOptions, openPortal, showPanelView, getCsrf } */
    configure(cfg) {
        this.cfg = cfg;
        this._debouncedFilter = debounce(() => this._fetchPage(true), 300);
    },

    /** Show the panel for a single gazetteer and load its first page.
     *  Called from emitGazetteerSelection when an Explore-mode radio is picked. */
    open(namespace, label) {
        if (!namespace || !this.cfg) return;
        this._ensureDom();
        this.ns = namespace;
        this.label = label || namespace;
        this.hits = []; this.aatLabels = {}; this.total = null;
        this.qstr = ''; this.ccode = ''; this.typeVal = ''; this.hasMore = false;
        this.els.search.value = '';
        this.els.ccodeSel.innerHTML = '<option value="">All countries</option>';
        this.els.typeSel.innerHTML = '<option value="">All types</option>';
        this.els.filters.style.display = 'none';
        this.els.title.textContent = this.label;
        this.els.rows.innerHTML = '';
        this.els.rows.style.height = '0px';
        this.els.scroll.scrollTop = 0;
        this.cfg.showPanelView(VIEW_ID);
        this._fetchPage(true);
    },

    /** Reopen the already-loaded Place List for the current gazetteer (used when
     *  the user re-clicks the selected Explore gazetteer after closing the list). */
    reopen() {
        if (this.ns && this.cfg) this.cfg.showPanelView(VIEW_ID);
    },

    /** Re-query the list from the top with the current filters (incl. the global
     *  Date Range). Called when the temporal filter changes — only while the
     *  Place List is the visible panel, to avoid wasted fetches. */
    refresh() {
        const view = document.getElementById(VIEW_ID);
        if (this.ns && this.cfg && view && view.classList.contains('active')) {
            this._fetchPage(true);
        }
    },

    /** Show/update the "Filtered by Date Range" indicator (``label`` = the range,
     *  or '' to hide). Driven by atlas.js on temporal changes / list open. */
    setTemporalFilter(label) {
        if (!this.els || !this.els.datefilter) return;
        if (label) {
            if (this.els.datefilterRange) this.els.datefilterRange.textContent = label;
            this.els.datefilter.hidden = false;
        } else {
            this.els.datefilter.hidden = true;
        }
    },

    /** Return to the Gazetteers (Explore) panel. Only steals the view when the
     *  Place List is the one on screen, so a Filter-mode toggle elsewhere is a
     *  no-op. */
    close() {
        const view = document.getElementById(VIEW_ID);
        if (view && view.classList.contains('active') && this.cfg) {
            this.cfg.showPanelView('gazetteers_offcanvas');
        }
    },

    _ensureDom() {
        const view = document.getElementById(VIEW_ID);
        if (!view) return;
        this.els = {
            view,
            title: view.querySelector('.placelist-title'),
            search: view.querySelector('.placelist-search'),
            filters: view.querySelector('.placelist-filters'),
            ccodeSel: view.querySelector('.placelist-ccode'),
            typeSel: view.querySelector('.placelist-type'),
            status: view.querySelector('.placelist-status'),
            scroll: view.querySelector('.placelist-scroll'),
            rows: view.querySelector('.placelist-rows'),
            datefilter: view.querySelector('.placelist-datefilter'),
            datefilterRange: view.querySelector('.placelist-datefilter-range'),
        };
        if (this.wired) return;
        this.wired = true;

        // Styled (Bootstrap) HTML tooltips for the virtualised rows — one
        // delegated instance so re-mounted chips are covered without per-row
        // init. Bootstrap may load slightly after first open, so retry briefly.
        if (!this._initTooltips()) {
            let tries = 0;
            const t = setInterval(() => {
                if (this._initTooltips() || ++tries > 20) clearInterval(t);
            }, 150);
        }

        view.querySelector('.placelist-back')
            .addEventListener('click', () => this.close());

        this.els.search.addEventListener('input', (e) => {
            this.qstr = e.target.value.trim();
            this._debouncedFilter();
        });

        // Country / Type facet filters — an offered set built from the gateway's
        // whole-gazetteer aggregations (populated on each fetch); changing either
        // re-queries from the top.
        this.els.ccodeSel.addEventListener('change', (e) => {
            this.ccode = e.target.value;
            this._fetchPage(true);
        });
        this.els.typeSel.addEventListener('change', (e) => {
            this.typeVal = e.target.value;
            this._fetchPage(true);
        });

        let ticking = false;
        this.els.scroll.addEventListener('scroll', () => {
            // Dismiss any open chip tooltip immediately so it can't orphan when
            // the row it's attached to is re-rendered out of the virtual window.
            this._hideRowTooltip();
            if (ticking) return;
            ticking = true;
            requestAnimationFrame(() => {
                ticking = false;
                this._render();
                this._maybeLoadMore();
            });
        });

        // One delegated click handler for every (re-mounted) row.
        this.els.rows.addEventListener('click', (e) => {
            // The share button copies a deep link — don't open the place.
            const share = e.target.closest('.pl-share');
            if (share) {
                e.stopPropagation();
                if (this.cfg.copyLink) this.cfg.copyLink(share.dataset.pid);
                return;
            }
            // The envelope button emails that link — also not a row-open.
            const invite = e.target.closest('.pl-invite');
            if (invite) {
                e.stopPropagation();
                if (this.cfg.emailLink) this.cfg.emailLink(invite.dataset.pid);
                return;
            }
            // A click on an AAT type-chip link opens Getty — don't also open the place.
            if (e.target.closest('a')) return;
            const row = e.target.closest('.placelist-row');
            if (!row) return;
            const idx = parseInt(row.dataset.idx, 10);
            if (!Number.isNaN(idx)) this._onRowClick(idx);
        });

        // A click on a MAP marker (not a list row) makes the map the source of
        // the shown place — drop the list's row highlight so it reads as a plain
        // list again.
        document.addEventListener('whg:map-place-click', () => this._clearSelection());
    },

    /** One delegated Bootstrap tooltip for all row chips (styled HTML). Rendered
     *  to <body> so it isn't clipped by the scroll overflow. Returns false until
     *  Bootstrap is available. */
    _initTooltips() {
        const bs = window.bootstrap;
        if (!bs || !bs.Tooltip || !this.els || this._tooltip) return !!this._tooltip;
        this._tooltip = new bs.Tooltip(this.els.scroll, {
            selector: '[data-bs-toggle="tooltip"]',
            html: true,
            container: 'body',
            trigger: 'hover',
            placement: 'top',
            // No fade: a tooltip must vanish instantly when the virtual list
            // re-renders its trigger out from under it (otherwise the fade-out
            // orphans a stuck tooltip at the page origin).
            animation: false,
            delay: { show: 200, hide: 0 },
        });
        return true;
    },

    /** Dismiss any open row tooltip — the trigger carries ``aria-describedby``
     *  while its tooltip is shown. Called before the virtual list re-renders
     *  (e.g. on scroll) so a tooltip can't detach and stick to the page. */
    _hideRowTooltip() {
        const bs = window.bootstrap;
        if (!bs || !bs.Tooltip || !this.els) return;
        this.els.rows.querySelectorAll('[aria-describedby]').forEach(el => {
            const inst = bs.Tooltip.getInstance(el);
            if (inst) { try { inst.hide(); } catch (e) { /* */ } }
        });
    },

    _clearSelection() {
        if (!this.els) return;
        this.els.rows.querySelectorAll('.placelist-row.pl-selected')
            .forEach(r => r.classList.remove('pl-selected'));
    },

    // ── Data ──────────────────────────────────────────────────────────────
    // Pages via the gateway's real `offset` pagination and accumulates hits.
    // With an empty box we send `browse:true` — a namespace-filtered, alphabetical
    // match-all with a REAL total; typing switches to the ranked search. Either
    // way `offset` walks the result list a page at a time.
    _fetchPage(reset) {
        if (!this.cfg || this.loading) return;
        if (!reset && (!this.hasMore || this.hits.length >= OFFSET_CAP)) return;

        const seq = reset ? ++this.reqSeq : this.reqSeq;
        const offset = reset ? 0 : this.hits.length;
        this.loading = true;
        if (reset) {
            this.hits = [];
            this.aatLabels = {};
            this.els.rows.innerHTML = '';
            this.els.rows.style.height = '0px';
            this.els.scroll.scrollTop = 0;
        }
        this._setStatus(reset ? 'Loading…' : 'Loading more…');

        // Base filters (temporal / type) from atlas.js, then scope + browse.
        // Bounds are deliberately dropped so geometry-less places are included —
        // this is a whole-gazetteer inventory, not a viewport query.
        const opts = this.cfg.getBaseOptions(this.qstr) || {};
        opts.qstr = this.qstr;
        opts.namespaces = [this.ns];
        // "in" = true substring/contains, now a real n-gram match for every
        // namespace after the gateway fix (place#127) — no prefix fallback needed.
        // The explicit `namespaces` also overrides the gateway's default
        // exclude_namespaces (e.g. gb), so GB1900 Explore searches too.
        opts.mode = 'in';
        opts.size = PAGE_SIZE;
        opts.offset = offset;
        opts.browse = !this.qstr;   // empty box → browse the whole gazetteer
        opts.cluster = false;
        opts.bounds = { type: 'GeometryCollection', geometries: [] };
        opts.spatial = 'none';
        // The Place List owns its own Country / Type filters (independent of the
        // main Atlas facets), driven by this panel's dropdowns. The Type pick is
        // routed to `aat_types` (AAT) or `types` (custom source id) by its prefix
        // (place#122).
        opts.countries = this.ccode ? [this.ccode] : [];
        opts.aat_types = this.typeVal.startsWith('aat:') ? [this.typeVal.slice(4)] : [];
        opts.types = this.typeVal.startsWith('src:') ? [this.typeVal.slice(4)] : [];

        fetch('/atlas/search/', {
            method: 'POST',
            credentials: 'same-origin',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': this.cfg.getCsrf(),
            },
            body: JSON.stringify(opts),
        })
            .then(r => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); })
            .then(data => {
                if (seq !== this.reqSeq) return;   // a newer query superseded this one
                this.loading = false;
                // Surface gateway-down to the shared banner (place#125 resilience).
                if (this.cfg.onGatewayStatus) this.cfg.onGatewayStatus(data.gateway !== false);
                const page = Array.isArray(data.hits) ? data.hits : [];
                this.hits = this.hits.concat(page);
                this.total = (typeof data.total === 'number') ? data.total : this.hits.length;
                // Facets are whole-namespace for browse; keep first-seen labels.
                ((data.facets && data.facets.aat_types) || [])
                    .forEach(f => { if (!(f.aat_id in this.aatLabels)) this.aatLabels[f.aat_id] = f.label; });
                if (offset === 0) this._populateFacets(data.facets);
                this.hasMore = page.length >= PAGE_SIZE
                    && this.hits.length < this.total
                    && this.hits.length < OFFSET_CAP;
                this.els.rows.style.height = (this.hits.length * ROW_H) + 'px';
                this._render();
                this._updateStatus();
                // Deep link: once the first page is in, focus the requested place
                // (opens its popup/modal + highlights its row if it's on this page).
                if (offset === 0 && this._pendingFocus) {
                    const p = this._pendingFocus;
                    const z = this._pendingZoom;
                    this._pendingFocus = null;
                    this._pendingZoom = null;
                    this.focusPlace(p, z);
                }
            })
            .catch(err => {
                if (seq !== this.reqSeq) return;
                this.loading = false;
                console.error('PlaceList: search failed', err);
                this._setStatus('Could not load places. Please try again.');
            });
    },

    _maybeLoadMore() {
        if (this.loading || !this.hasMore) return;
        const el = this.els.scroll;
        if (el.scrollTop + el.clientHeight >= el.scrollHeight - ROW_H * OVERSCAN) {
            this._fetchPage(false);
        }
    },

    // Offered-set Country / Type filters, built from the gateway's whole-gazetteer
    // aggregations. A facet is only (re)built while it is NOT the active filter —
    // a filtered aggregation collapses to the single selected value, so we keep
    // the full option list for whichever dimension the user is filtering on.
    _populateFacets(facets) {
        facets = facets || {};
        if (!this.ccode) {
            this._fillSelect(this.els.ccodeSel, 'All countries',
                (facets.countries || []).map(c => ({
                    value: c.code,
                    label: c.code + (c.count ? ` (${c.count.toLocaleString()})` : ''),
                })));
        }
        if (!this.typeVal) {
            // AAT types (routed to aat_types) + custom source types (place#122,
            // routed to types), each option value prefixed so _fetchPage can tell
            // them apart. Custom labels get a subtle marker.
            const aatOpts = (facets.aat_types || []).map(t => ({
                value: 'aat:' + t.aat_id,
                label: (t.label || t.aat_id) + (t.count ? ` (${t.count.toLocaleString()})` : ''),
            }));
            const customOpts = (facets.custom_types || []).map(t => ({
                value: 'src:' + t.identifier,
                label: (t.label || t.identifier) + ' ⚑' + (t.count ? ` (${t.count.toLocaleString()})` : ''),
            }));
            this._fillSelect(this.els.typeSel, 'All types', aatOpts.concat(customOpts));
        }
        const anyC = this.els.ccodeSel.options.length > 1;
        const anyT = this.els.typeSel.options.length > 1;
        this.els.filters.style.display = (anyC || anyT) ? '' : 'none';
    },

    _fillSelect(sel, allLabel, opts) {
        sel.innerHTML = `<option value="">${esc(allLabel)}</option>`
            + opts.map(o => `<option value="${esc(o.value)}">${esc(o.label)}</option>`).join('');
    },

    // ── Rendering (fixed-height virtual window) ───────────────────────────
    _render() {
        if (!this.els) return;
        // Replacing the rows' innerHTML below would orphan any open tooltip whose
        // trigger is about to be removed — dismiss it first.
        this._hideRowTooltip();
        const n = this.hits.length;
        const scrollTop = this.els.scroll.scrollTop;
        const clientH = this.els.scroll.clientHeight || 400;
        const start = Math.max(0, Math.floor(scrollTop / ROW_H) - OVERSCAN);
        const end = Math.min(n, Math.ceil((scrollTop + clientH) / ROW_H) + OVERSCAN);
        let html = '';
        for (let i = start; i < end; i++) html += this._rowHtml(this.hits[i], i);
        this.els.rows.innerHTML = html;
    },

    _rowHtml(hit, i) {
        if (!hit) return '';
        // The title and the variants line are shown in full inline, so a tooltip
        // that merely repeats them is noise — omit it. AAT type chips and cc
        // badges DO carry an informative tooltip (concept scope-note / country
        // name), so those get the info cursor.
        const title = esc(hit.title || hit.place_id || '(untitled)');
        const nogeom = !hitGeometry(hit);
        const types = hitTypeChips(hit, this.aatLabels).slice(0, 3);
        const ccodes = (hit.ccodes || []).slice(0, 3);
        const range = formatRange(hit.temporal_range);
        const variants = variantsHtml(variantLabels(hit.names, hit.title),
            { className: 'pl-row-variants', tooltip: false });
        // Chips use Bootstrap tooltips (styled HTML), initialised once as a
        // delegated instance on the scroll container (_initTooltips).
        let meta = '';
        if (nogeom) {
            meta += `<span class="pl-nogeom" data-bs-toggle="tooltip" data-bs-title="No location — opens as a detail card">no location</span>`;
        }
        if (range) {
            meta += `<span class="temporal-chip" data-bs-toggle="tooltip" data-bs-title="Attested date range">${esc(range)}</span>`;
        }
        meta += types.map(t => {
            // AAT types link to their canonical Getty concept page and carry the
            // Getty AAT / ODC-By credit in a rich (HTML) tooltip; the row-click
            // handler ignores clicks on links so this opens Getty, not the place.
            if (t.aat) {
                return `<a class="pl-chip pl-type pl-aat" href="${esc(aatUrl(t.aat))}"`
                    + ` target="_blank" rel="noopener noreferrer" data-aat="aat:${esc(t.aat)}"`
                    + ` data-bs-toggle="tooltip" data-bs-html="true"`
                    + ` data-bs-title="${esc(aatTooltipHtml(t.aat))}">${esc(t.label)}</a>`;
            }
            return `<span class="pl-chip pl-type">${esc(t.label)}</span>`;
        }).join('');
        meta += ccodes.map(c => {
            const name = ccName(c);
            const attrs = name ? ` data-bs-toggle="tooltip" data-bs-title="${esc(name)}"` : '';
            return `<span class="pl-chip pl-cc${name ? ' pl-info' : ''}"${attrs}>${esc(c)}</span>`;
        }).join('');
        const shareBtn = hit.place_id
            ? `<button type="button" class="pl-share" data-pid="${esc(hit.place_id)}"`
              + ` title="Copy a link to this place" aria-label="Copy link to this place">`
              + `<i class="fas fa-share-nodes"></i></button>`
            : '';
        // Email the same link (place#155) — signed-in users only, since it sends
        // mail in their name.
        const inviteBtn = (hit.place_id && isLoggedIn())
            ? `<button type="button" class="pl-invite" data-pid="${esc(hit.place_id)}"`
              + ` title="Email a link to this place" aria-label="Email a link to this place">`
              + `<i class="fas fa-envelope"></i></button>`
            : '';
        return `<div class="placelist-row${nogeom ? ' pl-nogeom-row' : ''}" data-idx="${i}" style="top:${i * ROW_H}px">`
            + shareBtn
            + inviteBtn
            + `<div class="pl-row-title">${title}</div>`
            + variants
            + `<div class="pl-row-meta">${meta}</div>`
            + `</div>`;
    },

    // ── Interaction ───────────────────────────────────────────────────────
    _onRowClick(idx) {
        const hit = this.hits[idx];
        if (!hit) return;
        this._clearSelection();
        const row = this.els.rows.querySelector(`.placelist-row[data-idx="${idx}"]`);
        if (row) row.classList.add('pl-selected');
        this._openHit(hit);
    },

    // Open a hit's popup (with geometry) or detail modal (without), and record it
    // as the URL-shareable focused place. Shared by row clicks and deep links.
    // ``zoom`` (optional) restores a shared view's zoom, centred on the place;
    // otherwise the map fits the place's geometry.
    _openHit(hit, zoom) {
        if (!hit || !hit.place_id) return;
        if (this.cfg.onPlaceFocused) this.cfg.onPlaceFocused(hit.place_id);
        const geom = hitGeometry(hit);
        if (geom) {
            let bb = null;
            try { if (typeof window.bbox === 'function') bb = window.bbox(geom); } catch (e) { /* */ }
            const lngLat = (Array.isArray(hit.repr_point) && hit.repr_point.length >= 2)
                ? [hit.repr_point[0], hit.repr_point[1]]
                : (bb ? [(bb[0] + bb[2]) / 2, (bb[1] + bb[3]) / 2] : null);
            if (zoom != null && lngLat) {
                try { heroMap.map.jumpTo({ center: lngLat, zoom: zoom }); } catch (e) { /* */ }
            } else if (bb) {
                try { heroMap.map.fitViewport(bb, 9); } catch (e) { /* */ }
            }
            if (lngLat) heroMap.openPlacePopup(hit.place_id, lngLat);
            else this.cfg.openPortal(hit.place_id);   // geometry present but unplottable
        } else {
            // No geometry — the standalone detail modal over the map area.
            this.cfg.openPortal(hit.place_id);
        }
    },

    /** Queue a place (and an optional shared-view zoom) to focus once the first
     *  page loads (deep link entry point). */
    setPendingFocus(pid, zoom) {
        this._pendingFocus = pid || null;
        this._pendingZoom = (zoom == null || isNaN(zoom)) ? null : Number(zoom);
    },

    /** Focus a specific place by id: highlight + scroll to its row if it's on the
     *  loaded page, then open its popup (with geometry) or detail modal (without).
     *  When the place isn't among the loaded hits (e.g. deep-linked into a large
     *  browse list), resolve it via /atlas/place/ and open it directly. ``zoom``
     *  restores a shared view's zoom, centred on the place. */
    focusPlace(placeId, zoom) {
        if (!placeId || !this.cfg) return;
        const idx = this.hits.findIndex(h => h && h.place_id === placeId);
        if (idx >= 0) {
            this._clearSelection();
            this._scrollToRow(idx);
            const row = this.els && this.els.rows.querySelector(`.placelist-row[data-idx="${idx}"]`);
            if (row) row.classList.add('pl-selected');
            this._openHit(this.hits[idx], zoom);
            return;
        }
        // Not on this page — resolve the record to decide popup vs modal.
        fetch('/atlas/place/?id=' + encodeURIComponent(placeId), { credentials: 'same-origin' })
            .then(r => (r.ok ? r.json() : null))
            .then(place => {
                if (place && hitGeometry(place)) {
                    this._openHit(place, zoom);
                } else {
                    if (this.cfg.onPlaceFocused) this.cfg.onPlaceFocused(placeId);
                    this.cfg.openPortal(placeId);
                }
            })
            .catch(() => { this.cfg.openPortal(placeId); });
    },

    /** Scroll the virtual list so row ``idx`` is comfortably in view. */
    _scrollToRow(idx) {
        if (!this.els) return;
        const target = Math.max(0, idx * ROW_H - (this.els.scroll.clientHeight / 2));
        this.els.scroll.scrollTop = target;
        this._render();
    },

    // ── Status line ───────────────────────────────────────────────────────
    _setStatus(text) { if (this.els) this.els.status.textContent = text; },

    _updateStatus() {
        if (!this.hits.length) {
            this._setStatus(this.qstr
                ? 'No matching places.'
                : 'No places to browse in this gazetteer.');
            return;
        }
        let s = `Showing ${this.hits.length.toLocaleString()}`;
        // "of Y" only when browsing — a search's total is an approximate candidate
        // pool (place#127), so we don't imply an exact match count.
        if (!this.qstr && this.total != null && this.total > this.hits.length) {
            s += ` of ${this.total.toLocaleString()}`;
        }
        if (this.hits.length >= OFFSET_CAP && this.total > this.hits.length) {
            s += ' · refine with search or filters to see more';
        }
        this._setStatus(s);
    },
};

export default PlaceList;
