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

// Type labels for a hit: AAT-resolved labels (from the result facets) when the
// place has an AAT mapping, else the source's own ``types[].sourceLabel`` so
// custom / unmapped types are still shown (place#122).
function hitTypeLabels(hit, aatLabels) {
    const seen = new Set();
    const out = [];
    const add = (label) => {
        if (label == null || label === '') return;
        const s = String(label);
        const k = s.toLowerCase();
        if (!seen.has(k)) { seen.add(k); out.push(s); }
    };
    (hit.aat_ids || []).forEach(id => add(aatLabels && aatLabels[id]));
    if (!out.length) {
        (hit.types || []).forEach(t => {
            if (t && typeof t === 'object') add(t.sourceLabel || t.label || t.identifier);
        });
    }
    return out;
}

// Alternate name forms (toponym variants) for a hit, other than its headline
// title. The gateway packs surface forms into comma-joined ``names[].label``
// (same shape the cluster cards read), so split, trim and de-dupe (ci), and
// drop the one that merely replicates the title.
function hitVariants(hit) {
    const titleKey = String(hit.title || '').trim().toLowerCase();
    const seen = new Set();
    const out = [];
    (hit.names || []).forEach(n => String((n && n.label) || '').split(',').forEach(t => {
        const s = t.trim();
        const k = s.toLowerCase();
        if (s && k !== titleKey && !seen.has(k)) { seen.add(k); out.push(s); }
    }));
    return out;
}

// Variant-name line for a row, mirroring the cluster cards' truncation rule
// (atlas.js ``toponymsList``): up to 5 shown inline; beyond that, the first 3
// plus a "<n> more" indicator. The list is fixed-row-height virtualised, so a
// row can't grow — the full set is offered via the ``title`` tooltip rather than
// an expanding <details> toggle.
function variantsHtml(variants) {
    if (!variants.length) return '';
    const full = variants.join(', ');
    let inner;
    if (variants.length <= 5) {
        inner = `<span class="pl-variant-names">${variants.map(esc).join(', ')}</span>`;
    } else {
        const shown = variants.slice(0, 3).map(esc).join(', ');
        const more = variants.length - 3;
        inner = `<span class="pl-variant-names">${shown}</span>`
            + `<span class="pl-variant-more">+${more} more</span>`;
    }
    return `<div class="pl-row-variants" title="${esc(full)}">${inner}</div>`;
}

const PlaceList = {
    cfg: null,
    ns: null, label: '',
    hits: [], aatLabels: {}, total: null,
    qstr: '', ccode: '', typeVal: '',   // active Country / Type facet selections
    //  typeVal encodes the Type dropdown pick: '' | 'aat:<id>' | 'src:<identifier>'
    reqSeq: 0, loading: false, hasMore: false,
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
        this.els.count.textContent = '';
        this.els.rows.innerHTML = '';
        this.els.rows.style.height = '0px';
        this.els.scroll.scrollTop = 0;
        this.cfg.showPanelView(VIEW_ID);
        this._fetchPage(true);
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
            count: view.querySelector('.placelist-count'),
            search: view.querySelector('.placelist-search'),
            filters: view.querySelector('.placelist-filters'),
            ccodeSel: view.querySelector('.placelist-ccode'),
            typeSel: view.querySelector('.placelist-type'),
            status: view.querySelector('.placelist-status'),
            scroll: view.querySelector('.placelist-scroll'),
            rows: view.querySelector('.placelist-rows'),
        };
        if (this.wired) return;
        this.wired = true;

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
        opts.mode = 'in';           // substring "contains" match (not exact/fuzzy)
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
                this.els.count.textContent = this.total != null
                    ? `${this.total.toLocaleString()} place${this.total === 1 ? '' : 's'}` : '';
                this._render();
                this._updateStatus();
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
        const title = esc(hit.title || hit.place_id || '(untitled)');
        const nogeom = !hitGeometry(hit);
        const types = hitTypeLabels(hit, this.aatLabels).slice(0, 3);
        const ccodes = (hit.ccodes || []).slice(0, 3);
        const variants = variantsHtml(hitVariants(hit));
        let meta = '';
        if (nogeom) {
            meta += `<span class="pl-nogeom" title="No location — opens as a detail card">no location</span>`;
        }
        meta += types.map(t => `<span class="pl-chip pl-type">${esc(t)}</span>`).join('');
        meta += ccodes.map(c => `<span class="pl-chip pl-cc">${esc(c)}</span>`).join('');
        return `<div class="placelist-row${nogeom ? ' pl-nogeom-row' : ''}" data-idx="${i}" style="top:${i * ROW_H}px">`
            + `<div class="pl-row-title" title="${title}">${title}</div>`
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

        const geom = hitGeometry(hit);
        if (geom) {
            let bb = null;
            try { if (typeof window.bbox === 'function') bb = window.bbox(geom); } catch (e) { /* */ }
            if (bb) { try { heroMap.map.fitViewport(bb, 9); } catch (e) { /* */ } }
            const lngLat = (Array.isArray(hit.repr_point) && hit.repr_point.length >= 2)
                ? [hit.repr_point[0], hit.repr_point[1]]
                : (bb ? [(bb[0] + bb[2]) / 2, (bb[1] + bb[3]) / 2] : null);
            if (lngLat) heroMap.openPlacePopup(hit.place_id, lngLat);
        } else {
            // No geometry — the standalone detail modal over the map area.
            this.cfg.openPortal(hit.place_id);
        }
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
        if (this.total != null && this.total > this.hits.length) s += ` of ${this.total.toLocaleString()}`;
        if (this.hits.length >= OFFSET_CAP && this.total > this.hits.length) {
            s += ' · refine with search or filters to see more';
        }
        this._setStatus(s);
    },
};

export default PlaceList;
