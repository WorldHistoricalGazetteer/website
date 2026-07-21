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

const PAGE_SIZE = 100;      // initial fetch + growth increment
const MAX_SIZE = 10000;     // ES max_result_window — the hard browse ceiling
const ROW_H = 56;           // px — fixed row height (enables simple virtualisation)
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

const PlaceList = {
    cfg: null,
    ns: null, label: '',
    hits: [], aatLabels: {}, total: null,
    qstr: '', reqSeq: 0, loading: false, hasMore: false,
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
        this.qstr = ''; this.hasMore = false;
        this.els.search.value = '';
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
    },

    // ── Data ──────────────────────────────────────────────────────────────
    _fetchPage(reset) {
        if (!this.cfg || this.loading) return;
        const size = reset ? PAGE_SIZE : Math.min(this.hits.length + PAGE_SIZE, MAX_SIZE);
        if (!reset && (!this.hasMore || this.hits.length >= MAX_SIZE)) return;

        const seq = reset ? ++this.reqSeq : this.reqSeq;
        this.loading = true;
        if (reset) {
            this.hits = [];
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
        opts.size = size;
        opts.cluster = false;
        opts.bounds = { type: 'GeometryCollection', geometries: [] };
        opts.spatial = 'none';

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
                this.hits = Array.isArray(data.hits) ? data.hits : [];
                this.total = (typeof data.total === 'number') ? data.total : this.hits.length;
                this.aatLabels = {};
                ((data.facets && data.facets.aat_types) || [])
                    .forEach(f => { this.aatLabels[f.aat_id] = f.label; });
                // A full page implies more may exist (until the ceiling).
                this.hasMore = this.hits.length >= size && this.hits.length < MAX_SIZE;
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
        const types = (hit.aat_ids || [])
            .map(id => this.aatLabels[id]).filter(Boolean).slice(0, 3);
        const ccodes = (hit.ccodes || []).slice(0, 3);
        let meta = '';
        if (nogeom) {
            meta += `<span class="pl-nogeom" title="No location — opens as a detail card">no location</span>`;
        }
        meta += types.map(t => `<span class="pl-chip pl-type">${esc(t)}</span>`).join('');
        meta += ccodes.map(c => `<span class="pl-chip pl-cc">${esc(c)}</span>`).join('');
        return `<div class="placelist-row${nogeom ? ' pl-nogeom-row' : ''}" data-idx="${i}" style="top:${i * ROW_H}px">`
            + `<div class="pl-row-title" title="${title}">${title}</div>`
            + `<div class="pl-row-meta">${meta}</div>`
            + `</div>`;
    },

    // ── Interaction ───────────────────────────────────────────────────────
    _onRowClick(idx) {
        const hit = this.hits[idx];
        if (!hit) return;
        this.els.rows.querySelectorAll('.placelist-row.pl-selected')
            .forEach(r => r.classList.remove('pl-selected'));
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
                : 'No places to browse — type to search this gazetteer.');
            return;
        }
        let s = `Showing ${this.hits.length.toLocaleString()}`;
        if (this.total != null && this.total > this.hits.length) s += ` of ${this.total.toLocaleString()}`;
        if (this.hits.length >= MAX_SIZE) s += ' · refine your search to see more';
        this._setStatus(s);
    },
};

export default PlaceList;
