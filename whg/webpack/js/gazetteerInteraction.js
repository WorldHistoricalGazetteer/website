// /whg/webpack/js/gazetteerInteraction.js
//
// Hover-cursor + click-popup behaviour for gazetteer features in Atlas
// Explore mode. Owned by heroMap; one instance per heroMap.
//
// Lifecycle: heroMap.showGazetteer() calls attach(id, baseIds) once the
// gazetteer's layers are in place; hideGazetteer() and the "switching
// gazetteer" branch call detach() before the source is erased.
//
// Click target layers: ``_fill``, ``_line``, ``_circle`` for each base ID
// produced by whg_maplibre.loadGazetteerStyle. Heatmap is excluded because
// cluster centroids carry no individual ``place_id``; labels are excluded
// because labels-without-shapes feel ambiguous.
//
// Popup data is fetched from ``/entity/place:{id}/api?variant=popup`` which
// returns the raw gateway PlaceDetail JSON. Rendering happens client-side
// so the full data is inspectable from the browser console.

const API_PREFIX = '/entity/place:';
const API_SUFFIX = '/api?variant=popup';
const SHAPE_SUFFIXES = ['_fill', '_line', '_circle'];
const POPUP_CLASS = 'whg-gazetteer-popup-anchor';
const LOADING_HTML = '<div class="whg-gazetteer-popup"><div class="popup-loading">Loading…</div></div>';
const ERROR_HTML = '<div class="whg-gazetteer-popup"><div class="popup-loading">Failed to load place details.</div></div>';

/* ─── HTML / URL helpers ─────────────────────────────────────────────── */

function esc(s) {
    if (s == null) return '';
    return String(s)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

/** Allow only http/https/mailto and same-origin paths to land in href attrs. */
function safeUrl(s) {
    if (!s) return '';
    const t = String(s).trim();
    if (/^(https?:\/\/|mailto:|\/)/i.test(t)) return t;
    return '';
}

// Per-namespace human-readable web URL templates. ``<id>`` is replaced
// with the authority's local id (the part after ``<namespace>:``).
// Namespaces without an entry fall back to ``/entity/place:<id>`` —
// the in-WHG resolver — which is the correct home for WHG-only
// gazetteers that have no public web equivalent (gb, iv, un, po, …).
//
// TODO: source from ``GazetteerRegistryEntry`` once the indexing
// pipeline (processing/settings.py: AUTHORITIES) pushes a ``web_item``
// template alongside the existing ``api_item``, and Django exposes it
// to the Atlas page (e.g. via ``available_sources``).
const NAMESPACE_WEB_TEMPLATES = {
    pl:  'https://pleiades.stoa.org/places/<id>',
    gn:  'https://www.geonames.org/<id>',
    tgn: 'https://vocab.getty.edu/tgn/<id>',
    wd:  'https://www.wikidata.org/wiki/<id>',
    loc: 'https://www.loc.gov/item/<id>/',
    tm:  'https://www.trismegistos.org/place/<id>',
};

/** Map a related place_id to a useful link target.
 *  - ``osm:r123`` / ``ohm:r123`` → ``{host}/relation/123`` (n=node, w=way)
 *  - any namespace in ``NAMESPACE_WEB_TEMPLATES`` → that authority's
 *    human-readable page (e.g. wikidata.org/wiki/Q84)
 *  - everything else → ``/entity/place:<id>`` for in-WHG resolution.
 *  Returns ``''`` if the id is empty or an unrecognised osm/ohm shape. */
function relatedPlaceUrl(relatedId) {
    if (!relatedId) return '';
    // OSM and OHM share the OSM data model: feature ids encode a kind
    // (r=relation, n=node, w=way) followed by the numeric id.
    const osmShape = relatedId.match(/^(osm|ohm):([rnw])(\d+)/);
    if (osmShape) {
        const host = osmShape[1] === 'osm' ? 'www.openstreetmap.org' : 'www.openhistoricalmap.org';
        const kind = { r: 'relation', n: 'node', w: 'way' }[osmShape[2]];
        return `https://${host}/${kind}/${osmShape[3]}`;
    }
    if (relatedId.startsWith('osm:') || relatedId.startsWith('ohm:')) return '';
    const colonIx = relatedId.indexOf(':');
    if (colonIx > 0) {
        const ns = relatedId.slice(0, colonIx);
        const localId = relatedId.slice(colonIx + 1);
        const template = NAMESPACE_WEB_TEMPLATES[ns];
        if (template && localId) {
            return template.replace('<id>', encodeURIComponent(localId));
        }
    }
    return `/entity/place:${encodeURIComponent(relatedId)}`;
}

// Some authorities set ``boundary`` to a gazetteer-source marker rather
// than an admin-level code (e.g. ``"indexvillaris"`` on iv: places).
// Those are not useful to surface as a chip — drop them at render time.
// TODO: clean up server-side so this list isn't needed.
const NON_ADMIN_BOUNDARIES = new Set(['indexvillaris']);

/* ─── Section renderers ──────────────────────────────────────────────── */

function renderTimespan(ts) {
    if (!ts) return '';
    const start = ts.start == null ? '?' : ts.start;
    const end = ts.end == null ? '?' : ts.end;
    return `${esc(start)}–${esc(end)}`;
}

function renderTimespanList(arr) {
    if (!Array.isArray(arr) || arr.length === 0) return '';
    return arr.map(renderTimespan).join(', ');
}

function renderHeader(data) {
    const placeId = data.place_id || '';
    const title = data.title || placeId;
    // The title points at the in-WHG record; render as a modal trigger
    // (same UX as relation links to in-WHG places) rather than a new-tab
    // /entity/ link that would land on raw LPF JSON.
    const titleHtml = placeId
        ? `<a class="popup-title whg-place-modal-trigger" href="#" data-whg-place-modal="${esc(placeId)}">${esc(title)}</a>`
        : `<span class="popup-title">${esc(title)}</span>`;
    return `
        <div class="popup-header">
            ${titleHtml}
            ${placeId ? `<div class="popup-place-id" title="Source identifier">${esc(placeId)}</div>` : ''}
        </div>
    `;
}

function renderDepictionHero(data) {
    const dep = (data.depictions || []).find((d) => d && (d['@id'] || d.id));
    if (!dep) return '';
    const url = safeUrl(dep['@id'] || dep.id);
    if (!url) return '';
    const title = dep.title || '';
    const license = dep.license || '';
    return `
        <div class="popup-depiction">
            <img src="${esc(url)}" alt="${esc(title)}" loading="lazy">
            ${(title || license) ? `<div class="popup-depiction-caption">${title ? esc(title) : ''}${license ? `<span class="popup-license">${esc(license)}</span>` : ''}</div>` : ''}
        </div>
    `;
}

function renderChips(data) {
    const chips = [];
    // Types — prefer the authority's ``sourceLabel`` over the WHG-resolved
    // ``label`` (the latter is often a generic AAT translation that loses
    // the gazetteer's own wording). Fall back to identifier so types never
    // silently disappear.
    const seenTypeLabels = new Set();
    let isPrimary = true;
    for (const t of data.types || []) {
        if (!t || typeof t !== 'object') continue;
        const label = t.sourceLabel || t.label || t.identifier || '';
        if (!label || seenTypeLabels.has(label)) continue;
        seenTypeLabels.add(label);
        const cls = isPrimary ? 'popup-chip popup-chip-type' : 'popup-chip popup-chip-type popup-chip-secondary';
        chips.push(`<span class="${cls}">${esc(label)}</span>`);
        isPrimary = false;
    }
    if (data.boundary && !NON_ADMIN_BOUNDARIES.has(String(data.boundary).toLowerCase())) {
        chips.push(`<span class="popup-chip popup-chip-admin">${esc(data.boundary)}</span>`);
    }
    for (const cc of data.ccodes || []) {
        chips.push(`<span class="popup-chip popup-chip-cc">${esc(cc)}</span>`);
    }
    if (chips.length === 0) return '';
    return `<div class="popup-chips">${chips.join('')}</div>`;
}

function renderMetaList(data) {
    const rows = [];
    const ts = (data.timespans || [])[0];
    if (ts && (ts.start != null || ts.end != null)) {
        rows.push(`<dt>Active</dt><dd>${renderTimespan(ts)}</dd>`);
    }
    if (typeof data.population === 'number' && data.population > 0) {
        rows.push(`<dt>Population</dt><dd>${esc(data.population.toLocaleString())}</dd>`);
    }
    if (typeof data.elevation === 'number') {
        rows.push(`<dt>Elevation</dt><dd>${esc(data.elevation)} m</dd>`);
    }
    if (rows.length === 0) return '';
    return `<dl class="popup-meta-list">${rows.join('')}</dl>`;
}

function renderDescription(data) {
    const descs = (data.descriptions || []).filter((d) => d && d.value);
    if (descs.length === 0) return '';
    const primary = descs[0];
    const truncated = primary.value.length > 320 ? primary.value.slice(0, 320) + '…' : primary.value;
    let html = `<p class="popup-description">${esc(truncated)}</p>`;
    if (descs.length > 1) {
        const extra = descs.slice(1).map((d) => `
            <p class="popup-description-extra">
                ${d.lang ? `<span class="popup-lang">${esc(d.lang)}</span>` : ''}
                ${esc(d.value.length > 320 ? d.value.slice(0, 320) + '…' : d.value)}
            </p>
        `).join('');
        const more = descs.length - 1;
        html += `
            <details class="popup-section">
                <summary>${more} more description${more === 1 ? '' : 's'}</summary>
                <div class="popup-section-body">${extra}</div>
            </details>
        `;
    }
    return html;
}

function renderNames(data) {
    const title = data.title || '';
    const names = (data.names || []).filter((n) => n && n.label && n.label !== title);
    if (names.length === 0) return '';
    const items = names.map((n) => `
        <li>
            <span class="popup-name-label">${esc(n.label)}</span>
            ${n.lang ? `<span class="popup-lang">${esc(n.lang)}</span>` : ''}
            ${(n.timespans && n.timespans.length) ? `<span class="popup-timespan-marker">${esc(renderTimespanList(n.timespans))}</span>` : ''}
        </li>
    `).join('');
    return `
        <details class="popup-section" open>
            <summary>Alternative names (${names.length})</summary>
            <div class="popup-section-body"><ul class="popup-name-list">${items}</ul></div>
        </details>
    `;
}

function renderTemporalGeometries(data) {
    // Only surface geometries that carry timespans — the geometry shape
    // itself is already rendered on the map. We deliberately do NOT print
    // coordinates: large polygons would dump a wall of numbers into the popup.
    const geoms = (data.geometries || []).filter((g) => g && g.timespans && g.timespans.length);
    if (geoms.length === 0) return '';
    const items = geoms.map((g) => {
        const type = (g.geom && g.geom.type) || 'Geometry';
        return `
            <li>
                <span class="popup-geom-type">${esc(type)}</span>
                <span class="popup-timespan-marker">${esc(renderTimespanList(g.timespans))}</span>
            </li>
        `;
    }).join('');
    return `
        <details class="popup-section" open>
            <summary>Geometry temporal validity</summary>
            <div class="popup-section-body"><ul class="popup-geom-list">${items}</ul></div>
        </details>
    `;
}

/** Render a place-id reference as either an external-tab link (when the
 *  id maps to a public authority page via NAMESPACE_WEB_TEMPLATES) or a
 *  modal trigger (when it would otherwise resolve to ``/entity/place:…``,
 *  which has no human page for CRC records). */
function renderPlaceRefHTML(relatedId, label) {
    const text = label || relatedId || '';
    if (!relatedId) return `<span>${esc(text)}</span>`;
    const url = relatedPlaceUrl(relatedId);
    if (!url) return `<span>${esc(text)}</span>`;
    if (url.startsWith('/entity/')) {
        return `<a class="whg-place-modal-trigger" href="#" data-whg-place-modal="${esc(relatedId)}">${esc(text)}</a>`;
    }
    return `<a href="${esc(url)}" target="_blank" rel="noopener">${esc(text)}</a>`;
}

function renderRelations(data) {
    const rels = (data.relations || []).filter((r) => r && (r.relation_type || r.related_place_id));
    if (rels.length === 0) return '';
    const items = rels.map((r) => {
        const label = r.label || r.related_place_id || '';
        const tsHtml = (r.timespans && r.timespans.length)
            ? `<span class="popup-timespan-marker">${esc(renderTimespanList(r.timespans))}</span>`
            : '';
        return `
            <li>
                ${r.relation_type ? `<span class="popup-relation-type">${esc(r.relation_type)}</span>` : ''}
                ${renderPlaceRefHTML(r.related_place_id || '', label)}
                ${tsHtml}
            </li>
        `;
    }).join('');
    return `
        <details class="popup-section">
            <summary>Relations (${rels.length})</summary>
            <div class="popup-section-body"><ul class="popup-relation-list">${items}</ul></div>
        </details>
    `;
}

function renderLinks(data) {
    const links = (data.links || []).filter((l) => l && l.identifier);
    if (links.length === 0) return '';
    // Group by host (URL) or namespace prefix, matching what the user
    // visually expects when scanning for "the wikipedia link" etc.
    const groups = new Map();
    for (const l of links) {
        let key = '';
        const id = l.identifier;
        if (/^https?:\/\//i.test(id)) {
            try {
                const u = new URL(id);
                key = u.host.replace(/^www\./, '');
            } catch (e) { key = id; }
        } else if (id.includes(':')) {
            key = id.split(':', 1)[0];
        } else {
            key = l.type || '';
        }
        if (!groups.has(key)) groups.set(key, []);
        groups.get(key).push(l);
    }
    const groupHtml = [...groups.entries()].sort((a, b) => a[0].localeCompare(b[0])).map(([authority, items]) => {
        const itemHtml = items.map((l) => {
            const url = safeUrl(l.identifier);
            const display = l.identifier.length > 56 ? l.identifier.slice(0, 53) + '…' : l.identifier;
            const linkHtml = url
                ? `<a href="${esc(url)}" target="_blank" rel="noopener">${esc(display)}</a>`
                : `<span>${esc(display)}</span>`;
            const typeHtml = (l.type && l.type !== 'closeMatch') ? `<span class="popup-link-type">${esc(l.type)}</span>` : '';
            return `<li>${linkHtml}${typeHtml}</li>`;
        }).join('');
        return `
            <div class="popup-link-group">
                <span class="popup-link-authority">${esc(authority)}</span>
                <ul class="popup-link-list">${itemHtml}</ul>
            </div>
        `;
    }).join('');
    return `
        <details class="popup-section">
            <summary>External links</summary>
            <div class="popup-section-body">${groupHtml}</div>
        </details>
    `;
}

function renderDepictionGallery(data) {
    const deps = (data.depictions || []).filter((d) => d && (d['@id'] || d.id));
    if (deps.length <= 1) return '';
    const extra = deps.slice(1).map((d) => {
        const url = safeUrl(d['@id'] || d.id);
        if (!url) return '';
        return `<a href="${esc(url)}" target="_blank" rel="noopener" title="${esc(d.title || '')}"><img src="${esc(url)}" alt="${esc(d.title || '')}" loading="lazy"></a>`;
    }).join('');
    if (!extra) return '';
    const more = deps.length - 1;
    return `
        <details class="popup-section">
            <summary>${more} more image${more === 1 ? '' : 's'}</summary>
            <div class="popup-section-body"><div class="popup-depiction-gallery">${extra}</div></div>
        </details>
    `;
}

/** Compose the full popup HTML from a gateway PlaceDetail dict. */
function renderPopup(data) {
    return `
<div class="whg-gazetteer-popup">
    ${renderHeader(data)}
    ${renderDepictionHero(data)}
    ${renderChips(data)}
    <div class="popup-body">
        ${renderMetaList(data)}
        ${renderDescription(data)}
        ${renderNames(data)}
        ${renderTemporalGeometries(data)}
        ${renderRelations(data)}
        ${renderLinks(data)}
        ${renderDepictionGallery(data)}
    </div>
</div>`;
}

/* ─── Place-detail modal (shared singleton) ──────────────────────────── */
//
// Internal-WHG place references can't open a useful new tab — for CRC
// places ``/entity/place:<id>`` redirects straight to the LPF JSON.
// We instead pop a Bootstrap modal containing the HTML preview snippet
// returned by ``/entity/place:<id>/preview`` (the same OpenRefine-style
// preview that the reconciliation API exposes).

let _modalEl = null;
let _modalBody = null;
let _modalInstance = null;
let _modalAbort = null;

function _ensureModal() {
    if (_modalEl) return;
    _modalEl = document.createElement('div');
    _modalEl.className = 'modal fade whg-place-modal';
    _modalEl.tabIndex = -1;
    _modalEl.setAttribute('aria-hidden', 'true');
    _modalEl.innerHTML = `
        <div class="modal-dialog modal-dialog-centered modal-dialog-scrollable">
            <div class="modal-content">
                <div class="modal-header">
                    <h5 class="modal-title">Place</h5>
                    <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                </div>
                <div class="modal-body whg-place-modal-body">
                    <div class="popup-loading">Loading…</div>
                </div>
            </div>
        </div>
    `;
    document.body.appendChild(_modalEl);
    _modalBody = _modalEl.querySelector('.whg-place-modal-body');
    if (typeof bootstrap !== 'undefined' && bootstrap.Modal) {
        _modalInstance = new bootstrap.Modal(_modalEl);
    }
    _modalEl.addEventListener('hidden.bs.modal', () => {
        if (_modalAbort) {
            try { _modalAbort.abort(); } catch (e) {}
            _modalAbort = null;
        }
        _modalBody.innerHTML = '<div class="popup-loading">Loading…</div>';
    });
}

function openPlaceModal(placeId) {
    if (!placeId) return;
    _ensureModal();
    if (!_modalInstance) {
        // Bootstrap unavailable — fall back to opening the preview in a tab.
        window.open(`/entity/place:${encodeURIComponent(placeId)}/preview`, '_blank', 'noopener');
        return;
    }
    _modalBody.innerHTML = '<div class="popup-loading">Loading…</div>';
    _modalInstance.show();
    if (_modalAbort) { try { _modalAbort.abort(); } catch (e) {} }
    _modalAbort = new AbortController();
    const headers = { 'Accept': 'text/html' };
    if (typeof window !== 'undefined' && window.csrfToken) {
        headers['X-CSRFToken'] = window.csrfToken;
    }
    const url = `/entity/place:${encodeURIComponent(placeId)}/preview`;
    fetch(url, { signal: _modalAbort.signal, credentials: 'same-origin', headers })
        .then((r) => {
            if (!r.ok) throw new Error(`HTTP ${r.status}`);
            return r.text();
        })
        .then((html) => { _modalBody.innerHTML = html; })
        .catch((err) => {
            if (err.name === 'AbortError') return;
            _modalBody.innerHTML = '<div class="popup-loading">Failed to load place details.</div>';
        });
}

// Single document-level delegate so the popup can be re-rendered freely
// without having to re-bind handlers each time setHTML is called.
let _modalDelegateBound = false;
function _bindModalDelegate() {
    if (_modalDelegateBound) return;
    _modalDelegateBound = true;
    document.addEventListener('click', (ev) => {
        const trigger = ev.target.closest && ev.target.closest('[data-whg-place-modal]');
        if (!trigger) return;
        ev.preventDefault();
        openPlaceModal(trigger.getAttribute('data-whg-place-modal'));
    });
}

/* ─── Controller ─────────────────────────────────────────────────────── */

export default class GazetteerInteraction {
    constructor(map) {
        this.map = map;
        this._currentId = null;
        this._handlers = [];
        this._popup = null;
        this._abortController = null;
        _bindModalDelegate();
    }

    /**
     * Wire mouseenter/mouseleave/click on every shape layer of the
     * supplied gazetteer. Idempotent — calls detach() first.
     */
    attach(id, baseIds) {
        this.detach();
        if (!this.map || !id || !Array.isArray(baseIds) || baseIds.length === 0) return;
        this._currentId = id;
        for (const baseId of baseIds) {
            for (const suffix of SHAPE_SUFFIXES) {
                const layerId = `${baseId}${suffix}`;
                if (!this.map.getLayer(layerId)) continue;
                const enter = () => this._onEnter();
                const leave = () => this._onLeave();
                const click = (e) => this._onClick(e);
                this.map.on('mouseenter', layerId, enter);
                this.map.on('mouseleave', layerId, leave);
                this.map.on('click', layerId, click);
                this._handlers.push({ layerId, enter, leave, click });
            }
        }
    }

    /** Remove all registered handlers, abort pending fetch, hide popup. */
    detach() {
        if (this._abortController) {
            try { this._abortController.abort(); } catch (e) {}
            this._abortController = null;
        }
        if (this._popup) {
            try { this._popup.remove(); } catch (e) {}
        }
        if (this.map) {
            for (const { layerId, enter, leave, click } of this._handlers) {
                try { this.map.off('mouseenter', layerId, enter); } catch (e) {}
                try { this.map.off('mouseleave', layerId, leave); } catch (e) {}
                try { this.map.off('click', layerId, click); } catch (e) {}
            }
            this.map.getCanvas().style.cursor = '';
        }
        this._handlers = [];
        this._currentId = null;
    }

    _onEnter() {
        if (!this.map) return;
        this.map.getCanvas().style.cursor = 'pointer';
    }

    _onLeave() {
        if (!this.map) return;
        this.map.getCanvas().style.cursor = '';
    }

    _onClick(e) {
        if (!this.map || !e.features || e.features.length === 0) return;
        const placeId = e.features[0].properties && e.features[0].properties.place_id;
        if (!placeId) return;

        if (this._abortController) {
            try { this._abortController.abort(); } catch (err) {}
        }
        this._abortController = new AbortController();
        const signal = this._abortController.signal;

        if (!this._popup) {
            this._popup = new whg_maplibre.Popup({
                closeButton: true,
                // Manage close manually so back-to-back feature clicks
                // re-target the same popup instead of MapLibre's
                // outside-click listener tearing it down.
                closeOnClick: false,
                maxWidth: '400px',
                className: POPUP_CLASS,
            });
        }
        this._popup
            .setLngLat(e.lngLat)
            .setHTML(LOADING_HTML)
            .addTo(this.map);

        const url = `${API_PREFIX}${encodeURIComponent(placeId)}${API_SUFFIX}`;
        // ``EntityFeatureView`` extends ``AuthenticatedAPIView``. Anonymous
        // visitors authenticate via the CSRF-token fallback in
        // ``TokenQueryOrBearerAuthentication``: a request bearing a valid
        // ``X-CSRFToken`` is accepted as ``CSRFUser`` (an AnonymousUser
        // whose ``is_authenticated`` is True). ``window.csrfToken`` is set
        // in base.js from the page's ``<meta name="csrf-token">`` tag.
        const headers = { 'Accept': 'application/json' };
        if (typeof window !== 'undefined' && window.csrfToken) {
            headers['X-CSRFToken'] = window.csrfToken;
        }
        fetch(url, { signal, credentials: 'same-origin', headers })
            .then((r) => {
                if (!r.ok) throw new Error(`HTTP ${r.status}`);
                return r.json();
            })
            .then((data) => {
                if (signal.aborted) return;
                // Expose the raw gateway response so the developer console
                // is enough to inspect any field we don't surface yet.
                if (typeof console !== 'undefined') {
                    console.log('[WHG popup]', placeId, data);
                }
                this._popup.setHTML(renderPopup(data));
            })
            .catch((err) => {
                if (err.name === 'AbortError') return;
                if (signal.aborted) return;
                this._popup.setHTML(ERROR_HTML);
            });
    }
}
