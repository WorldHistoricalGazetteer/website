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
import { getPreferredLanguage } from './languages.js';
import { variantLabels } from './toponyms.js';

// Popup data is fetched from ``/entity/place:{id}/api?variant=popup`` which
// returns the raw gateway PlaceDetail JSON. Rendering happens client-side
// so the full data is inspectable from the browser console.

const API_PREFIX = '/entity/place:';
const API_SUFFIX = '/api?variant=popup';
const SHAPE_SUFFIXES = ['_fill', '_line', '_circle'];
const POPUP_CLASS = 'whg-gazetteer-popup-anchor';
const HOVER_POPUP_CLASS = 'whg-hover-popup-anchor';
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
// Built-in fallbacks used until the registry map loads (and for resilience if
// it fails). The authoritative per-namespace ``web_item`` templates now come
// from ``GazetteerRegistryEntry`` via ``/api/sources/`` (place#121) — atlas.js
// fetches them and calls ``setWebTemplates()`` below, which merges them in
// (registry values win, and extend this to gazetteers not listed here, incl.
// user datasets). ``<id>`` is replaced with the place's local id at build time.
const NAMESPACE_WEB_TEMPLATES = {
    pl:  'https://pleiades.stoa.org/places/<id>',
    gn:  'https://www.geonames.org/<id>',
    tgn: 'https://vocab.getty.edu/tgn/<id>',
    wd:  'https://www.wikidata.org/wiki/<id>',
    loc: 'https://www.loc.gov/item/<id>/',
    tm:  'https://www.trismegistos.org/place/<id>',
};

/** Merge registry-supplied ``{namespace: web_item}`` templates into the map.
 *  Registry values override/extend the built-in fallbacks. Called by atlas.js
 *  once ``/api/sources/`` resolves. */
export function setWebTemplates(map) {
    if (!map || typeof map !== 'object') return;
    for (const [ns, tpl] of Object.entries(map)) {
        if (ns && tpl) NAMESPACE_WEB_TEMPLATES[ns] = tpl;
    }
}

// AAT place-type vocabulary (id → {label, desc}), prefetched by atlas.js and
// cached in IndexedDB (place#122). Empty until set; tooltips degrade to the
// aat:<id> alone when a concept isn't in the vocab.
let AAT_VOCAB = {};
export function setAatVocab(byId) {
    if (byId && typeof byId === 'object') AAT_VOCAB = byId;
}

/** The AAT concept id for a popup type entry, or '' — from the resolved
 *  ``aat_ids`` the gateway now ships, else an ``aat:NNN`` identifier. */
function typeAatKey(t) {
    if (t && Array.isArray(t.aat_ids) && t.aat_ids.length) return `aat:${t.aat_ids[0]}`;
    const id = t && t.identifier;
    return (typeof id === 'string' && /^aat:\d+$/.test(id)) ? id : '';
}

// Point-of-use attribution for AAT place types (Getty ODC-By 1.0). The full
// statement lives on the Credits page; here we carry the source name in the
// tooltip and link each concept to its canonical Getty URI (both required-
// friendly under ODC-By where the full statement can't fit inline).
const AAT_CREDIT = 'Place type: Getty AAT — ODC-By 1.0';

/** Canonical Getty concept page for an AAT id (bare ``12345`` or ``aat:12345``).
 *  '' when no numeric id can be extracted. */
export function aatUrl(idOrKey) {
    const m = String(idOrKey == null ? '' : idOrKey).match(/(\d+)/);
    return m ? `https://vocab.getty.edu/page/aat/${m[1]}` : '';
}

/** Tooltip text for an AAT concept: the namespaced id, its label, its scope-note
 *  description (when present), and the Getty AAT / ODC-By attribution line.
 *  Accepts a bare numeric id (``12345``) or an ``aat:12345`` key. Exported so the
 *  Explore place list carries the exact same tooltip as the popup (place#122). */
export function aatTooltip(idOrKey) {
    if (idOrKey == null || idOrKey === '') return '';
    const key = /^aat:/.test(String(idOrKey)) ? String(idOrKey) : `aat:${idOrKey}`;
    const v = AAT_VOCAB[key];
    const head = v
        ? (v.desc ? `${key} · ${v.label}\n${v.desc}` : `${key} · ${v.label}`)
        : key;
    return `${head}\n${AAT_CREDIT}`;
}

/** Rich HTML tooltip for an AAT concept — label, id, scope note and the Getty /
 *  ODC-By credit — for Bootstrap tooltips (html:true) in the Gazetteer list.
 *  Every interpolated value is escaped, so the result is safe as innerHTML. */
export function aatTooltipHtml(idOrKey) {
    if (idOrKey == null || idOrKey === '') return '';
    const key = /^aat:/.test(String(idOrKey)) ? String(idOrKey) : `aat:${idOrKey}`;
    const v = AAT_VOCAB[key];
    let html = '<div class="tt-aat">';
    if (v && v.label) html += `<div class="tt-aat-label">${esc(v.label)}</div>`;
    html += `<div class="tt-aat-id">${esc(key)}</div>`;
    if (v && v.desc) html += `<div class="tt-aat-desc">${esc(v.desc)}</div>`;
    html += '<div class="tt-aat-credit">Getty AAT · ODC-By 1.0 — click to open</div>';
    html += '</div>';
    return html;
}

/** Human-readable country name for an ISO code, from the page's global
 *  ``ccode_hash`` (GeoNames/TGN labels). '' when unknown. Shared by the popup
 *  and the place list so both carry the same country tooltip on cc badges. */
export function ccName(cc) {
    const e = ((typeof window !== 'undefined' && window.ccode_hash) || {})[cc];
    return (e && (e.gnlabel || e.tgnlabel)) || '';
}

/** The SOURCE's own human page for a place, built from its namespace's
 *  ``web_item`` template — e.g. ``gn:2988507`` → ``geonames.org/2988507``.
 *  Returns '' when the namespace has no template (WHG-only gazetteers) or the
 *  built URL isn't a safe http(s) link. Used for the popup "view at source"
 *  link (place#121); the header title itself still points at the in-WHG record. */
function sourceItemUrl(placeId) {
    if (!placeId) return '';
    const ix = placeId.indexOf(':');
    if (ix <= 0) return '';
    const ns = placeId.slice(0, ix);
    const localId = placeId.slice(ix + 1);
    const template = NAMESPACE_WEB_TEMPLATES[ns];
    if (!template || !localId) return '';
    return safeUrl(template.replace('<id>', encodeURIComponent(localId)));
}

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
    // Plain title (not a link): the modal-trigger opened an unwanted entity
    // preview, and — being the first focusable element — took a focus outline
    // when the popup opened from a shared ?place= deep link. The "view at source"
    // link below and the in-WHG detail modal (row click) remain the ways in.
    const titleHtml = `<span class="popup-title">${esc(title)}</span>`;
    // When the source publishes a per-item web page (registry ``web_item``),
    // make the source identifier a link to it (new tab); otherwise plain text.
    const srcUrl = sourceItemUrl(placeId);
    const idHtml = !placeId ? ''
        : srcUrl
            ? `<a class="popup-place-id popup-source-link" href="${esc(srcUrl)}" target="_blank" rel="noopener"
                  data-bs-toggle="tooltip" data-bs-title="View this place on the source's website">${esc(placeId)}<i class="fas fa-arrow-up-right-from-square popup-source-ext"></i></a>`
            : `<div class="popup-place-id" data-bs-toggle="tooltip" data-bs-title="Source identifier">${esc(placeId)}</div>`;
    // Copy a shareable deep link (?gazetteer=&place=) to this place. Handled by
    // a document-level delegate in atlas.js (where the URL builder lives).
    const shareHtml = placeId
        ? `<button type="button" class="popup-share" data-share-pid="${esc(placeId)}"
                   data-bs-toggle="tooltip" data-bs-title="Copy a link to this place" aria-label="Copy link to this place">
               <i class="fas fa-share-nodes"></i></button>`
        : '';
    // Title, share button and (when present) the Wikipedia mark share one row
    // (saves vertical space); the source id sits on the line below.
    const wikiHtml = renderWikipediaBadge(data);
    return `
        <div class="popup-header">
            <div class="popup-title-row">
                ${titleHtml}
                ${shareHtml}
                ${wikiHtml}
            </div>
            ${idHtml}
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
        // AAT-mapped types link to their canonical Getty concept page and carry
        // the AAT / ODC-By attribution in the tooltip (place#122; Getty ODC-By);
        // custom types keep the source id as a plain chip.
        const aatKey = typeAatKey(t);
        if (aatKey) {
            chips.push(`<a class="${cls}" href="${esc(aatUrl(aatKey))}" target="_blank"`
                + ` rel="noopener noreferrer" data-aat="${esc(aatKey)}"`
                + ` data-bs-toggle="tooltip" data-bs-html="true"`
                + ` data-bs-title="${esc(aatTooltipHtml(aatKey))}">${esc(label)}</a>`);
        } else {
            const titleAttr = t.identifier
                ? ` data-bs-toggle="tooltip" data-bs-title="${esc(String(t.identifier))}"` : '';
            chips.push(`<span class="${cls}"${titleAttr}>${esc(label)}</span>`);
        }
        isPrimary = false;
    }
    if (data.boundary && !NON_ADMIN_BOUNDARIES.has(String(data.boundary).toLowerCase())) {
        chips.push(`<span class="popup-chip popup-chip-admin">${esc(data.boundary)}</span>`);
    }
    for (const cc of data.ccodes || []) {
        const name = ccName(cc);
        const titleAttr = name ? ` data-bs-toggle="tooltip" data-bs-title="${esc(name)}"` : '';
        chips.push(`<span class="popup-chip popup-chip-cc"${titleAttr}>${esc(cc)}</span>`);
    }
    // Wikipedia mark now lives inline in the title row (renderHeader).
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
    // Drop the canonical title (case-insensitively) — it's already the popup
    // header, so it shouldn't repeat in the variants. Keep the full name objects
    // (lang / timespans) for the expanded detail.
    const titleKey = String(title).trim().toLowerCase();
    const names = (data.names || []).filter(
        (n) => n && n.label && String(n.label).trim().toLowerCase() !== titleKey);
    if (names.length === 0) return '';

    // Compact, truncated preview line (same rule as the cluster cards / place
    // list): up to 5 labels inline, else the first 3 + "+N more". The shared
    // extractor also splits any comma-packed labels and de-dupes.
    const labels = variantLabels(names, title);
    let preview;
    if (labels.length <= 5) {
        preview = `<span class="tv-names">${labels.map(esc).join(', ')}</span>`;
    } else {
        preview = `<span class="tv-names">${labels.slice(0, 3).map(esc).join(', ')}</span>`
            + `<span class="tv-more">+${labels.length - 3} more</span>`;
    }

    // Expanding reveals the full list with per-name language + attested dates.
    const items = names.map((n) => `
        <li>
            <span class="popup-name-label">${esc(n.label)}</span>
            ${n.lang ? `<span class="popup-lang">${esc(n.lang)}</span>` : ''}
            ${(n.timespans && n.timespans.length) ? `<span class="popup-timespan-marker">${esc(renderTimespanList(n.timespans))}</span>` : ''}
        </li>
    `).join('');
    return `
        <details class="popup-section popup-variants">
            <summary><span class="tv-lead">Also known as</span> ${preview}</summary>
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

/** A namespaced place id is a short lowercase namespace prefix followed
 *  by ``:`` and at least one alphanumeric character (e.g. ``wd:Q123``,
 *  ``gn:12345``, ``iv:IV:IV1680``). Bare codes like ``MDG`` or ``450`` —
 *  which authorities sometimes shove into ``related_place_id`` to mean
 *  "this is the ISO-3166 alpha-3 code", not "this is a place reference"
 *  — fail this test and are rendered as plain text alongside their
 *  label rather than as a (broken) link.
 *
 *  Test the identifier shape, not the ``relation_type``: a closeMatch can
 *  point at a non-place identifier just as easily as a hasIdentifier can
 *  point at a real namespaced place. */
const PLACE_ID_PATTERN = /^[a-z]{2,10}:[A-Za-z0-9].*/;
function looksLikePlaceId(id) {
    return PLACE_ID_PATTERN.test(id || '');
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
        const tsHtml = (r.timespans && r.timespans.length)
            ? `<span class="popup-timespan-marker">${esc(renderTimespanList(r.timespans))}</span>`
            : '';
        let bodyHtml;
        if (looksLikePlaceId(r.related_place_id)) {
            const label = r.label || r.related_place_id || '';
            bodyHtml = renderPlaceRefHTML(r.related_place_id, label);
        } else if (r.related_place_id) {
            // Non-place identifier value (e.g. ``hasIdentifier`` →
            // ``ISO 3166-1 alpha-3: MDG``). Show label and value as text.
            const valueText = r.label
                ? `${r.label}: ${r.related_place_id}`
                : r.related_place_id;
            bodyHtml = `<span>${esc(valueText)}</span>`;
        } else {
            bodyHtml = `<span>${esc(r.label || '')}</span>`;
        }
        return `
            <li>
                ${r.relation_type ? `<span class="popup-relation-type">${esc(r.relation_type)}</span>` : ''}
                ${bodyHtml}
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

/** Wikipedia links among the external links, with their language edition. */
function wikipediaLinks(data) {
    const out = [];
    for (const l of (data.links || [])) {
        const id = l && l.identifier;
        if (!id) continue;
        const m = /https?:\/\/([a-z-]+)\.(?:m\.)?wikipedia\.org\/[^\s]+/i.exec(id);
        if (!m) continue;
        const url = safeUrl(id);
        if (url) out.push({ url, lang: m[1].toLowerCase() });
    }
    return out;
}

/** Pick the best Wikipedia article for the badge, honouring the user's
 *  preferred language (persisted via languages.js), then English, then whatever
 *  edition is available. */
function preferredWikipediaLink(data) {
    const wl = wikipediaLinks(data);
    if (!wl.length) return null;
    let pref = 'en';
    try { pref = getPreferredLanguage(); } catch (e) {}
    // 'local' = "use map-local names" — no specific Wikipedia edition, so fall
    // through to English.
    return (pref && pref !== 'local' && wl.find((l) => l.lang === pref))
        || wl.find((l) => l.lang === 'en')
        || wl[0];
}

/** A small circular Wikipedia "W" badge that sits inline among the type / ccode
 *  chips, linking to the preferred-language article. Empty when none. */
function renderWikipediaBadge(data) {
    const link = preferredWikipediaLink(data);
    if (!link) return '';
    const label = link.lang && link.lang !== 'en'
        ? `Read this place on Wikipedia (${link.lang})`
        : 'Read this place on Wikipedia';
    return `<a class="popup-chip-wiki popup-wiki-mark" href="${esc(link.url)}" target="_blank" rel="noopener" data-bs-toggle="tooltip" data-bs-title="${esc(label)}" aria-label="${esc(label)}">W</a>`;
}

/** Colour-code the licence badge by permissiveness of the source's terms. */
function licenceClass(a) {
    const spdx = (a.license__spdx_id || '').toUpperCase();
    const pc = a.license__permits_commercial;
    const ar = a.license__attribution_required;
    const custom = a.license__custom;
    if (!spdx && !a.license__label) return 'lic-unknown';
    // Fully open: CC0 / PDDL / public domain, or commercial-OK with no attribution.
    if (spdx.includes('CC0') || spdx.includes('PDDL') ||
        spdx.indexOf('PUBLIC') === 0 || (pc === true && ar === false)) {
        return 'lic-open';
    }
    // Restrictive: non-commercial, all-rights-reserved, or bespoke terms.
    if (pc === false || spdx.includes('-NC') || spdx.includes('ARR') ||
        custom === true) {
        return 'lic-restrictive';
    }
    // Attribution and/or share-alike required (the CC-BY / CC-BY-SA middle ground).
    return 'lic-attribution';
}

/** "Source & licence" footer: a colour-coded badge (linking to the licence
 *  deed, with a tooltip) built from the registry attribution the server
 *  attaches to the popup payload. Empty when no attribution is available. */
function renderAttribution(data) {
    const a = data && data.attribution;
    if (!a) return '';
    const spdx = a.license__spdx_id || '';
    const label = a.license__label || '';
    const deed = a.license__url || a.license_url || '';
    const rights = a.rights_holder || '';
    const srcName = a.name || '';
    if (!spdx && !label && !rights && !srcName) return '';
    const code = spdx || '©';
    const cls = licenceClass(a);
    const tip = [
        label || (spdx || 'Licence not specified'),
        rights ? `© ${rights}` : '',
        deed ? 'Click for licence terms' : '',
    ].filter(Boolean).join(' · ');
    const inner = `<span class="whg-licence-code">${esc(code)}</span>`;
    const badge = deed
        ? `<a class="whg-licence-badge ${cls}" href="${esc(deed)}" target="_blank" rel="noopener noreferrer" data-bs-toggle="tooltip" data-bs-title="${esc(tip)}">${inner}</a>`
        : `<span class="whg-licence-badge ${cls}" data-bs-toggle="tooltip" data-bs-title="${esc(tip)}">${inner}</span>`;
    const src = srcName
        ? `<span class="whg-licence-source">${esc(srcName)}</span>`
        : '';
    return `
        <div class="popup-attribution">
            <span class="popup-attribution-label">Source &amp; licence</span>
            <span class="popup-attribution-value">${src}${badge}</span>
        </div>`;
}

/** Initialise Bootstrap tooltips inside the (transient) map popup. The popup
 *  DOM lives outside atlas.js's tooltip scopes, so it needs its own init;
 *  mirrors initAtlasTooltips (move native title → data-bs-title so the
 *  browser tooltip doesn't double up). Degrades to the native title when
 *  Bootstrap isn't loaded. */
function initPopupTooltips() {
    const bs = (typeof window !== 'undefined') && window.bootstrap;
    if (!bs || !bs.Tooltip) return;
    document.querySelectorAll('.whg-gazetteer-popup [data-bs-toggle="tooltip"]').forEach((el) => {
        const t = el.getAttribute('title');
        if (t != null) {
            if (!el.getAttribute('data-bs-title')) el.setAttribute('data-bs-title', t);
            el.removeAttribute('title');
        }
        bs.Tooltip.getOrCreateInstance(el, { trigger: 'hover', container: 'body' });
    });
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
    ${renderAttribution(data)}
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
    // No explicit ``Accept`` — only JSONRenderer is registered in DRF
    // (``REST_FRAMEWORK.DEFAULT_RENDERER_CLASSES``), so ``Accept: text/html``
    // would 406 before the view runs. The default ``*/*`` matches
    // JSONRenderer and the view's explicit ``HttpResponse(text/html)``
    // sets the actual response content-type.
    const headers = {};
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

// Styled (Bootstrap) HTML tooltips for popup content (type/cc chips, licence
// badge, share/wiki/source links) — one delegated instance, matching the Explore
// list, instead of native browser title tips. Bootstrap loads via a deferred CDN
// script, so this is called on each openPopup and no-ops once bound.
let _popupTooltipsBound = false;
function _bindPopupTooltips() {
    if (_popupTooltipsBound) return;
    const bs = (typeof window !== 'undefined') && window.bootstrap;
    if (!bs || !bs.Tooltip) return;   // not ready yet — retry on the next popup
    _popupTooltipsBound = true;
    new bs.Tooltip(document.body, {
        selector: '.whg-gazetteer-popup-anchor [data-bs-toggle="tooltip"]',
        html: true,
        container: 'body',
        trigger: 'hover',
        placement: 'top',
        animation: false,   // instant hide so a closing popup can't orphan a tip
        delay: { show: 150, hide: 0 },
    });
}

/* ─── Controller ─────────────────────────────────────────────────────── */

export default class GazetteerInteraction {
    constructor(map) {
        this.map = map;
        this._currentId = null;
        this._handlers = [];
        this._popup = null;
        this._hoverPopup = null;   // lightweight name tooltip (no fetch)
        this._hoverKey = null;     // currently-hovered feature key (dedupe)
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
                const move = (e) => this._onMove(e);
                const leave = () => this._onLeave();
                const click = (e) => this._onClick(e);
                this.map.on('mouseenter', layerId, enter);
                this.map.on('mousemove', layerId, move);
                this.map.on('mouseleave', layerId, leave);
                this.map.on('click', layerId, click);
                this._handlers.push({ layerId, enter, move, leave, click });
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
            this._hidePopupTooltips();
            try { this._popup.remove(); } catch (e) {}
        }
        if (this.map) {
            for (const { layerId, enter, move, leave, click } of this._handlers) {
                try { this.map.off('mouseenter', layerId, enter); } catch (e) {}
                try { this.map.off('mousemove', layerId, move); } catch (e) {}
                try { this.map.off('mouseleave', layerId, leave); } catch (e) {}
                try { this.map.off('click', layerId, click); } catch (e) {}
            }
            this.map.getCanvas().style.cursor = '';
        }
        this._hideHover();
        this._handlers = [];
        this._currentId = null;
    }

    /** Close the place popup (if open) without unbinding the map handlers, so
     *  subsequent feature clicks still work. Aborts any in-flight popup fetch. */
    closePopup() {
        if (this._abortController) {
            try { this._abortController.abort(); } catch (e) {}
            this._abortController = null;
        }
        if (this._popup) {
            this._hidePopupTooltips();
            try { this._popup.remove(); } catch (e) {}
        }
    }

    /** Dismiss any open Bootstrap tooltip inside the popup before it is removed,
     *  so a hovered chip's tooltip can't orphan (mirrors the list's guard). */
    _hidePopupTooltips() {
        const bs = window.bootstrap;
        if (!bs || !bs.Tooltip || !this._popup) return;
        let el = null;
        try { el = this._popup.getElement && this._popup.getElement(); } catch (e) {}
        if (!el) return;
        el.querySelectorAll('[aria-describedby]').forEach(t => {
            const inst = bs.Tooltip.getInstance(t);
            if (inst) { try { inst.hide(); } catch (e) {} }
        });
    }

    _onEnter() {
        if (!this.map) return;
        this.map.getCanvas().style.cursor = 'pointer';
    }

    _onLeave() {
        if (!this.map) return;
        this.map.getCanvas().style.cursor = '';
        this._hideHover();
    }

    /** Lightweight hover label tooltip — renders the feature's name straight
     *  from the vector-tile properties (NO server fetch). The click path (full
     *  detail) is unaffected. Deduped so it only rebuilds when the hovered
     *  feature changes. */
    _onMove(e) {
        if (!this.map || !e.features || e.features.length === 0) return;
        const f = e.features[0];
        const p = f.properties || {};
        const key = p.place_id
            || (p.point_count != null ? `cluster:${p.point_count}:${Math.round(e.lngLat.lng * 1e4)}` : null)
            || `${e.lngLat.lng},${e.lngLat.lat}`;
        if (key === this._hoverKey && this._hoverPopup) return;   // same feature — nothing to do
        const label = this._featureLabel(p);
        if (!label) { this._hideHover(); return; }
        this._hoverKey = key;
        // Anchor to the marker's own coordinates when it's a point (stable),
        // else the cursor position (polygon/line features).
        let lngLat = e.lngLat;
        const g = f.geometry;
        if (g && g.type === 'Point' && Array.isArray(g.coordinates)) lngLat = g.coordinates;
        if (!this._hoverPopup) {
            this._hoverPopup = new whg_maplibre.Popup({
                closeButton: false,
                closeOnClick: false,
                focusAfterOpen: false,
                offset: 10,
                maxWidth: '260px',
                className: HOVER_POPUP_CLASS,
            });
        }
        this._hoverPopup
            .setLngLat(lngLat)
            .setHTML(`<div class="whg-hover-label">${esc(label)}</div>`)
            .addTo(this.map);
    }

    _hideHover() {
        this._hoverKey = null;
        if (this._hoverPopup) { try { this._hoverPopup.remove(); } catch (e) {} }
    }

    /** The feature's display name from tile props (language-aware, mirroring the
     *  gazetteer label layer). Clustered aggregates show their count. */
    _featureLabel(p) {
        let lang = 'local';
        try { lang = getPreferredLanguage() || 'local'; } catch (e) {}
        const name = (!lang || lang === 'local')
            ? (p.name_local || p.name || p.name_en)
            : (p['name_' + lang] || p.name_en || p.name_local || p.name);
        if (name) return String(name);
        if (p.point_count != null) {
            const n = p.point_count_abbreviated || p.point_count;
            return `${n} places`;
        }
        return '';
    }

    _onClick(e) {
        if (!this.map || !e.features || e.features.length === 0) return;
        const placeId = e.features[0].properties && e.features[0].properties.place_id;
        if (!placeId) return;
        // Signal a MAP-driven place selection (distinct from a list-row click) so
        // the Place List can drop its row highlight — the map is now the source
        // of the shown place. Dispatched only here, not in openPopup (which the
        // list also drives).
        try {
            document.dispatchEvent(new CustomEvent('whg:map-place-click', { detail: { placeId } }));
        } catch (err) { /* CustomEvent unsupported */ }
        this.openPopup(placeId, e.lngLat);
    }

    /**
     * Open (or re-target) the place popup for ``placeId`` at ``lngLat``.
     * Shared by the on-map click handler (``_onClick``) and the Place List
     * panel, which drives it programmatically for list-row clicks. ``lngLat``
     * is a ``{lng, lat}`` object or ``[lng, lat]`` pair (a place's repr_point).
     */
    openPopup(placeId, lngLat) {
        if (!this.map || !placeId || !lngLat) return;

        _bindPopupTooltips();   // styled Bootstrap tooltips for popup content

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
                // Don't auto-focus the first focusable element on open — it left
                // a focus outline on the popup (title, then share button) when a
                // popup opened from a shared ?place= deep link.
                focusAfterOpen: false,
                maxWidth: '400px',
                className: POPUP_CLASS,
            });
            // When the popup is dismissed (× button or teardown), drop the
            // ?place= deep link so the URL reflects that nothing is focused.
            this._popup.on('close', () => {
                try { document.dispatchEvent(new CustomEvent('whg:map-popup-close')); } catch (e) {}
            });
        }
        this._popup
            .setLngLat(lngLat)
            .setHTML(LOADING_HTML)
            .addTo(this.map);

        const url = `${API_PREFIX}${encodeURIComponent(placeId)}${API_SUFFIX}`;
        // ``EntityFeatureView`` extends ``AuthenticatedAPIView``. Anonymous
        // visitors authenticate via the CSRF-token fallback in
        // ``TokenQueryOrBearerAuthentication``: a request bearing a valid
        // ``X-CSRFToken`` is accepted as ``CSRFUser`` (an AnonymousUser
        // whose ``is_authenticated`` is True). ``window.csrfToken`` is set
        // in base.js from the page's ``<meta name="csrf-token">`` tag.
        // We deliberately don't set ``Accept`` — see _bindModalDelegate
        // for why.
        const headers = {};
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
                initPopupTooltips();
            })
            .catch((err) => {
                if (err.name === 'AbortError') return;
                if (signal.aborted) return;
                this._popup.setHTML(ERROR_HTML);
            });
    }
}
