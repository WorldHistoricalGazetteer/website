// whg/webpack/js/atlasRecord.js
//
// The Concordance section of the beta Record page (place#325 Stage 2), served
// at /atlas/place/<pk>/ by places/views_record.py. The page itself is rendered
// server-side; the concordance costs 0.3-3 s per record on prod, so it is
// fetched here, after the page has painted, from /atlas/place/<pk>/concordance/.
//
// Rulings rendered here (place#325):
//  - the MAP shows the aggregated subset only (tiers 1-2, exactMatch/closeMatch,
//    resolved, with a point) plus this record; tier 3 is never on the map nor
//    in its extent;
//  - every link shows its type and provenance; inbound links say who attested;
//  - tier 3 is worded as co-citation, not identity;
//  - an unresolved target says WHY (not held / not found / index unreachable),
//    and an unreachable index is never reported as absence (place#272);
//  - a placeholder identifier (`wd:None`) is flagged as one, never as "not found"
//    (ruling 2026-10-21: show flagged, never aggregate);
//  - the empty concordance (the common case) points to the header's Explore similar
//    records, the page's one such button (round 2);
//  - records that share a point are never hidden by each other: a click on a point
//    lists every record at it, and a shared point is ringed and counted (Stage 5).
//
// Layout (Stage 5): the shared two-column shell of recordShell.css. The map lives in
// the right column's square .rs-map box (#concordance_map_box, shown only when there is
// something to map), its summary and legend directly under it (#concordance_map_notes);
// the tier lists stay in the left column's #concordance. "About this group" (gloss
// cards), the Wikipedia block of the "Elsewhere" card and the on-demand Environment
// card come from enrichment.js; EDOPS is asked only when the visitor clicks.
//
// Round 2 (Stephen's review 2026-10-22): the three tier lists are folds (<details>), closed
// at first with their totals in the header, as the template's Links and Related places are;
// a fragment link into a closed fold opens it first (initFolds).
//
// Linking (place#337): for a viewer the page enables linking for (attest.js,
// <meta name="whg-link-enabled">), every Concordance entry that names another record
// by a usable id carries "Same place" / "Not the same", which open attest.js's Link
// sheet with THIS record as the subject; so does each Explore-similar result, by way of
// ?link_from= on the header's Explore link. Entries the server sourced from links
// (source 'attestation') say who linked them and when, and `distinct` ones are a
// separate "Said not to be the same" list, never in the tiers, the counts or the map.
//
// Readiness for tests: window.__whgRecord = {concordance, mapDrawn, mapped, stacks, baseMap, sections}.
// `sections` is keyed links|related|outbound|inbound|cocited: {total, truncated, toggle, open}.
// `baseMap` is 'loaded' or 'fallback' (base style unreachable: plain background);
// `stacks` is the number of points shared by more than one mapped record.
// `concordance` ends as 'ready' | 'empty' | 'not_public' | 'error'.

// enrichment.js first: it brings the shared shell css, which this page's css refines.
import { loadGlossCards, mountEnvironmentCard, recordFetcher, renderWikipediaLinks } from './enrichment.js';
import '../css/atlasRecord.css';
import { attestationProvenanceHtml, isLinkEnabled, isLinkableId, linkButtonHtml, setLinkDoneHandler } from './attest.js';

const state = (window.__whgRecord = { concordance: 'loading', mapDrawn: 0, mapped: 0, stacks: 0, baseMap: null,
    sections: {} });
const FETCH_TIMEOUT_MS = 60000;
const BASEMAP_TIMEOUT_MS = 20000;
// whg_maplibre's own `load` handler reads style.metadata, so it must exist.
const BLANK_STYLE = {
    version: 8, sources: {}, metadata: { 'whg:backgroundcolour': '#eef2f5' },
    layers: [{ id: 'background', type: 'background', paint: { 'background-color': '#eef2f5' } }],
};

// This record as a Link subject ({id, label}), once the concordance has named it.
let subject = null;

function esc(s) {
    return String(s ?? '').replace(/[&<>"']/g, (c) => (
        { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

function isHttp(u) {
    return typeof u === 'string' && /^https?:\/\//i.test(u);
}

// Where a resolved member's own page is: a WHG record (Postgres, numeric id)
// has a Record page; a gateway record opens in the Atlas.
function memberHref(rec, via) {
    if (via === 'postgres' && Number.isInteger(rec.place_id)) return `/atlas/place/${rec.place_id}/`;
    if (typeof rec.place_id === 'string' && rec.place_id.includes(':')) {
        return `/atlas/?place=${encodeURIComponent(rec.place_id)}`;
    }
    return null;
}

function recordLabel(rec, via, withDataset = true) {
    const title = esc(rec.title || rec.place_id);
    const href = memberHref(rec, via);
    const name = href ? `<a href="${esc(href)}">${title}</a>` : title;
    if (!withDataset) return name;
    const where = rec.dataset_title
        ? ` <span class="text-muted">in ${esc(rec.dataset_title)}</span>`
        : (rec.namespace ? ` <span class="text-muted">(${esc(rec.namespace)})</span>` : '');
    return name + where;
}

function typeBadge(e) {
    const t = e.link_type || 'untyped';
    const raw = e.link_type_raw && e.link_type_raw !== t ? ` title="as given: ${esc(e.link_type_raw)}"` : '';
    const cls = e.aggregates ? 'conc-type conc-type-agg' : 'conc-type';
    return `<span class="badge ${cls}"${raw}>${esc(t)}</span>`;
}

function provenanceBadge(e) {
    const p = e.provenance || {};
    const basis = p.basis || 'unknown';
    const cls = { reviewed: 'conc-prov-reviewed', imported: 'conc-prov-imported' }[basis] || 'conc-prov-other';
    return `<span class="conc-prov ${cls}" data-basis="${esc(basis)}">${esc(p.label || basis)}</span>`;
}

// One sentence on what became of an outbound link's target.
function targetStatus(e) {
    const t = e.target || {};
    const ns = e.namespace ? `<code>${esc(e.namespace)}</code>` : 'this kind of identifier';
    switch (t.status) {
        case 'placeholder':
            // places.concordance.is_placeholder_identifier: `wd:None` and the like. Not an
            // identifier, so not "WHG's index holds no record": nothing was looked up.
            return { cls: 'placeholder', html: 'Placeholder, not a real identifier: the contributor’s data has no value here. It is listed so the gap can be seen, and is not looked up, mapped or counted.' };
        case 'resolved': {
            const recs = (t.records || []).map((r) => recordLabel(r, t.via)).join('; ');
            const mapped = !e.aggregates
                ? 'listed, not mapped: this link type does not assert identity'
                : (t.has_geometry ? 'on the map' : 'not mapped: the record has no geometry');
            return { cls: 'resolved', html: `Held by WHG: ${recs} <span class="text-muted">(${mapped})</span>` };
        }
        case 'not_found':
            return { cls: 'not-found', html: 'WHG’s index was asked and holds no record with this identifier, so it can be listed but not mapped.' };
        case 'unavailable':
            // A whg: target that is private OR does not exist: resolve_targets gives
            // both the same status on purpose, so this sentence must not tell them apart.
            return { cls: 'not-held', html: 'This names a WHG record that is not available to show: it may be private, or may no longer exist.' };
        case 'not_held':
            return { cls: 'not-held', html: `WHG does not hold records for ${ns}, so this can be listed but not mapped. That says nothing about whether the place exists there.` };
        case 'gateway_unavailable':
            return { cls: 'unavailable', html: 'WHG’s index could not be reached just now, so this was not looked up. That is not a finding that it is absent: reload to try again.' };
        case 'gateway_disabled':
            // Two causes (places.concordance.resolve_targets sets t.reason): only
            // 'not_configured' may say "not configured". A signed-out visitor (the
            // Record page is public once V4_LANDING_ENABLED is on) is looked up for only
            // in openly redistributable sources (ruling 2026-10-21;
            // places.concordance.anonymous_gateway_namespaces); this was not one.
            if (t.reason === 'anonymous') {
                return { cls: 'unavailable', html: 'Not looked up for signed-out visitors: for them WHG’s index is asked only about sources recorded as openly redistributable, and this is not one. Sign in to see it. That is not a finding that it is absent.' };
            }
            if (t.reason === 'not_configured') {
                return { cls: 'unavailable', html: 'WHG’s index was not asked (not configured on this server), so this was not looked up. That is not a finding that it is absent.' };
            }
            return { cls: 'unavailable', html: 'WHG’s index was not asked, so this was not looked up. That is not a finding that it is absent.' };
        default:
            return { cls: 'unavailable', html: 'Not looked up.' };
    }
}

function identifierHTML(e) {
    const shown = e.identifier || e.identifier_raw || (e.placeholder ? '(empty)' : '');
    if (isHttp(e.identifier_raw)) {
        return `<a href="${esc(e.identifier_raw)}" target="_blank" rel="noopener noreferrer"><code>${esc(shown)}</code></a>`;
    }
    return `<code>${esc(shown)}</code>`;
}

// "Same place" / "Not the same" for one other record, or '' (linking not offered,
// no usable id, or the record itself).
function linkActions(targetId, targetLabel) {
    if (!subject || !isLinkableId(targetId) || targetId === subject.id) return '';
    const common = { target: targetId, targetLabel: targetLabel || targetId, subject: subject.id,
        subjectLabel: subject.label, cls: 'btn btn-sm btn-outline-secondary conc-link-btn', icon: false };
    const same = linkButtonHtml({ ...common, relation: 'closeMatch', text: 'Same place' });
    if (!same) return '';
    return `<span class="conc-link-actions">${same}${linkButtonHtml({ ...common, relation: 'distinct', text: 'Not the same' })}</span>`;
}

const WHG_ID = (pk) => (Number.isInteger(pk) ? `whg:${pk}` : '');

// The other record an entry names, as {id, label}: its own identifier when that is a
// record id WHG resolved (or a whg: record-to-record link), else the record behind it.
function entryTarget(e) {
    const t = e.target || {};
    const rec = e.record || (t.records && t.records.length === 1 ? t.records[0] : null) || {};
    const label = rec.title || e.identifier || '';
    if (e.source === 'close_matches') return { id: e.identifier, label };
    if (e.record) return { id: WHG_ID(rec.place_id) || e.identifier, label };
    if (t.status === 'resolved' && !e.placeholder) return { id: e.identifier, label };
    if (e.source === 'attestation') return { id: e.identifier, label };
    return { id: '', label };
}

// An entry the server took from a link someone made with the Link sheet. It has no
// `record`: `target` resolves the other side and `identifier` is that side's id.
function attestationItem(e, tier) {
    const a = e.attestation || {};
    const tgt = entryTarget(e);
    const t = e.target || {};
    const who = e.record ? recordLabel(e.record, 'postgres')
        : (t.status === 'resolved' && t.records && t.records.length ? t.records.map((r) => recordLabel(r, t.via)).join('; ')
            : identifierHTML(e));
    // The viewer's own: the sheet lists it with Withdraw.
    const change = a.mine && subject
        ? linkButtonHtml({ target: tgt.id, targetLabel: tgt.label, subject: subject.id, subjectLabel: subject.label,
            relation: a.relation_type, text: 'Change or withdraw', cls: 'btn btn-sm btn-link p-0 align-baseline conc-link-btn', icon: false })
        : '';
    return `<li class="conc-entry conc-attestation" data-tier="${tier}" data-source="attestation"
            data-attestation-status="${esc(a.status || '')}" data-relation="${esc(a.relation_type || '')}">
        ${typeBadge(e)} ${who}
        <div class="conc-meta">${attestationProvenanceHtml(a)}${change ? ` · ${change}` : ''}</div>
    </li>`;
}

function outboundItem(e) {
    if (e.source === 'attestation') return attestationItem(e, 1);
    if (e.source === 'close_matches') {
        const rec = (e.target && e.target.records && e.target.records[0]) || {};
        return `<li class="conc-entry" data-tier="1" data-source="close_matches">
            ${typeBadge(e)} ${recordLabel(rec, 'postgres')}
            <div class="conc-meta">Record-to-record link made by this record's dataset · ${provenanceBadge(e)}
            ${rec.has_geometry ? '' : ' · <span class="text-muted">not mapped: no geometry</span>'}</div>
            ${linkActions(e.identifier, rec.title)}
        </li>`;
    }
    const st = targetStatus(e);
    const tgt = entryTarget(e);
    const flag = e.placeholder ? ' <span class="badge conc-placeholder-badge">placeholder</span>' : '';
    return `<li class="conc-entry${e.placeholder ? ' conc-placeholder' : ''}" data-tier="1" data-source="place_link" data-status="${esc((e.target || {}).status || '')}">
        ${typeBadge(e)} ${identifierHTML(e)}${flag}
        <div class="conc-meta">${provenanceBadge(e)}</div>
        <div class="conc-target conc-target-${st.cls}">${st.html}</div>
        ${linkActions(tgt.id, tgt.label)}
    </li>`;
}

function inboundItem(e) {
    if (e.source === 'attestation') return attestationItem(e, 2);
    const rec = e.record || {};
    const tgt = entryTarget(e);
    let who;
    if (e.source === 'close_matches' && e.direction === 'unknown') {
        who = `A record-to-record link between this record and ${recordLabel(rec, 'postgres')}; which side made it is not recorded`;
    } else if (e.source === 'close_matches') {
        who = `Linked by ${esc(rec.dataset_title || 'its dataset')}, for its record ${recordLabel(rec, 'postgres', false)}`;
    } else {
        who = `Linked by ${esc(rec.dataset_title || 'its dataset')}: its record ${recordLabel(rec, 'postgres', false)} cites ${identifierHTML(e)}`;
    }
    const notMapped = !e.aggregates
        ? ' · <span class="text-muted">listed, not mapped: this link type does not assert identity</span>'
        : (rec.has_geometry ? '' : ' · <span class="text-muted">not mapped: no geometry</span>');
    return `<li class="conc-entry" data-tier="2" data-source="${esc(e.source)}">
        ${typeBadge(e)} ${who}
        <div class="conc-meta">${provenanceBadge(e)}${notMapped}</div>
        ${linkActions(tgt.id, tgt.label)}
    </li>`;
}

// Tier 3 grouped BY RECORD (dev walkthrough 2026-10-21: grouped by identifier, the same
// four records repeated under eight identifiers). Rendering only: the entries are the
// endpoint's, uncounted and unaggregated, in its order (place id, then link id).
function cocitedByRecord(entries) {
    const groups = new Map();
    entries.forEach((e) => {
        const rec = e.record || {};
        const k = String(rec.place_id);
        if (!groups.has(k)) groups.set(k, { record: rec, links: [] });
        groups.get(k).links.push(e);
    });
    return groups;
}

// A fold (round 2, Stephen's review 2026-10-22): a list closed at first, its total in the
// header, as <details>/<summary> like the server-rendered Links and Related places. A list
// with nothing in it is no fold: the heading says (0) and `empty` says why. `truncated`
// (the endpoint's cap was reached) shows as "n+" in the header, and the body says so too.
function foldHTML({ id, cls, key, title, total, unit = '', truncated = false, body, empty }) {
    const noun = unit ? ` ${unit}${total === 1 ? '' : 's'}` : '';
    const count = truncated
        ? `(${total}+${noun}<span class="visually-hidden">: the list is capped</span>)`
        : `(${total}${noun})`;
    const attrs = `id="${id}" class="conc-tier ${cls}${total ? ' rec-fold' : ''}" data-section="${key}" data-total="${total}"`
        + ` data-truncated="${truncated}" data-collapsed="${total ? 'true' : 'none'}"`;
    const head = `<h3 class="h6 rec-fold-title">${esc(title)} <span class="rec-fold-count">${count}</span></h3>`;
    if (!total) return `<div ${attrs}>${head}<p class="small text-muted mb-0">${empty}</p></div>`;
    return `<details ${attrs}><summary class="rec-fold-head">`
        + `<i class="fa-solid fa-chevron-right rec-fold-chevron" aria-hidden="true"></i>${head}</summary>`
        + `<div class="rec-fold-body">${body}</div></details>`;
}

function cocitedHTML(entries, truncated) {
    const groups = cocitedByRecord(entries);
    let h = `<p class="small text-muted">These records cite an identifier this record also cites. That is co-citation, not identity:
        nobody has said they are the same place as this record, so they are listed here and left off the map.</p>
        <ul class="conc-list">`;
    groups.forEach(({ record, links }, pid) => {
        const shared = [...new Set(links.map((e) => e.cocited_identifier || e.identifier))];
        const items = links.map((e) => `<li data-identifier="${esc(e.cocited_identifier || e.identifier)}">${typeBadge(e)}
            <code>${esc(e.cocited_identifier || e.identifier)}</code> · ${provenanceBadge(e)}</li>`).join('');
        h += `<li class="conc-entry conc-cocited-record" data-tier="3" data-place-id="${esc(pid)}"
                data-identifiers="${esc(shared.join(' '))}">${recordLabel(record, 'postgres')}
            <div class="conc-meta">shares ${shared.length === 1 ? 'one identifier' : `${shared.length} identifiers`} with this record; its link${links.length === 1 ? '' : 's'}:</div>
            <ul class="conc-cocited-links">${items}</ul>${linkActions(WHG_ID(record.place_id), record.title)}</li>`;
    });
    h += '</ul>';
    if (truncated) h += '<p class="small text-muted">More records co-cite these identifiers than are listed here.</p>';
    // Counted in RECORDS (the grouping above), never in shared identifiers.
    return foldHTML({ id: 'concordance_tier3', cls: 'conc-tier-3', key: 'cocited',
        title: 'Co-cited: also linked to the same identifiers', total: groups.size, unit: 'record', truncated,
        body: h, empty: 'No other public record, beyond those listed above, cites an identifier this record cites.' });
}

// "Said not to be the same": links answering `distinct`, kept apart from the tiers (and
// so from the counts, the aggregate and the map). Rendered only when there are some.
function distinctHTML(entries) {
    if (!entries || !entries.length) return '';
    const items = entries.map((e) => {
        const t = e.target || {};
        const rec = e.record || (t.records && t.records[0]);
        const name = rec ? recordLabel(rec, e.record ? 'postgres' : t.via) : identifierHTML(e);
        return `<li class="conc-entry conc-distinct-entry" data-source="${esc(e.source || '')}"
                data-attestation-status="${esc((e.attestation || {}).status || '')}">${name}
            <div class="conc-meta">${attestationProvenanceHtml(e.attestation)}</div></li>`;
    }).join('');
    return `<div id="concordance_distinct" class="conc-tier conc-distinct" data-total="${entries.length}">
        <h3 class="h6">Said not to be the same (${entries.length})</h3>
        <p class="small text-muted mb-1">Someone has linked these records as a different place from this one.</p>
        <ul class="conc-list">${items}</ul></div>`;
}

// "Explore similar records" is the header's (#explore_similar_header), once on the page
// (round 2: it was also repeated at the foot of the Concordance).
function emptyStateHTML(root, data) {
    const excluded = data.excluded_self_links
        ? `<li>${data.excluded_self_links} link${data.excluded_self_links === 1 ? '' : 's'} naming this record itself, which ${data.excluded_self_links === 1 ? 'is' : 'are'} not ${data.excluded_self_links === 1 ? 'a Link' : 'Links'}, ${data.excluded_self_links === 1 ? 'was' : 'were'} left out.</li>`
        : '';
    return `<div class="conc-empty" id="concordance_empty">
        <div class="conc-empty-lead">
            <h3 class="h6 mb-2">No other record is linked as the same place yet</h3>
            <p class="mb-0">That is the common case: most records in WHG have no linked equivalents. Records that may describe
            the same place can still be found by searching for them, with <a href="#explore_similar_header"
            class="conc-explore-ref">Explore similar records</a> at the top of this page.</p>
        </div>
        <ul class="small text-muted mt-3 mb-0">
            <li>This record cites no identifier for the same place, and no other public record cites this one.</li>
            ${excluded}
            <li>Links come from contributors and from matches reviewed in WHG v3. Similar records found by searching are suggestions, not Links.</li>
        </ul>
    </div>`;
}

// The literal counts (fix a/d of place#325 Stage 5): `mappable` counts attested
// equivalents with a point and EXCLUDES this record's own point, which the sentence
// then names separately, and only when the record has one.
function summaryHTML(data, mappable, recordOnMap) {
    const agg = data.aggregate || [];
    const n = agg.length;
    const own = mappable
        ? (recordOnMap ? ', plus this record.' : '. This record itself has no location, so it is not on the map.')
        : '.';
    return `<p class="conc-summary mb-1" id="concordance_summary">
        <strong>${n}</strong> linked equivalent${n === 1 ? '' : 's'}
        (exactMatch or closeMatch, from this record or about it),
        <strong>${mappable}</strong> on the map${own}
        ${(data.outbound.length + data.inbound.length) > n ? 'Other links are listed with what became of each.' : ''}
    </p>`;
}

// ── the map: aggregated subset + this record; never tier 3 ──────────────────
// Records at the SAME point (exact coordinates: e.g. a TGN and a GeoNames record both
// at 22°E 39°N) are drawn on top of each other, and MapLibre hit-tests the top one only,
// so the page keeps its own groups by coordinate: `groups` maps a point's key to every
// record at it, and the popup lists the group, never just the clicked feature.
function pointKey(p) {
    return `${p[0]},${p[1]}`;
}

function aggregateFeatures(data) {
    const feats = [];
    const groups = new Map();
    const add = (coords, props, entry) => {
        const key = pointKey(coords);
        if (!groups.has(key)) groups.set(key, []);
        groups.get(key).push({ ...entry, coordinates: coords });
        feats.push({ type: 'Feature', geometry: { type: 'Point', coordinates: coords }, properties: { ...props, key } });
    };
    const rp = data.record && data.record.repr_point;
    if (Array.isArray(rp) && rp.length === 2 && rp.every(Number.isFinite)) {
        const title = data.record.title || '';
        add(rp, { role: 'record', title, href: '' }, { role: 'record', title, href: '', where: '' });
    }
    (data.aggregate || []).forEach((m) => {
        const p = m.repr_point;
        if (!(Array.isArray(p) && p.length === 2 && p.every(Number.isFinite))) return;
        const title = m.title || String(m.place_id);
        const href = memberHref(m, m.via) || '';
        add(p, { role: 'member', title, href, tiers: (m.tiers || []).join(',') },
            { role: 'member', title, href, where: m.dataset_title || m.namespace || '' });
    });
    groups.forEach((list) => list.sort((a, b) => (a.role === 'record' ? -1 : 0) - (b.role === 'record' ? -1 : 0)));
    feats.forEach((f) => { f.properties.count = groups.get(f.properties.key).length; });
    // One point per shared coordinate, for the ring under the stack.
    const stacks = [];
    groups.forEach((list, key) => {
        if (list.length > 1) {
            stacks.push({ type: 'Feature', geometry: { type: 'Point', coordinates: list[0].coordinates },
                properties: { key, count: list.length } });
        }
    });
    return { fc: { type: 'FeatureCollection', features: feats },
        stacks: { type: 'FeatureCollection', features: stacks }, groups };
}

function popupItem(r) {
    if (r.role === 'record') return `<li data-role="record">${esc(r.title)} <span class="text-muted">(this record)</span></li>`;
    const name = r.href ? `<a href="${esc(r.href)}">${esc(r.title)}</a>` : esc(r.title);
    const where = r.where ? ` <span class="text-muted">${esc(r.where)}</span>` : '';
    return `<li data-role="member">${name}${where}</li>`;
}

function popupHTML(records) {
    const head = records.length > 1
        ? `<p class="conc-popup-head mb-1"><strong>${records.length} records at this point</strong></p>` : '';
    return `<div class="conc-popup" data-count="${records.length}">${head}
        <ul class="conc-popup-list">${records.map(popupItem).join('')}</ul></div>`;
}

const POINT_LAYERS = ['concordance-stacks', 'concordance-members', 'concordance-record'];
const HIT_PX = 6;

function drawMap({ fc, stacks, groups }) {
    const whgMaplibre = window.whg_maplibre;
    if (!whgMaplibre || !whgMaplibre.Map) {
        document.getElementById('concordance_map_note').textContent = 'The map could not be loaded on this page.';
        return;
    }
    const container = document.getElementById('concordance_map');
    const map = new whgMaplibre.Map({ container: 'concordance_map', maxZoom: 12 });
    window.__whgRecordMap = map;

    // The extent of the points, fitted to the canvas as it is NOW.
    let dataOn = false;
    const fit = () => {
        const coords = fc.features.map((f) => f.geometry.coordinates);
        const lons = coords.map((c) => c[0]);
        const lats = coords.map((c) => c[1]);
        const sw = [Math.min(...lons), Math.min(...lats)];
        const ne = [Math.max(...lons), Math.max(...lats)];
        if (sw[0] === ne[0] && sw[1] === ne[1]) map.jumpTo({ center: sw, zoom: 6 });
        else map.fitBounds([sw, ne], { padding: 40, maxZoom: 10, duration: 0 });
    };
    // Dev walkthrough 2026-10-21: the canvas stayed 400x300 (MapLibre's fallback for a
    // zero-size container) in a ~1294 px box, because the map was created while the
    // section was still hidden. render() now unhides it first, and this keeps the
    // canvas tracking its container whenever its size changes (layout settling,
    // window resize, a font or the enrichment section loading above it), refitting
    // the points to the new size.
    let lastSize = '';
    const track = () => {
        const size = `${container.clientWidth}x${container.clientHeight}`;
        if (size === lastSize || !container.clientWidth) return;
        lastSize = size;
        map.resize();
        if (dataOn) fit();
    };
    if (window.ResizeObserver) new ResizeObserver(track).observe(container);
    else window.addEventListener('resize', track);
    requestAnimationFrame(track);

    // If the base style itself cannot be fetched, MapLibre never fires `load`
    // and the points would never be drawn. Fall back to a plain background so
    // the concordance is still mapped, and say the base map is missing.
    let loaded = false;
    let fellBack = false;
    const fallBack = (why) => {
        if (loaded || fellBack) return;
        fellBack = true;
        state.baseMap = 'fallback';
        console.warn(`Record page: base map unavailable (${why}); drawing on a plain background.`);
        const note = document.getElementById('concordance_map_note');
        if (note) note.insertAdjacentHTML('beforeend', ' <span class="conc-basemap-missing">The base map could not be loaded, so the points are shown on a plain background.</span>');
        map.setStyle(BLANK_STYLE);
    };
    map.on('error', (e) => {
        const err = (e && e.error) || {};
        const what = `${err.url || ''} ${err.message || ''}`;
        if (!e.sourceId && /style\.json|\/styles\//.test(what)) fallBack('style unreachable');
    });
    const fallbackTimer = setTimeout(() => fallBack('timeout'), BASEMAP_TIMEOUT_MS);
    map.on('load', () => {
        loaded = true;
        clearTimeout(fallbackTimer);
        if (!fellBack) state.baseMap = 'loaded';
        map.addSource('concordance', { type: 'geojson', data: fc });
        map.addSource('concordance-stacks', { type: 'geojson', data: stacks });
        // A shared point: a dark ring round the stack, under the dots, and a count badge.
        map.addLayer({
            id: 'concordance-stacks', type: 'circle', source: 'concordance-stacks',
            paint: { 'circle-radius': 13, 'circle-color': 'rgba(255,255,255,0.65)',
                'circle-stroke-color': '#1a202c', 'circle-stroke-width': 2 },
        });
        map.addLayer({
            id: 'concordance-members', type: 'circle', source: 'concordance',
            filter: ['==', ['get', 'role'], 'member'],
            paint: { 'circle-radius': 7, 'circle-color': '#2b6cb0', 'circle-stroke-color': '#fff', 'circle-stroke-width': 2 },
        });
        map.addLayer({
            id: 'concordance-record', type: 'circle', source: 'concordance',
            filter: ['==', ['get', 'role'], 'record'],
            paint: { 'circle-radius': 8, 'circle-color': '#c53030', 'circle-stroke-color': '#fff', 'circle-stroke-width': 2 },
        });
        map.resize();
        lastSize = `${container.clientWidth}x${container.clientHeight}`;
        dataOn = true;
        fit();

        // The badges are DOM markers, not a symbol layer: the plain-background fallback
        // style has no glyphs, and the count must show on it too.
        if (whgMaplibre.Marker) {
            stacks.features.forEach((f) => {
                const el = document.createElement('span');
                el.className = 'conc-stack-badge';
                el.dataset.key = f.properties.key;
                el.textContent = String(f.properties.count);
                el.setAttribute('aria-hidden', 'true');
                new whgMaplibre.Marker({ element: el, anchor: 'bottom-left', offset: [6, -6] })
                    .setLngLat(f.geometry.coordinates).addTo(map);
            });
        }

        // One click handler for all three layers: every point drawn within a few pixels of
        // the click, then every record of each such point's group (our own array, not the
        // features MapLibre happened to hit).
        const popup = new whgMaplibre.Popup({ closeButton: true });
        window.__whgRecordPopup = popup;
        map.on('click', (ev) => {
            const { x, y } = ev.point;
            const hits = map.queryRenderedFeatures([[x - HIT_PX, y - HIT_PX], [x + HIT_PX, y + HIT_PX]],
                { layers: POINT_LAYERS });
            const keys = [...new Set(hits.map((f) => f.properties && f.properties.key).filter((k) => groups.has(k)))];
            if (!keys.length) return;
            const records = keys.flatMap((k) => groups.get(k));
            popup.setLngLat(groups.get(keys[0])[0].coordinates).setHTML(popupHTML(records)).addTo(map);
        });
        POINT_LAYERS.forEach((layer) => {
            map.on('mouseenter', layer, () => { map.getCanvas().style.cursor = 'pointer'; });
            map.on('mouseleave', layer, () => { map.getCanvas().style.cursor = ''; });
        });
        map.once('idle', () => { state.mapDrawn += 1; });
    });
}

// ── render ──────────────────────────────────────────────────────────────────
function render(root, data) {
    const body = document.getElementById('concordance_body');
    const status = document.getElementById('concordance_status');
    const rec = data.record || {};
    subject = isLinkEnabled() && WHG_ID(rec.place_id) ? { id: WHG_ID(rec.place_id), label: rec.title || WHG_ID(rec.place_id) } : null;
    exploreLinksFrom(subject);

    if (!data.public) {
        body.innerHTML = `<div class="conc-empty conc-not-public">
            <p class="mb-0">Concordances are shown for records in public datasets only, and this record's dataset is not public.
            Nothing has been looked up.</p></div>`;
        state.concordance = 'not_public';
    } else if (!data.outbound.length && !data.inbound.length) {
        body.innerHTML = emptyStateHTML(root, data) + distinctHTML(data.distinct);
        state.concordance = 'empty';
    } else {
        const mapData = aggregateFeatures(data);
        const { fc } = mapData;
        const members = fc.features.filter((f) => f.properties.role === 'member').length;
        const recordOnMap = fc.features.some((f) => f.properties.role === 'record');
        state.mapped = members;
        state.stacks = mapData.stacks.features.length;
        // The summary and legend go under the map, in the right column (Stage 5).
        let notes = summaryHTML(data, members, recordOnMap);
        if (members) {
            const ring = state.stacks
                ? `<br><span class="conc-key conc-key-stack"></span> more than one record at the same point
                   (the number says how many; click it to list them).` : '';
            notes += `<p id="concordance_map_note" class="mb-0">
                <span class="conc-key conc-key-record"></span> this record
                <span class="conc-key conc-key-member ms-2"></span> linked equivalents (exactMatch, closeMatch).
                Co-cited records are not shown.${ring}</p>`;
        } else {
            notes += '<p id="concordance_map_note" class="mb-0">No linked equivalent has a location WHG holds, so there is nothing to map.</p>';
        }
        const notesEl = document.getElementById('concordance_map_notes');
        const box = document.getElementById('concordance_map_box');
        if (notesEl) {
            notesEl.innerHTML = notes;
            notesEl.hidden = false;
        }
        // Totals: every listed outbound row (placeholders and not-held ones included, as
        // they are listed), every inbound record entry, co-cited RECORDS.
        const inTrunc = !!(data.truncated && data.truncated.inbound);
        let h = foldHTML({ id: 'concordance_tier1', cls: 'conc-tier-1', key: 'outbound',
            title: "Outbound: Links made from this record", total: data.outbound.length,
            body: `<ul class="conc-list">${data.outbound.map(outboundItem).join('')}</ul>`,
            empty: 'This record has no Links of its own to other records.' });
        h += foldHTML({ id: 'concordance_tier2', cls: 'conc-tier-2', key: 'inbound',
            title: 'Inbound: other records that say they are this place', total: data.inbound.length, truncated: inTrunc,
            body: `<ul class="conc-list">${data.inbound.map(inboundItem).join('')}</ul>`
                + (inTrunc ? '<p class="small text-muted">More records link to this one than are listed here.</p>' : ''),
            empty: 'No other public record is linked as the same place as this one.' });
        h += cocitedHTML(data.cocited || [], !!(data.truncated && data.truncated.cocited));
        h += distinctHTML(data.distinct);
        body.innerHTML = h;
        state.concordance = 'ready';
        // Shown BEFORE the map is created: a map made in a hidden (zero-width)
        // container draws a 400x300 canvas (drawMap also tracks later resizes).
        status.hidden = true;
        body.hidden = false;
        if (members && box) {
            box.innerHTML = '<div id="concordance_map" class="rs-map-canvas"></div>';
            box.hidden = false;
            drawMap(mapData);
        }
    }
    status.hidden = true;
    body.hidden = false;
    syncSections();
    followHash(true);           // a #concordance_tier* link that arrived before the lists did
}

// ── folds: test flags, and fragment links into a closed one ─────────────────
// window.__whgRecord.sections[key] = {total, truncated, toggle, open}; `toggle` false is
// the zero case (a plain block); `open` null there. data-collapsed mirrors it on the block.
function syncSections() {
    const out = {};
    document.querySelectorAll('#atlas_record [data-section]').forEach((el) => {
        const toggle = el.tagName === 'DETAILS';
        if (toggle) el.dataset.collapsed = String(!el.open);
        out[el.dataset.section] = { total: Number(el.dataset.total), truncated: el.dataset.truncated === 'true',
            toggle, open: toggle ? el.open : null };
    });
    state.sections = out;
}

// Open every closed fold round `el` (or `el` itself), so a link to it lands on something seen.
function openFoldsAround(el) {
    let opened = false;
    for (let d = el && el.closest('details'); d; d = d.parentElement && d.parentElement.closest('details')) {
        if (!d.open) { d.open = true; opened = true; }
    }
    if (opened) syncSections();
    return opened;
}

function hashTarget(hash) {
    let id = (hash || '').replace(/^#/, '');
    try { id = decodeURIComponent(id); } catch (e) { /* keep it raw */ }
    return id ? document.getElementById(id) : null;
}

function followHash(scroll) {
    const t = hashTarget(location.hash);
    if (t && openFoldsAround(t) && scroll) t.scrollIntoView();
}

// The header's "Explore similar records" carries this record into the Atlas as the
// remembered Link subject (attest.js reads ?link_from=), so each result there links with it.
function exploreLinksFrom(subj) {
    const a = document.getElementById('explore_similar_header');
    if (!a) return;
    if (!a.dataset.baseHref) a.dataset.baseHref = a.getAttribute('href');
    const url = new URL(a.dataset.baseHref, location.origin);
    if (subj) {
        url.searchParams.set('link_from', subj.id);
        url.searchParams.set('link_from_label', subj.label);
    }
    a.setAttribute('href', url.pathname + url.search + url.hash);
}

function initFolds() {
    syncSections();
    // `toggle` does not bubble: listen in the capture phase for every fold, now and later.
    document.addEventListener('toggle', (e) => {
        if (e.target && e.target.matches && e.target.matches('#atlas_record details[data-section]')) syncSections();
    }, true);
    // An in-page link: open the fold first, then let the browser follow it to a visible target.
    document.addEventListener('click', (e) => {
        const a = e.target.closest && e.target.closest('a[href^="#"]');
        if (a) openFoldsAround(hashTarget(a.getAttribute('href')));
    });
    window.addEventListener('hashchange', () => followHash(true));
    followHash(true);
}

function renderError(root, message) {
    const status = document.getElementById('concordance_status');
    status.innerHTML = `<div class="alert alert-warning mb-0 conc-error">
        <p class="mb-2">${esc(message)} This is not a finding that the concordance is empty.</p>
        <button type="button" class="btn btn-sm btn-outline-secondary" id="concordance_retry">Try again</button></div>`;
    state.concordance = 'error';
    document.getElementById('concordance_retry').addEventListener('click', () => load(root));
}

function load(root) {
    const status = document.getElementById('concordance_status');
    status.hidden = false;
    status.innerHTML = '<div class="spinner-border spinner-border-sm text-secondary me-2" aria-hidden="true"></div>'
        + '<span>Looking up the Links about this record…</span>';
    state.concordance = 'loading';
    const ctl = new AbortController();
    const timer = setTimeout(() => ctl.abort(), FETCH_TIMEOUT_MS);
    fetch(root.dataset.endpoint, { headers: { Accept: 'application/json' }, credentials: 'same-origin', signal: ctl.signal })
        .then((r) => r.json().catch(() => ({})).then((data) => ({ ok: r.ok, status: r.status, data })))
        .then(({ ok, status: code, data }) => {
            clearTimeout(timer);
            if (!ok) {
                renderError(root, code === 403 ? 'The concordance is a beta feature and this account cannot see it.'
                    : `The concordance could not be loaded (HTTP ${code}).`);
                return;
            }
            render(root, data);
        })
        .catch((err) => {
            clearTimeout(timer);
            renderError(root, err && err.name === 'AbortError'
                ? 'The concordance took too long to load.' : 'The concordance could not be loaded.');
        });
}

// ── "About this group", Wikipedia, Environment (place#325 Stage 3, reshaped in Stage 5) ──
// The server renders #record_gloss and #record_environment only for a beta user, or for
// anyone with the v4 release switch on (then marked data-enrichment="public" and served
// by the public, throttled endpoints): their presence is the gate. The gloss loads with
// the page; the environment endpoint (EDOPS) only when the card's button is clicked.
function initEnrichment() {
    const gloss = document.getElementById('record_gloss');
    const env = document.getElementById('record_environment');
    if (gloss && gloss.dataset.endpoint) {
        loadGlossCards(gloss.querySelector('.enr-body'), recordFetcher(gloss.dataset.endpoint), {
            kind: 'concordance',
            onData: (json) => {
                // Wikipedia links held in the group's Wikidata records, following their
                // visibility (views_enrichment.atlas_record_gloss). Only where known.
                const card = document.getElementById('record_elsewhere');
                const slot = document.getElementById('record_wikipedia');
                if (!card || !slot) return;
                renderWikipediaLinks(slot, json.wikipedia, { total: json.wikipedia_total, headingTag: 'h3' });
                if (slot.childElementCount) card.hidden = false;
            },
        });
    }
    if (env && env.dataset.endpoint) {
        mountEnvironmentCard(env, { fetchEnvironment: recordFetcher(env.dataset.endpoint), title: 'Environment',
            invitation: 'River basin, climate and land-use figures for where this group of records lies, from EDOPS, '
                + 'a research service outside WHG. Nothing is looked up or sent until you ask.',
            headingTag: 'h2' });
    }
}

function init() {
    initFolds();
    const root = document.getElementById('concordance');
    if (root && root.dataset.endpoint) load(root);
    // A link made or withdrawn here changes what the Concordance lists: reload it once
    // the sheet closes (not per answer, which would rebuild the map under an open sheet).
    if (root && root.dataset.endpoint) {
        let changed = false;
        setLinkDoneHandler(() => { changed = true; });
        document.addEventListener('hidden.bs.modal', (e) => {
            if (changed && e.target && e.target.id === 'whg_link_sheet') { changed = false; load(root); }
        });
    }
    initEnrichment();
}

if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
else init();
