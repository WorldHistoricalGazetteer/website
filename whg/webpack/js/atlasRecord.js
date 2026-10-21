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
//  - the empty concordance (the common case) leads with Explore similar records.
//
// Readiness for tests: window.__whgRecord = {concordance, mapDrawn, mapped, baseMap}.
// `baseMap` is 'loaded' or 'fallback' (base style unreachable: plain background).
// `concordance` ends as 'ready' | 'empty' | 'not_public' | 'error'.

import '../css/atlasRecord.css';
import { initRecordEnrichment } from './enrichment.js';   // place#325 Stage 3: gloss + EDOPS

const state = (window.__whgRecord = { concordance: 'loading', mapDrawn: 0, mapped: 0, baseMap: null });
const FETCH_TIMEOUT_MS = 60000;
const BASEMAP_TIMEOUT_MS = 20000;
// whg_maplibre's own `load` handler reads style.metadata, so it must exist.
const BLANK_STYLE = {
    version: 8, sources: {}, metadata: { 'whg:backgroundcolour': '#eef2f5' },
    layers: [{ id: 'background', type: 'background', paint: { 'background-color': '#eef2f5' } }],
};

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
            // Record page is public once V4_LANDING_ENABLED is on) is not looked up
            // because WHG's index answers signed-in users only.
            if (t.reason === 'anonymous') {
                return { cls: 'unavailable', html: 'Not looked up for signed-out visitors: WHG’s index is asked only for signed-in users. Sign in to see it. That is not a finding that it is absent.' };
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

function outboundItem(e) {
    if (e.source === 'close_matches') {
        const rec = (e.target && e.target.records && e.target.records[0]) || {};
        return `<li class="conc-entry" data-tier="1" data-source="close_matches">
            ${typeBadge(e)} ${recordLabel(rec, 'postgres')}
            <div class="conc-meta">Record-to-record link attested by this record's dataset · ${provenanceBadge(e)}
            ${rec.has_geometry ? '' : ' · <span class="text-muted">not mapped: no geometry</span>'}</div>
        </li>`;
    }
    const st = targetStatus(e);
    const flag = e.placeholder ? ' <span class="badge conc-placeholder-badge">placeholder</span>' : '';
    return `<li class="conc-entry${e.placeholder ? ' conc-placeholder' : ''}" data-tier="1" data-source="place_link" data-status="${esc((e.target || {}).status || '')}">
        ${typeBadge(e)} ${identifierHTML(e)}${flag}
        <div class="conc-meta">${provenanceBadge(e)}</div>
        <div class="conc-target conc-target-${st.cls}">${st.html}</div>
    </li>`;
}

function inboundItem(e) {
    const rec = e.record || {};
    let who;
    if (e.source === 'close_matches' && e.direction === 'unknown') {
        who = `A record-to-record link between this record and ${recordLabel(rec, 'postgres')}; which side attested it is not recorded`;
    } else if (e.source === 'close_matches') {
        who = `Attested by ${esc(rec.dataset_title || 'its dataset')}, for its record ${recordLabel(rec, 'postgres', false)}`;
    } else {
        who = `Attested by ${esc(rec.dataset_title || 'its dataset')}: its record ${recordLabel(rec, 'postgres', false)} cites ${identifierHTML(e)}`;
    }
    const notMapped = !e.aggregates
        ? ' · <span class="text-muted">listed, not mapped: this link type does not assert identity</span>'
        : (rec.has_geometry ? '' : ' · <span class="text-muted">not mapped: no geometry</span>');
    return `<li class="conc-entry" data-tier="2" data-source="${esc(e.source)}">
        ${typeBadge(e)} ${who}
        <div class="conc-meta">${provenanceBadge(e)}${notMapped}</div>
    </li>`;
}

function cocitedGroups(entries) {
    const groups = new Map();
    entries.forEach((e) => {
        const k = e.cocited_identifier || e.identifier;
        if (!groups.has(k)) groups.set(k, []);
        groups.get(k).push(e);
    });
    return groups;
}

function cocitedHTML(entries, truncated) {
    if (!entries.length) return '';
    let h = `<div class="conc-tier conc-tier-3" id="concordance_tier3">
        <h3 class="h6">Co-cited: also linked to the same identifiers</h3>
        <p class="small text-muted">These records cite an identifier this record also cites. That is co-citation, not identity:
        nobody has said they are the same place as this record, so they are listed here and left off the map.</p>`;
    cocitedGroups(entries).forEach((list, ident) => {
        h += `<div class="conc-cocited-group" data-identifier="${esc(ident)}">
            <p class="mb-1">Also linked to <code>${esc(ident)}</code> by:</p><ul class="conc-list">`;
        list.forEach((e) => {
            const rec = e.record || {};
            h += `<li class="conc-entry" data-tier="3">${recordLabel(rec, 'postgres')}
                <div class="conc-meta">its link: ${typeBadge(e)} · ${provenanceBadge(e)}</div></li>`;
        });
        h += '</ul></div>';
    });
    if (truncated) h += '<p class="small text-muted">More records co-cite these identifiers than are listed here.</p>';
    return h + '</div>';
}

function exploreButton(root, primary) {
    const url = root.dataset.exploreUrl;
    const seeded = (root.dataset.exploreSeededBy || '').split(',').filter(Boolean);
    const hint = seeded.includes('name')
        ? `Searches the Atlas for records named like “${esc(root.dataset.recordTitle)}”.`
        : 'Opens the Atlas search.';
    return `<a class="btn ${primary ? 'btn-primary' : 'btn-outline-primary btn-sm'} conc-explore" href="${esc(url)}">
        <i class="fas fa-magnifying-glass me-1"></i>Explore similar records</a>
        <span class="small text-muted ms-2 conc-explore-hint">${hint}</span>`;
}

function emptyStateHTML(root, data) {
    const excluded = data.excluded_self_links
        ? `<li>${data.excluded_self_links} link${data.excluded_self_links === 1 ? '' : 's'} naming this record itself, which ${data.excluded_self_links === 1 ? 'is' : 'are'} not ${data.excluded_self_links === 1 ? 'an attestation' : 'attestations'}, ${data.excluded_self_links === 1 ? 'was' : 'were'} left out.</li>`
        : '';
    return `<div class="conc-empty" id="concordance_empty">
        <div class="conc-empty-lead">
            <h3 class="h6 mb-2">No other record is attested to be this place, yet</h3>
            <p class="mb-3">That is the common case: most records in WHG have no attested equivalents. Records that may describe
            the same place can still be found by searching for them.</p>
            <div>${exploreButton(root, true)}</div>
        </div>
        <ul class="small text-muted mt-3 mb-0">
            <li>This record cites no identifier for the same place, and no other public record cites this one.</li>
            ${excluded}
            <li>Attestations come from contributors and reviewed reconciliation. Similar records found by searching are suggestions, not attestations.</li>
        </ul>
    </div>`;
}

function summaryHTML(data, mappable) {
    const agg = data.aggregate || [];
    const n = agg.length;
    return `<p class="conc-summary">
        <strong>${n}</strong> attested equivalent${n === 1 ? '' : 's'}
        <span class="text-muted">(exactMatch or closeMatch, from this record or about it)</span>,
        <strong>${mappable}</strong> on the map.
        ${(data.outbound.length + data.inbound.length) > n ? '<span class="text-muted">Other links are listed below with what became of each.</span>' : ''}
    </p>`;
}

// ── the map: aggregated subset + this record; never tier 3 ──────────────────
function aggregateFeatures(data) {
    const feats = [];
    const rp = data.record && data.record.repr_point;
    if (Array.isArray(rp) && rp.length === 2) {
        feats.push({ type: 'Feature', geometry: { type: 'Point', coordinates: rp },
            properties: { role: 'record', title: data.record.title || '', href: '' } });
    }
    (data.aggregate || []).forEach((m) => {
        const p = m.repr_point;
        if (!(Array.isArray(p) && p.length === 2 && p.every(Number.isFinite))) return;
        feats.push({ type: 'Feature', geometry: { type: 'Point', coordinates: p },
            properties: { role: 'member', title: m.title || String(m.place_id), href: memberHref(m, m.via) || '',
                tiers: (m.tiers || []).join(',') } });
    });
    return { type: 'FeatureCollection', features: feats };
}

function drawMap(fc) {
    const whgMaplibre = window.whg_maplibre;
    if (!whgMaplibre || !whgMaplibre.Map) {
        document.getElementById('concordance_map_note').textContent = 'The map could not be loaded on this page.';
        return;
    }
    const map = new whgMaplibre.Map({ container: 'concordance_map', maxZoom: 12 });
    window.__whgRecordMap = map;

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
        const coords = fc.features.map((f) => f.geometry.coordinates);
        const lons = coords.map((c) => c[0]);
        const lats = coords.map((c) => c[1]);
        const sw = [Math.min(...lons), Math.min(...lats)];
        const ne = [Math.max(...lons), Math.max(...lats)];
        if (sw[0] === ne[0] && sw[1] === ne[1]) map.jumpTo({ center: sw, zoom: 6 });
        else map.fitBounds([sw, ne], { padding: 40, maxZoom: 10, duration: 0 });

        const popup = new whgMaplibre.Popup({ closeButton: true });
        ['concordance-members', 'concordance-record'].forEach((layer) => {
            map.on('click', layer, (ev) => {
                const f = ev.features && ev.features[0];
                if (!f) return;
                const p = f.properties || {};
                const label = p.role === 'record' ? `${esc(p.title)} <span class="text-muted">(this record)</span>`
                    : (p.href ? `<a href="${esc(p.href)}">${esc(p.title)}</a>` : esc(p.title));
                popup.setLngLat(f.geometry.coordinates).setHTML(label).addTo(map);
            });
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

    if (!data.public) {
        body.innerHTML = `<div class="conc-empty conc-not-public">
            <p class="mb-2">Concordances are shown for records in public datasets only, and this record's dataset is not public.
            Nothing has been looked up.</p>${exploreButton(root, false)}</div>`;
        state.concordance = 'not_public';
    } else if (!data.outbound.length && !data.inbound.length) {
        body.innerHTML = emptyStateHTML(root, data);
        state.concordance = 'empty';
    } else {
        const fc = aggregateFeatures(data);
        const members = fc.features.filter((f) => f.properties.role === 'member').length;
        state.mapped = members;
        let h = summaryHTML(data, members);
        if (members) {
            h += `<div class="conc-map-wrap"><div id="concordance_map" class="conc-map"></div>
                <p id="concordance_map_note" class="small text-muted mt-1 mb-0">
                <span class="conc-key conc-key-record"></span> this record
                <span class="conc-key conc-key-member ms-2"></span> attested equivalents (exactMatch, closeMatch).
                Co-cited records are not shown.</p></div>`;
        } else {
            h += '<p id="concordance_map_note" class="small text-muted">No attested equivalent has a location WHG holds, so there is nothing to map.</p>';
        }
        h += `<div class="conc-tier conc-tier-1" id="concordance_tier1">
            <h3 class="h6">Outbound: this record's own attestations</h3>`;
        h += data.outbound.length
            ? `<ul class="conc-list">${data.outbound.map(outboundItem).join('')}</ul>`
            : '<p class="small text-muted">This record attests no equivalents of its own.</p>';
        h += '</div>';
        h += `<div class="conc-tier conc-tier-2" id="concordance_tier2">
            <h3 class="h6">Inbound: other records that say they are this place</h3>`;
        h += data.inbound.length
            ? `<ul class="conc-list">${data.inbound.map(inboundItem).join('')}</ul>`
            : '<p class="small text-muted">No other public record attests that it is this place.</p>';
        if (data.truncated && data.truncated.inbound) h += '<p class="small text-muted">More records attest this than are listed here.</p>';
        h += '</div>';
        h += cocitedHTML(data.cocited || [], data.truncated && data.truncated.cocited);
        h += `<div class="conc-explore-foot mt-3">${exploreButton(root, false)}</div>`;
        body.innerHTML = h;
        state.concordance = 'ready';
        if (members) drawMap(fc);
    }
    status.hidden = true;
    body.hidden = false;
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
        + '<span>Looking up the attestations about this record…</span>';
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

function init() {
    const root = document.getElementById('concordance');
    if (root && root.dataset.endpoint) load(root);
    initRecordEnrichment();
}

if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
else init();
