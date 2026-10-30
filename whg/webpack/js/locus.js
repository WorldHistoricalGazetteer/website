// locus.js — the locus page: unminted, /atlas/locus/?r=<token>, and minted,
// /entity/locus:<PID>/ (place#325 Stages 2 and 4, place#172).
//
// The server has already decoded and validated the recipe (a bad token renders
// an error page and this script finds no recipe to run). This script:
//   1. POSTs the token to /atlas/locus/resolve/, which re-runs the recipe's
//      retrieval through the CRC gateway exactly as the Atlas does;
//   2. clusters the result in the browser with the recipe's LITERAL clustering
//      parameters (locus_resolve.js → clustering.js, the one clusterer there is);
//   3. picks the cluster the recipe meant (the fixed plurality rule) and shows
//      its current members, each linking to its record page, on a map.
// Both pages run the same recipe the same way; only the identity block differs.
//
// Minting (Stage 4, only when the server rendered data-mint-url, i.e.
// the v4 release switch and a signed-in beta user): the Mint button opens the
// modal the server rendered, which says what is being minted (the RECIPE,
// re-run on every visit, never the member list shown), lists existing loci
// sharing members, and needs an explicit confirmation before it POSTs
// {r: token, confirm: true, minter_public} to /atlas/locus/mint/ (minter_public
// is the modal's opt-in name box, default off). On success the browser goes to
// the canonical /entity/locus:<PID>/ URI. A withdrawn or redacted locus is a
// tombstone: the server renders no recipe for it, so there is nothing to re-run.
//
// Layout (place#325 Stage 5): the Record page's shared shell (recordShell.css).
// The recipe sits in a modal the template renders (#locus_recipe_modal, opened by
// Bootstrap's data API); this script still fills its stoplist row and marks its
// members. For a beta viewer the server also renders "About this cluster", an
// "Elsewhere" card and an Environment slot: once a cluster is selected, the gloss
// is fetched (themed cards, enrichment.js), the Wikipedia block is shown only when
// the gloss payload carries links, and the Environment card is mounted, fetching
// EDOPS only when someone clicks (its own button, or the cluster card's).
//
// Linking (place#337): where the page enables it, each member row carries attest.js's
// Link pick: Link on one member, then on another, opens the Link sheet for the pair.

import bbox from '@turf/bbox';
import { fetchJSON, failureHtml, atlasNotice } from './atlasNotice.js';
import { resolveRecipe, canonicalMemberId } from './locus_resolve.js';
import '../css/locus.css';
import { loadGlossCards, locusFetchers, mountEnvironmentCard, renderWikipediaLinks } from './enrichment.js';
import { linkButtonHtml } from './attest.js';

const RESOLVE_TIMEOUT_MS = 35000;

function isDebugEnabled() {
    try { if (localStorage.getItem('whg.debug') === '1') return true; } catch (e) { /* */ }
    try { return /[?&]debug\b/.test(window.location.search); } catch (e) { /* */ }
    return false;
}

function escapeHtml(s) {
    return String(s == null ? '' : s)
        .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function formatYear(y) {
    if (y == null) return '…';
    return y < 0 ? `${-y} BCE` : String(y);
}

function formatRange(tr) {
    if (!Array.isArray(tr) || tr.length !== 2) return '';
    const [s, e] = tr;
    if (s == null && e == null) return '';
    if (s === e) return formatYear(s);
    return `${formatYear(s)}–${formatYear(e)}`;
}

/** Expose the outcome for harnesses, behind the same debug gate the Atlas uses. */
function publish(state) {
    if (!isDebugEnabled()) return;
    try { window.__whgLocus = { t: Math.round(performance.now()), ...state }; } catch (e) { /* */ }
}

// ── map ────────────────────────────────────────────────────────────────────
let whgMap = null;
let mapReady = false;
let pendingFC = null;
let pendingHighlight = [];
let fitBox = null;          // the bbox last fitted, refitted when the map box changes size

function initMap() {
    const container = document.getElementById('locus_map');
    if (!container || !window.whg_maplibre) return;
    try {
        whgMap = new window.whg_maplibre.Map({ container: 'locus_map', maxZoom: 12 });
    } catch (e) {
        console.warn('Locus: map could not be created', e);
        return;
    }
    whgMap.on('load', () => {
        mapReady = true;
        try {
            whgMap.newSource('locus').newLayerset('locus', null, 'plain');
        } catch (e) { console.warn('Locus: layerset failed', e); return; }
        if (pendingFC) showOnMap(pendingFC, pendingHighlight);
    });
    // The square .rs-map box takes its size from CSS alone (recordShell.css): it
    // changes at the lg column switch, on window resize and as the column settles.
    // Keep the canvas tracking it and refit the cluster to the new size, as the
    // Record page's Concordance map does.
    let lastSize = '';
    const track = () => {
        const size = `${container.clientWidth}x${container.clientHeight}`;
        if (size === lastSize || !container.clientWidth) return;
        lastSize = size;
        try {
            whgMap.resize();
            if (mapReady && fitBox) whgMap.fitViewport(fitBox, 8);
        } catch (e) { /* */ }
    };
    if (window.ResizeObserver) new ResizeObserver(track).observe(container);
    else window.addEventListener('resize', track);
}

function showOnMap(fc, highlightIds) {
    pendingFC = fc;
    pendingHighlight = highlightIds;
    if (!mapReady || !whgMap) return;
    try {
        whgMap.getSource('locus').setData(fc);
        highlightIds.forEach((id) => {
            try { whgMap.setFeatureState({ source: 'locus', id }, { highlight: true }); } catch (e) { /* */ }
        });
        const feats = highlightIds.length ? highlightIds.map((i) => fc.features[i]).filter(Boolean) : fc.features;
        if (feats.length) {
            const b = bbox({ type: 'FeatureCollection', features: feats });
            if (b && b.every(Number.isFinite)) {
                fitBox = b;
                whgMap.fitViewport(b, 8);
            }
        }
    } catch (e) { console.warn('Locus: could not plot', e); }
}

function hitsToFC(hits) {
    const features = [];
    const indexByPid = new Map();
    hits.forEach((h) => {
        let geometry = null;
        if (Array.isArray(h.geometries) && h.geometries.length) {
            geometry = h.geometries.length === 1 ? h.geometries[0] : { type: 'GeometryCollection', geometries: h.geometries };
        } else if (Array.isArray(h.repr_point) && h.repr_point.length >= 2) {
            geometry = { type: 'Point', coordinates: [h.repr_point[0], h.repr_point[1]] };
        }
        if (!geometry) return;
        indexByPid.set(canonicalMemberId(h.place_id), features.length);
        features.push({ type: 'Feature', id: features.length, geometry,
            properties: { pid: h.place_id, title: h.title, namespace: h.namespace } });
    });
    return { fc: { type: 'FeatureCollection', features }, indexByPid };
}

// ── rendering ──────────────────────────────────────────────────────────────
function recordUrl(pid, recordUrls) {
    return (recordUrls && recordUrls[pid]) || `/entity/place:${encodeURIComponent(canonicalMemberId(pid))}/`;
}

function markRecipeMembers(surviving) {
    const alive = new Set(surviving);
    document.querySelectorAll('#locus_recipe_members .locus-member-id').forEach((el) => {
        const gone = !alive.has(canonicalMemberId(el.dataset.member));
        el.classList.toggle('locus-member-gone', gone);
        el.title = gone ? 'Not in the current retrieval' : 'In the current retrieval';
    });
}

function renderResult(recipe, data, out) {
    const el = document.getElementById('locus_result');
    const { selection, clusters, hits } = out;
    const recipeSet = new Set(recipe.members.map(canonicalMemberId));
    const nRecipe = recipe.members.length;
    const withheld = data.withheld && data.withheld.count
        ? `<p class="locus-withheld"><i class="fas fa-lock me-1"></i>${data.withheld.count} record${data.withheld.count === 1 ? '' : 's'} withheld: ${escapeHtml(data.withheld.reason)}.</p>`
        : '';
    const counts = `<p class="locus-counts">The retrieval currently returns ${hits.length} record${hits.length === 1 ? '' : 's'}`
        + (Number.isFinite(data.total) && data.total > hits.length ? ` (the ${hits.length} best of ${data.total.toLocaleString()} matches)` : '')
        + ` in ${clusters.length} cluster${clusters.length === 1 ? '' : 's'}; `
        + `${selection.surviving.length} of the recipe's ${nRecipe} member${nRecipe === 1 ? '' : 's'} ${selection.surviving.length === 1 ? 'is' : 'are'} among them.</p>`;
    markRecipeMembers(selection.surviving);

    if (!selection.cluster) {
        el.innerHTML = `<div class="locus-empty"><p><strong>No cluster can be selected.</strong> None of the recipe's members is in the current retrieval, so nothing says which of its clusters was meant.</p>${counts}${withheld}</div>`;
        return;
    }
    const cl = selection.cluster;
    const members = cl.members || [];
    const rep = members[0] || {};
    const reasonText = {
        plurality: `Selected as the cluster holding the most surviving recipe members (${selection.score} of ${selection.surviving.length}).`,
        tie_centroid: `${selection.candidates} clusters tied on surviving members; the one nearest the survivors' centroid was selected.`,
        tie_first_member: `${selection.candidates} clusters tied on surviving members; with no geometry to decide, the one holding the first surviving member id was selected.`,
    }[selection.reason] || selection.reason;
    // The Environment button opens the same modal as the right column's card; it
    // is rendered only where the server rendered that card's slot (beta viewers),
    // and stays hidden until the card is mounted (describeCluster).
    const envButton = document.getElementById('locus_environment_slot')
        ? '<button type="button" class="btn btn-link btn-sm locus-env-open" aria-haspopup="dialog" hidden>'
            + '<i class="fa-solid fa-water me-1" aria-hidden="true"></i>Environment</button>'
        : '';
    let h = `<div class="rs-card locus-cluster">`
        + `<div class="locus-cluster-head"><h3 class="rs-card-title locus-cluster-title">`
        + `<i class="fa-solid fa-layer-group rs-icon" aria-hidden="true"></i>${escapeHtml(rep.title || rep.place_id || '(untitled)')}`
        + ` <span class="locus-unminted-badge">${members.length} record${members.length === 1 ? '' : 's'} now</span></h3>${envButton}</div>`
        + `<p class="small text-muted mb-0">${escapeHtml(reasonText)}</p>`
        + `<ul class="locus-cluster-members">`;
    members.forEach((m) => {
        const pid = canonicalMemberId(m.place_id);
        const range = formatRange(m.temporal_range);
        h += `<li><a href="${escapeHtml(recordUrl(m.place_id, data.record_urls))}" class="locus-record-link" data-pid="${escapeHtml(pid)}">${escapeHtml(m.title || m.place_id)}</a>`
            + (range ? `<span class="locus-temporal" title="Attested date range">${escapeHtml(range)}</span>` : '')
            + `<span class="locus-ns">${escapeHtml(m.namespace || '')}</span>`
            + (recipeSet.has(pid) ? `<span class="locus-was-member" title="Listed in the recipe">in recipe</span>` : `<span class="locus-was-member" style="background:#f1f3f5;color:#5a5f64" title="Joined since the recipe was made">new</span>`)
            + linkButtonHtml({ target: pid, targetLabel: m.title || pid, cls: 'btn btn-link btn-sm p-0 ms-2 align-baseline locus-link-btn' })
            + `</li>`;
    });
    h += `</ul>${counts}${withheld}</div>`;
    el.innerHTML = h;
}

function renderFailure(kind, data) {
    const el = document.getElementById('locus_result');
    if (kind === 'ok' && data && data.error === 'invalid recipe') {
        el.innerHTML = `<div class="locus-error"><p><strong>The recipe could not be run.</strong> ${escapeHtml(data.message || '')}</p></div>`;
        return;
    }
    if (data && data.error === 'resolver could not reproduce the recipe\'s retrieval') {
        el.innerHTML = `<div class="locus-error"><p><strong>The resolver refused.</strong> It could not reproduce this recipe's retrieval exactly, so it did not answer a different question instead: ${escapeHtml(data.detail || '')}</p></div>`;
        return;
    }
    const cls = kind === 'beta' ? 'text-body' : 'text-danger';
    el.innerHTML = `<div class="locus-empty ${cls}">${failureHtml(kind, 'The re-run')}</div>`;
}

// The generic-name stoplist is NOT part of the recipe (place#325 ruling 3): it is
// the gateway's current list, taken from the response this page resolved against.
function renderStoplist(stoplist) {
    const el = document.getElementById('locus_stoplist');
    if (!el) return;
    const n = stoplist.length;
    if (!n) {
        el.innerHTML = 'none (the gateway sent no list with this re-run)';
        return;
    }
    el.innerHTML = `<details class="locus-stoplist"><summary>${n} name${n === 1 ? '' : 's'}: the gateway's current list</summary>`
        + `<div class="locus-stoplist-body">${stoplist.map((s) => `<span class="locus-chip">${escapeHtml(s)}</span>`).join('')}</div></details>`;
}

// ── About this cluster, Elsewhere, Environment (beta viewers) ──────────────────
// The server renders these slots only for a beta viewer (the endpoints are
// beta-gated); with no slot there is nothing to do and nothing is fetched.
function showWikipedia(data) {
    const card = document.getElementById('locus_elsewhere');
    if (!card) return;
    const box = card.querySelector('.locus-wikipedia');
    renderWikipediaLinks(box, data && data.wikipedia, { total: data && data.wikipedia_total, headingTag: 'h3' });
    // Only where known: no links (the gateway search response drops them today), no card.
    card.hidden = !box.innerHTML.trim();
}

function describeCluster(members) {
    const about = document.getElementById('locus_about');
    const envSlot = document.getElementById('locus_environment_slot');
    if ((!about && !envSlot) || !Array.isArray(members) || !members.length) return;
    const f = locusFetchers(members);
    if (about) {
        about.hidden = false;
        loadGlossCards(about.querySelector('.locus-about-body'), f.gloss, { kind: 'locus', cap: f.cap, onData: showWikipedia });
    }
    if (envSlot) {
        const env = mountEnvironmentCard(envSlot, { fetchEnvironment: f.environment, cap: f.cap, headingTag: 'h2' });
        if (!env) return;
        envSlot.hidden = false;
        document.querySelectorAll('#locus_result .locus-env-open').forEach((b) => {
            b.hidden = false;
            b.addEventListener('click', () => env.open());
        });
    }
}

// ── boot ───────────────────────────────────────────────────────────────────
async function resolve(root, recipe) {
    const token = root.dataset.token;
    const url = root.dataset.resolveUrl || '/atlas/locus/resolve/';
    const { kind, status, data } = await fetchJSON(url, {
        method: 'POST',
        body: JSON.stringify({ r: token }),
        headers: { 'Content-Type': 'application/json', 'X-CSRFToken': window.csrfToken || '' },
        timeoutMs: RESOLVE_TIMEOUT_MS,
    });
    if (kind !== 'ok' || !data || data.error) {
        console.error('Locus: resolve failed', status, kind, data);
        renderFailure(kind, data);
        publish({ resolved: false, kind, status, error: data && data.error });
        return;
    }
    if (data.timeout) {
        renderFailure('timeout', data);
        publish({ resolved: false, kind: 'timeout', status });
        return;
    }
    const out = resolveRecipe(recipe, data);
    renderStoplist(out.stoplist);
    renderResult(recipe, data, out);
    // place#325: describe the selected cluster's current members (gloss now; EDOPS on click).
    if (out.selection.cluster) describeCluster(out.selection.cluster.members || []);
    const { fc, indexByPid } = hitsToFC(out.hits);
    const highlight = out.selection.cluster
        ? (out.selection.cluster.memberIds || []).map((id) => indexByPid.get(canonicalMemberId(id))).filter((i) => i != null)
        : [];
    showOnMap(fc, highlight);
    publish({
        resolved: true,
        reason: out.selection.reason,
        selectedIds: out.selection.cluster ? out.selection.cluster.memberIds.map(canonicalMemberId) : [],
        surviving: out.selection.surviving,
        missing: out.selection.missing,
        hitCount: out.hits.length,
        clusterCount: out.clusters.length,
        recordUrls: data.record_urls || {},
        params: out.params,
        stoplistCount: out.stoplist.length,
    });
}

function copyText(text, doneMessage) {
    const done = () => atlasNotice(doneMessage, { level: 'info' });
    if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(text).then(done, () => atlasNotice('Copy failed; copy it from the page instead.', { level: 'warning' }));
    } else {
        atlasNotice('Copy it from the page instead.', { level: 'warning' });
    }
}

function initControls(root) {
    // Tooltips: the site-wide delegated one on <body> (base.js) covers [data-bs-toggle="tooltip"];
    // a second instance here doubled them, and text goes in data-bs-title, not title.
    const copy = document.getElementById('locus_copy_link');
    if (copy) {
        copy.addEventListener('click', () => {
            const minted = !!(root && root.dataset.pid);
            copyText(window.location.href, minted ? 'Link copied.' : 'Link copied. It is shareable but not permanent.');
        });
    }
    const copyPid = document.getElementById('locus_copy_pid');
    if (copyPid && root && root.dataset.entityUrl) {
        copyPid.addEventListener('click', () => copyText(root.dataset.entityUrl, 'Identifier copied. It is permanent; its membership is not.'));
    }
    initMinting(root);
}

// ── minting (Stage 4) ──────────────────────────────────────────────────────
function overlapHtml(pre) {
    const parts = [];
    if (pre.minted) {
        parts.push(`<p class="mb-1"><strong>This exact recipe is already minted</strong> as <a href="${escapeHtml(pre.minted.path)}">${escapeHtml(pre.minted.pid)}</a>; minting again returns it.</p>`);
    }
    const ov = Array.isArray(pre.overlapping) ? pre.overlapping : [];
    if (ov.length) {
        parts.push(`<p class="mb-1"><strong>${ov.length} existing ${ov.length === 1 ? 'locus shares' : 'loci share'} members with this recipe.</strong> Reusing one is better than minting another when it already says what you mean:</p><ul>`
            + ov.map((o) => `<li><a href="${escapeHtml(o.url)}">${escapeHtml(o.pid)}</a> (${o.shared.length} shared of its ${o.member_count}; ${escapeHtml(o.status)}${o.kind === 'legacy' ? ', legacy' : ''})</li>`).join('')
            + '</ul>');
    }
    return parts.join('');
}

function initMinting(root) {
    const btn = document.getElementById('locus_mint');
    const modalEl = document.getElementById('locus_mint_modal');
    if (!root || !root.dataset.mintUrl || !btn || btn.disabled || !modalEl) return;
    const bs = window.bootstrap;
    if (!bs || !bs.Modal) { console.warn('Locus: Bootstrap Modal is not available; minting disabled'); return; }
    const url = root.dataset.mintUrl;
    const token = root.dataset.token;
    const confirmBox = document.getElementById('locus_mint_confirm');
    const publicBox = document.getElementById('locus_mint_public');
    const go = document.getElementById('locus_mint_go');
    const errEl = document.getElementById('locus_mint_error');
    const overlapEl = document.getElementById('locus_mint_overlap');
    const allowanceEl = document.getElementById('locus_mint_allowance');
    const modal = bs.Modal.getOrCreateInstance(modalEl);
    const setError = (msg) => { errEl.textContent = msg || ''; };

    confirmBox.addEventListener('change', () => { go.disabled = !confirmBox.checked; });

    btn.addEventListener('click', async () => {
        confirmBox.checked = false;
        if (publicBox) publicBox.checked = false;
        go.disabled = true;
        setError('');
        overlapEl.innerHTML = '';
        allowanceEl.textContent = '';
        modal.show();
        const { kind, data } = await fetchJSON(`${url}?r=${encodeURIComponent(token)}`, { timeoutMs: 15000 });
        if (kind !== 'ok' || !data || data.error) {
            setError(data && data.detail ? data.detail : 'The minting service could not be reached. Nothing was minted.');
            publish({ mint: 'preflight_failed', kind, error: data && data.error });
            return;
        }
        overlapEl.innerHTML = overlapHtml(data);
        if (data.allowance) {
            allowanceEl.textContent = `You may mint ${data.allowance.hour} more this hour (${data.allowance.day} today).`;
            if (data.allowance.hour < 1 || data.allowance.day < 1) {
                setError('You have reached your minting limit for now.');
                confirmBox.disabled = true;
            }
        }
        publish({ mint: 'preflight', pid: data.pid, overlapping: (data.overlapping || []).length, already: !!data.minted });
    });

    go.addEventListener('click', async () => {
        if (!confirmBox.checked) return;
        go.disabled = true;
        setError('');
        const { kind, status, data } = await fetchJSON(url, {
            method: 'POST',
            // Opt-in name (rulings 2026-10-21): only a ticked box sends true; default off.
            body: JSON.stringify({ r: token, confirm: true, minter_public: !!(publicBox && publicBox.checked) }),
            headers: { 'Content-Type': 'application/json', 'X-CSRFToken': window.csrfToken || '' },
            timeoutMs: 20000,
        });
        if (kind !== 'ok' || !data || data.error || !data.path) {
            const msg = data && data.error === 'rate limited' ? `Minting limit reached; try again in ${Math.ceil((data.retry_after || 60) / 60)} min.`
                : data && data.error === 'collision' ? 'This recipe hashes to a PID already bound to a different recipe; it cannot be minted. Please report it.'
                : (data && (data.detail || data.message)) || 'Minting failed; nothing was minted.';
            setError(msg);
            go.disabled = !confirmBox.checked;
            publish({ mint: 'failed', status, error: data && data.error });
            return;
        }
        publish({ mint: data.created ? 'minted' : 'existing', pid: data.pid, path: data.path });
        atlasNotice(data.created ? `Minted ${data.pid}.` : `${data.pid} was already minted for this recipe.`, { level: 'info' });
        window.location.assign(data.path);
    });
}

function init() {
    const root = document.getElementById('locus_page');
    const holder = document.getElementById('locus_recipe');
    if (!root || !holder) {                       // the error page, a frozen legacy locus, or a tombstone
        initControls(root);
        if (root && root.dataset.withdrawn === '1') publish({ resolved: false, kind: 'withdrawn', pid: root.dataset.pid });
        else publish({ resolved: false, kind: 'no_recipe' });
        return;
    }
    let recipe;
    try { recipe = JSON.parse(holder.textContent); } catch (e) { publish({ resolved: false, kind: 'bad_recipe_json' }); return; }
    initControls(root);
    if (root.dataset.withdrawn === '1') {         // a tombstone (place#308): shown, never re-run
        publish({ resolved: false, kind: 'withdrawn', pid: root.dataset.pid });
        return;
    }
    initMap();
    resolve(root, recipe);
}

// base_webpack.html injects entry bundles after the document is interactive, so a
// bare DOMContentLoaded listener would never fire (reference_deferred_scripts_readystate).
if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
else init();
