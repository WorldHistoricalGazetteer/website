// whg/webpack/js/enrichment.js
//
// place#325, beta pages (and the Record page's public sections with the v4 release
// switch on): the "About this group / cluster" gloss (place#299, limited), the EDOPS
// environment, and the Wikipedia links WHG's Wikidata records hold, for the Record page
// (atlasRecord.js) and the Locus page (locus.js). Data comes from the endpoints in
// places/views_enrichment.py.
//
// ── CONTRACT for the page agents (Stage 5) ──────────────────────────────────────
// Shared look: whg/webpack/css/recordShell.css (imported here too, so a page that
// imports this module gets it): .rs-layout/.rs-main/.rs-side/.rs-map/.rs-card/
// .rs-card-title/.rs-facts/.rs-icon-<theme>. See that file's header.
//
// renderGlossCards(containerEl, glossPayload, {kind, cap} = {})
//     Replaces containerEl's content with one themed .rs-card per gloss facet
//     (sources, names, type, countries, dates, unknown), then "How this was written".
//     glossPayload: the endpoint's JSON ({gloss: {...}}) or the gloss object itself.
//     No `facets` (an older server) -> the Stage 3 paragraph rendering.
// loadGlossCards(containerEl, fetchGloss, {kind, cap, onData} = {}) -> Promise
//     Spinner, then fetchGloss() (a () => Promise<{ok, status, data, timeout?}>, e.g.
//     recordFetcher(url) / locusFetchers(members).gloss), then renderGlossCards; on
//     failure a plain message + "Try again"; {public: false} -> the not-public line.
//     onData(data) is called with the whole JSON on success: data.wikipedia is there.
// mountEnvironmentCard(containerEl, {fetchEnvironment, title = 'Environment', cap,
//                      invitation, headingTag = 'h2'}) -> {open(), element}
//     A compact card (title, EDOPS badge, one sentence, "Show environment" button).
//     NOTHING is fetched until the button is clicked; then fetchEnvironment() is called
//     and the answer is shown in a Bootstrap modal built on demand (bands A-E/T, scope
//     and level wording, attribution and 'research prototype' note as in Stage 3). The
//     answer is cached in the closure: reopening never refetches. A failure (incl. 429
//     and 403) is said plainly in the modal, nothing is cached, and the button retries.
// renderWikipediaLinks(containerEl, wikipedia, {total, max = 6, headingTag = 'h3'})
//     A "Wikipedia" block: language-labelled links, English first (server order), the
//     first `max` shown and the rest behind "+n more languages". Labelled as links held
//     in WHG's Wikidata record(s). Renders nothing (and empties containerEl) when the
//     list is empty: only where known.
// recordFetcher(url) -> () => Promise   GET, same-origin credentials (the Record pair).
// locusFetchers(members) -> {gloss, environment, cap}   POSTs the capped, slimmed
//     members with X-CSRFToken (the Locus pair), exactly as Stage 3 did.
// capPostedMembers(members), LOCUS_MAX_POSTED_MEMBERS: unchanged.
//
// Wording rules carried from the server: every status is its own sentence, and
// "could not be reached" is never worded as "no data"; Band D is present-day
// only; the years used for Band T, and why Band T was not asked for, are shown.
//
// Readiness for tests: window.__whgEnrichment = {gloss, environment, environmentStatus,
// environmentFetches}: gloss 'idle' | 'loading' | 'ready' | 'not_public' | 'error';
// environment the same, 'idle' until the button is clicked; environmentFetches counts
// fetchEnvironment() calls (a reopen must not add one).

import '../css/recordShell.css';
import '../css/enrichment.css';

const state = (window.__whgEnrichment = window.__whgEnrichment
    || { gloss: 'idle', environment: 'idle', environmentStatus: null, environmentFetches: 0 });
const FETCH_TIMEOUT_MS = 45000;
// = places/views_enrichment.py LOCUS_MAX_POSTED_MEMBERS: the locus endpoints refuse
// more (400), so a bigger cluster is described from a deterministic 200 of it.
export const LOCUS_MAX_POSTED_MEMBERS = 200;

function esc(s) {
    return String(s ?? '').replace(/[&<>"']/g, (c) => (
        { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

function yearText(y) {
    if (y == null) return '…';
    return y < 0 ? `${-y} BCE` : String(y);
}

function spanText(s) {
    if (!Array.isArray(s) || s.length !== 2) return '';
    return s[0] === s[1] ? yearText(s[0]) : `${yearText(s[0])}–${yearText(s[1])}`;
}

async function getJSON(url, init = {}) {
    const ctl = new AbortController();
    const timer = setTimeout(() => ctl.abort(), FETCH_TIMEOUT_MS);
    try {
        const r = await fetch(url, { credentials: 'same-origin', headers: { Accept: 'application/json' },
            ...init, signal: ctl.signal });
        let data = null;
        try { data = await r.json(); } catch (e) { data = null; }
        return { ok: r.ok, status: r.status, data };
    } catch (e) {
        return { ok: false, status: 0, data: null, timeout: e && e.name === 'AbortError' };
    } finally {
        clearTimeout(timer);
    }
}

function failureText(what, res) {
    if (res.status === 403) return `${what} is a beta feature and this account cannot see it.`;
    if (res.status === 429) return `${what} was asked for too often from this address just now; try again in a minute.`;
    if (res.timeout) return `${what} took too long to load.`;
    return `${what} could not be loaded${res.status ? ` (HTTP ${res.status})` : ''}.`;
}

function spinner(text) {
    return `<div class="enr-loading" role="status"><div class="spinner-border spinner-border-sm text-secondary me-2"
        aria-hidden="true"></div><span>${esc(text)}</span></div>`;
}

const NOT_PUBLIC_HTML = '<p class="text-muted small mb-0">Shown for records in public datasets only, and this record\'s dataset is not public. Nothing was looked up or sent anywhere.</p>';

// ── fetchers (request shapes as in Stage 3) ────────────────────────────────────
/** Record page: GET one of its two endpoints (data-endpoint on the Stage 3 sections). */
export function recordFetcher(url) {
    return () => getJSON(url);
}

// A wd: hit's Wikipedia sitelinks, if the hit carries `links` (the gateway search model
// does not declare them today: places/wikipedia_links.py). English first, at most 20 per
// member, so 200 members stay well inside the endpoint's 512 KiB body limit.
const MAX_POSTED_WIKIPEDIA_PER_MEMBER = 20;
function wikipediaLinksToPost(m) {
    const ns = String((m && m.namespace) || String((m && m.place_id) || '').split(':')[0]).toLowerCase();
    if (ns !== 'wd' || !Array.isArray(m.links)) return undefined;
    const wp = m.links.filter((l) => l && typeof l.identifier === 'string' && l.identifier.includes('.wikipedia.org/wiki/'));
    wp.sort((a, b) => Number(!a.identifier.includes('//en.')) - Number(!b.identifier.includes('//en.')));
    return wp.slice(0, MAX_POSTED_WIKIPEDIA_PER_MEMBER).map((l) => ({ type: l.type, identifier: l.identifier }));
}

/** Locus page: POST the selected cluster's members (capped, slimmed) to the two locus
 * endpoints. ``cap`` is capPostedMembers' answer, for the "first 200 of N" note. */
export function locusFetchers(members) {
    const cap = capPostedMembers(Array.isArray(members) ? members : []);
    state.posted = { sent: cap.sent.length, total: cap.total, capped: cap.capped };
    const slim = cap.sent.map((m) => ({
        place_id: m.place_id, title: m.title, namespace: m.namespace, repr_point: m.repr_point,
        names: m.names, types: m.types, ccodes: m.ccodes, temporal_range: m.temporal_range, timespans: m.timespans,
        links: wikipediaLinksToPost(m),
    }));
    const post = (url) => () => getJSON(url, {
        method: 'POST', body: JSON.stringify({ members: slim }),
        headers: { 'Content-Type': 'application/json', Accept: 'application/json', 'X-CSRFToken': window.csrfToken || '' },
    });
    return { gloss: post('/atlas/locus/gloss/'), environment: post('/atlas/locus/environment/'), cap };
}

// ── gloss ───────────────────────────────────────────────────────────────────
// Stage 3's paragraph rendering: the fallback when a payload carries no facets.
function renderGloss(body, data, kind) {
    const g = data.gloss || {};
    const sentences = (g.sentences || []).map((s) => `<p class="enr-gloss-sentence">${esc(s)}</p>`).join('');
    const unknown = (g.unknown || []).length
        ? `<h3 class="h6 mt-3 mb-1">Not known from these records</h3><ul class="enr-unknown">${
            g.unknown.map((u) => `<li>${esc(u)}</li>`).join('')}</ul>` : '';
    body.innerHTML = `<div class="enr-gloss" data-kind="${esc(kind)}">${sentences || '<p class="text-muted">Nothing to say.</p>'}
        ${unknown}
        ${howHTML(g)}</div>`;
}

function howHTML(g) {
    const limits = (g.limits || []).map((l) => `<li>${esc(l)}</li>`).join('');
    return `<details class="enr-how small text-muted mt-2"><summary>How this was written</summary><ul>${limits}</ul></details>`;
}

// facet key -> [theme, Font Awesome 6 icon] (FA 6.5.1 is on the site: base.js)
const FACET_ICONS = {
    sources: ['sources', 'fa-database'],
    names: ['names', 'fa-font'],
    type: ['type', 'fa-tag'],
    countries: ['countries', 'fa-flag'],
    dates: ['dates', 'fa-hourglass-half'],
    unknown: ['unknown', 'fa-circle-question'],
};

function facetCard(f) {
    const [theme, icon] = FACET_ICONS[f.key] || ['unknown', 'fa-circle-info'];
    const body = f.key === 'unknown'
        ? `<ul class="enr-unknown mb-0">${(f.items || []).map((i) => `<li>${esc(i.label)}</li>`).join('')}</ul>`
        : `<p class="mb-0">${esc(f.text)}</p>`;
    return `<div class="rs-card rs-facet rs-icon-${esc(theme)}" data-facet="${esc(f.key)}">
        <h3 class="rs-card-title"><i class="fa-solid ${esc(icon)} rs-icon" aria-hidden="true"></i>${esc(f.title)}</h3>
        ${body}</div>`;
}

function capNote(cap) {
    return cap && cap.capped
        ? `<p class="enr-capped small text-muted">Based on the first ${esc(cap.sent.length)} of ${esc(cap.total)} members (in order of identifier): this page describes at most ${esc(LOCUS_MAX_POSTED_MEMBERS)}.</p>`
        : '';
}

/** One themed card per gloss facet (see the contract at the top). */
export function renderGlossCards(containerEl, glossPayload, { kind = 'concordance', cap = null } = {}) {
    if (!containerEl) return;
    const data = glossPayload && glossPayload.gloss ? glossPayload : { gloss: glossPayload || {} };
    const g = data.gloss || {};
    if (!Array.isArray(g.facets)) {
        renderGloss(containerEl, data, kind);
    } else {
        const cards = g.facets.map(facetCard).join('');
        containerEl.innerHTML = `<div class="enr-gloss enr-gloss-cards" data-kind="${esc(kind)}">
            <div class="rs-facet-grid">${cards || '<p class="text-muted">Nothing to say.</p>'}</div>
            ${howHTML(g)}</div>`;
    }
    if (cap && cap.capped) containerEl.insertAdjacentHTML('afterbegin', capNote(cap));
}

/** Spinner, fetch, cards; failure says so and retries (see the contract at the top). */
export async function loadGlossCards(containerEl, fetchGloss, { kind = 'concordance', cap = null, onData = null } = {}) {
    if (!containerEl || typeof fetchGloss !== 'function') return;
    containerEl.innerHTML = spinner('Describing the records…');
    state.gloss = 'loading';
    const res = await fetchGloss();
    if (!res.ok || !res.data) {
        containerEl.innerHTML = errorHTML('The description', res);
        containerEl.querySelector('.enr-retry').addEventListener('click',
            () => loadGlossCards(containerEl, fetchGloss, { kind, cap, onData }));
        state.gloss = 'error';
        return;
    }
    if (res.data.public === false) {
        containerEl.innerHTML = NOT_PUBLIC_HTML;
        state.gloss = 'not_public';
        return;
    }
    renderGlossCards(containerEl, res.data, { kind, cap });
    state.gloss = 'ready';
    if (typeof onData === 'function') onData(res.data);
}

function errorHTML(what, res) {
    return `<div class="alert alert-warning mb-0 enr-error"><p class="mb-2">${esc(failureText(what, res))} This is not a finding that there is nothing to show.</p>
        <button type="button" class="btn btn-sm btn-outline-secondary enr-retry">Try again</button></div>`;
}

// ── Wikipedia ───────────────────────────────────────────────────────────────
function languageName(code) {
    try {
        const dn = new Intl.DisplayNames(['en'], { type: 'language' });
        const n = dn.of(code);
        if (n && n !== code) return n;
    } catch (e) { /* an unknown code, or no Intl.DisplayNames */ }
    return code;
}

function safeWikipediaUrl(u) {
    return typeof u === 'string' && /^https:\/\/[a-z][a-z0-9-]{0,19}\.wikipedia\.org\/wiki\/[^\s]+$/.test(u);
}

function wikiItem(w) {
    const name = languageName(w.lang);
    return `<li><span class="enr-wp-lang" aria-hidden="true">${esc(name)}</span><a href="${esc(w.url)}" target="_blank"`
        + ` rel="noopener noreferrer" hreflang="${esc(w.lang)}"><span class="visually-hidden">${esc(name)} Wikipedia: </span>`
        + `<span lang="${esc(w.lang)}">${esc(w.title)}</span></a></li>`;
}

/** The "Wikipedia" block for the right column's Elsewhere card (see the contract). */
export function renderWikipediaLinks(containerEl, wikipedia, { total = null, max = 6, headingTag = 'h3' } = {}) {
    if (!containerEl) return;
    const links = (Array.isArray(wikipedia) ? wikipedia : []).filter((w) => w && safeWikipediaUrl(w.url));
    if (!links.length) {
        containerEl.innerHTML = '';
        return;
    }
    const shown = links.slice(0, max);
    const rest = links.slice(max);
    const shownLangs = new Set(shown.map((w) => w.lang));
    const restLangs = new Set(rest.map((w) => w.lang));
    const languagesOnly = rest.length === restLangs.size && ![...restLangs].some((l) => shownLangs.has(l));
    const more = rest.length
        ? `<details class="enr-wp-more"><summary>+${esc(rest.length)} more ${languagesOnly
            ? (rest.length === 1 ? 'language' : 'languages') : (rest.length === 1 ? 'link' : 'links')}</summary>
            <ul class="enr-wp-list">${rest.map(wikiItem).join('')}</ul></details>` : '';
    const notAll = total != null && total > links.length
        ? `<p class="small text-muted mb-0">The first ${esc(links.length)} of ${esc(total)} are listed.</p>` : '';
    const tag = /^h[2-6]$/.test(headingTag) ? headingTag : 'h3';
    const several = links.some((w) => w.from !== links[0].from);
    containerEl.innerHTML = `<div class="enr-wikipedia">
        <${tag} class="rs-card-subtitle"><i class="fa-brands fa-wikipedia-w rs-icon" aria-hidden="true"></i>Wikipedia</${tag}>
        <p class="small text-muted mb-1">${several ? 'Links held in WHG\'s Wikidata records' : `Links held in WHG's Wikidata record ${esc(links[0].from)}`}:
            what Wikidata links to, not a claim WHG makes about the place.</p>
        <ul class="enr-wp-list">${shown.map(wikiItem).join('')}</ul>${more}${notAll}</div>`;
}

// ── environmental signature ────────────────────────────────────────────────
const YEAR_NOTES = {
    clamped: (y) => `clamped to EDOPS's 0–1998 CE coverage (the records span ${spanText(y.span)})`,
};

function yearsText(years) {
    if (!years) return '';
    if (years.used && !years.note) return `Years used for the temporal band (T): <strong>${esc(spanText(years.used))}</strong>, from the records' dates.`;
    if (years.used && years.note === 'clamped') return `Years used for the temporal band (T): <strong>${esc(spanText(years.used))}</strong>, ${esc(YEAR_NOTES.clamped(years))}.`;
    switch (years.note) {
        case 'undated': return 'Temporal band (T) not requested: no record is dated.';
        case 'outside': return `Temporal band (T) not requested: the records' dates (${esc(spanText(years.span))}) fall outside EDOPS's 0–1998 CE.`;
        case 'area_needs_single_year':
            return `Temporal band (T) not requested: EDOPS's area scope takes a single year, and the records span ${esc(spanText(years.span))}.`;
        default: return '';
    }
}

function scopeText(data) {
    const p = data.plan || {};
    const sig = (data.signatures || [])[0] || {};
    const unit = (sig.summary && sig.summary.unit) || {};
    // The level EDOPS answered at, else the level the server's plan sent
    // (api/edop_client.py BASIN_LEVEL / AREA_LEVEL): never a copy kept here.
    const level = unit.level || p.level;
    const lvl = level ? ` (BasinATLAS level ${esc(level)})` : '';
    switch (p.kind) {
        case 'basin':
            return p.n_located > 1
                ? `Scope: <strong>one basin</strong>${lvl}, the one containing the point where all ${esc(p.n_located)} located records lie.`
                : `Scope: <strong>one basin</strong>${lvl}, the one containing this location.`;
        case 'area': {
            const nb = unit.n_basins != null ? `, aggregated over ${esc(unit.n_basins)} basin${unit.n_basins === 1 ? '' : 's'}` : '';
            const why = p.tight_km != null
                ? ` They lie within ${esc(p.span_km)} km of each other (tight: ${esc(p.tight_km)} km or less), so one signature describes them.`
                : '';
            return `Scope: <strong>an area</strong>, the outline (convex hull) of the ${esc(p.n_located)} located records${nb}${lvl}.${why}`;
        }
        case 'per_member':
            return `Scope: <strong>each record's own basin</strong>${p.level ? ` (BasinATLAS level ${esc(p.level)})` : ''}. The records lie up to ${esc(p.span_km)} km apart, more than the ${esc(p.tight_km)} km within which one signature could describe them all.`;
        default: return '';
    }
}

function itemRow(it) {
    const unit = it.unit ? ` <span class="enr-unit">${esc(it.unit)}</span>` : '';
    const note = it.note ? `<div class="enr-note">${esc(it.note)}</div>` : '';
    return `<tr><th scope="row">${esc(it.label)}</th><td>${esc(it.value)}${unit}${note}</td></tr>`;
}

function bandsHTML(summary) {
    let h = '';
    (summary.bands || []).forEach((b) => {
        h += `<details class="enr-band" data-band="${esc(b.band)}"><summary><span class="enr-band-letter">${esc(b.band)}</span>
            ${esc(b.label)}</summary><p class="enr-band-desc">${esc(b.description)}</p>
            <table class="enr-table">${(b.items || []).map(itemRow).join('')}</table></details>`;
    });
    const t = summary.temporal;
    if (t) {
        const items = (t.items || []).map(itemRow).join('');
        const notes = (t.notes || []).map((n) => `<li>${esc(n)}</li>`).join('');
        h += `<details class="enr-band" data-band="T" open><summary><span class="enr-band-letter">T</span>
            Temporal, ${esc(spanText(t.years))}</summary>
            <p class="enr-band-desc">Reconstructed climate anomalies (LMR v2.1), volcanic events (eVolv2k v4) and land use (HYDE 3.4) for these years.</p>
            ${t.status === 'ok' ? `<table class="enr-table">${items}</table>` : '<p class="text-muted">EDOPS could not compute the temporal band for this request.</p>'}
            ${notes ? `<ul class="enr-notes">${notes}</ul>` : ''}</details>`;
    }
    return h;
}

function statusSentence(status, reason) {
    switch (status) {
        case 'no_basin': return 'EDOPS has no basin at this location (for example open sea or an ice sheet). That is a finding about the place, not an outage.';
        case 'unavailable': return 'EDOPS could not be reached just now. That is not a finding that there is no data; try again in a minute.';
        case 'rejected': return 'EDOPS refused the request WHG sent: a problem on WHG\'s side or a change in the service, not a finding about the place.';
        case 'disabled': return 'Environmental signatures are switched off on this server.';
        default: return reason ? String(reason) : 'No signature.';
    }
}

function signatureCard(sig, data, perMember) {
    const titles = new Map((data.members || []).map((m) => [m.key, m.title]));
    const who = perMember
        ? `<p class="enr-card-members small mb-1">${(sig.members || []).map((k) => esc(titles.get(k) || k)).join('; ')}
            ${(sig.members || []).length > 1 ? '<span class="text-muted">(these share one basin)</span>' : ''}</p>` : '';
    const t = sig.request && sig.request.from_year != null
        ? `<p class="small text-muted mb-1">Temporal band (T): ${esc(spanText([sig.request.from_year, sig.request.to_year]))}</p>` : '';
    if (sig.status !== 'ok' || !sig.summary) {
        return `<div class="enr-card enr-card-${esc(sig.status)}" data-status="${esc(sig.status)}">${who}
            <p class="mb-0">${esc(statusSentence(sig.status, sig.reason))}${sig.cached ? ' <span class="text-muted">(remembered from a moment ago)</span>' : ''}</p></div>`;
    }
    const s = sig.summary;
    const head = (s.headline || []).map((it) => `<span class="enr-chip" title="${esc(it.label)}"><span class="enr-chip-label">${esc(it.label)}</span> ${esc(it.value)}</span>`).join('');
    const pct = s.scope === 'area'
        ? '<p class="small text-muted mb-1">Over an area, EDOPS reports a <em>percentile score</em> (0–100) for continuous variables and the dominant class for categories, not averages.</p>' : '';
    const uncovered = s.unit && s.unit.uncovered_pct ? `<p class="small text-muted mb-1">${esc(s.unit.uncovered_pct)}% of the area is covered by no basin.</p>` : '';
    const caveats = (s.caveats || []).length ? `<ul class="enr-notes">${s.caveats.map((c) => `<li>${esc(c)}</li>`).join('')}</ul>` : '';
    return `<div class="enr-card" data-status="ok" data-scope="${esc(s.scope)}">${who}${t}
        <div class="enr-headline">${head}</div>${pct}${uncovered}
        ${bandsHTML(s)}${caveats}</div>`;
}

function attributionHTML(a) {
    if (!a) return '';
    return `<p class="enr-attribution small">Environmental data from <a href="${esc(a.url)}" target="_blank" rel="noopener noreferrer">${esc(a.service)}</a>,
        ${esc(a.provider)}, which draws on HydroATLAS/BasinATLAS, LMR v2.1, eVolv2k v4 and HYDE 3.4.
        <a href="${esc(a.codebook_url)}" target="_blank" rel="noopener noreferrer">What the variables mean</a>. ${esc(a.note)}</p>`;
}

function renderEnvironment(body, data) {
    const p = data.plan || {};
    let h = '';
    const scope = scopeText(data);
    if (scope) h += `<p class="enr-scope">${scope}</p>`;
    const years = p.years ? yearsText(p.years) : '';
    if (years) h += `<p class="enr-years small">${years}</p>`;
    if (data.unresolved) {
        h += `<p class="enr-incomplete small">${esc(data.unresolved)} attested equivalent${data.unresolved === 1 ? '' : 's'} could not be looked up in WHG's index just now, so ${data.unresolved === 1 ? 'it is' : 'they are'} not in this signature. Reload to try again.</p>`;
    }
    // The concordance capped its inbound attestations (places.concordance.INBOUND_LIMIT):
    // the group is partial, and an area's outline is the outline of part of it.
    if (data.truncated) {
        h += `<p class="enr-incomplete enr-truncated small">This group is partial: more records attest that they are this place than WHG reads for one page, so this signature describes only the ones read${p.kind === 'area' ? ', and the area is the outline of those only' : ''}.</p>`;
    }
    // Members an anonymous visitor could not see (an embargoed source shown to a beta
    // user) are never sent to EDOPS (places.record_group.mark_offsite): say so.
    const withheld = (p.withheld || []).length;
    if (withheld) {
        h += `<p class="enr-withheld small">${esc(withheld)} record${withheld === 1 ? ' is' : 's are'} from a source that is not public yet, so ${withheld === 1 ? 'it was' : 'they were'} not sent to EDOPS and ${withheld === 1 ? 'is' : 'are'} not in this signature.</p>`;
    }
    if (data.status === 'no_location') {
        h += '<p class="enr-card" data-status="no_location">None of these records has a location, so no signature was asked for.</p>';
    } else if (data.status === 'dispersed') {
        h += `<p class="enr-card" data-status="dispersed">The attested equivalents lie up to ${esc(p.span_km)} km apart, more than ${esc(p.limit_km)} km:
            one signature would describe none of them, so none was asked for. A spread this wide often means a mistaken link.</p>`;
    } else if (data.status === 'disabled') {
        h += `<p class="enr-card" data-status="disabled">${esc(statusSentence('disabled'))}</p>`;
    } else {
        const perMember = p.kind === 'per_member';
        h += (data.signatures || []).map((s) => signatureCard(s, data, perMember)).join('');
        if ((p.not_looked_up || []).length) {
            h += `<p class="small text-muted">${p.not_looked_up.length} more record${p.not_looked_up.length === 1 ? ' was' : 's were'} not looked up: this page asks for at most ${esc(p.max_member_signatures)} per-record signatures.</p>`;
        }
        if ((p.no_location || []).length) {
            h += `<p class="small text-muted">${p.no_location.length} record${p.no_location.length === 1 ? ' has' : 's have'} no location and ${p.no_location.length === 1 ? 'is' : 'are'} not included.</p>`;
        }
    }
    h += attributionHTML(data.attribution);
    body.innerHTML = h;
}

// ── the environment card + on-demand modal ───────────────────────────────────
let modalSeq = 0;

const DEFAULT_INVITATION = 'River basin, climate and land-use figures for where these records lie, from EDOPS, '
    + 'a research service outside WHG. Nothing is looked up or sent until you ask.';

/** The compact Environment card; EDOPS is asked only on click (see the contract). */
export function mountEnvironmentCard(containerEl, {
    fetchEnvironment, title = 'Environment', cap = null, invitation = DEFAULT_INVITATION, headingTag = 'h2',
} = {}) {
    if (!containerEl || typeof fetchEnvironment !== 'function') return null;
    const n = ++modalSeq;
    const tag = /^h[2-6]$/.test(headingTag) ? headingTag : 'h2';
    containerEl.innerHTML = `<div class="rs-card rs-icon-environment enr-env-card">
        <${tag} class="rs-card-title"><i class="fa-solid fa-water rs-icon" aria-hidden="true"></i>${esc(title)}
            <span class="enr-badge">EDOPS</span></${tag}>
        <p class="small mb-2">${esc(invitation)}</p>
        <button type="button" class="btn btn-sm btn-outline-secondary enr-env-show" aria-haspopup="dialog">Show environment</button>
        <span class="enr-env-status small text-muted ms-2" role="status"></span></div>`;
    const card = containerEl.firstElementChild;
    const button = card.querySelector('.enr-env-show');
    const status = card.querySelector('.enr-env-status');
    state.environment = 'idle';

    let cached = null;      // the endpoint's JSON once one answer has arrived (or {public: false})
    let inflight = null;
    let modalEl = null;
    let modal = null;

    function ensureModal() {
        if (modalEl) return modalEl;
        modalEl = document.createElement('div');
        modalEl.className = 'modal fade enr-env-modal';
        modalEl.id = `enr_env_modal_${n}`;
        modalEl.tabIndex = -1;
        modalEl.setAttribute('aria-labelledby', `enr_env_modal_${n}_h`);
        modalEl.setAttribute('aria-hidden', 'true');
        modalEl.innerHTML = `<div class="modal-dialog modal-lg modal-dialog-scrollable"><div class="modal-content">
            <div class="modal-header"><h2 class="modal-title h5" id="enr_env_modal_${n}_h">${esc(title)}
                <span class="enr-badge">EDOPS</span></h2>
                <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button></div>
            <div class="modal-body enr-body"></div>
            <div class="modal-footer"><button type="button" class="btn btn-sm btn-secondary" data-bs-dismiss="modal">Close</button></div>
            </div></div>`;
        document.body.appendChild(modalEl);
        const Modal = window.bootstrap && window.bootstrap.Modal;
        modal = Modal ? (Modal.getOrCreateInstance ? Modal.getOrCreateInstance(modalEl) : new Modal(modalEl)) : null;
        return modalEl;
    }

    function show() {
        ensureModal();
        if (modal) {
            modal.show();
        } else {
            // No Bootstrap JS: show the modal's body inline after the card instead.
            modalEl.classList.remove('fade', 'modal');
            modalEl.classList.add('enr-env-inline');
            card.insertAdjacentElement('afterend', modalEl);
        }
    }

    function paint(data) {
        const body = modalEl.querySelector('.modal-body');
        if (data.public === false) {
            body.innerHTML = NOT_PUBLIC_HTML;
            return;
        }
        renderEnvironment(body, data);
        if (cap && cap.capped) body.insertAdjacentHTML('afterbegin', capNote(cap));
    }

    async function load() {
        const body = modalEl.querySelector('.modal-body');
        body.innerHTML = spinner('Asking EDOPS for the environmental signature…');
        state.environment = 'loading';
        state.environmentFetches = (state.environmentFetches || 0) + 1;
        button.disabled = true;
        status.textContent = 'Asking EDOPS…';
        const res = await fetchEnvironment();
        button.disabled = false;
        if (!res || !res.ok || !res.data) {
            body.innerHTML = errorHTML('The environmental signature', res || {});
            body.querySelector('.enr-retry').addEventListener('click', start);
            status.textContent = 'Not loaded: try again.';
            state.environment = 'error';
            return;
        }
        cached = res.data;
        paint(cached);
        status.textContent = res.data.public === false ? '' : 'Loaded.';
        if (res.data.public !== false) state.environmentStatus = res.data.status;
        state.environment = res.data.public === false ? 'not_public' : 'ready';
    }

    function start() {
        if (!inflight) inflight = load().finally(() => { inflight = null; });
        return inflight;
    }

    function open() {
        show();
        if (cached) return Promise.resolve();          // a reopen never refetches
        return start();
    }

    button.addEventListener('click', open);
    return { open, element: card };
}

/** At most LOCUS_MAX_POSTED_MEMBERS distinct members, the same ones every time for
 * the same cluster: sorted by place id (the cluster's own order follows the search
 * response), then the first 200. ``total`` is the distinct count before the cap. */
export function capPostedMembers(members) {
    const seen = new Set();
    const distinct = [];
    for (const m of members) {
        const k = m && m.place_id != null ? String(m.place_id) : null;
        if (k === null || seen.has(k)) continue;
        seen.add(k);
        distinct.push(m);
    }
    if (distinct.length <= LOCUS_MAX_POSTED_MEMBERS) return { sent: distinct, total: distinct.length, capped: false };
    const byId = [...distinct].sort((a, b) => {
        const x = String(a.place_id); const y = String(b.place_id);
        return x < y ? -1 : (x > y ? 1 : 0);
    });
    return { sent: byId.slice(0, LOCUS_MAX_POSTED_MEMBERS), total: distinct.length, capped: true };
}
