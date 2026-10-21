// whg/webpack/js/enrichment.js
//
// place#325 Stage 3, beta pages only: the "About this group" gloss (place#299,
// limited) and the EDOPS "Environmental signature", for the Record page
// (atlasRecord.js -> initRecordEnrichment) and the Locus page (locus.js ->
// loadLocusEnrichment). Both are fetched after the page has painted, from the
// beta-gated endpoints in places/views_enrichment.py.
//
// Wording rules carried from the server: every status is its own sentence, and
// "could not be reached" is never worded as "no data"; Band D is present-day
// only; the years used for Band T, and why Band T was not asked for, are shown.
//
// Readiness for tests: window.__whgEnrichment = {gloss, environment}, each
// 'loading' | 'ready' | 'not_public' | 'error' (environment also carries the
// server's overall status in environmentStatus).

import '../css/enrichment.css';

const state = (window.__whgEnrichment = window.__whgEnrichment
    || { gloss: 'idle', environment: 'idle', environmentStatus: null });
const FETCH_TIMEOUT_MS = 45000;

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

// Beta only (place#325 Stage 3). The pages themselves may be open to everyone (Stage 4's
// V4_LANDING_ENABLED, minted loci), so the sections load only for a beta user; the
// endpoints refuse everyone else anyway (403).
function isBetaUser() {
    const m = document.querySelector('meta[name="beta-user"]');
    return !!(m && m.getAttribute('content') === '1');
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
    if (res.timeout) return `${what} took too long to load.`;
    return `${what} could not be loaded${res.status ? ` (HTTP ${res.status})` : ''}.`;
}

function spinner(text) {
    return `<div class="enr-loading" role="status"><div class="spinner-border spinner-border-sm text-secondary me-2"
        aria-hidden="true"></div><span>${esc(text)}</span></div>`;
}

// ── gloss ───────────────────────────────────────────────────────────────────
function renderGloss(body, data, kind) {
    const g = data.gloss || {};
    const sentences = (g.sentences || []).map((s) => `<p class="enr-gloss-sentence">${esc(s)}</p>`).join('');
    const unknown = (g.unknown || []).length
        ? `<h3 class="h6 mt-3 mb-1">Not known from these records</h3><ul class="enr-unknown">${
            g.unknown.map((u) => `<li>${esc(u)}</li>`).join('')}</ul>` : '';
    const limits = (g.limits || []).map((l) => `<li>${esc(l)}</li>`).join('');
    body.innerHTML = `<div class="enr-gloss" data-kind="${esc(kind)}">${sentences || '<p class="text-muted">Nothing to say.</p>'}
        ${unknown}
        <details class="enr-how small text-muted mt-2"><summary>How this was written</summary><ul>${limits}</ul></details></div>`;
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
    const lvl = unit.level ? ` (BasinATLAS level ${esc(unit.level)})` : '';
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
            return `Scope: <strong>each record's own basin</strong>. The records lie up to ${esc(p.span_km)} km apart, more than the ${esc(p.tight_km)} km within which one signature could describe them all.`;
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

// ── loaders ─────────────────────────────────────────────────────────────────
async function loadInto(section, what, key, fetcher, render) {
    const body = section.querySelector('.enr-body');
    body.innerHTML = spinner(what === 'gloss' ? 'Describing the records…' : 'Asking EDOPS for the environmental signature…');
    state[key] = 'loading';
    const res = await fetcher();
    if (!res.ok || !res.data) {
        body.innerHTML = `<div class="alert alert-warning mb-0 enr-error"><p class="mb-2">${esc(failureText(
            what === 'gloss' ? 'The description' : 'The environmental signature', res))} This is not a finding that there is nothing to show.</p>
            <button type="button" class="btn btn-sm btn-outline-secondary enr-retry">Try again</button></div>`;
        body.querySelector('.enr-retry').addEventListener('click', () => loadInto(section, what, key, fetcher, render));
        state[key] = 'error';
        return;
    }
    if (res.data.public === false) {
        body.innerHTML = '<p class="text-muted small mb-0">Shown for records in public datasets only, and this record\'s dataset is not public. Nothing was looked up or sent anywhere.</p>';
        state[key] = 'not_public';
        return;
    }
    render(body, res.data);
    if (key === 'environment') state.environmentStatus = res.data.status;
    state[key] = 'ready';
}

/** Record page: sections #record_gloss and #record_environment are in the template. */
export function initRecordEnrichment() {
    if (!isBetaUser()) return;
    const gloss = document.getElementById('record_gloss');
    const env = document.getElementById('record_environment');
    // One after the other: both read the same server-side group (the concordance,
    // 0.3-3 s), which the first request caches for the second.
    const first = (gloss && gloss.dataset.endpoint)
        ? loadInto(gloss, 'gloss', 'gloss', () => getJSON(gloss.dataset.endpoint),
            (b, d) => renderGloss(b, d, 'concordance'))
        : Promise.resolve();
    if (env && env.dataset.endpoint) {
        env.querySelector('.enr-body').innerHTML = spinner('Waiting to ask EDOPS…');
        state.environment = 'loading';
        first.finally(() => loadInto(env, 'environment', 'environment', () => getJSON(env.dataset.endpoint),
            renderEnvironment));
    }
}

function ensureLocusSections(anchor) {
    let wrap = document.getElementById('locus_enrichment');
    if (wrap) return wrap;
    wrap = document.createElement('div');
    wrap.id = 'locus_enrichment';
    wrap.className = 'enr-locus';
    wrap.innerHTML = `
        <section id="locus_gloss" class="enr-section" aria-labelledby="locus_gloss_h">
            <h2 id="locus_gloss_h" class="locus-h2">About this cluster <span class="enr-badge">generated from the records' data</span></h2>
            <div class="enr-body"></div></section>
        <section id="locus_environment" class="enr-section" aria-labelledby="locus_environment_h">
            <h2 id="locus_environment_h" class="locus-h2">Environmental signature <span class="enr-badge">EDOPS</span></h2>
            <div class="enr-body"></div></section>`;
    anchor.insertAdjacentElement('afterend', wrap);
    return wrap;
}

/** Locus page: describe ``members`` (the selected cluster's current hits). */
export function loadLocusEnrichment(members, anchor) {
    if (!isBetaUser() || !anchor || !Array.isArray(members) || !members.length) return;
    ensureLocusSections(anchor);
    const slim = members.map((m) => ({
        place_id: m.place_id, title: m.title, namespace: m.namespace, repr_point: m.repr_point,
        names: m.names, types: m.types, ccodes: m.ccodes, temporal_range: m.temporal_range, timespans: m.timespans,
    }));
    const post = (url) => () => getJSON(url, {
        method: 'POST', body: JSON.stringify({ members: slim }),
        headers: { 'Content-Type': 'application/json', Accept: 'application/json', 'X-CSRFToken': window.csrfToken || '' },
    });
    loadInto(document.getElementById('locus_gloss'), 'gloss', 'gloss', post('/atlas/locus/gloss/'),
        (b, d) => renderGloss(b, d, 'locus'));
    loadInto(document.getElementById('locus_environment'), 'environment', 'environment',
        post('/atlas/locus/environment/'), renderEnvironment);
}
