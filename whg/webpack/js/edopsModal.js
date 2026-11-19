// whg/webpack/js/edopsModal.js
//
// The beta EDOPS modal body: three tabs over the provisional ``whgmodal`` payload
// (api/edops_whgmodal.py; developer/edops-whgmodal-payload-proposal.md).
//   Profile    prose by scale, each passage labelled place/basin/area, "generated text",
//              version + date + a citation line, the sources that answered and why not.
//   Signature  the existing compact key-values display (passed in as ``renderSignature``).
//   Charts     SVG drawn here from the JSON (edopsCharts.js).
// Used only where a signature carries ``whgmodal`` (beta users). Everything displayed
// is escaped; nothing is fetched here. Tabs are plain buttons (no Bootstrap tab JS), so
// the modal works wherever the old one did.
import '../css/edopsModal.css';
import { renderCharts, esc } from './edopsCharts.js';

const SCALE_LABEL = { place: 'This place', basin: 'Whole basin', area: 'Whole area', grid_cell: 'Climate grid cell', global: 'Global' };
const TABS = [['profile', 'Profile'], ['signature', 'Signature'], ['charts', 'Charts']];

export function hasWhgmodal(data) {
    return !!(data && Array.isArray(data.signatures) && data.signatures.some((s) => s && s.whgmodal));
}

function scaleBadge(scale) {
    return `<span class="em-scale" data-scale="${esc(scale)}">${esc(SCALE_LABEL[scale] || scale)}</span>`;
}

function citation(w) {
    const v = w.version || {};
    const parts = ['EDOPS (Environmental Dimensions of Place Service), Computing Place'];
    if (v.signature_version) parts.push(`signature v${v.signature_version}`);
    if (v.profile_version) parts.push(`profile v${v.profile_version}`);
    if (v.generated) parts.push(`generated ${v.generated}`);
    parts.push((w.attribution && w.attribution.url) || 'https://computingplace.org/');
    return `${parts.join(', ')}; displayed by the World Historical Gazetteer.`;
}

function scopeLine(w) {
    const s = w.scope || {};
    if (s.type === 'basin') return `Basin${s.level ? `, BasinATLAS level ${esc(s.level)}` : ''}${s.upstream_area_km2 ? `, upstream area ${esc(s.upstream_area_km2)} km²` : ''}`;
    if (s.type === 'area') return `Area, aggregated over ${esc(s.n_basins == null ? '?' : s.n_basins)} basin(s)${s.level ? ` at BasinATLAS level ${esc(s.level)}` : ''}`;
    return '';
}

export function profileHTML(w, statusSentence) {
    if (!w || w.status !== 'ok' || !w.profile) {
        return `<p class="em-status" data-status="${esc(w && w.status)}">${esc(statusSentence(w && w.status, w && w.reason))}</p>`;
    }
    const p = w.profile;
    const placeholder = p.generated_by === 'whg-adapter-placeholder';
    const head = `<div class="em-meta"><span class="em-generated">Generated text</span>`
        + `${placeholder ? ' <span class="em-placeholder">placeholder</span>' : ''}`
        + `<span class="em-scope">${scopeLine(w)}</span></div>`
        + (p.note ? `<p class="em-note small">${esc(p.note)}</p>` : '')
        + '<p class="em-note small">This text was produced by rules from data, not written by a person and not by a language model. '
        + 'Each passage says whether it describes this place or the whole basin around it.</p>';
    const secs = (p.sections || []).map((s) => `<section class="em-section" data-scale="${esc(s.scale)}">`
        + `<h4 class="em-h">${esc(s.title)} ${scaleBadge(s.scale)}</h4><p>${esc(s.text)}</p>`
        + ((s.caveats || []).length ? `<ul class="em-caveats">${s.caveats.map((c) => `<li>${esc(c)}</li>`).join('')}</ul>` : '')
        + '</section>').join('') || '<p class="text-muted">No profile text for this location.</p>';
    const src = (w.sources || []).map((s) => `<li data-status="${esc(s.status)}">${esc(s.name)}${s.status === 'ok' ? '' : ` <em>(${esc(s.status)}${s.reason ? `: ${esc(s.reason)}` : ''})</em>`}</li>`).join('');
    const cav = (w.caveats || []).map((c) => `<li>${esc(c)}</li>`).join('');
    return `${head}${secs}${cav ? `<ul class="em-caveats">${cav}</ul>` : ''}`
        + `<details class="em-sources"><summary>Sources that answered</summary><ul>${src}</ul></details>`
        + `<p class="em-cite small"><strong>Cite as:</strong> <span class="em-cite-text">${esc(citation(w))}</span></p>`;
}

export function chartsHTML(w, statusSentence) {
    if (!w || w.status !== 'ok') return profileHTML(w, statusSentence);
    const { html, count } = renderCharts(w.series);
    if (count) return `<div class="em-charts" data-charts="${count}">${html}</div>`;
    const area = w.scope && w.scope.type === 'area';
    return `<p class="text-muted em-nocharts">${area
        ? 'EDOPS describes an area by distribution summaries, not by yearly or monthly series, so there is nothing to draw at this scope. The Signature tab has the values.'
        : 'EDOPS returned no series to draw for this location.'}</p>`;
}

/**
 * @param {HTMLElement} body        the modal body
 * @param {object} data             the environment endpoint's JSON
 * @param {{renderSignature: Function, statusSentence: Function, state?: object}} hooks
 */
export function renderEdopsTabs(body, data, { renderSignature, statusSentence, state = {} }) {
    const sigs = (data.signatures || []).filter((s) => s.whgmodal);
    const placeholder = sigs.every((s) => !s.whgmodal.profile || s.whgmodal.profile.generated_by === 'whg-adapter-placeholder');
    // Until EDOPS's own prose exists the Signature tab is the substantive one.
    let active = placeholder ? 'signature' : 'profile';
    const uid = `em${Math.random().toString(36).slice(2, 8)}`;
    const picker = sigs.length > 1
        ? `<label class="em-picker small">Signature <select class="form-select form-select-sm">${sigs.map((s, i) => `<option value="${i}">${
            esc((s.members || []).length ? `Basin ${(s.whgmodal.scope && s.whgmodal.scope.basin_id) || i + 1} (${(s.members || []).length} record${(s.members || []).length === 1 ? '' : 's'})` : `Signature ${i + 1}`)}</option>`).join('')}</select></label>` : '';
    body.innerHTML = `<div class="em-tabs"><ul class="nav nav-tabs em-nav" role="tablist" aria-label="EDOPS views">`
        + TABS.map(([id, label]) => `<li class="nav-item" role="presentation"><button type="button" class="nav-link" role="tab" id="${uid}-t-${id}" `
            + `aria-controls="${uid}-p-${id}" data-tab="${id}">${label}</button></li>`).join('')
        + `</ul>${picker}`
        + TABS.map(([id]) => `<div class="em-pane" role="tabpanel" id="${uid}-p-${id}" aria-labelledby="${uid}-t-${id}" data-pane="${id}" hidden></div>`).join('')
        + '</div>';
    const pane = (id) => body.querySelector(`[data-pane="${id}"]`);
    let current = 0;
    function paintSelected() {
        const w = sigs[current].whgmodal;
        pane('profile').innerHTML = profileHTML(w, statusSentence);
        pane('charts').innerHTML = chartsHTML(w, statusSentence);
    }
    renderSignature(pane('signature'), data);
    paintSelected();
    function select(id) {
        active = id;
        body.querySelectorAll('[role="tab"]').forEach((b) => {
            const on = b.dataset.tab === id;
            b.classList.toggle('active', on);
            b.setAttribute('aria-selected', on ? 'true' : 'false');
            b.tabIndex = on ? 0 : -1;
        });
        body.querySelectorAll('.em-pane').forEach((p) => { p.hidden = p.dataset.pane !== id; });
        state.edopsTab = id;
    }
    body.querySelectorAll('[role="tab"]').forEach((b, i, all) => {
        b.addEventListener('click', () => select(b.dataset.tab));
        b.addEventListener('keydown', (e) => {
            const d = { ArrowRight: 1, ArrowLeft: -1 }[e.key];
            if (!d) return;
            const next = all[(i + d + all.length) % all.length];
            next.focus();
            select(next.dataset.tab);
            e.preventDefault();
        });
    });
    const sel = body.querySelector('.em-picker select');
    if (sel) sel.addEventListener('change', () => { current = Number(sel.value); paintSelected(); });
    select(active);
}
