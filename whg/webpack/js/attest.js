// whg/webpack/js/attest.js
//
// The "Link" sheet (place#334 Phase 1, place#337): a signed-in contributor says
// whether two place records are the same place. Each answer is stored by the
// server as an attestation (POST /api/links); this module is the only client
// of that endpoint, and is shared by the Atlas popup and Place List
// (gazetteerInteraction.js, atlasPlaceList.js), the Record page (atlasRecord.js)
// and the Locus page (locus.js).
//
// Rulings rendered here (place#334, 2026-10-29):
//  - Q1: "Same place" sends closeMatch and is the default; "Same place, exactly"
//    (exactMatch) is offered only under "Advanced";
//  - Q10: "Not the same place" sends `distinct`, a plain binary answer;
//  - Q9: the user-visible verb is "Link". The wire vocabulary (attestation,
//    relation_type) stays out of the page text.
//
// The page decides whether linking is offered at all: <meta name="whg-link-enabled"
// content="1"> (main/base_webpack.html). Without it no control is rendered, so
// nothing here is reachable. The server makes its own decision on every request;
// the meta tag only saves showing a control the server would refuse.
//
// Two ways in:
//  - a button carrying both places (data-link-subject + data-link-target) opens
//    the sheet straight away: the Record page, where "this record" is the subject;
//  - a button carrying only a target (Atlas popup, Place List, Locus members) is
//    a two-step pick: the first click remembers that place, a banner says so, and
//    a Link click on a different place opens the sheet for the pair. A page
//    opened with ?link_from=<id>&link_from_label=<title> (the Record page's
//    "Explore similar records") starts with that place remembered.
//
// The pure helpers (isLinkableId, linkPayload, describeLinkResponse,
// attestationProvenance) take no DOM, so scripts/check-attest.mjs runs them in node.

import { setTooltipText } from './tooltipHygiene.js';

export const LINK_ENDPOINT = '/api/links';
export const JUSTIFICATION_MAX = 2000;

// Wire value -> what the user is shown. `advanced` ones sit under the disclosure.
export const RELATIONS = {
    closeMatch: { label: 'Same place', advanced: false,
        help: 'These two records describe the same place.' },
    exactMatch: { label: 'Same place, exactly', advanced: true,
        help: 'Interchangeable in every respect: the same place, the same extent, the same period. Use only when you are sure; "Same place" is right in most cases.' },
    distinct: { label: 'Not the same place', advanced: false,
        help: 'These records describe different places, however alike they look.' },
};
export const DEFAULT_RELATION = 'closeMatch';

const PLACEHOLDER_LOCALS = new Set(['', 'none', 'null', 'nan', 'undefined']);

function esc(s) {
    return String(s ?? '').replace(/[&<>"']/g, (c) => (
        { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

/** True for an id the endpoint can take: `<namespace>:<id>` (wd:Q90, gn:2988507,
 *  whg:12, whg:12:a1). Not a URL, not a placeholder such as `wd:None`. */
/** Whether a stored attestation side (its gateway id, and its Django pk when it is a WHG
 *  record) is the place named `id`, which may be either the gateway form or `whg:<pk>`. */
export function sameId(storedId, storedPk, id) {
    if (storedId === id) return true;
    return storedPk != null && id === `whg:${storedPk}`;
}

export function isLinkableId(id) {
    if (typeof id !== 'string') return false;
    const m = /^([a-z][a-z0-9_.-]*):(\S+)$/i.exec(id.trim());
    if (!m || m[2].startsWith('//')) return false;
    return !PLACEHOLDER_LOCALS.has(m[2].toLowerCase());
}

export class LinkError extends Error {}

/** The JSON body for POST /api/links. dataset_id and status are never sent: the
 *  server derives both. Throws LinkError for what the server would refuse anyway. */
export function linkPayload({ subject, target, relation = DEFAULT_RELATION, justification = '' } = {}) {
    if (!isLinkableId(subject) || !isLinkableId(target)) throw new LinkError('Both places need an identifier.');
    if (subject.trim() === target.trim()) throw new LinkError('A place cannot be linked to itself.');
    if (!Object.prototype.hasOwnProperty.call(RELATIONS, relation)) throw new LinkError(`Unknown answer: ${relation}`);
    const why = String(justification ?? '').trim();
    if (why.length > JUSTIFICATION_MAX) {
        throw new LinkError(`The reason is ${why.length} characters long; the limit is ${JUSTIFICATION_MAX}.`);
    }
    const body = { place_a: subject.trim(), place_b: target.trim(), relation_type: relation };
    if (why) body.justification = why;
    return body;
}

/** What to tell the user about one answer from /api/links.
 *  -> {kind, message}; kind is created | exists | withdrawn | signin | notfound |
 *  limit | invalid | error. `message` is plain text. */
export function describeLinkResponse(status, body, { action = 'link' } = {}) {
    const err = body && typeof body.error === 'string' && body.error.trim() ? body.error.trim() : '';
    if (status === 200 || status === 201) {
        if (action === 'withdraw') return { kind: 'withdrawn', message: 'Withdrawn. The link no longer counts.' };
        const pending = body && body.status === 'pending';
        const tail = pending ? ' It is visible to you and the dataset owners until it is published.' : '';
        return status === 201
            ? { kind: 'created', message: `Saved.${tail}` }
            : { kind: 'exists', message: `You had already linked these two places; nothing has changed.${tail}` };
    }
    if (status === 401 || (status === 403 && !err)) {
        return { kind: 'signin', message: 'Sign in to link places.' };
    }
    if (status === 404) {
        return { kind: 'notfound', message: 'One of these places could not be found or is not visible to you, so they cannot be linked.' };
    }
    if (status === 429) {
        return { kind: 'limit', message: err || 'You have reached the daily limit for links. Please try again tomorrow.' };
    }
    if (status === 400 || status === 403) {
        return { kind: 'invalid', message: err || 'The link was refused as given.' };
    }
    // A 5xx or a dropped connection may or may not have stored anything.
    return { kind: 'error', message: status
        ? `Something went wrong (HTTP ${status}); the link may not have been saved. Reload to check.`
        : 'The server could not be reached; the link may not have been saved. Reload to check.' };
}

/** "Linked by NAME, DATE" as {name, orcid, date}: the parts of an attestation's
 *  provenance line, or null without one. `orcid` is a full https URL or ''. */
export function attestationProvenance(att) {
    if (!att) return null;
    const by = att.attested_by || {};
    const name = (by.name || '').trim() || 'a WHG contributor';
    let orcid = (by.orcid || '').trim();
    if (orcid && !/^https?:\/\//i.test(orcid)) orcid = `https://orcid.org/${orcid}`;
    if (orcid && !/^https:\/\/orcid\.org\/\d{4}-\d{4}-\d{4}-\d{3}[\dX]$/i.test(orcid)) orcid = '';
    let date = '';
    const d = att.asserted_at ? new Date(att.asserted_at) : null;
    if (d && !Number.isNaN(d.getTime())) date = d.toISOString().slice(0, 10);
    return { name, orcid, date, pending: att.status === 'pending', mine: !!att.mine };
}

/** The provenance line as HTML (Record page Concordance, the sheet's own list). */
export function attestationProvenanceHtml(att) {
    const p = attestationProvenance(att);
    if (!p) return '';
    const who = p.orcid
        ? `<a href="${esc(p.orcid)}" target="_blank" rel="noopener noreferrer">${esc(p.name)}</a>`
        : esc(p.name);
    let h = `<span class="whg-link-prov">Linked by ${p.mine ? 'you' : who}${p.date ? `, ${esc(p.date)}` : ''}</span>`;
    if (p.pending) h += ' <span class="whg-link-pending">Visible to you and the dataset owners until it is published</span>';
    return h;
}

// ── the page's switch ────────────────────────────────────────────────────────
export function isLinkEnabled() {
    if (typeof document === 'undefined') return false;
    const m = document.querySelector('meta[name="whg-link-enabled"]');
    return !!(m && m.content === '1');
}

function csrfToken() {
    const m = document.querySelector('meta[name="csrf-token"]');
    return (m && m.content) || '';
}

// ── the API ──────────────────────────────────────────────────────────────────
async function send(method, url, body) {
    const init = { method, credentials: 'same-origin', headers: { Accept: 'application/json' } };
    if (body !== undefined) {
        init.headers['Content-Type'] = 'application/json';
        init.headers['X-CSRFToken'] = csrfToken();
        init.body = JSON.stringify(body);
    }
    let r;
    try {
        r = await fetch(url, init);
    } catch (e) {
        return { status: 0, body: null };
    }
    const data = await r.json().catch(() => null);
    return { status: r.status, body: data };
}

export function postLink(payload) {
    return send('POST', LINK_ENDPOINT, payload);
}

export function withdrawLink(id) {
    return send('DELETE', LINK_ENDPOINT, { id });
}

/** The links about one place that the viewer may see: {count, result} or null. */
export async function fetchAttestationsAbout(placeId) {
    if (!isLinkableId(placeId)) return null;
    const { status, body } = await send('GET', `${LINK_ENDPOINT}?about=${encodeURIComponent(placeId)}`);
    return status === 200 && body && Array.isArray(body.result) ? body : null;
}

// ── buttons ──────────────────────────────────────────────────────────────────
/** A Link button, or '' when linking is not offered or the target has no usable id.
 *  With `subject`, the click opens the sheet for the pair; without, it is one
 *  step of the two-step pick. `relation` preselects an answer; `text` replaces
 *  the default label (the Record page's "Same place" / "Not the same");
 *  `iconOnly` draws the glyph alone (the Place List's narrow rows). */
export function linkButtonHtml({ target, targetLabel = '', subject = '', subjectLabel = '', relation = '',
    text = '', cls = 'btn btn-sm btn-outline-secondary', icon = true, iconOnly = false } = {}) {
    if (!isLinkEnabled() || !isLinkableId(target)) return '';
    if (subject && (!isLinkableId(subject) || subject === target)) return '';
    const label = targetLabel || target;
    const tip = subject
        ? `${RELATIONS[relation] ? RELATIONS[relation].label : 'Link'}: ${subjectLabel || subject} and ${label}`
        : pickTooltip(target);
    const attrs = [
        `data-link-target="${esc(target)}"`, `data-link-target-label="${esc(label)}"`,
        subject ? `data-link-subject="${esc(subject)}" data-link-subject-label="${esc(subjectLabel || subject)}"` : '',
        relation ? `data-link-relation="${esc(relation)}"` : '',
    ].filter(Boolean).join(' ');
    return `<button type="button" class="${esc(cls)} whg-link-btn" ${attrs} data-bs-toggle="tooltip"`
        + ` data-bs-title="${esc(tip)}" aria-label="${esc(tip)}">`
        + (icon || iconOnly ? `<i class="fa-solid fa-link${iconOnly ? '' : ' me-1'}" aria-hidden="true"></i>` : '')
        + (iconOnly ? '' : esc(text || 'Link')) + '</button>';
}

// ── the two-step pick ────────────────────────────────────────────────────────
// Shared through window, not module scope: more than one bundle on a page can
// carry a copy of this module (whg_maplibre and the page's own entry both reach
// gazetteerInteraction.js), and they must agree on the remembered place, the
// sheet and the done-callback.
const S = typeof window === 'undefined' ? {}
    : (window.__whgLink = window.__whgLink || { pending: null, sheet: null, onDone: null });

function pickTooltip(target) {
    if (S.pending && S.pending.id !== target) return `Link this place with ${S.pending.label}`;
    if (S.pending && S.pending.id === target) return 'Chosen as the first place to link. Choose Link on another place.';
    return 'Link this place with another: choose Link here, then on the other place';
}

// Every pick button's tooltip names the remembered place, so it changes with it.
// setTooltipText, never the attribute alone: a hovered tooltip keeps its first title.
function refreshPickButtons() {
    document.querySelectorAll('.whg-link-btn:not([data-link-subject])').forEach((b) => {
        const tip = pickTooltip(b.dataset.linkTarget);
        setTooltipText(b, tip);
        b.setAttribute('aria-label', tip);
        b.classList.toggle('whg-link-picked', !!S.pending && S.pending.id === b.dataset.linkTarget);
    });
}

function renderBanner() {
    let el = document.getElementById('whg_link_banner');
    if (!S.pending) {
        if (el) el.remove();
        return;
    }
    if (!el) {
        el = document.createElement('div');
        el.id = 'whg_link_banner';
        el.className = 'whg-link-banner';
        el.setAttribute('role', 'status');
        document.body.appendChild(el);
    }
    el.innerHTML = `<i class="fa-solid fa-link me-2" aria-hidden="true"></i>Linking from <strong>${esc(S.pending.label)}</strong>:`
        + ' choose Link on the other place. <button type="button" class="btn btn-sm btn-link whg-link-cancel">Cancel</button>';
}

export function setPendingSubject(id, label, sticky = false) {
    S.pending = isLinkableId(id) ? { id, label: label || id, sticky } : null;
    renderBanner();
    refreshPickButtons();
}

export function getPendingSubject() {
    return S.pending ? { ...S.pending } : null;
}

// ── the sheet ────────────────────────────────────────────────────────────────
const SHEET_ID = 'whg_link_sheet';

// The sheet is shared by four bundles, and scripts/check-attest.mjs imports this
// module in node, which cannot load a .css import: the few rules live here.
const STYLES = `
.whg-link-sheet .whg-link-pair code { font-size: .8em; color: #5a5f64; }
.whg-link-sheet .whg-link-choice { margin-bottom: .35rem; }
.whg-link-sheet .whg-link-choice .form-text { margin-top: 0; }
.whg-link-sheet details.whg-link-advanced > summary { cursor: pointer; font-size: .875rem; color: #5a5f64; margin: .25rem 0 .35rem; }
.whg-link-sheet .whg-link-status:empty { display: none; }
.whg-link-sheet .whg-link-mine li { margin-bottom: .25rem; }
.whg-link-pending { display: inline-block; font-size: .75rem; background: #fff3cd; color: #664d03; border-radius: .25rem; padding: 0 .35rem; }
.whg-link-banner { position: fixed; left: 50%; bottom: 1rem; transform: translateX(-50%); z-index: 1060;
  background: #fff; border: 1px solid #ced4da; border-radius: .5rem; box-shadow: 0 .25rem .75rem rgba(0,0,0,.15);
  padding: .4rem .75rem; font-size: .875rem; max-width: calc(100vw - 2rem); }
.whg-link-btn.whg-link-picked { outline: 2px solid #2b6cb0; }
.whg-link-sheet.whg-link-noboot { display: block; background: rgba(0,0,0,.35); }
`;

function ensureStyles() {
    if (document.getElementById('whg_link_styles')) return;
    const s = document.createElement('style');
    s.id = 'whg_link_styles';
    s.textContent = STYLES;
    document.head.appendChild(s);
}

function choiceHtml(value) {
    const r = RELATIONS[value];
    return `<div class="form-check whg-link-choice">
        <input class="form-check-input" type="radio" name="whg_link_relation" id="whg_link_rel_${value}" value="${value}">
        <label class="form-check-label" for="whg_link_rel_${value}">${esc(r.label)}</label>
        <div class="form-text">${esc(r.help)}</div></div>`;
}

function sheetHtml() {
    return `<div class="modal-dialog modal-dialog-scrollable"><div class="modal-content">
      <div class="modal-header">
        <h2 class="modal-title h5" id="whg_link_title">Link two places</h2>
        <button type="button" class="btn-close whg-link-close" data-bs-dismiss="modal" aria-label="Close"></button>
      </div>
      <div class="modal-body">
        <p class="whg-link-pair mb-2"></p>
        <form class="whg-link-form" novalidate>
          <fieldset class="mb-2"><legend class="visually-hidden">Your answer</legend>
            ${choiceHtml('closeMatch')}
            ${choiceHtml('distinct')}
            <details class="whg-link-advanced"><summary>Advanced</summary>${choiceHtml('exactMatch')}</details>
          </fieldset>
          <label class="form-label small mb-1" for="whg_link_why">Why? <span class="text-muted">(optional; shown with the link)</span></label>
          <textarea class="form-control form-control-sm" id="whg_link_why" rows="3" maxlength="${JUSTIFICATION_MAX}"
                    placeholder="e.g. same coordinates and the same name in both sources"></textarea>
          <div class="form-text text-end whg-link-count">0 / ${JUSTIFICATION_MAX}</div>
          <div class="whg-link-status alert mt-2 mb-0" role="status" aria-live="polite"></div>
        </form>
        <div class="whg-link-mine mt-3"></div>
      </div>
      <div class="modal-footer">
        <button type="button" class="btn btn-sm btn-outline-secondary whg-link-close" data-bs-dismiss="modal">Close</button>
        <button type="button" class="btn btn-sm btn-primary whg-link-submit">Link</button>
      </div></div></div>`;
}

function buildSheet() {
    ensureStyles();
    const el = document.createElement('div');
    el.id = SHEET_ID;
    el.className = 'modal fade whg-link-sheet';
    el.tabIndex = -1;
    el.setAttribute('aria-labelledby', 'whg_link_title');
    el.setAttribute('aria-hidden', 'true');
    el.innerHTML = sheetHtml();
    document.body.appendChild(el);
    const why = el.querySelector('#whg_link_why');
    why.addEventListener('input', () => {
        el.querySelector('.whg-link-count').textContent = `${why.value.length} / ${JUSTIFICATION_MAX}`;
    });
    el.querySelector('.whg-link-submit').addEventListener('click', submit);
    el.querySelector('.whg-link-form').addEventListener('submit', (e) => { e.preventDefault(); submit(); });
    el.addEventListener('click', (e) => {
        const w = e.target.closest('.whg-link-withdraw');
        if (w) withdraw(w.dataset.linkId, w);
        // Without Bootstrap (its CDN script failed) the sheet is shown by hand, so closed by hand.
        if (S.sheet && !S.sheet.modal && e.target.closest('.whg-link-close')) hideSheet();
    });
    const Modal = window.bootstrap && window.bootstrap.Modal;
    return { el, modal: Modal ? Modal.getOrCreateInstance(el) : null };
}

function hideSheet() {
    if (!S.sheet) return;
    if (S.sheet.modal) S.sheet.modal.hide();
    else { S.sheet.el.classList.remove('show', 'whg-link-noboot'); S.sheet.el.setAttribute('aria-hidden', 'true'); }
}

function setStatus(kind, html) {
    const s = S.sheet.el.querySelector('.whg-link-status');
    const cls = { created: 'alert-success', exists: 'alert-info', withdrawn: 'alert-info', signin: 'alert-warning',
        notfound: 'alert-warning', limit: 'alert-warning', invalid: 'alert-danger', error: 'alert-danger' }[kind];
    s.className = `whg-link-status alert mt-2 mb-0 ${cls || 'alert-secondary'}`;
    s.dataset.kind = kind || '';
    s.innerHTML = html;
}

function statusHtml(d) {
    if (d.kind === 'signin') {
        const next = encodeURIComponent(location.pathname + location.search + location.hash);
        return `${esc(d.message)} <a class="alert-link" href="/accounts/login/?next=${next}">Sign in</a>`;
    }
    return esc(d.message);
}

/** Open the Link sheet for a pair.
 *  subject/target: {id, label} (or a bare id string); relation preselects an answer;
 *  onDone(result) runs after a link is saved or withdrawn, with
 *  {action: 'link'|'withdraw', relation, response}. */
export function openLinkSheet({ subject, target, relation = DEFAULT_RELATION, onDone = null } = {}) {
    const s = typeof subject === 'string' ? { id: subject, label: subject } : (subject || {});
    const t = typeof target === 'string' ? { id: target, label: target } : (target || {});
    if (!isLinkableId(s.id) || !isLinkableId(t.id) || s.id === t.id) return false;
    if (!S.sheet) S.sheet = buildSheet();
    Object.assign(S.sheet, { subject: { id: s.id, label: s.label || s.id }, target: { id: t.id, label: t.label || t.id },
        onDone, busy: false });
    const el = S.sheet.el;
    el.querySelector('.whg-link-pair').innerHTML = `Are <strong>${esc(S.sheet.subject.label)}</strong> <code>${esc(s.id)}</code>`
        + ` and <strong>${esc(S.sheet.target.label)}</strong> <code>${esc(t.id)}</code> the same place?`;
    const rel = RELATIONS[relation] ? relation : DEFAULT_RELATION;
    el.querySelectorAll('input[name="whg_link_relation"]').forEach((i) => { i.checked = i.value === rel; });
    el.querySelector('details.whg-link-advanced').open = RELATIONS[rel].advanced;
    el.querySelector('#whg_link_why').value = '';
    el.querySelector('.whg-link-count').textContent = `0 / ${JUSTIFICATION_MAX}`;
    el.querySelector('.whg-link-submit').disabled = false;
    el.querySelector('.whg-link-mine').innerHTML = '';
    setStatus('', '');
    // Bootstrap arrives by a deferred CDN script, so it may be here now if it was not at build.
    if (!S.sheet.modal && window.bootstrap && window.bootstrap.Modal) S.sheet.modal = window.bootstrap.Modal.getOrCreateInstance(el);
    if (S.sheet.modal) S.sheet.modal.show();
    else { el.classList.add('show', 'whg-link-noboot'); el.removeAttribute('aria-hidden'); }
    showMine(S.sheet.subject.id, S.sheet.target.id);
    return true;
}

// The viewer's own earlier answers about this pair, each with Withdraw. Best effort:
// if the list cannot be fetched the sheet works as before, with nothing listed.
async function showMine(subjectId, targetId) {
    const data = await fetchAttestationsAbout(subjectId);
    if (!S.sheet || S.sheet.subject.id !== subjectId || S.sheet.target.id !== targetId || !data) return;
    const rows = data.result.filter((r) => r.mine && r.status !== 'withdrawn'
        && ((sameId(r.place_a, r.place_a_pk, subjectId) && sameId(r.place_b, r.place_b_pk, targetId)) || (sameId(r.place_a, r.place_a_pk, targetId) && sameId(r.place_b, r.place_b_pk, subjectId))));
    if (!rows.length) return;
    S.sheet.el.querySelector('.whg-link-mine').innerHTML = '<p class="small mb-1"><strong>You have already linked these two places:</strong></p>'
        + `<ul class="list-unstyled small mb-0">${rows.map((r) => `<li>${esc((RELATIONS[r.relation_type] || {}).label || r.relation_type)}`
            + ` · ${attestationProvenanceHtml(r)}`
            + ` <button type="button" class="btn btn-link btn-sm p-0 align-baseline whg-link-withdraw" data-link-id="${esc(r.id)}">Withdraw</button></li>`).join('')}</ul>`;
}

async function submit() {
    if (!S.sheet || S.sheet.busy) return;
    const el = S.sheet.el;
    const checked = el.querySelector('input[name="whg_link_relation"]:checked');
    let payload;
    try {
        payload = linkPayload({ subject: S.sheet.subject.id, target: S.sheet.target.id,
            relation: checked ? checked.value : DEFAULT_RELATION, justification: el.querySelector('#whg_link_why').value });
    } catch (e) {
        setStatus('invalid', esc(e.message));
        return;
    }
    S.sheet.busy = true;
    const btn = el.querySelector('.whg-link-submit');
    btn.disabled = true;
    setStatus('', 'Saving…');
    const res = await postLink(payload);
    S.sheet.busy = false;
    const d = describeLinkResponse(res.status, res.body);
    let html = statusHtml(d);
    if ((d.kind === 'created' || d.kind === 'exists') && res.body && res.body.id != null) {
        html += ` <button type="button" class="btn btn-link btn-sm p-0 align-baseline whg-link-withdraw" data-link-id="${esc(res.body.id)}">Withdraw</button>`;
    } else {
        btn.disabled = false;    // refused or failed: the user may change the answer and retry
    }
    setStatus(d.kind, html);
    if ((d.kind === 'created' || d.kind === 'exists') && typeof S.sheet.onDone === 'function') {
        try { S.sheet.onDone({ action: 'link', relation: payload.relation_type, response: res.body }); } catch (e) { console.error(e); }
    }
}

async function withdraw(id, button) {
    if (!S.sheet || !id) return;
    if (button) button.disabled = true;
    const res = await withdrawLink(Number.isFinite(Number(id)) ? Number(id) : id);
    const d = describeLinkResponse(res.status, res.body, { action: 'withdraw' });
    setStatus(d.kind, statusHtml(d));
    if (d.kind === 'withdrawn') {
        S.sheet.el.querySelector('.whg-link-submit').disabled = false;
        const li = button && button.closest('li');
        if (li) li.remove();
        if (typeof S.sheet.onDone === 'function') {
            try { S.sheet.onDone({ action: 'withdraw', id, response: res.body }); } catch (e) { console.error(e); }
        }
    } else if (button) {
        button.disabled = false;
    }
}

// ── one delegate for every Link button on the page ──────────────────────────
/** A page-wide callback for links made from data-attribute buttons (the Record
 *  page reloads its Concordance with it). */
export function setLinkDoneHandler(fn) {
    S.onDone = typeof fn === 'function' ? fn : null;
}

function onClick(e) {
    if (!e.target || !e.target.closest) return;
    if (e.target.closest('.whg-link-cancel')) {
        setPendingSubject(null);
        return;
    }
    const btn = e.target.closest('.whg-link-btn');
    if (!btn || !isLinkEnabled()) return;
    e.preventDefault();
    const target = { id: btn.dataset.linkTarget, label: btn.dataset.linkTargetLabel };
    const relation = btn.dataset.linkRelation || DEFAULT_RELATION;
    if (btn.dataset.linkSubject) {
        openLinkSheet({ subject: { id: btn.dataset.linkSubject, label: btn.dataset.linkSubjectLabel }, target, relation,
            onDone: S.onDone });
        return;
    }
    if (!S.pending || S.pending.id === target.id) {
        setPendingSubject(target.id, target.label);
        return;
    }
    const subject = S.pending;
    if (!subject.sticky) setPendingSubject(null);
    openLinkSheet({ subject, target, relation, onDone: S.onDone });
}

function readLinkFrom() {
    try {
        const q = new URLSearchParams(location.search);
        const id = q.get('link_from');
        if (id && isLinkableId(id)) setPendingSubject(id, q.get('link_from_label') || id, true);
    } catch (e) { /* no usable query */ }
}

function init() {
    if (S.init) return;      // only one copy of this module installs the delegate (see S)
    S.init = true;
    document.addEventListener('click', onClick);
    if (isLinkEnabled()) readLinkFrom();
}

if (typeof document !== 'undefined') {
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
    else init();
}
