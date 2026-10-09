// /whg/webpack/js/atlasNotice.js
//
// Honest failure reporting for the Atlas (place#272 theme: "we could not ask"
// must never read as "there is nothing").
//
//  • atlasNotice(message)   — a small, non-modal Bootstrap 5 toast for failures
//    that previously went only to the console (map init, basemap swap, gazetteer
//    layer load, coverage maps, AAT vocabulary, the gateway status probe).
//    Deduplicated per message, so a flaky connection cannot stack a pile of
//    identical toasts.
//  • classifyResponse()/fetchJSON() — one place that turns an HTTP status (or a
//    network failure / client timeout) into the kind of failure the user should
//    be told about: beta access, slow, unavailable, not found.
//  • betaAccessHtml()       — the Decision-Q4 wording for non-beta users, with a
//    "request access" link that opens the site's contact dialog.
//
// Deliberately free of Atlas imports so heroMap.js, atlasPlaceList.js and
// areaSearchRouter.js can all use it without a cycle. Uses window.bootstrap
// (the CDN global — see reference_atlas_bootstrap_global), never an import.

const CONTAINER_ID = 'atlas_notice_container';
const REPEAT_SUPPRESS_MS = 60 * 1000;   // the same message at most once a minute
const _lastShown = new Map();           // message → timestamp

function _container() {
    let el = document.getElementById(CONTAINER_ID);
    if (el) return el;
    el = document.createElement('div');
    el.id = CONTAINER_ID;
    el.className = 'toast-container position-fixed top-0 end-0 p-3';
    // Above the map and offcanvas panels, below Bootstrap modals (1055). Kept
    // away from the bottom edge, where the gateway banner deliberately lives.
    el.style.zIndex = '1050';
    el.style.marginTop = '56px';
    el.setAttribute('aria-live', 'polite');
    el.setAttribute('aria-atomic', 'true');
    document.body.appendChild(el);
    return el;
}

function _escape(s) {
    return String(s == null ? '' : s)
        .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

/**
 * Show a small dismissible toast. Returns true if shown, false if suppressed
 * as a repeat (or if the page has no body/Bootstrap yet — it then logs only).
 *
 * @param {string} message   plain text (escaped)
 * @param {object} [opts]    { level: 'warning'|'danger'|'info', delay: ms }
 */
export function atlasNotice(message, opts = {}) {
    const level = opts.level || 'warning';
    const now = Date.now();
    const last = _lastShown.get(message);
    if (last && now - last < REPEAT_SUPPRESS_MS) return false;
    _lastShown.set(message, now);

    try {
        if (!document.body || !(window.bootstrap && window.bootstrap.Toast)) {
            console.warn('Atlas notice:', message);
            return false;
        }
        const icon = level === 'info' ? 'fa-circle-info' : 'fa-triangle-exclamation';
        const toast = document.createElement('div');
        toast.className = `toast align-items-center border-0 text-bg-${level === 'danger' ? 'danger' : (level === 'info' ? 'light' : 'warning')}`;
        toast.setAttribute('role', level === 'info' ? 'status' : 'alert');
        toast.dataset.atlasNotice = message;
        toast.innerHTML = `
            <div class="d-flex">
                <div class="toast-body small">
                    <i class="fas ${icon} me-1" aria-hidden="true"></i>${_escape(message)}
                </div>
                <button type="button" class="btn-close me-2 m-auto" data-bs-dismiss="toast" aria-label="Close"></button>
            </div>`;
        _container().appendChild(toast);
        toast.addEventListener('hidden.bs.toast', () => toast.remove(), { once: true });
        window.bootstrap.Toast.getOrCreateInstance(toast, {
            autohide: true, delay: opts.delay || 9000,
        }).show();
        return true;
    } catch (e) {
        console.warn('Atlas notice (toast failed):', message, e);
        return false;
    }
}

/* ── Beta access (Decision Q4: page stays open; say why, offer a way in) ── */

export const BETA_ACCESS_TEXT = 'This is a beta feature — request access to use it.';

export function betaAccessHtml() {
    return 'This is a beta feature — '
        + '<a href="#" class="atlas-beta-request">request access</a> to use it.';
}

// One delegated handler for every "request access" link, wherever it was
// rendered (result panel, place list status, area dropdown, portal). The
// contact dialog is the existing [data-whg-modal] target; dynamically inserted
// links never received whg-modal.js's attribute wiring, so open it directly.
let _betaLinkWired = false;
function _wireBetaLink() {
    if (_betaLinkWired || typeof document === 'undefined') return;
    _betaLinkWired = true;
    document.addEventListener('click', (e) => {
        const a = e.target && e.target.closest && e.target.closest('.atlas-beta-request');
        if (!a) return;
        e.preventDefault();
        if (typeof window.openWHGModal === 'function') window.openWHGModal('/contact_modal/');
    });
}
_wireBetaLink();

/* ── Failure classification ── */

/**
 * Map an HTTP status to what the user should be told.
 *   'ok' | 'beta' (403) | 'notfound' (404) | 'timeout' (504) |
 *   'unavailable' (502/503, any other 5xx) | 'error' (anything else)
 */
export function classifyStatus(status) {
    if (status >= 200 && status < 300) return 'ok';
    if (status === 403) return 'beta';
    if (status === 404) return 'notfound';
    if (status === 504) return 'timeout';
    if (status >= 500) return 'unavailable';
    return 'error';
}

/**
 * fetch() + JSON with a client-side timeout (AbortController), returning a
 * classified outcome instead of throwing:
 *   { kind, status, data }
 * kind is classifyStatus()'s, plus 'timeout' for a client-side timeout,
 * 'unavailable' for a network failure, and 'aborted' when the caller's own
 * signal cancelled it (a superseded request — callers should ignore these).
 *
 * @param {string} url
 * @param {object} [init]  fetch init; plus { timeoutMs, signal }
 */
export async function fetchJSON(url, init = {}) {
    const { timeoutMs = 30000, signal: outer, ...rest } = init;
    const ctrl = new AbortController();
    let timedOut = false;
    const timer = setTimeout(() => { timedOut = true; ctrl.abort(); }, timeoutMs);
    const onOuterAbort = () => ctrl.abort();
    if (outer) {
        if (outer.aborted) ctrl.abort();
        else outer.addEventListener('abort', onOuterAbort, { once: true });
    }
    try {
        const resp = await fetch(url, { credentials: 'same-origin', ...rest, signal: ctrl.signal });
        let data = null;
        try { data = await resp.json(); } catch (e) { data = null; }
        return { kind: classifyStatus(resp.status), status: resp.status, data };
    } catch (e) {
        if (timedOut) return { kind: 'timeout', status: 0, data: null };
        if (outer && outer.aborted) return { kind: 'aborted', status: 0, data: null };
        return { kind: 'unavailable', status: 0, data: null };
    } finally {
        clearTimeout(timer);
        if (outer) outer.removeEventListener('abort', onOuterAbort);
    }
}

/** User-facing sentence for a failure kind (plain text). */
export function failureText(kind, what = 'This') {
    switch (kind) {
        case 'beta': return BETA_ACCESS_TEXT;
        case 'timeout': return `${what} took too long to come back. The service is running — please try again.`;
        case 'unavailable': return `${what} is temporarily unavailable — the search service did not answer. Please try again shortly.`;
        case 'notfound': return `${what} could not be found.`;
        default: return `${what} failed. Please try again.`;
    }
}

/** As failureText, but HTML (the beta case carries the request-access link). */
export function failureHtml(kind, what = 'This') {
    return kind === 'beta' ? betaAccessHtml() : _escape(failureText(kind, what));
}
