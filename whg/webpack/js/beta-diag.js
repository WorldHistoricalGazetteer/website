// beta-diag.js — beta-tester diagnostics (plan-beta-diagnostics; snag debugging for place#115).
//
// Loaded via base.js on every page but ACTIVE only for beta users (meta name="beta-user" = "1").
// It correlates client + server errors + snags through a per-browser-session id, enriches GlitchTip
// with beta context + per-request breadcrumbs, prefills the "Report a snag" link, and shows a one-time
// consent notice. Exposes window.WHGDiag (a no-op object for non-beta) so page scripts can call
// breadcrumb()/event() unconditionally.
//
// Privacy: detailed capture is gated on the beta cohort and lives in GlitchTip (the diagnostic channel).
// Plausible only ever gets coarse, non-identifying funnel events. No dataset contents / free text.

function meta(name) { const m = document.querySelector(`meta[name="${name}"]`); return m ? m.content : ''; }

function makeSession() {
  try {
    let id = sessionStorage.getItem('whg-beta-session');
    if (!id) {
      id = 'wb-' + Math.random().toString(16).slice(2, 10) + Date.now().toString(16).slice(-4);
      sessionStorage.setItem('whg-beta-session', id);
    }
    return id;
  } catch (_) { return 'wb-nostore'; }
}

const NOOP = { session: null, role: '', enabled: false, breadcrumb() {}, event() {} };

// The beta tags for Sentry.init's `initialScope`, so EVERY captured event carries beta_session from the
// very start — including errors thrown during page init, before initBetaDiag() runs (which previously
// left the earliest, often most telling, errors untagged). Returns null for non-beta users. makeSession()
// persists to sessionStorage, so the same id is reused by initBetaDiag() later.
export function betaInitialScope() {
  if (meta('beta-user') !== '1') return null;
  return { tags: { beta: 'true', beta_session: makeSession(), user_role: meta('user-role') || 'beta' } };
}

// Which same-origin calls we augment (Workbench + place API). String-URL requests only, so we never
// wrestle with Request-object headers.
function isRelevant(url) {
  return typeof url === 'string' &&
    // '/reconcile' (the reconciliation API — the busiest beta call) is NOT a substring of
    // '/reconciliation/' (they diverge at 'e' vs 'i'), so both must be listed or server-side reconcile
    // errors never carry the X-WHG-Beta-Session header and can't be tied back to a snag.
    (url.indexOf('/reconcile') > -1 || url.indexOf('/reconciliation/') > -1 ||
     url.indexOf('/workbench/') > -1 || url.indexOf('/api/place') > -1);
}
function shortPath(url) {
  try { return new URL(url, location.origin).pathname; } catch (_) { return String(url).split('?')[0]; }
}
// A coarse, low-cardinality Plausible funnel event inferred from a successful call (never with ids).
function semanticEvent(method, path, status) {
  if (status >= 400 || method !== 'POST') return null;
  if (/\/checkout\//.test(path)) return 'wb_checkout';
  if (/\/publish\/$/.test(path)) return 'wb_publish';
  if (/\/suggestions\/$/.test(path)) return 'wb_suggest_submit';
  if (/\/suggestions\/\d+\/review\//.test(path)) return 'wb_suggest_review';
  return null;
}

function patchFetch(diag, session) {
  const orig = window.fetch;
  if (!orig || orig.__whgBeta) return;
  const wrapped = function (input, init) {
    const url = typeof input === 'string' ? input : (input && input.url) || '';
    const relevant = isRelevant(url);
    if (relevant && typeof input === 'string') {
      init = init || {};
      const h = new Headers(init.headers || {});
      h.set('X-WHG-Beta-Session', session);      // correlate this call with the tester's session
      init.headers = h;
    }
    const method = ((init && init.method) || 'GET').toUpperCase();
    const p = orig.call(this, input, init);
    if (relevant) {
      p.then(function (res) {
        const path = shortPath(url);
        diag.breadcrumb('fetch', method + ' ' + path, { status: res.status });
        if (res.status >= 500) diag.event('wb_error', { path, status: String(res.status) });
        const ev = semanticEvent(method, path, res.status);
        if (ev) diag.event(ev);
      }, function () { diag.breadcrumb('fetch', method + ' ' + shortPath(url) + ' (network error)'); });
    }
    return p;
  };
  wrapped.__whgBeta = true;
  window.fetch = wrapped;
}

// ── snag report ────────────────────────────────────────────────────────────────
// Open the on-site snag form (account-free for testers) in a new tab, passing the page being tested so
// the tester keeps their place. The form reads the session id from window.WHGDiag itself.
function wireSnagReport() {
  const link = document.getElementById('wb-report-snag');
  if (!link) return;
  link.addEventListener('click', function (e) {
    e.preventDefault();
    const base = link.getAttribute('href') || '/beta/snag/';
    // Pass the page AND the session id: the form opens in a NEW tab (own sessionStorage), so it can't
    // read this tab's session id otherwise.
    const sid = (window.WHGDiag && window.WHGDiag.session) || '';
    const url = base + (base.indexOf('?') > -1 ? '&' : '?') +
      'page=' + encodeURIComponent(location.href) + '&session=' + encodeURIComponent(sid);
    window.open(url, '_blank', 'noopener');
  });
}

// ── one-time consent notice ────────────────────────────────────────────────────
function maybeConsentNotice() {
  try { if (localStorage.getItem('whg-beta-diag-ack')) return; } catch (_) { return; }
  const bar = document.createElement('div');
  bar.style.cssText = 'position:fixed;bottom:0;left:0;right:0;z-index:19000;background:#332701;color:#ffe9a8;' +
    'padding:.6rem 1rem;font-size:.85rem;display:flex;gap:.75rem;align-items:center;justify-content:center;flex-wrap:wrap;';
  bar.innerHTML = '<span><i class="fas fa-flask me-1"></i>You\'re signed in as a <strong>beta tester</strong>. ' +
    'To help us fix issues quickly, your Workbench actions and any errors are logged in more detail while ' +
    'you test. <a href="/privacy_policy/" target="_blank" rel="noopener" style="color:#ffd257;text-decoration:underline;">Privacy</a>.</span>' +
    '<button type="button" id="whg-diag-ack" style="background:#ffd257;color:#332701;border:0;border-radius:.35rem;padding:.2rem .7rem;font-weight:600;cursor:pointer;">Got it</button>';
  document.body.appendChild(bar);
  const btn = bar.querySelector('#whg-diag-ack');
  if (btn) btn.addEventListener('click', function () { try { localStorage.setItem('whg-beta-diag-ack', new Date().toISOString()); } catch (_) { /* ignore */ } bar.remove(); });
}

export function initBetaDiag(Sentry) {
  const isBeta = meta('beta-user') === '1';
  if (!isBeta) { window.WHGDiag = NOOP; return; }
  const session = makeSession();
  const role = meta('user-role') || 'beta';

  try {
    if (Sentry) { Sentry.setTag('beta', 'true'); Sentry.setTag('beta_session', session); Sentry.setTag('user_role', role); }
  } catch (_) { /* diagnostics must never break the page */ }

  const diag = {
    session, role, enabled: true,
    breadcrumb(category, message, data) {
      try { if (window.Sentry) window.Sentry.addBreadcrumb({ category: 'wb.' + category, message, data: data || {}, level: 'info' }); } catch (_) { /* ignore */ }
    },
    event(name, props) {
      try { if (typeof window.plausible === 'function') window.plausible(name, { props: Object.assign({ beta: '1' }, props || {}) }); } catch (_) { /* ignore */ }
    },
  };
  window.WHGDiag = diag;

  patchFetch(diag, session);
  wireSnagReport();
  maybeConsentNotice();
  diag.breadcrumb('session', 'beta session started', { session });
}
