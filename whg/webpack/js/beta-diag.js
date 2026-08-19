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

// ── snag / suggestion report ───────────────────────────────────────────────────
// The form opens in a DRAGGABLE PANEL over the page being reported, not in a new tab: a tester
// describing a problem needs to keep looking at it, and navigating away also risks whatever unsaved
// work sits behind (a Workbench project mid-reconciliation, a half-drawn geometry). The panel has no
// backdrop and can be dragged aside, so the screen under discussion stays visible and usable, and a
// half-typed report is only hidden — never destroyed — on close, so it survives being put down and
// picked up again. See place#181 (and place#192 for what happens to a SUBMITTED one).
const REPORT_PANEL_ID = 'whg-beta-report-panel';
let reportPanels = {};   // url → panel element, so each form keeps its own typed state

function panelChrome(title) {
  const wrap = document.createElement('div');
  wrap.className = 'whg-beta-report';
  wrap.style.cssText = 'position:fixed;z-index:20000;top:6vh;right:3vw;width:min(680px,94vw);' +
    'max-height:88vh;display:flex;flex-direction:column;background:#fff;color:#212529;' +
    'border:1px solid rgba(0,0,0,.2);border-radius:.5rem;box-shadow:0 1rem 3rem rgba(0,0,0,.35);';
  wrap.innerHTML =
    '<div class="whg-beta-report-hdr" style="display:flex;align-items:center;gap:.5rem;padding:.5rem .75rem;' +
    'border-bottom:1px solid rgba(0,0,0,.15);cursor:move;user-select:none;background:#f8f9fa;' +
    'border-radius:.5rem .5rem 0 0;">' +
    '<i class="fas fa-grip-lines text-muted"></i>' +
    `<strong style="flex:1 1 auto;font-size:.95rem;">${title}</strong>` +
    '<span class="text-muted small me-2" style="font-size:.75rem;">drag to move</span>' +
    '<button type="button" class="btn-close" aria-label="Close"></button></div>' +
    '<div class="whg-beta-report-body" style="overflow:auto;padding:.25rem .25rem 1rem;"></div>';
  return wrap;
}

// Drag by the header. Pointer events (not mouse) so a trackpad, touch screen or pen all work, and
// capture so a fast drag that leaves the header doesn't strand the panel mid-move.
function makeDraggable(panel) {
  const hdr = panel.querySelector('.whg-beta-report-hdr');
  let startX = 0, startY = 0, originX = 0, originY = 0, dragging = false;
  hdr.addEventListener('pointerdown', (e) => {
    if (e.target.closest('.btn-close')) return;
    const r = panel.getBoundingClientRect();
    panel.style.left = r.left + 'px'; panel.style.top = r.top + 'px';
    panel.style.right = 'auto'; panel.style.bottom = 'auto';
    dragging = true; startX = e.clientX; startY = e.clientY; originX = r.left; originY = r.top;
    hdr.setPointerCapture(e.pointerId);
  });
  hdr.addEventListener('pointermove', (e) => {
    if (!dragging) return;
    // Keep a strip of the header on screen, so the panel can always be grabbed again.
    const maxX = window.innerWidth - 60, maxY = window.innerHeight - 40;
    panel.style.left = Math.min(maxX, Math.max(0 - panel.offsetWidth + 60, originX + (e.clientX - startX))) + 'px';
    panel.style.top = Math.min(maxY, Math.max(0, originY + (e.clientY - startY))) + 'px';
  });
  const end = (e) => { if (dragging) { dragging = false; try { hdr.releasePointerCapture(e.pointerId); } catch (_) {} } };
  hdr.addEventListener('pointerup', end);
  hdr.addEventListener('pointercancel', end);
}

// Submit inside the panel and swap in whatever comes back (the confirmation, or the form again with
// an error). Nothing navigates, so the page being reported on is never disturbed.
// The confirmation and the form both carry a Close control. In the panel it must
// hide the panel — it used to be an <a href="javascript:window.close()">, which did
// nothing at all here (there is no window to close) and nothing on the standalone
// page either unless the tab had been opened by script. See place#187.
// Closing a panel that is still showing a half-typed FORM only hides it, so the draft survives being
// put down and picked up again. Closing one that is showing a SUBMISSION CONFIRMATION discards it
// instead — otherwise the next report opens on the previous "thank you" message with no way back to a
// blank form (place#192). `data-report-spent` marks the panel as showing a confirmation.
function closeReportPanel(panel) {
  if (panel.dataset.reportSpent === '1') {
    delete reportPanels[panel.dataset.reportUrl];
    panel.remove();
    return;
  }
  panel.style.display = 'none';
}

function wireReportClose(panel) {
  panel.querySelectorAll('[data-beta-close]').forEach((b) => b.addEventListener('click', (e) => {
    e.preventDefault();
    closeReportPanel(panel);
  }));
}

function wireReportForm(panel, url) {
  const body = panel.querySelector('.whg-beta-report-body');
  wireReportClose(panel);
  const form = body.querySelector('form');
  if (!form) return;
  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    const btn = form.querySelector('button[type="submit"]');
    const label = btn ? btn.innerHTML : '';
    if (btn) { btn.disabled = true; btn.innerHTML = '<i class="fas fa-spinner fa-spin me-1"></i>Sending…'; }
    try {
      const fd = new FormData(form);
      fd.set('embed', '1');
      const res = await fetch(url, { method: 'POST', body: fd, credentials: 'same-origin' });
      body.innerHTML = await res.text();
      // A confirmation is not worth preserving across a close; a re-rendered form (validation error) is.
      panel.dataset.reportSpent = body.querySelector('form') ? '' : '1';
      wireReportClose(panel);   // the confirmation carries its own Close
      // The confirmation offers "Report another": re-fetch a blank form into the same panel.
      body.querySelectorAll('a[href*="/beta/"]').forEach((a) => a.addEventListener('click', async (ev) => {
        ev.preventDefault();
        body.innerHTML = await (await fetch(embedUrl(a.getAttribute('href')), { credentials: 'same-origin' })).text();
        panel.dataset.reportSpent = '';   // a blank form again — worth keeping if they close mid-typing
        prefillReportForm(body);
        wireReportForm(panel, url);   // rebinds submit AND close
      }));
    } catch (err) {
      console.error('[beta] report submit failed', err);
      if (btn) { btn.disabled = false; btn.innerHTML = label; }
      const warn = document.createElement('div');
      warn.className = 'alert alert-danger py-2 mx-3';
      warn.textContent = 'Could not send the report — check your connection and try again.';
      form.prepend(warn);
    }
  });
}

// The hidden diagnostic fields the standalone page fills from the URL. In the panel we are ON the page
// being reported, so they can be read directly — no round trip through query parameters.
function prefillReportForm(body) {
  const set = (sel, val) => { const f = body.querySelector(sel); if (f && !f.value) f.value = val; };
  set('input[name="page_url"]', location.href);
  set('input[name="user_agent"]', (navigator.userAgent || '').slice(0, 300));
  set('input[name="session_id"]', (window.WHGDiag && window.WHGDiag.session) || '');
}

function embedUrl(href) {
  const base = href || '/beta/snag/';
  return base + (base.indexOf('?') > -1 ? '&' : '?') + 'embed=1&page=' + encodeURIComponent(location.href);
}

async function openReportPanel(href, title) {
  const url = href || '/beta/snag/';
  const existing = reportPanels[url];
  if (existing) { existing.style.display = 'flex'; return; }
  const panel = panelChrome(title);
  panel.id = REPORT_PANEL_ID + '-' + url.replace(/\W+/g, '');
  panel.dataset.reportUrl = url;
  document.body.appendChild(panel);
  reportPanels[url] = panel;
  makeDraggable(panel);
  panel.querySelector('.btn-close').addEventListener('click', () => closeReportPanel(panel));
  const body = panel.querySelector('.whg-beta-report-body');
  body.innerHTML = '<p class="text-muted small p-3 mb-0"><i class="fas fa-spinner fa-spin me-1"></i>Loading…</p>';
  try {
    const res = await fetch(embedUrl(url), { credentials: 'same-origin' });
    if (!res.ok) throw new Error('HTTP ' + res.status);
    body.innerHTML = await res.text();
    prefillReportForm(body);
    wireReportForm(panel, url);
  } catch (err) {
    console.error('[beta] report form failed to load', err);
    // Never strand the tester: fall back to the standalone page rather than an empty panel.
    body.innerHTML = `<p class="p-3 mb-0">Couldn't open the form here. <a href="${embedUrl(url).replace('embed=1&', '')}" target="_blank" rel="noopener">Open it in a new tab</a>.</p>`;
  }
}

function wireSnagReport() {
  document.querySelectorAll('a[href^="/beta/snag/"], a[href^="/beta/suggestion/"], #wb-report-snag').forEach((link) => {
    link.addEventListener('click', function (e) {
      e.preventDefault();
      const href = link.getAttribute('href') || '/beta/snag/';
      openReportPanel(href, /suggestion/.test(href) ? 'Suggest an improvement' : 'Report a snag');
    });
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
