// licensePicker.js
//
// A guided, in-app licence picker presented as a Bootstrap modal — the same
// "answer a few questions, compare the options" experience as the public
// /licenses/ page, but with per-licence "Use this licence" actions so a
// contributor can choose terms without leaving the Workbench.
//
//   import { pickLicense } from './licensePicker';
//   const chosen = await pickLicense({ current: { spdx: 'CC-BY-NC-4.0',
//       commercial_on_request: true, adaptations_on_request: false } });
//   // chosen === { spdx_id, label, deed_url, …, commercial_on_request,
//   //              adaptations_on_request }  or null if cancelled.
//
// The vocabulary is fetched once from /licenses/data.json (the same catalogue
// that backs the public page), so the picker never drifts from the DB.
//
// "By arrangement": when the chosen licence forbids commercial use and/or
// adaptations, a second step lets the contributor offer those on request
// (contact routed via WHG) — a nuance the standard licences don't cover.

const JSON_URL = '/licenses/data.json';

let _catalogPromise = null;   // cached fetch
let _root = null;             // modal DOM (built once)
let _bsModal = null;          // bootstrap.Modal instance
let _resolve = null;          // pending pickLicense() resolver
let _settled = false;
let _resetFilter = null;      // set once by setupFilter(); clears the questions
let _byId = {};               // spdx_id -> entry, for the current catalogue
let _pendingCurrent = null;   // {spdx, commercial_on_request, adaptations_on_request}
let _refine = null;           // {entry, offerCommercial, offerAdaptations} while refining

const QUESTIONS = [
  { key: 'commercial',  label: 'Allow <strong>commercial</strong> use?' },
  { key: 'adaptations', label: 'Allow <strong>adaptations</strong> (remixing)?' },
  { key: 'attribution', label: 'Require <strong>attribution</strong> (credit)?' },
  { key: 'sharealike',  label: 'Require <strong>ShareAlike</strong> (keep open)?' },
];

function fetchCatalog() {
  if (!_catalogPromise) {
    _catalogPromise = fetch(JSON_URL, { headers: { Accept: 'application/json' } })
      .then((r) => { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); })
      .catch((err) => { _catalogPromise = null; throw err; });
  }
  return _catalogPromise;
}

function esc(s) {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

// Which "by arrangement" offers are meaningful for a licence: only where it
// actually forbids the axis (custom licences have unknown adaptations → skip).
function offersFor(entry) {
  return {
    commercial: entry && entry.permits_commercial === false,
    adaptations: !!(entry && entry.no_derivatives === true),
  };
}

function injectStyles() {
  if (document.getElementById('whg-licpick-styles')) return;
  const css = `
  .whg-licpick .modal-dialog { max-width: 940px; }
  .whg-licpick .lp-picker { background:#f4f9fd; border:1px solid #cfe0ee; border-radius:9px; padding:.8rem .9rem; margin-bottom:1rem; }
  .whg-licpick .lp-questions { display:grid; grid-template-columns:repeat(auto-fit,minmax(230px,1fr)); gap:.55rem 1.3rem; }
  .whg-licpick .lp-q { display:flex; align-items:center; justify-content:space-between; gap:.6rem; }
  .whg-licpick .lp-q .lp-label { font-size:.85rem; color:#333; }
  .whg-licpick .lp-seg { display:inline-flex; border:1px solid #b9cede; border-radius:7px; overflow:hidden; flex-shrink:0; }
  .whg-licpick .lp-seg button { border:0; background:#fff; color:#4a5b6b; font-size:.76rem; padding:.2rem .55rem; cursor:pointer; border-left:1px solid #dbe6ef; }
  .whg-licpick .lp-seg button:first-child { border-left:0; }
  .whg-licpick .lp-seg button:hover { background:#eef5fb; }
  .whg-licpick .lp-seg button.active { background:#2f6db0; color:#fff; }
  .whg-licpick .lp-foot { display:flex; align-items:center; gap:1rem; margin-top:.7rem; font-size:.83rem; }
  .whg-licpick .lp-foot .lp-count { color:#234; font-weight:500; }
  .whg-licpick .lp-foot .lp-reset { color:#2f6db0; cursor:pointer; text-decoration:none; }
  .whg-licpick .lp-foot .lp-reset:hover { text-decoration:underline; }
  .whg-licpick .lp-legend { font-size:.78rem; color:#667; display:flex; flex-wrap:wrap; gap:.4rem 1rem; margin-bottom:.8rem; }
  .whg-licpick .lp-section-title { font-size:.95rem; font-weight:600; margin:1rem 0 .5rem; color:#234; }
  .whg-licpick .lp-grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(260px,1fr)); gap:.75rem; }
  .whg-licpick .lp-card { border:1px solid #e2e6ea; border-radius:9px; padding:.8rem .85rem; background:#fff; display:flex; flex-direction:column; }
  .whg-licpick .lp-card.current { border-color:#2f6db0; box-shadow:0 0 0 2px rgba(47,109,176,.18); }
  .whg-licpick .lp-name { font-size:.85rem; color:#333; display:block; margin-top:.3rem; }
  .whg-licpick .lp-props { list-style:none; padding:0; margin:.6rem 0 .3rem; display:flex; flex-wrap:wrap; gap:.3rem; }
  .whg-licpick .lp-prop { font-size:.71rem; padding:.1rem .45rem; border-radius:999px; display:inline-flex; align-items:center; gap:.28rem; border:1px solid transparent; white-space:nowrap; }
  .whg-licpick .lp-prop.perm.yes { background:#e6f4ea; color:#1b7a3d; border-color:#bfe3cb; }
  .whg-licpick .lp-prop.cond.yes { background:#fff3d6; color:#8a6400; border-color:#f0dca2; }
  .whg-licpick .lp-prop.no, .whg-licpick .lp-prop.unknown { background:#f2f3f5; color:#99a0a8; border-color:#e3e6e9; }
  .whg-licpick .lp-actions { margin-top:auto; padding-top:.6rem; display:flex; align-items:center; gap:.75rem; flex-wrap:wrap; }
  .whg-licpick .lp-read { font-size:.78rem; text-decoration:none; }
  .whg-licpick .lp-read:hover { text-decoration:underline; }
  .whg-licpick .lp-empty { display:none; color:#8a6400; background:#fff8e6; border:1px solid #f0dca2; border-radius:8px; padding:.6rem .8rem; font-size:.85rem; }
  .whg-licpick .lp-hidden { display:none !important; }
  .whg-licpick .lp-refine { padding:.4rem 0; }
  .whg-licpick .lp-refine h6 { font-weight:600; }
  .whg-licpick .lp-refine-opts { display:flex; flex-direction:column; gap:.6rem; margin:1rem 0; }
  .whg-licpick .lp-refine-opts label { display:flex; align-items:flex-start; gap:.5rem; font-size:.9rem; background:#fbfdff; border:1px solid #dce7f1; border-radius:8px; padding:.6rem .75rem; cursor:pointer; }
  .whg-licpick .lp-refine-opts input { margin-top:.2rem; }
  .whg-licpick .lp-refine-note { font-size:.8rem; color:#667; }
  .whg-licpick-badge { font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:.78rem; font-weight:600; color:#fff; background:#495867; padding:.08rem .4rem; border-radius:5px; }
  .whg-licpick-arr { font-size:.72rem; padding:.08rem .45rem; border-radius:999px; background:#fff3d6; color:#8a6400; border:1px solid #f0dca2; white-space:nowrap; }
  `;
  const style = document.createElement('style');
  style.id = 'whg-licpick-styles';
  style.textContent = css;
  document.head.appendChild(style);
}

function segHtml(q) {
  return `<div class="lp-q"><span class="lp-label">${q.label}</span>`
    + `<span class="lp-seg" data-filter="${q.key}">`
    + `<button type="button" data-val="any" class="active">Any</button>`
    + `<button type="button" data-val="1">Yes</button>`
    + `<button type="button" data-val="0">No</button>`
    + `</span></div>`;
}

function propsHtml(e) {
  const perm = (yes, lbl) =>
    `<li class="lp-prop perm ${yes ? 'yes' : 'no'}"><i class="fas ${yes ? 'fa-check' : 'fa-xmark'}"></i>${lbl}</li>`;
  const cond = (yes, lbl) =>
    `<li class="lp-prop cond ${yes ? 'yes' : 'no'}"><i class="fas ${yes ? 'fa-check' : 'fa-xmark'}"></i>${lbl}</li>`;
  const adapt = e.no_derivatives === null
    ? `<li class="lp-prop unknown"><i class="fas fa-circle-question"></i>Adaptations: see terms</li>`
    : perm(!e.no_derivatives, 'Adaptations');
  return `<ul class="lp-props">`
    + perm(e.permits_commercial, 'Commercial use') + adapt
    + cond(e.attribution_required, 'Attribution') + cond(e.share_alike, 'ShareAlike')
    + `</ul>`;
}

function cardHtml(e, currentId) {
  const isCurrent = currentId && e.spdx_id === currentId;
  const dataset = `data-commercial="${e.permits_commercial ? 1 : 0}" `
    + `data-adaptations="${e.no_derivatives === null ? 'unknown' : (e.no_derivatives ? 0 : 1)}" `
    + `data-attribution="${e.attribution_required ? 1 : 0}" `
    + `data-sharealike="${e.share_alike ? 1 : 0}"`;
  const read = e.deed_url
    ? `<a class="lp-read" href="${esc(e.deed_url)}" target="_blank" rel="noopener">Read licence <i class="fas fa-arrow-up-right-from-square fa-xs"></i></a>`
    : `<span class="lp-read text-muted" title="${esc(e.deed_note || 'No external licence text.')}">No licence deed <i class="fas fa-circle-info fa-xs"></i></span>`;
  return `<article class="lp-card ${isCurrent ? 'current' : ''}" ${dataset}>`
    + `<div><span class="whg-licpick-badge">${esc(e.spdx_id)}</span>`
    + (isCurrent ? `<span class="lp-name" style="color:#2f6db0;font-weight:600;">Currently selected</span>` : '')
    + `<span class="lp-name">${esc(e.label)}</span></div>`
    + propsHtml(e)
    + `<div class="lp-actions">`
    + `<button type="button" class="btn btn-sm btn-primary lp-use" data-spdx="${esc(e.spdx_id)}">Use this licence</button>`
    + read + `</div></article>`;
}

function buildModal() {
  if (_root) return;
  injectStyles();
  _root = document.createElement('div');
  _root.className = 'modal fade whg-licpick';
  _root.tabIndex = -1;
  _root.setAttribute('aria-hidden', 'true');
  _root.innerHTML = `
    <div class="modal-dialog modal-dialog-scrollable modal-dialog-centered">
      <div class="modal-content">
        <div class="modal-header">
          <h5 class="modal-title"><i class="fas fa-scale-balanced me-2"></i>Choose a licence for your data</h5>
          <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
        </div>
        <div class="modal-body">
          <div class="lp-browse">
            <div class="lp-picker">
              <div><strong>Not sure which licence?</strong> Answer a few questions — the options below filter to those that match. Leave a question on “Any” to ignore it.</div>
              <div class="lp-questions">${QUESTIONS.map(segHtml).join('')}</div>
              <div class="lp-foot"><span class="lp-count"></span><a class="lp-reset" hidden>Reset all</a></div>
            </div>
            <div class="lp-legend">
              <span><span class="lp-prop perm yes"><i class="fas fa-check"></i></span> permitted</span>
              <span><span class="lp-prop cond yes"><i class="fas fa-check"></i></span> required condition</span>
              <span><span class="lp-prop no"><i class="fas fa-xmark"></i></span> not permitted / not required</span>
            </div>
            <div class="lp-empty">No licence matches every answer. Try setting one question back to “Any”.</div>
            <div class="lp-cards"></div>
          </div>
          <div class="lp-refine" hidden></div>
        </div>
        <div class="modal-footer">
          <a class="lp-fulllink me-auto small" href="/licenses/" target="_blank" rel="noopener">Full licence reference &amp; translations <i class="fas fa-arrow-up-right-from-square fa-xs"></i></a>
          <button type="button" class="btn btn-outline-secondary" data-bs-dismiss="modal">Cancel</button>
        </div>
      </div>
    </div>`;
  document.body.appendChild(_root);

  const BS = window.bootstrap;
  _bsModal = BS && BS.Modal ? new BS.Modal(_root) : null;

  _root.addEventListener('hidden.bs.modal', () => finish(null));

  // Stage 1: "Use this licence" on a card → either finish or open the refine step.
  _root.querySelector('.lp-cards').addEventListener('click', (ev) => {
    const btn = ev.target.closest('.lp-use');
    if (btn) chooseLicense(btn.getAttribute('data-spdx'));
  });
  // Stage 2: refine-panel actions.
  _root.querySelector('.lp-refine').addEventListener('click', (ev) => {
    if (ev.target.closest('.lp-back')) { showBrowse(); return; }
    if (ev.target.closest('.lp-confirm')) {
      finish(_refine.entry.spdx_id, {
        commercial_on_request: !!(_root.querySelector('.lp-opt-commercial') || {}).checked,
        adaptations_on_request: !!(_root.querySelector('.lp-opt-adaptations') || {}).checked,
      });
    }
  });

  setupFilter();
}

function showBrowse() {
  _refine = null;
  _root.querySelector('.lp-browse').hidden = false;
  _root.querySelector('.lp-refine').hidden = true;
}

// Stage 1 → decide whether the by-arrangement step is relevant.
function chooseLicense(spdx) {
  const entry = _byId[spdx];
  if (!entry) return;
  const offers = offersFor(entry);
  if (!offers.commercial && !offers.adaptations) {
    finish(spdx, { commercial_on_request: false, adaptations_on_request: false });
    return;
  }
  _refine = { entry, offers };
  const cur = _pendingCurrent && _pendingCurrent.spdx === spdx ? _pendingCurrent : null;
  const forbids = [];
  if (offers.commercial) forbids.push('commercial use');
  if (offers.adaptations) forbids.push('adaptations');
  const opt = (cls, on, label) =>
    `<label><input type="checkbox" class="${cls}"${on ? ' checked' : ''}>`
    + `<span>Consider <strong>${label}</strong> requests on a case-by-case basis <span class="text-muted">(by arrangement)</span></span></label>`;
  _root.querySelector('.lp-refine').innerHTML = `
    <button type="button" class="lp-back btn btn-sm btn-link ps-0"><i class="fas fa-arrow-left me-1"></i>Back to all licences</button>
    <h6 class="mt-1">Your choice: <span class="whg-licpick-badge">${esc(entry.spdx_id)}</span></h6>
    <p class="lp-refine-note mb-2">${esc(entry.label)}. This licence does not permit ${forbids.join(' or ')}. You can still allow ${offers.commercial && offers.adaptations ? 'them' : 'it'} <strong>by arrangement</strong> — WHG will pass enquiries to you, and your contact details stay private.</p>
    <div class="lp-refine-opts">
      ${offers.commercial ? opt('lp-opt-commercial', cur && cur.commercial_on_request, 'commercial-use') : ''}
      ${offers.adaptations ? opt('lp-opt-adaptations', cur && cur.adaptations_on_request, 'adaptation') : ''}
    </div>
    <p class="lp-refine-note">Leave these unticked to keep the licence's terms exactly as stated.</p>
    <div class="lp-refine-actions d-flex gap-2 mt-3">
      <button type="button" class="lp-confirm btn btn-primary btn-sm">Use this licence</button>
      <button type="button" class="lp-back btn btn-outline-secondary btn-sm">Back</button>
    </div>`;
  _root.querySelector('.lp-browse').hidden = true;
  _root.querySelector('.lp-refine').hidden = false;
}

// Resolve the pending promise exactly once, then hide.
function finish(spdxOrNull, flags) {
  if (_settled) return;
  _settled = true;
  const resolve = _resolve;
  _resolve = null;
  let chosen = null;
  if (spdxOrNull && _byId[spdxOrNull]) {
    const offers = offersFor(_byId[spdxOrNull]);
    chosen = Object.assign({}, _byId[spdxOrNull], {
      // Only allow a flag where the licence actually restricts that axis.
      commercial_on_request: !!(flags && flags.commercial_on_request) && offers.commercial,
      adaptations_on_request: !!(flags && flags.adaptations_on_request) && offers.adaptations,
    });
  }
  try { if (_bsModal && spdxOrNull) _bsModal.hide(); } catch (e) {}
  if (resolve) resolve(chosen);
}

function renderCards(catalog, currentId) {
  const wrap = _root.querySelector('.lp-cards');
  _byId = {};
  (catalog.entries || []).forEach((e) => { _byId[e.spdx_id] = e; });
  const bySection = {};
  (catalog.entries || []).forEach((e) => { (bySection[e.category] = bySection[e.category] || []).push(e); });
  wrap.innerHTML = (catalog.sections || []).map((s) => {
    const cards = (bySection[s.key] || []).map((e) => cardHtml(e, currentId)).join('');
    if (!cards) return '';
    return `<section class="lp-section" data-section="${s.key}">`
      + `<div class="lp-section-title">${esc(s.title)}</div>`
      + `<div class="lp-grid">${cards}</div></section>`;
  }).join('');
}

// Wire the filter ONCE (listeners on the persistent question buttons). Sets the
// module-level ``_resetFilter`` used to clear state + re-apply on each open —
// so re-opening the modal never stacks duplicate listeners.
function setupFilter() {
  const state = { commercial: 'any', adaptations: 'any', attribution: 'any', sharealike: 'any' };
  const keys = Object.keys(state);
  const countEl = _root.querySelector('.lp-count');
  const resetEl = _root.querySelector('.lp-reset');
  const emptyEl = _root.querySelector('.lp-empty');

  function apply() {
    const cards = Array.prototype.slice.call(_root.querySelectorAll('.lp-card'));
    let shown = 0;
    cards.forEach((card) => {
      let ok = true;
      for (let i = 0; i < keys.length; i++) {
        const want = state[keys[i]];
        if (want === 'any') continue;
        if (card.dataset[keys[i]] !== want) { ok = false; break; }
      }
      card.classList.toggle('lp-hidden', !ok);
      if (ok) shown++;
    });
    _root.querySelectorAll('.lp-section').forEach((sec) => {
      sec.classList.toggle('lp-hidden', !sec.querySelector('.lp-card:not(.lp-hidden)'));
    });
    const filtering = keys.some((k) => state[k] !== 'any');
    countEl.textContent = filtering ? (shown + ' of ' + cards.length + ' licences match') : '';
    resetEl.hidden = !filtering;
    emptyEl.style.display = (filtering && shown === 0) ? 'block' : 'none';
  }

  _root.querySelectorAll('.lp-seg').forEach((seg) => {
    const key = seg.getAttribute('data-filter');
    seg.querySelectorAll('button').forEach((btn) => {
      btn.addEventListener('click', () => {
        seg.querySelectorAll('button').forEach((b) => b.classList.remove('active'));
        btn.classList.add('active');
        state[key] = btn.getAttribute('data-val');
        apply();
      });
    });
  });
  resetEl.addEventListener('click', () => _resetFilter());
  _resetFilter = () => {
    keys.forEach((k) => { state[k] = 'any'; });
    _root.querySelectorAll('.lp-seg button').forEach((b) => {
      b.classList.toggle('active', b.getAttribute('data-val') === 'any');
    });
    apply();
  };
  _resetFilter();
}

/** Resolve an spdx_id to its catalogue entry ({spdx_id, label, …}), or a bare
 *  {spdx_id,label} stub if not in the vocabulary. null for empty input. */
export function describeLicense(spdx) {
  if (!spdx) return Promise.resolve(null);
  return fetchCatalog()
    .then((cat) => (cat.entries || []).find((e) => e.spdx_id === spdx) || { spdx_id: spdx, label: spdx })
    .catch(() => ({ spdx_id: spdx, label: spdx }));
}

/**
 * Wire a "Choose a licence…" control backed by a get/set of the whole choice
 * ({spdx, commercial_on_request, adaptations_on_request}). Opens the picker on
 * click and reflects the choice as a badge + any "by arrangement" annotations.
 *
 * @param {{button:HTMLElement, display:HTMLElement, clearBtn?:HTMLElement,
 *          getChoice:()=>object, setChoice:(c:object)=>void}} cfg
 * @returns {{render:()=>void}}
 */
export function wireLicenseControl(cfg) {
  injectStyles();
  function render() {
    const c = cfg.getChoice() || {};
    const spdx = c.spdx || '';
    if (!spdx) {
      cfg.display.innerHTML = '<span class="text-muted">No licence chosen yet</span>';
      if (cfg.clearBtn) cfg.clearBtn.hidden = true;
      return;
    }
    const arr = [];
    if (c.commercial_on_request) arr.push('<span class="whg-licpick-arr">commercial use by arrangement</span>');
    if (c.adaptations_on_request) arr.push('<span class="whg-licpick-arr">adaptations by arrangement</span>');
    cfg.display.innerHTML = '<span class="whg-licpick-badge">' + esc(spdx) + '</span> ' + arr.join(' ');
    if (cfg.clearBtn) cfg.clearBtn.hidden = false;
    describeLicense(spdx).then((e) => {
      if (e && e.label && e.label !== spdx && (cfg.getChoice() || {}).spdx === spdx) {
        cfg.display.innerHTML = '<span class="whg-licpick-badge">' + esc(spdx)
          + '</span> <span class="text-muted small">' + esc(e.label) + '</span> ' + arr.join(' ');
      }
    });
  }
  cfg.button.addEventListener('click', () => {
    pickLicense({ current: cfg.getChoice() }).then((chosen) => {
      if (chosen && chosen.spdx_id) {
        cfg.setChoice({
          spdx: chosen.spdx_id,
          commercial_on_request: !!chosen.commercial_on_request,
          adaptations_on_request: !!chosen.adaptations_on_request,
        });
        render();
      }
    });
  });
  if (cfg.clearBtn) {
    cfg.clearBtn.addEventListener('click', () => {
      cfg.setChoice({ spdx: '', commercial_on_request: false, adaptations_on_request: false });
      render();
    });
  }
  render();
  return { render };
}

/**
 * Open the licence-picker modal.
 * @param {{current?: (string|{spdx?:string, commercial_on_request?:boolean, adaptations_on_request?:boolean})}} [opts]
 * @returns {Promise<object|null>} the chosen licence entry (+ by-arrangement flags), or null if cancelled.
 */
export function pickLicense(opts) {
  let current = (opts && opts.current) || null;
  if (typeof current === 'string') current = { spdx: current };
  _pendingCurrent = current || null;
  const currentId = current && current.spdx ? current.spdx : null;
  return fetchCatalog().then((catalog) => new Promise((resolve) => {
    buildModal();
    _resolve = resolve;
    _settled = false;
    showBrowse();
    renderCards(catalog, currentId);
    if (_resetFilter) _resetFilter();
    if (_bsModal) _bsModal.show();
    else { window.open('/licenses/', '_blank'); finish(null); }
  }));
}

export default { pickLicense };
