// recon-tour.js — guided "Take a tour" product tour for the Reconciliation workbench.
//
// Drives the REAL UI — loads the sample dataset, runs reconciliation, opens each pane — while a
// spotlight dims the page and a coachmark narrates what is happening at each step. This is the
// "JS drives the process with highlighted indicators" onboarding Stephen asked for.
//
// Decoupled from reconciliation.js: startTour() receives a small `api` of driving hooks
// ({ loadSample, reconcile, openPane, hasProject, isRunning }) and otherwise works against the
// live DOM. Lazy-loaded (its own webpack chunk) so it costs nothing until the user asks for it.

let root = null;      // fixed-position container holding catch/ring/tip
let elCatch = null;   // transparent full-screen layer that swallows page clicks while touring
let elRing = null;    // spotlight — a box-shadow "hole" that dims everything but the target
let elTip = null;     // coachmark card
let steps = [];
let idx = 0;
let active = false;
let api = null;
let onResize = null;
let busy = false;     // an async advance() (load / reconcile) is in flight

function esc(v) {
  return String(v == null ? '' : v).replace(/[&<>"']/g, (c) =>
    ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

// ── Step definitions ────────────────────────────────────────────────────────
// Each step:
//   target()   → the element to spotlight (null = centred, full-screen dim)
//   enter()    → prepare the view before the step shows (open a pane, scroll into view)
//   advance()  → async work run when the user clicks the primary button, BEFORE moving on
//   nextLabel  → override the primary-button text
function tourSteps() {
  const q = (sel) => () => document.querySelector(sel);

  return [
    {
      key: 'welcome',
      target: null,
      title: 'Welcome to Map your Data',
      body: `This quick tour turns a raw table of place names into <strong>located, dated, standardised</strong>
        places — matched to the World Historical Gazetteer, every step running <strong>in your browser</strong>.
        We'll load a small demo dataset and drive the whole flow for you, pausing to explain each stage.
        <div class="recon-tour-note"><i class="fas fa-lock me-1"></i>Nothing is uploaded; only per-row
        name queries ever reach WHG.</div>`,
      nextLabel: 'Start tour',
    },
    {
      key: 'import',
      target: q('#recon-load-sample'),
      enter: async () => { api.openPane('recon-pane-import'); scrollTo('#recon-load-sample'); },
      title: '1 · Import a dataset',
      body: `Everything starts with a table — CSV, TSV or JSON. We'll use the built-in demo: <strong>14
        English places</strong> with county &amp; parish columns, British grid references, and messy
        historical dates. Click below and it's parsed locally.`,
      nextLabel: '<i class="fas fa-vial me-1"></i>Load the sample',
      advance: async () => { await api.loadSample(); },
    },
    {
      key: 'roles',
      target: q('#recon-map-body'),
      enter: async () => { api.openPane('recon-result'); scrollTo('#recon-map-body'); },
      title: '2 · Confirm column roles',
      body: `A <em>role</em> is guessed for each column, and the spatial hierarchy is set right in the role
        dropdown: <strong>County</strong> “Contains Parish”, <strong>Parish</strong> “Contains Place”,
        <strong>Place</strong> is the name to match. Change any that are wrong.
        <div class="recon-tour-note">Each row also has a <i class="fas fa-grip-vertical"></i> drag handle to
        <strong>reorder</strong>, a <i class="fas fa-wand-magic-sparkles"></i> button to <strong>clean/transform</strong>
        its values (trim, case, find &amp; replace…), and a <i class="fas fa-trash-alt"></i> to delete it —
        all undoable with <kbd>Ctrl</kbd>+<kbd>Z</kbd>.</div>`,
    },
    {
      key: 'coords',
      target: q('#recon-coords'),
      enter: async () => { api.openPane('recon-result'); scrollTo('#recon-coords'); },
      title: 'Coordinates — converted for you',
      body: `It recognised <strong>British National Grid</strong> references (e.g. <code>SU 123 456</code>)
        and converts them to WGS-84 latitude/longitude. <strong>Insert WGS84 columns</strong> adds them to
        your table as real columns — no spreadsheet formulas, no lookup tables.`,
    },
    {
      key: 'dates',
      target: q('#recon-dates'),
      enter: async () => { api.openPane('recon-result'); scrollTo('#recon-dates'); },
      title: 'Dates — parsed from free text',
      body: `Free-text dates become ISO start/end spans: <code>c.1200</code>, <code>15th century</code>,
        ranges, even regnal years like <code>8 Henry VI</code>. <strong>Insert ISO date columns</strong>
        adds the parsed dates to your table.`,
    },
    {
      key: 'browser',
      target: q('#recon-preview-scroll'),
      enter: async () => { api.openPane('recon-result'); scrollTo('#recon-preview-scroll'); },
      title: 'Browse &amp; edit your data',
      body: `The <strong>Data</strong> table shows <em>every</em> row (not just a sample) and stays fast on
        large files. <strong>Search all columns</strong> to filter, and switch on <strong>Edit cells</strong>
        to fix values inline — every edit is undoable, and if a matched column changes, that row is re-flagged
        for reconciliation.`,
    },
    {
      key: 'types',
      target: q('#recon-type-prompt'),
      enter: async () => { api.openPane('recon-result'); scrollTo('#recon-type-prompt'); },
      title: 'Place types — only if you contribute',
      body: `A Getty <strong>AAT place type</strong> is needed to <em>contribute</em> to WHG, but is
        <strong>optional</strong> for CSV/JSON export. If your data has no type column, add one here; then in
        <strong>Edit cells</strong> mode click a type cell to pick from the AAT hierarchy — with a one-click
        option to apply it to every row sharing that value. Not contributing? You can skip this entirely.`,
    },
    {
      key: 'scope',
      target: q('#recon-scope-btn'),
      enter: async () => { api.openPane('recon-recon'); scrollTo('#recon-scope-btn'); },
      title: 'Scope — narrow before matching',
      body: `Optionally constrain the <em>whole</em> dataset before matching, in three parts:
        <strong>Where</strong> (country codes, a WHG region, or an area you draw), <strong>What</strong>
        (a Getty <strong>AAT place type</strong>), and <strong>When</strong>. Under <em>When</em> you can
        match your data to a canonical <strong>PeriodO</strong> historical period — search by name or pick
        one suggested from your data's own area and dates; it fills the year range and travels into your
        export. Sharper scope means fewer wrong candidates. <strong>Sources</strong> similarly restricts
        which gazetteers a column uses.`,
    },
    {
      key: 'reconcile',
      target: q('#recon-run'),
      enter: async () => { api.openPane('recon-recon'); scrollTo('#recon-run'); },
      title: '3 · Reconcile against WHG',
      body: `Now we match each place to WHG — one column at a time. Only the <strong>name</strong> of each
        row is sent, never your full table. You reconcile the outermost column first
        (<strong>County</strong>) and confirm it, then the next unlocks — <strong>Parish within County</strong>,
        <strong>Place within Parish</strong> — so same-named places disambiguate by containment. Phonetic
        matching runs in-browser. We'll drive the whole chain for you now.`,
      nextLabel: '<i class="fas fa-wand-magic-sparkles me-1"></i>Run reconciliation',
      advance: async () => { await api.reconcile(); },
    },
    {
      key: 'results',
      target: q('#recon-results-wrap'),
      enter: async () => { api.openPane('recon-recon'); scrollTo('#recon-recon-summary'); },
      title: 'Matches — row by row',
      body: `Every row was reconciled <strong>individually</strong>: the three different <em>Newton</em>s
        each found their own match rather than being merged. A <strong>clear</strong> top match
        auto-confirms; anything ambiguous is held back for review rather than guessed.`,
    },
    {
      key: 'filters',
      target: q('#recon-filters'),
      enter: async () => { api.openPane('recon-recon'); scrollTo('#recon-filters'); },
      title: 'Filter to what needs attention',
      body: `Slice the results by <strong>status</strong> (needs review / auto-confirmed / no match),
        <strong>score</strong>, a column's values, coordinate/date presence, or a name search. The filter
        drives the results table, the review queue <em>and</em> the map — so on a big dataset you can focus
        on just the rows that need a decision.`,
    },
    {
      key: 'review',
      target: q('#recon-review-card'),
      enter: async () => { api.openPane('recon-review'); scrollTo('#recon-review-card'); },
      title: '4 · Review &amp; confirm',
      body: `Anything below the threshold lands here for a human decision — keyboard-first
        (<kbd>1</kbd>–<kbd>9</kbd> accept, <kbd>x</kbd> reject, <kbd>→</kbd> next), with a map, the full
        candidate list, and a <strong>Wikipedia</strong> link where a Wikidata match has one. You can accept
        more than one candidate as close matches.`,
    },
    {
      key: 'map',
      target: q('#recon-fullmap'),
      enter: async () => { api.openPane('recon-fullmap-pane'); scrollTo('#recon-fullmap'); await wait(700); },
      title: '5 · See it on the map',
      body: `Your whole dataset on one map, built from the converted coordinates, each point labelled with its
        place name. Points <strong>cluster</strong> as you zoom out and a heatmap takes over at low zoom, so it
        stays fast even with thousands of places. Hover any point for its details.`,
    },
    {
      key: 'validate',
      target: q('#recon-validate-panel'),
      enter: async () => { api.openPane('recon-export'); scrollTo('#recon-validate-panel'); },
      title: '6 · That’s a gazetteer — export or contribute',
      body: `Your table is now a <strong>gazetteer</strong>: located, dated, standardised, place-linked data.
        Export it as CSV, JSON or Linked Places (LPF) any time. Or <strong>Contribute to WHG</strong> — the LPF
        is built and validated <em>in your browser</em> first; the button stays disabled and lists what's
        missing (a place type on every row among them) until it all passes, so nothing is rejected on
        submission.`,
    },
    {
      key: 'collaborate',
      target: q('#recon-collab'),
      enter: async () => { api.openPane('recon-result'); scrollTo('#recon-collab'); },
      title: '7 · Collaborate &amp; share',
      body: `Working with others? <strong>Collaborate</strong> saves the project to a <strong>team</strong>
        — with <strong>owner / editor / viewer</strong> roles — or mints a <strong>read-only link</strong>
        anyone can import their own copy from. Team projects sync <strong>live</strong>: a teammate's
        confirmed matches appear for you within a second (watch for the <strong>● live</strong> indicator),
        and the shared copy is kept safe on the server so no one overwrites anyone else.
        <div class="recon-tour-note"><i class="fas fa-lock me-1"></i>Still local-first — nothing leaves your
        browser until you choose to save, share, or contribute.</div>`,
    },
    {
      key: 'done',
      target: null,
      title: 'That’s the tour!',
      body: `The demo is loaded and reconciled — explore any pane freely. When you're ready,
        <strong>Clear my data</strong> (top of the column-roles pane) and drop in your own file.
        Everything you just saw runs the same way on a single place or thousands of rows.`,
      nextLabel: 'Finish',
    },
  ];
}

// ── Chrome (overlay + spotlight + coachmark) ─────────────────────────────────
function buildChrome() {
  root = document.createElement('div');
  root.className = 'recon-tour';
  root.setAttribute('role', 'dialog');
  root.setAttribute('aria-modal', 'true');
  root.setAttribute('aria-label', 'Guided tour');

  elCatch = document.createElement('div');
  elCatch.className = 'recon-tour-catch';

  elRing = document.createElement('div');
  elRing.className = 'recon-tour-ring';

  elTip = document.createElement('div');
  elTip.className = 'recon-tour-tip';

  root.appendChild(elCatch);
  root.appendChild(elRing);
  root.appendChild(elTip);
  document.body.appendChild(root);

  onResize = () => reposition();
  window.addEventListener('resize', onResize, { passive: true });
  window.addEventListener('scroll', onResize, { passive: true, capture: true });
}

function teardownChrome() {
  window.removeEventListener('resize', onResize);
  window.removeEventListener('scroll', onResize, { capture: true });
  if (root && root.parentNode) root.parentNode.removeChild(root);
  root = elCatch = elRing = elTip = onResize = null;
}

function scrollTo(sel) {
  const n = document.querySelector(sel);
  if (n) try { n.scrollIntoView({ block: 'center', behavior: 'smooth' }); } catch (_) { n.scrollIntoView(); }
}

function wait(ms) { return new Promise((r) => setTimeout(r, ms)); }

// Poll until cond() is truthy (or timeout). Used to await async view changes (pane render, map draw).
async function waitFor(cond, timeout = 15000, interval = 120) {
  const start = Date.now();
  while (Date.now() - start < timeout) {
    try { if (cond()) return true; } catch (_) { /* keep polling */ }
    await wait(interval);
  }
  return false;
}

// Place the spotlight ring over the current target (or shrink to nothing for a centred step).
function positionSpotlight(target) {
  if (!elRing) return;
  if (!target) {
    // Centred step: a zero-size ring in the middle → full-screen dim, no visible cutout.
    elRing.style.width = elRing.style.height = '0px';
    elRing.style.left = '50%';
    elRing.style.top = '50%';
    elRing.classList.add('recon-tour-ring--empty');
    return;
  }
  const r = target.getBoundingClientRect();
  const pad = 8;
  elRing.classList.remove('recon-tour-ring--empty');
  elRing.style.left = Math.max(0, r.left - pad) + 'px';
  elRing.style.top = Math.max(0, r.top - pad) + 'px';
  elRing.style.width = Math.min(window.innerWidth, r.width + pad * 2) + 'px';
  elRing.style.height = r.height + pad * 2 + 'px';
}

// Place the coachmark near the target, flipping above/below and clamping to the viewport.
function positionTip(target) {
  if (!elTip) return;
  const tip = elTip.getBoundingClientRect();
  const vw = window.innerWidth, vh = window.innerHeight, gap = 14, margin = 12;
  elTip.classList.remove('recon-tour-tip--top', 'recon-tour-tip--bottom', 'recon-tour-tip--center');

  if (!target) {
    elTip.style.left = Math.round((vw - tip.width) / 2) + 'px';
    elTip.style.top = Math.round((vh - tip.height) / 2) + 'px';
    elTip.classList.add('recon-tour-tip--center');
    return;
  }
  const r = target.getBoundingClientRect();
  const below = r.bottom + gap;
  const placeBelow = below + tip.height + margin <= vh || r.top - gap - tip.height < margin;
  let top = placeBelow ? below : r.top - gap - tip.height;
  top = Math.min(Math.max(margin, top), vh - tip.height - margin);

  let left = r.left + r.width / 2 - tip.width / 2;
  left = Math.min(Math.max(margin, left), vw - tip.width - margin);

  elTip.style.left = Math.round(left) + 'px';
  elTip.style.top = Math.round(top) + 'px';
  elTip.classList.add(placeBelow ? 'recon-tour-tip--bottom' : 'recon-tour-tip--top');

  // Caret points at the target's horizontal centre (clamped within the card).
  const caretX = Math.min(Math.max(18, r.left + r.width / 2 - left), tip.width - 18);
  elTip.style.setProperty('--caret-x', Math.round(caretX) + 'px');
}

function reposition() {
  if (!active) return;
  const step = steps[idx];
  const target = step && step.target ? step.target() : null;
  positionSpotlight(target && isOnscreen(target) ? target : (target || null));
  positionTip(target && isOnscreen(target) ? target : null);
}

function isOnscreen(node) {
  const r = node.getBoundingClientRect();
  return r.width > 0 && r.height > 0;
}

// ── Rendering a step ─────────────────────────────────────────────────────────
function renderTip(step) {
  const n = idx + 1, total = steps.length;
  const nextLabel = idx === total - 1 ? (step.nextLabel || 'Finish') : (step.nextLabel || 'Next');
  const backDisabled = idx === 0 || busy;
  elTip.innerHTML = `
    <div class="recon-tour-caret"></div>
    <button type="button" class="recon-tour-x" aria-label="End tour" data-act="end">&times;</button>
    <div class="recon-tour-step">Step ${n} of ${total}</div>
    <h4 class="recon-tour-title">${step.title}</h4>
    <div class="recon-tour-body">${step.body || ''}</div>
    <div class="recon-tour-dots">${steps.map((_, i) =>
      `<span class="recon-tour-dot${i === idx ? ' is-on' : ''}"></span>`).join('')}</div>
    <div class="recon-tour-actions">
      <button type="button" class="recon-tour-skip" data-act="end">Skip tour</button>
      <div class="recon-tour-nav">
        <button type="button" class="btn btn-sm btn-outline-secondary recon-tour-back" data-act="back"${backDisabled ? ' disabled' : ''}>Back</button>
        <button type="button" class="btn btn-sm btn-primary recon-tour-next" data-act="next">${nextLabel}</button>
      </div>
    </div>`;
  elTip.querySelectorAll('[data-act]').forEach((b) =>
    b.addEventListener('click', () => onAct(b.dataset.act)));
}

async function showStep() {
  const step = steps[idx];
  if (step.enter) { try { await step.enter(); } catch (err) { console.error('[tour] enter failed', err); } }
  renderTip(step);
  // Let scroll/layout settle, then measure & place.
  await wait(step.enter ? 260 : 60);
  reposition();
  // Nudge into place again once smooth-scroll has finished.
  await wait(220);
  reposition();
}

function onAct(act) {
  if (busy) return;
  if (act === 'end') return endTour(false);
  if (act === 'back') { if (idx > 0) { idx--; showStep(); } return; }
  if (act === 'next') return goNext();
}

async function goNext() {
  const step = steps[idx];
  if (step.advance) {
    busy = true;
    const btn = elTip.querySelector('.recon-tour-next');
    let label = '';
    if (btn) { label = btn.innerHTML; btn.disabled = true; btn.innerHTML = '<i class="fas fa-spinner fa-spin me-1"></i>Working…'; }
    const back = elTip.querySelector('.recon-tour-back'); if (back) back.disabled = true;
    try { await step.advance(); }
    catch (err) { console.error('[tour] advance failed', err); }
    finally {
      busy = false;
      if (btn) { btn.disabled = false; btn.innerHTML = label; }
    }
  }
  if (idx >= steps.length - 1) return endTour(true);
  idx++;
  showStep();
}

// ── Public API ───────────────────────────────────────────────────────────────
export function startTour(hooks) {
  if (active) return;
  api = hooks || {};
  steps = tourSteps();
  idx = 0;
  active = true;
  busy = false;
  document.body.classList.add('recon-tour-open');
  buildChrome();
  document.addEventListener('keydown', keydown, true);
  showStep();
}

function endTour(completed) {
  if (!active) return;
  active = false;
  document.removeEventListener('keydown', keydown, true);
  document.body.classList.remove('recon-tour-open');
  teardownChrome();
  if (api && typeof api.onEnd === 'function') { try { api.onEnd(completed); } catch (_) { /* */ } }
}

export function isTourActive() { return active; }

function keydown(e) {
  if (!active) return;
  if (e.key === 'Escape') { e.preventDefault(); e.stopPropagation(); endTour(false); }
  else if ((e.key === 'ArrowRight' || e.key === 'Enter') && !busy) { e.preventDefault(); e.stopPropagation(); goNext(); }
  else if (e.key === 'ArrowLeft' && !busy && idx > 0) { e.preventDefault(); e.stopPropagation(); idx--; showStep(); }
}
