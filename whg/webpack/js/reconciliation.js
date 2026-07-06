// reconciliation.js
// Gazetteer Workbench — browser-based, local-first Reconciliation UI.
// STAFF-ONLY, UNPUBLISHED preview. See WorldHistoricalGazetteer/place#111 (spec), #112 (collaboration),
// and developer/plan-gazetteerWorkbench.prompt.md (build order).
//
// Phase 1: import a tabular file entirely in the browser, confirm/override a guessed role per column,
// and persist the whole project to IndexedDB so it survives reloads (resumability — a core invariant
// of the plan). Nothing is uploaded; later phases add the reconciliation queue against WHG's standard
// /reconcile service, candidate review, enrichment, and selective submission.

import '../css/reconciliation.css';

// Lazy-loaded chunks (proj4, Temporal) are served from Django's static dir. Set the webpack public
// path explicitly: this entry is loaded as a type="module" script, so document.currentScript is null
// and webpack's automatic publicPath detection can't find it — chunks would 404 without this.
// eslint-disable-next-line camelcase, no-undef
__webpack_public_path__ = '/static/webpack/';

// Heavy modules — proj4 (coordinate conversion) and the Temporal polyfill (date/calendar parsing) —
// are lazy-loaded on first use via dynamic import(), so the initial workbench bundle stays small and
// only pays for them when a coordinate or date column is actually present.
let Coords = null; // { COORD_FORMATS, detectCoordFormat, parseCoord, parseLatLonPair }
let Dates = null;  // { parseDate }
let ReconMap = null; // { renderReviewMap } — MapLibre map for the review pane
const loadCoords = async () => (Coords || (Coords = await import(/* webpackChunkName: "recon-coords" */ './recon-coords.js')));
const loadDates = async () => (Dates || (Dates = await import(/* webpackChunkName: "recon-dates" */ './recon-dates.js')));
const loadReconMap = async () => (ReconMap || (ReconMap = await import(/* webpackChunkName: "recon-map" */ './recon-map.js')));
let Symphonym = null; // in-browser phonetic encoder (Phase 7) — clusters variant spellings locally
const loadSymphonym = async () => (Symphonym || (Symphonym = await import(/* webpackChunkName: "recon-symphonym" */ './recon-symphonym.js')));

const PREVIEW_ROWS = 20;
const RECON_ENDPOINT = '/reconcile';   // WHG standard OpenRefine reconciliation service (same-origin)
const RECON_BATCH = 25;                // queries per POST (service cap is 50)
const RECON_CAND_LIMIT = 10;           // candidates requested per query initially ('load more' fetches extra)
// Candidate palette — shared with recon-map.js so the list number badges match the map markers.
const RECON_COLORS = ['#1565c0', '#c2410c', '#2e7d32', '#6a1b9a', '#00838f', '#b26a00', '#455a64', '#c2185b', '#5d4037'];
const RECON_RESULTS_PREVIEW = 200;     // rows shown in the results table (summary counts cover all)
const DB_NAME = 'whg-recon-workbench';
const DB_VERSION = 1;
const STORE = 'project';
const CURRENT = 'current';

// Roles a column can play — used now as preview hints and later as reconciliation query constraints.
const ROLES = [
  ['name', 'Place name'],
  ['county', 'County / region'],
  ['country', 'Country / ccode'],
  ['type', 'Feature type'],
  ['lat', 'Latitude'],
  ['lon', 'Longitude'],
  ['coords', 'Coordinates / grid ref'],
  ['date', 'Date / year'],
  ['id', 'Identifier'],
  ['other', 'Other (ignore)'],
];

// Guess a column's role from its name — conservative synonym/regex hints; the user confirms/overrides.
const ROLE_HINTS = [
  ['name', /^(place|placename|name|toponym|title|label)s?$/i],
  ['county', /^(county|adm2|admin2|region|parish|province|state|district)$/i],
  ['country', /^(country|ccode|iso|nation)$/i],
  ['type', /^(type|feature.?type|fclass|category|placetype|kind)$/i],
  ['lat', /^(lat|latitude|y)$/i],
  ['lon', /^(lon|lng|long|longitude|x)$/i],
  ['coords', /coord|geometry|geom|wkt|gridref|grid.?ref|osgb|national.?grid|easting|northing/i],
  ['date', /^(date|year|start|end|from|to|period|century)$/i],
  ['id', /^(id|uid|key|identifier|wikidata|qid|geonames|gn.?id)$/i],
];

function detectRole(columnName) {
  const n = String(columnName || '').trim();
  for (const [role, re] of ROLE_HINTS) if (re.test(n)) return role;
  return 'other';
}

// ── IndexedDB (tiny promise wrapper; no dependency) ─────────────────────────
function openDB() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, DB_VERSION);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains(STORE)) db.createObjectStore(STORE, { keyPath: 'id' });
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}
function idbRun(mode, fn) {
  return openDB().then((db) => new Promise((resolve, reject) => {
    const tx = db.transaction(STORE, mode);
    const req = fn(tx.objectStore(STORE));
    tx.oncomplete = () => resolve(req && req.result);
    tx.onerror = () => reject(tx.error);
    tx.onabort = () => reject(tx.error);
  }));
}
const putProject = (p) => idbRun('readwrite', (s) => s.put(p));
const getProject = () => idbRun('readonly', (s) => s.get(CURRENT));
const deleteProject = () => idbRun('readwrite', (s) => s.delete(CURRENT));

// ── Parsing (in-browser; a Web-Worker streaming parser is a later enhancement) ──
function parseDelimited(text, delimiter) {
  const rows = [];
  let field = '', row = [], inQuotes = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (inQuotes) {
      if (c === '"') { if (text[i + 1] === '"') { field += '"'; i++; } else { inQuotes = false; } }
      else { field += c; }
    } else if (c === '"') { inQuotes = true; }
    else if (c === delimiter) { row.push(field); field = ''; }
    else if (c === '\n') { row.push(field); field = ''; rows.push(row); row = []; }
    else if (c === '\r') { /* handled by \n */ }
    else { field += c; }
  }
  if (field.length > 0 || row.length > 0) { row.push(field); rows.push(row); }
  return rows.filter((r) => r.length > 1 || (r.length === 1 && r[0] !== ''));
}
function guessDelimiter(sample) {
  const firstLine = sample.split('\n')[0] || '';
  return (firstLine.match(/\t/g) || []).length > (firstLine.match(/,/g) || []).length ? '\t' : ',';
}
function fromJSON(data) {
  const records = Array.isArray(data) ? data : (Array.isArray(data.features) ? data.features : null);
  if (!records || !records.length) throw new Error('Expected a non-empty JSON array of records.');
  const flat = records.map((rec) => {
    if (rec && typeof rec === 'object' && rec.fields && typeof rec.fields === 'object') {
      return Object.assign({ id: rec.id }, rec.fields);          // {id, fields:{...}} (WHG examples)
    }
    if (rec && rec.properties && typeof rec.properties === 'object') return rec.properties; // GeoJSON-ish
    return rec;
  });
  const columns = [], seen = new Set();
  flat.forEach((r) => Object.keys(r || {}).forEach((k) => { if (!seen.has(k)) { seen.add(k); columns.push(k); } }));
  const rows = flat.map((r) => columns.map((c) => {
    const v = r ? r[c] : '';
    return v == null ? '' : (typeof v === 'object' ? JSON.stringify(v) : String(v));
  }));
  return { columns, rows, total: rows.length };
}
function fromDelimited(text) {
  const delimiter = guessDelimiter(text.slice(0, 4096));
  const matrix = parseDelimited(text, delimiter);
  if (!matrix.length) throw new Error('No rows found.');
  const columns = matrix[0].map((h, i) => (h && h.trim()) || `column_${i + 1}`);
  return { columns, rows: matrix.slice(1), total: matrix.length - 1, delimiter };
}

// ── State + DOM helpers ─────────────────────────────────────────────────────
let project = null; // { id, fileName, importedAt, columns:[{name,role}], rows:[[...]], total, delimiter? }

function el(id) { return document.getElementById(id); }
function esc(v) {
  return String(v == null ? '' : v).replace(/[&<>"']/g, (c) =>
    ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}
// Truncate then HTML-escape — every value rendered via innerHTML passes through here.
function truncate(v, max = 80) {
  const raw = String(v == null ? '' : v);
  return esc(raw.length > max ? raw.slice(0, max - 1) + '…' : raw);
}
function firstSample(colIndex) {
  if (!project) return '';
  for (const r of project.rows) { if (r[colIndex] != null && r[colIndex] !== '') return r[colIndex]; }
  return '';
}
function fmtTime(iso) { try { return new Date(iso).toLocaleString(); } catch (_) { return iso; } }

function flashSaved(msg) {
  const s = el('recon-saved');
  if (!s) return;
  s.innerHTML = `<i class="fas fa-check me-1"></i>${msg}`;
  s.classList.add('recon-saved--show');
}

async function persist() {
  if (!project) return;
  try { await putProject(project); flashSaved(`Saved locally · ${new Date().toLocaleTimeString()}`); }
  catch (err) { console.error('[recon] persist failed', err); flashSaved('⚠ could not save locally'); }
}

// ── Rendering ───────────────────────────────────────────────────────────────
function roleSelectHTML(colIndex, role) {
  const opts = ROLES.map(([val, label]) =>
    `<option value="${val}"${val === role ? ' selected' : ''}>${label}</option>`).join('');
  return `<select class="form-select form-select-sm recon-role-select role-${role}" data-col="${colIndex}">${opts}</select>`;
}

// ── Coordinate-format detection panel ───────────────────────────────────────
function coordColumnSamples(idx) {
  const out = [];
  for (const r of project.rows) {
    const v = r[idx];
    if (v != null && String(v).trim() !== '') out.push(v);
    if (out.length >= 200) break;
  }
  return out;
}
function coordCoverage(format, idx) {
  let parsed = 0, sample = null;
  const cap = Math.min(project.rows.length, 500);
  for (let i = 0; i < cap; i++) {
    const c = Coords.parseCoord(format, project.rows[i][idx]);
    if (c) { parsed++; if (!sample) sample = { raw: project.rows[i][idx], c }; }
  }
  return { parsed, checked: cap, sample };
}
function pairCoverage(latIdx, lonIdx, swapped) {
  let parsed = 0, sample = null;
  const cap = Math.min(project.rows.length, 500);
  for (let i = 0; i < cap; i++) {
    const c = Coords.parseLatLonPair(project.rows[i][latIdx], project.rows[i][lonIdx], swapped);
    if (c) { parsed++; if (!sample) sample = { raw: `${project.rows[i][latIdx]}, ${project.rows[i][lonIdx]}`, c }; }
  }
  return { parsed, checked: cap, sample };
}
function sampleHtml(cov, elseMsg) {
  return cov.sample
    ? `e.g. <code>${truncate(cov.sample.raw, 30)}</code> → <strong>${cov.sample.c.lat.toFixed(5)}, ${cov.sample.c.lon.toFixed(5)}</strong>`
    : `<span class="text-warning">${elseMsg}</span>`;
}

async function renderCoords() {
  const box = el('recon-coords');
  const coordsIdx = colIndexByRole('coords');
  const latIdx = colIndexByRole('lat');
  const lonIdx = colIndexByRole('lon');
  if (coordsIdx < 0 && !(latIdx >= 0 && lonIdx >= 0)) { box.classList.add('d-none'); box.innerHTML = ''; return; }
  await loadCoords(); // lazy-load proj4 on first use

  let head = '';
  if (coordsIdx >= 0) {
    const detection = Coords.detectCoordFormat(coordColumnSamples(coordsIdx));
    const chosen = project.coordFormat || detection.format;
    const cov = coordCoverage(chosen, coordsIdx);
    const opts = Coords.COORD_FORMATS.map(([id, l]) => `<option value="${id}"${id === chosen ? ' selected' : ''}>${esc(l)}</option>`).join('');
    head =
      `<div class="d-flex align-items-center flex-wrap gap-2">
         <i class="fas fa-map-location-dot text-secondary"></i>
         <span>Coordinate column detected as</span>
         <select id="recon-coord-format" class="form-select form-select-sm" style="width:auto">${opts}</select>
         ${detection.ambiguous ? '<span class="badge bg-warning text-dark">lat/lon order ambiguous — check the sample and change if wrong</span>' : ''}
       </div>
       <div class="small text-muted mt-1">Sample: <strong>${cov.parsed.toLocaleString()}</strong> of ${cov.checked.toLocaleString()} → WGS84 · ${sampleHtml(cov, 'no values parsed with this format — pick another')}</div>`;
  } else if (latIdx >= 0 && lonIdx >= 0) {
    const swapped = !!project.coordSwap;
    const cov = pairCoverage(latIdx, lonIdx, swapped);
    head =
      `<div class="d-flex align-items-center flex-wrap gap-2">
         <i class="fas fa-map-location-dot text-secondary"></i>
         <span>Latitude + Longitude columns (decimal degrees)</span>
         <label class="small mb-0"><input type="checkbox" id="recon-coord-swap"${swapped ? ' checked' : ''}> swap lat/lon</label>
       </div>
       <div class="small text-muted mt-1">Sample: <strong>${cov.parsed.toLocaleString()}</strong> of ${cov.checked.toLocaleString()} · ${sampleHtml(cov, 'no valid decimal pairs — try swapping lat/lon')}</div>`;
  } else {
    box.classList.add('d-none');
    box.innerHTML = '';
    return;
  }

  box.innerHTML =
    `<div class="recon-coords-inner">
       ${head}
       <div class="mt-2">
         <button type="button" id="recon-coord-checkall" class="btn btn-sm btn-outline-secondary">
           <i class="fas fa-list-check me-1"></i>Validate all ${project.total.toLocaleString()} rows
         </button>
       </div>
       <div id="recon-coord-report" class="recon-coord-report mt-2"></div>
     </div>`;
  box.classList.remove('d-none');

  const sel = el('recon-coord-format');
  if (sel) sel.addEventListener('change', () => { project.coordFormat = sel.value; persist(); renderCoords(); });
  const sw = el('recon-coord-swap');
  if (sw) sw.addEventListener('change', () => { project.coordSwap = sw.checked; persist(); renderCoords(); });
  const chk = el('recon-coord-checkall');
  if (chk) chk.addEventListener('click', checkAllCoords);
}

function currentCoordFormat() {
  const sel = el('recon-coord-format');
  if (sel) return sel.value;
  const coordsIdx = colIndexByRole('coords');
  if (coordsIdx >= 0) return project.coordFormat || Coords.detectCoordFormat(coordColumnSamples(coordsIdx)).format;
  return null;
}

// Validate EVERY row against the chosen coordinate format and report the ones that cannot convert.
async function checkAllCoords() {
  await loadCoords();
  const coordsIdx = colIndexByRole('coords');
  const latIdx = colIndexByRole('lat');
  const lonIdx = colIndexByRole('lon');
  const single = coordsIdx >= 0;
  const fmt = single ? currentCoordFormat() : null;
  const swap = !!project.coordSwap;
  const blankStr = (v) => v == null || String(v).trim() === '';

  let valid = 0, blank = 0;
  const failures = [];
  const total = project.rows.length;
  for (let i = 0; i < total; i++) {
    const r = project.rows[i];
    let c, raw, isBlank;
    if (single) {
      raw = r[coordsIdx]; c = Coords.parseCoord(fmt, raw); isBlank = blankStr(raw);
    } else {
      raw = `${blankStr(r[latIdx]) ? '' : r[latIdx]}, ${blankStr(r[lonIdx]) ? '' : r[lonIdx]}`;
      c = Coords.parseLatLonPair(r[latIdx], r[lonIdx], swap);
      isBlank = blankStr(r[latIdx]) && blankStr(r[lonIdx]);
    }
    if (c) valid++;
    else if (isBlank) blank++;
    else failures.push({ row: i + 1, raw });
  }
  renderCoordReport({ valid, blank, failures, total });
}

function renderCoordReport(res) {
  const box = el('recon-coord-report');
  if (!box) return;
  const bad = res.failures.length;
  const allGood = bad === 0;
  let html =
    `<div class="${allGood ? 'text-success' : 'text-danger'}">` +
    `<i class="fas ${allGood ? 'fa-circle-check' : 'fa-triangle-exclamation'} me-1"></i>` +
    `<strong>${res.valid.toLocaleString()}</strong> of ${res.total.toLocaleString()} rows convert to valid WGS84` +
    (res.blank ? ` · <span class="text-muted">${res.blank.toLocaleString()} blank</span>` : '') +
    (bad ? ` · <strong>${bad.toLocaleString()} could not be converted</strong>` : ' — all good.') +
    `</div>`;
  if (bad) {
    const show = res.failures.slice(0, 100);
    html += `<div class="recon-coord-failures mt-1"><table class="table table-sm mb-1">` +
      `<thead><tr><th style="width:5rem">Row</th><th>Unconvertible value</th></tr></thead><tbody>` +
      show.map((f) => `<tr><td>${f.row}</td><td><code>${truncate(f.raw, 70)}</code></td></tr>`).join('') +
      `</tbody></table>` +
      (bad > show.length ? `<div class="small text-muted">…and ${(bad - show.length).toLocaleString()} more.</div>` : '') +
      `</div>`;
  }
  box.innerHTML = html;
}

// ── Date-parsing panel ──────────────────────────────────────────────────────
async function renderDates() {
  const box = el('recon-dates');
  const idx = colIndexByRole('date');
  if (idx < 0) { box.classList.add('d-none'); box.innerHTML = ''; return; }
  await loadDates(); // lazy-load the date/Temporal parser on first use

  let sample = null, parsed = 0, checked = 0;
  const cap = Math.min(project.rows.length, 500);
  for (let i = 0; i < cap; i++) {
    const v = project.rows[i][idx];
    if (v == null || String(v).trim() === '') continue;
    checked++;
    const r = Dates.parseDate(v, { locale: 'uk' });
    if (r) { parsed++; if (!sample) sample = { raw: v, r }; }
  }
  const sh = sample
    ? `e.g. <code>${truncate(sample.raw, 34)}</code> → <strong>${sample.r.startISO || '…'} … ${sample.r.endISO || '…'}</strong>` +
      (sample.r.calendar ? ` <span class="badge bg-info text-dark">${truncate(sample.r.calendar, 32)}</span>` : '') +
      (sample.r.approximate ? ' <span class="badge bg-secondary">approx</span>' : '')
    : '<span class="text-warning">no values parsed — check the column</span>';

  box.innerHTML =
    `<div class="recon-coords-inner">
       <div class="d-flex align-items-center flex-wrap gap-2">
         <i class="fas fa-calendar-days text-secondary"></i>
         <span>Date column → ISO start/end · UK day/month · BCE/CE · regnal, feast &amp; global calendars
           (Hijri, Hebrew, Śaka, French Republican…)</span>
       </div>
       <div class="small text-muted mt-1">Sample: <strong>${parsed.toLocaleString()}</strong> of ${checked.toLocaleString()} parsed · ${sh}</div>
       <div class="mt-2">
         <button type="button" id="recon-date-checkall" class="btn btn-sm btn-outline-secondary">
           <i class="fas fa-list-check me-1"></i>Validate all ${project.total.toLocaleString()} rows
         </button>
       </div>
       <div id="recon-date-report" class="recon-coord-report mt-2"></div>
     </div>`;
  box.classList.remove('d-none');
  const chk = el('recon-date-checkall');
  if (chk) chk.addEventListener('click', checkAllDates);
}

// Validate EVERY row's date and report the ones that cannot be parsed.
async function checkAllDates() {
  await loadDates();
  const idx = colIndexByRole('date');
  if (idx < 0) return;
  let valid = 0, blank = 0, ambiguous = 0;
  const failures = [];
  const total = project.rows.length;
  for (let i = 0; i < total; i++) {
    const v = project.rows[i][idx];
    if (v == null || String(v).trim() === '') { blank++; continue; }
    const r = Dates.parseDate(v, { locale: 'uk' });
    if (r) { valid++; if (r.ambiguous) ambiguous++; }
    else failures.push({ row: i + 1, raw: v });
  }
  renderDateReport({ valid, blank, ambiguous, failures, total });
}

function renderDateReport(res) {
  const box = el('recon-date-report');
  if (!box) return;
  const bad = res.failures.length;
  const good = bad === 0;
  let html =
    `<div class="${good ? 'text-success' : 'text-danger'}">` +
    `<i class="fas ${good ? 'fa-circle-check' : 'fa-triangle-exclamation'} me-1"></i>` +
    `<strong>${res.valid.toLocaleString()}</strong> of ${res.total.toLocaleString()} rows parsed to ISO dates` +
    (res.blank ? ` · <span class="text-muted">${res.blank.toLocaleString()} blank</span>` : '') +
    (res.ambiguous ? ` · <span class="text-muted">${res.ambiguous.toLocaleString()} ambiguous day/month (assumed UK)</span>` : '') +
    (bad ? ` · <strong>${bad.toLocaleString()} could not be parsed</strong>` : ' — all good.') +
    `</div>`;
  if (bad) {
    const show = res.failures.slice(0, 100);
    html += `<div class="recon-coord-failures mt-1"><table class="table table-sm mb-1">` +
      `<thead><tr><th style="width:5rem">Row</th><th>Unparseable value</th></tr></thead><tbody>` +
      show.map((f) => `<tr><td>${f.row}</td><td><code>${truncate(f.raw, 70)}</code></td></tr>`).join('') +
      `</tbody></table>` +
      (bad > show.length ? `<div class="small text-muted">…and ${(bad - show.length).toLocaleString()} more.</div>` : '') +
      `</div>`;
  }
  box.innerHTML = html;
}

function renderMapping() {
  el('recon-map-body').innerHTML = project.columns.map((c, i) =>
    `<tr>
       <td class="recon-map-col">${truncate(c.name, 50)}</td>
       <td>${roleSelectHTML(i, c.role)}</td>
       <td class="text-muted">${truncate(firstSample(i), 60)}</td>
     </tr>`).join('');

  el('recon-map-body').querySelectorAll('.recon-role-select').forEach((sel) => {
    sel.addEventListener('change', () => {
      const i = Number(sel.dataset.col);
      project.columns[i].role = sel.value;
      sel.className = `form-select form-select-sm recon-role-select role-${sel.value}`;
      persist();
      renderPreview();       // 'other' (ignore) columns are hidden in the preview
      renderCoords();        // coords/lat/lon mapping affects the coordinate panel
      renderDates();         // date mapping affects the date panel
      refreshReconSection(); // name/country mapping affects what can be reconciled
    });
  });
}

function renderPreview() {
  const showIgnored = !!project.showIgnored;
  const box = el('recon-show-ignored'); if (box) box.checked = showIgnored;
  // Columns mapped to role 'other' (ignore) are hidden by default; the toggle reveals them.
  const vis = project.columns.map((c, i) => i).filter((i) => showIgnored || project.columns[i].role !== 'other');
  el('recon-preview-head').innerHTML = '<tr>' + vis.map((i) => `<th>${truncate(project.columns[i].name, 40)}</th>`).join('') + '</tr>';
  el('recon-preview-body').innerHTML = project.rows.slice(0, PREVIEW_ROWS).map((r) =>
    '<tr>' + vis.map((i) => `<td>${truncate(r[i])}</td>`).join('') + '</tr>').join('');
}

function renderAll() {
  el('recon-result').classList.remove('d-none');
  const delimNote = project.delimiter ? ` · delimiter <code>${project.delimiter === '\t' ? 'TAB' : project.delimiter}</code>` : ' · JSON';
  el('recon-summary').innerHTML =
    `<strong>${truncate(project.fileName, 60)}</strong> — <strong>${project.total.toLocaleString()}</strong> ` +
    `row${project.total === 1 ? '' : 's'} · <strong>${project.columns.length}</strong> ` +
    `column${project.columns.length === 1 ? '' : 's'}${delimNote} · imported ${fmtTime(project.importedAt)}.`;
  renderMapping();
  renderCoords();
  renderDates();
  renderPreview();
  refreshReconSection();
  renderColSwitcher();
  refreshReview();  // show pane 4's header (with a "reconcile first" placeholder) even before matches
  refreshFullMapPane();
  refreshExport();
  renderLangControl(); // populate the phonetic-matching language selector (default detected from data)
  updatePaneSummaries();
  openPane('recon-result'); // once a dataset is loaded, focus the mapping step (accordion: others close)
}

// Strict accordion — open one pane, collapse all the others (keeps their headers + summaries visible).
function openPane(id) {
  document.querySelectorAll('.recon-pane').forEach((p) => p.classList.toggle('recon-collapsed', p.id !== id));
  // The review map is created inside this (previously collapsed) pane, so its MapLibre container was
  // 0×0 at init. Now that the pane is visible, tell the map to resize — otherwise it renders blank.
  if (id === 'recon-review' && ReconMap && ReconMap.resizeReviewMap) {
    requestAnimationFrame(() => ReconMap.resizeReviewMap());
  }
  // The full-dataset map is built lazily — only when this pane is opened.
  if (id === 'recon-fullmap-pane') requestAnimationFrame(() => updateFullMap());
}
function updatePaneSummaries() {
  if (!project) return;
  const sr = el('recon-pane-sum-result');
  if (sr) sr.textContent = project.fileName ? `${project.fileName} · ${project.total.toLocaleString()} rows · ${project.columns.length} cols` : '';
  const sc = el('recon-pane-sum-recon');
  if (sc) {
    if (project.matches && Object.keys(project.matches).length) {
      const built = buildUniqueQueries();
      let matched = 0, nomatch = 0, pending = 0;
      if (built) built.map.forEach((v, key) => { const m = project.matches[key]; if (!m) pending++; else if (m.top) matched++; else nomatch++; });
      sc.textContent = `${matched.toLocaleString()} matched · ${nomatch.toLocaleString()} no match · ${pending.toLocaleString()} pending`;
    } else sc.textContent = '';
  }
}

function showResume() {
  const banner = el('recon-resume');
  if (!banner) return;
  el('recon-resume-text').innerHTML =
    `Resumed your saved dataset — <strong>${truncate(project.fileName, 50)}</strong> ` +
    `(${project.total.toLocaleString()} rows, imported ${fmtTime(project.importedAt)}). It stays in this browser.`;
  banner.classList.remove('d-none');
}

function resetUI() {
  project = null;
  stopRequested = true;
  reviewMeta = []; reviewPos = 0;
  document.querySelectorAll('.recon-pane').forEach((p) => p.classList.remove('recon-collapsed')); // expand import again
  el('recon-result').classList.add('d-none');
  el('recon-resume').classList.add('d-none');
  el('recon-recon').classList.add('d-none');
  el('recon-progress-wrap').classList.add('d-none');
  el('recon-results-wrap').classList.add('d-none');
  el('recon-review').classList.add('d-none');
  el('recon-review-map').classList.add('d-none');
  el('recon-fullmap-pane').classList.add('d-none');
  el('recon-export').classList.add('d-none');
  ['recon-map-body', 'recon-preview-head', 'recon-preview-body', 'recon-summary', 'recon-saved',
    'recon-coords', 'recon-dates', 'recon-results-body', 'recon-recon-summary', 'recon-progress-text',
    'recon-review-card', 'recon-review-progress'].forEach((id) => {
    const n = el(id); if (n) n.innerHTML = '';
  });
  const input = el('recon-file'); if (input) input.value = '';
}

// ── Import + lifecycle ──────────────────────────────────────────────────────
function handleFile(file) {
  const reader = new FileReader();
  reader.onload = async () => {
    try {
      const text = String(reader.result);
      // A .whgproj backup dropped/chosen here → restore the whole project instead of parsing as data.
      if (/\.whgproj$/i.test(file.name) || (text.trim().startsWith('{') && text.indexOf('"_whgproj"') >= 0)) {
        const parsed = JSON.parse(text);
        const p = parsed && parsed._whgproj ? parsed.project : parsed;
        if (!p || !Array.isArray(p.columns) || !Array.isArray(p.rows)) throw new Error('Not a valid .whgproj backup.');
        p.id = CURRENT; project = p; reviewMeta = []; reviewPos = 0;
        el('recon-resume').classList.add('d-none');
        renderAll();
        if (navigator.storage && navigator.storage.persist) { try { await navigator.storage.persist(); } catch (_) { /* */ } }
        await persist();
        console.log(`[recon] restored .whgproj: ${project.total} rows`);
        return;
      }
      const isJSON = /\.json$/i.test(file.name) || text.trim().startsWith('[') || text.trim().startsWith('{');
      const parsed = isJSON ? fromJSON(JSON.parse(text)) : fromDelimited(text);
      project = {
        id: CURRENT,
        fileName: file.name,
        importedAt: new Date().toISOString(),
        columns: parsed.columns.map((name) => ({ name, role: detectRole(name) })),
        rows: parsed.rows,
        total: parsed.total,
        delimiter: parsed.delimiter || null,
      };
      el('recon-resume').classList.add('d-none'); // fresh import, not a resume
      console.log(`[recon] parsed "${file.name}" locally: ${project.total} rows, ${project.columns.length} cols`);
      renderAll();
      if (navigator.storage && navigator.storage.persist) {
        try { await navigator.storage.persist(); } catch (_) { /* best effort */ }
      }
      await persist();
    } catch (err) {
      console.error('[recon] parse failed:', err);
      el('recon-result').classList.remove('d-none');
      el('recon-summary').innerHTML =
        `<span class="text-danger"><i class="fas fa-exclamation-triangle me-1"></i>` +
        `Could not parse <strong>${truncate(file.name, 60)}</strong>: ${err.message}</span>`;
      el('recon-map-body').innerHTML = '';
      el('recon-preview-head').innerHTML = '';
      el('recon-preview-body').innerHTML = '';
    }
  };
  reader.onerror = () => console.error('[recon] file read error', reader.error);
  reader.readAsText(file);
}

async function clearData() {
  try { await deleteProject(); } catch (err) { console.error('[recon] clear failed', err); }
  resetUI();
  console.log('[recon] local project cleared from IndexedDB');
}

// Phase 2 — downloadable .whgproj backup (the whole project: data, mapping, matches, decisions).
function downloadBackup() {
  if (!project) return;
  const blob = new Blob([JSON.stringify({ _whgproj: 1, savedAt: new Date().toISOString(), project })],
    { type: 'application/json' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = (project.fileName ? project.fileName.replace(/\.[^.]+$/, '') : 'workbench') + '.whgproj';
  document.body.appendChild(a); a.click(); a.remove();
  setTimeout(() => URL.revokeObjectURL(a.href), 2000);
  flashSaved('Backup saved');
}
// (Restoring a .whgproj is handled by handleFile — the same dropzone / choose-file control.)

// ── Phase 5: Enrich & export ────────────────────────────────────────────────
// Produce an augmented copy of the table for use in other software: the original columns plus,
// optionally, converted WGS84 coordinates, ISO start/end dates, the confirmed WHG match, and richer
// details enriched from WHG. All computed here in the browser — nothing is uploaded.

function hasCoordRole() {
  return colIndexByRole('coords') >= 0 || (colIndexByRole('lat') >= 0 && colIndexByRole('lon') >= 0);
}
// Resolve a row's coordinate to WGS84 without touching the DOM (works whether or not the pane is open).
function rowCoordValue(i) {
  const coordsIdx = colIndexByRole('coords'), latIdx = colIndexByRole('lat'), lonIdx = colIndexByRole('lon');
  try {
    if (coordsIdx >= 0) {
      const fmt = project.coordFormat || Coords.detectCoordFormat(coordColumnSamples(coordsIdx)).format;
      return Coords.parseCoord(fmt, project.rows[i][coordsIdx]);
    }
    if (latIdx >= 0 && lonIdx >= 0) {
      return Coords.parseLatLonPair(project.rows[i][latIdx], project.rows[i][lonIdx], !!project.coordSwap);
    }
  } catch (_) { /* ignore a single bad cell */ }
  return null;
}

function currentExportOptions() {
  const fmtEl = document.querySelector('input[name="recon-exp-fmt"]:checked');
  return {
    coords: !!(el('recon-exp-coords') && el('recon-exp-coords').checked) && hasCoordRole(),
    dates: !!(el('recon-exp-dates') && el('recon-exp-dates').checked) && colIndexByRole('date') >= 0,
    match: !!(el('recon-exp-match') && el('recon-exp-match').checked),
    enrich: !!(el('recon-exp-enrich') && el('recon-exp-enrich').checked),
    format: fmtEl ? fmtEl.value : 'csv',
  };
}

// Assemble a per-row augmented record set. Returns { origHeaders, augHeaders, records } where each
// record is { orig:[cellValues], aug:{header:value}, coord:{lat,lon}|null, whenStart, whenEnd, match }.
async function buildExportRecords(opts, onProgress) {
  const nameCol = colIndexByRole('name');
  const built = buildUniqueQueries(nameCol); // export the NAME column's match as the primary whg_match_*
  const decisions = project.decisions || {};
  const matches = project.matches || {};
  // Parent columns reconciled ahead of the name (County, Parish, …) get their own match columns.
  const adminCols = reconChain().filter((c) => c !== nameCol);
  const colSlug = (i) => String(project.columns[i].name).trim().replace(/\W+/g, '_').toLowerCase().replace(/^_|_$/g, '') || ('col' + i);
  // Load the coord parser whenever a coordinate column exists — even if the WGS84 columns aren't
  // requested — so LPF/LP-TSV geometry and geometry-override centroids can be computed.
  if (opts.coords || hasCoordRole()) await loadCoords();
  if (opts.dates) await loadDates();

  const dateIdx = colIndexByRole('date');
  const augHeaders = [];
  if (opts.coords) augHeaders.push('wgs84_lat', 'wgs84_lon');
  if (opts.dates) augHeaders.push('date_start', 'date_end');
  if (opts.match) {
    augHeaders.push('whg_match_id', 'whg_match_title', 'whg_match_score', 'whg_match_source');
    adminCols.forEach((c) => augHeaders.push(`${colSlug(c)}_whg_id`, `${colSlug(c)}_whg_title`)); // parent containment matches
  }
  if (opts.enrich) augHeaders.push('whg_match_lon', 'whg_match_lat', 'whg_match_variants', 'whg_match_description', 'whg_match_types', 'whg_wikipedia');

  // Pre-fetch coordinates for accepted matches when enriching (reuses the review-pane cache).
  if (opts.enrich) {
    const ids = [];
    if (built) built.map.forEach((v, key) => { acceptedList(decisions[key]).forEach((a) => { if (a.place_id && !(a.place_id in _candCoord)) ids.push(a.place_id); }); });
    for (let k = 0; k < ids.length; k++) { await fetchCandidateCoord(ids[k]); if (onProgress) onProgress(`enriching ${k + 1} / ${ids.length}…`); }
  }

  const records = [];
  for (let i = 0; i < project.rows.length; i++) {
    const orig = project.rows[i].map((v) => (v == null ? '' : v));
    const aug = {};
    let whenStart = '', whenEnd = '', match = null;
    const info = built && keyForRow(built, i);

    // A geometry override (cloned from a match or drawn on the map) wins over the dataset coordinate.
    const ov = (project.geom && info && project.geom[info.key]) || null;
    const geom = ov ? ov.geometry : null;
    const coord = geom ? firstLngLat(geom) : (hasCoordRole() ? rowCoordValue(i) : null);

    if (opts.coords) {
      aug.wgs84_lat = coord ? +coord.lat.toFixed(6) : '';
      aug.wgs84_lon = coord ? +coord.lon.toFixed(6) : '';
    }
    if (opts.dates) {
      const raw = project.rows[i][dateIdx];
      const d = (raw != null && String(raw).trim() !== '') ? Dates.parseDate(raw, { locale: 'uk' }) : null;
      whenStart = (d && d.startISO) || '';
      whenEnd = (d && d.endISO) || '';
      aug.date_start = whenStart;
      aug.date_end = whenEnd;
    }
    if (opts.match || opts.enrich) {
      const dec = info && decisions[info.key];
      const accepted = acceptedList(dec);
      if (accepted.length) {
        const cands = (matches[info.key] && matches[info.key].candidates) || [];
        match = { list: accepted.map((a) => ({ id: a.place_id, title: a.label, score: a.score, source: nsName(a.place_id), cand: cands[a.ci] || null })) };
        match.first = match.list[0];
      }
    }
    if (opts.match) {
      aug.whg_match_id = match ? match.list.map((x) => x.id).join('; ') : '';
      aug.whg_match_title = match ? match.list.map((x) => x.title).join('; ') : '';
      aug.whg_match_score = match ? match.list.map((x) => x.score).join('; ') : '';
      aug.whg_match_source = match ? [...new Set(match.list.map((x) => x.source))].join('; ') : '';
      // Parent-column (containment) matches: accepted, else the auto-confirmed top.
      adminCols.forEach((c) => {
        const key = c + ':' + i;
        const a = acceptedList(decisions[key])[0];
        let id = a && a.place_id, title = a && a.label;
        if (!id) { const m = matches[key]; if (m && m.top && isAutoConfirmed(m.top, getThreshold())) { id = m.top.id; title = m.top.name; } }
        aug[`${colSlug(c)}_whg_id`] = id || '';
        aug[`${colSlug(c)}_whg_title`] = title || '';
      });
    }
    if (opts.enrich) {
      const f = match && match.first;
      const mc = f && (_candCoord[f.id] || null);
      aug.whg_match_lon = mc ? +mc.lon.toFixed(6) : '';
      aug.whg_match_lat = mc ? +mc.lat.toFixed(6) : '';
      aug.whg_match_variants = (f && f.cand && (f.cand.alt_names || [])).join('; ') || '';
      aug.whg_match_description = (f && f.cand && f.cand.description) || '';
      aug.whg_match_types = (f && f.cand && (f.cand.type || []).map((t) => (t && (t.name || t.id)) || t).join('; ')) || '';
      // Wikipedia article URLs (from Wikidata sitelinks surfaced by /reconcile) across accepted matches.
      aug.whg_wikipedia = match ? [...new Set(match.list.flatMap((x) => (x.cand && x.cand.wikipedia || []).map((w) => w.url)))].join('; ') : '';
    }
    records.push({ orig, aug, coord, geom, whenStart, whenEnd, match });
  }
  return { origHeaders: project.columns.map((c) => c.name), augHeaders, records };
}

// Minimal GeoJSON-geometry → WKT (Point / LineString / Polygon) for the LP-TSV geowkt column.
function geojsonToWKT(g) {
  if (!g) return '';
  const pair = (c) => `${+(+c[0]).toFixed(6)} ${+(+c[1]).toFixed(6)}`;
  const ring = (r) => r.map(pair).join(', ');
  const poly = (p) => p.map((r) => `(${ring(r)})`).join(', ');
  switch (g.type) {
    case 'Point': return `POINT (${pair(g.coordinates)})`;
    case 'LineString': return `LINESTRING (${ring(g.coordinates)})`;
    case 'Polygon': return `POLYGON (${poly(g.coordinates)})`;
    case 'MultiPoint': return `MULTIPOINT (${ring(g.coordinates)})`;
    case 'MultiLineString': return `MULTILINESTRING (${g.coordinates.map((l) => `(${ring(l)})`).join(', ')})`;
    case 'MultiPolygon': return `MULTIPOLYGON (${g.coordinates.map((p) => `(${poly(p)})`).join(', ')})`;
    default: return '';
  }
}

function csvCell(v) {
  const s = String(v == null ? '' : v);
  return /[",\r\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
}
function serializeCSV(data) {
  const headers = data.origHeaders.concat(data.augHeaders);
  const lines = [headers.map(csvCell).join(',')];
  for (const rec of data.records) lines.push(rec.orig.concat(data.augHeaders.map((h) => rec.aug[h])).map(csvCell).join(','));
  return lines.join('\r\n');
}
function serializeJSON(data) {
  const out = data.records.map((rec) => {
    const o = {};
    data.origHeaders.forEach((h, j) => { o[h] = rec.orig[j] == null ? '' : rec.orig[j]; });
    data.augHeaders.forEach((h) => { o[h] = rec.aug[h]; });
    return o;
  });
  return JSON.stringify(out, null, 2);
}
// WHG LP-TSV (a subset of the canonical columns — enough as a starting point for contribution).
function serializeLPTSV(data) {
  const idIdx = colIndexByRole('id'), nameIdx = colIndexByRole('name');
  const countryIdx = colIndexByRole('country'), countyIdx = colIndexByRole('county');
  const cols = ['id', 'title', 'title_source', 'ccodes', 'start', 'end', 'lon', 'lat', 'geowkt', 'matches', 'parent_name', 'description'];
  const src = project.fileName || 'workbench';
  const lines = [cols.join('\t')];
  data.records.forEach((rec, i) => {
    const cell = (v) => String(v == null ? '' : v).replace(/[\t\r\n]/g, ' ');
    // A point geometry travels as lon/lat; lines and polygons travel as geowkt (WKT).
    const isPoint = !rec.geom || rec.geom.type === 'Point';
    const row = {
      id: idIdx >= 0 ? rec.orig[idIdx] : (i + 1),
      title: nameIdx >= 0 ? rec.orig[nameIdx] : '',
      title_source: src,
      ccodes: countryIdx >= 0 && isCcode(rec.orig[countryIdx]) ? String(rec.orig[countryIdx]).toUpperCase() : '',
      start: rec.whenStart, end: rec.whenEnd,
      lon: rec.coord ? +rec.coord.lon.toFixed(6) : '', lat: rec.coord ? +rec.coord.lat.toFixed(6) : '',
      geowkt: (rec.geom && !isPoint) ? geojsonToWKT(rec.geom) : '',
      matches: rec.match ? rec.match.list.map((x) => x.id).join(';') : '',
      parent_name: countyIdx >= 0 ? rec.orig[countyIdx] : '',
      description: rec.match ? 'closeMatch: ' + rec.match.list.map((x) => `${x.title} (${x.source})`).join('; ') : '',
    };
    lines.push(cols.map((c) => cell(row[c])).join('\t'));
  });
  return lines.join('\n');
}
// Linked Places Format (LPF) GeoJSON FeatureCollection.
function serializeLPF(data) {
  const idIdx = colIndexByRole('id'), nameIdx = colIndexByRole('name'), countryIdx = colIndexByRole('country');
  const features = data.records.map((rec, i) => {
    const title = nameIdx >= 0 ? String(rec.orig[nameIdx] || '') : '';
    const cc = countryIdx >= 0 && isCcode(rec.orig[countryIdx]) ? [String(rec.orig[countryIdx]).toUpperCase()] : [];
    const props = { title };
    if (cc.length) props.ccodes = cc;
    const feat = {
      '@id': String(idIdx >= 0 ? rec.orig[idIdx] : (i + 1)),
      type: 'Feature',
      properties: props,
      names: title ? [{ toponym: title }] : [],
    };
    if (rec.whenStart || rec.whenEnd) feat.when = { timespans: [{ start: { in: rec.whenStart || undefined }, end: { in: rec.whenEnd || undefined } }] };
    if (rec.geom) feat.geometry = rec.geom;                              // override (point / line / polygon) wins
    else if (rec.coord) feat.geometry = { type: 'Point', coordinates: [+rec.coord.lon.toFixed(6), +rec.coord.lat.toFixed(6)] };
    if (rec.match) feat.links = rec.match.list.map((x) => ({ type: 'closeMatch', identifier: x.id }));
    return feat;
  });
  return JSON.stringify({
    type: 'FeatureCollection',
    '@context': 'https://raw.githubusercontent.com/LinkedPasts/linked-places-format/master/linkedplaces-context-v1.1.jsonld',
    features,
  }, null, 2);
}

function downloadText(filename, text, mime) {
  const blob = new Blob([text], { type: mime + ';charset=utf-8' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = filename;
  document.body.appendChild(a); a.click(); a.remove();
  setTimeout(() => URL.revokeObjectURL(a.href), 2000);
}

async function runExport() {
  if (!project) return;
  const opts = currentExportOptions();
  const status = el('recon-export-status');
  const btn = el('recon-export-btn');
  if (btn) btn.disabled = true;
  if (status) status.textContent = 'preparing…';
  try {
    const data = await buildExportRecords(opts, (msg) => { if (status) status.textContent = msg; });
    const base = (project.fileName ? project.fileName.replace(/\.[^.]+$/, '') : 'workbench') + '-augmented';
    const FMT = {
      csv: [serializeCSV, 'csv', 'text/csv'],
      json: [serializeJSON, 'json', 'application/json'],
      lptsv: [serializeLPTSV, 'tsv', 'text/tab-separated-values'],
      lpf: [serializeLPF, 'lpf.geojson', 'application/geo+json'],
    };
    const [fn, ext, mime] = FMT[opts.format] || FMT.csv;
    downloadText(`${base}.${ext}`, fn(data), mime);
    if (status) status.textContent = `exported ${data.records.length.toLocaleString()} rows`;
  } catch (err) {
    console.error('[recon] export failed', err);
    if (status) status.textContent = 'export failed — see console';
  } finally {
    if (btn) btn.disabled = false;
  }
}

// One-click contribute: build the LPF in the background and submit it straight to WHG's dataset
// validation/creation pipeline (POST /datasets/validate/), reusing the whole existing flow — no
// manual export/upload. Lands the user on WHG's validation-progress page; the local project stays in
// the browser. (The same build-LPF-and-submit path will serve collaborative reconciliation, #112.)
async function contributeToWHG() {
  if (!project) return;
  const btn = el('recon-contribute-btn');
  const status = el('recon-contribute-status');
  if (btn) btn.disabled = true;
  if (status) status.textContent = 'building Linked Places file…';
  try {
    const data = await buildExportRecords(
      { coords: hasCoordRole(), dates: colIndexByRole('date') >= 0, match: true, enrich: false },
      (msg) => { if (status) status.textContent = msg; });
    const base = project.fileName ? project.fileName.replace(/\.[^.]+$/, '') : 'workbench';
    const file = new File([serializeLPF(data)], base + '.geojson', { type: 'application/geo+json' });
    const form = document.createElement('form');
    form.method = 'POST'; form.action = '/datasets/validate/'; form.enctype = 'multipart/form-data'; form.style.display = 'none';
    const addHidden = (name, value) => { const i = document.createElement('input'); i.type = 'hidden'; i.name = name; i.value = value; form.appendChild(i); };
    addHidden('csrfmiddlewaretoken', getCsrf());
    addHidden('title', truncate(project.fileName || base, 100));
    const fileInput = document.createElement('input'); fileInput.type = 'file'; fileInput.name = 'file';
    const dt = new DataTransfer(); dt.items.add(file); fileInput.files = dt.files; // programmatically attach the file
    form.appendChild(fileInput);
    document.body.appendChild(form);
    if (status) status.textContent = 'uploading to WHG…';
    form.submit(); // navigates to the validation/progress page; the local project remains in this browser
  } catch (err) {
    console.error('[recon] contribute failed', err);
    if (status) status.textContent = 'contribution failed — see console';
    if (btn) btn.disabled = false;
  }
}

// Show the export pane once a dataset is loaded; enable match/enrich only when there are matches.
function refreshExport() {
  const sec = el('recon-export');
  if (!sec || !project) return;
  const nameIdx = colIndexByRole('name');
  sec.classList.toggle('d-none', nameIdx < 0 && !hasCoordRole() && colIndexByRole('date') < 0);
  const hasMatches = !!(project.matches && Object.keys(project.matches).length);
  const cn = el('recon-exp-coords-note'); if (cn) cn.textContent = hasCoordRole() ? '' : '(no coordinate column)';
  const dn = el('recon-exp-dates-note'); if (dn) dn.textContent = colIndexByRole('date') >= 0 ? '' : '(no date column)';
  ['recon-exp-coords', 'recon-exp-dates', 'recon-exp-match', 'recon-exp-enrich'].forEach((id) => {
    const box = el(id); if (!box) return;
    if (id === 'recon-exp-coords') box.disabled = !hasCoordRole();
    if (id === 'recon-exp-dates') box.disabled = colIndexByRole('date') < 0;
    if (id === 'recon-exp-match' || id === 'recon-exp-enrich') box.disabled = !hasMatches;
  });
  const sum = el('recon-pane-sum-export');
  if (sum) {
    let accepted = 0;
    if (project.decisions) Object.values(project.decisions).forEach((d) => { if (d.status === 'accepted') accepted += 1; });
    sum.textContent = hasMatches ? `${accepted.toLocaleString()} confirmed match${accepted === 1 ? '' : 'es'}` : 'augmented columns ready';
  }
}

// ── Full-dataset map (pane between Review and Export) ────────────────────────
// Shown once a dataset has locatable rows. Built lazily (only when the pane is opened) to avoid the
// O(rows) work + coord parsing on large datasets until the user asks for it; a GPU circle layer with
// clustering (in recon-map.js) then handles tens of thousands of points client-side.
function refreshFullMapPane() {
  const sec = el('recon-fullmap-pane'); if (!sec || !project) return;
  const locatable = hasCoordRole() || (project.geom && Object.keys(project.geom).length > 0);
  sec.classList.toggle('d-none', !locatable);
}
async function updateFullMap() {
  const box = el('recon-fullmap'); if (!box || !project) return;
  const nameCol = colIndexByRole('name'), countyIdx = colIndexByRole('county'), dateIdx = colIndexByRole('date');
  const decisions = project.decisions || {}, matches = project.matches || {};
  if (hasCoordRole()) await loadCoords();
  const feats = [];
  for (let i = 0; i < project.rows.length; i++) {
    const ov = project.geom && project.geom[nameCol + ':' + i];
    const c = ov ? firstLngLat(ov.geometry) : (hasCoordRole() ? rowCoordValue(i) : null);
    if (!c) continue;
    const key = nameCol + ':' + i;
    const acc = acceptedList(decisions[key])[0]
      || (matches[key] && matches[key].top && isAutoConfirmed(matches[key].top, getThreshold()) ? { label: matches[key].top.name, score: matches[key].top.score } : null);
    const cell = (idx) => (idx >= 0 ? String(project.rows[i][idx] == null ? '' : project.rows[i][idx]) : '');
    feats.push({ type: 'Feature', geometry: { type: 'Point', coordinates: [+c.lon.toFixed(6), +c.lat.toFixed(6)] }, properties: {
      title: cell(nameCol), admin: cell(countyIdx), date: cell(dateIdx),
      match: acc ? acc.label : '', score: acc ? acc.score : '',
      coord: c.lat.toFixed(4) + ', ' + c.lon.toFixed(4),
    } });
  }
  const sum = el('recon-fullmap-sum');
  if (sum) sum.textContent = `${feats.length.toLocaleString()} located place${feats.length === 1 ? '' : 's'}` +
    (project.rows.length > feats.length ? ` · ${(project.rows.length - feats.length).toLocaleString()} without coordinates` : '');
  try { const mod = await loadReconMap(); mod.renderFullMap(box, { type: 'FeatureCollection', features: feats }); }
  catch (err) { console.error('[recon] full map failed', err); }
}

// ── Phonetic (vector) matching: language-conditioned Symphonym embeddings sent with reconcile ─────
// The in-browser Symphonym model embeds each row's name (int8, 128-d) and the vector rides along on
// the reconcile request, so the gateway ranks candidates by phonetic/vector similarity — offloading
// the server embed and letting the user set the query language.
function phoneticEnabled() {
  const box = el('recon-phonetic');
  if (box) return box.checked;
  return !(project && project.phonetic === false); // default on
}
// Languages offered in the override dropdown (value = Symphonym lang code; 'und' = undetermined).
const RECON_LANGS = [
  ['und', 'Undetermined'], ['en', 'English'], ['fr', 'French'], ['de', 'German'], ['es', 'Spanish'],
  ['it', 'Italian'], ['pt', 'Portuguese'], ['nl', 'Dutch'], ['ru', 'Russian'], ['pl', 'Polish'],
  ['ar', 'Arabic'], ['zh', 'Chinese'], ['ja', 'Japanese'], ['ko', 'Korean'], ['el', 'Greek'],
  ['he', 'Hebrew'], ['tr', 'Turkish'], ['hi', 'Hindi'], ['th', 'Thai'],
];
// Guess a default language from the dominant non-Latin script across the dataset's names; Latin → English.
function detectLang() {
  const nameIdx = colIndexByRole('name'); if (nameIdx < 0) return 'und';
  const RANGES = [['ru', 0x0400, 0x04FF], ['ar', 0x0600, 0x06FF], ['el', 0x0370, 0x03FF], ['he', 0x0590, 0x05FF],
    ['th', 0x0E00, 0x0E7F], ['hi', 0x0900, 0x097F], ['ja', 0x3040, 0x30FF], ['ko', 0xAC00, 0xD7AF], ['zh', 0x4E00, 0x9FFF]];
  const counts = {}; let latin = 0, total = 0;
  const cap = Math.min(project.rows.length, 300);
  for (let i = 0; i < cap; i++) {
    const v = project.rows[i][nameIdx]; if (v == null || v === '') continue;
    for (const ch of String(v)) {
      const cp = ch.codePointAt(0); total++;
      if ((cp >= 0x41 && cp <= 0x7A) || (cp >= 0xC0 && cp <= 0x24F)) { latin++; continue; }
      for (const [lg, lo, hi] of RANGES) if (cp >= lo && cp <= hi) { counts[lg] = (counts[lg] || 0) + 1; break; }
    }
  }
  let best = null, n = 0; for (const k in counts) if (counts[k] > n) { best = k; n = counts[k]; }
  if (best && n > (total - latin) * 0.5 && n > total * 0.15) return best; // a non-Latin script dominates
  return 'en';
}
function getLang() {
  const sel = el('recon-lang'); if (sel && sel.value) return sel.value;
  return (project && project.lang) || 'und';
}
function renderLangControl() {
  const sel = el('recon-lang'); if (!sel || !project) return;
  if (!project.lang) project.lang = detectLang(); // default from the data (once); user can override
  sel.innerHTML = RECON_LANGS.map(([v, l]) => `<option value="${v}"${v === project.lang ? ' selected' : ''}>${esc(l)}</option>`).join('');
}

async function showCapabilities() {
  const caps = [];
  caps.push(`IndexedDB ${('indexedDB' in window) ? '✓' : '✗'}`);
  caps.push(`OPFS ${(navigator.storage && navigator.storage.getDirectory) ? '✓' : '✗'}`);
  caps.push(`Web Workers ${('Worker' in window) ? '✓' : '✗'}`);
  if (navigator.storage && navigator.storage.estimate) {
    try {
      const { usage, quota } = await navigator.storage.estimate();
      if (quota) caps.push(`storage ~${(quota / 1048576).toFixed(0)} MB quota (${(((usage || 0) / quota) * 100).toFixed(1)}% used)`);
    } catch (_) { /* ignore */ }
  }
  if (navigator.storage && navigator.storage.persisted) {
    try { caps.push(`persistent ${(await navigator.storage.persisted()) ? '✓' : '✗ (grants on import)'}`); } catch (_) { /* ignore */ }
  }
  el('recon-caps').innerHTML = '<i class="fas fa-info-circle me-1"></i>Browser capabilities: ' + caps.join(' &middot; ');
}

async function loadSaved() {
  try {
    const saved = await getProject();
    if (saved && saved.columns && saved.rows) {
      project = saved;
      renderAll();
      showResume();
      console.log(`[recon] resumed saved project: ${project.total} rows`);
    }
  } catch (err) { console.error('[recon] could not load saved project', err); }
}

// ── Reconciliation engine (Phase 3) ─────────────────────────────────────────
// Sends one place-name query PER ROW to WHG's standard /reconcile service (same-origin,
// authenticated by the logged-in session + CSRF), caches candidates per unique key, and fans
// results back to every row sharing that key. Candidate review / accept-reject is Phase 4.
let running = false;
let stopRequested = false;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
function getCsrf() {
  const input = document.querySelector('input[name=csrfmiddlewaretoken]');
  if (input && input.value) return input.value;
  const m = document.cookie.match(/(?:^|;\s*)csrftoken=([^;]+)/);
  return m ? decodeURIComponent(m[1]) : '';
}
function normName(s) { return String(s == null ? '' : s).trim().toLowerCase().replace(/\s+/g, ' '); }
function colIndexByRole(role) { return project ? project.columns.findIndex((c) => c.role === role) : -1; }
const isCcode = (v) => /^[A-Za-z]{2}$/.test(String(v == null ? '' : v).trim());

// ── Multi-column (iterative, containment-chained) reconciliation ─────────────
// Reconcile more than one column, parent → child: the admin-parent columns (role "county/region",
// in dataset order) first, then the place-name column. Each child column's query is scoped by the
// parent column's resolved place_id (contained_in), so e.g. a Parish is matched within its County and
// a Name within its Parish — sharpening disambiguation of same-name places. The reconciliation chain
// is the ordered list of column indices.
function reconChain() {
  if (!project) return [];
  const chain = project.columns.map((c, i) => i).filter((i) => project.columns[i].role === 'county');
  const nameIdx = colIndexByRole('name');
  if (nameIdx >= 0) chain.push(nameIdx);
  return chain;
}
let reconActiveIdx = -1; // which chain position the review/results panes show; -1 → the name column (last)
function activeReconCol() {
  const chain = reconChain();
  if (!chain.length) return -1;
  if (reconActiveIdx < 0 || reconActiveIdx >= chain.length) return chain[chain.length - 1]; // default: name
  return chain[reconActiveIdx];
}
// The place_id resolved for a (column,row): an explicit accept, else an auto-confirmed top match.
function resolvedPlaceId(colIndex, rowIdx) {
  const key = colIndex + ':' + rowIdx;
  const a = acceptedList(project.decisions && project.decisions[key])[0];
  if (a) return a.place_id;
  const m = project.matches && project.matches[key];
  if (m && m.top && isAutoConfirmed(m.top, getThreshold())) return m.top.id;
  return null;
}

// Every row is reconciled INDIVIDUALLY — no de-duplication by name (users have already disambiguated,
// so two rows with the same toponym may be different places). Keyed by "<colIndex>:<rowIndex>" so each
// reconciled column keeps its own per-row matches/decisions. Defaults to the active chain column.
function buildUniqueQueries(colIndex) {
  if (colIndex == null) colIndex = activeReconCol();
  if (colIndex < 0) return null;
  const countryIdx = colIndexByRole('country');
  const map = new Map(); // key "<col>:<row>" -> { query, country, rows:[i] }
  project.rows.forEach((r, i) => {
    const val = String(r[colIndex] == null ? '' : r[colIndex]).trim();
    if (!val) return;
    const country = (countryIdx >= 0 && isCcode(r[countryIdx])) ? String(r[countryIdx]).trim().toUpperCase() : '';
    map.set(colIndex + ':' + i, { query: val, country, rows: [i] });
  });
  return { colIndex, nameIdx: colIndex, countryIdx, map };
}
function keyForRow(built, i) {
  const r = project.rows[i];
  const val = String(r[built.colIndex] == null ? '' : r[built.colIndex]).trim();
  if (!val) return null;
  const country = (built.countryIdx >= 0 && isCcode(r[built.countryIdx])) ? String(r[built.countryIdx]).trim().toUpperCase() : '';
  return { name: val, country, key: built.colIndex + ':' + i };
}

async function postReconcile(queries, csrf, attempt = 0) {
  // Send OpenRefine-standard form encoding (`queries=<json>`). The service reads this via
  // request.POST; the application/json path reads request.body, which raises RawPostDataException
  // under DRF (the auth layer has already consumed the request stream). Same contract, safe path.
  const res = await fetch(RECON_ENDPOINT, {
    method: 'POST',
    credentials: 'same-origin',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'X-CSRFToken': csrf },
    body: 'queries=' + encodeURIComponent(JSON.stringify(queries)),
  });
  if (res.status === 429 || res.status >= 500) {
    if (attempt >= 4) throw new Error(`server ${res.status} after retries`);
    await sleep(500 * Math.pow(2, attempt)); // exponential backoff
    return postReconcile(queries, csrf, attempt + 1);
  }
  if (res.status === 401 || res.status === 403) throw new Error(`not authorised (${res.status}); please log in as staff`);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

function setReconSummary(html) { el('recon-recon-summary').innerHTML = html; }
function toggleRunning(on) {
  running = on;
  el('recon-run').classList.toggle('d-none', on);
  el('recon-stop').classList.toggle('d-none', !on);
}
function updateProgress(done, total) {
  el('recon-progress-wrap').classList.remove('d-none');
  const pct = total ? Math.round((done / total) * 100) : 0;
  el('recon-progress-bar').style.width = pct + '%';
  el('recon-progress-text').textContent = `${done.toLocaleString()} / ${total.toLocaleString()} rows (${pct}%)`;
}

function getThreshold() {
  const box = el('recon-threshold');
  const n = box ? parseInt(box.value, 10) : NaN;
  return Number.isFinite(n) ? Math.min(100, Math.max(0, n)) : 90;
}
// Auto-confirm a top candidate when the name matched exactly, or its score clears the threshold.
function isAutoConfirmed(top, threshold) {
  return !!top && (top.match || Number(top.score) >= threshold);
}

// ── Candidate review (Phase 4) ───────────────────────────────────────────────
let reviewMeta = []; // [{key, rows, name, country}] — one per reviewable row
let reviewPos = 0;

const REVIEW_BADGE = {
  accepted: '<span class="badge bg-success">accepted ✓</span>',
  auto: '<span class="badge bg-success">auto ✓</span>',
  candidate: '<span class="badge bg-info text-dark">candidate</span>',
  rejected: '<span class="badge bg-dark">rejected</span>',
  skipped: '<span class="badge bg-secondary">skipped</span>',
  nomatch: '<span class="badge bg-warning text-dark">no match</span>',
  none: '<span class="badge bg-warning text-dark">no match</span>',
};
// Effective status of a unique key: explicit decision > auto-confirm > candidate/no-match.
function effectiveStatus(key) {
  const dec = project.decisions && project.decisions[key];
  if (dec) return dec.status;
  const m = project.matches && project.matches[key];
  if (m && m.top) return isAutoConfirmed(m.top, getThreshold()) ? 'auto' : 'candidate';
  return 'none';
}
// Accepted candidates for a key as an array [{ci, place_id, label, score}]. A place may closeMatch
// more than one WHG record (multi-select). Migrates the legacy single-accept shape transparently.
function acceptedList(dec) {
  if (!dec || dec.status !== 'accepted') return [];
  if (Array.isArray(dec.accepted)) return dec.accepted;
  if (dec.place_id != null) return [{ ci: dec.ci, place_id: dec.place_id, label: dec.label, score: dec.score }];
  return [];
}
// Candidate indices to show as "selected" (ringed) on the map: explicit accepts, or the auto-confirmed top.
function selectedCis(key) {
  const dec = project.decisions && project.decisions[key];
  const cis = acceptedList(dec).map((a) => a.ci);
  if (!dec) { const m = project.matches && project.matches[key]; if (m && m.top && isAutoConfirmed(m.top, getThreshold())) cis.push(0); }
  return cis;
}
// Highlight the list row for candidate `ci` (called when its marker is hovered on the map).
function highlightCandidateRow(ci) {
  const card = el('recon-review-card'); if (!card) return;
  card.querySelectorAll('.recon-cand').forEach((li) => li.classList.toggle('recon-cand--maphover', Number(li.dataset.ci) === ci));
}
// The first candidate accepted for a key (explicit accept, or auto-confirmed top), or null.
function acceptedCandidate(key) {
  const m = project.matches && project.matches[key];
  if (!m) return null;
  const dec = project.decisions && project.decisions[key];
  if (dec) { const a = acceptedList(dec)[0]; return a ? (m.candidates && m.candidates[a.ci]) || null : null; }
  return (m.top && isAutoConfirmed(m.top, getThreshold())) ? m.top : null;
}

function renderResultsTable(built) {
  const matches = project.matches || {};
  const threshold = getThreshold();
  let matched = 0, auto = 0, nomatch = 0, pending = 0, rowsMatched = 0, accepted = 0;
  built.map.forEach((v, key) => {
    const m = matches[key];
    if (!m) { pending += 1; return; }
    if (m.top) { matched += 1; rowsMatched += v.rows.length; if (isAutoConfirmed(m.top, threshold)) auto += 1; }
    else nomatch += 1;
    if (project.decisions && project.decisions[key] && project.decisions[key].status === 'accepted') accepted += 1;
  });
  setReconSummary(
    `<span class="text-success"><strong>${matched.toLocaleString()}</strong> matched</span> ` +
    `<span class="text-muted">(<strong>${auto.toLocaleString()}</strong> auto, <strong>${accepted.toLocaleString()}</strong> accepted)</span> · ` +
    `<span class="text-warning"><strong>${nomatch.toLocaleString()}</strong> no match</span> · ` +
    `<span class="text-muted"><strong>${pending.toLocaleString()}</strong> pending</span> — ` +
    `across <strong>${built.map.size.toLocaleString()}</strong> rows, ` +
    `covering <strong>${rowsMatched.toLocaleString()}</strong> of ${project.total.toLocaleString()} rows.`);

  // Build the full ordered row-info list once; the table is virtualised (only the visible window is
  // in the DOM), so it copes with very large datasets — off-screen rows are evicted on scroll.
  _resultRows = [];
  for (let i = 0; i < project.rows.length; i++) { const info = keyForRow(built, i); if (info) _resultRows.push(info); }
  el('recon-results-wrap').classList.remove('d-none');
  el('recon-results-note').textContent = _resultRows.length
    ? `${_resultRows.length.toLocaleString()} rows — scroll to load more (off-screen rows are evicted).` : '';
  renderResultsWindow(true);
  updatePaneSummaries();
}

// ── Virtualised results table (lazy load + auto-eviction) ────────────────────
let _resultRows = [];        // full ordered list of row infos {name, country, key}
let RESULT_ROW_H = 34;       // px per row; self-calibrated from the first render

function resultRowHtml(info) {
  const m = project.matches[info.key];
  let status, top = '', score = '';
  if (!m) status = '<span class="badge bg-secondary">pending</span>';
  else {
    status = REVIEW_BADGE[effectiveStatus(info.key)] || '';
    const show = acceptedCandidate(info.key) || m.top;
    if (show) { top = `${truncate(show.name, 50)} <span class="text-muted small">${truncate(show.description || '', 30)}</span>`; score = show.score; }
  }
  return `<tr data-row><td>${truncate(info.name, 50)}${info.country ? ` <span class="text-muted">(${esc(info.country)})</span>` : ''}</td>` +
         `<td>${status}</td><td>${top}</td><td>${score}</td></tr>`;
}
function renderResultsWindow(calibrate) {
  const wrap = el('recon-results-wrap'), tb = el('recon-results-body');
  if (!wrap || !tb) return;
  const total = _resultRows.length;
  const viewH = wrap.clientHeight || 440;
  const buffer = 8;
  const first = Math.max(0, Math.floor(wrap.scrollTop / RESULT_ROW_H) - buffer);
  const count = Math.ceil(viewH / RESULT_ROW_H) + buffer * 2;
  const last = Math.min(total, first + count);
  const spacer = (h) => (h > 0 ? `<tr aria-hidden="true"><td colspan="4" style="height:${h}px;padding:0;border:0"></td></tr>` : '');
  const rows = [];
  for (let i = first; i < last; i++) rows.push(resultRowHtml(_resultRows[i]));
  tb.innerHTML = spacer(first * RESULT_ROW_H) + rows.join('') + spacer((total - last) * RESULT_ROW_H);
  // Self-calibrate the row height from a real rendered row (once), then re-render if it was off.
  if (calibrate) {
    const sample = tb.querySelector('tr[data-row]');
    if (sample && sample.offsetHeight && Math.abs(sample.offsetHeight - RESULT_ROW_H) > 1) {
      RESULT_ROW_H = sample.offsetHeight;
      renderResultsWindow(false);
    }
  }
}
function renderResults(built) { renderColSwitcher(); renderResultsTable(built); refreshReview(); refreshFullMapPane(); refreshExport(); }

// Column switcher (multi-column reconciliation): pills for each chain column, parent → child, showing
// which column the results/review panes are currently displaying. Hidden for single-column datasets.
function renderColSwitcher() {
  const box = el('recon-col-switcher'); if (!box || !project) return;
  const chain = reconChain();
  if (chain.length <= 1) { box.innerHTML = ''; box.classList.add('d-none'); return; }
  box.classList.remove('d-none');
  const active = activeReconCol();
  const matches = project.matches || {};
  const pills = chain.map((ci, idx) => {
    const name = truncate(project.columns[ci].name, 22);
    let n = 0; for (const k in matches) { if (k.slice(0, k.indexOf(':')) === String(ci) && matches[k] && matches[k].top) n += 1; }
    const arrow = idx < chain.length - 1 ? '<i class="fas fa-angle-right mx-1 text-muted"></i>' : '';
    return `<button type="button" class="btn btn-sm ${ci === active ? 'btn-primary' : 'btn-outline-secondary'} recon-col-pill" data-idx="${idx}" title="${esc(project.columns[ci].name)}">${esc(name)}${n ? ` <span class="badge bg-light text-dark ms-1">${n.toLocaleString()}</span>` : ''}</button>${arrow}`;
  }).join('');
  box.innerHTML = `<span class="text-muted small me-2"><i class="fas fa-layer-group me-1"></i>Columns (parent → child):</span>${pills}`;
  box.querySelectorAll('.recon-col-pill').forEach((b) => b.addEventListener('click', () => {
    reconActiveIdx = Number(b.dataset.idx);
    const built = buildUniqueQueries(); if (built) renderResults(built);
  }));
}

// Reviewable rows (those with ≥1 candidate).
function reviewableKeys(built) {
  const arr = [];
  built.map.forEach((v, key) => { const m = project.matches[key]; if (m && m.top) arr.push({ key, rows: v.rows.length, name: v.query, country: v.country }); });
  arr.sort((a, b) => b.rows - a.rows);
  return arr;
}
function needsReview(key) {
  const m = project.matches[key];
  return !(project.decisions && project.decisions[key]) && m && m.top && !isAutoConfirmed(m.top, getThreshold());
}
function refreshReview() {
  const sec = el('recon-review');
  if (!sec) return;
  const built = buildUniqueQueries();
  if (!built) { sec.classList.add('d-none'); return; } // no reconcilable column mapped yet
  sec.classList.remove('d-none'); // header stays visible once a name column is mapped
  const hasMatches = project.matches && Object.keys(project.matches).length;
  reviewMeta = hasMatches ? reviewableKeys(built) : [];
  if (!reviewMeta.length) {
    const card = el('recon-review-card');
    if (card) card.innerHTML = `<div class="text-muted small py-3"><i class="fas fa-circle-info me-1"></i>${
      hasMatches
        ? 'Nothing to review for this column — matches were auto-confirmed. Tick “review all” to revisit them.'
        : 'Run <strong>reconciliation</strong> first (Step 3), then confirm matches here.'}</div>`;
    const map = el('recon-review-map'); if (map) map.classList.add('d-none');
    updateReviewProgress();
    return;
  }
  if (reviewPos >= reviewMeta.length) reviewPos = 0;
  const all = el('recon-review-all') && el('recon-review-all').checked;
  if (!all && !needsReview(reviewMeta[reviewPos].key)) {
    const j = reviewMeta.findIndex((r) => needsReview(r.key));
    if (j >= 0) reviewPos = j;
  }
  renderReviewCard();
  updateReviewProgress();
}
function updateReviewProgress() {
  const pending = reviewMeta.filter((r) => needsReview(r.key)).length;
  const decided = reviewMeta.filter((r) => project.decisions && project.decisions[r.key]).length;
  const p = el('recon-review-progress');
  if (p) p.textContent = `${decided.toLocaleString()} decided · ${pending.toLocaleString()} to review · ${reviewMeta.length.toLocaleString()} rows`;
}
function renderReviewCard() {
  const card = el('recon-review-card');
  if (!card || !reviewMeta.length) { if (card) card.innerHTML = ''; return; }
  const meta = reviewMeta[reviewPos];
  const m = project.matches[meta.key];
  const dec = project.decisions && project.decisions[meta.key];
  const auto = m.top && isAutoConfirmed(m.top, getThreshold());
  // Multi-select: a set of accepted candidate indices (auto-confirm previews candidate 0).
  const acceptedCis = new Set(acceptedList(dec).map((a) => a.ci));
  if (!dec && auto) acceptedCis.add(0);
  const list = (m.candidates || []).map((c, i) =>
    `<li class="recon-cand${acceptedCis.has(i) ? ' recon-cand--accepted' : ''}" data-ci="${i}">
       <span class="recon-cand-key" style="background:${RECON_COLORS[i % RECON_COLORS.length]}">${i + 1}</span>
       <span class="recon-cand-check" title="${acceptedCis.has(i) ? 'selected — click to unselect' : 'click to select (you can pick more than one)'}">${acceptedCis.has(i) ? '✓' : ''}</span>
       <span class="recon-cand-body">
         <span class="recon-cand-name">${truncate(c.name, 60)}</span>` +
    (c.match ? '<span class="badge bg-success ms-1">exact</span>' : '') +
    `<span class="recon-cand-ns ms-1">${esc(nsName(c.id))}</span>` +
    `<span class="text-muted small ms-1">${truncate(c.description || '', 36)}</span>` +
    (c.alt_names && c.alt_names.length
      ? `<span class="recon-cand-alt">also: ${c.alt_names.slice(0, 8).map((n) => truncate(n, 28)).join(', ')}${c.alt_names.length > 8 ? '…' : ''}</span>`
      : '') +
    `</span>
       <span class="recon-cand-score">${c.score}</span>
     </li>`).join('');
  const loadMore = !m.top ? ''
    : m.exhausted ? '<div class="small text-muted mt-1">all candidates shown.</div>'
    : `<div class="mt-1"><button type="button" class="btn btn-sm btn-link p-0 recon-loadmore" data-act="more">load more candidates</button></div>`;
  card.innerHTML =
    `<div class="recon-review-head d-flex justify-content-between align-items-start flex-wrap gap-2">
       <div><span class="fw-bold">${truncate(meta.name, 60)}</span>${meta.country ? ` <span class="text-muted">(${esc(meta.country)})</span>` : ''}
         <span class="text-muted small ms-2">${meta.rows.toLocaleString()} row${meta.rows === 1 ? '' : 's'} · ${reviewPos + 1} of ${reviewMeta.length}</span></div>
       <div>${REVIEW_BADGE[effectiveStatus(meta.key)] || ''}</div>
     </div>
     <ol class="recon-cand-list">${list || '<li class="text-muted">No candidates were returned for this name.</li>'}</ol>
     ${loadMore}
     <div class="recon-review-actions d-flex flex-wrap align-items-center gap-2 mt-2">
       <button type="button" class="btn btn-sm btn-outline-secondary" data-act="prev" title="Back (←)"><i class="fas fa-arrow-left"></i></button>
       <button type="button" class="btn btn-sm btn-outline-danger" data-act="reject">Reject <kbd>x</kbd></button>
       <button type="button" class="btn btn-sm btn-outline-secondary" data-act="skip">Skip <kbd>s</kbd></button>
       <button type="button" class="btn btn-sm btn-outline-warning" data-act="nomatch">No match <kbd>n</kbd></button>
       <button type="button" class="btn btn-sm btn-outline-secondary" data-act="undo">Undo <kbd>u</kbd></button>
       <button type="button" class="btn btn-sm btn-primary ms-auto" data-act="next">Next <i class="fas fa-arrow-right"></i></button>
     </div>
     <div class="recon-geom-tools d-flex flex-wrap align-items-center gap-1 mt-2 small">
       <span class="text-muted me-1"><i class="fas fa-location-dot"></i> Location:</span>
       <button type="button" class="btn btn-sm btn-outline-primary" data-geom="clone" title="Copy the selected match's coordinates into your dataset (point, line, or polygon)">Use match location</button>
       <span class="text-muted mx-1">or draw:</span>
       <button type="button" class="btn btn-sm btn-outline-secondary" data-geom="point">Point</button>
       <button type="button" class="btn btn-sm btn-outline-secondary" data-geom="line">Line</button>
       <button type="button" class="btn btn-sm btn-outline-secondary" data-geom="polygon">Polygon</button>
       <button type="button" class="btn btn-sm btn-outline-secondary" data-geom="finish">Finish</button>
       <button type="button" class="btn btn-sm btn-outline-danger" data-geom="clear"${(project.geom && project.geom[meta.key]) ? '' : ' disabled'}>Clear</button>
       <span id="recon-geom-status" class="text-muted ms-1">${esc(geomStatusText(meta.key))}</span>
     </div>`;
  card.querySelectorAll('.recon-cand').forEach((li) => {
    const ci = Number(li.dataset.ci);
    li.addEventListener('click', () => acceptCandidate(ci));
    // Hovering a candidate in the list highlights its marker on the map (and vice versa).
    li.addEventListener('mouseenter', () => { if (ReconMap && ReconMap.setMarkerHover) ReconMap.setMarkerHover(ci); });
    li.addEventListener('mouseleave', () => { if (ReconMap && ReconMap.setMarkerHover) ReconMap.setMarkerHover(null); });
  });
  card.querySelectorAll('[data-act]').forEach((b) => b.addEventListener('click', () => reviewAction(b.dataset.act)));
  card.querySelectorAll('[data-geom]').forEach((b) => b.addEventListener('click', () => geomAction(b.dataset.geom, meta.key)));
  updateReviewMap(meta.key); // async: plot candidate + own coordinates on a map
}

// Candidate source namespace (from the id, e.g. "place:gn:745044" → "gn") → a human name.
// Fallback display names for common namespaces; the authoritative list + names + descriptions come
// from the gazetteer registry (/api/sources/), loaded once and cached in _sources.
const NS_NAMES = {
  gn: 'GeoNames', wd: 'Wikidata', tgn: 'Getty TGN', osm: 'OpenStreetMap', ohm: 'OpenHistoricalMap',
  pl: 'Pleiades', pleiades: 'Pleiades', whg: 'World Historical Gazetteer', chgis: 'CHGIS',
  hgis: 'HGIS de las Indias', alc: 'Alcedo', gb1900: 'GB1900', ukhc: 'UK Historic Counties',
};
let _sources = null;   // [{namespace,name,description,record_count,core,gazetteer_type,temporal_extent}]
let _sourcesByNs = {}; // namespace -> entry
async function loadSources() {
  if (_sources) return _sources;
  try {
    const res = await fetch('/api/sources/', { credentials: 'same-origin', headers: { Accept: 'application/json' } });
    if (res.ok) { const d = await res.json(); _sources = (d && d.sources) || []; _sourcesByNs = {}; _sources.forEach((s) => { _sourcesByNs[s.namespace] = s; }); }
  } catch (_) { /* fall back to the static NS_NAMES list */ }
  return _sources || [];
}
function nsFromId(id) { const p = String(id || '').split(':'); return p.length >= 3 ? p[1] : 'whg'; }
function nsName(id) { const ns = nsFromId(id); return (_sourcesByNs[ns] && _sourcesByNs[ns].name) || NS_NAMES[ns] || ns.toUpperCase(); }

// ── Source-gazetteer (namespace) picker: prioritise / restrict, persisted ─────
const NS_LS_KEY = 'whg-recon-ns';
function getNsFilter() {
  if (project && project.nsFilter) return project.nsFilter;
  try { const s = JSON.parse(localStorage.getItem(NS_LS_KEY) || 'null'); if (s && s.mode) return s; } catch (_) { /* */ }
  return { mode: 'all', namespaces: [] };
}
function availableNamespaces() {
  // Curated baseline (so a partially-seeded registry never drops known-good sources) ∪ registry
  // authorities (adds e.g. ukhc, supplies names/counts) ∪ namespaces seen in the current matches.
  const set = new Set(Object.keys(NS_NAMES));
  if (_sources && _sources.length) _sources.forEach((s) => set.add(s.namespace));
  if (project && project.matches) Object.values(project.matches).forEach((m) => (m.candidates || []).forEach((c) => set.add(nsFromId(c.id))));
  return [...set];
}
function sortByNsPriority(cands, namespaces) {
  const pri = (c) => (namespaces.includes(nsFromId(c.id)) ? 0 : 1);
  return cands.map((c, i) => ({ c, i })).sort((a, b) => (pri(a.c) - pri(b.c)) || (b.c.score - a.c.score) || (a.i - b.i)).map((x) => x.c);
}
// Apply the current filter to a freshly-fetched candidate list (prioritise re-orders; only is enforced
// server-side via the query, but re-filter defensively too).
function applyNsToCandidates(result) {
  const f = getNsFilter();
  if (!f.namespaces.length) return result;
  if (f.mode === 'only') return result.filter((c) => f.namespaces.includes(nsFromId(c.id)));
  if (f.mode === 'prioritise') return sortByNsPriority(result, f.namespaces);
  return result;
}
function updateSourcesLabel() {
  const f = getNsFilter(); const lbl = el('recon-sources-label'); if (!lbl) return;
  lbl.textContent = f.mode === 'only' ? `Only ${f.namespaces.length} source${f.namespaces.length === 1 ? '' : 's'}`
    : f.mode === 'prioritise' && f.namespaces.length ? `Prioritising ${f.namespaces.length}` : 'Sources';
}
async function populateSourcesModal() {
  await loadSources(); // registry-driven list (names, record counts, descriptions)
  const f = getNsFilter();
  const modeInput = document.querySelector(`input[name="recon-ns-mode"][value="${f.mode}"]`);
  if (modeInput) modeInput.checked = true;
  const box = el('recon-ns-list');
  const nss = availableNamespaces();
  if (box) box.innerHTML = nss.map((ns) => {
    const s = _sourcesByNs[ns];
    const label = (s && s.name) || NS_NAMES[ns] || ns.toUpperCase();
    const count = s && s.record_count ? ` <span class="text-muted small">· ${Number(s.record_count).toLocaleString()} records</span>` : '';
    const tip = s && s.description ? `${label} — ${s.description}` : label;
    return `<label class="recon-ns-item" data-ns="${esc(ns)}" title="${esc(tip)}">` +
      `<input type="checkbox" class="recon-ns-cb" value="${esc(ns)}"${f.namespaces.includes(ns) ? ' checked' : ''}> ` +
      `${esc(label)} <span class="text-muted small">(${esc(ns)})</span>${count}</label>`;
  }).join('');
}
function applyNsFilter() {
  const mode = (document.querySelector('input[name="recon-ns-mode"]:checked') || {}).value || 'all';
  const namespaces = [...document.querySelectorAll('.recon-ns-cb:checked')].map((c) => c.value);
  const f = { mode, namespaces };
  if (project) { project.nsFilter = f; }
  try { localStorage.setItem(NS_LS_KEY, JSON.stringify(f)); } catch (_) { /* */ }
  updateSourcesLabel();
  // 'prioritise' re-orders existing candidates immediately; 'only'/'all' take effect on the next run.
  if (project && project.matches && Object.keys(project.matches).length && mode === 'prioritise') {
    Object.values(project.matches).forEach((m) => { if (m.candidates) { m.candidates = sortByNsPriority(m.candidates, namespaces); m.top = m.candidates[0] || null; } });
  }
  if (project) persist();
  if (project && project.matches) { const built = buildUniqueQueries(); if (built) renderResults(built); }
}

// ── Review map: fetch candidate coordinates and plot them ────────────────────
const _candCoord = {}; // candidate id -> {lon,lat} | null (cache)
function firstLngLat(geom) {
  if (!geom) return null;
  if (geom.type === 'GeometryCollection') { for (const g of geom.geometries || []) { const p = firstLngLat(g); if (p) return p; } return null; }
  let c = geom.coordinates;
  while (Array.isArray(c) && Array.isArray(c[0])) c = c[0];
  return (Array.isArray(c) && typeof c[0] === 'number') ? { lon: c[0], lat: c[1] } : null;
}
async function fetchCandidateCoord(id) {
  if (id in _candCoord) return _candCoord[id];
  try {
    const res = await fetch(`/entity/${encodeURIComponent(id)}/api`, { credentials: 'same-origin', headers: { Accept: 'application/json' } });
    if (!res.ok) throw new Error(String(res.status));
    const feat = await res.json();
    _candCoord[id] = firstLngLat(feat && feat.geometry);
  } catch (_) { _candCoord[id] = null; }
  return _candCoord[id];
}
async function rowOwnCoord(key) {
  const coordsIdx = colIndexByRole('coords'), latIdx = colIndexByRole('lat'), lonIdx = colIndexByRole('lon');
  if (coordsIdx < 0 && !(latIdx >= 0 && lonIdx >= 0)) return null;
  const built = buildUniqueQueries(); const v = built && built.map.get(key);
  if (!v || !v.rows.length) return null;
  const r = project.rows[v.rows[0]];
  await loadCoords();
  const c = coordsIdx >= 0 ? Coords.parseCoord(currentCoordFormat(), r[coordsIdx]) : Coords.parseLatLonPair(r[latIdx], r[lonIdx], !!project.coordSwap);
  return c ? { lon: c.lon, lat: c.lat } : null;
}
let _mapToken = 0;
async function updateReviewMap(key) {
  const box = el('recon-review-map'); if (!box) return;
  const token = ++_mapToken; // guard against out-of-order results when navigating fast
  const m = project.matches[key]; const cands = (m.candidates || []).slice(0, 15);
  const points = [];
  await Promise.all(cands.map(async (c, i) => {
    const pt = await fetchCandidateCoord(c.id);
    if (pt) points.push({ ci: i, lon: pt.lon, lat: pt.lat, name: c.name, namespace: nsName(c.id), altNames: c.alt_names || [], score: c.score });
  }));
  let rowPoint = null;
  try { rowPoint = await rowOwnCoord(key); } catch (_) { /* ignore */ }
  if (token !== _mapToken) return; // a newer card was requested meanwhile
  if (!points.length && !rowPoint) { box.classList.add('d-none'); return; }
  box.classList.remove('d-none');
  try {
    const mod = await loadReconMap();
    if (token === _mapToken) mod.renderReviewMap(box, points, rowPoint, {
      onAccept: (ci) => acceptCandidate(ci),
      onGeom: (g) => onReviewGeom(key, g),                         // user drew / cleared on the map
      override: (project.geom && project.geom[key] && project.geom[key].geometry) || null,
      selected: selectedCis(key),                                 // accepted candidates get a ring
      onHover: (ci) => highlightCandidateRow(ci),                 // map hover → highlight the list row
    });
  } catch (err) { console.error('[recon] review map failed', err); box.classList.add('d-none'); }
}
// Full geometry (point / line / polygon) for a candidate — used when cloning a match's location.
async function fetchCandidateGeometry(id) {
  try {
    const res = await fetch(`/entity/${encodeURIComponent(id)}/api`, { credentials: 'same-origin', headers: { Accept: 'application/json' } });
    if (!res.ok) throw new Error(String(res.status));
    const feat = await res.json();
    return (feat && feat.geometry) || null;
  } catch (_) { return null; }
}
// Record / clear a geometry override for a place (all rows sharing the key). Overrides win on export.
function onReviewGeom(key, geometry) {
  project.geom = project.geom || {};
  if (geometry) project.geom[key] = { source: 'drawn', geometry };
  else delete project.geom[key];
  persist();
  updateGeomStatus(key);
  refreshExport();
}
function geomStatusText(key) {
  const g = project.geom && project.geom[key];
  if (!g) return 'from dataset coordinates';
  const t = (g.geometry && g.geometry.type) || 'geometry';
  return `${g.source === 'match' ? 'cloned from match' : 'drawn'} · ${t}`;
}
function updateGeomStatus(key) {
  const s = el('recon-geom-status'); if (s) s.textContent = geomStatusText(key);
  const card = el('recon-review-card');
  if (card) card.querySelectorAll('[data-geom]').forEach((b) => {
    if (b.dataset.geom === 'clear') b.disabled = !(project.geom && project.geom[key]);
  });
}
// Toolbar actions for the location picker in the review card.
async function geomAction(kind, key) {
  const mod = await loadReconMap();
  if (kind === 'point' || kind === 'line' || kind === 'polygon') {
    mod.startDraw(kind);
    const s = el('recon-geom-status');
    const Btn = kind.charAt(0).toUpperCase() + kind.slice(1); // Point / Line / Polygon
    if (s) s.textContent = kind === 'point'
      ? `click the map to place a point — press the “${Btn}” button again to add more (→ Multi)`
      : `click the map to add points, then Finish — press the “${Btn}” button again for another part (→ Multi)`;
    return;
  }
  if (kind === 'finish') { mod.finishDraw(); return; }
  if (kind === 'clear') { mod.clearGeom(); return; } // fires onGeom(null) → onReviewGeom clears it
  if (kind === 'clone') {
    const m = project.matches[key]; if (!m) return;
    const dec = project.decisions && project.decisions[key];
    const ci = acceptedList(dec).length ? acceptedList(dec)[0].ci : 0; // clone the first accepted match
    const cand = (m.candidates || [])[ci] || m.top; if (!cand) return;
    const s = el('recon-geom-status'); if (s) s.textContent = 'fetching match geometry…';
    const g = await fetchCandidateGeometry(cand.id);
    if (!g) { if (s) s.textContent = 'no geometry available for this match'; return; }
    project.geom = project.geom || {};
    project.geom[key] = { source: 'match', geometry: g };
    mod.setOverride(g);
    persist(); updateGeomStatus(key); refreshExport();
  }
}
// Toggle a candidate's acceptance. More than one may be accepted (each becomes a closeMatch); the
// card stays put so the reviewer can pick several, then advances with Next / →.
function acceptCandidate(ci) {
  const meta = reviewMeta[reviewPos]; if (!meta) return;
  const c = (project.matches[meta.key].candidates || [])[ci]; if (!c) return;
  project.decisions = project.decisions || {};
  const cur = project.decisions[meta.key];
  const list = (cur && cur.status === 'accepted') ? acceptedList(cur).slice() : [];
  const at = list.findIndex((a) => a.ci === ci);
  if (at >= 0) list.splice(at, 1);
  else list.push({ ci, place_id: c.id, label: c.name, score: c.score });
  if (list.length) project.decisions[meta.key] = { status: 'accepted', accepted: list };
  else delete project.decisions[meta.key]; // unselected the last → back to undecided
  afterDecision(false); // don't auto-advance; multi-select stays on the card
}
function reviewAction(act) {
  if (act === 'next') return advance(1);
  if (act === 'prev') return advance(-1);
  if (act === 'more') return loadMoreCandidates();
  const meta = reviewMeta[reviewPos]; if (!meta) return;
  if (act === 'undo') { if (project.decisions) delete project.decisions[meta.key]; return afterDecision(false); }
  project.decisions = project.decisions || {};
  project.decisions[meta.key] = { status: act === 'reject' ? 'rejected' : act === 'skip' ? 'skipped' : 'nomatch' };
  afterDecision(true);
}
// Fetch a larger batch of candidates for the current name (re-query with a higher limit).
async function loadMoreCandidates() {
  const meta = reviewMeta[reviewPos]; if (!meta) return;
  const m = project.matches[meta.key];
  const want = ((m.candidates && m.candidates.length) || 0) + 10;
  const btn = el('recon-review-card').querySelector('.recon-loadmore');
  if (btn) { btn.textContent = 'loading…'; btn.disabled = true; }
  try {
    const nsf = getNsFilter();
    const q = { q0: { query: meta.name, type: 'place', limit: want } };
    if (meta.country) q.q0.countries = [meta.country];
    if (nsf.mode === 'only' && nsf.namespaces.length) q.q0.namespaces = nsf.namespaces;
    const data = await postReconcile(q, getCsrf());
    const result = applyNsToCandidates((data.q0 && data.q0.result) || []);
    m.candidates = result;
    m.top = result[0] || null;
    m.exhausted = result.length < want; // fewer than asked → no more to fetch
    await persist();
    renderReviewCard();
  } catch (err) {
    console.error('[recon] load more failed', err);
    if (btn) { btn.textContent = 'load more failed — retry'; btn.disabled = false; }
  }
}
function afterDecision(advanceAfter) {
  persist();
  const built = buildUniqueQueries(); if (built) renderResultsTable(built);
  updateReviewProgress();
  refreshExport();
  if (advanceAfter) advance(1); else renderReviewCard();
}
function advance(dir) {
  if (!reviewMeta.length) return;
  const all = el('recon-review-all') && el('recon-review-all').checked;
  let i = reviewPos + dir;
  if (!all) { while (i >= 0 && i < reviewMeta.length && !needsReview(reviewMeta[i].key)) i += dir; }
  reviewPos = Math.max(0, Math.min(reviewMeta.length - 1, i));
  renderReviewCard();
  updateReviewProgress();
}
function reviewKeydown(e) {
  const sec = el('recon-review');
  if (!sec || sec.classList.contains('d-none')) return;
  const a = document.activeElement;
  if (a && (a.tagName === 'INPUT' || a.tagName === 'SELECT' || a.tagName === 'TEXTAREA')) return;
  const k = e.key;
  if (k >= '1' && k <= '9') acceptCandidate(Number(k) - 1);
  else if (k === 'x' || k === 'X') reviewAction('reject');
  else if (k === 's' || k === 'S') reviewAction('skip');
  else if (k === 'n' || k === 'N') reviewAction('nomatch');
  else if (k === 'u' || k === 'U') reviewAction('undo');
  else if (k === 'ArrowRight') advance(1);
  else if (k === 'ArrowLeft') advance(-1);
  else return;
  e.preventDefault();
}

// Show/refresh the reconcile section based on whether a 'name' column is mapped.
function refreshReconSection() {
  const hasName = colIndexByRole('name') >= 0;
  el('recon-recon').classList.toggle('d-none', !hasName);
  const thr = el('recon-threshold');
  if (thr && project && project.autoThreshold != null) thr.value = project.autoThreshold;
  if (hasName && project.matches && Object.keys(project.matches).length) {
    const built = buildUniqueQueries();
    if (built) renderResults(built);
  }
}

async function reconcileAll() {
  if (running) return;
  const chain = reconChain();
  if (!chain.length) {
    setReconSummary('<span class="text-warning">Map a “Place name” column first (Step 2).</span>');
    return;
  }
  project.matches = project.matches || {};
  toggleRunning(true);
  openPane('recon-recon'); // focus the reconcile step (accordion collapses the others)
  stopRequested = false;
  const csrf = getCsrf();

  // Reconcile each column of the chain, parent → child. Each child column's query is scoped by the
  // parent column's resolved place_id (contained_in), so Parish is matched within its County and Name
  // within its Parish — sharpening disambiguation of same-name places.
  for (let p = 0; p < chain.length && !stopRequested; p++) {
    await reconcilePass(chain[p], p > 0 ? chain[p - 1] : -1, csrf, p, chain.length);
  }

  toggleRunning(false);
  reconActiveIdx = chain.length - 1; // land on the name column for review
  const built = buildUniqueQueries();
  if (built) renderResults(built);
  renderColSwitcher();
  await persist();
  console.log(`[recon] reconciliation ${stopRequested ? 'stopped' : 'complete'} — ${chain.length} column(s)`);
}

// Reconcile one column of the chain. parentCol < 0 for the top of the chain.
async function reconcilePass(colIndex, parentCol, csrf, passNo, passTotal) {
  const built = buildUniqueQueries(colIndex);
  if (!built || !built.map.size) return;
  reconActiveIdx = passNo; // show this column's progress/results while it runs
  renderColSwitcher();
  const colName = truncate(project.columns[colIndex].name, 30);
  const passLabel = passTotal > 1 ? `<span class="text-muted">column ${passNo + 1}/${passTotal} · <strong>${esc(colName)}</strong></span> · ` : '';
  const entries = [...built.map.entries()].filter(([key]) => !project.matches[key]); // resume: skip done
  const total = built.map.size;
  let done = total - entries.length;
  updateProgress(done, total);

  // Language-conditioned Symphonym embeddings (int8, 128-d) for this column's values.
  let embByKey = null;
  if (phoneticEnabled() && entries.length) {
    try {
      const mod = await loadSymphonym();
      const lang = getLang();
      const names = entries.map(([, v]) => v.query);
      const int8 = await mod.embedNames(names, {
        lang,
        onProgress: (d, t) => setReconSummary(`${passLabel}<i class="fas fa-spinner fa-spin me-1"></i>embeddings ${d.toLocaleString()} / ${t.toLocaleString()}…`),
      });
      embByKey = {};
      entries.forEach(([key], idx) => { embByKey[key] = Array.from(int8.subarray(idx * 128, idx * 128 + 128)); });
    } catch (err) { console.error('[recon] embedding failed; using text matching', err); }
  }

  for (let b = 0; b < entries.length && !stopRequested; b += RECON_BATCH) {
    const slice = entries.slice(b, b + RECON_BATCH);
    const queries = {};
    const nsf = getNsFilter();
    slice.forEach(([key, v], j) => {
      const q = { query: v.query, type: 'place', limit: RECON_CAND_LIMIT };
      if (v.country) q.countries = [v.country];
      if (nsf.mode === 'only' && nsf.namespaces.length) q.namespaces = nsf.namespaces; // restrict sources
      if (embByKey && embByKey[key]) q.embedding = embByKey[key]; // phonetic (vector) matching
      // Containment: scope this column's query by the parent column's resolved place for the same row.
      if (parentCol >= 0) {
        const pid = resolvedPlaceId(parentCol, key.slice(key.indexOf(':') + 1));
        if (pid) { q.contained_in = [pid]; q.containment = 'fuzzy'; q.relation = 'within'; }
      }
      queries['q' + j] = q;
    });
    let data;
    try { data = await postReconcile(queries, csrf); }
    catch (err) {
      console.error('[recon] batch failed', err);
      setReconSummary(`<span class="text-danger"><i class="fas fa-exclamation-triangle me-1"></i>Reconciliation stopped: ${esc(err.message)}</span>`);
      stopRequested = true;
      break;
    }
    slice.forEach(([key], j) => {
      const result = applyNsToCandidates((data['q' + j] && data['q' + j].result) || []);
      project.matches[key] = { candidates: result, top: result[0] || null, exhausted: result.length < RECON_CAND_LIMIT, at: new Date().toISOString() };
    });
    done += slice.length;
    updateProgress(done, total);
    renderResults(built);
    await persist();
    if (!stopRequested && b + RECON_BATCH < entries.length) await sleep(150); // gentle throttle
  }
}

// Fetch + parse the bundled demo dataset. Resolves once the mapping pane (step 2) has rendered, so
// callers (the sample button, the guided tour) can await a fully-loaded project.
async function loadSampleDataset() {
  try {
    const res = await fetch('/static/webpack/samples/reconciliation-demo.csv', { credentials: 'same-origin' });
    if (!res.ok) throw new Error('HTTP ' + res.status);
    handleFile(new File([await res.text()], 'reconciliation-demo.csv', { type: 'text/csv' }));
  } catch (err) {
    console.error('[recon] load sample failed', err);
    const caps = el('recon-caps'); if (caps) caps.innerHTML = '<span class="text-danger">Could not load the sample dataset.</span>';
    throw err;
  }
  // handleFile parses via FileReader (async) then renderAll(); wait for the mapping table to appear.
  await waitUntil(() => project && !el('recon-result').classList.contains('d-none') && el('recon-map-body').children.length > 0);
}

// Small poller used by the guided tour to await async view changes (parse, reconcile, map draw).
async function waitUntil(cond, timeout = 20000, interval = 120) {
  const start = Date.now();
  while (Date.now() - start < timeout) { try { if (cond()) return true; } catch (_) { /* keep polling */ } await sleep(interval); }
  return false;
}

// Driving hooks handed to the guided tour (recon-tour.js) so it can run the real workbench.
function tourApi() {
  return {
    hasProject: () => !!project,
    isRunning: () => running,
    openPane: (id) => openPane(id),
    loadSample: async () => {
      // If the demo is already loaded, don't re-fetch; otherwise (re)load it fresh for the tour.
      if (!(project && project.fileName === 'reconciliation-demo.csv')) await loadSampleDataset();
    },
    reconcile: async () => {
      if (!(project && project.matches && Object.keys(project.matches).length)) await reconcileAll();
      await waitUntil(() => !running);
    },
  };
}

function init() {
  const dz = el('recon-dropzone');
  const input = el('recon-file');
  if (!dz || !input) return; // not on this page

  const openPicker = () => input.click();
  dz.addEventListener('click', openPicker);
  dz.addEventListener('keydown', (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); openPicker(); } });
  input.addEventListener('change', () => { if (input.files[0]) handleFile(input.files[0]); });

  // Load a bundled sample dataset (fetched from a static URL) for demonstration — no file picker.
  const sampleBtn = el('recon-load-sample');
  if (sampleBtn) sampleBtn.addEventListener('click', () => { sampleBtn.disabled = true; loadSampleDataset().finally(() => { sampleBtn.disabled = false; }); });

  // "Take a tour" — guided product tour that drives the whole flow on the sample data. The tour
  // engine is lazy-loaded (its own chunk) so it costs nothing until requested.
  const tourBtn = el('recon-take-tour');
  if (tourBtn) tourBtn.addEventListener('click', async () => {
    tourBtn.disabled = true;
    try { const mod = await import(/* webpackChunkName: "recon-tour" */ './recon-tour.js'); mod.startTour(tourApi()); }
    catch (err) { console.error('[recon] tour failed to load', err); }
    finally { tourBtn.disabled = false; }
  });

  ['dragenter', 'dragover'].forEach((ev) => dz.addEventListener(ev, (e) => { e.preventDefault(); dz.classList.add('recon-dropzone--over'); }));
  ['dragleave', 'drop'].forEach((ev) => dz.addEventListener(ev, (e) => { e.preventDefault(); dz.classList.remove('recon-dropzone--over'); }));
  dz.addEventListener('drop', (e) => { const f = e.dataTransfer && e.dataTransfer.files[0]; if (f) handleFile(f); });

  // "Clear my data" opens a styled confirmation modal (not the browser-native confirm).
  const openClearModal = () => {
    const m = el('recon-clear-modal');
    if (m && window.bootstrap && window.bootstrap.Modal) window.bootstrap.Modal.getOrCreateInstance(m).show();
    else clearData(); // graceful fallback if Bootstrap JS is unavailable
  };
  const clear = el('recon-clear');
  if (clear) clear.addEventListener('click', openClearModal);
  const startover = el('recon-startover');
  if (startover) startover.addEventListener('click', openClearModal);
  const clearConfirm = el('recon-clear-confirm');
  if (clearConfirm) clearConfirm.addEventListener('click', clearData);

  const run = el('recon-run');
  if (run) run.addEventListener('click', reconcileAll);
  const stop = el('recon-stop');
  if (stop) stop.addEventListener('click', () => { stopRequested = true; });

  const thr = el('recon-threshold');
  if (thr) thr.addEventListener('input', () => {
    if (!project) return;
    project.autoThreshold = getThreshold();
    persist();
    const built = buildUniqueQueries();
    if (built && project.matches && Object.keys(project.matches).length) renderResults(built);
  });

  const showIgn = el('recon-show-ignored');
  if (showIgn) showIgn.addEventListener('change', () => { if (!project) return; project.showIgnored = showIgn.checked; persist(); renderPreview(); });

  const backupBtn = el('recon-backup');
  if (backupBtn) backupBtn.addEventListener('click', downloadBackup);
  const exportBtn = el('recon-export-btn');
  if (exportBtn) exportBtn.addEventListener('click', runExport);
  const contribBtn = el('recon-contribute-btn');
  if (contribBtn) contribBtn.addEventListener('click', contributeToWHG);

  // Contribute submits a form that navigates to WHG's validation page, leaving the button disabled
  // and showing "uploading to WHG…". If the user comes Back — especially via the bfcache, which
  // restores the DOM exactly as it was left and does NOT re-run init() — those stay frozen. Reset
  // them on pageshow so the button is usable again.
  window.addEventListener('pageshow', () => {
    if (contribBtn) contribBtn.disabled = false;
    const cs = el('recon-contribute-status'); if (cs) cs.textContent = '';
  });

  // Phonetic (vector) matching (Symphonym, in-browser) — default on; toggle + language persist.
  const phon = el('recon-phonetic');
  if (phon) {
    if (project && project.phonetic === false) phon.checked = false;
    phon.addEventListener('change', () => { if (project) { project.phonetic = phon.checked; persist(); } });
  }
  const langSel = el('recon-lang');
  if (langSel) langSel.addEventListener('change', () => { if (project) { project.lang = langSel.value; persist(); } });
  const rw = el('recon-results-wrap');
  if (rw) rw.addEventListener('scroll', () => { if (_resultRows.length) requestAnimationFrame(() => renderResultsWindow(false)); });

  // Accordion (strict): opening a pane collapses the others; clicking the open one collapses it.
  document.querySelectorAll('.recon-pane-toggle').forEach((btn) => btn.addEventListener('click', () => {
    const p = document.getElementById(btn.dataset.pane);
    if (!p) return;
    if (p.classList.contains('recon-collapsed')) openPane(btn.dataset.pane);
    else p.classList.add('recon-collapsed');
  }));

  // Phase 4 — candidate review: keyboard-first + the "review all" toggle.
  document.addEventListener('keydown', reviewKeydown);
  const revAll = el('recon-review-all');
  if (revAll) revAll.addEventListener('change', () => { reviewPos = 0; refreshReview(); });

  // Source-gazetteer (namespace) picker modal.
  const sourcesModal = el('recon-sources-modal');
  if (sourcesModal) sourcesModal.addEventListener('show.bs.modal', populateSourcesModal);
  const nsApply = el('recon-ns-apply');
  if (nsApply) nsApply.addEventListener('click', applyNsFilter);
  updateSourcesLabel();
  // Warm the registry source list so candidate/source labels use real names (e.g. "UK Historic
  // Counties" not "UKHC"); re-render once it lands if matches are already on screen.
  loadSources().then(() => { if (project && project.matches && Object.keys(project.matches).length) { const built = buildUniqueQueries(); if (built) renderResults(built); } });

  showCapabilities();
  loadSaved();
}

if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
else init();
