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
// Sends de-duplicated place-name queries to WHG's standard /reconcile service (same-origin,
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

// Build the map of unique (name[, country]) queries → the rows that share each.
function buildUniqueQueries() {
  const nameIdx = colIndexByRole('name');
  if (nameIdx < 0) return null;
  const countryIdx = colIndexByRole('country');
  const map = new Map(); // key -> { query, country, rows:[rowIndex,...] }
  project.rows.forEach((r, i) => {
    const name = String(r[nameIdx] == null ? '' : r[nameIdx]).trim();
    if (!name) return;
    const country = (countryIdx >= 0 && isCcode(r[countryIdx])) ? String(r[countryIdx]).trim().toUpperCase() : '';
    const key = normName(name) + '|' + country;
    if (!map.has(key)) map.set(key, { query: name, country, rows: [] });
    map.get(key).rows.push(i);
  });
  return { nameIdx, countryIdx, map };
}
function keyForRow(built, i) {
  const r = project.rows[i];
  const name = String(r[built.nameIdx] == null ? '' : r[built.nameIdx]).trim();
  if (!name) return null;
  const country = (built.countryIdx >= 0 && isCcode(r[built.countryIdx])) ? String(r[built.countryIdx]).trim().toUpperCase() : '';
  return { name, country, key: normName(name) + '|' + country };
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
  el('recon-progress-text').textContent = `${done.toLocaleString()} / ${total.toLocaleString()} unique queries (${pct}%)`;
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
let reviewMeta = []; // [{key, rows, name, country}] — reviewable unique names, highest-impact first
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
// The candidate accepted for a key (explicit accept, or auto-confirmed top), or null.
function acceptedCandidate(key) {
  const m = project.matches && project.matches[key];
  if (!m) return null;
  const dec = project.decisions && project.decisions[key];
  if (dec) return dec.status === 'accepted' ? (m.candidates && m.candidates[dec.ci]) || null : null;
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
    `across <strong>${built.map.size.toLocaleString()}</strong> unique names, ` +
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
function renderResults(built) { renderResultsTable(built); refreshReview(); }

// Reviewable unique names (those with ≥1 candidate), highest row-impact first.
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
  if (!built || !project.matches || !Object.keys(project.matches).length) { sec.classList.add('d-none'); return; }
  reviewMeta = reviewableKeys(built);
  if (!reviewMeta.length) { sec.classList.add('d-none'); return; }
  sec.classList.remove('d-none');
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
  if (p) p.textContent = `${decided.toLocaleString()} decided · ${pending.toLocaleString()} to review · ${reviewMeta.length.toLocaleString()} unique names`;
}
function renderReviewCard() {
  const card = el('recon-review-card');
  if (!card || !reviewMeta.length) { if (card) card.innerHTML = ''; return; }
  const meta = reviewMeta[reviewPos];
  const m = project.matches[meta.key];
  const dec = project.decisions && project.decisions[meta.key];
  const auto = m.top && isAutoConfirmed(m.top, getThreshold());
  const acceptedCi = dec && dec.status === 'accepted' ? dec.ci : (auto && !dec ? 0 : -1);
  const list = (m.candidates || []).map((c, i) =>
    `<li class="recon-cand${i === acceptedCi ? ' recon-cand--accepted' : ''}" data-ci="${i}">
       <span class="recon-cand-key" style="background:${RECON_COLORS[i % RECON_COLORS.length]}">${i + 1}</span>
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
     </div>`;
  card.querySelectorAll('.recon-cand').forEach((li) => li.addEventListener('click', () => acceptCandidate(Number(li.dataset.ci))));
  card.querySelectorAll('[data-act]').forEach((b) => b.addEventListener('click', () => reviewAction(b.dataset.act)));
  updateReviewMap(meta.key); // async: plot candidate + own coordinates on a map
}

// Candidate source namespace (from the id, e.g. "place:gn:745044" → "gn") → a human name.
const NS_NAMES = {
  gn: 'GeoNames', wd: 'Wikidata', tgn: 'Getty TGN', osm: 'OpenStreetMap', ohm: 'OpenHistoricalMap',
  pl: 'Pleiades', pleiades: 'Pleiades', whg: 'World Historical Gazetteer', chgis: 'CHGIS',
  hgis: 'HGIS de las Indias', alc: 'Alcedo', gb1900: 'GB1900',
};
function nsFromId(id) { const p = String(id || '').split(':'); return p.length >= 3 ? p[1] : 'whg'; }
function nsName(id) { const ns = nsFromId(id); return NS_NAMES[ns] || ns.toUpperCase(); }

// ── Source-gazetteer (namespace) picker: prioritise / restrict, persisted ─────
const NS_LS_KEY = 'whg-recon-ns';
function getNsFilter() {
  if (project && project.nsFilter) return project.nsFilter;
  try { const s = JSON.parse(localStorage.getItem(NS_LS_KEY) || 'null'); if (s && s.mode) return s; } catch (_) { /* */ }
  return { mode: 'all', namespaces: [] };
}
function availableNamespaces() {
  const set = new Set(['gn', 'wd', 'tgn', 'osm', 'ohm', 'pl', 'whg', 'chgis', 'hgis', 'alc', 'gb1900']);
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
function populateSourcesModal() {
  const f = getNsFilter();
  const modeInput = document.querySelector(`input[name="recon-ns-mode"][value="${f.mode}"]`);
  if (modeInput) modeInput.checked = true;
  const box = el('recon-ns-list');
  if (box) box.innerHTML = availableNamespaces().map((ns) =>
    `<label class="recon-ns-item"><input type="checkbox" class="recon-ns-cb" value="${esc(ns)}"${f.namespaces.includes(ns) ? ' checked' : ''}> ` +
    `${esc(NS_NAMES[ns] || ns.toUpperCase())} <span class="text-muted small">(${esc(ns)})</span></label>`).join('');
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
    if (token === _mapToken) mod.renderReviewMap(box, points, rowPoint, (ci) => acceptCandidate(ci));
  } catch (err) { console.error('[recon] review map failed', err); box.classList.add('d-none'); }
}
function acceptCandidate(ci) {
  const meta = reviewMeta[reviewPos]; if (!meta) return;
  const c = (project.matches[meta.key].candidates || [])[ci]; if (!c) return;
  project.decisions = project.decisions || {};
  project.decisions[meta.key] = { status: 'accepted', ci, place_id: c.id, label: c.name, score: c.score };
  afterDecision(true);
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
  const built = buildUniqueQueries();
  if (!built || !built.map.size) {
    setReconSummary('<span class="text-warning">Map a “Place name” column first (Step 2).</span>');
    return;
  }
  project.matches = project.matches || {};
  const entries = [...built.map.entries()].filter(([key]) => !project.matches[key]); // resume: skip done
  const total = built.map.size;
  let done = total - entries.length;

  toggleRunning(true);
  openPane('recon-recon'); // focus the reconcile step (accordion collapses the others)
  stopRequested = false;
  updateProgress(done, total);
  const csrf = getCsrf();

  for (let b = 0; b < entries.length && !stopRequested; b += RECON_BATCH) {
    const slice = entries.slice(b, b + RECON_BATCH);
    const queries = {};
    const nsf = getNsFilter();
    slice.forEach(([, v], j) => {
      const q = { query: v.query, type: 'place', limit: RECON_CAND_LIMIT };
      if (v.country) q.countries = [v.country];
      if (nsf.mode === 'only' && nsf.namespaces.length) q.namespaces = nsf.namespaces; // restrict sources
      queries['q' + j] = q;
    });
    let data;
    try { data = await postReconcile(queries, csrf); }
    catch (err) {
      console.error('[recon] batch failed', err);
      setReconSummary(`<span class="text-danger"><i class="fas fa-exclamation-triangle me-1"></i>Reconciliation stopped: ${esc(err.message)}</span>`);
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

  toggleRunning(false);
  renderResults(built);
  await persist();
  console.log(`[recon] reconciliation ${stopRequested ? 'stopped' : 'complete'}: ${done}/${total} unique queries`);
}

function init() {
  const dz = el('recon-dropzone');
  const input = el('recon-file');
  if (!dz || !input) return; // not on this page

  const openPicker = () => input.click();
  dz.addEventListener('click', openPicker);
  dz.addEventListener('keydown', (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); openPicker(); } });
  input.addEventListener('change', () => { if (input.files[0]) handleFile(input.files[0]); });

  ['dragenter', 'dragover'].forEach((ev) => dz.addEventListener(ev, (e) => { e.preventDefault(); dz.classList.add('recon-dropzone--over'); }));
  ['dragleave', 'drop'].forEach((ev) => dz.addEventListener(ev, (e) => { e.preventDefault(); dz.classList.remove('recon-dropzone--over'); }));
  dz.addEventListener('drop', (e) => { const f = e.dataTransfer && e.dataTransfer.files[0]; if (f) handleFile(f); });

  const clear = el('recon-clear');
  if (clear) clear.addEventListener('click', clearData);
  const startover = el('recon-startover');
  if (startover) startover.addEventListener('click', clearData);

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

  showCapabilities();
  loadSaved();
}

if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
else init();
