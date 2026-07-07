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
let Validate = null; // Ajv LPF validator (Phase: contribution validation) — replicates server schema check
const loadValidate = async () => (Validate || (Validate = await import(/* webpackChunkName: "recon-validate" */ './recon-validate.js')));

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
// Fixed (single-select) roles. Containment is expressed separately as a "contains:<childCol>" role
// (see roleSelectHTML / reconChain) so the spatial hierarchy is agnostic and self-ordering.
const ROLES = [
  ['name', 'Place name'],
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
// 'container' is a transient marker for admin/area columns; initChain() wires the actual containment
// links (contains:<child>) once all columns are known.
const ROLE_HINTS = [
  ['name', /^(place|placename|name|toponym|title|label)s?$/i],
  ['container', /^(county|counties|adm\d|admin\d|region|parish|province|state|district|department|prefecture|municipality|commune|canton|shire|hundred|wapentake|borough)$/i],
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

// Turn the transient 'container' markers into a default containment chain: link the detected
// container columns (in dataset order, coarse → fine) down to the 'name' column via contains:<child>.
// If no name column was detected, the deepest container becomes the name (the toponym reconciled).
function initChain(columns) {
  const containers = columns.map((c, i) => i).filter((i) => columns[i].role === 'container');
  let nameIdx = columns.findIndex((c) => c.role === 'name');
  if (nameIdx < 0 && containers.length) { nameIdx = containers.pop(); columns[nameIdx].role = 'name'; }
  const seq = nameIdx >= 0 ? [...containers, nameIdx] : containers;
  columns.forEach((c) => { if (c.role === 'container') { c.role = 'other'; delete c.child; } }); // any leftover marker
  for (let k = 0; k < seq.length - 1; k++) { columns[seq[k]].role = 'contains'; columns[seq[k]].child = seq[k + 1]; }
  return columns;
}

// Drop any containment link whose child is no longer a valid place/area column (e.g. it was set to
// Ignore or a coordinate/date role, or removed). Cascades: an orphaned container above it is dropped
// too. Keeps the hierarchy consistent after a role change.
function normalizeChain() {
  if (!project) return;
  let changed = true;
  while (changed) {
    changed = false;
    project.columns.forEach((c, i) => {
      if (c.role !== 'contains') return;
      const ch = c.child;
      const valid = Number.isInteger(ch) && ch >= 0 && ch < project.columns.length && ch !== i
        && (project.columns[ch].role === 'name' || project.columns[ch].role === 'contains');
      if (!valid) { c.role = 'other'; delete c.child; changed = true; }
    });
  }
}

// Migrate a project saved under the old model (role 'county' admin columns + project.chainOrder) to
// the containment-link model, preserving the previous parent → child order.
function migrateLegacyChain() {
  if (!project || !project.columns.some((c) => c.role === 'county')) return;
  const admin = project.columns.map((c, i) => i).filter((i) => project.columns[i].role === 'county');
  const order = (project.chainOrder || []).filter((i) => admin.includes(i));
  const seq = [...order, ...admin.filter((i) => !order.includes(i))];
  const nameIdx = project.columns.findIndex((c) => c.role === 'name');
  const full = nameIdx >= 0 ? [...seq, nameIdx] : seq;
  project.columns.forEach((c) => { if (c.role === 'county') c.role = 'other'; });
  for (let k = 0; k < full.length - 1; k++) { project.columns[full[k]].role = 'contains'; project.columns[full[k]].child = full[k + 1]; }
  delete project.chainOrder;
  persist();
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
function isChildRole(role) { return role === 'name' || role === 'contains'; } // a valid containment child
function roleSelectHTML(colIndex, col) {
  const cur = col.role === 'contains' ? `contains:${col.child}` : col.role;
  const std = ROLES.map(([val, label]) => `<option value="${val}"${val === cur ? ' selected' : ''}>${label}</option>`);
  // Containment options: this column CONTAINS another column (its child). Only place/area columns —
  // the place name or another container — are offered as children; ignored/coordinate/date/… columns
  // are not. Selecting one makes THIS column their container (one level up in the hierarchy).
  const links = project.columns.map((c2, j) => (j === colIndex || !isChildRole(c2.role) ? '' :
    `<option value="contains:${j}"${cur === `contains:${j}` ? ' selected' : ''}>↳ Contains “${esc(truncate(c2.name, 22))}”</option>`)).filter(Boolean);
  const grp = links.length ? `<optgroup label="Spatial hierarchy">${links.join('')}</optgroup>` : '';
  return `<select class="form-select form-select-sm recon-role-select role-${col.role}" data-col="${colIndex}">${std.join('')}${grp}</select>`;
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
       <div class="mt-2 d-flex flex-wrap gap-1">
         <button type="button" id="recon-coord-checkall" class="btn btn-sm btn-outline-secondary">
           <i class="fas fa-list-check me-1"></i>Validate all ${project.total.toLocaleString()} rows
         </button>
         ${coordsIdx >= 0 ? '<button type="button" id="recon-coord-insert" class="btn btn-sm btn-outline-primary" title="Add converted WGS84 latitude &amp; longitude as columns in your table"><i class="fas fa-table-columns me-1"></i>Insert WGS84 columns</button>' : ''}
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
  const ins = el('recon-coord-insert');
  if (ins) ins.addEventListener('click', insertWgs84Columns);
}
// Materialise the converted WGS84 lat/lon as real columns (roles lat/lon), superseding the source
// grid-ref column (set to 'ignore'). Appends columns so existing column indices — and thus match keys —
// are unchanged. Records an undo op.
async function insertWgs84Columns() {
  const coordsIdx = colIndexByRole('coords'); if (coordsIdx < 0) return;
  await loadCoords();
  const snap = columnSnapshot();
  const fmt = currentCoordFormat();
  project.columns.push({ name: 'wgs84_lat', role: 'lat' }, { name: 'wgs84_lon', role: 'lon' });
  project.rows.forEach((r) => { const c = Coords.parseCoord(fmt, r[coordsIdx]); r.push(c ? +c.lat.toFixed(6) : '', c ? +c.lon.toFixed(6) : ''); });
  project.columns[coordsIdx].role = 'other'; // superseded by the decimal columns
  project.showIgnored = true;                 // so the (now-ignored) source column stays visible
  normalizeChain();
  pushUndo({ type: 'columns', label: 'add WGS84 columns', snapshot: snap });
  persist(); rerenderData();
  flashSaved('Added wgs84_lat / wgs84_lon columns');
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
       <div class="mt-2 d-flex flex-wrap gap-1">
         <button type="button" id="recon-date-checkall" class="btn btn-sm btn-outline-secondary">
           <i class="fas fa-list-check me-1"></i>Validate all ${project.total.toLocaleString()} rows
         </button>
         <button type="button" id="recon-date-insert" class="btn btn-sm btn-outline-primary" title="Add the parsed ISO start &amp; end dates as columns in your table"><i class="fas fa-table-columns me-1"></i>Insert ISO date columns</button>
       </div>
       <div id="recon-date-report" class="recon-coord-report mt-2"></div>
     </div>`;
  box.classList.remove('d-none');
  const chk = el('recon-date-checkall');
  if (chk) chk.addEventListener('click', checkAllDates);
  const ins = el('recon-date-insert');
  if (ins) ins.addEventListener('click', insertIsoDateColumns);
}
// Materialise the parsed ISO start/end dates as real columns. Appended (indices/keys unchanged), role
// 'other' (data, not a reconciliation hint) but shown via showIgnored. Records an undo op.
async function insertIsoDateColumns() {
  const idx = colIndexByRole('date'); if (idx < 0) return;
  await loadDates();
  const snap = columnSnapshot();
  project.columns.push({ name: 'date_start_iso', role: 'other' }, { name: 'date_end_iso', role: 'other' });
  project.rows.forEach((r) => {
    const raw = r[idx];
    const d = (raw != null && String(raw).trim() !== '') ? Dates.parseDate(raw, { locale: 'uk' }) : null;
    r.push((d && d.startISO) || '', (d && d.endISO) || '');
  });
  project.showIgnored = true;
  pushUndo({ type: 'columns', label: 'add ISO date columns', snapshot: snap });
  persist(); rerenderData();
  flashSaved('Added date_start_iso / date_end_iso columns');
}

// ── Column management — reorder (drag) and delete ─────────────────────────────────────────────────
// Reordering/deleting columns changes column indices, which would break the colIndex-keyed matches /
// decisions / geometry / source-config and the containment `child` references. remapColumns() rebuilds
// ALL of that from an old→new index map so nothing drifts.
let _dragCol = -1;
function rerenderData() {
  renderMapping(); renderPreview(); renderTypePrompt(); renderCoords(); renderDates();
  refreshReconSection(); renderColSwitcher(); refreshReview(); refreshFullMapPane(); refreshExport(); updatePaneSummaries();
}
function columnSnapshot() {
  return {
    columns: project.columns.map((c) => Object.assign({}, c)),
    rows: project.rows.map((r) => r.slice()),
    matches: project.matches, decisions: project.decisions, geom: project.geom, colConfig: project.colConfig,
  };
}
function restoreColumnSnapshot(s) {
  project.columns = s.columns; project.rows = s.rows;
  project.matches = s.matches; project.decisions = s.decisions; project.geom = s.geom; project.colConfig = s.colConfig;
  reconActiveIdx = -1; normalizeChain();
}
// mapping[newIdx] = oldIdx. Columns absent from mapping are deleted.
function remapColumns(mapping) {
  const oldCols = project.columns;
  const inv = {}; // old index -> new index (absent ⇒ deleted)
  mapping.forEach((oldIdx, newIdx) => { inv[oldIdx] = newIdx; });
  project.columns = mapping.map((o) => oldCols[o]);
  project.rows = project.rows.map((r) => mapping.map((o) => r[o]));
  project.columns.forEach((c) => { if (c.child != null) { const nc = inv[c.child]; if (nc == null) { delete c.child; if (c.role === 'contains') c.role = 'other'; } else c.child = nc; } });
  const remapKeyed = (obj) => {
    if (!obj) return obj; const out = {};
    for (const k in obj) { const ci = Number(k.slice(0, k.indexOf(':'))); if (inv[ci] != null) out[inv[ci] + k.slice(k.indexOf(':'))] = obj[k]; }
    return out;
  };
  project.matches = remapKeyed(project.matches);
  project.decisions = remapKeyed(project.decisions);
  project.geom = remapKeyed(project.geom);
  if (project.colConfig) { const nc = {}; for (const k in project.colConfig) { if (inv[k] != null) nc[inv[k]] = project.colConfig[k]; } project.colConfig = nc; }
  reconActiveIdx = -1; normalizeChain();
}
function moveColumn(from, to) {
  if (from === to || from < 0 || to < 0) return;
  const snap = columnSnapshot();
  const order = project.columns.map((_, i) => i);
  const [m] = order.splice(from, 1); order.splice(to, 0, m);
  remapColumns(order);
  pushUndo({ type: 'columns', label: 'reorder columns', snapshot: snap });
  persist(); rerenderData();
}
function deleteColumn(col) {
  if (!project || project.columns.length <= 1) return;
  const name = project.columns[col].name;
  const snap = columnSnapshot();
  remapColumns(project.columns.map((_, i) => i).filter((i) => i !== col));
  pushUndo({ type: 'columns', label: `delete “${name}”`, snapshot: snap });
  persist(); rerenderData();
  flashSaved(`Deleted column “${truncate(name, 24)}”`);
}

// ── Undo / redo (session history of data mutations: transforms, column ops, role changes) ─────────
// Each op is symmetric: applying its inverse both reverts the change AND produces the op needed to
// re-apply it, so the same routine drives undo and redo (swap between the two stacks). Review decisions
// have their own in-card undo and are NOT in this stack. Session-only (not persisted).
let _redoStack = [];
function resetHistory() { _undoStack = []; _redoStack = []; updateUndoButtons(); }
// Apply op's stored "restore", returning the inverse op for the opposite stack.
function applyOpInverse(op) {
  if (op.type === 'cell') { // single-cell edit from the data browser
    const cur = project.rows[op.row][op.col];
    project.rows[op.row][op.col] = op.before;
    return { type: 'cell', col: op.col, row: op.row, before: cur, label: op.label };
  }
  if (op.type === 'cells') {
    const cur = project.rows.map((r) => r[op.col]);
    project.rows.forEach((r, i) => { r[op.col] = op.before[i]; });
    return { type: 'cells', col: op.col, before: cur, label: op.label };
  }
  if (op.type === 'columns') {
    const cur = columnSnapshot();
    restoreColumnSnapshot(op.snapshot);
    return { type: 'columns', snapshot: cur, label: op.label };
  }
  return null;
}
function undo() {
  if (!_undoStack.length) return;
  const op = _undoStack.pop();
  const inv = applyOpInverse(op); if (inv) _redoStack.push(inv);
  persist(); rerenderData(); updateUndoButtons(); flashSaved(`Undid: ${op.label}`);
}
function redo() {
  if (!_redoStack.length) return;
  const op = _redoStack.pop();
  const inv = applyOpInverse(op); if (inv) _undoStack.push(inv);
  persist(); rerenderData(); updateUndoButtons(); flashSaved(`Redid: ${op.label}`);
}
function updateUndoButtons() {
  const u = el('recon-undo'), r = el('recon-redo');
  if (u) { u.disabled = !_undoStack.length; u.title = _undoStack.length ? `Undo: ${_undoStack[_undoStack.length - 1].label}` : 'Nothing to undo'; }
  if (r) { r.disabled = !_redoStack.length; r.title = _redoStack.length ? `Redo: ${_redoStack[_redoStack.length - 1].label}` : 'Nothing to redo'; }
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
  const tbody = el('recon-map-body');
  tbody.innerHTML = project.columns.map((c, i) =>
    `<tr class="recon-map-row${c.role === 'other' ? ' recon-map-ignored' : ''}" data-col="${i}">
       <td class="recon-map-handle" draggable="true" title="Drag to reorder"><i class="fas fa-grip-vertical"></i></td>
       <td class="recon-map-col">${truncate(c.name, 50)}
         <button type="button" class="btn btn-sm btn-link p-0 ms-1 recon-transform-btn" data-col="${i}" title="Clean / transform this column's values"><i class="fas fa-wand-magic-sparkles"></i></button>
       </td>
       <td>${roleSelectHTML(i, c)}</td>
       <td class="text-muted">${truncate(firstSample(i), 60)}</td>
       <td class="text-end"><button type="button" class="btn btn-sm btn-link p-0 recon-col-del" data-col="${i}" title="Delete this column"><i class="fas fa-trash-alt text-danger"></i></button></td>
     </tr>`).join('');

  // Drag-to-reorder (handle initiates; rows are drop targets) — works for ignored columns too.
  tbody.querySelectorAll('.recon-map-handle').forEach((h) => h.addEventListener('dragstart', (e) => {
    _dragCol = Number(h.closest('tr').dataset.col); e.dataTransfer.effectAllowed = 'move';
    try { e.dataTransfer.setData('text/plain', String(_dragCol)); } catch (_) { /* ignore */ }
  }));
  tbody.querySelectorAll('.recon-map-row').forEach((tr) => {
    tr.addEventListener('dragover', (e) => { if (_dragCol < 0) return; e.preventDefault(); e.dataTransfer.dropEffect = 'move'; tr.classList.add('recon-map-dropover'); });
    tr.addEventListener('dragleave', () => tr.classList.remove('recon-map-dropover'));
    tr.addEventListener('drop', (e) => { e.preventDefault(); tr.classList.remove('recon-map-dropover'); const to = Number(tr.dataset.col); if (_dragCol >= 0 && _dragCol !== to) moveColumn(_dragCol, to); _dragCol = -1; });
    tr.addEventListener('dragend', () => { tbody.querySelectorAll('.recon-map-row').forEach((x) => x.classList.remove('recon-map-dropover')); _dragCol = -1; });
  });
  tbody.querySelectorAll('.recon-col-del').forEach((b) => b.addEventListener('click', (e) => { e.stopPropagation(); deleteColumn(Number(b.dataset.col)); }));
  tbody.querySelectorAll('.recon-transform-btn').forEach((b) => b.addEventListener('click', () => openTransformModal(Number(b.dataset.col))));
  tbody.querySelectorAll('.recon-role-select').forEach((sel) => {
    sel.addEventListener('change', () => {
      const i = Number(sel.dataset.col);
      const snap = columnSnapshot(); // role changes can reset matches → snapshot for undo
      const before = reconChain().join(',');
      const v = sel.value;
      pushUndo({ type: 'columns', label: `role of “${truncate(project.columns[i].name, 20)}”`, snapshot: snap });
      if (v.startsWith('contains:')) { project.columns[i].role = 'contains'; project.columns[i].child = Number(v.slice(9)); }
      else { project.columns[i].role = v; delete project.columns[i].child; }
      sel.className = `form-select form-select-sm recon-role-select role-${project.columns[i].role}`;
      normalizeChain();      // drop any containment links this change orphaned (e.g. an ignored child)
      // A change that alters the reconciliation chain invalidates existing matches (they were scoped by
      // the old containment) — reset them so the columns are reconciled again with the new hierarchy.
      if (reconChain().join(',') !== before && project.matches && Object.keys(project.matches).length) {
        project.matches = {}; project.decisions = {}; reconActiveIdx = -1;
        reconStaleNote = 'Hierarchy changed — reconciliation was reset; reconcile the columns again.';
      }
      renderMapping();       // dropdowns show contextual "Contains …" state; re-render so all stay in sync
      persist();
      renderPreview();       // 'other' (ignore) columns are hidden in the preview
      renderCoords();        // coords/lat/lon mapping affects the coordinate panel
      renderDates();         // date mapping affects the date panel
      refreshReconSection(); // name/country mapping affects what can be reconciled
      renderColSwitcher();   // the chain (which columns/levels) may have changed → refresh the pills
      refreshReview();       // and the review pane's active column may no longer exist
      refreshFullMapPane(); refreshExport();
    });
  });
}

// ── Cell transforms (light, OpenRefine-style) — clean a column's values in place ──────────────────
// A menu of common text transforms plus find/replace (literal or regex) on one column, with a live
// preview. Applying mutates project.rows and records an undo op (before-snapshot). Transforming a column
// that's already reconciled invalidates its matches (they were computed on the old values).
function stripDiacritics(s) { return String(s == null ? '' : s).normalize('NFKD').replace(/[̀-ͯ]/g, ''); }
function titleCase(s) { return String(s == null ? '' : s).toLowerCase().replace(/\b(\p{L})/gu, (m, c) => c.toUpperCase()); }
const CELL_TRANSFORMS = [
  { id: 'trim', label: 'Trim whitespace', fn: (v) => String(v == null ? '' : v).trim() },
  { id: 'collapse', label: 'Collapse spaces', fn: (v) => String(v == null ? '' : v).replace(/\s+/g, ' ').trim() },
  { id: 'upper', label: 'UPPERCASE', fn: (v) => String(v == null ? '' : v).toUpperCase() },
  { id: 'lower', label: 'lowercase', fn: (v) => String(v == null ? '' : v).toLowerCase() },
  { id: 'title', label: 'Title Case', fn: (v) => titleCase(v) },
  { id: 'accents', label: 'Strip accents', fn: (v) => stripDiacritics(v) },
];
let _transformCol = -1;
let _pendingTransform = null; // { fn, label }
let _undoStack = [];          // reversible ops (consumed by the undo/redo feature) — cells snapshots etc.
function pushUndo(op) { _undoStack.push(op); if (_undoStack.length > 50) _undoStack.shift(); _redoStack = []; updateUndoButtons(); }

function openTransformModal(col) {
  _transformCol = col; _pendingTransform = null;
  const m = el('recon-transform-modal'); if (!m) return;
  const title = el('recon-transform-title'); if (title) title.textContent = `Transform column — ${truncate(project.columns[col].name, 40)}`;
  const box = el('recon-transform-common');
  if (box) box.innerHTML = CELL_TRANSFORMS.map((t) => `<button type="button" class="btn btn-sm btn-outline-secondary recon-tf-common" data-id="${t.id}">${esc(t.label)}</button>`).join('');
  const fi = el('recon-tf-find'); if (fi) fi.value = '';
  const ri = el('recon-tf-replace'); if (ri) ri.value = '';
  const rx = el('recon-tf-regex'); if (rx) rx.checked = false;
  const ci = el('recon-tf-case'); if (ci) ci.checked = false;
  renderTransformPreview();
  if (box) box.querySelectorAll('.recon-tf-common').forEach((b) => b.addEventListener('click', () => {
    const t = CELL_TRANSFORMS.find((x) => x.id === b.dataset.id);
    box.querySelectorAll('.recon-tf-common').forEach((x) => x.classList.remove('active', 'btn-primary'));
    b.classList.add('active', 'btn-primary');
    _pendingTransform = { fn: t.fn, label: t.label };
    renderTransformPreview();
  }));
  if (window.bootstrap && window.bootstrap.Modal) window.bootstrap.Modal.getOrCreateInstance(m).show();
}
// Build the transform fn from the find/replace inputs (returns null if 'find' is empty or the regex is bad).
function findReplaceTransform() {
  const find = (el('recon-tf-find') || {}).value || '';
  if (!find) return null;
  const replace = (el('recon-tf-replace') || {}).value || '';
  const useRe = !!(el('recon-tf-regex') || {}).checked;
  const flags = 'g' + ((el('recon-tf-case') || {}).checked ? '' : 'i');
  let re;
  try { re = useRe ? new RegExp(find, flags) : new RegExp(find.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), flags); }
  catch (err) { return { error: err.message }; }
  return { fn: (v) => String(v == null ? '' : v).replace(re, replace), label: `replace “${find}”` };
}
function onFindReplaceInput() {
  el('recon-transform-common').querySelectorAll('.recon-tf-common').forEach((x) => x.classList.remove('active', 'btn-primary'));
  const fr = findReplaceTransform();
  if (fr && fr.error) { _pendingTransform = null; el('recon-transform-preview').innerHTML = `<span class="text-danger small">Invalid regex: ${esc(fr.error)}</span>`; return; }
  _pendingTransform = fr;
  renderTransformPreview();
}
function renderTransformPreview() {
  const box = el('recon-transform-preview'); const applyBtn = el('recon-transform-apply');
  if (!box) return;
  if (!_pendingTransform) { box.innerHTML = '<span class="text-muted small">Choose a transform to preview.</span>'; if (applyBtn) applyBtn.disabled = true; return; }
  const fn = _pendingTransform.fn;
  const col = _transformCol;
  let changed = 0; const samples = []; const seen = new Set();
  for (let i = 0; i < project.rows.length; i++) {
    const old = project.rows[i][col]; const nv = fn(old);
    if (String(old == null ? '' : old) !== String(nv == null ? '' : nv)) {
      changed += 1;
      const k = String(old);
      if (samples.length < 8 && !seen.has(k)) { seen.add(k); samples.push([old, nv]); }
    }
  }
  const rows = samples.map(([o, nv]) => `<div class="recon-tf-prevrow"><span class="recon-tf-before">${esc(truncate(String(o == null ? '' : o), 34))}</span> <i class="fas fa-arrow-right text-muted mx-1"></i> <span class="recon-tf-after">${esc(truncate(String(nv == null ? '' : nv), 34))}</span></div>`).join('');
  box.innerHTML = changed
    ? `<div class="small text-muted mb-1"><strong>${changed.toLocaleString()}</strong> of ${project.rows.length.toLocaleString()} cells will change:</div>${rows}`
    : '<span class="text-muted small">No cells would change.</span>';
  if (applyBtn) applyBtn.disabled = !changed;
}
function applyTransform() {
  if (!_pendingTransform || _transformCol < 0 || !project) return;
  const col = _transformCol; const fn = _pendingTransform.fn;
  const before = project.rows.map((r) => r[col]);
  let changed = 0;
  project.rows.forEach((r) => { const nv = fn(r[col]); if (String(r[col] == null ? '' : r[col]) !== String(nv == null ? '' : nv)) { r[col] = nv; changed += 1; } });
  if (!changed) return;
  pushUndo({ type: 'cells', col, before, label: `${_pendingTransform.label} · ${project.columns[col].name}` });
  // Transforming a reconciled column invalidates its matches (they were run on the old values).
  if (colHasMatches(col)) {
    colKeys(col).forEach((k) => { delete project.matches[k]; if (project.decisions) delete project.decisions[k]; if (project.geom) delete project.geom[k]; });
    invalidateDownstream(col);
    reconStaleNote = 'Column values changed — reconcile the affected column(s) again.';
    reconActiveIdx = -1;
  }
  persist();
  // Values changed everywhere → re-render the dependent panes.
  renderMapping(); renderPreview(); renderCoords(); renderDates();
  refreshReconSection(); renderColSwitcher(); refreshReview(); refreshFullMapPane(); refreshExport();
  flashSaved(`${_pendingTransform.label} applied to ${changed.toLocaleString()} cell${changed === 1 ? '' : 's'}`);
}

// ── Data browser (virtualised, filterable, editable) ─────────────────────────────────────────────
// The whole dataset is already in memory (project.rows), so this is DOM virtualisation, not fetching:
// only the visible window of rows lives in the DOM; off-screen rows are evicted and re-rendered on
// scroll. A text filter narrows the view; edit mode commits per-cell changes (undoable, re-invalidating
// any affected matches). Row height is measured once so the scroll maths survive font/border variation.
let _previewFilter = '';        // lower-cased text search across visible columns
let _previewEdit = false;       // edit-mode toggle
let _previewView = null;        // filtered row indices, or null = all rows (identity mapping)
let _previewVisCols = [];       // visible column indices (respects showIgnored)
let _previewColW = [];          // px width per visible column — stable, so windowed rows don't jitter
let _previewRowH = 31;          // measured data-row height
let _previewScrollWired = false;
let _previewEditing = null;     // { ri, ci } cell currently open for editing
const PREVIEW_OVERSCAN = 8;

function previewVisibleCols() {
  const showIgnored = !!project.showIgnored;
  return project.columns.map((c, i) => i).filter((i) => showIgnored || project.columns[i].role !== 'other');
}
function buildPreviewView() {
  const q = _previewFilter;
  if (!q) { _previewView = null; return; }
  const rows = project.rows, vis = _previewVisCols, out = [];
  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    for (let c = 0; c < vis.length; c++) { const v = r[vis[c]]; if (v != null && String(v).toLowerCase().includes(q)) { out.push(i); break; } }
  }
  _previewView = out;
}
function previewColWidth(ci) {
  // Stable width estimate from the header + a capped sample of values, so table-layout:fixed keeps
  // columns aligned no matter which rows the virtualiser happens to have painted.
  let maxLen = String(project.columns[ci].name || '').length;
  const n = Math.min(project.rows.length, 200);
  for (let i = 0; i < n; i++) { const v = project.rows[i][ci]; if (v != null) { const L = String(v).length; if (L > maxLen) maxLen = L; } }
  return Math.max(70, Math.min(320, maxLen * 7 + 22));
}
function renderPreview() {
  if (!project) return;
  const box = el('recon-show-ignored'); if (box) box.checked = !!project.showIgnored;
  _previewVisCols = previewVisibleCols();
  _previewColW = _previewVisCols.map(previewColWidth);
  const table = el('recon-preview-scroll') && el('recon-preview-scroll').querySelector('table');
  if (table) {
    table.style.width = _previewColW.reduce((a, b) => a + b, 0) + 'px';
    let cg = table.querySelector('colgroup');
    if (!cg) { cg = document.createElement('colgroup'); table.insertBefore(cg, table.firstChild); }
    cg.innerHTML = _previewColW.map((w) => `<col style="width:${w}px">`).join('');
  }
  el('recon-preview-head').innerHTML = '<tr>' + _previewVisCols.map((i) =>
    `<th title="${esc(project.columns[i].name)}">${truncate(project.columns[i].name, 40)}</th>`).join('') + '</tr>';
  buildPreviewView();
  const scroll = el('recon-preview-scroll');
  if (scroll) scroll.classList.toggle('recon-editing', _previewEdit);
  if (!_previewScrollWired && scroll) {
    let raf = null;
    scroll.addEventListener('scroll', () => { if (raf) return; raf = requestAnimationFrame(() => { raf = null; paintPreviewWindow(); }); });
    _previewScrollWired = true;
  }
  paintPreviewWindow();
  if (measurePreviewRowH()) paintPreviewWindow(); // self-correct the row height, then repaint once
  updatePreviewCount();
}
function measurePreviewRowH() {
  const tr = el('recon-preview-body') && el('recon-preview-body').querySelector('tr[data-ri]');
  if (tr) { const h = tr.getBoundingClientRect().height; if (h && Math.abs(h - _previewRowH) > 0.5) { _previewRowH = h; return true; } }
  return false;
}
function paintPreviewWindow() {
  const scroll = el('recon-preview-scroll'), body = el('recon-preview-body');
  if (!scroll || !body || !project) return;
  const vis = _previewVisCols, view = _previewView, nc = vis.length;
  const total = view ? view.length : project.rows.length;
  const rowH = _previewRowH || 31;
  const vh = scroll.clientHeight || 420;
  const first = Math.max(0, Math.floor(scroll.scrollTop / rowH) - PREVIEW_OVERSCAN);
  const last = Math.min(total, first + Math.ceil(vh / rowH) + PREVIEW_OVERSCAN * 2);
  const parts = [`<tr class="recon-vspacer"><td colspan="${nc}" style="height:${first * rowH}px"></td></tr>`];
  for (let vi = first; vi < last; vi++) {
    const ri = view ? view[vi] : vi;
    const r = project.rows[ri];
    let tds = '';
    for (let c = 0; c < nc; c++) {
      const ci = vis[c]; const raw = r[ci];
      if (project.columns[ci].role === 'type') {
        const types = rowTypesFor(ri);
        if (types.length) {
          tds += `<td data-ci="${ci}" class="recon-type-cell" title="${esc(types.map((t) => t.text).join(', '))}">` +
            types.map((t) => `<span class="recon-type-chip">${truncate(t.text, 22)}</span>`).join(' ') + '</td>';
        } else {
          const hint = raw != null && String(raw).trim() !== '' ? `<span class="recon-type-hint">${truncate(raw, 22)}</span>` : '';
          tds += `<td data-ci="${ci}" class="recon-type-cell recon-type-empty" title="${esc(raw)}">${hint}<span class="recon-type-assign">+ type</span></td>`;
        }
      } else {
        tds += `<td data-ci="${ci}" title="${esc(raw)}">${truncate(raw, 60)}</td>`;
      }
    }
    parts.push(`<tr data-ri="${ri}">${tds}</tr>`);
  }
  parts.push(`<tr class="recon-vspacer"><td colspan="${nc}" style="height:${Math.max(0, (total - last) * rowH)}px"></td></tr>`);
  body.innerHTML = parts.join('');
}
function updatePreviewCount() {
  const c = el('recon-preview-count'); if (!c || !project) return;
  const total = project.rows.length;
  const shown = _previewView ? _previewView.length : total;
  c.textContent = _previewFilter ? `${shown.toLocaleString()} of ${total.toLocaleString()} rows` : `${total.toLocaleString()} row${total === 1 ? '' : 's'}`;
}

// ── Data-browser edit mode ────────────────────────────────────────────────────────────────────────
let _persistTimer = null;
function schedulePersist() { clearTimeout(_persistTimer); _persistTimer = setTimeout(() => { _persistTimer = null; persist(); }, 400); }
// A cell edit in a reconciled column makes that row's match (and its children's, via containment) stale.
// Drop just those per-row entries — not the whole column — so one edit doesn't discard thousands of good
// matches. Returns true if anything was cleared (cf. applyTransform's whole-column analogue).
function invalidateRowMatches(col, row) {
  const chain = reconChain();
  const pos = chain.indexOf(col);
  if (pos < 0) return false;
  let changed = false;
  for (let p = pos; p < chain.length; p++) {
    const k = chain[p] + ':' + row;
    if (project.matches && project.matches[k]) { delete project.matches[k]; changed = true; }
    if (project.decisions) delete project.decisions[k];
    if (project.geom) delete project.geom[k];
  }
  if (changed) reconStaleNote = 'A value changed after matching — re-reconcile the affected column(s) to refresh those rows.';
  return changed;
}
function setPreviewEdit(on) {
  _previewEdit = !!on;
  if (!_previewEdit) cancelCellEdit();
  const scroll = el('recon-preview-scroll'); if (scroll) scroll.classList.toggle('recon-editing', _previewEdit);
  const btn = el('recon-preview-edit');
  if (btn) {
    btn.classList.toggle('recon-preview-edit--on', _previewEdit);
    btn.setAttribute('aria-pressed', _previewEdit ? 'true' : 'false');
    btn.innerHTML = _previewEdit ? '<i class="fas fa-check me-1"></i>Done editing' : '<i class="fas fa-pen-to-square me-1"></i>Edit cells';
  }
}
function previewCellEl(ri, ci) {
  const body = el('recon-preview-body'); if (!body) return null;
  const tr = body.querySelector(`tr[data-ri="${ri}"]`);
  return tr ? tr.querySelector(`td[data-ci="${ci}"]`) : null;
}
function startCellEdit(ri, ci) {
  cancelCellEdit();
  const td = previewCellEl(ri, ci); if (!td) return;
  _previewEditing = { ri, ci };
  td.classList.add('recon-cell-editing');
  td.innerHTML = `<input class="recon-cell-input" type="text" value="${esc(project.rows[ri][ci])}">`;
  const inp = td.querySelector('input');
  inp.focus(); inp.select();
  inp.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') { e.preventDefault(); commitCellEdit(); startCellEditRelative(ri, ci, 1, 0); }
    else if (e.key === 'Tab') { e.preventDefault(); commitCellEdit(); startCellEditRelative(ri, ci, 0, e.shiftKey ? -1 : 1); }
    else if (e.key === 'Escape') { e.preventDefault(); cancelCellEdit(); }
  });
  inp.addEventListener('blur', () => { if (_previewEditing && _previewEditing.ri === ri && _previewEditing.ci === ci) commitCellEdit(); });
}
function cancelCellEdit() {
  if (!_previewEditing) return;
  const { ri, ci } = _previewEditing; _previewEditing = null;
  const td = previewCellEl(ri, ci);
  if (td) { td.classList.remove('recon-cell-editing'); td.innerHTML = truncate(project.rows[ri][ci], 60); }
}
function commitCellEdit() {
  if (!_previewEditing) return;
  const { ri, ci } = _previewEditing; _previewEditing = null;
  const td = previewCellEl(ri, ci); const inp = td && td.querySelector('input');
  if (!inp) return;
  const before = project.rows[ri][ci], after = inp.value;
  if (String(before == null ? '' : before) === String(after)) { td.classList.remove('recon-cell-editing'); td.innerHTML = truncate(before, 60); return; }
  pushUndo({ type: 'cell', col: ci, row: ri, before, label: `edit ${project.columns[ci].name}` });
  project.rows[ri][ci] = after;
  const invalidated = invalidateRowMatches(ci, ri);
  td.classList.remove('recon-cell-editing'); td.title = esc(after); td.innerHTML = truncate(after, 60);
  schedulePersist();
  refreshAfterCellEdit(ci, invalidated);
}
function startCellEditRelative(ri, ci, dRow, dCol) {
  if (!_previewEdit) return;
  const view = _previewView, vis = _previewVisCols;
  const total = view ? view.length : project.rows.length;
  let vi = view ? view.indexOf(ri) : ri;
  let cIdx = vis.indexOf(ci);
  if (vi < 0 || cIdx < 0) return;
  vi += dRow; cIdx += dCol;
  if (cIdx < 0) { cIdx = vis.length - 1; vi -= 1; }
  else if (cIdx >= vis.length) { cIdx = 0; vi += 1; }
  if (vi < 0 || vi >= total) return;
  ensurePreviewRowVisible(vi);
  paintPreviewWindow();
  startCellEdit(view ? view[vi] : vi, vis[cIdx]);
}
function ensurePreviewRowVisible(vi) {
  const scroll = el('recon-preview-scroll'); if (!scroll) return;
  const rowH = _previewRowH || 31, y = vi * rowH;
  const headH = (el('recon-preview-head') && el('recon-preview-head').offsetHeight) || 0;
  if (y < scroll.scrollTop) scroll.scrollTop = y;
  else if (y + rowH > scroll.scrollTop + scroll.clientHeight - headH) scroll.scrollTop = y + rowH - scroll.clientHeight + headH;
}
function refreshAfterCellEdit(ci, invalidated) {
  const role = project.columns[ci].role;
  if (role === 'coords' || role === 'lat' || role === 'lon') renderCoords();
  if (role === 'date') renderDates();
  if (invalidated) { reconActiveIdx = -1; refreshReconSection(); renderColSwitcher(); refreshReview(); refreshFullMapPane(); }
  refreshExport(); updatePaneSummaries();
}

function renderAll() {
  el('recon-result').classList.remove('d-none');
  resetFilters();  // a freshly loaded/imported project starts unfiltered (filters are session-only)
  resetHistory();  // undo/redo history is session-only, cleared on load
  // Data-browser state is session-only too: clear filter/edit-mode and scroll back to the top.
  _previewFilter = ''; _previewView = null; _previewEditing = null; setPreviewEdit(false);
  const psearch = el('recon-preview-search'); if (psearch) psearch.value = '';
  const pscroll = el('recon-preview-scroll'); if (pscroll) pscroll.scrollTop = 0;
  const delimNote = project.delimiter ? ` · delimiter <code>${project.delimiter === '\t' ? 'TAB' : project.delimiter}</code>` : ' · JSON';
  el('recon-summary').innerHTML =
    `<strong>${truncate(project.fileName, 60)}</strong> — <strong>${project.total.toLocaleString()}</strong> ` +
    `row${project.total === 1 ? '' : 's'} · <strong>${project.columns.length}</strong> ` +
    `column${project.columns.length === 1 ? '' : 's'}${delimNote} · imported ${fmtTime(project.importedAt)}.`;
  renderMapping();
  renderCoords();
  renderDates();
  renderPreview();
  renderTypePrompt();
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
  // Validate the LPF against WHG's schema when the export/contribute pane is opened.
  if (id === 'recon-export') runValidation();
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
        columns: initChain(parsed.columns.map((name) => ({ name, role: detectRole(name) }))),
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
    // Coordinates + ISO dates are materialised as columns in Step 2, so they're no longer export toggles.
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
  const dateIdx = colIndexByRole('date');
  // Coordinates (for LPF geometry + map) and parsed dates (for LPF `when`) are always derived from the
  // column roles — users materialise them as columns in Step 2 (the coordinate/date panels) if they want
  // them in a CSV/JSON export, so they are no longer per-export toggles.
  if (hasCoordRole()) await loadCoords();
  if (dateIdx >= 0) await loadDates();

  const augHeaders = [];
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

    // Parsed ISO start/end for the LPF `when` (always, when a date column exists).
    if (dateIdx >= 0) {
      const raw = project.rows[i][dateIdx];
      const d = (raw != null && String(raw).trim() !== '') ? Dates.parseDate(raw, { locale: 'uk' }) : null;
      whenStart = (d && d.startISO) || '';
      whenEnd = (d && d.endISO) || '';
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
        if (!id) { const m = matches[key]; if (m && m.top && isAutoConfirmed(m.top, getThreshold(), m.candidates)) { id = m.top.id; title = m.top.name; } }
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
    records.push({ row: i, orig, aug, coord, geom, whenStart, whenEnd, match });
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
  const countryIdx = colIndexByRole('country'), countyIdx = primaryAdminCol();
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
// Build the LPF FeatureCollection OBJECT (used both for serialisation and for in-browser validation).
function buildLPF(data) {
  const idIdx = colIndexByRole('id'), nameIdx = colIndexByRole('name'), countryIdx = colIndexByRole('country');
  const features = data.records.map((rec, i) => {
    const title = nameIdx >= 0 ? String(rec.orig[nameIdx] || '') : '';
    const cc = countryIdx >= 0 && isCcode(rec.orig[countryIdx]) ? [String(rec.orig[countryIdx]).toUpperCase()] : [];
    const props = { title };
    if (cc.length) props.ccodes = cc;
    // LPF @id must be a URL or a namespace term (word:term). A bare id-column value or row index isn't,
    // so wrap non-conforming ids as `row:<value>` (a within-dataset local identifier).
    let atId = String(idIdx >= 0 && rec.orig[idIdx] != null && rec.orig[idIdx] !== '' ? rec.orig[idIdx] : (i + 1));
    if (!/^\w+:[^\s]+$/.test(atId) && !/^https?:\/\//.test(atId)) atId = 'row:' + atId.trim().replace(/\s+/g, '_');
    const feat = {
      '@id': atId,
      type: 'Feature',
      properties: props,
      names: title ? [{ toponym: title }] : [],
    };
    // Place types: assigned per row in the table editor (project.rowTypes, keyed by source row index).
    const rt = rowTypesFor(rec.row);
    if (rt.length) feat.types = rt.map((t) => ({ identifier: t.id, label: t.text }));  // LPF place types (needed to contribute)
    if (rec.whenStart || rec.whenEnd) feat.when = { timespans: [{ start: { in: rec.whenStart || undefined }, end: { in: rec.whenEnd || undefined } }] };
    // Dataset-scope PeriodO period(s) apply to every place (scope-level, not per row).
    const scp = scopePeriods();
    if (scp.length) { feat.when = feat.when || {}; feat.when.periods = scp.map((p) => { const o = { name: p.label }; if (p.uri) o['@id'] = p.uri; return o; }); }
    if (rec.geom) feat.geometry = rec.geom;                              // override (point / line / polygon) wins
    else if (rec.coord) feat.geometry = { type: 'Point', coordinates: [+rec.coord.lon.toFixed(6), +rec.coord.lat.toFixed(6)] };
    if (rec.match) feat.links = rec.match.list.map((x) => ({ type: 'closeMatch', identifier: x.id }));
    return feat;
  });
  return {
    type: 'FeatureCollection',
    '@context': 'https://raw.githubusercontent.com/LinkedPasts/linked-places-format/master/linkedplaces-context-v1.1.jsonld',
    features,
  };
}
function serializeLPF(data) { return JSON.stringify(buildLPF(data), null, 2); }

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
      { match: true, enrich: false },
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
  ['recon-exp-match', 'recon-exp-enrich'].forEach((id) => { const box = el(id); if (box) box.disabled = !hasMatches; });
  const sum = el('recon-pane-sum-export');
  if (sum) {
    let accepted = 0;
    if (project.decisions) Object.values(project.decisions).forEach((d) => { if (d.status === 'accepted') accepted += 1; });
    sum.textContent = hasMatches ? `${accepted.toLocaleString()} confirmed match${accepted === 1 ? '' : 'es'}` : 'augmented columns ready';
  }
}

// ── Contribution validation: check the LPF against WHG's schema before enabling "Contribute" ──────
// The server rejects LPF that fails validation/static/lpf_v2.0.jsonld (jsonschema Draft-7). We run the
// same schema in the browser (Ajv) plus friendly per-record requirement checks, so problems surface —
// and the Contribute button is gated — before anything is submitted.
let _validation = null;
async function runValidation() {
  if (!project) return null;
  const body = el('recon-validate-body');
  if (body) body.innerHTML = '<span class="text-muted small"><i class="fas fa-spinner fa-spin me-1"></i>checking…</span>';
  let data;
  try {
    data = await buildExportRecords({ match: true, enrich: false }, null);
  } catch (e) {
    if (body) body.innerHTML = `<span class="text-danger small">Could not build the Linked Places file: ${esc(e.message)}</span>`;
    return null;
  }
  const fc = buildLPF(data);
  const n = fc.features.length;
  // Friendly per-feature requirement counts (each of these is required by the LPF schema).
  const miss = { title: 0, names: 0, geometry: 0, when: 0, types: 0 };
  fc.features.forEach((f) => {
    if (!(f.properties && f.properties.title)) miss.title += 1;
    if (!(f.names && f.names.length)) miss.names += 1;
    if (!f.geometry) miss.geometry += 1;
    if (!f.when) miss.when += 1;
    if (!(f.types && f.types.length) && !(f.properties && f.properties.fclasses)) miss.types += 1;
  });
  // Authoritative schema pass (Ajv, same schema the server uses). Null if the validator can't load.
  let schemaOk = null; let schemaErrs = 0; let schemaSummary = [];
  try { const mod = await loadValidate(); const res = await mod.validateLPF(fc); schemaOk = res.ok; schemaErrs = res.errorCount; schemaSummary = res.summary; }
  catch (e) { console.error('[recon] LPF validator unavailable', e); }
  _validation = { total: n, miss, schemaOk, schemaErrs, schemaSummary };
  renderValidation(_validation);
  updateContributeGate();
  return _validation;
}
const VALIDATE_LABELS = {
  title: 'have no place name (title)',
  names: 'have no name variant',
  geometry: 'have no location (coordinates or drawn geometry)',
  when: 'have no date/period',
  types: 'have no place type (AAT) — needed only to contribute',
};
function renderValidation(v) {
  const body = el('recon-validate-body'); if (!body || !v) return;
  const issues = Object.keys(VALIDATE_LABELS)
    .filter((k) => v.miss[k] > 0)
    .map((k) => `<li${k === 'types' ? ' class="fw-semibold"' : ''}><strong>${v.miss[k].toLocaleString()}</strong> of ${v.total.toLocaleString()} ${VALIDATE_LABELS[k]}</li>`);
  const valid = v.schemaOk === true && !issues.length;
  if (valid) {
    body.innerHTML = `<div class="text-success"><i class="fas fa-circle-check me-1"></i><strong>Ready to contribute.</strong> All ${v.total.toLocaleString()} places pass WHG's Linked Places validation.</div>`;
    return;
  }
  // Schema-error groups the friendly checks don't already cover (e.g. bad @id, malformed dates).
  const covered = /place name|name variant|location|date\/period|place type/;
  const schemaLines = (v.schemaSummary || [])
    .filter((g) => !covered.test(g.msg))
    .slice(0, 6)
    .map((g) => `<li>${esc(g.msg)}${g.count > 1 ? ` <span class="text-muted">(${g.count}×)</span>` : ''}</li>`);
  const note = v.schemaOk == null ? ' <span class="text-muted">(schema check unavailable)</span>' : '';
  body.innerHTML = `<div class="text-danger mb-1"><i class="fas fa-triangle-exclamation me-1"></i><strong>Not ready to contribute.</strong>${note}</div>` +
    ((issues.length || schemaLines.length) ? `<ul class="recon-validate-issues small mb-1">${issues.join('')}${schemaLines.join('')}</ul>` : '') +
    '<div class="small text-muted">Resolve the items above (assign a place type to each row in the table at Step 2, map a coordinate/date column, …), then re-check.</div>';
}
// Enable "Contribute to WHG" only when the LPF passes validation. Manual Export stays available.
function updateContributeGate() {
  const btn = el('recon-contribute-btn'); if (!btn) return;
  const ok = _validation && _validation.schemaOk === true &&
    !Object.keys(VALIDATE_LABELS).some((k) => _validation.miss[k] > 0);
  // schemaOk === null (validator failed to load) → don't hard-block; let the server be the gate.
  const block = _validation && _validation.schemaOk !== null && !ok;
  btn.disabled = !!block;
  btn.title = block ? 'Resolve the validation issues above before contributing' : '';
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
  const nameCol = colIndexByRole('name'), countyIdx = primaryAdminCol(), dateIdx = colIndexByRole('date');
  const decisions = project.decisions || {}, matches = project.matches || {};
  if (hasCoordRole()) await loadCoords();
  const fbuilt = buildUniqueQueries(); // active column, for filter predicates
  const feats = [];
  for (let i = 0; i < project.rows.length; i++) {
    if (fbuilt && filtersActive() && fbuilt.map.has(fbuilt.colIndex + ':' + i) && !rowPasses(i, fbuilt)) continue;
    const ov = project.geom && project.geom[nameCol + ':' + i];
    const c = ov ? firstLngLat(ov.geometry) : (hasCoordRole() ? rowCoordValue(i) : null);
    if (!c) continue;
    const key = nameCol + ':' + i;
    const acc = acceptedList(decisions[key])[0]
      || (matches[key] && matches[key].top && isAutoConfirmed(matches[key].top, getThreshold(), matches[key].candidates) ? { label: matches[key].top.name, score: matches[key].top.score } : null);
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
      migrateLegacyChain(); // convert old 'county'-role + chainOrder projects to contains: links
      normalizeChain();     // defensively drop any orphaned containment links
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
// The spatial hierarchy is expressed per-column: a container column has role 'contains' with a
// `child` index (the column it directly contains). The reconciliation chain is DERIVED by walking
// those links from the 'name' (leaf) column up to the root — coarsest parent first, name last. Each
// child column's query is scoped by the parent's resolved place_id (contained_in), so e.g. a Parish is
// matched within its County. Re-ordering the hierarchy is just editing the 'contains' links (no sorter).
function reconChain() {
  if (!project) return [];
  const nameIdx = colIndexByRole('name');
  if (nameIdx < 0) return [];
  const chain = [nameIdx];
  let cur = nameIdx, guard = 0;
  while (guard++ < project.columns.length) {
    const parent = project.columns.findIndex((c) => c.role === 'contains' && c.child === cur);
    if (parent < 0 || chain.includes(parent)) break; // reached the root, or a cycle
    chain.unshift(parent);
    cur = parent;
  }
  return chain;
}
// The primary admin column for display (place popups / export context): the coarsest container (root).
function primaryAdminCol() { const chain = reconChain(); return chain.length >= 2 ? chain[0] : -1; }
let reconActiveIdx = -1; // which chain position the review/results panes focus; -1 → derive (current stage)
function activeReconCol() {
  const chain = reconChain();
  if (!chain.length) return -1;
  if (reconActiveIdx >= 0 && reconActiveIdx < chain.length) return chain[reconActiveIdx]; // explicit focus
  const p = currentStagePos(); // default: the column you'd act on next, else the last
  return chain[p < chain.length ? p : chain.length - 1];
}
// The place_id resolved for a (column,row): an explicit accept, else an auto-confirmed top match.
function resolvedPlaceId(colIndex, rowIdx) {
  return resolvedPlaceIds(colIndex, rowIdx)[0] || null;
}
// ALL resolved place_ids for a (column,row) — a row may closeMatch several records (multi-accept),
// and every one is passed as containment for the child column so it's scoped "within any of them".
function resolvedPlaceIds(colIndex, rowIdx) {
  const key = colIndex + ':' + rowIdx;
  const acc = acceptedList(project.decisions && project.decisions[key]);
  if (acc.length) return acc.map((a) => a.place_id).filter(Boolean);
  const m = project.matches && project.matches[key];
  if (m && m.top && isAutoConfirmed(m.top, getThreshold(), m.candidates)) return [m.top.id];
  return [];
}
// The /reconcile containment resolver expects a BARE gazetteer id (e.g. `ukhc:CMB`, `wd:Q23306`,
// `5297709`), but our candidate ids carry a `place:` prefix. Passing the prefixed form fails to resolve
// the container, so the service silently returns UN-contained results (a Parish query "within" a county
// then matches same-named parishes in *other* counties). Strip the prefix before sending `contained_in`.
// Keep the prefixed id everywhere else (it is the identifier we surface and export). See place#111.
function barePlaceId(id) { return typeof id === 'string' && id.startsWith('place:') ? id.slice(6) : id; }

// ── Stage state machine (iterative, review-gated reconciliation) ─────────────
// A column is reconciled only after the column above it in the chain has been reviewed & confirmed,
// so each child inherits containment from confirmed parents. State is DERIVED from matches/decisions
// (never stored) so it can't drift from the source of truth.
//   locked    — a parent isn't confirmed yet; can't reconcile/review this column
//   ready     — parent confirmed (or top of chain); reconcile can run here
//   review    — reconciled, but rows still need decisions
//   confirmed — every sub-threshold row decided (auto-confirmed rows count automatically)
function colKeys(col) {
  const out = [];
  if (project && project.matches) for (const k in project.matches) { if (k.slice(0, k.indexOf(':')) === String(col)) out.push(k); }
  return out;
}
function colHasMatches(col) { return colKeys(col).length > 0; }
function colPendingReview(col) { let n = 0; colKeys(col).forEach((k) => { if (needsReview(k)) n += 1; }); return n; }
function columnState(pos) {
  const chain = reconChain();
  const col = chain[pos];
  if (!colHasMatches(col)) return (pos === 0 || columnState(pos - 1) === 'confirmed') ? 'ready' : 'locked';
  return colPendingReview(col) > 0 ? 'review' : 'confirmed';
}
// First chain position that's actionable (ready or in review); === chain.length when all confirmed.
function currentStagePos() {
  const chain = reconChain();
  for (let p = 0; p < chain.length; p++) { const s = columnState(p); if (s === 'ready' || s === 'review') return p; }
  return chain.length;
}
// Changing a confirmed parent's decision makes already-reconciled child columns stale (their
// containment used the old parent ids). Clear those children's matches/decisions/geom so they
// re-lock and must be reconciled again with the corrected containment. Returns true if anything was
// cleared.
let reconStaleNote = '';
function invalidateDownstream(col) {
  const chain = reconChain();
  const pos = chain.indexOf(col);
  if (pos < 0 || pos >= chain.length - 1) return false;
  let changed = false;
  for (let p = pos + 1; p < chain.length; p++) {
    const c = String(chain[p]);
    if (project.matches) for (const k in project.matches) { if (k.slice(0, k.indexOf(':')) === c) { delete project.matches[k]; changed = true; } }
    if (project.decisions) for (const k in project.decisions) { if (k.slice(0, k.indexOf(':')) === c) delete project.decisions[k]; }
    if (project.geom) for (const k in project.geom) { if (k.slice(0, k.indexOf(':')) === c) delete project.geom[k]; }
  }
  return changed;
}

// ── Dataset-wide scope (country / date / feature-type / region) ───────────────
// A single filter applied to EVERY reconcile query, narrowing the whole dataset to a place, period, or
// kind before per-row matching. Lives on `project.scope` (so it persists with the project). The spatial
// region can be expressed three ways — a set of country codes, a WHG place chosen by name (used as a
// `contained_in` container), or a polygon drawn on a map (used as `bounds`). See place#111.
function defaultScope() {
  // types.selected = the AAT concepts the user picked ({id:'aat:…', text}); types.ids = those expanded
  // to include all descendants (what the query actually filters on, since types.identifier is exact-match).
  // periods = dataset-scope PeriodO periods ({id:'period:…', uri, label, start, stop}); scope-level only
  // (not per row). Selecting one seeds start/end and travels into LPF `when.periods`.
  return { region: { mode: 'none', ccodes: [], place: null, geometry: null }, start: null, end: null, undated: false, types: { selected: [], ids: [] }, periods: [] };
}
function getScope() { return (project && project.scope) || null; }
function scopeRegion() { const s = getScope(); return (s && s.region) || { mode: 'none' }; }
// Is any facet of the scope actually constraining the query?
function scopeActive() {
  const s = getScope(); if (!s) return false;
  const r = s.region || {};
  const hasRegion = (r.mode === 'ccodes' && r.ccodes && r.ccodes.length) || (r.mode === 'whg' && r.place) || (r.mode === 'draw' && r.geometry);
  const hasTypes = s.types && s.types.selected && s.types.selected.length;
  const hasPeriods = s.periods && s.periods.length;
  return !!(hasRegion || s.start != null || s.end != null || hasTypes || hasPeriods);
}
function scopePeriods() { const s = getScope(); return (s && s.periods) || []; }
function scopeTypes() { const s = getScope(); return (s && s.types) || { selected: [], ids: [] }; }
// Short human summary for the Scope button label (kept compact).
function scopeSummary() {
  const s = getScope(); if (!s) return '';
  const bits = [];
  const r = s.region || {};
  if (r.mode === 'ccodes' && r.ccodes && r.ccodes.length) bits.push(r.ccodes.slice(0, 3).join(', ') + (r.ccodes.length > 3 ? '…' : ''));
  else if (r.mode === 'whg' && r.place) bits.push('in ' + truncateText(r.place.title, 16));
  else if (r.mode === 'draw' && r.geometry) bits.push('drawn area');
  const per = (s.periods && s.periods) || [];
  if (per.length === 1) bits.push(truncateText(per[0].label, 16));
  else if (per.length > 1) bits.push(per.length + ' periods');
  else if (s.start != null || s.end != null) bits.push((s.start != null ? s.start : '…') + '–' + (s.end != null ? s.end : '…'));
  const sel = (s.types && s.types.selected) || [];
  if (sel.length === 1) bits.push(truncateText(sel[0].text, 16));
  else if (sel.length > 1) bits.push(sel.length + ' types');
  return bits.join(' · ');
}
// Plain (un-escaped, un-truncated-to-HTML) truncation helper for building label strings.
function truncateText(v, max) { const r = String(v == null ? '' : v); return r.length > max ? r.slice(0, max - 1) + '…' : r; }

// Mutate a per-row reconcile query `q` with the dataset-wide scope. Attribute filters (country, date,
// feature class) apply to every column; the spatial *region* applies only to the ROOT column, because
// child columns are already spatially scoped by their parent's confirmed places (`contained_in`) and a
// dataset-wide region would either duplicate or fight that. Never overrides a per-row country hint or
// an existing containment. `isRoot` = this column has no parent; `hasRowCountry` = the row set q.countries.
function applyGlobalScopeToQuery(q, isRoot, hasRowCountry) {
  const s = getScope(); if (!s) return;
  const r = s.region || {};
  // Country codes — a dataset-wide default; a per-row country hint always wins.
  if (!hasRowCountry && !q.countries && r.mode === 'ccodes' && r.ccodes && r.ccodes.length) q.countries = r.ccodes.slice();
  // AAT place types (already expanded to descendants). Both back-ends filter on types.identifier.
  if (!q.types && s.types && s.types.ids && s.types.ids.length) q.types = s.types.ids.slice();
  // Temporal window. Legacy WHG ES needs `temporal` + `start`/`end`; the CRC gateway reads `start`/`end`.
  if (s.start != null || s.end != null) {
    q.temporal = true;
    if (s.start != null) q.start = s.start;
    if (s.end != null) q.end = s.end;
    if (s.undated) q.undated = true;
  }
  // Spatial region — root column only, and never over an existing parent containment.
  if (isRoot && !q.contained_in) {
    if (r.mode === 'whg' && r.place && r.place.id) { q.contained_in = [barePlaceId(r.place.id)]; q.containment = 'fuzzy'; q.relation = 'within'; }
    else if (r.mode === 'draw' && r.geometry) { q.bounds = r.geometry; }
  }
}

// Changing the scope invalidates ALL existing matches (every query was run under the old scope). Wipe
// matches/decisions/geometry across every column so the whole dataset is reconciled again — mirrors the
// hierarchy-change reset. Returns true if anything was cleared.
function invalidateAllMatches() {
  if (!project) return false;
  const had = (project.matches && Object.keys(project.matches).length) || (project.decisions && Object.keys(project.decisions).length);
  project.matches = {}; project.decisions = {}; project.geom = {};
  reconActiveIdx = -1;
  return !!had;
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
  const rr = el('recon-rerun'); if (rr && on) rr.classList.add('d-none'); // restored by updateRerunButton after the run
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
// Auto-confirm a top candidate when the name matched exactly, or its score clears the threshold —
// UNLESS another DISTINCT candidate ties the top score. An exact tie between different places (e.g.
// "Devon" in GB vs AU, both 100) is genuinely ambiguous and belongs in review, not an auto-guess.
// Same-place duplicates from multiple sources (identical name + description) are NOT ambiguous.
function isAutoConfirmed(top, threshold, cands) {
  if (!top) return false;
  if (!(top.match || Number(top.score) >= threshold)) return false;
  if (cands && cands.length > 1) {
    const t = cands[0];
    for (let i = 1; i < cands.length && Number(cands[i].score) >= Number(t.score); i++) {
      if (cands[i].name !== t.name || (cands[i].description || '') !== (t.description || '')) return false;
    }
  }
  return true;
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
  if (m && m.top) return isAutoConfirmed(m.top, getThreshold(), m.candidates) ? 'auto' : 'candidate';
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
  if (!dec) { const m = project.matches && project.matches[key]; if (m && m.top && isAutoConfirmed(m.top, getThreshold(), m.candidates)) cis.push(0); }
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
  return (m.top && isAutoConfirmed(m.top, getThreshold(), m.candidates)) ? m.top : null;
}

function renderResultsTable(built) {
  const matches = project.matches || {};
  const threshold = getThreshold();
  let matched = 0, auto = 0, nomatch = 0, pending = 0, rowsMatched = 0, accepted = 0;
  built.map.forEach((v, key) => {
    const m = matches[key];
    if (!m) { pending += 1; return; }
    if (m.top) { matched += 1; rowsMatched += v.rows.length; if (isAutoConfirmed(m.top, threshold, m.candidates)) auto += 1; }
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
  for (let i = 0; i < project.rows.length; i++) { const info = keyForRow(built, i); if (info && rowPasses(i, built)) _resultRows.push(info); }
  el('recon-results-wrap').classList.remove('d-none');
  el('recon-results-note').textContent = _resultRows.length
    ? `${_resultRows.length.toLocaleString()}${filtersActive() ? ' filtered' : ''} rows — scroll to load more (off-screen rows are evicted).` : (filtersActive() ? 'No rows match the current filters.' : '');
  renderResultsWindow(true);
  updatePaneSummaries();
}

// ── Virtualised results table (lazy load + auto-eviction) ────────────────────
let _resultRows = [];        // full ordered list of row infos {name, country, key}
let RESULT_ROW_H = 34;       // px per row; self-calibrated from the first render

function resultRowHtml(info) {
  const m = (project.matches || {})[info.key];
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
// ── Facets & filters (OpenRefine-style) — slice the dataset by status / score / value / … ─────────
// Session-only view state (NOT persisted, so a reload always shows every row). A row is visible when it
// passes EVERY active facet; within one facet, any selected value matches (OpenRefine semantics). The
// filtered row set drives the results table, the review queue and the full-dataset map. Facets read the
// ACTIVE reconciliation column's matches.
let _filters = { status: new Set(), score: new Set(), coord: 'any', date: 'any', col: -1, colVals: new Set(), text: '' };
function filtersActive() {
  return !!(_filters.status.size || _filters.score.size || _filters.coord !== 'any' || _filters.date !== 'any' || _filters.colVals.size || _filters.text);
}
function resetFilters() { _filters = { status: new Set(), score: new Set(), coord: 'any', date: 'any', col: -1, colVals: new Set(), text: '' }; }
const STATUS_LABELS = { accepted: 'accepted', auto: 'auto-confirmed', candidate: 'needs review', rejected: 'rejected', skipped: 'skipped', nomatch: 'no match', none: 'no match' };
const STATUS_ORDER = ['candidate', 'auto', 'accepted', 'nomatch', 'none', 'skipped', 'rejected'];
const SCORE_LABELS = { 100: '100 (exact)', 90: '90–99', 80: '80–89', lt80: 'below 80', nomatch: 'no match' };
const SCORE_ORDER = ['100', '90', '80', 'lt80', 'nomatch'];
function scoreBand(key) {
  const m = project.matches && project.matches[key];
  if (!m || !m.top) return 'nomatch';
  const s = m.top.score;
  return s >= 100 ? '100' : s >= 90 ? '90' : s >= 80 ? '80' : 'lt80';
}
function rowHasCoordCheap(i) {
  if (project.geom) { for (const k in project.geom) { if (k.slice(k.indexOf(':') + 1) === String(i)) return true; } }
  const ciIdx = colIndexByRole('coords'), latIdx = colIndexByRole('lat'), lonIdx = colIndexByRole('lon');
  const ne = (idx) => idx >= 0 && project.rows[i][idx] != null && String(project.rows[i][idx]).trim() !== '';
  return ne(ciIdx) || (ne(latIdx) && ne(lonIdx));
}
function rowHasDateCheap(i) { const d = colIndexByRole('date'); return d >= 0 && project.rows[i][d] != null && String(project.rows[i][d]).trim() !== ''; }
function cellVal(i, col) { return String(project.rows[i][col] == null ? '' : project.rows[i][col]).trim(); }
function rowKeyIndex(key) { return Number(key.slice(key.indexOf(':') + 1)); }
function rowPasses(i, built) {
  const key = built.colIndex + ':' + i;
  if (_filters.text) { const v = built.map.get(key); const name = (v ? v.query : cellVal(i, built.colIndex)).toLowerCase(); if (!name.includes(_filters.text)) return false; }
  if (_filters.status.size && !_filters.status.has(effectiveStatus(key))) return false;
  if (_filters.score.size && !_filters.score.has(scoreBand(key))) return false;
  if (_filters.coord !== 'any' && (rowHasCoordCheap(i) !== (_filters.coord === 'yes'))) return false;
  if (_filters.date !== 'any' && (rowHasDateCheap(i) !== (_filters.date === 'yes'))) return false;
  if (_filters.col >= 0 && _filters.colVals.size && !_filters.colVals.has(cellVal(i, _filters.col))) return false;
  return true;
}
// Re-render everything the filters affect.
function applyFilters() { const built = buildUniqueQueries(); if (built) renderResults(built); }

// The facet panel: chips with counts for status/score, yes/no toggles for coordinate/date, a text box,
// and a "facet any column" value list. Counts are over all rows in the active column (not cross-filtered).
function facetChip(kind, val, label, count, on) {
  return `<button type="button" class="recon-facet-chip${on ? ' recon-facet-chip--on' : ''}" data-facet="${kind}" data-val="${esc(String(val))}">` +
    `${esc(label)} <span class="recon-facet-n">${count.toLocaleString()}</span></button>`;
}
function renderFilters() {
  const panel = el('recon-filters'); if (!panel || !project) return;
  const built = buildUniqueQueries();
  const hasMatches = built && project.matches && Object.keys(project.matches).length;
  if (!hasMatches) { panel.classList.add('d-none'); return; }
  panel.classList.remove('d-none');
  const statusC = {}, scoreC = {}, colVals = {};
  let coordYes = 0, dateYes = 0, total = 0;
  built.map.forEach((v, key) => {
    const i = rowKeyIndex(key); total += 1;
    statusC[effectiveStatus(key)] = (statusC[effectiveStatus(key)] || 0) + 1;
    scoreC[scoreBand(key)] = (scoreC[scoreBand(key)] || 0) + 1;
    if (rowHasCoordCheap(i)) coordYes += 1;
    if (rowHasDateCheap(i)) dateYes += 1;
    if (_filters.col >= 0) { const cv = cellVal(i, _filters.col); colVals[cv] = (colVals[cv] || 0) + 1; }
  });
  const visible = [...built.map.keys()].filter((key) => rowPasses(rowKeyIndex(key), built)).length;
  const statusChips = STATUS_ORDER.filter((s) => statusC[s]).map((s) => facetChip('status', s, STATUS_LABELS[s] || s, statusC[s], _filters.status.has(s))).join('');
  const scoreChips = SCORE_ORDER.filter((s) => scoreC[s]).map((s) => facetChip('score', s, SCORE_LABELS[s], scoreC[s], _filters.score.has(s))).join('');
  const triState = (kind, cur) => ['any', 'yes', 'no'].map((o) =>
    `<button type="button" class="recon-facet-chip${cur === o ? ' recon-facet-chip--on' : ''}" data-facet="${kind}" data-val="${o}">${o}</button>`).join('');
  const colOpts = project.columns.map((c, j) => `<option value="${j}"${_filters.col === j ? ' selected' : ''}>${esc(truncate(c.name, 24))}</option>`).join('');
  const colValList = _filters.col >= 0 ? Object.entries(colVals).sort((a, b) => b[1] - a[1]).slice(0, 40).map(([val, count]) =>
    `<label class="recon-facet-cval"><input type="checkbox" data-facet="colval" value="${esc(val)}"${_filters.colVals.has(val) ? ' checked' : ''}> ${esc(truncate(val || '(blank)', 26))} <span class="recon-facet-n">${count.toLocaleString()}</span></label>`).join('') : '';
  panel.innerHTML =
    '<div class="recon-filters-head d-flex align-items-center gap-2 flex-wrap">' +
      '<i class="fas fa-filter text-secondary"></i><strong class="small">Filter rows</strong>' +
      `<input type="search" id="recon-filter-text" class="form-control form-control-sm" style="max-width:200px" placeholder="name contains…" value="${esc(_filters.text)}">` +
      `<span class="small text-muted ms-auto">Showing <strong id="recon-filter-count">${visible.toLocaleString()}</strong> of ${total.toLocaleString()} rows</span>` +
      `<button type="button" id="recon-filter-clear" class="btn btn-sm btn-link p-0${filtersActive() ? '' : ' d-none'}">clear filters</button>` +
    '</div>' +
    '<div class="recon-filter-groups">' +
      (statusChips ? `<div class="recon-filter-group"><span class="recon-filter-label">Status</span>${statusChips}</div>` : '') +
      (scoreChips ? `<div class="recon-filter-group"><span class="recon-filter-label">Score</span>${scoreChips}</div>` : '') +
      (colIndexByRole('coords') >= 0 || colIndexByRole('lat') >= 0 ? `<div class="recon-filter-group"><span class="recon-filter-label">Has coordinate</span>${triState('coord', _filters.coord)} <span class="recon-facet-n">${coordYes}/${total}</span></div>` : '') +
      (colIndexByRole('date') >= 0 ? `<div class="recon-filter-group"><span class="recon-filter-label">Has date</span>${triState('date', _filters.date)} <span class="recon-facet-n">${dateYes}/${total}</span></div>` : '') +
      `<div class="recon-filter-group"><span class="recon-filter-label">Facet column</span><select id="recon-filter-col" class="form-select form-select-sm" style="width:auto"><option value="-1">— choose —</option>${colOpts}</select></div>` +
      (colValList ? `<div class="recon-filter-cvals">${colValList}</div>` : '') +
    '</div>';
  // Wire the panel's controls.
  panel.querySelectorAll('.recon-facet-chip[data-facet="status"], .recon-facet-chip[data-facet="score"]').forEach((b) => b.addEventListener('click', () => {
    const set = b.dataset.facet === 'status' ? _filters.status : _filters.score;
    if (set.has(b.dataset.val)) set.delete(b.dataset.val); else set.add(b.dataset.val);
    applyFilters();
  }));
  panel.querySelectorAll('.recon-facet-chip[data-facet="coord"], .recon-facet-chip[data-facet="date"]').forEach((b) => b.addEventListener('click', () => { _filters[b.dataset.facet] = b.dataset.val; applyFilters(); }));
  panel.querySelectorAll('input[data-facet="colval"]').forEach((cb) => cb.addEventListener('change', () => { if (cb.checked) _filters.colVals.add(cb.value); else _filters.colVals.delete(cb.value); applyFilters(); }));
  const colSel = el('recon-filter-col');
  if (colSel) colSel.addEventListener('change', () => { _filters.col = Number(colSel.value); _filters.colVals = new Set(); applyFilters(); });
  const clr = el('recon-filter-clear'); if (clr) clr.addEventListener('click', () => { resetFilters(); applyFilters(); });
  const txt = el('recon-filter-text');
  if (txt) {
    let t = null;
    // Light path: re-filter the views + update the count/clear button WITHOUT rebuilding the panel, so
    // the text box keeps focus while typing.
    txt.addEventListener('input', () => {
      clearTimeout(t);
      t = setTimeout(() => {
        _filters.text = txt.value.trim().toLowerCase();
        const b = buildUniqueQueries();
        if (b) { renderResultsTable(b); refreshReview(); refreshFullMapPane(); refreshExport(); updateFilterCount(b); }
      }, 250);
    });
  }
}
function updateFilterCount(built) {
  const c = el('recon-filter-count'); if (!c) return;
  const visible = [...built.map.keys()].filter((key) => rowPasses(rowKeyIndex(key), built)).length;
  c.textContent = visible.toLocaleString();
  const clr = el('recon-filter-clear'); if (clr) clr.classList.toggle('d-none', !filtersActive());
}

function renderResults(built) { renderColSwitcher(); renderFilters(); renderResultsTable(built); refreshReview(); refreshFullMapPane(); refreshExport(); }

// Column switcher (multi-column reconciliation): pills for each chain column, parent → child, showing
// each column's stage state and which one the results/review panes focus. Rendered into BOTH the
// reconcile pane and the review pane (all .recon-col-switcher containers) so the user can focus a
// column and act on it (Sources, Re-reconcile) without hopping panes. Hidden for single-column sets.
function renderColSwitcher() {
  const boxes = document.querySelectorAll('.recon-col-switcher');
  if (!boxes.length || !project) return;
  const chain = reconChain();
  if (chain.length <= 1) { boxes.forEach((b) => { b.innerHTML = ''; b.classList.add('d-none'); }); return; }
  const active = activeReconCol();
  const STATE_ICON = { locked: '<i class="fas fa-lock me-1"></i>', ready: '<i class="far fa-circle me-1"></i>', review: '<i class="fas fa-list-check me-1"></i>', confirmed: '<i class="fas fa-check me-1"></i>' };
  const pills = chain.map((ci, idx) => {
    const state = columnState(idx);
    const name = truncate(project.columns[ci].name, 22);
    const cls = `recon-col-pill recon-col-pill--${state}${ci === active ? ' recon-col-pill--active' : ''}`;
    const pill = `<button type="button" class="${cls}" data-idx="${idx}" title="${esc(project.columns[ci].name)} — ${state}"${state === 'locked' ? ' disabled' : ''}>${STATE_ICON[state]}${esc(name)}</button>`;
    const arrow = idx < chain.length - 1 ? '<i class="fas fa-angle-right mx-1 text-muted"></i>' : '';
    return `${pill}${arrow}`;
  }).join('');
  const note = reconStaleNote ? `<div class="recon-col-stale small mt-1"><i class="fas fa-triangle-exclamation me-1"></i>${esc(reconStaleNote)}</div>` : '';
  const html = `<div class="d-flex align-items-center flex-wrap gap-1"><span class="text-muted small me-1"><i class="fas fa-layer-group me-1"></i>Columns (parent → child):</span>${pills}</div>${note}`;
  boxes.forEach((box) => {
    box.classList.remove('d-none');
    box.innerHTML = html;
    box.querySelectorAll('.recon-col-pill').forEach((b) => b.addEventListener('click', () => {
      if (b.disabled) return; // locked columns aren't reviewable yet
      reconActiveIdx = Number(b.dataset.idx);
      const built = buildUniqueQueries(); if (built) renderResults(built);
      updateReconButton();   // focus changed → refresh the Re-reconcile button + Sources target
    }));
  });
}

// Reviewable rows (those with ≥1 candidate).
function reviewableKeys(built) {
  const arr = [];
  built.map.forEach((v, key) => { const m = project.matches[key]; if (m && m.top && rowPasses(rowKeyIndex(key), built)) arr.push({ key, rows: v.rows.length, name: v.query, country: v.country }); });
  arr.sort((a, b) => b.rows - a.rows);
  return arr;
}
function needsReview(key) {
  const m = project.matches[key];
  return !(project.decisions && project.decisions[key]) && m && m.top && !isAutoConfirmed(m.top, getThreshold(), m.candidates);
}
function refreshReview() {
  const sec = el('recon-review');
  if (!sec) return;
  if (!project) { sec.classList.add('d-none'); return; }
  sec.classList.remove('d-none'); // header always visible once a dataset is loaded
  const built = buildUniqueQueries();
  if (!built) { // no reconcilable (place-name) column mapped yet — keep the header, guide the user
    const card = el('recon-review-card');
    if (card) card.innerHTML = '<div class="text-muted small py-3"><i class="fas fa-circle-info me-1"></i>Map a <strong>“Place name”</strong> column in Step 2 and reconcile it (Step 3) — matches then appear here to confirm.</div>';
    const map = el('recon-review-map'); if (map) map.classList.add('d-none');
    reviewMeta = []; updateReviewProgress();
    return;
  }
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
// A candidate's Wikipedia link (from Wikidata sitelinks surfaced by /reconcile as [{lang, url}]).
// Prefers English; shows "+N" when the article exists in more languages. Empty string when none.
function wikiLinkHtml(wiki) {
  const w = wiki || [];
  if (!w.length) return '';
  const pick = w.find((x) => x.lang === 'en') || w[0];
  const more = w.length > 1 ? ` <span class="text-muted">+${w.length - 1}</span>` : '';
  const title = w.length > 1 ? `Wikipedia (${w.map((x) => x.lang).join(', ')})` : `Wikipedia (${pick.lang})`;
  return `<a class="recon-cand-wiki" href="${esc(pick.url)}" target="_blank" rel="noopener noreferrer" title="${esc(title)}">` +
    `<i class="fab fa-wikipedia-w"></i> Wikipedia${more}</a>`;
}
function renderReviewCard() {
  const card = el('recon-review-card');
  if (!card || !reviewMeta.length) { if (card) card.innerHTML = ''; return; }
  const meta = reviewMeta[reviewPos];
  const m = project.matches[meta.key];
  const dec = project.decisions && project.decisions[meta.key];
  const auto = m.top && isAutoConfirmed(m.top, getThreshold(), m.candidates);
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
    wikiLinkHtml(c.wikipedia) +
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
  // A Wikipedia link inside a candidate must open the article, NOT toggle acceptance of the candidate.
  card.querySelectorAll('.recon-cand-wiki').forEach((a) => a.addEventListener('click', (e) => e.stopPropagation()));
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
// Per-column source filter. Each chain column can restrict/prioritise its own source gazetteers
// (e.g. UK Historic Counties for the County column, a parishes gazetteer for Parish). A column with
// no explicit choice defaults to ALL sources — a per-column pick never seeds another column's default.
function getNsFilter(col) {
  if (project && col != null && project.colConfig && project.colConfig[col] && project.colConfig[col].nsFilter) return project.colConfig[col].nsFilter;
  return { mode: 'all', namespaces: [] };
}
// The Sources picker (and the Re-reconcile button) configure the FOCUSED column — the one shown in
// the review/results panes (selected via a switcher pill, defaulting to the current stage). This lets
// you revisit a confirmed column, change its sources, and re-reconcile it.
function sourcesTargetCol() { return activeReconCol(); }
function availableNamespaces() {
  // Registry authorities are the source of truth; NS_NAMES is only a pre-fetch fallback (before
  // /api/sources/ resolves). Union in any namespaces present in the current matches so nothing
  // selectable can vanish.
  const set = new Set();
  if (_sources && _sources.length) _sources.forEach((s) => set.add(s.namespace));
  else Object.keys(NS_NAMES).forEach((ns) => set.add(ns));
  if (project && project.matches) Object.values(project.matches).forEach((m) => (m.candidates || []).forEach((c) => set.add(nsFromId(c.id))));
  return [...set];
}
function sortByNsPriority(cands, namespaces) {
  const pri = (c) => (namespaces.includes(nsFromId(c.id)) ? 0 : 1);
  return cands.map((c, i) => ({ c, i })).sort((a, b) => (pri(a.c) - pri(b.c)) || (b.c.score - a.c.score) || (a.i - b.i)).map((x) => x.c);
}
// Apply the current filter to a freshly-fetched candidate list (prioritise re-orders; only is enforced
// server-side via the query, but re-filter defensively too).
function applyNsToCandidates(result, col) {
  const f = getNsFilter(col);
  if (!f.namespaces.length) return result;
  if (f.mode === 'only') return result.filter((c) => f.namespaces.includes(nsFromId(c.id)));
  if (f.mode === 'prioritise') return sortByNsPriority(result, f.namespaces);
  return result;
}
// Show/hide a small circular count badge on a pane-3 button.
function setBtnBadge(id, count, title) {
  const b = el(id); if (!b) return;
  if (count > 0) { b.textContent = String(count); if (title) b.title = title; b.classList.remove('d-none'); }
  else { b.classList.add('d-none'); b.removeAttribute('title'); }
}
function updateSourcesLabel() {
  const lbl = el('recon-sources-label'); if (!lbl) return;
  const col = sourcesTargetCol();
  const f = getNsFilter(col >= 0 ? col : undefined);
  const colName = (col >= 0 && project && reconChain().length > 1) ? ` · ${truncate(project.columns[col].name, 14)}` : '';
  lbl.textContent = 'Sources' + colName;
  // Circular badge = number of chosen source gazetteers (only when restricting/prioritising, not "all").
  const active = (f.mode === 'only' || f.mode === 'prioritise') && f.namespaces.length > 0;
  setBtnBadge('recon-sources-badge', active ? f.namespaces.length : 0,
    f.mode === 'only' ? `Only these ${f.namespaces.length} source(s)` : `Prioritising ${f.namespaces.length} source(s)`);
}
async function populateSourcesModal() {
  await loadSources(); // registry-driven list (names, record counts, descriptions)
  const col = sourcesTargetCol();
  const f = getNsFilter(col >= 0 ? col : undefined);
  const title = el('recon-sources-title');
  if (title) title.textContent = (col >= 0 && project && reconChain().length > 1)
    ? `Source gazetteers — ${truncate(project.columns[col].name, 30)}` : 'Source gazetteers';
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
  const col = sourcesTargetCol();
  if (project && col >= 0) { project.colConfig = project.colConfig || {}; project.colConfig[col] = Object.assign({}, project.colConfig[col], { nsFilter: f }); }
  // The choice lives only on that column (project.colConfig) — it is NOT remembered as a global
  // default, so every other/fresh column stays on "all sources" until explicitly set.
  updateSourcesLabel();
  // 'prioritise' re-orders THIS column's existing candidates immediately; 'only'/'all' take effect on
  // the next run of the column.
  if (project && project.matches && Object.keys(project.matches).length && mode === 'prioritise') {
    for (const k in project.matches) {
      if (col >= 0 && k.slice(0, k.indexOf(':')) !== String(col)) continue;
      const m = project.matches[k]; if (m && m.candidates) { m.candidates = sortByNsPriority(m.candidates, namespaces); m.top = m.candidates[0] || null; }
    }
  }
  if (project) persist();
  if (project && project.matches) { const built = buildUniqueQueries(); if (built) renderResults(built); }
}

// ── Dataset-wide Scope picker (country / date / feature-type / region) ────────
// Staged region selections that aren't plain form fields (the WHG place and the drawn geometry) live
// here while the modal is open; they're committed to project.scope only on Apply.
let _scopeDraft = { whgPlace: null, geometry: null }; // AAT type selection lives in the scopeAat picker

function parseCcodes(text) {
  return [...new Set(String(text || '').toUpperCase().match(/[A-Z]{2}/g) || [])];
}
// How many of the three scope facets (where / when / what) are active — drives the button badge.
function scopeFacetCount() {
  const s = getScope(); if (!s) return 0;
  const r = s.region || {};
  const where = (r.mode === 'ccodes' && r.ccodes && r.ccodes.length) || (r.mode === 'whg' && r.place) || (r.mode === 'draw' && r.geometry);
  const when = (s.start != null || s.end != null || (s.periods && s.periods.length));
  const what = s.types && s.types.selected && s.types.selected.length;
  return (where ? 1 : 0) + (when ? 1 : 0) + (what ? 1 : 0);
}
function updateScopeLabel() {
  const lbl = el('recon-scope-label'); if (!lbl) return;
  const on = scopeActive();
  const sum = on ? scopeSummary() : '';
  lbl.textContent = sum ? `Scope: ${sum}` : 'Scope';
  const btn = el('recon-scope-btn');
  if (btn) { btn.classList.toggle('btn-primary', on); btn.classList.toggle('btn-outline-secondary', !on); }
  // Circular badge = number of active scope facets (where/when/what) constraining results.
  setBtnBadge('recon-scope-badge', on ? scopeFacetCount() : 0, 'Active scope filters');
}
// Show one region-method panel, hide the others; lazy-init the draw map when its panel opens.
function showScopeRegionMode(mode) {
  [['ccodes', 'recon-scope-region-ccodes'], ['whg', 'recon-scope-region-whg'], ['draw', 'recon-scope-region-draw']]
    .forEach(([m, id]) => { const p = el(id); if (p) p.classList.toggle('d-none', m !== mode); });
  if (mode === 'draw') initScopeMap();
}
function populateScopeModal() {
  const s = project ? (project.scope || defaultScope()) : defaultScope();
  const r = s.region || { mode: 'none' };
  _scopeDraft = { whgPlace: r.place ? Object.assign({}, r.place) : null, geometry: r.geometry || null };
  _scopeDrawing = false;
  // Region mode radio
  const modeInput = document.querySelector(`input[name="recon-scope-region-mode"][value="${r.mode || 'none'}"]`);
  if (modeInput) modeInput.checked = true;
  const cc = el('recon-scope-ccodes'); if (cc) cc.value = (r.ccodes || []).join(', ');
  renderScopeWhgSelected();
  updateScopeDrawStatus();
  el('recon-scope-whg-results') && (el('recon-scope-whg-results').innerHTML = '');
  el('recon-scope-whg-q') && (el('recon-scope-whg-q').value = '');
  // Temporal
  const st = el('recon-scope-start'); if (st) st.value = s.start != null ? s.start : '';
  const en = el('recon-scope-end'); if (en) en.value = s.end != null ? s.end : '';
  const ud = el('recon-scope-undated'); if (ud) ud.checked = !!s.undated;
  // PeriodO period(s) — restore selection and load data-tailored suggestions.
  _scopePeriods = (s.periods || []).map((p) => Object.assign({}, p));
  el('recon-scope-period-q') && (el('recon-scope-period-q').value = '');
  el('recon-scope-period-results') && (el('recon-scope-period-results').innerHTML = '');
  renderScopePeriods();
  loadPeriodSuggestions();
  // AAT place types — reset the picker to the saved selection.
  scopeAat.reset((s.types && s.types.selected) || []);
  showScopeRegionMode(r.mode || 'none');
}
function renderScopeWhgSelected() {
  const box = el('recon-scope-whg-selected'); if (!box) return;
  const p = _scopeDraft.whgPlace;
  box.innerHTML = p
    ? `<span class="badge bg-primary">${esc(truncateText(p.title, 40))}</span> <span class="text-muted small">${esc(p.id)}</span> ` +
      `<button type="button" class="btn btn-sm btn-link p-0 ms-1" id="recon-scope-whg-clear">clear</button>`
    : '<span class="text-muted small">No place selected.</span>';
  const clr = el('recon-scope-whg-clear');
  if (clr) clr.addEventListener('click', () => { _scopeDraft.whgPlace = null; renderScopeWhgSelected(); });
}
let _scopeDrawing = false; // true while the polygon is being drawn (waiting for map clicks)
function updateScopeDrawStatus() {
  const st = el('recon-scope-draw-status'); if (!st) return;
  if (_scopeDrawing) st.innerHTML = '<span class="text-primary"><i class="fas fa-draw-polygon me-1"></i>Drawing — click points on the map, then <strong>Finish</strong> (need ≥ 3).</span>';
  else st.textContent = _scopeDraft.geometry ? 'Area drawn — reconciliation will be scoped to inside it.' : 'No area drawn yet.';
  // Reflect live draw mode on the Draw button so it's obvious the map is armed.
  const drawBtn = document.querySelector('[data-scope-draw="polygon"]');
  if (drawBtn) { drawBtn.classList.toggle('btn-primary', _scopeDrawing); drawBtn.classList.toggle('btn-outline-secondary', !_scopeDrawing); }
}
// Search WHG by place name (reuses the /reconcile service) so a region can be chosen by name and used
// as a `contained_in` container. Best results come from picking an administrative area (a country,
// region, or county) that has polygon geometry.
async function searchScopeWhg() {
  const q = (el('recon-scope-whg-q') || {}).value;
  const box = el('recon-scope-whg-results'); if (!box) return;
  if (!q || !q.trim()) { box.innerHTML = ''; return; }
  box.innerHTML = '<span class="text-muted small"><i class="fas fa-spinner fa-spin me-1"></i>searching…</span>';
  try {
    const data = await postReconcile({ q0: { query: q.trim(), type: 'place', limit: 8 } }, getCsrf());
    const results = (data.q0 && data.q0.result) || [];
    if (!results.length) { box.innerHTML = '<span class="text-muted small">No places found.</span>'; return; }
    box.innerHTML = results.map((c) =>
      `<button type="button" class="btn btn-sm btn-outline-secondary text-start d-block w-100 mb-1 recon-scope-whg-hit" data-id="${esc(c.id)}" data-title="${esc(c.name)}">` +
      `${truncate(c.name, 44)} <span class="recon-cand-ns ms-1">${esc(nsName(c.id))}</span>` +
      (c.description ? ` <span class="text-muted small">${truncate(c.description, 30)}</span>` : '') + '</button>').join('');
    box.querySelectorAll('.recon-scope-whg-hit').forEach((b) => b.addEventListener('click', () => {
      _scopeDraft.whgPlace = { id: b.dataset.id, title: b.dataset.title };
      renderScopeWhgSelected();
    }));
  } catch (err) { box.innerHTML = `<span class="text-danger small">Search failed: ${esc(err.message)}</span>`; }
}
async function initScopeMap() {
  const container = el('recon-scope-map'); if (!container) return;
  try {
    const mod = await loadReconMap();
    mod.renderScopeMap(container, _scopeDraft.geometry, (geom) => { _scopeDraft.geometry = geom; updateScopeDrawStatus(); });
  } catch (err) { console.error('[recon] scope map failed', err); }
}
async function scopeDrawAction(kind) {
  const mod = await loadReconMap();
  if (kind === 'polygon') { mod.scopeDraw(); _scopeDrawing = true; }
  else if (kind === 'finish') { mod.scopeFinish(); _scopeDrawing = false; }
  else if (kind === 'clear') { mod.scopeClear(); _scopeDrawing = false; _scopeDraft.geometry = null; }
  updateScopeDrawStatus();
}

// ── Dataset-scope PeriodO period(s) (the Scope "When" section) ─────────────────────────────────────
// Scope-level only (place#… / follow-up): match the WHOLE dataset's temporal scope to canonical PeriodO
// period(s) — never per row. Suggestions are ranked server-side by the data's geographic (ccodes) and
// temporal (year) scope; a curated list of broad eras is always offered as name-search seeds ("hard-coded
// scope values"). Selecting a period seeds the From/To years and travels into LPF `when.periods`.
let _scopePeriods = []; // draft [{id, uri, label, start, stop}] while the modal is open
// Region-neutral eras always offered as search seeds — the hard-coded scope values.
const PERIOD_SEEDS = ['Prehistoric', 'Bronze Age', 'Iron Age', 'Classical antiquity', 'Roman', 'Late antiquity', 'Early Middle Ages', 'Middle Ages', 'Early modern', 'Modern', 'Contemporary'];
function fmtYear(y) { if (y == null || y === '') return ''; const n = Number(y); if (!Number.isFinite(n)) return String(y); return n < 0 ? (Math.abs(n) + ' BCE') : String(n); }
function fmtSpan(a, b) { const s = fmtYear(a), e = fmtYear(b); return (s || e) ? `${s || '…'} – ${e || '…'}` : ''; }
// Geographic scope of the data: scope ccodes (if any) ∪ valid 2-letter codes from a country column.
function datasetCcodesHint() {
  const out = []; const seen = new Set();
  const s = getScope();
  if (s && s.region && s.region.mode === 'ccodes') (s.region.ccodes || []).forEach((c) => { const u = String(c).toUpperCase(); if (/^[A-Z]{2}$/.test(u) && !seen.has(u)) { seen.add(u); out.push(u); } });
  const ci = colIndexByRole('country');
  if (ci >= 0 && project) for (const r of project.rows) { const u = String(r[ci] == null ? '' : r[ci]).toUpperCase().trim(); if (/^[A-Z]{2}$/.test(u) && !seen.has(u)) { seen.add(u); out.push(u); if (out.length >= 12) break; } }
  return out;
}
// Temporal scope of the data: the scope years if set, else the min/max parsed year of a date column.
function datasetTemporalHint() {
  const s = getScope();
  let start = s && s.start != null ? s.start : null;
  let end = s && s.end != null ? s.end : null;
  if (start == null && end == null && Dates && project) {
    const di = colIndexByRole('date');
    if (di >= 0) {
      let mn = null, mx = null, n = 0;
      for (const r of project.rows) {
        const v = r[di]; if (v == null || String(v).trim() === '') continue;
        let d = null; try { d = Dates.parseDate(String(v), { locale: 'uk' }); } catch (e) { d = null; }
        if (!d) continue;
        const y0 = d.startISO ? parseInt(d.startISO, 10) : null;
        const y1 = d.endISO ? parseInt(d.endISO, 10) : null;
        if (y0 != null && !Number.isNaN(y0)) mn = mn == null ? y0 : Math.min(mn, y0);
        if (y1 != null && !Number.isNaN(y1)) mx = mx == null ? y1 : Math.max(mx, y1);
        if (++n >= 400) break;
      }
      start = mn; end = mx;
    }
  }
  return { start, end };
}
function renderScopePeriods() {
  const box = el('recon-scope-period-selected'); if (!box) return;
  if (!_scopePeriods.length) { box.innerHTML = '<span class="text-muted small">No period selected — the dataset carries no canonical period.</span>'; return; }
  box.innerHTML = _scopePeriods.map((p) => {
    const span = fmtSpan(p.start, p.stop);
    return `<span class="recon-aat-chip">${esc(truncateText(p.label, 28))}${span ? ` <span class="text-muted">${esc(span)}</span>` : ''}` +
      `<button type="button" class="recon-aat-chip-x" data-id="${esc(p.id)}" title="remove" aria-label="remove">×</button></span>`;
  }).join(' ');
  box.querySelectorAll('.recon-aat-chip-x').forEach((b) => b.addEventListener('click', () => { _scopePeriods = _scopePeriods.filter((x) => x.id !== b.dataset.id); renderScopePeriods(); }));
}
function addScopePeriod(p) {
  if (!p || !p.id || _scopePeriods.some((x) => x.id === p.id)) return;
  _scopePeriods.push({ id: p.id, uri: p.uri || '', label: p.label || '', start: p.start != null ? p.start : null, stop: p.stop != null ? p.stop : null });
  // Seed From/To years from the period bounds when the user hasn't set them.
  const st = el('recon-scope-start'), en = el('recon-scope-end');
  if (st && st.value === '' && p.start != null) st.value = p.start;
  if (en && en.value === '' && p.stop != null) en.value = p.stop;
  renderScopePeriods();
}
function periodHitButton(p) {
  const span = fmtSpan(p.start, p.stop);
  const cc = (p.ccodes && p.ccodes.length) ? ` <span class="recon-cand-ns ms-1">${esc(p.ccodes.slice(0, 4).join(' '))}</span>` : '';
  return `<button type="button" class="btn btn-sm btn-outline-secondary text-start d-block w-100 mb-1 recon-period-hit" ` +
    `data-id="${esc(p.id)}" data-uri="${esc(p.uri || '')}" data-label="${esc(p.label)}" data-start="${p.start == null ? '' : p.start}" data-stop="${p.stop == null ? '' : p.stop}">` +
    `${esc(truncate(p.label, 40))}${span ? ` <span class="text-muted small">${esc(span)}</span>` : ''}${cc}</button>`;
}
function bindPeriodHits(box) {
  box.querySelectorAll('.recon-period-hit').forEach((b) => b.addEventListener('click', () => addScopePeriod({
    id: b.dataset.id, uri: b.dataset.uri, label: b.dataset.label,
    start: b.dataset.start === '' ? null : Number(b.dataset.start), stop: b.dataset.stop === '' ? null : Number(b.dataset.stop),
  })));
}
// Curated era seeds (always available) — each runs a PeriodO name search when clicked.
function periodSeedHtml() {
  return `<div class="mt-2"><span class="text-muted small me-1">Common periods:</span>` +
    PERIOD_SEEDS.map((n) => `<button type="button" class="btn btn-sm btn-link p-0 me-2 align-baseline recon-period-seed">${esc(n)}</button>`).join('') + `</div>`;
}
function bindPeriodSeeds(box) {
  box.querySelectorAll('.recon-period-seed').forEach((b) => b.addEventListener('click', () => { const q = el('recon-scope-period-q'); if (q) q.value = b.textContent; searchScopePeriods(); }));
}
async function loadPeriodSuggestions() {
  const box = el('recon-scope-period-suggest'); if (!box) return;
  const cc = datasetCcodesHint(); const t = datasetTemporalHint();
  if (!cc.length && t.start == null && t.end == null) {
    box.innerHTML = `<div class="text-muted small">Set a country scope (Where) or date range (above) for tailored suggestions — or search / pick a common period below.</div>${periodSeedHtml()}`;
    bindPeriodSeeds(box); return;
  }
  const label = [cc.length ? cc.slice(0, 4).join(', ') + (cc.length > 4 ? '…' : '') : '', (t.start != null || t.end != null) ? fmtSpan(t.start, t.end) : ''].filter(Boolean).join(' · ');
  box.innerHTML = `<div class="text-muted small"><i class="fas fa-spinner fa-spin me-1"></i>suggesting periods for your data${label ? ` (${esc(label)})` : ''}…</div>`;
  const params = new URLSearchParams();
  if (cc.length) params.set('ccodes', cc.join(','));
  if (t.start != null) params.set('start', t.start);
  if (t.end != null) params.set('end', t.end);
  params.set('limit', '8');
  try {
    const data = await fetchJson(`/reconcile/periods/suggest?${params.toString()}`);
    const hits = (data && data.result) || [];
    box.innerHTML = (hits.length
      ? `<div class="text-muted small mb-1">Suggested for your data${label ? ` <span class="fst-italic">(${esc(label)})</span>` : ''}:</div>` + hits.map(periodHitButton).join('')
      : `<div class="text-muted small">No PeriodO periods matched the data's scope — try a search or a common period.</div>`) + periodSeedHtml();
    bindPeriodHits(box); bindPeriodSeeds(box);
  } catch (err) {
    box.innerHTML = `<div class="text-danger small">Suggestions failed: ${esc(err.message)}</div>${periodSeedHtml()}`;
    bindPeriodSeeds(box);
  }
}
async function searchScopePeriods() {
  const q = (el('recon-scope-period-q') || {}).value;
  const box = el('recon-scope-period-results'); if (!box) return;
  if (!q || q.trim().length < 2) { box.innerHTML = '<span class="text-muted small">Type at least 2 letters.</span>'; return; }
  box.innerHTML = '<span class="text-muted small"><i class="fas fa-spinner fa-spin me-1"></i>searching PeriodO…</span>';
  const cc = datasetCcodesHint(); const t = datasetTemporalHint();
  const params = new URLSearchParams(); params.set('q', q.trim()); params.set('limit', '12');
  if (cc.length) params.set('ccodes', cc.join(','));  // rank name matches by the data's scope
  if (t.start != null) params.set('start', t.start);
  if (t.end != null) params.set('end', t.end);
  try {
    const data = await fetchJson(`/reconcile/periods/suggest?${params.toString()}`);
    const hits = (data && data.result) || [];
    box.innerHTML = hits.length ? hits.map(periodHitButton).join('') : '<span class="text-muted small">No matching PeriodO periods.</span>';
    bindPeriodHits(box);
  } catch (err) { box.innerHTML = `<span class="text-danger small">Search failed: ${esc(err.message)}</span>`; }
}
// ── Reusable AAT place-type picker (Getty AAT hierarchy via the placetypes /types endpoints) ───
// A self-contained multi-select widget: search (with live typeahead ≥3 chars) or browse the AAT
// hierarchy, chosen concepts shown as removable chips. Instantiated once for the Scope filter and once
// for the submission place-types control; each instance owns its own selection state + DOM element ids.
async function fetchJson(url) {
  const res = await fetch(url, { credentials: 'same-origin', headers: { Accept: 'application/json' } });
  if (!res.ok) throw new Error('HTTP ' + res.status);
  return res.json();
}
function createAatPicker(ids, opts) {
  opts = opts || {};
  let selection = [];      // [{id, text}]
  let treeLoaded = false;
  const selIds = () => new Set(selection.map((t) => t.id));
  const notify = () => { if (opts.onChange) opts.onChange(getSelection()); };
  function renderSelected() {
    const box = el(ids.selected); if (!box) return;
    if (!selection.length) { box.innerHTML = `<span class="text-muted small">${esc(opts.emptyText || 'None selected.')}</span>`; return; }
    box.innerHTML = selection.map((t) =>
      `<span class="recon-aat-chip">${esc(truncateText(t.text, 30))}` +
      `<button type="button" class="recon-aat-chip-x" data-id="${esc(t.id)}" title="remove" aria-label="remove">×</button></span>`).join(' ');
    box.querySelectorAll('.recon-aat-chip-x').forEach((b) => b.addEventListener('click', () => remove(b.dataset.id)));
  }
  function add(id, text) { if (!selection.some((t) => t.id === id)) { selection.push({ id, text }); renderSelected(); syncChecks(); notify(); } }
  function remove(id) { selection = selection.filter((t) => t.id !== id); renderSelected(); syncChecks(); notify(); }
  function syncChecks() {
    const tree = el(ids.tree); if (!tree) return; const sel = selIds();
    tree.querySelectorAll('.aat-cb').forEach((cb) => { const li = cb.closest('.aat-node'); if (li) cb.checked = sel.has(li.dataset.id); });
  }
  // One tree row. Guide terms (AAT organisational nodes) are expand-only — no checkbox.
  function nodeHtml(node) {
    const sel = selIds();
    const caret = node.children === true ? '<span class="aat-caret" role="button" title="expand">▸</span>' : '<span class="aat-caret-spacer"></span>';
    const cb = node.guide ? '' : `<input type="checkbox" class="aat-cb"${sel.has(node.id) ? ' checked' : ''} title="${esc(opts.checkboxTitle || 'select this type')}">`;
    const fc = (node.fclasses && node.fclasses.length) ? ` <span class="text-muted small">${esc(node.fclasses.join(''))}</span>` : '';
    return `<li class="aat-node" data-id="${esc(node.id)}" data-loaded="0">` +
      `<div class="aat-row">${caret}${cb}<span class="aat-label${node.guide ? ' aat-guide' : ''}">${esc(node.text)}</span>${fc}</div>` +
      '<ul class="aat-children d-none"></ul></li>';
  }
  async function initTree() {
    if (treeLoaded) return;
    const tree = el(ids.tree); if (!tree) return;
    tree.innerHTML = '<span class="text-muted small"><i class="fas fa-spinner fa-spin me-1"></i>loading…</span>';
    try { const nodes = await fetchJson('/types/tree/'); tree.innerHTML = `<ul class="aat-tree">${nodes.map(nodeHtml).join('')}</ul>`; treeLoaded = true; }
    catch (err) { tree.innerHTML = `<span class="text-danger small">Could not load types: ${esc(err.message)}</span>`; }
  }
  async function expandNode(li) {
    const ul = li.querySelector(':scope > .aat-children');
    const caret = li.querySelector(':scope > .aat-row .aat-caret');
    if (li.dataset.loaded === '1') { ul.classList.toggle('d-none'); if (caret) caret.textContent = ul.classList.contains('d-none') ? '▸' : '▾'; return; }
    if (caret) caret.textContent = '⟳';
    try { const kids = await fetchJson(`/types/tree/${li.dataset.id.slice(4)}/`); ul.innerHTML = kids.map(nodeHtml).join(''); li.dataset.loaded = '1'; ul.classList.remove('d-none'); if (caret) caret.textContent = '▾'; }
    catch (err) { if (caret) caret.textContent = '▸'; }
  }
  function toggleFromNode(li, on) { const text = li.querySelector(':scope > .aat-row .aat-label').textContent; if (on) add(li.dataset.id, text); else remove(li.dataset.id); }
  function bindTree() {
    const tree = el(ids.tree); if (!tree || tree.dataset.bound) return;
    tree.dataset.bound = '1';
    tree.addEventListener('click', (e) => {
      const caret = e.target.closest('.aat-caret');
      if (caret) { const li = caret.closest('.aat-node'); if (li) expandNode(li); return; }
      const label = e.target.closest('.aat-label');
      if (label && !label.classList.contains('aat-guide')) { const li = label.closest('.aat-node'); const cb = li.querySelector(':scope > .aat-row .aat-cb'); if (cb) { cb.checked = !cb.checked; toggleFromNode(li, cb.checked); } }
    });
    tree.addEventListener('change', (e) => { if (e.target.classList && e.target.classList.contains('aat-cb')) toggleFromNode(e.target.closest('.aat-node'), e.target.checked); });
  }
  async function search() {
    const q = (el(ids.q) || {}).value;
    const box = el(ids.results); if (!box) return;
    if (!q || q.trim().length < 2) { box.innerHTML = '<span class="text-muted small">Type at least 2 letters.</span>'; return; }
    box.innerHTML = '<span class="text-muted small"><i class="fas fa-spinner fa-spin me-1"></i>searching…</span>';
    try {
      const results = await fetchJson(`/types/tree/search/?q=${encodeURIComponent(q.trim())}`);
      if (!results.length) { box.innerHTML = '<span class="text-muted small">No matching types.</span>'; return; }
      const sel = selIds();
      box.innerHTML = results.map((r) => { const id = 'aat:' + r.aat_id;
        return `<button type="button" class="btn btn-sm ${sel.has(id) ? 'btn-primary' : 'btn-outline-secondary'} text-start d-block w-100 mb-1 recon-aat-hit" data-id="${esc(id)}" data-text="${esc(r.text)}">` +
          `${truncate(r.text, 44)} <span class="text-muted small">aat:${esc(String(r.aat_id))}</span></button>`; }).join('');
      box.querySelectorAll('.recon-aat-hit').forEach((b) => b.addEventListener('click', () => {
        if (selection.some((t) => t.id === b.dataset.id)) remove(b.dataset.id); else add(b.dataset.id, b.dataset.text);
        b.classList.toggle('btn-primary'); b.classList.toggle('btn-outline-secondary');
      }));
    } catch (err) { box.innerHTML = `<span class="text-danger small">Search failed: ${esc(err.message)}</span>`; }
  }
  function reset(newSelection) {
    selection = (newSelection || []).map((t) => ({ id: t.id, text: t.text }));
    treeLoaded = false;
    const tree = el(ids.tree); if (tree) tree.innerHTML = '';
    const browse = el(ids.browse); if (browse) browse.open = false;
    const q = el(ids.q); if (q) q.value = '';
    const results = el(ids.results); if (results) results.innerHTML = '';
    renderSelected();
  }
  function getSelection() { return selection.map((t) => ({ id: t.id, text: t.text })); }
  function init() {
    bindTree();
    const s = el(ids.search); if (s) s.addEventListener('click', search);
    const q = el(ids.q);
    if (q) {
      q.addEventListener('keydown', (e) => { if (e.key === 'Enter') { e.preventDefault(); search(); } });
      let timer = null;
      q.addEventListener('input', () => { clearTimeout(timer); const v = q.value.trim(); if (v.length < 3) { const r = el(ids.results); if (r) r.innerHTML = ''; return; } timer = setTimeout(search, 300); });
    }
    const browse = el(ids.browse); if (browse) browse.addEventListener('toggle', () => { if (browse.open) initTree(); });
  }
  return { init, reset, getSelection };
}

// Scope-filter instance (in the Scope modal's "What" section).
const scopeAat = createAatPicker(
  { q: 'recon-scope-aat-q', search: 'recon-scope-aat-search', results: 'recon-scope-aat-results', selected: 'recon-scope-aat-selected', tree: 'recon-scope-aat-tree', browse: 'recon-scope-aat-browse' },
  { emptyText: 'No place types selected — any type is allowed.', checkboxTitle: 'scope to this type and its descendants' },
);
// ── Per-row AAT place types ──────────────────────────────────────────────────────────────────────
// Types are assigned per row in the data-browser table (turn on Edit cells → click a cell in the
// type-role column → AAT picker). Stored in project.rowTypes = { rowIndex: [{id,text}] }; the LPF build
// reads them per row. Needed only to CONTRIBUTE to WHG — plain CSV/JSON export never requires them.
function rowTypesFor(i) { return (project && project.rowTypes && project.rowTypes[i]) || []; }
function untypedRowCount() { if (!project) return 0; let n = 0; for (let i = 0; i < project.rows.length; i++) if (!rowTypesFor(i).length) n += 1; return n; }
// Append a blank type-role column ("Place type") when the dataset has none. Undoable (column snapshot).
function addPlaceTypeColumn() {
  if (!project) return;
  const snap = columnSnapshot();
  project.columns.push({ name: 'Place type', role: 'type' });
  project.rows.forEach((r) => r.push(''));
  pushUndo({ type: 'columns', label: 'add Place type column', snapshot: snap });
  persist(); rerenderData();
  flashSaved('Added a “Place type” column — switch on “Edit cells” and click its cells to assign AAT types');
}
// Early nudge (Step 2): shown only when rows are untyped, framed as optional (contribute-only).
function renderTypePrompt() {
  const box = el('recon-type-prompt'); if (!box || !project) return;
  const untyped = untypedRowCount();
  if (!untyped) { box.classList.add('d-none'); return; }
  box.classList.remove('d-none');
  const typeCol = colIndexByRole('type');
  const optional = '<span class="text-muted">Optional — needed only to contribute to WHG.</span>';
  if (typeCol < 0) {
    box.innerHTML = `<i class="fas fa-shapes me-1 text-secondary"></i>No place-type column detected. ` +
      `<button type="button" class="btn btn-sm btn-link p-0 align-baseline" id="recon-add-type-col">Add a “Place type” column</button> ` +
      `to classify your places with Getty AAT types. ${optional}`;
    const b = el('recon-add-type-col'); if (b) b.addEventListener('click', addPlaceTypeColumn);
  } else {
    box.innerHTML = `<i class="fas fa-shapes me-1 text-secondary"></i><strong>${untyped.toLocaleString()}</strong> of ${project.rows.length.toLocaleString()} rows have no place type. ` +
      `Switch on <strong>Edit cells</strong> and click a cell in the <em>${esc(truncate(project.columns[typeCol].name, 24))}</em> column to pick a Getty AAT type. ${optional}`;
  }
}
let _typeRow = -1; // the row whose place type is being assigned in the picker modal
// Type-map picker instance (its own modal).
const typeMapAat = createAatPicker(
  { q: 'recon-tm-aat-q', search: 'recon-tm-aat-search', results: 'recon-tm-aat-results', selected: 'recon-tm-aat-selected', tree: 'recon-tm-aat-tree', browse: 'recon-tm-aat-browse' },
  { emptyText: 'No type assigned to this value yet.', checkboxTitle: 'apply this type to rows with this value' },
);
// Open the AAT picker (shared modal) to assign type(s) to a single row — reached by clicking a cell in
// the type-role column while the data browser is in edit mode.
function openRowTypeModal(rowIndex) {
  _typeRow = rowIndex;
  const m = el('recon-typemap-modal'); if (!m || !project) return;
  const col = colIndexByRole('type');
  const val = col >= 0 ? String(project.rows[rowIndex][col] == null ? '' : project.rows[rowIndex][col]).trim() : '';
  const title = el('recon-typemap-title'); if (title) title.textContent = val ? `Place type for “${truncate(val, 40)}”` : `Place type for row ${rowIndex + 1}`;
  // Bulk option: apply to every row sharing this cell's value (fast for a repeated "kind" column).
  const wrap = el('recon-typemap-applyall-wrap'), cb = el('recon-typemap-applyall');
  if (wrap && cb) {
    let n = 0; if (col >= 0) project.rows.forEach((r) => { if (String(r[col] == null ? '' : r[col]).trim() === val) n += 1; });
    if (col >= 0 && n > 1) {
      wrap.classList.remove('d-none'); cb.checked = false;
      const en = el('recon-typemap-applyall-n'), ev = el('recon-typemap-applyall-val');
      if (en) en.textContent = n.toLocaleString(); if (ev) ev.textContent = truncate(val || '(blank)', 24);
    } else wrap.classList.add('d-none');
  }
  typeMapAat.reset(rowTypesFor(rowIndex));
  if (window.bootstrap && window.bootstrap.Modal) window.bootstrap.Modal.getOrCreateInstance(m).show();
}
function applyRowType() {
  if (_typeRow < 0 || !project) return;
  project.rowTypes = project.rowTypes || {};
  const sel = typeMapAat.getSelection().map((t) => ({ id: t.id, text: t.text }));
  const setRow = (i) => { if (sel.length) project.rowTypes[i] = sel.slice(); else delete project.rowTypes[i]; };
  const col = colIndexByRole('type'), cb = el('recon-typemap-applyall'), wrap = el('recon-typemap-applyall-wrap');
  if (col >= 0 && cb && cb.checked && wrap && !wrap.classList.contains('d-none')) {
    const val = String(project.rows[_typeRow][col] == null ? '' : project.rows[_typeRow][col]).trim();
    project.rows.forEach((r, i) => { if (String(r[col] == null ? '' : r[col]).trim() === val) setRow(i); });
  } else setRow(_typeRow);
  persist(); paintPreviewWindow(); renderTypePrompt(); refreshExport(); runValidation();
}

// Read the modal into a fresh scope object, commit it, and (if it changed) reset existing matches so
// the dataset is reconciled again under the new scope. Async because selected AAT types are expanded to
// their descendants server-side before being stored.
async function applyScope() {
  if (!project) return;
  const mode = (document.querySelector('input[name="recon-scope-region-mode"]:checked') || {}).value || 'none';
  const scope = defaultScope();
  scope.region.mode = mode;
  if (mode === 'ccodes') scope.region.ccodes = parseCcodes((el('recon-scope-ccodes') || {}).value);
  else if (mode === 'whg') scope.region.place = _scopeDraft.whgPlace || null;
  else if (mode === 'draw') scope.region.geometry = _scopeDraft.geometry || null;
  // A region method with nothing chosen falls back to "no region".
  if ((mode === 'ccodes' && !scope.region.ccodes.length) || (mode === 'whg' && !scope.region.place) || (mode === 'draw' && !scope.region.geometry)) scope.region.mode = 'none';
  const start = parseInt((el('recon-scope-start') || {}).value, 10);
  const end = parseInt((el('recon-scope-end') || {}).value, 10);
  scope.start = Number.isFinite(start) ? start : null;
  scope.end = Number.isFinite(end) ? end : null;
  scope.undated = !!(el('recon-scope-undated') || {}).checked;
  // PeriodO scope period(s) — dataset-level canonical period(s).
  scope.periods = _scopePeriods.map((p) => Object.assign({}, p));
  // AAT types: keep the picked concepts for display, expand to descendant ids for the query.
  const selected = scopeAat.getSelection();
  let ids = [];
  if (selected.length) {
    try {
      const data = await fetchJson(`/types/expand/?ids=${encodeURIComponent(selected.map((t) => t.id).join(','))}`);
      ids = (data && data.ids) || [];
    } catch (err) { console.error('[recon] type expansion failed; using selected ids only', err); ids = selected.map((t) => t.id); }
  }
  scope.types = { selected, ids };

  const before = JSON.stringify(project.scope || defaultScope());
  const after = JSON.stringify(scope);
  project.scope = scope;
  if (before !== after && invalidateAllMatches()) {
    reconStaleNote = 'Scope changed — reconciliation was reset; reconcile the columns again with the new scope.';
    setReconSummary('<span class="text-warning"><i class="fas fa-triangle-exclamation me-1"></i>Scope changed — reconcile again to apply it.</span>');
  }
  persist();
  updateScopeLabel();
  refreshReconSection();
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
    if (pt) points.push({ ci: i, lon: pt.lon, lat: pt.lat, name: c.name, namespace: nsName(c.id), altNames: c.alt_names || [], score: c.score, wikipedia: c.wikipedia || [] });
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
    const revCol = activeReconCol();
    const nsf = getNsFilter(revCol);
    const q = { q0: { query: meta.name, type: 'place', limit: want } };
    if (meta.country) q.q0.countries = [meta.country];
    if (nsf.mode === 'only' && nsf.namespaces.length) q.q0.namespaces = nsf.namespaces;
    const data = await postReconcile(q, getCsrf());
    const result = applyNsToCandidates((data.q0 && data.q0.result) || [], revCol);
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
  // A decision on a parent column invalidates already-reconciled child columns (their containment
  // used the old parent ids) — clear & re-lock them so they're reconciled again.
  if (invalidateDownstream(activeReconCol())) {
    reconStaleNote = 'A parent decision changed — downstream columns were reset and must be reconciled again.';
  }
  persist();
  const built = buildUniqueQueries(); if (built) renderResultsTable(built);
  updateReviewProgress();
  renderColSwitcher();
  updateReconButton();
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
  if (!project) return;
  const hasName = colIndexByRole('name') >= 0;
  el('recon-recon').classList.remove('d-none'); // header always visible once a dataset is loaded
  const thr = el('recon-threshold');
  if (thr && project && project.autoThreshold != null) thr.value = project.autoThreshold;
  if (hasName && project.matches && Object.keys(project.matches).length) {
    const built = buildUniqueQueries();
    if (built) renderResults(built);
  } else {
    // No matches yet (a freshly imported or re-imported dataset) — clear any stale results/summary
    // left in the DOM from a previous dataset so they don't misrepresent the new one.
    const rb = el('recon-results-body'); if (rb) rb.innerHTML = '';
    _resultRows = [];
    el('recon-results-wrap').classList.add('d-none');
    setReconSummary('');
    el('recon-progress-wrap').classList.add('d-none');
  }
  updateReconButton();
}

// Drive the Reconcile button + help text from the current stage: reconcile one column, review it,
// then the next column unlocks.
function updateReconButton() {
  const btn = el('recon-run'); if (!btn || !project) return;
  const help = el('recon-recon-help');
  const chain = reconChain();
  updateRerunButton(chain);
  updateSourcesLabel(); // keep the Sources button label pointed at the focused column
  updateScopeLabel();   // reflect any saved dataset-wide scope (e.g. after resume)
  if (colIndexByRole('name') < 0) { // no place-name column yet → guide the user to map one
    btn.disabled = true;
    btn.innerHTML = '<i class="fas fa-wand-magic-sparkles me-1"></i>Reconcile';
    if (help) help.innerHTML = 'Map a <strong>“Place name”</strong> column in <strong>Step 2</strong> (Confirm column roles) — then you can reconcile it against WHG here.';
    return;
  }
  if (chain.length <= 1) { // single (name) column — the classic one-shot
    btn.disabled = false;
    btn.innerHTML = '<i class="fas fa-wand-magic-sparkles me-1"></i>Reconcile';
    return;
  }
  const pos = currentStagePos();
  if (pos >= chain.length) {
    btn.disabled = true;
    btn.innerHTML = '<i class="fas fa-check me-1"></i>All columns confirmed';
    if (help) help.innerHTML = 'Every column in the chain has been reconciled and confirmed. Review any column via its pill above, or continue to the map/export.';
    return;
  }
  const col = chain[pos];
  const colName = esc(truncate(project.columns[col].name, 20));
  const parentName = pos > 0 ? esc(truncate(project.columns[chain[pos - 1]].name, 18)) : '';
  if (columnState(pos) === 'review') {
    btn.disabled = true;
    const isLast = pos >= chain.length - 1;
    btn.innerHTML = `<i class="fas fa-list-check me-1"></i>${isLast ? `Review ${colName}` : `Confirm ${colName} to continue`}`;
    if (help) help.innerHTML = isLast
      ? `Reconciled <strong>${colName}</strong> — the final column. <strong>Review &amp; confirm</strong> its matches (Step 4).`
      : `Reconciled <strong>${colName}</strong>. <strong>Review &amp; confirm</strong> its matches (Step 4) — then <strong>${esc(truncate(project.columns[chain[pos + 1]].name, 18))}</strong> unlocks, scoped within the places you confirmed here.`;
  } else {
    btn.disabled = false;
    btn.innerHTML = `<i class="fas fa-wand-magic-sparkles me-1"></i>Reconcile ${colName}${parentName ? ` within ${parentName}` : ''}`;
    if (help) help.innerHTML = parentName
      ? `Reconcile the <strong>${colName}</strong> column, scoped <em>within</em> the confirmed <strong>${parentName}</strong> places (containment). Choose its <strong>Sources</strong> first if it needs a specific gazetteer.`
      : `Reconcile the <strong>${colName}</strong> column first. Choose its <strong>Sources</strong> (e.g. UK Historic Counties) if it needs a specific gazetteer; you'll review it before the next column unlocks.`;
  }
}

// Show a "Re-reconcile <col>" button when the FOCUSED column already has matches, so the user can
// change its Sources and run it again (multi-column chains only).
function updateRerunButton(chain) {
  const rr = el('recon-rerun'); if (!rr) return;
  const focus = activeReconCol();
  const show = chain.length > 1 && focus >= 0 && colHasMatches(focus) && !running;
  rr.classList.toggle('d-none', !show);
  if (show) {
    const lbl = el('recon-rerun-label');
    if (lbl) lbl.textContent = `Re-reconcile ${truncate(project.columns[focus].name, 18)}`;
  }
}

// Reconcile the CURRENT stage's column only (review-gated). The child columns stay locked until this
// one is confirmed; each inherits containment from the confirmed parents.
async function reconcileStage() {
  if (running || !project) return;
  const chain = reconChain();
  if (!chain.length) { setReconSummary('<span class="text-warning">Map a “Place name” column first (Step 2).</span>'); return; }
  const pos = currentStagePos();
  if (pos >= chain.length) { setReconSummary('<span class="text-success"><i class="fas fa-check me-1"></i>All columns reconciled &amp; confirmed.</span>'); return; }
  if (columnState(pos) === 'review') { setReconSummary('<span class="text-warning">Confirm this column’s matches (Step 4) before reconciling the next.</span>'); return; }
  reconStaleNote = ''; // a fresh run clears any "parent changed" notice
  project.matches = project.matches || {};
  toggleRunning(true);
  openPane('recon-recon');
  stopRequested = false;
  await reconcilePass(chain[pos], pos > 0 ? chain[pos - 1] : -1, getCsrf(), pos, chain.length);
  toggleRunning(false);
  reconActiveIdx = pos; // review/results panes follow the column we just ran…
  if (currentStagePos() > pos) reconActiveIdx = Math.min(currentStagePos(), chain.length - 1); // …unless it auto-confirmed → advance focus to the next stage
  const built = buildUniqueQueries();
  if (built) renderResults(built);
  renderColSwitcher();
  updateReconButton();
  reviewPos = 0; refreshReview();
  await persist();
  console.log(`[recon] reconciled column ${pos + 1}/${chain.length} (${project.columns[chain[pos]].name}) — ${stopRequested ? 'stopped' : 'done'}`);
}

// Re-reconcile an already-reconciled column (typically after changing its Sources): clear its
// matches + decisions, invalidate & re-lock downstream columns (their containment is now stale),
// then run it again as the current stage.
async function reReconcileColumn(col) {
  if (running || !project) return;
  const chain = reconChain();
  if (chain.indexOf(col) < 0) return;
  const c = String(col);
  if (project.matches) for (const k in project.matches) { if (k.slice(0, k.indexOf(':')) === c) delete project.matches[k]; }
  if (project.decisions) for (const k in project.decisions) { if (k.slice(0, k.indexOf(':')) === c) delete project.decisions[k]; }
  invalidateDownstream(col);
  reconStaleNote = '';
  reconActiveIdx = chain.indexOf(col); // focus the column we're about to re-run
  await persist();
  await reconcileStage(); // col is now the earliest non-confirmed column → this runs it
}

// Demo helper for the guided tour: run the WHOLE chain end-to-end, auto-confirming any rows still
// needing review between columns so the tour flows through to the name column (the real UI is
// review-gated; this shortcut is only for the walkthrough).
async function tourReconcileAll() {
  const chain = reconChain();
  for (let guard = 0; guard <= chain.length + 1; guard++) {
    const pos = currentStagePos();
    if (pos >= chain.length) break;
    if (columnState(pos) === 'ready') { await reconcileStage(); await waitUntil(() => !running); }
    // Auto-confirm the top candidate for any pending rows so the next column can unlock.
    if (columnState(currentStagePos() < chain.length ? currentStagePos() : pos) === 'review') {
      const col = chain[currentStagePos() < chain.length ? currentStagePos() : pos];
      colKeys(col).forEach((k) => {
        if (!needsReview(k)) return;
        const m = project.matches[k];
        if (m && m.candidates && m.candidates[0]) {
          project.decisions = project.decisions || {};
          project.decisions[k] = { status: 'accepted', accepted: [{ ci: 0, place_id: m.candidates[0].id, label: m.candidates[0].name, score: m.candidates[0].score }] };
        }
      });
      await persist();
    }
  }
  reconActiveIdx = chain.length - 1;
  const built = buildUniqueQueries(); if (built) renderResults(built);
  renderColSwitcher(); updateReconButton();
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
    const nsf = getNsFilter(colIndex); // this column's own source gazetteers
    slice.forEach(([key, v], j) => {
      const q = { query: v.query, type: 'place', limit: RECON_CAND_LIMIT };
      if (v.country) q.countries = [v.country];
      if (nsf.mode === 'only' && nsf.namespaces.length) q.namespaces = nsf.namespaces; // restrict sources
      if (embByKey && embByKey[key]) q.embedding = embByKey[key]; // phonetic (vector) matching
      // Containment: scope this column's query by ALL the parent column's confirmed places for the
      // same row (a parent may closeMatch several records) — "within any of them".
      if (parentCol >= 0) {
        const pids = resolvedPlaceIds(parentCol, key.slice(key.indexOf(':') + 1)).map(barePlaceId);
        if (pids.length) { q.contained_in = pids; q.containment = 'fuzzy'; q.relation = 'within'; }
      }
      applyGlobalScopeToQuery(q, parentCol < 0, !!v.country); // dataset-wide scope (country/date/type/region)
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
      const result = applyNsToCandidates((data['q' + j] && data['q' + j].result) || [], colIndex);
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
      await tourReconcileAll();
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
  if (run) run.addEventListener('click', reconcileStage);
  const rerun = el('recon-rerun');
  if (rerun) rerun.addEventListener('click', () => reReconcileColumn(activeReconCol()));
  const stop = el('recon-stop');
  if (stop) stop.addEventListener('click', () => { stopRequested = true; });

  const thr = el('recon-threshold');
  if (thr) thr.addEventListener('input', () => {
    if (!project) return;
    project.autoThreshold = getThreshold();
    persist();
    const built = buildUniqueQueries();
    if (built && project.matches && Object.keys(project.matches).length) renderResults(built);
    updateReconButton(); // threshold changes which rows auto-confirm → which stage is current
  });

  const showIgn = el('recon-show-ignored');
  if (showIgn) showIgn.addEventListener('change', () => { if (!project) return; project.showIgnored = showIgn.checked; persist(); renderPreview(); });

  // Data browser: text filter, edit-mode toggle, and click-to-edit (delegated on the tbody).
  const psearch = el('recon-preview-search');
  if (psearch) {
    let t = null;
    psearch.addEventListener('input', () => {
      clearTimeout(t);
      t = setTimeout(() => {
        if (!project) return;
        _previewFilter = psearch.value.trim().toLowerCase();
        buildPreviewView();
        const s = el('recon-preview-scroll'); if (s) s.scrollTop = 0;
        paintPreviewWindow(); updatePreviewCount();
      }, 200);
    });
  }
  const peditBtn = el('recon-preview-edit');
  if (peditBtn) peditBtn.addEventListener('click', () => { if (project) setPreviewEdit(!_previewEdit); });
  const pbody = el('recon-preview-body');
  if (pbody) pbody.addEventListener('mousedown', (e) => {
    if (!_previewEdit || !project) return;
    const td = e.target.closest && e.target.closest('td[data-ci]');
    if (!td || td.querySelector('input')) return; // ignore spacers / the cell already being edited
    const tr = td.closest('tr[data-ri]'); if (!tr) return;
    e.preventDefault(); // don't start a text selection; we focus our own input / open the picker
    const ci = Number(td.dataset.ci), ri = Number(tr.dataset.ri);
    // A type-role cell opens the AAT picker instead of free-text editing.
    if (project.columns[ci].role === 'type') { openRowTypeModal(ri); return; }
    startCellEdit(ri, ci);
  });

  const backupBtn = el('recon-backup');
  if (backupBtn) backupBtn.addEventListener('click', downloadBackup);
  // Undo / redo (data mutations: transforms, column ops, role changes) — buttons + Ctrl/Cmd+Z / Y.
  const undoBtn = el('recon-undo'); if (undoBtn) undoBtn.addEventListener('click', undo);
  const redoBtn = el('recon-redo'); if (redoBtn) redoBtn.addEventListener('click', redo);
  document.addEventListener('keydown', (e) => {
    if (!(e.ctrlKey || e.metaKey)) return;
    const tag = (e.target && e.target.tagName) || '';
    if (/^(INPUT|TEXTAREA|SELECT)$/.test(tag) || (e.target && e.target.isContentEditable)) return; // don't hijack text editing
    const k = (e.key || '').toLowerCase();
    if (k === 'z' && !e.shiftKey) { e.preventDefault(); undo(); }
    else if (k === 'y' || (k === 'z' && e.shiftKey)) { e.preventDefault(); redo(); }
  });
  const exportBtn = el('recon-export-btn');
  if (exportBtn) exportBtn.addEventListener('click', runExport);
  const contribBtn = el('recon-contribute-btn');
  if (contribBtn) contribBtn.addEventListener('click', contributeToWHG);
  // Contribution validation: re-check button, "use Scope type(s)" shortcut, and re-validate when the
  // export options (which change the built LPF) change.
  const recheck = el('recon-validate-recheck');
  if (recheck) recheck.addEventListener('click', runValidation);
  ['recon-exp-match', 'recon-exp-enrich'].forEach((id) => {
    const box = el(id); if (box) box.addEventListener('change', () => { if (!el('recon-export').classList.contains('recon-collapsed')) runValidation(); });
  });

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

  // Dataset-wide Scope picker modal (country / date / feature-type / region).
  const scopeModal = el('recon-scope-modal');
  if (scopeModal) scopeModal.addEventListener('show.bs.modal', populateScopeModal);
  document.querySelectorAll('input[name="recon-scope-region-mode"]').forEach((r) =>
    r.addEventListener('change', () => showScopeRegionMode(r.value)));
  const scopeSearch = el('recon-scope-whg-search');
  if (scopeSearch) scopeSearch.addEventListener('click', searchScopeWhg);
  const scopeQ = el('recon-scope-whg-q');
  if (scopeQ) scopeQ.addEventListener('keydown', (e) => { if (e.key === 'Enter') { e.preventDefault(); searchScopeWhg(); } });
  const scopePeriodSearch = el('recon-scope-period-search');
  if (scopePeriodSearch) scopePeriodSearch.addEventListener('click', searchScopePeriods);
  const scopePeriodQ = el('recon-scope-period-q');
  if (scopePeriodQ) scopePeriodQ.addEventListener('keydown', (e) => { if (e.key === 'Enter') { e.preventDefault(); searchScopePeriods(); } });
  document.querySelectorAll('[data-scope-draw]').forEach((b) => b.addEventListener('click', () => scopeDrawAction(b.dataset.scopeDraw)));
  // AAT place-type pickers: the Scope filter, and the shared per-row type modal (data-browser cells).
  scopeAat.init();
  typeMapAat.init();
  const tmApply = el('recon-typemap-apply');
  if (tmApply) tmApply.addEventListener('click', applyRowType);
  // Cell-transform modal: live preview on find/replace edits, and Apply.
  ['recon-tf-find', 'recon-tf-replace'].forEach((id) => { const e = el(id); if (e) e.addEventListener('input', onFindReplaceInput); });
  ['recon-tf-regex', 'recon-tf-case'].forEach((id) => { const e = el(id); if (e) e.addEventListener('change', onFindReplaceInput); });
  const tfApply = el('recon-transform-apply');
  if (tfApply) tfApply.addEventListener('click', applyTransform);
  const scopeApply = el('recon-scope-apply');
  if (scopeApply) scopeApply.addEventListener('click', applyScope);
  const scopeClear = el('recon-scope-clear');
  if (scopeClear) scopeClear.addEventListener('click', () => {
    const ccb = el('recon-scope-ccodes'); if (ccb) ccb.value = '';
    const stb = el('recon-scope-start'); if (stb) stb.value = '';
    const enb = el('recon-scope-end'); if (enb) enb.value = '';
    const udb = el('recon-scope-undated'); if (udb) udb.checked = false;
    const none = document.querySelector('input[name="recon-scope-region-mode"][value="none"]'); if (none) none.checked = true;
    _scopeDraft = { whgPlace: null, geometry: null };
    _scopePeriods = [];
    const pq = el('recon-scope-period-q'); if (pq) pq.value = '';
    const pr = el('recon-scope-period-results'); if (pr) pr.innerHTML = '';
    renderScopePeriods(); loadPeriodSuggestions();
    renderScopeWhgSelected(); updateScopeDrawStatus(); scopeAat.reset([]);
    showScopeRegionMode('none');
  });
  updateScopeLabel();
  // Warm the registry source list so candidate/source labels use real names (e.g. "UK Historic
  // Counties" not "UKHC"); re-render once it lands if matches are already on screen.
  loadSources().then(() => { if (project && project.matches && Object.keys(project.matches).length) { const built = buildUniqueQueries(); if (built) renderResults(built); } });

  showCapabilities();
  loadSaved();
}

if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
else init();
