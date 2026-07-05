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
import { COORD_FORMATS, detectCoordFormat, parseCoord, parseLatLonPair } from './recon-coords.js';

const PREVIEW_ROWS = 20;
const RECON_ENDPOINT = '/reconcile';   // WHG standard OpenRefine reconciliation service (same-origin)
const RECON_BATCH = 25;                // queries per POST (service cap is 50)
const RECON_CAND_LIMIT = 5;            // candidates requested per query
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
    const c = parseCoord(format, project.rows[i][idx]);
    if (c) { parsed++; if (!sample) sample = { raw: project.rows[i][idx], c }; }
  }
  return { parsed, checked: cap, sample };
}
function pairCoverage(latIdx, lonIdx, swapped) {
  let parsed = 0, sample = null;
  const cap = Math.min(project.rows.length, 500);
  for (let i = 0; i < cap; i++) {
    const c = parseLatLonPair(project.rows[i][latIdx], project.rows[i][lonIdx], swapped);
    if (c) { parsed++; if (!sample) sample = { raw: `${project.rows[i][latIdx]}, ${project.rows[i][lonIdx]}`, c }; }
  }
  return { parsed, checked: cap, sample };
}
function sampleHtml(cov, elseMsg) {
  return cov.sample
    ? `e.g. <code>${truncate(cov.sample.raw, 30)}</code> → <strong>${cov.sample.c.lat.toFixed(5)}, ${cov.sample.c.lon.toFixed(5)}</strong>`
    : `<span class="text-warning">${elseMsg}</span>`;
}

function renderCoords() {
  const box = el('recon-coords');
  const coordsIdx = colIndexByRole('coords');
  const latIdx = colIndexByRole('lat');
  const lonIdx = colIndexByRole('lon');

  let head = '';
  if (coordsIdx >= 0) {
    const detection = detectCoordFormat(coordColumnSamples(coordsIdx));
    const chosen = project.coordFormat || detection.format;
    const cov = coordCoverage(chosen, coordsIdx);
    const opts = COORD_FORMATS.map(([id, l]) => `<option value="${id}"${id === chosen ? ' selected' : ''}>${esc(l)}</option>`).join('');
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
  if (coordsIdx >= 0) return project.coordFormat || detectCoordFormat(coordColumnSamples(coordsIdx)).format;
  return null;
}

// Validate EVERY row against the chosen coordinate format and report the ones that cannot convert.
function checkAllCoords() {
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
      raw = r[coordsIdx]; c = parseCoord(fmt, raw); isBlank = blankStr(raw);
    } else {
      raw = `${blankStr(r[latIdx]) ? '' : r[latIdx]}, ${blankStr(r[lonIdx]) ? '' : r[lonIdx]}`;
      c = parseLatLonPair(r[latIdx], r[lonIdx], swap);
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
      renderCoords();        // coords/lat/lon mapping affects the coordinate panel
      refreshReconSection(); // name/country mapping affects what can be reconciled
    });
  });
}

function renderPreview() {
  const cols = project.columns.map((c) => c.name);
  el('recon-preview-head').innerHTML = '<tr>' + cols.map((c) => `<th>${truncate(c, 40)}</th>`).join('') + '</tr>';
  el('recon-preview-body').innerHTML = project.rows.slice(0, PREVIEW_ROWS).map((r) =>
    '<tr>' + cols.map((_, i) => `<td>${truncate(r[i])}</td>`).join('') + '</tr>').join('');
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
  renderPreview();
  refreshReconSection();
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
  el('recon-result').classList.add('d-none');
  el('recon-resume').classList.add('d-none');
  el('recon-recon').classList.add('d-none');
  el('recon-progress-wrap').classList.add('d-none');
  el('recon-results-wrap').classList.add('d-none');
  ['recon-map-body', 'recon-preview-head', 'recon-preview-body', 'recon-summary', 'recon-saved',
    'recon-coords', 'recon-results-body', 'recon-recon-summary', 'recon-progress-text'].forEach((id) => {
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

function renderResults(built) {
  const matches = project.matches || {};
  const threshold = getThreshold();
  let matched = 0, auto = 0, nomatch = 0, pending = 0, rowsMatched = 0, rowsAuto = 0;
  built.map.forEach((v, key) => {
    const m = matches[key];
    if (!m) { pending += 1; return; }
    if (m.top) {
      matched += 1; rowsMatched += v.rows.length;
      if (isAutoConfirmed(m.top, threshold)) { auto += 1; rowsAuto += v.rows.length; }
    } else nomatch += 1;
  });
  setReconSummary(
    `<span class="text-success"><strong>${matched.toLocaleString()}</strong> matched</span> ` +
    `<span class="text-muted">(<strong>${auto.toLocaleString()}</strong> auto-confirmed)</span> · ` +
    `<span class="text-warning"><strong>${nomatch.toLocaleString()}</strong> no match</span> · ` +
    `<span class="text-muted"><strong>${pending.toLocaleString()}</strong> pending</span> — ` +
    `across <strong>${built.map.size.toLocaleString()}</strong> unique names, ` +
    `covering <strong>${rowsMatched.toLocaleString()}</strong> of ${project.total.toLocaleString()} rows ` +
    `(${rowsAuto.toLocaleString()} auto-confirmed).`);

  const rowsToShow = Math.min(project.rows.length, RECON_RESULTS_PREVIEW);
  const body = [];
  for (let i = 0; i < rowsToShow; i++) {
    const info = keyForRow(built, i);
    if (!info) continue;
    const m = matches[info.key];
    let status, top = '', score = '';
    if (!m) status = '<span class="badge bg-secondary">pending</span>';
    else if (m.top) {
      status = isAutoConfirmed(m.top, threshold)
        ? '<span class="badge bg-success">auto ✓</span>'
        : '<span class="badge bg-info text-dark">candidate</span>';
      top = `${truncate(m.top.name, 50)} <span class="text-muted small">${truncate(m.top.description || '', 30)}</span>`;
      score = m.top.score;
    } else status = '<span class="badge bg-warning text-dark">no match</span>';
    body.push(`<tr><td>${truncate(info.name, 50)}${info.country ? ` <span class="text-muted">(${esc(info.country)})</span>` : ''}</td>` +
              `<td>${status}</td><td>${top}</td><td>${score}</td></tr>`);
  }
  el('recon-results-body').innerHTML = body.join('');
  el('recon-results-wrap').classList.remove('d-none');
  el('recon-results-note').textContent = project.rows.length > RECON_RESULTS_PREVIEW
    ? `Showing the first ${RECON_RESULTS_PREVIEW} rows; summary counts cover all ${project.total.toLocaleString()}.` : '';
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
  stopRequested = false;
  updateProgress(done, total);
  const csrf = getCsrf();

  for (let b = 0; b < entries.length && !stopRequested; b += RECON_BATCH) {
    const slice = entries.slice(b, b + RECON_BATCH);
    const queries = {};
    slice.forEach(([, v], j) => {
      const q = { query: v.query, type: 'place', limit: RECON_CAND_LIMIT };
      if (v.country) q.countries = [v.country];
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
      const result = (data['q' + j] && data['q' + j].result) || [];
      project.matches[key] = { candidates: result, top: result[0] || null, at: new Date().toISOString() };
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

  showCapabilities();
  loadSaved();
}

if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
else init();
