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
import TypeTreeWidget from './typeTreeWidget.js';
import { loadAatVocab, aatLabel } from './aatVocab.js';
import { wireLicenseControl } from './licensePicker.js';

// Load the shared AAT vocab (version-gated IndexedDB cache, shared with Atlas +
// the Workbench — place#134) so chosen concepts can show a Getty label for a
// stored aat:<id> even before its tree branch is expanded.
loadAatVocab();

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
let Xlsx = null; // SheetJS — loaded on demand only when a spreadsheet is imported (kept out of the initial bundle)
const loadXlsx = async () => (Xlsx || (Xlsx = await import(/* webpackChunkName: "recon-xlsx" */ './recon-xlsx.js')));
let TextExtract = null; // mammoth (.docx) + pdf.js (.pdf) for the NER importer — loaded only when such a file is chosen
const loadTextExtract = async () => (TextExtract || (TextExtract = await import(/* webpackChunkName: "recon-textextract" */ './recon-textextract.js')));
import * as Sync from './recon-sync.js'; // collaborative-project API (place#112, Phase 0/1) — tiny, always bundled

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
  ['alt_names', 'Name variant(s)'],
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
  ['alt_names', /^(alt.?names?|alternat(e|ive).?names?|name.?variants?|variant.?names?|variants?|aka|also.?known.?as|aliases?)$/i],
  ['container', /^(county|counties|adm\d|admin\d|region|parish|province|state|district|department|prefecture|municipality|commune|canton|shire|hundred|wapentake|borough|riding|barony|arrondissement)$/i],
  ['country', /^(country|ccode|iso|nation)$/i],
  ['type', /^(type|feature.?type|fclass|category|placetype|kind)$/i],
  ['lat', /^(lat|latitude|y)$/i],
  ['lon', /^(lon|lng|long|longitude|x)$/i],
  ['coords', /coord|geometry|geom|wkt|gridref|grid.?ref|osgb|national.?grid|easting|northing/i],
  ['date', /^(date|year|start|end|from|to|period|century)$/i],
  ['id', /^(id|uid|key|identifier|wikidata|qid|geonames|gn.?id)$/i],
];

// Known administrative hierarchy: coarse → fine. Used by initChain() to order the containment chain by
// the OBVIOUS real-world nesting (country > region > county > hundred/wapentake/parish > place) rather
// than just the left-to-right column order, so e.g. a "Parish, County, Region" spreadsheet still nests
// correctly. Lower rank = coarser (contains the others). Columns whose header isn't recognised keep
// their dataset order, sorted after the known levels.
const ADMIN_RANK = [
  [/^(country|nation|ccode|iso)$/i, 0],
  [/^(region|province|state|territory|arrondissement)$/i, 10],
  [/^(county|counties|shire|department|canton|prefecture|riding|barony)$/i, 20],
  [/^(district|borough|municipality|commune|adm\d|admin\d)$/i, 30],
  [/^(hundred|wapentake)$/i, 40],
  [/^(parish)$/i, 45],
];
function adminRank(columnName, fallbackIdx) {
  const n = String(columnName || '').trim();
  for (const [re, rank] of ADMIN_RANK) if (re.test(n)) return rank;
  return 100 + fallbackIdx; // unknown levels: keep dataset order, after all known levels
}

function detectRole(columnName) {
  const n = String(columnName || '').trim();
  for (const [role, re] of ROLE_HINTS) if (re.test(n)) return role;
  return 'other';
}

// Turn the transient 'container' markers into a default containment chain: link the detected
// container columns (in dataset order, coarse → fine) down to the 'name' column via contains:<child>.
// If no name column was detected, the deepest container becomes the name (the toponym reconciled).
function initChain(columns) {
  let containers = columns.map((c, i) => i).filter((i) => columns[i].role === 'container');
  // Order by the KNOWN administrative hierarchy (country > region > county > hundred/wapentake >
  // parish …), falling back to dataset column order for unrecognised levels. So a spreadsheet whose
  // admin columns are in an odd order still nests coarse → fine correctly. See ADMIN_RANK.
  containers = containers
    .map((i) => ({ i, rank: adminRank(columns[i].name, i) }))
    .sort((a, b) => a.rank - b.rank)
    .map((o) => o.i);
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
  // If the WGS84 columns materialised from a coordinate source have since been deleted, restore that
  // source's coordinate role so its panel reappears (and can be re-inserted) — mirroring the date panel,
  // which keeps its source 'date' column. `wasCoords` is tagged by insertWgs84Columns on the demoted
  // source, and cleared here on restore or by a manual role change.
  const hasCoords = project.columns.some((c) => c.role === 'coords');
  const hasLatLonPair = project.columns.some((c) => c.role === 'lat') && project.columns.some((c) => c.role === 'lon');
  if (!hasCoords && !hasLatLonPair) {
    const src = project.columns.find((c) => c.wasCoords);
    if (src) { src.role = 'coords'; delete src.wasCoords; }
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

// ── HTML-entity detection & decoding (snag #149) ────────────────────────────
// Some source CSVs carry HTML-encoded characters (e.g. "Br&ygrave;n" for "Brỳn"). Standard named and
// numeric entities decode via the browser; but a few Welsh diacritics (ŵ ŷ and their grave forms) have
// NO standard named entity, and data often hand-codes them as &wcirc;/&ycirc;/&ygrave;/&wgrave;, which
// browsers leave raw. We handle both. Purely opt-in — offered on import, never applied silently.
const NONSTD_ENTITIES = {
  wcirc: 'ŵ', Wcirc: 'Ŵ', ycirc: 'ŷ', Ycirc: 'Ŷ',
  ygrave: 'ỳ', Ygrave: 'Ỳ', wgrave: 'ẁ', Wgrave: 'Ẁ',
  yacute: 'ý', Yacute: 'Ý', wacute: 'ẃ', Wacute: 'Ẃ',
};
let _entityTextarea = null;
function decodeEntities(s) {
  s = String(s == null ? '' : s);
  if (s.indexOf('&') < 0) return s;
  s = s.replace(/&([A-Za-z]+);/g, (m, name) => (name in NONSTD_ENTITIES ? NONSTD_ENTITIES[name] : m));
  if (s.indexOf('&') < 0) return s;
  _entityTextarea = _entityTextarea || document.createElement('textarea');
  _entityTextarea.innerHTML = s; // decodes standard named + numeric (&#…;/&#x…;) entities
  return _entityTextarea.value;
}
const ENTITY_RE = /&(#\d+|#x[0-9a-fA-F]+|[A-Za-z][A-Za-z0-9]+);/;
// Count cells that look HTML-encoded (a decode would actually change them), with a small sample.
function scanEntities(rows) {
  let n = 0; const sample = [];
  for (let i = 0; i < rows.length; i++) {
    const r = rows[i]; if (!r) continue;
    for (let j = 0; j < r.length; j++) {
      const v = r[j];
      if (typeof v === 'string' && ENTITY_RE.test(v) && decodeEntities(v) !== v) {
        n += 1; if (sample.length < 4) sample.push(v.trim());
      }
    }
  }
  return { n, sample };
}

// ── State + DOM helpers ─────────────────────────────────────────────────────
let project = null; // { id, fileName, importedAt, columns:[{name,role}], rows:[[...]], total, delimiter? }
let _entityScan = null; // {n, sample} from the last import — drives the encoding-fix offer

function renderEncodingNotice() {
  const box = el('recon-encoding-notice'); if (!box) return;
  if (!project || !_entityScan || !_entityScan.n) { box.innerHTML = ''; return; }
  const eg = _entityScan.sample.map((s) => `<code>${esc(truncate(s, 24))}</code>`).join(', ');
  box.innerHTML = `<div class="alert alert-warning py-2 px-3 mb-0 d-flex align-items-center flex-wrap gap-2">
    <span><i class="fas fa-triangle-exclamation me-1"></i><strong>${_entityScan.n.toLocaleString()}</strong>
    value${_entityScan.n === 1 ? '' : 's'} look HTML-encoded (e.g. ${eg}). These won't match well until decoded.</span>
    <button type="button" class="btn btn-sm btn-outline-primary py-0" id="recon-fix-encoding">
      <i class="fas fa-wand-magic-sparkles me-1"></i>Convert to proper characters</button>
    <button type="button" class="btn btn-sm btn-link text-muted py-0" id="recon-dismiss-encoding">Ignore</button></div>`;
  const fix = el('recon-fix-encoding'); if (fix) fix.addEventListener('click', fixEncoding);
  const dis = el('recon-dismiss-encoding'); if (dis) dis.addEventListener('click', () => { _entityScan = null; renderEncodingNotice(); });
}
function fixEncoding() {
  if (!project) return;
  pushUndo({ type: 'columns', label: 'convert HTML-encoded characters', snapshot: columnSnapshot() });
  let changed = 0;
  project.rows.forEach((r) => { for (let j = 0; j < r.length; j++) { const d = decodeEntities(r[j]); if (d !== r[j]) { r[j] = d; changed += 1; } } });
  _entityScan = null;
  normalizeChain(); // values changed → matches may be stale; handled by the invalidation note below
  if (project.matches && Object.keys(project.matches).length) { reconStaleNote = 'Values changed (encoding fix) — re-reconcile affected columns.'; }
  persist();
  renderAll();
  renderEncodingNotice();
  flashSaved(`Converted ${changed.toLocaleString()} value${changed === 1 ? '' : 's'} to proper characters.`);
}

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
  schedulePush(); // if this project is server-backed and editable, sync in the background (Collab)
}

// ── Rendering ───────────────────────────────────────────────────────────────
function isChildRole(role) { return role === 'name' || role === 'contains'; } // a valid containment child
function roleSelectHTML(colIndex, col) {
  const cur = col.role === 'contains' ? `contains:${col.child}` : col.role;
  // 'Other (ignore)' is de-emphasised (greyed/italic) so it reads as "not a real role" and stands out
  // among the meaningful options instead of hiding among them (snag #153).
  const std = ROLES.map(([val, label]) => `<option value="${val}"${val === cur ? ' selected' : ''}${val === 'other' ? ' class="recon-role-opt-other"' : ''}>${val === 'other' ? '— ' + label + ' —' : label}</option>`);
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
  project.columns[coordsIdx].wasCoords = true; // remember it was the coord source: if the wgs84 columns
                                               // are later deleted, normalizeChain restores this role so
                                               // the panel reappears (see normalizeChain)
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
      delete project.columns[i].wasCoords; // a manual role choice supersedes the demoted-coord-source tag
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
  renderIdNotice();
}

// ── Identifier column: duplicate detection + UUID generation (issue #143) ─────
// A dataset's Identifier column should hold UNIQUE values (they become the LPF @id and the reconciliation
// row key). If the mapped 'id' column has duplicates, warn and offer to mint UUIDs into a new column
// instead. Minting a UUID id column is offered in any case (even with no id column mapped).
function idDupInfo() {
  const idIdx = colIndexByRole('id');
  if (idIdx < 0) return { idIdx, dups: 0, filled: 0 };
  const seen = new Set(); let dups = 0, filled = 0;
  project.rows.forEach((r) => {
    const v = String(r[idIdx] == null ? '' : r[idIdx]).trim();
    if (!v) return;
    filled += 1;
    if (seen.has(v)) dups += 1; else seen.add(v);
  });
  return { idIdx, dups, filled };
}
function renderIdNotice() {
  const box = el('recon-id-notice'); if (!box || !project) return;
  const { idIdx, dups } = idDupInfo();
  const genBtn = '<button type="button" class="btn btn-sm btn-outline-primary py-0" id="recon-gen-uuid"><i class="fas fa-fingerprint me-1"></i>Generate UUIDs in a new column</button>';
  if (idIdx >= 0 && dups > 0) {
    box.innerHTML = `<div class="alert alert-warning py-2 px-3 mb-0 d-flex align-items-center flex-wrap gap-2">
      <span><i class="fas fa-triangle-exclamation me-1"></i>The <strong>Identifier</strong> column
      “${esc(truncate(project.columns[idIdx].name, 30))}” has <strong>${dups.toLocaleString()}</strong>
      duplicate value${dups === 1 ? '' : 's'}. Identifiers should be unique.</span>${genBtn}</div>`;
  } else {
    // No duplicate problem — still offer to add a UUID identifier column.
    box.innerHTML = `<span class="text-muted">${genBtn} <span class="ms-1">Add a unique identifier column if your data has none.</span></span>`;
  }
  const b = el('recon-gen-uuid'); if (b) b.addEventListener('click', generateUuidColumn);
}
function uuid4() {
  if (window.crypto && window.crypto.randomUUID) return window.crypto.randomUUID();
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (window.crypto ? window.crypto.getRandomValues(new Uint8Array(1))[0] & 15 : Math.floor(Math.random() * 16));
    return (c === 'x' ? r : (r & 0x3) | 0x8).toString(16);
  });
}
function generateUuidColumn() {
  if (!project) return;
  pushUndo({ type: 'columns', label: 'generate UUID identifier column', snapshot: columnSnapshot() });
  // Demote any existing id column to 'other' so the new UUID column is the sole Identifier.
  const oldId = colIndexByRole('id');
  if (oldId >= 0) { project.columns[oldId].role = 'other'; delete project.columns[oldId].child; }
  let name = 'uuid';
  const taken = new Set(project.columns.map((c) => c.name));
  let n = 2; while (taken.has(name)) name = `uuid_${n++}`;
  project.columns.push({ name, role: 'id' });
  project.rows.forEach((r) => r.push(uuid4()));
  normalizeChain();
  renderMapping(); persist(); renderPreview(); refreshExport();
  flashSaved(`Added “${name}” with a unique UUID per row`);
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
  const sd = el('recon-tf-splitdelim'); if (sd) sd.value = ',';
  const sr = el('recon-tf-splitrev'); if (sr) sr.checked = false;
  const nc = el('recon-tf-newcol'); if (nc) nc.checked = false;
  const nn = el('recon-tf-newcolname');
  if (nn) { nn.value = ''; nn.placeholder = truncate(project.columns[col].name, 28) + ' (transformed)'; nn.classList.add('d-none'); }
  renderTransformPreview();
  renderSplitPreview();
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
  // Writing to a new column is worth doing even when every value comes through unchanged (the point may
  // be to keep a copy), so Apply stays enabled there — unlike an in-place edit that would be a no-op.
  const toNew = !!((el('recon-tf-newcol') || {}).checked);
  box.innerHTML = (changed
    ? `<div class="small text-muted mb-1"><strong>${changed.toLocaleString()}</strong> of ${project.rows.length.toLocaleString()} cells will change:</div>${rows}`
    : '<span class="text-muted small">No cells would change.</span>')
    + (toNew ? '<div class="small text-muted mt-1"><i class="fas fa-arrow-turn-down me-1"></i>written to a new column; this one is left as it is.</div>' : '');
  if (applyBtn) applyBtn.disabled = !changed && !toNew;
}
// Write the transform's output to a NEW column, leaving the source untouched — OpenRefine's "add column
// based on this column" (place#194). The new column starts as 'other' (ignored): it is the user's job to
// give it a role, and adding one silently to the reconciliation chain would invalidate existing matches.
function applyTransformToNewColumn() {
  const col = _transformCol; const fn = _pendingTransform.fn;
  const snap = columnSnapshot();
  const typed = ((el('recon-tf-newcolname') || {}).value || '').trim();
  const name = uniqueHeader(typed || `${project.columns[col].name} (transformed)`, project.columns.map((c) => c.name));
  const at = project.columns.length;
  project.columns.push({ name, role: 'other' });
  project.rows.forEach((r) => { r[at] = fn(r[col]); });
  project.showIgnored = true; // …or the new column would be created out of sight
  pushUndo({ type: 'columns', label: `${_pendingTransform.label} → “${truncate(name, 20)}”`, snapshot: snap });
  persist(); rerenderData();
  flashSaved(`${_pendingTransform.label} written to new column “${truncate(name, 24)}”`);
}
function applyTransform() {
  if (!_pendingTransform || _transformCol < 0 || !project) return;
  const newCol = el('recon-tf-newcol');
  if (newCol && newCol.checked) return applyTransformToNewColumn();
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

// ── Split one field into containment-level columns ──────────────────────────
// A single field like "Rotherhithe, Surrey, England, UK" (or "Rotherhithe, ,,UK" with empty
// placeholder levels that keep the levels aligned across rows) is split into separate columns wired as
// a containment hierarchy: the innermost value becomes the place `name`, each outer value a `contains`
// container of the next level in. Positional — token 0 is the innermost, unless "outermost first" is on.
function splitDelimiter() { const v = (el('recon-tf-splitdelim') || {}).value; return (v && v.length) ? v : ','; }
function splitReversed() { return !!((el('recon-tf-splitrev') || {}).checked); }
function splitRowLevels(cell, delim, reversed) {
  const toks = String(cell == null ? '' : cell).split(delim).map((s) => s.trim());
  return reversed ? toks.reverse() : toks; // index 0 = innermost (place)
}
function splitStats(col, delim, reversed) {
  let maxN = 0; let sample = null;
  for (let i = 0; i < project.rows.length; i++) {
    const t = splitRowLevels(project.rows[i][col], delim, reversed);
    if (t.length > maxN) maxN = t.length;
    if (!sample && t.filter((x) => x !== '').length >= 2) sample = t;
  }
  return { maxN, sample };
}
function renderSplitPreview() {
  const box = el('recon-tf-splitpreview'); const btn = el('recon-tf-splitbtn');
  if (!box || _transformCol < 0 || !project) return;
  const { maxN, sample } = splitStats(_transformCol, splitDelimiter(), splitReversed());
  if (maxN < 2) {
    box.innerHTML = '<span class="text-warning">No rows have 2+ levels with this delimiter — nothing to split.</span>';
    if (btn) btn.disabled = true; return;
  }
  const chips = (sample || []).map((v, i) => `<span class="badge ${i === 0 ? 'bg-primary' : 'bg-secondary'}">${esc(truncate(v || '∅', 18))}</span>`).join(' ');
  box.innerHTML = `Creates up to <strong>${maxN}</strong> columns · innermost <span class="badge bg-primary">place</span> → outermost container.` +
    (sample ? `<div class="mt-1">e.g. ${chips}</div>` : '');
  if (btn) btn.disabled = false;
}
function applySplit() {
  const col = _transformCol;
  if (col < 0 || !project) return;
  const delim = splitDelimiter(); const reversed = splitReversed();
  const rowLevels = project.rows.map((r) => splitRowLevels(r[col], delim, reversed));
  const N = rowLevels.reduce((m, t) => Math.max(m, t.length), 0);
  if (N < 2) return;
  const snap = columnSnapshot();
  const src = project.columns[col].name;
  const taken = project.columns.map((c) => c.name);
  const startIdx = project.columns.length;
  for (let i = 0; i < N; i++) {
    const nm = uniqueHeader(`${src}_${i + 1}`, taken); taken.push(nm);
    project.columns.push({ name: nm, role: 'other' });
  }
  project.rows.forEach((r, ri) => { const t = rowLevels[ri]; for (let i = 0; i < N; i++) r.push(i < t.length ? t[i] : ''); });
  // The split's innermost level is now the authoritative place name, so any pre-existing Place-name
  // column is superseded — demote it to 'other'. Otherwise two columns claim role 'name' and
  // colIndexByRole('name') resolves to the stale one (wrong map marker/popup, wrong LPF title).
  project.columns.forEach((c, i) => { if (i < startIdx && c.role === 'name') c.role = 'other'; });
  // Wire the hierarchy: index 0 = innermost place name; each outer level "contains" the level within it.
  project.columns[startIdx].role = 'name';
  for (let i = 1; i < N; i++) { const c = project.columns[startIdx + i]; c.role = 'contains'; c.child = startIdx + (i - 1); }
  project.columns[col].role = 'other'; // the combined source is superseded by the split columns
  project.showIgnored = true;
  normalizeChain();
  pushUndo({ type: 'columns', label: `split “${src}” into ${N} levels`, snapshot: snap });
  persist(); rerenderData();
  flashSaved(`Split “${truncate(src, 24)}” into ${N} containment columns`);
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
  return Math.max(88, Math.min(320, maxLen * 7 + 40)); // +40 leaves room for the header's transform wand
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
  // Each header carries the same column-transform affordance as the mapping table above. Testers were
  // finding only "Edit cells" and concluding the OpenRefine-style tools had been dropped — they were
  // there all along, behind one small wand icon in a table most people scroll straight past (place#194).
  el('recon-preview-head').innerHTML = '<tr>' + _previewVisCols.map((i) =>
    `<th title="${esc(project.columns[i].name)}"><span class="recon-preview-th-name">${truncate(project.columns[i].name, 40)}</span>` +
    `<button type="button" class="btn btn-link p-0 recon-preview-tf" data-col="${i}"
       title="Clean or transform this column — trim, case, accents, find &amp; replace (regex), split into containment columns"
       aria-label="Transform column ${esc(project.columns[i].name)}"><i class="fas fa-wand-magic-sparkles"></i></button></th>`).join('') + '</tr>';
  el('recon-preview-head').querySelectorAll('.recon-preview-tf').forEach((b) =>
    b.addEventListener('click', (e) => { e.stopPropagation(); openTransformModal(Number(b.dataset.col)); }));
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
  if (_rtPresence.length) paintPresenceCursors(); // keep teammates' cursors as the window repaints
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
  rtSetCursor({ row: ri, col: ci }); // tell teammates which cell I'm in
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
  rtSetCursor(null);
  const { ri, ci } = _previewEditing; _previewEditing = null;
  const td = previewCellEl(ri, ci);
  if (td) { td.classList.remove('recon-cell-editing'); td.innerHTML = truncate(project.rows[ri][ci], 60); }
}
function commitCellEdit() {
  if (!_previewEditing) return;
  rtSetCursor(null);
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
      // Don't report a fail-closed scope as "N no match" — nothing was matched against (place#144).
      sc.textContent = (scopeFailed() && !matched)
        ? 'region not applied — no results'
        : `${matched.toLocaleString()} matched · ${nomatch.toLocaleString()} no match · ${pending.toLocaleString()} pending`;
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
  lastScope = null; lastVariantsDropped = 0; lastDerivedForms = new Set(); // drop the previous dataset's gateway report
  ['recon-map-body', 'recon-preview-head', 'recon-preview-body', 'recon-summary', 'recon-saved',
    'recon-coords', 'recon-dates', 'recon-results-body', 'recon-recon-summary', 'recon-progress-text',
    'recon-scope-notice', 'recon-review-card', 'recon-review-progress'].forEach((id) => {
    const n = el(id); if (n) n.innerHTML = '';
  });
  const input = el('recon-file'); if (input) input.value = '';
}

// ── Import + lifecycle ──────────────────────────────────────────────────────
// ── Anonymous usage analytics (Plausible custom events) ──────────────────────────────────────────
// Privacy-first, so it's safe on a local-first tool: cookieless, no user id, and NEVER any dataset
// contents — only funnel/friction signals with coarse, non-identifying props (bucketed row counts,
// chosen export format). Sent to WHG's self-hosted Plausible (script already loaded in
// base_webpack.html). Lets us see where people drop off (import → reconcile → export/contribute)
// without touching the data they bring. See developer note + place#112 / Palak monitoring.
const _tracked = new Set(); // per-page-load dedupe for one-shot funnel events
function track(event, props) {
  try {
    if (typeof window !== 'undefined' && typeof window.plausible === 'function') {
      window.plausible(event, props ? { props } : undefined);
    }
  } catch (_) { /* analytics must never break the workbench */ }
}
function trackOnce(event, props) { if (_tracked.has(event)) return; _tracked.add(event); track(event, props); }
// Coarse buckets so a row count can never fingerprint a specific dataset.
function bucketCount(n) {
  n = Number(n) || 0;
  if (n <= 0) return '0';
  if (n <= 10) return '1-10';
  if (n <= 50) return '11-50';
  if (n <= 200) return '51-200';
  if (n <= 1000) return '201-1000';
  if (n <= 5000) return '1001-5000';
  return '5000+';
}

// Turn a parsed { columns, rows, total, delimiter? } into the working project + render. Shared by
// every importer (CSV/TSV/JSON file, Excel spreadsheet, Google Sheet URL).
async function finishImport(parsed, fileName, format) {
  project = {
    id: CURRENT,
    fileName,
    importedAt: new Date().toISOString(),
    columns: initChain(parsed.columns.map((name) => ({ name, role: detectRole(name) }))),
    rows: parsed.rows,
    total: parsed.total,
    delimiter: parsed.delimiter || null,
  };
  el('recon-resume').classList.add('d-none'); // fresh import, not a resume
  _tracked.clear(); // new dataset → let the once-per-dataset funnel events fire again
  console.log(`[recon] imported "${fileName}" locally: ${project.total} rows, ${project.columns.length} cols`);
  track('MyD: import', {
    source: fileName === 'reconciliation-demo.csv' ? 'sample' : (format || 'file'),
    format: format || 'csv',
    rows: bucketCount(project.total),
    cols: String(project.columns.length),
  });
  stopRealtime();               // a fresh import replaces any prior (server) project
  setCollabBadge('local');      // …so it's device-only until saved (clear any stale badge)
  _entityScan = scanEntities(project.rows); // detect HTML-encoded characters to offer a one-click fix
  renderAll();
  renderEncodingNotice();
  if (navigator.storage && navigator.storage.persist) {
    try { await navigator.storage.persist(); } catch (_) { /* best effort */ }
  }
  await persist();
}

function importError(name, err) {
  console.error('[recon] import failed:', err);
  el('recon-result').classList.remove('d-none');
  el('recon-summary').innerHTML =
    `<span class="text-danger"><i class="fas fa-exclamation-triangle me-1"></i>` +
    `Could not import <strong>${esc(truncate(name, 60))}</strong>: ${esc(err.message)}</span>`;
  el('recon-map-body').innerHTML = '';
  el('recon-preview-head').innerHTML = '';
  el('recon-preview-body').innerHTML = '';
}

function handleFile(file) {
  // Excel workbooks are binary — read as an ArrayBuffer and parse with the lazy SheetJS chunk.
  if (/\.(xlsx|xls)$/i.test(file.name)) { handleSpreadsheetFile(file); return; }
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
        track('MyD: resume', { rows: bucketCount(project.total) });
        console.log(`[recon] restored .whgproj: ${project.total} rows`);
        return;
      }
      const isJSON = /\.json$/i.test(file.name) || text.trim().startsWith('[') || text.trim().startsWith('{');
      const parsed = isJSON ? fromJSON(JSON.parse(text)) : fromDelimited(text);
      await finishImport(parsed, file.name, isJSON ? 'json' : 'csv');
    } catch (err) { importError(file.name, err); }
  };
  reader.onerror = () => console.error('[recon] file read error', reader.error);
  reader.readAsText(file);
}

async function handleSpreadsheetFile(file) {
  try {
    const buf = await file.arrayBuffer();
    const mod = await loadXlsx();
    const parsed = mod.parseWorkbook(buf);
    await finishImport(parsed, file.name, 'xlsx');
    // Keep the original workbook bytes so the user can save their edits back to Excel with every OTHER
    // sheet intact (round-trip). Skip very large files to avoid bloating the local store. The ArrayBuffer
    // rides along in IndexedDB (structured clone); it is NOT included in a JSON .whgproj backup.
    if (buf.byteLength <= 12 * 1024 * 1024) {
      project.sourceXlsx = { bytes: buf, sheet: parsed.sheet, fileName: file.name };
      await persist();
    }
  } catch (err) { importError(file.name, err); }
}

// Save the current working table back to Excel. If the dataset arrived as an .xlsx, the imported sheet
// is rewritten in-place and every other sheet is preserved; otherwise a fresh single-sheet workbook is
// produced. Uses the lazy SheetJS chunk.
async function saveAsExcel() {
  if (!project) return;
  const btn = el('recon-xlsx-save');
  if (btn) btn.disabled = true;
  try {
    const mod = await loadXlsx();
    const src = project.sourceXlsx || null;
    const cols = project.columns.map((c) => c.name);
    const bytes = mod.writeWorkbook(src && src.bytes, src && src.sheet, cols, project.rows);
    const base = ((src && src.fileName) || project.fileName || 'workbook').replace(/\.[^.]+$/, '');
    downloadText(base + '.xlsx', bytes, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    flashSaved(src ? 'Saved to Excel (other sheets kept)' : 'Saved as Excel');
  } catch (err) {
    console.error('[recon] xlsx save failed', err);
    flashSaved('⚠ could not save the Excel file');
  } finally {
    if (btn) btn.disabled = false;
  }
}

// Import a shared Google Sheet: the server fetches it as CSV (bypassing browser CORS), we parse it
// locally like any other delimited file. The sheet must be shared "anyone with the link can view".
async function importGoogleSheet() {
  const input = el('recon-gsheet-url');
  const msg = el('recon-gsheet-msg');
  const btn = el('recon-gsheet-btn');
  const url = (input && input.value || '').trim();
  const setMsg = (html) => { if (msg) msg.innerHTML = html; };
  if (!url) { setMsg('<span class="text-muted">Paste a Google Sheet link first.</span>'); return; }
  if (btn) btn.disabled = true;
  setMsg('<span class="text-muted"><i class="fas fa-spinner fa-spin me-1"></i>Fetching the sheet…</span>');
  try {
    const res = await Sync.importGSheet(url);
    if (res.status !== 200 || !res.data || res.data.csv == null) {
      setMsg(`<span class="text-danger">${esc((res.data && res.data.error) || 'Could not fetch that sheet.')}</span>`);
      return;
    }
    const parsed = fromDelimited(res.data.csv);
    await finishImport(parsed, res.data.name || 'google-sheet.csv', 'gsheet');
    setMsg('');
    if (input) input.value = '';
  } catch (err) {
    console.warn('[recon] gsheet import failed', err);
    setMsg('<span class="text-danger">Import failed — check the link and your connection, then try again.</span>');
  } finally {
    if (btn) btn.disabled = false;
  }
}

// ── Place-name extraction from text (NER) ────────────────────────────────────────────────────────
// Get plain text from the textarea or an uploaded .txt/.md/.html file, send it to WHG's server-side
// spaCy service (the one MyD step that leaves the browser — the UI says so), and turn the detected
// place names into a small table (name · mentions · context) that becomes the project. Binary formats
// (.docx, .pdf) and Google Docs are a planned fast-follow.
async function nerReadFile(file) {
  const name = file.name || '';
  if (/\.docx$/i.test(name)) { const mod = await loadTextExtract(); return mod.extractDocx(await file.arrayBuffer()); }
  if (/\.pdf$/i.test(name)) { const mod = await loadTextExtract(); return mod.extractPdf(await file.arrayBuffer()); }
  const text = await file.text();
  if (/\.html?$/i.test(name)) {
    try { return new DOMParser().parseFromString(text, 'text/html').body.textContent || ''; }
    catch (_) { return text; }
  }
  return text; // .txt / .md / other plain text
}
async function extractPlaceNames() {
  const area = el('recon-ner-text');
  const msg = el('recon-ner-msg');
  const btn = el('recon-ner-btn');
  const setMsg = (html) => { if (msg) msg.innerHTML = html; };
  const text = (area && area.value || '').trim();
  if (!text) { setMsg('<span class="text-muted">Paste or upload some text first.</span>'); return; }
  if (btn) btn.disabled = true;
  setMsg('<span class="text-muted"><i class="fas fa-spinner fa-spin me-1"></i>Finding place names…</span>');
  try {
    const res = await Sync.ner(text);
    if (res.status !== 200 || !res.data || !Array.isArray(res.data.entities)) {
      setMsg(`<span class="text-danger">${esc((res.data && res.data.error) || 'Extraction failed — please try again.')}</span>`);
      return;
    }
    const ents = res.data.entities;
    if (!ents.length) { setMsg('<span class="text-warning">No place names were found in that text.</span>'); return; }
    // When the server has a preliminary reconciliation (gazetteer + geo-disambiguation), surface it as
    // extra columns so the extracted places arrive already located and identified.
    const hasMatch = ents.some((e) => e.match);
    const cols = hasMatch
      ? ['place_name', 'mentions', 'whg_match', 'country', 'lon', 'lat', 'context']
      : ['place_name', 'mentions', 'context'];
    const rows = ents.map((e) => {
      const m = e.match || {};
      const row = [String(e.name || ''), String(e.count || 1)];
      if (hasMatch) {
        row.push(String(m.title || ''), (m.ccodes || []).join(' '),
          m.lng != null ? String(m.lng) : '', m.lat != null ? String(m.lat) : '');
      }
      row.push(String(e.context || ''));
      return row;
    });
    await finishImport({ columns: cols, rows, total: ents.length }, 'extracted-places.csv', 'ner');
    // The extracted names ARE the toponyms — assign the place-name role deterministically (the generic
    // column-name heuristic doesn't recognise "place_name"), so reconciliation is ready immediately.
    const nameCol = project.columns.find((c) => c.name === 'place_name');
    if (nameCol && nameCol.role !== 'name') { nameCol.role = 'name'; normalizeChain(); renderAll(); await persist(); }
    const n = res.data.reconciled || 0;
    setMsg(n ? `<span class="text-success">Located ${n} of ${ents.length} place${ents.length === 1 ? '' : 's'} against WHG (preliminary — review to confirm).</span>` : '');
    if (area) area.value = '';
  } catch (err) {
    console.warn('[recon] NER failed', err);
    setMsg('<span class="text-danger">Extraction failed — check your connection and try again.</span>');
  } finally {
    if (btn) btn.disabled = false;
  }
}
async function nerPasteFromClipboard() {
  const area = el('recon-ner-text');
  if (!area || !navigator.clipboard || !navigator.clipboard.readText) return;
  try { const t = await navigator.clipboard.readText(); if (t) area.value = t; area.focus(); }
  catch (_) { /* permission denied — the textarea still accepts a manual paste */ }
}
async function nerLoadFile(file) {
  const area = el('recon-ner-text');
  const msg = el('recon-ner-msg');
  if (!file || !area) return;
  if (msg) msg.innerHTML = '<span class="text-muted"><i class="fas fa-spinner fa-spin me-1"></i>Reading file…</span>';
  try { area.value = await nerReadFile(file); if (msg) msg.innerHTML = ''; area.focus(); }
  catch (err) { console.warn('[recon] file read failed', err); if (msg) msg.innerHTML = `<span class="text-danger">Could not read that file — is it a valid ${/\.pdf$/i.test(file.name) ? 'PDF' : /\.docx$/i.test(file.name) ? 'Word (.docx)' : 'text'} file?</span>`; }
}
// Import a shared Google Doc as plain text (via the same-origin proxy) into the extractor textarea.
async function importGoogleDoc() {
  const input = el('recon-ner-gdoc-url');
  const msg = el('recon-ner-msg');
  const btn = el('recon-ner-gdoc-btn');
  const area = el('recon-ner-text');
  const url = (input && input.value || '').trim();
  const setMsg = (h) => { if (msg) msg.innerHTML = h; };
  if (!url) { setMsg('<span class="text-muted">Paste a Google Doc link first.</span>'); return; }
  if (btn) btn.disabled = true;
  setMsg('<span class="text-muted"><i class="fas fa-spinner fa-spin me-1"></i>Fetching the document…</span>');
  try {
    const res = await Sync.importGDoc(url);
    if (res.status !== 200 || !res.data || res.data.text == null) {
      setMsg(`<span class="text-danger">${esc((res.data && res.data.error) || 'Could not fetch that document.')}</span>`);
      return;
    }
    if (area) { area.value = res.data.text; area.focus(); }
    setMsg('<span class="text-success">Loaded — now click “Extract place names”.</span>');
    if (input) input.value = '';
  } catch (err) {
    console.warn('[recon] gdoc import failed', err);
    setMsg('<span class="text-danger">Import failed — check the link and your connection, then try again.</span>');
  } finally {
    if (btn) btn.disabled = false;
  }
}

async function clearData() {
  stopRealtime(); // drop any live collab session before wiping local state
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

// LPF expresses confidence as `certainty` on a relation — an enum, not a number. A match a person
// confirmed is `certain` whatever it scored; an auto-confirmed one is only as good as its score, and
// the user's own auto-confirm threshold is the bar they set for "good enough". The numeric score
// travels alongside as `whg_match_score` for anything that wants the raw value. See place#184.
function certaintyFor(match) {
  if (!match) return 'uncertain';
  if (match.accepted) return 'certain';
  const score = Number(match.score);
  if (!Number.isFinite(score)) return 'less-certain';
  if (score >= getThreshold()) return 'certain';
  return score >= 70 ? 'less-certain' : 'uncertain';
}

// The reader's Wikipedia edition for a match: their chosen interface language, else English, else
// whatever the place has. `whg_lang` is the same preference that drives map labels (place#… language
// preference), so an export follows the language the user has been reading the site in.
function preferredWikipedia(match) {
  const arts = (match && match.cand && match.cand.wikipedia) || [];
  if (!arts.length) return '';
  let lang = 'en';
  try { lang = (localStorage.getItem('whg_lang') || 'en').split('-')[0].toLowerCase(); } catch (_) { /* default */ }
  const pick = arts.find((w) => (w.lang || '').toLowerCase() === lang)
    || arts.find((w) => (w.lang || '').toLowerCase() === 'en')
    || arts[0];
  return (pick && pick.url) || '';
}
// The AAT types the MATCHED record carries (`place_types` from /reconcile) — what the place is, as
// opposed to the OpenRefine entity type, which is "Place" for everything.
function matchPlaceTypes(match) {
  const t = (match && match.cand && match.cand.place_types) || [];
  return t.filter((x) => x && (x.identifier || x.label));
}

function currentExportOptions() {
  const fmtEl = document.querySelector('input[name="recon-exp-fmt"]:checked');
  return {
    // Coordinates + ISO dates are materialised as columns in Step 2, so they're no longer export toggles.
    match: !!(el('recon-exp-match') && el('recon-exp-match').checked),
    enrich: !!(el('recon-exp-enrich') && el('recon-exp-enrich').checked),
    wikipedia: !!(el('recon-exp-wikipedia') && el('recon-exp-wikipedia').checked),
    format: fmtEl ? fmtEl.value : 'csv',
  };
}

// Assemble a per-row augmented record set. Returns { origHeaders, augHeaders, records } where each
// record is { orig:[cellValues], aug:{header:value}, coord:{lat,lon}|null, whenStart, whenEnd, match }.
// A header that can't collide with any name in `taken` (case-insensitive) — suffix _2, _3, … if needed.
function uniqueHeader(base, taken) {
  const lower = new Set(taken.map((h) => String(h).toLowerCase()));
  if (!lower.has(base.toLowerCase())) return base;
  let i = 2;
  while (lower.has((base + '_' + i).toLowerCase())) i += 1;
  return base + '_' + i;
}

async function buildExportRecords(opts, onProgress) {
  const nameCol = colIndexByRole('name');
  const built = buildUniqueQueries(nameCol); // export the NAME column's match as the primary whg_match_*
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
    augHeaders.push('whg_match_id', 'whg_match_title', 'whg_match_score', 'whg_match_source', 'whg_match_note');
    // Parent containment matches — id, title AND score, so the confidence in the container travels
    // with it exactly as it does for the primary match (place#184).
    adminCols.forEach((c) => augHeaders.push(`${colSlug(c)}_whg_id`, `${colSlug(c)}_whg_title`, `${colSlug(c)}_whg_score`));
  }
  // No `whg_match_description`: /reconcile builds a candidate's `description` synthetically, as
  // "Country: GB", to disambiguate candidates in the review cards. Exported under that name it read
  // as the matched place's description while only ever restating the country column (place#184).
  if (opts.enrich) augHeaders.push('whg_match_lon', 'whg_match_lat', 'whg_match_variants', 'whg_match_types');
  // Wikipedia link — a separate, explicit opt-in column, populated ONLY from Wikidata (wd) matches.
  // Its header is made unique so it can never collide with an existing column (or another aug column).
  const wikiHeader = opts.wikipedia ? uniqueHeader('wikipedia', project.columns.map((c) => c.name).concat(augHeaders)) : null;
  if (wikiHeader) augHeaders.push(wikiHeader);

  // Pre-fetch coordinates for resolved matches when enriching (reuses the review-pane cache).
  if (opts.enrich) {
    const ids = [];
    if (built) built.map.forEach((v, key) => { resolvedMatchList(key).forEach((x) => { if (x.id && !(x.id in _candCoord)) ids.push(x.id); }); });
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
    // Containers (County, Parish, …) resolved for this row. Computed regardless of the CSV/JSON
    // toggles because LPF carries them as relations, not as columns.
    const parents = adminCols
      .map((c) => ({ col: c, name: project.columns[c].name, match: resolvedMatchList(c + ':' + i)[0] || null }))
      .filter((p) => p.match);
    if (opts.match || opts.enrich || opts.wikipedia) {
      const list = info ? resolvedMatchList(info.key) : [];
      if (list.length) {
        match = { list };
        match.first = match.list[0];
      }
    }
    if (opts.match) {
      aug.whg_match_id = match ? match.list.map((x) => barePlaceId(x.id)).join('; ') : '';
      aug.whg_match_title = match ? match.list.map((x) => x.title).join('; ') : '';
      aug.whg_match_score = match ? match.list.map((x) => x.score).join('; ') : '';
      aug.whg_match_source = match ? [...new Set(match.list.map((x) => x.source))].join('; ') : '';
      // The reviewer's rationale for this row (place#180) — recorded even where
      // nothing matched, since "why I could not match this" is worth keeping too.
      aug.whg_match_note = info ? noteFor(info.key) : '';
      // Parent-column (containment) matches: explicit accepts, else the auto-confirmed top.
      adminCols.forEach((c) => {
        const a = (parents.find((p) => p.col === c) || {}).match || null;
        aug[`${colSlug(c)}_whg_id`] = (a && barePlaceId(a.id)) || '';
        aug[`${colSlug(c)}_whg_title`] = (a && a.title) || '';
        aug[`${colSlug(c)}_whg_score`] = (a && a.score != null) ? a.score : '';
      });
    }
    if (opts.enrich) {
      const f = match && match.first;
      const mc = f && (_candCoord[f.id] || null);
      aug.whg_match_lon = mc ? +mc.lon.toFixed(6) : '';
      aug.whg_match_lat = mc ? +mc.lat.toFixed(6) : '';
      aug.whg_match_variants = (f && f.cand && (f.cand.alt_names || []).join('; ')) || '';
      // The place's own AAT types. This used to read `cand.type`, which is the OpenRefine entity
      // type — so every row said "Place" whatever it was.
      aug.whg_match_types = matchPlaceTypes(f).map((t) => t.label || t.identifier).join('; ');
    }
    if (wikiHeader) {
      // ONE article per match, in the reader's own language where the place has it. A Wikidata
      // record carries a sitelink for every language edition it appears in — Reykjavík has 80-odd —
      // and dumping them all made the column unreadable and unusable. Blank unless the row was
      // reconciled to a record carrying sitelinks (in practice, a Wikidata one).
      aug[wikiHeader] = match ? [...new Set(match.list.map(preferredWikipedia).filter(Boolean))].join('; ') : '';
    }
    records.push({ row: i, orig, aug, coord, geom, whenStart, whenEnd, match, parents,
                   note: info ? noteFor(info.key) : '' });
  }
  // The chosen options travel WITH the records: the LPF builder needs to know whether enrichment and
  // the Wikipedia link were asked for, and it is handed only this object.
  return { origHeaders: project.columns.map((c) => c.name), augHeaders, records, opts };
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
      matches: rec.match ? rec.match.list.map((x) => barePlaceId(x.id)).join(';') : '',
      // Prefer the CONFIRMED container's title over the raw cell — it's the same place, spelled the
      // way the gazetteer spells it, which is what a consumer can match on (place#184).
      parent_name: (rec.parents && rec.parents.length && (rec.parents[0].match.title || '')) ||
        (countyIdx >= 0 ? rec.orig[countyIdx] : ''),
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
  const opts = data.opts || {};
  const features = data.records.map((rec, i) => {
    const title = nameIdx >= 0 ? String(rec.orig[nameIdx] || '') : '';
    const cc = countryIdx >= 0 && isCcode(rec.orig[countryIdx]) ? [String(rec.orig[countryIdx]).toUpperCase()] : [];
    const props = { title };
    if (cc.length) props.ccodes = cc;
    // LPF @id must be a URL or a namespace term (word:term). A bare id-column value or row index isn't,
    // so wrap non-conforming ids as `row:<value>` (a within-dataset local identifier).
    let atId = String(idIdx >= 0 && rec.orig[idIdx] != null && rec.orig[idIdx] !== '' ? rec.orig[idIdx] : (i + 1));
    if (!/^\w+:[^\s]+$/.test(atId) && !/^https?:\/\//.test(atId)) atId = 'row:' + atId.trim().replace(/\s+/g, '_');
    // Names: the primary toponym plus any alt_names variants the user tagged for this row (issue #143).
    const names = [];
    if (title) names.push({ toponym: title });
    rowVariants(rec.row).forEach((v) => { if (v && v !== title) names.push({ toponym: v }); });
    // Enrichment: the matched record's own toponyms. Opt-in (the Enrich box), and each carries a
    // citation naming the record it came from, so a reader can tell the contributor's names from the
    // gazetteer's. Previously these reached the CSV and were dropped from the LPF entirely (place#184).
    if (opts.enrich && rec.match && rec.match.first) {
      const seen = new Set(names.map((n) => n.toponym.toLowerCase()));
      const src = barePlaceId(rec.match.first.id);
      ((rec.match.first.cand && rec.match.first.cand.alt_names) || []).forEach((v) => {
        const t = String(v || '').trim();
        if (!t || seen.has(t.toLowerCase())) return;
        seen.add(t.toLowerCase());
        names.push({ toponym: t, citations: [{ label: `WHG reconciliation match ${src}`, '@id': src }] });
      });
    }
    const feat = {
      '@id': atId,
      type: 'Feature',
      properties: props,
      names,
    };
    // Place types: assigned per row in the table editor (project.rowTypes, keyed by source row index);
    // rows without their own type fall back to the global Scope → "What" AAT selection.
    const rt = rowTypesFor(rec.row);
    if (rt.length) feat.types = rt.map((t) => ({ identifier: t.id, label: t.text }));  // LPF place types (needed to contribute)
    else {
      const st = (scopeTypes().selected) || [];
      if (st.length) feat.types = st.map((t) => ({ identifier: t.id, label: t.text }));
      // Enrichment: fall back to the AAT types the MATCHED record carries. A row typed by neither the
      // user nor the dataset scope would otherwise export untyped — and place type is one of the
      // fields WHG's ingest requires, so this is often what makes an export contributable.
      else if (opts.enrich) {
        const mt = matchPlaceTypes(rec.match && rec.match.first);
        if (mt.length) feat.types = mt.map((t) => ({ identifier: t.identifier, label: t.label }));
      }
    }
    // Temporality: per-row parsed dates, else the global Scope → "When" year range (Scope period(s)
    // below). Build only the bounds that exist — an empty {in: undefined} fails LPF validation.
    const timespanFrom = (a, b) => {
      const t = {};
      if (a !== '' && a != null) t.start = { in: String(a) };
      if (b !== '' && b != null) t.end = { in: String(b) };
      return (t.start || t.end) ? t : null;
    };
    let ts = timespanFrom(rec.whenStart, rec.whenEnd);
    if (!ts) { const s = getScope(); if (s) ts = timespanFrom(s.start, s.end); }
    if (ts) feat.when = { timespans: [ts] };
    // Dataset-scope PeriodO period(s) apply to every place (scope-level, not per row).
    const scp = scopePeriods();
    if (scp.length) { feat.when = feat.when || {}; feat.when.periods = scp.map((p) => { const o = { name: p.label }; if (p.uri) o['@id'] = p.uri; return o; }); }
    if (rec.geom) feat.geometry = rec.geom;                              // override (point / line / polygon) wins
    else if (rec.coord) feat.geometry = { type: 'Point', coordinates: [+rec.coord.lon.toFixed(6), +rec.coord.lat.toFixed(6)] };
    else if (opts.enrich && rec.match && rec.match.first && _candCoord[rec.match.first.id]) {
      // Enrichment: the matched record's location, for a row that brought no coordinates of its own.
      // It is the gazetteer's point, not the contributor's, so it is cited as such and marked
      // less-certain — this is a located-by-match, not a surveyed position (place#184).
      const mc = _candCoord[rec.match.first.id];
      const src = barePlaceId(rec.match.first.id);
      feat.geometry = {
        type: 'Point',
        coordinates: [+mc.lon.toFixed(6), +mc.lat.toFixed(6)],
        certainty: 'less-certain',
        citations: [{ label: `WHG reconciliation match ${src}`, '@id': src }],
      };
    }
    // Each accepted/auto-confirmed match becomes a closeMatch link, carrying WHG's reconciliation
    // confidence under the SAME name the CSV/JSON exports use, so the score is findable by one name in
    // every format. `whg_match_score` is not an LPF term, but the schema puts no additionalProperties
    // bar on a link and WHG's ingest reads only type/identifier, so it rides along as an annotation
    // for whoever opens the file rather than changing how it is understood. See place#183.
    // Containment: the county/parish a row was reconciled within is data, not just a spreadsheet
    // column, so it travels as an LPF relation. `gvp:broaderPartitive` is the Getty term for
    // "is part of", which is what a container column asserts. `certainty` is LPF's own confidence
    // vocabulary; the numeric score rides alongside for anything that wants the raw value. WHG's
    // ingest stores relations (validation/create_dataset.py), so a contributed dataset keeps its
    // hierarchy instead of losing it at the door. See place#184.
    if (rec.parents && rec.parents.length) {
      feat.relations = rec.parents.map((p) => {
        const rel = {
          relationType: 'gvp:broaderPartitive',
          relationTo: barePlaceId(p.match.id),
          label: p.match.title || p.name,
          certainty: certaintyFor(p.match),
        };
        const score = Number(p.match.score);
        if (Number.isFinite(score)) rel.whg_match_score = score;
        return rel;
      });
    }
    const links = [];
    if (rec.match) {
      rec.match.list.forEach((x) => {
        // `certainty` is LPF's own confidence vocabulary. The spec offers it on relations but not on
        // links, which is a gap — a link is an assertion ("this place closeMatches that one") and a
        // reconciliation has a confidence in it. WHG's schema copy admits it on links (proposed
        // upstream as LinkedPasts/linked-places-format#52); consumers that don't know the field
        // ignore it, and the numeric score is carried alongside for those that want the raw value.
        const link = { type: 'closeMatch', identifier: barePlaceId(x.id), certainty: certaintyFor(x) };
        const score = Number(x.score);
        if (Number.isFinite(score)) link.whg_match_score = score;
        // The reviewer's own words about this identification. `certainty` says how
        // sure; this says why — the part no vocabulary can carry (place#180).
        if (rec.note) link.whg_match_note = rec.note;
        links.push(link);
      });
      // The Wikipedia article as LPF's own `primaryTopicOf` — the term the spec documents for exactly
      // this. Opt-in with the Wikipedia box, and previously it only ever reached the CSV (place#184).
      if (opts.wikipedia) {
        [...new Set(rec.match.list.map(preferredWikipedia).filter(Boolean))]
          .forEach((url) => links.push({ type: 'primaryTopicOf', identifier: url }));
      }
    }
    if (links.length) feat.links = links;
    return feat;
  });
  const fc = {
    type: 'FeatureCollection',
    '@context': 'https://raw.githubusercontent.com/LinkedPasts/linked-places-format/master/linkedplaces-context-v1.1.jsonld',
  };
  // Embed the dataset's citation & provenance before `features` so WHG's ingest picks it up when the
  // file is contributed/uploaded. `indexing` (schema.org) is what WHG reads today (citationIndexing);
  // `citation` (CSL-JSON, the format the LPF schema natively allows) is carried for future consumption.
  // Both sit ahead of the (possibly huge) features array so the server's streaming reader stops early.
  const cm = project ? citationModel() : null;
  if (cm) {
    const idx = citationIndexing(cm);
    if (idx && (idx.name || idx.creator || idx.citation || idx.url)) fc.indexing = idx;
    fc.citation = buildCslCitation(cm);
  }
  fc.features = features;
  return fc;
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
    track('MyD: export', { format: opts.format, rows: bucketCount(data.records.length) });
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
    // The licence the contributor chose in the citation builder. Server-side it is
    // validated against the vocabulary and resolved to Dataset.license; an empty or
    // unknown value leaves the dataset unlicensed rather than guessing (place#158).
    const citedLicence = (project.citation && project.citation.license) || '';
    if (citedLicence) addHidden('license', citedLicence);
    const fileInput = document.createElement('input'); fileInput.type = 'file'; fileInput.name = 'file';
    const dt = new DataTransfer(); dt.items.add(file); fileInput.files = dt.files; // programmatically attach the file
    form.appendChild(fileInput);
    document.body.appendChild(form);
    if (status) status.textContent = 'uploading to WHG…';
    track('MyD: contribute', { rows: bucketCount(data.records.length) });
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
  ['recon-exp-match', 'recon-exp-enrich', 'recon-exp-wikipedia'].forEach((id) => { const box = el(id); if (box) box.disabled = !hasMatches; });
  const sum = el('recon-pane-sum-export');
  if (sum) {
    // Count what the export will actually carry — explicit accepts AND auto-confirmed rows. Counting
    // decisions alone reported "0 confirmed matches" for a fully auto-matched dataset (place#183).
    let confirmed = 0;
    Object.keys(project.matches || {}).forEach((key) => { if (resolvedMatchList(key).length) confirmed += 1; });
    sum.textContent = hasMatches ? `${confirmed.toLocaleString()} confirmed match${confirmed === 1 ? '' : 'es'}` : 'augmented columns ready';
  }
  renderCitation();
}

// ── Citation & provenance builder (with CRediT) ─────────────────────────────
// Collects schema.org Dataset-style metadata + CRediT-tagged contributors and produces a formatted
// citation, a CITATION.cff file, and schema.org JSON-LD (whose contributor Roles carry the CRediT
// term URIs). Everything lives on project.citation so it persists with the project.
// NB: `license` is deliberately absent — it is chosen via the controlled picker
// (wireLicenseControl below), not typed, but still lives on project.citation.
const CITE_FIELDS = ['title', 'year', 'version', 'publisher', 'url'];
function citationDefaults() {
  const base = (project && project.fileName ? project.fileName : 'My dataset').replace(/\.[^.]+$/, '');
  return {
    title: base, year: String(new Date().getFullYear()), version: '1.0',
    publisher: 'World Historical Gazetteer', url: '', license: '', contributors: [],
  };
}
function citationModel() { return Object.assign(citationDefaults(), (project && project.citation) || {}); }
function citeContributors() { return (project && project.citation && Array.isArray(project.citation.contributors)) ? project.citation.contributors : []; }

// Seed the contributor row from the signed-in user's own profile — name, ORCiD,
// affiliation, and "Data curation", which is what reconciling somebody's table
// actually is. Only when the dataset has NO contributors yet and the row is
// untouched: this fills the form, it does not add anyone. The user still presses
// Add, can edit every field first, and never has a contributor appear in their
// citation without asking. See place#186.
let _citeSeeded = false;
function seedContributorFromProfile() {
  if (_citeSeeded || !project) return;
  const nameEl = el('cite-c-name');
  if (!nameEl || nameEl.value.trim() || citeContributors().length) return;
  const meta = (n) => (document.querySelector(`meta[name="${n}"]`) || {}).content || '';
  const name = meta('user-name').trim();
  if (!name) return;                       // signed out, or no name on the profile
  _citeSeeded = true;
  nameEl.value = name;
  // ORCiD is stored as a URL; the field asks for the identifier.
  const orcid = meta('user-orcid').trim().replace(/^https?:\/\/(www\.)?orcid\.org\//i, '');
  const setIf = (id, val) => { const x = el(id); if (x && !x.value.trim() && val) x.value = val; };
  setIf('cite-c-orcid', orcid);
  setIf('cite-c-affil', meta('user-affiliation').trim());
  const role = el('cite-c-role');
  if (role && !role.value) {
    const opt = [...role.options].find((o) => o.value === 'data-curation');
    if (opt) role.value = opt.value;
  }
}

function renderCitation() {
  if (!project) return;
  const m = citationModel();
  CITE_FIELDS.forEach((f) => { const inp = el('cite-' + f); if (inp && document.activeElement !== inp) inp.value = m[f] || ''; });
  seedContributorFromProfile();
  renderCiteContributors();
  const prev = el('cite-preview'); if (prev) prev.textContent = formatCitation(currentCitation());
}
function currentCitation() {
  const m = citationModel();
  CITE_FIELDS.forEach((f) => { const inp = el('cite-' + f); if (inp) m[f] = inp.value; });
  return m; // contributors are edited via add/remove, not free inputs
}
function saveCitation() {
  if (!project) return;
  project.citation = currentCitation();
  const prev = el('cite-preview'); if (prev) prev.textContent = formatCitation(project.citation);
  persist();
}

// CRediT role slug → human label, read from the server-rendered <select> (single source of truth).
function creditRoleLabel(slug) {
  const sel = el('cite-c-role');
  if (sel) { const o = Array.from(sel.options).find((x) => x.value === slug); if (o) return o.text; }
  return slug;
}
function renderCiteContributors() {
  const ul = el('cite-contrib-list');
  if (!ul) return;
  const list = citeContributors();
  if (!list.length) { ul.innerHTML = '<li class="text-muted">No contributors yet — add people or organisations with their CRediT role.</li>'; return; }
  ul.innerHTML = list.map((c, i) => {
    const orcid = c.orcid ? ` <a href="https://orcid.org/${esc(c.orcid.replace(/^https?:\/\/orcid\.org\//, ''))}" target="_blank" rel="noopener" title="ORCiD"><i class="fab fa-orcid"></i></a>` : '';
    const role = c.role ? esc(creditRoleLabel(c.role)) : '<span class="text-muted">unspecified role</span>';
    const deg = c.degree ? ` <span class="text-muted">(${esc(c.degree)})</span>` : '';
    const corr = c.corresponding ? ' <span class="badge bg-light text-dark border">corresponding</span>' : '';
    const affil = c.affiliation ? ` <span class="text-muted">· ${esc(c.affiliation)}</span>` : '';
    return `<li class="mb-1"><span class="fw-semibold">${esc(c.name)}</span>${orcid} — ${role}${deg}${corr}${affil}` +
      ` <a href="#" class="cite-contrib-del text-danger ms-1" data-i="${i}" title="Remove"><i class="fas fa-times fa-xs"></i></a></li>`;
  }).join('');
  ul.querySelectorAll('.cite-contrib-del').forEach((a) => a.addEventListener('click', (e) => { e.preventDefault(); removeCiteContributor(Number(a.dataset.i)); }));
}
function addCiteContributor() {
  const nameEl = el('cite-c-name');
  const name = (nameEl && nameEl.value || '').trim();
  if (!name) { if (nameEl) nameEl.focus(); return; }
  if (!project.citation) project.citation = citationDefaults();
  if (!Array.isArray(project.citation.contributors)) project.citation.contributors = [];
  project.citation.contributors.push({
    name,
    orcid: ((el('cite-c-orcid') || {}).value || '').trim(),
    affiliation: ((el('cite-c-affil') || {}).value || '').trim(),
    role: (el('cite-c-role') || {}).value || '',
    degree: (el('cite-c-degree') || {}).value || '',
    corresponding: !!((el('cite-c-corr') || {}).checked),
  });
  ['cite-c-name', 'cite-c-orcid', 'cite-c-affil'].forEach((id) => { const x = el(id); if (x) x.value = ''; });
  ['cite-c-role', 'cite-c-degree'].forEach((id) => { const x = el(id); if (x) x.value = ''; });
  const corr = el('cite-c-corr'); if (corr) corr.checked = false;
  saveCitation(); renderCiteContributors();
  if (nameEl) nameEl.focus();
}
function removeCiteContributor(i) {
  const list = citeContributors();
  if (i < 0 || i >= list.length) return;
  list.splice(i, 1);
  saveCitation(); renderCiteContributors();
}

// One entry per unique person/org (a person tagged with several roles appears once), in add-order.
function citeUniqueContributors() {
  const seen = new Set(); const out = [];
  citeContributors().forEach((c) => {
    const key = (c.name || '').toLowerCase() + '|' + (c.orcid || '');
    if (c.name && !seen.has(key)) { seen.add(key); out.push(c); }
  });
  return out;
}
function nameParts(name) {
  const comma = String(name || '').indexOf(',');
  if (comma > 0) return { family: name.slice(0, comma).trim(), given: name.slice(comma + 1).trim() };
  return { name: String(name || '').trim() }; // organisation / single-token entity
}
function orcidUrl(o) { return o ? (/^https?:\/\//.test(o) ? o : 'https://orcid.org/' + o.replace(/^orcid\.org\//, '')) : ''; }

function formatCitation(m) {
  const authors = citeUniqueContributors().map((c) => c.name).join('; ');
  const who = authors || (m.publisher || '');
  const bits = [];
  bits.push(`${who}${who ? ' ' : ''}(${m.year || 'n.d.'}).`);
  bits.push(`${m.title || 'Untitled dataset'}${m.version ? ` (Version ${m.version})` : ''} [Data set].`);
  if (m.publisher && m.publisher !== authors) bits.push(`${m.publisher}.`);
  if (m.url) bits.push(m.url);
  return bits.join(' ').replace(/\s+/g, ' ').trim();
}
function yamlStr(s) { return '"' + String(s == null ? '' : s).replace(/\\/g, '\\\\').replace(/"/g, '\\"') + '"'; }
function buildCff(m) {
  const authors = citeUniqueContributors();
  const lines = [
    'cff-version: 1.2.0',
    'message: "If you use this dataset, please cite it as below."',
    `title: ${yamlStr(m.title)}`,
    'type: dataset',
  ];
  if (authors.length) {
    lines.push('authors:');
    authors.forEach((c) => {
      const p = nameParts(c.name);
      if (p.name) lines.push(`  - name: ${yamlStr(p.name)}`);
      else { lines.push(`  - family-names: ${yamlStr(p.family)}`); lines.push(`    given-names: ${yamlStr(p.given)}`); }
      if (c.orcid) lines.push(`    orcid: ${yamlStr(orcidUrl(c.orcid))}`);
      if (c.affiliation) lines.push(`    affiliation: ${yamlStr(c.affiliation)}`);
    });
  }
  if (m.version) lines.push(`version: ${yamlStr(m.version)}`);
  if (m.year) lines.push(`date-released: ${yamlStr(m.year)}`);
  if (m.url) lines.push(/^10\./.test(m.url.trim()) ? `doi: ${yamlStr(m.url.trim())}` : `url: ${yamlStr(m.url.trim())}`);
  if (m.license) lines.push(`license: ${yamlStr(m.license)}`);
  return lines.join('\n') + '\n';
}
function personNode(c) {
  const p = nameParts(c.name);
  const node = p.name
    ? { '@type': 'Organization', name: p.name }
    : { '@type': 'Person', familyName: p.family, givenName: p.given, name: `${p.given} ${p.family}`.trim() };
  if (c.orcid) node['@id'] = orcidUrl(c.orcid);
  if (c.affiliation) node.affiliation = { '@type': 'Organization', name: c.affiliation };
  return node;
}
function schemaOrgDataset(m) {
  const doc = { '@context': 'https://schema.org/', '@type': 'Dataset', name: m.title || 'Untitled dataset' };
  if (m.year) doc.datePublished = m.year;
  if (m.version) doc.version = m.version;
  if (m.publisher) doc.publisher = { '@type': 'Organization', name: m.publisher };
  if (m.license) doc.license = m.license;
  if (m.url) { if (/^10\./.test(m.url.trim())) doc.identifier = 'https://doi.org/' + m.url.trim(); else doc.url = m.url.trim(); }
  // Group contributions by person; each CRediT role becomes a schema.org Role wrapper carrying the
  // canonical CRediT term URI (https://credit.niso.org/contributor-roles/<slug>/).
  const order = []; const idx = {};
  citeContributors().forEach((c) => {
    const key = (c.name || '').toLowerCase() + '|' + (c.orcid || '');
    if (!(key in idx)) { idx[key] = order.length; order.push({ person: personNode(c), roles: [] }); }
    if (c.role) order[idx[key]].roles.push(c.role);
  });
  const creators = [];
  order.forEach((e) => {
    if (!e.roles.length) { creators.push(e.person); return; }
    e.roles.forEach((r) => creators.push({
      '@type': 'Role', roleName: `https://credit.niso.org/contributor-roles/${r}/`, contributor: e.person,
    }));
  });
  if (creators.length) doc.creator = creators.length === 1 ? creators[0] : creators;
  return doc;
}
function buildSchemaOrg(m) { return JSON.stringify(schemaOrgDataset(m), null, 2); }

// The metadata block embedded at the top of an exported/contributed LPF (`indexing`). WHG's ingest
// (validation.extract_dataset_metadata) reads this key: creator name(s) → Dataset.creator, name →
// title, description → description, url → webpage, citation → Dataset.citation. It's the schema.org
// Dataset with the formatted citation string appended, so the LPF carries its own provenance.
function citationIndexing(m) {
  const doc = schemaOrgDataset(m);
  // Guarantee a resolvable `url` (WHG maps it to the dataset webpage) even when the identifier is a DOI.
  if (!doc.url && doc.identifier) doc.url = doc.identifier;
  const cite = formatCitation(m);
  if (cite) doc.citation = cite;
  return doc;
}

// A CSL-JSON name node. WHG's csl-citation schema REQUIRES `family` on every author (it does not
// accept a literal-only name), so organisations / single-token names are placed in `family` too.
function cslNameNode(c) {
  const p = nameParts(c.name);
  if (p.name) return { family: p.name.replace(/^\[|\]$/g, '').trim() };
  const node = { family: p.family };
  if (p.given) node.given = p.given;
  return node;
}
// A CSL-JSON citation cluster for the LPF's top-level `citation` slot (lpf_v2.0.jsonld $refs
// csl-citation.json). Emitted alongside the schema.org `indexing` block — not consumed on ingest yet,
// but carried so it can be. Every level satisfies the schema's additionalProperties:false.
function buildCslCitation(m) {
  const slug = (m.title || 'dataset').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 40) || 'dataset';
  const item = { type: 'dataset', id: slug, title: m.title || 'Untitled dataset' };
  const authors = citeUniqueContributors().map(cslNameNode).filter((n) => n.family);
  if (authors.length) item.author = authors;
  const yr = parseInt(m.year, 10);
  if (!isNaN(yr)) item.issued = { 'date-parts': [[yr]] };
  if (m.publisher) item.publisher = m.publisher;
  if (m.version) item.version = String(m.version);
  if (m.url) { const u = m.url.trim(); if (u) item[/^10\./.test(u) ? 'DOI' : 'URL'] = u; }
  return {
    schema: 'https://whgazetteer.org/schema/csl-citation.json',
    citationID: 'whg-' + slug,
    citationItems: [{ id: slug, itemData: item }],
  };
}
function citeFlash(msg) { const s = el('cite-status'); if (s) { s.textContent = msg; setTimeout(() => { if (s.textContent === msg) s.textContent = ''; }, 2500); } }
function citeBaseName() {
  const m = currentCitation();
  return (m.title || 'dataset').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 40) || 'dataset';
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
// Each contributable-requirement → a plain-English "what's missing" + the specific fix, shown only
// when that requirement actually fails. Raw schema messages stay hidden behind a disclosure.
const VALIDATE_HELP = {
  types:    ['no place type', 'assign one per row in the table (Step&nbsp;2), or set a dataset-wide type in <strong>Scope&nbsp;→&nbsp;What</strong>'],
  when:     ['no date or period', 'add a date column in Step&nbsp;2, or set a year range or historical period in <strong>Scope&nbsp;→&nbsp;When</strong>'],
  geometry: ['no location', 'add coordinates in Step&nbsp;2, or draw geometry on the map'],
  title:    ['no place name', 'map your place-name column in Step&nbsp;2'],
  names:    ['no name variant', ''],
};
function renderValidation(v) {
  const body = el('recon-validate-body'); if (!body || !v) return;
  const total = v.total;
  const plural = (n) => (n === 1 ? 'place' : 'places');
  // Friendly, actionable bullets — only for requirements that actually fail.
  const bullets = Object.keys(VALIDATE_HELP)
    .filter((k) => v.miss[k] > 0)
    .map((k) => { const [what, fix] = VALIDATE_HELP[k];
      return `<li><strong>${v.miss[k].toLocaleString()}</strong> of ${total.toLocaleString()} ${plural(v.miss[k])} have ${what}${fix ? ` — ${fix}` : ''}.</li>`; });
  // Schema errors the friendly checks don't already explain (bad @id, malformed values, …) → one bullet.
  const covered = /place name|name variant|location|date\/period|place type|fclasses|types|missing (in|earliest|latest)|anyOf|oneOf/i;
  const other = (v.schemaSummary || []).filter((g) => !covered.test(g.msg));
  if (other.length) {
    const n = other.reduce((a, g) => a + (g.count || 1), 0);
    bullets.push(`<li><strong>${n.toLocaleString()}</strong> ${n === 1 ? 'record has' : 'records have'} other format issues — see the technical details below.</li>`);
  }

  if (v.schemaOk === true && !bullets.length) {
    body.innerHTML = `<div class="text-success"><i class="fas fa-circle-check me-1"></i><strong>Ready to contribute.</strong> All ${total.toLocaleString()} places pass WHG's Linked Places validation.</div>`;
    return;
  }
  if (v.schemaOk == null && !bullets.length) {
    body.innerHTML = `<div class="small text-muted"><i class="fas fa-circle-info me-1"></i>The full schema check couldn't run in your browser, but the basic requirements (name, location, place type, date) all look present. Try <strong>Re-check</strong>, or just export — WHG validates again on upload.</div>`;
    return;
  }
  // Full raw list, hidden behind a disclosure for the technically-minded.
  const techLines = (v.schemaSummary || []).map((g) => `<li>${esc(g.msg)}${g.count > 1 ? ` <span class="text-muted">(${g.count}×)</span>` : ''}</li>`);
  const tech = techLines.length
    ? `<details class="recon-validate-tech small mt-2"><summary class="text-muted">Technical validation details (${techLines.length})</summary><ul class="mb-0 mt-1">${techLines.join('')}</ul></details>`
    : '';
  const note = v.schemaOk == null ? ' <span class="text-muted fw-normal">(showing the basic requirements — full schema check unavailable)</span>' : '';
  body.innerHTML =
    `<div class="text-danger mb-1"><i class="fas fa-triangle-exclamation me-1"></i><strong>Not ready to contribute.</strong>${note}</div>` +
    (bullets.length ? `<ul class="recon-validate-issues small mb-0">${bullets.join('')}</ul>` : '') +
    tech;
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
      || (autoConfirmed(key) ? { label: matches[key].top.name, score: matches[key].top.score } : null);
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
  return !!(project && project.phonetic === true); // default off
}
// Languages offered in the override dropdown (value = Symphonym lang code; 'und' = undetermined).
// The Symphonym model is conditioned on ~1,900 languages (static/webpack/symphonym/lang_vocab.json);
// this list is only what we surface. Keep the Celtic languages in it: a Welsh/Irish/Gaelic toponym
// list embedded as English gets materially worse phonetic matches, and until these were offered the
// only Latin-script choice was English (detectLang() defaults there). Codes verified against the
// vocab; anything it doesn't know falls back to <UNK> rather than failing.
const RECON_LANGS = [
  ['und', 'Undetermined'], ['en', 'English'],
  ['cy', 'Welsh'], ['ga', 'Irish'], ['gd', 'Scottish Gaelic'], ['gv', 'Manx'], ['kw', 'Cornish'], ['br', 'Breton'],
  ['fr', 'French'], ['de', 'German'], ['es', 'Spanish'], ['it', 'Italian'], ['pt', 'Portuguese'],
  ['nl', 'Dutch'], ['ca', 'Catalan'], ['eu', 'Basque'], ['la', 'Latin'],
  ['da', 'Danish'], ['no', 'Norwegian'], ['sv', 'Swedish'], ['is', 'Icelandic'], ['fi', 'Finnish'], ['et', 'Estonian'],
  ['pl', 'Polish'], ['cs', 'Czech'], ['sk', 'Slovak'], ['sl', 'Slovene'], ['hu', 'Hungarian'], ['ro', 'Romanian'],
  ['hr', 'Croatian'], ['sr', 'Serbian'], ['bg', 'Bulgarian'], ['uk', 'Ukrainian'], ['ru', 'Russian'],
  ['lt', 'Lithuanian'], ['lv', 'Latvian'],
  ['el', 'Greek'], ['tr', 'Turkish'], ['ar', 'Arabic'], ['fa', 'Persian'], ['he', 'Hebrew'], ['yi', 'Yiddish'],
  ['hi', 'Hindi'], ['ur', 'Urdu'], ['th', 'Thai'], ['vi', 'Vietnamese'], ['id', 'Indonesian'], ['ms', 'Malay'],
  ['sw', 'Swahili'], ['zh', 'Chinese'], ['ja', 'Japanese'], ['ko', 'Korean'],
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
      _entityScan = scanEntities(project.rows); // still offer the encoding fix on a resumed project
      renderAll();
      renderEncodingNotice();
      showResume();
      applyReadOnlyMode();
      setCollabBadge(collabState());
      maybeStartRealtime();
      console.log(`[recon] resumed saved project: ${project.total} rows`);
    }
  } catch (err) { console.error('[recon] could not load saved project', err); }
}

// ── Collaboration (place#112, Phase 0/1) ─────────────────────────────────────
// A server-backed project keeps client-only sync metadata (serverId/serverVersion/role/team/share)
// ON the `project` object (so it persists to IndexedDB) but STRIPPED from the snapshot pushed to the
// server — the snapshot is the shared document, the metadata is per-client. Editing a server project
// debounces a background push (optimistic-lock: PUT with base_version → fast-forward / auto-merge /
// conflict). Solo local projects are untouched (no serverId → no network).
const SYNC_KEYS = ['serverId', 'serverVersion', 'role', 'teamId', 'teamTitle', 'teamPersonal', 'sharedToken', 'sharedUrl'];
let _pushTimer = null, _pushing = false, _pushQueued = false, _retryTimer = null;
let _pendingConflict = null; // { mine, merged, conflicts, version }

// Real-time (Phase 2): lazy Yjs/Hocuspocus chunk, loaded only for team (non-personal) server
// projects. When connected, the whole project is a CRDT — the Phase-1 REST push is suppressed and
// the doc is mirrored bidirectionally (see recon-collab-rt.js).
let RT = null;
let _applyingRemote = false;   // guard: don't mirror while adopting a peer's change
let _mirrorTimer = null;       // debounce local→Yjs mirroring
let _rtPresence = [];          // other members' awareness states
const loadRT = async () => (RT || (RT = await import(/* webpackChunkName: "recon-collab-rt" */ './recon-collab-rt.js')));
function rtActive() { return !!(RT && RT.isConnected()); }

function cleanSnapshot() {
  const out = {};
  for (const k of Object.keys(project)) if (!SYNC_KEYS.includes(k)) out[k] = project[k];
  return out;
}
function clone(x) { return JSON.parse(JSON.stringify(x)); }
function isServerProject() { return !!(project && project.serverId); }
function isReadOnly() { return !!(project && project.role === 'viewer'); }
function canPush() { return isServerProject() && !isReadOnly(); }

// Replace the working document with `snap`, preserving this client's sync metadata + the IndexedDB
// key, then re-run the same post-load steps as a resume so the whole UI reflects the new state.
function applySnapshot(snap) {
  const meta = {};
  for (const k of SYNC_KEYS) if (k in project) meta[k] = project[k];
  project = Object.assign({}, snap, meta);
  project.id = CURRENT;
  reviewMeta = []; reviewPos = 0;
  migrateLegacyChain();
  normalizeChain();
  renderAll();
  showResume();
  applyReadOnlyMode();
}

function schedulePush() {
  // While a real-time session is live, the CRDT owns sync — mirror the whole project into the Yjs
  // doc instead of doing the Phase-1 REST push.
  if (rtActive()) { scheduleMirror(); return; }
  if (!canPush()) return;
  clearTimeout(_pushTimer);
  _pushTimer = setTimeout(doPush, 1200);
  setCollabBadge('syncing');
}

// ── Real-time orchestration (Phase 2) ────────────────────────────────────────
// Connect a team (non-personal) server project to Hocuspocus and mirror the whole project as a CRDT.
async function maybeStartRealtime() {
  if (!isServerProject() || project.teamPersonal) return;
  let tok;
  try { tok = await Sync.collabToken(project.serverId); }
  catch (_) { return; } // offline → stay on REST
  if (!tok || tok.status !== 200 || !tok.data || !tok.data.token) return; // 501/403 → REST fallback
  try {
    const mod = await loadRT();
    const wsUrl = (window.location.protocol === 'https:' ? 'wss://' : 'ws://') + window.location.host + '/collab';
    _rtMe = rtIdentity(tok.data.token); // my display identity, for chat attribution (place#154)
    resetChat();
    mod.connect({
      serverId: project.serverId, token: tok.data.token, wsUrl,
      user: _rtMe,
      onStatus: (s) => { setCollabBadge(s === 'connected' ? 'live' : (s === 'unauthorized' ? 'offline' : 'syncing'));
                         if (s === 'connected') { _chatConnectedAt = Date.now(); showChatToggle(true); } },
      onSynced: () => rtOnSynced(mod),
      onRemote: applyRemoteProject,
      onPresence: renderPresence,
    });
  } catch (err) { console.warn('[recon] realtime connect failed — staying on REST', err); }
}

// On first sync we ADOPT the shared copy — we never seed from the client. The Hocuspocus server
// seeds the doc from the project's stored snapshot (single-threaded, once), so a populated doc always
// arrives. If we seeded from the client instead, two tabs connecting to a fresh doc would each insert
// the whole dataset and the CRDT would merge both → duplicated rows/columns.
function rtOnSynced(mod) {
  const remote = mod.readProject();
  const remoteHasData = (remote.rows && remote.rows.length) || (remote.columns && remote.columns.length);
  if (remoteHasData) adoptRemote(remote);
  // else: empty doc — keep our local project and wait for the server's snapshot seed to arrive via the
  // normal remote-change path (which then adopts it). Never mirror-seed here.
  setCollabBadge('live');
}

function scheduleMirror() {
  if (_applyingRemote || !rtActive()) return;
  clearTimeout(_mirrorTimer);
  _mirrorTimer = setTimeout(() => { if (rtActive() && !_applyingRemote) RT.mirror(cleanSnapshot()); }, 300);
}

// Full adopt (initial connect): replace the working project with the shared copy + full render.
function adoptRemote(remote) {
  if (!project || !remote) return;
  _applyingRemote = true;
  const meta = {};
  for (const k of SYNC_KEYS) if (k in project) meta[k] = project[k];
  project = Object.assign({}, remote, meta);
  project.id = CURRENT;
  normalizeChain();
  putProject(project);
  renderAll();
  applyReadOnlyMode();
  setCollabBadge('live');
  _applyingRemote = false;
}

// Granular apply (ongoing peer edits): overwrite ONLY the sections a teammate changed and repaint
// just those panes — preserving the local user's scroll position, filters, open pane and review
// spot. Avoids the disruptive full renderAll (which resets scroll/filters/pane) on every keystroke.
function applyRemoteProject(remote, sections) {
  if (!project || !remote) return;
  _applyingRemote = true;
  const changed = new Set(sections && sections.length
    ? sections : ['rows', 'columns', 'decisions', 'matches', 'geom', 'rowTypes', 'meta']);
  if (changed.has('rows')) { project.rows = remote.rows; project.total = remote.rows.length; }
  if (changed.has('columns')) project.columns = remote.columns;
  ['decisions', 'matches', 'geom', 'rowTypes'].forEach((k) => { if (changed.has(k)) project[k] = remote[k]; });
  if (changed.has('meta')) {
    for (const k of Object.keys(remote)) {
      if (['rows', 'columns', 'decisions', 'matches', 'geom', 'rowTypes', 'total', 'id'].includes(k)) continue;
      if (SYNC_KEYS.includes(k)) continue;
      project[k] = remote[k];
    }
  }
  project.id = CURRENT;
  putProject(project);
  repaintForSections(changed);
  _applyingRemote = false;
}

function repaintForSections(changed) {
  const editing = !!_previewEditing; // don't tear down a cell the local user is typing in
  if (changed.has('columns')) {
    normalizeChain();
    renderMapping(); renderColSwitcher(); refreshReconSection();
    renderCoords(); renderDates();
    if (!editing) renderPreview();
  } else if (changed.has('rows')) {
    if (!editing) renderPreview();
    renderCoords(); renderDates();
  } else if (changed.has('rowTypes') && !editing) {
    renderPreview();
  }
  if (changed.has('decisions') || changed.has('matches') || changed.has('geom')) {
    const built = buildUniqueQueries();
    if (built) renderResults(built); else refreshReview(); // refreshReview keeps the review position
    // A teammate's reconcile/decisions change the stage state and the remaining-to-reconcile count, so
    // refresh the Reconcile/Continue button and the column pills — without this the receiving client
    // showed a stale "N remaining" (e.g. 223 vs 23 across members). Safe: no review-position reset. #150
    renderColSwitcher();
    updateReconButton();
  }
  if (changed.has('meta')) { refreshReconSection(); renderCoords(); renderDates(); refreshExport(); }
  updatePaneSummaries();
  setCollabBadge('live');
}

function stopRealtime() {
  if (RT) { try { RT.disconnect(); } catch (_) { /* ignore */ } }
  _applyingRemote = false;
  _rtPresence = [];
  resetChat(); // team chat is per-live-session; clear it and hide the toggle
  renderPresence([]);
}

// Decode the (already-trusted) collab JWT to label this member; colour from the user id.
function rtIdentity(token) {
  try {
    const p = JSON.parse(atob(token.split('.')[1].replace(/-/g, '+').replace(/_/g, '/')));
    return { name: p.name || 'Someone', color: colorFor(String(p.sub || p.name || '')) };
  } catch (_) { return { name: 'Someone', color: colorFor('anon') }; }
}
function colorFor(seed) {
  let h = 0;
  for (let i = 0; i < seed.length; i++) h = (h * 31 + seed.charCodeAt(i)) >>> 0;
  return `hsl(${h % 360} 70% 45%)`;
}

// Publish where I'm working (a cell or a review row) so teammates see my cursor.
function rtSetCursor(cursor) { if (rtActive()) RT.setCursor(cursor); }
function rtSetActivity(activity) { if (rtActive()) RT.setActivity(activity); }
let _peerReconciling = null; // {name, column} when a teammate is mid-reconcile — soft-locks our button

// ── Presence UI (awareness) ──────────────────────────────────────────────────
function initials(name) {
  return String(name || 'Someone').trim().split(/\s+/).map((w) => w[0] || '').slice(0, 2).join('').toUpperCase();
}
function renderPresence(states) {
  _rtPresence = states || [];
  // Advisory reconcile lock (place#112): is any teammate mid-reconcile? If so, soft-block our own run.
  const busy = _rtPresence.find((s) => s.activity && s.activity.type === 'reconciling');
  const wasLocked = !!_peerReconciling;
  _peerReconciling = busy ? { name: (busy.user && busy.user.name) || 'A teammate', column: busy.activity.column } : null;
  if (wasLocked !== !!_peerReconciling || _peerReconciling) updateReconButton(); // reflect (un)lock
  const box = el('recon-presence');
  if (box) {
    box.innerHTML = _rtPresence.map((s) => {
      const name = (s.user && s.user.name) || 'Someone';
      const color = (s.user && s.user.color) || '#888';
      return `<span class="recon-presence-chip" style="background:${color}" title="${esc(name)} is editing this project">${esc(initials(name))}</span>`;
    }).join('');
    box.classList.toggle('d-none', !_rtPresence.length);
  }
  receiveChat(_rtPresence); // pick up any new chat messages / typing state riding on awareness (#154)
  paintPresenceCursors();
}

// ── Ephemeral team chat (place#154) ──────────────────────────────────────────
// Messages are carried on the awareness channel (see recon-collab-rt sendMessage/setTyping): live-only,
// never persisted, seen only by teammates connected right now. Each peer's `msg` field holds their most
// recent message; we render each new id once, ignoring anything older than our own connect time so a
// late joiner doesn't replay the last thing said.
let _rtMe = null;              // my {name, color} for attribution
let _chatSeen = new Set();     // message ids already shown
let _chatSeq = 0;              // local message counter
const _chatTag = Math.random().toString(36).slice(2, 8); // per-tab id prefix
let _chatOpen = false;
let _chatUnread = 0;
let _chatConnectedAt = 0;      // ms; ignore messages older than this
function resetChat() {
  _chatSeen = new Set(); _chatUnread = 0; _chatOpen = false; _chatConnectedAt = 0;
  const list = el('recon-chat-messages'); if (list) list.innerHTML = '';
  const t = el('recon-chat-typing'); if (t) t.textContent = '';
  const p = el('recon-chat-panel'); if (p) { p.classList.remove('open'); p.hidden = true; }
  showChatToggle(false); updateChatPip();
}
function showChatToggle(on) { const b = el('recon-chat-toggle'); if (b) b.classList.toggle('d-none', !on); }
function updateChatPip() {
  const pip = el('recon-chat-pip'); if (!pip) return;
  pip.textContent = _chatUnread > 9 ? '9+' : String(_chatUnread);
  pip.classList.toggle('d-none', !_chatUnread || _chatOpen);
}
// The teammate-facing bit of my identity + the record I'm on, sent with each message so a recipient can
// jump to my view. Kept tiny — a column + the review row is the meaningful "where I am".
// The id of the accordion tab (pane) currently open — derived from the DOM, so it covers every pane
// (import / mapping / reconcile / review / map / export) without a whitelist to keep in step. Exactly
// one pane is un-collapsed once a dataset is loaded (openPane collapses the rest). Null if none.
function openPaneId() {
  const open = document.querySelector('.recon-pane:not(.recon-collapsed)');
  return open ? open.id : null;
}
function captureViewContext() {
  const ctx = { pane: openPaneId() }; // which accordion tab the sender had open — opening it, not just
  const col = activeReconCol();       // scrolling, is what makes the jump land in the right place
  if (col >= 0) { ctx.col = col; ctx.colName = project.columns[col] && project.columns[col].name; }
  const meta = reviewMeta[reviewPos];
  if (meta) { ctx.reviewKey = meta.key; ctx.rowName = meta.name; }
  return ctx;
}
// Jump to the perspective captured with a message. First OPEN the accordion tab they had open (scrolling
// alone doesn't reveal a collapsed pane), then focus their column + review record. Perspectives can ROT —
// the tab may be hidden for us, the column re-mapped, or the record edited away — so we get as close as we
// can and say so rather than silently doing nothing.
function applyViewContext(ctx) {
  if (!ctx || !project) return;
  let rotted = false;
  const paneEl = ctx.pane ? el(ctx.pane) : null;
  if (paneEl && paneEl.classList.contains('recon-pane') && !paneEl.classList.contains('d-none')) {
    openPane(ctx.pane); // open that accordion tab (collapsing the others)
  } else if (ctx.pane) {
    rotted = true;      // that tab no longer exists / isn't available in our current state
  }
  const chain = reconChain();
  if (ctx.col != null) {
    const pos = chain.indexOf(ctx.col);
    if (pos >= 0) { reconActiveIdx = pos; renderColSwitcher(); } else rotted = true; // column no longer in the chain
  }
  refreshReview();
  if (ctx.reviewKey) {
    const i = reviewMeta.findIndex((r) => r.key === ctx.reviewKey);
    if (i >= 0) { reviewPos = i; renderReviewCard(); updateReviewProgress(); } else rotted = true; // record gone from the queue
  }
  const target = paneEl || el('recon-recon');
  if (target && target.scrollIntoView) target.scrollIntoView({ block: 'center', behavior: 'smooth' });
  flashSaved(rotted ? 'Jumped as close as possible — that exact spot has changed since.' : 'Synced to that view.');
}
function chatTime(ts) { try { return new Date(ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }); } catch (_) { return ''; } }
function appendChatMessage(msg, mine) {
  const list = el('recon-chat-messages'); if (!list) return;
  const who = mine ? 'You' : ((msg.from && msg.from.name) || 'Teammate');
  const color = mine ? '#1565c0' : ((msg.from && msg.from.color) || '#888');
  const ctx = msg.context;
  const hint = ctx && (ctx.rowName || ctx.colName)
    ? `<div class="recon-chat-ctx">on ${ctx.rowName ? '“' + esc(truncate(ctx.rowName, 22)) + '”' : ''}${ctx.colName ? (ctx.rowName ? ' · ' : '') + esc(truncate(ctx.colName, 18)) : ''}</div>` : '';
  // Every message carries a jump-to-the-sender's-perspective button (captured at send time). The
  // perspective may have rotted since — a BS tooltip says so.
  const tip = `Jump to where ${mine ? 'you were' : 'they were'} working when this was sent. `
    + 'Views can move on (columns re-mapped, records edited, the queue advanced), so it may no longer match.';
  const persp = ctx ? `<button type="button" class="btn btn-sm recon-chat-goto" data-bs-toggle="tooltip"`
    + ` data-bs-placement="top" title="${esc(tip)}"><i class="fas fa-location-crosshairs me-1"></i>`
    + `${mine ? 'My view then' : 'Their view'}</button>` : '';
  const div = document.createElement('div');
  div.className = 'recon-chat-msg' + (mine ? ' recon-chat-msg--mine' : '');
  div.innerHTML = `<div class="recon-chat-meta"><span class="recon-chat-name" style="color:${esc(color)}">${esc(who)}</span>`
    + `<span class="recon-chat-time">${chatTime(msg.ts)}</span></div><div class="recon-chat-text">${esc(msg.text)}</div>${hint}${persp}`;
  if (persp) {
    const b = div.querySelector('.recon-chat-goto');
    b.addEventListener('click', () => applyViewContext(ctx));
    if (window.bootstrap && window.bootstrap.Tooltip) { try { new window.bootstrap.Tooltip(b); } catch (_) { /* ignore */ } }
  }
  list.appendChild(div);
  list.scrollTop = list.scrollHeight;
}
function receiveChat(states) {
  let typers = [];
  (states || []).forEach((s) => {
    const m = s.msg;
    if (m && m.id && !_chatSeen.has(m.id) && (m.ts || 0) >= _chatConnectedAt) {
      _chatSeen.add(m.id);
      appendChatMessage(m, false);
      if (!_chatOpen) { _chatUnread += 1; updateChatPip(); }
    }
    if (s.typing && (Date.now() - s.typing) < 4000) typers.push((s.user && s.user.name) || 'Someone');
  });
  const t = el('recon-chat-typing');
  if (t) t.textContent = typers.length ? (typers.length === 1 ? `${typers[0]} is typing…` : `${typers.length} people are typing…`) : '';
}
function sendChat() {
  const inp = el('recon-chat-input'); if (!inp) return;
  const text = (inp.value || '').trim(); if (!text || !rtActive()) return;
  const msg = { id: `${_chatTag}-${++_chatSeq}`, text, ts: Date.now(), from: _rtMe || { name: 'You' }, context: captureViewContext() };
  RT.sendMessage(msg);
  appendChatMessage(msg, true);
  inp.value = ''; rtSetTyping(false);
}
function rtSetTyping(on) { if (rtActive()) RT.setTyping(on); }
function toggleChat(open) {
  const p = el('recon-chat-panel'); if (!p) return;
  _chatOpen = (open != null) ? open : !_chatOpen;
  p.hidden = false;
  requestAnimationFrame(() => p.classList.toggle('open', _chatOpen));
  if (_chatOpen) { _chatUnread = 0; updateChatPip(); const inp = el('recon-chat-input'); if (inp) inp.focus(); }
  else updateChatPip();
}
let _typingTimer = null;
function wireChat() {
  const on = (id, ev, fn) => { const e = el(id); if (e) e.addEventListener(ev, fn); };
  on('recon-chat-toggle', 'click', () => toggleChat());
  on('recon-chat-close', 'click', () => toggleChat(false));
  on('recon-chat-send', 'click', sendChat);
  on('recon-chat-input', 'keydown', (e) => { if (e.key === 'Enter') { e.preventDefault(); sendChat(); } });
  on('recon-chat-input', 'input', () => {
    rtSetTyping(true);
    clearTimeout(_typingTimer);
    _typingTimer = setTimeout(() => rtSetTyping(false), 1800);
  });
}
// Outline the cells teammates are editing (only those currently visible in the virtualised table).
function paintPresenceCursors() {
  const body = el('recon-preview-body');
  if (!body) return;
  body.querySelectorAll('.recon-cursor').forEach((td) => {
    td.classList.remove('recon-cursor');
    td.style.removeProperty('--cursor-color');
    td.removeAttribute('data-cursor-name');
  });
  for (const s of _rtPresence) {
    const c = s.cursor;
    if (!c || c.row == null || c.col == null) continue;
    const td = body.querySelector(`tr[data-ri="${c.row}"] td[data-ci="${c.col}"]`);
    if (td) {
      td.classList.add('recon-cursor');
      td.style.setProperty('--cursor-color', (s.user && s.user.color) || '#888');
      td.setAttribute('data-cursor-name', (s.user && s.user.name) || 'Someone');
    }
  }
}

async function doPush() {
  if (!canPush()) return;
  if (_pushing) { _pushQueued = true; return; }
  _pushing = true; _pushQueued = false;
  const mine = cleanSnapshot();
  try {
    const res = await Sync.pushSnapshot(project.serverId, mine, project.serverVersion || 0);
    if (res.status === 200 && res.data) {
      project.serverVersion = res.data.version;
      if (res.data.snapshot) applySnapshot(res.data.snapshot); // adopted an auto-merge
      await putProject(project);
      setCollabBadge('synced');
    } else if (res.status === 409 && res.data && res.data.status === 'conflict') {
      openConflictModal({ mine, merged: res.data.merged, conflicts: res.data.conflicts, version: res.data.version });
    } else if (res.status === 409 && res.data && res.data.status === 'stale') {
      applySnapshot(res.data.snapshot);
      project.serverVersion = res.data.version;
      await putProject(project);
      setCollabBadge('synced');
      flashSaved('Reloaded the latest version from the server');
    } else if (res.status === 403) {
      project.role = 'viewer'; applyReadOnlyMode(); setCollabBadge('viewer');
      flashSaved('⚠ your access to this project changed — now view-only');
    } else {
      setCollabBadge('offline');
    }
  } catch (err) {
    console.warn('[recon] push failed (offline?)', err);
    setCollabBadge('offline');
    clearTimeout(_retryTimer);
    _retryTimer = setTimeout(() => { if (canPush()) doPush(); }, 8000);
  } finally {
    _pushing = false;
    if (_pushQueued && canPush()) schedulePush();
  }
}

// Conflict UI — same key edited differently on both sides. Start from the server's auto-merge
// (which already keeps this client's non-conflicting edits) and let the user pick mine/theirs per
// conflict, then re-push at the server's current version (a clean fast-forward).
function fieldLabel(k) {
  return ({ columns: 'Column setup', rows: 'Table data', scope: 'Scope filter', title: 'Project title',
            submissionTypes: 'Place types', coordFormat: 'Coordinate format',
            decisions: 'Match decision', matches: 'Candidates', geom: 'Geometry', rowTypes: 'Row place-type' })[k] || k;
}
function conflictValueHTML(v) {
  if (v == null) return '<em class="text-muted">(none)</em>';
  const s = typeof v === 'object' ? JSON.stringify(v) : String(v);
  return `<code>${esc(s.length > 120 ? s.slice(0, 119) + '…' : s)}</code>`;
}
function openConflictModal(state) {
  _pendingConflict = state;
  track('MyD: conflict', { count: state.conflicts.length });
  setCollabBadge('conflict');
  const body = el('recon-conflict-body');
  if (!body) { // no modal in DOM → safe default: keep server version
    applySnapshot(state.merged); project.serverVersion = state.version; putProject(project); return;
  }
  body.innerHTML = state.conflicts.map((c, i) => {
    const isField = c.kind === 'field';
    const label = isField ? fieldLabel(c.key) : `${fieldLabel(c.kind)} · ${esc(c.key)}`;
    const mineVal = isField ? state.mine[c.key] : (c.mine);
    const theirsVal = isField ? state.merged[c.key] : (c.theirs);
    return `<div class="recon-conflict border rounded p-2 mb-2" data-i="${i}">
        <div class="small fw-bold mb-1">${esc(label)}</div>
        <label class="d-block small mb-1"><input type="radio" name="cf-${i}" value="theirs" checked>
          Keep theirs (server): ${conflictValueHTML(theirsVal)}</label>
        <label class="d-block small"><input type="radio" name="cf-${i}" value="mine">
          Keep mine: ${conflictValueHTML(mineVal)}</label>
      </div>`;
  }).join('');
  showModal('recon-conflict-modal');
}
async function resolveConflicts() {
  const state = _pendingConflict;
  if (!state) return;
  const resolved = clone(state.merged);
  state.conflicts.forEach((c, i) => {
    const sel = document.querySelector(`input[name=cf-${i}]:checked`);
    if (!sel || sel.value !== 'mine') return; // default keeps theirs (already in resolved)
    if (c.kind === 'field') {
      resolved[c.key] = state.mine[c.key];
    } else {
      resolved[c.kind] = resolved[c.kind] || {};
      const src = state.mine[c.kind] || {};
      if (c.key in src) resolved[c.kind][c.key] = src[c.key];
      else delete resolved[c.kind][c.key];
    }
  });
  hideModal('recon-conflict-modal');
  applySnapshot(resolved);
  project.serverVersion = state.version; // base for the re-push == server's current version
  _pendingConflict = null;
  await putProject(project);
  doPush(); // clean fast-forward now
}

// ── read-only (viewer) mode ────────────────────────────────────────────────
function applyReadOnlyMode() {
  const ro = isReadOnly();
  document.body.classList.toggle('recon-viewer', ro);
  ['recon-run', 'recon-rerun', 'recon-contribute-btn', 'recon-undo', 'recon-redo',
   'recon-preview-edit', 'recon-clear'].forEach((id) => { const b = el(id); if (b) b.disabled = ro; });
  const badge = el('recon-viewer-badge');
  if (badge) badge.classList.toggle('d-none', !ro);
}

// ── collaborate badge on the button ──────────────────────────────────────────
function setCollabBadge(state) {
  const b = el('recon-collab-badge');
  if (b) {
    // Transient badge on the Collaborate button. 'local' is no longer blank — an empty badge is exactly
    // what let a device-only project pass for a shared one.
    const map = { local: ['· not shared', 'text-warning'], synced: ['✓ synced', 'text-success'], syncing: ['⋯ syncing', 'text-muted'],
                  offline: ['⚠ offline', 'text-warning'], conflict: ['⚠ conflict', 'text-danger'],
                  viewer: ['view only', 'text-muted'], live: ['● live', 'text-success'] };
    const [txt, cls] = map[state] || map.local;
    b.textContent = txt;
    b.className = 'recon-collab-badge small ms-1 ' + cls;
  }
  renderCollabStatus(); // keep the always-visible status in step with every badge change
}
// The persistent, unmistakable sharing state (place#112). Distinguishes the cases the old blank badge
// conflated: on-device-only, private workbench, a live team project (named), a team project whose
// realtime is down (still saving via REST), and read-only.
function collabState() {
  if (!isServerProject()) return 'local';
  if (isReadOnly()) return 'viewer';
  if (project && project.teamPersonal) return 'personal';
  return rtActive() ? 'live' : 'team-offline';
}
const COLLAB_STATUS = {
  local:          { icon: 'fa-laptop', cls: 'text-warning fw-semibold', text: 'On this device only — not shared' },
  personal:       { icon: 'fa-lock', cls: 'text-muted', text: 'Private workbench (only you)' },
  live:           { icon: 'fa-circle text-success', cls: 'text-success fw-semibold', text: 'Live' },
  'team-offline': { icon: 'fa-triangle-exclamation', cls: 'text-warning', text: 'Team project — reconnecting, saving via backup' },
  viewer:         { icon: 'fa-eye', cls: 'text-muted', text: 'View only' },
};
function renderCollabStatus() {
  const box = el('recon-collab-status'); if (!box) return;
  if (!project) { box.innerHTML = ''; return; }
  const st = collabState();
  const d = COLLAB_STATUS[st] || COLLAB_STATUS.local;
  const named = (st === 'live' || st === 'team-offline') && project.teamTitle ? ` · ${esc(truncateText(project.teamTitle, 24))}` : '';
  const tip = st === 'local' ? 'Save to a team (Collaborate) to work on this with others — right now nothing is shared.'
    : st === 'personal' ? 'Saved to your own private workbench — not visible to any team.'
    : st === 'live' ? 'Connected to your team in real time; edits sync to everyone.'
    : st === 'team-offline' ? 'Saved to a team but the live connection is down — edits are still saved and will sync.'
    : 'You have view-only access to this project.';
  box.className = 'recon-collab-status small ' + d.cls;
  box.title = tip;
  box.innerHTML = `<i class="fas ${d.icon} me-1"></i>${d.text}${named}`;
}

// ── modal helpers ──────────────────────────────────────────────────────────
function showModal(id) { const m = el(id); if (m && window.bootstrap && window.bootstrap.Modal) window.bootstrap.Modal.getOrCreateInstance(m).show(); }
function hideModal(id) { const m = el(id); if (m && window.bootstrap && window.bootstrap.Modal) { const inst = window.bootstrap.Modal.getInstance(m); if (inst) inst.hide(); } }

// ── the Collaborate hub modal (body rendered by JS to keep the template lean) ──
async function openCollabModal() {
  if (!project) return;
  showModal('recon-collab-modal');
  renderCollabBody('<div class="text-muted small">Loading…</div>');
  await renderCollab();
}
function renderCollabBody(html) { const b = el('recon-collab-body'); if (b) b.innerHTML = html; }

async function renderCollab() {
  const parts = [];
  // Status
  if (isServerProject()) {
    parts.push(`<div class="alert alert-light border small mb-3">
      <i class="fas fa-cloud me-1"></i> Saved to <strong>${esc(project.teamTitle || 'a team')}</strong>
      · version ${project.serverVersion || 1} · your role: <strong>${esc(project.role || 'owner')}</strong></div>`);
  } else {
    parts.push(`<div class="alert alert-light border small mb-3">
      <i class="fas fa-laptop me-1"></i> This project is on <strong>your device only</strong>.
      Save it to enable sharing and team editing.</div>`);
  }
  // Share (Phase 0)
  parts.push('<h6 class="small text-uppercase text-muted">Share a read-only link</h6>');
  if (project.sharedUrl) {
    parts.push(`<div class="input-group input-group-sm mb-2">
        <input type="text" class="form-control" id="recon-share-url" readonly value="${esc(project.sharedUrl)}">
        <button class="btn btn-outline-secondary" id="recon-share-copy" type="button">Copy</button>
      </div>
      <button class="btn btn-sm btn-link text-danger p-0 mb-3" id="recon-share-stop">Stop sharing</button>`);
  } else {
    parts.push(`<p class="small text-muted mb-2">Anyone with the link can import a read-only copy of the current data.</p>
      <button class="btn btn-sm btn-outline-primary mb-3" id="recon-share-create">
        <i class="fas fa-link me-1"></i>Create read-only link</button>`);
  }
  // Teams
  parts.push('<hr><h6 class="small text-uppercase text-muted">Collaborate with a team</h6>');
  if (!isServerProject()) {
    const tr = await Sync.listTeams();
    const teams = (tr.data && tr.data.teams) || [];
    const opts = teams.map((t) => `<option value="${t.id}">${esc(t.title)} (${esc(t.role)})</option>`).join('');
    parts.push(`<div class="mb-2">
        <label class="small d-block mb-1">Save this project to a team:</label>
        <div class="input-group input-group-sm mb-2">
          <select class="form-select" id="recon-team-select">
            <option value="">My workbench (private)</option>${opts}
          </select>
          <button class="btn btn-outline-primary" id="recon-save-team" type="button">Save</button>
        </div>
        <label class="small d-block mb-1 mt-2">…or create a new team:</label>
        <div class="input-group input-group-sm">
          <input type="text" class="form-control" id="recon-new-team" placeholder="New team name" aria-label="New team name">
          <button class="btn btn-outline-secondary" id="recon-create-team" type="button">Create team</button>
        </div>
      </div>`);
  } else if (project.teamPersonal) {
    parts.push(`<p class="small text-muted">This project is saved to your <strong>private workbench</strong>.
      To collaborate, share a read-only link above, or start a new team project (a project's team is
      fixed once saved).</p>`);
  } else if (project.teamId && (project.role === 'owner')) {
    parts.push('<div id="recon-members" class="small mb-2 text-muted">Loading members…</div>');
    parts.push(`<div class="input-group input-group-sm mb-1">
        <input type="text" class="form-control" id="recon-invite-id" placeholder="username or email">
        <select class="form-select" id="recon-invite-role" style="max-width:8rem">
          <option value="editor">Editor</option><option value="viewer">Viewer</option><option value="owner">Owner</option>
        </select>
        <button class="btn btn-outline-primary" id="recon-invite-btn" type="button">Invite</button>
      </div>
      <div id="recon-invite-status" class="small mb-3"></div>`);
  } else if (isServerProject()) {
    parts.push('<p class="small text-muted">Only the team owner can manage members.</p>');
  }
  // Open a server project
  parts.push('<hr><h6 class="small text-uppercase text-muted">Open a saved project</h6>');
  parts.push('<div id="recon-open-list" class="small text-muted">Loading…</div>');

  renderCollabBody(parts.join(''));
  wireCollab();
  loadOpenList();
  if (isServerProject() && project.role === 'owner' && !project.teamPersonal) loadMembers();
}

function wireCollab() {
  const on = (id, ev, fn) => { const e = el(id); if (e) e.addEventListener(ev, fn); };
  on('recon-share-create', 'click', shareCreate);
  on('recon-share-stop', 'click', shareStop);
  on('recon-share-copy', 'click', () => { const i = el('recon-share-url'); if (i) { i.select(); document.execCommand && document.execCommand('copy'); } });
  on('recon-save-team', 'click', () => saveToServer(el('recon-team-select') ? el('recon-team-select').value : ''));
  on('recon-create-team', 'click', createTeamThenSave);
  on('recon-invite-btn', 'click', inviteMember);
}

// Ensure the project exists server-side (creating it in `team` if given), return true on success.
async function ensureServer(team) {
  if (isServerProject()) return true;
  const res = await Sync.createProject(cleanSnapshot(), project.fileName || 'Untitled project', team || undefined);
  if (res.status !== 201 || !res.data) { flashSaved('⚠ could not save to the server'); return false; }
  project.serverId = res.data.id;
  project.serverVersion = res.data.version;
  project.role = res.data.role || 'owner';
  project.teamId = res.data.team;
  project.teamPersonal = !team;
  if (!team) project.teamTitle = 'My workbench';
  await putProject(project);
  track('MyD: team save', { team: team ? 'team' : 'personal' });
  setCollabBadge('synced');
  return true;
}
async function saveToServer(team, title) {
  if (await ensureServer(team)) {
    // Prefer an explicit title (e.g. a just-created team); otherwise read the dropdown label.
    if (title) project.teamTitle = title;
    else if (team) { const s = el('recon-team-select'); project.teamTitle = s ? (s.options[s.selectedIndex] || {}).text : ''; }
    await putProject(project);
    await renderCollab();
    maybeStartRealtime(); // a team project goes live immediately
  }
}
async function createTeamThenSave() {
  const name = (el('recon-new-team') || {}).value;
  if (!name || !name.trim()) return;
  const res = await Sync.createTeam(name.trim());
  if (res.status !== 201 || !res.data) { flashSaved('⚠ could not create the team'); return; }
  await saveToServer(res.data.id, res.data.title);
}
async function shareCreate() {
  if (!(await ensureServer(project.teamId))) return;
  const res = await Sync.shareProject(project.serverId);
  if (res.status === 200 && res.data && res.data.url) {
    project.sharedToken = res.data.token; project.sharedUrl = res.data.url;
    await putProject(project);
    track('MyD: shared');
    await renderCollab();
  } else { flashSaved('⚠ could not create a share link'); }
}
async function shareStop() {
  if (!isServerProject()) return;
  await Sync.unshareProject(project.serverId);
  project.sharedToken = null; project.sharedUrl = null;
  await putProject(project);
  await renderCollab();
}
async function loadMembers() {
  const box = el('recon-members');
  if (!box) return;
  const res = await Sync.listMembers(project.teamId);
  if (res.status !== 200 || !res.data) { box.textContent = 'Could not load members.'; return; }
  box.innerHTML = '<div class="fw-bold mb-1">Members</div>' + res.data.members.map((m) =>
    `<div class="d-flex justify-content-between align-items-center py-1">
       <span>${esc(m.name || m.username)} <span class="text-muted">(${esc(m.role)})</span></span>
       ${m.role !== 'owner' ? `<button class="btn btn-sm btn-link text-danger p-0" data-uid="${m.user_id}">remove</button>` : ''}
     </div>`).join('');
  box.querySelectorAll('button[data-uid]').forEach((b) => b.addEventListener('click', async () => {
    await Sync.removeMember(project.teamId, b.dataset.uid); loadMembers();
  }));
}
function setInviteStatus(html, kind) {
  const s = el('recon-invite-status');
  if (s) s.innerHTML = html ? `<span class="text-${kind || 'muted'}">${html}</span>` : '';
}
async function inviteMember() {
  const idf = (el('recon-invite-id') || {}).value;
  const role = (el('recon-invite-role') || {}).value || 'editor';
  if (!idf || !idf.trim()) { setInviteStatus('Enter a username or email address.', 'danger'); return; }
  setInviteStatus('<i class="fas fa-spinner fa-spin me-1"></i>Adding…');
  const res = await Sync.addMember(project.teamId, idf.trim(), role, project.serverId);
  if (res.status === 200 && res.data) {
    if (el('recon-invite-id')) el('recon-invite-id').value = '';
    const who = esc(res.data.name || res.data.username || 'member');
    if (res.data.notified) setInviteStatus(`<i class="fas fa-check me-1"></i>Added ${who} and emailed them an invitation.`, 'success');
    else setInviteStatus(`<i class="fas fa-triangle-exclamation me-1"></i>Added ${who}, but they have no verified email yet, so they couldn't be notified. Ask them to verify an email address on their profile.`, 'warning');
    loadMembers();
  } else if (res.status === 404) {
    // Explicit "no such user" check for the add-a-member UI.
    setInviteStatus('<i class="fas fa-circle-xmark me-1"></i>No WHG user found with that username or email. They must have a WHG account (with a verified email) before you can add them.', 'danger');
  } else {
    setInviteStatus('<i class="fas fa-circle-exclamation me-1"></i>' + esc((res.data && res.data.error) || 'Could not add that person.'), 'danger');
  }
}
async function loadOpenList() {
  const box = el('recon-open-list');
  if (!box) return;
  const res = await Sync.listProjects();
  const list = (res.data && res.data.projects) || [];
  if (!list.length) { box.innerHTML = '<span class="text-muted">No saved projects yet.</span>'; return; }
  box.innerHTML = list.map((p) => `<div class="d-flex justify-content-between align-items-center py-1 border-bottom">
      <span>${esc(p.title)} <span class="text-muted">· ${esc(p.team_title)} · v${p.version} · ${esc(p.role)}</span>
        ${p.id === project.serverId ? '<span class="badge bg-secondary ms-1">open</span>' : ''}</span>
      ${p.id === project.serverId ? '' : `<button class="btn btn-sm btn-outline-primary py-0" data-pid="${p.id}">Open</button>`}
    </div>`).join('');
  box.querySelectorAll('button[data-pid]').forEach((b) => b.addEventListener('click', () => openServerProject(b.dataset.pid)));
}
async function openServerProject(pid) {
  if (!project) project = { id: CURRENT }; // fresh session (e.g. arriving via an ?open=<id> invite link)
  const res = await Sync.fetchProject(pid);
  if (res.status !== 200 || !res.data) { flashSaved('⚠ could not open that project'); return; }
  const d = res.data;
  // Replace local sync metadata wholesale with the opened project's identity.
  SYNC_KEYS.forEach((k) => { delete project[k]; });
  project.serverId = d.id; project.serverVersion = d.version; project.role = d.role;
  project.teamId = d.team; project.teamTitle = d.team_title || ''; project.teamPersonal = !!d.team_personal;
  applySnapshot(d.snapshot);
  await putProject(project);
  setCollabBadge(collabState());
  maybeStartRealtime();
  hideModal('recon-collab-modal');
  flashSaved('Opened the team project');
}

// ── shared-link bootstrap (?shared=<token>) ──────────────────────────────────
async function handleSharedBootstrap(token) {
  try {
    const res = await Sync.fetchShared(token);
    if (res.status !== 200 || !res.data || !res.data.snapshot) { await loadSaved(); return; }
    const snap = res.data.snapshot;
    // Import a LOCAL copy — the recipient edits their own device-only project (no serverId).
    project = Object.assign({}, snap);
    project.id = CURRENT;
    SYNC_KEYS.forEach((k) => { delete project[k]; });
    reviewMeta = []; reviewPos = 0;
    migrateLegacyChain(); normalizeChain();
    await putProject(project);
    renderAll(); showResume(); applyReadOnlyMode();
    setCollabBadge('local');
    track('MyD: shared open');
    flashSaved('Imported a read-only copy shared with you');
  } catch (err) {
    console.warn('[recon] shared import failed', err);
    await loadSaved();
  }
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

// Columns the user tagged as name variants (alt_names). A row's variants are alternative spellings of
// its place name, tried alongside the primary toponym during reconciliation (issue #143). Cells may
// hold several variants separated by ';' or '|'.
function altNameCols() { return project ? project.columns.map((c, i) => i).filter((i) => project.columns[i].role === 'alt_names') : []; }
function rowVariants(rowIdx) {
  const cols = altNameCols(); if (!cols.length) return [];
  const out = [];
  cols.forEach((ci) => String(project.rows[rowIdx][ci] == null ? '' : project.rows[rowIdx][ci])
    .split(/[;|]/).forEach((s) => { const t = s.trim(); if (t) out.push(t); }));
  return [...new Set(out)];
}
// Name forms DERIVED from the value itself, for the shapes historical sources habitually use and a
// literal query cannot match (place#188, reported by Justin Colson):
//
//   "Melford, Long"              an index inverted to sort by head-word     → "Long Melford"
//   "Stow, or Stow-on-the-Wold"  an alternative offered inline              → both names
//   "Newton with Scales"         hyphenation differs between gazetteers     → "Newton-with-Scales"
//   "Broxbourn (St. Augustine)"  a dedication qualifying the name           → "Broxbourn"
//   "Bondgate, with Aismunderby" two townships under one heading            → both names
//   "Bury St. Edmunds, Suffolk"  a county qualifying the name               → "Bury St. Edmunds"
//
// Measured against the index: "Melford, Long" and "Walden, Saffron" return NOTHING, while their
// inverted forms return the town each time. These are query aids only — they are never written back
// into the data, and never exported as alt_names, because they are our guesses and not what the
// source recorded.
//
// Deliberately conservative. Inversion needs exactly one comma and a tail of at most two words, so
// "Melford, Long, Suffolk" is left alone. A junk inversion ("Newcastle, Northumberland" →
// "Northumberland Newcastle") is self-limiting: it is not a place name, so it matches nothing.
// The head-word rule (place#205) is bounded the other way round — by the HEAD, which must be two
// words or more, because a bare junk head is NOT self-limiting: it is a real toponym elsewhere and
// its exact matches displace the answer. See the rule itself for the measurements.
const NAME_CONNECTIVES = /\b(?:on|upon|under|with|in|by|next|cum|super|sub|juxta|le|la|de|du|des)\b/i;
const ALT_CONNECTIVE = /^(?:or|alias|otherwise|aka)\s+(.+)$/i;   // "X, or Y"   — one place, two names
const CONJ_CONNECTIVE = /^(?:with|and|cum)\s+(.+)$/i;            // "X, with Y" — two conjoined names
function derivedNameVariants(primary) {
  const s = String(primary == null ? '' : primary).trim().replace(/\s+/g, ' ');
  if (!s) return [];
  const out = [];
  const seen = new Set([normName(s)]);
  const push = (v) => {
    const t = String(v).trim().replace(/\s+/g, ' ').replace(/^[,;\s]+|[,;\s]+$/g, '');
    const n = normName(t);
    if (t && !seen.has(n)) { seen.add(n); out.push(t); }
  };
  // Parenthetical qualifiers. Registers and church listings qualify a name with a dedication or a
  // disambiguator — "Broxbourn (St. Augustine)". No gazetteer indexes the bracketed form, so the
  // literal value matches nothing and the row came back empty every time (place#199). Query the name
  // without its bracket, and the two run together (some gazetteers spell the dedication inline).
  // NEVER the bracketed part on its own: "(St. Mary and St. Sexburgh)" would match churches the world
  // over, which is exactly the false auto-match reported in place#198.
  const bases = [s];
  const par = s.match(/^([^()]*)\(([^()]*)\)([^()]*)$/);
  if (par) {
    const outside = (par[1] + ' ' + par[3]).replace(/\s+/g, ' ').replace(/\s+,/g, ',').replace(/^[,;\s]+|[,;\s]+$/g, '');
    const inside = par[2].trim();
    if (outside) {
      push(outside);
      if (inside) push(outside + ' ' + inside);
      bases.push(outside);
    }
  }
  // Comma forms, applied to the value and to its de-bracketed form alike.
  bases.forEach((b) => {
    const parts = b.split(',');
    if (parts.length !== 2) return;
    const head = parts[0].trim(), tail = parts[1].trim();
    if (!head || !tail) return;
    const alt = tail.match(ALT_CONNECTIVE), conj = tail.match(CONJ_CONNECTIVE);
    // "X, or Y" names TWO places-worth of name: both sides are real, so try both.
    if (alt) { push(alt[1]); push(head); }
    // "X, with Y" conjoins two townships under one heading — the head is the place proper, so it
    // goes first, but the second name is real too and belongs in the query set (place#200).
    else if (conj) { push(head); push(conj[1]); }
    else if (tail.split(' ').length <= 2) push(tail + ' ' + head);
  });
  // The bare head of a comma form. "Bury St. Edmunds, Suffolk" is a place qualified by its county, and
  // no gazetteer carries the qualifier in the name, so the literal value lands in an unrelated
  // phonetic neighbourhood: measured on prod against `gn`, that row returns *Urochishche Shybyndysor*
  // and a Florida radio station at score 100 (confidence 25), while "Bury St. Edmunds" alone returns
  // the town at 99. Inversion above does not rescue it — "Suffolk Bury St. Edmunds" gives a result set
  // byte-identical to sending no variant at all (place#205).
  //
  // ONLY for a head of two words or more. A single-word head is a common toponym in its own right and
  // its exact matches outrank the answer we already had: "Melford, Long" + "Melford" surfaces Melford
  // (Canada) at confidence 90 and LOSES Long Melford at 88.8, and "Kingston, Surrey" + "Kingston"
  // turns obvious junk at 25 into four arbitrary Kingstons at 90. That is the worse failure — #198's
  // resemblance guard passes on a head-word, since it is contained in the primary, so a confident
  // wrong place would auto-confirm where an empty list would not. Single-word heads therefore stay
  // out, and "Yorks, West Riding" is left to place#204, which fixes it at the data end by giving the
  // county the variants it lacks.
  bases.forEach((b) => {
    const parts = b.split(',');
    if (parts.length < 2) return;
    // Count words on the de-hyphenated head, so "Ashby-de-la-Zouch, Leics" qualifies. Hyphens join
    // words; they don't make a name less distinctive, which is the property the rule is testing for.
    const head = parts[0].trim();
    if (head.replace(/-/g, ' ').split(' ').filter(Boolean).length >= 2) push(head);
  });
  // The same alternation without a comma — "Glandford Brigg or Bridge". Both sides must look like a
  // name (at most four words) so a descriptive phrase isn't chopped into nonsense.
  bases.forEach((b) => {
    if (b.indexOf(',') > -1) return;
    const m = b.match(/^(.*?)\s+(?:or|alias|otherwise)\s+(.*)$/i);
    if (!m) return;
    const a = m[1].trim(), c = m[2].trim();
    if (a && c && a.split(' ').length <= 4 && c.split(' ').length <= 4) { push(a); push(c); }
  });
  // Hyphenation: offer each form the other way round. Only for forms with no comma or bracket left in
  // them — de-hyphenating the raw "Stow, or Stow-on-the-Wold" would send the punctuation back as a query.
  out.slice().concat(bases).forEach((form) => {
    if (form.indexOf(',') > -1 || form.indexOf('(') > -1) return;
    if (form.indexOf('-') > -1) push(form.replace(/\s*-\s*/g, ' '));
    else if (NAME_CONNECTIVES.test(form)) push(form.replace(/ /g, '-'));
  });
  return out;
}

// The variants actually SENT with a reconcile query: the ones the user tagged as alt_names first, then
// the derived forms above filling any slots left. Normalised here exactly as the gateway would do it —
// blanks dropped, case-insensitive duplicates of each other and of the primary toponym removed, capped
// at MAX_QUERY_VARIANTS — so the gateway has nothing left to discard and `variants` stays positionally
// aligned with the `variant_vectors` we compute in-browser. See place#144.
// `userDropped` counts only the user's OWN variants that didn't make it, which is what the "not
// queried" note reports; derived forms losing out to the cap are our business, not theirs.
const MAX_QUERY_VARIANTS = 10;
function queryVariantForms(rowIdx, primary) {
  const seen = new Set([normName(primary)]);
  const forms = [];
  let userDropped = 0;
  rowVariants(rowIdx).forEach((v) => {
    const n = normName(v);
    if (!n || seen.has(n) || forms.length >= MAX_QUERY_VARIANTS) { userDropped += 1; return; }
    seen.add(n); forms.push(v);
  });
  derivedNameVariants(primary).forEach((v) => {
    const n = normName(v);
    if (!n || seen.has(n) || forms.length >= MAX_QUERY_VARIANTS) return;
    seen.add(n); forms.push(v);
  });
  return { forms, userDropped };
}
function queryVariants(rowIdx, primary) { return queryVariantForms(rowIdx, primary).forms; }

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
  // Explicit focus, UNLESS that column has since been locked (an upstream column was invalidated, so
  // this one has no containment yet). A locked column can't be acted on — its switcher pill is
  // disabled — so focusing it would aim Sources and Re-reconcile at a target the user can neither
  // configure nor move away from. See place#184.
  if (reconActiveIdx >= 0 && reconActiveIdx < chain.length && columnState(reconActiveIdx) !== 'locked') {
    return chain[reconActiveIdx];
  }
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
  if (autoConfirmed(key)) return [m.top.id];
  return [];
}
// Candidate ids arrive as `place:<gazetteer id>` — `place:` is the OpenRefine entity-type prefix, and
// what follows is the gazetteer identifier proper (`wd:Q23306`, `gn:2028461`, `whg:<dataset>:<src_id>`).
// Strip it for anything that speaks gazetteer ids rather than OpenRefine ids: the /reconcile containment
// resolver (the prefixed form fails to resolve the container, and the service then silently returns
// UN-contained results — a Parish query "within" a county matching same-named parishes in *other*
// counties, see place#111) and everything we EXPORT, since `place:…` is not a registered LPF namespace
// and would fail schema validation. Keep the prefixed form for /entity/<id>/api, which parses it.
function barePlaceId(id) { return typeof id === 'string' && id.startsWith('place:') ? id.slice(6) : id; }

// ── Parent context ("Wrexham, in Denbighshire") ───────────────────────────────
// When confirming a child column you need to see the containers it sits in — that containment is
// what scoped the query, so without it a candidate list is hard to judge. Prefers the parent's
// CONFIRMED match label (the place actually used as the container) and falls back to the raw cell
// value when the parent isn't resolved. Coarsest LAST, so it reads "parish, in county, in region".
function parentContext(colIndex, rowIdx) {
  if (!project) return [];
  const chain = reconChain();
  const pos = chain.indexOf(colIndex);
  if (pos <= 0) return [];
  const r = Number(rowIdx);
  const out = [];
  for (let p = pos - 1; p >= 0; p--) {
    const c = chain[p];
    const raw = String(project.rows[r][c] == null ? '' : project.rows[r][c]).trim();
    const k = c + ':' + r;
    const acc = acceptedList(project.decisions && project.decisions[k]);
    const m = (project.matches || {})[k];
    let matched = null;
    if (acc.length) matched = acc[0].label;
    else if (autoConfirmed(k)) matched = m.top.name;
    if (raw || matched) out.push({ colName: project.columns[c].name, value: raw, matched });
  }
  return out;
}
function parentContextHTML(colIndex, rowIdx, max) {
  const ctx = parentContext(colIndex, rowIdx);
  if (!ctx.length) return '';
  const bits = ctx.slice(0, max || 2).map((c) => {
    const label = c.matched || c.value;
    const tip = `${c.colName}: ${c.value}${c.matched && c.matched !== c.value ? ` → matched “${c.matched}”` : (c.matched ? '' : ' (not resolved)')}`;
    return `<span title="${esc(tip)}"${c.matched ? '' : ' class="fst-italic"'}>${esc(truncateText(label, 24))}</span>`;
  });
  return `<span class="recon-parent-ctx text-muted small ms-1"> in ${bits.join(' <span class="text-muted">·</span> ')}</span>`;
}

// ── Merged admin values: one reconciliation, one decision, applied to every row ───────────────────
// Admin/parent columns merge identical values so a county appearing in hundreds of rows is reconciled
// ONCE — and, equally, reviewed once: confirming it applies to every row sharing that value. The merge
// unit is the value + country hint + confirmed parent containment, so two same-named places under
// different parents stay separate decisions. Place-name columns are never merged. reconcilePass() and
// the review queue BOTH derive their grouping from mergeSig(), so they can't drift apart. See #143.
function mergeSig(colIndex, rowIdx, parentCol) {
  const r = Number(rowIdx);
  const row = project.rows[r]; if (!row) return null;
  if (parentCol === undefined) {
    const chain = reconChain(); const pos = chain.indexOf(colIndex);
    parentCol = pos > 0 ? chain[pos - 1] : -1;
  }
  const val = String(row[colIndex] == null ? '' : row[colIndex]).trim();
  if (!val) return null;
  const countryIdx = colIndexByRole('country');
  const country = (countryIdx >= 0 && isCcode(row[countryIdx])) ? String(row[countryIdx]).trim().toUpperCase() : '';
  const pids = parentCol >= 0 ? resolvedPlaceIds(parentCol, r).map(barePlaceId).sort().join(',') : '';
  return normName(val) + '|' + country + '|' + pids;
}
// Is this column one whose identical values are merged? (everything except the place-name leaf)
function mergesValues(colIndex) { return colIndex >= 0 && colIndex !== colIndexByRole('name'); }
// Every "<col>:<row>" key that shares a merge unit with this one — just itself for a place-name column.
function mergeGroupKeys(colIndex, rowIdx) {
  const self = colIndex + ':' + Number(rowIdx);
  if (!mergesValues(colIndex)) return [self];
  const mine = mergeSig(colIndex, rowIdx);
  if (mine == null) return [self];
  const out = [];
  for (let r = 0; r < project.rows.length; r++) if (mergeSig(colIndex, r) === mine) out.push(colIndex + ':' + r);
  return out.length ? out : [self];
}
// Write (or clear) a decision for a key AND every row merged with it, so one confirmation propagates.
function setDecision(key, dec) {
  project.decisions = project.decisions || {};
  const ci = key.indexOf(':');
  const keys = mergeGroupKeys(Number(key.slice(0, ci)), key.slice(ci + 1));
  keys.forEach((k) => {
    if (dec) project.decisions[k] = clone(dec); // clone: siblings must not alias one object
    else delete project.decisions[k];
    if (dec && project.flags) delete project.flags[k]; // decided — the "look again" flag is spent
  });
  return keys.length;
}

// ── Flagging an auto-confirmed match for review (place#202) ──────────────────
// Scanning the results table is the fastest way to spot a bad auto-match, but the only way to revisit
// one used to be "review all", which drops the whole column into the queue. A flag puts THIS row in
// the review queue and leaves everything else alone. It is a request to look again, not a decision:
// the match stays auto-confirmed (and stays in exports) until the reviewer actually decides. Fanned
// out to merged sibling rows exactly as a decision is, since they share one review.
function isFlagged(key) { return !!(project && project.flags && project.flags[key]); }
function toggleFlag(key) {
  if (!project) return false;
  project.flags = project.flags || {};
  const ci = key.indexOf(':');
  const on = !isFlagged(key);
  mergeGroupKeys(Number(key.slice(0, ci)), key.slice(ci + 1))
    .forEach((k) => { if (on) project.flags[k] = true; else delete project.flags[k]; });
  return on;
}

// ── Decision rationale (place#180) ───────────────────────────────────────────
// A free-text note explaining WHY a match was chosen, or what remains uncertain
// about it. Kept beside the decisions rather than inside them: a note is just as
// worth recording for a row that auto-confirmed (nothing was decided by hand) or
// for one left as "no match", and burying it in the decision object would tie its
// life to a status that can change. Fanned out to merged sibling rows exactly as a
// decision is, since they share one review.
function noteFor(key) { return (project && project.notes && project.notes[key]) || ''; }
function setNote(key, text) {
  if (!project) return;
  project.notes = project.notes || {};
  const ci = key.indexOf(':');
  const keys = mergeGroupKeys(Number(key.slice(0, ci)), key.slice(ci + 1));
  const val = String(text || '').trim().slice(0, 2000);
  keys.forEach((k) => { if (val) project.notes[k] = val; else delete project.notes[k]; });
  persist();
}

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
  const hasTypes = (s.types && s.types.selected && s.types.selected.length) || colsWithOwnTypes().length;
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
  const perLevel = colsWithOwnTypes().length;
  if (perLevel) bits.push(`types by level (${perLevel})`);
  return bits.join(' · ');
}
// Plain (un-escaped, un-truncated-to-HTML) truncation helper for building label strings.
function truncateText(v, max) { const r = String(v == null ? '' : v); return r.length > max ? r.slice(0, max - 1) + '…' : r; }

// Mutate a per-row reconcile query `q` with the dataset-wide scope. Attribute filters (country, date,
// feature class) apply to every column; the spatial *region* applies only to the ROOT column, because
// child columns are already spatially scoped by their parent's confirmed places (`contained_in`) and a
// dataset-wide region would either duplicate or fight that. Never overrides a per-row country hint or
// an existing containment. `isRoot` = this column has no parent; `hasRowCountry` = the row set q.countries.
function applyGlobalScopeToQuery(q, isRoot, hasRowCountry, colIndex) {
  const s = getScope(); if (!s) return;
  const r = s.region || {};
  // Country codes — a dataset-wide default; a per-row country hint always wins.
  if (!hasRowCountry && !q.countries && r.mode === 'ccodes' && r.ccodes && r.ccodes.length) q.countries = r.ccodes.slice();
  // AAT place types (already expanded to descendants). Both back-ends filter on types.identifier.
  // This column's own types win over the dataset-wide selection, so a County column can look for
  // administrative units while the Place column looks for settlements (place#184).
  const own = getColTypes(colIndex);
  const typeIds = own ? own.ids : ((s.types && s.types.ids) || []);
  if (!q.types && typeIds.length) q.types = typeIds.slice();
  // Temporal window. Legacy WHG ES needs `temporal` + `start`/`end`; the CRC gateway reads `start`/`end`.
  if (s.start != null || s.end != null) {
    q.temporal = true;
    if (s.start != null) q.start = s.start;
    if (s.end != null) q.end = s.end;
    if (s.undated) q.undated = true;
  }
  // Spatial region — applied whenever the query has NO parent containment, on ANY column, not just the
  // root. A child column is normally scoped by its parent's confirmed place(s) (`contained_in`, tighter
  // than the dataset region), so we don't override that. BUT when a parent was skipped or found no
  // match, the child inherits NO containment — and without this it would fall through to a GLOBAL
  // search, returning (and auto-confirming) places on the wrong continent despite the dataset scope.
  // So the region is a FLOOR: parent containment if present, else the dataset region. See issue #143.
  if (!q.contained_in && !q.bounds) {
    // `intersects`, not `within`: the dataset-wide scope is a coarse "restrict to this area" filter,
    // and an AREA candidate is rarely strictly inside its container once boundaries differ even
    // slightly. Measured: UKHC historic Welsh counties return ZERO hits as `within` wd:Q25 (Wales)
    // but match under `intersects` — historic county borders don't nest exactly inside the modern
    // national polygon. With scope now a HARD filter (place#144), `within` silently discarded valid
    // matches. See issue #143.
    if (r.mode === 'whg' && r.place && r.place.id) {
      // Same containment strictness the user chose for the chain — a dataset-wide region is the same
      // kind of constraint as a parent column's, and having one obey the knobs while the other
      // ignored them would be indefensible.
      const sp = spatialSettings();
      q.contained_in = [barePlaceId(r.place.id)];
      q.containment = sp.containment;
      q.relation = sp.relation;
    }
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

// ── Gateway scope / variant reporting (place#144) ────────────────────────────
// The gateway reports, per query, whether the geographic scope could actually be applied. Crucially
// `scope.applied === false` means it FAILED CLOSED: it refused to answer an explicitly scoped query
// with unscoped results, so an empty result is deliberate and must NOT be presented as "no matches".
// A gateway predating this sends no `scope` key at all — treat undefined as "previous behaviour",
// never as applied:false.
let lastScope = null;          // scope object from the most recent run (dataset-wide, so one per run)
let lastVariantsDropped = 0;   // name variants we sent that the gateway didn't query (cap/dedupe)
let lastDerivedForms = new Set(); // name forms the GATEWAY derived for itself (place#199/#206)
function scopeFailed() { return !!(lastScope && lastScope.applied === false); }
function idList(ids, max) {
  const a = (ids || []).slice(0, max || 4).map((s) => esc(String(s)));
  const more = (ids || []).length - a.length;
  return a.join(', ') + (more > 0 ? ` +${more} more` : '');
}
function renderScopeNotice() {
  const box = el('recon-scope-notice'); if (!box) return;
  if (!lastScope) { box.innerHTML = (lastVariantsDropped ? variantNoteHTML() : '') + derivedFormsHTML(); return; }
  const s = lastScope;
  const parts = [];
  if (s.applied === false) {
    // Fail-closed: say the region couldn't be applied, NOT "no candidates found".
    parts.push(`<div class="alert alert-danger py-2 px-3 mb-2">
      <i class="fas fa-circle-exclamation me-1"></i><strong>The region you chose couldn’t be applied</strong>, so no
      results were returned — this is <em>not</em> “no matches”.
      ${s.message ? `<div class="mt-1">${esc(s.message)}</div>` : ''}
      ${(s.containers_unresolved || []).length ? `<div class="mt-1 text-muted">No usable geometry for: ${idList(s.containers_unresolved, 6)}</div>` : ''}
      <div class="mt-1">Pick a different region in <strong>Scope&nbsp;→&nbsp;Where</strong>, or clear the scope and reconcile again.</div>
    </div>`);
  } else {
    if (s.mode === 'linked-polygon' && (s.containers_linked || []).length) {
      // A real boundary, just borrowed from a sameAs/exactMatch record of the same place — a quiet note.
      parts.push(`<div class="text-muted mb-1"><i class="fas fa-circle-info me-1"></i>Boundary taken from
        ${idList(s.containers_linked, 3)} (same place).</div>`);
    }
    if (s.mode === 'polygon' && (s.containers_approximated || []).length) {
      // Only a PARTIAL honour: other containers supplied real boundaries and the point-only ones were
      // dropped from the union, so the scope is NARROWER than asked for. Deliberately not shown for
      // mode 'linked-polygon' — there the point-only container is still listed here, but it was
      // resolved through a co-referent's boundary rather than ignored, so warning would be wrong.
      parts.push(`<div class="alert alert-warning py-2 px-3 mb-2">
        <i class="fas fa-triangle-exclamation me-1"></i>Some places you scoped to have no boundary and were
        <strong>ignored</strong>: ${idList(s.containers_approximated, 6)}. Results are narrower than you asked for.</div>`);
    }
    if (s.approximate === true) {
      parts.push(`<div class="text-muted mb-1"><i class="fas fa-circle-info me-1"></i>Scope is approximate
        (bounding box) — results may include places just outside it.</div>`);
    }
  }
  if (lastVariantsDropped) parts.push(variantNoteHTML());
  parts.push(derivedFormsHTML());
  box.innerHTML = parts.join('');
}
function variantNoteHTML() {
  return `<div class="text-muted"><i class="fas fa-circle-info me-1"></i>${lastVariantsDropped.toLocaleString()}
    name variant${lastVariantsDropped === 1 ? ' was' : 's were'} not queried (duplicates removed, max 10 per row).</div>`;
}
function derivedFormsHTML() {
  if (!lastDerivedForms.size) return '';
  const forms = [...lastDerivedForms];
  const shown = forms.slice(0, 6).map((f) => `<code>${esc(f)}</code>`).join(', ');
  const more = forms.length - 6;
  return `<div class="text-muted"><i class="fas fa-wand-magic-sparkles me-1"></i>WHG also searched for
    ${shown}${more > 0 ? ` and ${more.toLocaleString()} more form${more === 1 ? '' : 's'}` : ''},
    derived from your values. Your data is unchanged.</div>`;
}
function toggleRunning(on) {
  running = on;
  el('recon-run').classList.toggle('d-none', on);
  const stop = el('recon-stop');
  stop.classList.toggle('d-none', !on);
  if (on) { stop.disabled = false; stop.innerHTML = '<i class="fas fa-stop me-1"></i>Cancel — keep matches so far'; } // reset from a prior cancel
  const rr = el('recon-rerun'); if (rr && on) rr.classList.add('d-none'); // restored by updateRerunButton after the run
  if (!on) rtSetActivity(null); // release the advisory lock when the run ends (reconcilePass set it)
}
function updateProgress(done, total) {
  el('recon-progress-wrap').classList.remove('d-none');
  const pct = total ? Math.round((done / total) * 100) : 0;
  el('recon-progress-bar').style.width = pct + '%';
  el('recon-progress-text').textContent = `${done.toLocaleString()} / ${total.toLocaleString()} rows (${pct}%)`;
}

// ── Spatial constraints (place#184) ─────────────────────────────────────────
// Two filters, both enforced by the gateway rather than used as ranking hints. The row-coordinate
// circle answers "my table already says where this is" — the strongest disambiguator a dataset can
// carry, and previously ignored entirely. The containment knobs expose what the chain has always
// sent as fixed values: `fuzzy` tests membership against an H3 grid (fast, tolerant) and
// `intersects` accepts any overlap, which is why a confirmed county did not strictly bound results.
// The row-coordinate circle now reaches the gateway as lat/lng/radius and is
// resolved there as an H3 disc — a terms match on the covers already in the index —
// instead of becoming a polygon put through Shapely union, polyfill and prepared
// geometry. That polygon path is the one twice implicated in a wedged worker
// (2026-08-18), and a radial filter never needed it. See place#184.
const NEARBY_FILTER_ENABLED = true;

function spatialSettings() {
  const d = { nearby: false, radiusKm: 25, containment: 'fuzzy', relation: 'intersects' };
  const s = (project && project.spatial) || {};
  return {
    nearby: NEARBY_FILTER_ENABLED && (s.nearby != null ? !!s.nearby : d.nearby),
    radiusKm: Number.isFinite(+s.radiusKm) && +s.radiusKm > 0 ? +s.radiusKm : d.radiusKm,
    containment: s.containment === 'exact' ? 'exact' : d.containment,
    relation: s.relation === 'within' ? 'within' : d.relation,
  };
}
// Great-circle distance in km — for the client-side radius check below.
function haversineKm(a, b) {
  const R = 6371, toRad = (d) => (d * Math.PI) / 180;
  const dLat = toRad(b.lat - a.lat), dLon = toRad(b.lon - a.lon);
  const s1 = Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(a.lat)) * Math.cos(toRad(b.lat)) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.min(1, Math.sqrt(s1)));
}

// Reflect the saved spatial settings, and offer the coordinate circle only when the dataset has
// coordinates to filter by — an unusable checkbox is worse than none.
function refreshSpatialControls() {
  const sp = spatialSettings();
  const wrap = el('recon-nearby-wrap');
  const has = hasCoordRole() && NEARBY_FILTER_ENABLED;
  if (wrap) wrap.classList.toggle('d-none', !has);
  const cb = el('recon-nearby'); if (cb) cb.checked = has && sp.nearby;
  const rad = el('recon-nearby-radius'); if (rad) { rad.value = sp.radiusKm; rad.disabled = !(has && sp.nearby); }
  const con = el('recon-containment'); if (con) con.value = sp.containment;
  const rel = el('recon-relation'); if (rel) rel.value = sp.relation;
  const note = el('recon-spatial-note');
  if (note) {
    const bits = [];
    if (has && sp.nearby) bits.push(`matches must lie within ${sp.radiusKm} km of each row`);
    if (sp.relation === 'within') bits.push('a place must lie WHOLLY inside its container — historic boundaries rarely nest, so this can return nothing');
    note.textContent = bits.join(' · ');
  }
}
// Save a spatial setting. Existing matches were found under the OLD constraint, so they are left
// alone and flagged stale — the same treatment a Sources change gets — rather than silently
// discarded or, worse, silently kept as if they still met the filter.
function setSpatialSetting(patch) {
  if (!project) return;
  project.spatial = Object.assign({}, spatialSettings(), patch);
  persist();
  refreshSpatialControls();
  if (project.matches && Object.keys(project.matches).length) {
    reconStaleNote = 'Spatial constraints changed — re-reconcile a column to apply them to its matches.';
    renderColSwitcher();
    // The switcher is hidden for a single-column set, so the note would have nowhere to appear —
    // and a filter that silently doesn't apply to the matches on screen is exactly the confusion
    // this feature exists to remove.
    if (reconChain().length <= 1) {
      setReconSummary('<span class="text-warning"><i class="fas fa-triangle-exclamation me-1"></i>' +
        'Spatial constraints changed — reconcile again to apply them.</span>');
    }
  }
}
function wireSpatialControls() {
  const cb = el('recon-nearby');
  if (cb) cb.addEventListener('change', () => setSpatialSetting({ nearby: cb.checked }));
  const rad = el('recon-nearby-radius');
  if (rad) rad.addEventListener('change', () => {
    const km = parseFloat(rad.value);
    setSpatialSetting({ radiusKm: Number.isFinite(km) && km > 0 ? km : 25 });
  });
  const con = el('recon-containment');
  if (con) con.addEventListener('change', () => setSpatialSetting({ containment: con.value }));
  const rel = el('recon-relation');
  if (rel) rel.addEventListener('change', () => setSpatialSetting({ relation: rel.value }));
}

function getThreshold() {
  const box = el('recon-threshold');
  const n = box ? parseInt(box.value, 10) : NaN;
  return Number.isFinite(n) ? Math.min(100, Math.max(0, n)) : 90;
}
// ── Does the winning candidate even resemble the value? (place#198) ──────────
// Scores are RELATIVE: the service normalises by the best candidate in the pool it retrieved, so
// something always comes back at ~100 however poor that pool is. A score is therefore a ranking, not
// evidence of similarity — which is how "Minster-in-Sheppy (St. Mary and St. Sexburgh)" came to be
// auto-confirmed against "ST JAMES'S", and "Glandford Brigg or Bridge" against "Gilbertine Order".
// So auto-confirm additionally requires the matched name to LOOK like one of the forms we queried.
// This only ever withholds an auto-confirm: the candidate is still offered, in review, where the
// person decides. Nothing is discarded and no score is altered.
const AUTO_NAME_SIM = 0.45;
// Comparison form: accents folded, punctuation to spaces, lower-cased. "St. Mary's" ≡ "st marys".
// Letters of every script are kept, so two Cyrillic or two Chinese names still compare properly.
function simNorm(str) {
  return String(str == null ? '' : str)
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .toLowerCase().replace(/[^\p{L}\p{N}]+/gu, ' ').trim();
}
// Names written in different scripts cannot be compared letter by letter: an endonym and its exonym
// share nothing (Київ / Kyiv scores zero however right the match is). We can't judge those, so the
// guard stands aside rather than withholding a good match.
function comparableScripts(a, b) {
  const latin = (x) => /\p{Script=Latin}/u.test(String(x || ''));
  return latin(a) === latin(b);
}
function simBigrams(str) {
  const out = new Set();
  for (let i = 0; i < str.length - 1; i++) out.add(str.slice(i, i + 2));
  return out;
}
// Sørensen–Dice over character bigrams, lifted to 1 when one name's words are wholly contained in the
// other's ("Long Melford" vs "Long Melford railway station"): a qualified form of the same name is a
// resemblance the bigram measure under-reports on long qualifiers.
function nameSimilarity(a, b) {
  const x = simNorm(a), y = simNorm(b);
  if (!x || !y) return 0;
  if (x === y) return 1;
  const xt = x.split(' '), yt = y.split(' ');
  const short = xt.length <= yt.length ? xt : yt, long = new Set(xt.length <= yt.length ? yt : xt);
  if (short.every((t) => long.has(t))) return 1;
  const A = simBigrams(x), B = simBigrams(y);
  if (!A.size || !B.size) return 0;
  let common = 0;
  A.forEach((g) => { if (B.has(g)) common += 1; });
  return (2 * common) / (A.size + B.size);
}
// Every name form this key was queried with: the cell value, the user's own alt_names, and the forms
// we derived from the value. A match on any of them is a real resemblance.
function queryFormsForKey(key) {
  const ci = key.indexOf(':');
  const col = Number(key.slice(0, ci)), row = Number(key.slice(ci + 1));
  if (!project || !project.rows[row]) return [];
  const primary = cellVal(row, col);
  if (!primary) return [];
  return [primary].concat(queryVariantForms(row, primary).forms);
}
const _simCache = new Map();
function candidateResembles(key, cand) {
  if (!cand) return false;
  if (cand.match) return true; // the service matched the name exactly — nothing to second-guess
  const forms = queryFormsForKey(key);
  if (!forms.length) return true; // no value to compare against; don't invent a reason to withhold
  // Keyed by every form we would compare, not just the value: two rows can share a place name and
  // carry different alt_names, and they must not share a verdict.
  const ck = forms.map(normName).join('\u0001') + '\u0000' + (cand.id || '') + '\u0000' + normName(cand.name || '');
  if (_simCache.has(ck)) return _simCache.get(ck);
  const names = [cand.name].concat((cand.alt_names || []).slice(0, 20));
  let best = 0, judged = false;
  for (let i = 0; i < forms.length && best < AUTO_NAME_SIM; i++) {
    for (let j = 0; j < names.length && best < AUTO_NAME_SIM; j++) {
      if (!comparableScripts(forms[i], names[j])) continue;
      judged = true;
      best = Math.max(best, nameSimilarity(forms[i], names[j]));
    }
  }
  const ok = !judged || best >= AUTO_NAME_SIM;
  _simCache.set(ck, ok);
  return ok;
}

// ── Absolute match quality from the gateway (place#206) ──────────────────────
// `score` is normalised against the best candidate in the response, so the top one reads ~100 whether
// the match is perfect or the best of a bad lot — the defect behind place#198. `confidence` is
// absolute and comparable between queries. measured against the live
// gateway, 2026-08-20:
//
//   100        exactly spelled                   (a variant's exact match: 90)
//   87–91      a derived head-word match         ("Bury St. Edmunds, Suffolk", place#205)
//   32         lexically near                    ("Broxbourn (St. Augustine)" → Broxbourne)
//   22–26      noise, and phonetic-only matches  correct or not, no lexical evidence either way
//
// 30 is the gateway's own recommended line ("< ~30 means phonetic-only, unverified — not wrong") and
// sits in the observed gap between 25.7 and 32.1. The margin is narrow on the near-miss side, so this
// wants re-checking if the gateway's scoring moves. Auto-confirm therefore needs evidence in the
// SPELLING; a phonetic-only match is offered in review instead. That is a deliberate tightening for
// cross-script matching, where the lexical guard below stands aside and such a match used to
// auto-confirm unchecked.
const MIN_AUTO_CONFIDENCE = 30;
function candConfidence(cand) {
  const c = cand ? Number(cand.confidence) : NaN;
  return Number.isFinite(c) ? c : null;   // null = not measured (legacy path, or a non-fuzzy mode)
}
// Why auto-confirm was withheld from a top candidate that otherwise clears the threshold, or null.
// Candidates reconciled BEFORE this shipped carry no confidence, so they keep falling back to the
// lexical heuristic and a saved project's matches don't change verdict when it is reopened.
function autoWithheldReason(key, cand) {
  if (!cand) return null;
  const conf = candConfidence(cand);
  if (conf !== null) return conf < MIN_AUTO_CONFIDENCE ? 'confidence' : null;
  return candidateResembles(key, cand) ? null : 'resemblance';
}

// Auto-confirm a top candidate when the name matched exactly, or its score clears the threshold —
// UNLESS the match is too poor in absolute terms (above), or another DISTINCT candidate ties the
// top score. An exact tie between different places (e.g. "Devon" in GB vs AU, both 100) is genuinely
// ambiguous and belongs in review, not an auto-guess.
// Same-place duplicates from multiple sources (identical name + description) are NOT ambiguous.
function isAutoConfirmed(top, threshold, cands, key) {
  if (!top) return false;
  if (!(top.match || Number(top.score) >= threshold)) return false;
  if (autoWithheldReason(key, top)) return false;
  if (cands && cands.length > 1) {
    const t = cands[0];
    for (let i = 1; i < cands.length && Number(cands[i].score) >= Number(t.score); i++) {
      // An INEXACT name is no rival to an exact one, however the scores tie. Searching
      // "Sherborne" turns up "Sherborne railway station" at the same 100 — relevance
      // scoring cannot separate them — and treating that as ambiguity blocked the
      // auto-confirm of a plainly exact match. A second EXACT candidate still counts:
      // two different places both actually called Sherborne IS ambiguous. See place#184.
      if (!!t.match && !cands[i].match) continue;
      if (cands[i].name !== t.name || (cands[i].description || '') !== (t.description || '')) return false;
    }
  }
  return true;
}

// Is the match for this key auto-confirmed? (the form every caller wants — the guard above needs the
// key to know what was asked for, so callers should not hand-roll the argument list.)
function autoConfirmed(key) {
  const m = project && project.matches && project.matches[key];
  return !!(m && m.top && isAutoConfirmed(m.top, getThreshold(), m.candidates, key));
}

// ── Candidate review (Phase 4) ───────────────────────────────────────────────
let reviewMeta = []; // [{key, rows, name, country}] — one per reviewable row
let reviewPos = 0;
let _lastDecisionKey = null; // the row most recently decided — so Undo reverts THAT even after auto-advance (snag #148)

const REVIEW_BADGE = {
  accepted: '<span class="badge bg-success">accepted ✓</span>',
  auto: '<span class="badge bg-success">auto ✓</span>',
  flagged: '<span class="badge bg-primary">flagged <i class="fas fa-flag"></i></span>',
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
  if (isFlagged(key)) return 'flagged';
  if (m && m.top) return autoConfirmed(key) ? 'auto' : 'candidate';
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
  if (!dec && autoConfirmed(key)) cis.push(0);
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
  return autoConfirmed(key) ? m.top : null;
}

// The matches RESOLVED for a key, in the shape the exporters want: explicit accepts, else the
// auto-confirmed top candidate. Auto-confirmed rows never enter review, so nothing is ever written
// to `decisions` for them — reading decisions alone dropped every auto-match from the exported
// CSV/JSON/LP-TSV/LPF (place#183). An explicit decision always wins, so a rejected/skipped/no-match
// row resolves to nothing even when its top candidate would otherwise auto-confirm.
function resolvedMatchList(key) {
  const m = (project.matches && project.matches[key]) || null;
  const cands = (m && m.candidates) || [];
  const dec = project.decisions && project.decisions[key];
  if (dec) return acceptedList(dec).map((a) => ({ id: a.place_id, title: a.label, score: a.score, source: nsName(a.place_id), cand: cands[a.ci] || null, accepted: true }));
  if (autoConfirmed(key)) {
    return [{ id: m.top.id, title: m.top.name, score: m.top.score, source: nsName(m.top.id), cand: m.top, accepted: false }];
  }
  return [];
}

function renderResultsTable(built) {
  const matches = project.matches || {};
  const threshold = getThreshold();
  let matched = 0, auto = 0, nomatch = 0, pending = 0, rowsMatched = 0, accepted = 0;
  built.map.forEach((v, key) => {
    const m = matches[key];
    if (!m) { pending += 1; return; }
    if (m.top) { matched += 1; rowsMatched += v.rows.length; if (isAutoConfirmed(m.top, threshold, m.candidates, key)) auto += 1; }
    else nomatch += 1;
    if (project.decisions && project.decisions[key] && project.decisions[key].status === 'accepted') accepted += 1;
  });
  // When the gateway failed the scope closed, every row came back empty BY DESIGN. Reporting that as
  // "N no match" would send the user hunting for a data problem that doesn't exist, so say what
  // actually happened instead (the full explanation is in the scope notice above). See place#144.
  if (scopeFailed() && !matched) {
    setReconSummary(
      `<span class="text-danger"><i class="fas fa-circle-exclamation me-1"></i><strong>No results — the region you
       chose couldn’t be applied.</strong></span> <span class="text-muted">These rows were not matched against
       anything; this is not “no match”. Adjust <strong>Scope&nbsp;→&nbsp;Where</strong> and reconcile again.</span>`);
  } else {
    setReconSummary(
      `<span class="text-success"><strong>${matched.toLocaleString()}</strong> matched</span> ` +
      `<span class="text-muted">(<strong>${auto.toLocaleString()}</strong> auto, <strong>${accepted.toLocaleString()}</strong> accepted)</span> · ` +
      `<span class="text-warning"><strong>${nomatch.toLocaleString()}</strong> no match</span> · ` +
      `<span class="text-muted"><strong>${pending.toLocaleString()}</strong> pending</span> — ` +
      `across <strong>${built.map.size.toLocaleString()}</strong> rows, ` +
      `covering <strong>${rowsMatched.toLocaleString()}</strong> of ${project.total.toLocaleString()} rows.` +
      // The flag affordance is invisible until you know it's there, and the people who most need it
      // are the ones scanning a table full of wrong auto-matches (place#202).
      ((auto || nomatch) ? ` <span class="text-muted"><i class="fas fa-flag me-1"></i>Click a row’s status badge to send it
        to review — a wrong <span class="badge bg-success">auto ✓</span>, or a
        <span class="badge bg-warning text-dark">no match</span> you want to search for by hand.</span>` : ''));
  }

  // Build the full ordered row-info list once; the table is virtualised (only the visible window is
  // in the DOM), so it copes with very large datasets — off-screen rows are evicted on scroll.
  _resultRows = [];
  for (let i = 0; i < project.rows.length; i++) { const info = keyForRow(built, i); if (info && rowPasses(i, built)) _resultRows.push(info); }
  el('recon-results-wrap').classList.remove('d-none');
  renderResultsWindow(true);
  updatePaneSummaries();
}

// ── Virtualised results table (lazy load + auto-eviction) ────────────────────
let _resultRows = [];        // full ordered list of row infos {name, country, key}
let RESULT_ROW_H = 34;       // px per row; self-calibrated from the first render

// How many rows the table holds, and whether there are more than fit on screen. The old wording
// ("scroll to load more — off-screen rows are evicted") promised an action that doesn't exist and
// described our virtualisation rather than anything the reader does; it was shown even when every
// row was already visible. See place#196.
function setResultsNote(total, shown) {
  const box = el('recon-results-note');
  if (!box) return;
  const filtered = filtersActive() ? ' filtered' : '';
  box.textContent = total
    ? `${total.toLocaleString()}${filtered} row${total === 1 ? '' : 's'}${shown < total ? ' — scroll the table for the rest' : ''}.`
    : (filtersActive() ? 'No rows match the current filters.' : '');
}

function resultRowHtml(info) {
  const m = (project.matches || {})[info.key];
  let status, top = '', score = '';
  if (!m) status = '<span class="badge bg-secondary">pending</span>';
  else {
    const st = effectiveStatus(info.key);
    status = REVIEW_BADGE[st] || '';
    // Clicking an auto-confirmed badge sends that row to review, and clicking again takes it back
    // out — the quickest route from "that one is obviously wrong" to actually fixing it (place#202).
    const FLAG_TITLE = {
      auto: 'Wrong? Click to send this match to review',
      none: 'Nothing matched — click to send this row to review and search for it by hand',
      flagged: 'Flagged for review — click to unflag',
    };
    if (FLAG_TITLE[st]) {
      status = `<span class="recon-flag-toggle" role="button" tabindex="0" data-flag="${esc(info.key)}" title="${FLAG_TITLE[st]}">${status}</span>`;
    }
    const show = acceptedCandidate(info.key) || m.top;
    if (show) { top = `${truncate(show.name, 50)} <span class="text-muted small">${truncate(show.description || '', 30)}</span>`; score = show.score; }
  }
  const ci = info.key.indexOf(':');
  const ctx = parentContextHTML(Number(info.key.slice(0, ci)), info.key.slice(ci + 1));
  return `<tr data-row><td>${truncate(info.name, 50)}${info.country ? ` <span class="text-muted">(${esc(info.country)})</span>` : ''}${ctx}</td>` +
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
  setResultsNote(total, last - first);
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
const STATUS_LABELS = { accepted: 'accepted', auto: 'auto-confirmed', flagged: 'flagged for review', candidate: 'needs review', rejected: 'rejected', skipped: 'skipped', nomatch: 'no match', none: 'no match' };
const STATUS_ORDER = ['candidate', 'flagged', 'auto', 'accepted', 'nomatch', 'none', 'skipped', 'rejected'];
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

// Rows the review pane can show. Rows with NO candidates are included: they need no decision (see
// needsReview), but they are precisely the ones a reviewer wants to open and search by hand — a
// queue that excluded them left "no match" as a dead end (place#201).
function reviewableKeys(built) {
  const arr = [];
  const merged = mergesValues(built.colIndex); // admin column → one review per distinct value
  const seen = merged ? new Map() : null;      // mergeSig → index into arr
  built.map.forEach((v, key) => {
    const m = project.matches[key];
    if (!(m && rowPasses(rowKeyIndex(key), built))) return;
    const entry = { key, rows: v.rows.length, name: v.query, country: v.country };
    if (!merged) { arr.push(entry); return; }
    const sig = mergeSig(built.colIndex, rowKeyIndex(key));
    if (sig == null) { arr.push(entry); return; }
    const at = seen.get(sig);
    // One card per merge unit; `rows` accumulates so the reviewer sees (and is sorted by) real impact.
    if (at == null) { seen.set(sig, arr.length); entry.merged = true; arr.push(entry); }
    else arr[at].rows += v.rows.length;
  });
  arr.sort((a, b) => b.rows - a.rows);
  return arr;
}
function needsReview(key) {
  if (project.decisions && project.decisions[key]) return false;
  const m = project.matches[key];
  if (!m) return false;
  // A row the reviewer flagged in the results table joins the queue whatever its status — that IS the
  // point of the flag: to come back to a wrong auto-match, or to a row that matched nothing and wants
  // a hand search, without turning on "review all" (place#202). Unflagged rows with no candidates
  // need no decision, or every column would be permanently un-confirmable.
  if (isFlagged(key)) return true;
  return !!(m.top && !autoConfirmed(key));
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
// The absolute quality of a candidate, beside its (relative) score. Shown only when the service
// measured it — an absent chip means "not measured", never "poor". See place#206.
function confidenceChipHTML(cand) {
  const conf = candConfidence(cand);
  if (conf === null) return '';
  const band = conf >= 90 ? ['exact', 'text-success']
    : conf >= MIN_AUTO_CONFIDENCE ? ['near', 'text-body-secondary']
    : ['unverified', 'text-warning-emphasis'];
  return `<span class="recon-cand-conf ${band[1]}" title="Match quality ${Math.round(conf)}/100 — how well this name
    actually fits what you asked for, independent of the other candidates. The score beside it is only a ranking.">${band[0]}</span>`;
}

function renderReviewCard() {
  const card = el('recon-review-card');
  if (!card || !reviewMeta.length) { if (card) card.innerHTML = ''; return; }
  const all = !!(el('recon-review-all') && el('recon-review-all').checked); // reviewing auto-confirmed too?
  const meta = reviewMeta[reviewPos];
  const m = project.matches[meta.key];
  const dec = project.decisions && project.decisions[meta.key];
  const auto = autoConfirmed(meta.key);
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
    (c.found_by ? `<span class="badge bg-secondary ms-1" title="Found by your search for “${esc(c.found_by)}” — not returned by the reconciliation run">searched</span>` : '') +
    `<span class="recon-cand-ns ms-1">${esc(nsName(c.id))}</span>` +
    `<span class="text-muted small ms-1">${truncate(c.description || '', 36)}</span>` +
    (c.alt_names && c.alt_names.length
      ? `<span class="recon-cand-alt">also: ${c.alt_names.slice(0, 8).map((n) => truncate(n, 28)).join(', ')}${c.alt_names.length > 8 ? '…' : ''}</span>`
      : '') +
    wikiLinkHtml(c.wikipedia) +
    `</span>
       <span class="recon-cand-score">${c.score}${confidenceChipHTML(c)}</span>
     </li>`).join('');
  const loadMore = !m.top ? ''
    : m.exhausted ? '<div class="small text-muted mt-1">all candidates shown.</div>'
    : `<div class="mt-1"><button type="button" class="btn btn-sm btn-link p-0 recon-loadmore" data-act="more">load more candidates</button></div>`;
  // Search the gazetteers by hand — for the rows where the reviewer can see WHY nothing matched
  // (place#201). Prefilled with the value so it's a quick edit rather than retyping.
  const manualSearch =
    `<div class="recon-review-search mt-2">
       <div class="input-group input-group-sm">
         <span class="input-group-text"><i class="fas fa-magnifying-glass"></i></span>
         <input type="text" id="recon-review-search-q" class="form-control" value="${esc(meta.name)}"
                placeholder="search the gazetteers for another name…"
                aria-label="Search the gazetteers for another name">
         <button type="button" class="btn btn-outline-secondary" data-act="search">Search</button>
       </div>
       <div id="recon-review-search-note" class="small mt-1"><span class="text-muted">Not what you expected? Search for a
         spelling you know — results are added to the list above, and your dataset-wide scope filters are not applied.</span></div>
     </div>`;
  // Why a match scoring above the threshold is nonetheless sitting in review (place#198). Scores are
  // relative to the pool the service retrieved, so a poor pool still yields a ~100; saying so beats
  // leaving the reviewer to wonder whether the threshold is broken.
  const why = (!auto && !dec && m.top && Number(m.top.score) >= getThreshold())
    ? autoWithheldReason(meta.key, m.top) : null;
  const withheld = why
    ? `<div class="small text-warning-emphasis mt-1"><i class="fas fa-circle-exclamation me-1"></i>Not auto-confirmed:
        the top match scores ${esc(String(m.top.score))}, but ${why === 'confidence'
          ? `its measured quality is only ${Math.round(candConfidence(m.top))}/100 — it may sound like your value without
             being spelled like it`
          : 'its name doesn’t resemble your value'}, and a score only ranks what the search found.
        Accept it if it is right.</div>` : '';
  const flagged = isFlagged(meta.key)
    ? `<div class="small mt-1"><i class="fas fa-flag text-primary me-1"></i>${auto
        ? 'You flagged this auto-confirmed match for review.'
        : 'You flagged this row for review — search below for a name the gazetteers might hold.'}
        <button type="button" class="btn btn-sm btn-link p-0 align-baseline" data-act="unflag">${
          auto ? 'keep it as auto-confirmed' : 'remove the flag'}</button></div>` : '';
  card.innerHTML =
    `<div class="recon-review-head d-flex justify-content-between align-items-start flex-wrap gap-2">
       <div><span class="fw-bold">${truncate(meta.name, 60)}</span>${meta.country ? ` <span class="text-muted">(${esc(meta.country)})</span>` : ''}
         ${parentContextHTML(Number(meta.key.slice(0, meta.key.indexOf(':'))), meta.key.slice(meta.key.indexOf(':') + 1), 3)}
         <span class="text-muted small ms-2">${meta.rows.toLocaleString()} row${meta.rows === 1 ? '' : 's'} · ${reviewPos + 1} of ${reviewMeta.length}</span>
         ${meta.merged && meta.rows > 1 ? `<span class="badge bg-info-subtle text-info-emphasis border border-info-subtle ms-1"
            title="Identical values under the same parent share one reconciliation and one decision">
            <i class="fas fa-object-group me-1"></i>applies to all ${meta.rows.toLocaleString()} rows</span>` : ''}</div>
       <div>${REVIEW_BADGE[effectiveStatus(meta.key)] || ''}</div>
     </div>
     ${flagged}
     <ol class="recon-cand-list">${list || '<li class="text-muted">No candidates were returned for this name.</li>'}</ol>
     ${withheld}
     ${loadMore}
     ${manualSearch}
     <div class="recon-review-actions d-flex flex-wrap align-items-center gap-2 mt-2">
       <button type="button" class="btn btn-sm btn-outline-secondary" data-act="prev" title="Back (←)"><i class="fas fa-arrow-left"></i></button>
       <button type="button" class="btn btn-sm btn-outline-danger" data-act="reject">Reject <kbd>x</kbd></button>
       <button type="button" class="btn btn-sm btn-outline-secondary" data-act="skip">Skip <kbd>s</kbd></button>
       <button type="button" class="btn btn-sm btn-outline-warning" data-act="nomatch">No match <kbd>n</kbd></button>
       <button type="button" class="btn btn-sm btn-outline-secondary" data-act="undo">Undo <kbd>u</kbd></button>
       ${(() => {
         const pend = reviewMeta.filter((r) => needsReview(r.key)).length;
         if (pend <= 1) return '';
         // In "review all" mode the queue is every reviewable value (743, say) but only the ones that
         // did NOT auto-confirm are unreviewed — and only those may be skipped: writing 'skipped' over
         // an auto-confirmed match would DOWNGRADE it, silently discarding a good match the reviewer
         // never asked to lose. So the count stays the pending count; what changes with the toggle is
         // the wording, which used to leave "(5)" unexplained beside a queue of 743 (place#195).
         const auto = all ? reviewMeta.length - pend : 0;
         return `<button type="button" class="btn btn-sm btn-outline-secondary ms-auto" data-act="skipall"
              title="Mark every unreviewed value in this column as skipped, so you can move on to the next column.${
                auto ? ` The ${auto.toLocaleString()} auto-confirmed match${auto === 1 ? '' : 'es'} in this queue are kept as they are.` : ''}">
              Skip all unreviewed (${pend.toLocaleString()}${auto ? ` of ${reviewMeta.length.toLocaleString()}` : ''})</button>`;
       })()}
       <button type="button" class="btn btn-sm btn-primary${reviewMeta.filter((r) => needsReview(r.key)).length > 1 ? '' : ' ms-auto'}" data-act="next">Next <i class="fas fa-arrow-right"></i></button>
     </div>
     <div class="recon-review-note mt-2">
       <label class="small text-muted" for="recon-review-note-input">
         <i class="fas fa-pen-to-square me-1"></i>Why this decision? <span class="text-muted">(optional — reasoning, or what you are unsure about; travels with the match into your exports)</span>
       </label>
       <input type="text" id="recon-review-note-input" class="form-control form-control-sm"
              maxlength="2000" placeholder="e.g. chose the parish, not the village of the same name — parish register context"
              value="${esc(noteFor(meta.key))}">
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
  const searchInput = card.querySelector('#recon-review-search-q');
  if (searchInput) {
    searchInput.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') { e.preventDefault(); manualSearchCandidates(); }
      e.stopPropagation();   // x/s/n/u are review shortcuts — not while typing a name
    });
  }
  const noteInput = card.querySelector('#recon-review-note-input');
  if (noteInput) {
    // Save on the way out (blur) and on Enter — not per keystroke, which would
    // persist the project on every letter typed.
    const save = () => setNote(meta.key, noteInput.value);
    noteInput.addEventListener('blur', save);
    noteInput.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') { e.preventDefault(); save(); noteInput.blur(); }
      e.stopPropagation();   // the review pane has single-key shortcuts (x/s/n/u)
    });
  }
  card.querySelectorAll('[data-act]').forEach((b) => b.addEventListener('click', () => {
    if (noteInput) setNote(meta.key, noteInput.value);   // don't lose an unsaved note
    reviewAction(b.dataset.act);
  }));
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
  if (project && col != null && project.colConfig && project.colConfig[col] && project.colConfig[col].nsFilter) {
    const f = project.colConfig[col].nsFilter;
    // Repair the incoherent state the old picker could save: sources ticked but the mode left on
    // "all", which silently restricted nothing (the ticks were stored and then ignored by both the
    // query builder and applyNsToCandidates). Ticking sources can only have meant "only these".
    if (f.mode === 'all' && f.namespaces && f.namespaces.length) return { mode: 'only', namespaces: f.namespaces };
    return f;
  }
  return { mode: 'all', namespaces: [] };
}
// Per-column AAT place types. A hierarchy reconciles County → Parish → Place, and those levels want
// DIFFERENT types: scoping the whole dataset to "settlements" makes the county column unmatchable, and
// switching the one dataset-wide type mid-chain reset every column's work (place#184). A column with no
// types of its own falls back to the dataset-wide Scope → What selection.
function getColTypes(col) {
  const c = project && col != null && project.colConfig && project.colConfig[col];
  const t = c && c.types;
  return (t && t.ids && t.ids.length) ? t : null;
}
function colTypeSelection(col) {
  const c = project && col != null && project.colConfig && project.colConfig[col];
  return ((c && c.types && c.types.selected) || []).map((t) => ({ id: t.id, text: t.text }));
}
// Which columns carry their own types — for the Scope button summary and the reset decision.
function colsWithOwnTypes() {
  if (!project || !project.colConfig) return [];
  return Object.keys(project.colConfig)
    .filter((k) => getColTypes(Number(k)))
    .map(Number)
    .filter((c) => reconChain().indexOf(c) >= 0);
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
  // Keep the two controls honest. Ticking a source while the mode is still "all" used to save a
  // filter that restricted nothing, so ticking now promotes the mode to "only"; conversely choosing
  // "all" clears the ticks, since they'd have no meaning.
  if (box) box.querySelectorAll('.recon-ns-cb').forEach((cb) => cb.addEventListener('change', () => {
    if (!cb.checked) return;
    const cur = document.querySelector('input[name="recon-ns-mode"]:checked');
    if (!cur || cur.value === 'all') {
      const only = document.querySelector('input[name="recon-ns-mode"][value="only"]');
      if (only) only.checked = true;
    }
  }));
  document.querySelectorAll('input[name="recon-ns-mode"]').forEach((r) => r.addEventListener('change', () => {
    if (r.checked && r.value === 'all' && box) box.querySelectorAll('.recon-ns-cb').forEach((cb) => { cb.checked = false; });
  }));
}
function applyNsFilter() {
  let mode = (document.querySelector('input[name="recon-ns-mode"]:checked') || {}).value || 'all';
  let namespaces = [...document.querySelectorAll('.recon-ns-cb:checked')].map((c) => c.value);
  // Keep mode and selection coherent, so a saved filter can never be a silent no-op:
  //   "all" + ticked sources  → the ticks are meaningless; drop them.
  //   "only"/"prioritise" with nothing ticked → would match nothing; fall back to "all".
  if (mode === 'all') namespaces = [];
  else if (!namespaces.length) mode = 'all';
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
  // Restricting sources can't be applied to matches already in hand: the earlier, unrestricted query
  // returned the top candidates across ALL gazetteers, so the sources you've just chosen may not
  // appear in it at all. Only a re-run actually asks the gateway for those sources. Say so, rather
  // than leaving the old cross-gazetteer candidates on screen looking like the filter was ignored.
  if (mode === 'only' && project && col >= 0 && colHasMatches(col)) {
    reconStaleNote = 'Sources changed — re-reconcile this column to search only the sources you chose '
      + '(the matches below came from the earlier, unrestricted search).';
  }
  if (project) persist();
  if (project && project.matches) { const built = buildUniqueQueries(); if (built) renderResults(built); }
  renderColSwitcher(); updateReconButton();
}

// ── Dataset-wide Scope picker (country / date / feature-type / region) ────────
// Staged region selections that aren't plain form fields (the WHG place and the drawn geometry) live
// here while the modal is open; they're committed to project.scope only on Apply.
let _scopeDraft = { whgPlace: null, geometry: null }; // AAT type selection lives in the scopeAat picker

function parseCcodes(text) {
  return [...new Set(String(text || '').toUpperCase().match(/[A-Z]{2}/g) || [])];
}
// ── Country-code picker (Where → Country codes): validated, typeahead + removable badges ───────────
// Backed by window.ccode_hash (code → {gnlabel}), lazy-loaded from /static/js/parents.js. Draft codes
// live in _scopeCcodes while the modal is open; committed to scope.region.ccodes on Apply.
let _scopeCcodes = [];
let _ccodeList = null; // [{code, name}] cache, built once the hash is loaded
let _ccodeActive = -1; // highlighted menu index for keyboard nav
function ensureCcodeHash() {
  if (window.ccode_hash) return Promise.resolve(window.ccode_hash);
  if (ensureCcodeHash._p) return ensureCcodeHash._p;
  ensureCcodeHash._p = new Promise((resolve) => {
    const s = document.createElement('script');
    s.src = '/static/js/parents.js';
    s.onload = () => resolve(window.ccode_hash || {});
    s.onerror = () => resolve({});
    document.head.appendChild(s);
  });
  return ensureCcodeHash._p;
}
function ccodeName(code) { const h = window.ccode_hash || {}; return (h[code] && h[code].gnlabel) || code; }
function isKnownCcode(code) { return !!(window.ccode_hash && window.ccode_hash[code]); }
function countryList() {
  if (_ccodeList) return _ccodeList;
  const h = window.ccode_hash; if (!h) return [];
  _ccodeList = Object.keys(h)
    .filter((c) => /^[A-Z]{2}$/.test(c) && h[c] && h[c].gnlabel && h[c].gnlabel !== 'unspecified')
    .map((c) => ({ code: c, name: h[c].gnlabel.trim() }))
    .sort((a, b) => a.name.localeCompare(b.name));
  return _ccodeList;
}
function renderScopeCcodeBadges() {
  const box = el('recon-scope-ccode-badges'); if (!box) return;
  box.innerHTML = _scopeCcodes.map((c) => {
    const known = isKnownCcode(c);
    return `<span class="recon-ccode-badge${known ? '' : ' recon-ccode-badge-bad'}" title="${esc(known ? ccodeName(c) : 'Unknown country code')}">` +
      `${esc(c)}<button type="button" class="recon-ccode-badge-x" data-code="${esc(c)}" aria-label="remove">×</button></span>`;
  }).join('');
  box.querySelectorAll('.recon-ccode-badge-x').forEach((b) => b.addEventListener('click', () => removeScopeCcode(b.dataset.code)));
}
function addScopeCcode(code) {
  const c = String(code || '').toUpperCase().trim();
  if (!/^[A-Z]{2}$/.test(c) || _scopeCcodes.includes(c)) return;
  _scopeCcodes.push(c);
  renderScopeCcodeBadges();
}
function removeScopeCcode(code) { _scopeCcodes = _scopeCcodes.filter((c) => c !== code); renderScopeCcodeBadges(); }
function hideCcodeMenu() { const m = el('recon-scope-ccode-menu'); if (m) { m.classList.add('d-none'); m.innerHTML = ''; } _ccodeActive = -1; }
function ccodeMatches(q) {
  const list = countryList(); const s = q.trim().toLowerCase(); if (!s) return [];
  const starts = [], contains = [];
  for (const it of list) {
    if (_scopeCcodes.includes(it.code)) continue;
    const n = it.name.toLowerCase();
    if (it.code.toLowerCase() === s || n.startsWith(s)) starts.push(it);
    else if (n.includes(s)) contains.push(it);
    if (starts.length >= 8) break;
  }
  return starts.concat(contains).slice(0, 8);
}
function renderCcodeMenu(items) {
  const m = el('recon-scope-ccode-menu'); if (!m) return;
  if (!items.length) { hideCcodeMenu(); return; }
  m.innerHTML = items.map((it, i) =>
    `<button type="button" class="recon-ccode-opt${i === _ccodeActive ? ' active' : ''}" data-code="${esc(it.code)}">` +
    `<span class="recon-ccode-opt-code">${esc(it.code)}</span> ${esc(it.name)}</button>`).join('');
  m.classList.remove('d-none');
  m.querySelectorAll('.recon-ccode-opt').forEach((b) => b.addEventListener('mousedown', (e) => {
    e.preventDefault(); addScopeCcode(b.dataset.code); const inp = el('recon-scope-ccode-input'); if (inp) inp.value = ''; hideCcodeMenu(); if (inp) inp.focus();
  }));
}
function onCcodeInput() {
  const inp = el('recon-scope-ccode-input'); if (!inp) return;
  ensureCcodeHash().then(() => { _ccodeActive = -1; renderCcodeMenu(ccodeMatches(inp.value)); });
}
function onCcodeKeydown(e) {
  const inp = el('recon-scope-ccode-input'); if (!inp) return;
  const items = ccodeMatches(inp.value);
  if (e.key === 'ArrowDown') { e.preventDefault(); if (items.length) { _ccodeActive = Math.min(_ccodeActive + 1, items.length - 1); renderCcodeMenu(items); } return; }
  if (e.key === 'ArrowUp') { e.preventDefault(); if (items.length) { _ccodeActive = Math.max(_ccodeActive - 1, 0); renderCcodeMenu(items); } return; }
  if (e.key === 'Escape') { hideCcodeMenu(); return; }
  if (e.key === 'Backspace' && inp.value === '' && _scopeCcodes.length) { removeScopeCcode(_scopeCcodes[_scopeCcodes.length - 1]); return; }
  if (e.key === 'Enter' || e.key === ',' || e.key === ' ') {
    // Commit: a highlighted suggestion wins; else a raw valid 2-letter code; else the first suggestion.
    const raw = inp.value.toUpperCase().trim();
    const pick = _ccodeActive >= 0 ? items[_ccodeActive] : null;
    let code = null;
    if (pick) code = pick.code;
    else if (/^[A-Z]{2}$/.test(raw)) code = raw;
    else if (items.length) code = items[0].code;
    if (code) { e.preventDefault(); addScopeCcode(code); inp.value = ''; hideCcodeMenu(); }
  }
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
// ── Scope → What: which level the picked types apply to (place#184) ──────────
// One tree widget, several targets: the dataset default plus every column in the reconciliation
// chain. The active target's selection is held in the widget; the others live in this draft until
// Apply. Without it there was a single dataset-wide type, so a chain could only ever be scoped to
// one kind of place — and changing it reset every column that had already been reconciled.
let _scopeTypeTarget = 'default';
let _scopeTypeDraft = { default: [], cols: {} };

function seedScopeTypeDraft() {
  const s = project ? (project.scope || defaultScope()) : defaultScope();
  _scopeTypeDraft = { default: ((s.types && s.types.selected) || []).map((t) => ({ id: t.id, text: t.text })), cols: {} };
  reconChain().forEach((c) => { _scopeTypeDraft.cols[c] = colTypeSelection(c); });
  _scopeTypeTarget = 'default';
}
function scopeTypeDraftFor(target) {
  return target === 'default' ? _scopeTypeDraft.default : (_scopeTypeDraft.cols[target] || []);
}
// Move the widget's current selection into whichever target is active — called before switching
// targets and before Apply, so an edit is never lost to a click on another pill.
function stashActiveScopeTypes() {
  const sel = scopeAat.getSelection();
  if (_scopeTypeTarget === 'default') _scopeTypeDraft.default = sel;
  else _scopeTypeDraft.cols[_scopeTypeTarget] = sel;
}
function renderScopeTypeTargets() {
  const box = el('recon-scope-aat-targets');
  const hint = el('recon-scope-aat-target-hint');
  if (!box) return;
  const chain = reconChain();
  // With a single column there is no "level" to distinguish — the dataset default IS that column.
  if (chain.length < 2) { box.classList.add('d-none'); if (hint) hint.classList.add('d-none'); return; }
  box.classList.remove('d-none');
  const targets = [{ key: 'default', label: 'All levels' }]
    .concat(chain.map((c) => ({ key: String(c), label: truncateText(project.columns[c].name, 18) })));
  box.innerHTML = '<span class="small text-muted me-1">Types for:</span>' + targets.map((t) => {
    const active = String(_scopeTypeTarget) === t.key;
    const n = scopeTypeDraftFor(t.key === 'default' ? 'default' : Number(t.key)).length;
    const badge = n ? `<span class="badge bg-secondary ms-1">${n}</span>` : '';
    return `<button type="button" class="btn btn-sm ${active ? 'btn-secondary' : 'btn-outline-secondary'} me-1 mb-1" data-scope-type-target="${esc(t.key)}">${esc(t.label)}${badge}</button>`;
  }).join('');
  box.querySelectorAll('[data-scope-type-target]').forEach((b) => b.addEventListener('click', () => {
    stashActiveScopeTypes();
    const key = b.dataset.scopeTypeTarget;
    _scopeTypeTarget = key === 'default' ? 'default' : Number(key);
    scopeAat.reset(scopeTypeDraftFor(_scopeTypeTarget));
    renderScopeTypeTargets();
  }));
  if (hint) {
    hint.classList.remove('d-none');
    hint.textContent = _scopeTypeTarget === 'default'
      ? 'Applies to every column that has no types of its own — a column you set separately keeps its own.'
      : `Applies to the “${project.columns[_scopeTypeTarget].name}” column only. Changing it re-runs that column and the ones below it, leaving the rest as they are.`;
  }
}

function populateScopeModal() {
  const s = project ? (project.scope || defaultScope()) : defaultScope();
  const r = s.region || { mode: 'none' };
  _scopeDraft = { whgPlace: r.place ? Object.assign({}, r.place) : null, geometry: r.geometry || null };
  _scopeDrawing = false;
  // Region mode radio
  const modeInput = document.querySelector(`input[name="recon-scope-region-mode"][value="${r.mode || 'none'}"]`);
  if (modeInput) modeInput.checked = true;
  _scopeCcodes = (r.ccodes || []).map((c) => String(c).toUpperCase());
  const cci = el('recon-scope-ccode-input'); if (cci) cci.value = '';
  hideCcodeMenu();
  ensureCcodeHash().then(renderScopeCcodeBadges); // labels/validity once the hash is available
  renderScopeCcodeBadges();
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
  seedScopeTypeDraft();
  scopeAat.reset(scopeTypeDraftFor('default'));
  renderScopeTypeTargets();
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
// Bounding box (GeoJSON Polygon) of the dataset's own coordinates — the geographic scope of the data.
// Used as the spatial constraint for PeriodO suggestions (the gateway needs one for a name-less query).
function datasetBBoxHint() {
  if (!project || !hasCoordRole() || !Coords) return null;
  let minLon = Infinity, minLat = Infinity, maxLon = -Infinity, maxLat = -Infinity, n = 0, seen = 0;
  for (let i = 0; i < project.rows.length; i++) {
    const c = rowCoordValue(i);
    if (!c || !isFinite(c.lon) || !isFinite(c.lat) || Math.abs(c.lat) > 90 || Math.abs(c.lon) > 180) continue;
    if (c.lon < minLon) minLon = c.lon; if (c.lon > maxLon) maxLon = c.lon;
    if (c.lat < minLat) minLat = c.lat; if (c.lat > maxLat) maxLat = c.lat;
    seen += 1; if (++n >= 800) break;
  }
  if (!seen || !isFinite(minLon)) return null;
  const padLon = Math.max((maxLon - minLon) * 0.05, 0.1), padLat = Math.max((maxLat - minLat) * 0.05, 0.1);
  minLon -= padLon; maxLon += padLon; minLat = Math.max(minLat - padLat, -90); maxLat = Math.min(maxLat + padLat, 90);
  return { type: 'Polygon', coordinates: [[[minLon, minLat], [maxLon, minLat], [maxLon, maxLat], [minLon, maxLat], [minLon, minLat]]] };
}
// The spatial constraint for period suggestions: an explicit scope region wins, else the data's bbox.
function scopeSpatialParams() {
  const s = getScope(); const r = (s && s.region) || {};
  if (r.mode === 'draw' && r.geometry) return { bounds: r.geometry };
  if (r.mode === 'whg' && r.place && r.place.id) return { contained_in: barePlaceId(r.place.id) };
  const bbox = datasetBBoxHint();
  if (bbox) return { bounds: bbox };
  return {};
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
  // The chosen period IS the dataset's temporal scope, so set the From/To years from its bounds
  // (overwrite — a later pick supersedes an earlier one; the years drive the reconcile date filter).
  const st = el('recon-scope-start'), en = el('recon-scope-end');
  if (st && p.start != null) st.value = p.start;
  if (en && p.stop != null) en.value = p.stop;
  renderScopePeriods();
}
function periodHitButton(p) {
  const span = fmtSpan(p.start, p.stop);
  // Geographic cue: structured ccodes when populated, else the free-text coverage description
  // (the latter is what's available until Period.ccodes is synced from the enrichment pipeline).
  const geo = (p.ccodes && p.ccodes.length)
    ? ` <span class="recon-cand-ns ms-1">${esc(p.ccodes.slice(0, 4).join(' '))}</span>`
    : (p.coverage ? ` <span class="text-muted small fst-italic">${esc(truncate(p.coverage, 28))}</span>` : '');
  return `<button type="button" class="btn btn-sm btn-outline-secondary text-start d-block w-100 mb-1 recon-period-hit" ` +
    `data-id="${esc(p.id)}" data-uri="${esc(p.uri || '')}" data-label="${esc(p.label)}" data-start="${p.start == null ? '' : p.start}" data-stop="${p.stop == null ? '' : p.stop}">` +
    `${esc(truncate(p.label, 40))}${span ? ` <span class="text-muted small">${esc(span)}</span>` : ''}${geo}</button>`;
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
// Add the data's geo/temporal scope to a period-suggest query. `spatial` toggles sending the spatial
// constraint (required for a name-less suggest; optional-but-useful for ranking a name search).
function addPeriodScopeParams(params, spatial) {
  const cc = datasetCcodesHint(); const t = datasetTemporalHint();
  if (cc.length) params.set('ccodes', cc.join(','));
  if (t.start != null) params.set('start', t.start);
  if (t.end != null) params.set('end', t.end);
  if (spatial) {
    const sp = scopeSpatialParams();
    if (sp.bounds) params.set('bounds', JSON.stringify(sp.bounds));
    else if (sp.contained_in) params.set('contained_in', sp.contained_in);
    return sp;
  }
  return {};
}
async function loadPeriodSuggestions() {
  const box = el('recon-scope-period-suggest'); if (!box) return;
  const t = datasetTemporalHint();
  const params = new URLSearchParams(); params.set('limit', '8');
  const sp = addPeriodScopeParams(params, true);
  // PeriodO lives in the ES gateway (namespace `po`); a name-less suggest needs a spatial constraint.
  if (!sp.bounds && !sp.contained_in) {
    box.innerHTML = `<div class="text-muted small">Add coordinates or set a region (Where above) for suggestions tailored to your data — or search by name / pick a common period below.</div>${periodSeedHtml()}`;
    bindPeriodSeeds(box); return;
  }
  const where = sp.contained_in ? 'your region' : "your data's area";
  const label = (t.start != null || t.end != null) ? `${where} · ${fmtSpan(t.start, t.end)}` : where;
  box.innerHTML = `<div class="text-muted small"><i class="fas fa-spinner fa-spin me-1"></i>suggesting periods for ${esc(label)}…</div>`;
  try {
    const data = await fetchJson(`/reconcile/periods/suggest?${params.toString()}`);
    const hits = (data && data.result) || [];
    box.innerHTML = (hits.length
      ? `<div class="text-muted small mb-1">Suggested for <span class="fst-italic">${esc(label)}</span>:</div>` + hits.map(periodHitButton).join('')
      : `<div class="text-muted small">No PeriodO periods matched your data's scope — try a search or a common period.</div>`) + periodSeedHtml();
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
  // Pure name search — no spatial/ccodes/temporal, which the gateway would apply as hard filters and
  // could drop a legitimately-named period outside the data's area/time.
  const params = new URLSearchParams(); params.set('q', q.trim()); params.set('limit', '12');
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
  // The shared TypeTreeWidget (typeTreeWidget.js) is the single source of truth —
  // search + browse + tri-state selection all live in it (place#134 retires the
  // inline duplicate tree that used to live here). The chip list below is a view
  // of the widget's selection; `labelFor` remembers labels for stored ids whose
  // tree branch hasn't been expanded, so chips + the scope summary stay populated.
  let widget = null;
  let pending = [];        // selection [{id, text}] set before the widget mounts
  const labelFor = {};     // id → label, for chips/summary when a node isn't rendered
  const notify = () => { if (opts.onChange) opts.onChange(getSelection()); };

  function currentConcepts() {
    return (widget && widget._initialised) ? widget.getSelectedConcepts(labelFor) : pending.slice();
  }
  function renderSelected() {
    const box = el(ids.selected); if (!box) return;
    const sel = currentConcepts();
    if (!sel.length) { box.innerHTML = `<span class="text-muted small">${esc(opts.emptyText || 'None selected.')}</span>`; return; }
    box.innerHTML = sel.map((t) => {
      const label = t.text || labelFor[t.id] || aatLabel(t.id) || t.id;
      return `<span class="recon-aat-chip" title="${esc(t.id)}">${esc(truncateText(label, 30))}` +
        `<button type="button" class="recon-aat-chip-x" data-id="${esc(t.id)}" title="remove" aria-label="remove">×</button></span>`;
    }).join(' ');
    box.querySelectorAll('.recon-aat-chip-x').forEach((b) => b.addEventListener('click', () => {
      if (widget && widget._initialised) widget.deselect(b.dataset.id);
      else { pending = pending.filter((t) => t.id !== b.dataset.id); }
      renderSelected(); notify();
    }));
  }
  function ensureWidget() {
    if (widget) return widget;
    const mount = el(ids.tree); if (!mount) return null;
    widget = new TypeTreeWidget('#' + ids.tree, {
      onchange: () => {
        // Keep the label cache warm so chips survive a later collapse/reset.
        currentConcepts().forEach((t) => { if (t.text) labelFor[t.id] = t.text; });
        renderSelected(); notify();
      },
    });
    // Seed the selection before init() so it's held in _pending and applied as the
    // tree renders (chips never flicker empty during the initial load).
    if (pending.length) widget.setSelected(pending);
    widget.init();
    return widget;
  }
  function reset(newSelection) {
    pending = (newSelection || []).map((t) => ({ id: t.id, text: t.text }));
    pending.forEach((t) => { if (t.text) labelFor[t.id] = t.text; });
    if (widget && widget._initialised) widget.setSelected(pending);
    renderSelected();
  }
  function getSelection() { return currentConcepts().map((t) => ({ id: t.id, text: t.text })); }
  function init() {
    // Mount lazily when the "browse" <details> first opens (keeps first paint
    // cheap); if there's no <details> wrapper, mount immediately.
    const browse = el(ids.browse);
    if (browse) {
      if (browse.open) ensureWidget();
      browse.addEventListener('toggle', () => { if (browse.open) ensureWidget(); });
    } else ensureWidget();
    renderSelected();
  }
  return { init, reset, getSelection };
}

// Scope-filter instance (in the Scope modal's "What" section).
const scopeAat = createAatPicker(
  { selected: 'recon-scope-aat-selected', tree: 'recon-scope-aat-tree', browse: 'recon-scope-aat-browse' },
  { emptyText: 'No place types selected — any type is allowed.' },
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
  { selected: 'recon-tm-aat-selected', tree: 'recon-tm-aat-tree', browse: 'recon-tm-aat-browse' },
  { emptyText: 'No type assigned to this value yet.' },
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
  if (sel.length) trackOnce('MyD: place type assigned');
}

// Read the modal into a fresh scope object, commit it, and (if it changed) reset existing matches so
// the dataset is reconciled again under the new scope. Async because selected AAT types are expanded to
// their descendants server-side before being stored.
async function applyScope() {
  if (!project) return;
  const mode = (document.querySelector('input[name="recon-scope-region-mode"]:checked') || {}).value || 'none';
  const scope = defaultScope();
  scope.region.mode = mode;
  if (mode === 'ccodes') scope.region.ccodes = _scopeCcodes.slice();
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
  // AAT types: keep the picked concepts for display, expand to descendant ids for the query. Each
  // target (the dataset default and any per-column selection) expands separately — expansion is what
  // the query actually filters on, since types.identifier is an exact match.
  stashActiveScopeTypes();
  const expand = async (selected) => {
    if (!selected.length) return [];
    try {
      const data = await fetchJson(`/types/expand/?ids=${encodeURIComponent(selected.map((t) => t.id).join(','))}`);
      return (data && data.ids) || [];
    } catch (err) { console.error('[recon] type expansion failed; using selected ids only', err); return selected.map((t) => t.id); }
  };
  const selected = _scopeTypeDraft.default;
  scope.types = { selected, ids: await expand(selected) };

  // Per-column types: which columns changed decides what gets re-reconciled.
  const changedCols = [];
  project.colConfig = project.colConfig || {};
  for (const col of reconChain()) {
    const sel = (_scopeTypeDraft.cols[col] || []);
    const beforeSel = JSON.stringify(colTypeSelection(col));
    if (beforeSel === JSON.stringify(sel)) continue;
    const cfg = project.colConfig[col] = project.colConfig[col] || {};
    if (sel.length) cfg.types = { selected: sel, ids: await expand(sel) };
    else delete cfg.types;
    changedCols.push(col);
  }

  const before = JSON.stringify(project.scope || defaultScope());
  const after = JSON.stringify(scope);
  project.scope = scope;
  if (before !== after && invalidateAllMatches()) {
    reconStaleNote = 'Scope changed — reconciliation was reset; reconcile the columns again with the new scope.';
    setReconSummary('<span class="text-warning"><i class="fas fa-triangle-exclamation me-1"></i>Scope changed — reconcile again to apply it.</span>');
  } else if (changedCols.length) {
    // Only the levels whose types changed are re-run (and whatever sits below them, whose containment
    // came from those matches). The whole point of per-level types is that setting the Place column's
    // type doesn't throw away the counties you already confirmed (place#184).
    changedCols.forEach((c) => {
      const k = String(c) + ':';
      if (project.matches) for (const key in project.matches) { if (key.startsWith(k)) delete project.matches[key]; }
      if (project.decisions) for (const key in project.decisions) { if (key.startsWith(k)) delete project.decisions[key]; }
      invalidateDownstream(c);
    });
    // DERIVE the focus rather than pinning it to the changed column: clearing a column's matches can
    // LOCK the ones below it (their containment is gone), and a locked column can't be acted on — its
    // switcher pill is disabled, so pinning the focus there left the Sources and Re-reconcile controls
    // aimed at a column the user could neither configure nor move away from. Deriving lands on the
    // column you'd act on next, which is the one being asked to reconcile again.
    reconActiveIdx = -1;
    const names = changedCols.map((c) => project.columns[c].name).join(', ');
    reconStaleNote = `Place types changed for ${names} — reconcile that column again (levels above it keep their matches).`;
    setReconSummary(`<span class="text-warning"><i class="fas fa-triangle-exclamation me-1"></i>Place types changed for <strong>${esc(names)}</strong> — reconcile again to apply them.</span>`);
  }
  persist();
  updateScopeLabel();
  refreshReconSection();
  if (scopeActive()) {
    const r = scope.region || {};
    track('MyD: scope applied', {
      where: ((r.mode === 'ccodes' && r.ccodes.length) || (r.mode === 'whg' && r.place) || (r.mode === 'draw' && r.geometry)) ? 'yes' : 'no',
      when: (scope.start != null || scope.end != null || (scope.periods && scope.periods.length)) ? 'yes' : 'no',
      what: (scope.types && scope.types.selected && scope.types.selected.length) ? 'yes' : 'no',
      period: (scope.periods && scope.periods.length) ? 'yes' : 'no',
    });
  }
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
  // Propagates to every row merged with this one (admin columns) — candidate indices are shared,
  // because the merged rows were fanned the same candidate list. See setDecision / mergeSig.
  if (list.length) setDecision(meta.key, { status: 'accepted', accepted: list });
  else setDecision(meta.key, null); // unselected the last → back to undecided
  _lastDecisionKey = list.length ? meta.key : null;
  afterDecision(false); // don't auto-advance; multi-select stays on the card
}
function reviewAction(act) {
  if (act === 'next') return advance(1);
  if (act === 'prev') return advance(-1);
  if (act === 'more') return loadMoreCandidates();
  if (act === 'search') return manualSearchCandidates();
  if (act === 'unflag') {
    const mk = reviewMeta[reviewPos];
    if (mk) { toggleFlag(mk.key); persist(); const b = buildUniqueQueries(); if (b) { renderResultsTable(b); renderFilters(); } refreshReview(); }
    return;
  }
  if (act === 'skipall') return skipAllRemaining();
  const meta = reviewMeta[reviewPos]; if (!meta) return;
  if (act === 'undo') {
    // Undo the MOST RECENT decision even though Reject/Skip auto-advanced past it — then jump back to
    // that row so the reversal is visible. Falls back to the current row when nothing was just decided.
    const k = (_lastDecisionKey && project.decisions && project.decisions[_lastDecisionKey]) ? _lastDecisionKey : meta.key;
    const label = keyLabel(k);
    setDecision(k, null);
    _lastDecisionKey = null;
    const idx = reviewMeta.findIndex((r) => r.key === k);
    if (idx >= 0) reviewPos = idx; // return focus to the row we just reverted
    flashSaved(label ? `Undone — “${truncate(label, 30)}” is back to undecided.` : 'Decision undone.');
    return afterDecision(false);
  }
  setDecision(meta.key, { status: act === 'reject' ? 'rejected' : act === 'skip' ? 'skipped' : 'nomatch' });
  _lastDecisionKey = meta.key;
  const verb = act === 'reject' ? 'Rejected' : act === 'skip' ? 'Skipped' : 'Marked “no match”';
  flashSaved(`${verb} “${truncate(meta.name, 30)}” — press Undo (u) to reverse.`);
  afterDecision(true);
}
// The display name for a review key (row value), for undo feedback.
function keyLabel(key) {
  const hit = reviewMeta.find((r) => r.key === key);
  if (hit) return hit.name;
  const ci = key.indexOf(':');
  const col = Number(key.slice(0, ci)), row = Number(key.slice(ci + 1));
  return project.rows[row] ? String(project.rows[row][col] || '') : '';
}
// Skip every value in the ACTIVE column still awaiting review, marking each 'skipped', so the column
// counts as confirmed and the next column in the chain unlocks. Auto-confirmed and already-decided
// rows are untouched; a skipped parent simply supplies no containment to its children (each can still
// be undone individually). Lets you move on to the next column without hand-deciding a long tail. #143
function skipAllRemaining() {
  if (!project) return;
  const built = buildUniqueQueries(); if (!built) return;
  let n = 0;
  // colKeys covers the whole column (not just the filtered/visible queue); setDecision propagates to
  // any rows merged with each key, and once a key is decided needsReview() is false, so siblings and
  // repeats aren't double-counted.
  colKeys(built.colIndex).forEach((k) => { if (needsReview(k)) { setDecision(k, { status: 'skipped' }); n += 1; } });
  if (!n) { flashSaved('Nothing left to review in this column.'); return; }
  reconStaleNote = '';
  reconActiveIdx = -1; // let the stage machine advance focus to the next actionable column
  persist();
  const b2 = buildUniqueQueries(); if (b2) renderResults(b2);
  renderColSwitcher(); updateReconButton(); refreshReview();
  flashSaved(`Skipped ${n.toLocaleString()} unreviewed value${n === 1 ? '' : 's'} — the next column can now be reconciled.`);
}
// Toggle the review flag on a row from the results table (place#202).
function flagForReview(key) {
  if (!project) return;
  const on = toggleFlag(key);
  persist();
  const built = buildUniqueQueries();
  if (built) { renderResultsTable(built); renderFilters(); }
  refreshReview();
  // Deliberately no pane switch: flagging is a marking pass down the results table, and hopping to
  // the review pane on the first click would break the run of them.
  flashSaved(on
    ? `Flagged “${truncate(keyLabel(key), 30)}” for review — it is now in the review queue below.`
    : `Unflagged “${truncate(keyLabel(key), 30)}” — left as auto-confirmed.`);
}

// Write a candidate list to a key AND every row merged with it. Merged rows share one review and one
// decision, and a decision stores candidate INDICES — so a list that grew for one of them must grow
// for all, or an accepted index would point at a different place after a reload (the fanned-out rows
// share one array in memory, but not once the project has been serialised and read back).
function setMatchCandidates(key, candidates, top, exhausted) {
  const ci = key.indexOf(':');
  mergeGroupKeys(Number(key.slice(0, ci)), key.slice(ci + 1)).forEach((k) => {
    const mm = project.matches[k];
    if (!mm) return;
    mm.candidates = clone(candidates);
    mm.top = top === undefined ? (mm.candidates[0] || null) : (top || null);
    if (exhausted !== undefined) mm.exhausted = exhausted;
  });
}

// Search the gazetteers by hand from the review card (place#201). When a row comes back with nothing
// useful the reviewer can usually see why — a spelling the source never used, a name the register
// abbreviated — and typing the name they know is far quicker than editing the data and reconciling
// the whole column again. Results are APPENDED to the candidate list (scores from a different query
// are not comparable with the reconciliation's, so they don't get to re-rank it) and are accepted
// with the same click as any other candidate. The dataset-wide scope is deliberately NOT applied:
// this is the escape hatch for when the automatic query was too narrow.
async function manualSearchCandidates() {
  const meta = reviewMeta[reviewPos]; if (!meta) return;
  const input = el('recon-review-search-q');
  const q = input ? input.value.trim() : '';
  const setNote = (html) => { const n = el('recon-review-search-note'); if (n) n.innerHTML = html; };
  if (!q) { setNote('<span class="text-muted">Type a place name to search for.</span>'); return; }
  setNote('<i class="fas fa-spinner fa-spin me-1"></i>searching…');
  try {
    const revCol = activeReconCol();
    const nsf = getNsFilter(revCol);
    const query = { q0: { query: q, type: 'place', limit: 10 } };
    if (nsf.mode === 'only' && nsf.namespaces.length) query.q0.namespaces = nsf.namespaces;
    const data = await postReconcile(query, getCsrf());
    const found = applyNsToCandidates((data.q0 && data.q0.result) || [], revCol);
    const m = project.matches[meta.key];
    const have = new Set((m.candidates || []).map((c) => c && c.id));
    const fresh = found.filter((c) => c && !have.has(c.id)).map((c) => Object.assign({}, c, { found_by: q }));
    if (!fresh.length) {
      setNote(found.length
        ? `<span class="text-muted">Every result for “${esc(q)}” is already listed above.</span>`
        : `<span class="text-muted">Nothing found for “${esc(q)}”.</span>`);
      return;
    }
    setMatchCandidates(meta.key, (m.candidates || []).concat(fresh), m.top);
    await persist();
    renderReviewCard();
    const box = el('recon-review-search-q'); if (box) box.value = q;
    setNote(`<span class="text-success">Added ${fresh.length} result${fresh.length === 1 ? '' : 's'} for “${esc(q)}” — click one to accept it.</span>`);
  } catch (err) {
    console.error('[recon] manual search failed', err);
    setNote(`<span class="text-danger">Search failed: ${esc(err.message)}</span>`);
  }
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
    // fewer than asked → no more to fetch
    setMatchCandidates(meta.key, result, result[0] || null, result.length < want);
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
  refreshSpatialControls();
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

// Value-bearing units in a column that have no match yet — i.e. rows still to reconcile (e.g. after a
// Cancel, which keeps what was fetched and leaves the rest unqueried). Merged admin values count once.
function unreconciledUnits(col) {
  if (col == null || col < 0 || !project) return 0;
  const built = buildUniqueQueries(col); if (!built) return 0;
  let n = 0; built.map.forEach((v, key) => { if (!project.matches[key]) n += 1; });
  return n;
}
// Show a "Continue reconciling (N)" button when the focused column is PARTIALLY reconciled — some
// matches present, some rows still unqueried — so a cancelled run can be resumed without the
// Re-reconcile button's clear-and-restart. Hidden while running or soft-locked by a teammate.
function updateContinueButton() {
  const b = el('recon-continue'); if (!b || !project) return;
  const col = activeReconCol();
  const rem = (col >= 0 && colHasMatches(col)) ? unreconciledUnits(col) : 0;
  const show = rem > 0 && !running && !_peerReconciling;
  b.classList.toggle('d-none', !show);
  if (show) b.innerHTML = `<i class="fas fa-play me-1"></i>Continue reconciling (${rem.toLocaleString()})`;
}
// Reconcile only the not-yet-matched rows of the focused column (reconcilePass skips rows that already
// have matches), keeping existing matches. The resume companion to Cancel.
async function continueReconcile() {
  if (running || !project) return;
  const chain = reconChain();
  const col = activeReconCol();
  const pos = chain.indexOf(col);
  if (pos < 0) return;
  reconStaleNote = '';
  trackOnce('MyD: reconcile', { columns: String(chain.length) });
  toggleRunning(true); openPane('recon-recon'); stopRequested = false;
  await reconcilePass(col, pos > 0 ? chain[pos - 1] : -1, getCsrf(), pos, chain.length);
  toggleRunning(false);
  reconActiveIdx = pos;
  const built = buildUniqueQueries(); if (built) renderResults(built);
  renderColSwitcher(); updateReconButton(); refreshReview();
  await persist();
}

// Drive the Reconcile button + help text from the current stage: reconcile one column, review it,
// then the next column unlocks.
function updateReconButton() {
  const btn = el('recon-run'); if (!btn || !project) return;
  btn.title = ''; // cleared each render; the review-state branch sets an explanatory tooltip
  const help = el('recon-recon-help');
  const chain = reconChain();
  updateRerunButton(chain);
  updateContinueButton();
  updateSourcesLabel(); // keep the Sources button label pointed at the focused column
  updateScopeLabel();   // reflect any saved dataset-wide scope (e.g. after resume)
  // Advisory lock: a teammate is running a reconcile — soft-block ours (and Re-reconcile) so two
  // members don't fire the same heavy, gateway-hammering pass at once (place#112). Advisory only:
  // awareness clears if they drop, so it can never deadlock. Never blocks our own in-flight run.
  if (_peerReconciling && !running) {
    const who = esc(_peerReconciling.name);
    const oncol = _peerReconciling.column ? ` <strong>${esc(truncate(_peerReconciling.column, 20))}</strong>` : '';
    btn.disabled = true;
    btn.innerHTML = `<i class="fas fa-user-lock me-1"></i>${who} is reconciling…`;
    const rr = el('recon-rerun'); if (rr) rr.disabled = true;
    if (help) help.innerHTML = `${who} is reconciling${oncol} for this team right now. Your Reconcile is paused so the team doesn’t run the same search twice — it re-enables when they finish.`;
    return;
  }
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
    // Why it's disabled, as a hover tooltip (snag #153). Disabled buttons don't fire hover events, so
    // recon.css re-enables pointer-events on this one so the title actually shows.
    btn.title = isLast
      ? `Reconciled — now review and confirm its matches in Step 4 (below).`
      : `Review and confirm this column's matches in Step 4 before the next column can be reconciled.`;
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
  lastScope = null; lastVariantsDropped = 0; lastDerivedForms = new Set(); renderScopeNotice(); // and any previous scope report
  project.matches = project.matches || {};
  trackOnce('MyD: reconcile', { columns: String(chain.length) });
  toggleRunning(true);
  openPane('recon-recon');
  stopRequested = false;
  await reconcilePass(chain[pos], pos > 0 ? chain[pos - 1] : -1, getCsrf(), pos, chain.length);
  toggleRunning(false);
  // The review/results panes stay on the column we just ran, EVEN IF every value auto-confirmed. Focus
  // used to jump straight to the next stage in that case (so the Sources picker pointed at the column
  // you'd run next), but that whisked a fully auto-confirmed column out of sight before its matches
  // could be looked at — the reviewer had to notice, and click back to, its switcher pill. The
  // Reconcile button targets currentStagePos(), not this, so the next column is still one click away
  // whichever column has focus. See place#190.
  reconActiveIdx = pos;
  const built = buildUniqueQueries();
  if (built) renderResults(built);
  renderColSwitcher();
  updateReconButton();
  reviewPos = 0; refreshReview();
  await persist();
  // Finer signal: did this pass actually find candidates? (bucketed key counts, no data)
  try {
    const rc = chain[pos]; let matched = 0, nomatch = 0;
    colKeys(rc).forEach((k) => { const m = project.matches[k]; if (m && m.candidates && m.candidates.length) matched += 1; else nomatch += 1; });
    track('MyD: reconcile result', { column: String(pos + 1), matched: bucketCount(matched), nomatch: bucketCount(nomatch) });
  } catch (_) { /* analytics never breaks the run */ }
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
  // Ancestor columns coarsest→finest above this one, for containment fallback (parent, grandparent, …).
  const ancestorCols = reconChain().slice(0, Math.max(0, passNo)); // chain[0..passNo-1]
  reconActiveIdx = passNo; // show this column's progress/results while it runs
  renderColSwitcher();
  rtSetActivity({ type: 'reconciling', column: project.columns[colIndex] && project.columns[colIndex].name }); // tell teammates (advisory lock)
  const colName = truncate(project.columns[colIndex].name, 30);
  const passLabel = passTotal > 1 ? `<span class="text-muted">column ${passNo + 1}/${passTotal} · <strong>${esc(colName)}</strong></span> · ` : '';
  const nameCol = colIndexByRole('name');
  const perRow = colIndex === nameCol; // place-name column: reconcile every row individually
  const rawEntries = [...built.map.entries()].filter(([key]) => !project.matches[key]); // resume: skip done

  // Reconciliation units. Place names (perRow) → one unit per row (never merged: identical toponyms may
  // be different places the user has already disambiguated). Admin/parent columns → merge identical
  // values so a county appearing in hundreds of rows is reconciled ONCE and fanned to every row — but
  // only within the same containment context (same confirmed parent place(s)) and country, so two
  // same-named places under different parents stay distinct. See issue #143.
  const units = []; // { repKey, memberKeys:[...], v }
  if (perRow) {
    rawEntries.forEach(([key, v]) => units.push({ repKey: key, memberKeys: [key], v }));
  } else {
    const groups = new Map();
    rawEntries.forEach(([key, v]) => {
      const gk = mergeSig(colIndex, key.slice(key.indexOf(':') + 1), parentCol);
      if (gk == null) { units.push({ repKey: key, memberKeys: [key], v }); return; }
      let g = groups.get(gk);
      if (!g) { g = { repKey: key, memberKeys: [], v }; groups.set(gk, g); }
      g.memberKeys.push(key);
    });
    groups.forEach((g) => units.push(g));
  }
  const total = units.length;
  let done = 0;
  updateProgress(done, total);

  // Language-conditioned Symphonym embeddings (int8, 128-d) — one per unit (representative value),
  // plus one per DISTINCT name variant across the pass. Embedding the variants here means the gateway
  // doesn't have to embed them server-side (place#144), and they're deduped so a spelling repeated
  // across rows costs one embed. Primaries occupy [0, units.length), variants follow.
  let embByKey = null;      // unit repKey → vector
  let embByVariant = null;  // variant string → vector
  if (phoneticEnabled() && units.length) {
    try {
      const mod = await loadSymphonym();
      const lang = getLang();
      const names = units.map((u) => u.v.query);
      const variantForms = [];
      if (perRow) {
        const seen = new Set();
        units.forEach((u) => queryVariants(u.repKey.slice(u.repKey.indexOf(':') + 1), u.v.query)
          .forEach((s) => { if (!seen.has(s)) { seen.add(s); variantForms.push(s); } }));
      }
      const int8 = await mod.embedNames(names.concat(variantForms), {
        lang,
        onProgress: (d, t) => setReconSummary(`${passLabel}<i class="fas fa-spinner fa-spin me-1"></i>embeddings ${d.toLocaleString()} / ${t.toLocaleString()}…`),
      });
      const vecAt = (i) => Array.from(int8.subarray(i * 128, i * 128 + 128));
      embByKey = {};
      units.forEach((u, idx) => { embByKey[u.repKey] = vecAt(idx); });
      if (variantForms.length) {
        embByVariant = {};
        variantForms.forEach((s, i) => { embByVariant[s] = vecAt(units.length + i); });
      }
    } catch (err) { console.error('[recon] embedding failed; using text matching', err); }
  }

  for (let b = 0; b < units.length && !stopRequested; b += RECON_BATCH) {
    const slice = units.slice(b, b + RECON_BATCH);
    const queries = {};
    const nsf = getNsFilter(colIndex); // this column's own source gazetteers
    const sp = spatialSettings();      // coordinate circle + containment strictness
    slice.forEach((u, j) => {
      const key = u.repKey, v = u.v, row = key.slice(key.indexOf(':') + 1);
      const q = { query: v.query, type: 'place', limit: RECON_CAND_LIMIT };
      // The row's coordinate describes the PLACE, not the county or region containing it, so the
      // circle belongs only to the name column. Applied to a container column it asks for a county
      // whose record sits within 10km of a town inside it — which is usually false (Devon's point is
      // ~30km from Exeter's), so the container silently failed to match and containment could then
      // not be applied at all. See place#184.
      const rowCoord = (sp.nearby && hasCoordRole() && colIndex === colIndexByRole('name'))
        ? rowCoordValue(Number(row)) : null;
      if (v.country) q.countries = [v.country];
      if (nsf.mode === 'only' && nsf.namespaces.length) q.namespaces = nsf.namespaces; // restrict sources
      if (embByKey && embByKey[key]) q.embedding = embByKey[key]; // phonetic (vector) matching
      // Name variants (alt_names): alternative spellings tried alongside the primary toponym — only for
      // the place-name column (variants of a container's own value aren't a modelled concept). Their
      // in-browser embeddings ride along positionally, so the gateway can skip the server-side embed;
      // a null entry (or none at all) just means "embed this one yourself".
      if (perRow) {
        const vars = queryVariants(row, v.query);
        if (vars.length) {
          q.variants = vars;
          if (embByVariant) {
            const vecs = vars.map((s) => embByVariant[s] || null);
            if (vecs.some(Boolean)) q.variant_vectors = vecs;
          }
        }
      }
      // Containment: scope this column's query within the confirmed places of the NEAREST resolved
      // ancestor — the direct parent if it matched, else its parent (grandparent), and so on up the
      // chain. Only when NO ancestor resolved for this row does it fall through to the dataset-wide
      // scope below. This keeps precision when an intermediate level (e.g. a parish) was skipped or
      // unmatched but a coarser one (its county) was confirmed — without it, such a row would widen all
      // the way to the whole dataset. "within any of them" (an ancestor may closeMatch several records).
      // `intersects`, not `within`: administrative polygons from DIFFERENT gazetteers almost never nest
      // exactly — a Kain parish boundary is not strictly inside a UKHC county, any more than a UKHC
      // county is strictly inside the modern Wales polygon (measured: zero hits under `within`). Since
      // the gateway enforces containment as a hard filter (place#144), `within` discards valid children
      // over sub-kilometre boundary disagreement. See issue #143.
      let pids = [];
      for (let a = ancestorCols.length - 1; a >= 0 && !pids.length; a--) {
        pids = resolvedPlaceIds(ancestorCols[a], row).map(barePlaceId);
      }
      if (pids.length) { q.contained_in = pids; q.containment = sp.containment; q.relation = sp.relation; }
      // The row's own coordinate as a circular filter. The service converts lat/lng/radius into a
      // bounding polygon and filters on it — so a row that says where it is can no longer match a
      // same-named place on another continent. Sent alongside containment; where both apply the
      // service honours the container, so the radius is also enforced locally on the results.
      if (sp.nearby && rowCoord) { q.lat = +rowCoord.lat.toFixed(6); q.lng = +rowCoord.lon.toFixed(6); q.radius = sp.radiusKm; }
      applyGlobalScopeToQuery(q, parentCol < 0, !!v.country, colIndex); // scope: dataset-wide, or this column's own types
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
    slice.forEach((u, j) => {
      const qd = data['q' + j] || {};
      // Gateway scope/variant reporting (place#144). Scope is dataset-wide, so the first one we see
      // in a run describes the whole run. `undefined` means an older gateway — leave lastScope null.
      if (qd.scope && !lastScope) lastScope = qd.scope;
      // Forms the gateway derived ITSELF — de-bracketing "Broxbourn (St. Augustine)" and the like
      // (place#199). Worth telling the user: it explains a match their value could not have made on
      // its own, and it is not the same thing as the variants they supplied.
      if (Array.isArray(qd.derived_forms)) qd.derived_forms.forEach((f) => { if (f) lastDerivedForms.add(f); });
      // Variants not queried. The user's own dropped alt_names are known here (we do the dedupe and the
      // capping), so they are counted client-side; comparing a raw alt_names count against
      // `variants_used` would now under-report, since that list also carries the derived forms
      // (place#188). Anything the gateway discarded on top of ours is added to the same total.
      if (perRow) {
        const vf = queryVariantForms(u.repKey.slice(u.repKey.indexOf(':') + 1), u.v.query);
        let dropped = vf.userDropped;
        if (Array.isArray(qd.variants_used) && vf.forms.length > qd.variants_used.length) {
          dropped += vf.forms.length - qd.variants_used.length;
        }
        lastVariantsDropped += dropped;
      }
      let result = applyNsToCandidates(qd.result || [], colIndex);
      // Enforce the radius here as well as on the service. Where a row has BOTH a confirmed
      // container and a coordinate the service honours the container and ignores the circle, so
      // without this the radius would silently not apply to exactly the rows that have the most
      // context. Candidates with no known position are KEPT — a place without coordinates cannot be
      // shown to be out of range, and dropping it would lose a valid match to a data gap.
      if (sp.nearby && hasCoordRole() && colIndex === colIndexByRole('name')) {
        const rc = rowCoordValue(Number(u.repKey.slice(u.repKey.indexOf(':') + 1)));
        if (rc) {
          result = result.filter((c) => {
            const p = c && c.repr_point;
            if (!Array.isArray(p) || p.length < 2) return true;
            return haversineKm({ lat: rc.lat, lon: rc.lon }, { lat: p[1], lon: p[0] }) <= sp.radiusKm;
          });
        }
      }
      const at = new Date().toISOString();
      // Fan the single reconciliation to every row sharing this value+containment (admin merge).
      u.memberKeys.forEach((mk) => {
        project.matches[mk] = { candidates: result, top: result[0] || null, exhausted: result.length < RECON_CAND_LIMIT, at };
      });
    });
    renderScopeNotice();
    done += slice.length;
    updateProgress(done, total);
    renderResults(built);
    await persist();
    if (!stopRequested && b + RECON_BATCH < units.length) await sleep(150); // gentle throttle
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

  // Import from a shared Google Sheet URL.
  const gsBtn = el('recon-gsheet-btn');
  if (gsBtn) gsBtn.addEventListener('click', importGoogleSheet);
  const gsUrl = el('recon-gsheet-url');
  if (gsUrl) gsUrl.addEventListener('keydown', (e) => { if (e.key === 'Enter') { e.preventDefault(); importGoogleSheet(); } });

  // Place-name extraction (NER) from pasted / uploaded text.
  const nerBtn = el('recon-ner-btn');
  if (nerBtn) nerBtn.addEventListener('click', extractPlaceNames);
  const nerPaste = el('recon-ner-paste');
  if (nerPaste) nerPaste.addEventListener('click', nerPasteFromClipboard);
  const nerFile = el('recon-ner-file');
  if (nerFile) nerFile.addEventListener('change', (e) => { const f = e.target.files && e.target.files[0]; if (f) nerLoadFile(f); e.target.value = ''; });
  const nerGdocBtn = el('recon-ner-gdoc-btn');
  if (nerGdocBtn) nerGdocBtn.addEventListener('click', importGoogleDoc);
  const nerGdocUrl = el('recon-ner-gdoc-url');
  if (nerGdocUrl) nerGdocUrl.addEventListener('keydown', (e) => { if (e.key === 'Enter') { e.preventDefault(); importGoogleDoc(); } });

  // Load a bundled sample dataset (fetched from a static URL) for demonstration — no file picker.
  const sampleBtn = el('recon-load-sample');
  if (sampleBtn) sampleBtn.addEventListener('click', () => { sampleBtn.disabled = true; loadSampleDataset().finally(() => { sampleBtn.disabled = false; }); });

  // "Take a tour" — guided product tour that drives the whole flow on the sample data. The tour
  // engine is lazy-loaded (its own chunk) so it costs nothing until requested.
  const tourBtn = el('recon-take-tour');
  if (tourBtn) tourBtn.addEventListener('click', async () => {
    tourBtn.disabled = true;
    track('MyD: tour');
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
  const cont = el('recon-continue');
  if (cont) cont.addEventListener('click', continueReconcile);
  const stop = el('recon-stop');
  if (stop) stop.addEventListener('click', () => {
    stopRequested = true;
    stop.disabled = true; stop.innerHTML = '<i class="fas fa-hourglass-half me-1"></i>Finishing current batch…';
    flashSaved('Stopping — matches so far are kept; run again to continue.');
  });

  const thr = el('recon-threshold');
  if (thr) thr.addEventListener('input', () => {
    if (!project) return;
    project.autoThreshold = getThreshold();
    persist();
    const built = buildUniqueQueries();
    if (built && project.matches && Object.keys(project.matches).length) renderResults(built);
    updateReconButton(); // threshold changes which rows auto-confirm → which stage is current
  });

  wireSpatialControls();

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
  const collabBtn = el('recon-collab');
  if (collabBtn) collabBtn.addEventListener('click', openCollabModal);
  const conflictApply = el('recon-conflict-apply');
  if (conflictApply) conflictApply.addEventListener('click', resolveConflicts);
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
  const xlsxSaveBtn = el('recon-xlsx-save');
  if (xlsxSaveBtn) xlsxSaveBtn.addEventListener('click', saveAsExcel);

  // Citation & provenance builder: live preview + persist on edit; add contributors; copy / download.
  CITE_FIELDS.forEach((f) => { const inp = el('cite-' + f); if (inp) inp.addEventListener('input', saveCitation); });
  // Licence: the controlled picker shared with /licenses/ and the Workbench editors, so the value
  // resolves to a licensing.License row and can reach Dataset.license on contribute (place#158).
  const citeLicBtn = el('cite-license-btn');
  if (citeLicBtn) {
    wireLicenseControl({
      button: citeLicBtn,
      display: el('cite-license-display'),
      clearBtn: el('cite-license-clear'),
      getChoice: () => {
        const m = citationModel();
        return m.license ? { spdx: m.license } : null;
      },
      setChoice: (c) => {
        if (!project) return;
        project.citation = Object.assign(citationModel(), { license: (c && c.spdx) || '' });
        const prev = el('cite-preview');
        if (prev) prev.textContent = formatCitation(project.citation);
        persist();
      },
    });
  }
  const citeAdd = el('cite-c-add');
  if (citeAdd) citeAdd.addEventListener('click', addCiteContributor);
  const citeCName = el('cite-c-name');
  if (citeCName) citeCName.addEventListener('keydown', (e) => { if (e.key === 'Enter') { e.preventDefault(); addCiteContributor(); } });
  const citeCopy = el('cite-copy');
  if (citeCopy) citeCopy.addEventListener('click', async () => {
    const text = formatCitation(currentCitation());
    try { await navigator.clipboard.writeText(text); citeFlash('Citation copied'); }
    catch (_) { const t = el('cite-title'); if (t) { t.focus(); } citeFlash('Copy failed — select & copy the preview'); }
  });
  const citeCff = el('cite-cff');
  if (citeCff) citeCff.addEventListener('click', () => { downloadText('CITATION.cff', buildCff(currentCitation()), 'application/x-yaml'); citeFlash('CITATION.cff downloaded'); });
  const citeJsonld = el('cite-jsonld');
  if (citeJsonld) citeJsonld.addEventListener('click', () => { downloadText(citeBaseName() + '.schema.json', buildSchemaOrg(currentCitation()), 'application/ld+json'); citeFlash('schema.org JSON downloaded'); });

  const contribBtn = el('recon-contribute-btn');
  if (contribBtn) contribBtn.addEventListener('click', contributeToWHG);
  // Contribution validation: re-check button, "use Scope type(s)" shortcut, and re-validate when the
  // export options (which change the built LPF) change.
  const recheck = el('recon-validate-recheck');
  // A user-initiated re-check that still fails is a genuine "stuck at contribute" signal (unlike the
  // background validation that runs on every edit, so we only track this explicit click).
  if (recheck) recheck.addEventListener('click', async () => {
    const v = await runValidation();
    if (v && v.schemaOk === false) track('MyD: contribute blocked', { errors: bucketCount(v.schemaErrs) });
  });
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

  // Flagging a match from the results table (place#202). The table is virtualised — its rows are
  // re-rendered on every scroll — so the handler is delegated to the tbody, which is not.
  const resultsBody = el('recon-results-body');
  if (resultsBody) {
    const flagHit = (e) => {
      const t = e.target.closest ? e.target.closest('[data-flag]') : null;
      return t ? t.dataset.flag : null;
    };
    resultsBody.addEventListener('click', (e) => { const k = flagHit(e); if (k) { e.preventDefault(); flagForReview(k); } });
    resultsBody.addEventListener('keydown', (e) => {
      if (e.key !== 'Enter' && e.key !== ' ') return;
      const k = flagHit(e); if (!k) return;
      e.preventDefault(); flagForReview(k);
    });
  }

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
  if (scopeQ) {
    scopeQ.addEventListener('keydown', (e) => { if (e.key === 'Enter') { e.preventDefault(); searchScopeWhg(); } });
    // Typeahead: search-as-you-type (debounced) once ≥3 chars.
    let wtimer = null;
    scopeQ.addEventListener('input', () => { clearTimeout(wtimer); const v = scopeQ.value.trim(); if (v.length < 3) { const r = el('recon-scope-whg-results'); if (r) r.innerHTML = ''; return; } wtimer = setTimeout(searchScopeWhg, 300); });
  }
  // Country-code picker: typeahead + removable badges (Where → Country codes).
  const ccInput = el('recon-scope-ccode-input');
  if (ccInput) {
    ensureCcodeHash();
    ccInput.addEventListener('input', onCcodeInput);
    ccInput.addEventListener('keydown', onCcodeKeydown);
    ccInput.addEventListener('blur', () => setTimeout(hideCcodeMenu, 150));
  }
  const ccBox = el('recon-scope-ccode-box');
  if (ccBox) ccBox.addEventListener('click', () => { const i = el('recon-scope-ccode-input'); if (i) i.focus(); });
  const scopePeriodSearch = el('recon-scope-period-search');
  if (scopePeriodSearch) scopePeriodSearch.addEventListener('click', searchScopePeriods);
  const scopePeriodQ = el('recon-scope-period-q');
  if (scopePeriodQ) {
    scopePeriodQ.addEventListener('keydown', (e) => { if (e.key === 'Enter') { e.preventDefault(); searchScopePeriods(); } });
    // Typeahead: search PeriodO as you type (debounced) once ≥2 chars.
    let ptimer = null;
    scopePeriodQ.addEventListener('input', () => { clearTimeout(ptimer); const v = scopePeriodQ.value.trim(); if (v.length < 2) { const r = el('recon-scope-period-results'); if (r) r.innerHTML = ''; return; } ptimer = setTimeout(searchScopePeriods, 300); });
  }
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
  // "put the result in a new column" — reveals the name box and re-renders the preview (place#194).
  const tfNew = el('recon-tf-newcol');
  if (tfNew) tfNew.addEventListener('change', () => {
    const nn = el('recon-tf-newcolname');
    if (nn) { nn.classList.toggle('d-none', !tfNew.checked); if (tfNew.checked) nn.focus(); }
    renderTransformPreview();
  });
  // Split-into-containment-columns controls (in the same transform modal).
  const sdEl = el('recon-tf-splitdelim'); if (sdEl) sdEl.addEventListener('input', renderSplitPreview);
  const srEl = el('recon-tf-splitrev'); if (srEl) srEl.addEventListener('change', renderSplitPreview);
  const splitBtn = el('recon-tf-splitbtn'); if (splitBtn) splitBtn.addEventListener('click', applySplit);
  const scopeApply = el('recon-scope-apply');
  if (scopeApply) scopeApply.addEventListener('click', applyScope);
  const scopeClear = el('recon-scope-clear');
  if (scopeClear) scopeClear.addEventListener('click', () => {
    _scopeCcodes = []; renderScopeCcodeBadges(); hideCcodeMenu();
    const cci = el('recon-scope-ccode-input'); if (cci) cci.value = '';
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
  wireChat(); // team-chat panel (place#154); the toggle stays hidden until a live team project connects
  // A ?shared=<token> link opens a read-only copy someone shared; a ?open=<id> link (e.g. from a team
  // invitation email) opens that saved team project directly; otherwise resume local work.
  const sp = new URLSearchParams(window.location.search);
  const sharedToken = sp.get('shared');
  const openId = sp.get('open');
  if (sharedToken) {
    try { window.history.replaceState({}, '', window.location.pathname); } catch (_) { /* ignore */ }
    handleSharedBootstrap(sharedToken);
  } else if (openId) {
    try { window.history.replaceState({}, '', window.location.pathname); } catch (_) { /* ignore */ }
    loadSaved().then(() => openServerProject(openId));
  } else {
    loadSaved();
  }
}

if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
else init();
