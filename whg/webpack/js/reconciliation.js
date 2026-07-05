// reconciliation.js
// Gazetteer Workbench — browser-based, local-first Reconciliation UI (Phase 1 skeleton).
// STAFF-ONLY, UNPUBLISHED preview. See WorldHistoricalGazetteer/place#111 (spec), #112 (collaboration),
// and developer/plan-gazetteerWorkbench.prompt.md (build order).
//
// This first increment proves the local-first premise: a user picks a file and we parse its header
// and first rows *entirely in the browser* to preview columns and guess their roles. No network, no
// server-side persistence. Later phases add IndexedDB/OPFS storage, the reconciliation queue against
// WHG's standard /reconcile service, candidate review, enrichment, and selective submission.

import '../css/reconciliation.css';

const PREVIEW_ROWS = 20;

// Naive role detection — synonym/regex hints, mirroring the schema-detection step in the plan (§Phase 1).
// Deliberately conservative: this only *suggests* a role for the preview; the real column-mapping UI
// (with user confirmation and CRS handling) is a later phase.
const ROLE_HINTS = [
  ['name', /^(place|placename|name|toponym|title|label)s?$/i],
  ['county', /^(county|adm2|admin2|region|parish|province|state|district)$/i],
  ['country', /^(country|ccode|iso|nation)$/i],
  ['type', /^(type|feature.?type|fclass|category|placetype|kind)$/i],
  ['lat', /^(lat|latitude|y)$/i],
  ['lon', /^(lon|lng|long|longitude|x)$/i],
  ['coords', /^(coord|coordinates|geom|geometry|wkt|point|gridref|grid.?ref|osgb|national.?grid)$/i],
  ['date', /^(date|year|start|end|from|to|period|century)$/i],
  ['id', /^(id|uid|key|identifier|wikidata|qid|geonames|gn.?id)$/i],
];

function detectRole(columnName) {
  const n = String(columnName || '').trim();
  for (const [role, re] of ROLE_HINTS) {
    if (re.test(n)) return role;
  }
  return 'other';
}

// Minimal RFC-4180-ish delimited parser: handles quoted fields, escaped quotes ("") and newlines
// inside quotes. Good enough for a preview; the production import uses a streaming Web Worker parser.
function parseDelimited(text, delimiter) {
  const rows = [];
  let field = '';
  let row = [];
  let inQuotes = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (inQuotes) {
      if (c === '"') {
        if (text[i + 1] === '"') { field += '"'; i++; }
        else { inQuotes = false; }
      } else { field += c; }
    } else if (c === '"') {
      inQuotes = true;
    } else if (c === delimiter) {
      row.push(field); field = '';
    } else if (c === '\n') {
      row.push(field); field = '';
      rows.push(row); row = [];
    } else if (c === '\r') {
      // swallow — handled by the \n branch
    } else {
      field += c;
    }
  }
  if (field.length > 0 || row.length > 0) { row.push(field); rows.push(row); }
  return rows.filter((r) => r.length > 1 || (r.length === 1 && r[0] !== ''));
}

function guessDelimiter(sample) {
  const firstLine = sample.split('\n')[0] || '';
  const tabs = (firstLine.match(/\t/g) || []).length;
  const commas = (firstLine.match(/,/g) || []).length;
  return tabs > commas ? '\t' : ',';
}

// Normalise a parsed JSON payload to {columns, rows}. Accepts an array of flat objects, or the
// WHG "user example" shape [{id, fields:{...}}] (e.g. developer/user_examples/Places.json).
function fromJSON(data) {
  let records = Array.isArray(data) ? data : (Array.isArray(data.features) ? data.features : null);
  if (!records || !records.length) throw new Error('Expected a non-empty JSON array of records.');
  const flat = records.map((rec) => {
    if (rec && typeof rec === 'object' && rec.fields && typeof rec.fields === 'object') {
      return Object.assign({ id: rec.id }, rec.fields); // {id, fields:{...}} shape
    }
    if (rec && rec.properties && typeof rec.properties === 'object') {
      return rec.properties; // GeoJSON-ish
    }
    return rec;
  });
  const columns = [];
  const seen = new Set();
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
  const rows = matrix.slice(1);
  return { columns, rows, total: rows.length, delimiter };
}

function el(id) { return document.getElementById(id); }

function truncate(value, max = 80) {
  const s = String(value == null ? '' : value);
  return s.length > max ? s.slice(0, max - 1) + '…' : s;
}

function renderPreview(parsed) {
  const { columns, rows, total } = parsed;
  el('recon-result').classList.remove('d-none');

  const delimNote = parsed.delimiter
    ? ` · delimiter <code>${parsed.delimiter === '\t' ? 'TAB' : parsed.delimiter}</code>`
    : ' · JSON';
  el('recon-summary').innerHTML =
    `<strong>${total.toLocaleString()}</strong> row${total === 1 ? '' : 's'} · ` +
    `<strong>${columns.length}</strong> column${columns.length === 1 ? '' : 's'}${delimNote}. ` +
    `Roles below are <em>guesses</em> for preview only.`;

  // Column chips with guessed roles
  el('recon-columns').innerHTML = columns.map((c) => {
    const role = detectRole(c);
    return `<span class="recon-col-chip role-${role}" title="guessed role: ${role}">` +
           `${truncate(c, 40)}<span class="recon-col-role">${role}</span></span>`;
  }).join('');

  // Preview table
  el('recon-preview-head').innerHTML =
    '<tr>' + columns.map((c) => `<th>${truncate(c, 40)}</th>`).join('') + '</tr>';
  el('recon-preview-body').innerHTML = rows.slice(0, PREVIEW_ROWS).map((r) =>
    '<tr>' + columns.map((_, i) => `<td>${truncate(r[i])}</td>`).join('') + '</tr>'
  ).join('');
}

function handleFile(file) {
  const reader = new FileReader();
  reader.onload = () => {
    try {
      const text = String(reader.result);
      const isJSON = /\.json$/i.test(file.name) || text.trim().startsWith('[') || text.trim().startsWith('{');
      const parsed = isJSON ? fromJSON(JSON.parse(text)) : fromDelimited(text);
      console.log(`[recon] parsed "${file.name}" locally: ${parsed.total} rows, ${parsed.columns.length} cols`);
      renderPreview(parsed);
    } catch (err) {
      console.error('[recon] parse failed:', err);
      el('recon-result').classList.remove('d-none');
      el('recon-summary').innerHTML =
        `<span class="text-danger"><i class="fas fa-exclamation-triangle me-1"></i>` +
        `Could not parse <strong>${truncate(file.name, 60)}</strong>: ${err.message}</span>`;
      el('recon-columns').innerHTML = '';
      el('recon-preview-head').innerHTML = '';
      el('recon-preview-body').innerHTML = '';
    }
  };
  reader.onerror = () => console.error('[recon] file read error', reader.error);
  reader.readAsText(file);
}

async function showCapabilities() {
  const caps = [];
  caps.push(`IndexedDB ${('indexedDB' in window) ? '✓' : '✗'}`);
  caps.push(`OPFS ${(navigator.storage && navigator.storage.getDirectory) ? '✓' : '✗'}`);
  caps.push(`Web Workers ${('Worker' in window) ? '✓' : '✗'}`);
  if (navigator.storage && navigator.storage.estimate) {
    try {
      const { usage, quota } = await navigator.storage.estimate();
      if (quota) {
        const pct = ((usage || 0) / quota * 100).toFixed(1);
        caps.push(`storage ~${(quota / 1048576).toFixed(0)} MB quota (${pct}% used)`);
      }
    } catch (_) { /* ignore */ }
  }
  el('recon-caps').innerHTML =
    '<i class="fas fa-info-circle me-1"></i>Browser capabilities for the full workbench: ' +
    caps.join(' &middot; ');
}

function init() {
  const dz = el('recon-dropzone');
  const input = el('recon-file');
  if (!dz || !input) return; // not on this page

  const openPicker = () => input.click();
  dz.addEventListener('click', openPicker);
  dz.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); openPicker(); }
  });
  input.addEventListener('change', () => { if (input.files[0]) handleFile(input.files[0]); });

  ['dragenter', 'dragover'].forEach((ev) => dz.addEventListener(ev, (e) => {
    e.preventDefault(); dz.classList.add('recon-dropzone--over');
  }));
  ['dragleave', 'drop'].forEach((ev) => dz.addEventListener(ev, (e) => {
    e.preventDefault(); dz.classList.remove('recon-dropzone--over');
  }));
  dz.addEventListener('drop', (e) => {
    const file = e.dataTransfer && e.dataTransfer.files[0];
    if (file) handleFile(file);
  });

  const clear = el('recon-clear');
  if (clear) clear.addEventListener('click', () => {
    input.value = '';
    el('recon-result').classList.add('d-none');
    el('recon-columns').innerHTML = '';
    el('recon-preview-head').innerHTML = '';
    el('recon-preview-body').innerHTML = '';
    console.log('[recon] local preview cleared');
  });

  showCapabilities();
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', init);
} else {
  init();
}
