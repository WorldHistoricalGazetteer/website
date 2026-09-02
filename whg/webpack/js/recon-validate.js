// recon-validate.js
// Client-side Linked Places (LPF) validation — mirrors the server's validation (validation/tasks.py
// runs jsonschema.Draft7Validator against validation/static/lpf_v2.0.jsonld) so the Workbench can flag
// problems and gate the "Contribute to WHG" button BEFORE anything is submitted. Lazy-loaded because
// Ajv + the schema are only needed on the export/contribute pane.

import Ajv from 'ajv';

// Both schemas are versioned by deploy, like the bundles: without the cache-buster a browser holding
// yesterday's copy validates against yesterday's rules, and a schema fix (e.g. widening the accepted
// link namespaces) silently fails to reach the people it was written for.
const ASSET_V = (document.querySelector('meta[name="asset-version"]') || {}).content || '';
const V = ASSET_V ? `?v=${encodeURIComponent(ASSET_V)}` : '';
const SCHEMA_URL = `/static/lpf_v2.0.jsonld${V}`;
const CSL_URL = `/static/csl-citation.json${V}`; // lpf_v2.0.jsonld $refs this for name citations
let _validate = null;

async function getJson(url, required) {
  const r = await fetch(url, { credentials: 'same-origin', headers: { Accept: 'application/json' } });
  if (!r.ok) { if (required) throw new Error(`schema HTTP ${r.status}`); return null; }
  return r.json();
}

async function getValidator() {
  if (_validate) return _validate;
  const [schema, csl] = await Promise.all([getJson(SCHEMA_URL, true), getJson(CSL_URL, false).catch(() => null)]);
  // strict:false → tolerate the schema's draft-07 idioms / unknown formats without throwing;
  // allErrors → collect every problem (not just the first) so we can summarise per feature;
  // unicodeRegExp:false → compile `pattern` regexes WITHOUT the /u flag (the LPF schema uses \: / \-
  // escapes that are invalid in unicode mode and would otherwise throw at compile time).
  const ajv = new Ajv({ allErrors: true, strict: false, validateFormats: false, unicodeRegExp: false });
  if (csl) ajv.addSchema(csl); // registered under its $id so the LPF schema's csl-citation $ref resolves
  _validate = ajv.compile(schema);
  return _validate;
}

// Validate an LPF FeatureCollection against the WHG schema.
// Returns { ok, errorCount, summary:[{msg, count}] } — errors grouped by field+message across features
// so the panel can show "@id must match pattern (14×)" rather than 42 raw entries.
export async function validateLPF(fc) {
  const validate = await getValidator();
  const ok = validate(fc);
  const errors = ok ? [] : (validate.errors || []);

  // The temporal requirement is one rule expressed as an `anyOf` over six locations, so a single
  // undated feature fails every branch and Ajv reports each failure separately — "missing when",
  // "missing relations", "missing citation", "missing geometry", and so on. Those are the machinery
  // of one rule, not several faults, and listing them makes a dataset look far more broken than it
  // is. They collapse into the one statement that is true.
  const TEMPORAL = /\/definitions\/feature\/allOf\/1\/anyOf\//;
  const groups = {};              // msg → { count, records:Set }
  const add = (msg, fi) => {
    const g = groups[msg] || (groups[msg] = { count: 0, records: new Set() });
    g.count += 1;
    if (fi != null) g.records.add(fi);
  };
  for (const e of errors) {
    const raw = (e.instancePath || '').split('/').filter(Boolean);
    const fi = (raw[0] === 'features' && /^\d+$/.test(raw[1])) ? Number(raw[1]) : null;
    if (TEMPORAL.test(e.schemaPath || '')) { add('missing when (in any location the format accepts)', fi); continue; }
    // Drop numeric (array-index) segments so per-feature errors collapse: /features/3/@id → "@id".
    const parts = raw.filter((p) => !/^\d+$/.test(p));
    const field = parts.length ? parts[parts.length - 1] : '(feature)';
    const msg = e.keyword === 'required' ? `missing ${(e.params && e.params.missingProperty) || 'property'}`
      : `${field} ${e.message}`;
    add(msg, fi);
  }
  const summary = Object.entries(groups)
    .map(([msg, g]) => ({ msg, count: g.count, records: g.records.size, features: [...g.records] }))
    .sort((a, b) => (b.records - a.records) || (b.count - a.count));
  // How many FEATURES are affected — the number a contributor can act on. Counting raw errors and
  // calling them records reported "678 records" for a 71-record file.
  const affected = new Set();
  summary.forEach((g) => g.features.forEach((i) => affected.add(i)));
  return { ok, errorCount: errors.length, recordCount: affected.size, summary };
}
