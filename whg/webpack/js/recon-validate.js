// recon-validate.js
// Client-side Linked Places (LPF) validation — mirrors the server's validation (validation/tasks.py
// runs jsonschema.Draft7Validator against validation/static/lpf_v2.0.jsonld) so the Workbench can flag
// problems and gate the "Contribute to WHG" button BEFORE anything is submitted. Lazy-loaded because
// Ajv + the schema are only needed on the export/contribute pane.

import Ajv from 'ajv';

const SCHEMA_URL = '/static/lpf_v2.0.jsonld';
const CSL_URL = '/static/csl-citation.json'; // lpf_v2.0.jsonld $refs this for name citations
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

// Validate an LPF FeatureCollection object against the WHG schema.
// Returns { ok:boolean, errors:[ajv error objects] } (errors carry instancePath like /features/3/...).
export async function validateLPF(fc) {
  const validate = await getValidator();
  const ok = validate(fc);
  return { ok, errors: ok ? [] : (validate.errors || []) };
}
