#!/usr/bin/env node
/**
 * Contract checks for Map your Data's column-role guessing and its dataset-metadata writer.
 *
 * Both of these have now failed in production, and neither failure was the kind a person spots by
 * reading the code:
 *
 *   - The role hints failed in BOTH directions. `coordinate_method` was claimed as a coordinate
 *     column because the coords hint is unanchored, which silenced every real coordinate in the
 *     dataset (place#225); and `ccodes` — the spelling Linked Places itself uses — was claimed by
 *     nothing at all, so containment reconciled to the wrong country. Three of those were patched
 *     one at a time with nothing to catch the fourth.
 *   - `description` was read by the ingest and emitted by nobody, so every contributed dataset
 *     landed with a placeholder where its description should be, on a public page (place#227).
 *     A field added to the reader after the writer was written fails exactly this way: silently.
 *
 * Run with `npm run test:myd`, or via `python manage.py test main` which shells out to this.
 * Exits non-zero on the first broken contract, listing every failure.
 */
const fs = require('fs');
const path = require('path');

const SRC = path.join(__dirname, '..', 'whg', 'webpack', 'js', 'reconciliation.js');
const src = fs.readFileSync(SRC, 'utf8');
const failures = [];
const ok = (name) => console.log(`  ok    ${name}`);
const bad = (name, detail) => { failures.push(`${name}: ${detail}`); console.log(`  FAIL  ${name}\n          ${detail}`); };

// ── 1. Column-role guessing ──────────────────────────────────────────────────
// The hint table and the qualifier guard are read out of the real source and applied with the real
// regex engine, so this tests what ships rather than a copy of it.
function loadDetector() {
  // Evaluate the REAL `detectRole` together with the real hint table and guard, rather than
  // reimplementing the lookup here. The first version of this harness rebuilt the logic from the
  // parsed regexes — and so kept passing when the guard was deleted from `detectRole`, because the
  // harness was still applying it itself. A check that cannot tell a working function from a broken
  // one is worse than no check: it reports success either way.
  const grab = (re, what) => {
    const m = src.match(re);
    if (!m) throw new Error(`${what} not found in reconciliation.js — has it been renamed?`);
    return m[0];
  };
  const hints = grab(/^const ROLE_HINTS = \[[\s\S]*?^\];/m, 'ROLE_HINTS');
  const qualifier = grab(/^const ROLE_QUALIFIER = \/[\s\S]*?\/i;/m, 'ROLE_QUALIFIER');
  const fn = grab(/^function detectRole\([\s\S]*?^\}/m, 'detectRole');
  // eslint-disable-next-line no-new-func
  const detect = new Function(`${hints}\n${qualifier}\n${fn}\nreturn detectRole;`)();
  if (typeof detect !== 'function') throw new Error('detectRole did not evaluate to a function');
  return detect;
}

// header → the role it must be guessed as. Every entry is a header seen on real contributed data.
const ROLE_CASES = [
  // Metadata ABOUT a value is never that value. These cost a whole dataset its coordinates.
  ['coordinate_method', 'other'], ['coordinate_precision_m', 'other'], ['coordinate_source', 'other'],
  ['geometry_note', 'other'], ['location_accuracy', 'other'], ['position_uncertainty', 'other'],
  ['date_source', 'other'], ['name_note', 'other'], ['coordinate_system', 'other'], ['crs', 'other'],
  // …but a coordinate is still a coordinate.
  ['coords', 'coords'], ['coordinates', 'coords'], ['gridref', 'coords'], ['grid_ref', 'coords'],
  ['easting', 'coords'], ['northing', 'coords'], ['osgb', 'coords'],
  ['lat', 'lat'], ['latitude', 'lat'], ['lon', 'lon'], ['lng', 'lon'], ['longitude', 'lon'],
  ['wkt', 'geowkt'], ['geom_wkt', 'geowkt'], ['the_geom', 'geowkt'],
  // Linked Places' own spellings must be recognised — this is the file format we publish.
  ['ccodes', 'country'], ['ccode', 'country'], ['country', 'country'], ['iso', 'country'],
  ['aat_type', 'type'], ['aat_id', 'type'], ['type', 'type'], ['fclass', 'type'], ['placetype', 'type'],
  ['place_name', 'name'], ['place name', 'name'], ['placename', 'name'], ['title', 'name'], ['toponym', 'name'],
  ['alt_names', 'alt_names'], ['variants', 'alt_names'],
  // Administrative levels, including ones outside western Europe.
  ['county', 'container'], ['parish', 'container'], ['district', 'container'], ['province', 'container'],
  ['oblast', 'container'], ['rayon', 'container'], ['viloyat', 'container'], ['aimag', 'container'],
  // Dates: a capture date describes the geometry, a plain date describes the place.
  ['acquisition_date', 'geom_date'], ['geometry_date', 'geom_date'], ['capture_date', 'geom_date'],
  ['image_date', 'geom_date'], ['survey_date', 'geom_date'],
  ['date', 'date'], ['year', 'date'], ['period', 'date'],
  ['id', 'id'], ['uid', 'id'], ['wikidata', 'id'],
  // Columns that name something OTHER than the place must not be claimed as its name.
  ['basin_name', 'other'], ['mountain_range', 'other'], ['record_status', 'other'],
];

function checkRoles() {
  let detect;
  try { detect = loadDetector(); } catch (e) { bad('role hints: parse', e.message); return; }
  const wrong = ROLE_CASES.filter(([h, want]) => detect(h) !== want)
    .map(([h, want]) => `${h} → ${detect(h)} (expected ${want})`);
  if (wrong.length) bad('column roles', `${wrong.length} of ${ROLE_CASES.length} mis-guessed:\n          ` + wrong.join('\n          '));
  else ok(`column roles (${ROLE_CASES.length} headers)`);
}

// ── 2. The dataset-metadata contract ─────────────────────────────────────────
// Every key WHG's ingest reads out of `indexing` must be a key Map your Data can write. The reader
// side is pinned by a Django test; this is the writer half.
function checkIndexingContract(required) {
  const start = src.indexOf('function schemaOrgDataset');
  const end = src.indexOf('\nfunction ', src.indexOf('function citationIndexing'));
  if (start < 0 || end < 0) { bad('indexing contract', 'could not locate schemaOrgDataset/citationIndexing'); return; }
  const body = src.slice(start, end)
    .replace(/\/\*[\s\S]*?\*\//g, '')            // block comments
    .replace(/(^|[^:])\/\/.*$/gm, '$1');         // line comments — a key named only in prose is not written
  const missing = required.filter((k) => !(
    new RegExp(`doc\\.${k}\\s*=`).test(body) || new RegExp(`(^|[{,\\s])${k}\\s*:`).test(body)
  ));
  if (missing.length) {
    bad('indexing contract', `WHG's ingest reads ${missing.join(', ')} from \`indexing\`, but Map your Data never writes ${missing.length === 1 ? 'it' : 'them'}. ` +
      'A contributed dataset will show that field blank on its public page.');
  } else ok(`indexing contract (${required.length} fields the ingest reads)`);
}

const required = (process.argv[2] || 'creator,name,description,url,citation').split(',').filter(Boolean);
console.log('Map your Data contract checks');
checkRoles();
checkIndexingContract(required);
if (failures.length) { console.error(`\n${failures.length} contract(s) broken.`); process.exit(1); }
console.log('\nAll contracts hold.');
