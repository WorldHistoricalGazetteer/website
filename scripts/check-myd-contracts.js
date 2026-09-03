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
  // A column of years that dates OBSERVATIONS of a place, not the place. Emitting these as the
  // place's own `when` would collapse to [min, max] and hide the place outside that window — a
  // lake measured in 1911 and 2009 existed before and after both. It must stay unclaimed.
  ['observation_years', 'other'], ['observation_year', 'other'], ['observed_years', 'other'],
  ['observation_date', 'other'], ['attestation_years', 'other'],
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

// ── 3. The round trip ────────────────────────────────────────────────────────
// MyD writes dataset metadata into `indexing` on export and reads it back on import. Every field
// the citation form holds must survive that trip, or a contributor who exports and re-imports
// silently loses it. `description` did exactly this: the import seeding was written before the
// field existed, so a file that carried a description produced a dataset reporting it had none.
function checkCitationRoundTrip() {
  const fm = src.match(/const CITE_FIELDS = \[([^\]]*)\]/);
  if (!fm) { bad('citation round trip', 'CITE_FIELDS not found'); return; }
  const fields = [...fm[1].matchAll(/'(\w+)'/g)].map((m) => m[1]);
  const start = src.indexOf('function lpfCitation');
  if (start < 0) { bad('citation round trip', 'lpfCitation not found'); return; }
  const body = src.slice(start, src.indexOf('\nfunction ', start + 1))
    .replace(/\/\*[\s\S]*?\*\//g, '').replace(/(^|[^:])\/\/.*$/gm, '$1');
  const lost = fields.filter((f) => !new RegExp(`out\\.${f}\\s*=`).test(body));
  if (lost.length) {
    bad('citation round trip', `Map your Data writes ${lost.join(', ')} into \`indexing\` but lpfCitation never reads ${lost.length === 1 ? 'it' : 'them'} back. ` +
      'Export then re-import and the contributor loses it.');
  } else ok(`citation round trip (${fields.length} citation fields)`);
}

// ── 4. The feature round trip ────────────────────────────────────────────────
// Every part of a Linked Places FEATURE that the importer reads must be re-emitted by the exporter.
// The citation round trip (3) covers dataset metadata; this covers the records themselves, and it is
// the check that was missing when `links` went in and nothing came out — 147 links on import,
// including two identity assertions a contributor had already resolved, and an exported file with
// none (place#228).
// Keyed on the DATA PATH, not on the output key. The first version of this check asserted that
// `buildLPF` assigns `feat.links` — which it always did; the bug was that the array it assigned was
// built from reconciliation alone and never included the file's own links. A check that passes on
// the broken state is the failure it exists to prevent, so each part names the accessor that
// carries the importer's output into the exporter.
const FEATURE_PARTS = [
  ['names', /rowVariants\(/, 'the alt_names column the importer fills'],
  ['names (lang + citations)', /rec\.fileNames/, 'project.rowNames'],
  ['types', /rowTypesFor\(/, 'project.rowTypes'],
  ['when', /rec\.whenStart/, 'the date column the importer fills'],
  ['geometry', /rec\.geom\b/, 'the geometry column'],
  ['geometry (certainty + approximation)', /rec\.fileGeomMeta/, 'project.rowGeomMeta'],
  ['links', /rec\.fileLinks/, 'project.rowLinks'],
];
function checkFeatureRoundTrip() {
  const start = src.indexOf('function buildLPF');
  if (start < 0) { bad('feature round trip', 'buildLPF not found'); return; }
  const body = src.slice(start, src.indexOf('\nfunction ', start + 1))
    .replace(/\/\*[\s\S]*?\*\//g, '').replace(/(^|[^:])\/\/.*$/gm, '$1');
  const lost = FEATURE_PARTS.filter(([, re]) => !re.test(body));
  if (lost.length) {
    bad('feature round trip', lost.map(([k, , via]) =>
      `the importer reads \`${k}\` out of a Linked Places feature and stores it in ${via}, but buildLPF never reads that back`).join('; ') +
      '. Import then export and the contributor loses it.');
  } else ok(`feature round trip (${FEATURE_PARTS.length} feature parts)`);
}


// ── 5. Variant-spelling grouping (place#235) ─────────────────────────────────
// `mergeVariantUnits` folds near-duplicate reconciliation units into one so a place
// spelled several ways costs one query. Two properties are easy to break and neither
// shows up as an error — the run just gets slower, or silently conflates places:
//
//   • it must NOT stamp a `namespace` on the pseudo-hits. Every unit comes from the
//     user's own file, so declaring one trips clustering.js's same-namespace
//     repulsion and blocks EVERY merge. The feature would be inert, not broken.
//   • the surviving unit must carry the absorbed units' memberKeys, or the rows
//     behind an absorbed spelling get no match written back at all.
//
// The REAL function is evaluated, with clusterHits injected so the fold can be
// tested deterministically without the 21 MB embedding model.
function loadMergeVariantUnits(recordHits, clustersToReturn) {
  const m = src.match(/^function mergeVariantUnits\([\s\S]*?^\}/m);
  if (!m) throw new Error('mergeVariantUnits not found in reconciliation.js — has it been renamed?');
  // The real margin too, not a copy: if someone retunes it the test follows.
  const margin = src.match(/^const VARIANT_THETA_MARGIN = .*;$/m);
  if (!margin) throw new Error('VARIANT_THETA_MARGIN not found in reconciliation.js');
  const factory = new Function(
    'clusterHits', 'DEFAULT_PARAMS', 'rowCoordValue', 'console',
    `${margin[0]}\n${m[0]}; return mergeVariantUnits;`,
  );
  return factory(
    (args) => { recordHits.push(...args.hits); return { clusters: clustersToReturn(args) }; },
    { thresholds: { theta_query: 0.55 }, same_ns_penalty: 0.15 },
    () => null,                                   // no coordinates in this fixture
    { log() {}, error() {} },
  );
}

function checkVariantGrouping() {
  const unit = (key, q, rows) => ({ repKey: key, memberKeys: rows, v: { query: q } });

  // Newton/Newtown cluster together; Lisbon stands alone.
  const units = [
    unit('u:0', 'Newton', ['u:0']),
    unit('u:1', 'Newtown', ['u:1', 'u:9']),
    unit('u:2', 'Lisbon', ['u:2']),
  ];
  const hits = [];
  const fn = loadMergeVariantUnits(hits, () => ([
    { root: 'u:1', memberIds: ['u:0', 'u:1'] },
    { root: 'u:2', memberIds: ['u:2'] },
  ]));
  const groups = fn(units, { 'u:0': [1], 'u:1': [1], 'u:2': [1] });

  if (hits.some((h) => 'namespace' in h)) {
    bad('variant grouping / namespace', 'pseudo-hits carry a `namespace`, which trips ' +
      "clustering.js's same-namespace repulsion and blocks every merge — the feature goes inert");
  } else ok('variant grouping: pseudo-hits declare no namespace');

  if (units.length !== 2) {
    bad('variant grouping / fold', `expected 2 units after folding, got ${units.length}`);
  } else if (!units.some((u) => u.memberKeys.length === 3)) {
    bad('variant grouping / memberKeys',
      'the surviving unit does not carry the absorbed unit\'s memberKeys — the rows behind ' +
      'that spelling would get no match written back');
  } else if (groups.length !== 1 || groups[0].kept !== 'Newtown') {
    bad('variant grouping / representative',
      `expected the commonest spelling ("Newtown", 2 rows) to survive, got ${JSON.stringify(groups)}`);
  } else ok('variant grouping: folds units, keeps the commonest spelling, carries memberKeys');

  // A scorer failure must degrade to reconciling every unit separately, not throw
  // mid-run and abandon the pass.
  const units2 = [unit('u:0', 'Newton', ['u:0']), unit('u:1', 'Newtown', ['u:1'])];
  const boom = new Function(
    'clusterHits', 'DEFAULT_PARAMS', 'rowCoordValue', 'console',
    `${src.match(/^const VARIANT_THETA_MARGIN = .*;$/m)[0]}\n`
    + `${src.match(/^function mergeVariantUnits\([\s\S]*?^\}/m)[0]}; return mergeVariantUnits;`,
  )(() => { throw new Error('scorer exploded'); },
    { thresholds: { theta_query: 0.55 }, same_ns_penalty: 0.15 }, () => null,
    { log() {}, error() {} });
  let threw = false;
  let out;
  try { out = boom(units2, {}); } catch (e) { threw = true; }
  if (threw) bad('variant grouping / degradation', 'a scorer failure propagates and kills the run');
  else if (out.length !== 0 || units2.length !== 2) {
    bad('variant grouping / degradation', 'a scorer failure did not leave the units untouched');
  } else ok('variant grouping: a scorer failure degrades to one query per unit');
}

const required = (process.argv[2] || 'creator,name,description,url,citation').split(',').filter(Boolean);
console.log('Map your Data contract checks');
checkRoles();
checkIndexingContract(required);
checkCitationRoundTrip();
checkFeatureRoundTrip();
checkVariantGrouping();
if (failures.length) { console.error(`\n${failures.length} contract(s) broken.`); process.exit(1); }
console.log('\nAll contracts hold.');
