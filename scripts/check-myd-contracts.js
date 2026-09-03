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


// ── 5. Cluster & merge similar values (place#235) ────────────────────────────
// `buildClusterTransform` decides which of the user's cell values get REWRITTEN, so a
// bug here corrupts their data rather than merely slowing a run. Three properties, each
// of which would fail silently:
//
//   • a value that is not in any ticked group must pass through UNCHANGED — a mapping
//     that returned the canonical for everything would flatten the column;
//   • unticking a group must exclude it, because that tick is the only thing standing
//     between a proposed merge and two real places being conflated;
//   • the pseudo-hits handed to clusterHits must carry NO `namespace`: every value comes
//     from one column, so declaring one trips clustering.js's same-namespace repulsion
//     and no group is ever proposed. The feature would look like "no variants found".
//
// The REAL functions are evaluated against a minimal fake DOM.
function loadClusterTransform(groups, ticked, canonicals) {
  const m = src.match(/^function buildClusterTransform\(\)[\s\S]*?^\}/m);
  if (!m) throw new Error('buildClusterTransform not found in reconciliation.js — renamed?');
  const box = {
    innerHTML: '',
    querySelectorAll: (sel) => (sel.includes('recon-cl-use')
      ? groups.map((_, i) => ({ checked: ticked[i], dataset: { i: String(i) } }))
      : []),
    querySelector: (sel) => {
      const i = Number((sel.match(/data-i="(\d+)"/) || [])[1]);
      return { value: canonicals[i] };
    },
  };
  let pending = null;
  const fn = new Function(
    'el', '_clusterGroups', 'renderTransformPreview', 'setPending',
    `${m[0]}; return buildClusterTransform;`,
  );
  // `_pendingTransform` is module-level in the real file; capture assignments to it by
  // running the body with a local of that name and reading it back through a getter.
  const body = m[0].replace(/_pendingTransform =/g, 'pending =');
  const runner = new Function(
    'el', '_clusterGroups', 'renderTransformPreview',
    `let pending = null; ${body}; buildClusterTransform(); return pending;`,
  );
  return runner(() => box, groups, () => {});
}

function checkValueClustering() {
  const groups = [
    { canonical: 'Newton', variants: ['Newton', 'Newtown', 'Neweton'], rows: 3 },
    { canonical: 'Lisbon', variants: ['Lisbon', 'Lisboa'], rows: 2 },
  ];

  // Both groups ticked.
  const t1 = loadClusterTransform(groups, [true, true], ['Newton', 'Lisbon']);
  if (!t1 || typeof t1.fn !== 'function') {
    bad('value clustering / transform', 'no transform was built from two ticked groups');
  } else if (t1.fn('Newtown') !== 'Newton' || t1.fn('Neweton') !== 'Newton') {
    bad('value clustering / mapping', 'a variant was not rewritten to the canonical value');
  } else if (t1.fn('Berlin') !== 'Berlin') {
    bad('value clustering / passthrough',
      'a value outside every group was rewritten — the column would be flattened');
  } else if (t1.fn('Newton') !== 'Newton') {
    bad('value clustering / canonical', 'the canonical value itself was rewritten');
  } else ok('value clustering: rewrites variants, leaves everything else alone');

  // Second group unticked — its variants must survive untouched.
  const t2 = loadClusterTransform(groups, [true, false], ['Newton', 'Lisbon']);
  if (!t2 || t2.fn('Lisboa') !== 'Lisboa') {
    bad('value clustering / opt-out',
      'unticking a group did not spare it — the only guard against conflating two real places');
  } else if (t2.fn('Newtown') !== 'Newton') {
    bad('value clustering / opt-out', 'unticking one group also disabled the others');
  } else ok('value clustering: unticking a group spares its values');

  // An edited canonical must win over the proposed one.
  const t3 = loadClusterTransform(groups, [true, false], ['Newtown', 'Lisbon']);
  if (!t3 || t3.fn('Newton') !== 'Newtown') {
    bad('value clustering / edited canonical',
      'editing the canonical spelling had no effect on the mapping');
  } else ok('value clustering: an edited canonical spelling is used');

  // The namespace trap, read off the real source.
  const run = src.match(/^async function runValueClustering\(\)[\s\S]*?^\}/m);
  if (!run) {
    bad('value clustering / namespace', 'runValueClustering not found — renamed?');
  } else if (/namespace/.test(run[0].replace(/\/\/.*$/gm, ''))) {
    bad('value clustering / namespace', 'the pseudo-hits carry a `namespace`, which trips ' +
      "clustering.js's same-namespace repulsion — no group would ever be proposed");
  } else ok('value clustering: pseudo-hits declare no namespace');
}

const required = (process.argv[2] || 'creator,name,description,url,citation').split(',').filter(Boolean);
console.log('Map your Data contract checks');
checkRoles();
checkIndexingContract(required);
checkCitationRoundTrip();
checkFeatureRoundTrip();
checkValueClustering();
if (failures.length) { console.error(`\n${failures.length} contract(s) broken.`); process.exit(1); }
console.log('\nAll contracts hold.');
