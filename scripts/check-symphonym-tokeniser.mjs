#!/usr/bin/env node
/**
 * Golden-fixture check for the in-browser Symphonym tokeniser and quantiser.
 *
 * whg3 computes its own `query_vector` in the browser and posts it to /api/reconcile. That vector
 * has to be the one the gateway would have computed, because it is compared against 72.7M vectors
 * that were written by the same code. Until 5 September 2026 four implementations of this tokeniser
 * disagreed; on 46,483,973 documents (63.9% of the index) the query vector did not match the
 * document's own stored vector, and for CJK/Kana/Hangul the two were ANTI-correlated. whg3 was the
 * fourth implementation and is the last one to be brought into line.
 *
 * The fixture in `fixtures/symphonym_golden.json` was generated from the canonical Python
 * (indexing repo, hf/inference.py, commit 97a8b31 — the code the gateway serves). It is the
 * contract, and the contract is EXACT EQUALITY on char_ids, script_id, lang_id and the int8 vector,
 * not approximate similarity. Every case in it reaches a real divergence: romanisation, the
 * <SPACE> id, the language-tag normalisation, the alphabetic-only script vote, or the empty guard.
 * That last point is the reason the fixture is not a corpus sample — the equivalence test that
 * missed the script-vote bug ran over 6,013 real names containing zero digits, so it could not have
 * detected it. A test that cannot reach the boundary passes for the wrong reason.
 *
 * Run with `npm run test:symphonym`. Exits non-zero listing every failing case.
 *
 * The vector half of the check needs the ONNX encoder that ships to the browser
 * (static/webpack/symphonym/symphonym.onnx). It runs the real onnxruntime-web WASM build under
 * Node, so what is measured is what the browser executes.
 */
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(HERE, '..');
const ASSETS = path.join(ROOT, 'static', 'webpack', 'symphonym');

const { tokenise } = await import(path.join(ROOT, 'whg', 'webpack', 'js', 'recon-symphonym-preprocess.js'));
const { quantiseByte } = await import(path.join(ROOT, 'whg', 'webpack', 'js', 'recon-symphonym-quantise.js'));

const fixture = JSON.parse(fs.readFileSync(path.join(HERE, 'fixtures', 'symphonym_golden.json'), 'utf8'));
const readVocab = (f, key) => { const j = JSON.parse(fs.readFileSync(path.join(ASSETS, f), 'utf8')); return j[key] || j; };
const vocabs = {
  charToId: readVocab('char_vocab.json', 'char_to_id'),
  scriptToId: readVocab('script_vocab.json', 'script_to_id'),
  langToId: readVocab('lang_vocab.json', 'lang_to_id'),
};

const failures = [];
const label = (c) => `${JSON.stringify(c.name)}/${JSON.stringify(c.lang)} [${c.case}]`;
const bad = (c, detail) => { failures.push(`${label(c)}: ${detail}`); console.log(`  FAIL  ${label(c)}\n          ${detail}`); };

// ── 0. The anyascii lockstep ─────────────────────────────────────────────────
// D1 romanises CJK/Kana through anyascii, so whg3's npm `any-ascii` and the gateway's PyPI
// `anyascii` must be the same version. Their tables were verified byte-identical across 94,624
// codepoints AT 0.3.3, spanning every romanised range; that guarantee does not survive a one-sided
// bump, and `npm audit fix`, Dependabot and a routine `pip install -U` can all deliver one without
// anyone deciding to.
//
// The fixture cases would not catch it. They romanise a handful of strings; a bump that changed any
// other CJK codepoint would pass every one of them. So the version is asserted directly, and
// package.json must pin it EXACTLY — a caret range on a 0.x version silently admits 0.3.4.
//
// This is a stamp, and a stamp is forgeable: someone can bump the dependency and edit the line
// below. That is the point. It cannot be done by accident, and the failure message says what has
// to be re-established before the number may change.
const ANYASCII_VERIFIED = '0.3.3';
const pkg = JSON.parse(fs.readFileSync(path.join(ROOT, 'package.json'), 'utf8'));
const declared = (pkg.dependencies || {})['any-ascii'];
const installed = JSON.parse(fs.readFileSync(path.join(ROOT, 'node_modules', 'any-ascii', 'package.json'), 'utf8')).version;
console.log(`\nanyascii lockstep (verified byte-identical to PyPI anyascii ${ANYASCII_VERIFIED} across 94,624 codepoints)`);
if (declared !== ANYASCII_VERIFIED) {
  failures.push(`package.json pins any-ascii as "${declared}", not exactly "${ANYASCII_VERIFIED}"`);
  console.log(`  FAIL  package.json declares "${declared}" — must be exactly "${ANYASCII_VERIFIED}", no range`);
} else if (installed !== ANYASCII_VERIFIED) {
  failures.push(`any-ascii ${installed} is installed but only ${ANYASCII_VERIFIED} has been verified against PyPI anyascii`);
  console.log(`  FAIL  installed ${installed}, verified ${ANYASCII_VERIFIED}\n`
    + `          Re-verify the two tables across the romanised ranges and bump the PyPI side in the\n`
    + `          same change before updating ANYASCII_VERIFIED. See scripts/fixtures/README-symphonym.md.`);
} else {
  console.log(`  ok    pinned and installed at ${installed}`);
}

// ── 1. Tokeniser: char_ids, script_id, lang_id ───────────────────────────────
console.log(`\nSymphonym tokeniser vs golden fixture v${fixture.meta.fixture_version} `
  + `(${fixture.meta.source} @ ${(fixture.meta.git_commit || '?').slice(0, 8)})`);
let tokOk = 0;
const tokenised = new Map();
for (const c of fixture.cases) {
  // A case carrying `warning` is an expectation the fixture itself marks as unstable — currently
  // only U+0870, which is alphabetic under the index writer's Unicode 14.0.0 and not under the
  // gateway's 13.0.0. We are pinned to the gateway, so it IS an expectation for us and is asserted
  // like any other; it is echoed so that a failure here reads as "the pin moved" rather than as a
  // mystery. See symphonym-unicode.js for the three-interpreter table.
  if (c.warning) console.log(`  note  ${label(c)}\n          ${c.warning}`);
  const t = tokenise(c.name, c.lang, vocabs);
  tokenised.set(c, t);
  const got = Array.from(t.charIds);
  let failed = false;
  if (got.length !== c.char_ids.length || got.some((v, i) => v !== c.char_ids[i])) {
    bad(c, `char_ids\n            want ${JSON.stringify(c.char_ids)}\n            got  ${JSON.stringify(got)}`);
    failed = true;
  }
  if (t.scriptId !== c.script_id) { bad(c, `script_id want ${c.script_id} got ${t.scriptId}`); failed = true; }
  if (t.langId !== c.lang_id) { bad(c, `lang_id want ${c.lang_id} got ${t.langId}`); failed = true; }
  // `length` feeds the encoder's mask and must be the id count, not the character count.
  if (t.length !== c.char_ids.length) { bad(c, `length want ${c.char_ids.length} got ${t.length}`); failed = true; }
  if (!failed) tokOk++;
}
console.log(`  ${tokOk}/${fixture.cases.length} cases exact on char_ids + script_id + lang_id + length`);

// ── 2. Quantiser + encoder: the int8 vector ──────────────────────────────────
// Skipped, loudly, if the ONNX asset is absent — a check that silently does nothing is worse than
// no check. `--no-vector` skips it deliberately (the model load is ~8 MB and a few seconds).
// WHY THIS HALF IS A BOUND AND NOT AN EQUALITY, unlike the tokeniser above. The fixture's vectors
// come from the fp32 PyTorch reference; `symphonym.onnx` is a DYNAMICALLY QUANTISED int8 export
// (DynamicQuantizeLinear + MatMulInteger in the graph), so its arithmetic cannot reproduce the fp32
// result bit for bit and never could. Measured across all 27 cases the export costs at most 2 of
// 127 on any one component and never drops cosine below 0.9976.
//
// That leaves this check with a real and narrower job: catching a STALE OR MISMATCHED ONNX asset —
// a re-export, a model version bump, a half-copied static/webpack/symphonym/. It cannot be
// mistaken for the tokeniser check: a tokeniser regression is not a rounding difference. The
// pre-fix implementation, measured against these same 27 cases, scored cos 0.9506 for
// 'Bury St Edmunds', 0.8765 for a name with leading spaces, 0.0235 for '서울' and -0.3071 for
// '東京' — the last matching the -0.3036 the server measured for the same name. The floor below
// separates those from export noise by two orders of magnitude.
const COS_FLOOR = 0.995;
const DIFF_CEILING = 4;
let worstCos = 1;
let worstDiff = 0;
const cosine = (a, b) => {
  let dot = 0; let na = 0; let nb = 0;
  for (let i = 0; i < 128; i++) { dot += a[i] * b[i]; na += a[i] * a[i]; nb += b[i] * b[i]; }
  return dot / Math.sqrt(na * nb);
};

const wantVector = !process.argv.includes('--no-vector');
const onnxPath = path.join(ASSETS, 'symphonym.onnx');
let vecOk = 0;
let vecRun = 0;
if (!wantVector) {
  console.log('  SKIP  int8 vector check (--no-vector)');
} else if (!fs.existsSync(onnxPath)) {
  failures.push(`ONNX encoder missing at ${onnxPath} — the vector half of the contract was not checked`);
  console.log(`  FAIL  ONNX encoder missing at ${onnxPath}`);
} else {
  const ort = await import('onnxruntime-web');
  ort.env.wasm.numThreads = 1;
  ort.env.logLevel = 'error';
  const session = await ort.InferenceSession.create(onnxPath, { executionProviders: ['wasm'] });
  const big = (v) => BigInt64Array.from(v, (x) => BigInt(x));
  for (const c of fixture.cases) {
    const t = tokenised.get(c);
    const out = await session.run({
      char_ids: new ort.Tensor('int64', big(t.charIds), [1, t.charIds.length]),
      script_id: new ort.Tensor('int64', big([t.scriptId]), [1]),
      lang_id: new ort.Tensor('int64', big([t.langId]), [1]),
      length: new ort.Tensor('int64', big([t.length]), [1]),
    });
    const q = new Int8Array(128);
    quantiseByte(out.embedding.data, q, 0);
    vecRun++;
    const diffs = Array.from(q, (v, i) => Math.abs(v - c.int8[i]));
    const maxDiff = Math.max(...diffs);
    const cos = cosine(q, c.int8);
    worstCos = Math.min(worstCos, cos);
    worstDiff = Math.max(worstDiff, maxDiff);
    if (cos >= COS_FLOOR && maxDiff <= DIFF_CEILING) vecOk++;
    else bad(c, `int8 vector: cos ${cos.toFixed(5)} (floor ${COS_FLOOR}), max |Δ| ${maxDiff} (ceiling ${DIFF_CEILING}), `
      + `${diffs.filter((d) => d > 0).length}/128 components differ`);
  }
  console.log(`  ${vecOk}/${vecRun} cases within the export-noise bound `
    + `(worst cos ${worstCos.toFixed(5)}, worst |Δ| ${worstDiff})`);
}

// ── 3. Breadth: the differential corpus ──────────────────────────────────────
// 15,853 cases run through the same canonical Python. The golden fixture pins the four named
// divergences precisely; this one exists because three MORE ways to get the tokeniser wrong are not
// reachable from 27 cases at all:
//
//   - the script table's PRECEDENCE rule. The canonical Python builds a codepoint→script dict by
//     iterating its range list, so where two scripts overlap the LATER entry wins. Exactly one block
//     overlaps: U+FB00–FB17, Hebrew presentation forms shadowed by the Armenian ligatures, which
//     means the Latin ligature 'ﬁ' scores as ARMENIAN. A first-match-wins scan — what this file did
//     before — fails 835 of these and none of the 27.
//   - Python's whitespace set vs JS's. Substituting /\s/u fails 699.
//   - the Unicode version behind str.isalpha(). Substituting \p{L}, which tracks whatever the
//     browser's ICU carries rather than the reference interpreter's tables, fails 189.
//
// Every one of those numbers was measured by making the substitution and re-running, not predicted.
const diff = JSON.parse(fs.readFileSync(path.join(HERE, 'fixtures', 'symphonym_differential.json'), 'utf8'));
let diffBad = 0;
const diffSamples = [];
for (const c of diff.cases) {
  const t = tokenise(c.t, c.l, vocabs);
  const ids = Array.from(t.charIds);
  const wrong = !(ids.length === c.ids.length && ids.every((v, i) => v === c.ids[i]))
    || t.scriptId !== c.s || t.langId !== c.g;
  if (wrong) {
    diffBad++;
    if (diffSamples.length < 10) {
      diffSamples.push(`${JSON.stringify(c.t)}/${JSON.stringify(c.l)}: `
        + `ids want ${JSON.stringify(c.ids)} got ${JSON.stringify(ids)}; `
        + `script want ${c.s} got ${t.scriptId}; lang want ${c.g} got ${t.langId}`);
    }
  }
}
if (diffBad) {
  failures.push(`differential corpus: ${diffBad}/${diff.cases.length} cases diverge from the canonical Python`);
  console.log(`  FAIL  differential corpus: ${diffBad}/${diff.cases.length} cases diverge`);
  for (const d of diffSamples) console.log(`          ${d}`);
} else {
  console.log(`  ${diff.cases.length}/${diff.cases.length} differential cases exact`);
}

// ── Result ───────────────────────────────────────────────────────────────────
if (failures.length) {
  console.log(`\n${failures.length} failure(s):`);
  for (const f of failures) console.log(`  - ${f}`);
  process.exit(1);
}
console.log(`\nAll ${fixture.cases.length} golden cases (tokeniser exact, vector within the export bound) `
  + `and all ${diff.cases.length} differential cases exact.\n`);
