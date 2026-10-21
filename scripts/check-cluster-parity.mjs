#!/usr/bin/env node
/**
 * The JS half of the clusterer parity check (place#325, SG ruling 2026-10-21: "port the
 * clusterer to Python with parity tests").
 *
 * whg/webpack/js/clustering.js (clusterHits/scorePair) and locus_resolve.js
 * (selectLocusCluster/resolveRecipe) are the CONTRACT; search/clustering.py reproduces them.
 * This script runs the SHIPPED modules (never a copy) over every fixture in
 * scripts/fixtures/cluster_parity/<name>.json and compares the outcome with the committed
 * oracle <name>.expected.json: cluster assignments, cluster roots and membership, the
 * selected cluster, the debug counters, and every pair's score and signals. The Django test
 * search.tests_cluster_parity then holds the Python port to those same files (and, when
 * node is available, re-runs this script to prove the oracle is not stale).
 *
 *   node scripts/check-cluster-parity.mjs            check every fixture against its oracle
 *   node scripts/check-cluster-parity.mjs --write    (re)generate the oracles
 *   node scripts/check-cluster-parity.mjs --emit X   print the JS outcome for fixture X as JSON
 *
 * CLUSTER_PARITY_DIR points at another fixture directory (the Django test uses it to show
 * this checker fails on a tampered oracle: a parity check that cannot fail proves nothing).
 *
 * Exits non-zero listing every discrepancy.
 */
import crypto from 'crypto';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(HERE, '..');
const JS_DIR = path.join(ROOT, 'whg', 'webpack', 'js');
const clustering = await import(path.join(JS_DIR, 'clustering.js'));
const resolve = await import(path.join(JS_DIR, 'locus_resolve.js'));
const { clusterHits, scorePair } = clustering;
const { resolveRecipe, selectLocusCluster } = resolve;

const DIR = process.env.CLUSTER_PARITY_DIR || path.join(HERE, 'fixtures', 'cluster_parity');

const sha = (p) => crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex');
const JS_SHA = {
	'clustering.js': sha(path.join(JS_DIR, 'clustering.js')),
	'locus_resolve.js': sha(path.join(JS_DIR, 'locus_resolve.js')),
};

// clustering.js keeps defaultAccessors() private. Every accessor is invoked as a method on
// the merged accessor object (`acc.id(h)`), so an override can capture that object through
// `this`: one call overriding `id` yields the real default for every OTHER key, and one
// overriding `namespace` yields the real default `id`. No accessor is re-implemented here.
function defaultAccessors() {
	const probe = [{ place_id: 'probe:1', namespace: 'probe' }];
	let viaId = null, viaNs = null;
	clusterHits({ hits: probe, accessors: { id(h) { viaId = this; return h.place_id; } } });
	clusterHits({ hits: probe, accessors: { namespace(h) { viaNs = this; return null; } } });
	if (!viaId || !viaNs) throw new Error('could not capture clustering.js default accessors');
	return { ...viaId, id: viaNs.id };
}
const ACC = defaultAccessors();

// clustering.js's private pairKey, for the edge index scorePair reads. Guarded below: the
// count of pairs this path scores at or above theta must equal clusterHits' own
// debug.thresholdMerges for the same case, so a drift here cannot pass unnoticed.
const pairKey = (a, b) => (a < b ? `${a}\u0000${b}` : `${b}\u0000${a}`);

function clusterOut(cl) {
	return { root: cl.root, memberIds: cl.memberIds.slice() };
}

// Every pair's scorePair result for one weight set (a pair's score depends on the weights,
// the response's edges and stoplist, never on theta/margin/respect_distinct), as compact
// rows [i, j, composite, weightUsed, [name, spatial, temporal, type, link]] with null for
// a signal that degraded out. Computed once per distinct weight set in a fixture.
const SIGNAL_ORDER = ['name', 'spatial', 'temporal', 'type', 'link'];
function scorePairs(data, weights) {
	const hits = data.hits || [];
	const edgeIndex = new Map();
	for (const e of (data.edges || [])) edgeIndex.set(pairKey(e.a, e.b), e);
	const stopSet = new Set((data.toponym_stoplist || []).map((s) => String(s).toLowerCase()));
	const pairs = [];
	for (let i = 0; i < hits.length; i++) {
		for (let j = i + 1; j < hits.length; j++) {
			const r = scorePair(hits[i], hits[j], { acc: ACC, weights, edgeIndex, stopSet });
			const extra = Object.keys(r.signals).filter((k) => !SIGNAL_ORDER.includes(k));
			if (extra.length) throw new Error(`scorePair returned an unknown signal ${extra}`);
			pairs.push([i, j, r.composite, r.weightUsed, SIGNAL_ORDER.map((k) => (k in r.signals ? r.signals[k] : null))]);
		}
	}
	return pairs;
}

function runCase(data, c, pairSets) {
	const recipe = { v: 1, retrieval: {}, clustering: c.clustering, members: c.members };
	const out = resolveRecipe(recipe, data);
	const sel = out.selection;
	const key = JSON.stringify(c.clustering.weights);
	if (!(key in pairSets)) pairSets[key] = scorePairs(data, c.clustering.weights);
	const atOrAbove = pairSets[key].filter((p) => p[2] >= c.clustering.theta).length;
	if (atOrAbove !== out.debug.thresholdMerges) {
		throw new Error(`case ${c.name}: harness scored ${atOrAbove} pairs >= theta but clusterHits saw ${out.debug.thresholdMerges}`);
	}
	return {
		name: c.name,
		assignments: Object.fromEntries(out.assignments),
		clusters: out.clusters.map(clusterOut),
		selection: {
			cluster: sel.cluster ? clusterOut(sel.cluster) : null,
			reason: sel.reason, surviving: sel.surviving, missing: sel.missing,
			score: sel.score, candidates: sel.candidates,
		},
		params: out.params,
		debug: out.debug,
		stoplist_count: out.stoplist.length,
		pairs_key: key,
	};
}

function runFixture(fx) {
	const pairSets = {};
	const cases = fx.cases.map((c) => runCase(fx.data, c, pairSets));
	return { fixture: fx.name, js_sha256: JS_SHA, signal_order: SIGNAL_ORDER, cases, pair_sets: pairSets };
}

function fixtureNames() {
	return fs.readdirSync(DIR).filter((f) => f.endsWith('.json') && !f.endsWith('.expected.json'))
		.map((f) => f.slice(0, -5)).sort();
}
const load = (name) => JSON.parse(fs.readFileSync(path.join(DIR, name + '.json'), 'utf8'));
const expectedPath = (name) => path.join(DIR, name + '.expected.json');

// Deep comparison that reports the first differing path. Numbers compare exactly: the same
// engine produced both sides, so any difference is a change in the JS or the fixture.
function diff(a, b, at = '') {
	if (a === b) return null;
	if (typeof a !== typeof b || a === null || b === null) return `${at}: ${JSON.stringify(a)} != ${JSON.stringify(b)}`;
	if (Array.isArray(a)) {
		if (!Array.isArray(b) || a.length !== b.length) return `${at}: array length ${a.length} != ${Array.isArray(b) ? b.length : typeof b}`;
		for (let i = 0; i < a.length; i++) { const d = diff(a[i], b[i], `${at}[${i}]`); if (d) return d; }
		return null;
	}
	if (typeof a === 'object') {
		const ka = Object.keys(a), kb = Object.keys(b);
		if (ka.length !== kb.length) return `${at}: keys ${JSON.stringify(ka)} != ${JSON.stringify(kb)}`;
		for (const k of ka) { if (!(k in b)) return `${at}.${k}: missing`; const d = diff(a[k], b[k], `${at}.${k}`); if (d) return d; }
		return null;
	}
	if (typeof a === 'number' && Number.isNaN(a) && Number.isNaN(b)) return null;
	return `${at}: ${JSON.stringify(a)} != ${JSON.stringify(b)}`;
}

const args = process.argv.slice(2);
if (args[0] === '--emit') {
	// No process.exit() here: it would cut a piped stdout at 64 KiB before it drained.
	process.stdout.write(JSON.stringify(runFixture(load(args[1]))) + '\n');
} else {
	check();
}

function check() {
const write = args.includes('--write');
const names = fixtureNames();
if (!names.length) { console.error(`cluster parity: no fixtures in ${DIR}`); process.exit(2); }
const failures = [];
let cases = 0, pairs = 0;
for (const name of names) {
	const got = runFixture(load(name));
	cases += got.cases.length;
	for (const k of Object.keys(got.pair_sets)) pairs += got.pair_sets[k].length;
	if (write) {
		fs.writeFileSync(expectedPath(name), JSON.stringify(got) + '\n');
		continue;
	}
	if (!fs.existsSync(expectedPath(name))) { failures.push(`${name}: no oracle (run with --write)`); continue; }
	const want = JSON.parse(fs.readFileSync(expectedPath(name), 'utf8'));
	if (want.js_sha256['clustering.js'] !== JS_SHA['clustering.js'] || want.js_sha256['locus_resolve.js'] !== JS_SHA['locus_resolve.js']) {
		failures.push(`${name}: oracle was generated from different JS sources (clustering.js/locus_resolve.js changed); regenerate with --write and re-run the Django parity test`);
	}
	const d = diff(got.cases, want.cases, 'cases') || diff(got.pair_sets, want.pair_sets, 'pair_sets');
	if (d) failures.push(`${name}: ${d}`);
}
if (failures.length) {
	console.error(`cluster parity: ${failures.length} of ${names.length} fixtures FAILED\n\n` + failures.join('\n'));
	process.exit(1);
}
console.log(`cluster parity: ${write ? 'wrote' : 'checked'} ${names.length} fixtures, ${cases} cases, ${pairs} pair scores against the shipped clustering.js + locus_resolve.js`);
}
