// clustering.js — client-side pair scorer + Union-Find clustering for WHG.
//
// Phase 1 (this file): the pure, UI-agnostic core. It consumes the fuel the
// CRC gateway already ships (live on prod 2026-07-11) and produces clusters.
// No DOM, no map, no sliders — those are Phase 2 (see the handoff plan).
//
// Design (indexing `developer/plan-outstanding-2026-07.md` §1, "FOR THE whg3
// AGENT"): ALL scoring + clustering runs in the browser; the gateway is a
// retrieve-and-ship service. This module is shared by the Atlas UI and the
// local-first Map-your-Data / Collaborative Workbench, so it must be
// embedding-source-agnostic and degrade gracefully when a signal is absent.
//
// The five pair signals and the composite mirror the offline calibration math
// EXACTLY so the browser scores as the gateway calibrated — source of truth:
//   indexing `clustering/calibrate_params.py` (haversine_km, spatial_signal,
//   temporal_overlap, wu_palmer, type_signal, cosine_byte) and
//   `clustering/data/clustering_params.json` (weights + thresholds).
// Any change to the maths here must track that file.

// ---------------------------------------------------------------------------
// Default params — mirror clustering/data/clustering_params.json (calibrated:
// false). The gateway ships `clustering_params` per query when
// include_clustering_fields=true; prefer the shipped copy, fall back to these
// so the module works before the first fuelled response.
// ---------------------------------------------------------------------------
export const DEFAULT_PARAMS = Object.freeze({
	version: 0,
	calibrated: false,
	weights: { name: 0.35, spatial: 0.20, temporal: 0.15, type: 0.15, link: 0.15 },
	thresholds: {
		theta_query: 0.55,
		theta_bridge: 0.65,
		theta_synth: 0.70,
		theta_synth_structural: 0.60,
		tau_name: 0.75,
		tau_link: 1.0,
	},
	// Browser-side heuristic (NOT part of the offline calibration): a single
	// gazetteer rarely lists the same place twice, so a cluster should hold at
	// most one record per namespace. A merge that would put two same-namespace
	// records in one cluster is BLOCKED unless the bridging pair's composite
	// clears θ by this margin (the "other signals are very strong" override).
	// Applied cluster-wide (catches transitive collisions via cross-namespace
	// hubs), so it is a constraint in clusterHits(), not a pair penalty. 0
	// disables it. Hard-link (sameAs/exactMatch/closeMatch) merges bypass it.
	same_ns_penalty: 0.15,
});

// Hard-link relation types that force a MERGE (authoritative co-reference).
const MERGE_RELATIONS = new Set(['sameAs', 'exactMatch', 'closeMatch']);
// `distinct` is an explicit "not the same place" assertion. Whether it is a
// hard split or a strong negative is an OPEN QUESTION (plan §1 / place#25);
// default to respecting it as a cannot-link, but keep it toggleable.
const SPLIT_RELATION = 'distinct';

// Generic high-frequency names (the gateway's toponym_stoplist) are weak
// co-reference evidence — many unrelated places share them — so the name
// signal's weight is scaled by this factor when either hit's matched toponym
// is on the stoplist. Down-weight, not drop (plan §1).
const STOPLIST_NAME_FACTOR = 0.2;

// ---------------------------------------------------------------------------
// Signal functions — one-to-one with clustering/calibrate_params.py.
// Each returns [0,1]; each returns 0.0 when its evidence is missing on either
// side (so a dropped term contributes nothing before renormalisation).
// ---------------------------------------------------------------------------

/** Great-circle distance in km. Mirrors haversine_km. */
export function haversineKm(lat1, lon1, lat2, lon2) {
	const R = 6371.0;
	const toRad = (d) => (d * Math.PI) / 180.0;
	const dLat = toRad(lat2 - lat1);
	const dLon = toRad(lon2 - lon1);
	const a = Math.sin(dLat / 2) ** 2
		+ Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
	return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

/** Distance → [0,1]: 1 at 0 km, 0.5 at half_life_km. null km → 0. */
export function spatialSignal(km, halfLifeKm = 25.0) {
	if (km === null || km === undefined || Number.isNaN(km)) return 0.0;
	return 1.0 / (1.0 + km / halfLifeKm);
}

/** Interval-overlap (Jaccard) over [start,end] year ranges, either bound
 *  possibly null = UNBOUNDED (an open-start boundary such as ukhc, or an
 *  ongoing one such as un). Either side wholly undated → 0.
 *
 *  An unknown bound adopts the other record's on that side, so it neither adds
 *  nor removes span: [null,1974] vs [1889,1974] → 1.0 (nothing distinguishes
 *  them), vs [1200,1250] → ~0.06 (what is known barely coincides). Where
 *  neither record bounds a side, both are anchored to the same year and that
 *  side contributes nothing to intersection or union.
 *
 *  ⚠️ Must stay identical to `temporal_overlap` in indexing's
 *  clustering/calibrate_params.py — the browser scores as that calibrated
 *  (place#169). */
export function temporalOverlap(a, b) {
	if (!a || !b || a.length < 2 || b.length < 2) return 0.0;
	const a0 = a[0] ?? null, a1 = a[1] ?? null;
	const b0 = b[0] ?? null, b1 = b[1] ?? null;
	if ((a0 === null && a1 === null) || (b0 === null && b1 === null)) return 0.0;

	let sA = a0 !== null ? a0 : b0;
	let sB = b0 !== null ? b0 : a0;
	let eA = a1 !== null ? a1 : b1;
	let eB = b1 !== null ? b1 : a1;

	if (sA === null || sB === null) {          // neither record has a start
		if (eA === null || eB === null) return 0.0;   // nothing bounded either side
		sA = sB = Math.min(eA, eB);
	}
	if (eA === null || eB === null) {          // neither record has an end
		eA = eB = Math.max(sA, sB);
	}

	const lo = Math.max(sA, sB);
	const hi = Math.min(eA, eB);
	if (hi < lo) return 0.0;
	const inter = hi - lo;
	const union = Math.max(eA, eB) - Math.min(sA, sB);
	if (union <= 0) return 1.0; // both the same single year
	return inter / union;
}

/** Wu-Palmer over two materialised AAT paths (dot-separated root→leaf ids):
 *  2·depth(LCA) / (depth(a)+depth(b)); LCA = longest shared prefix. */
export function wuPalmer(pathA, pathB) {
	const ca = pathA.split('.');
	const cb = pathB.split('.');
	let lca = 0;
	const n = Math.min(ca.length, cb.length);
	for (let i = 0; i < n; i++) {
		if (ca[i] === cb[i]) lca += 1; else break;
	}
	const da = ca.length;
	const db = cb.length;
	if (da + db === 0) return 0.0;
	return (2.0 * lca) / (da + db);
}

/** Best Wu-Palmer over every AAT-path pair. Either side empty → 0. */
export function typeSignal(pathsA, pathsB) {
	if (!pathsA || !pathsB || !pathsA.length || !pathsB.length) return 0.0;
	let best = 0.0;
	for (const pa of pathsA) {
		for (const pb of pathsB) {
			const s = wuPalmer(pa, pb);
			if (s > best) best = s;
		}
	}
	return best;
}

/** Cosine similarity of two int8-quantised embeddings. Mirrors cosine_byte. */
export function cosineByte(a, b) {
	if (!a || !b || a.length !== b.length || !a.length) return 0.0;
	let dot = 0, na = 0, nb = 0;
	for (let i = 0; i < a.length; i++) {
		dot += a[i] * b[i];
		na += a[i] * a[i];
		nb += b[i] * b[i];
	}
	if (na === 0 || nb === 0) return 0.0;
	return dot / (Math.sqrt(na) * Math.sqrt(nb));
}

// ---------------------------------------------------------------------------
// Field extraction — pluggable so the same core serves the Atlas gateway
// payload and the Workbench's local records. Defaults read the gateway field
// names (plan §1). `spatial` needs a lat/lon; the gateway ships `h3` (a cell),
// so the caller may pass `h3ToLatLng` (e.g. from h3-js) — absent → spatial
// degrades out. All extractors return null/[] when the field is missing.
// ---------------------------------------------------------------------------
function defaultAccessors(opts = {}) {
	const h3ToLatLng = opts.h3ToLatLng || null;
	return {
		id: (h) => h.id ?? h.place_id ?? h.pid,
		latLng: (h) => {
			if (Array.isArray(h.point) && h.point.length >= 2) return [h.point[0], h.point[1]];
			if (Array.isArray(h.repr_point) && h.repr_point.length >= 2) return [h.repr_point[1], h.repr_point[0]]; // [lon,lat]→[lat,lon]
			if (h.h3 && h3ToLatLng) { const ll = h3ToLatLng(h.h3); return ll ? [ll[0], ll[1]] : null; }
			return null;
		},
		temporal: (h) => (Array.isArray(h.temporal_range) ? h.temporal_range : null),
		aatPaths: (h) => (Array.isArray(h.aat_paths) ? h.aat_paths : []),
		// The CRC gateway attaches the int8 Symphonym embedding PER NAME
		// (hit.names[].phon_emb), not on the hit. Use the query-matched
		// toponym's embedding when identifiable (most relevant to a search),
		// else the first name that carries one (the calibration's
		// "first attested toponym" representative).
		embedding: (h) => {
			if (Array.isArray(h.phon_emb)) return h.phon_emb;
			const names = Array.isArray(h.names) ? h.names : [];
			const qname = h.query_match && h.query_match.name;
			if (qname) {
				const m = names.find(n => n && n.label === qname && Array.isArray(n.phon_emb));
				if (m) return m.phon_emb;
			}
			const first = names.find(n => n && Array.isArray(n.phon_emb));
			return first ? first.phon_emb : null;
		},
		// Representative toponym string (for stoplist lookup): the query-matched
		// name, else the first name label, else the title.
		nameStr: (h) => (h.query_match && h.query_match.name)
			|| (Array.isArray(h.names) && h.names[0] && h.names[0].label)
			|| h.title || null,
		// Source gazetteer (for the same-namespace repulsion, below).
		namespace: (h) => h.namespace ?? h.ns ?? null,
	};
}

// ---------------------------------------------------------------------------
// Union-Find
// ---------------------------------------------------------------------------
export class UnionFind {
	constructor(ids = []) {
		this.parent = new Map();
		this.rank = new Map();
		for (const id of ids) this.add(id);
	}
	add(id) {
		if (!this.parent.has(id)) { this.parent.set(id, id); this.rank.set(id, 0); }
	}
	find(id) {
		this.add(id);
		let root = id;
		while (this.parent.get(root) !== root) root = this.parent.get(root);
		// path compression
		let cur = id;
		while (this.parent.get(cur) !== root) { const nxt = this.parent.get(cur); this.parent.set(cur, root); cur = nxt; }
		return root;
	}
	union(a, b) {
		const ra = this.find(a); const rb = this.find(b);
		if (ra === rb) return ra;
		const raRank = this.rank.get(ra); const rbRank = this.rank.get(rb);
		let root, child;
		if (raRank < rbRank) { root = rb; child = ra; }
		else if (raRank > rbRank) { root = ra; child = rb; }
		else { root = ra; child = rb; this.rank.set(ra, raRank + 1); }
		this.parent.set(child, root);
		return root;
	}
	connected(a, b) { return this.find(a) === this.find(b); }
	groups() {
		const out = new Map();
		for (const id of this.parent.keys()) {
			const r = this.find(id);
			if (!out.has(r)) out.set(r, []);
			out.get(r).push(id);
		}
		return out;
	}
}

// NB: the separator is written as the ESCAPE `\u0000`, never as a literal NUL
// byte. A literal one makes `file` report this module as binary data, and grep
// then SILENTLY SKIPS it — so a codebase-wide search for any symbol defined here
// returns a false negative. Same runtime value; the file stays greppable.
const pairKey = (a, b) => (a < b ? `${a}\u0000${b}` : `${b}\u0000${a}`);

/**
 * Score one pair of hits. Graceful degradation: any signal whose evidence is
 * absent is dropped and the remaining weights are renormalised, so the
 * composite always lies in [0,1] regardless of which fields the payload
 * carried. `s.l` is 1.0 when a merge hard-link edge exists for the pair.
 *
 * @returns {{composite:number, signals:object, weightUsed:number}}
 */
export function scorePair(ha, hb, ctx) {
	const { acc, weights, edgeIndex, stopSet } = ctx;
	const signals = {};
	const w = {};

	// name (s.n) — needs both embeddings; down-weighted when either matched
	// toponym is a generic high-frequency name (toponym_stoplist).
	const ea = acc.embedding(ha); const eb = acc.embedding(hb);
	if (ea && eb) {
		signals.name = cosineByte(ea, eb);
		w.name = weights.name;
		if (stopSet && stopSet.size) {
			const na = (acc.nameStr(ha) || '').toLowerCase();
			const nb = (acc.nameStr(hb) || '').toLowerCase();
			// Scale the signal VALUE (not the weight): under graceful-degradation
			// renormalisation, scaling the weight is a no-op when name is the only
			// present signal, whereas scaling the value correctly weakens the match.
			if (stopSet.has(na) || stopSet.has(nb)) signals.name *= STOPLIST_NAME_FACTOR;
		}
	}

	// spatial (s.sp) — needs both lat/lng
	const la = acc.latLng(ha); const lb = acc.latLng(hb);
	if (la && lb) { signals.spatial = spatialSignal(haversineKm(la[0], la[1], lb[0], lb[1])); w.spatial = weights.spatial; }

	// temporal (s.t) — needs both ranges
	const ta = acc.temporal(ha); const tb = acc.temporal(hb);
	if (ta && tb) { signals.temporal = temporalOverlap(ta, tb); w.temporal = weights.temporal; }

	// type (s.ty) — needs both AAT path sets
	const pa = acc.aatPaths(ha); const pb = acc.aatPaths(hb);
	if (pa.length && pb.length) { signals.type = typeSignal(pa, pb); w.type = weights.type; }

	// link (s.l) — presence of a merge hard-link edge for the pair. Always
	// defined (0 when no edge), so the link weight always participates.
	const edge = edgeIndex ? edgeIndex.get(pairKey(acc.id(ha), acc.id(hb))) : null;
	signals.link = edge && MERGE_RELATIONS.has(edge.relation_type) ? 1.0 : 0.0;
	w.link = weights.link;

	let wSum = 0, acc2 = 0;
	for (const k of Object.keys(w)) { wSum += w[k]; acc2 += w[k] * signals[k]; }
	const composite = wSum > 0 ? acc2 / wSum : 0.0;
	return { composite, signals, weightUsed: wSum };
}

/**
 * Cluster a result set. Order: (1) forced merges from authoritative hard-link
 * edges; (2) collect `distinct` cannot-link constraints; (3) threshold merges
 * over all candidate pairs whose composite ≥ θ_query and which no cannot-link
 * blocks. Synthetic-edge/bridge passes (θ_bridge/θ_synth) are Phase 2.
 *
 * @param {object} p
 * @param {Array}  p.hits              gateway hits[] (or Workbench records)
 * @param {Array}  [p.edges]           gateway edges[] (include_hard_links)
 * @param {object} [p.params]          shipped clustering_params (falls back to DEFAULT_PARAMS)
 * @param {object} [p.weights]         weight overrides (slider values) — merged over params.weights
 * @param {number} [p.theta]           merge threshold override — else params.thresholds.theta_query
 * @param {boolean}[p.respectDistinct] treat `distinct` as cannot-link (default true; see open Q)
 * @param {object} [p.accessors]       field-extractor overrides (e.g. {h3ToLatLng})
 * @returns {{clusters:Array, assignments:Map, params:object, debug:object}}
 */
export function clusterHits(p) {
	const params = p.params || DEFAULT_PARAMS;
	const weights = Object.assign({}, params.weights || DEFAULT_PARAMS.weights, p.weights || {});
	const theta = p.theta ?? (params.thresholds || DEFAULT_PARAMS.thresholds).theta_query;
	const respectDistinct = p.respectDistinct !== false;
	const acc = Object.assign(defaultAccessors(p.accessors || {}), p.accessors || {});
	// Generic-name down-weighting set (lower-cased toponym_stoplist).
	const stopSet = new Set((p.stoplist || []).map((s) => String(s).toLowerCase()));
	// Same-namespace repulsion margin (override → params → default). A same-
	// gazetteer collision is allowed only when the pair's composite ≥ θ + margin.
	const sameNsMargin = p.sameNsPenalty ?? (params.same_ns_penalty ?? DEFAULT_PARAMS.same_ns_penalty);
	const hits = p.hits || [];

	const ids = hits.map((h) => acc.id(h)).filter((x) => x != null);
	const uf = new UnionFind(ids);

	// Edge index (both directions of assertion collapse to one canonical key).
	const edgeIndex = new Map();
	const cannotLink = [];
	for (const e of (p.edges || [])) {
		edgeIndex.set(pairKey(e.a, e.b), e);
		if (MERGE_RELATIONS.has(e.relation_type)) {
			uf.union(e.a, e.b); // forced merge — may pull in an off-result id (1-hop)
		} else if (e.relation_type === SPLIT_RELATION && respectDistinct) {
			cannotLink.push([e.a, e.b]);
		}
	}

	// A merge is blocked if it would put a cannot-link pair in one component.
	const violatesCannotLink = (rootA, rootB) => {
		for (const [x, y] of cannotLink) {
			const rx = uf.find(x); const ry = uf.find(y);
			if ((rx === rootA && ry === rootB) || (rx === rootB && ry === rootA)) return true;
		}
		return false;
	};

	// Per-component namespace multiset — a gazetteer rarely lists a place twice,
	// so a cluster should hold ≤1 record per namespace. Seeded AFTER forced
	// merges so it reflects the hard-linked components. `find()` always resolves
	// to a live root, so stale non-root entries left after a union are harmless.
	const nsById = new Map();
	for (const h of hits) { const id = acc.id(h); if (id != null && acc.namespace) nsById.set(id, acc.namespace(h)); }
	const nsByRoot = new Map();
	const addNs = (root, ns) => {
		if (ns == null) return;
		if (!nsByRoot.has(root)) nsByRoot.set(root, new Map());
		const m = nsByRoot.get(root); m.set(ns, (m.get(ns) || 0) + 1);
	};
	for (const h of hits) { const id = acc.id(h); if (id != null) addNs(uf.find(id), nsById.get(id)); }
	// Would merging components ra,rb collide two records of one namespace?
	const wouldCollideNamespace = (ra, rb) => {
		const ma = nsByRoot.get(ra); const mb = nsByRoot.get(rb);
		if (!ma || !mb) return false;
		for (const ns of ma.keys()) { if (mb.has(ns)) return true; }
		return false;
	};

	// Threshold merges over all in-result pairs (O(n²); fine for a result page).
	const scored = [];
	for (let i = 0; i < hits.length; i++) {
		for (let j = i + 1; j < hits.length; j++) {
			const r = scorePair(hits[i], hits[j], { acc, weights, edgeIndex, stopSet });
			if (r.composite >= theta) scored.push({ a: acc.id(hits[i]), b: acc.id(hits[j]), ...r });
		}
	}
	// Merge highest-composite first so cannot-link blocks the weakest links.
	scored.sort((x, y) => y.composite - x.composite);
	let sameNsBlocked = 0;
	for (const s of scored) {
		const ra = uf.find(s.a); const rb = uf.find(s.b);
		if (ra === rb) continue;
		if (respectDistinct && violatesCannotLink(ra, rb)) continue;
		// Same-gazetteer repulsion: block a merge that would put two records of
		// one namespace in a cluster, unless THIS pair is very strong (≥ θ+margin).
		if (sameNsMargin > 0 && s.composite < theta + sameNsMargin && wouldCollideNamespace(ra, rb)) { sameNsBlocked++; continue; }
		const root = uf.union(s.a, s.b);
		// Fold the absorbed component's namespace multiset into the surviving root.
		const other = root === ra ? rb : ra;
		if (other !== root) {
			const om = nsByRoot.get(other);
			if (om) {
				const rm = nsByRoot.get(root) || new Map();
				for (const [ns, c] of om) rm.set(ns, (rm.get(ns) || 0) + c);
				nsByRoot.set(root, rm);
				nsByRoot.delete(other);
			}
		}
	}

	// Assemble clusters (only over the actual result hits; off-result 1-hop ids
	// that got unioned in are recorded on the cluster but not treated as hits).
	const byRoot = new Map();
	const assignments = new Map();
	for (const h of hits) {
		const id = acc.id(h);
		const root = uf.find(id);
		assignments.set(id, root);
		if (!byRoot.has(root)) byRoot.set(root, { root, members: [], memberIds: [] });
		byRoot.get(root).members.push(h);
		byRoot.get(root).memberIds.push(id);
	}
	const clusters = Array.from(byRoot.values()).sort((a, b) => b.members.length - a.members.length);

	return {
		clusters,
		assignments,
		params: { weights, theta, sameNsMargin, calibrated: !!params.calibrated },
		debug: {
			hitCount: hits.length,
			edgeCount: (p.edges || []).length,
			forcedMerges: (p.edges || []).filter((e) => MERGE_RELATIONS.has(e.relation_type)).length,
			cannotLinks: cannotLink.length,
			thresholdMerges: scored.length,
			sameNsMargin,
			sameNsBlocked,
			clusterCount: clusters.length,
		},
	};
}

/**
 * Best-guess merge threshold (θ) for THIS result set, so disambiguation adapts
 * to the query's own structure. Scores every candidate pair, then finds the
 * widest gap ("valley") in the plausibly-mergeable score band — the natural
 * split between co-referent duplicates (high composite) and distinct same-name
 * places (lower). Clamped to a sane window; falls back to `fallback` when the
 * set is too small or has no clear valley (where auto-fit would be noise).
 *
 * @returns {{theta:number, reason:'gap'|'sparse'|'no-gap', gap:number, n:number}}
 */
export function suggestTheta(p) {
	const fallback = p.fallback ?? 0.65;
	const LO = p.min ?? 0.55, HI = p.max ?? 0.80;
	const FLOOR = p.floor ?? 0.45;   // ignore clearly-unrelated pairs
	const MIN_PAIRS = p.minPairs ?? 8;
	const MIN_GAP = p.minGap ?? 0.03;
	const params = p.params || DEFAULT_PARAMS;
	const weights = Object.assign({}, params.weights || DEFAULT_PARAMS.weights, p.weights || {});
	const acc = Object.assign(defaultAccessors(p.accessors || {}), p.accessors || {});
	const stopSet = new Set((p.stoplist || []).map((s) => String(s).toLowerCase()));
	const edgeIndex = new Map();
	for (const e of (p.edges || [])) edgeIndex.set(pairKey(e.a, e.b), e);
	const H = p.hits || [];

	const cand = [];
	for (let i = 0; i < H.length; i++) {
		for (let j = i + 1; j < H.length; j++) {
			const c = scorePair(H[i], H[j], { acc, weights, edgeIndex, stopSet }).composite;
			if (c >= FLOOR) cand.push(c);
		}
	}
	if (cand.length < MIN_PAIRS) return { theta: fallback, reason: 'sparse', gap: 0, n: cand.length };
	cand.sort((a, b) => a - b);
	let bestGap = 0, bestTheta = null;
	for (let k = 1; k < cand.length; k++) {
		const mid = (cand[k] + cand[k - 1]) / 2;
		if (mid < LO || mid > HI) continue;
		const gap = cand[k] - cand[k - 1];
		if (gap > bestGap) { bestGap = gap; bestTheta = mid; }
	}
	if (bestTheta == null || bestGap < MIN_GAP) return { theta: fallback, reason: 'no-gap', gap: bestGap, n: cand.length };
	const theta = Math.min(HI, Math.max(LO, Math.round(bestTheta * 100) / 100));
	return { theta, reason: 'gap', gap: Math.round(bestGap * 1000) / 1000, n: cand.length };
}

export default { clusterHits, scorePair, suggestTheta, UnionFind, DEFAULT_PARAMS };
