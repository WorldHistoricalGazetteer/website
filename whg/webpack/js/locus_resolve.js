// locus_resolve.js — resolve an (unminted) locus recipe against a result set
// (place#172, place#325 Stage 2). Pure: no DOM, no fetch, no map.
//
// A recipe names (a) the retrieval that fetched the candidates, (b) the LITERAL
// clustering parameters, and (c) the member ids of the one cluster that was
// meant. Resolving re-runs the clusterer over the CURRENT retrieval with those
// literal parameters and then picks the cluster the members point at. The
// members are not the answer: they settle WHICH cluster, the re-run says what
// it currently holds.
//
// THE SELECTION RULE IS A FIXED CONTRACT (place#172: "a fixed contract rather
// than a per-locus field: change it later and every PID already issued may
// resolve to a different cluster"). It is:
//
//   1. surviving = recipe members that are in the current result set;
//   2. each cluster scores |surviving ∩ cluster|; candidates = the clusters
//      with the greatest score, if that score is >= 1;
//   3. one candidate → it is the answer ("plurality");
//   4. several candidates (a tie) → the one whose members' centroid is nearest
//      the centroid of ALL surviving members' points ("tie_centroid"); when no
//      point is available, the candidate holding the code-point-smallest
//      surviving member id ("tie_first_member"), so the choice is deterministic;
//   5. no survivor → nothing is selected ("no_members_survive"), with the
//      re-run's cluster count so the reader can see the recipe still retrieves.
//
// Clustering parameters are taken ONLY from the recipe. The gateway's shipped
// clustering_params and toponym_stoplist are NOT consulted here: a recipe that
// leaned on "today's defaults" would change meaning when they are recalibrated.

import { clusterHits } from './clustering.js';

/** The clusterHits() arguments a recipe's literal parameters imply. */
export function clusteringArgs(recipe) {
	const c = recipe.clustering;
	return {
		params: {
			weights: { ...c.weights },
			thresholds: { theta_query: c.theta, tau_name: c.tau_name, tau_link: c.tau_link },
			same_ns_penalty: c.same_ns_penalty,
			calibrated: false,
		},
		weights: { ...c.weights },
		theta: c.theta,
		sameNsPenalty: c.same_ns_penalty,
		respectDistinct: c.respect_distinct,
		stoplist: c.stoplist.slice(),
	};
}

/** Re-run the clusterer over `hits`/`edges` with the recipe's literal parameters. */
export function runRecipe(recipe, hits, edges) {
	return clusterHits({ hits: hits || [], edges: edges || [], ...clusteringArgs(recipe) });
}

function latLngOf(hit) {
	if (!hit) return null;
	if (Array.isArray(hit.point) && hit.point.length >= 2) return [hit.point[0], hit.point[1]];
	if (Array.isArray(hit.repr_point) && hit.repr_point.length >= 2) return [hit.repr_point[1], hit.repr_point[0]];
	return null;
}

function centroid(points) {
	if (!points.length) return null;
	let lat = 0, lng = 0;
	for (const [a, b] of points) { lat += a; lng += b; }
	return [lat / points.length, lng / points.length];
}

function dist2(a, b) {
	const dlat = a[0] - b[0];
	const dlng = a[1] - b[1];
	return dlat * dlat + dlng * dlng;
}

function compareCodePoints(a, b) {
	const ia = a[Symbol.iterator](), ib = b[Symbol.iterator]();
	for (;;) {
		const x = ia.next(), y = ib.next();
		if (x.done && y.done) return 0;
		if (x.done) return -1;
		if (y.done) return 1;
		const cx = x.value.codePointAt(0), cy = y.value.codePointAt(0);
		if (cx !== cy) return cx < cy ? -1 : 1;
	}
}

/** Lower-cased namespace, verbatim local part — the recipe's member-id form. */
export function canonicalMemberId(pid) {
	let s = String(pid == null ? '' : pid).trim();
	if (s.startsWith('place:')) s = s.slice('place:'.length);
	const i = s.indexOf(':');
	return i < 0 ? s : s.slice(0, i).toLowerCase() + ':' + s.slice(i + 1);
}

/**
 * Pick the cluster the recipe meant.
 *
 * @param {Array}  clusters   clusterHits().clusters ({root, members, memberIds})
 * @param {Array}  members    recipe.members (canonical ids)
 * @param {Array}  hits       the result set the clusters were built over
 * @returns {{cluster:object|null, reason:string, surviving:string[], missing:string[],
 *            score:number, candidates:number}}
 */
export function selectLocusCluster(clusters, members, hits) {
	const byId = new Map();
	for (const h of (hits || [])) {
		const id = h && (h.id ?? h.place_id ?? h.pid);
		if (id != null) byId.set(canonicalMemberId(id), h);
	}
	const want = Array.from(new Set((members || []).map(canonicalMemberId))).sort(compareCodePoints);
	const surviving = want.filter((m) => byId.has(m));
	const missing = want.filter((m) => !byId.has(m));
	const base = { surviving, missing, candidates: 0, score: 0 };
	if (!surviving.length) return { cluster: null, reason: 'no_members_survive', ...base };

	let best = 0;
	const scored = [];
	for (const cl of (clusters || [])) {
		const ids = new Set((cl.memberIds || []).map(canonicalMemberId));
		let n = 0;
		for (const m of surviving) if (ids.has(m)) n++;
		scored.push({ cl, n, ids });
		if (n > best) best = n;
	}
	const candidates = scored.filter((s) => s.n === best && best >= 1);
	base.score = best;
	base.candidates = candidates.length;
	if (!candidates.length) return { cluster: null, reason: 'no_members_survive', ...base };
	if (candidates.length === 1) return { cluster: candidates[0].cl, reason: 'plurality', ...base };

	// Tie: nearest to the surviving members' centroid, if anything has a point.
	const target = centroid(surviving.map((m) => latLngOf(byId.get(m))).filter(Boolean));
	if (target) {
		let bestD = Infinity, pick = null;
		for (const c of candidates) {
			const cen = centroid((c.cl.members || []).map(latLngOf).filter(Boolean));
			if (!cen) continue;
			const d = dist2(cen, target);
			if (d < bestD) { bestD = d; pick = c; }
		}
		if (pick) return { cluster: pick.cl, reason: 'tie_centroid', ...base };
	}
	// No geometry anywhere: the candidate holding the code-point-smallest survivor.
	for (const m of surviving) {
		const c = candidates.find((x) => x.ids.has(m));
		if (c) return { cluster: c.cl, reason: 'tie_first_member', ...base };
	}
	return { cluster: candidates[0].cl, reason: 'tie_first_member', ...base };
}

/**
 * Resolve a recipe end to end: cluster, then select. `data` is the resolver's
 * JSON (hits, edges). Returns the clustering output plus the selection.
 */
export function resolveRecipe(recipe, data) {
	const hits = (data && data.hits) || [];
	const edges = (data && data.edges) || [];
	const run = runRecipe(recipe, hits, edges);
	const selection = selectLocusCluster(run.clusters, recipe.members, hits);
	return { ...run, selection, hits };
}

export default { clusteringArgs, runRecipe, selectLocusCluster, resolveRecipe, canonicalMemberId };
