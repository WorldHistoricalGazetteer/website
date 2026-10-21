// atlas_cluster_defaults.js — the Atlas's literal clustering and search defaults,
// in ONE place (place#325 Stage 2: the Record page's "cluster at default settings"
// link).
//
// atlas.js seeds its sliders and its search-type dropdown from these. The server
// mirrors them as search.locus.ATLAS_DEFAULT_CLUSTERING / ATLAS_DEFAULT_MATCH_MODE:
// the Record page's cluster link uses the match mode and theta always, and the
// rest only as a labelled fallback when the gateway's calibrated params are
// unavailable.
// places.tests_record_page runs this module under Node and asserts the two are
// equal, so changing a number here without the mirror fails a test.
//
// These are the FALLBACK literals. On a live search the Atlas seeds its weight
// sliders from the gateway's calibrated clustering_params and auto-fits θ to the
// result set (atlas.js seedClusterControls). No DOM, no dependencies beyond
// clustering.js.

import { DEFAULT_PARAMS } from './clustering.js';

/* Default merge sensitivity (θ_query). Bumped from the calibration default 0.55
   to a more disambiguating 0.65: empirically (spread-vs-θ sweep on ambiguous
   names) the max intra-cluster geographic spread collapses from ~30 km to ~4 km
   between 0.55 and 0.65 — tightening loose same-name merges — while 0.70 starts
   over-splitting genuine multi-source places into pairs. */
export const ATLAS_DEFAULT_THETA = 0.65;

/* Main Places search match type: exact | starts | in (contains) | phonetic. */
export const ATLAS_DEFAULT_MATCH_MODE = 'in';

/* The literal clustering block of a locus recipe at the Atlas defaults. */
export const ATLAS_DEFAULT_CLUSTERING = Object.freeze({
	weights: Object.freeze({ ...DEFAULT_PARAMS.weights }),
	theta: ATLAS_DEFAULT_THETA,
	tau_name: DEFAULT_PARAMS.thresholds.tau_name,
	tau_link: DEFAULT_PARAMS.thresholds.tau_link,
	same_ns_penalty: DEFAULT_PARAMS.same_ns_penalty,
	respect_distinct: true,   // clusterHits' default; the Atlas never overrides it
});

export default { ATLAS_DEFAULT_THETA, ATLAS_DEFAULT_MATCH_MODE, ATLAS_DEFAULT_CLUSTERING };
