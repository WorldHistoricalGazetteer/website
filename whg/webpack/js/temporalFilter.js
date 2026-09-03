// temporalFilter.js — the two-mode date-filter predicate (place#176, place#234).
//
// A PURE module on purpose: no imports, no side effects, no maplibregl. Both
// filter paths need it — whg_maplibre.js for the dynamic gazetteer layers and
// heroMap.js for the base-style boundary layers — and heroMap must NOT import
// whg_maplibre, which is its own webpack entry that the Atlas page already loads
// separately: doing so bundled a second copy of it into atlas.bundle.js (+100 KB)
// and would have evaluated its prototype patches and CSS twice.

// ── Two-mode temporal filtering of gazetteer tile layers (place#176 §3) ──
//
// The tiles carry up to four per-feature year props (indexing
// ``processing/generate_tiles.py::_temporal_props``):
//
//   start / end          the POSSIBLE envelope — outer bounds, with a sentinel
//                        on any side nothing bounds, so both are present on any
//                        feature with temporal information at all.
//   start_def / end_def  the ATTESTED CORE — inner bounds. Present only when a
//                        timespan pins both, which is exactly when a
//                        *definitely* test can be satisfied.
//   (all four absent)    genuinely undated.
//
// Both modes are an interval-OVERLAP test, mirroring the gateway clause by clause
// (``gateway/es_helpers.py::_temporal_filter``): the feature must have started at
// or before the window ends and still been there at or after it begins. The mode
// only selects WHICH bound stands for "the feature's start" — the envelope's
// outer bound for *possibly*, the core's inner bound for *definitely*. Keeping
// that identical to the server is the whole point: the map and the result list
// must not disagree about what is in the window.
//
// ⚠️ The tile builder's own comment sketches *definitely* as a CONTAINMENT test
// (``start_def <= from && end_def >= to``, i.e. alive throughout the window).
// That is not what the gateway does and not what the deployed hit-mirror
// (``atlas.js::temporalHitPasses``) does. Overlap is authoritative here.
//
// The sentinels are ±9999 and are compared as ordinary numbers — deliberately no
// magic-value branch. Note they are NOT reserved: -9999 is inside every
// namespace's admissible year range, and +9999 is inside ``po``'s, so a real
// reading can collide with one. The collision is benign in both directions: it
// widens a feature's envelope only over [-inf, -9999] or [9999, +inf], windows
// the Atlas cannot express, and in both cases the feature already matched.

/**
 * The filter clause for a date window in the given mode, or null for no filter.
 *
 * @param {string} mode — ``off`` | ``possibly`` | ``definitely``
 * @param {number} fromYear — window start
 * @param {number} toYear — window end
 * @returns {Array|null} a MapLibre filter expression, or null when unfiltered
 */
export function temporalFilterClause(mode, fromYear, toYear) {
	if (mode !== 'possibly' && mode !== 'definitely') return null;
	if (!Number.isFinite(fromYear) || !Number.isFinite(toYear)) return null;
	if (mode === 'definitely') {
		// No core ⇒ no window can ever be satisfied — the client's half of the
		// gateway's ``unbounded_passes=False``. The ``has`` guards also keep
		// ``<=``/``>=`` from ever seeing a null operand.
		return ['all',
			['has', 'start_def'], ['has', 'end_def'],
			['<=', ['get', 'start_def'], toYear],
			['>=', ['get', 'end_def'], fromYear],
		];
	}
	// Undated features are unbounded, so nothing can rule them out of a
	// *possibly* window — matching the gateway's ``undated`` branch, which the
	// Atlas turns on precisely when the mode is *possibly*.
	return ['any',
		['!', ['has', 'start']],
		['all',
			['has', 'start'], ['has', 'end'],
			['<=', ['get', 'start'], toYear],
			['>=', ['get', 'end'], fromYear],
		],
	];
}

/**
 * Combine a layer's own filter with a temporal clause.
 * A base of ``['all', ...]`` is flattened rather than nested so the result stays
 * readable in the MapLibre inspector. Exported because the base-style boundary
 * layers compose their filters through ``heroMap.showBoundaries`` rather than
 * through the ``registerTemporalLayer`` registry, and must compose them the
 * same way (place#234).
 */
export function withTemporalClause(base, clause) {
	if (!clause) return base || null;
	if (!base) return clause;
	if (Array.isArray(base) && base[0] === 'all') return [...base, clause];
	return ['all', base, clause];
}
