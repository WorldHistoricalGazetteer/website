# Gateway spec: `contained_in` should ignore containers with no usable area geometry

**Component:** CRC reconciliation gateway — `/api/reconcile`, `contained_in` handling
**Reported by:** Gazetteer Workbench containment testing (place#111)
**Status:** proposed — gateway-side fix

## Summary

When a `contained_in` id **cannot be resolved to a usable area geometry** — because the id is
point-only (`has_geom = false`), nonexistent, or otherwise unresolvable — the gateway does **not**
ignore that container. Instead it applies a **degraded, container-independent filter**: the result
set shifts away from the honest unconstrained result but no longer depends on *which* container was
passed. This silently returns results that are *not* contained by anything, while looking as though
containment was applied.

Desired behaviour: an id that does not resolve to a usable area geometry should be **dropped from the
containment set**. If no usable container remains, the query should run **without spatial containment**
(identical to sending no `contained_in` at all).

## Why it matters

The Workbench reconciles a spatial hierarchy one column at a time, scoping each child by its confirmed
parent's `place_id` (`contained_in`, `relation:'within'`). Many administrative records — especially
`wd:` (Wikidata) and `ohm:` (OpenHistoricalMap) counties — carry only a point (`has_geom = false`).
For those parents the child is currently matched against a degraded set that neither excludes
out-of-region candidates nor honestly falls back, so "Whitchurch within Hampshire" and "Whitchurch
within Shropshire" return identical candidates.

## Evidence (reproductions against prod `/reconcile`)

All queries `{query, type:'place', limit, countries:['GB'], containment:'fuzzy', relation:'within'}`
plus the `contained_in` shown. Compared by candidate-id list.

1. **Polygon container genuinely filters.** `contained_in:['ukhc:CMB']` (Cambridgeshire, `has_geom:true`)
   ≠ unconstrained. ✅ Works as intended.

2. **Point-only container ignores container identity but still isn't unconstrained.** For query
   `"Whitchurch"`:
   - `['ohm:n2141018537']` (Shropshire, `has_geom:false`) **==** `['wd:Q60576135']` (Hampshire,
     `has_geom:false`) — identical results despite different containers.
   - …yet **≠** the unconstrained query. → a degraded, container-independent filter, **not** a clean
     fallback.

3. **Unresolvable ids behave the same as point-only.** A nonexistent id (`['wd:Q999999999']`) returns
   the same degraded set — no error, no fallback.

(Separately, note the gateway expects **bare** namespaced ids per the `contained_in` schema, e.g.
`ukhc:CMB`, not `place:ukhc:CMB`. The Workbench previously sent the `place:`-prefixed candidate id,
which never resolved and hit exactly this degraded path; that has been fixed client-side in
`whg/webpack/js/reconciliation.js` — `barePlaceId()`. This spec is about the *server-side* fallback
that should apply whenever an id doesn't resolve, regardless of client.)

## Root-cause hypothesis

The container-resolution step, when it cannot build a usable polygon for an id, appears to fall through
to some default containment predicate (e.g. an empty/degenerate region, or "is-contained-by-anything")
rather than removing that id from the spatial test. The gateway already determines usable geometry per
place — whg3's `api/crc_client.py` reads the gateway's per-geometry `has_geom` flag to set a candidate's
`has_geom` (`_source.has_geom = any(g.get("has_geom") for g in geometries)`), so the same signal is
available at container-resolution time.

## Desired behaviour / acceptance criteria

For each id in `contained_in`:
- resolve it to its geometry;
- **include** it in the `within`/`intersects` test only if it yields a usable **area** geometry
  (polygon/multipolygon — the same condition as `has_geom = true`);
- **drop** it otherwise (point-only, unresolvable, malformed).

Then:
- if ≥1 usable container remains → test containment against the union of those, as today;
- if **none** remain → run the query with **no spatial containment** (result identical to omitting
  `contained_in`).

Acceptance tests (query `"Whitchurch"`, `countries:['GB']`):
- `['ohm:n2141018537']` (point-only) → **==** unconstrained result.
- `['wd:Q60576135']` (point-only) → **==** unconstrained result.
- `['wd:Q999999999']` (unresolvable) → **==** unconstrained result.
- `['ukhc:CMB']` (polygon) → **≠** unconstrained (unchanged from today).
- `['ukhc:CMB','wd:Q60576135']` (one polygon + one point-only) → same as `['ukhc:CMB']` alone.

Applies to both `containment:'fuzzy'` and `containment:'exact'` (both currently exhibit the issue).

## Optional robustness

Accept and normalise a leading `place:` on `contained_in` ids (strip it), so a client passing the
canonical candidate id form doesn't silently hit the no-resolve path.
