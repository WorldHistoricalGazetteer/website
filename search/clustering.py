"""Server-side twin of the Atlas clusterer (place#325, SG ruling 2026-10-21:
"port the clusterer to Python with parity tests").

ONE behaviour, TWO implementations, proven equal:

* ``whg/webpack/js/clustering.js`` (``clusterHits`` / ``scorePair``) and
  ``whg/webpack/js/locus_resolve.js`` (``selectLocusCluster`` / ``resolveRecipe``)
  stay the source for the browser, and are the CONTRACT this module reproduces;
* this module exists so ``/entity/locus:<PID>`` JSON can carry the selected
  cluster's members, which only a browser could compute before.

The JS is ported line for line, including what it does with absent evidence,
not re-derived from the offline calibration reference (indexing
``clustering/calibrate_params.py``): where the JS and that reference disagree
on a malformed input, the JS wins here, because the JS is what the Atlas ran
when every recipe was made. ``search/tests_cluster_parity.py`` asserts, on the
fixtures under ``scripts/fixtures/cluster_parity/``, that cluster assignments
and the selected cluster are IDENTICAL to what Node computes with the shipped
JS modules, and that per-pair scores agree to ``PAIR_SCORE_TOLERANCE``; it
also shows the comparison can fail. ``npm run test:cluster-parity`` is the
Node half.

Floating point: every arithmetic step is done in the same order as the JS so
that IEEE-754 doubles round identically; the only operations whose result may
differ in the last unit are ``sin``/``cos``/``atan2`` (V8's fdlibm port vs
glibc), which is why pair scores are compared to a tolerance while cluster
assignments and selection are compared exactly.

Out of contract (the JS coerces, this raises or ignores; no recipe can produce
it from a gateway response): non-numeric coordinates, non-list ``names``,
and ids that are JSON numbers rather than strings.
"""
from __future__ import annotations

import math
import time
from typing import Any, Optional

# ---------------------------------------------------------------------------
# Constants — mirror clustering.js exactly (DEFAULT_PARAMS, MERGE_RELATIONS,
# SPLIT_RELATION, STOPLIST_NAME_FACTOR).
# ---------------------------------------------------------------------------
DEFAULT_PARAMS: dict = {
    "version": 0,
    "calibrated": False,
    "weights": {"name": 0.35, "spatial": 0.20, "temporal": 0.15, "type": 0.15, "link": 0.15},
    "thresholds": {
        "theta_query": 0.55,
        "theta_bridge": 0.65,
        "theta_synth": 0.70,
        "theta_synth_structural": 0.60,
        "tau_name": 0.75,
        "tau_link": 1.0,
    },
    "same_ns_penalty": 0.15,
}

MERGE_RELATIONS = frozenset({"sameAs", "exactMatch", "closeMatch"})
SPLIT_RELATION = "distinct"
STOPLIST_NAME_FACTOR = 0.2

# Half-life of the spatial signal, km (spatialSignal's default argument).
SPATIAL_HALF_LIFE_KM = 25.0

# Absolute tolerance for per-pair score parity (composite and each signal).
# Justification: all arithmetic is correctly rounded and performed in the JS
# order except sin/cos/atan2 inside haversineKm, where V8 and glibc may differ
# by one unit in the last place (~1e-16 relative); through 1/(1+km/25) and the
# weighted mean that is a composite difference below 1e-14. 1e-9 leaves five
# orders of magnitude of headroom while being seven orders below the smallest
# constant change a port could get wrong (e.g. 0.2 -> 0.21 on the stoplist
# factor moves a composite by ~1e-3). Measured on the fixtures: see the test.
PAIR_SCORE_TOLERANCE = 1e-9


# ---------------------------------------------------------------------------
# Signal functions — one-to-one with clustering.js / calibrate_params.py.
# ---------------------------------------------------------------------------

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km (haversineKm). Operation order as the JS:
    ``d * Math.PI / 180`` not ``math.radians`` (which is the same formula but
    keeping it literal removes one thing to wonder about)."""
    r = 6371.0
    d_lat = (lat2 - lat1) * math.pi / 180.0
    d_lon = (lon2 - lon1) * math.pi / 180.0
    a = (math.sin(d_lat / 2) ** 2
         + math.cos(lat1 * math.pi / 180.0) * math.cos(lat2 * math.pi / 180.0) * math.sin(d_lon / 2) ** 2)
    return r * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def spatial_signal(km: Optional[float], half_life_km: Optional[float] = None) -> float:
    """Distance -> [0,1]: 1 at 0 km, 0.5 at half_life_km. None/NaN -> 0.
    The half-life is read at call time (not bound as a default) so the parity
    test's mutation of SPATIAL_HALF_LIFE_KM reaches it."""
    if km is None or (isinstance(km, float) and math.isnan(km)):
        return 0.0
    if half_life_km is None:
        half_life_km = SPATIAL_HALF_LIFE_KM
    return 1.0 / (1.0 + km / half_life_km)


def temporal_overlap(a, b) -> float:
    """Jaccard over [start,end] year ranges, either bound possibly None (=
    unbounded). Mirrors temporalOverlap, including: a side shorter than two
    entries scores 0 (the reference would raise); JS ``??`` reads a missing
    entry as null."""
    if a is None or b is None or len(a) < 2 or len(b) < 2:
        return 0.0
    a0, a1 = a[0], a[1]
    b0, b1 = b[0], b[1]
    if (a0 is None and a1 is None) or (b0 is None and b1 is None):
        return 0.0
    s_a = a0 if a0 is not None else b0
    s_b = b0 if b0 is not None else a0
    e_a = a1 if a1 is not None else b1
    e_b = b1 if b1 is not None else a1
    if s_a is None or s_b is None:          # neither record has a start
        if e_a is None or e_b is None:
            return 0.0                      # nothing bounded either side
        s_a = s_b = min(e_a, e_b)
    if e_a is None or e_b is None:          # neither record has an end
        e_a = e_b = max(s_a, s_b)
    lo = max(s_a, s_b)
    hi = min(e_a, e_b)
    if hi < lo:
        return 0.0
    inter = hi - lo
    union = max(e_a, e_b) - min(s_a, s_b)
    if union <= 0:
        return 1.0
    return inter / union


def wu_palmer(path_a: str, path_b: str) -> float:
    """2·depth(LCA) / (depth(a)+depth(b)) over dot-separated AAT paths."""
    ca = str(path_a).split(".")
    cb = str(path_b).split(".")
    lca = 0
    for x, y in zip(ca, cb):
        if x == y:
            lca += 1
        else:
            break
    da, db = len(ca), len(cb)
    if da + db == 0:
        return 0.0
    return (2.0 * lca) / (da + db)


def type_signal(paths_a, paths_b) -> float:
    """Best Wu-Palmer over every path pair. Either side empty -> 0."""
    if not paths_a or not paths_b:
        return 0.0
    best = 0.0
    for pa in paths_a:
        for pb in paths_b:
            s = wu_palmer(pa, pb)
            if s > best:
                best = s
    return best


def cosine_byte(a, b) -> float:
    """Cosine of two int8 embeddings (cosineByte): unequal lengths or empty ->
    0 (the reference zips, i.e. truncates). The three sums are exact integers
    in both languages, so the result is bit-identical to the JS."""
    if a is None or b is None or len(a) != len(b) or not len(a):
        return 0.0
    dot = 0
    na = 0
    nb = 0
    for x, y in zip(a, b):
        dot += x * y
        na += x * x
        nb += y * y
    if na == 0 or nb == 0:
        return 0.0
    return dot / (math.sqrt(na) * math.sqrt(nb))


# ---------------------------------------------------------------------------
# Field extraction — the defaultAccessors of clustering.js (without h3ToLatLng:
# neither the Atlas nor the locus resolver passes one, so an h3-only hit has no
# location and its spatial signal degrades out, on both sides).
# ---------------------------------------------------------------------------

def _is_list(v) -> bool:
    return isinstance(v, list)


def _js_truthy(v) -> bool:
    """JS truthiness for the values that reach ``||`` in the accessors: null,
    undefined, '', 0, NaN and false are falsy; a list (even empty) is truthy."""
    if v is None or v is False:
        return False
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return not (v == 0 or (isinstance(v, float) and math.isnan(v)))
    if isinstance(v, str):
        return v != ""
    return True


def hit_id(h: dict):
    """``h.id ?? h.place_id ?? h.pid`` (null-coalescing: '' is an id)."""
    for k in ("id", "place_id", "pid"):
        if h.get(k) is not None:
            return h[k]
    return None


def hit_lat_lng(h: dict):
    """``[lat, lon]`` from ``point`` ([lat, lon]) or ``repr_point`` ([lon, lat]);
    None when neither is a 2+ list."""
    p = h.get("point")
    if _is_list(p) and len(p) >= 2:
        return [p[0], p[1]]
    rp = h.get("repr_point")
    if _is_list(rp) and len(rp) >= 2:
        return [rp[1], rp[0]]
    return None


def hit_temporal(h: dict):
    t = h.get("temporal_range")
    return t if _is_list(t) else None


def hit_aat_paths(h: dict) -> list:
    p = h.get("aat_paths")
    return p if _is_list(p) else []


def hit_embedding(h: dict):
    """The hit's own ``phon_emb`` if a list; else the embedding of the query-
    matched name (first name whose label === query_match.name and carries a
    list); else the first name carrying a list; else None."""
    if _is_list(h.get("phon_emb")):
        return h["phon_emb"]
    names = h.get("names")
    names = names if _is_list(names) else []
    qm = h.get("query_match")
    qname = qm.get("name") if isinstance(qm, dict) else None
    if _js_truthy(qname):
        for n in names:
            if isinstance(n, dict) and n.get("label") == qname and _is_list(n.get("phon_emb")):
                return n["phon_emb"]
    for n in names:
        if isinstance(n, dict) and _is_list(n.get("phon_emb")):
            return n["phon_emb"]
    return None


def hit_name_str(h: dict):
    """Representative toponym for the stoplist lookup: query_match.name ||
    names[0].label || title || null (JS ``||``: '' falls through)."""
    qm = h.get("query_match")
    qname = qm.get("name") if isinstance(qm, dict) else None
    if _js_truthy(qname):
        return qname
    names = h.get("names")
    if _is_list(names) and names and isinstance(names[0], dict) and _js_truthy(names[0].get("label")):
        return names[0]["label"]
    if _js_truthy(h.get("title")):
        return h["title"]
    return None


def hit_namespace(h: dict):
    """``h.namespace ?? h.ns ?? null``."""
    if h.get("namespace") is not None:
        return h["namespace"]
    if h.get("ns") is not None:
        return h["ns"]
    return None


# ---------------------------------------------------------------------------
# Union-Find — union by rank with path compression, exactly as the JS, because
# WHICH id becomes a cluster's root is part of the output being compared.
# ---------------------------------------------------------------------------

class UnionFind:
    def __init__(self, ids=()):
        self.parent: dict = {}
        self.rank: dict = {}
        for i in ids:
            self.add(i)

    def add(self, i):
        if i not in self.parent:
            self.parent[i] = i
            self.rank[i] = 0

    def find(self, i):
        self.add(i)
        root = i
        while self.parent[root] != root:
            root = self.parent[root]
        cur = i
        while self.parent[cur] != root:
            nxt = self.parent[cur]
            self.parent[cur] = root
            cur = nxt
        return root

    def union(self, a, b):
        ra = self.find(a)
        rb = self.find(b)
        if ra == rb:
            return ra
        ra_rank = self.rank[ra]
        rb_rank = self.rank[rb]
        if ra_rank < rb_rank:
            root, child = rb, ra
        elif ra_rank > rb_rank:
            root, child = ra, rb
        else:
            root, child = ra, rb
            self.rank[ra] = ra_rank + 1
        self.parent[child] = root
        return root

    def connected(self, a, b) -> bool:
        return self.find(a) == self.find(b)


def pair_key(a, b):
    """Canonical unordered key. The JS stringifies both ids
    (```${a}\\u0000${b}```) and orders by ``<``; the key is never output, so
    only "both directions give one key" and "same string form, same key"
    have to hold, and they do."""
    sa, sb = str(a), str(b)
    return (sa, sb) if sa < sb else (sb, sa)


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def _stop_set(stoplist) -> set:
    """Lower-cased stoplist (``String(s).toLowerCase()``)."""
    return {str(s).lower() for s in (stoplist or [])}


def score_pair(ha: dict, hb: dict, weights: dict, edge_index: Optional[dict], stop_set: Optional[set]) -> dict:
    """scorePair: graceful degradation (an absent signal drops with its weight),
    stoplist down-weighting of the name VALUE (x0.2), link always present.
    Returns ``{"composite", "signals", "weightUsed"}``. Signal/weight insertion
    order (name, spatial, temporal, type, link) is the JS ``Object.keys`` order
    and is the order the weighted sum is accumulated in."""
    signals: dict = {}
    w: dict = {}

    ea = hit_embedding(ha)
    eb = hit_embedding(hb)
    if ea is not None and eb is not None:           # a list, even an empty one, is present
        signals["name"] = cosine_byte(ea, eb)
        w["name"] = weights["name"]
        if stop_set:
            na = (hit_name_str(ha) or "").lower()
            nb = (hit_name_str(hb) or "").lower()
            if na in stop_set or nb in stop_set:
                signals["name"] *= STOPLIST_NAME_FACTOR

    la = hit_lat_lng(ha)
    lb = hit_lat_lng(hb)
    if la is not None and lb is not None:
        signals["spatial"] = spatial_signal(haversine_km(la[0], la[1], lb[0], lb[1]))
        w["spatial"] = weights["spatial"]

    ta = hit_temporal(ha)
    tb = hit_temporal(hb)
    if ta is not None and tb is not None:
        signals["temporal"] = temporal_overlap(ta, tb)
        w["temporal"] = weights["temporal"]

    pa = hit_aat_paths(ha)
    pb = hit_aat_paths(hb)
    if len(pa) and len(pb):
        signals["type"] = type_signal(pa, pb)
        w["type"] = weights["type"]

    edge = edge_index.get(pair_key(hit_id(ha), hit_id(hb))) if edge_index else None
    signals["link"] = 1.0 if (edge is not None and edge.get("relation_type") in MERGE_RELATIONS) else 0.0
    w["link"] = weights["link"]

    w_sum = 0
    acc2 = 0
    for k in w:
        w_sum += w[k]
        acc2 += w[k] * signals[k]
    composite = acc2 / w_sum if w_sum > 0 else 0.0
    return {"composite": composite, "signals": signals, "weightUsed": w_sum}


# ---------------------------------------------------------------------------
# Clustering
# ---------------------------------------------------------------------------

class ClusterBudgetExceeded(Exception):
    """Raised when ``cluster_hits`` passes its ``deadline`` (place#325 review #2).
    Not part of the JS twin: the browser has no shared server to protect."""


def _check_deadline(deadline):
    if deadline is not None and time.monotonic() > deadline:
        raise ClusterBudgetExceeded()


def cluster_hits(hits=None, edges=None, params=None, weights=None, theta=None,
                 respect_distinct: bool = True, same_ns_penalty=None, stoplist=None,
                 deadline=None) -> dict:
    """clusterHits. Returns ``{"clusters": [{"root", "members", "memberIds"}],
    "assignments": {id: root}, "params": {...}, "debug": {...}}`` with clusters
    ordered by size descending (stable, so first-seen root first among equals)
    and members in hit order, as the JS.

    Keyword mapping from the JS: ``p.params`` -> params, ``p.weights`` ->
    weights (merged over params.weights), ``p.theta`` -> theta,
    ``p.respectDistinct`` -> respect_distinct, ``p.sameNsPenalty`` ->
    same_ns_penalty, ``p.stoplist`` -> stoplist.

    ``deadline`` (a ``time.monotonic()`` value; Python only): checked once per
    outer row of the pair loop and once per candidate merge, raising
    :class:`ClusterBudgetExceeded` when passed. None (the default) never checks,
    so the result is exactly the unguarded one (parity).
    """
    params = params or DEFAULT_PARAMS
    wts = dict(params.get("weights") or DEFAULT_PARAMS["weights"])
    wts.update(weights or {})
    if theta is None:
        theta = (params.get("thresholds") or DEFAULT_PARAMS["thresholds"])["theta_query"]
    respect_distinct = respect_distinct is not False
    stop_set = _stop_set(stoplist)
    if same_ns_penalty is None:
        same_ns_penalty = params.get("same_ns_penalty")
        if same_ns_penalty is None:
            same_ns_penalty = DEFAULT_PARAMS["same_ns_penalty"]
    hits = hits or []
    edges = edges or []

    ids = [i for i in (hit_id(h) for h in hits) if i is not None]
    uf = UnionFind(ids)

    edge_index: dict = {}
    cannot_link: list = []
    for e in edges:
        edge_index[pair_key(e.get("a"), e.get("b"))] = e
        rel = e.get("relation_type")
        if rel in MERGE_RELATIONS:
            uf.union(e.get("a"), e.get("b"))      # forced merge; may pull in an off-result id
        elif rel == SPLIT_RELATION and respect_distinct:
            cannot_link.append((e.get("a"), e.get("b")))

    def violates_cannot_link(root_a, root_b) -> bool:
        for x, y in cannot_link:
            rx = uf.find(x)
            ry = uf.find(y)
            if (rx == root_a and ry == root_b) or (rx == root_b and ry == root_a):
                return True
        return False

    ns_by_id: dict = {}
    for h in hits:
        i = hit_id(h)
        if i is not None:
            ns_by_id[i] = hit_namespace(h)
    ns_by_root: dict = {}

    def add_ns(root, ns):
        if ns is None:
            return
        m = ns_by_root.setdefault(root, {})
        m[ns] = m.get(ns, 0) + 1

    for h in hits:
        i = hit_id(h)
        if i is not None:
            add_ns(uf.find(i), ns_by_id.get(i))

    def would_collide_namespace(ra, rb) -> bool:
        ma = ns_by_root.get(ra)
        mb = ns_by_root.get(rb)
        if not ma or not mb:
            return False
        for ns in ma:
            if ns in mb:
                return True
        return False

    scored: list = []
    n = len(hits)
    for i in range(n):
        _check_deadline(deadline)
        hi = hits[i]
        for j in range(i + 1, n):
            r = score_pair(hi, hits[j], wts, edge_index, stop_set)
            if r["composite"] >= theta:
                scored.append({"a": hit_id(hi), "b": hit_id(hits[j]), **r})
    # Highest composite first; Python's sort and V8's are both stable, so equal
    # composites keep enumeration order (i, then j).
    scored.sort(key=lambda s: -s["composite"])
    same_ns_blocked = 0
    for s in scored:
        _check_deadline(deadline)
        ra = uf.find(s["a"])
        rb = uf.find(s["b"])
        if ra == rb:
            continue
        if respect_distinct and violates_cannot_link(ra, rb):
            continue
        if same_ns_penalty > 0 and s["composite"] < theta + same_ns_penalty and would_collide_namespace(ra, rb):
            same_ns_blocked += 1
            continue
        root = uf.union(s["a"], s["b"])
        other = rb if root == ra else ra
        if other != root:
            om = ns_by_root.get(other)
            if om:
                rm = ns_by_root.get(root) or {}
                for ns, c in om.items():
                    rm[ns] = rm.get(ns, 0) + c
                ns_by_root[root] = rm
                del ns_by_root[other]

    by_root: dict = {}
    assignments: dict = {}
    for h in hits:
        i = hit_id(h)
        root = uf.find(i)
        assignments[i] = root
        cl = by_root.get(root)
        if cl is None:
            cl = by_root[root] = {"root": root, "members": [], "memberIds": []}
        cl["members"].append(h)
        cl["memberIds"].append(i)
    clusters = sorted(by_root.values(), key=lambda c: -len(c["members"]))

    return {
        "clusters": clusters,
        "assignments": assignments,
        "params": {"weights": wts, "theta": theta, "sameNsMargin": same_ns_penalty,
                   "calibrated": bool(params.get("calibrated"))},
        "debug": {
            "hitCount": len(hits),
            "edgeCount": len(edges),
            "forcedMerges": sum(1 for e in edges if e.get("relation_type") in MERGE_RELATIONS),
            "cannotLinks": len(cannot_link),
            "thresholdMerges": len(scored),
            "sameNsMargin": same_ns_penalty,
            "sameNsBlocked": same_ns_blocked,
            "clusterCount": len(clusters),
        },
    }


def score_all_pairs(hits, edges=None, weights=None, stoplist=None) -> list:
    """Every (i, j<i) pair's scorePair result, for the parity harness; not used
    by resolution. ``weights`` must be the complete five-key dict."""
    edge_index = {pair_key(e.get("a"), e.get("b")): e for e in (edges or [])}
    stop_set = _stop_set(stoplist)
    out = []
    for i in range(len(hits)):
        for j in range(i + 1, len(hits)):
            out.append((i, j, score_pair(hits[i], hits[j], weights, edge_index, stop_set)))
    return out


# ---------------------------------------------------------------------------
# Locus resolution — locus_resolve.js
# ---------------------------------------------------------------------------

def gateway_stoplist(data) -> list:
    """The stoplist a resolver must use for ``data``: its own toponym_stoplist,
    or [] when the gateway sent none (place#325 ruling 3)."""
    s = data.get("toponym_stoplist") if isinstance(data, dict) else None
    return list(s) if _is_list(s) else []


def clustering_args(recipe: dict, stoplist) -> dict:
    """The cluster_hits() keyword arguments a recipe's literal parameters imply
    (clusteringArgs). The gateway's shipped clustering_params are NOT consulted."""
    c = recipe["clustering"]
    return {
        "params": {
            "weights": dict(c["weights"]),
            "thresholds": {"theta_query": c["theta"], "tau_name": c["tau_name"], "tau_link": c["tau_link"]},
            "same_ns_penalty": c["same_ns_penalty"],
            "calibrated": False,
        },
        "weights": dict(c["weights"]),
        "theta": c["theta"],
        "same_ns_penalty": c["same_ns_penalty"],
        "respect_distinct": c["respect_distinct"],
        "stoplist": list(stoplist) if _is_list(stoplist) else [],
    }


def run_recipe(recipe: dict, hits, edges, stoplist, deadline=None) -> dict:
    return cluster_hits(hits=hits or [], edges=edges or [], deadline=deadline, **clustering_args(recipe, stoplist))


def canonical_member_id(pid) -> str:
    """Lower-cased namespace, verbatim local part; leading ``place:`` stripped."""
    s = ("" if pid is None else str(pid)).strip()
    if s.startswith("place:"):
        s = s[len("place:"):]
    i = s.find(":")
    return s if i < 0 else s[:i].lower() + ":" + s[i + 1:]


def _centroid(points):
    if not points:
        return None
    lat = 0
    lng = 0
    for a, b in points:
        lat += a
        lng += b
    return [lat / len(points), lng / len(points)]


def _dist2(a, b) -> float:
    dlat = a[0] - b[0]
    dlng = a[1] - b[1]
    return dlat * dlat + dlng * dlng


def select_locus_cluster(clusters, members, hits) -> dict:
    """selectLocusCluster: the fixed selection contract (place#172).

    1. surviving = recipe members present in ``hits`` (canonical ids, de-duplicated,
       code-point order);
    2. each cluster scores |surviving ∩ cluster|; candidates = the clusters with the
       greatest score, if >= 1;
    3. one candidate -> ``plurality``;
    4. a tie -> the candidate whose members' centroid is nearest the centroid of all
       surviving members' points (``tie_centroid``); no point anywhere -> the
       candidate holding the code-point-smallest survivor (``tie_first_member``);
    5. no survivor -> ``no_members_survive``.

    Returns ``{"cluster", "reason", "surviving", "missing", "score", "candidates"}``.
    """
    by_id: dict = {}
    for h in hits or []:
        i = hit_id(h) if isinstance(h, dict) else None
        if i is not None:
            by_id[canonical_member_id(i)] = h
    want = sorted({canonical_member_id(m) for m in (members or [])})   # code-point order
    surviving = [m for m in want if m in by_id]
    missing = [m for m in want if m not in by_id]
    base = {"surviving": surviving, "missing": missing, "candidates": 0, "score": 0}
    if not surviving:
        return {"cluster": None, "reason": "no_members_survive", **base}

    best = 0
    scored = []
    for cl in clusters or []:
        ids = {canonical_member_id(i) for i in (cl.get("memberIds") or [])}
        n = sum(1 for m in surviving if m in ids)
        scored.append((cl, n, ids))
        if n > best:
            best = n
    candidates = [s for s in scored if s[1] == best and best >= 1]
    base["score"] = best
    base["candidates"] = len(candidates)
    if not candidates:
        return {"cluster": None, "reason": "no_members_survive", **base}
    if len(candidates) == 1:
        return {"cluster": candidates[0][0], "reason": "plurality", **base}

    target = _centroid([p for p in (hit_lat_lng(by_id[m]) for m in surviving) if p is not None])
    if target is not None:
        best_d = math.inf
        pick = None
        for c in candidates:
            cen = _centroid([p for p in (hit_lat_lng(h) for h in (c[0].get("members") or [])) if p is not None])
            if cen is None:
                continue
            d = _dist2(cen, target)
            if d < best_d:
                best_d = d
                pick = c
        if pick is not None:
            return {"cluster": pick[0], "reason": "tie_centroid", **base}
    for m in surviving:
        for c in candidates:
            if m in c[2]:
                return {"cluster": c[0], "reason": "tie_first_member", **base}
    return {"cluster": candidates[0][0], "reason": "tie_first_member", **base}


def resolve_recipe(recipe: dict, data: dict, deadline=None) -> dict:
    """resolveRecipe: cluster ``data`` (hits, edges, toponym_stoplist) with the
    recipe's literal parameters and this response's stoplist, then select.
    Returns the cluster_hits output plus ``selection``, ``hits``, ``stoplist``."""
    hits = (data or {}).get("hits") or []
    edges = (data or {}).get("edges") or []
    stoplist = gateway_stoplist(data)
    run = run_recipe(recipe, hits, edges, stoplist, deadline=deadline)
    selection = select_locus_cluster(run["clusters"], recipe.get("members"), hits)
    return {**run, "selection": selection, "hits": hits, "stoplist": stoplist}


def selection_summary(resolution: dict) -> dict:
    """What a JSON document should say about a resolution: the selected
    cluster's member ids in canonical form (hit order), the selection reason and
    counts; nothing per user."""
    sel = resolution["selection"]
    cl = sel["cluster"]
    return {
        "cluster_members": [canonical_member_id(i) for i in cl["memberIds"]] if cl else None,
        "reason": sel["reason"],
        "surviving": sel["surviving"],
        "missing": sel["missing"],
        "score": sel["score"],
        "candidates": sel["candidates"],
        "cluster_count": resolution["debug"]["clusterCount"],
        "stoplist_count": len(resolution["stoplist"]),
        "params": resolution["params"],
    }
