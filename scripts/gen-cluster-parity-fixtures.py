#!/usr/bin/env python3
"""Build the cluster-parity fixtures under scripts/fixtures/cluster_parity/.

Each fixture is a gateway-shaped search response (``data``: hits, edges,
toponym_stoplist, ...) plus ``cases``, each a locus recipe's clustering block
and member list to resolve it with. The JS oracle (``npm run
test:cluster-parity -- --write``) writes ``<name>.expected.json`` beside each;
``search/tests_cluster_parity.py`` holds the Python port to those.

Synthetic fixtures are deterministic (seeded) and regenerate identically:

    python3 scripts/gen-cluster-parity-fixtures.py

Real fixtures are trimmed from captured responses (read-only searches made
through the dev web container's crc_client, 2026-10-21); the raw captures are
not committed. ``--from-capture RAW OUT NAME`` applies the one trimming rule
below to a capture, and the trimming is also asserted harmless in the Django
test (the JS clusters the trimmed hit exactly as the raw one: the accessor
reads the same embedding).

Trimming rule for a captured hit: drop ``geometries`` and ``h3_cover`` (never
read by the clusterer); keep every name's ``label``/``lang`` but keep
``phon_emb`` only on the two names the accessor can reach, the first whose
label equals ``query_match.name`` and the first carrying one. Response keys
other than hits/edges/toponym_stoplist/clustering_params/total are dropped.
"""
from __future__ import annotations

import json
import os
import random
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(HERE, "fixtures", "cluster_parity")
STOPLIST_FILE = os.path.join(HERE, "fixtures", "gateway_toponym_stoplist.json")

with open(STOPLIST_FILE, encoding="utf-8") as _f:
    REAL_STOPLIST = json.load(_f)["toponym_stoplist"]

DEFAULT_WEIGHTS = {"name": 0.35, "spatial": 0.20, "temporal": 0.15, "type": 0.15, "link": 0.15}
CALIBRATED_WEIGHTS = {"name": 0.14045797658094575, "spatial": 0.2903334049072834,
                      "temporal": 0.3613341403053407, "type": 0.05787447820642999, "link": 0.15}


def clustering(theta=0.65, weights=None, same_ns=0.15, respect_distinct=True, tau_name=0.75, tau_link=1.0):
    return {"weights": dict(weights or DEFAULT_WEIGHTS), "theta": theta, "tau_name": tau_name,
            "tau_link": tau_link, "same_ns_penalty": same_ns, "respect_distinct": respect_distinct}


def case(name, members, **kw):
    return {"name": name, "clustering": clustering(**kw), "members": list(members)}


# --------------------------------------------------------------------------- synthetic helpers

def emb(rng: random.Random, base=None, noise=0, dim=128):
    """An int8 vector; with ``base`` a jittered copy (cosine stays high)."""
    if base is None:
        return [rng.randint(-127, 127) for _ in range(dim)]
    out = []
    for v in base:
        x = v + rng.randint(-noise, noise)
        out.append(max(-127, min(127, x)))
    return out


def hit(pid, ns, lat=None, lon=None, names=None, temporal=None, aat=None, title=None, *, qmatch=None, **extra):
    h = {"place_id": pid, "namespace": ns, "title": title or (names[0]["label"] if names else pid)}
    if lat is not None and lon is not None:
        h["repr_point"] = [lon, lat]
    if names is not None:
        h["names"] = names
    if temporal is not None:
        h["temporal_range"] = temporal
    if aat is not None:
        h["aat_paths"] = aat
    if qmatch is not None:
        h["query_match"] = {"name": qmatch, "score": 100.0}
    h.update(extra)
    return h


def name(label, e=None, lang="en"):
    n = {"label": label, "lang": lang}
    if e is not None:
        n["phon_emb"] = e
    return n


AAT_CITY = "300264550.300008346.300008347"
AAT_TOWN = "300264550.300008346.300008372"
AAT_RIVER = "300264550.300008791.300008707"


def fixture(name, note, data, cases):
    data.setdefault("edges", [])
    data.setdefault("toponym_stoplist", [])
    data.setdefault("total", len(data.get("hits") or []))
    return {"name": name, "note": note, "data": data, "cases": cases}


# --------------------------------------------------------------------------- the synthetic set

def fx_shape_berlin_three():
    """search/tests_locus.py's HITS and CLUSTERING verbatim (4-dim embeddings, real 500-entry stoplist)."""
    emb_a = [100, -20, 30, 5]
    emb_b = [-40, 90, 10, 0]
    hits = [
        {"place_id": "gn:1", "title": "Berlin", "namespace": "gn", "repr_point": [13.4, 52.52],
         "names": [{"label": "Berlin", "phon_emb": emb_a}], "temporal_range": [1200, 2000], "ccodes": ["DE"]},
        {"place_id": "wd:Q64", "title": "Berlin", "namespace": "wd", "repr_point": [13.41, 52.51],
         "names": [{"label": "Berlin", "phon_emb": emb_a}], "temporal_range": [1230, 2000], "ccodes": ["DE"]},
        {"place_id": "gn:2", "title": "Berlin", "namespace": "gn", "repr_point": [-71.2, 44.47],
         "names": [{"label": "Berlin", "phon_emb": emb_b}], "temporal_range": [1771, 2000], "ccodes": ["US"]},
    ]
    w = {"name": 0.41, "spatial": 0.23, "temporal": 0.11, "type": 0.09, "link": 0.16}
    return fixture(
        "shape_berlin_three",
        "The fake-gateway shape of search/tests_locus.py (HITS, PARAMS, CLUSTERING), with the real 500-entry stoplist.",
        {"hits": hits, "toponym_stoplist": list(REAL_STOPLIST)},
        [
            case("tests_locus_recipe", ["gn:1", "wd:Q64"], theta=0.33, weights=w, same_ns=0.12, tau_name=0.71, tau_link=0.9),
            case("members_absent", ["gn:999", "wd:Q0"], theta=0.33, weights=w, same_ns=0.12),
            case("member_spelling", ["GN:1", "place:wd:Q64"], theta=0.33, weights=w, same_ns=0.12),
            case("atlas_default", ["gn:1"]),
        ])


def fx_empty():
    return fixture("empty", "No hits at all: nothing clusters, nothing is selected.",
                   {"hits": [], "toponym_stoplist": ["berlin"]},
                   [case("nothing", ["gn:1"]), case("no_members_either", [])])


def fx_single():
    rng = random.Random(1)
    h = hit("wd:Q64", "wd", 52.52, 13.405, [name("Berlin", emb(rng))], [1237, None], [AAT_CITY], "Berlin")
    return fixture("single", "One hit: one cluster of one; selected iff it is a member.",
                   {"hits": [h], "edges": [{"a": "wd:Q64", "b": "gn:2950159", "relation_type": "sameAs"}]},
                   [case("is_member", ["wd:Q64"]), case("not_member", ["gn:2950159"]),
                    case("member_with_prefix", ["place:WD:Q64"])])


def fx_degradation():
    """Every way evidence can be absent, on both sides of a pair."""
    rng = random.Random(42)
    e0 = emb(rng)
    e1 = emb(rng, e0, 6)
    e2 = emb(rng, e0, 6)
    hits = [
        hit("gn:100", "gn", 48.85, 2.35, [name("Paris", e0)], [1000, None], [AAT_CITY], "Paris"),
        hit("wd:Q90", "wd", 48.86, 2.34, [name("Paris", e1)], [None, None], [AAT_CITY, AAT_TOWN], "Paris"),
        hit("osm:r7444", "osm", 48.85, 2.36, [name("Paris", e2)], None, [], "Paris"),       # undated, untyped
        hit("tgn:7008038", "tgn", None, None, [name("Paris", emb(rng, e0, 6))], [None, 1800], [AAT_TOWN], "Paris"),  # no point
        hit("ohm:1", "ohm", 48.84, 2.33, [name("Paris")], [1200, 1900], [AAT_CITY], "Paris"),   # name without embedding
        hit("gn:101", "gn", 48.87, 2.30, None, [1500, 1600], None, "Paris"),                   # no names key
        hit("wd:Q91", "wd", 48.83, 2.37, [], None, None, "Paris"),                             # empty names
        hit("osm:n1", "osm", None, None, [name("Paris", emb(rng, e0, 6))], None, None, "Paris", h3="871fb4662ffffff"),  # h3 only
        hit("gn:102", "gn", 48.80, 2.40, [name("Paris", emb(rng, e0, 6))], [1700, None], [AAT_RIVER], "Paris", point=[48.81, 2.41]),  # point wins over repr_point
        hit("wd:Q92", "wd", 48.82, 2.39, [name("Parigi", emb(rng, e0, 6)), name("Paris", emb(rng, e0, 6))], [1800, 1850], [AAT_CITY], "Paris", query_match="Paris"),  # query-matched second name
        hit("ohm:2", "ohm", 48.86, 2.31, [name("Paris", emb(rng, e0, 6))], [1800, 1850], [AAT_CITY], "Paris", query_match="Lutetia"),  # query match not among names
        hit("tgn:1", "tgn", 48.85, 2.35, [name("Paris", [])], [], [AAT_CITY], "Paris"),      # EMPTY embedding and EMPTY range: present, scores 0
        hit("gn:103", "gn", 48.85, 2.35, [name("Paris", emb(rng, e0, 6))], [1850], [AAT_CITY], "Paris"),  # one-entry range
        hit("wd:Q93", "wd", 48.85, 2.35, [name("Paris", emb(rng, e0, 200))], [1850, 1850], [AAT_CITY], "Paris"),  # unrelated embedding; single-year range
        hit("osm:n2", "osm", 48.85, 2.35, [name("Paris", e0)], [1850, 1850], [""], "Paris"),  # empty-string AAT path
        {"id": "pid-only:1", "ns": "x", "repr_point": [2.35, 48.85], "names": [name("Paris", e0)]},  # id/ns keys
        {"pid": "p:2", "repr_point": [2.35, 48.85]},                                             # pid key, nothing else
    ]
    return fixture(
        "degradation",
        "Missing or empty embeddings, points, dates, types; h3-only; point vs repr_point; id/ns alternates. Graceful degradation renormalises the weights.",
        {"hits": hits, "toponym_stoplist": list(REAL_STOPLIST)},
        [case("theta_default", ["gn:100", "wd:Q90"]),
         case("theta_low", ["gn:100"], theta=0.40),
         case("theta_high_ns0", ["osm:r7444"], theta=0.80, same_ns=0),
         case("calibrated", ["pid-only:1"], theta=0.21, weights=CALIBRATED_WEIGHTS),
         case("absent_member", ["osm:n9"], theta=0.40)])


def fx_stoplist():
    """Generic names down-weight the name VALUE by 0.2 when either side's representative name is on the list."""
    rng = random.Random(7)
    e = emb(rng)
    e_ = lambda: emb(rng, e, 4)   # noqa: E731
    hits = [
        hit("gn:1", "gn", 51.5, -0.12, [name("London", e_())], None, None, "London", query_match="London"),
        hit("wd:Q84", "wd", 51.51, -0.13, [name("London", e_())], None, None, "London", query_match="LONDON"),  # case
        hit("osm:r1", "osm", 51.49, -0.11, [name("london", e_())], None, None, "London"),          # names[0] fallback
        hit("tgn:1", "tgn", 51.50, -0.12, [name("Londinium", e_())], None, None, "London"),         # not on list
        hit("ohm:1", "ohm", 51.50, -0.12, [name("Londres", e_())], None, None, "London", query_match=""),  # '' falls through
        hit("gn:2", "gn", 51.50, -0.12, [name("", e_())], None, None, "Springfield"),               # title fallback (on list)
        hit("wd:Q2", "wd", 51.50, -0.12, [name("Londra", e_())], None, None, "Londra"),
        hit("osm:n2", "osm", 43.0, -70.0, [name("London", e_())], None, None, "London", query_match="London"),  # far away, same name
        hit("gn:3", "gn", 43.01, -70.01, [name("London", e_())], None, None, "London", query_match="London"),
    ]
    stop = ["london", "Springfield", "NEWTON", "Londra"]
    return fixture(
        "stoplist",
        "Stoplisted representative names (query_match.name, names[0].label, title fallbacks; case-insensitive) scale the name signal by 0.2.",
        {"hits": hits, "toponym_stoplist": stop},
        [case("with_stoplist", ["gn:1", "wd:Q84"], theta=0.55),
         case("low_theta_merges_generic_names", ["gn:1", "wd:Q84"], theta=0.30),
         case("tight", ["osm:n2"], theta=0.70),
         case("ns_margin_zero", ["tgn:1"], theta=0.55, same_ns=0)])


def fx_edges():
    """Hard-link edges force merges; `distinct` edges are cannot-links; other relations do nothing."""
    rng = random.Random(99)
    ea = emb(rng)
    eb = emb(rng)
    hits = [
        hit("gn:1", "gn", 40.0, -74.0, [name("Alpha", emb(rng, ea, 5))], [1800, 1900], [AAT_CITY]),
        hit("wd:Q1", "wd", 40.0, -74.0, [name("Alpha", emb(rng, ea, 5))], [1800, 1900], [AAT_CITY]),
        hit("osm:1", "osm", 40.0, -74.0, [name("Alpha", emb(rng, ea, 5))], [1800, 1900], [AAT_CITY]),
        hit("tgn:1", "tgn", 40.0, -74.0, [name("Alpha", emb(rng, ea, 5))], [1800, 1900], [AAT_CITY]),
        hit("gn:2", "gn", 10.0, 10.0, [name("Beta", emb(rng, eb, 5))], [1500, 1600], [AAT_TOWN]),
        hit("wd:Q2", "wd", 10.0, 10.0, [name("Beta", emb(rng, eb, 5))], [1500, 1600], [AAT_TOWN]),
        hit("osm:2", "osm", 10.0, 10.0, [name("Beta", emb(rng, eb, 5))], [1500, 1600], [AAT_TOWN]),
        hit("ohm:3", "ohm", -30.0, 150.0, [name("Gamma", emb(rng))], None, None),
        hit("gn:3", "gn", -30.0, 150.0, [name("Gamma", emb(rng))], None, None),
    ]
    edges = [
        {"a": "gn:1", "b": "wd:Q1", "relation_type": "sameAs", "source": "gn"},
        {"a": "wd:Q1", "b": "gn:1", "relation_type": "sameAs", "source": "loc"},     # duplicate, reversed
        {"a": "osm:1", "b": "wd:Q1", "relation_type": "closeMatch"},
        {"a": "tgn:1", "b": "off:result", "relation_type": "exactMatch"},            # 1-hop off-result id
        {"a": "gn:2", "b": "wd:Q2", "relation_type": "distinct"},                     # cannot-link a strong pair
        {"a": "osm:2", "b": "gn:2", "relation_type": "related"},                      # no effect
        {"a": "ohm:3", "b": "gn:3", "relation_type": "exactMatch"},                   # forces a weak pair
        {"a": "wd:Q2", "b": "osm:2", "relation_type": "distinct"},                    # transitive block
        {"a": "wd:Q1", "b": "gn:2", "relation_type": "seeAlso"},
    ]
    return fixture(
        "edges",
        "sameAs/exactMatch/closeMatch force merges (duplicates, reversed, off-result); distinct is a cannot-link, transitively; related/seeAlso do nothing.",
        {"hits": hits, "edges": edges},
        [case("respect_distinct", ["gn:2", "wd:Q2"], theta=0.65),
         case("ignore_distinct", ["gn:2", "wd:Q2"], theta=0.65, respect_distinct=False),
         case("forced_weak_pair", ["ohm:3"], theta=0.90),
         case("hub", ["tgn:1", "osm:1"], theta=0.60, same_ns=0)])


def fx_same_namespace():
    """Same-gazetteer repulsion: blocked below theta+margin, allowed above; transitive collisions through a cross-namespace hub."""
    rng = random.Random(2024)
    e = emb(rng)
    hits = [
        hit("osm:n1", "osm", 52.0, 0.0, [name("Delta", emb(rng, e, 2))], [1900, 2000], [AAT_CITY]),
        hit("osm:r1", "osm", 52.0, 0.0, [name("Delta", emb(rng, e, 2))], [1900, 2000], [AAT_CITY]),   # node+relation: very strong
        hit("wd:Q1", "wd", 52.01, 0.01, [name("Delta", emb(rng, e, 12))], [1900, 2000], [AAT_TOWN]),
        hit("gn:1", "gn", 52.02, 0.02, [name("Delta", emb(rng, e, 12))], [1850, 1950], [AAT_TOWN]),
        hit("gn:2", "gn", 52.02, 0.03, [name("Delta", emb(rng, e, 12))], [1850, 1950], [AAT_RIVER]),  # second gn: weakly linked via wd hub
        hit("tgn:1", "tgn", 52.0, 0.0, [name("Delta", emb(rng, e, 2))], [1900, 2000], [AAT_CITY]),
        hit("tgn:2", "tgn", 52.0, 0.0, [name("Delta", emb(rng, e, 2))], [1900, 2000], [AAT_CITY]),    # tgn pair: strong override
        hit("x:1", None, 52.0, 0.0, [name("Delta", emb(rng, e, 2))], [1900, 2000], [AAT_CITY]),       # null namespace: never collides
        hit("x:2", None, 52.0, 0.0, [name("Delta", emb(rng, e, 2))], [1900, 2000], [AAT_CITY]),
    ]
    edges = [{"a": "gn:1", "b": "wd:Q1", "relation_type": "sameAs"}]
    return fixture(
        "same_namespace",
        "A cluster should hold <=1 record per namespace unless the bridging pair clears theta+margin; hard links bypass; null namespaces never collide.",
        {"hits": hits, "edges": edges},
        [case("margin_015", ["osm:n1"], theta=0.60, same_ns=0.15),
         case("margin_0", ["osm:n1"], theta=0.60, same_ns=0),
         case("margin_05", ["gn:1", "wd:Q1"], theta=0.60, same_ns=0.5),
         case("theta_065", ["gn:2"], theta=0.65, same_ns=0.15),
         case("theta_low", ["gn:2"], theta=0.50, same_ns=0.15)])


def fx_ties():
    """Equal composites (sort stability decides merge order, hence roots) and tied selections."""
    rng = random.Random(5)
    bases = [emb(rng) for _ in range(3)]
    # Three identical-within triples, far apart, unrelated embeddings between triples.
    hits = []
    # Asymmetric positions, so a three-way tie's nearest cluster is NOT the first candidate.
    for k, (lat, lon) in enumerate([(-40.0, -40.0), (0.0, 0.0), (30.0, 30.0)]):
        for ns in ("gn", "wd", "osm"):
            hits.append(hit(f"{ns}:{k}", ns, lat, lon, [name("Echo", bases[k])], [1800, 1900], [AAT_CITY]))
    # No point anywhere. The survivors' code-point order is NOT their UTF-16 order:
    # U+FF21 (fullwidth A) sorts before U+1D518 (astral) by code point, after it by UTF-16 unit.
    nogeo = []
    for k, ns_ids in enumerate([("gn:\U0001d518", "wd:0", "osm:0"), ("gn:\uff21", "wd:1", "osm:1")]):
        for i, pid in enumerate(ns_ids):
            nogeo.append(hit(pid, ("gn", "wd", "osm")[i], None, None, [name("Echo", bases[k])], [1800, 1900], [AAT_CITY]))
    mixed = [hit("gn:0", "gn", 10.0, 10.0, [name("Echo", bases[0])], [1800, 1900], [AAT_CITY]),
             hit("wd:0", "wd", 10.0, 10.0, [name("Echo", bases[0])], [1800, 1900], [AAT_CITY]),
             hit("gn:1", "gn", None, None, [name("Echo", bases[1])], [1800, 1900], [AAT_CITY]),   # cluster without any point
             hit("wd:1", "wd", None, None, [name("Echo", bases[1])], [1800, 1900], [AAT_CITY]),
             hit("osm:2", "osm", None, None, [name("Echo", bases[2])], [1800, 1900], [AAT_CITY]),  # another without a point
             hit("tgn:2", "tgn", None, None, [name("Echo", bases[2])], [1800, 1900], [AAT_CITY])]
    return [
        fixture("ties", "Identical composites within each triple; selections tied between clusters resolve by centroid.",
                {"hits": hits},
                [case("one_survivor_each_centroid", ["gn:0", "wd:1"], theta=0.65),
                 case("two_vs_one", ["gn:0", "wd:0", "osm:1"], theta=0.65),
                 case("all_three_tied", ["gn:0", "gn:1", "gn:2"], theta=0.65),
                 case("near_second", ["osm:1", "gn:2"], theta=0.65)]),
        fixture("ties_no_geometry", "Tied selection with no point anywhere: the code-point-smallest survivor decides (and code-point order is not UTF-16 order).",
                {"hits": nogeo},
                [case("astral_vs_fullwidth", ["gn:\U0001d518", "gn:\uff21"], theta=0.65),
                 case("plain", ["wd:1", "osm:0"], theta=0.65),
                 case("plurality_wins", ["wd:1", "osm:1", "gn:\U0001d518"], theta=0.65)]),
        fixture("ties_mixed_geometry", "A tie where one candidate cluster has no centroid (the other wins by centroid); a tie where NO candidate has one although a survivor has a point (first member).",
                {"hits": mixed},
                [case("one_without_centroid", ["gn:0", "gn:1"], theta=0.65),
                 case("no_candidate_centroid", ["gn:1", "wd:1", "osm:2", "tgn:2", "gn:0"], theta=0.65),
                 case("none_with_points", ["wd:1", "tgn:2"], theta=0.65)]),
    ]


def fx_hundred():
    """100 hits: 20 places x 5 namespaces, jittered coordinates and embeddings, mixed dates/types, some edges, some stoplisted."""
    rng = random.Random(100)
    hits, edges = [], []
    nss = ["gn", "wd", "osm", "tgn", "ohm"]
    stop = ["springfield", "newton", "mill"]
    labels = ["Springfield", "Newton", "Riverton", "Mill", "Oakford", "Kingsbury", "Ashby"]
    for p in range(20):
        lat, lon = rng.uniform(-60, 60), rng.uniform(-170, 170)
        base = emb(rng)
        label = labels[p % len(labels)]
        start = rng.choice([None, rng.randint(-500, 1800)])
        end = rng.choice([None, 2000, rng.randint(1800, 2000)])
        paths = rng.choice([[], [AAT_CITY], [AAT_TOWN], [AAT_CITY, AAT_RIVER]])
        for i, ns in enumerate(nss):
            pid = f"{ns}:{p * 10 + i}"
            dist = rng.choice([0.001, 0.01, 0.1, 0.5, 2.0])
            names = [name(label, emb(rng, base, rng.choice([2, 6, 20, 60])))]
            if rng.random() < 0.2:
                names = [name(label)]                   # no embedding
            tr = None if rng.random() < 0.3 else [start, end]
            h = hit(pid, ns, lat + rng.uniform(-dist, dist), lon + rng.uniform(-dist, dist), names, tr,
                    None if rng.random() < 0.4 else paths, label, qmatch=label if rng.random() < 0.7 else None)
            if rng.random() < 0.1:
                h.pop("repr_point", None)
            hits.append(h)
        if rng.random() < 0.5:
            edges.append({"a": f"gn:{p * 10}", "b": f"wd:{p * 10 + 1}", "relation_type": "sameAs"})
        if rng.random() < 0.2:
            edges.append({"a": f"osm:{p * 10 + 2}", "b": f"tgn:{p * 10 + 3}", "relation_type": "distinct"})
    rng.shuffle(hits)
    return fixture("hundred", "100 synthetic hits (20 places x 5 namespaces) with mixed evidence, edges and stoplisted names; the performance fixture.",
                   {"hits": hits, "edges": edges, "toponym_stoplist": stop},
                   [case("atlas_default", ["gn:0", "wd:1"], theta=0.65),
                    case("calibrated", ["gn:0", "wd:1"], theta=0.21, weights=CALIBRATED_WEIGHTS),
                    case("strict", ["osm:52"], theta=0.85, same_ns=0.3),
                    case("loose_no_distinct", ["tgn:73", "ohm:74"], theta=0.45, respect_distinct=False)])


def fx_unicode_ids():
    """Ids and names outside ASCII: code-point ordering of survivors, namespace case-folding, astral characters."""
    rng = random.Random(11)
    e = emb(rng)
    hits = [
        hit("WD:Q1", "wd", 35.0, 139.0, [name("東京", e)], None, None, "東京"),
        hit("gn:é1", "gn", 35.0, 139.0, [name("東京", e)], None, None, "東京"),
        hit("gn:𝔘1", "gn", 35.0, 139.0, [name("東京", e)], None, None, "東京"),   # astral (U+1D518)
        hit("gn:z1", "gn", 35.0, 139.0, [name("東京", e)], None, None, "東京"),
        hit("osm:ß", "osm", 35.0, 139.0, [name("Tōkyō", e)], None, None, "Tōkyō"),
    ]
    return fixture("unicode_ids", "Non-ASCII and astral ids; members spelt with upper-case namespaces and a place: prefix.",
                   {"hits": hits, "toponym_stoplist": ["TŌKYŌ"]},
                   [case("fold", ["place:wd:Q1", "GN:é1"], theta=0.65, same_ns=0),
                    case("astral_order", ["gn:𝔘1", "gn:z1", "gn:é1"], theta=0.65, same_ns=0.15),
                    case("all_same_ns_blocked", ["gn:z1"], theta=0.65, same_ns=0.15)])


SYNTHETIC = [fx_shape_berlin_three, fx_empty, fx_single, fx_degradation, fx_stoplist, fx_edges,
             fx_same_namespace, fx_ties, fx_hundred, fx_unicode_ids]


# --------------------------------------------------------------------------- real captures

def trim_hit(h: dict) -> dict:
    out = {k: v for k, v in h.items() if k not in ("geometries", "h3_cover")}
    names = h.get("names")
    if isinstance(names, list):
        qname = (h.get("query_match") or {}).get("name") if isinstance(h.get("query_match"), dict) else None
        keep = set()
        if qname:
            for i, n in enumerate(names):
                if isinstance(n, dict) and n.get("label") == qname and isinstance(n.get("phon_emb"), list):
                    keep.add(i)
                    break
        for i, n in enumerate(names):
            if isinstance(n, dict) and isinstance(n.get("phon_emb"), list):
                keep.add(i)
                break
        out["names"] = [n if i in keep else {k: v for k, v in n.items() if k != "phon_emb"}
                        for i, n in enumerate(names)]
    return out


def trim_capture(raw: dict) -> dict:
    r = raw["response"]
    return {"hits": [trim_hit(h) for h in r.get("hits") or []],
            "edges": r.get("edges") or [],
            "toponym_stoplist": r.get("toponym_stoplist") or [],
            "clustering_params": r.get("clustering_params"),
            "total": r.get("total")}


def real_cases(data: dict) -> list:
    """Members: the two ends of the first merge edge whose ends are both hits (a
    real co-reference), then two hits far apart (a tie at one survivor each)."""
    ids = [h.get("place_id") for h in data["hits"]]
    present = set(ids)
    pair = None
    for e in data["edges"]:
        if e.get("relation_type") in ("sameAs", "exactMatch", "closeMatch") and e["a"] in present and e["b"] in present:
            pair = [e["a"], e["b"]]
            break
    if pair is None:
        pair = ids[:2]
    far = [ids[0], ids[-1]]
    cases = [
        case("atlas_default_linked_pair", pair, theta=0.65),
        case("calibrated_linked_pair", pair, theta=0.21, weights=CALIBRATED_WEIGHTS),
        case("atlas_default_far_pair", far, theta=0.65),
        case("strict_single", [ids[0]], theta=0.80, same_ns=0.3),
        case("loose_no_distinct", [ids[1]], theta=0.45, same_ns=0, respect_distinct=False),
    ]
    return cases


def from_capture(raw_path: str, out_name: str, note: str) -> dict:
    with open(raw_path, encoding="utf-8") as f:
        raw = json.load(f)
    data = trim_capture(raw)
    full = (f"Real CRC gateway response, captured {raw['captured_at']} via the dev web container's crc_search_status "
            f"(read-only). Request body: {json.dumps(raw['request'], ensure_ascii=False)}. Trimmed per the rule in "
            f"scripts/gen-cluster-parity-fixtures.py (geometries/h3_cover dropped; phon_emb kept only on the names the "
            f"accessor can reach). {note}")
    return fixture(out_name, full, data, real_cases(data))


def write(fx: dict):
    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, fx["name"] + ".json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(fx, f, ensure_ascii=False, separators=(",", ":"))
        f.write("\n")
    print(f"wrote {path} ({len(fx['data']['hits'])} hits, {len(fx['cases'])} cases)")


def main(argv):
    if argv[1:2] == ["--from-capture"]:
        raw, name = argv[2], argv[3]
        note = argv[4] if len(argv) > 4 else ""
        write(from_capture(raw, name, note))
        return
    for fn in SYNTHETIC:
        out = fn()
        for fx in (out if isinstance(out, list) else [out]):
            write(fx)


if __name__ == "__main__":
    main(sys.argv)
