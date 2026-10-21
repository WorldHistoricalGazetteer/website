"""Parity of search/clustering.py (the Python port) with the shipped browser
clusterer (place#325, SG ruling 2026-10-21).

Run locally (never in the prod container):

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 DB_NAME=whgv3beta_f3 \\
        venv/bin/python manage.py test search.tests_cluster_parity --settings=whg.settings_localtest

The contract is whg/webpack/js/clustering.js + locus_resolve.js. The oracle is
scripts/fixtures/cluster_parity/<name>.expected.json, written by
``node scripts/check-cluster-parity.mjs --write`` from the shipped modules over
<name>.json (synthetic fixtures from scripts/gen-cluster-parity-fixtures.py and
three real gateway responses captured 2026-10-21). For every fixture and case
the port must give:

* IDENTICAL cluster assignments (id -> root), cluster roots, membership and
  order, debug counters and parameters;
* the IDENTICAL selected cluster, reason, surviving/missing lists and counts;
* every pair's composite and each present signal within PAIR_SCORE_TOLERANCE
  (1e-9; see search/clustering.py for why: only sin/cos/atan2 may differ in
  the last unit between V8 and glibc), with the same set of present signals.

Written so it cannot pass vacuously: the oracle must have been generated from
the JS files as they are now (their sha256 is recorded in it), the comparison
is shown to FAIL on two mutated ports and on a tampered oracle, and when node
is available the oracle is re-derived live and must equal the committed one.
"""
from __future__ import annotations

import glob
import importlib.util
import json
import os
import shutil
import subprocess
import tempfile
import time
from unittest import mock

from django.conf import settings
from django.test import SimpleTestCase

from search import clustering as C

FIXTURE_DIR = os.path.join(settings.BASE_DIR, "scripts", "fixtures", "cluster_parity")
RUNNER = os.path.join(settings.BASE_DIR, "scripts", "check-cluster-parity.mjs")
JS_DIR = os.path.join(settings.BASE_DIR, "whg", "webpack", "js")
INDEXING_REPO = os.environ.get("WHG_INDEXING_REPO", os.path.expanduser("~/PycharmProjects/indexing"))
REFERENCE = os.path.join(INDEXING_REPO, "clustering", "calibrate_params.py")

# Generous budget for clustering 100 hits (measured well under 0.5 s; see the test's output).
PERF_BUDGET_S = 2.0


def _sha256(path):
    import hashlib
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def fixture_names():
    names = sorted(os.path.basename(p)[:-5] for p in glob.glob(os.path.join(FIXTURE_DIR, "*.json"))
                   if not p.endswith(".expected.json"))
    assert names, f"no fixtures in {FIXTURE_DIR}"
    return names


def load(name):
    with open(os.path.join(FIXTURE_DIR, name + ".json"), encoding="utf-8") as f:
        fx = json.load(f)
    with open(os.path.join(FIXTURE_DIR, name + ".expected.json"), encoding="utf-8") as f:
        want = json.load(f)
    return fx, want


def _recipe(c):
    return {"v": 1, "retrieval": {}, "clustering": c["clustering"], "members": c["members"]}


def _cluster_out(cl):
    return {"root": cl["root"], "memberIds": list(cl["memberIds"])} if cl else None


def compare_case(tc: SimpleTestCase, fx, want_case, pairs, tol=C.PAIR_SCORE_TOLERANCE):
    """Run the port on one case and assert equality with the JS oracle. Returns
    the largest absolute score difference seen (for reporting)."""
    cases = {c["name"]: c for c in fx["cases"]}
    c = cases[want_case["name"]]
    label = f"{fx['name']}/{c['name']}"
    got = C.resolve_recipe(_recipe(c), fx["data"])

    tc.assertEqual(got["assignments"], want_case["assignments"], f"{label}: assignments")
    tc.assertEqual([_cluster_out(cl) for cl in got["clusters"]], want_case["clusters"], f"{label}: clusters")
    sel = got["selection"]
    tc.assertEqual({"cluster": _cluster_out(sel["cluster"]), "reason": sel["reason"], "surviving": sel["surviving"],
                    "missing": sel["missing"], "score": sel["score"], "candidates": sel["candidates"]},
                   want_case["selection"], f"{label}: selection")
    tc.assertEqual(got["debug"], want_case["debug"], f"{label}: debug")
    tc.assertEqual(got["params"], want_case["params"], f"{label}: params")
    tc.assertEqual(len(got["stoplist"]), want_case["stoplist_count"], f"{label}: stoplist")

    # Pair scores, to tolerance, with the same set of present signals.
    hits = fx["data"]["hits"]
    mine = C.score_all_pairs(hits, fx["data"].get("edges"), c["clustering"]["weights"], fx["data"].get("toponym_stoplist"))
    tc.assertEqual(len(mine), len(pairs), f"{label}: pair count")
    order = ["name", "spatial", "temporal", "type", "link"]
    worst = 0.0
    for (i, j, r), row in zip(mine, pairs):
        wi, wj, composite, weight_used, sig = row
        tc.assertEqual((i, j), (wi, wj), f"{label}: pair order")
        d = abs(r["composite"] - composite)
        worst = max(worst, d)
        tc.assertLessEqual(d, tol, f"{label}: pair ({hits[i].get('place_id')}, {hits[j].get('place_id')}) composite {r['composite']} vs JS {composite}")
        tc.assertLessEqual(abs(r["weightUsed"] - weight_used), tol, f"{label}: weightUsed")
        present = [k for k, v in zip(order, sig) if v is not None]
        tc.assertEqual(sorted(r["signals"]), sorted(present), f"{label}: present signals for pair {i},{j}")
        for k, v in zip(order, sig):
            if v is not None:
                d = abs(r["signals"][k] - v)
                worst = max(worst, d)
                tc.assertLessEqual(d, tol, f"{label}: signal {k} for pair {i},{j}: {r['signals'][k]} vs JS {v}")
    return worst


class OracleFreshnessTests(SimpleTestCase):
    """The oracle is only an oracle for the JS as it is NOW."""

    def test_oracles_record_the_current_js_sources(self):
        js = {n: _sha256(os.path.join(JS_DIR, n)) for n in ("clustering.js", "locus_resolve.js")}
        names = fixture_names()
        for name in names:
            _, want = load(name)
            self.assertEqual(want["js_sha256"], js,
                             f"{name}.expected.json was generated from other JS sources; run "
                             "`npm run test:cluster-parity -- --write` and re-run this test")
        self.assertGreaterEqual(len(names), 12)

    def test_oracle_equals_a_live_node_run(self):
        node = shutil.which("node")
        if not node:
            self.skipTest("node is not available: the committed oracle was NOT re-derived live")
        live_cases = 0
        for name in fixture_names():
            _, want = load(name)
            out = subprocess.run([node, "--no-warnings", RUNNER, "--emit", name], capture_output=True, text=True,
                                 timeout=120, cwd=settings.BASE_DIR)
            self.assertEqual(out.returncode, 0, out.stderr)
            live = json.loads(out.stdout)
            self.assertEqual(live["cases"], want["cases"], name)
            self.assertEqual(live["pair_sets"], want["pair_sets"], name)
            live_cases += len(live["cases"])
        self.assertGreaterEqual(live_cases, 50)


class ParityTests(SimpleTestCase):
    """The port equals the JS on every fixture and case."""

    def test_every_fixture_and_case(self):
        worst = 0.0
        n_cases = n_pairs = 0
        reasons = set()
        for name in fixture_names():
            fx, want = load(name)
            self.assertEqual([c["name"] for c in fx["cases"]], [c["name"] for c in want["cases"]], name)
            for wc in want["cases"]:
                pairs = want["pair_sets"][wc["pairs_key"]]
                worst = max(worst, compare_case(self, fx, wc, pairs))
                n_cases += 1
                n_pairs += len(pairs)
                reasons.add(wc["selection"]["reason"])
        # Every selection branch is exercised somewhere, or the harness is blind to it.
        self.assertEqual(reasons, {"plurality", "tie_centroid", "tie_first_member", "no_members_survive"})
        self.assertGreaterEqual(n_cases, 50)
        self.assertGreaterEqual(n_pairs, 40000)
        print(f"\ncluster parity: {n_cases} cases, {n_pairs} pair scores; worst |Δ| = {worst:.3e} (tolerance {C.PAIR_SCORE_TOLERANCE:g})")
        # The measured worst difference must sit far inside the tolerance, or the
        # tolerance is doing work it was not justified for.
        self.assertLess(worst, C.PAIR_SCORE_TOLERANCE / 1000)

    def test_edge_cases_are_present_in_the_fixtures(self):
        """Absence assertions paired with presence: the cases the brief names exist."""
        names = fixture_names()
        for needed in ("empty", "single", "degradation", "stoplist", "edges", "same_namespace", "ties",
                       "ties_no_geometry", "hundred", "unicode_ids"):
            self.assertIn(needed, names)
        real = [n for n in names if n.startswith("real_")]
        self.assertGreaterEqual(len(real), 2)
        fx, _ = load("hundred")
        self.assertEqual(len(fx["data"]["hits"]), 100)
        fx, _ = load("empty")
        self.assertEqual(fx["data"]["hits"], [])
        fx, want = load("edges")
        self.assertTrue(any(e["relation_type"] == "distinct" for e in fx["data"]["edges"]))
        by = {c["name"]: c for c in want["cases"]}
        self.assertEqual(by["respect_distinct"]["debug"]["cannotLinks"], 2)
        self.assertEqual(by["ignore_distinct"]["debug"]["cannotLinks"], 0)
        self.assertNotEqual(by["respect_distinct"]["assignments"], by["ignore_distinct"]["assignments"])
        fx, want = load("same_namespace")
        by = {c["name"]: c for c in want["cases"]}
        self.assertGreater(by["margin_015"]["debug"]["sameNsBlocked"], 0)
        self.assertEqual(by["margin_0"]["debug"]["sameNsBlocked"], 0)
        fx, want = load("stoplist")
        self.assertTrue(set(s.lower() for s in fx["data"]["toponym_stoplist"]) & {"london", "springfield"})
        for name in real:
            fx, _ = load(name)
            self.assertIn("captured 2026-10-21", fx["note"])
            self.assertIn("Request body", fx["note"])
            self.assertEqual(len(fx["data"]["toponym_stoplist"]), 500)


class CanFailTests(SimpleTestCase):
    """A parity check that cannot fail proves nothing."""

    def test_a_mutated_stoplist_factor_fails(self):
        fx, want = load("stoplist")
        wc = want["cases"][0]
        compare_case(self, fx, wc, want["pair_sets"][wc["pairs_key"]])          # passes as shipped
        with mock.patch.object(C, "STOPLIST_NAME_FACTOR", 0.21):
            with self.assertRaises(AssertionError) as cm:
                compare_case(self, fx, wc, want["pair_sets"][wc["pairs_key"]])
        self.assertIn("composite", str(cm.exception))

    def test_a_mutated_tie_break_fails_on_selection_only(self):
        fx, want = load("ties")
        wc = {c["name"]: c for c in want["cases"]}["all_three_tied"]
        self.assertEqual(wc["selection"]["reason"], "tie_centroid")
        self.assertNotEqual(wc["selection"]["cluster"], wc["clusters"][0])   # the nearest is not the first candidate
        compare_case(self, fx, wc, want["pair_sets"][wc["pairs_key"]])
        with mock.patch.object(C, "_dist2", lambda a, b: 0.0):             # every candidate equally near
            with self.assertRaises(AssertionError) as cm:
                compare_case(self, fx, wc, want["pair_sets"][wc["pairs_key"]])
        self.assertIn("selection", str(cm.exception))

    def test_a_mutated_half_life_fails_within_tolerance_reach(self):
        """A 1-in-10^4 change to a constant is far above the tolerance."""
        fx, want = load("real_coventry_2026-10-21")
        wc = want["cases"][0]
        with mock.patch.object(C, "SPATIAL_HALF_LIFE_KM", 25.0025):
            with self.assertRaises(AssertionError):
                compare_case(self, fx, wc, want["pair_sets"][wc["pairs_key"]])

    def test_the_node_runner_fails_on_a_tampered_oracle(self):
        node = shutil.which("node")
        if not node:
            self.skipTest("node is not available: the runner's failure path was NOT exercised")
        with tempfile.TemporaryDirectory() as tmp:
            for name in ("single", "empty"):
                for suffix in (".json", ".expected.json"):
                    shutil.copy(os.path.join(FIXTURE_DIR, name + suffix), tmp)
            env = {**os.environ, "CLUSTER_PARITY_DIR": tmp}
            ok = subprocess.run([node, "--no-warnings", RUNNER], capture_output=True, text=True, env=env, timeout=60)
            self.assertEqual(ok.returncode, 0, ok.stderr)
            self.assertIn("checked 2 fixtures", ok.stdout)
            p = os.path.join(tmp, "single.expected.json")
            with open(p, encoding="utf-8") as f:
                doc = json.load(f)
            doc["cases"][0]["selection"]["reason"] = "tie_centroid"
            with open(p, "w", encoding="utf-8") as f:
                json.dump(doc, f)
            bad = subprocess.run([node, "--no-warnings", RUNNER], capture_output=True, text=True, env=env, timeout=60)
            self.assertEqual(bad.returncode, 1)
            self.assertIn("single: cases[0].selection.reason", bad.stderr)


class ReferenceOracleTests(SimpleTestCase):
    """The offline calibration reference (indexing clustering/calibrate_params.py)
    as a second oracle for the five signal functions, when a read-only clone is
    present. Skipping is worded as NOT verified, never as a pass."""

    def _reference(self):
        if not os.path.exists(REFERENCE):
            self.skipTest(f"calibration reference not present at {REFERENCE}: signal functions NOT verified against it")
        spec = importlib.util.spec_from_file_location("calibrate_params_ref", REFERENCE)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    def test_signal_functions_agree_with_the_reference(self):
        ref = self._reference()
        n = 0
        worst = 0.0
        for name in fixture_names():
            fx, _ = load(name)
            hits = fx["data"]["hits"]
            for i in range(len(hits)):
                for j in range(i + 1, len(hits)):
                    a, b = hits[i], hits[j]
                    la, lb = C.hit_lat_lng(a), C.hit_lat_lng(b)
                    if la and lb:
                        km = C.haversine_km(la[0], la[1], lb[0], lb[1])
                        d = abs(km - ref.haversine_km(la[0], la[1], lb[0], lb[1]))
                        worst = max(worst, d / max(km, 1.0))
                        self.assertLessEqual(d, 1e-9 * max(km, 1.0))
                        self.assertEqual(C.spatial_signal(km), ref.spatial_signal(km))
                    ta, tb = C.hit_temporal(a), C.hit_temporal(b)
                    if ta and tb and len(ta) >= 2 and len(tb) >= 2:
                        self.assertEqual(C.temporal_overlap(ta, tb), ref.temporal_overlap(ta, tb))
                    pa, pb = C.hit_aat_paths(a), C.hit_aat_paths(b)
                    if pa and pb:
                        self.assertEqual(C.type_signal(pa, pb), ref.type_signal(pa, pb))
                    ea, eb = C.hit_embedding(a), C.hit_embedding(b)
                    if ea and eb and len(ea) == len(eb):
                        self.assertEqual(C.cosine_byte(ea, eb), ref.cosine_byte(ea, eb))
                    n += 1
        self.assertGreater(n, 20000)
        print(f"\nreference oracle: {n} pairs; worst relative haversine |Δ| = {worst:.2e}")

    def test_where_js_and_reference_disagree_the_js_wins(self):
        """Documented divergences on MALFORMED input (no gateway response produces
        them). The port follows the JS, the contract every recipe was made with."""
        ref = self._reference()
        # A one-entry range: JS temporalOverlap -> 0; the reference raises.
        self.assertEqual(C.temporal_overlap([1850], [1800, 1900]), 0.0)
        with self.assertRaises(IndexError):
            ref.temporal_overlap([1850], [1800, 1900])
        # Unequal embedding lengths: JS cosineByte -> 0; the reference zips (truncates).
        self.assertEqual(C.cosine_byte([1, 2, 3], [1, 2]), 0.0)
        self.assertNotEqual(ref.cosine_byte([1, 2, 3], [1, 2]), 0.0)
        # And on well-formed input they agree (so the two assertions above are not vacuous).
        self.assertEqual(C.temporal_overlap([None, 1974], [1889, 1974]), ref.temporal_overlap([None, 1974], [1889, 1974]))
        self.assertEqual(C.cosine_byte([1, 2, 3], [3, 2, 1]), ref.cosine_byte([1, 2, 3], [3, 2, 1]))


class PerformanceTests(SimpleTestCase):
    def test_hundred_hits_cluster_well_under_a_second(self):
        timings = {}
        for name in ("hundred", "real_berlin_2026-10-21", "real_coventry_2026-10-21", "real_springfield_2026-10-21"):
            fx, _ = load(name)
            self.assertEqual(len(fx["data"]["hits"]), 100)
            c = fx["cases"][0]
            C.resolve_recipe(_recipe(c), fx["data"])                      # warm
            best = min(_timed(lambda: C.resolve_recipe(_recipe(c), fx["data"])) for _ in range(3))
            timings[name] = best
            self.assertLess(best, PERF_BUDGET_S, name)
        print("\ncluster performance (100 hits, best of 3): " + ", ".join(f"{k} {v * 1000:.0f} ms" for k, v in timings.items()))


def _timed(fn):
    t0 = time.perf_counter()
    fn()
    return time.perf_counter() - t0


class PortUnitTests(SimpleTestCase):
    """Behaviour the fixtures reach only implicitly, pinned directly."""

    def test_canonical_member_id(self):
        self.assertEqual(C.canonical_member_id(" place:WD:Q64 "), "wd:Q64")
        self.assertEqual(C.canonical_member_id("GN:ABC"), "gn:ABC")
        self.assertEqual(C.canonical_member_id("noprefix"), "noprefix")
        self.assertEqual(C.canonical_member_id(None), "")

    def test_accessors_follow_the_js_coalescing_rules(self):
        self.assertEqual(C.hit_id({"id": "", "place_id": "x"}), "")           # ?? keeps ''
        self.assertEqual(C.hit_id({"place_id": None, "pid": "p"}), "p")
        self.assertIsNone(C.hit_id({}))
        self.assertEqual(C.hit_lat_lng({"point": [1, 2], "repr_point": [9, 9]}), [1, 2])
        self.assertEqual(C.hit_lat_lng({"repr_point": [10, 20]}), [20, 10])
        self.assertIsNone(C.hit_lat_lng({"repr_point": [1]}))
        self.assertIsNone(C.hit_lat_lng({"h3": "87..."}))
        self.assertEqual(C.hit_name_str({"query_match": {"name": ""}, "names": [{"label": "A"}]}), "A")  # || skips ''
        self.assertEqual(C.hit_name_str({"names": [{"label": ""}], "title": "T"}), "T")
        self.assertIsNone(C.hit_name_str({"names": []}))
        self.assertEqual(C.hit_embedding({"phon_emb": []}), [])                 # present, empty
        self.assertEqual(C.hit_embedding({"names": [{"label": "a"}, {"label": "b", "phon_emb": [1]}],
                                          "query_match": {"name": "a"}}), [1])
        self.assertIsNone(C.hit_embedding({"names": "not a list"}))
        self.assertEqual(C.hit_namespace({"namespace": None, "ns": "x"}), "x")

    def test_empty_present_evidence_scores_zero_but_counts(self):
        """An empty list is present in JS (truthy), so its weight stays in the denominator."""
        a = {"place_id": "a", "phon_emb": [], "temporal_range": []}
        b = {"place_id": "b", "phon_emb": [], "temporal_range": []}
        w = C.DEFAULT_PARAMS["weights"]
        r = C.score_pair(a, b, w, None, None)
        self.assertEqual(sorted(r["signals"]), ["link", "name", "temporal"])
        self.assertEqual(r["composite"], 0.0)
        self.assertEqual(r["weightUsed"], w["name"] + w["temporal"] + w["link"])
        # ...and absent evidence is dropped, renormalising the rest.
        r = C.score_pair({"place_id": "a"}, {"place_id": "b"}, w, None, None)
        self.assertEqual(r["signals"], {"link": 0.0})
        self.assertEqual(r["weightUsed"], w["link"])

    def test_selection_summary_shape(self):
        fx, _ = load("shape_berlin_three")
        out = C.resolve_recipe(_recipe(fx["cases"][0]), fx["data"])
        s = C.selection_summary(out)
        self.assertEqual(s["cluster_members"], ["gn:1", "wd:Q64"])
        self.assertEqual(s["reason"], "plurality")
        self.assertEqual((s["cluster_count"], s["stoplist_count"], s["candidates"], s["score"]), (2, 500, 1, 2))
        out = C.resolve_recipe(_recipe(fx["cases"][1]), fx["data"])
        s = C.selection_summary(out)
        self.assertIsNone(s["cluster_members"])
        self.assertEqual(s["reason"], "no_members_survive")
