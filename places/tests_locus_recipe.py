"""Tests for places/locus_recipe.py — the canonical locus recipe (place#172, place#325 Stage 1).

Every absence or difference asserted here is paired with a presence or equality in the
same test, so each test can fail. No database: SimpleTestCase throughout.
"""
import base64
import copy
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
from unittest import mock

from django.conf import settings
from django.test import SimpleTestCase

from places import locus_recipe as lr

FIXTURE = os.path.join(settings.BASE_DIR, "scripts", "fixtures", "locus_recipe_vectors.json")
# The gateway's real 500-entry toponym_stoplist, captured 2026-10-21 (test data only).
GATEWAY_STOPLIST_FIXTURE = os.path.join(settings.BASE_DIR, "scripts", "fixtures", "gateway_toponym_stoplist.json")
JS_CHECKER = os.path.join(settings.BASE_DIR, "scripts", "check-locus-recipe.mjs")

CLUSTERING = {
    "weights": {"name": 0.35, "spatial": 0.20, "temporal": 0.15, "type": 0.15, "link": 0.15},
    "theta": 0.65, "tau_name": 0.75, "tau_link": 1.0, "same_ns_penalty": 0.15,
    "respect_distinct": True,
}
VIEWPORT = {"type": "Polygon", "coordinates": [[
    [13.0881224999999, 52.3382448], [13.7611609, 52.3382448],
    [13.7611609, 52.6755087000001], [13.0881224999999, 52.6755087000001],
    [13.0881224999999, 52.3382448],
]]}
BASE = {
    "v": 1,
    "retrieval": {"query": "Berlin", "mode": "in", "size": 100, "bounds": VIEWPORT,
                  "temporal": {"start": 1200, "end": 1900, "mode": "possibly", "undated": True}},
    "clustering": CLUSTERING,
    "members": ["gn:2950159", "wd:Q64", "tgn:7003712"],
}


def recipe(**changes):
    """A deep copy of BASE with dotted-path overrides; the value DELETE removes the key."""
    r = copy.deepcopy(BASE)
    for dotted, value in changes.items():
        node = r
        parts = dotted.split(".")
        for p in parts[:-1]:
            node = node[p]
        if value is DELETE:
            del node[parts[-1]]
        else:
            node[parts[-1]] = value
    return r


DELETE = object()


def load_fixture():
    with open(FIXTURE, encoding="utf-8") as f:
        return json.load(f)


class CanonicalFormTests(SimpleTestCase):

    def test_round_trip_through_canonical_json_and_token(self):
        canon = lr.canonical_json(BASE)
        self.assertEqual(lr.canonical_json(json.loads(canon)), canon)
        self.assertEqual(lr.parse_recipe(lr.parse_recipe(BASE)), lr.parse_recipe(BASE))
        token = lr.encode_recipe_token(BASE)
        self.assertRegex(token, r"^[A-Za-z0-9_-]+$")
        self.assertNotIn("=", token)
        self.assertEqual(lr.canonical_json(lr.decode_recipe_token(token)), canon)
        self.assertEqual(lr.recipe_pid(lr.decode_recipe_token(token)), lr.recipe_pid(BASE))

    def test_semantically_equal_recipes_give_identical_bytes_and_pid(self):
        noisy = {
            "members": ["place:TGN:7003712", " wd:Q64", "gn:2950159", "wd:Q64"],   # prefix, case, dupes, order
            "clustering": {
                "respect_distinct": True, "same_ns_penalty": 0.15000000000000002, "tau_link": 1,
                "tau_name": 0.75, "theta": 0.650000001,
                "weights": {"type": 0.15, "link": 0.15, "temporal": 0.15, "spatial": 0.2000000000001, "name": 0.35},
            },
            "retrieval": {
                "temporal": {"undated": True, "mode": "POSSIBLY", "end": 1900.0, "start": 1200},
                "bounds": {"type": "GeometryCollection", "geometries": [VIEWPORT]},   # single member unwrapped
                "size": 100, "mode": "In", "query": "  Berlin\t ",
                "ccodes": [], "types": None, "aat_types": [],
            },
            "v": 1,
        }
        self.assertEqual(lr.canonical_bytes(noisy), lr.canonical_bytes(BASE))
        self.assertEqual(lr.recipe_pid(noisy), lr.recipe_pid(BASE))
        self.assertEqual(lr.encode_recipe_token(noisy), lr.encode_recipe_token(BASE))
        # ...and the comparison is live: a different recipe does not match.
        other = recipe(**{"clustering.theta": 0.66})
        self.assertNotEqual(lr.canonical_bytes(other), lr.canonical_bytes(BASE))
        self.assertNotEqual(lr.recipe_pid(other), lr.recipe_pid(BASE))

    def test_each_field_change_changes_the_pid(self):
        same = lr.recipe_pid(recipe())
        self.assertEqual(same, lr.recipe_pid(BASE))
        changes = {
            "theta": {"clustering.theta": 0.66},
            "weight": {"clustering.weights.name": 0.3},
            "tau_name": {"clustering.tau_name": 0.7},
            "tau_link": {"clustering.tau_link": 0.9},
            "same_ns_penalty": {"clustering.same_ns_penalty": 0},
            "respect_distinct": {"clustering.respect_distinct": False},
            "member_added": {"members": BASE["members"] + ["gn:1"]},
            "member_removed": {"members": BASE["members"][:-1]},
            "member_local_case": {"members": ["gn:2950159", "wd:q64", "tgn:7003712"]},
            "query": {"retrieval.query": "berlin"},
            "mode": {"retrieval.mode": "exact"},
            "size": {"retrieval.size": 99},
            "bounds_absent": {"retrieval.bounds": DELETE},
            "bounds_6dp": {"retrieval.bounds": {"type": "Polygon", "coordinates": [[
                [13.088123, 52.3382448], [13.7611609, 52.3382448], [13.7611609, 52.6755087000001],
                [13.0881224999999, 52.6755087000001], [13.088123, 52.3382448]]]}},
            "temporal_start": {"retrieval.temporal.start": 1201},
            "temporal_mode": {"retrieval.temporal.mode": "definitely"},
            "undated": {"retrieval.temporal.undated": False},
            "ccodes": {"retrieval.ccodes": ["DE"]},
            "namespaces": {"retrieval.namespaces": ["gn"]},
            "aat_types": {"retrieval.aat_types": [300008347]},
        }
        pids = {}
        for name, change in changes.items():
            pids[name] = lr.recipe_pid(recipe(**change))
            self.assertNotEqual(pids[name], same, name)
        self.assertEqual(len(set(pids.values())), len(pids), "two different changes collided")

    def test_coordinate_noise_below_six_decimals_is_not_identity(self):
        a = recipe(**{"retrieval.bounds.coordinates": [[
            [13.0881224, 52.3382448], [13.7611609, 52.3382448], [13.7611609, 52.6755087],
            [13.0881224, 52.6755087], [13.0881224, 52.3382448]]]})
        b = recipe(**{"retrieval.bounds.coordinates": [[
            [13.08812241, 52.33824481], [13.76116091, 52.33824481], [13.76116091, 52.67550871],
            [13.08812241, 52.67550871], [13.08812241, 52.33824481]]]})     # 8th decimal noise
        c = recipe(**{"retrieval.bounds.coordinates": [[
            [13.088123, 52.3382448], [13.7611609, 52.3382448], [13.7611609, 52.6755087],
            [13.088123, 52.6755087], [13.088123, 52.3382448]]]})           # 6th decimal moved
        self.assertEqual(lr.recipe_pid(a), lr.recipe_pid(b))
        self.assertNotEqual(lr.recipe_pid(a), lr.recipe_pid(c))

    def test_numbers_are_quantised_half_up_on_the_shortest_repr(self):
        r = recipe(**{"clustering.theta": 0.12345, "clustering.same_ns_penalty": 1,
                      "clustering.tau_link": 1.0, "clustering.weights.name": 0.00004,
                      "clustering.weights.link": 1e-7, "clustering.weights.spatial": 0.99995})
        canon = lr.canonical_json(r)
        self.assertIn('"theta":0.1235', canon)
        self.assertIn('"same_ns_penalty":1,', canon)
        self.assertIn('"tau_link":1,', canon)
        self.assertIn('"link":0,"name":0,"spatial":1,', canon)
        self.assertNotIn("1.0", canon)
        self.assertNotIn("e-", canon)
        self.assertNotIn("-0", canon)

    def test_defaults_are_never_filled_for_clustering_but_stripped_for_retrieval(self):
        for key in lr._CLUSTERING_KEYS:
            with self.assertRaises(lr.RecipeError) as cm:
                lr.parse_recipe(recipe(**{f"clustering.{key}": DELETE}))
            self.assertEqual(cm.exception.path, f"clustering.{key}")
        canon = lr.canonical_json(recipe(**{"retrieval.temporal.undated": False,
                                            "retrieval.bounds": {"type": "GeometryCollection", "geometries": []},
                                            "retrieval.ccodes": []}))
        self.assertNotIn("undated", canon)
        self.assertNotIn("bounds", canon)
        self.assertNotIn("ccodes", canon)
        self.assertIn('"undated":true', lr.canonical_json(BASE))
        self.assertIn('"bounds"', lr.canonical_json(BASE))

    def test_canonical_text_is_sorted_compact_and_versioned(self):
        canon = lr.canonical_json(BASE)
        self.assertTrue(canon.startswith('{"clustering":{'))
        self.assertTrue(canon.endswith(',"v":1}'))
        self.assertNotIn(" ", canon)
        self.assertNotIn("\n", canon)
        keys = list(json.loads(canon).keys())
        self.assertEqual(keys, sorted(keys))
        self.assertEqual(list(json.loads(canon)["clustering"]["weights"]), ["link", "name", "spatial", "temporal", "type"])


class HashAndVersionTests(SimpleTestCase):

    def test_hash_is_over_exactly_the_canonical_bytes(self):
        b = lr.canonical_bytes(BASE)
        self.assertEqual(lr.recipe_digest(BASE), hashlib.sha256(b).digest())
        self.assertEqual(lr.recipe_pid(BASE), lr.pid_from_digest(hashlib.sha256(b).digest()))
        self.assertNotEqual(lr.recipe_pid(BASE), lr.pid_from_digest(hashlib.sha256(b + b"x").digest()))

    def test_version_is_part_of_the_hashed_bytes(self):
        b = lr.canonical_bytes(BASE)
        self.assertIn(b'"v":1', b)
        self.assertEqual(b.count(b'"v":'), 1)
        v2 = b.replace(b'"v":1', b'"v":2')
        self.assertNotEqual(v2, b)
        self.assertNotEqual(lr.pid_from_digest(hashlib.sha256(v2).digest()),
                            lr.pid_from_digest(hashlib.sha256(b).digest()))
        self.assertEqual(lr.pid_from_digest(hashlib.sha256(bytes(b)).digest()), lr.recipe_pid(BASE))
        # This code refuses to hash a version it does not speak.
        for v in (2, 0, "1", 1.5, True, None):
            with self.assertRaises(lr.RecipeError) as cm:
                lr.parse_recipe(recipe(v=v))
            self.assertEqual(cm.exception.path, "v")
        self.assertEqual(lr.parse_recipe(BASE)["v"], 1)
        self.assertEqual(lr.parse_recipe(recipe(v=1.0))["v"], 1)   # JSON 1.0 is 1, as it is in JS


class Place306Tests(SimpleTestCase):
    """User acceptances/rejections must not be able to enter the recipe as forced merges."""

    def test_no_field_can_carry_a_merge_judgement(self):
        self.assertEqual(lr.recipe_pid(BASE), lr.recipe_pid(recipe()))   # the base is accepted
        judgement = [{"a": "gn:2950159", "b": "wd:Q64", "relation_type": "exactMatch"}]
        for key in sorted(lr.FORBIDDEN_KEYS):
            for where in ("", "retrieval.", "clustering."):
                with self.assertRaises(lr.RecipeError, msg=f"{where}{key}") as cm:
                    lr.parse_recipe(recipe(**{f"{where}{key}": judgement}))
                self.assertEqual(cm.exception.path, f"{where}{key}")
                self.assertIn("place#306", cm.exception.message)

    def test_the_recipe_has_no_edge_or_pair_structure_at_all(self):
        allowed = lr._TOP_KEYS | lr._RETRIEVAL_KEYS | lr._CLUSTERING_KEYS | lr._TEMPORAL_KEYS | set(lr.WEIGHT_KEYS)
        self.assertIn("members", allowed)
        self.assertFalse(allowed & lr.FORBIDDEN_KEYS)
        # `link` is the hard-link WEIGHT (authority links reach the clusterer from the index at
        # resolution time), not a list of links; `members` is the only id list and it is flat.
        parsed = lr.parse_recipe(BASE)
        self.assertIsInstance(parsed["clustering"]["weights"]["link"], float)
        self.assertTrue(all(isinstance(m, str) for m in parsed["members"]))


class ValidationTests(SimpleTestCase):

    def assertRejected(self, obj, path):
        with self.assertRaises(lr.RecipeError) as cm:
            lr.parse_recipe(obj)
        self.assertEqual(cm.exception.path, path, cm.exception)
        self.assertTrue(cm.exception.message)
        return cm.exception

    def test_unknown_keys_are_rejected_everywhere(self):
        lr.parse_recipe(BASE)
        self.assertRejected(recipe(extra=1), "extra")
        self.assertRejected(recipe(**{"retrieval.fclasses": "A"}), "retrieval.fclasses")
        self.assertRejected(recipe(**{"retrieval.browse": True}), "retrieval.browse")
        self.assertRejected(recipe(**{"clustering.theta_bridge": 0.65}), "clustering.theta_bridge")
        self.assertRejected(recipe(**{"clustering.weights.phonetic": 0.1}), "clustering.weights.phonetic")
        self.assertRejected(recipe(**{"retrieval.temporal.fuzz": 1}), "retrieval.temporal.fuzz")
        self.assertRejected(recipe(**{"retrieval.bounds.bbox": [0, 0, 1, 1]}), "retrieval.bounds.bbox")

    def test_malformed_values_name_their_path(self):
        self.assertRejected([], "")
        self.assertRejected(recipe(members=[]), "members")
        self.assertRejected(recipe(members=["2950159"]), "members[0]")
        self.assertRejected(recipe(members=["gn:1", "gn:29 50"]), "members[1]")
        self.assertRejected(recipe(**{"clustering.theta": 1.5}), "clustering.theta")
        self.assertRejected(recipe(**{"clustering.theta": True}), "clustering.theta")
        self.assertRejected(recipe(**{"clustering.theta": float("nan")}), "clustering.theta")
        self.assertRejected(recipe(**{"clustering.theta": "0.65"}), "clustering.theta")
        self.assertRejected(recipe(**{"clustering.weights": {"name": 1}}), "clustering.weights.link")
        self.assertRejected(recipe(**{"clustering.respect_distinct": 1}), "clustering.respect_distinct")
        self.assertRejected(recipe(**{"retrieval.mode": "regex"}), "retrieval.mode")
        self.assertRejected(recipe(**{"retrieval.size": 0}), "retrieval.size")
        self.assertRejected(recipe(**{"retrieval.size": lr.SIZE_MAX + 1}), "retrieval.size")
        self.assertRejected(recipe(**{"retrieval.ccodes": ["DEU"]}), "retrieval.ccodes[0]")
        self.assertRejected(recipe(**{"retrieval.aat_types": ["300008347"]}), "retrieval.aat_types[0]")
        self.assertRejected(recipe(**{"retrieval.namespaces": ["gn:"]}), "retrieval.namespaces[0]")
        self.assertRejected(recipe(**{"retrieval.temporal": {"start": 1, "end": 2}}), "retrieval.temporal.mode")
        self.assertRejected(recipe(**{"retrieval.temporal": {"mode": "possibly"}}), "retrieval.temporal")
        self.assertRejected(recipe(**{"retrieval.temporal": {"start": 2, "end": 1, "mode": "possibly"}}), "retrieval.temporal")
        self.assertRejected(recipe(**{"retrieval.containment": "exact"}), "retrieval.containment")
        self.assertRejected(recipe(**{"retrieval.contained_in": ["osm:r1"]}), "retrieval.containment")
        self.assertRejected(recipe(**{"retrieval.bounds": DELETE, "retrieval.relation": "within"}), "retrieval.relation")
        self.assertRejected(recipe(**{"retrieval.bounds": DELETE, "retrieval.query": "  "}), "retrieval")
        self.assertRejected(recipe(**{"retrieval.query": "Ber\x07lin"}), "retrieval.query")
        self.assertRejected(recipe(**{"retrieval.query": "Ber\ud800lin"}), "retrieval.query")
        self.assertRejected(recipe(**{"retrieval.bounds": {"type": "Point", "coordinates": [0, 0]}}), "retrieval.bounds.type")

        def polygon(ring):
            return recipe(**{"retrieval.bounds": {"type": "Polygon", "coordinates": [ring]}})

        self.assertRejected(polygon([[0, 0], [1, 0], [1, 1], [0, 1]]), "retrieval.bounds.coordinates[0]")
        self.assertRejected(polygon([[0, 0, 0], [1, 0, 0], [1, 1, 0], [0, 0, 0]]), "retrieval.bounds.coordinates[0][0]")
        self.assertRejected(polygon([[0, 91], [1, 0], [1, 1], [0, 91]]), "retrieval.bounds.coordinates[0][0][1]")
        self.assertRejected(recipe(**{"retrieval.bounds": {"type": "GeometryCollection", "geometries": [
            {"type": "GeometryCollection", "geometries": [VIEWPORT]}]}}), "retrieval.bounds.geometries[0]")

    def test_token_decoding_rejects_garbage(self):
        good = lr.encode_recipe_token(BASE)
        self.assertEqual(lr.decode_recipe_token(good), lr.parse_recipe(BASE))
        for bad in ("", "not base64!", "e30", "eyJ2IjogMX0", good[:-10], "x" * (lr.TOKEN_MAX_BYTES + 1)):
            with self.assertRaises(lr.RecipeError, msg=repr(bad[:20])):
                lr.decode_recipe_token(bad)

        def token_of(text):
            return base64.urlsafe_b64encode(text.encode()).decode().rstrip("=")

        self.assertEqual(lr.decode_recipe_token(token_of(lr.canonical_json(BASE))), lr.parse_recipe(BASE))
        dup = lr.canonical_json(BASE)[:-1] + ',"v":1}'
        with self.assertRaises(lr.RecipeError) as cm:
            lr.decode_recipe_token(token_of(dup))
        self.assertEqual(cm.exception.path, "v")
        nan = lr.canonical_json(BASE).replace('"theta":0.65', '"theta":NaN')
        with self.assertRaises(lr.RecipeError):
            lr.decode_recipe_token(token_of(nan))


def load_gateway_stoplist():
    with open(GATEWAY_STOPLIST_FIXTURE, encoding="utf-8") as f:
        return json.load(f)["toponym_stoplist"]


class StoplistRulingTests(SimpleTestCase):
    """place#325 Stage 1 ruling 3 (SG, 2026-10-21): the stoplist is gateway data, not hashed."""

    def test_a_recipe_carrying_a_stoplist_is_refused_with_the_ruling(self):
        self.assertEqual(lr.parse_recipe(BASE)["clustering"]["theta"], 0.65)     # without it: accepted
        self.assertNotIn("stoplist", lr.parse_recipe(BASE)["clustering"])
        for value in (["city", "town"], [], "city"):
            with self.assertRaises(lr.RecipeError) as cm:
                lr.parse_recipe(recipe(**{"clustering.stoplist": value}))
            self.assertEqual(cm.exception.path, "clustering.stoplist")
            self.assertIn("place#325 ruling 3", cm.exception.message)
            self.assertIn("gateway", cm.exception.message)
        # The reason is the retired-key message, not the generic one other keys get.
        with self.assertRaises(lr.RecipeError) as cm:
            lr.parse_recipe(recipe(**{"clustering.stoplists": ["city"]}))
        self.assertEqual(cm.exception.message, "unknown key")

    def test_token_from_a_response_with_the_real_gateway_stoplist_stays_short(self):
        """The 414 cannot return: a cluster-card recipe built beside a response that
        carries the gateway's real 500-entry stoplist is far under the request-line
        limits (gunicorn 4094 bytes by default, nginx 8k), because the list stays out."""
        stoplist = load_gateway_stoplist()
        self.assertEqual(len(stoplist), 500)                               # the fixture is the real list
        response = {   # what /atlas/search/ hands the card: the request echo plus the clustering fuel
            "request": {"query": "Berlin", "mode": "in", "size": 100, "geom": "full",
                        "include_hard_links": True, "include_clustering_fields": True,
                        "include_embeddings": True, "bounds": VIEWPORT, "start_year": 1200,
                        "end_year": 1900, "undated": True, "temporal_mode": "possibly"},
            "toponym_stoplist": stoplist,
        }
        members = [f"whg:{1000 + i}:src-{i}" for i in range(10)] + ["gn:2950159", "wd:Q64", "tgn:7003712"]
        r = {"v": 1, "retrieval": lr.retrieval_from_search_body(response["request"]),
             "clustering": CLUSTERING, "members": members}
        token = lr.encode_recipe_token(r)
        request_line = f"GET /atlas/locus/?r={token} HTTP/1.1"
        self.assertLess(len(token), 2000, len(token))
        self.assertLess(len(request_line), 4094)
        # ...and the ceiling is one the old design broke: the same recipe with the list
        # inside it (as it was before the ruling) is over nginx's 8k on its own.
        old_bytes = lr.canonical_json(r)[:-1].encode() + b',"stoplist":' + json.dumps(stoplist, ensure_ascii=False).encode() + b"}"
        self.assertGreater(len(base64.urlsafe_b64encode(old_bytes)), 8192)


class PidTests(SimpleTestCase):

    def test_shape_alphabet_and_legacy_disjointness(self):
        pid = lr.recipe_pid(BASE)
        self.assertRegex(pid, lr.PID_RE)
        for ch in "ILOUZ":
            self.assertNotIn(ch, pid)
        self.assertEqual(len(lr.PID_ALPHABET), 31)
        self.assertEqual(len(set(lr.PID_ALPHABET)), 31)
        self.assertFalse(lr.is_legacy_locus_id(pid))
        self.assertTrue(lr.is_legacy_locus_id("14047598"))
        self.assertFalse(lr.is_legacy_locus_id("4047598"))
        with self.assertRaises(ValueError):
            lr.normalise_pid("14047598")

    def test_normalise_accepts_lenient_input_and_verifies_the_check_digit(self):
        pid = lr.recipe_pid(BASE)
        compact = pid.replace("-", "")
        self.assertEqual(lr.normalise_pid(pid), pid)
        self.assertEqual(lr.normalise_pid(compact.lower()), pid)
        self.assertEqual(lr.normalise_pid(" " + pid.replace("1", "l").replace("0", "o") + " "), pid)
        # Every single substitution at every position is caught (no 0/Z blind spot: the
        # alphabet has 31 symbols, so no two digits differ by the modulus)...
        caught = 0
        for i in range(12):
            for ch in lr.PID_ALPHABET + "Z":
                if ch == compact[i]:
                    continue
                with self.assertRaises(ValueError):
                    lr.normalise_pid(compact[:i] + ch + compact[i + 1:])
                caught += 1
        self.assertEqual(caught, 12 * 31)
        # ...and every adjacent transposition of unequal digits.
        swaps = 0
        for i in range(11):
            if compact[i] == compact[i + 1]:
                continue
            with self.assertRaises(ValueError):
                lr.normalise_pid(compact[:i] + compact[i + 1] + compact[i] + compact[i + 2:])
            swaps += 1
        self.assertGreater(swaps, 0)
        for bad in (compact + "0", compact[:11], compact[:11] + "U", 123, ""):
            with self.assertRaises(ValueError):
                lr.normalise_pid(bad)

    def test_pid_is_the_first_54_bits_of_the_digest_in_base_31(self):
        self.assertGreater(31 ** 11, 2 ** 54)          # eleven digits always suffice...
        self.assertLess(31 ** 11, 2 ** 55)             # ...and the hash bits are not padded with slack
        self.assertEqual(lr.pid_from_digest(bytes(32)), "0000-0000-0000")
        # Value 1 (bit 53 set, the last of the 54) -> digits ...0001, check = 11*1 mod 31 = 11 = 'B'.
        one = bytes([0] * 6 + [0b00000100] + [0] * 25)
        self.assertEqual(lr.pid_from_digest(one), "0000-0000-001B")
        # Value 31 -> base-31 "10": the two low digits are 1, 0; check = 10*1 mod 31 = 10 = 'A'.
        thirty_one = bytes([0] * 6 + [0b01111100] + [0] * 25)
        self.assertEqual(lr.pid_from_digest(thirty_one), "0000-0000-010A")
        # Bit 54 and beyond do not matter; bit 53 does.
        a = bytes([0] * 6 + [0b11111100, 0x00] + [0] * 24)
        b = bytes([0] * 6 + [0b11111111, 0xFF] + [0] * 24)
        self.assertEqual(lr.pid_from_digest(a), lr.pid_from_digest(b))
        c = bytes([0] * 6 + [0b11111000, 0x00] + [0] * 24)
        self.assertNotEqual(lr.pid_from_digest(a), lr.pid_from_digest(c))
        # All 54 bits set is 2**54 - 1, which the digits must reproduce exactly.
        digits = [lr.PID_ALPHABET.index(ch) for ch in lr.pid_from_digest(bytes([0xFF] * 32)).replace("-", "")[:11]]
        self.assertEqual(sum(d * 31 ** (10 - i) for i, d in enumerate(digits)), 2 ** 54 - 1)


class SearchBodyAdapterTests(SimpleTestCase):
    """The retrieval block is the gateway search body crc_client actually sends."""

    def test_adapter_accepts_every_key_crc_client_emits(self):
        from api import crc_client
        captured = {}

        def fake_post(url, json=None, headers=None, timeout=None):
            captured["body"] = json
            return mock.Mock(status_code=200, json=lambda: {"hits": [], "total": 0})

        options = {
            "qstr": "Berlin", "mode": "in", "size": 100, "countries": "de,at", "types": ["aat:300008347"],
            "aat_types": [300008347], "namespaces": ["gn", "wd"], "bounds": VIEWPORT,
            "contained_in": ["place:osm:r62422"], "containment": "exact", "relation": "intersects",
            "temporal": True, "start": 1200, "end": 1900, "undated": True, "temporal_mode": "possibly",
            # Atlas option keys crc_client ignores (not part of the gateway body, so not of the recipe):
            "idx": "whg", "fclasses": "A,P", "cluster": True, "spatial": "region", "regions": [], "userareas": [],
        }
        with mock.patch.object(crc_client, "_is_enabled", return_value=True), \
             mock.patch.object(crc_client, "_gateway_url", return_value="http://gateway.test"), \
             mock.patch.object(crc_client, "_headers", return_value={}), \
             mock.patch.object(crc_client.requests, "post", side_effect=fake_post):
            data, failure = crc_client.crc_search_status(options, user=object())
        self.assertIsNone(failure)
        body = captured["body"]
        self.assertEqual(body["geom"], "full")   # the fixed contract is really in the body...
        retrieval = lr.retrieval_from_search_body(body)
        self.assertNotIn("geom", retrieval)       # ...and really dropped by the adapter.
        parsed = lr.parse_recipe({"v": 1, "retrieval": retrieval, "clustering": CLUSTERING, "members": ["gn:1"]})
        r = parsed["retrieval"]
        self.assertEqual(r["query"], "Berlin")
        self.assertEqual(r["ccodes"], ["AT", "DE"])
        self.assertEqual(r["contained_in"], ["osm:r62422"])
        self.assertEqual(r["temporal"], {"end": 1900, "mode": "possibly", "start": 1200, "undated": True})
        # A key crc_client starts sending that the recipe does not know must fail loudly, not vanish.
        with self.assertRaises(lr.RecipeError) as cm:
            lr.retrieval_from_search_body({**body, "new_filter": 1})
        self.assertEqual(cm.exception.path, "body.new_filter")
        for k in ("browse", "offset"):
            with self.assertRaises(lr.RecipeError) as cm:
                lr.retrieval_from_search_body({**body, k: 1})
            self.assertEqual(cm.exception.path, f"body.{k}")


class VectorFixtureTests(SimpleTestCase):
    """The committed vectors are the cross-language contract; Python must reproduce them exactly."""

    def test_python_reproduces_every_vector(self):
        fx = load_fixture()
        self.assertEqual(fx["recipe_version"], lr.RECIPE_VERSION)
        self.assertGreaterEqual(len(fx["valid"]), 5)
        self.assertGreaterEqual(len(fx["invalid"]), 30)
        for v in fx["valid"]:
            self.assertEqual(lr.canonical_json(v["input"]), v["canonical"], v["name"])
            self.assertEqual(lr.recipe_pid(v["input"]), v["pid"], v["name"])
            self.assertEqual(lr.encode_recipe_token(v["input"]), v["token"], v["name"])
            self.assertEqual(lr.canonical_json(lr.decode_recipe_token(v["token"])), v["canonical"], v["name"])
        by_name = {v["name"]: v for v in fx["valid"]}
        self.assertEqual(by_name["atlas_default"]["pid"], by_name["atlas_default_reordered_noisy"]["pid"])
        self.assertNotEqual(by_name["atlas_default"]["pid"], by_name["spatial_sweep_no_query"]["pid"])
        for v in fx["invalid"]:
            with self.assertRaises(lr.RecipeError, msg=v["name"]) as cm:
                lr.parse_recipe(v["input"])
            self.assertEqual(cm.exception.path, v["error_path"], v["name"])
            if v.get("check_message"):
                self.assertEqual(cm.exception.message, v["error"], v["name"])
        self.assertIn("stoplist_retired", {v["name"] for v in fx["invalid"] if v.get("check_message")})
        for a in fx["adapter"]:
            if "error_path" in a:
                with self.assertRaises(lr.RecipeError) as cm:
                    lr.retrieval_from_search_body(a["body"])
                self.assertEqual(cm.exception.path, a["error_path"], a["name"])
            else:
                r = {"v": 1, "retrieval": lr.retrieval_from_search_body(a["body"]),
                     "clustering": a["clustering"], "members": a["members"]}
                self.assertEqual(lr.canonical_json(r), a["canonical"], a["name"])
        for p in fx["pids"]["valid"]:
            self.assertEqual(lr.normalise_pid(p["input"]), p["canonical"])
        for p in fx["pids"]["invalid"]:
            with self.assertRaises(ValueError, msg=p["why"]):
                lr.normalise_pid(p["input"])


class BrowserTwinTests(SimpleTestCase):
    """whg/webpack/js/locus_recipe.js must agree with the Python module byte for byte."""

    def _node(self):
        node = shutil.which("node")
        if not node:
            self.skipTest("node is not available; run `npm run test:locus-recipe` where it is")
        return node

    def test_browser_module_matches_the_python_vectors(self):
        result = subprocess.run([self._node(), JS_CHECKER], capture_output=True, text=True, timeout=120)
        self.assertEqual(result.returncode, 0, "locus recipe JS twin has drifted:\n" + result.stdout + result.stderr)
        self.assertIn("checks passed", result.stdout)

    def test_the_checker_fails_on_a_tampered_vector(self):
        """Proves the parity check can fail: a one-character change to one expected PID is caught."""
        node = self._node()
        fx = load_fixture()
        pid = fx["valid"][0]["pid"]
        fx["valid"][0]["pid"] = pid[:-1] + ("0" if pid[-1] != "0" else "1")
        with tempfile.TemporaryDirectory() as d:
            tampered = os.path.join(d, "vectors.json")
            with open(tampered, "w", encoding="ascii") as f:
                json.dump(fx, f, ensure_ascii=True)
            result = subprocess.run([node, JS_CHECKER], capture_output=True, text=True, timeout=120,
                                    env={**os.environ, "LOCUS_RECIPE_VECTORS": tampered})
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(fx["valid"][0]["name"], result.stderr)
        self.assertIn("pid", result.stderr)
