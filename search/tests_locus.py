"""The unminted locus page and resolver (place#325 Stage 2, items 3-4; place#172).

Run locally (never in the prod container — see reference_never_run_suite_on_prod):

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test search.tests_locus --settings=whg.settings_localtest

Written so no assertion can pass vacuously: every refusal is paired with the
beta-tester case on the same route that is let through; every invalid-token
case is paired with a valid token on the same page that renders its members;
the gateway is faked at ``api.crc_client.requests.post`` (the one HTTP seam),
so the REAL ``crc_search_status`` body builder runs and the round trip
``retrieval -> options -> body -> retrieval_from_search_body -> retrieval``
is asserted against the body the fake actually received.
"""
import copy
import json
import os
from unittest.mock import patch

import requests
from django.conf import settings
from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings

from places import locus_recipe as lr
from search.locus import describe_recipe, search_options_for_retrieval

GATEWAY = "http://gateway.invalid"
POST = "api.crc_client.requests.post"


class _Resp:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.text = json.dumps(self._payload)

    def json(self):
        return self._payload


# Shipped clustering params with values no default shares, so a recipe that
# leaned on a default instead of the shipped literal would hash differently.
PARAMS = {
    "version": 7, "calibrated": True,
    "weights": {"name": 0.41, "spatial": 0.23, "temporal": 0.11, "type": 0.09, "link": 0.16},
    "thresholds": {"theta_query": 0.33, "theta_bridge": 0.4, "theta_synth": 0.5,
                   "theta_synth_structural": 0.6, "tau_name": 0.71, "tau_link": 0.9},
    "same_ns_penalty": 0.12,
}
# The gateway's REAL 500-entry toponym_stoplist (captured 2026-10-21): the
# resolver must take it from the response (place#325 ruling 3), and a token must
# stay short beside it (the 414 it once caused).
with open(os.path.join(settings.BASE_DIR, "scripts", "fixtures", "gateway_toponym_stoplist.json"),
          encoding="utf-8") as _f:
    STOPLIST = json.load(_f)["toponym_stoplist"]
TOKEN_CEILING = 2000   # characters; gunicorn's request-line default is 4094 bytes, nginx's 8k
EMB_A = [100, -20, 30, 5]
EMB_B = [-40, 90, 10, 0]
HITS = [
    {"place_id": "gn:1", "title": "Berlin", "namespace": "gn", "repr_point": [13.4, 52.52],
     "names": [{"label": "Berlin", "phon_emb": EMB_A}], "temporal_range": [1200, 2000], "ccodes": ["DE"]},
    {"place_id": "wd:Q64", "title": "Berlin", "namespace": "wd", "repr_point": [13.41, 52.51],
     "names": [{"label": "Berlin", "phon_emb": EMB_A}], "temporal_range": [1230, 2000], "ccodes": ["DE"]},
    {"place_id": "gn:2", "title": "Berlin", "namespace": "gn", "repr_point": [-71.2, 44.47],
     "names": [{"label": "Berlin", "phon_emb": EMB_B}], "temporal_range": [1771, 2000], "ccodes": ["US"]},
]
SEARCH_OK = {"hits": HITS, "total": 3, "edges": [], "clustering_params": PARAMS,
             "toponym_stoplist": STOPLIST, "facets": {}}


class FakeGateway:
    """Answers /api/search with SEARCH_OK and records every body it was sent."""

    def __init__(self, hits=None):
        self.bodies = []
        self.hits = hits

    def __call__(self, url, json=None, **kwargs):
        if url.endswith("/api/search"):
            self.bodies.append(copy.deepcopy(json))
            payload = copy.deepcopy(SEARCH_OK)
            if self.hits is not None:
                payload["hits"] = copy.deepcopy(self.hits)
                payload["total"] = len(self.hits)
            return _Resp(200, payload)
        if url.endswith("/api/places"):
            ids = set((json or {}).get("ids") or [])
            known = {h["place_id"]: h for h in (self.hits if self.hits is not None else HITS)}
            return _Resp(200, {"places": [copy.deepcopy(known[i]) for i in ids if i in known]})
        return _Resp(404, {})


CLUSTERING = {
    "weights": PARAMS["weights"], "theta": 0.33, "tau_name": 0.71, "tau_link": 0.9,
    "same_ns_penalty": 0.12, "respect_distinct": True,
}
VIEWPORT = {"type": "Polygon", "coordinates": [[
    [13.0881224999999, 52.3382448], [13.7611609, 52.3382448],
    [13.7611609, 52.6755087000001], [13.0881224999999, 52.6755087000001],
    [13.0881224999999, 52.3382448],
]]}
SIMPLE = {"query": "Berlin", "mode": "in", "size": 100}
FULL = {"query": "Berlin", "mode": "starts", "size": 250, "ccodes": ["US", "DE"], "types": ["city"],
        "aat_types": [300008347, 300008389], "namespaces": ["wd", "gn"], "bounds": VIEWPORT,
        "contained_in": ["osm:r62422"], "containment": "exact", "relation": "within",
        "temporal": {"start": -500, "end": 1500, "mode": "definitely", "undated": True}}
SWEEP = {"mode": "fuzzy", "size": 40, "bounds": VIEWPORT}
END_ONLY = {"query": "Berlin", "mode": "exact", "size": 7,
            "temporal": {"end": 1900, "mode": "possibly", "undated": False}}


def recipe(retrieval, members=("gn:1", "wd:Q64")):
    return {"v": 1, "retrieval": copy.deepcopy(retrieval), "clustering": copy.deepcopy(CLUSTERING),
            "members": list(members)}


def token(retrieval=SIMPLE, **kw):
    return lr.encode_recipe_token(recipe(retrieval, **kw))


@override_settings(CRC_GATEWAY_URL=GATEWAY, CRC_GATEWAY_TIMEOUT=1)
class LocusBase(TestCase):
    @classmethod
    def setUpTestData(cls):
        User = get_user_model()
        cls.normal = User.objects.create_user(
            username="locus_normal", email="ln@example.org", password="pw",
            given_name="Norma", surname="User", role="normal")
        cls.beta = User.objects.create_user(
            username="locus_beta", email="lb@example.org", password="pw",
            given_name="Bea", surname="Tester", role="beta_tester")

    def setUp(self):
        self.assertFalse(self.normal.can_access_beta)
        self.assertTrue(self.beta.can_access_beta)
        self.gateway = FakeGateway()

    def page(self, tok):
        return self.client.get("/atlas/locus/", {"r": tok})

    def resolve(self, tok):
        return self.client.post("/atlas/locus/resolve/", data=json.dumps({"r": tok}),
                                content_type="application/json")


class GateTests(LocusBase):
    def test_page_gate(self):
        tok = token()
        resp = self.page(tok)
        self.assertEqual(resp.status_code, 302)
        self.assertTrue(resp["Location"].startswith("/accounts/login/?next=/atlas/locus/"), resp["Location"])
        self.client.force_login(self.normal)
        self.assertEqual(self.page(tok).status_code, 404)
        self.client.force_login(self.beta)
        resp = self.page(tok)
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, 'id="locus_recipe_members"')

    def test_resolve_gate(self):
        tok = token()
        with patch(POST, side_effect=self.gateway) as post:
            self.assertEqual(self.resolve(tok).status_code, 403)
            self.client.force_login(self.normal)
            self.assertEqual(self.resolve(tok).status_code, 403)
            post.assert_not_called()
            self.client.force_login(self.beta)
            resp = self.resolve(tok)
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(len(resp.json()["hits"]), 3)
        self.assertEqual(len(self.gateway.bodies), 1)
        # The resolver hands the browser the gateway's own stoplist (ruling 3); the
        # recipe it echoes carries none.
        self.assertEqual(resp.json()["toponym_stoplist"], STOPLIST)
        self.assertEqual(len(resp.json()["toponym_stoplist"]), 500)
        self.assertIn("theta", resp.json()["recipe"]["clustering"])
        self.assertNotIn("stoplist", resp.json()["recipe"]["clustering"])

    def test_tokens_stay_under_the_request_line_ceiling(self):
        """Every retrieval shape, beside a response carrying the real 500-entry
        stoplist, gives a token far under the request-line limits."""
        self.assertEqual(len(SEARCH_OK["toponym_stoplist"]), 500)
        lengths = {name: len(token(r, members=[f"whg:{100 + i}:s{i}" for i in range(12)]))
                   for name, r in (("simple", SIMPLE), ("full", FULL), ("sweep", SWEEP), ("end_only", END_ONLY))}
        for name, n in lengths.items():
            self.assertLess(n, TOKEN_CEILING, name)
        self.assertGreater(min(lengths.values()), 100)   # a real token, not an empty string

    def test_resolve_needs_post(self):
        self.client.force_login(self.beta)
        self.assertEqual(self.client.get("/atlas/locus/resolve/", {"r": token()}).status_code, 405)


class PageTokenTests(LocusBase):
    """A tampered or truncated token is a plain 400 error page, never a 500;
    a valid one renders the recipe and its members."""

    def setUp(self):
        super().setUp()
        self.client.force_login(self.beta)

    def test_valid_token_renders_recipe_and_members(self):
        resp = self.page(token(FULL, members=["wd:Q64", "gn:1", "tgn:7003712"]))
        self.assertEqual(resp.status_code, 200)
        html = resp.content.decode()
        for m in ("gn:1", "wd:Q64", "tgn:7003712"):
            self.assertIn(f'data-member="{m}"', html)
        self.assertIn("shareable but not permanent", html)
        self.assertIn('id="locus_mint"', html)
        self.assertIn("disabled", html)
        self.assertIn('id="locus_recipe"', html)              # json_script for the page script
        self.assertIn("starts with", html)                     # human-readable mode
        self.assertIn("500 BCE to 1500", html)
        self.assertIn("undated records included", html)
        self.assertIn("osm:r62422", html)
        # The stoplist is not in the recipe (ruling 3): the row is a placeholder the
        # page script fills from the re-run, and no entry of the list is in the page.
        self.assertIn('id="locus_stoplist"', html)
        self.assertIn("not in the recipe", html)
        self.assertIn("Berlin", html)
        self.assertNotIn(STOPLIST[0], html)
        self.assertNotIn(STOPLIST[-1], html)
        self.assertNotIn("locus-error", html)
        # The page never presents a would-be PID as an identifier.
        self.assertNotRegex(html, r"\b[0-9A-HJKMNP-TV-Y]{4}-[0-9A-HJKMNP-TV-Y]{4}-[0-9A-HJKMNP-TV-Y]{4}\b")

    def test_invalid_tokens_are_clear_400s(self):
        good = token()
        tampered = lr.encode_recipe_token(recipe(SIMPLE))
        # A syntactically fine token whose recipe smuggles a merge judgement.
        obj = recipe(SIMPLE)
        obj["edges"] = [["gn:1", "wd:Q64"]]
        import base64
        smuggled = base64.urlsafe_b64encode(json.dumps(obj).encode()).decode().rstrip("=")
        cases = {
            "garbage": "not a token!!",
            "truncated": tampered[:-12],
            "smuggled": smuggled,
            "wrong_version": base64.urlsafe_b64encode(json.dumps({**recipe(SIMPLE), "v": 2}).encode()).decode().rstrip("="),
            "empty": "",
        }
        for name, bad in cases.items():
            with self.subTest(case=name):
                resp = self.page(bad)
                self.assertEqual(resp.status_code, 400)
                html = resp.content.decode()
                self.assertIn("This locus link is not valid", html)
                self.assertNotIn('id="locus_recipe_members"', html)
                self.assertNotIn('id="locus_recipe"', html)
        self.assertIn("place#306", self.page(cases["smuggled"]).content.decode())
        self.assertIn("unsupported recipe version", self.page(cases["wrong_version"]).content.decode())
        self.assertIn("no recipe token", self.page("").content.decode())
        # The pairing: the same page, with a good token, has what the bad ones lack.
        self.assertIn('id="locus_recipe_members"', self.page(good).content.decode())

    def test_non_canonical_token_is_normalised(self):
        obj = recipe(SIMPLE, members=["WD:Q64", "gn:1", "gn:1"])
        obj["retrieval"]["query"] = "  Berlin  "
        loose = __import__("base64").urlsafe_b64encode(json.dumps(obj).encode()).decode().rstrip("=")
        resp = self.page(loose)
        self.assertEqual(resp.status_code, 200)
        self.assertIn("The address was normalised", resp.content.decode())
        self.assertEqual(resp.context["token"], lr.encode_recipe_token(obj))
        self.assertNotIn("The address was normalised", self.page(token()).content.decode())


class SearchEchoTests(LocusBase):
    """/atlas/search/ echoes the body it sent, and the echo is what a card
    builds its recipe from (Stage 1 note)."""

    def test_request_echo_is_the_body_sent(self):
        self.client.force_login(self.beta)
        with patch(POST, side_effect=self.gateway):
            resp = self.client.post("/atlas/search/", data=json.dumps({
                "qstr": "Berlin", "mode": "in", "size": 50, "countries": ["DE"],
                "temporal": True, "start": 1200, "end": 1900, "temporal_mode": "possibly", "undated": True,
            }), content_type="application/json")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(len(self.gateway.bodies), 1)
        self.assertEqual(data["request"], self.gateway.bodies[0])
        self.assertIn("include_clustering_fields", data["request"])   # the fixed flags travel too
        parsed = lr.parse_recipe(recipe(lr.retrieval_from_search_body(data["request"])))
        self.assertEqual(parsed["retrieval"], {
            "query": "Berlin", "mode": "in", "size": 50, "ccodes": ["DE"],
            "temporal": {"start": 1200, "end": 1900, "mode": "possibly", "undated": True},
        })

    def test_echo_survives_a_timeout(self):
        self.client.force_login(self.beta)
        with patch(POST, side_effect=requests.Timeout("slow")):
            resp = self.client.post("/atlas/search/", data=json.dumps({"qstr": "Berlin"}),
                                    content_type="application/json")
        self.assertEqual(resp.status_code, 200)
        self.assertIs(resp.json()["timeout"], True)
        self.assertEqual(resp.json()["request"]["query"], "Berlin")


class ResolveRoundTripTests(LocusBase):
    """The resolver sends the gateway exactly the recipe's retrieval, for
    every retrieval field, proved on the body the fake gateway received."""

    def setUp(self):
        super().setUp()
        self.client.force_login(self.beta)

    def test_every_retrieval_shape_round_trips(self):
        for name, retrieval in (("simple", SIMPLE), ("full", FULL), ("sweep", SWEEP), ("end_only", END_ONLY)):
            with self.subTest(retrieval=name):
                self.gateway.bodies.clear()
                rec = lr.parse_recipe(recipe(retrieval))
                with patch(POST, side_effect=self.gateway):
                    resp = self.resolve(lr.encode_recipe_token(rec))
                self.assertEqual(resp.status_code, 200, resp.content[:300])
                self.assertEqual(len(self.gateway.bodies), 1)
                body = self.gateway.bodies[0]
                # (1) the body the gateway got, re-read through the adapter, IS the recipe's retrieval
                again = lr.parse_recipe({**rec, "retrieval": lr.retrieval_from_search_body(body)})
                self.assertEqual(again["retrieval"], rec["retrieval"])
                # (2) the resolver echoes both
                data = resp.json()
                self.assertEqual(data["request"], body)
                self.assertEqual(data["recipe"], rec)
                self.assertEqual(data["record_urls"]["gn:1"], "/entity/place:gn:1/")
                self.assertEqual(data["record_urls"]["wd:Q64"], "/entity/place:wd:Q64/")
                # (3) the fixed-contract flags are present on the wire but not in the recipe
                self.assertIs(body["include_embeddings"], True)
                self.assertNotIn("include_embeddings", again["retrieval"])

    def test_options_builder_direct(self):
        """The inverse adapter alone, without a view: a changed value must show."""
        opts = search_options_for_retrieval(lr.parse_recipe(recipe(FULL))["retrieval"])
        self.assertEqual(opts["qstr"], "Berlin")
        self.assertEqual(opts["countries"], ["DE", "US"])
        self.assertEqual(opts["temporal_mode"], "definitely")
        self.assertIs(opts["undated"], True)
        self.assertEqual(opts["contained_in"], ["osm:r62422"])
        self.assertNotIn("countries", search_options_for_retrieval(SIMPLE))

    def test_resolver_refuses_when_it_cannot_reproduce_the_retrieval(self):
        """If the inverse adapter drifted from the body builder, the resolver
        says so (500 with the field) rather than answering a different question."""
        from search import locus as locus_mod
        real = locus_mod.search_options_for_retrieval

        def dropping_countries(retrieval):
            out = real(retrieval)
            out.pop("countries", None)
            return out

        with patch.object(locus_mod, "search_options_for_retrieval", side_effect=dropping_countries), \
                patch(POST, side_effect=self.gateway):
            resp = self.resolve(token(FULL))
        self.assertEqual(resp.status_code, 500)
        self.assertEqual(resp.json()["error"], "resolver could not reproduce the recipe's retrieval")
        self.assertIn("ccodes", resp.json()["detail"])
        # Control: the undoctored adapter on the same token answers 200.
        with patch(POST, side_effect=self.gateway):
            self.assertEqual(self.resolve(token(FULL)).status_code, 200)

    def test_bad_token_is_400_json(self):
        with patch(POST, side_effect=self.gateway) as post:
            resp = self.resolve("??")
            self.assertEqual(resp.status_code, 400)
            self.assertEqual(resp.json()["error"], "invalid recipe")
            post.assert_not_called()

    def test_gateway_failures_keep_their_statuses(self):
        with patch(POST, side_effect=requests.Timeout("slow")):
            resp = self.resolve(token())
        self.assertEqual(resp.status_code, 200)
        self.assertIs(resp.json()["timeout"], True)
        self.assertEqual(resp.json()["recipe"]["members"], ["gn:1", "wd:Q64"])
        with patch(POST, side_effect=requests.ConnectionError("down")):
            self.assertEqual(self.resolve(token()).status_code, 503)

    def test_contributed_hit_links_to_the_record_page_and_withheld_is_counted(self):
        from datasets.models import Dataset
        from places.models import Place
        mine = Dataset.objects.create(owner=self.beta, label="locus_mine", title="Mine", description="D", public=False)
        theirs = Dataset.objects.create(owner=self.normal, label="locus_theirs", title="Theirs", description="D",
                                        public=False)
        p = Place.objects.create(title="Mine 1", src_id="m1", dataset=mine, ccodes=[])
        Place.objects.create(title="Theirs 1", src_id="t1", dataset=theirs, ccodes=[])
        hits = HITS + [
            {"place_id": f"whg:{mine.id}:m1", "title": "Mine 1", "namespace": "whg", "repr_point": [1, 1]},
            {"place_id": f"whg:{theirs.id}:t1", "title": "Theirs 1", "namespace": "whg", "repr_point": [2, 2]},
        ]
        gw = FakeGateway(hits=hits)
        with patch(POST, side_effect=gw):
            resp = self.resolve(token())
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        pids = [h["place_id"] for h in data["hits"]]
        self.assertIn(f"whg:{mine.id}:m1", pids)
        self.assertNotIn(f"whg:{theirs.id}:t1", pids)             # place#310: hidden, not served
        self.assertEqual(data["withheld"], {"count": 1, "reason": "records in datasets not visible to this account"})
        self.assertEqual(data["record_urls"][f"whg:{mine.id}:m1"], f"/atlas/place/{p.id}/")
        # The Atlas modal's endpoint says the same for the same record, and
        # nothing for an authority record (which has no Record page).
        with patch(POST, side_effect=gw):
            mine_resp = self.client.get("/atlas/place/", {"id": f"whg:{mine.id}:m1"})
            auth_resp = self.client.get("/atlas/place/", {"id": "gn:1"})
        self.assertEqual(mine_resp.status_code, 200)
        self.assertEqual(mine_resp.json()["record_url"], f"/atlas/place/{p.id}/")
        self.assertEqual(auth_resp.status_code, 200)
        self.assertNotIn("record_url", auth_resp.json())


class DescribeTests(TestCase):
    def test_describe_is_literal_and_complete(self):
        d = describe_recipe(lr.parse_recipe(recipe(FULL)))
        labels = [k for k, _ in d["retrieval"]]
        self.assertEqual(labels, ["Name", "Candidates", "Countries", "Sources", "Source types", "AAT types",
                                  "Within", "Contained in", "Spatial relation", "When"])
        self.assertEqual(dict(d["weights"])["Name"], "0.41")
        self.assertEqual(dict(d["clustering"])["Merge threshold (θ)"], "0.33")
        self.assertNotIn("stoplist", d)
        self.assertNotIn("stoplist_count", d)
        self.assertIn("clustering", d)
        self.assertEqual(d["members"], ["gn:1", "wd:Q64"])
        s = describe_recipe(lr.parse_recipe(recipe(SWEEP)))
        self.assertIn("spatial sweep", s["retrieval"][0][1])
        self.assertEqual([k for k, _ in s["retrieval"]], ["Name", "Candidates", "Within"])
