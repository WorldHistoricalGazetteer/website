"""Atlas JSON endpoints: beta gating, and "could not ask" vs "not found".

Run locally (never in the prod container — see reference_never_run_suite_on_prod):

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test search.tests_atlas --settings=whg.settings_localtest

🛑 Written so that no assertion can pass vacuously:

* every "refused" (403) case is paired with a beta-tester case on the SAME
  endpoint that is let through, so a gate that refused everyone fails;
* every gateway-failure case (503/504) is paired with a clean-miss case that
  must still be 404, so a change that made everything 503 fails;
* the gateway is faked at ``api.crc_client.requests.post`` — the single HTTP
  seam — so the real ``crc_places`` / ``crc_search_status`` failure
  classification runs, and a test that never reached the gateway is caught by
  asserting the fake WAS called.

Nothing here touches the network or Elasticsearch: ``requests.post`` is
patched for every test that can reach the gateway, and the gateway URL is a
reserved ``.invalid`` host so an unpatched call could not resolve anyway.
"""

import json
from unittest.mock import patch

import requests
from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings

GATEWAY = "http://gateway.invalid"
POST = "api.crc_client.requests.post"


class _Resp:
    """Minimal stand-in for a requests.Response."""

    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.text = json.dumps(self._payload)

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}")


PLACE = {"place_id": "gn:2643743", "title": "London", "namespace": "gn",
         "names": [{"label": "London"}], "types": [], "geometries": []}

SEARCH_OK = {
    "hits": [{
        "place_id": "osm:r65606", "title": "Greater London", "namespace": "osm",
        "types": [{"identifier": "administrative"}], "repr_point": [-0.1, 51.5],
        "ccodes": ["GB"],
    }],
    "total": 1, "edges": [], "clustering_params": None, "toponym_stoplist": [],
}


def _fake_gateway(url, json=None, **kwargs):
    """A gateway that answers: /api/places knows only PLACE, /api/search
    returns SEARCH_OK."""
    if url.endswith("/api/places"):
        ids = (json or {}).get("ids") or []
        return _Resp(200, {"places": [PLACE] if PLACE["place_id"] in ids else []})
    if url.endswith("/api/search"):
        return _Resp(200, SEARCH_OK)
    return _Resp(404, {})


@override_settings(CRC_GATEWAY_URL=GATEWAY, CRC_GATEWAY_TIMEOUT=1)
class AtlasTestBase(TestCase):
    @classmethod
    def setUpTestData(cls):
        User = get_user_model()
        cls.normal = User.objects.create_user(
            username="atlas_normal", email="normal@example.org", password="pw",
            given_name="Norma", surname="User", role="normal")
        cls.beta = User.objects.create_user(
            username="atlas_beta", email="beta@example.org", password="pw",
            given_name="Bea", surname="Tester", role="beta_tester")

    def setUp(self):
        # Establish, don't inherit: confirm the fixture users mean what we think.
        self.assertFalse(self.normal.can_access_beta)
        self.assertTrue(self.beta.can_access_beta)

    # ── request helpers ──
    def place(self, pid="gn:2643743"):
        return self.client.get("/atlas/place/", {"id": pid})

    def boundaries(self, q="London"):
        return self.client.get("/atlas/boundaries/", {"q": q})

    def search(self, qstr="London"):
        return self.client.post("/atlas/search/", data=json.dumps({"qstr": qstr}),
                                content_type="application/json")


class BetaGatingTests(AtlasTestBase):
    """Each gated endpoint: anonymous and normal users are refused with 403;
    a beta tester is let through (the presence half)."""

    ENDPOINTS = ("place", "boundaries", "search")

    def test_anonymous_is_refused(self):
        with patch(POST, side_effect=_fake_gateway) as post:
            for name in self.ENDPOINTS:
                with self.subTest(endpoint=name):
                    resp = getattr(self, name)()
                    self.assertEqual(resp.status_code, 403)
                    self.assertEqual(resp.json().get("error"), "beta access required")
        post.assert_not_called()

    def test_normal_user_is_refused(self):
        self.client.force_login(self.normal)
        with patch(POST, side_effect=_fake_gateway) as post:
            for name in self.ENDPOINTS:
                with self.subTest(endpoint=name):
                    resp = getattr(self, name)()
                    self.assertEqual(resp.status_code, 403)
                    self.assertEqual(resp.json().get("error"), "beta access required")
        post.assert_not_called()

    def test_beta_tester_is_let_through(self):
        self.client.force_login(self.beta)
        with patch(POST, side_effect=_fake_gateway) as post:
            resp = self.place()
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.json()["place_id"], "gn:2643743")

            resp = self.boundaries()
            self.assertEqual(resp.status_code, 200)
            self.assertEqual([r["place_id"] for r in resp.json()["results"]], ["osm:r65606"])

            resp = self.search()
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(len(resp.json()["hits"]), 1)
            self.assertIs(resp.json()["gateway"], True)
        self.assertTrue(post.called)

    def test_status_is_honest_for_anonymous(self):
        """/atlas/status/ is auth-gated only: anonymous gets gateway:false
        without the gateway being probed; a signed-in user gets a real probe."""
        with patch("api.crc_client.requests.get", return_value=_Resp(200)) as get:
            resp = self.client.get("/atlas/status/")
            self.assertEqual(resp.json(), {"gateway": False})
            get.assert_not_called()
            self.client.force_login(self.normal)
            resp = self.client.get("/atlas/status/")
            self.assertEqual(resp.json(), {"gateway": True})
            get.assert_called_once()


class PortalFailureVsMissTests(AtlasTestBase):
    """/atlas/place/: 404 only when the gateway ANSWERED and had no such place."""

    def setUp(self):
        super().setUp()
        self.client.force_login(self.beta)

    def test_clean_miss_is_404(self):
        with patch(POST, side_effect=_fake_gateway) as post:
            resp = self.place("gn:99999999999")
        post.assert_called_once()
        self.assertEqual(resp.status_code, 404)
        self.assertEqual(resp.json()["error"], "not found")

    def test_gateway_404_for_the_id_is_a_miss(self):
        with patch(POST, return_value=_Resp(404, {"detail": "no such id"})):
            resp = self.place("zz:nonsense")
        self.assertEqual(resp.status_code, 404)

    def test_timeout_is_504_not_404(self):
        with patch(POST, side_effect=requests.Timeout("slow")) as post:
            resp = self.place()
        post.assert_called_once()
        self.assertEqual(resp.status_code, 504)
        body = resp.json()
        self.assertIs(body["gateway"], False)
        self.assertEqual(body["failure"], "timeout")
        self.assertIs(body["timeout"], True)
        self.assertEqual(resp["Retry-After"], "30")

    def test_connection_error_is_503(self):
        with patch(POST, side_effect=requests.ConnectionError("refused")):
            resp = self.place()
        self.assertEqual(resp.status_code, 503)
        self.assertEqual(resp.json()["failure"], "connection")
        self.assertIs(resp.json()["timeout"], False)

    def test_gateway_5xx_is_503(self):
        with patch(POST, return_value=_Resp(502, {"detail": "bad gateway"})):
            resp = self.place()
        self.assertEqual(resp.status_code, 503)
        self.assertEqual(resp.json()["failure"], "http")

    @override_settings(CRC_GATEWAY_URL="")
    def test_unconfigured_gateway_is_503_for_a_beta_user(self):
        with patch(POST, side_effect=_fake_gateway) as post:
            resp = self.place()
        post.assert_not_called()
        self.assertEqual(resp.status_code, 503)
        self.assertEqual(resp.json()["failure"], "disabled")


class BoundariesFailureTests(AtlasTestBase):
    """/atlas/boundaries/: an outage is not "No matching areas found"."""

    def setUp(self):
        super().setUp()
        self.client.force_login(self.beta)

    def test_no_matches_is_200_empty(self):
        with patch(POST, return_value=_Resp(200, {"hits": [], "total": 0})) as post:
            resp = self.boundaries("Nowhereshire")
        post.assert_called_once()
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["results"], [])
        self.assertIs(resp.json()["gateway"], True)

    def test_timeout_is_504(self):
        with patch(POST, side_effect=requests.Timeout("slow")) as post:
            resp = self.boundaries()
        self.assertEqual(post.call_count, 2, "a read timeout is retried once")
        self.assertEqual(resp.status_code, 504)
        self.assertEqual(resp.json()["results"], [])

    def test_connection_error_is_503(self):
        with patch(POST, side_effect=requests.ConnectionError("refused")):
            resp = self.boundaries()
        self.assertEqual(resp.status_code, 503)
        self.assertEqual(resp.json()["failure"], "unreachable")


class SearchFailureTests(AtlasTestBase):
    """/atlas/search/: a slow search keeps its 200 "took too long" shape (the
    service is up); an outage is a 503, not an empty 200 read as no results."""

    def setUp(self):
        super().setUp()
        self.client.force_login(self.beta)

    def test_timeout_stays_200_with_timeout_flag(self):
        with patch(POST, side_effect=requests.Timeout("slow")):
            resp = self.search()
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertIs(body["timeout"], True)
        self.assertIs(body["gateway"], True, "slow is not offline — no banner")
        self.assertEqual(body["hits"], [])

    def test_unreachable_is_503(self):
        with patch(POST, side_effect=requests.ConnectionError("refused")):
            resp = self.search()
        self.assertEqual(resp.status_code, 503)
        self.assertIs(resp.json()["gateway"], False)
        self.assertEqual(resp.json()["failure"], "unreachable")

    def test_passes_gateway_meta_through(self):
        """#294: whatever the gateway adds (e.g. namespaces_excluded) reaches
        the client untouched."""
        payload = dict(SEARCH_OK, namespaces_excluded=["gb"])
        with patch(POST, return_value=_Resp(200, payload)):
            resp = self.search()
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["namespaces_excluded"], ["gb"])
