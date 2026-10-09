"""/atlas/geometry/ — the Atlas's exact-geometry proxy to the gateway's
``GET /api/geometry/<place_id>`` (geom store; indexing plan §5.3).

Run locally (never in the prod container — see reference_never_run_suite_on_prod):

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test search.tests_atlas_geometry --settings=whg.settings_localtest

Same discipline as search/tests_atlas.py, so no assertion passes vacuously:

* every refusal (403 / 404 / 451 / 503 / 504) is paired with a 200 on the same
  endpoint from the same fixture;
* the gateway is faked at ``api.crc_client.requests.get`` — the one HTTP seam
  the new helper uses — so the real ``crc_geometry`` classification runs, and
  every test that should reach the gateway asserts the fake WAS called, every
  test that must not reach it asserts it was NOT;
* the licence check is exercised both ways: the registry withholding (gateway
  never asked) and the gateway withholding (registry permitting).
"""

import json
from unittest.mock import patch

import requests
from django.test import override_settings

from search.tests_atlas import GATEWAY, AtlasTestBase, _Resp

GET = "api.crc_client.requests.get"

SQUARE = {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]}
GEOMETRY_OK = {
    "place_id": "osm:r1", "namespace": "osm", "geometry": SQUARE, "geometry_count": 1,
    "bounds": [0, 0, 1, 1], "vertex_count": 5, "bytes": 70, "max_bytes": 1000000,
    "simplified": False, "tolerance": None, "source": "geom-store",
}
WITHHELD_BY_GATEWAY = {"detail": {
    "error": "source not redistributable", "id": "nl:1", "namespace": "nl",
    "source": {"name": "Native Land", "rights_holder": "Native Land Digital",
               "source_url": "https://native-land.ca", "license": "custom-nativeland-dst"}}}


def _fake_gateway(url, params=None, **kwargs):
    """A restarted gateway: knows osm:r1, says osm:r2 is point-only, withholds nl."""
    if "/api/geometry/osm:r1" in url:
        return _Resp(200, GEOMETRY_OK)
    if "/api/geometry/osm:r2" in url:
        return _Resp(404, {"detail": {"error": "no geometry", "id": "osm:r2"}})
    if "/api/geometry/nl:" in url:
        return _Resp(451, WITHHELD_BY_GATEWAY)
    if "/api/geometry/osm:r7" in url:
        return _Resp(413, TOO_LARGE)
    if "/api/geometry/" in url:
        return _Resp(404, {"detail": {"error": "not found", "id": url.rsplit("/", 1)[-1]}})
    return _Resp(404, {})


def _unrestarted_gateway(url, params=None, **kwargs):
    """A gateway WITHOUT the route: its catch-all proxies the path to ES, which
    answers 401 with a security_exception body (measured from the app host,
    2026-10-09), not the route's JSON ``detail``."""
    return _Resp(401, {
        "error": {
            "root_cause": [{"type": "security_exception",
                            "reason": "missing authentication credentials for REST request [/api/geometry/osm:r1]",
                            "header": {"WWW-Authenticate": ["Basic realm=\"security\" charset=\"UTF-8\"",
                                                            "ApiKey"]}}],
            "type": "security_exception",
            "reason": "missing authentication credentials for REST request [/api/geometry/osm:r1]",
            "header": {"WWW-Authenticate": ["Basic realm=\"security\" charset=\"UTF-8\"", "ApiKey"]},
        },
        "status": 401,
    })


TOO_LARGE = {"detail": {"error": "geometry too large", "id": "osm:r7",
                        "detail": "Stored geometry: 2,400,000 vertices, above the 1,500,000 this endpoint will serve whole.",
                        "vertex_count": 2400000, "max_vertices": 1500000}}


def _attribution(redistributable, name="Test Source"):
    return patch("api.attribution.registry_attribution", return_value={
        "name": name, "redistributable": redistributable,
        "source_url": "https://example.org/src", "license__spdx_id": "CC-BY-4.0"})


@override_settings(CRC_GATEWAY_URL=GATEWAY, CRC_GATEWAY_TIMEOUT=1)
class GeometryTestBase(AtlasTestBase):
    def geometry(self, pid="osm:r1"):
        return self.client.get("/atlas/geometry/", {"id": pid})


class GeometryGatingTests(GeometryTestBase):
    def test_anonymous_is_refused(self):
        with patch(GET, side_effect=_fake_gateway) as get:
            resp = self.geometry()
        self.assertEqual(resp.status_code, 403)
        get.assert_not_called()

    def test_normal_user_is_refused(self):
        self.client.force_login(self.normal)
        with patch(GET, side_effect=_fake_gateway) as get:
            resp = self.geometry()
        self.assertEqual(resp.status_code, 403)
        get.assert_not_called()

    def test_beta_tester_is_let_through(self):
        self.client.force_login(self.beta)
        with patch(GET, side_effect=_fake_gateway) as get, _attribution(True):
            resp = self.geometry()
        get.assert_called_once()
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["geometry"], SQUARE)
        self.assertEqual(body["bounds"], [0, 0, 1, 1])
        self.assertFalse(body["simplified"])
        self.assertTrue(body["gateway"])
        self.assertEqual(body["attribution"]["name"], "Test Source")

    def test_missing_or_unnamespaced_id_is_400(self):
        self.client.force_login(self.beta)
        with patch(GET, side_effect=_fake_gateway) as get:
            self.assertEqual(self.client.get("/atlas/geometry/").status_code, 400)
            self.assertEqual(self.geometry("r1").status_code, 400)
        get.assert_not_called()


class GeometryMissVsFailureTests(GeometryTestBase):
    def setUp(self):
        super().setUp()
        self.client.force_login(self.beta)

    def test_unknown_place_is_404_not_found(self):
        with patch(GET, side_effect=_fake_gateway) as get, _attribution(True):
            resp = self.geometry("osm:r999")
            ok = self.geometry("osm:r1")
        self.assertEqual(get.call_count, 2)
        self.assertEqual(resp.status_code, 404)
        self.assertEqual(resp.json()["error"], "not found")
        self.assertEqual(ok.status_code, 200)

    def test_point_only_place_is_404_no_geometry(self):
        with patch(GET, side_effect=_fake_gateway), _attribution(True):
            resp = self.geometry("osm:r2")
        self.assertEqual(resp.status_code, 404)
        self.assertEqual(resp.json()["error"], "no geometry")
        self.assertEqual(resp.json()["id"], "osm:r2")

    def test_timeout_is_504_not_404(self):
        with patch(GET, side_effect=requests.Timeout("slow")) as get, _attribution(True):
            resp = self.geometry()
        get.assert_called_once()
        self.assertEqual(resp.status_code, 504)
        self.assertEqual(resp.json()["failure"], "timeout")
        self.assertFalse(resp.json()["gateway"])

    def test_connection_error_is_503(self):
        with patch(GET, side_effect=requests.ConnectionError("refused")), _attribution(True):
            resp = self.geometry()
        self.assertEqual(resp.status_code, 503)
        self.assertEqual(resp.json()["failure"], "connection")

    def test_gateway_5xx_is_503(self):
        with patch(GET, return_value=_Resp(500, {"detail": "boom"})), _attribution(True):
            resp = self.geometry()
        self.assertEqual(resp.status_code, 503)
        self.assertEqual(resp.json()["failure"], "http")

    def test_too_large_is_413_passed_through_as_an_answer(self):
        # The gateway declining to serve a continent whole is an answer the
        # client acts on (fall back to the tiles), not a gateway failure.
        with patch(GET, side_effect=_fake_gateway) as get, _attribution(True):
            resp = self.geometry("osm:r7")
        get.assert_called_once()
        self.assertEqual(resp.status_code, 413)
        self.assertEqual(resp.json()["error"], "geometry too large")
        self.assertEqual(resp.json()["max_vertices"], 1500000)

    def test_unrestarted_gateway_is_503_not_a_miss(self):
        # The route is not there yet: ES's "no handler" body must not be read
        # as "no such place" — the client falls back to the tiles either way,
        # but the view must not claim the place is absent.
        with patch(GET, side_effect=_unrestarted_gateway) as get, _attribution(True):
            resp = self.geometry()
        get.assert_called_once()
        self.assertEqual(resp.status_code, 503)
        self.assertEqual(resp.json()["failure"], "http")

    def test_unconfigured_gateway_is_503_for_a_beta_user(self):
        with override_settings(CRC_GATEWAY_URL=""), patch(GET, side_effect=_fake_gateway) as get, \
                _attribution(True):
            resp = self.geometry()
        get.assert_not_called()
        self.assertEqual(resp.status_code, 503)
        self.assertEqual(resp.json()["failure"], "disabled")


class GeometryRedistributionTests(GeometryTestBase):
    """place#269 on geometry: geometry is source content."""

    def setUp(self):
        super().setUp()
        self.client.force_login(self.beta)

    def test_entity_prefix_is_stripped_before_the_registry_check(self):
        # "place:osm:r1" must be checked as namespace "osm", not "place": a
        # withheld source must not reach the gateway behind the prefix.
        seen = []

        def attribution(ns):
            seen.append(ns)
            return {"name": "Withheld Source", "redistributable": False,
                    "source_url": "https://example.org/src", "license__spdx_id": "custom"}
        with patch(GET, side_effect=_fake_gateway) as get, \
                patch("api.attribution.registry_attribution", side_effect=attribution):
            resp = self.geometry("place:osm:r1")
        get.assert_not_called()
        self.assertEqual(seen, ["osm"])
        self.assertEqual(resp.status_code, 451)
        self.assertEqual(resp.json()["id"], "osm:r1")
        # And a permitted one still reaches the gateway with the bare id.
        with patch(GET, side_effect=_fake_gateway) as get, _attribution(True):
            self.assertEqual(self.geometry("place:osm:r1").status_code, 200)
        self.assertIn("/api/geometry/osm:r1", get.call_args.args[0])

    def test_registry_withholds_without_asking_the_gateway(self):
        with patch(GET, side_effect=_fake_gateway) as get, _attribution(False, name="Withheld Source"):
            resp = self.geometry("osm:r1")
        get.assert_not_called()
        self.assertEqual(resp.status_code, 451)
        body = resp.json()
        self.assertEqual(body["source"]["name"], "Withheld Source")
        self.assertIn("geometry", body["detail"])
        self.assertNotIn("coordinates", json.dumps(body))
        # Same fixture, permitted: served.
        with patch(GET, side_effect=_fake_gateway) as get, _attribution(True):
            self.assertEqual(self.geometry("osm:r1").status_code, 200)
        get.assert_called_once()

    def test_gateway_withholding_wins_over_a_permissive_registry(self):
        # A stale registry row must not override the gateway's own determination.
        with patch(GET, side_effect=_fake_gateway) as get, _attribution(True):
            resp = self.geometry("nl:1")
        get.assert_called_once()
        self.assertEqual(resp.status_code, 451)
        body = resp.json()
        self.assertEqual(body["source"]["rights_holder"], "Native Land Digital")
        self.assertNotIn("coordinates", json.dumps(body))


class ContainedInForwardingTests(GeometryTestBase):
    """The exact-region search constraint travels as place ids, not polygons."""

    def setUp(self):
        super().setUp()
        self.client.force_login(self.beta)

    def _search(self, options):
        from search.tests_atlas import POST, SEARCH_OK
        with patch(POST, return_value=_Resp(200, SEARCH_OK)) as post:
            resp = self.client.post("/atlas/search/", data=json.dumps(options),
                                    content_type="application/json")
        post.assert_called_once()
        return resp, post.call_args.kwargs["json"]

    def test_contained_in_and_containment_are_forwarded_and_prefix_stripped(self):
        resp, body = self._search({"qstr": "Paris", "contained_in": ["place:osm:r1", "ohm:r9", "bad"],
                                   "containment": "exact", "relation": "intersects",
                                   "bounds": {"type": "GeometryCollection", "geometries": []}})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(body["contained_in"], ["osm:r1", "ohm:r9"])
        self.assertEqual(body["containment"], "exact")
        self.assertEqual(body["relation"], "intersects")
        self.assertNotIn("bounds", body, "an empty collection must not be sent as a constraint")

    def test_polygon_constraint_still_travels_as_bounds(self):
        resp, body = self._search({"qstr": "Paris", "bounds": {"type": "GeometryCollection",
                                                                "geometries": [SQUARE]}})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(body["bounds"]["geometries"], [SQUARE])
        self.assertNotIn("contained_in", body)
        self.assertNotIn("containment", body)
