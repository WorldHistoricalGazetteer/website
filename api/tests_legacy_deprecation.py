"""The legacy v3 JSON endpoints are deprecated (SG 2026-11-17; retired when WHG
moves to v4). One helper, ``api.deprecation.deprecated_endpoint``, wraps each route
in api/urls.py. Per endpoint this module pins: the notice is the FIRST root key of
a JSON-object body on success AND on a JSON error; the ``Deprecation`` and ``Link``
headers are present (no ``Sunset``); and the body is otherwise byte-for-byte what
the view produced (the notice is spliced in, nothing else moves).

Run locally only:

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test api.tests_legacy_deprecation --settings=whg.settings_localtest
"""

import json
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.gis.geos import Point
from django.test import RequestFactory, SimpleTestCase, TestCase

from api import deprecation
from api.deprecation import _insert_member, deprecated_endpoint, notice


class HelperTests(SimpleTestCase):

    def test_insert_member_is_first_and_leaves_the_rest_byte_identical(self):
        member = {"deprecated": True}
        for body in (b'{"a": 1, "b": [1, 2]}', b'{\n  "a": 1\n}', b'{}', b'  {"x": "\xc3\xa9"}', b'{ }'):
            out = _insert_member(body, member)
            parsed = json.loads(out)
            self.assertEqual(next(iter(parsed)), "deprecation")
            self.assertEqual(parsed.pop("deprecation"), member)
            self.assertEqual(parsed, json.loads(body))
            self.assertTrue(out.endswith(body.lstrip()[1:]))
        self.assertIsNone(_insert_member(b'[{"a": 1}]', member))
        self.assertIsNone(_insert_member(b"<h3>html</h3>", member))

    def test_every_notice_has_the_same_keys(self):
        keys = {"deprecated", "message", "replacement", "docs", "token_docs", "token_required", "sunset"}
        for key in deprecation._ENDPOINTS:
            n = notice(key)
            self.assertEqual(set(n), keys, key)
            self.assertIs(n["token_required"], True)
            self.assertIsNone(n["sunset"])
            self.assertTrue(n["message"].startswith("This endpoint is deprecated and will be retired when WHG "
                                                    "moves to v4."), key)
            self.assertIn("API token", n["message"])
            self.assertIn("#api-tokens", n["message"])

    def test_spatial_message_unchanged_from_its_first_version(self):
        self.assertEqual(notice("spatial")["message"], (
            "This endpoint is deprecated and will be retired when WHG moves to v4. Switch to the Reconciliation "
            "API (POST /reconcile) spatial filters (lat/lng/radius or bounds), which require an API token: "
            "generate one on your profile page (https://whgazetteer.org/profile/); see "
            "https://docs.whgazetteer.org/content/technical/apis.html#api-tokens."))

    def test_unconfirmed_replacements_say_so(self):
        self.assertIn("to be confirmed", notice("place_src")["replacement"])
        self.assertIn("to be confirmed", notice("geojson")["replacement"])
        for key in ("spatial", "db", "index", "place"):
            self.assertNotIn("to be confirmed", notice(key)["replacement"], key)

    def test_origin_classification_is_logged(self):
        from django.http import JsonResponse
        view = deprecated_endpoint("db")(lambda request: JsonResponse({"ok": 1}))
        rf = RequestFactory()
        cases = (
            (rf.get("/api/db/", HTTP_REFERER="http://testserver/datasets/1/places"), "in-app"),
            (rf.get("/api/db/", HTTP_REFERER="https://elsewhere.example/page"), "external"),
            (rf.get("/api/db/"), "external"),
        )
        session = rf.get("/api/db/")
        session.COOKIES["sessionid"] = "abc"
        cases += ((session, "in-app"),)
        for req, want in cases:
            with self.assertLogs("api.deprecation", level="INFO") as logs:
                view(req)
            self.assertIn(f"origin={want}", logs.output[-1])
            self.assertIn("endpoint=db", logs.output[-1])


class LegacyEndpointTests(TestCase):

    @classmethod
    def setUpTestData(cls):
        from datasets.models import Dataset
        from places.models import Place, PlaceGeom, PlaceName
        User = get_user_model()
        cls.owner = User.objects.create_user(username="dep_owner", email="dep_owner@example.org", password="pw",
                                             given_name="Dep", surname="Owner")
        with patch("datasets.signals.doi"):
            cls.ds = Dataset.objects.create(
                owner=cls.owner, label="dep_ds", title="Deprecation ds", description="D",
                creator="A Contributor", public=True, ds_status="indexed",
                uri_base="https://example.org/places/")
        cls.place = Place.objects.create(title="Depville", src_id="dep1", dataset=cls.ds, ccodes=["GB"],
                                         minmax=[1500, 1600], timespans=[[1500, 1600]], fclasses=["P"])
        PlaceName.objects.create(place=cls.place, toponym="Depville", jsonb={"toponym": "Depville"})
        PlaceGeom.objects.create(place=cls.place, geom=Point(-1.0, 51.0, srid=4326),
                                 jsonb={"type": "Point", "coordinates": [-1.0, 51.0]})

    def assertNotice(self, r, key, status):
        self.assertEqual(r.status_code, status, r.content[:300])
        self.assertEqual(r["Deprecation"], "@1794873600")
        self.assertIn('rel="successor-version"', r["Link"])
        self.assertNotIn("Sunset", r)
        body = r.json()
        self.assertEqual(next(iter(body)), "deprecation")
        self.assertEqual(body["deprecation"], notice(key))
        return body

    def test_db(self):
        body = self.assertNotice(self.client.get("/api/db/", {"name": "Depville"}), "db", 200)
        self.assertEqual([f["properties"]["title"] for f in body["features"]], ["Depville"])
        self.assertEqual(body["type"], "FeatureCollection")

    def test_db_html_hint_gets_headers_only(self):
        r = self.client.get("/api/db/")
        self.assertEqual(r.status_code, 200)  # the view's own (odd) status for this HTML hint, unchanged
        self.assertEqual(r["Deprecation"], "@1794873600")
        self.assertTrue(r.content.startswith(b"<h3>"))  # unchanged, no member spliced into HTML

    def test_index(self):
        with patch("api.views.collector", return_value={"items": []}):
            body = self.assertNotice(self.client.get("/api/index/", {"name": "Depville"}), "index", 200)
        self.assertEqual(body["features"], [])
        body = self.assertNotice(self.client.get("/api/index/"), "index", 400)
        self.assertIn("error", body)

    def test_place_single_and_list(self):
        body = self.assertNotice(self.client.get(f"/api/place/{self.place.id}/"), "place", 200)
        self.assertEqual(body["title"], "Depville")
        body = self.assertNotice(self.client.get(f"/api/place/{self.place.id}-{self.place.id}/"), "place", 200)
        self.assertIn("names", body)
        body = self.assertNotice(self.client.get("/api/place/999999999/"), "place", 404)
        self.assertEqual(body["error"], "No places found")
        body = self.assertNotice(self.client.get("/api/place/abc/def/ghi/"), "place", 400)  # the sinkhole
        self.assertEqual(body["error"], "Malformed URL")

    def test_place_by_src_id(self):
        body = self.assertNotice(self.client.get("/api/place/dep_ds/dep1/"), "place_src", 200)
        self.assertEqual(body["title"], "Depville")
        body = self.assertNotice(self.client.get("/api/place/dep_ds/nosuch/"), "place_src", 404)
        self.assertIn("detail", body)

    def test_geojson(self):
        r = self.client.get("/api/geojson/", {"id": self.ds.id})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r["Deprecation"], "@1794873600")
        body = self.assertNotice(r, "geojson", 200)
        # DRF's paginated envelope {count, next, previous, results}: unchanged after the notice.
        self.assertEqual(list(body)[1:], ["count", "next", "previous", "results"])
        self.assertEqual(body["count"], 1)
        body = self.assertNotice(self.client.get("/api/geojson/", {"id": 999999999}), "geojson", 404)

    def test_spatial(self):
        body = self.assertNotice(self.client.get("/api/spatial/", {"type": "bbox", "sw": "-2,50", "ne": "0,52"}),
                                 "spatial", 200)
        self.assertEqual(body["features"][0]["properties"]["title"], "Depville")

    def test_place_compare_is_not_deprecated(self):
        # Internal: datasets/views.py reads it in-process for dataset updates.
        r = self.client.get(f"/api/place_compare/{self.place.id}/")
        self.assertEqual(r.status_code, 200)
        self.assertNotIn("Deprecation", r)
        self.assertNotIn("deprecation", r.json())
