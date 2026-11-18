"""The v4 cutover layer for the legacy v3 JSON endpoints (``api.legacy_compat``):
``LEGACY_API_MODE`` = ``deprecated`` (default, unchanged) | ``compat`` | ``gone``.

Pins, per mode: what answers, the status, the headers, whether the deprecation
member is there, that the old parameters are mapped to the successor request
exactly, that a token is never echoed, and that WHG's own pages keep the legacy
views while ``LEGACY_API_INAPP_PASSTHROUGH`` is on. The gateway is never called:
``process_queries`` is patched where the compat layer imports it.

Run locally only:

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test api.tests_legacy_compat --settings=whg.settings_localtest
"""

import json
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.gis.geos import Point
from django.test import RequestFactory, SimpleTestCase, TestCase, override_settings

from api import legacy_compat
from api.deprecation import DEPRECATION_HEADER, notice
from api.legacy_compat import LegacyParamError, check_legacy_api_settings, endpoint_mode, plan_for
from api.spatial_legacy import to_reconcile_query

RF = RequestFactory()


def plan(key, path="/x/", kwargs=None, **params):
    return plan_for(key, RF.get(path, params), kwargs or {})


class ModeSettingTests(SimpleTestCase):

    def test_default_is_deprecated(self):
        from django.conf import settings
        self.assertEqual(settings.LEGACY_API_MODE, "deprecated")
        self.assertTrue(settings.LEGACY_API_INAPP_PASSTHROUGH)
        for key in ("spatial", "db", "index", "place", "place_src", "geojson"):
            self.assertEqual(endpoint_mode(key), "deprecated", key)
        self.assertEqual(check_legacy_api_settings(), [])

    @override_settings(LEGACY_API_MODE="compat", LEGACY_API_MODE_OVERRIDES={"geojson": "gone"})
    def test_override_per_endpoint(self):
        self.assertEqual(endpoint_mode("spatial"), "compat")
        self.assertEqual(endpoint_mode("geojson"), "gone")
        self.assertEqual(check_legacy_api_settings(), [])

    @override_settings(LEGACY_API_MODE="gnoe", LEGACY_API_MODE_OVERRIDES={"spatail": "compat", "db": "off"})
    def test_a_typo_never_retires_an_api_and_is_reported(self):
        with self.assertLogs("api.deprecation", level="ERROR"):
            self.assertEqual(endpoint_mode("spatial"), "deprecated")
        ids = sorted(e.id for e in check_legacy_api_settings())
        self.assertEqual(ids, ["api.E101", "api.E101", "api.E103"])


class MappingTests(SimpleTestCase):
    """Old parameters -> the successor request. Pure apart from a dataset label lookup."""

    def test_spatial_is_the_documented_translation(self):
        p = plan("spatial", type="bbox", sw="-2,50", ne="0,52", fc="P", pagesize="7", token="SECRET")
        self.assertEqual(p["kind"], "reconcile")
        self.assertEqual(p["query"], to_reconcile_query({"type": "bbox", "sw": "-2,50", "ne": "0,52", "fc": "P",
                                                          "pagesize": "7"})["body"]["queries"]["q0"])
        self.assertNotIn("token", p["params"])  # never echoed

    def test_spatial_dataset_is_passed_to_reconcile_as_is(self):
        # /reconcile filters on `dataset` itself, so the translation no longer narrows to `whg`.
        p = plan("spatial", type="nearby", lon="0", lat="0", km="5", dataset="42")
        self.assertEqual((p["query"]["dataset"], p["dataset_pk"]), (42, 42))
        self.assertNotIn("namespaces", p["query"])

    def test_spatial_refusal_is_a_param_error(self):
        with self.assertRaises(LegacyParamError):
            plan("spatial", type="circle")

    def test_db_name_search(self):
        p = plan("db", name="Depville", fc="p,a", ccode="gb,FR", year="1550", pagesize="5", context="x")
        self.assertEqual(p["kind"], "reconcile")
        self.assertEqual(p["query"], {"query": "Depville", "fclasses": ["A", "P"], "countries": ["FR", "GB"],
                                      "start": 1550, "end": 1550, "limit": 5})
        self.assertIn("context", p["dropped"])

    def test_db_name_contains_is_flagged(self):
        p = plan("db", name_contains="ville")
        self.assertEqual(p["query"]["query"], "ville")
        self.assertTrue(any("substring" in n for n in p["notes"]))

    def test_db_id_becomes_entity_places(self):
        p = plan("db", id="3,4", name="ignored")
        self.assertEqual((p["kind"], p["ids"]), ("entity_places", [3, 4]))
        self.assertIn("name", p["dropped"])

    def test_db_refusals(self):
        for bad in ({}, {"name": "x", "ccode": "GBR"}, {"name": "x", "pagesize": "500"},
                    {"name": "x", "year": "late"}, {"id": "1,x"}):
            with self.assertRaises(LegacyParamError, msg=bad):
                plan("db", **bad)

    def test_index(self):
        p = plan("index", name="Rome", fclass="P", pagesize="3000")
        self.assertEqual(p["query"], {"query": "Rome", "fclasses": ["P"], "limit": 1000})
        self.assertEqual(plan("index", pid="12")["ids"], [12])
        self.assertEqual(plan("index", whgid="abc")["kind"], "none")
        with self.assertRaises(LegacyParamError):
            plan("index")

    def test_place_routes(self):
        self.assertEqual(plan("place", kwargs={"pk": "7"})["ids"], [7])
        self.assertTrue(plan("place", kwargs={"pk": "7"})["single"])
        p = plan("place", kwargs={"pk_list": "7-8-9"})
        self.assertEqual((p["ids"], p["single"]), ([7, 8, 9], False))
        self.assertEqual(plan("place")["kind"], "legacy")  # the malformed-URL trap
        with self.assertRaises(LegacyParamError):
            plan("place", kwargs={"pk_list": "-".join(["1"] * 51)})
        p = plan("place_src", kwargs={"dslabel": "ds", "src_id": "a1"})
        self.assertEqual((p["kind"], p["dslabel"], p["src_id"]), ("entity_src", "ds", "a1"))

    def test_geojson(self):
        self.assertEqual(plan("geojson", id="5")["path"], "/entity/dataset:5/api")
        self.assertEqual(plan("geojson", coll="5")["kind"], "none")
        for bad in ({}, {"id": "x"}):
            with self.assertRaises(LegacyParamError):
                plan("geojson", **bad)


class _Fixture(TestCase):

    @classmethod
    def setUpTestData(cls):
        from api.models import APIToken
        from datasets.models import Dataset
        from places.models import Place, PlaceGeom, PlaceName
        User = get_user_model()
        cls.owner = User.objects.create_user(username="lc_owner", email="lc_owner@example.org", password="pw",
                                             given_name="LC", surname="Owner")
        cls.caller = User.objects.create_user(username="lc_caller", email="lc_caller@example.org", password="pw",
                                              given_name="LC", surname="Caller")
        APIToken.objects.create(user=cls.caller, key="lc-token")
        with patch("datasets.signals.doi"):
            cls.ds = Dataset.objects.create(
                owner=cls.owner, label="lc_ds", title="Compat ds", description="D", creator="A Contributor",
                public=True, ds_status="indexed", uri_base="https://example.org/places/")
            cls.private = Dataset.objects.create(
                owner=cls.owner, label="lc_private", title="Private ds", description="D", creator="A Contributor",
                public=False, ds_status="uploaded", uri_base="https://example.org/places/")
        cls.place = Place.objects.create(title="Compatville", src_id="lc1", dataset=cls.ds, ccodes=["GB"],
                                         minmax=[1500, 1600], timespans=[[1500, 1600]], fclasses=["P"])
        PlaceName.objects.create(place=cls.place, toponym="Compatville", jsonb={"toponym": "Compatville"})
        PlaceGeom.objects.create(place=cls.place, geom=Point(-1.0, 51.0, srid=4326),
                                 jsonb={"type": "Point", "coordinates": [-1.0, 51.0]})
        cls.hidden = Place.objects.create(title="Hiddenville", src_id="h1", dataset=cls.private, ccodes=["GB"],
                                          fclasses=["P"])

    def setUp(self):
        from django.core.cache import caches
        for alias in ("default", "throttle"):
            try:
                caches[alias].clear()
            except Exception:
                pass

    def assertHeaders(self, r):
        self.assertEqual(r["Deprecation"], DEPRECATION_HEADER)
        self.assertIn('rel="successor-version"', r["Link"])


@override_settings(LEGACY_API_MODE="gone")
class GoneModeTests(_Fixture):

    def test_every_endpoint_is_410_with_the_way_forward(self):
        urls = {
            "spatial": "/api/spatial/?type=bbox&sw=-2,50&ne=0,52",
            "db": "/api/db/?name=Compatville",
            "index": "/api/index/?name=Compatville",
            "place": f"/api/place/{self.place.id}/",
            "place_src": "/api/place/lc_ds/lc1/",
            "geojson": f"/api/geojson/?id={self.ds.id}",
        }
        for key, url in urls.items():
            r = self.client.get(url)
            self.assertEqual(r.status_code, 410, (key, r.content[:300]))
            self.assertHeaders(r)
            body = r.json()
            self.assertNotIn("deprecation", body, key)  # the body IS the retirement notice
            self.assertEqual((body["error"], body["retired"], body["token_required"]), ("gone", True, True))
            self.assertTrue(body["message"].startswith("This endpoint was retired when WHG moved to v4."), key)
            self.assertIn("#api-tokens", body["message"])
            self.assertEqual(body["replacement"], notice(key)["replacement"])
            self.assertEqual(body["docs"], notice(key)["docs"])
            self.assertIsNotNone(body["successor_request"], key)

    def test_spatial_410_carries_the_exact_reconcile_body(self):
        body = self.client.get("/api/spatial/", {"type": "nearby", "lon": "-1", "lat": "51", "km": "5",
                                                 "collection": "3", "token": "SECRET"}).json()
        sr = body["successor_request"]
        self.assertEqual((sr["method"], sr["url"]), ("POST", "http://testserver/reconcile"))
        self.assertEqual(sr["body"], {"queries": {"q0": {"lat": 51.0, "lng": -1.0, "radius": 5.0,
                                                         "relation": "within", "containment": "fuzzy",
                                                         "limit": 20}}})
        self.assertIn("collection", body["dropped"])
        self.assertNotIn("SECRET", json.dumps(body))

    def test_untranslatable_and_malformed_requests_still_get_the_notice(self):
        body = self.client.get("/api/spatial/", {"type": "circle"}).json()
        self.assertIsNone(body["successor_request"])
        self.assertIn("translation_error", body)
        body = self.client.get("/api/geojson/", {"coll": "1"}).json()
        self.assertIn("no_successor", body)

    def test_inapp_requests_keep_the_legacy_view(self):
        r = self.client.get("/api/db/", {"name": "Compatville"}, HTTP_REFERER="http://testserver/places/1")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(next(iter(r.json())), "deprecation")
        self.assertEqual(r.json()["features"][0]["properties"]["title"], "Compatville")
        self.client.cookies["sessionid"] = "x"
        self.assertEqual(self.client.get(f"/api/place/{self.place.id}/").status_code, 200)

    @override_settings(LEGACY_API_INAPP_PASSTHROUGH=False)
    def test_inapp_passthrough_can_be_switched_off(self):
        r = self.client.get("/api/db/", {"name": "Compatville"}, HTTP_REFERER="http://testserver/places/1")
        self.assertEqual(r.status_code, 410)

    def test_logged(self):
        with self.assertLogs("api.deprecation", level="INFO") as logs:
            self.client.get("/api/db/", {"name": "x"})
        self.assertIn("mode=gone served=gone", logs.output[-1])


def _candidates(*cands):
    return {"q0": {"result": list(cands)}, "attribution": {"sources": {}}}


GN = {"id": "place:gn:2643743", "name": "London", "score": 100, "match": True, "namespace": "gn",
      "alt_names": [], "ccodes": ["GB"], "repr_point": [-0.12, 51.5], "place_types": [], "confidence": 97}


@override_settings(LEGACY_API_MODE="compat")
class CompatModeTests(_Fixture):

    def get(self, url, params=None, token=True, **extra):
        params = dict(params or {})
        if token:
            params["token"] = "lc-token"
        return self.client.get(url, params, **extra)

    def assertCompat(self, r, key, status=200):
        self.assertEqual(r.status_code, status, r.content[:400])
        self.assertHeaders(r)
        body = r.json()
        self.assertEqual(next(iter(body)), "deprecation")
        self.assertEqual(body["deprecation"], notice(key))
        self.assertEqual(body["compat"]["served_by"], "compat")
        self.assertNotIn("lc-token", r.content.decode())
        return body

    def test_no_token_is_401_with_the_successor_request(self):
        with patch("api.reconcile.process_queries") as pq:
            r = self.get("/api/spatial/", {"type": "bbox", "sw": "-2,50", "ne": "0,52"}, token=False)
        pq.assert_not_called()
        self.assertEqual(r.status_code, 401)
        body = r.json()
        self.assertEqual(body["successor_request"]["method"], "POST")
        self.assertIn("?token=", body["message"])

    @override_settings(LEGACY_API_COMPAT_ANONYMOUS=True, LEGACY_API_COMPAT_ANON_RATE=2)
    def test_anonymous_can_be_admitted_at_a_per_ip_rate(self):
        with patch("api.reconcile.process_queries", return_value=_candidates(GN)), \
                patch("api.legacy_compat.consume_budget", side_effect=[(True, 0), (False, 17)]) as budget:
            ok = self.get("/api/spatial/", {"type": "bbox", "sw": "-2,50", "ne": "0,52"}, token=False)
            limited = self.get("/api/spatial/", {"type": "bbox", "sw": "-2,50", "ne": "0,52"}, token=False)
        self.assertEqual(ok.status_code, 200)
        self.assertEqual((limited.status_code, limited["Retry-After"]), (429, "17"))
        self.assertTrue(budget.call_args[0][0].startswith("legacy:anon:"))
        self.assertEqual(budget.call_args[0][1], 2)

    def test_spatial(self):
        with patch("api.reconcile.process_queries", return_value=_candidates(GN)) as pq:
            r = self.get("/api/spatial/", {"type": "bbox", "sw": "-2,50", "ne": "0,52", "match": "intersects"})
        body = self.assertCompat(r, "spatial")
        sent = pq.call_args[0][0]["q0"]
        self.assertEqual(sent, to_reconcile_query({"type": "bbox", "sw": "-2,50", "ne": "0,52",
                                                   "match": "intersects"})["body"]["queries"]["q0"])
        self.assertEqual(pq.call_args[1]["user"], self.caller)
        self.assertEqual((body["type"], body["count"], body["pagesize"], body["errors"]),
                         ("FeatureCollection", None, 1, None))
        self.assertEqual((body["geometry_mode"], body["match"]), ("point", "intersects"))
        f = body["features"][0]
        self.assertEqual(f["geometry"], {"type": "Point", "coordinates": [-0.12, 51.5]})
        self.assertEqual(f["properties"]["title"], "London")
        self.assertEqual(f["properties"]["place_id"], "gn:2643743")
        self.assertEqual(f["properties"]["confidence"], 97)
        self.assertEqual(f["properties"]["uri"], "http://testserver/entity/place:gn:2643743/api")
        self.assertEqual(body["parameters"], {"type": "bbox", "sw": "-2,50", "ne": "0,52", "match": "intersects"})
        self.assertEqual(body["attribution"], {"sources": {}})
        self.assertIn("count", body["compat"]["no_equivalent"])

    def _through_reconcile(self, url, params):
        """The compat view over the REAL process_queries / reconcile_place_es, with a fake gateway
        (honours `namespaces`) and a fake legacy index (ignores the dataset label)."""
        def gw(place_id):
            return {"_id": place_id, "_score": 3.0,
                    "_source": {"place_id": place_id, "title": "Compatville", "ccodes": ["GB"]}}

        def legacy(pk, label, src_id):
            return {"_id": pk, "_score": 3.0, "_source": {"place_id": pk, "dataset": label, "src_id": src_id,
                                                          "title": "Compatville", "ccodes": ["GB"]}}

        gw_hits = [gw("gn:2643743"), gw(f"whg:{self.ds.id}:lc9"), gw("whg:99999:z")]
        legacy_hits = [legacy(str(self.place.id), "lc_ds", "lc1"), legacy("8000001", "other_ds", "o1")]

        def fake_crc(query, user=None, namespaces=None, meta=None):
            return [h for h in gw_hits if not namespaces or h["_source"]["place_id"].split(":")[0] in namespaces]

        from api.tests_reconcile_dataset_filter import inline_reconcile
        pool, conn = inline_reconcile()  # the workers must see this TestCase's uncommitted rows
        with pool, conn, patch("api.reconcile.crc_reconcile_search", side_effect=fake_crc), \
                patch("api.reconcile.es_search", return_value=legacy_hits):
            return self.get(url, params)

    def test_dataset_is_filtered_by_reconcile_itself(self):
        # The compat layer used to narrow to `whg` and keep the candidates whose id starts
        # `place:whg:<dataset id>:`. /reconcile now filters on `dataset`, and the layer does not.
        # Equivalence: the old rule applied to the UNFILTERED candidates gives exactly what is now
        # served with `dataset` — and the unfiltered list is the presence control.
        circle = {"type": "nearby", "lon": "-1", "lat": "51", "km": "5"}
        lc = f"place:whg:{self.ds.id}:"
        cases = (
            ("/api/db/", {"name": "Compatville"}, {"dataset": "lc_ds"}, [lc + "lc1", lc + "lc9"]),
            ("/api/index/", {"name": "Compatville"}, {"dataset": "lc_ds"}, [lc + "lc1", lc + "lc9"]),
            # A circle suppresses the legacy arm (the gateway serves `whg`), so only lc9 here.
            ("/api/spatial/", circle, {"dataset": str(self.ds.id)}, [lc + "lc9"]),
        )
        for url, base, ds_param, expected in cases:
            with self.subTest(url=url):
                key = url.strip("/").split("/")[1]
                ids = [f["id"] for f in self.assertCompat(self._through_reconcile(url, base), key)["features"]]
                self.assertIn("place:gn:2643743", ids)
                self.assertIn("place:whg:99999:z", ids)
                old_rule = sorted(i for i in ids if i.startswith(lc))
                self.assertEqual(old_rule, expected)
                body = self.assertCompat(self._through_reconcile(url, {**base, **ds_param}), key)
                self.assertEqual(sorted(f["id"] for f in body["features"]), old_rule)

    def test_db_unknown_dataset_is_400(self):
        r = self.get("/api/db/", {"name": "x", "dataset": "nosuch"})
        self.assertEqual(r.status_code, 400)

    def test_db_private_dataset_answers_as_an_unknown_one(self):
        # place#310: the label of a dataset outside the caller's circle is not confirmed.
        nosuch = self.get("/api/db/", {"name": "x", "dataset": "nosuch"})
        private = self.get("/api/db/", {"name": "x", "dataset": "lc_private"})
        self.assertEqual((private.status_code, nosuch.status_code), (400, 400))
        self.assertEqual(private.json()["error"].replace("lc_private", "?"),
                         nosuch.json()["error"].replace("nosuch", "?"))
        # Presence control: the owner's own private label does resolve.
        from api.models import APIToken
        APIToken.objects.create(user=self.owner, key="lc-owner-token")
        with patch("api.reconcile.process_queries", return_value=_candidates()) as pq:
            r = self.client.get("/api/db/", {"name": "x", "dataset": "lc_private", "token": "lc-owner-token"})
        self.assertEqual(r.status_code, 200, r.content[:300])
        self.assertEqual(pq.call_args[0][0]["q0"]["dataset"], self.private.id)

    def test_a_failed_query_is_400_not_an_empty_200(self):
        with patch("api.reconcile.process_queries",
                   return_value={"q0": {"error": "Invalid bounds", "result": []}}):
            r = self.get("/api/spatial/", {"type": "bbox", "sw": "-2,50", "ne": "0,52"})
        body = self.assertCompat(r, "spatial", 400)
        self.assertEqual(body["error"], "Invalid bounds")

    def test_gateway_failure_is_reported(self):
        gw = {"answered": False, "error": "timeout", "message": "retry"}
        with patch("api.reconcile.process_queries",
                   return_value={"q0": {"result": [], "gateway": gw}, "attribution": None}):
            body = self.get("/api/index/", {"name": "Rome"}).json()
        self.assertEqual(body["gateway"], gw)

    def test_query_budget_is_charged(self):
        with patch("api.legacy_compat.consume_query_budget", return_value=(False, 9)), \
                patch("api.reconcile.process_queries") as pq:
            r = self.get("/api/spatial/", {"type": "bbox", "sw": "-2,50", "ne": "0,52"})
        pq.assert_not_called()
        self.assertEqual((r.status_code, r["Retry-After"]), (429, "9"))

    def test_bad_parameters_are_400(self):
        with patch("api.reconcile.process_queries") as pq:
            r = self.get("/api/spatial/", {"type": "circle"})
        pq.assert_not_called()
        self.assertEqual(r.status_code, 400)
        self.assertHeaders(r)
        body = r.json()
        self.assertEqual(list(body), ["deprecation", "error"])
        self.assertIn("'type' must be", body["error"])

    def test_place_single_is_the_entity_lpf_feature(self):
        body = self.assertCompat(self.get(f"/api/place/{self.place.id}/"), "place")
        self.assertEqual(body["type"], "Feature")
        self.assertEqual(body["@id"], f"http://testserver/entity/place:whg:{self.ds.id}:lc1/api")
        self.assertEqual(body["properties"]["title"], "Compatville")
        self.assertEqual(body["geometry"]["type"], "Point")
        self.assertEqual(body["compat"]["successor_request"]["url"],
                         f"http://testserver/entity/place:{self.place.id}/api")

    def test_place_list_and_visibility(self):
        body = self.assertCompat(self.get(f"/api/place/{self.place.id}-{self.hidden.id}/"), "place")
        self.assertEqual(body["type"], "FeatureCollection")
        self.assertEqual([f["properties"]["title"] for f in body["features"]], ["Compatville"])
        r = self.get(f"/api/place/{self.hidden.id}/")
        body = self.assertCompat(r, "place", 404)
        self.assertEqual(body["error"], "No places found")

    def test_place_trap_is_the_legacy_error(self):
        r = self.get("/api/place/abc/def/ghi/")
        self.assertEqual(r.status_code, 400)
        self.assertEqual(r.json()["error"], "Malformed URL")

    def test_place_by_src_id(self):
        body = self.assertCompat(self.get("/api/place/lc_ds/lc1/"), "place_src")
        self.assertEqual(body["properties"]["src_id"], "lc1")
        self.assertCompat(self.get("/api/place/lc_private/h1/"), "place_src", 404)

    def test_db_id(self):
        body = self.assertCompat(self.get("/api/db/", {"id": str(self.place.id)}), "db")
        self.assertEqual(body["features"][0]["properties"]["title"], "Compatville")

    def test_index_pid_and_whgid(self):
        body = self.assertCompat(self.get("/api/index/", {"pid": str(self.place.id)}), "index")
        self.assertEqual(body["count"], 1)
        r = self.get("/api/index/", {"whgid": "123"})
        self.assertEqual(r.status_code, 410)
        self.assertIn("no_successor", r.json())

    def test_geojson_redirects_to_the_entity_api_with_the_token(self):
        r = self.get("/api/geojson/", {"id": str(self.ds.id)})
        self.assertEqual(r.status_code, 307)
        self.assertEqual(r["Location"], f"/entity/dataset:{self.ds.id}/api?token=lc-token")
        self.assertHeaders(r)
        self.assertEqual(self.get("/api/geojson/", {"coll": "1"}).status_code, 410)

    def test_inapp_keeps_legacy_view(self):
        r = self.client.get("/api/spatial/", {"type": "bbox", "sw": "-2,50", "ne": "0,52"},
                            HTTP_REFERER="http://testserver/portal/")
        self.assertEqual(r.status_code, 200)
        body = r.json()
        self.assertNotIn("compat", body)
        self.assertEqual(body["features"][0]["properties"]["title"], "Compatville")

    def test_session_user_is_served(self):
        self.client.force_login(self.caller)
        with patch("api.reconcile.process_queries", return_value=_candidates(GN)):
            with self.settings(LEGACY_API_INAPP_PASSTHROUGH=False):
                r = self.client.get("/api/index/", {"name": "London"})
        self.assertEqual(r.status_code, 200)
        self.assertIn("compat", r.json())


class DeprecatedModeUnchangedTests(_Fixture):
    """No setting: the legacy views answer, as pinned in tests_legacy_deprecation."""

    def test_legacy_view_answers(self):
        with patch("api.reconcile.process_queries") as pq:
            r = self.client.get("/api/spatial/", {"type": "bbox", "sw": "-2,50", "ne": "0,52", "token": "lc-token"})
        pq.assert_not_called()
        body = r.json()
        self.assertEqual(r.status_code, 200)
        self.assertNotIn("compat", body)
        self.assertEqual(body["features"][0]["properties"]["title"], "Compatville")
