"""place#325 Stage 3: the enrichment endpoints (EDOPS signature and gloss) for the
beta Record page and Locus page. EDOPS is never called: ``_requests_transport`` is
replaced by a recording fake serving the fixtures in ``scripts/fixtures/edop/``.

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test places.tests_enrichment_views --settings=whg.settings_localtest
"""
import json
from unittest.mock import patch

from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.gis.geos import Point, Polygon
from django.core.cache import caches
from django.test import TestCase, override_settings

from api.tests_edop_client import BASIN, BASIN_T, NO_BASIN, AREA, FakeTransport

LOCMEM = {**settings.CACHES, "default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache",
                                         "LOCATION": "enrich-tests"}}
GATEWAY = {"gn:100": {"place_id": "gn:100", "title": "Gateway Town", "namespace": "gn", "repr_point": [0.2, 51.4],
                      "names": [{"label": "Gateway Town"}, {"label": "Porta"}], "types": [{"label": "P"}],
                      "ccodes": ["GB"], "timespans": [{"start": 1650, "end": 1650}],
                      "geometries": [{"type": "Point", "coordinates": [0.2, 51.4]}]}}


def fake_fetch(ids, user=None, allow_anonymous=False, meta=None):
    return {i: GATEWAY[i] for i in ids if i in GATEWAY}


def failing_fetch(ids, user=None, allow_anonymous=False, meta=None):
    if meta is not None:
        meta["error"] = "timeout"
    return {}


@override_settings(CACHES=LOCMEM, EDOP_ENABLED=True)
class EnrichmentViewTests(TestCase):
    def setUp(self):
        from api.models import GazetteerRegistryEntry
        from datasets.models import Dataset
        from places.models import Place, PlaceGeom, PlaceLink, PlaceName, PlaceType

        caches["default"].clear()
        User = get_user_model()
        self.beta = User.objects.create_user(username="en_beta", email="en_beta@example.org", password="pw",
                                             given_name="E", surname="Beta", role="beta_tester")
        self.plain = User.objects.create_user(username="en_plain", email="en_plain@example.org", password="pw",
                                              given_name="E", surname="Plain")

        def ds(key, public=True):
            return Dataset.objects.create(owner=self.beta, label=f"en325_{key}", title=f"Dataset {key}",
                                          description="D", creator="X", public=public, ds_status="indexed",
                                          uri_base="https://example.org/places/")

        def place(d, title, src_id, geom=None, links=(), minmax=None, types=()):
            p = Place.objects.create(title=title, src_id=src_id, dataset=d, ccodes=["GB"], minmax=minmax)
            if geom is not None:
                PlaceGeom.objects.create(place=p, geom=geom, jsonb={"type": geom.geom_type})
            PlaceName.objects.create(place=p, toponym=title, jsonb={"toponym": title})
            for t in types:
                PlaceType.objects.create(place=p, jsonb={"sourceLabel": t}, fclass="P")
            for t, ident in links:
                PlaceLink.objects.create(place=p, jsonb={"type": t, "identifier": ident})
            return p

        with patch("datasets.signals.doi"):
            a, b, priv = ds("a"), ds("b"), ds("priv", public=False)
        GazetteerRegistryEntry.objects.update_or_create(
            id="gn", defaults=dict(name="GeoNames", namespace="gn", entry_class="authority", status="published"))
        self.A = place(a, "Record A", "A1", geom=Point(-0.1278, 51.5074, srid=4326),
                       links=[("exactMatch", "gn:100")], minmax=[1600, 1700], types=["parish"])
        # B says it is A, with a polygon: the hull must take its geometry, not just a point.
        self.B = place(b, "Record B", "B1", geom=Polygon(((-0.3, 51.3), (-0.2, 51.3), (-0.2, 51.4), (-0.3, 51.3)),
                                                          srid=4326),
                       links=[("closeMatch", f"whg:{self.A.pk}")])
        self.Lonely = place(a, "Lonely", "L1", geom=Point(-0.1278, 51.5074, srid=4326), minmax=[1600, 1700])
        self.Sea = place(a, "Sea", "S1", geom=Point(-30, 0, srid=4326))
        self.Nowhere = place(a, "Nowhere", "N1")
        self.Private = place(priv, "Private", "P1", geom=Point(1, 1, srid=4326))
        self.client.force_login(self.beta)

    def env(self, pk, transport, fetch=fake_fetch):
        with patch("api.edop_client._requests_transport", side_effect=transport), \
                patch("api.crc_client.crc_fetch_places", side_effect=fetch):
            return self.client.get(f"/atlas/place/{pk}/environment/")

    # ---- gating ---------------------------------------------------------------
    def test_beta_gated_and_visibility(self):
        t = FakeTransport(BASIN)
        self.client.force_login(self.plain)
        self.assertEqual(self.env(self.Lonely.pk, t).status_code, 403)
        self.assertEqual(self.client.get(f"/atlas/place/{self.Lonely.pk}/gloss/").status_code, 403)
        self.assertEqual(self.client.post("/atlas/locus/gloss/", "{}", content_type="application/json").status_code, 403)
        self.client.logout()
        self.assertEqual(self.env(self.Lonely.pk, t).status_code, 403)
        self.client.force_login(self.beta)
        self.assertEqual(self.env(999999, t).status_code, 404)
        ok = self.env(self.Lonely.pk, t)
        self.assertEqual(ok.status_code, 200)                 # presence beside the absences above
        self.assertEqual(len(t.calls), 1)

    def test_private_record_sends_nothing_to_edops(self):
        t = FakeTransport(BASIN)
        r = self.env(self.Private.pk, t).json()          # the owner can see it ...
        self.assertEqual(r, {"public": False, "place_id": self.Private.pk})
        self.assertEqual(t.calls, [])                    # ... but no coordinate leaves WHG
        g = self.client.get(f"/atlas/place/{self.Private.pk}/gloss/").json()
        self.assertEqual(g["public"], False)
        lonely = self.env(self.Lonely.pk, t).json()       # control: a public record does ask
        self.assertEqual((lonely["status"], len(t.calls)), ("ok", 1))

    # ---- scope choice -----------------------------------------------------------
    def test_single_record_is_one_basin_with_its_years(self):
        t = FakeTransport(BASIN_T)
        r = self.env(self.Lonely.pk, t).json()
        self.assertEqual(r["status"], "ok")
        self.assertEqual(r["plan"]["kind"], "basin")
        self.assertEqual(t.calls[0]["params"], {"scope": "basin", "lat": 51.5074, "lon": -0.1278,
                                                "bands": "ABCDET", "level": 6, "from_year": 1600, "to_year": 1700})
        (sig,) = r["signatures"]
        self.assertEqual(sig["request"]["from_year"], 1600)
        self.assertEqual(sig["summary"]["temporal"]["years"], [1600, 1700])
        self.assertEqual(r["attribution"]["url"], "https://computingplace.org/")
        self.assertNotIn("raw", sig)

    def test_concordance_is_one_area_over_the_members_hull(self):
        t = FakeTransport(AREA)
        r = self.env(self.A.pk, t).json()
        self.assertEqual((r["status"], r["plan"]["kind"]), ("ok", "area"))
        (call,) = t.calls
        p = call["params"]
        self.assertEqual((p["scope"], p["bands"], p["level"]), ("area", "ABCDE", 6))
        from django.contrib.gis.geos import GEOSGeometry
        hull = GEOSGeometry(p["geom_wkt"], srid=4326)
        for x, y in [(-0.1278, 51.5074), (0.2, 51.4), (-0.3, 51.3), (-0.2, 51.4)]:   # A, gateway, B's polygon
            self.assertTrue(hull.buffer(1e-6).contains(Point(x, y, srid=4326)), (x, y))
        self.assertFalse(hull.contains(Point(5, 5, srid=4326)))
        # The group spans 1600-1700 (A) and 1650 (gn:100): a range, so Band T is not sent to
        # area, and the response says why.
        self.assertNotIn("from_year", p)
        self.assertEqual(r["plan"]["years"]["note"], "area_needs_single_year")
        self.assertEqual({m["key"] for m in r["members"]}, {f"whg:{self.A.pk}", f"whg:{self.B.pk}", "gn:100"})
        self.assertEqual(r["signatures"][0]["members"], [f"whg:{self.A.pk}", f"whg:{self.B.pk}", "gn:100"])
        self.assertNotIn("geom_wkt", r["signatures"][0]["request"])
        self.assertGreater(r["signatures"][0]["request"]["geom_wkt_vertices"], 3)

    def test_no_basin_and_no_location_and_unavailable_are_distinct(self):
        sea = self.env(self.Sea.pk, FakeTransport(NO_BASIN)).json()
        nowhere_t = FakeTransport(BASIN)
        nowhere = self.env(self.Nowhere.pk, nowhere_t).json()
        down = self.env(self.Lonely.pk, FakeTransport((503, ""))).json()
        self.assertEqual(sea["status"], "no_basin")
        self.assertEqual(nowhere["status"], "no_location")
        self.assertEqual(nowhere_t.calls, [])
        self.assertEqual(down["status"], "unavailable")
        self.assertIsNone(down["signatures"][0]["summary"])
        ok = self.env(self.Lonely.pk, FakeTransport(BASIN)).json()   # cached failure (30 s) is labelled
        self.assertEqual((ok["status"], ok["signatures"][0]["cached"]), ("unavailable", True))

    def test_off_switch(self):
        t = FakeTransport(BASIN)
        with override_settings(EDOP_ENABLED=False):
            r = self.env(self.Lonely.pk, t).json()
        self.assertEqual((r["status"], t.calls), ("disabled", []))

    # ---- gloss ----------------------------------------------------------------------
    def test_record_gloss(self):
        with patch("api.crc_client.crc_fetch_places", side_effect=fake_fetch):
            g = self.client.get(f"/atlas/place/{self.A.pk}/gloss/").json()
        text = " ".join(g["gloss"]["sentences"])
        self.assertIn("This record and 2 attested equivalents", text)
        self.assertIn("Dataset a", text)
        self.assertIn("GeoNames", text)                       # registry name, not "gn"
        self.assertIn("populated place (1)", text)            # gn feature class P, read as a type
        self.assertIn("parish (1)", text)
        self.assertNotIn("Record C", text)

    def test_record_gloss_when_the_index_is_down(self):
        with patch("api.crc_client.crc_fetch_places", side_effect=failing_fetch):
            g = self.client.get(f"/atlas/place/{self.A.pk}/gloss/").json()
        # gn:100 could not be resolved, so it is missing from the group: said, not hidden.
        self.assertTrue(any("1 attested equivalent could not be looked up" in u and "incomplete group" in u
                            for u in g["gloss"]["unknown"]), g["gloss"]["unknown"])
        self.assertNotIn("populated place", " ".join(g["gloss"]["sentences"]))
        with patch("api.crc_client.crc_fetch_places", side_effect=fake_fetch):  # not cached thin
            g2 = self.client.get(f"/atlas/place/{self.A.pk}/gloss/").json()
        self.assertFalse(any("could not be looked up" in u for u in g2["gloss"]["unknown"]))
        self.assertIn("populated place (1)", " ".join(g2["gloss"]["sentences"]))

    def test_gateway_details_unavailable_is_not_none(self):
        """The concordance resolved gn:100 but the details call then failed: the member
        stays in the group, counted by name only, and the gloss says its details are not
        known (not that it has none)."""
        calls = []

        def first_ok_then_fail(ids, user=None, allow_anonymous=False, meta=None):
            calls.append(list(ids))
            return fake_fetch(ids) if len(calls) == 1 else failing_fetch(ids, meta=meta)
        with patch("api.crc_client.crc_fetch_places", side_effect=first_ok_then_fail):
            g = self.client.get(f"/atlas/place/{self.A.pk}/gloss/").json()
        self.assertEqual(len(calls), 2)
        self.assertIn("This record and 2 attested equivalents", " ".join(g["gloss"]["sentences"]))
        self.assertTrue(any("could not be read just now" in u for u in g["gloss"]["unknown"]))

    def test_environment_reports_an_incomplete_group(self):
        r = self.env(self.A.pk, FakeTransport(AREA), fetch=failing_fetch).json()
        self.assertEqual(r["unresolved"], 1)
        self.assertNotIn("gn:100", [m["key"] for m in r["members"]])
        ok = self.env(self.A.pk, FakeTransport(AREA)).json()
        self.assertEqual(ok["unresolved"], 0)

    # ---- locus -----------------------------------------------------------------------
    def post(self, url, body, transport=None):
        with patch("api.edop_client._requests_transport", side_effect=transport or FakeTransport(BASIN)):
            return self.client.post(url, json.dumps(body), content_type="application/json")

    def test_locus_validation(self):
        self.assertEqual(self.post("/atlas/locus/environment/", {}).status_code, 400)
        self.assertEqual(self.post("/atlas/locus/environment/", {"members": [{"title": "no id"}]}).status_code, 400)
        self.assertEqual(self.post("/atlas/locus/gloss/", {"members": [{"place_id": "x"}] * 201}).status_code, 400)
        self.assertEqual(self.client.get("/atlas/locus/gloss/").status_code, 405)
        self.assertEqual(self.post("/atlas/locus/gloss/", {"members": [{"place_id": "gn:1"}]}).status_code, 200)

    def test_locus_tight_is_one_area(self):
        t = FakeTransport(AREA)
        r = self.post("/atlas/locus/environment/", {"members": [
            {"place_id": "gn:1", "title": "A", "repr_point": [-0.1, 51.5]},
            {"place_id": "wd:Q1", "title": "A", "repr_point": [-0.05, 51.52]}]}, t).json()
        self.assertEqual((r["plan"]["kind"], len(t.calls), t.calls[0]["params"]["scope"]), ("area", 1, "area"))
        self.assertEqual(r["plan"]["tight_km"], 50.0)
        self.assertLess(r["plan"]["span_km"], 50)

    def test_locus_dispersed_is_per_member_and_shared_basins_merge(self):
        t = FakeTransport(BASIN)       # every call answers basin 4620: they "share" it
        r = self.post("/atlas/locus/environment/", {"members": [
            {"place_id": "gn:1", "title": "A", "repr_point": [-0.1, 51.5]},
            {"place_id": "gn:2", "title": "A", "repr_point": [2.0, 51.5]},
            {"place_id": "gn:3", "title": "A"}]}, t).json()
        self.assertEqual(r["plan"]["kind"], "per_member")
        self.assertEqual(len(t.calls), 2)
        self.assertEqual({c["params"]["scope"] for c in t.calls}, {"basin"})
        self.assertEqual(len(r["signatures"]), 1)
        self.assertEqual(r["signatures"][0]["members"], ["gn:1", "gn:2"])
        self.assertEqual(r["plan"]["no_location"], ["gn:3"])

    def test_locus_gloss(self):
        r = self.post("/atlas/locus/gloss/", {"members": [
            {"place_id": "gn:1", "namespace": "gn", "title": "Berlin", "types": [{"label": "P"}], "ccodes": ["DE"]},
            {"place_id": "wd:Q64", "namespace": "wd", "title": "Berlin", "ccodes": ["DE"]}]}).json()
        text = " ".join(r["gloss"]["sentences"])
        self.assertIn("2 records currently in this cluster", text)
        self.assertIn("GeoNames", text)
        self.assertIn("Country code DE in every record.", text)
        self.assertNotIn("disagree", text)
