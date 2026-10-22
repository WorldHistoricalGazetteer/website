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
# An embargoed namespace's record (visible to a beta user only): it lies OUTSIDE the
# hull of the others, so had it been sent it would be a hull vertex.
EMB_XY = (0.5, 51.6)
GATEWAY["emb:7"] = {"place_id": "emb:7", "title": "Embargoed Town", "namespace": "emb", "repr_point": list(EMB_XY),
                    "names": [{"label": "Embargoed Town"}], "types": [], "ccodes": ["GB"],
                    "geometries": [{"type": "Point", "coordinates": list(EMB_XY)}]}


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
        GazetteerRegistryEntry.objects.update_or_create(
            id="wd", defaults=dict(name="Wikidata", namespace="wd", entry_class="authority", status="published"))
        GazetteerRegistryEntry.objects.update_or_create(
            id="emb", defaults=dict(name="Embargoed Source", namespace="emb", entry_class="authority",
                                    status="embargoed"))
        self.ds_a, self.ds_priv = a, priv
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
                                                "bands": "ABCDET", "level": 8, "from_year": 1600, "to_year": 1700})
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

    def test_plan_states_the_basinatlas_level_it_asked_for(self):
        """Every plan that asks EDOPS names the BasinATLAS level it sent, from the
        constants (api.edop_client.BASIN_LEVEL / AREA_LEVEL), so the page can say it
        even when the answer is a failure: 8 for a basin (one point, or each member of a
        dispersed locus), 6 for an area. Nothing asked, no level."""
        from api import edop_client as edop
        self.assertEqual((edop.BASIN_LEVEL, edop.AREA_LEVEL), (8, 6))
        basin = self.env(self.Lonely.pk, FakeTransport(BASIN)).json()
        area = self.env(self.A.pk, FakeTransport(AREA)).json()
        down = self.env(self.Sea.pk, FakeTransport((503, ""))).json()       # a failed basin still names it
        nowhere = self.env(self.Nowhere.pk, FakeTransport(BASIN)).json()
        self.assertEqual((basin["plan"]["kind"], basin["plan"]["level"]), ("basin", edop.BASIN_LEVEL))
        self.assertEqual((area["plan"]["kind"], area["plan"]["level"]), ("area", edop.AREA_LEVEL))
        self.assertEqual((down["status"], down["plan"]["level"]), ("unavailable", edop.BASIN_LEVEL))
        self.assertEqual(nowhere["plan"]["kind"], "no_location")
        self.assertNotIn("level", nowhere["plan"])
        far = self.post("/atlas/locus/environment/", {"members": [
            {"place_id": "gn:1", "title": "A", "repr_point": [-0.1, 51.5]},
            {"place_id": "gn:2", "title": "A", "repr_point": [2.0, 51.5]}]}, FakeTransport(BASIN)).json()
        tight = self.post("/atlas/locus/environment/", {"members": [
            {"place_id": "gn:1", "title": "A", "repr_point": [-0.1, 51.5]},
            {"place_id": "wd:Q1", "title": "A", "repr_point": [-0.05, 51.52]}]}, FakeTransport(AREA)).json()
        self.assertEqual((far["plan"]["kind"], far["plan"]["level"]), ("per_member", edop.BASIN_LEVEL))
        self.assertEqual((tight["plan"]["kind"], tight["plan"]["level"]), ("area", edop.AREA_LEVEL))
        # The level in the plan is the level in the request actually sent.
        with patch.object(edop, "BASIN_LEVEL", 6):
            t = FakeTransport(BASIN)
            caches["default"].clear()
            r = self.env(self.Lonely.pk, t).json()
        self.assertEqual((r["plan"]["level"], t.calls[0]["params"]["level"]), (6, 6))

    def test_locus_gloss(self):
        r = self.post("/atlas/locus/gloss/", {"members": [
            {"place_id": "gn:1", "namespace": "gn", "title": "Berlin", "types": [{"label": "P"}], "ccodes": ["DE"]},
            {"place_id": "wd:Q64", "namespace": "wd", "title": "Berlin", "ccodes": ["DE"]}]}).json()
        text = " ".join(r["gloss"]["sentences"])
        self.assertIn("2 records currently in this cluster", text)
        self.assertIn("GeoNames", text)
        self.assertIn("Country code DE in every record.", text)
        self.assertNotIn("disagree", text)

    # ---- nothing off-site that an anonymous caller could not see (review 2026-10-21) ----
    def _points_sent(self, t):
        """Every coordinate in every request the fake transport received."""
        from django.contrib.gis.geos import GEOSGeometry
        out = []
        for c in t.calls:
            p = c["params"]
            if p.get("geom_wkt"):
                out.append(GEOSGeometry(p["geom_wkt"], srid=4326))
            if p.get("lat") is not None:
                out.append(Point(p["lon"], p["lat"], srid=4326))
        return out

    def test_embargoed_gateway_member_never_reaches_edops(self):
        from places.models import PlaceLink
        PlaceLink.objects.create(place=self.A, jsonb={"type": "closeMatch", "identifier": "emb:7"})
        t = FakeTransport(AREA)
        r = self.env(self.A.pk, t).json()
        self.assertEqual((r["status"], r["plan"]["kind"]), ("ok", "area"))
        (hull,) = self._points_sent(t)
        # Presence: the public gateway member and both records are in the hull sent ...
        for xy in [(-0.1278, 51.5074), (0.2, 51.4), (-0.3, 51.3)]:
            self.assertTrue(hull.buffer(1e-6).contains(Point(*xy, srid=4326)), xy)
        # ... absence: the embargoed one is not, and is listed as withheld.
        self.assertFalse(hull.buffer(1e-3).contains(Point(*EMB_XY, srid=4326)))
        self.assertNotIn(f"{EMB_XY[0]:.4f} {EMB_XY[1]:.4f}", t.calls[0]["params"]["geom_wkt"])
        self.assertEqual(r["plan"]["withheld"], ["emb:7"])
        self.assertEqual(r["signatures"][0]["members"], [f"whg:{self.A.pk}", f"whg:{self.B.pk}", "gn:100"])
        sent = {m["key"]: m["sent_to_edops"] for m in r["members"]}
        self.assertEqual(sent, {f"whg:{self.A.pk}": True, f"whg:{self.B.pk}": True, "gn:100": True,
                                "emb:7": False})
        # The beta user still SEES it in the gloss (not sent off-site).
        with patch("api.crc_client.crc_fetch_places", side_effect=fake_fetch):
            g = self.client.get(f"/atlas/place/{self.A.pk}/gloss/").json()
        self.assertIn("Embargoed Source", " ".join(g["gloss"]["sentences"]))
        self.assertIn("GeoNames", " ".join(g["gloss"]["sentences"]))

    def test_embargoed_member_alone_is_not_a_basin_for_it(self):
        """A record whose only equivalent is embargoed: one basin, at the RECORD's point."""
        from places.models import PlaceLink
        PlaceLink.objects.create(place=self.Sea, jsonb={"type": "exactMatch", "identifier": "emb:7"})
        t = FakeTransport(BASIN)
        r = self.env(self.Sea.pk, t).json()
        self.assertEqual(r["plan"]["kind"], "basin")
        self.assertEqual((t.calls[0]["params"]["lon"], t.calls[0]["params"]["lat"]), (-30, 0))
        self.assertEqual(r["plan"]["withheld"], ["emb:7"])

    def test_locus_posts_only_what_an_anonymous_caller_could_see(self):
        members = [
            {"place_id": "gn:1", "namespace": "gn", "title": "A", "repr_point": [-0.1, 51.5]},
            {"place_id": f"whg:{self.ds_a.pk}:A1", "namespace": "whg", "title": "A", "repr_point": [-0.11, 51.49]},
            {"place_id": "emb:7", "namespace": "emb", "title": "A", "repr_point": list(EMB_XY)},
            {"place_id": f"whg:{self.ds_priv.pk}:P1", "namespace": "whg", "title": "A", "repr_point": [-0.2, 51.45]},
            {"place_id": "nope:1", "namespace": "nope", "title": "A", "repr_point": [-0.15, 51.55]},
            {"place_id": "whg:12345", "namespace": "whg", "title": "A", "repr_point": [-0.12, 51.52]},
        ]
        t = FakeTransport(AREA)
        r = self.post("/atlas/locus/environment/", {"members": members}, t).json()
        self.assertEqual(r["plan"]["kind"], "area")
        (hull,) = self._points_sent(t)
        for xy in ([-0.1, 51.5], [-0.11, 51.49]):                       # presence
            self.assertTrue(hull.buffer(1e-6).contains(Point(*xy, srid=4326)), xy)
        for xy in (EMB_XY, (-0.2, 51.45), (-0.15, 51.55)):               # absence
            self.assertFalse(hull.buffer(1e-4).contains(Point(*xy, srid=4326)), xy)
        self.assertEqual(sorted(r["plan"]["withheld"]),
                         sorted(["emb:7", f"whg:{self.ds_priv.pk}:P1", "nope:1", "whg:12345"]))
        # The same members, all withheld but one: per-member basins would be per member,
        # so check a dispersed posting too (each request is one member's point).
        t2 = FakeTransport(BASIN)
        far = [dict(members[0]), {**members[2], "repr_point": [20.0, 51.6]}, {**members[0], "place_id": "gn:2",
                                                                          "repr_point": [10.0, 51.5]}]
        r2 = self.post("/atlas/locus/environment/", {"members": far}, t2).json()
        self.assertEqual(r2["plan"]["kind"], "per_member")
        lons = sorted(c["params"]["lon"] for c in t2.calls)
        self.assertEqual(lons, [-0.1, 10.0])                               # 20.0 (emb) absent
        # The gloss of the same posting keeps every member (not sent off-site).
        g = self.post("/atlas/locus/gloss/", {"members": members}).json()
        self.assertEqual(g["gloss"]["n_records"], 6)


@override_settings(CACHES=LOCMEM, EDOP_ENABLED=True, V4_LANDING_ENABLED=True)
class SharedGroupCacheTests(TestCase):
    """Defaults applied 2026-10-21 (SG): the concordance and enrichment endpoints share
    one group cache. One build per (record, viewer class) per TTL; the class (beta /
    signed in / anonymous) is in the key, so a beta user's group, which may hold an
    embargoed namespace's member, is never served to a lesser viewer."""

    def setUp(self):
        from api.models import GazetteerRegistryEntry
        from datasets.models import Dataset
        from licensing.models import License
        from places.models import Place, PlaceGeom, PlaceLink

        caches["default"].clear()
        User = get_user_model()
        self.beta = User.objects.create_user(username="gc_beta", email="gc_beta@example.org", password="pw",
                                             given_name="G", surname="Beta", role="beta_tester")
        self.plain = User.objects.create_user(username="gc_plain", email="gc_plain@example.org", password="pw",
                                              given_name="G", surname="Plain")
        with patch("datasets.signals.doi"):
            ds = Dataset.objects.create(owner=self.beta, label="gc325", title="GC", description="D", creator="X",
                                        public=True, ds_status="indexed", uri_base="https://example.org/places/")
        lic, _ = License.objects.get_or_create(spdx_id="CC-BY-4.0", defaults={"label": "CC BY 4.0"})
        GazetteerRegistryEntry.objects.update_or_create(
            id="gn", defaults=dict(name="GeoNames", namespace="gn", entry_class="authority", status="published",
                                   redistributable=True, license=lic))
        GazetteerRegistryEntry.objects.update_or_create(
            id="emb", defaults=dict(name="Embargoed Source", namespace="emb", entry_class="authority",
                                    status="embargoed", redistributable=True, license=lic))
        self.A = Place.objects.create(title="Cache A", src_id="CA1", dataset=ds, ccodes=["GB"])
        PlaceGeom.objects.create(place=self.A, geom=Point(-0.1278, 51.5074, srid=4326), jsonb={"type": "Point"})
        for ident in ("gn:100", "emb:7"):
            PlaceLink.objects.create(place=self.A, jsonb={"type": "closeMatch", "identifier": ident})
        from places import concordance
        self.builds = []
        real = concordance.build_concordance

        def counting(place, user=None, **kw):
            self.builds.append(getattr(user, "username", None) or "anonymous")
            return real(place, user=user, **kw)
        p = patch("places.concordance.build_concordance", side_effect=counting)
        p.start()
        self.addCleanup(p.stop)
        p = patch("api.crc_client.crc_fetch_places", side_effect=fake_fetch)
        p.start()
        self.addCleanup(p.stop)

    def get(self, path, user=None):
        self.client.logout()
        if user is not None:
            self.client.force_login(user)
        with patch("api.edop_client._requests_transport", side_effect=FakeTransport(BASIN)):
            return self.client.get(path).json()

    def members(self, data):
        return {m["place_id"] for m in data.get("aggregate") or []}

    def test_one_build_serves_concordance_gloss_and_environment(self):
        base = f"/atlas/place/{self.A.pk}"
        conc = self.get(f"{base}/concordance/", self.beta)
        gloss = self.get(f"{base}/gloss/", self.beta)
        env = self.get(f"{base}/environment/", self.beta)
        conc2 = self.get(f"{base}/concordance/", self.beta)
        self.assertEqual(self.builds, ["gc_beta"])                       # ONE build for four requests
        self.assertIn("gn:100", self.members(conc))
        self.assertEqual(conc2["aggregate"], conc["aggregate"])
        self.assertEqual({m["key"] for m in gloss["members"]}, {f"whg:{self.A.pk}", "gn:100", "emb:7"})
        self.assertEqual(env["status"], "ok")

    def test_viewer_class_isolation(self):
        base = f"/atlas/place/{self.A.pk}"
        beta = self.get(f"{base}/concordance/", self.beta)               # warms the beta entry first
        anon = self.get(f"{base}/concordance/")
        plain = self.get(f"{base}/concordance/", self.plain)
        # presence: the beta user's group has the embargoed namespace's member ...
        self.assertIn("emb:7", self.members(beta))
        # ... absence: neither lesser viewer is served it, and each was built for itself
        self.assertNotIn("emb:7", self.members(anon))
        self.assertNotIn("emb:7", self.members(plain))
        self.assertIn("gn:100", self.members(anon))                      # (open namespace, flag on)
        self.assertIn("gn:100", self.members(plain))
        self.assertEqual(self.builds, ["gc_beta", "anonymous", "gc_plain"])
        # A second beta user shares the beta entry (the class, not the person, is keyed).
        other = get_user_model().objects.create_user(username="gc_beta2", email="gc_b2@example.org", password="pw",
                                                     given_name="G", surname="B2", role="beta_tester")
        self.assertIn("emb:7", self.members(self.get(f"{base}/concordance/", other)))
        self.assertEqual(len(self.builds), 3)

    def test_gateway_failure_is_not_cached(self):
        base = f"/atlas/place/{self.A.pk}"
        with patch("api.crc_client.crc_fetch_places", side_effect=failing_fetch):
            down = self.get(f"{base}/concordance/", self.beta)
        up = self.get(f"{base}/concordance/", self.beta)
        self.assertNotIn("gn:100", self.members(down))
        self.assertIn("gn:100", self.members(up))
        self.assertEqual(len(self.builds), 2)

    def test_a_record_made_private_stops_rendering_from_cache(self):
        base = f"/atlas/place/{self.A.pk}"
        self.assertTrue(self.get(f"{base}/concordance/", self.beta)["public"])
        self.A.dataset.public = False
        self.A.dataset.save(update_fields=["public"])
        again = self.get(f"{base}/concordance/", self.beta)
        self.assertFalse(again.get("public"))
        self.assertNotIn("aggregate", again)

    def test_group_cache_is_keyed_by_viewer_class(self):
        """The enrichment endpoints are beta-only today, but the group cache must not
        rely on that: record_group itself keys by viewer class."""
        from django.contrib.auth.models import AnonymousUser
        from places.record_group import record_group
        beta = record_group(self.A, user=self.beta)
        anon = record_group(self.A, user=AnonymousUser())
        keys = lambda g: {m["key"] for m in g["members"]}  # noqa: E731
        self.assertIn("emb:7", keys(beta))                              # presence
        self.assertNotIn("emb:7", keys(anon))                           # absence
        self.assertIn("gn:100", keys(anon))
        self.assertIs(record_group(self.A, user=self.beta)["members"][0]["is_record"], True)
        self.assertEqual(self.builds, ["gc_beta", "anonymous"])


# ---------------------------------------------------------------------------------------
# Ruling 2026-10-21 (release-behaviour interview): the gloss and environment sections are
# PUBLIC at release, for anonymous visitors too, behind the release switch.
# ---------------------------------------------------------------------------------------

def honest_fetch(ids, user=None, allow_anonymous=False, meta=None):
    """The gateway as api.crc_client.crc_fetch_places behaves: a signed-out caller is
    answered only with the anonymous opt-in (``gateway_disabled / anonymous`` else)."""
    if not getattr(user, "is_authenticated", False) and not allow_anonymous:
        if meta is not None:
            meta.update(disabled=True, disabled_reason="anonymous")
        return {}
    return fake_fetch(ids)


class _PublicFixture(TestCase):
    """Record A (public) with an open gateway equivalent (gn:100), an EMBARGOED gateway
    namespace's equivalent (emb:7, a beta user sees it) and a public WHG equivalent (B);
    records in an embargoed and in a private dataset also claim to be A."""

    def setUp(self):
        from api.models import GazetteerRegistryEntry
        from datasets.models import Dataset
        from licensing.models import License
        from places.models import Place, PlaceGeom, PlaceLink, PlaceName

        caches["default"].clear()
        User = get_user_model()
        self.beta = User.objects.create_user(username="pe_beta", email="pe_beta@example.org", password="pw",
                                             given_name="P", surname="Beta", role="beta_tester")
        self.plain = User.objects.create_user(username="pe_plain", email="pe_plain@example.org", password="pw",
                                              given_name="P", surname="Plain")

        def ds(key, public=True):
            return Dataset.objects.create(owner=self.beta, label=f"pe325_{key}", title=f"Dataset {key}",
                                          description="D", creator="X", public=public, ds_status="indexed",
                                          uri_base="https://example.org/places/")

        def place(d, title, src_id, xy, links=()):
            p = Place.objects.create(title=title, src_id=src_id, dataset=d, ccodes=["GB"], minmax=[1600, 1700])
            PlaceGeom.objects.create(place=p, geom=Point(*xy, srid=4326), jsonb={"type": "Point"})
            PlaceName.objects.create(place=p, toponym=title, jsonb={"toponym": title})
            for t, ident in links:
                PlaceLink.objects.create(place=p, jsonb={"type": t, "identifier": ident})
            return p

        with patch("datasets.signals.doi"):
            a, emb_ds, priv_ds = ds("a"), ds("emb"), ds("priv", public=False)
        lic, _ = License.objects.get_or_create(spdx_id="CC-BY-4.0", defaults={"label": "CC BY 4.0"})
        GazetteerRegistryEntry.objects.update_or_create(
            id="gn", defaults=dict(name="GeoNames", namespace="gn", entry_class="authority", status="published",
                                   redistributable=True, license=lic))
        GazetteerRegistryEntry.objects.update_or_create(
            id="emb", defaults=dict(name="Embargoed Source", namespace="emb", entry_class="authority",
                                    status="embargoed", redistributable=True, license=lic))
        GazetteerRegistryEntry.objects.create(id=f"whg:{emb_ds.pk}", name="Emb DS", namespace="whg",
                                              entry_class="dataset", status="embargoed")
        self.A = place(a, "Record A", "A1", (-0.1278, 51.5074),
                       links=[("exactMatch", "gn:100"), ("closeMatch", "emb:7")])
        self.B = place(a, "Record B", "B1", (-0.3, 51.3), links=[("closeMatch", f"whg:{self.A.pk}")])
        self.EmbRec = place(emb_ds, "Embargoed Record", "E1", (0.9, 51.9), links=[("closeMatch", f"whg:{self.A.pk}")])
        self.Priv = place(priv_ds, "Private Record", "P1", (0.8, 51.8), links=[("closeMatch", f"whg:{self.A.pk}")])
        self.missing = Place.objects.order_by("-id").first().id + 1000
        self.SECRET = ("Embargoed Source", "Embargoed Town", "Embargoed Record", "Private Record", "emb:7")
        self.SECRET_XY = [EMB_XY, (0.9, 51.9), (0.8, 51.8)]

    def call(self, path, transport=None, user=None, ip="203.0.113.5", fetch=honest_fetch):
        self.client.logout()
        if user is not None:
            self.client.force_login(user)
        with patch("api.edop_client._requests_transport", side_effect=transport or FakeTransport(AREA)), \
                patch("api.crc_client.crc_fetch_places", side_effect=fetch):
            return self.client.get(path, REMOTE_ADDR=ip)

    def gloss(self, pk, **kw):
        return self.call(f"/atlas/place/{pk}/gloss/", **kw)

    def env(self, pk, **kw):
        return self.call(f"/atlas/place/{pk}/environment/", **kw)


@override_settings(CACHES=LOCMEM, EDOP_ENABLED=True, V4_LANDING_ENABLED=False)
class PublicEnrichmentSwitchOffTests(_PublicFixture):
    def test_off_anonymous_and_non_beta_get_todays_403_bytes(self):
        t = FakeTransport(AREA)
        for user in (None, self.plain):
            for path in (f"/atlas/place/{self.A.pk}/gloss/", f"/atlas/place/{self.A.pk}/environment/",
                         f"/atlas/place/{self.missing}/gloss/"):
                r = self.call(path, transport=t, user=user)
                self.assertEqual((r.status_code, r.content), (403, b'{"error": "beta access required"}'), path)
                self.assertEqual(r["Content-Type"], "application/json")
        self.assertEqual(t.calls, [])
        ok = self.env(self.A.pk, transport=t, user=self.beta)            # presence: beta unchanged
        self.assertEqual((ok.status_code, len(t.calls)), (200, 1))


@override_settings(CACHES=LOCMEM, EDOP_ENABLED=True, V4_LANDING_ENABLED=True)
class PublicEnrichmentTests(_PublicFixture):
    def assert_no_secret(self, data):
        blob = json.dumps(data)
        for s in self.SECRET:
            self.assertNotIn(s, blob)

    def test_anonymous_gloss_and_environment_200(self):
        t = FakeTransport(AREA)
        g = self.gloss(self.A.pk, transport=t)
        e = self.env(self.A.pk, transport=t)
        self.assertEqual((g.status_code, e.status_code), (200, 200))
        g, e = g.json(), e.json()
        keys = {f"whg:{self.A.pk}", f"whg:{self.B.pk}", "gn:100"}
        self.assertEqual({m["key"] for m in g["members"]}, keys)           # presence: what anyone may see
        self.assertIn("GeoNames", " ".join(g["gloss"]["sentences"]))
        self.assertIn("This record and 2 attested equivalents", " ".join(g["gloss"]["sentences"]))
        self.assert_no_secret(g)                                           # absence: embargoed/private
        self.assertEqual((e["status"], e["plan"]["kind"]), ("ok", "area"))
        self.assertEqual({m["key"] for m in e["members"]}, keys)
        self.assertEqual(e["plan"]["withheld"], [])
        self.assert_no_secret(e)

    def test_signed_in_non_beta_is_served_too_and_beta_is_unchanged(self):
        t = FakeTransport(AREA)
        plain = self.gloss(self.A.pk, transport=t, user=self.plain).json()
        self.assertEqual({m["key"] for m in plain["members"]}, {f"whg:{self.A.pk}", f"whg:{self.B.pk}", "gn:100"})
        self.assert_no_secret(plain)
        beta = self.gloss(self.A.pk, transport=t, user=self.beta).json()
        # By design a beta user's gloss still describes the embargoed namespace's member.
        self.assertIn("emb:7", {m["key"] for m in beta["members"]})
        self.assertIn("Embargoed Source", " ".join(beta["gloss"]["sentences"]))

    def test_anonymous_is_never_served_a_beta_group(self):
        from places import record_group as rg
        built = []
        real = rg.build_record_group

        def counting(place, user=None):
            from places.concordance import viewer_class
            built.append(viewer_class(user))
            return real(place, user=user)
        t = FakeTransport(AREA, AREA)
        with patch("places.record_group.build_record_group", side_effect=counting):
            beta = self.gloss(self.A.pk, transport=t, user=self.beta).json()     # warms the beta entry first
            anon_g = self.gloss(self.A.pk, transport=t).json()
            anon_e = self.env(self.A.pk, transport=t).json()
        self.assertIn("emb:7", {m["key"] for m in beta["members"]})               # presence
        self.assert_no_secret(anon_g)                                              # absence
        self.assert_no_secret(anon_e)
        self.assertEqual(built, ["beta", "anonymous_open"])                       # one anonymous build, its own
        self.assertIsNotNone(caches["default"].get(f"record_group:v{rg.GROUP_CACHE_VERSION}:{self.A.pk}:anonymous_open"))

    def test_nothing_an_anonymous_caller_cannot_see_reaches_edops(self):
        from django.contrib.gis.geos import GEOSGeometry
        t = FakeTransport(AREA)
        self.env(self.A.pk, transport=t, user=self.beta)                   # the beta path, same record
        t_anon = FakeTransport(AREA)
        r = self.env(self.A.pk, transport=t_anon).json()
        self.assertEqual(r["status"], "ok")
        sent = t.calls + t_anon.calls
        self.assertTrue(sent)
        for c in sent:
            p = c["params"]
            hull = GEOSGeometry(p["geom_wkt"], srid=4326)
            for xy in [(-0.1278, 51.5074), (0.2, 51.4), (-0.3, 51.3)]:    # presence: A, gn:100, B
                self.assertTrue(hull.buffer(1e-6).contains(Point(*xy, srid=4326)), xy)
            for xy in self.SECRET_XY:                                      # absence: emb:7, embargoed, private
                self.assertFalse(hull.buffer(1e-3).contains(Point(*xy, srid=4326)), xy)
            self.assertNotIn("Embargoed", json.dumps(p))

    def test_private_embargoed_and_missing_are_one_404(self):
        t = FakeTransport(AREA)
        answers = {}
        for name, pk in (("private", self.Priv.pk), ("embargoed", self.EmbRec.pk), ("missing", self.missing)):
            for kind in ("gloss", "environment"):
                r = self.call(f"/atlas/place/{pk}/{kind}/", transport=t)
                answers[(name, kind)] = (r.status_code, r["Content-Type"],
                                         r.content.replace(str(pk).encode(), b"<pk>"))
        self.assertEqual(len(set(answers.values())), 1, answers)
        self.assertEqual(next(iter(answers.values()))[0], 404)
        self.assertEqual(t.calls, [])
        self.assertEqual(self.gloss(self.A.pk, transport=t).status_code, 200)     # presence

    def test_locus_endpoints_stay_beta_gated(self):
        body = json.dumps({"members": [{"place_id": "gn:1", "title": "A", "repr_point": [-0.1, 51.5]}]})
        t = FakeTransport(BASIN)
        with patch("api.edop_client._requests_transport", side_effect=t):
            for user in (None, self.plain):
                self.client.logout()
                if user:
                    self.client.force_login(user)
                for url in ("/atlas/locus/gloss/", "/atlas/locus/environment/"):
                    r = self.client.post(url, body, content_type="application/json")
                    self.assertEqual(r.status_code, 403, (user, url))
            self.client.force_login(self.beta)
            self.assertEqual(self.client.post("/atlas/locus/environment/", body,
                                              content_type="application/json").status_code, 200)
        self.assertEqual(len(t.calls), 1)

    def test_public_rates(self):
        from places import views_enrichment as ve
        self.assertEqual((ve.PUBLIC_GLOSS_RATE, ve.PUBLIC_ENVIRONMENT_RATE), ("60/min", "20/min"))

    def test_throttles_per_ip_per_endpoint_and_beta_is_not_throttled(self):
        from places import views_enrichment as ve
        with patch.object(ve, "PUBLIC_GLOSS_RATE", "2/min"), patch.object(ve, "PUBLIC_ENVIRONMENT_RATE", "1/min"):
            g = [self.gloss(self.A.pk).status_code for _ in range(3)]
            self.assertEqual(g, [200, 200, 429])
            r = self.gloss(self.A.pk)
            self.assertEqual(r.status_code, 429)
            self.assertGreater(int(r["Retry-After"]), 0)
            self.assertEqual(r.json()["error"], "rate limited")
            self.assertEqual(self.gloss(self.A.pk, ip="198.51.100.9").status_code, 200)   # another IP
            e = [self.env(self.A.pk).status_code for _ in range(2)]        # its own scope, own rate
            self.assertEqual(e, [200, 429])
            self.assertEqual([self.gloss(self.A.pk, user=self.plain).status_code for _ in range(3)],
                             [200, 200, 429])                               # signed in: per user
            self.assertEqual([self.gloss(self.A.pk, user=self.beta).status_code for _ in range(4)],
                             [200] * 4)                                     # beta: as before, unthrottled
            # A throttled request does no work: the transport is not called for it.
            t = FakeTransport(AREA)
            self.assertEqual(self.env(self.A.pk, transport=t).status_code, 429)
            self.assertEqual(t.calls, [])


class PublicSectionsTemplateTests(_PublicFixture):
    """The Record page renders the two sections for a non-beta visitor only with the
    switch on, marked data-enrichment="public"; the beta page carries no marker."""

    def page(self, user=None):
        self.client.logout()
        if user is not None:
            self.client.force_login(user)
        with patch("api.crc_client.crc_clustering_params", return_value=(None, "disabled")):
            return self.client.get(f"/atlas/place/{self.A.pk}/")

    @override_settings(CACHES=LOCMEM, V4_LANDING_ENABLED=True)
    def test_on(self):
        for user in (None, self.plain):
            html = self.page(user).content.decode()
            self.assertIn('id="record_gloss"', html)
            self.assertIn('id="record_environment"', html)
            self.assertEqual(html.count('data-enrichment="public"'), 2)
            self.assertIn('href="#record_gloss"', html)
        beta = self.page(self.beta).content.decode()
        self.assertIn('id="record_gloss"', beta)                       # presence
        self.assertNotIn("data-enrichment", beta)                      # absence: beta unchanged

    @override_settings(CACHES=LOCMEM, V4_LANDING_ENABLED=False)
    def test_off(self):
        self.assertEqual(self.page().status_code, 302)                 # to sign in, as today
        self.assertEqual(self.page(self.plain).status_code, 404)
        beta = self.page(self.beta).content.decode()
        self.assertIn('id="record_gloss"', beta)
        self.assertNotIn("data-enrichment", beta)
