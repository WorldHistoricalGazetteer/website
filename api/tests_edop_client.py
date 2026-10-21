"""api.edop_client (place#325 Stage 3): requests, error classes, cache, scope choice.

Never calls the live service: a fake transport serves the responses recorded in
``scripts/fixtures/edop/`` (2026-10-21; see its README for the exact requests).

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test api.tests_edop_client --settings=whg.settings_localtest
"""
import json
import os
import re
from unittest.mock import patch

from django.conf import settings
from django.core.cache import caches
from django.test import SimpleTestCase, override_settings

from api import edop_client as edop

FIX = os.path.join(settings.BASE_DIR, "scripts", "fixtures", "edop")


def fixture_text(name):
    with open(os.path.join(FIX, name), encoding="utf-8") as f:
        return f.read()


LOCMEM = {**settings.CACHES, "default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache",
                                         "LOCATION": "edop-tests"}}


class FakeTransport:
    """Records every call; answers from a queue of (status, body) or exceptions."""

    def __init__(self, *answers):
        self.answers = list(answers)
        self.calls = []

    def __call__(self, url, params, timeout):
        self.calls.append({"url": url, "params": dict(params), "timeout": timeout})
        a = self.answers.pop(0) if len(self.answers) > 1 else self.answers[0]
        if isinstance(a, BaseException):
            raise a
        return a


BASIN_T = (200, fixture_text("basin_london_ABCDET_1600-1700.json"))
BASIN = (200, fixture_text("basin_london_ABCDE.json"))
AREA = (200, fixture_text("area_london_ABCDE.json"))
AREA_T = (200, fixture_text("area_london_ABCDET_1650.json"))
NO_BASIN = (404, fixture_text("404_no_basin.json"))
MISSING_SCOPE = (422, fixture_text("422_missing_scope.json"))
AREA_RANGE = (422, fixture_text("422_area_band_t_range.json"))
LONDON = (-0.1278, 51.5074)
BOX = "POLYGON((-0.5 51.3,0.3 51.3,0.3 51.7,-0.5 51.7,-0.5 51.3))"


@override_settings(CACHES=LOCMEM, EDOP_ENABLED=True)
class RequestTests(SimpleTestCase):
    def setUp(self):
        caches["default"].clear()

    def test_basin_parameters_exact(self):
        self.assertEqual(edop.basin_request(*LONDON),
                         {"scope": "basin", "lat": 51.5074, "lon": -0.1278, "bands": "ABCDE", "level": 6})
        self.assertEqual(edop.basin_request(*LONDON, years=(1600, 1700)),
                         {"scope": "basin", "lat": 51.5074, "lon": -0.1278, "bands": "ABCDET", "level": 6,
                          "from_year": 1600, "to_year": 1700})

    def test_area_parameters_exact_and_band_t_single_year_only(self):
        self.assertEqual(edop.area_request(BOX),
                         {"scope": "area", "geom_wkt": BOX, "bands": "ABCDE", "level": 6})
        self.assertEqual(edop.area_request(BOX, year=1650),
                         {"scope": "area", "geom_wkt": BOX, "bands": "ABCDET", "level": 6,
                          "from_year": 1650, "to_year": 1650})

    def test_every_parameter_we_send_is_in_the_spec(self):
        """The contract is the live OpenAPI spec (recorded copy): each name we send is a
        declared parameter, scope is the required one, and our scope values are the
        ones its description lists."""
        spec = json.loads(fixture_text("openapi_v0.4.json"))
        self.assertEqual(spec["info"]["version"], "0.4")
        params = {p["name"]: p for p in spec["paths"]["/api/signature"]["get"]["parameters"]}
        sent = set(edop.basin_request(*LONDON, years=(1, 2))) | set(edop.area_request(BOX, year=5))
        self.assertTrue(sent <= set(params), sent - set(params))
        self.assertTrue(params["scope"]["required"])
        self.assertEqual([n for n, p in params.items() if p.get("required")], ["scope"])
        for scope in ("'basin'", "'area'"):
            self.assertIn(scope, params["scope"]["description"])
        self.assertIn("8 or 6", params["level"]["description"])
        self.assertEqual(edop.LEVEL, 6)
        self.assertEqual(edop.SIGNATURE_PATH, "/api/signature")
        self.assertIn("/api/signature", spec["paths"])

    def test_band_t_years(self):
        self.assertEqual(edop.band_t_years([1600, 1700]), ((1600, 1700), None))
        self.assertEqual(edop.band_t_years([1700, 1600]), ((1600, 1700), None))
        self.assertEqual(edop.band_t_years([-500, 300]), ((0, 300), "clamped"))
        self.assertEqual(edop.band_t_years([1800, 2020]), ((1800, 1998), "clamped"))
        self.assertEqual(edop.band_t_years([2000, 2020]), (None, "outside"))
        self.assertEqual(edop.band_t_years([-900, -100]), (None, "outside"))
        self.assertEqual(edop.band_t_years(None), (None, "undated"))
        self.assertEqual(edop.band_t_years([None, 1500]), ((1500, 1500), None))

    def test_timeouts_are_short_and_by_scope(self):
        t = FakeTransport(BASIN)
        edop.fetch_signature(edop.basin_request(*LONDON), transport=t)
        t2 = FakeTransport(AREA)
        edop.fetch_signature(edop.area_request(BOX), transport=t2)
        self.assertEqual(t.calls[0]["timeout"], (2.0, 5.0))
        self.assertEqual(t2.calls[0]["timeout"], (2.0, 12.0))
        self.assertEqual(t.calls[0]["url"], "https://computingplace.org/api/signature")


@override_settings(CACHES=LOCMEM, EDOP_ENABLED=True)
class StatusTests(SimpleTestCase):
    def setUp(self):
        caches["default"].clear()

    def fetch(self, *answers, params=None, **kw):
        t = FakeTransport(*answers)
        return edop.fetch_signature(params or edop.basin_request(*LONDON), transport=t, **kw), t

    def test_ok_is_trimmed_and_summarised(self):
        r, _ = self.fetch(BASIN_T, params=edop.basin_request(*LONDON, years=(1600, 1700)))
        self.assertEqual(r["status"], "ok")
        self.assertIsNone(r["reason"])
        self.assertIn("signature_bands", r["raw"])
        self.assertNotIn("geom_geojson", r["raw"])                            # bulk dropped ...
        self.assertNotIn("pdsi_series", r["raw"]["signature_bands"]["T"])
        self.assertIn("pdsi_mean", r["raw"]["signature_bands"]["T"])           # ... the rest kept
        s = edop.summarise(r)
        self.assertEqual(s["scope"], "basin")
        self.assertIn({"label": "Ecoregion", "value": "English Lowlands beech forests", "unit": None, "note": None},
                      s["headline"])
        self.assertEqual([b["band"] for b in s["bands"]], ["A", "B", "C", "D", "E"])
        self.assertEqual(s["unit"]["basin_id"], 4620)
        self.assertEqual(s["temporal"]["years"], [1600, 1700])
        t_labels = [i["label"] for i in s["temporal"]["items"]]
        self.assertIn("Drought index (PDSI), mean", t_labels)
        self.assertIn("Major volcanic events in the period", t_labels)
        d = next(b for b in s["bands"] if b["band"] == "D")
        self.assertIn("Present-day", d["description"])

    def test_area_summary(self):
        r, _ = self.fetch(AREA_T, params=edop.area_request(BOX, year=1650))
        s = edop.summarise(r)
        self.assertEqual((s["scope"], s["unit"]["n_basins"], s["unit"]["uncovered_pct"]), ("area", 3, 0))
        self.assertNotIn("detail", json.dumps(r["raw"]["variables"][0]))
        biome = next(i for i in s["headline"] if i["label"] == "Biome")
        self.assertEqual(biome["value"], "Temperate Broadleaf & Mixed Forests")
        c = next(b for b in s["bands"] if b["band"] == "C")
        temp = next(i for i in c["items"] if i["label"] == "Mean annual temperature")
        self.assertEqual((temp["value"], temp["unit"]), (37.8, "percentile score"))
        self.assertEqual(s["temporal"]["years"], [1650, 1650])
        self.assertTrue(any("CCSM4" in c for c in s["caveats"]))

    def test_no_basin_is_its_own_status(self):
        r, _ = self.fetch(NO_BASIN)
        self.assertEqual(r["status"], "no_basin")
        self.assertEqual(r["reason"], "No basin covers this point")
        self.assertNotIn("raw", r)

    def test_other_404_is_unavailable_not_no_basin(self):
        r, _ = self.fetch((404, '{"detail":"Not Found"}'))
        self.assertEqual(r["status"], "unavailable")
        self.assertNotEqual(r["status"], "no_basin")

    def test_422_both_shapes_are_rejected(self):
        r1, _ = self.fetch(MISSING_SCOPE)
        r2, _ = self.fetch(AREA_RANGE, params=edop.area_request(BOX))
        self.assertEqual((r1["status"], r2["status"]), ("rejected", "rejected"))
        self.assertIn("Field required", r1["reason"])
        self.assertIn("single year", r2["reason"])

    def test_unavailable_classes(self):
        import requests
        for answer in [(500, "oops"), (502, ""), (429, "{}"), (200, "<html>not json</html>"),
                       requests.Timeout("slow"), requests.ConnectionError("refused")]:
            caches["default"].clear()
            r, _ = self.fetch(answer)
            self.assertEqual(r["status"], "unavailable", answer)
            self.assertNotIn("raw", r)
            self.assertIsNone(edop.summarise(r))

    def test_disabled_asks_nothing(self):
        with override_settings(EDOP_ENABLED=False):
            r, t = self.fetch(BASIN)
        self.assertEqual(r["status"], "disabled")
        self.assertEqual(t.calls, [])
        r2, t2 = self.fetch(BASIN)                           # and on again: it asks
        self.assertEqual((r2["status"], len(t2.calls)), ("ok", 1))

    def test_logs_scope_and_status_but_no_url_or_geometry(self):
        with self.assertLogs("api.edop_client", level="INFO") as logs:
            self.fetch(AREA, params=edop.area_request(BOX))
        text = "\n".join(logs.output)
        self.assertIn("EDOPS area -> ok", text)
        self.assertNotIn("POLYGON", text)
        self.assertNotIn("computingplace", text)
        self.assertNotIn("geom_wkt", text)


@override_settings(CACHES=LOCMEM, EDOP_ENABLED=True)
class CacheAndRetryTests(SimpleTestCase):
    def setUp(self):
        caches["default"].clear()

    def test_hit_and_miss(self):
        t = FakeTransport(BASIN)
        p = edop.basin_request(*LONDON)
        first = edop.fetch_signature(p, transport=t)
        second = edop.fetch_signature(dict(p), transport=t)
        self.assertEqual(len(t.calls), 1)
        self.assertEqual((first["cached"], second["cached"]), (False, True))
        self.assertEqual(second["raw"], first["raw"])
        edop.fetch_signature(edop.basin_request(LONDON[0], 51.6), transport=t)   # another request: a miss
        edop.fetch_signature(edop.basin_request(*LONDON, years=(1600, 1700)), transport=t)
        self.assertEqual(len(t.calls), 3)

    def test_key_is_the_request(self):
        a = edop.cache_key({"scope": "basin", "lat": 1, "lon": 2})
        self.assertEqual(a, edop.cache_key({"lon": 2, "lat": 1, "scope": "basin"}))
        self.assertNotEqual(a, edop.cache_key({"scope": "basin", "lat": 1, "lon": 2.000001}))
        with override_settings(EDOP_BASE_URL="https://example.org"):
            self.assertNotEqual(a, edop.cache_key({"scope": "basin", "lat": 1, "lon": 2}))
        self.assertTrue(re.fullmatch(r"edop:v1:[0-9a-f]{64}", a))

    def test_ttls_by_status(self):
        seen = []
        real_set = caches["default"].set

        def spy(key, value, timeout=None, **kw):
            seen.append((value["status"], timeout))
            return real_set(key, value, timeout, **kw)

        with patch.object(caches["default"], "set", side_effect=spy):
            for i, answer in enumerate([BASIN, NO_BASIN, (503, ""), MISSING_SCOPE]):
                edop.fetch_signature(edop.basin_request(0, i), transport=FakeTransport(answer))
        self.assertEqual(seen, [("ok", 30 * 24 * 3600), ("no_basin", 30 * 24 * 3600), ("unavailable", 30)])
        self.assertNotIn("rejected", [s for s, _ in seen])

    def test_cached_failure_comes_back_labelled(self):
        t = FakeTransport((503, ""))
        edop.fetch_signature(edop.basin_request(*LONDON), transport=t)
        again = edop.fetch_signature(edop.basin_request(*LONDON), transport=t)
        self.assertEqual((again["status"], again["cached"], len(t.calls)), ("unavailable", True, 1))

    @patch("api.edop_client.time.sleep")
    def test_no_retry_by_default_and_bounded_when_asked(self, _sleep):
        t = FakeTransport((503, ""))
        edop.fetch_signature(edop.basin_request(*LONDON), transport=t, use_cache=False)
        self.assertEqual(len(t.calls), 1)
        t = FakeTransport((503, ""), BASIN)
        r = edop.fetch_signature(edop.basin_request(*LONDON), transport=t, retries=1, use_cache=False)
        self.assertEqual((len(t.calls), r["status"]), (2, "ok"))
        t = FakeTransport((503, ""))
        edop.fetch_signature(edop.basin_request(*LONDON), transport=t, retries=99, use_cache=False)
        self.assertEqual(len(t.calls), 1 + edop.MAX_RETRIES)
        t = FakeTransport(NO_BASIN)                          # a finding is never retried
        edop.fetch_signature(edop.basin_request(*LONDON), transport=t, retries=2, use_cache=False)
        self.assertEqual(len(t.calls), 1)


class GeometryAndPlanTests(SimpleTestCase):
    def test_span_and_tightness(self):
        # 0.1 degree of latitude is ~11.1 km.
        self.assertAlmostEqual(edop.span_km([[0, 0], [0, 0.1]]), 11.12, places=1)
        self.assertTrue(edop.is_tight([[0, 0], [0, 0.1], [0.05, 0.05]]))
        self.assertFalse(edop.is_tight([[0, 0], [0, 0.5]]))            # ~56 km
        self.assertTrue(edop.is_tight([[0, 0], [0, 0.44]]))            # ~48.9 km
        self.assertEqual(edop.LOCUS_TIGHT_KM, 50.0)
        self.assertEqual(edop.span_km([[0, 0]]), 0.0)
        self.assertEqual(edop.span_km([[0, 0], [999, 0]]), 0.0)        # invalid point ignored

    def test_hull_of_collinear_points_is_a_polygon(self):
        from django.contrib.gis.geos import GEOSGeometry, Point
        wkt = edop.hull_wkt([Point(0, 0, srid=4326), Point(0.1, 0.1, srid=4326)])
        self.assertTrue(wkt.startswith("POLYGON(("))
        g = GEOSGeometry(wkt, srid=4326)
        self.assertTrue(g.valid)
        self.assertTrue(g.contains(Point(0.05, 0.05, srid=4326)))

    def test_hull_is_simplified_to_the_vertex_cap(self):
        from django.contrib.gis.geos import Point
        circle = Point(10, 10, srid=4326).buffer(1.0, quadsegs=100)        # ~400 vertices
        wkt = edop.hull_wkt([circle])
        self.assertLessEqual(wkt.count(",") + 1, edop.MAX_HULL_VERTICES + 1)
        self.assertGreater(wkt.count(",") + 1, 4)
        self.assertTrue(re.search(r"\d+\.\d{5} ", wkt))

    def test_concordance_plans(self):
        from django.contrib.gis.geos import Point
        one = edop.plan_concordance([{"repr_point": list(LONDON)}], span=[1600, 1700])
        self.assertEqual(one["kind"], "basin")
        self.assertEqual(one["requests"], [edop.basin_request(*LONDON, years=(1600, 1700))])

        near = [{"repr_point": [-0.1, 51.5], "geometry": Point(-0.1, 51.5, srid=4326)},
                {"repr_point": [0.1, 51.6], "geometry": Point(0.1, 51.6, srid=4326)}]
        area = edop.plan_concordance(near, span=[1600, 1700])
        self.assertEqual(area["kind"], "area")
        (req,) = area["requests"]
        self.assertEqual((req["scope"], req["bands"], req["level"]), ("area", "ABCDE", 6))
        self.assertNotIn("from_year", req)                    # a range is not sent to area ...
        self.assertEqual(area["years"]["note"], "area_needs_single_year")   # ... and says why
        self.assertTrue(req["geom_wkt"].startswith("POLYGON(("))
        single = edop.plan_concordance(near, span=[1650, 1650])["requests"][0]
        self.assertEqual((single["bands"], single["from_year"], single["to_year"]), ("ABCDET", 1650, 1650))

        same = edop.plan_concordance([{"repr_point": [1, 1]}, {"repr_point": [1, 1]}])
        self.assertEqual(same["kind"], "basin")

        far = edop.plan_concordance([{"repr_point": [0, 0]}, {"repr_point": [20, 0]}])
        self.assertEqual((far["kind"], far["requests"]), ("dispersed", []))
        self.assertGreater(far["span_km"], edop.CONCORDANCE_MAX_SPAN_KM)

        none = edop.plan_concordance([{"repr_point": None}])
        self.assertEqual((none["kind"], none["requests"]), ("no_location", []))

    def test_locus_plans(self):
        def m(key, x, y, years=None):
            return {"key": key, "repr_point": [x, y], "years": years}
        tight = edop.plan_locus([m("a", 0, 0), m("b", 0.1, 0.1), m("c", 0.2, 0)])
        self.assertEqual((tight["kind"], len(tight["requests"])), ("area", 1))
        self.assertEqual(tight["groups"], [["a", "b", "c"]])

        loose = edop.plan_locus([m(f"k{i}", i * 2.0, 0, [1500, 1600] if i == 0 else None) for i in range(12)])
        self.assertEqual(loose["kind"], "per_member")
        self.assertEqual(len(loose["requests"]), edop.LOCUS_MAX_MEMBER_SIGNATURES)
        self.assertEqual(loose["not_looked_up"], [f"k{i}" for i in range(8, 12)])
        self.assertEqual(loose["requests"][0], edop.basin_request(0.0, 0, years=(1500, 1600)))
        self.assertEqual(loose["requests"][1], edop.basin_request(2.0, 0))
        self.assertTrue(all(r["scope"] == "basin" for r in loose["requests"]))

        mixed = edop.plan_locus([m("a", 0, 0), {"key": "nowhere", "repr_point": None}, m("b", 3, 0)])
        self.assertEqual((mixed["kind"], mixed["no_location"]), ("per_member", ["nowhere"]))

        dated = edop.plan_locus([m("a", 0, 0, [1650, 1650]), m("b", 0.1, 0, [1650, 1650])])
        self.assertEqual(dated["requests"][0]["from_year"], 1650)
        ranged = edop.plan_locus([m("a", 0, 0, [1600, 1650]), m("b", 0.1, 0, [1650, 1700])])
        self.assertNotIn("from_year", ranged["requests"][0])
        self.assertEqual(ranged["years"]["note"], "area_needs_single_year")
