"""api.edops_whgmodal: the provisional chart-modal payload built from recorded EDOPS answers.

Never calls the live service; the answers are the fixtures in ``scripts/fixtures/edop/``.

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test api.tests_edops_whgmodal --settings=whg.settings_localtest

``whgmodal_basin_london_L8_provisional.json`` is what the adapter produced for the level-8
fixture when the payload was proposed; it is what is shown to Karl and used by the browser
test. ``test_committed_example_matches_the_adapter`` fails when either side changes alone
(regenerate with ``python manage.py shell`` per its docstring).
"""
import json
import os

from django.test import SimpleTestCase, override_settings

from api import edop_client as edop
from api import edops_whgmodal as wm
from api.tests_edop_client import AREA, BASIN, BASIN_T8, FIX, FakeTransport, LOCMEM, NO_BASIN

EXAMPLE = os.path.join(FIX, "whgmodal_basin_london_L8_provisional.json")


@override_settings(CACHES=LOCMEM)
class AdapterTests(SimpleTestCase):
    def result(self, answer, params):
        from django.core.cache import caches
        caches["default"].clear()
        return edop.fetch_signature(params, transport=FakeTransport(answer))

    def basin_t(self):
        return self.result(BASIN_T8, edop.basin_request(-0.1278, 51.5074, years=(1600, 1700)))

    def test_basin_payload_shape_and_series(self):
        w = wm.from_signature_result(self.basin_t())
        self.assertEqual((w["schema"], w["status"]), (wm.SCHEMA, "ok"))
        self.assertEqual(w["scope"]["type"], "basin")
        self.assertEqual(w["scope"]["level"], 8)
        self.assertEqual(w["version"]["signature_version"], "0.4")
        self.assertTrue(w["version"]["generated"].startswith("2026-10-21"))
        s = w["series"]
        self.assertEqual(len(s["monthly"]["precipitation_mm"]), 12)
        self.assertEqual(len(s["monthly"]["temperature_c"]), 12)
        t = s["timeline"]
        self.assertEqual((t["years"][0], t["years"][-1], len(t["years"])), (1600, 1700, 101))
        for k in ("pdsi", "air_anom_k", "prate_anom_mm_day"):
            self.assertEqual(len(t[k]), 101)
        self.assertIs(t["reconstructed"], True)
        self.assertTrue(any("LMR" in c for c in t["caveats"]))            # EDOPS's own hedge, passed through
        self.assertEqual([e["year"] for e in s["land_use"]["epochs"]], [1600, 1700])
        self.assertEqual(len(s["volcanic"]["events"]), 3)
        self.assertEqual({x["id"] for x in w["sources"]} >= {"basin", "temporal_climate", "volcanic", "land_use_temporal"}, True)

    def test_every_passage_names_its_scale_and_the_text_is_labelled_generated(self):
        w = wm.from_signature_result(self.basin_t())
        p = w["profile"]
        self.assertIs(p["generated_text"], True)
        self.assertEqual(p["generated_by"], "whg-adapter-placeholder")    # not mistakable for EDOPS's own prose
        self.assertTrue(p["sections"])
        for sec in p["sections"]:
            self.assertIn(sec["scale"], ("place", "basin", "area"))
        self.assertEqual({s["scale"] for s in p["sections"]} >= {"place", "basin"}, True)
        past = next(s for s in p["sections"] if s["id"] == "basin_past")
        self.assertIn("reconstructed", past["text"])
        self.assertTrue(past["caveats"])

    def test_no_whg_data_in_the_payload(self):
        blob = json.dumps(wm.from_signature_result(self.basin_t()))
        self.assertIn("English Lowlands", blob)                           # presence: EDOPS's values are there
        for forbidden in ("geom_wkt", "51.5074", "-0.1278", "members", "place_id", "geom_geojson"):
            self.assertNotIn(forbidden, blob)

    def test_area_has_no_series_but_says_so_structurally(self):
        w = wm.from_signature_result(self.result(AREA, edop.area_request("POLYGON((-0.5 51.3,0.3 51.3,0.3 51.7,-0.5 51.7,-0.5 51.3))")))
        self.assertEqual((w["status"], w["scope"]["type"]), ("ok", "area"))
        self.assertIsNone(w["series"])
        self.assertTrue(w["signature"]["bands"])
        self.assertTrue(all(b["scale"] == "area" for b in w["signature"]["bands"]))

    def test_no_basin_is_a_status_not_empty_data(self):
        w = wm.from_signature_result(self.result(NO_BASIN, edop.basin_request(-30.0, 0.0)))
        self.assertEqual(w["status"], "no_basin")
        self.assertIsNone(w["signature"])
        self.assertIsNone(w["profile"])

    def test_without_band_t_there_is_no_timeline_but_monthly_remains(self):
        w = wm.from_signature_result(self.result(BASIN, edop.basin_request(-0.1278, 51.5074)))
        self.assertEqual(w["status"], "ok")
        self.assertIsNone(w["series"]["timeline"])                         # absence ...
        self.assertEqual(len(w["series"]["monthly"]["precipitation_mm"]), 12)   # ... beside a presence

    def test_committed_example_matches_the_adapter(self):
        """Regenerate: from api import edops_whgmodal as wm, json; see AdapterTests.basin_t;
        write json.dumps(payload, indent=1, sort_keys=True) to EXAMPLE."""
        w = wm.from_signature_result(self.basin_t())
        with open(EXAMPLE, encoding="utf-8") as f:
            self.assertEqual(json.load(f), json.loads(json.dumps(w)))
