"""The ``whgmodal`` block on the environment endpoints (prototype EDOPS modal; beta and public).

EDOPS is never called (fixtures through a fake transport).

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test places.tests_edops_whgmodal_views --settings=whg.settings_localtest
"""
import json
from unittest.mock import patch

from django.test import override_settings

from api.tests_edop_client import AREA, BASIN_T8, FakeTransport
from places.tests_enrichment_views import LOCMEM, _PublicFixture

MEMBERS = {"members": [{"place_id": "gn:1", "title": "Somewhere", "repr_point": [-0.1278, 51.5074]}]}


@override_settings(CACHES=LOCMEM, EDOP_ENABLED=True, V4_LANDING_ENABLED=True)
class WhgmodalViewTests(_PublicFixture):
    def test_every_caller_gets_whgmodal_on_the_record_endpoint(self):
        """SG 2026-11-19: the public Record page gets the new modal too. Presence first: each
        caller's answer is ok and has signatures; then each signature carries the block."""
        for who, user in (("beta", self.beta), ("anon", None), ("plain", self.plain)):
            t = FakeTransport(AREA, AREA)
            j = self.env(self.A.pk, transport=t, user=user).json() if user else self.env(self.A.pk, transport=t).json()
            self.assertEqual(j["status"], "ok", who)
            self.assertGreaterEqual(len(j["signatures"]), 1, who)
            self.assertTrue(all(s["whgmodal"]["schema"] == "whgmodal/0-provisional" for s in j["signatures"]), who)
            self.assertNotIn("raw", json.dumps(j), who)                         # the raw EDOPS response is not echoed
            self.assertNotIn("geom_wkt", json.dumps(j["signatures"][0]["whgmodal"]), who)

    def test_locus_basin_whgmodal_has_series_and_no_whg_names(self):
        self.client.force_login(self.beta)
        t = FakeTransport(BASIN_T8)
        with patch("api.edop_client._requests_transport", side_effect=t):
            r = self.client.post("/atlas/locus/environment/", json.dumps(MEMBERS), content_type="application/json")
        self.assertEqual(r.status_code, 200)
        (sig,) = r.json()["signatures"]
        w = sig["whgmodal"]
        self.assertEqual(w["scope"]["type"], "basin")
        self.assertEqual(len(w["series"]["monthly"]["precipitation_mm"]), 12)
        self.assertNotIn("Somewhere", json.dumps(w))                           # nothing of WHG's inside it
        self.assertNotIn("gn:1", json.dumps(w))

    def test_locus_stays_beta_only(self):
        self.client.force_login(self.plain)
        r = self.client.post("/atlas/locus/environment/", json.dumps(MEMBERS), content_type="application/json")
        self.assertEqual(r.status_code, 403)
