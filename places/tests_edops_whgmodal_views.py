"""The beta-only ``whgmodal`` block on the environment endpoints (prototype EDOPS modal).

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
    def test_beta_record_signature_carries_whgmodal_but_public_does_not(self):
        t = FakeTransport(AREA, AREA)
        beta = self.env(self.A.pk, transport=t, user=self.beta).json()
        anon = self.env(self.A.pk, transport=t).json()
        self.assertEqual((beta["status"], anon["status"]), ("ok", "ok"))        # both answered (presence) ...
        self.assertTrue(all("whgmodal" in s for s in beta["signatures"]))
        self.assertEqual(beta["signatures"][0]["whgmodal"]["schema"], "whgmodal/0-provisional")
        self.assertFalse(any("whgmodal" in s for s in anon["signatures"]))      # ... only beta gets the block
        plain = self.env(self.A.pk, transport=FakeTransport(AREA), user=self.plain).json()
        self.assertFalse(any("whgmodal" in s for s in plain["signatures"]))     # signed-in non-beta too
        self.assertNotIn("raw", json.dumps(beta))

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
