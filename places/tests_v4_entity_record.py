"""place#325 ruling 2026-10-21: ``/entity/place:<pid>/`` RENDERS the Record page at
the v4 release, behind the release switch, content-negotiated. Run locally only:

    DB_NAME=whgv3beta_f2 GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test places.tests_v4_entity_record --settings=whg.settings_localtest

On: HTML is the Record page (the places.views_record view, not a fork) served at
the entity URI, with ``<link rel="canonical">`` naming that URI; JSON-LD / JSON is
a small machine representation of the record plus its concordance summary
(``places.concordance`` output). ``/atlas/place/<pk>/`` keeps working and its
canonical names the entity URI too. Off: nothing changes (the legacy 302, and
the beta preview's noindex). Private, embargoed and missing stay one 404
(places/tests_v4_entity_271.py), and ``/api`` stays closed anonymously.

Proved able to fail (2026-10-21): with the render branch in
``EntityDetailView.get`` removed (the old 302 to /atlas/place/ restored),
``test_html_is_the_record_page_at_the_entity_uri`` fails; with the canonical
tag returning "" when on, ``test_canonical_link_names_the_entity_uri`` fails.
"""
import json
from unittest.mock import patch

from places.tests_v4_landing import OFF, ON, _Fixture


class _Gatewayless(_Fixture):
    @classmethod
    def setUpTestData(cls):
        super().setUpTestData()
        from places.models import PlaceLink
        PlaceLink.objects.create(place=cls.Pub, jsonb={"type": "closeMatch", "identifier": "gn:2950159"})

    def setUp(self):
        super().setUp()
        p = patch("api.crc_client.crc_fetch_places", return_value={})
        p.start()
        self.addCleanup(p.stop)

    def canonical(self, pk):
        return f'<link rel="canonical" href="http://testserver/entity/place:{pk}/">'


@ON
class EntityRecordOnTests(_Gatewayless):

    def test_html_is_the_record_page_at_the_entity_uri(self):
        for ident in (str(self.Pub.pk), f"whg:{self.Pub.pk}", f"whg:{self.ds_pub.pk}:P1"):
            r = self.anon(f"/entity/place:{ident}/", HTTP_ACCEPT="text/html,application/xhtml+xml,*/*;q=0.8")
            self.assertEqual(r.status_code, 200, ident)
            self.assertTrue(r["Content-Type"].startswith("text/html"), ident)
            self.assertIn("Accept", r["Vary"])
            html = r.content.decode()
            self.assertIn('id="atlas_record"', html)                # the Record page's own template
            self.assertIn("Public Town", html)
            self.assertIn(self.canonical(self.Pub.pk), html, ident)  # every spelling names the pk URI
            self.assertNotIn('content="noindex', html)
        # Signed-in callers get the same page; the owner sees their private record.
        r = self.as_user(self.owner, f"/entity/place:{self.Priv.pk}/", HTTP_ACCEPT="text/html")
        self.assertEqual(r.status_code, 200)
        self.assertIn("Secret Town", r.content.decode())

    def test_json_is_a_small_honest_record_with_its_concordance(self):
        for accept, ctype in (("application/ld+json", "application/ld+json"), ("application/json", "application/json"),
                              ("*/*", "application/json")):
            r = self.anon(f"/entity/place:{self.Pub.pk}/", HTTP_ACCEPT=accept)
            self.assertEqual(r.status_code, 200, accept)
            self.assertTrue(r["Content-Type"].startswith(ctype), (accept, r["Content-Type"]))
            self.assertIn("Accept", r["Vary"])
        doc = json.loads(self.anon(f"/entity/place:{self.Pub.pk}/", HTTP_ACCEPT="application/ld+json").content)
        self.assertEqual(doc["@id"], f"http://testserver/entity/place:{self.Pub.pk}/")
        self.assertEqual(doc["@type"], "Record")
        self.assertEqual(doc["title"], "Public Town")
        self.assertEqual(doc["dataset"]["label"], self.ds_pub.label)
        self.assertIn("one contributor's account", doc["statement"])
        self.assertTrue(doc["lpf"].endswith(f"/entity/place:{self.Pub.pk}/api"))
        conc = doc["concordance"]
        self.assertEqual([e["identifier"] for e in conc["outbound"]], ["gn:2950159"])
        self.assertEqual(conc["outbound"][0]["link_type"], "closeMatch")
        self.assertTrue(conc["outbound"][0]["aggregates"])
        self.assertIn("provenance", conc["outbound"][0])
        self.assertEqual(conc["inbound"], [])
        self.assertEqual(conc["aggregated_types"], ["exactMatch", "closeMatch"])
        self.assertNotIn("Secret Town", json.dumps(doc))

    def test_private_embargoed_missing_are_one_404_in_both_representations(self):
        self.client.raise_request_exception = False
        for accept in ("text/html", "application/ld+json"):
            answers = {}
            for ident in (str(self.Priv.pk), str(self.Emb.pk), str(self.missing)):
                r = self.anon(f"/entity/place:{ident}/", HTTP_ACCEPT=accept)
                answers.setdefault((r.status_code, r.content.replace(ident.encode(), b"<id>")), []).append(ident)
            self.assertEqual(len(answers), 1, (accept, answers))
            self.assertEqual(next(iter(answers))[0], 404)
            self.assertEqual(self.anon(f"/entity/place:{self.Pub.pk}/", HTTP_ACCEPT=accept).status_code, 200)

    def test_canonical_link_names_the_entity_uri(self):
        r = self.anon(f"/atlas/place/{self.Pub.pk}/")                  # the beta route keeps working
        self.assertEqual(r.status_code, 200)
        self.assertIn(self.canonical(self.Pub.pk), r.content.decode())

    def test_authority_ids_and_api_are_unchanged(self):
        r = self.anon("/entity/place:gn:1/", HTTP_ACCEPT="text/html")
        self.assertEqual((r.status_code, r["Location"]), (302, "/entity/place:gn:1/api"))
        self.assertEqual(self.anon(f"/entity/place:{self.Pub.pk}/api").status_code, 401)
        self.assertEqual(self.as_user(self.plain, f"/entity/place:{self.Pub.pk}/api").status_code, 200)


@ON
class EntityRecordThrottleTests(_Gatewayless):
    """Review #6: the anonymous Record PAGE is not held to the 60/min/IP identifier-
    resolution throttle (shared-NAT visitors would get a DRF 429 JSON instead of a page);
    it has its own, higher rate. The JSON branch keeps the resolution throttle."""

    def test_html_has_its_own_higher_limit_and_json_keeps_the_old_one(self):
        from api.views_entity import EntityRecordPageAnonThrottle, EntityResolveAnonThrottle
        url = f"/entity/place:{self.Pub.pk}/"
        with patch.object(EntityResolveAnonThrottle, "rate", "2/min"):
            html = [self.anon(url, HTTP_ACCEPT="text/html").status_code for _ in range(4)]
            js = [self.anon(url, HTTP_ACCEPT="application/json").status_code for _ in range(3)]
        self.assertEqual(html, [200, 200, 200, 200])
        self.assertEqual(js, [200, 200, 429])
        from django.core.cache import cache
        cache.clear()
        with patch.object(EntityRecordPageAnonThrottle, "rate", "2/min"):   # the page's own limit exists
            html = [self.anon(url, HTTP_ACCEPT="text/html").status_code for _ in range(3)]
        self.assertEqual(html, [200, 200, 429])
        self.assertGreater(int(EntityRecordPageAnonThrottle.rate.split("/")[0]),
                           int(EntityResolveAnonThrottle.rate.split("/")[0]))


@OFF
class EntityRecordOffTests(_Gatewayless):

    def test_off_nothing_changes(self):
        self.assertEqual(self.anon(f"/entity/place:{self.Pub.pk}/").status_code, 401)
        for accept in ("text/html", "application/json"):
            r = self.as_user(self.plain, f"/entity/place:{self.Pub.pk}/", HTTP_ACCEPT=accept)
            self.assertEqual((r.status_code, r["Location"]), (302, f"/places/portal/{self.Pub.pk}/"), accept)
        # The beta preview: noindex, and no canonical (the entity URI is not the record page yet).
        html = self.as_user(self.beta, f"/atlas/place/{self.Pub.pk}/").content.decode()
        self.assertIn("Public Town", html)
        self.assertIn('<meta name="robots" content="noindex, nofollow">', html)
        self.assertNotIn('rel="canonical"', html)
