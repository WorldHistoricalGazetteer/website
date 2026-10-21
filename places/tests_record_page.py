"""place#325 Stage 2 (items 1-2): the beta Record page and its Concordance JSON.

Run locally (never in a prod container):

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test places.tests_record_page --settings=whg.settings_localtest

Every absence asserted here sits beside a presence asserted on the same
response (~/.claude/memory/a-check-that-cannot-fail.md). The CRC gateway is
faked at ``api.crc_client.crc_fetch_places``.
"""
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.gis.geos import Point
from django.template.loader import get_template
from django.test import TestCase
from django.urls import resolve, reverse

from places.views_record import explore_similar_url

GATEWAY_DB = {
    "gn:100": {"place_id": "gn:100", "title": "Gateway Town", "namespace": "gn",
               "repr_point": [10.0, 20.0], "geometries": [{"repr_point": [10.0, 20.0]}]},
}


def fake_fetch(ids, user=None, allow_anonymous=False, meta=None):
    return {i: GATEWAY_DB[i] for i in ids if i in GATEWAY_DB}


def failing_fetch(ids, user=None, allow_anonymous=False, meta=None):
    if meta is not None:
        meta["error"] = "timeout"
    return {}


class RecordPageTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        from api.models import GazetteerRegistryEntry
        from datasets.models import Dataset
        from places.models import (Place, PlaceDepiction, PlaceDescription, PlaceGeom, PlaceLink,
                                   PlaceRelated)

        User = get_user_model()
        mk = User.objects.create_user
        cls.owner = mk(username="r_owner", email="r_owner@example.org", password="pw",
                       given_name="R", surname="Owner", role="beta_tester")
        cls.beta = mk(username="r_beta", email="r_beta@example.org", password="pw",
                      given_name="R", surname="Beta", role="beta_tester")
        cls.plain = mk(username="r_plain", email="r_plain@example.org", password="pw",
                       given_name="R", surname="Plain")

        def ds(key, public=True):
            return Dataset.objects.create(owner=cls.owner, label=f"r325_{key}", title=f"Dataset {key}",
                                          description="D", creator="X", public=public, ds_status="indexed",
                                          uri_base="https://example.org/places/")

        def place(d, title, src_id, xy=None, links=()):
            p = Place.objects.create(title=title, src_id=src_id, dataset=d, ccodes=[])
            if xy:
                PlaceGeom.objects.create(place=p, geom=Point(*xy, srid=4326),
                                         jsonb={"type": "Point", "coordinates": list(xy)})
            for t, ident in links:
                PlaceLink.objects.create(place=p, jsonb={"type": t, "identifier": ident})
            return p

        with patch("datasets.signals.doi"):
            cls.ds_a = ds("a")
            cls.ds_b = ds("b")
            cls.ds_priv = ds("priv", public=False)
        GazetteerRegistryEntry.objects.update_or_create(
            id="gn", defaults=dict(name="gn", namespace="gn", entry_class="authority", status="published"))
        GazetteerRegistryEntry.objects.filter(namespace__in=["viaf"]).delete()

        cls.A = place(cls.ds_a, "Record/A", "A1", xy=(0.0, 0.0), links=[
            ("exactMatch", "gn:100"),
            ("closeMatch", "viaf:55"),
        ])
        PlaceDescription.objects.create(place=cls.A, jsonb={"value": "A town by a river.", "lang": "en"})
        PlaceDepiction.objects.create(place=cls.A, jsonb={"@id": "https://img.example.org/a.jpg",
                                                          "title": "A view of A", "license": "CC0"})
        PlaceDepiction.objects.create(place=cls.A, jsonb={"@id": "javascript:alert(1)", "title": "Evil"})
        PlaceRelated.objects.create(place=cls.A, jsonb={"relationType": "gvp:broaderPartitive",
                                                        "relationTo": "https://example.org/r/1",
                                                        "label": "Bigger Region"})
        PlaceRelated.objects.create(place=cls.A, jsonb={"relationType": "gvp:broaderPartitive",
                                                        "relationTo": "javascript:alert(2)",
                                                        "label": "Sly Region"})
        cls.B = place(cls.ds_b, "Record B", "B1", xy=(0.1, 0.1), links=[("closeMatch", f"whg:{cls.A.pk}")])
        cls.C = place(cls.ds_b, "Record C", "C1", xy=(5.0, 5.0), links=[("closeMatch", "gn:100")])
        cls.Lonely = place(cls.ds_a, "Lonely", "L1", xy=(2.0, 2.0))
        cls.Priv = place(cls.ds_priv, "Secret Own", "S1", xy=(3.0, 3.0))

    def page(self, place, user=None):
        if user is not None:
            self.client.force_login(user)
        return self.client.get(reverse("atlas-record", args=[place.pk]))

    def conc(self, place, user=None, fetch=fake_fetch):
        if user is not None:
            self.client.force_login(user)
        with patch("api.crc_client.crc_fetch_places", side_effect=fetch):
            return self.client.get(reverse("atlas-record-concordance", args=[place.pk]))

    # ---- routes ----------------------------------------------------------
    def test_routes_sit_under_atlas_place(self):
        self.assertEqual(reverse("atlas-record", args=[7]), "/atlas/place/7/")
        self.assertEqual(reverse("atlas-record-concordance", args=[7]), "/atlas/place/7/concordance/")
        # the existing query-string JSON endpoint is still the Atlas portal's
        self.assertEqual(resolve("/atlas/place/").url_name, "atlas-place")

    # ---- the beta gate ---------------------------------------------------
    def test_gate(self):
        r = self.page(self.A)
        self.assertEqual(r.status_code, 302)
        self.assertIn("login", r["Location"])
        self.assertEqual(self.conc(self.A).status_code, 403)
        self.assertEqual(self.page(self.A, self.plain).status_code, 404)
        self.assertEqual(self.conc(self.A, self.plain).status_code, 403)
        self.assertEqual(self.page(self.A, self.beta).status_code, 200)
        self.assertEqual(self.conc(self.A, self.beta).status_code, 200)

    def test_private_record(self):
        # a stranger (beta) is told nothing: the same 404 as no such record
        self.assertEqual(self.page(self.Priv, self.beta).status_code, 404)
        self.assertEqual(self.conc(self.Priv, self.beta).status_code, 404)
        self.assertEqual(self.page(self.A, self.beta).status_code, 200)
        # the owner sees the record, and is told it has no concordance (not an empty one)
        self.assertEqual(self.page(self.Priv, self.owner).status_code, 200)
        r = self.conc(self.Priv, self.owner)
        self.assertEqual(r.status_code, 200)
        self.assertIs(r.json()["public"], False)
        self.assertNotIn("outbound", r.json())
        self.assertIs(self.conc(self.A, self.owner).json()["public"], True)

    # ---- the page is fast: it never computes the concordance ---------------
    def test_page_does_not_build_concordance(self):
        with patch("places.concordance.build_concordance", side_effect=AssertionError("page computed it")):
            r = self.page(self.A, self.beta)
        self.assertEqual(r.status_code, 200)
        html = r.content.decode()
        self.assertIn('id="concordance_status"', html)
        self.assertIn('data-endpoint="/atlas/place/%d/concordance/"' % self.A.pk, html)
        self.assertIn("atlas_record.bundle.js", html)
        self.assertNotIn("Gateway Town", html)  # nothing resolved server-side

    # ---- what the portal payload carries but portal.js never shows ---------
    def test_payload_fields_rendered(self):
        html = self.page(self.A, self.beta).content.decode()
        self.assertIn("<title>WHG · Record/A (record, beta)</title>", html)
        self.assertIn("A town by a river.", html)                                  # description
        self.assertIn('href="https://img.example.org/a.jpg"', html)                 # depiction
        self.assertIn("A view of A", html)
        self.assertIn('href="https://example.org/r/1"', html)                       # related
        self.assertIn("Bigger Region", html)
        self.assertIn("gn:100", html)                                               # links
        self.assertIn("viaf:55", html)
        # a javascript: URL from contributed data never becomes an href
        self.assertIn("Sly Region", html)
        self.assertNotIn("javascript:alert", html)
        self.assertNotIn("Evil", html)  # a depiction with no usable URL is not shown
        # Django comments did not leak into the page
        self.assertNotIn("{#", html)
        self.assertNotIn("place#325 Stage 2, beta only", html)

    def test_empty_record_page_says_so(self):
        html = self.page(self.Lonely, self.beta).content.decode()
        self.assertIn("This record has no descriptions.", html)
        self.assertIn("This record cites no other identifiers.", html)
        self.assertIn("Explore similar records", html)

    # ---- the concordance JSON ----------------------------------------------
    def test_concordance_tiers_and_aggregate(self):
        r = self.conc(self.A, self.beta)
        d = r.json()
        self.assertEqual(r.status_code, 200)
        self.assertTrue(d["public"])
        self.assertEqual(d["record"]["repr_point"], [0.0, 0.0])
        out = {e["identifier"]: e for e in d["outbound"]}
        self.assertEqual(out["gn:100"]["target"]["status"], "resolved")
        self.assertEqual(out["viaf:55"]["target"]["status"], "not_held")
        self.assertEqual({e["record"]["place_id"] for e in d["inbound"]}, {self.B.pk})
        self.assertEqual({e["record"]["place_id"] for e in d["cocited"]}, {self.C.pk})
        agg = {m["place_id"] for m in d["aggregate"]}
        self.assertEqual(agg, {"gn:100", self.B.pk})  # tier 3 (C) never aggregates
        self.assertIn("imported", d["provenance_labels"])
        self.assertTrue(d["explore_similar"]["url"].startswith("/atlas/"))
        self.assertIsInstance(d["elapsed_ms"], int)

    def test_gateway_failure_is_not_absence(self):
        d = self.conc(self.A, self.beta, fetch=failing_fetch).json()
        out = {e["identifier"]: e for e in d["outbound"]}
        self.assertEqual(out["gn:100"]["target"]["status"], "gateway_unavailable")
        self.assertEqual(out["viaf:55"]["target"]["status"], "not_held")
        self.assertNotIn("gn:100", {m["place_id"] for m in d["aggregate"]})
        self.assertIn(self.B.pk, {m["place_id"] for m in d["aggregate"]})

    def test_empty_concordance(self):
        d = self.conc(self.Lonely, self.beta).json()
        self.assertTrue(d["public"])
        self.assertEqual((d["outbound"], d["inbound"], d["cocited"], d["aggregate"]), ([], [], [], []))
        self.assertTrue(self.conc(self.A, self.beta).json()["outbound"])

    # ---- Explore similar records -------------------------------------------
    def test_explore_similar_url(self):
        e = explore_similar_url(self.A)
        self.assertEqual(e["seeded_by"], ["name"])
        self.assertEqual(e["url"], "/atlas/Record%20A")
        m = resolve("/atlas/Record A")
        self.assertEqual((m.url_name, m.kwargs["toponym"]), ("atlas-page-toponym", "Record A"))

    def test_template_has_no_multiline_hash_comments(self):
        from django.template.base import TextNode
        t = get_template("places/atlas_record.html").template

        def walk(nl):
            for n in nl:
                if isinstance(n, TextNode) and "{#" in n.s:
                    yield n.s
                for attr in ("nodelist", "nodelist_true", "nodelist_false"):
                    if getattr(n, attr, None):
                        yield from walk(getattr(n, attr))
        self.assertEqual(list(walk(t.nodelist)), [])
