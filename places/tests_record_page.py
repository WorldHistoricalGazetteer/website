"""place#325 Stage 2 (items 1-2): the beta Record page and its Concordance JSON.

Run locally (never in a prod container):

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test places.tests_record_page --settings=whg.settings_localtest

Every absence asserted here sits beside a presence asserted on the same
response (~/.claude/memory/a-check-that-cannot-fail.md). The CRC gateway is
faked at ``api.crc_client.crc_fetch_places``.
"""
import json
import os
import shutil
import subprocess
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.gis.geos import Point
from django.template.loader import get_template
from django.test import TestCase, override_settings
from django.urls import resolve, reverse

from places import locus_recipe as lr
from places.views_record import default_cluster_url, explore_similar_url, gateway_place_id

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
        # Dataset a is in the search index (the ingest pushed its registry row);
        # dataset b is not. The default-settings locus link depends on it.
        GazetteerRegistryEntry.objects.update_or_create(
            id=f"whg:{cls.ds_a.pk}", defaults=dict(name="Dataset a", namespace="whg",
                                                   entry_class="dataset", status="published"))
        GazetteerRegistryEntry.objects.filter(id=f"whg:{cls.ds_b.pk}").delete()

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
        # Two records sharing a src_id: the indexer disambiguates them, so the
        # plain whg:<ds>:<src_id> form names neither for certain.
        cls.Dup1 = place(cls.ds_a, "Twin", "T1")
        cls.Dup2 = place(cls.ds_a, "Twin", "T1")
        # Links to a private record and to one that does not exist: one status for both.
        cls.Linker = place(cls.ds_b, "Linker", "K1", xy=(7.0, 7.0), links=[
            ("closeMatch", f"whg:{cls.Priv.pk}"), ("closeMatch", f"whg:{cls.Priv.pk + 100000}"),
            ("closeMatch", f"whg:{cls.B.pk}")])

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

    def test_whg_target_private_or_missing_is_unavailable(self):
        d = self.conc(self.Linker, self.beta).json()
        out = {e["identifier"]: e for e in d["outbound"]}
        self.assertEqual(out[f"whg:{self.B.pk}"]["target"]["status"], "resolved")      # a public one resolves
        private = out[f"whg:{self.Priv.pk}"]["target"]
        missing = out[f"whg:{self.Priv.pk + 100000}"]["target"]
        self.assertEqual(private["status"], "unavailable")
        self.assertEqual(private, missing)
        self.assertNotIn("Secret Own", r := repr(d))
        self.assertIn("Record B", r)

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

    # ---- See this record's cluster at default settings ----------------------
    def _default_link(self, html):
        """The href of the secondary link, or None; asserts the primary is there too."""
        self.assertIn('id="explore_similar_header"', html)
        self.assertIn("Explore similar records", html)
        import re
        m = re.search(r'id="default_cluster_link" href="([^"]+)"', html)
        return m.group(1).replace("&amp;", "&") if m else None

    def test_default_cluster_link_is_the_atlas_default_recipe(self):
        html = self.page(self.A, self.beta).content.decode()
        href = self._default_link(html)
        self.assertIsNotNone(href, "an indexed record has the link")
        self.assertIn("See this record's cluster at default settings", html)
        u = urlparse(href)
        self.assertEqual(u.path, reverse("atlas-locus"))
        recipe = lr.decode_recipe_token(parse_qs(u.query)["r"][0])
        self.assertEqual(recipe["members"], [f"whg:{self.ds_a.pk}:A1"])
        self.assertEqual(recipe["retrieval"], {"query": "Record/A", "mode": "in", "size": 100})
        from search.locus import ATLAS_DEFAULT_CLUSTERING
        self.assertEqual(recipe["clustering"], lr.parse_recipe(
            {"v": 1, "retrieval": recipe["retrieval"], "clustering": ATLAS_DEFAULT_CLUSTERING,
             "members": recipe["members"]})["clustering"])
        self.assertNotIn("stoplist", recipe["clustering"])
        # The locus page accepts it (200), as it would any card's token.
        r = self.client.get(href)
        self.assertEqual(r.status_code, 200)
        self.assertIn(f'data-member="whg:{self.ds_a.pk}:A1"', r.content.decode())

    @override_settings(CRC_GATEWAY_URL="http://gateway.invalid")
    def test_default_retrieval_is_what_the_atlas_sends_for_the_name(self):
        """The recipe's retrieval canonicalises to the body the REAL crc_client
        builder sends for the Atlas's default Places-search options (what
        atlas.js gatherToponymOptions sends with nothing set) for this title."""
        from api import crc_client
        sent = []

        def fake_post(url, json=None, headers=None, timeout=None):
            sent.append(json)

            class R:
                status_code = 200
                text = "{}"

                def json(self):
                    return {"hits": [], "total": 0}
            return R()

        atlas_defaults = {   # gatherToponymOptions with no filter, viewport off, temporal off
            "qstr": self.A.title, "idx": "whg", "fclasses": "", "types": [], "temporal": False,
            "start": 800, "end": 1800, "undated": False, "mode": "in", "cluster": True,
            "bounds": {"type": "GeometryCollection", "geometries": []}, "regions": [], "countries": [],
            "userareas": [], "spatial": "none", "aat_types": [],
        }
        with patch.object(crc_client, "_is_enabled", return_value=True), \
                patch("api.crc_client.requests.post", side_effect=fake_post):
            crc_client.crc_search_status(atlas_defaults, user=self.beta)
        self.assertEqual(len(sent), 1)
        recipe = lr.decode_recipe_token(parse_qs(urlparse(default_cluster_url(self.A)).query)["r"][0])
        from_body = lr.parse_recipe({**recipe, "retrieval": lr.retrieval_from_search_body(sent[0])})
        self.assertEqual(from_body["retrieval"], recipe["retrieval"])
        # ...and the comparison can fail: a different mode is a different retrieval.
        other = lr.parse_recipe({**recipe, "retrieval": lr.retrieval_from_search_body({**sent[0], "mode": "exact"})})
        self.assertNotEqual(other["retrieval"], recipe["retrieval"])

    def test_default_cluster_link_omitted_where_it_cannot_resolve(self):
        self.assertIsNotNone(gateway_place_id(self.A))
        # dataset not in the index (no registry row): no link, primary still there
        html_b = self.page(self.B, self.beta).content.decode()
        self.assertIsNone(self._default_link(html_b))
        self.assertIsNone(gateway_place_id(self.B))
        self.assertNotIn("default_cluster_link", html_b)
        # a src_id shared within the dataset: no link
        self.assertIsNone(gateway_place_id(self.Dup1))
        self.assertIsNone(self._default_link(self.page(self.Dup1, self.beta).content.decode()))
        # no src_id, or a src_id that is not a place id (a space): no link
        from places.models import Place
        Place.objects.filter(pk=self.Lonely.pk).update(src_id="has space")
        self.Lonely.refresh_from_db()
        self.assertIsNone(default_cluster_url(self.Lonely))
        Place.objects.filter(pk=self.Lonely.pk).update(src_id="")
        self.Lonely.refresh_from_db()
        self.assertIsNone(gateway_place_id(self.Lonely))
        # an embargoed (not publicly visible) registry row counts as not indexed
        from api.models import GazetteerRegistryEntry
        GazetteerRegistryEntry.objects.filter(id=f"whg:{self.ds_a.pk}").update(status="embargoed")
        self.assertIsNone(gateway_place_id(self.A))
        GazetteerRegistryEntry.objects.filter(id=f"whg:{self.ds_a.pk}").update(status="published")
        self.assertIsNotNone(self._default_link(self.page(self.A, self.beta).content.decode()))

    def test_atlas_default_literals_match_the_js_module(self):
        """search.locus.ATLAS_DEFAULT_* equal what atlas_cluster_defaults.js exports
        (the module atlas.js seeds its controls from), read by running it under Node."""
        node = shutil.which("node")
        if not node:
            self.skipTest("node is not available")
        mod = os.path.join(settings.BASE_DIR, "whg", "webpack", "js", "atlas_cluster_defaults.js")
        script = (f"const m = await import({json.dumps('file://' + mod)});"
                  "console.log(JSON.stringify({c: m.ATLAS_DEFAULT_CLUSTERING, mode: m.ATLAS_DEFAULT_MATCH_MODE,"
                  " theta: m.ATLAS_DEFAULT_THETA}));")
        out = subprocess.run([node, "--input-type=module", "-e", script], capture_output=True, text=True, timeout=60)
        self.assertEqual(out.returncode, 0, out.stderr)
        js = json.loads(out.stdout.strip().splitlines()[-1])
        from search.locus import ATLAS_DEFAULT_CLUSTERING, ATLAS_DEFAULT_MATCH_MODE
        self.assertEqual(js["c"], ATLAS_DEFAULT_CLUSTERING)
        self.assertEqual(js["mode"], ATLAS_DEFAULT_MATCH_MODE)
        self.assertEqual(js["theta"], ATLAS_DEFAULT_CLUSTERING["theta"])
        # ...and atlas.js takes its defaults from that module rather than its own literals.
        with open(os.path.join(settings.BASE_DIR, "whg", "webpack", "js", "atlas.js"), encoding="utf-8") as f:
            atlas = f.read()
        self.assertIn("from './atlas_cluster_defaults.js'", atlas)
        self.assertNotIn("const ATLAS_DEFAULT_THETA =", atlas)
        self.assertIn("let searchMatchMode = ATLAS_DEFAULT_MATCH_MODE;", atlas)

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
