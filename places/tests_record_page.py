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
from places.views_record import default_cluster_link, explore_similar_url, gateway_place_id

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


# A gateway clustering_params block with values no default shares (and, like the
# live one today, no same_ns_penalty), so a recipe that used a default instead of
# the calibrated value would show it.
GATEWAY_PARAMS = {
    "version": 1, "calibrated": True,
    "weights": {"name": 0.1405, "spatial": 0.2903, "temporal": 0.3613, "type": 0.0579, "link": 0.15},
    "thresholds": {"theta_query": 0.21, "theta_bridge": 0.31, "tau_name": 0.71, "tau_link": 0.9},
}
# The default cache is file-based and shared with local dev runs: these tests use
# a private in-memory one (the other aliases unchanged).
LOCMEM = {**settings.CACHES,
          "default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache", "LOCATION": "r325"}}


@override_settings(CACHES=LOCMEM)
class RecordPageTests(TestCase):
    def setUp(self):
        from django.core.cache import cache
        cache.clear()
        # The gateway's clustering_params probe, faked: every render that builds a
        # default-settings link reads it. Tests that need another answer re-patch.
        self.params_calls = []

        def fake_params(user=None, timeout=None):
            self.params_calls.append(timeout)
            return dict(GATEWAY_PARAMS), None
        self._params_patch = patch("api.crc_client.crc_clustering_params", side_effect=fake_params)
        self._params_patch.start()
        self.addCleanup(self._params_patch.stop)

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
    # Assumes the v4 release switch is off: say so, clearing the alias too, so the test
    # holds where the env sets either (dev sets both; place#325 review #5).
    @override_settings(V4_LANDING_ENABLED=False, LOCUS_MINTING_ENABLED=False)
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
        m = re.search(r'<a[^>]*id="default_cluster_link"[^>]*href="([^"]+)"', html)
        return m.group(1).replace("&amp;", "&") if m else None

    @staticmethod
    def _recipe(href):
        return lr.decode_recipe_token(parse_qs(urlparse(href).query)["r"][0])

    def test_default_cluster_link_uses_the_gateway_calibrated_settings(self):
        html = self.page(self.A, self.beta).content.decode()
        href = self._default_link(html)
        self.assertIsNotNone(href, "an indexed record has the link")
        self.assertIn("See this record's cluster at gateway-calibrated settings", html)
        self.assertIn('data-settings="gateway"', html)
        self.assertNotIn("WHG fallback settings", html)
        self.assertIn("fixed default (0.65)", html)           # the tooltip says theta is not the gateway's
        u = urlparse(href)
        self.assertEqual(u.path, reverse("atlas-locus"))
        recipe = self._recipe(href)
        self.assertEqual(recipe["members"], [f"whg:{self.ds_a.pk}:A1"])
        self.assertEqual(recipe["retrieval"], {"query": "Record/A", "mode": "in", "size": 100})
        c = recipe["clustering"]
        self.assertEqual(c["weights"], GATEWAY_PARAMS["weights"])            # the calibrated weights
        self.assertEqual((c["tau_name"], c["tau_link"]), (0.71, 0.9))       # the calibrated thresholds
        from search.locus import ATLAS_DEFAULT_CLUSTERING
        self.assertEqual(c["theta"], ATLAS_DEFAULT_CLUSTERING["theta"])    # NOT theta_query 0.21
        self.assertNotEqual(c["theta"], GATEWAY_PARAMS["thresholds"]["theta_query"])
        self.assertEqual(c["same_ns_penalty"], ATLAS_DEFAULT_CLUSTERING["same_ns_penalty"])   # none shipped
        self.assertIs(c["respect_distinct"], True)
        self.assertNotEqual(c["weights"], ATLAS_DEFAULT_CLUSTERING["weights"])
        self.assertEqual(set(c), {"weights", "theta", "tau_name", "tau_link", "same_ns_penalty",
                                  "respect_distinct"})                      # only what the clusterer reads
        self.assertEqual(self.params_calls, [1.5])                          # one probe, short timeout
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
        recipe = self._recipe(default_cluster_link(self.A, self.beta)["url"])
        from_body = lr.parse_recipe({**recipe, "retrieval": lr.retrieval_from_search_body(sent[0])})
        self.assertEqual(from_body["retrieval"], recipe["retrieval"])
        # ...and the comparison can fail: a different mode is a different retrieval.
        other = lr.parse_recipe({**recipe, "retrieval": lr.retrieval_from_search_body({**sent[0], "mode": "exact"})})
        self.assertNotEqual(other["retrieval"], recipe["retrieval"])

    def test_default_cluster_link_omitted_where_it_cannot_resolve(self):
        self.assertIsNotNone(gateway_place_id(self.A))
        # dataset not in the index (no registry row): no link, primary still there,
        # and the gateway is not even asked for its params
        html_b = self.page(self.B, self.beta).content.decode()
        self.assertEqual(self.params_calls, [])
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
        self.assertIsNone(default_cluster_link(self.Lonely, self.beta))
        Place.objects.filter(pk=self.Lonely.pk).update(src_id="")
        self.Lonely.refresh_from_db()
        self.assertIsNone(gateway_place_id(self.Lonely))
        # an embargoed (not publicly visible) registry row counts as not indexed
        from api.models import GazetteerRegistryEntry
        GazetteerRegistryEntry.objects.filter(id=f"whg:{self.ds_a.pk}").update(status="embargoed")
        self.assertIsNone(gateway_place_id(self.A))
        GazetteerRegistryEntry.objects.filter(id=f"whg:{self.ds_a.pk}").update(status="published")
        self.assertIsNotNone(self._default_link(self.page(self.A, self.beta).content.decode()))

    def _fallback_page(self, failure):
        from django.core.cache import cache
        cache.clear()
        calls = []

        def failing(user=None, timeout=None):
            calls.append(timeout)
            return None, failure
        with patch("api.crc_client.crc_clustering_params", side_effect=failing):
            html = self.page(self.A, self.beta).content.decode()
            html2 = self.page(self.A, self.beta).content.decode()
        return html, html2, calls

    def test_gateway_failure_falls_back_and_says_so(self):
        from search.locus import ATLAS_DEFAULT_CLUSTERING
        for failure in ("timeout", "unreachable", "error"):
            html, html2, calls = self._fallback_page(failure)
            href = self._default_link(html)
            self.assertIsNotNone(href, failure)
            self.assertIn("See this record's cluster at WHG fallback settings", html)
            self.assertIn('data-settings="fallback"', html)
            self.assertIn(f'data-fallback-reason="{failure}"', html)
            self.assertNotIn("gateway-calibrated settings", html)
            c = self._recipe(href)["clustering"]
            self.assertEqual(c["weights"], ATLAS_DEFAULT_CLUSTERING["weights"])
            self.assertEqual(c["theta"], ATLAS_DEFAULT_CLUSTERING["theta"])
            self.assertNotEqual(c["weights"], GATEWAY_PARAMS["weights"])
            # the failure is cached briefly: the second render did not ask again
            self.assertEqual(calls, [1.5], failure)
            self.assertIn("WHG fallback settings", html2)
        # ...and with the gateway back (fresh cache) the calibrated label returns
        from django.core.cache import cache
        cache.clear()
        self.assertIn("gateway-calibrated settings", self.page(self.A, self.beta).content.decode())

    def test_incomplete_or_invalid_gateway_params_fall_back(self):
        from django.core.cache import cache
        for params, reason in (({"weights": {"name": 0.3}}, "incomplete"),
                               ({**GATEWAY_PARAMS, "weights": {**GATEWAY_PARAMS["weights"], "name": 4.0}}, "invalid")):
            cache.clear()
            with patch("api.crc_client.crc_clustering_params", return_value=(params, None)):
                link = default_cluster_link(self.A, self.beta)
            self.assertEqual((link["source"], link["reason"]), ("fallback", reason))
        cache.clear()
        self.assertEqual(default_cluster_link(self.A, self.beta)["source"], "gateway")

    def test_cache_hit_makes_no_second_gateway_call(self):
        from django.core.cache import cache
        a = self._default_link(self.page(self.A, self.beta).content.decode())
        lonely = self._default_link(self.page(self.Lonely, self.beta).content.decode())
        self.assertIsNotNone(a)
        self.assertIsNotNone(lonely)
        self.assertEqual(len(self.params_calls), 1)               # two renders, one probe
        cache.clear()
        self.page(self.A, self.beta)
        self.assertEqual(len(self.params_calls), 2)               # the count is live: a miss asks again

    def test_probe_is_short_and_reads_clustering_params(self):
        """api.crc_client.crc_clustering_params itself (unpatched): the probe body,
        the short timeout, success, timeout and the anonymous/disabled answer."""
        import requests as _rq
        probe = self._params_patch.temp_original     # the real function, not setUp's fake
        self.assertEqual(probe.__module__, "api.crc_client")
        seen = []

        class R:
            status_code = 200

            def json(self):
                return {"hits": [], "clustering_params": GATEWAY_PARAMS}

        def ok(url, json=None, headers=None, timeout=None):
            seen.append((url, json, timeout))
            return R()
        with override_settings(CRC_GATEWAY_URL="http://gateway.invalid"):
            with patch("api.crc_client.requests.post", side_effect=ok):
                self.assertEqual(probe(self.beta), (GATEWAY_PARAMS, None))
                self.assertEqual(probe(None), (None, "disabled"))
            self.assertEqual(len(seen), 1)                          # disabled never called out
            url, body, timeout = seen[0]
            self.assertTrue(url.endswith("/api/search"))
            self.assertEqual((body["size"], body["include_clustering_fields"]), (1, True))
            self.assertLessEqual(max(timeout), 1.5)
            with patch("api.crc_client.requests.post", side_effect=_rq.Timeout("slow")):
                self.assertEqual(probe(self.beta), (None, "timeout"))

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
