"""place#325 Stage 3: the gloss and EDOPS sections, driven in HEADLESS Chromium (Playwright).

Opt-in, because it starts a browser and a live server:

    WHG_BROWSER_TESTS=1 GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test places.tests_enrichment_browser --settings=whg.settings_localtest

``WHG_BROWSER_SHOTS=<dir>`` keeps screenshots. Playwright's bundled chromium with a fresh
profile (never the user's Chrome, never a visible window); the COMMITTED bundles are
served. EDOPS is never called: ``api.edop_client._requests_transport`` is replaced by a
fake serving the recorded fixtures (same process, so it reaches the server thread), and
the CRC gateway is faked as in the Stage 2 browser tests. Every absence is asserted beside
a presence in the same evaluation, and ``test_harness_can_fail`` runs the same checks with
the page bundle blocked and requires every one of them to fail.
"""
import json
import os
import unittest
from unittest.mock import patch

from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.gis.geos import Point
from django.test import Client, LiveServerTestCase, override_settings

from api.tests_edop_client import AREA, BASIN_T, BASIN_T8, FakeTransport
# place#325 Stage 5: WHG_RECORD_BUNDLE_DIR serves the Record page bundle from a scratch build.
from places.tests_record_layout_browser import BUNDLE_DIR, bundle_route
# ...and WHG_LOCUS_BUNDLE_DIR the Locus page bundle (search/tests_locus_layout_support.py).
from search.tests_locus_layout_support import LOCUS_BUNDLE_DIR, _bundle_route as locus_bundle_route

RUN = os.environ.get("WHG_BROWSER_TESTS") == "1"
SHOTS = os.environ.get("WHG_BROWSER_SHOTS")
BLOCKED_HOSTS = ("analytics.whgazetteer.org", "errors.whgazetteer.org", "plausible", "computingplace.org")
FIXTURE_STYLE = {"version": 8, "name": "fixture", "sources": {}, "metadata": {"whg:backgroundcolour": "#ddeeff"},
                 "layers": [{"id": "fixture-bg", "type": "background", "paint": {"background-color": "#ddeeff"}}]}
GATEWAY_DB = {"gn:100": {"place_id": "gn:100", "title": "Gateway Town", "namespace": "gn",
                         "repr_point": [0.2, 51.4], "names": [{"label": "Gateway Town"}], "types": [{"label": "P"}],
                         "ccodes": ["GB"], "geometries": [{"type": "Point", "coordinates": [0.2, 51.4]}]}}
LOCMEM = {**settings.CACHES, "default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache",
                                         "LOCATION": "enr325"}}


def fake_fetch(ids, user=None, allow_anonymous=False, meta=None):
    return {i: GATEWAY_DB[i] for i in ids if i in GATEWAY_DB}


@unittest.skipUnless(RUN, "set WHG_BROWSER_TESTS=1 to drive the enrichment sections in headless Chromium")
@override_settings(CACHES=LOCMEM, EDOP_ENABLED=True)
class EnrichmentBrowserTests(LiveServerTestCase):
    @classmethod
    def setUpClass(cls):
        os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
        super().setUpClass()
        from playwright.sync_api import sync_playwright
        cls._pw = sync_playwright().start()
        cls.browser = cls._pw.chromium.launch(headless=True)

    @classmethod
    def tearDownClass(cls):
        try:
            cls.browser.close()
            cls._pw.stop()
        finally:
            super().tearDownClass()

    def setUp(self):
        from api.models import GazetteerRegistryEntry
        from datasets.models import Dataset
        from places.models import Place, PlaceGeom, PlaceLink, PlaceName
        from django.core.cache import cache

        cache.clear()
        User = get_user_model()
        self.beta = User.objects.create_user(username="enb_beta", email="enb@example.org", password="pw",
                                             given_name="E", surname="B", role="beta_tester")
        with patch("datasets.signals.doi"):
            a = Dataset.objects.create(owner=self.beta, label="enb_a", title="Dataset a", description="D",
                                       creator="X", public=True, ds_status="indexed",
                                       uri_base="https://example.org/places/")
        GazetteerRegistryEntry.objects.update_or_create(
            id="gn", defaults=dict(name="GeoNames", namespace="gn", entry_class="authority", status="published"))
        # wd too: the locus cluster has a wd member, and only a namespace an anonymous
        # caller can see may be sent to EDOPS (places.record_group.mark_offsite).
        GazetteerRegistryEntry.objects.update_or_create(
            id="wd", defaults=dict(name="Wikidata", namespace="wd", entry_class="authority", status="published"))

        def place(title, src, xy, links=(), minmax=None):
            p = Place.objects.create(title=title, src_id=src, dataset=a, ccodes=["GB"], minmax=minmax)
            PlaceGeom.objects.create(place=p, geom=Point(*xy, srid=4326), jsonb={"type": "Point", "coordinates": list(xy)})
            PlaceName.objects.create(place=p, toponym=title, jsonb={"toponym": title})
            for t, i in links:
                PlaceLink.objects.create(place=p, jsonb={"type": t, "identifier": i})
            return p
        self.A = place("Record A", "A1", (-0.1278, 51.5074), links=[("exactMatch", "gn:100")], minmax=[1600, 1700])
        self.Lonely = place("Lonely", "L1", (-0.1278, 51.5074), minmax=[1600, 1700])
        pp = patch("api.crc_client.crc_clustering_params", return_value=(None, "unreachable"))
        pp.start()
        self.addCleanup(pp.stop)

    def new_page(self, block=None, user=None, anonymous=False):
        ctx = self.browser.new_context(viewport={"width": 1280, "height": 1000})
        if not anonymous:
            c = Client()
            c.force_login(user or self.beta)
            ctx.add_cookies([{"name": settings.SESSION_COOKIE_NAME,
                              "value": c.cookies[settings.SESSION_COOKIE_NAME].value, "url": self.live_server_url}])
        # Closed before the live server's teardown flush, so no in-flight request of
        # this page races the table truncation.
        self.addCleanup(ctx.close)
        page = ctx.new_page()
        self.external = []

        def route(r):
            url = r.request.url
            if "computingplace.org" in url:
                self.external.append(url)       # the page must never call EDOPS itself
            if any(h in url for h in BLOCKED_HOSTS) or (block and block in url):
                return r.abort()
            if BUNDLE_DIR and "/atlas_record.bundle." in url:
                return bundle_route(r, r.request)
            if LOCUS_BUNDLE_DIR and "/static/webpack/locus.bundle." in url:
                return locus_bundle_route(r, r.request)
            if "/styles/" in url and url.endswith("style.json"):
                return r.fulfill(status=200, content_type="application/json",
                                 headers={"Access-Control-Allow-Origin": "*"}, body=json.dumps(FIXTURE_STYLE))
            return r.continue_()
        page.route("**/*", route)
        self.errors = []
        page.on("pageerror", lambda e: self.errors.append(str(e)))
        return page

    def wait_for(self, page, expr, ms):
        try:
            page.wait_for_function(expr, timeout=ms)
            return True
        except Exception:  # noqa: BLE001 — a timeout is a finding
            return False

    def shot(self, page, name, selector=None):
        if SHOTS:
            os.makedirs(SHOTS, exist_ok=True)
            if selector:
                page.locator(selector).screenshot(path=os.path.join(SHOTS, f"{name}.png"))
            else:
                page.screenshot(path=os.path.join(SHOTS, f"{name}.png"), full_page=True)

    # Stage 5: the environment is an on-demand card; EDOPS is asked only on a click, and
    # the answer is shown in a modal (.enr-env-modal). ENV is where its text is read.
    ENV = ".enr-env-modal .modal-body"

    def show_environment(self, page, wait_ms=30000):
        """Click "Show environment" (after checking nothing was fetched before it) and
        wait for the answer. ``idle_before``: the environment state before the click."""
        idle = page.evaluate("""() => !!(window.__whgEnrichment && window.__whgEnrichment.environment === 'idle'
            && !(window.__whgEnrichment.environmentFetches) && !document.querySelector('.enr-env-modal'))""")
        try:
            page.click("#record_environment .enr-env-show", timeout=min(wait_ms, 10000))
        except Exception:  # noqa: BLE001 — no button is a finding (the checks fail)
            return idle, False
        return idle, self.wait_for(page, "window.__whgEnrichment && window.__whgEnrichment.environment === 'ready'",
                                   wait_ms)

    # Stage 5, Locus page: the gloss is "About this cluster" (#locus_about), and the
    # environment card sits in #locus_environment_slot; EDOPS only on its button.
    LOCUS_GLOSS = "#locus_about .locus-about-body"

    def locus_show_environment(self, page, wait_ms=45000):
        """Wait for the gloss, check nothing environmental was fetched, click, wait.
        Returns (gloss_ready, idle_before_click, environment_ready)."""
        gloss = self.wait_for(page, "window.__whgEnrichment && window.__whgEnrichment.gloss === 'ready'", wait_ms)
        idle = page.evaluate("""() => !!(window.__whgEnrichment && window.__whgEnrichment.environment === 'idle'
            && !(window.__whgEnrichment.environmentFetches))""")
        try:
            page.click("#locus_environment_slot .enr-env-show", timeout=10000)
        except Exception:  # noqa: BLE001 — no button is a finding
            return gloss, idle, False
        return gloss, idle, self.wait_for(page, "window.__whgEnrichment.environment === 'ready'", wait_ms)

    def record_checks(self, page, wait_ms=30000):
        r = {}
        r["gloss_ready"] = self.wait_for(page, "window.__whgEnrichment && window.__whgEnrichment.gloss === 'ready'", wait_ms)
        r["env_idle_before_click"], r["env_ready"] = self.show_environment(page, wait_ms)
        d = page.evaluate("""(env) => {
            const t = (s) => (document.querySelector(s) || {}).innerText || '';
            const a = document.querySelector(env + ' .enr-attribution a');
            return {gloss: t('#record_gloss .enr-body'), env: t(env),
                    chips: Array.from(document.querySelectorAll(env + ' .enr-chip')).map(e => e.innerText),
                    bands: Array.from(document.querySelectorAll(env + ' .enr-band')).map(e => e.dataset.band),
                    attr: a && a.getAttribute('href'),
                    status: window.__whgEnrichment && window.__whgEnrichment.environmentStatus};
        }""", self.ENV)
        r["gloss_text"] = ("This record and 1 attested equivalent" in d["gloss"] and "GeoNames" in d["gloss"]
                           and "Not known from these records" in d["gloss"] and "Record C" not in d["gloss"])
        r["env_area_scope"] = ("an area" in d["env"] and "convex hull" in d["env"] and "one basin" not in d["env"])
        r["env_years_reason"] = "area scope takes a single year" in d["env"]
        r["env_chips"] = any("Temperate Broadleaf" in c for c in d["chips"]) and d["status"] == "ok"
        r["env_bands"] = d["bands"] == ["A", "B", "C", "D", "E"]
        r["env_not_outage"] = "could not be reached" not in d["env"] and "present-day" in d["env"].lower()
        r["env_attribution"] = d["attr"] == "https://computingplace.org/" and "EDOPS" in d["env"]
        return r, d

    # ---- tests ------------------------------------------------------------------------
    def test_record_page_sections(self):
        t = FakeTransport(AREA)
        page = self.new_page()
        with patch("api.edop_client._requests_transport", side_effect=t), \
                patch("api.crc_client.crc_fetch_places", side_effect=fake_fetch):
            page.goto(f"{self.live_server_url}/atlas/place/{self.A.pk}/#record_environment")
            results, d = self.record_checks(page)
            page.evaluate("document.querySelectorAll('.enr-env-modal details').forEach(e => e.open = true)")
            self.shot(page, "record_environment_area", ".enr-env-modal .modal-content")
            self.shot(page, "record_gloss", "#record_gloss")
        failed = {k: v for k, v in results.items() if not v}
        self.assertEqual(failed, {}, f"{failed}; dom={d}; errors={self.errors}")
        self.assertEqual(len(t.calls), 1)
        self.assertEqual(t.calls[0]["params"]["scope"], "area")
        self.assertEqual(self.external, [])
        self.assertEqual(self.errors, [])

    def test_single_record_basin_with_band_t(self):
        t = FakeTransport(BASIN_T8)     # the level-8 answer (ruling 2026-10-21)
        page = self.new_page()
        with patch("api.edop_client._requests_transport", side_effect=t), \
                patch("api.crc_client.crc_fetch_places", side_effect=fake_fetch):
            page.goto(f"{self.live_server_url}/atlas/place/{self.Lonely.pk}/#record_environment")
            self.wait_for(page, "window.__whgEnrichment && window.__whgEnrichment.gloss === 'ready'", 30000)
            _idle, ok = self.show_environment(page)
            d = page.evaluate("""() => ({env: document.querySelector('.enr-env-modal .modal-body').innerText,
                t: !!document.querySelector('.enr-env-modal .enr-band[data-band="T"][open]'),
                gloss: document.querySelector('#record_gloss .enr-body').innerText})""")
            self.shot(page, "record_environment_basin", ".enr-env-modal .modal-content")
        self.assertTrue(ok)
        self.assertIn("one basin", d["env"])
        self.assertIn("BasinATLAS level 8", d["env"])                      # the page says which level
        self.assertNotIn("level 6", d["env"])
        self.assertEqual(t.calls[0]["params"]["level"], 8)
        self.assertNotIn("an area", d["env"])
        self.assertIn("1600–1700", d["env"])
        self.assertIn("Drought index (PDSI), mean", d["env"])
        self.assertTrue(d["t"])
        self.assertIn("This record alone", d["gloss"])
        self.assertNotIn("This group is partial", d["env"])                # cf. test_partial_group_...
        self.assertNotIn("describes part of the group", d["gloss"])
        self.assertEqual(t.calls[0]["params"]["from_year"], 1600)

    def test_partial_group_is_said_in_both_sections(self):
        """Review 2026-10-21 (#3): a group whose inbound attestations hit the cap is
        partial, and both sections say so. INBOUND_LIMIT is patched to 1 with two
        attesters; test_single_record_basin_with_band_t is the same record's page
        without the note (asserted there)."""
        from places.models import Place, PlaceGeom, PlaceLink
        for i in range(2):
            p = Place.objects.create(title=f"Attester {i}", src_id=f"T{i}", dataset=self.Lonely.dataset, ccodes=[])
            PlaceGeom.objects.create(place=p, geom=Point(-0.1278, 51.5074, srid=4326),
                                     jsonb={"type": "Point", "coordinates": [-0.1278, 51.5074]})
            PlaceLink.objects.create(place=p, jsonb={"type": "closeMatch", "identifier": f"whg:{self.Lonely.pk}"})
        page = self.new_page()
        with patch("api.edop_client._requests_transport", side_effect=FakeTransport(BASIN_T)), \
                patch("api.crc_client.crc_fetch_places", side_effect=fake_fetch), \
                patch("places.concordance.INBOUND_LIMIT", 1):
            page.goto(f"{self.live_server_url}/atlas/place/{self.Lonely.pk}/#record_environment")
            self.wait_for(page, "window.__whgEnrichment && window.__whgEnrichment.gloss === 'ready'", 30000)
            _idle, env_ok = self.show_environment(page)
            ok = env_ok and page.evaluate("window.__whgEnrichment.gloss === 'ready'")
            d = page.evaluate("""() => ({env: document.querySelector('.enr-env-modal .modal-body').innerText,
                gloss: document.querySelector('#record_gloss .enr-body').innerText})""")
        self.assertTrue(ok)
        self.assertIn("This group is partial", d["env"])
        self.assertIn("describes part of the group", d["gloss"])
        self.assertIn("This record and 1 attested equivalent", d["gloss"])     # the one read

    def test_edops_unreachable_is_not_no_data(self):
        page = self.new_page()
        with patch("api.edop_client._requests_transport", side_effect=FakeTransport((503, ""))), \
                patch("api.crc_client.crc_fetch_places", side_effect=fake_fetch):
            page.goto(f"{self.live_server_url}/atlas/place/{self.Lonely.pk}/#record_environment")
            self.wait_for(page, "window.__whgEnrichment && window.__whgEnrichment.gloss === 'ready'", 30000)
            _idle, ok = self.show_environment(page)
            d = page.evaluate("""() => ({env: document.querySelector('.enr-env-modal .modal-body').innerText,
                chips: document.querySelectorAll('.enr-env-modal .enr-chip').length,
                card: (document.querySelector('.enr-env-modal .enr-card') || {dataset: {}}).dataset.status,
                gloss: window.__whgEnrichment.gloss})""")
            self.shot(page, "record_environment_unavailable", ".enr-env-modal .modal-content")
        self.assertTrue(ok)
        self.assertEqual(d["card"], "unavailable")
        self.assertIn("could not be reached just now", d["env"])
        self.assertIn("not a finding that there is no data", d["env"])
        self.assertEqual(d["chips"], 0)
        self.assertEqual(d["gloss"], "ready")          # the other section is unaffected

    @override_settings(CRC_GATEWAY_URL="http://gateway.invalid", CRC_GATEWAY_TIMEOUT=2)
    def test_locus_page_sections(self):
        from search.tests_locus import FakeGateway, token
        gw = FakeGateway()
        t = FakeTransport(AREA)
        page = self.new_page()
        page.add_init_script("try { localStorage.setItem('whg.debug', '1'); } catch (e) {}")
        with patch("api.crc_client.requests.post", side_effect=gw), \
                patch("api.edop_client._requests_transport", side_effect=t):
            page.goto(f"{self.live_server_url}/atlas/locus/?r={token()}")
            gloss_ok, idle, env_ok = self.locus_show_environment(page)
            d = page.evaluate("""(g) => ({gloss: (document.querySelector(g) || {}).innerText || '',
                env: (document.querySelector('.enr-env-modal .modal-body') || {}).innerText || '',
                selected: (window.__whgLocus || {}).selectedIds || []})""", self.LOCUS_GLOSS)
            self.shot(page, "locus_enrichment", ".enr-env-modal .modal-content")
        self.assertTrue(gloss_ok and idle and env_ok, f"{(gloss_ok, idle, env_ok)}; errors={self.errors}")
        self.assertIn("2 records currently in this cluster", d["gloss"])
        self.assertIn("Country code DE in every record.", d["gloss"])
        self.assertNotIn("US", d["gloss"])                  # the other Berlin is another cluster
        self.assertIn("an area", d["env"])
        self.assertIn("BasinATLAS level 6", d["env"])
        self.assertNotIn("level 8", d["env"])
        self.assertIn("tight: 50", d["env"])
        self.assertNotIn("Based on the first", d["gloss"] + d["env"])   # no cap note under the cap
        self.assertEqual(sorted(d["selected"]), ["gn:1", "wd:Q64"])
        self.assertEqual(len(t.calls), 1)
        self.assertEqual(self.external, [])

    @override_settings(CRC_GATEWAY_URL="http://gateway.invalid", CRC_GATEWAY_TIMEOUT=2)
    def test_locus_per_member_scope_names_basinatlas_level_8(self):
        """Issue 325 (status 2026-10-21): the Locus page's per-member text ("each record's
        own basin") did not say which BasinATLAS level. It now says level 8, read from the
        server's plan (api.edop_client.BASIN_LEVEL), even when EDOPS is down. The two
        Berlins are ~1.3 km apart; LOCUS_TIGHT_KM is patched below that, so the
        same cluster is described per member (test_locus_page_sections: one area, level 6)."""
        from search.tests_locus import FakeGateway, token
        t = FakeTransport((503, ""), (503, ""))
        page = self.new_page()
        with patch("api.crc_client.requests.post", side_effect=FakeGateway()), \
                patch("api.edop_client.LOCUS_TIGHT_KM", 0.5), \
                patch("api.edop_client._requests_transport", side_effect=t):
            page.goto(f"{self.live_server_url}/atlas/locus/?r={token()}")
            _g, idle, ok = self.locus_show_environment(page)
            env = page.evaluate("() => (document.querySelector('.enr-env-modal .modal-body') || {}).innerText || ''")
            self.shot(page, "locus_environment_per_member", ".enr-env-modal .modal-content")
        self.assertTrue(idle and ok, f"errors={self.errors}")
        self.assertIn("each record's own basin", env)
        self.assertIn("BasinATLAS level 8", env)
        self.assertNotIn("level 6", env)
        self.assertIn("could not be reached", env)           # the level came from the plan, not an answer
        self.assertEqual({c["params"]["level"] for c in t.calls}, {8})
        self.assertEqual(len(t.calls), 2)

    @override_settings(CRC_GATEWAY_URL="http://gateway.invalid", CRC_GATEWAY_TIMEOUT=2)
    def test_locus_over_200_members_posts_a_deterministic_200_and_says_so(self):
        """Review #10: the locus endpoints refuse more than 200 members (400), so a big
        cluster showed an error card. The page now posts the first 200 by place id and
        says so; the 2-member cluster of test_locus_page_sections carries no such note."""
        import copy
        from places import locus_recipe as lr
        from search.tests_locus import HITS, FakeGateway, recipe
        hits = [copy.deepcopy(HITS[0]), copy.deepcopy(HITS[1])]
        for i in range(203):
            h = copy.deepcopy(HITS[0])
            h.update(place_id=f"gn:{1000 + i}", repr_point=[13.4 + 0.0001 * i, 52.52])
            hits.append(h)
        gw = FakeGateway(hits=hits)
        t = FakeTransport(AREA)
        page = self.new_page()
        posted = []
        page.on("request", lambda r: posted.append((r.url, r.post_data))
                if ("/atlas/locus/gloss/" in r.url or "/atlas/locus/environment/" in r.url)
                and r.method == "POST" else None)
        tok = lr.encode_recipe_token(recipe({"query": "Berlin", "mode": "in", "size": 250}))
        with patch("api.crc_client.requests.post", side_effect=gw), \
                patch("api.edop_client._requests_transport", side_effect=t):
            page.goto(f"{self.live_server_url}/atlas/locus/?r={tok}")
            gloss_ok, idle, env_ok = self.locus_show_environment(page, 60000)
            ok = gloss_ok and idle and env_ok
            d = page.evaluate("""(g) => ({gloss: (document.querySelector(g) || {}).innerText || '',
                env: (document.querySelector('.enr-env-modal .modal-body') || {}).innerText || '',
                posted: window.__whgEnrichment.posted, errors: document.querySelectorAll('.enr-error').length})""",
                              self.LOCUS_GLOSS)
            self.shot(page, "locus_enrichment_capped", ".enr-env-modal .modal-content")
        self.assertTrue(ok, f"errors={self.errors}; d={d}")
        self.assertEqual(d["posted"], {"sent": 200, "total": 205, "capped": True})
        bodies = [json.loads(b) for u, b in posted]
        self.assertEqual(len(bodies), 2)                                   # gloss + environment
        want = sorted(h["place_id"] for h in hits)[:200]
        for b in bodies:
            self.assertEqual([m["place_id"] for m in b["members"]], want)
        self.assertEqual(d["errors"], 0)
        for text in (d["gloss"], d["env"]):
            self.assertIn("Based on the first 200 of 205 members", text)
        self.assertIn("200 records currently in this cluster", d["gloss"])
        self.assertEqual(self.external, [])

    def _public_fixture(self):
        """The anonymous gateway rule needs an openly redistributable, licensed row
        (places.concordance.anonymous_gateway_namespaces); emb:7 is an EMBARGOED
        namespace's record that Record A also claims, which only a beta user sees."""
        from api.models import GazetteerRegistryEntry
        from licensing.models import License
        from places.models import PlaceLink
        lic, _ = License.objects.get_or_create(spdx_id="CC-BY-4.0", defaults={"label": "CC BY 4.0"})
        GazetteerRegistryEntry.objects.filter(id="gn").update(redistributable=True, license=lic)
        GazetteerRegistryEntry.objects.update_or_create(
            id="emb", defaults=dict(name="Embargoed Source", namespace="emb", entry_class="authority",
                                    status="embargoed", redistributable=True, license=lic))
        PlaceLink.objects.create(place=self.A, jsonb={"type": "closeMatch", "identifier": "emb:7"})
        GATEWAY_DB["emb:7"] = {"place_id": "emb:7", "title": "Embargoed Town", "namespace": "emb",
                               "repr_point": [0.5, 51.6], "names": [{"label": "Embargoed Town"}], "types": [],
                               "ccodes": ["GB"], "geometries": [{"type": "Point", "coordinates": [0.5, 51.6]}]}
        self.addCleanup(GATEWAY_DB.pop, "emb:7", None)

    def honest_fetch(self, ids, user=None, allow_anonymous=False, meta=None):
        """crc_fetch_places as it behaves: a signed-out caller only with the opt-in."""
        if not getattr(user, "is_authenticated", False) and not allow_anonymous:
            if meta is not None:
                meta.update(disabled=True, disabled_reason="anonymous")
            return {}
        return fake_fetch(ids)

    def public_page_checks(self, page, path, t):
        with patch("api.edop_client._requests_transport", side_effect=t), \
                patch("api.crc_client.crc_fetch_places", side_effect=self.honest_fetch):
            page.goto(f"{self.live_server_url}{path}")
            r, d = self.record_checks(page)
            d.update(page.evaluate("""() => ({
                navAbout: !!document.querySelector('a[href="#record_gloss"]'),
                publicAttr: (document.getElementById('record_gloss') || {dataset: {}}).dataset.enrichment,
                conc: window.__whgRecord && window.__whgRecord.concordance})"""))
        return r, d

    @override_settings(V4_LANDING_ENABLED=True)
    def test_signed_out_visitor_sees_both_sections_with_the_switch_on(self):
        """Ruling 2026-10-21: the gloss and environment are public at release. A
        signed-out visitor gets both sections, describing only what an anonymous
        caller may see: the embargoed namespace's member is absent from the gloss
        and from what was sent to EDOPS; the open gateway member is present."""
        self._public_fixture()
        t = FakeTransport(AREA)
        page = self.new_page(anonymous=True)
        r, d = self.public_page_checks(page, f"/atlas/place/{self.A.pk}/", t)
        self.shot(page, "record_public_anonymous", ".enr-env-modal .modal-content")
        failed = {k: v for k, v in r.items() if not v}
        self.assertEqual(failed, {}, f"{failed}; dom={d}; errors={self.errors}")
        self.assertEqual((d["navAbout"], d["publicAttr"]), (True, "public"))
        self.assertNotIn("Embargoed", d["gloss"] + d["env"])               # absence beside gloss_text (presence)
        self.assertNotIn("not public yet", d["env"])                        # nothing withheld: never in the group
        self.assertEqual(len(t.calls), 1)
        self.assertNotIn("0.5 51.6", t.calls[0]["params"]["geom_wkt"])
        self.assertEqual(self.external, [])
        self.assertEqual(self.errors, [])

    @override_settings(V4_LANDING_ENABLED=True)
    def test_open_record_page_shows_public_sections_to_non_beta(self):
        """The same for a signed-in user without beta access (the switch, not the
        account, opens them)."""
        self._public_fixture()
        plain = get_user_model().objects.create_user(username="enb_plain", email="enbp@example.org", password="pw",
                                                    given_name="P", surname="L")
        t = FakeTransport(AREA)
        page = self.new_page(user=plain)
        r, d = self.public_page_checks(page, f"/atlas/place/{self.A.pk}/", t)
        failed = {k: v for k, v in r.items() if not v}
        self.assertEqual(failed, {}, f"{failed}; dom={d}; errors={self.errors}")
        self.assertEqual(d["publicAttr"], "public")
        self.assertNotIn("Embargoed", d["gloss"] + d["env"])
        self.assertEqual(len(t.calls), 1)

    @override_settings(V4_LANDING_ENABLED=True)
    def test_beta_page_is_unchanged_with_the_switch_on(self):
        """A beta user's page: no public marker, and the embargoed member is described
        to them in the gloss (by design) but not sent to EDOPS (said so)."""
        self._public_fixture()
        t = FakeTransport(AREA)
        page = self.new_page()
        r, d = self.public_page_checks(page, f"/atlas/place/{self.A.pk}/", t)
        self.assertTrue(r["gloss_ready"] and r["env_ready"], d)
        self.assertIsNone(d["publicAttr"])
        self.assertIn("Embargoed Source", d["gloss"])
        self.assertIn("not public yet", d["env"])

    @override_settings(V4_LANDING_ENABLED=False, LOCUS_MINTING_ENABLED=False)
    def test_signed_out_visitor_gets_nothing_with_the_switch_off(self):
        t = FakeTransport(AREA)
        page = self.new_page(anonymous=True)
        with patch("api.edop_client._requests_transport", side_effect=t):
            page.goto(f"{self.live_server_url}/atlas/place/{self.A.pk}/")
            d = page.evaluate("""() => ({url: location.pathname,
                gloss: !!document.getElementById('record_gloss'), env: !!document.getElementById('record_environment')})""")
        self.assertTrue(d["url"].startswith("/accounts/login") or "login" in d["url"], d)     # presence: the login page
        self.assertEqual((d["gloss"], d["env"]), (False, False))
        self.assertEqual(t.calls, [])

    def test_harness_can_fail(self):
        """The same checks against a page whose bundle never loads must ALL fail."""
        t = FakeTransport(AREA)
        page = self.new_page(block="atlas_record.bundle.js")
        with patch("api.edop_client._requests_transport", side_effect=t), \
                patch("api.crc_client.crc_fetch_places", side_effect=fake_fetch):
            page.goto(f"{self.live_server_url}/atlas/place/{self.A.pk}/")
            results, d = self.record_checks(page, wait_ms=3000)
        passed = {k: v for k, v in results.items() if v}
        self.assertEqual(passed, {}, f"checks passed on a page with no bundle: {passed}; dom={d}")
        self.assertEqual(t.calls, [])
