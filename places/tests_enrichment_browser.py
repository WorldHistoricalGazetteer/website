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

from api.tests_edop_client import AREA, BASIN_T, FakeTransport

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

    def new_page(self, block=None, user=None):
        c = Client()
        c.force_login(user or self.beta)
        ctx = self.browser.new_context(viewport={"width": 1280, "height": 1000})
        ctx.add_cookies([{"name": settings.SESSION_COOKIE_NAME, "value": c.cookies[settings.SESSION_COOKIE_NAME].value,
                          "url": self.live_server_url}])
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

    def record_checks(self, page, wait_ms=30000):
        r = {}
        r["gloss_ready"] = self.wait_for(page, "window.__whgEnrichment && window.__whgEnrichment.gloss === 'ready'", wait_ms)
        r["env_ready"] = self.wait_for(page, "window.__whgEnrichment && window.__whgEnrichment.environment === 'ready'", wait_ms)
        d = page.evaluate("""() => {
            const t = (s) => (document.querySelector(s) || {}).innerText || '';
            const a = document.querySelector('#record_environment .enr-attribution a');
            return {gloss: t('#record_gloss .enr-body'), env: t('#record_environment .enr-body'),
                    chips: Array.from(document.querySelectorAll('#record_environment .enr-chip')).map(e => e.innerText),
                    bands: Array.from(document.querySelectorAll('#record_environment .enr-band')).map(e => e.dataset.band),
                    attr: a && a.getAttribute('href'),
                    status: window.__whgEnrichment && window.__whgEnrichment.environmentStatus};
        }""")
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
            page.evaluate("document.querySelectorAll('#record_environment details').forEach(e => e.open = true)")
            self.shot(page, "record_environment_area", "#record_environment")
            self.shot(page, "record_gloss", "#record_gloss")
        failed = {k: v for k, v in results.items() if not v}
        self.assertEqual(failed, {}, f"{failed}; dom={d}; errors={self.errors}")
        self.assertEqual(len(t.calls), 1)
        self.assertEqual(t.calls[0]["params"]["scope"], "area")
        self.assertEqual(self.external, [])
        self.assertEqual(self.errors, [])

    def test_single_record_basin_with_band_t(self):
        t = FakeTransport(BASIN_T)
        page = self.new_page()
        with patch("api.edop_client._requests_transport", side_effect=t), \
                patch("api.crc_client.crc_fetch_places", side_effect=fake_fetch):
            page.goto(f"{self.live_server_url}/atlas/place/{self.Lonely.pk}/#record_environment")
            ok = self.wait_for(page, "window.__whgEnrichment && window.__whgEnrichment.environment === 'ready'", 30000)
            d = page.evaluate("""() => ({env: document.querySelector('#record_environment .enr-body').innerText,
                t: !!document.querySelector('#record_environment .enr-band[data-band="T"][open]'),
                gloss: document.querySelector('#record_gloss .enr-body').innerText})""")
            self.shot(page, "record_environment_basin", "#record_environment")
        self.assertTrue(ok)
        self.assertIn("one basin", d["env"])
        self.assertNotIn("an area", d["env"])
        self.assertIn("1600–1700", d["env"])
        self.assertIn("Drought index (PDSI), mean", d["env"])
        self.assertTrue(d["t"])
        self.assertIn("This record alone", d["gloss"])
        self.assertEqual(t.calls[0]["params"]["from_year"], 1600)

    def test_edops_unreachable_is_not_no_data(self):
        page = self.new_page()
        with patch("api.edop_client._requests_transport", side_effect=FakeTransport((503, ""))), \
                patch("api.crc_client.crc_fetch_places", side_effect=fake_fetch):
            page.goto(f"{self.live_server_url}/atlas/place/{self.Lonely.pk}/#record_environment")
            ok = self.wait_for(page, "window.__whgEnrichment && window.__whgEnrichment.environment === 'ready'", 30000)
            d = page.evaluate("""() => ({env: document.querySelector('#record_environment .enr-body').innerText,
                chips: document.querySelectorAll('#record_environment .enr-chip').length,
                card: (document.querySelector('#record_environment .enr-card') || {dataset: {}}).dataset.status,
                gloss: window.__whgEnrichment.gloss})""")
            self.shot(page, "record_environment_unavailable", "#record_environment")
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
            ok = self.wait_for(page, "window.__whgEnrichment && window.__whgEnrichment.environment === 'ready' "
                                     "&& window.__whgEnrichment.gloss === 'ready'", 45000)
            d = page.evaluate("""() => ({gloss: (document.querySelector('#locus_gloss') || {}).innerText || '',
                env: (document.querySelector('#locus_environment') || {}).innerText || '',
                selected: (window.__whgLocus || {}).selectedIds || []})""")
            self.shot(page, "locus_enrichment", "#locus_enrichment")
        self.assertTrue(ok, f"errors={self.errors}")
        self.assertIn("2 records currently in this cluster", d["gloss"])
        self.assertIn("Country code DE in every record.", d["gloss"])
        self.assertNotIn("US", d["gloss"])                  # the other Berlin is another cluster
        self.assertIn("an area", d["env"])
        self.assertIn("tight: 50", d["env"])
        self.assertEqual(sorted(d["selected"]), ["gn:1", "wd:Q64"])
        self.assertEqual(len(t.calls), 1)
        self.assertEqual(self.external, [])

    @override_settings(V4_LANDING_ENABLED=True)
    def test_open_record_page_shows_no_enrichment_to_non_beta(self):
        """With Stage 4's switch on the Record page is open to everyone; the gloss and
        EDOPS stay beta-only: absent for a non-beta user, beside the concordance, which
        is present on the same page."""
        plain = get_user_model().objects.create_user(username="enb_plain", email="enbp@example.org", password="pw",
                                                    given_name="P", surname="L")
        t = FakeTransport(AREA)
        page = self.new_page(user=plain)
        with patch("api.edop_client._requests_transport", side_effect=t), \
                patch("api.crc_client.crc_fetch_places", side_effect=fake_fetch):
            page.goto(f"{self.live_server_url}/atlas/place/{self.A.pk}/")
            conc = self.wait_for(page, "window.__whgRecord && window.__whgRecord.concordance === 'ready'", 30000)
            d = page.evaluate("""() => ({h1: (document.querySelector('#atlas_record h1') || {}).textContent,
                conc: !!document.querySelector('#concordance_tier1'),
                gloss: !!document.getElementById('record_gloss'), env: !!document.getElementById('record_environment'),
                navAbout: !!document.querySelector('a[href="#record_gloss"]'),
                enr: window.__whgEnrichment})""")
        self.assertTrue(conc)
        self.assertEqual(d["h1"], "Record A")
        self.assertTrue(d["conc"])
        self.assertEqual((d["gloss"], d["env"], d["navAbout"]), (False, False, False))
        self.assertEqual(d["enr"]["environment"], "idle")
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
