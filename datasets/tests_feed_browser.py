"""place#338: the contributor feed ("Link your records") in HEADLESS Chromium.

Opt-in, because it starts a browser and a live server:

    WHG_BROWSER_TESTS=1 WHG_FEED_BUNDLE_DIR=<dir> GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test datasets.tests_feed_browser --settings=whg.settings_localtest

``WHG_FEED_BUNDLE_DIR`` serves ``match_feed.bundle.js``/``.css`` from a scratch build (the entry is
new and ``static/webpack`` is the lead's to rebuild); without it the committed static file is used,
which does not exist yet, and every check fails (as the harness control requires).

The gateway answer is RECORDED: the CRC response for "Coventry" captured on 2026-10-21
(``scripts/fixtures/cluster_parity/real_coventry_2026-10-21.json``), trimmed to 20 hits and served
by the ``GatewayFake`` of ``tests_candidates`` in place of ``api.crc_client.crc_search_status``.
``requests`` inside the client fails the test, so nothing reaches the CRC. The Links are written for
real, through ``api.attestations.create``, into the test database. Playwright's bundled chromium,
fresh profile, never a visible window; the page's readiness flag (``__whgMatchFeed.ready``) is
waited on, not a timer. ``test_harness_can_fail`` runs the flow with the bundle blocked and requires
every check to fail.
"""
import json
import os
import unittest
from unittest.mock import patch

from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.gis.geos import Point
from django.test import Client, LiveServerTestCase

from datasets import candidates
from datasets.models_matching import CandidateGeneration, RecordReviewState
from datasets.tests_candidates import GatewayFake, no_network

RUN = os.environ.get("WHG_BROWSER_TESTS") == "1"
BUNDLE_DIR = os.environ.get("WHG_FEED_BUNDLE_DIR") or ""
BLOCKED_HOSTS = ("analytics.whgazetteer.org", "errors.whgazetteer.org", "plausible", "computingplace.org")
FIXTURE = os.path.join(settings.BASE_DIR, "scripts", "fixtures", "cluster_parity", "real_coventry_2026-10-21.json")
COVENTRY_GN = "gn:2652221"


def recorded_coventry():
    with open(FIXTURE) as f:
        data = json.load(f)["data"]
    hits = data["hits"][:20]
    assert any(h["place_id"] == COVENTRY_GN for h in hits), "fixture no longer holds gn:2652221"
    return {**data, "hits": hits}


@unittest.skipUnless(RUN, "set WHG_BROWSER_TESTS=1 to drive the contributor feed in headless Chromium")
class FeedBrowserTests(LiveServerTestCase):
    # LiveServerTestCase flushes the database after each test, which drops the gazetteer registry
    # rows the migrations seed; a Link to gn: is then refused as an unknown namespace, so the
    # second test would fail for its neighbour's reason. Restore them.
    serialized_rollback = True
    @classmethod
    def setUpClass(cls):
        os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
        super().setUpClass()
        from playwright.sync_api import sync_playwright
        cls._pw = sync_playwright().start()
        cls.browser = cls._pw.chromium.launch(headless=True)
        print(f"\n[match feed] bundle from {BUNDLE_DIR or 'static/webpack/'}")

    @classmethod
    def tearDownClass(cls):
        try:
            cls.browser.close()
            cls._pw.stop()
        finally:
            super().tearDownClass()

    def setUp(self):
        from datasets.models import Dataset
        from places.models import Place, PlaceGeom
        self.fake = GatewayFake()
        for target, fn in (("api.crc_client.crc_search_status", self.fake),
                           ("api.crc_client.requests.post", no_network),
                           ("api.crc_client.requests.get", no_network)):
            p = patch(target, side_effect=fn)
            p.start()
            self.addCleanup(p.stop)
        self.recorded = recorded_coventry()
        gn = next(h for h in self.recorded["hits"] if h["place_id"] == COVENTRY_GN)
        self.fake.by_query["Coventry"] = self.recorded
        self.user = get_user_model().objects.create_user(username="mf_beta", email="mf@example.org", password="pw",
                                                         given_name="M", surname="F", role="beta_tester")
        with patch("datasets.signals.doi"):
            self.ds = Dataset.objects.create(owner=self.user, label="mf338", title="Parishes", description="D",
                                             creator="X", public=False, ds_status="uploaded")
        self.rec = Place.objects.create(title="Coventry", src_id="p1", dataset=self.ds, ccodes=[])
        PlaceGeom.objects.create(place=self.rec, geom=Point(*gn["repr_point"], srid=4326),
                                 jsonb={"type": "Point", "coordinates": gn["repr_point"]})
        self.gen = CandidateGeneration.objects.create(dataset=self.ds, requested_by=self.user, status="finished",
                                                      recipe=candidates.build_recipe(self.ds))
        self.tier = candidates.record_candidates(self.rec, self.gen, self.user)

    def new_page(self, block_bundle=False):
        ctx = self.browser.new_context(viewport={"width": 1280, "height": 1000})
        c = Client()
        c.force_login(self.user)
        ctx.add_cookies([{"name": settings.SESSION_COOKIE_NAME, "value": c.cookies[settings.SESSION_COOKIE_NAME].value,
                          "url": self.live_server_url}])
        page = ctx.new_page()

        def route_all(route):
            url = route.request.url
            if any(h in url for h in BLOCKED_HOSTS) or (block_bundle and "match_feed.bundle" in url):
                return route.abort()
            name = url.rsplit("/", 1)[-1].split("?", 1)[0]
            path = os.path.join(BUNDLE_DIR, name)
            if BUNDLE_DIR and name.startswith("match_feed.bundle") and os.path.isfile(path):
                with open(path, "rb") as f:
                    return route.fulfill(status=200, body=f.read(),
                                         content_type="text/css" if name.endswith(".css") else "application/javascript")
            return route.continue_()

        page.route("**/*", route_all)
        return page

    def wait_for(self, page, expr, timeout_ms=20000):
        try:
            page.wait_for_function(expr, timeout=timeout_ms)
            return True
        except Exception:  # noqa: BLE001 — a timeout is a finding
            return False

    def flow(self, page):
        """The feed checks as named results (also run by the harness control)."""
        r = {}
        page.goto(f"{self.live_server_url}/datasets/{self.ds.pk}/matches/?tier={self.tier}")
        r["ready"] = self.wait_for(page, "window.__whgMatchFeed && window.__whgMatchFeed.ready === true")
        shown = page.evaluate("""(gn) => ({
            title: (document.getElementById('mf_record_title') || {}).textContent || '',
            cands: document.querySelectorAll('#mf_cands .mf-cand').length,
            gnIndex: [...document.querySelectorAll('#mf_cands .mf-cand')].findIndex((li) => li.dataset.candidate === gn),
            why: [...document.querySelectorAll('#mf_cands .mf-why')].map((x) => x.textContent).join(' | '),
            text: (document.getElementById('match_feed') || {}).textContent || '' })""", COVENTRY_GN)
        r["record_shown"] = shown["title"].startswith("Coventry")
        r["recorded_candidates_shown"] = shown["cands"] >= 10 and shown["gnIndex"] >= 0
        r["explained"] = "km" in shown["why"]
        r["vocabulary"] = bool(shown["text"]) and not any(w in shown["text"].lower()
                                                          for w in ("attest", "accession", "reconcil"))
        idx = shown["gnIndex"]
        if 0 <= idx < 9:
            page.keyboard.press(str(idx + 1))      # choose the GeoNames Coventry...
            page.keyboard.press("Enter")           # ...and say it is the same place
        r["saved"] = self.wait_for(page, "window.__whgMatchFeed.saves === 1", 20000)
        r["announced"] = "Saved." in (page.evaluate("(document.getElementById('mf_live') || {}).textContent || ''"))
        from api.models import ContributorAttestation
        rows = list(ContributorAttestation.objects.filter(user=self.user))
        r["link_written"] = (len(rows) == 1 and COVENTRY_GN in (rows[0].place_a, rows[0].place_b)
                             and rows[0].relation_type == "closeMatch" and rows[0].candidate_id is not None)
        page.keyboard.press("u")
        r["undone"] = self.wait_for(page, "(document.getElementById('mf_live') || {}).textContent === 'Undone.'", 10000)
        rows = list(ContributorAttestation.objects.filter(user=self.user))
        r["undo_withdrew"] = len(rows) == 1 and rows[0].status == "withdrawn"
        # Hand search: silence is said as silence, beside an answered search through the same box.
        self.fake.by_query["Coventry Cathedral"] = self.recorded
        page.fill("#mf_search_q", "Coventry Cathedral")
        page.keyboard.press("Enter")
        r["search_answered"] = self.wait_for(page, "document.querySelectorAll('#mf_search_results .mf-cand').length >= 10", 10000)
        self.fake.failures["Coventry Cathedral"] = "timeout"
        page.click("#mf_search [data-act=search]")
        r["search_silence_said"] = self.wait_for(page, "!!document.getElementById('mf_search_silent')", 10000)
        return r

    def test_link_undo_and_hand_search_with_the_keyboard(self):
        self.assertEqual(self.tier, "weak")       # the recorded response has no confidence (mode "in")
        self.assertEqual(RecordReviewState.objects.get(place=self.rec).candidate_count, 20)
        page = self.new_page()
        results = self.flow(page)
        failed = {k: v for k, v in results.items() if not v}
        self.assertEqual(failed, {}, results)

    def test_harness_can_fail(self):
        """The same checks with the page bundle blocked: every one must fail."""
        page = self.new_page(block_bundle=True)
        with patch.object(self, "wait_for", lambda p, e, t=20000: FeedBrowserTests.wait_for(self, p, e, 2000)):
            try:
                results = self.flow(page)
            except Exception as exc:  # noqa: BLE001 — a flow that cannot even run has failed every check
                results = {"flow_raised": False, "error": str(exc)[:80] and False}
        passed = {k: v for k, v in results.items() if v}
        self.assertEqual(passed, {}, f"checks passed on a page with no feed bundle: {passed}")
        self.assertIn("Link your records: Parishes", page.content())  # the page was served; only its bundle is missing
