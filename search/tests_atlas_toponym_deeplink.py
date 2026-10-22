"""Browser test for the Atlas toponym deep link ``/atlas/<toponym>`` (place#325).

This is what the Record page's *Explore similar records* button links to
(``places.views_record.explore_similar_url``). It used to load an Atlas that
had not searched: Areas mode, an empty box, no results, no console error. The
deep-link block set the box to the toponym and THEN called
``switchSearchMode('toponyms')``, which ends with ``input.value = ''``; the
search 300 ms later read an empty box and returned at "need a search term".
The Places toggle was never marked active either, so the bar still said Areas.

Headless Playwright (its own bundled chromium, throwaway profile; nothing
touches the user's Chrome, no window opens) against a Django live server with
the CRC gateway faked in-process, as ``search.tests_locus_browser``; the base
map is a minimal local style, so nothing depends on tiles. Run locally only:

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 WHG_BROWSER_TESTS=1 \\
        venv/bin/python manage.py test search.tests_atlas_toponym_deeplink --settings=whg.settings_localtest

``WHG_ATLAS_BUNDLE_DIR=<dir>`` serves ``atlas.bundle.js`` (and its css) from a
scratch build instead of ``static/webpack/``, to test source not yet bundled.

What it proves, and how each check can fail:

* a beta visitor on ``/atlas/Greece`` gets the toponym in the box, the Places
  toggle active, and exactly ONE gateway search whose query is "Greece"
  (counted server side, by the fake gateway, after the cards render and a
  further settle period); without the fix the gateway receives nothing and the
  box is empty, so every one of these fails;
* spaces, non-ASCII and '%' survive the path (``/atlas/New%20York``,
  ``/atlas/Z%C3%BCrich``, ``/atlas/50%25``): the query the gateway receives is
  the decoded name, compared to a literal here, not read back from the page;
* a search that cannot run says so: with ``/atlas/search/`` answering 503 a
  beta visitor sees the "temporarily unavailable" notice in the results panel,
  and an anonymous visitor's legacy ``/search/index/`` request carries the
  toponym and, answered 500, shows "Search failed";
* the harness can fail: the search-count wait against a page with no Atlas
  reports never.

Skipped loudly when Playwright is not importable or WHG_BROWSER_TESTS is unset.
"""
import json
import os
import time
import unittest
from unittest.mock import patch

from django.conf import settings
from django.contrib.auth import get_user_model
from django.test import LiveServerTestCase, override_settings

from search.tests_locus import GATEWAY, POST, FakeGateway, _Resp
from search.tests_locus_browser import GL_ARGS, SHOT_DIR, _poll, _tiles_route

try:
    from playwright.sync_api import sync_playwright
    HAVE_PLAYWRIGHT = True
except ImportError:  # pragma: no cover
    HAVE_PLAYWRIGHT = False

BUNDLE_DIR = os.environ.get("WHG_ATLAS_BUNDLE_DIR") or ""
PLACES_ACTIVE = ("document.querySelector('.search-mode-toggle .btn[data-search-mode=\"toponyms\"]')"
                 ".classList.contains('active')")
AREAS_ACTIVE = ("document.querySelector('.search-mode-toggle .btn[data-search-mode=\"areas\"]')"
                ".classList.contains('active')")


def _bundle_route(route, request):
    name = request.url.rsplit("/", 1)[-1].split("?", 1)[0]
    path = os.path.join(BUNDLE_DIR, name)
    if os.path.isfile(path):
        ctype = "text/css" if name.endswith(".css") else "application/javascript"
        with open(path, "rb") as f:
            route.fulfill(status=200, content_type=ctype, body=f.read())
    else:
        route.continue_()


@unittest.skipUnless(HAVE_PLAYWRIGHT, "playwright is not importable here")
@unittest.skipUnless(os.environ.get("WHG_BROWSER_TESTS") == "1", "set WHG_BROWSER_TESTS=1 to run browser tests")
@override_settings(CRC_GATEWAY_URL=GATEWAY, CRC_GATEWAY_TIMEOUT=2)
class AtlasToponymDeepLinkTests(LiveServerTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._prev_async_unsafe = os.environ.get("DJANGO_ALLOW_ASYNC_UNSAFE")
        os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
        cls._pw = sync_playwright().start()
        cls.browser = cls._pw.chromium.launch(args=GL_ARGS)
        print(f"\n[atlas deep link] atlas bundle from {BUNDLE_DIR or 'static/webpack/'}")

    @classmethod
    def tearDownClass(cls):
        try:
            cls.browser.close()
            cls._pw.stop()
            if cls._prev_async_unsafe is None:
                os.environ.pop("DJANGO_ALLOW_ASYNC_UNSAFE", None)
            else:
                os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = cls._prev_async_unsafe
        finally:
            super().tearDownClass()

    def setUp(self):
        User = get_user_model()
        self.beta = User.objects.create_user(
            username="atlas_deeplink_beta", email="adl@example.org", password="pw",
            given_name="Bea", surname="Tester", role="beta_tester")
        self.assertTrue(self.beta.can_access_beta)
        self.gateway = FakeGateway()
        for target, side in ((POST, self.gateway), ("api.crc_client.requests.get", lambda *a, **k: _Resp(200))):
            p = patch(target, side_effect=side)
            p.start()
            self.addCleanup(p.stop)
        os.makedirs(SHOT_DIR, exist_ok=True)

    def _page(self, signed_in=True):
        ctx = self.browser.new_context(viewport={"width": 1400, "height": 900})
        if signed_in:
            self.client.force_login(self.beta)
            ctx.add_cookies([{
                "name": settings.SESSION_COOKIE_NAME,
                "value": self.client.cookies[settings.SESSION_COOKIE_NAME].value,
                "url": self.live_server_url,
            }])
        # Debug gate for the boot flag only. The tour is NOT pre-dismissed: the
        # deep link itself must keep the first-visit tour away.
        ctx.add_init_script("try { localStorage.setItem('whg.debug', '1'); } catch (e) {}")
        ctx.route("https://tiles.whgazetteer.org/**", _tiles_route)
        if BUNDLE_DIR:
            ctx.route("**/static/webpack/atlas.bundle.*", _bundle_route)
        self.addCleanup(ctx.close)
        page = ctx.new_page()
        console = []
        # SwiftShader's "performance warning" lines would bury the app's own.
        page.on("console", lambda m: None if "performance warning" in m.text
                else console.append(f"{m.type}: {m.text}"))
        page.on("pageerror", lambda e: console.append(f"pageerror: {e}"))
        return page, console

    def _open(self, page, path, console):
        page.goto(self.live_server_url + path)
        booted = _poll(page, "window.__whgAtlas && (window.__whgAtlas.booted || window.__whgAtlas.error) && window.__whgAtlas", 90)
        if not booted or not booted.get("booted"):
            page.screenshot(path=os.path.join(SHOT_DIR, "deeplink-never-booted.png"))
            self.fail(f"the Atlas never booted on {path}: {booted}; console tail: {console[-10:]}")

    def _wait_for_searches(self, n, timeout_s):
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if len(self.gateway.bodies) >= n:
                return len(self.gateway.bodies)
            time.sleep(0.25)
        return None

    def _assert_deep_link_searched(self, path, expected_query, shot):
        page, console = self._page()
        self.gateway.bodies.clear()
        self._open(page, path, console)
        self.assertEqual(page.evaluate("atlas_toponym"), expected_query)   # the server decoded the path
        n = self._wait_for_searches(1, 20)
        cards = _poll(page, "document.querySelectorAll('#atlas_search_results .cluster-card').length", 20)
        time.sleep(2)   # settle: a second search would land here
        page.screenshot(path=os.path.join(SHOT_DIR, shot))
        state = page.evaluate(f"""({{
            value: document.getElementById('atlas_search_input').value,
            places: {PLACES_ACTIVE}, areas: {AREAS_ACTIVE},
            tinted: document.getElementById('floating_search').classList.contains('mode-toponyms'),
            tour: !!document.querySelector('.driver-popover'),
        }})""")
        self.assertEqual(n, 1, f"{path}: the gateway was never asked; state {state}; console tail: {console[-8:]}")
        self.assertEqual(len(self.gateway.bodies), 1, f"{path}: searched more than once: {self.gateway.bodies}")
        self.assertEqual(self.gateway.bodies[0].get("query"), expected_query)
        self.assertEqual(state["value"], expected_query, state)
        self.assertTrue(state["places"] and not state["areas"] and state["tinted"], state)
        self.assertFalse(state["tour"], "the first-visit tour opened over a deep link")
        self.assertEqual(cards, 2, f"{path}: the results never rendered; console tail: {console[-8:]}")

    def test_deep_link_runs_one_places_search(self):
        self._assert_deep_link_searched("/atlas/Greece", "Greece", "deeplink-greece.png")

    def test_spaces_unicode_and_percent_survive(self):
        for path, expected in (("/atlas/New%20York", "New York"),
                               ("/atlas/Z%C3%BCrich", "Zürich"),
                               ("/atlas/50%25", "50%")):
            with self.subTest(path=path):
                self._assert_deep_link_searched(path, expected, "deeplink-" + str(abs(hash(path))) + ".png")

    def test_gateway_unavailable_shows_the_notice(self):
        page, console = self._page()
        asked = []

        def unavailable(route, request):
            asked.append(json.loads(request.post_data or "{}"))
            route.fulfill(status=503, content_type="application/json", body="{}")

        page.route("**/atlas/search/", unavailable)
        self._open(page, "/atlas/Greece", console)
        text = _poll(page, "(document.getElementById('atlas_search_results') || {}).innerText", 20)
        page.screenshot(path=os.path.join(SHOT_DIR, "deeplink-unavailable.png"))
        self.assertTrue(asked, f"the deep link never asked /atlas/search/; console tail: {console[-8:]}")
        self.assertEqual(asked[0].get("qstr"), "Greece")
        self.assertIn("temporarily unavailable", text or "")
        self.assertTrue(page.evaluate("document.getElementById('atlas_search_results').offsetParent !== null"),
                        "the notice is not visible")

    def test_anonymous_visitor_legacy_search_carries_the_toponym(self):
        page, console = self._page(signed_in=False)
        asked = []

        def legacy(route, request):
            asked.append(json.loads(request.post_data or "{}"))
            route.fulfill(status=500, content_type="application/json", body="{}")

        page.route("**/search/index/", legacy)
        self._open(page, "/atlas/Greece", console)
        text = _poll(page, "(document.getElementById('atlas_search_results') || {}).innerText.includes('Search failed')"
                           " && document.getElementById('atlas_search_results').innerText", 20)
        page.screenshot(path=os.path.join(SHOT_DIR, "deeplink-anonymous.png"))
        self.assertEqual([a.get("qstr") for a in asked], ["Greece"], f"console tail: {console[-8:]}")
        self.assertIn("Search failed", text or "")
        self.assertEqual(page.evaluate("document.getElementById('atlas_search_input').value"), "Greece")
        self.assertEqual(self.gateway.bodies, [], "an anonymous visitor must not reach the gateway")

    def test_the_harness_can_fail(self):
        page, console = self._page()
        page.goto("data:text/html,<title>no atlas here</title><p>nothing</p>")
        t0 = time.monotonic()
        self.assertIsNone(self._wait_for_searches(1, 2))
        self.assertIsNone(_poll(page, "window.__whgAtlas && window.__whgAtlas.booted", 1))
        self.assertLess(time.monotonic() - t0, 6, "the bounded waits overshot their budget")
