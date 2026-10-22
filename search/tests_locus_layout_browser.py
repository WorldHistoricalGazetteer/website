"""The Locus page's Stage 5 layout in HEADLESS Chromium (place#325).

Playwright's own bundled chromium with a throwaway profile (never the user's Chrome,
no window), against a Django live server with the CRC gateway faked in-process
(``search.tests_locus``) and EDOPS faked at ``api.edop_client._requests_transport``
with the recorded fixture (``api.tests_edop_client.AREA``); computingplace.org is
blocked and any request to it is recorded and fails the test. The base map is a
minimal local style. Run locally only:

    WHG_BROWSER_TESTS=1 GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        WHG_LOCUS_BUNDLE_DIR=<scratch build> WHG_SHOT_DIR=<dir> \\
        venv/bin/python manage.py test search.tests_locus_layout_browser --settings=whg.settings_localtest

(``WHG_LOCUS_BUNDLE_DIR``: see ``search.tests_locus_layout_support``; unset, the
committed bundle is served, and until it is rebuilt from this source these tests fail.)

What it proves, and how each check can fail:

* at 1280, 992, 768 and 390 px, for a minted recipe locus and an unminted token:
  the map box is square (or capped at 560 px) and the MapLibre canvas fills it
  (a canvas left at its creation size, or the old fixed 380 px box, fails); at
  >= 992 the map is in a column right of the header, below that it comes straight
  after the header and before the member list; the page never scrolls sideways;
  the gloss arrives as themed cards; the recipe's text is not rendered until the
  Recipe button opens its modal, and then is; the Environment card is there and
  EDOPS has not been asked (no request to the locus environment endpoint, no
  EDOPS transport call: the Stage 3 page asked on load, and fails this);
* EDOPS is asked once, on a click on the cluster card's Environment button; the
  modal shows the area scope and the BasinATLAS level; reopening from the side
  card does not ask again;
* the Elsewhere card stays hidden while the gloss carries no Wikipedia links (as
  today), and appears with the language-labelled link when the payload carries
  one (the gloss response is rewritten in the browser to add it), so the block
  lights up when the gateway starts sending links;
* the harness can fail: the same waits against a page with no locus report never.

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

from api.tests_edop_client import AREA, FakeTransport
from places import locus_registry as registry
from search.tests_locus import GATEWAY, POST, SIMPLE, FakeGateway, _Resp, recipe, token
from search.tests_locus_browser import GL_ARGS, SHOT_DIR, _poll, _tiles_route
from search.tests_locus_layout_support import LOCUS_BUNDLE_DIR, route_locus_bundle

try:
    from playwright.sync_api import sync_playwright
    HAVE_PLAYWRIGHT = True
except ImportError:  # pragma: no cover
    HAVE_PLAYWRIGHT = False

WIDTHS = (1280, 992, 768, 390)
BLOCKED_HOSTS = ("analytics.whgazetteer.org", "errors.whgazetteer.org", "plausible", "computingplace.org")
LOCMEM = {**settings.CACHES, "default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache",
                                         "LOCATION": "locus-layout-325"}}
RESOLVED = "window.__whgLocus && window.__whgLocus.resolved !== undefined && window.__whgLocus"
GLOSS_DONE = ("window.__whgEnrichment && ['ready', 'error', 'not_public'].includes(window.__whgEnrichment.gloss)"
              " && window.__whgEnrichment.gloss")
MEASURE = """() => {
    const r = (el) => { if (!el) return null; const b = el.getBoundingClientRect();
        return {x: b.left, y: b.top + window.scrollY, w: b.width, h: b.height}; };
    const canvas = document.querySelector('#locus_map canvas.maplibregl-canvas');
    const modal = document.getElementById('locus_recipe_modal');
    return {
        header: r(document.querySelector('.rs-main > .rs-header')),
        map: r(document.querySelector('.rs-side > .rs-map')),
        canvas: canvas ? {w: canvas.clientWidth, h: canvas.clientHeight} : null,
        box: {w: document.getElementById('locus_map').clientWidth, h: document.getElementById('locus_map').clientHeight},
        result: r(document.getElementById('locus_result')),
        about: r(document.getElementById('locus_about')),
        envCard: r(document.querySelector('#locus_environment_slot .enr-env-card')),
        scrollW: document.documentElement.scrollWidth, clientW: document.documentElement.clientWidth,
        // The page's own content: the right-most edge of anything in #locus_page.
        pageRight: Math.max(...Array.from(document.querySelectorAll('#locus_page, #locus_page *'))
            .filter((e) => e.getClientRects().length).map((e) => e.getBoundingClientRect().right)),
        // What else sticks out (the site chrome, not this page), for the log line.
        overflowers: Array.from(document.querySelectorAll('body *')).filter((e) => !e.closest('#locus_page')
            && e.getClientRects().length && e.getBoundingClientRect().right > document.documentElement.clientWidth + 1)
            .slice(0, 3).map((e) => e.tagName.toLowerCase() + (e.id ? '#' + e.id : '') + '.' + String(e.className).split(' ')[0]),
        facets: document.querySelectorAll('#locus_about .rs-facet').length,
        facetKeys: Array.from(document.querySelectorAll('#locus_about .rs-facet')).map((f) => f.dataset.facet),
        aboutHidden: document.getElementById('locus_about').hidden,
        elsewhereHidden: document.getElementById('locus_elsewhere').hidden,
        sideText: document.querySelector('.rs-side').innerText,
        bodyText: document.body.innerText,
        modalShown: modal.classList.contains('show'),
        envButtons: Array.from(document.querySelectorAll('#locus_result .locus-env-open')).filter((b) => !b.hidden).length,
        env: window.__whgEnrichment.environment, envFetches: window.__whgEnrichment.environmentFetches,
    };
}"""


@unittest.skipUnless(HAVE_PLAYWRIGHT, "playwright is not importable here")
@unittest.skipUnless(os.environ.get("WHG_BROWSER_TESTS") == "1", "set WHG_BROWSER_TESTS=1 to run browser tests")
@override_settings(CRC_GATEWAY_URL=GATEWAY, CRC_GATEWAY_TIMEOUT=2, V4_LANDING_ENABLED=True,
                   CACHES=LOCMEM, EDOP_ENABLED=True)
class LocusLayoutBrowserTests(LiveServerTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._prev_async_unsafe = os.environ.get("DJANGO_ALLOW_ASYNC_UNSAFE")
        os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
        cls._pw = sync_playwright().start()
        cls.browser = cls._pw.chromium.launch(args=GL_ARGS)
        print(f"\n[locus layout] locus bundle from {LOCUS_BUNDLE_DIR or 'static/webpack/'}")

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
        from api.models import GazetteerRegistryEntry
        from django.core.cache import cache
        cache.clear()
        User = get_user_model()
        self.beta = User.objects.create_user(
            username="locus_layout_beta", email="llb@example.org", password="pw",
            given_name="Bea", surname="Tester", role="beta_tester")
        # Only namespaces an anonymous caller can see go to EDOPS (places.record_group.mark_offsite).
        for ns, name in (("gn", "GeoNames"), ("wd", "Wikidata")):
            GazetteerRegistryEntry.objects.update_or_create(
                id=ns, defaults=dict(name=name, namespace=ns, entry_class="authority", status="published"))
        self.gateway = FakeGateway()
        self.edops = FakeTransport(AREA)
        for target, side in ((POST, self.gateway), ("api.crc_client.requests.get", lambda *a, **k: _Resp(200)),
                             ("api.edop_client._requests_transport", self.edops)):
            p = patch(target, side_effect=side)
            p.start()
            self.addCleanup(p.stop)
        os.makedirs(SHOT_DIR, exist_ok=True)

    def _page(self, width):
        ctx = self.browser.new_context(viewport={"width": width, "height": 900})
        self.client.force_login(self.beta)
        ctx.add_cookies([{"name": settings.SESSION_COOKIE_NAME,
                          "value": self.client.cookies[settings.SESSION_COOKIE_NAME].value,
                          "url": self.live_server_url}])
        ctx.add_init_script("try { localStorage.setItem('whg.debug', '1'); } catch (e) {}")
        self.external = []
        self.env_posts = []

        def guard(route, request):
            url = request.url
            if "computingplace.org" in url:
                self.external.append(url)          # the page must never call EDOPS itself
            if any(h in url for h in BLOCKED_HOSTS):
                return route.abort()
            if "/atlas/locus/environment/" in url:
                self.env_posts.append(request.method)
            return route.continue_()
        # Routes run in reverse order of registration: bundle, then tiles, then the guard.
        ctx.route("**/*", guard)
        ctx.route("https://tiles.whgazetteer.org/**", _tiles_route)
        self.bundle_from = route_locus_bundle(ctx)
        self.addCleanup(ctx.close)
        page = ctx.new_page()
        console = []
        page.on("console", lambda m: None if "performance warning" in m.text
                else console.append(f"{m.type}: {m.text}"))
        page.on("pageerror", lambda e: console.append(f"pageerror: {e}"))
        return page, console

    def _open(self, page, path, console):
        resp = page.goto(self.live_server_url + path)
        self.assertEqual(resp.status, 200, path)
        state = _poll(page, RESOLVED, 60)
        self.assertTrue(state and state.get("resolved"), f"{path} never resolved: {state}; console: {console[-8:]}")
        self.assertEqual(sorted(state["selectedIds"]), ["gn:1", "wd:Q64"])
        gloss = _poll(page, GLOSS_DONE, 30)
        self.assertEqual(gloss, "ready", f"{path}: the gloss never loaded; console: {console[-8:]}")
        # Let the map settle into its box (ResizeObserver + refit) before measuring.
        _poll(page, "(() => { const c = document.querySelector('#locus_map canvas.maplibregl-canvas');"
                    " const b = document.getElementById('locus_map'); return c && b && b.clientWidth > 0"
                    " && Math.abs(c.clientWidth - b.clientWidth) <= 1 && Math.abs(c.clientHeight - b.clientHeight) <= 1; })()", 10)

    def _paths(self):
        entry, _ = registry.mint_recipe(recipe(SIMPLE), self.beta)
        return (("minted", f"/entity/locus:{entry.pid}/"), ("unminted", "/atlas/locus/?r=" + token(members=("gn:1",))))

    def test_layout_at_four_widths(self):
        for kind, path in self._paths():
            for width in WIDTHS:
                with self.subTest(kind=kind, width=width):
                    page, console = self._page(width)
                    self._open(page, path, console)
                    time.sleep(1.0)              # an EDOPS call made on load would land here
                    m = page.evaluate(MEASURE)
                    page.screenshot(path=os.path.join(SHOT_DIR, f"locus-layout-{kind}-{width}.png"), full_page=True)
                    mp, hd, res = m["map"], m["header"], m["result"]
                    # The map: square, or capped at 560 px high; the canvas fills the box.
                    self.assertTrue(mp and mp["w"] > 100, m)
                    self.assertLessEqual(mp["h"], 561, mp)
                    self.assertTrue(abs(mp["w"] - mp["h"]) <= 2 or (mp["h"] >= 559 and mp["w"] > mp["h"]), mp)
                    # (.rs-map's 1px border inside; the map container itself carries none)
                    self.assertTrue(abs(m["box"]["w"] - (mp["w"] - 2)) <= 1 and abs(m["box"]["h"] - (mp["h"] - 2)) <= 1,
                                    (m["box"], mp))
                    self.assertTrue(m["canvas"] and abs(m["canvas"]["w"] - m["box"]["w"]) <= 1
                                    and abs(m["canvas"]["h"] - m["box"]["h"]) <= 1, (m["canvas"], m["box"]))
                    if width >= 992:      # two columns: the map to the right of the header, level with it
                        self.assertGreaterEqual(mp["x"], hd["x"] + hd["w"] - 1, (mp, hd))
                        self.assertLess(abs(mp["y"] - hd["y"]), 40, (mp, hd))
                    else:                 # one column: header, then the map, then the members
                        self.assertGreaterEqual(mp["y"], hd["y"] + hd["h"] - 1, (mp, hd))
                        self.assertLessEqual(mp["y"] + mp["h"], res["y"] + 1, (mp, res))
                    # No horizontal scroll from the Locus page itself. (At 992 px the SITE navbar of a
                    # beta user is wider than the viewport on every page: logged, not this page's.)
                    self.assertLessEqual(m["pageRight"], m["clientW"] + 1, f"the locus page overflows at {width}px")
                    if m["scrollW"] > m["clientW"]:
                        print(f"\n[locus layout] {kind} {width}px: document scrolls sideways ({m['scrollW']} > "
                              f"{m['clientW']}) because of {m['overflowers']} (outside #locus_page)")
                    # About this cluster: themed cards, after the members.
                    self.assertFalse(m["aboutHidden"])
                    self.assertGreater(m["facets"], 0, m["facetKeys"])
                    self.assertIn("countries", m["facetKeys"])
                    # Environment: the card and the cluster card's button are there; nothing asked yet.
                    self.assertIsNotNone(m["envCard"])
                    self.assertEqual(m["envButtons"], 1)
                    self.assertIn("Show environment", m["sideText"])
                    self.assertEqual((m["env"], m["envFetches"]), ("idle", 0))
                    self.assertEqual(self.env_posts, [], "the environment endpoint was asked on load")
                    self.assertEqual(self.edops.calls, [], "EDOPS was asked on load")
                    # No Wikipedia links today: no Elsewhere card, no "Wikipedia" heading.
                    self.assertTrue(m["elsewhereHidden"])
                    self.assertNotIn("Wikipedia", m["sideText"])
                    # The recipe: not rendered until asked for, then rendered.
                    self.assertFalse(m["modalShown"])
                    # (lower-cased: the modal's h3s are text-transform: uppercase in innerText)
                    self.assertNotIn("members when the recipe was made", m["bodyText"].lower())
                    self.assertNotIn("generic-name stoplist", m["bodyText"].lower())
                    page.click("#locus_recipe_open")
                    shown = _poll(page, "(() => { const e = document.getElementById('locus_recipe_modal');"
                                        " return e.classList.contains('show') && getComputedStyle(e).display === 'block'"
                                        " && e.innerText; })()", 5)
                    time.sleep(0.4)              # the fade, for the screenshot only
                    page.screenshot(path=os.path.join(SHOT_DIR, f"locus-recipe-modal-{kind}-{width}.png"))
                    self.assertTrue(shown, f"the recipe modal never opened; console: {console[-8:]}")
                    self.assertIn("members when the recipe was made", shown.lower())
                    self.assertIn("generic-name stoplist", shown.lower())
                    self.assertIn("500 names: the gateway's current list", shown)
                    self.assertIn("the 100 best-scoring records", shown)
                    self.assertLessEqual(page.evaluate("document.getElementById('locus_recipe_modal').scrollWidth"),
                                         page.evaluate("document.getElementById('locus_recipe_modal').clientWidth"))
                    self.assertEqual(self.external, [])

    def test_environment_is_asked_once_and_only_on_click(self):
        _, path = self._paths()[0]
        page, console = self._page(1280)
        self._open(page, path, console)
        time.sleep(1.5)
        self.assertEqual((self.env_posts, len(self.edops.calls)), ([], 0))          # nothing on load
        page.click("#locus_result .locus-env-open")                                 # the cluster card's button
        ready = _poll(page, "window.__whgEnrichment.environment === 'ready'"
                            " && document.querySelector('.enr-env-modal.show .modal-body').innerText", 30)
        time.sleep(0.4)
        page.screenshot(path=os.path.join(SHOT_DIR, "locus-environment-modal.png"))
        self.assertTrue(ready, f"the environment never loaded; console: {console[-8:]}")
        self.assertIn("an area", ready)
        self.assertIn("BasinATLAS level 6", ready)
        self.assertIn("research prototype", ready.lower())
        self.assertEqual((self.env_posts, len(self.edops.calls)), (["POST"], 1))
        page.keyboard.press("Escape")
        self.assertTrue(_poll(page, "!document.querySelector('.enr-env-modal.show')", 5), "the modal did not close")
        page.click("#locus_environment_slot .enr-env-show")                       # the side card: a reopen
        self.assertTrue(_poll(page, "!!document.querySelector('.enr-env-modal.show')", 5))
        time.sleep(0.5)
        self.assertEqual(page.evaluate("window.__whgEnrichment.environmentFetches"), 1)
        self.assertEqual((self.env_posts, len(self.edops.calls)), (["POST"], 1))
        self.assertEqual(self.external, [])

    def test_wikipedia_block_appears_only_when_links_arrive(self):
        _, path = self._paths()[0]
        page, console = self._page(1280)

        def add_links(route, request):
            resp = route.fetch()
            data = resp.json()
            data["wikipedia"] = [{"lang": "en", "url": "https://en.wikipedia.org/wiki/Berlin", "title": "Berlin",
                                  "from": "wd:Q64"}]
            data["wikipedia_total"] = 1
            route.fulfill(response=resp, body=json.dumps(data), content_type="application/json")
        page.route("**/atlas/locus/gloss/", add_links)
        self._open(page, path, console)
        card = _poll(page, "!document.getElementById('locus_elsewhere').hidden"
                           " && document.getElementById('locus_elsewhere').innerText", 10)
        page.screenshot(path=os.path.join(SHOT_DIR, "locus-elsewhere-wikipedia.png"))
        self.assertTrue(card, f"the Elsewhere card never appeared; console: {console[-8:]}")
        self.assertIn("Elsewhere", card)
        self.assertIn("Wikipedia", card)
        self.assertIn("wd:Q64", card)
        self.assertEqual(page.evaluate("Array.from(document.querySelectorAll('#locus_elsewhere a')).map(a => a.href)"),
                         ["https://en.wikipedia.org/wiki/Berlin"])

    def test_the_harness_can_fail(self):
        page, _ = self._page(1280)
        page.goto("data:text/html,<title>no locus here</title><p>nothing</p>")
        t0 = time.monotonic()
        self.assertIsNone(_poll(page, RESOLVED, 1))
        self.assertIsNone(_poll(page, GLOSS_DONE, 1))
        self.assertIsNone(_poll(page, "document.getElementById('locus_recipe_modal') && "
                                      "document.getElementById('locus_recipe_modal').classList.contains('show')", 1))
        self.assertLess(time.monotonic() - t0, 6, "the bounded waits overshot their budget")
