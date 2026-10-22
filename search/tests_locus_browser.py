"""Browser test for the cluster-card locus link and the locus page (place#325 Stage 2).

Headless Playwright against a Django live server, with the CRC gateway faked
at ``api.crc_client.requests.post`` (same process, so the patch reaches the
server thread). Uses Playwright's own bundled chromium with a throwaway
profile: nothing touches the user's Chrome and no window opens
(~/.claude/memory/playwright-maplibre-headless-testing.md).

Run locally only (never in the prod container):

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 WHG_SHOT_DIR=/some/dir \\
        venv/bin/python manage.py test search.tests_locus_browser --settings=whg.settings_localtest

What it proves, and how each check can fail:

* a cluster card carries a locus link whose token the SERVER decodes to the
  members the card shows (a token built from the wrong body, or with a
  non-literal parameter, hashes differently: the expected PID is computed in
  Python from the body the fake gateway received, not read back from the
  page), and the BROWSER's own recipePid agrees;
* the locus page re-runs the recipe, selects the right cluster and lists its
  members with record links; a tampered token is a 400 error page with no
  members (the same query that found members on the good page finds none);
* the harness can fail: the card wait is run against a page with no Atlas
  on it and must report "never".

Skipped (loudly, as a skip in the summary, never as a pass) when Playwright
is not importable in this interpreter.
"""
import base64
import json
import os
import re
import tempfile
import time
import unittest
from unittest.mock import patch

from django.conf import settings
from django.contrib.auth import get_user_model
from django.test import LiveServerTestCase, override_settings

from places import locus_recipe as lr
from search.tests_locus import (GATEWAY, PARAMS, POST, SEARCH_OK, STOPLIST, TOKEN_CEILING, FakeGateway, _Resp)

try:
    from playwright.sync_api import sync_playwright
    HAVE_PLAYWRIGHT = True
except ImportError:  # pragma: no cover
    HAVE_PLAYWRIGHT = False

GL_ARGS = ["--enable-unsafe-swiftshader", "--use-gl=angle", "--use-angle=swiftshader"]
SHOT_DIR = os.environ.get("WHG_SHOT_DIR") or tempfile.mkdtemp(prefix="whg-locus-shots-")
# The Atlas auto-fits θ per result set; with three hits (3 pairs < 8) it falls
# back to its default. The harness reads the slider rather than assuming this.
ATLAS_DEFAULT_THETA = 0.65


MINIMAL_STYLE = {
    "version": 8, "name": "harness", "sources": {},
    # whg_maplibre reads the style's own metadata at boot (atlasRecord.js's
    # fallback style carries the same key for the same reason).
    "metadata": {"whg:backgroundcolour": "#eef2f5"},
    "layers": [{"id": "bg", "type": "background", "paint": {"background-color": "#e8eef2"}}],
    "glyphs": "https://tiles.whgazetteer.org/fonts/{fontstack}/{range}.pbf",
}
_CORS = {"Access-Control-Allow-Origin": "*"}


def _tiles_route(route, request):
    if "/styles/" in request.url and request.url.endswith("style.json"):
        route.fulfill(status=200, content_type="application/json", headers=_CORS, body=json.dumps(MINIMAL_STYLE))
    else:
        route.fulfill(status=204, headers=_CORS, body="")


def _poll(page, js, timeout_s, every=0.25):
    """Main-world poll (page.waitForFunction runs in an isolated world where the
    app's globals do not exist). Returns the first truthy value, or None on timeout
    — a timeout is a RESULT, not an exception."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            v = page.evaluate(js)
        except Exception:  # noqa: BLE001 — navigation mid-evaluate
            v = None
        if v:
            return v
        time.sleep(every)
    return None


@unittest.skipUnless(HAVE_PLAYWRIGHT, "playwright is not importable here: venv/bin/python -m pip install playwright")
@override_settings(CRC_GATEWAY_URL=GATEWAY, CRC_GATEWAY_TIMEOUT=2)
class LocusBrowserTests(LiveServerTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # Playwright's sync API drives an asyncio loop in this thread; Django's
        # ORM then refuses every query ("SynchronousOnlyOperation") unless told
        # the test knows what it is doing. Scoped to this class.
        cls._prev_async_unsafe = os.environ.get("DJANGO_ALLOW_ASYNC_UNSAFE")
        os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
        cls._pw = sync_playwright().start()
        cls.browser = cls._pw.chromium.launch(args=GL_ARGS)

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
            username="locus_browser_beta", email="lbb@example.org", password="pw",
            given_name="Bea", surname="Tester", role="beta_tester")
        self.assertTrue(self.beta.can_access_beta)
        self.gateway = FakeGateway()
        for target, side in ((POST, self.gateway), ("api.crc_client.requests.get", lambda *a, **k: _Resp(200))):
            p = patch(target, side_effect=side)
            p.start()
            self.addCleanup(p.stop)
        os.makedirs(SHOT_DIR, exist_ok=True)

    def _context(self):
        ctx = self.browser.new_context(viewport={"width": 1400, "height": 900})
        self.client.force_login(self.beta)
        ctx.add_cookies([{
            "name": settings.SESSION_COOKIE_NAME,
            "value": self.client.cookies[settings.SESSION_COOKIE_NAME].value,
            "url": self.live_server_url,
        }])
        # Suppress the first-visit tour (its overlay swallows clicks) and open the
        # debug gate the app's readiness/locus globals sit behind. Injected, not
        # ?debug: the Atlas rewrites its own URL.
        ctx.add_init_script("try { localStorage.setItem('whg_atlas_tour_seen', 'true'); "
                            "localStorage.setItem('whg.debug', '1'); } catch (e) {}")
        # The tileserver refuses a localhost Origin (no CORS header), so the style
        # never loads and the Atlas never boots. Serve a minimal style from here
        # and answer every other tiles request with 204: the map exists, the test
        # asserts nothing about tiles, and no request leaves the machine for them.
        ctx.route("https://tiles.whgazetteer.org/**", _tiles_route)
        self.addCleanup(ctx.close)
        return ctx

    def _wait_for_cards(self, page, timeout_s):
        return _poll(page, "document.querySelectorAll('#atlas_search_results .cluster-card').length", timeout_s)

    def _search(self, page, query, console):
        page.goto(self.live_server_url + "/atlas/")
        page.wait_for_selector("#atlas_search_input", timeout=60000)
        # Wait on the app's OWN readiness flag (set behind the debug gate as the
        # last act of its boot), never on a MapLibre event: a plain /atlas/ load
        # spins the globe and never reaches map.loaded(). The mode-toggle handlers
        # are bound by that boot, so clicking earlier does nothing.
        booted = _poll(page, "window.__whgAtlas && (window.__whgAtlas.booted || window.__whgAtlas.error) && window.__whgAtlas", 90)
        if not booted or not booted.get("booted"):
            page.screenshot(path=os.path.join(SHOT_DIR, "atlas-never-booted.png"))
            diag = page.evaluate("({hidden: document.hidden, hasMap: !!window.heroMapInstance, flag: window.__whgAtlas || null})")
            self.fail(f"the Atlas never booted: {diag}; console tail: {console[-10:]}")
        # Mode switch and search in SEPARATE calls (the switch must settle first).
        page.evaluate("document.querySelector('.search-mode-toggle .btn[data-search-mode=\"toponyms\"]').click()")
        self.assertTrue(_poll(page, "document.querySelector('.search-mode-toggle .btn[data-search-mode=\"toponyms\"]').classList.contains('active')", 10),
                        f"the Places mode never became active; console tail: {console[-10:]}")
        page.evaluate(f"document.getElementById('atlas_search_input').value = {json.dumps(query)}")
        page.evaluate("document.getElementById('atlas_initiate_search').click()")

    # Assumes the v4 release switch is off: say so, clearing the alias too, so the test
    # holds where the env sets either (dev sets both; place#325 review #5).
    @override_settings(V4_LANDING_ENABLED=False, LOCUS_MINTING_ENABLED=False)
    def test_card_token_agrees_with_the_server_and_the_page_resolves_it(self):
        ctx = self._context()
        page = ctx.new_page()
        console = []
        page.on("console", lambda m: console.append(f"{m.type}: {m.text}"))
        page.on("pageerror", lambda e: console.append(f"pageerror: {e}"))
        self._search(page, "Berlin", console)
        n_cards = self._wait_for_cards(page, 90)
        page.screenshot(path=os.path.join(SHOT_DIR, "atlas-cards.png"))
        # Predicted in advance, not read off the page: gn:1 + wd:Q64 share an
        # embedding and a location (composite 1.0 >= any θ), gn:2 does not.
        self.assertEqual(n_cards, 2, f"cards never rendered; console tail: {console[-8:]}")
        self.assertEqual(len(self.gateway.bodies), 1, "the Atlas should have searched exactly once")
        body = self.gateway.bodies[0]

        cards = page.evaluate("""Array.from(document.querySelectorAll('#atlas_search_results .cluster-card')).map(c => ({
            pids: c.dataset.pids.split(' '),
            href: (c.querySelector('a.cluster-locus-open') || {}).getAttribute ? c.querySelector('a.cluster-locus-open').getAttribute('href') : null,
            unavailable: !!c.querySelector('.cluster-locus-unavailable'),
        }))""")
        self.assertTrue(all(c["href"] and not c["unavailable"] for c in cards), cards)
        multi = next(c for c in cards if len(c["pids"]) == 2)
        self.assertEqual(sorted(multi["pids"]), ["gn:1", "wd:Q64"])
        m = re.match(r"^/atlas/locus/\?r=([A-Za-z0-9_-]+)$", multi["href"])
        self.assertIsNotNone(m, multi["href"])
        tok = m.group(1)

        # The literal θ the render used is what the slider shows.
        theta_shown = float(page.evaluate("document.getElementById('atlas_cluster_theta_val').textContent"))
        self.assertEqual(theta_shown, ATLAS_DEFAULT_THETA)

        # (a) The server decodes the card's token to the card's members and the shipped literals.
        from_token = lr.decode_recipe_token(tok)
        self.assertEqual(from_token["members"], ["gn:1", "wd:Q64"])
        self.assertEqual(from_token["clustering"]["theta"], theta_shown)
        self.assertEqual(from_token["clustering"]["weights"], PARAMS["weights"])
        # Ruling 3: the stoplist is not in the card's recipe, though the response the
        # card was built beside carried the gateway's real 500-entry list, so the
        # token is short (it was ~10.5k characters and would 414 with it).
        self.assertEqual(len(SEARCH_OK["toponym_stoplist"]), 500)
        self.assertNotIn("stoplist", from_token["clustering"])
        self.assertIn("same_ns_penalty", from_token["clustering"])
        self.assertLess(len(tok), TOKEN_CEILING, len(tok))
        print(f"\n[locus browser] card token {len(tok)} chars; request line "
              f"{len('GET ' + multi['href'] + ' HTTP/1.1')} bytes")
        pid_from_token = lr.recipe_pid(from_token)

        # (b) Built independently in Python from the body the gateway RECEIVED,
        # the same recipe must hash to the same PID (cross-language parity on a
        # real captured body).
        expected = {
            "v": 1,
            "retrieval": lr.retrieval_from_search_body(body),
            "clustering": {
                "weights": PARAMS["weights"], "theta": theta_shown,
                "tau_name": PARAMS["thresholds"]["tau_name"], "tau_link": PARAMS["thresholds"]["tau_link"],
                "same_ns_penalty": PARAMS["same_ns_penalty"], "respect_distinct": True,
            },
            "members": multi["pids"],
        }
        self.assertEqual(lr.canonical_bytes(expected), lr.canonical_bytes(from_token))
        self.assertEqual(lr.recipe_pid(expected), pid_from_token)
        # The comparator can fail: one literal nudged changes the PID.
        nudged = json.loads(json.dumps(expected))
        nudged["clustering"]["theta"] = round(theta_shown + 0.01, 2)
        self.assertNotEqual(lr.recipe_pid(nudged), pid_from_token)

        # (c) The browser's own twin agrees with Python on this token.
        browser_pid = page.evaluate(
            "(async (t) => { const L = window.__whgAtlasLocus; return await L.recipePid(L.decodeRecipeToken(t)); })",
            tok)
        self.assertEqual(browser_pid, pid_from_token)
        self.assertRegex(browser_pid, lr.PID_RE)

        # (d) The page re-runs the recipe and selects the cluster the members name.
        resp = page.goto(self.live_server_url + multi["href"])
        self.assertEqual(resp.status, 200)
        state = _poll(page, "window.__whgLocus && window.__whgLocus.resolved !== undefined && window.__whgLocus", 60)
        page.screenshot(path=os.path.join(SHOT_DIR, "locus-page.png"), full_page=True)
        self.assertIsNotNone(state, f"the locus page never resolved; console tail: {console[-8:]}")
        self.assertIs(state["resolved"], True, state)
        self.assertEqual(state["reason"], "plurality")
        self.assertEqual(sorted(state["selectedIds"]), ["gn:1", "wd:Q64"])
        self.assertEqual(state["surviving"], ["gn:1", "wd:Q64"])
        self.assertEqual(state["missing"], [])
        self.assertEqual(state["hitCount"], 3)
        self.assertEqual(state["clusterCount"], 2)
        # The resolver clustered with the gateway's stoplist from ITS response.
        self.assertEqual(state["stoplistCount"], len(STOPLIST))
        self.assertEqual(len(self.gateway.bodies), 2, "the page should have re-run the retrieval once")
        # The re-run is the Atlas body plus the resolver's own opt-in for hit links, which is
        # not part of any recipe (locus_recipe._BODY_FIXED); nothing else may differ.
        self.assertEqual(self.gateway.bodies[1], {**body, "include_links": True},
                         "the re-run must send the body the recipe was made from, plus include_links")
        self.assertNotIn("include_links", body)

        links = page.evaluate("Array.from(document.querySelectorAll('#locus_result .locus-record-link')).map(a => a.getAttribute('href'))")
        self.assertEqual(sorted(links), ["/entity/place:gn:1/", "/entity/place:wd:Q64/"])
        text = page.evaluate("document.getElementById('locus_page').innerText")
        self.assertIn("shareable but not permanent", text)
        self.assertIn("2 of the recipe's 2 members are among them", text)
        stop_cell = page.evaluate("document.getElementById('locus_stoplist').innerText")
        self.assertIn("500 names: the gateway's current list", stop_cell)
        self.assertNotIn("once the re-run loads", stop_cell)
        self.assertTrue(page.evaluate("document.getElementById('locus_mint').disabled"))
        self.assertNotRegex(text, r"\b[0-9A-HJKMNP-TV-Y]{4}-[0-9A-HJKMNP-TV-Y]{4}-[0-9A-HJKMNP-TV-Y]{4}\b")
        good_member_count = len(links)
        self.assertGreater(good_member_count, 0)

        # (e) A tampered token: a 400 error page with no members. Same query,
        # different answer, in the same test as the good page.
        obj = json.loads(base64.urlsafe_b64decode(tok + "=" * (-len(tok) % 4)))
        obj["accepted"] = [["gn:1", "wd:Q64"]]
        bad = base64.urlsafe_b64encode(json.dumps(obj).encode()).decode().rstrip("=")
        resp = page.goto(self.live_server_url + "/atlas/locus/?r=" + bad)
        self.assertEqual(resp.status, 400)
        page.screenshot(path=os.path.join(SHOT_DIR, "locus-error.png"))
        err = page.evaluate("(document.querySelector('.locus-error') || {}).innerText || ''")
        self.assertIn("This locus link is not valid", err)
        self.assertIn("place#306", err)
        self.assertEqual(page.evaluate("document.querySelectorAll('#locus_result .locus-record-link').length"), 0)
        self.assertEqual(page.evaluate("document.querySelectorAll('#locus_recipe_members li').length"), 0)
        self.assertEqual(len(self.gateway.bodies), 2, "an invalid token must never reach the gateway")

    def test_the_harness_can_fail(self):
        """The card wait against a page with no Atlas reports never, and the
        locus state poll against a page with no locus reports never."""
        ctx = self._context()
        page = ctx.new_page()
        page.goto("data:text/html,<title>no atlas here</title><p>nothing</p>")
        t0 = time.monotonic()
        self.assertIsNone(self._wait_for_cards(page, 2))
        self.assertIsNone(_poll(page, "window.__whgLocus && window.__whgLocus.resolved !== undefined && window.__whgLocus", 1))
        self.assertLess(time.monotonic() - t0, 6, "the bounded waits overshot their budget")
        # And a non-beta visitor never sees the page (404), nor the resolver (403).
        User = get_user_model()
        normal = User.objects.create_user(username="locus_browser_normal", email="lbn@example.org",
                                          password="pw", given_name="N", surname="U", role="normal")
        self.client.force_login(normal)
        tok = lr.encode_recipe_token({"v": 1, "retrieval": {"query": "Berlin", "mode": "in", "size": 100},
                                      "clustering": {"weights": PARAMS["weights"], "theta": 0.5, "tau_name": 0.7,
                                                     "tau_link": 1, "same_ns_penalty": 0.1, "respect_distinct": True},
                                      "members": ["gn:1"]})
        self.assertEqual(self.client.get("/atlas/locus/", {"r": tok}).status_code, 404)
        self.assertEqual(self.client.post("/atlas/locus/resolve/", data=json.dumps({"r": tok}),
                                          content_type="application/json").status_code, 403)
        self.client.force_login(self.beta)
        self.assertEqual(self.client.get("/atlas/locus/", {"r": tok}).status_code, 200)
