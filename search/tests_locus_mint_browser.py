"""Browser test for the minting modal (place#325 Stage 4, place#172).

Headless Playwright (its own bundled chromium, throwaway profile; nothing
touches the user's Chrome, no window opens) against a Django live server
with the CRC gateway faked in-process, as ``search.tests_locus_browser``.
Run locally only:

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 WHG_BROWSER_TESTS=1 \\
        venv/bin/python manage.py test search.tests_locus_mint_browser --settings=whg.settings_localtest

What it proves, and how each check can fail:

* with the setting ON, the unminted page's Mint button opens the modal, the
  modal names the PID the SERVER computes for the recipe (computed here in
  Python, not read back from the page) and says what is minted; the Mint
  button stays disabled until the confirmation box is ticked; confirming
  POSTs, the registry gains exactly one row bound to the recipe's canonical
  bytes, and the browser lands on /entity/locus:<PID>/, which re-runs the
  recipe to the same cluster the unminted page selected;
* with the setting OFF, the same page's button is disabled, there is no modal
  in the document, and nothing is minted;
* the harness can fail: waiting for the modal on a page without one reports
  "never".

Skipped loudly when Playwright is not importable or WHG_BROWSER_TESTS is unset.
"""
import os
import unittest
from unittest.mock import patch

from django.conf import settings
from django.contrib.auth import get_user_model
from django.test import LiveServerTestCase, override_settings

from places import locus_recipe as lr
from places.models_locus import LocusRegistryEntry
from search.tests_locus import GATEWAY, POST, SIMPLE, FakeGateway, _Resp, recipe, token
from search.tests_locus_browser import GL_ARGS, SHOT_DIR, _poll, _tiles_route

try:
    from playwright.sync_api import sync_playwright
    HAVE_PLAYWRIGHT = True
except ImportError:  # pragma: no cover
    HAVE_PLAYWRIGHT = False


@unittest.skipUnless(HAVE_PLAYWRIGHT, "playwright is not importable here")
@unittest.skipUnless(os.environ.get("WHG_BROWSER_TESTS") == "1", "set WHG_BROWSER_TESTS=1 to run browser tests")
@override_settings(CRC_GATEWAY_URL=GATEWAY, CRC_GATEWAY_TIMEOUT=2, LOCUS_MINTING_ENABLED=True)
class LocusMintBrowserTests(LiveServerTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
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
            username="locus_mint_browser_beta", email="lmb@example.org", password="pw",
            given_name="Bea", surname="Tester", role="beta_tester")
        self.gateway = FakeGateway()
        for target, side in ((POST, self.gateway), ("api.crc_client.requests.get", lambda *a, **k: _Resp(200))):
            p = patch(target, side_effect=side)
            p.start()
            self.addCleanup(p.stop)
        os.makedirs(SHOT_DIR, exist_ok=True)

    def _page(self):
        ctx = self.browser.new_context(viewport={"width": 1400, "height": 900})
        self.client.force_login(self.beta)
        ctx.add_cookies([{
            "name": settings.SESSION_COOKIE_NAME,
            "value": self.client.cookies[settings.SESSION_COOKIE_NAME].value,
            "url": self.live_server_url,
        }])
        ctx.add_init_script("try { localStorage.setItem('whg.debug', '1'); } catch (e) {}")
        ctx.route("https://tiles.whgazetteer.org/**", _tiles_route)
        self.addCleanup(ctx.close)
        page = ctx.new_page()
        console = []
        page.on("console", lambda m: console.append(f"{m.type}: {m.text}"))
        return page, console

    def test_modal_mints_the_recipe_and_lands_on_the_entity_uri(self):
        r = recipe(SIMPLE)
        pid = lr.recipe_pid(r)
        tok = token(SIMPLE)
        page, console = self._page()
        page.goto(self.live_server_url + "/atlas/locus/?r=" + tok)
        state = _poll(page, "window.__whgLocus && window.__whgLocus.resolved !== undefined && window.__whgLocus", 60)
        self.assertTrue(state and state.get("resolved"), f"unminted page never resolved: {state}; console: {console[-8:]}")
        selected_before = sorted(state["selectedIds"])
        self.assertTrue(selected_before, "the unminted page selected no cluster")
        self.assertFalse(page.evaluate("document.getElementById('locus_mint').disabled"))
        self.assertEqual(page.evaluate("document.querySelectorAll('#locus_mint_modal').length"), 1)

        page.evaluate("document.getElementById('locus_mint').click()")
        pre = _poll(page, "window.__whgLocus && window.__whgLocus.mint && window.__whgLocus", 30)
        self.assertTrue(pre and pre.get("mint") == "preflight", f"preflight never published: {pre}; console: {console[-8:]}")
        self.assertEqual(pre["pid"], pid)
        self.assertTrue(_poll(page, "document.getElementById('locus_mint_modal').classList.contains('show')", 10),
                        "the modal never became visible")
        page.screenshot(path=os.path.join(SHOT_DIR, "locus-mint-modal.png"))
        modal_text = page.evaluate("document.getElementById('locus_mint_modal').innerText")
        self.assertIn(f"/entity/locus:{pid}", modal_text)
        self.assertIn("names this recipe, not the records shown now", modal_text)
        self.assertIn("You may mint", modal_text)
        self.assertTrue(page.evaluate("document.getElementById('locus_mint_go').disabled"))
        self.assertEqual(LocusRegistryEntry.objects.count(), 0)          # opening the modal mints nothing

        page.evaluate("const c = document.getElementById('locus_mint_confirm'); c.checked = true; c.dispatchEvent(new Event('change'));")
        self.assertFalse(page.evaluate("document.getElementById('locus_mint_go').disabled"))
        page.evaluate("document.getElementById('locus_mint_go').click()")
        landed = _poll(page, f"location.pathname === '/entity/locus:{pid}/' && location.pathname", 30)
        self.assertEqual(landed, f"/entity/locus:{pid}/", f"never landed on the entity URI; console: {console[-8:]}")
        rows = list(LocusRegistryEntry.objects.all())
        self.assertEqual([e.pid for e in rows], [pid])
        self.assertEqual(rows[0].canonical_bytes, lr.canonical_bytes(r))
        self.assertEqual(rows[0].minted_by, self.beta)

        state = _poll(page, "window.__whgLocus && window.__whgLocus.resolved !== undefined && window.__whgLocus", 60)
        page.screenshot(path=os.path.join(SHOT_DIR, "locus-minted.png"), full_page=True)
        self.assertTrue(state and state.get("resolved"), f"minted page never resolved: {state}; console: {console[-8:]}")
        self.assertEqual(sorted(state["selectedIds"]), selected_before)   # same recipe, same cluster
        self.assertEqual(page.evaluate("document.querySelectorAll('.locus-minted-badge').length"), 1)
        self.assertEqual(page.evaluate("document.querySelectorAll('#locus_mint').length"), 0)
        self.assertIn("permanent; its membership is not", page.evaluate("document.body.innerText"))

    def test_with_the_setting_off_nothing_can_be_minted(self):
        tok = token(SIMPLE)
        page, console = self._page()
        with override_settings(LOCUS_MINTING_ENABLED=False):
            page.goto(self.live_server_url + "/atlas/locus/?r=" + tok)
            state = _poll(page, "window.__whgLocus && window.__whgLocus.resolved !== undefined && window.__whgLocus", 60)
            self.assertTrue(state and state.get("resolved"), f"page never resolved: {state}; console: {console[-8:]}")
            self.assertTrue(page.evaluate("document.getElementById('locus_mint').disabled"))
            self.assertEqual(page.evaluate("document.querySelectorAll('#locus_mint_modal').length"), 0)
            page.evaluate("document.getElementById('locus_mint').click()")
            self.assertIsNone(_poll(page, "window.__whgLocus && window.__whgLocus.mint && window.__whgLocus", 3))
            self.assertEqual(LocusRegistryEntry.objects.count(), 0)

    def test_the_harness_can_fail(self):
        page, _ = self._page()
        page.goto("data:text/html,<title>no locus here</title><p>nothing</p>")
        self.assertIsNone(_poll(page, "document.getElementById('locus_mint_modal') && document.getElementById('locus_mint_modal').classList.contains('show')", 2))
