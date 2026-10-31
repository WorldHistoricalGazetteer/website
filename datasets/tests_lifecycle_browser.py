"""place#340: ``datasets/static/datasets/lifecycle.js`` and the owner's panel, driven in a real
(headless) browser against a live server.

Opt-in, because it starts a browser and a live server:

    WHG_BROWSER_TESTS=1 GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test datasets.tests_lifecycle_browser --settings=whg.settings_localtest

Same harness as ``places.tests_record_links_browser``: Playwright's bundled chromium with a fresh
profile (never the user's Chrome, never a visible window), analytics hosts and every other
off-server host aborted. ``WHG_LIFECYCLE_JS_DIR`` serves ``lifecycle.js`` from a scratch copy, so a
deliberately broken copy can prove these tests fail (the shared checkout is never edited for that).

The state is always read back from the database, not from the page; every absence sits beside a
presence in the same test (the disabled Submit is followed by the enabled one; the dismissed
dialog by the accepted one; the error text by the success that reloads).
"""
import os
import unittest
from unittest import mock

from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.staticfiles.testing import StaticLiveServerTestCase
from django.test import Client, override_settings

from datasets.models import Dataset
from datasets.tests_lifecycle import patch_side_effects

RUN = os.environ.get("WHG_BROWSER_TESTS") == "1"
JS_DIR = os.environ.get("WHG_LIFECYCLE_JS_DIR") or ""
BLOCKED = ("analytics.whgazetteer.org", "errors.whgazetteer.org", "plausible", "computingplace.org")
LOCMEM = {**settings.CACHES, "default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache",
                                         "LOCATION": "lcb340"}}
PANEL = '[data-testid="lifecycle-panel"]'
SUBMIT_BTN = '[data-testid="lifecycle-submit-button"]'


@unittest.skipUnless(RUN, "set WHG_BROWSER_TESTS=1 to drive the lifecycle panel in headless Chromium")
@override_settings(CACHES=LOCMEM)
class LifecyclePanelBrowserTests(StaticLiveServerTestCase):
    @classmethod
    def setUpClass(cls):
        os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
        super().setUpClass()
        from playwright.sync_api import sync_playwright
        cls._pw = sync_playwright().start()
        cls.browser = cls._pw.chromium.launch(headless=True)  # bundled chromium, temp profile

    @classmethod
    def tearDownClass(cls):
        try:
            cls.browser.close()
            cls._pw.stop()
        finally:
            super().tearDownClass()

    def setUp(self):
        from django.core.cache import cache
        from licensing.models import License
        cache.clear()
        self.mocks = patch_side_effects(self)
        for target in ("datasets.tasks.index_dataset_on_publish",):
            p = mock.patch(target)
            p.start()
            self.addCleanup(p.stop)
        User = get_user_model()
        self.owner = User.objects.create_user(username="lcb_owner", email="lcb_owner@example.org", password="pw",
                                              given_name="Olive", surname="Owner", name="Olive Owner",
                                              role="beta_tester")
        licence, _ = License.objects.get_or_create(spdx_id="CC-BY-4.0", defaults={"label": "CC BY 4.0"})
        self.ds = Dataset.objects.create(owner=self.owner, label="lcb340", title="Browser lifecycle test",
                                         description="D", creator="X", public=False, license=licence,
                                         uri_base="https://example.org/places/")
        self.dialogs = []

    # ---- helpers
    def set_state(self, state, public=False, **extra):
        Dataset.objects.filter(pk=self.ds.pk).update(lifecycle_state=state, public=public, **extra)

    def db(self):
        return Dataset.objects.values("lifecycle_state", "public", "sensitivity_indigenous",
                                      "sensitivity_restricted").get(pk=self.ds.pk)

    def page(self, answer=None):
        """A signed-in page. ``answer`` is "accept" / "dismiss" for confirm dialogs; with None a
        dialog fails the test (nothing here should ask unexpectedly)."""
        c = Client()
        c.force_login(self.owner)
        ctx = self.browser.new_context(viewport={"width": 1280, "height": 1000})
        ctx.add_cookies([{"name": settings.SESSION_COOKIE_NAME,
                          "value": c.cookies[settings.SESSION_COOKIE_NAME].value, "url": self.live_server_url}])
        self.addCleanup(ctx.close)
        page = ctx.new_page()
        self.js_errors = []
        self.posts = []

        def route(r):
            url = r.request.url
            if not url.startswith(self.live_server_url) or any(h in url for h in BLOCKED):
                return r.abort()
            if url.split("?", 1)[0].endswith("/datasets/lifecycle.js") and JS_DIR:
                with open(os.path.join(JS_DIR, "lifecycle.js"), "rb") as f:
                    return r.fulfill(status=200, content_type="application/javascript", body=f.read())
            if r.request.method == "POST":
                self.posts.append(url)
            return r.continue_()
        page.route("**/*", route)
        page.on("pageerror", lambda e: self.js_errors.append(str(e)))

        def on_dialog(d):
            self.dialogs.append(d.message)
            if answer == "accept":
                d.accept()
            elif answer == "dismiss":
                d.dismiss()
            else:
                d.dismiss()
                self.fail(f"unexpected dialog: {d.message}")
        page.on("dialog", on_dialog)
        return page

    def status(self, page):
        resp = page.goto(f"{self.live_server_url}/datasets/{self.ds.pk}/status", wait_until="load")
        self.assertEqual(resp.status, 200)
        return page

    def wait_state(self, page, state):
        page.wait_for_selector(f'{PANEL}[data-state="{state}"]', timeout=15000)

    def js_ready(self, page):
        page.wait_for_function("window.__whgLifecycleBound === true", timeout=15000)

    # ---- tests
    def test_start_then_sensitivity_then_submit(self):
        self.set_state(None)
        page = self.page(answer="accept")
        self.status(page)
        self.assertEqual(page.get_attribute(PANEL, "data-state"), "none")
        self.assertEqual(page.locator(SUBMIT_BTN).count(), 0)            # not offered before Start
        self.js_ready(page)
        page.click('[data-testid="lifecycle-start"] button')
        page.wait_for_selector(f'{PANEL}[data-state="draft"]', timeout=15000)   # reload showed the draft panel
        self.assertEqual(self.db()["lifecycle_state"], "draft")
        self.assertEqual(len(self.dialogs), 1)
        self.assertIn("published only after that review", self.dialogs[0])
        self.assertEqual(page.locator('[data-testid="lifecycle-start"]').count(), 0)  # Start is gone
        self.assertEqual(page.locator(SUBMIT_BTN).count(), 1)             # Submit takes its place

        # Submit is disabled, and says why, until both answers are saved
        self.js_ready(page)
        btn = page.locator(SUBMIT_BTN)
        self.assertTrue(btn.is_disabled())
        blocked = page.locator('[data-testid="lifecycle-submit-blocked"]')
        self.assertIn("Answer the two questions", blocked.inner_text())
        page.check('input[name="sensitivity_indigenous"][value="no"]')
        page.check('input[name="sensitivity_restricted"][value="no"]')
        self.assertTrue(btn.is_disabled())                                # ticking alone changes nothing
        page.click('[data-testid="lifecycle-sensitivity"] button[type="submit"]')
        page.wait_for_selector(f'{PANEL}[data-state="draft"] {SUBMIT_BTN}:not([disabled])', timeout=15000)
        self.assertEqual(page.locator('[data-testid="lifecycle-submit-blocked"]').count(), 0)
        d = self.db()
        self.assertEqual((d["sensitivity_indigenous"], d["sensitivity_restricted"]), ("no", "no"))

        page.fill("#lifecycle_note", "Ready for you")
        page.click(SUBMIT_BTN)
        self.wait_state(page, "submitted")
        self.assertEqual(self.db()["lifecycle_state"], "submitted")
        self.assertEqual(page.locator(SUBMIT_BTN).count(), 0)
        self.assertEqual(len(self.dialogs), 1)                            # Submit asks nothing
        self.assertEqual(self.js_errors, [])

    def test_refused_action_shows_the_endpoints_message_beside_the_button(self):
        self.set_state("draft", sensitivity_indigenous="no", sensitivity_restricted="no")
        page = self.page()
        self.status(page)
        self.js_ready(page)
        btn = page.locator(SUBMIT_BTN)
        self.assertFalse(btn.is_disabled())                               # the page offers it
        err = page.locator('[data-testid="lifecycle-submit"] .lifecycle-error')
        self.assertEqual(err.inner_text(), "")
        self.set_state("accepted")                                        # the world moves on: the page is stale
        btn.click()
        page.wait_for_function(
            "document.querySelector('[data-testid=\"lifecycle-submit\"] .lifecycle-error').textContent.length > 0",
            timeout=15000)
        shown = err.inner_text()
        self.assertGreater(len(shown), 10)
        self.assertNotIn("Something went wrong", shown)                   # the endpoint's own words
        # the same words as the endpoint's JSON
        c = Client()
        c.force_login(self.owner)
        import json
        r = c.post(f"/datasets/{self.ds.pk}/lifecycle/submit/", "{}", content_type="application/json")
        self.assertEqual(r.status_code, 409)
        self.assertEqual(shown, json.loads(r.content)["error"])
        self.assertEqual(self.db()["lifecycle_state"], "accepted")        # refused: nothing changed
        self.assertFalse(btn.is_disabled())                               # and the button is usable again
        self.assertEqual(page.get_attribute(PANEL, "data-state"), "draft")  # page did not reload
        self.assertEqual(self.js_errors, [])

    def test_publish_confirmation_dismissed_then_accepted(self):
        self.set_state("accepted")
        page = self.page(answer="dismiss")
        self.status(page)
        self.js_ready(page)
        page.click('[data-testid="lifecycle-publish"] button')
        page.wait_for_timeout(1500)
        self.assertEqual(len(self.dialogs), 1)                            # it did ask
        self.assertIn("anyone can see and download it", self.dialogs[0])
        self.assertEqual(self.posts, [])                                  # and sent nothing
        self.assertEqual(self.db()["lifecycle_state"], "accepted")
        self.assertFalse(self.db()["public"])
        self.assertFalse(page.locator('[data-testid="lifecycle-publish"] button').is_disabled())

        page2 = self.page(answer="accept")                                # same state, now say yes
        self.status(page2)
        self.js_ready(page2)
        page2.click('[data-testid="lifecycle-publish"] button')
        self.wait_state(page2, "published")
        d = self.db()
        self.assertEqual((d["lifecycle_state"], d["public"]), ("published", True))
        self.assertEqual(len(self.posts), 1)

    def test_withdraw_confirmation_dismissed_then_accepted(self):
        self.set_state("published", public=True)
        page = self.page(answer="dismiss")
        self.status(page)
        self.js_ready(page)
        page.fill("#lifecycle_reason", "testing")
        page.click('[data-testid="lifecycle-withdraw"] button')
        page.wait_for_timeout(1500)
        self.assertEqual(len(self.dialogs), 1)
        self.assertIn("removed from the search index", self.dialogs[0])
        self.assertEqual(self.posts, [])
        d = self.db()
        self.assertEqual((d["lifecycle_state"], d["public"]), ("published", True))

        page2 = self.page(answer="accept")
        self.status(page2)
        self.js_ready(page2)
        page2.click('[data-testid="lifecycle-withdraw"] button')
        self.wait_state(page2, "withdrawn")
        d = self.db()
        self.assertEqual((d["lifecycle_state"], d["public"]), ("withdrawn", False))

    def test_public_null_dataset_shows_only_the_hint(self):
        self.set_state(None, public=True)
        page = self.page()
        self.status(page)
        hint = page.locator('[data-testid="lifecycle-public-hint"]')
        self.assertEqual(hint.inner_text(),
                         "To put this dataset through the review process, make it private first.")
        self.assertEqual(page.locator(PANEL).count(), 0)
        self.set_state(None, public=False)                                # control: private gets the Start control
        self.status(page)
        self.assertEqual(page.locator('[data-testid="lifecycle-start"]').count(), 1)
        self.assertEqual(page.locator('[data-testid="lifecycle-public-hint"]').count(), 0)

    def test_harness_can_fail_without_the_script(self):
        """With lifecycle.js blocked, the Start button is there (presence) but a click does
        nothing: no request, no state change."""
        self.set_state(None)
        page = self.page(answer="accept")
        page.route("**/datasets/lifecycle.js*", lambda r: r.abort())
        self.status(page)
        btn = page.locator('[data-testid="lifecycle-start"] button')
        self.assertEqual(btn.count(), 1)
        btn.click(no_wait_after=True)
        page.wait_for_timeout(1500)
        self.assertEqual(self.dialogs, [])
        self.assertEqual(self.posts, [])
        self.assertIsNone(self.db()["lifecycle_state"])
