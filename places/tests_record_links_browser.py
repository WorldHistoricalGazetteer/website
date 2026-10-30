"""place#343: the "Links made by people" section of the Record page, and the Profile checkbox
"Show my name on Links I make", in HEADLESS Chromium.

Opt-in, because it starts a browser and a live server:

    WHG_BROWSER_TESTS=1 GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test places.tests_record_links_browser --settings=whg.settings_localtest

Same harness as ``places.tests_record_layout_browser``: Playwright's bundled chromium with a
fresh profile (never the user's Chrome, never a visible window), the CRC gateway faked at
``api.crc_client``, analytics hosts blocked, base-map tiles not served. ``WHG_RECORD_BUNDLE_DIR``
serves ``atlas_record.bundle.*`` from a scratch build, as there.

Every absence is asserted beside a presence in the same test (the shown Link is in the DOM
whenever the pending / withdrawn / private ones are asserted absent), and
``test_harness_can_fail`` blocks the page bundle and requires the dispute click to do nothing.
"""
import os
import unittest
from unittest.mock import patch

from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.gis.geos import Point
from django.test import Client, LiveServerTestCase, override_settings

RUN = os.environ.get("WHG_BROWSER_TESTS") == "1"
BUNDLE_DIR = os.environ.get("WHG_RECORD_BUNDLE_DIR") or ""
BLOCKED_HOSTS = ("analytics.whgazetteer.org", "errors.whgazetteer.org", "plausible", "computingplace.org")
LOCMEM = {**settings.CACHES, "default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache",
                                         "LOCATION": "rlk343"}}
SECTION = "#record_public_links"


def bundle_route(route, request):
    name = request.url.rsplit("/", 1)[-1].split("?", 1)[0]
    path = os.path.join(BUNDLE_DIR, name)
    if BUNDLE_DIR and os.path.isfile(path):
        ctype = "text/css" if name.endswith(".css") else "application/javascript"
        with open(path, "rb") as f:
            return route.fulfill(status=200, content_type=ctype, body=f.read())
    return route.continue_()


class Base(LiveServerTestCase):
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
        from datasets.models import Dataset
        from django.core.cache import cache
        from places.models import Place, PlaceGeom, PlaceName

        cache.clear()
        User = get_user_model()
        # The viewer: a beta user with a verified email, who is also the maker in the Profile test.
        self.beta = User.objects.create_user(username="rlk_beta", email="rlk_beta@example.org", password="pw",
                                             given_name="Bea", surname="Tester", role="beta_tester")
        self.beta.email_confirmed = True
        self.beta.save()
        self.maker = User.objects.create_user(username="rlk_maker", email="rlk_maker@example.org", password="pw",
                                              given_name="Maria", surname="Maker")
        self.plain = User.objects.create_user(username="rlk_plain", email="rlk_plain@example.org", password="pw",
                                              given_name="Pat", surname="Plain")

        def ds(key, public=True):
            with patch("datasets.signals.doi"):
                return Dataset.objects.create(owner=self.maker, label=f"rlk_{key}", title=f"Dataset {key}",
                                              description="D", creator="X", public=public, ds_status="indexed",
                                              uri_base="https://example.org/places/")
        pub, priv = ds("pub"), ds("priv", public=False)

        def place(d, title, src, xy):
            p = Place.objects.create(title=title, src_id=src, dataset=d, ccodes=["GR"])
            PlaceGeom.objects.create(place=p, geom=Point(*xy, srid=4326), jsonb={"type": "Point", "coordinates": list(xy)})
            PlaceName.objects.create(place=p, toponym=title, jsonb={"toponym": title})
            return p
        self.R = place(pub, "Subject Larissa", "R1", (22.4, 39.6))
        self.shown = place(pub, "Shown Twin", "S1", (22.5, 39.6))
        self.pending = place(pub, "Pending Twin", "P1", (22.6, 39.6))
        self.withdrawn = place(pub, "Withdrawn Twin", "W1", (22.7, 39.6))
        self.private = place(priv, "Private Twin", "V1", (22.8, 39.6))
        self.link(self.shown, "active")
        self.link(self.pending, "pending")
        self.link(self.withdrawn, "withdrawn")
        self.link(self.private, "active")       # should never exist, but the page must not rely on that
        for p in (patch("api.crc_client.crc_clustering_params", return_value=(None, "unreachable")),
                  patch("api.crc_client.crc_fetch_places", side_effect=lambda ids, **kw: {}),
                  patch("api.signals.forward_attestation_post"), patch("api.signals.forward_attestation_revoke")):
            p.start()
            self.addCleanup(p.stop)

    def gid(self, p):
        return f"whg:{p.dataset.pk}:{p.src_id}"

    def link(self, other, status, relation="closeMatch", user=None):
        from api.models import ContributorAttestation
        ids = sorted([(self.gid(self.R), self.R.id), (self.gid(other), other.id)])
        return ContributorAttestation.objects.create(
            user=user or self.maker, dataset_id=f"whg:{self.R.dataset.pk}", place_a=ids[0][0], place_b=ids[1][0],
            place_a_pk=ids[0][1], place_b_pk=ids[1][1], relation_type=relation, status=status)

    def new_page(self, user=None, block=None):
        ctx = self.browser.new_context(viewport={"width": 1280, "height": 1000})
        if user is not None:
            c = Client()
            c.force_login(user)
            ctx.add_cookies([{"name": settings.SESSION_COOKIE_NAME,
                              "value": c.cookies[settings.SESSION_COOKIE_NAME].value, "url": self.live_server_url}])
        self.addCleanup(ctx.close)
        page = ctx.new_page()
        self.errors = []

        def route(r):
            url = r.request.url
            if any(h in url for h in BLOCKED_HOSTS) or (block and block in url):
                return r.abort()
            if "/styles/" in url and url.endswith("style.json"):
                return r.abort()
            if "/static/webpack/atlas_record.bundle." in url:
                return bundle_route(r, r.request)
            return r.continue_()
        page.route("**/*", route)
        page.on("pageerror", lambda e: self.errors.append(str(e)))
        return page

    def goto_record(self, page):
        resp = page.goto(f"{self.live_server_url}/atlas/place/{self.R.pk}/", wait_until="domcontentloaded")
        return resp.status


@unittest.skipUnless(RUN, "set WHG_BROWSER_TESTS=1 to drive the Record page Links section in headless Chromium")
@override_settings(CACHES=LOCMEM, V4_LANDING_ENABLED=False)
class RecordLinksSectionBrowserTests(Base):
    def section_text(self, page):
        return page.inner_text(SECTION)

    def test_beta_user_sees_the_active_link_and_nothing_else(self):
        page = self.new_page(self.beta)
        self.assertEqual(self.goto_record(page), 200)
        page.wait_for_selector(f"{SECTION} .record-public-link")
        text = self.section_text(page)
        # presence: the one active Link, in plain words
        self.assertEqual(page.locator(f"{SECTION} .record-public-link").count(), 1)
        self.assertIn("Same place", text)
        self.assertIn("Shown Twin", text)
        self.assertIn("Linked by Maria Maker", text)       # a beta viewer is always told the maker
        self.assertIn("CC BY 4.0", text)
        self.assertEqual(page.locator(f"{SECTION} button.record-link-dispute").inner_text(), "Dispute this Link")
        self.assertEqual(page.get_attribute(SECTION, "data-total"), "1")
        # absence, whole page (not only the section): pending, withdrawn, private never reach the DOM
        html = page.content()
        for title in ("Pending Twin", "Withdrawn Twin", "Private Twin"):
            self.assertNotIn(title, html)
        for p in (self.pending, self.withdrawn, self.private):
            self.assertNotIn(f"/atlas/place/{p.pk}/", html)
            self.assertNotIn(self.gid(p), html)
        self.assertNotIn("relation_type", html)
        self.assertEqual(self.errors, [])

    def test_dispute_opens_the_link_sheet_preset_to_the_opposite_answer(self):
        page = self.new_page(self.beta)
        self.goto_record(page)
        btn = page.locator(f"{SECTION} button.record-link-dispute")
        btn.wait_for()
        self.assertEqual(btn.get_attribute("data-link-relation"), "distinct")
        self.assertEqual(page.locator(".whg-link-sheet.show").count(), 0)  # not open before the click
        # no wait on window.__whgRecord: the button itself is the readiness signal
        page.wait_for_selector(f"{SECTION} button.record-link-dispute:not([disabled])", timeout=30000)
        btn.click()
        page.wait_for_selector(".whg-link-sheet.show", timeout=15000)
        self.assertEqual(page.locator('input[name="whg_link_relation"]:checked').get_attribute("value"), "distinct")
        self.assertEqual(page.inner_text('label[for="whg_link_rel_distinct"]'), "Not the same place")
        self.assertEqual(page.locator('input[name="whg_link_relation"]:checked').count(), 1)
        self.assertIn("Shown Twin", page.inner_text(".whg-link-pair"))

    def test_signed_in_non_beta_cannot_open_the_page(self):
        plain = self.new_page(self.plain)
        self.assertEqual(self.goto_record(plain), 404)
        self.assertEqual(plain.locator(SECTION).count(), 0)
        self.assertNotIn("Shown Twin", plain.content())
        beta = self.new_page(self.beta)                                    # control: same URL, 200 + the section
        self.assertEqual(self.goto_record(beta), 200)
        self.assertEqual(beta.locator(SECTION).count(), 1)

    def test_anonymous_is_sent_to_login(self):
        page = self.new_page(None)
        self.goto_record(page)
        self.assertIn("login", page.url)
        self.assertEqual(page.locator(SECTION).count(), 0)

    def test_harness_can_fail(self):
        """With the page bundle blocked the server-rendered button is still there (presence)
        but it stays disabled, says why after the timeout, and a click opens no sheet."""
        page = self.new_page(self.beta, block="atlas_record.bundle.js")
        page.add_init_script("window.__whgLinkBtnTimeoutMs = 1500")
        self.goto_record(page)
        btn = page.locator(f"{SECTION} button.record-link-dispute")
        btn.wait_for()
        self.assertTrue(btn.is_disabled())
        self.assertEqual(btn.get_attribute("title"), "Loading\u2026")
        page.wait_for_function(
            "document.querySelector('#record_public_links button.record-link-dispute').title.startsWith('This did not load')",
            timeout=10000)
        self.assertEqual(btn.get_attribute("title"), "This did not load. Reload the page to try again.")
        self.assertTrue(btn.is_disabled())
        btn.click(force=True)
        page.wait_for_timeout(1000)
        self.assertEqual(page.locator(".whg-link-sheet.show").count(), 0)
        self.assertIsNone(page.evaluate("window.__whgRecord || null"))

    def test_buttons_are_disabled_until_the_bundle_is_ready_then_enabled(self):
        page = self.new_page(self.beta)
        self.goto_record(page)
        btn = page.locator(f"{SECTION} button.record-link-dispute")
        btn.wait_for()
        # the server-rendered markup is disabled (raw response, before any script runs)
        raw = page.request.get(f"{self.live_server_url}/atlas/place/{self.R.pk}/").text()
        self.assertRegex(raw, r'record-link-dispute[^>]*disabled')
        page.wait_for_function("!!window.__whgRecord", timeout=30000)
        page.wait_for_function(
            "!document.querySelector('#record_public_links button.record-link-dispute').disabled", timeout=5000)
        self.assertFalse(btn.is_disabled())
        self.assertIsNone(btn.get_attribute("title"))


@unittest.skipUnless(RUN, "set WHG_BROWSER_TESTS=1 to drive the Profile page in headless Chromium")
@override_settings(CACHES=LOCMEM, V4_LANDING_ENABLED=False)
class ProfileLinkNameBrowserTests(Base):
    LABEL = "Show my name on Links I make"

    def anon_text(self):
        """The Links section as a signed-out visitor sees it (the page is public once the
        release switch is on; the Profile control's effect is on exactly this view)."""
        with override_settings(V4_LANDING_ENABLED=True):
            page = self.new_page(None)
            self.assertEqual(self.goto_record(page), 200)
            page.wait_for_selector(f"{SECTION} .record-public-link")
            self.assertEqual(page.locator(f"{SECTION} .record-public-link").count(), 1)
            return page.inner_text(SECTION)

    def test_checkbox_persists_and_decides_whether_the_record_page_names_the_maker(self):
        # the Link is made by the beta user themself here
        from api.models import ContributorAttestation
        ContributorAttestation.objects.update(user=self.beta)
        page = self.new_page(self.beta)
        page.goto(f"{self.live_server_url}/profile/", wait_until="domcontentloaded")
        box = page.locator("#id_show_name_on_links")
        box.wait_for()
        self.assertEqual(page.inner_text('label[for="id_show_name_on_links"]').strip(), self.LABEL)
        self.assertFalse(box.is_checked())
        before = self.anon_text()
        self.assertIn("Linked by a WHG contributor", before)
        self.assertNotIn("Bea Tester", before)

        box.check()
        page.click("#link-name-form button[type=submit]")
        page.wait_for_url("**/profile/#link-name-form")
        page.reload(wait_until="domcontentloaded")
        self.assertTrue(page.locator("#id_show_name_on_links").is_checked())     # persisted
        self.beta.refresh_from_db()
        self.assertTrue(self.beta.show_name_on_links)
        after = self.anon_text()
        self.assertIn("Linked by Bea Tester", after)
        self.assertNotIn("a WHG contributor", after)

        page.locator("#id_show_name_on_links").uncheck()                           # and back off
        page.click("#link-name-form button[type=submit]")
        page.wait_for_url("**/profile/#link-name-form")
        page.reload(wait_until="domcontentloaded")
        self.assertFalse(page.locator("#id_show_name_on_links").is_checked())
        again = self.anon_text()
        self.assertIn("Linked by a WHG contributor", again)
        self.assertNotIn("Bea Tester", again)

    def test_control_is_offered_only_where_linking_is_open_and_email_verified(self):
        page = self.new_page(self.beta)
        page.goto(f"{self.live_server_url}/profile/", wait_until="domcontentloaded")
        self.assertEqual(page.locator("#id_show_name_on_links").count(), 1)          # presence
        # a non-beta user, same page, release switch off: the Preferences section is there, the control is not
        self.plain.email_confirmed = True
        self.plain.save()
        other = self.new_page(self.plain)
        other.goto(f"{self.live_server_url}/profile/", wait_until="domcontentloaded")
        self.assertEqual(other.locator("#news-form").count(), 1)
        self.assertEqual(other.locator("#id_show_name_on_links").count(), 0)
