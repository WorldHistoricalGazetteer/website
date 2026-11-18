"""The About menu and its pages: /about/, /principles/ (switch-gated draft), /people_overview/,
and the permanent redirects from the retired v1/v2 siblings (/licensing/, /credits/, /system/).

Each absence asserted here is paired with a presence in the same response, so a page that failed
to render cannot pass for one that rendered without the thing.
"""
import re

from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings
from django.urls import reverse

from sitemap.views import StaticViewSitemap


def about_menu_hrefs(html):
    """The hrefs of the About dropdown, in order (base_webpack.html, ul#aboutDropdownMenu)."""
    m = re.search(r'<ul class="dropdown-menu[^"]*" id="aboutDropdownMenu">(.*?)</ul>', html, re.S)
    assert m, "About dropdown not found in the page"
    return re.findall(r'class="dropdown-item"\s+href="([^"]+)"', m.group(1))


class AboutPagesTests(TestCase):
    def setUp(self):
        User = get_user_model()
        self.staff = User.objects.create_user(username="ab_staff", email="ab-staff@example.org", password="pw",
                                              given_name="Sam", surname="Staff", is_staff=True)
        self.user = User.objects.create_user(username="ab_user", email="ab-user@example.org", password="pw",
                                             given_name="Una", surname="User")

    # -- status codes and redirects ---------------------------------------------------------------

    def test_about_and_people_pages_render(self):
        for name, path in (("about", "/about/"), ("credits", "/people_overview/"), ("licenses", "/licenses/")):
            self.assertEqual(reverse(name), path)
            self.assertEqual(self.client.get(path).status_code, 200, path)

    def test_retired_siblings_redirect_permanently(self):
        for old, new in (("/licensing/", "/licenses/"), ("/credits/", "/people_overview/"), ("/system/", "/about/")):
            resp = self.client.get(old)
            self.assertEqual(resp.status_code, 301, old)
            self.assertEqual(resp["Location"], new, old)
            self.assertEqual(self.client.get(resp["Location"]).status_code, 200, new)

    # -- the About page ---------------------------------------------------------------------------

    def test_about_page_is_current_and_carries_funding(self):
        html = self.client.get("/about/").content.decode()
        self.assertIn('id="funding"', html)
        self.assertIn("Funding and awards", html)
        self.assertIn("PW‑253719‑17", html)
        self.assertIn("Digital Humanities Advancement Grant", html)
        self.assertIn("Best DH Tool or Suite of Tools Award", html)
        self.assertIn("Our mission", html)
        self.assertNotIn("2019", html)  # the stale "Version 1 beta launched in October, 2019" text
        for name in ("credits", "publications", "licenses", "software-licenses", "terms_of_use"):
            self.assertIn(f'href="{reverse(name)}"', html, name)
        self.assertIn('href="https://docs.whgazetteer.org/"', html)

    def test_people_overview_points_to_moved_funding(self):
        html = self.client.get("/people_overview/").content.decode()
        self.assertIn("Current Project Team", html)
        self.assertIn('id="funding-pointer"', html)
        self.assertIn('href="/about/#funding"', html)
        self.assertNotIn("Digital Humanities Advancement Grant", html)
        self.assertIn('id="credits_right"', html)  # partner/funder logos stay here

    # -- the menu ---------------------------------------------------------------------------------

    def test_menu_order_anonymous_switch_off(self):
        hrefs = about_menu_hrefs(self.client.get("/about/").content.decode())
        self.assertEqual(hrefs[:4], ["/about/", "/people_overview/", "/publications/", "/licenses/"])
        self.assertNotIn("/principles/", hrefs)

    def test_menu_offers_draft_principles_to_staff_only(self):
        self.client.force_login(self.user)
        self.assertNotIn("/principles/", about_menu_hrefs(self.client.get("/about/").content.decode()))
        self.client.force_login(self.staff)
        html = self.client.get("/about/").content.decode()
        hrefs = about_menu_hrefs(html)
        self.assertEqual(hrefs[:3], ["/about/", "/principles/", "/people_overview/"])
        self.assertRegex(html, r'href="/principles/">Principles <span class="badge[^"]*">draft</span>')

    @override_settings(PRINCIPLES_PAGE_ENABLED=True)
    def test_menu_offers_principles_to_everyone_when_enabled(self):
        html = self.client.get("/about/").content.decode()
        self.assertEqual(about_menu_hrefs(html)[:3], ["/about/", "/principles/", "/people_overview/"])
        self.assertIn('href="/principles/">Principles</a>', html)

    def test_about_toggle_lists_its_active_paths(self):
        html = self.client.get("/about/").content.decode()
        m = re.search(r'id="aboutDropdown"[^>]*data-active-paths="([^"]+)"', html, re.S)
        self.assertIsNotNone(m)
        self.assertEqual(m.group(1).split(), ["/about/", "/people_overview/", "/publications/", "/licenses/",
                                              "/principles/", "/announcements/"])

    # -- the Principles page ----------------------------------------------------------------------

    def test_principles_hidden_when_switch_off(self):
        self.assertEqual(reverse("principles"), "/principles/")
        self.assertEqual(self.client.get("/principles/").status_code, 404)
        self.client.force_login(self.user)
        self.assertEqual(self.client.get("/principles/").status_code, 404)

    def test_principles_staff_preview_is_marked_draft_and_noindex(self):
        self.client.force_login(self.staff)
        resp = self.client.get("/principles/")
        self.assertEqual(resp.status_code, 200)
        html = resp.content.decode()
        self.assertIn("Principles of the World Historical Gazetteer", html)
        self.assertIn('id="principles-draft-banner"', html)
        self.assertIn('<meta name="robots" content="noindex, nofollow">', html)

    @override_settings(PRINCIPLES_PAGE_ENABLED=True)
    def test_principles_public_when_enabled(self):
        resp = self.client.get("/principles/")
        self.assertEqual(resp.status_code, 200)
        html = resp.content.decode()
        self.assertIn("Principles of the World Historical Gazetteer", html)
        self.assertNotIn('id="principles-draft-banner"', html)
        self.assertNotIn('content="noindex', html)
        # ...and the About page links to it.
        self.assertIn('href="/principles/"', self.client.get("/about/").content.decode())

    # -- sitemap ----------------------------------------------------------------------------------

    def test_sitemap_static_entries_switch_off(self):
        sm = StaticViewSitemap()
        locs = [sm.location(i) for i in sm.items()]
        for path in ("/about/", "/people_overview/", "/licenses/", "/publications/"):
            self.assertIn(path, locs)
        self.assertNotIn("/principles/", locs)
        for retired in ("/licensing/", "/credits/", "/system/"):
            self.assertNotIn(retired, locs)

    @override_settings(PRINCIPLES_PAGE_ENABLED=True)
    def test_sitemap_lists_principles_when_enabled(self):
        sm = StaticViewSitemap()
        locs = [sm.location(i) for i in sm.items()]
        self.assertIn("/about/", locs)
        self.assertIn("/principles/", locs)
