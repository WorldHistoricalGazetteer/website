"""The staff admin dashboard (/dashboard_admin/): Tools is the first tab and the one open on
arrival, and every other way of choosing a tab still works.

Two layers:

* ``DashboardAdminMarkupTests`` (always runs): the server-rendered tab strip. Tools is the first
  ``.nav-link`` and the only active one, its pane is the only ``show active`` pane, every tab
  points at a pane that exists, and no template comment leaks into the page. Against the old
  template (Datasets first and active) ``test_tools_tab_is_first_and_active`` fails.
* ``DashboardAdminBrowserTests`` (headless Playwright, bundled chromium, throwaway context; run
  with WHG_BROWSER_TESTS=1): what the page's own script then does. Plain arrival stays on Tools;
  ``?section=<tab>`` and ``#<tab>`` open that tab AND load its list over AJAX; a reload keeps
  the tab that was open; a fresh arrival after visiting another tab goes back to Tools. Each
  check asserts the expected tab is active and that its list actually loaded (rows > 0), so a
  page whose script never ran cannot pass by leaving the markup default in place. Against the
  old template the arrival check fails (Datasets), as does ``#collections`` (hashes were
  ignored).

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 WHG_BROWSER_TESTS=1 \\
        venv/bin/python manage.py test main.tests_dashboard_admin --settings=whg.settings_localtest
"""
import os
import time
import unittest

from bs4 import BeautifulSoup
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.test import LiveServerTestCase, TestCase

from utils.livetest import drain_live_server_threads, stub_type_vocab

try:
    from playwright.sync_api import sync_playwright
    HAVE_PLAYWRIGHT = True
except ImportError:  # pragma: no cover
    HAVE_PLAYWRIGHT = False

URL = "/dashboard_admin/"
TABS = ["tools", "datasets", "collections", "areas", "groups"]


def _admin(username):
    user = get_user_model().objects.create_user(
        username=username, email=f"{username}@example.org", password="pw",
        given_name="Ad", surname="Min", is_staff=True)
    user.groups.add(Group.objects.get_or_create(name="whg_admins")[0])
    return user


class DashboardAdminMarkupTests(TestCase):
    def setUp(self):
        self.admin = _admin("dash_admin_markup")
        self.client.force_login(self.admin)

    def _soup(self, path=URL):
        r = self.client.get(path)
        self.assertEqual(r.status_code, 200)
        return BeautifulSoup(r.content.decode(), "html.parser")

    def test_tools_tab_is_first_and_active(self):
        soup = self._soup()
        links = soup.select("#adminTabs .nav-link")
        self.assertEqual([a["href"] for a in links], ["#" + t for t in TABS])  # presence + order
        self.assertIn("active", links[0]["class"])
        self.assertEqual(links[0]["aria-selected"], "true")
        self.assertEqual([a["href"] for a in links if "active" in a["class"]], ["#tools"])
        panes = soup.select("#adminTabsContent > .tab-pane")
        self.assertEqual(sorted(p["id"] for p in panes), sorted(TABS))
        self.assertEqual([p["id"] for p in panes if "active" in p["class"]], ["tools"])
        self.assertIn("show", soup.select_one("#tools")["class"])

    def test_every_tab_controls_its_own_pane(self):
        soup = self._soup()
        for a in soup.select("#adminTabs .nav-link"):
            self.assertEqual(a["aria-controls"], a["href"][1:], a)
            self.assertIsNotNone(soup.select_one(a["href"]), a)

    def test_tools_content_and_no_leaked_template_comments(self):
        r = self.client.get(URL)
        html = r.content.decode()
        soup = BeautifulSoup(html, "html.parser")
        tools = soup.select_one("#tools")
        hrefs = [a["href"] for a in tools.select("a")]
        for expected in ("/admin", "/dashboard_admin/analytics/", "https://errors.whgazetteer.org/"):
            self.assertIn(expected, hrefs)
        self.assertIsNotNone(tools.select_one("#grace-card"))  # admins see GRACE
        self.assertEqual(soup.title.string, "WHG · Admin dashboard")
        for leak in ("{#", "#}", "{% comment", "endcomment"):
            self.assertNotIn(leak, html)

    def test_section_param_is_passed_through_escaped(self):
        html = self.client.get(URL + "?section=datasets").content.decode()
        self.assertIn("known('datasets')", html)
        html = self.client.get(URL + "?section=x');alert(1);//").content.decode()
        self.assertNotIn("alert(1);//')", html)

    def test_anonymous_is_still_sent_to_login(self):
        self.client.logout()
        r = self.client.get(URL)
        self.assertEqual(r.status_code, 302)
        self.assertIn("login", r["Location"])


def _poll(page, js, timeout_s=15, every=0.2):
    """Main-world poll; a timeout returns None (a result, not a hang)."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            v = page.evaluate(js)
        except Exception:  # noqa: BLE001 - navigation mid-evaluate
            v = None
        if v:
            return v
        time.sleep(every)
    return None


STATE = """() => {
  const a = document.querySelector('#adminTabs .nav-link.active');
  if (!window.jQuery || !a) return null;
  const tab = a.getAttribute('href').slice(1);
  const pane = document.getElementById(tab);
  return {tab, paneActive: pane.classList.contains('active'),
          rows: pane.querySelectorAll('tbody tr').length,
          tools: !!pane.querySelector('.tool-link')};
}"""


@unittest.skipUnless(HAVE_PLAYWRIGHT, "playwright is not importable here")
@unittest.skipUnless(os.environ.get("WHG_BROWSER_TESTS") == "1", "set WHG_BROWSER_TESTS=1 to run browser tests")
class DashboardAdminBrowserTests(LiveServerTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._prev_async_unsafe = os.environ.get("DJANGO_ALLOW_ASYNC_UNSAFE")
        os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
        cls._pw = sync_playwright().start()
        cls.browser = cls._pw.chromium.launch()  # no map on this page: no GL flags needed

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
            drain_live_server_threads()

    def setUp(self):
        stub_type_vocab(self)
        from collection.models import Collection, CollectionGroup
        from areas.models import Area
        from datasets.models import Dataset
        from places.models import Place
        self.admin = _admin("dash_admin_browser")
        ds = Dataset.objects.create(label="dash_ds", title="Dash dataset", description="d",
                                    owner=self.admin, numrows=1, ds_status="uploaded")
        Place.objects.create(dataset=ds, src_id="p1", title="P1", ccodes=[])
        Collection.objects.create(title="Dash coll", description="d", collection_class="place",
                                  status="sandbox", owner=self.admin)
        Area.objects.create(title="Dash area", type="drawn", description="d", owner=self.admin,
                            geojson={"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]]})
        CollectionGroup.objects.create(title="Dash group", type="class", owner=self.admin)

        self.ctx = self.browser.new_context(viewport={"width": 1280, "height": 900})
        self.client.force_login(self.admin)
        self.ctx.add_cookies([{"name": settings.SESSION_COOKIE_NAME,
                               "value": self.client.cookies[settings.SESSION_COOKIE_NAME].value,
                               "url": self.live_server_url}])
        self.addCleanup(self.ctx.close)

    def _open(self, page, suffix=""):
        page.goto(self.live_server_url + URL + suffix)
        return self._settled(page)

    def _settled(self, page, want=None):
        # Settled = jQuery is up, the active tab's pane is active, and (for a list tab) its rows
        # have arrived. Returns the state either way so a failure message shows what was there.
        cond = f"(s => s && s.paneActive && (s.tab === 'tools' ? s.tools : s.rows > 0) " \
               f"{'&& s.tab === ' + repr(want) if want else ''} && s)(({STATE})())"
        return _poll(page, cond) or page.evaluate(STATE)

    def test_arrival_opens_tools(self):
        page = self.ctx.new_page()
        s = self._open(page)
        self.assertEqual(s and s["tab"], "tools", s)
        self.assertTrue(s["tools"], s)

    def test_deep_links_still_select_their_tab(self):
        for suffix, tab in (("?section=datasets", "datasets"), ("?section=groups", "groups"),
                            ("#collections", "collections"), ("#areas", "areas")):
            page = self.ctx.new_page()
            page.goto(self.live_server_url + URL + suffix)
            s = self._settled(page, tab)
            self.assertEqual(s and s["tab"], tab, f"{suffix}: {s}")
            self.assertGreater(s["rows"], 0, f"{suffix}: list never loaded: {s}")
            page.close()

    def test_reload_keeps_tab_but_fresh_arrival_goes_to_tools(self):
        page = self.ctx.new_page()
        self.assertEqual(self._open(page)["tab"], "tools")
        page.evaluate("() => document.querySelector('#datasets-tab').click()")
        s = self._settled(page, "datasets")
        self.assertEqual(s and s["tab"], "datasets", s)
        page.reload()
        s = self._settled(page, "datasets")
        self.assertEqual(s and s["tab"], "datasets", f"reload lost the tab: {s}")
        page.goto(self.live_server_url + "/licenses/")
        s = self._open(page)
        self.assertEqual(s and s["tab"], "tools", f"fresh arrival: {s}")
