"""Browser test: the site navbar never makes the page scroll sideways.

Every page extends ``main/base_webpack.html``. Its navbar expanded at
Bootstrap's ``lg`` breakpoint (992 px) for everyone, but a beta visitor's
expanded menu (Atlas / Gazetteers / Workbench beta pills, the snag bug icon,
plus the brand text "World Historical Gazetteer" on every page but the home
page) cannot be narrower than ~1054 px (its pills already squeezed onto three
lines), and lg's container leaves the menu 886 px, so from 992 to 1199 px the
whole document scrolled sideways (to 1132 px at 992). A beta user's nav now
expands at xl (1200 px); everyone else's still expands at lg.

Headless Playwright (its own bundled chromium, throwaway context; nothing
touches the user's Chrome, no window opens) against a Django live server. No
gateway is involved: the pages are the home page and /licenses/.
Run locally only:

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 WHG_BROWSER_TESTS=1 \\
        venv/bin/python manage.py test main.tests_navbar_overflow --settings=whg.settings_localtest

``WHG_BASE_BUNDLE_DIR=<dir>`` serves ``base.bundle.js``/``base.bundle.css``
from a scratch build instead of ``static/webpack/``, to test CSS not yet
bundled. ``WHG_NAVBAR_MEASURE=1`` prints each nav item's width per viewport;
``WHG_NAVBAR_PATHS=/a/,/b/`` checks other pages instead.

What it proves, and how it can fail:

* for a beta user, a signed-in non-beta user and an anonymous visitor, at
  every width in WIDTHS, ``document.documentElement.scrollWidth`` is no wider
  than the viewport, on the home page and on /licenses/ (which shows the brand
  text; /people_overview/ was not used because its own logos overflow at 768).
  Against the unfixed template the beta case on /licenses/ fails at 992, 1024,
  1100 and 1199 px (scrollWidth 1132, 1148, 1186, 1235);
* where the menu is collapsed (390 px for everyone; 1100 px for a beta user,
  whose nav now expands at xl) the hamburger opens it, it lists its items
  (beta: with the "Report a snag" label), and the open menu does not overflow;
* the same call asserts the nav is THERE and is the nav of that kind of
  visitor (the Atlas pill for beta, the user menu and no pill for non-beta,
  the Login link for anonymous), and that Bootstrap's CSS has actually applied
  (the toggler's display matches the width), so a page that never styled or
  never rendered its nav cannot pass by having nothing to overflow.

Skipped loudly when Playwright is not importable or WHG_BROWSER_TESTS is unset.
"""
import json
import os
import tempfile
import unittest

from django.conf import settings
from django.contrib.auth import get_user_model
from django.test import LiveServerTestCase
from django.urls import reverse

try:
    from playwright.sync_api import sync_playwright
    HAVE_PLAYWRIGHT = True
except ImportError:  # pragma: no cover
    HAVE_PLAYWRIGHT = False

BUNDLE_DIR = os.environ.get("WHG_BASE_BUNDLE_DIR") or ""
SHOT_DIR = os.environ.get("WHG_SHOT_DIR") or tempfile.mkdtemp(prefix="whg-navbar-shots-")
MEASURE = os.environ.get("WHG_NAVBAR_MEASURE") == "1"
WIDTHS = [320, 375, 768, 991, 992, 1024, 1100, 1199, 1200, 1440]
# Width at and above which the navbar is expanded (no hamburger). Read from the
# template's navbar-expand-* class, so the toggler check follows the template.
EXPAND_AT = {"navbar-expand-lg": 992, "navbar-expand-xl": 1200}

# Ready = base.js has run (it swaps #links_home/#links_other on non-home pages and
# sets window.Spinner) and Bootstrap's stylesheet is in force (.collapse:not(.show)
# is display:none below the breakpoint and the toggler is hidden above it; without
# Bootstrap the toggler is always shown, so test both sides in the probe below).
READY = ("window.Spinner && document.fonts && document.fonts.status === 'loaded' && "
         "[...document.styleSheets].some(s => { try { return /bootstrap/.test(s.href || ''); } catch (e) { return false; } })")

PROBE = """() => {
  const nav = document.querySelector('header nav.navbar');
  const collapse = document.getElementById('navbarSupportedContent');
  const toggler = nav && nav.querySelector('.navbar-toggler');
  const expandClass = nav ? [...nav.classList].find(c => c.startsWith('navbar-expand')) : null;
  const items = collapse ? [...collapse.querySelectorAll(':scope > ul > li, :scope > ul > div, :scope > ul > div > a')]
    .filter(el => el.offsetParent !== null)
    .map(el => ({
      what: (el.id || el.querySelector('a, span')?.id || el.textContent.replace(/\\s+/g, ' ').trim().slice(0, 28)
             || el.querySelector('i')?.className || el.tagName),
      w: Math.round(el.getBoundingClientRect().width * 10) / 10,
    })) : [];
  return {
    inner: window.innerWidth,
    scroll: document.documentElement.scrollWidth,
    navPresent: !!(nav && nav.offsetHeight > 0),
    expandClass,
    togglerDisplay: toggler ? getComputedStyle(toggler).display : null,
    collapseW: collapse ? Math.round(collapse.getBoundingClientRect().width) : null,
    collapseScrollW: collapse ? collapse.scrollWidth : null,
    headerRight: Math.round(Math.max(0, ...[...document.querySelectorAll('header *')]
      .filter(el => el.offsetParent !== null).map(el => el.getBoundingClientRect().right))),
    containerW: nav ? Math.round(nav.querySelector('.container').getBoundingClientRect().width) : null,
    atlasPill: !!document.querySelector('#navbarSupportedContent a[href^="/atlas/"]'),
    userMenu: !!document.querySelector('#navbarSupportedContent a.dropdown-toggle .fa-user'),
    login: !!document.querySelector('#navbarSupportedContent a[href*="login"]'),
    brandShown: (() => { const b = document.querySelector('#links_other'); return !!(b && b.offsetParent); })(),
    items,
  };
}"""


def _bundle_route(route, request):
    name = request.url.rsplit("/", 1)[-1].split("?", 1)[0]
    path = os.path.join(BUNDLE_DIR, name)
    if os.path.isfile(path):
        ctype = {"css": "text/css", "js": "application/javascript", "ttf": "font/ttf", "woff": "font/woff",
                 "woff2": "font/woff2", "png": "image/png", "gif": "image/gif", "svg": "image/svg+xml",
                 }.get(name.rsplit(".", 1)[-1], "application/octet-stream")
        with open(path, "rb") as f:
            route.fulfill(status=200, content_type=ctype, body=f.read())
    else:
        route.continue_()


@unittest.skipUnless(HAVE_PLAYWRIGHT, "playwright is not importable here")
@unittest.skipUnless(os.environ.get("WHG_BROWSER_TESTS") == "1", "set WHG_BROWSER_TESTS=1 to run browser tests")
class NavbarOverflowTests(LiveServerTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._prev_async_unsafe = os.environ.get("DJANGO_ALLOW_ASYNC_UNSAFE")
        os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
        cls._pw = sync_playwright().start()
        cls.browser = cls._pw.chromium.launch()
        print(f"\n[navbar] base bundle from {BUNDLE_DIR or 'static/webpack/'}; shots in {SHOT_DIR}")

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
            username="navbar_beta", email="navbar-beta@example.org", password="pw",
            given_name="Bea", surname="Tester", role="beta_tester")
        self.plain = User.objects.create_user(
            username="navbar_plain", email="navbar-plain@example.org", password="pw",
            given_name="Pat", surname="Plain")
        self.assertTrue(self.beta.can_access_beta)
        self.assertFalse(self.plain.can_access_beta)
        os.makedirs(SHOT_DIR, exist_ok=True)

    def _context(self, user, width):
        ctx = self.browser.new_context(viewport={"width": width, "height": 800})
        if user is not None:
            self.client.force_login(user)
            ctx.add_cookies([{
                "name": settings.SESSION_COOKIE_NAME,
                "value": self.client.cookies[settings.SESSION_COOKIE_NAME].value,
                "url": self.live_server_url,
            }])
        if BUNDLE_DIR:
            # The whole dir, not just base.bundle.*: its css names hashed font files.
            ctx.route("**/static/webpack/**", _bundle_route)
        return ctx

    def _load(self, page, path):
        page.goto(self.live_server_url + path)
        page.wait_for_function(READY, timeout=30000)

    def _check(self, kind, user, path):
        failures, table = [], []
        ctx = self._context(user, WIDTHS[0])
        try:
            page = ctx.new_page()
            self._load(page, path)
            for w in WIDTHS:
                page.set_viewport_size({"width": w, "height": 800})
                page.wait_for_timeout(150)
                m = page.evaluate(PROBE)
                # Presence, in the same call as the absence of overflow.
                self.assertTrue(m["navPresent"], f"{kind} {path} @{w}: no visible navbar: {m}")
                self.assertEqual(m["inner"], w, f"{kind} {path}: viewport is {m['inner']}, wanted {w}")
                expand_at = EXPAND_AT.get(m["expandClass"])
                self.assertIsNotNone(expand_at, f"unknown navbar class {m['expandClass']}")
                self.assertEqual(m["togglerDisplay"], "none" if w >= expand_at else "block",
                                 f"{kind} {path} @{w}: Bootstrap's navbar CSS is not in force: {m}")
                if kind == "beta":
                    self.assertTrue(m["atlasPill"] and m["userMenu"], f"beta nav lacks its items: {m}")
                elif kind == "non-beta":
                    self.assertTrue(m["userMenu"] and not m["atlasPill"], f"non-beta nav is wrong: {m}")
                else:
                    self.assertTrue(m["login"] and not m["userMenu"] and not m["atlasPill"],
                                    f"anonymous nav is wrong: {m}")
                if w >= expand_at and path != "/":
                    self.assertTrue(m["brandShown"], f"{kind} {path} @{w}: brand text not shown: {m}")
                table.append((w, m))
                if m["scroll"] > m["inner"]:
                    failures.append(f"@{w}: scrollWidth {m['scroll']} > innerWidth {m['inner']} "
                                    f"(collapse {m['collapseW']} wide, scrolls to {m['collapseScrollW']}; "
                                    f"container {m['containerW']}; rightmost header element ends at {m['headerRight']})")
            if MEASURE:
                print(f"\n[navbar] {kind} {path}")
                for w, m in table:
                    print(f"  {w}: scroll={m['scroll']} collapse={m['collapseW']}/{m['collapseScrollW']} "
                          f"container={m['containerW']} headerRight={m['headerRight']} items={json.dumps([(i['what'], i['w']) for i in m['items']])}")
            # Where the menu is collapsed, the hamburger opens it, it lists the items, and the open
            # menu does not overflow either (1100 is collapsed only where the nav expands at xl).
            for w in (390, 1100):
                if w >= expand_at:
                    continue
                page.set_viewport_size({"width": w, "height": 800})
                page.click("header .navbar-toggler")
                page.wait_for_function("document.querySelector('#navbarSupportedContent.collapse.show')", timeout=5000)
                o = page.evaluate("""() => ({
                  scroll: document.documentElement.scrollWidth, inner: window.innerWidth,
                  h: document.getElementById('navbarSupportedContent').getBoundingClientRect().height,
                  search: !!document.querySelector('#navbarSupportedContent a[href="/search/"]')?.offsetParent,
                  snagLabel: (() => { const s = document.querySelector('#atlas-report-snag span');
                                      return s ? !!s.offsetParent : null; })(),
                })""")
                self.assertTrue(o["h"] > 100 and o["search"], f"{kind} {path} @{w}: the opened menu is not shown: {o}")
                if kind == "beta":
                    self.assertTrue(o["snagLabel"], f"@{w}: the collapsed menu hides 'Report a snag': {o}")
                if o["scroll"] > o["inner"]:
                    failures.append(f"@{w} menu open: scrollWidth {o['scroll']} > innerWidth {o['inner']}")
                slug = "home" if path == "/" else "page"
                page.screenshot(path=os.path.join(SHOT_DIR, f"nav-{kind}-{slug}-{w}-open.png"),
                                clip={"x": 0, "y": 0, "width": w, "height": min(800, 80 + int(o["h"]))})
                page.click("header .navbar-toggler")
                page.wait_for_function("!document.querySelector('#navbarSupportedContent.show, #navbarSupportedContent.collapsing')",
                                       timeout=5000)
            # Screenshots (desktop, the lg..xl range, phone with the menu collapsed) for a human to look at.
            for w in (1280, 1100, 992, 390):
                page.set_viewport_size({"width": w, "height": 800})
                page.mouse.move(1, 799)  # off the menu: the toggler clicks left it hovering "Data"
                page.wait_for_timeout(150)
                slug = "home" if path == "/" else "page"
                page.screenshot(path=os.path.join(SHOT_DIR, f"nav-{kind}-{slug}-{w}.png"),
                                clip={"x": 0, "y": 0, "width": w, "height": 140})
        finally:
            ctx.close()
        self.assertEqual(failures, [], f"{kind} visitor on {path}: the page scrolls sideways")

    def _paths(self):
        override = os.environ.get("WHG_NAVBAR_PATHS")
        return override.split(",") if override else ["/", reverse("licenses")]

    def test_beta_user(self):
        for path in self._paths():
            with self.subTest(path=path):
                self._check("beta", self.beta, path)

    def test_signed_in_non_beta_user(self):
        for path in self._paths():
            with self.subTest(path=path):
                self._check("non-beta", self.plain, path)

    def test_anonymous_visitor(self):
        for path in self._paths():
            with self.subTest(path=path):
                self._check("anon", None, path)
