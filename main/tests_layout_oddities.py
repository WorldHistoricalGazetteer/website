"""Three layout oddities the navbar overflow fix (main/tests_navbar_overflow.py) found and left alone.

1. ``NavMarkupTests`` (plain TestCase, always runs): the site header's markup
   is well formed. The rendered ``<header>`` is parsed with the stdlib
   ``html.parser`` keeping a stack of open elements: every end tag must close
   the element on top of the stack, everything opened must be closed, and a
   ``<ul>`` may only hold ``<li>`` children. Checked for an anonymous visitor,
   a signed-in user, a beta tester and a staff user (the Data menu's
   divider/dashboard links and the About menu's staff entry are conditional).
   The same call asserts the menus are THERE: the Data toggle's own dropdown
   holds Published Datasets ... Suggest a Source, so a header that rendered
   nothing cannot pass by having nothing to mis-nest.

2. ``BetaPillTests`` (browser): between 1200 and 1399 px a beta user's nav
   (Atlas / Gazetteers / Workbench beta pills, bug icon, and on every page but
   the home page the brand text) had ~85 px too little room in the 1140 px
   container, so the Atlas and Gazetteers pills wrapped onto three lines each
   and the header grew to 108 px. Each pill must now be one line box (one
   client rect, its own height no taller than one line) at 1200, 1280, 1366
   and 1399 px, and the document must not scroll sideways.

3. ``PeopleOverviewTests`` (browser): /people_overview/'s funder logos had
   fixed widths (up to 252 px) in a ``col-sm-3`` column that is 156 px wide at
   768, so the page scrolled sideways (scrollWidth 828 at 768). No horizontal
   overflow at 320, 375, 768, 1024 and 1440, and every logo is loaded and no
   wider than its column.

Browser tests: headless Playwright (its own chromium, throwaway contexts;
never the user's Chrome) against a Django live server, as in
main/tests_navbar_overflow.py, whose ``WHG_BASE_BUNDLE_DIR`` (serve a scratch
build of base.bundle.*) and ``WHG_SHOT_DIR`` they honour. Run locally only:

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 WHG_BROWSER_TESTS=1 \\
        venv/bin/python manage.py test main.tests_layout_oddities --settings=whg.settings_localtest
"""
import os
import unittest
from html.parser import HTMLParser

from django.conf import settings
from django.contrib.auth import get_user_model
from django.test import LiveServerTestCase, TestCase
from django.urls import reverse

from main import tests_navbar_overflow as navtests
from main.tests_navbar_overflow import HAVE_PLAYWRIGHT, READY, SHOT_DIR, _bundle_route

if HAVE_PLAYWRIGHT:
    from playwright.sync_api import sync_playwright

VOID = {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "source", "track", "wbr",
        # SVG elements the header writes self-closed (<stop/>, <rect/>) arrive via handle_startendtag.
        }
# Elements whose end tag HTML lets you omit; the header writes them all out, so they are checked like the rest.


class _HeaderNesting(HTMLParser):
    """Stack of open elements inside <header>; records every nesting fault."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.stack, self.faults, self.in_header, self.done = [], [], False, False
        self.ul_children = []  # (ul description, child tag) for every direct child of a <ul>
        self.data_menu = None  # texts of the links inside the Data toggle's own dropdown
        self._data_toggle_seen = False
        self._data_ul_depth = None
        self._text = []

    def _where(self):
        return "line %d: %s" % (self.getpos()[0], " > ".join(t for t, _ in self.stack[-4:]))

    def handle_starttag(self, tag, attrs):
        if self.done:
            return
        if tag == "header" and not self.in_header:
            self.in_header = True
        if not self.in_header:
            return
        if self.stack and self.stack[-1][0] == "ul":
            self.ul_children.append((self.stack[-1][1], tag, self._where()))
        if tag in VOID:
            return
        cls = dict(attrs).get("class") or ""
        self.stack.append((tag, cls))
        if tag == "ul" and self._data_toggle_seen and self._data_ul_depth is None:
            self._data_ul_depth = len(self.stack)
            self.data_menu = []
        if tag == "a":
            self._text = []

    def handle_startendtag(self, tag, attrs):
        if self.in_header and not self.done and self.stack and self.stack[-1][0] == "ul":
            self.ul_children.append((self.stack[-1][1], tag, self._where()))

    def handle_data(self, data):
        if self.in_header and not self.done:
            self._text.append(data)

    def handle_endtag(self, tag):
        if not self.in_header or self.done or tag in VOID:
            return
        if tag == "a" and self.stack and self.stack[-1][0] == "a":
            text = " ".join("".join(self._text).split())
            if text == "Data" and not self._data_toggle_seen:
                self._data_toggle_seen = True
            elif self._data_ul_depth and self._data_ul_depth > 0 and len(self.stack) > self._data_ul_depth:
                self.data_menu.append(text)
            self._text = []
        if not self.stack:
            self.faults.append(f"</{tag}> with nothing open ({self._where()})")
            return
        if self.stack[-1][0] != tag:
            self.faults.append(f"</{tag}> closes <{self.stack[-1][0]} class='{self.stack[-1][1]}'> ({self._where()})")
            # Recover the way a browser would: pop to the matching element if there is one.
            for i in range(len(self.stack) - 1, -1, -1):
                if self.stack[i][0] == tag:
                    del self.stack[i:]
                    break
        else:
            if tag == "ul" and self._data_ul_depth == len(self.stack):
                self._data_ul_depth = -1  # the Data dropdown is closed; collect no more
            self.stack.pop()
        if tag == "header":
            self.done = True
            if self.stack:
                self.faults.append(f"<header> closed with {[t for t, _ in self.stack]} still open")


def check_header(html):
    p = _HeaderNesting()
    p.feed(html)
    p.close()
    return p


class NavMarkupTests(TestCase):
    """The header's ul/li nesting is well formed for every kind of visitor."""

    def setUp(self):
        User = get_user_model()
        self.users = {
            "anon": None,
            "plain": User.objects.create_user(username="nav_plain", email="nav-plain@example.org", password="pw",
                                              given_name="Pat", surname="Plain"),
            "beta": User.objects.create_user(username="nav_beta", email="nav-beta@example.org", password="pw",
                                             given_name="Bea", surname="Tester", role="beta_tester"),
            "staff": User.objects.create_user(username="nav_staff", email="nav-staff@example.org", password="pw",
                                              given_name="Sam", surname="Staff", is_staff=True),
        }

    def test_header_nesting(self):
        expected_data = ["Published Datasets", "Published Collections", "API", "Volunteering", "Suggest a Source"]
        for kind, user in self.users.items():
            for path in ("/", reverse("licenses")):
                with self.subTest(kind=kind, path=path):
                    self.client.logout()
                    if user is not None:
                        self.client.force_login(user)
                    resp = self.client.get(path)
                    self.assertEqual(resp.status_code, 200)
                    p = check_header(resp.content.decode())
                    # Presence: the header was parsed to its end and the Data menu is in its own dropdown.
                    self.assertTrue(p.in_header and p.done, f"{kind} {path}: no complete <header> found")
                    self.assertIsNotNone(p.data_menu, f"{kind} {path}: no Data dropdown found")
                    for label in expected_data:
                        self.assertIn(label, p.data_menu, f"{kind} {path}: Data menu lacks {label!r}: {p.data_menu}")
                    if user is not None:
                        self.assertIn("My Data", p.data_menu)
                    if kind == "beta":
                        self.assertIn("navbar-expand-xl", resp.content.decode())
                    self.assertGreater(len(p.ul_children), 20, f"{kind} {path}: too few list items parsed")
                    # Absence of faults, in the same call.
                    self.assertEqual(p.faults, [], f"{kind} {path}: header end tags do not match")
                    bad = [(ul, child, where) for ul, child, where in p.ul_children if child != "li"]
                    self.assertEqual(bad, [], f"{kind} {path}: a <ul> holds something other than <li>")

    def test_checker_catches_misnesting(self):
        """The parser above is not blind: each fault it looks for is reported on a small header."""
        ok = "<header><ul><li><a>Data</a><ul><li><a>API</a></li></ul></li></ul></header>"
        p = check_header(ok)
        self.assertEqual((p.faults, p.data_menu, p.done), ([], ["API"], True))
        p = check_header(ok.replace("</li></ul></li></ul>", "</ul></li></ul>"))  # an inner </li> dropped
        self.assertTrue(p.faults, "a missing </li> went unreported")
        p = check_header("<header><ul><div>x</div></ul></header>")
        self.assertEqual([c for _, c, _ in p.ul_children], ["div"])
        p = check_header("<header><ul><li>x</li>")
        self.assertFalse(p.done)


class _BrowserBase(LiveServerTestCase):
    maxDiff = None  # show every failing width, not the first

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._prev_async_unsafe = os.environ.get("DJANGO_ALLOW_ASYNC_UNSAFE")
        os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
        cls._pw = sync_playwright().start()
        cls.browser = cls._pw.chromium.launch()
        print(f"\n[layout] base bundle from {navtests.BUNDLE_DIR or 'static/webpack/'}; shots in {SHOT_DIR}")

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

    def _context(self, user, width):
        ctx = self.browser.new_context(viewport={"width": width, "height": 800})
        if user is not None:
            self.client.force_login(user)
            ctx.add_cookies([{
                "name": settings.SESSION_COOKIE_NAME,
                "value": self.client.cookies[settings.SESSION_COOKIE_NAME].value,
                "url": self.live_server_url,
            }])
        if navtests.BUNDLE_DIR:
            ctx.route("**/static/webpack/**", _bundle_route)
        return ctx

    def _load(self, page, path):
        page.goto(self.live_server_url + path)
        page.wait_for_function(READY, timeout=30000)
        page.wait_for_timeout(1100)  # the body fades in over 1 s; only for legible screenshots


PILLS = """() => {
  const nav = document.querySelector('header nav.navbar');
  const pills = [...document.querySelectorAll('#navbarSupportedContent .nav-beta-pill')].map(s => {
    const lh = parseFloat(getComputedStyle(s).lineHeight);
    return {text: s.textContent.replace(/\\s+/g, ' ').trim(), rects: s.getClientRects().length,
            h: s.getBoundingClientRect().height, w: s.getBoundingClientRect().width, lh,
            // An inline-block's content wraps INSIDE its one rect: count the lines its words sit on
            // (rects of the text nodes, grouped where one starts below the previous line's bottom).
            lines: (() => {
              const w = document.createTreeWalker(s, NodeFilter.SHOW_TEXT), rects = [];
              for (let n; (n = w.nextNode());) {
                if (!n.textContent.trim()) continue;
                const r = document.createRange(); r.selectNodeContents(n);
                rects.push(...[...r.getClientRects()].filter(x => x.width > 0));
              }
              rects.sort((a, b) => a.top - b.top);
              let lines = 0, bottom = -Infinity;
              for (const r of rects) { if (r.top >= bottom - 1) { lines++; bottom = r.bottom; } else bottom = Math.max(bottom, r.bottom); }
              return lines;
            })(),
            ws: getComputedStyle(s).whiteSpace};
  });
  return {inner: innerWidth, scroll: document.documentElement.scrollWidth,
          navH: nav ? nav.getBoundingClientRect().height : 0,
          expanded: nav ? getComputedStyle(nav.querySelector('.navbar-toggler')).display === 'none' : false,
          brandShown: (() => { const b = document.querySelector('#links_other'); return !!(b && b.offsetParent); })(),
          pills};
}"""


@unittest.skipUnless(HAVE_PLAYWRIGHT, "playwright is not importable here")
@unittest.skipUnless(os.environ.get("WHG_BROWSER_TESTS") == "1", "set WHG_BROWSER_TESTS=1 to run browser tests")
class BetaPillTests(_BrowserBase):
    WIDTHS = [1200, 1280, 1366, 1399]

    def test_beta_pills_single_line(self):
        User = get_user_model()
        beta = User.objects.create_user(username="pill_beta", email="pill-beta@example.org", password="pw",
                                        given_name="Bea", surname="Tester", role="beta_tester")
        os.makedirs(SHOT_DIR, exist_ok=True)
        failures = []
        ctx = self._context(beta, self.WIDTHS[0])
        try:
            page = ctx.new_page()
            for path in (reverse("licenses"), "/"):
                self._load(page, path)
                for w in self.WIDTHS:
                    page.set_viewport_size({"width": w, "height": 800})
                    page.wait_for_timeout(150)
                    m = page.evaluate(PILLS)
                    # Presence, in the same call: the expanded beta nav with its three pills (and the
                    # brand text off the home page, the case that used to squeeze them).
                    self.assertEqual(m["inner"], w)
                    self.assertTrue(m["expanded"], f"{path} @{w}: the nav is not expanded: {m}")
                    self.assertEqual([p["text"] for p in m["pills"]],
                                     ["Atlas beta", "Gazetteers beta", "Workbench beta"], f"{path} @{w}: {m}")
                    if path != "/":
                        self.assertTrue(m["brandShown"], f"{path} @{w}: brand text not shown: {m}")
                    for p in m["pills"]:
                        if p["rects"] != 1 or p["lines"] != 1 or p["h"] > p["lh"] + 4:
                            failures.append(f"{path} @{w}: {p['text']!r} wraps: {p}")
                    if m["scroll"] > m["inner"]:
                        failures.append(f"{path} @{w}: scrollWidth {m['scroll']} > innerWidth {m['inner']}")
                    slug = "home" if path == "/" else "page"
                    page.screenshot(path=os.path.join(SHOT_DIR, f"pills-beta-{slug}-{w}.png"),
                                    clip={"x": 0, "y": 0, "width": w, "height": 120})
        finally:
            ctx.close()
        self.assertEqual(failures, [], "a beta pill wraps or the page scrolls sideways")


PEOPLE = """() => {
  const col = document.getElementById('credits_right');
  const imgs = col ? [...col.querySelectorAll('img')] : [];
  const cr = col ? col.getBoundingClientRect() : null;
  return {inner: innerWidth, scroll: document.documentElement.scrollWidth,
          heading: !![...document.querySelectorAll('h3')].find(h => /Project overview/.test(h.textContent)),
          colW: cr ? cr.width : 0,
          imgs: imgs.map(i => ({src: i.src.split('/').pop(), loaded: i.complete && i.naturalWidth > 0,
                                w: i.getBoundingClientRect().width, h: i.getBoundingClientRect().height,
                                right: i.getBoundingClientRect().right, colRight: cr.right}))};
}"""


@unittest.skipUnless(HAVE_PLAYWRIGHT, "playwright is not importable here")
@unittest.skipUnless(os.environ.get("WHG_BROWSER_TESTS") == "1", "set WHG_BROWSER_TESTS=1 to run browser tests")
class PeopleOverviewTests(_BrowserBase):
    WIDTHS = [320, 375, 768, 1024, 1440]

    def test_no_horizontal_overflow(self):
        os.makedirs(SHOT_DIR, exist_ok=True)
        failures = []
        ctx = self._context(None, self.WIDTHS[0])
        try:
            page = ctx.new_page()
            self._load(page, "/people_overview/")
            page.wait_for_function("[...document.querySelectorAll('#credits_right img')].every(i => i.complete)",
                                   timeout=15000)
            for w in self.WIDTHS:
                page.set_viewport_size({"width": w, "height": 900})
                page.wait_for_timeout(150)
                m = page.evaluate(PEOPLE)
                # Presence, in the same call: the page and all eight logos, loaded and drawn.
                self.assertEqual(m["inner"], w)
                self.assertTrue(m["heading"], f"@{w}: not the people overview page: {m}")
                self.assertEqual(len(m["imgs"]), 8, f"@{w}: logos missing: {m}")
                for i in m["imgs"]:
                    self.assertTrue(i["loaded"] and i["w"] > 20 and i["h"] > 20, f"@{w}: logo not drawn: {i}")
                    if i["right"] > i["colRight"] + 0.5:
                        failures.append(f"@{w}: {i['src']} ends at {i['right']:.0f}, its column at {i['colRight']:.0f}")
                if m["scroll"] > m["inner"]:
                    failures.append(f"@{w}: scrollWidth {m['scroll']} > innerWidth {m['inner']}")
                if w in (768, 1440):
                    page.screenshot(path=os.path.join(SHOT_DIR, f"people-{w}.png"), full_page=True)
        finally:
            ctx.close()
        self.assertEqual(failures, [], "/people_overview/ scrolls sideways or a logo leaves its column")
