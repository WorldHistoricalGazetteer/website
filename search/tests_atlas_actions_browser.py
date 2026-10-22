"""Browser test for the Atlas's beta per-record actions (place#325).

A beta viewer gets three small actions on one record, in the map popup and on
each Place List row (gazetteerInteraction.js ``recordActionsHtml``):
Record (the beta Record page, new tab; contributed records only), Cluster (an
unminted locus page for the one record, new tab) and Similar (a Places search
for its title, run in place). Everyone else gets none of them.

Headless Playwright (its own bundled chromium, throwaway profile; nothing
touches the user's Chrome, no window opens) against a Django live server with
the CRC gateway faked in-process, as ``search.tests_atlas_toponym_deeplink``;
the base map is a minimal local style, so nothing depends on tiles. Run locally:

    DB_NAME=<private> GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 WHG_BROWSER_TESTS=1 \\
        WHG_ATLAS_BUNDLE_DIR=<scratch build> WHG_SHOT_DIR=<dir> \\
        venv/bin/python manage.py test search.tests_atlas_actions_browser --settings=whg.settings_localtest --noinput

What it proves, and how each check can fail:

* the Cliopatria popup, opened from a ``?gazetteer=clio&place=`` deep link,
  has Cluster and Similar and NO Record button; the Cluster link opens in a
  new tab and its ``r=`` token decodes, with the SERVER's decoder, to the
  expected recipe built here from the server's own default constants (members =
  the record, query = its title, the Atlas match mode and size, the Atlas
  clustering literals): a token with any other content fails the equality;
* Similar (popup) fills the box with the title, makes Places active, closes
  the popup and sends exactly ONE gateway search, whose query is the title
  (counted by the fake gateway after a settle period);
* the contributed record's popup has Record -> /atlas/place/<pk>/, new tab;
* Place List rows: the authority row has Cluster + Similar and no Record; the
  contributed row has Record; a title the recipe refuses (a control character)
  has Similar but no Cluster; a blank title has neither. Clicking a row's
  Similar runs one search and does NOT open that row's popup; clicking its
  Record opens a new tab and does not open the popup either;
* the contributed popup (LPF form from the entity view) shows its title and
  source-id line, for a beta and an anonymous viewer (blank before the fix);
  and, with ``WHG_ATLAS_BASELINE_BUNDLE_DIR`` set to a build without that fix,
  the authority popup's HTML is identical in both builds while the contributed
  popup's differs (skipped, loudly, without a baseline build);
* anonymous and signed-in non-beta visitors get a popup with NONE of the three
  (paired, in the same test, with a beta popup through the same route that has
  them, so a popup that never rendered cannot pass);
* the harness can fail: the bounded waits report "never" on a page with no Atlas.

``WHG_ATLAS_BUNDLE_DIR=<dir>`` serves ``atlas.bundle.js`` (and its css) from a
scratch build instead of ``static/webpack/``. Skipped loudly when Playwright is
not importable or WHG_BROWSER_TESTS is unset.
"""
import os
import time
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

from django.conf import settings
from django.contrib.auth import get_user_model
from django.test import LiveServerTestCase, override_settings

from places import locus_recipe as lr
from search.tests_atlas_actions import CLIO, Gateway
from search.tests_atlas_toponym_deeplink import BUNDLE_DIR, PLACES_ACTIVE

# A second scratch build to compare against (the same source with the change
# under test reverted): the authority popup must render identically.
BASELINE_DIR = os.environ.get("WHG_ATLAS_BASELINE_BUNDLE_DIR") or ""


def _bundle_router(bundle_dir):
    """``search.tests_atlas_toponym_deeplink._bundle_route`` for any directory."""
    def route_it(route, request):
        name = request.url.rsplit("/", 1)[-1].split("?", 1)[0]
        path = os.path.join(bundle_dir, name)
        if os.path.isfile(path):
            ctype = "text/css" if name.endswith(".css") else "application/javascript"
            with open(path, "rb") as f:
                route.fulfill(status=200, content_type=ctype, body=f.read())
        else:
            route.continue_()
    return route_it
from search.tests_locus import GATEWAY, POST, _Resp
from search.tests_locus_browser import GL_ARGS, SHOT_DIR, _poll, _tiles_route

try:
    from playwright.sync_api import sync_playwright
    HAVE_PLAYWRIGHT = True
except ImportError:  # pragma: no cover
    HAVE_PLAYWRIGHT = False

# The Cliopatria record, with what a real row shows (dates, type, countries,
# name variants), so the layout checks see a full meta line.
CLIO_FULL = dict(CLIO, temporal_range=[1738, 1740], ccodes=["IR", "AF", "IQ"],
                 types=[{"sourceLabel": "polity"}],
                 names=[{"label": "(Afsharid Iran)"}, {"label": "Afsharid dynasty"}, {"label": "Afshariyan"}])
BAD = {"place_id": "clio:bad_title", "title": "Bad\u0007name", "namespace": "clio", "repr_point": [50.0, 34.0]}
BLANK = {"place_id": "clio:blank_title", "title": "   ", "namespace": "clio", "repr_point": [49.0, 33.0]}
POPUP = ".whg-gazetteer-popup"


def _row(idx):
    # The fake gateway answers the browse with its hits in order: [CLIO, whg, BAD, BLANK].
    return f'.placelist-row[data-idx="{idx}"]'


@unittest.skipUnless(HAVE_PLAYWRIGHT, "playwright is not importable here")
@unittest.skipUnless(os.environ.get("WHG_BROWSER_TESTS") == "1", "set WHG_BROWSER_TESTS=1 to run browser tests")
@override_settings(CRC_GATEWAY_URL=GATEWAY, CRC_GATEWAY_TIMEOUT=2)
class AtlasRecordActionsTests(LiveServerTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._prev_async_unsafe = os.environ.get("DJANGO_ALLOW_ASYNC_UNSAFE")
        os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
        cls._pw = sync_playwright().start()
        cls.browser = cls._pw.chromium.launch(args=GL_ARGS)
        print(f"\n[atlas actions] atlas bundle from {BUNDLE_DIR or 'static/webpack/'}; shots in {SHOT_DIR}")

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
        from datasets.models import Dataset
        from places.models import Place
        User = get_user_model()
        self.beta = User.objects.create_user(
            username="atlas_actions_beta", email="aab@example.org", password="pw",
            given_name="Bea", surname="Tester", role="beta_tester")
        self.normal = User.objects.create_user(
            username="atlas_actions_normal", email="aan@example.org", password="pw",
            given_name="Norma", surname="User", role="normal")
        self.assertTrue(self.beta.can_access_beta)
        self.assertFalse(self.normal.can_access_beta)
        ds = Dataset.objects.create(owner=self.normal, label="act_browser_public", title="Persian towns",
                                    description="D", public=True, uri_base="https://example.org/place/")
        self.place = Place.objects.create(title="Isfahan", src_id="p1", dataset=ds, ccodes=["IR"])
        # A full contributed record, so the LPF-form popup shows what it renders
        # of each part (names, type, link, dates, description).
        from places.models import PlaceDescription, PlaceLink, PlaceName, PlaceType, PlaceWhen
        PlaceName.objects.create(place=self.place, toponym="Isfahan", jsonb={"toponym": "Isfahan", "lang": "en"})
        PlaceName.objects.create(place=self.place, toponym="Esfahan", jsonb={"toponym": "Esfahan", "lang": "fa"})
        PlaceType.objects.create(place=self.place, aat_id=300008389, fclass="P",
                                 jsonb={"identifier": "aat:300008389", "label": "cities", "sourceLabel": "city"})
        PlaceLink.objects.create(place=self.place, jsonb={"type": "closeMatch", "identifier": "wd:Q42053"})
        PlaceWhen.objects.create(place=self.place, minmax=[1050, 1722],
                                 jsonb={"timespans": [{"start": {"in": "1050"}, "end": {"in": "1722"}}]})
        PlaceDescription.objects.create(place=self.place, jsonb={"value": "Capital of Safavid Persia."})
        self.whg = {"place_id": f"whg:{ds.id}:p1", "title": "Isfahan", "namespace": "whg",
                    "repr_point": [51.67, 32.65]}
        # The live-server DB is flushed between tests, migration seeds and all:
        # the two gazetteers the deep links name are made here.
        GazetteerRegistryEntry.objects.update_or_create(id="clio", defaults=dict(
            name="Cliopatria", namespace="clio", entry_class="authority", status="published",
            h3_coverage=[], temporal_extent=[], core=False, no_explore=False, gazetteer_type="standard"))
        GazetteerRegistryEntry.objects.update_or_create(id=f"whg:{ds.id}", defaults=dict(
            name="Persian towns", namespace="whg", entry_class="dataset", status="published",
            h3_coverage=[], temporal_extent=[], gazetteer_type="standard"))
        self.gateway = Gateway(hits=[CLIO_FULL, self.whg, BAD, BLANK])
        for target, side in ((POST, self.gateway), ("api.crc_client.requests.get", lambda *a, **k: _Resp(200))):
            p = patch(target, side_effect=side)
            p.start()
            self.addCleanup(p.stop)
        os.makedirs(SHOT_DIR, exist_ok=True)

    # ── harness ────────────────────────────────────────────────────────────
    def _page(self, user=None, width=1280, height=860, bundle_dir=None):
        ctx = self.browser.new_context(viewport={"width": width, "height": height})
        if user is not None:
            self.client.force_login(user)
            ctx.add_cookies([{
                "name": settings.SESSION_COOKIE_NAME,
                "value": self.client.cookies[settings.SESSION_COOKIE_NAME].value,
                "url": self.live_server_url,
            }])
        # Debug gate (boot flag + the popup hook), tour and welcome panel out of the way.
        ctx.add_init_script("try { localStorage.setItem('whg.debug', '1');"
                            " localStorage.setItem('whg_atlas_tour_seen', 'true'); } catch (e) {}")
        ctx.route("https://tiles.whgazetteer.org/**", _tiles_route)
        bundle_dir = BUNDLE_DIR if bundle_dir is None else bundle_dir
        if bundle_dir:
            ctx.route("**/static/webpack/atlas.bundle.*", _bundle_router(bundle_dir))
        self.addCleanup(ctx.close)
        page = ctx.new_page()
        console = []
        page.on("console", lambda m: None if "performance warning" in m.text
                else console.append(f"{m.type}: {m.text}"))
        page.on("pageerror", lambda e: console.append(f"pageerror: {e}"))
        return ctx, page, console

    def _open(self, page, path, console):
        page.goto(self.live_server_url + path)
        booted = _poll(page, "window.__whgAtlas && (window.__whgAtlas.booted || window.__whgAtlas.error) && window.__whgAtlas", 90)
        if not booted or not booted.get("booted"):
            page.screenshot(path=os.path.join(SHOT_DIR, "actions-never-booted.png"))
            self.fail(f"the Atlas never booted on {path}: {booted}; console tail: {console[-10:]}")
        # Screenshots only: with no tiles the map never reports loaded, so its
        # (pointer-events: none) loading veil stays over the map and hides the
        # popup in a shot while it is in the DOM and hit-testable.
        page.add_style_tag(content="#map_loading_overlay { display: none !important; }")

    def _popup_title(self, page, title, timeout=25):
        js = (f"(document.querySelector('{POPUP} .popup-title') || {{}}).textContent === {title!r}"
              f" && document.querySelector('{POPUP}').innerHTML")
        return _poll(page, js, timeout)

    def _count(self, page, sel):
        return page.evaluate(f"document.querySelectorAll({sel!r}).length")

    def _wait_for_searches(self, n, timeout_s):
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if len(self.gateway.bodies) >= n:
                return len(self.gateway.bodies)
            time.sleep(0.25)
        return None

    def _expected_recipe(self, pid, title):
        from api.crc_client import DEFAULT_SEARCH_SIZE
        from search.locus import ATLAS_DEFAULT_CLUSTERING, ATLAS_DEFAULT_MATCH_MODE
        return lr.parse_recipe({
            "v": 1,
            "retrieval": {"query": title, "mode": ATLAS_DEFAULT_MATCH_MODE, "size": DEFAULT_SEARCH_SIZE},
            "clustering": dict(ATLAS_DEFAULT_CLUSTERING, weights=dict(ATLAS_DEFAULT_CLUSTERING["weights"])),
            "members": [pid],
        })

    def _assert_cluster_link(self, page, scope, pid, title):
        a = page.locator(f"{scope} .{'popup' if scope == POPUP else 'pl'}-rec-cluster")
        self.assertEqual(a.count(), 1)
        self.assertEqual(a.get_attribute("target"), "_blank")
        self.assertIn("noopener", a.get_attribute("rel") or "")
        href = a.get_attribute("href")
        parsed = urlparse(href)
        self.assertEqual(parsed.path, "/atlas/locus/")
        token = parse_qs(parsed.query)["r"][0]
        self.assertEqual(lr.decode_recipe_token(token), self._expected_recipe(pid, title))

    def _settle_one_search(self, before, query):
        n = self._wait_for_searches(before + 1, 20)
        time.sleep(2)   # settle: a second search would land here
        self.assertEqual(n, before + 1, "the Similar action never reached the gateway")
        self.assertEqual(len(self.gateway.bodies), before + 1, f"searched more than once: {self.gateway.bodies[before:]}")
        self.assertEqual(self.gateway.bodies[-1].get("query"), query)
        self.assertNotIn("namespaces", self.gateway.bodies[-1])   # all gazetteers, not the explored one

    # ── tests ──────────────────────────────────────────────────────────────
    def test_clio_popup_from_deep_link_cluster_and_similar_no_record(self):
        for width, height in ((1280, 860), (390, 844)):
            with self.subTest(width=width):
                self.gateway.bodies.clear()
                ctx, page, console = self._page(self.beta, width, height)
                self._open(page, f"/atlas/?z=9.00&gazetteer=clio&place={CLIO['place_id']}", console)
                html = self._popup_title(page, CLIO["title"])
                if width < 500:
                    # On a phone the map is short and the popup, anchored at the
                    # centred place, opens under the floating search bar (as it
                    # always has, share/invite included). Drag the map down, as a
                    # user would, to reach the header.
                    page.evaluate("window.heroMapInstance.panBy([0, -260], {duration: 0})")
                page.wait_for_timeout(600)
                page.screenshot(path=os.path.join(SHOT_DIR, f"actions-popup-clio-{width}.png"))
                self.assertTrue(html, f"the clio popup never rendered; console tail: {console[-8:]}")
                self.assertEqual(self._count(page, f"{POPUP} .popup-rec-record"), 0)
                self.assertEqual(self._count(page, f"{POPUP} .popup-rec-similar"), 1)
                self._assert_cluster_link(page, POPUP, CLIO["place_id"], CLIO["title"])
                # Tooltip text lives in data-bs-title, not title.
                self.assertEqual(page.get_attribute(f"{POPUP} .popup-rec-similar", "data-bs-title"),
                                 "Search for similar records")
                self.assertIsNone(page.get_attribute(f"{POPUP} .popup-rec-similar", "title"))
                # The actions sit inside the popup box (no overflow at phone width).
                boxes = page.evaluate(f"""(() => {{
                    const p = document.querySelector('{POPUP}').getBoundingClientRect();
                    const a = document.querySelector('{POPUP} .popup-rec-actions').getBoundingClientRect();
                    return {{p: [p.left, p.right], a: [a.left, a.right], vw: window.innerWidth}};
                }})()""")
                self.assertGreaterEqual(boxes["a"][0], boxes["p"][0], boxes)
                self.assertLessEqual(boxes["a"][1], boxes["p"][1] + 0.5, boxes)
                self.assertLessEqual(boxes["p"][1], boxes["vw"], boxes)

                # Similar: one search for the title, Places mode, popup closed.
                before = len(self.gateway.bodies)
                page.click(f"{POPUP} .popup-rec-similar")
                self._settle_one_search(before, CLIO["title"])
                state = page.evaluate(f"""({{
                    value: document.getElementById('atlas_search_input').value,
                    places: {PLACES_ACTIVE},
                    popup: !!document.querySelector('{POPUP}'),
                    cards: document.querySelectorAll('#atlas_search_results .cluster-card').length,
                    // The cluster cards' own locus links share the moved LOCUS_* constants.
                    locus: Array.from(document.querySelectorAll('#atlas_search_results a.cluster-locus-open'))
                        .map(a => a.getAttribute('href')),
                }})""")
                page.screenshot(path=os.path.join(SHOT_DIR, f"actions-similar-results-{width}.png"))
                self.assertEqual(state["value"], CLIO["title"], state)
                self.assertTrue(state["places"], state)
                self.assertFalse(state["popup"], state)
                self.assertGreater(state["cards"], 0, f"{state}; console tail: {console[-8:]}")
                self.assertEqual(len(state["locus"]), state["cards"], state)
                self.assertTrue(all(h.startswith("/atlas/locus/?r=") for h in state["locus"]), state)
                self.assertFalse([c for c in console if c.startswith("pageerror")], console)
                ctx.close()

    def test_contributed_popup_has_the_record_page(self):
        ctx, page, console = self._page(self.beta)
        gaz = self.whg["place_id"].rsplit(":", 1)[0]
        self._open(page, f"/atlas/?gazetteer={gaz}&place={self.whg['place_id']}", console)
        # A contributed popup is the LPF record: its title comes from properties.
        ok = _poll(page, f"document.querySelector('{POPUP} .popup-rec-record')", 25)
        page.wait_for_timeout(600)
        page.screenshot(path=os.path.join(SHOT_DIR, "actions-popup-contributed-1280.png"))
        self.assertTrue(ok, f"no Record action on the contributed popup; console tail: {console[-8:]}")
        a = page.locator(f"{POPUP} .popup-rec-record")
        self.assertEqual(a.get_attribute("href"), f"/atlas/place/{self.place.id}/")
        self.assertEqual(a.get_attribute("target"), "_blank")
        self.assertEqual(a.get_attribute("data-bs-title"), "Open the Record page")
        self._assert_cluster_link(page, POPUP, self.whg["place_id"], "Isfahan")
        self.assertEqual(self._count(page, f"{POPUP} .popup-rec-similar"), 1)
        print("\n[atlas actions] contributed popup text:", page.inner_text(POPUP).replace("\n", " | "))
        # Phone width: same popup, screenshot only (layout read by eye).
        ctx2, page2, console2 = self._page(self.beta, 390, 844)
        self._open(page2, f"/atlas/?gazetteer={gaz}&place={self.whg['place_id']}", console2)
        _poll(page2, f"document.querySelector('{POPUP} .popup-rec-record')", 25)
        page2.evaluate("window.heroMapInstance.panBy([0, -200], {duration: 0})")   # out from under the search bar
        page2.wait_for_timeout(600)
        page2.screenshot(path=os.path.join(SHOT_DIR, "actions-popup-contributed-390.png"))
        self.assertEqual(self._count(page2, f"{POPUP} .popup-rec-record"), 1)

    def test_contributed_popup_header_shows_title_and_id(self):
        """The entity view answers a contributed id in LPF form (title under
        ``properties``, no top-level ``place_id``): the header must still show
        the title and the source-id line, for every viewer."""
        pid = self.whg["place_id"]
        js_header = (f"(() => {{ const t = document.querySelector('{POPUP} .popup-title');"
                     f" const i = document.querySelector('{POPUP} .popup-place-id');"
                     f" return document.querySelector('{POPUP} .popup-header') &&"
                     f" {{title: t && t.textContent, id: i && i.textContent.trim()}}; }})()")
        for label, user in (("beta", self.beta), ("anonymous", None)):
            ctx, page, console = self._page(user)
            self._open(page, "/atlas/", console)
            page.evaluate(f"window.__whgAtlas.openPlacePopup({pid!r}, {self.whg['repr_point']!r})")
            header = _poll(page, js_header, 25)
            self.assertTrue(header, f"{label}: the contributed popup never rendered; console tail: {console[-8:]}")
            self.assertEqual(header["title"], "Isfahan", f"{label}: {header}")
            self.assertEqual(header["id"], pid, f"{label}: {header}")
            ctx.close()

    @unittest.skipUnless(BASELINE_DIR, "set WHG_ATLAS_BASELINE_BUNDLE_DIR to compare with a baseline build")
    def test_authority_popup_is_unchanged(self):
        """The authority (PlaceDetail) popup, rendered by this build and by the
        baseline build, is the same HTML, for a beta and an anonymous viewer.
        The contributed popup through the same route differs, so the
        comparison is not between two things that are always equal."""
        def render(bundle_dir, user, pid, point):
            ctx, page, console = self._page(user, bundle_dir=bundle_dir)
            self._open(page, "/atlas/", console)
            page.evaluate(f"window.__whgAtlas.openPlacePopup({pid!r}, {point!r})")
            html = _poll(page, f"document.querySelector('{POPUP} .popup-header') && "
                               f"document.querySelector('{POPUP}').outerHTML", 25)
            ctx.close()
            self.assertTrue(html, f"no popup from {bundle_dir}: {console[-8:]}")
            return html
        for label, user in (("beta", self.beta), ("anonymous", None)):
            with self.subTest(viewer=label):
                self.assertEqual(render(BUNDLE_DIR, user, CLIO["place_id"], CLIO["repr_point"]),
                                 render(BASELINE_DIR, user, CLIO["place_id"], CLIO["repr_point"]))
        self.assertNotEqual(render(BUNDLE_DIR, None, self.whg["place_id"], self.whg["repr_point"]),
                            render(BASELINE_DIR, None, self.whg["place_id"], self.whg["repr_point"]))

    def test_place_list_rows(self):
        ctx, page, console = self._page(self.beta)
        asked = []   # every popup fetch: a row click opens one, a row ACTION must not
        page.on("request", lambda r: asked.append(r.url) if "/entity/place:" in r.url else None)
        self._open(page, "/atlas/?gazetteer=clio", console)
        rows = _poll(page, "document.querySelectorAll('.placelist-row').length >= 4"
                           " && document.querySelectorAll('.placelist-row').length", 25)
        self.assertEqual(rows, 4, f"the Place List never filled; console tail: {console[-8:]}")
        page.wait_for_timeout(400)
        page.locator("#atlas_placelists_view").screenshot(path=os.path.join(SHOT_DIR, "actions-placelist-1280.png"))
        clio, whg, bad, blank = _row(0), _row(1), _row(2), _row(3)
        self.assertEqual(page.text_content(f"{clio} .pl-row-title"), CLIO["title"])
        self.assertEqual(page.text_content(f"{whg} .pl-row-title"), "Isfahan")
        self.assertEqual(self._count(page, f"{clio} .pl-rec-record"), 0)
        self.assertEqual(self._count(page, f"{clio} .pl-rec-similar"), 1)
        self._assert_cluster_link(page, clio, CLIO["place_id"], CLIO["title"])
        self.assertEqual(page.get_attribute(f"{whg} .pl-rec-record", "href"), f"/atlas/place/{self.place.id}/")
        self.assertEqual(self._count(page, f"{bad} .pl-rec-cluster"), 0)       # recipe refuses a control character
        self.assertEqual(self._count(page, f"{bad} .pl-rec-similar"), 1)
        self.assertEqual(self._count(page, f"{blank} .pl-rec-actions"), 0)     # blank title: nothing to search for
        # Layout: the actions sit inside the row, clear of the title and meta text.
        geo = page.evaluate(f"""(() => {{
            const r = document.querySelector('{whg}');
            const box = (el) => {{ const b = el.getBoundingClientRect(); return [b.left, b.top, b.right, b.bottom]; }};
            return {{ row: box(r), acts: box(r.querySelector('.pl-rec-actions')),
                      title: box(r.querySelector('.pl-row-title')), panel: r.getBoundingClientRect().width }};
        }})()""")
        self.assertGreaterEqual(geo["acts"][0], geo["row"][0], geo)
        self.assertLessEqual(geo["acts"][2], geo["row"][2], geo)
        self.assertLessEqual(geo["acts"][3], geo["row"][3], geo)
        self.assertGreaterEqual(geo["acts"][1], geo["title"][3], geo)   # below the title line
        print(f"\n[atlas actions] Place List row width at 1280: {geo['panel']:.0f}px")

        # Control: a click on the row itself DOES fetch and open its popup (so
        # the "no popup" checks below can fail).
        fetched = len(asked)
        page.click(f"{whg} .pl-row-title")
        _poll(page, f"document.querySelector('{POPUP} .popup-rec-record')", 15)
        self.assertEqual(len(asked) - fetched, 1, asked[fetched:])

        # Row Similar: one search, and the row's popup does NOT open.
        page.evaluate(f"document.querySelectorAll('{POPUP}').forEach(p => p.closest('.maplibregl-popup').remove())")
        before, fetched = len(self.gateway.bodies), len(asked)
        page.click(f"{clio} .pl-rec-similar")
        self._settle_one_search(before, CLIO["title"])
        self.assertEqual(asked[fetched:], [], "the row's popup opened as well")
        self.assertEqual(self._count(page, POPUP), 0, "the row's popup opened as well")
        self.assertEqual(page.evaluate("document.getElementById('atlas_search_input').value"), CLIO["title"])

        # Row Record: a new tab, and no popup on this page.
        ctx2, page2, console2 = self._page(self.beta)
        asked2 = []
        page2.on("request", lambda r: asked2.append(r.url) if "/entity/place:" in r.url else None)
        self._open(page2, "/atlas/?gazetteer=clio", console2)
        _poll(page2, f"document.querySelector('{whg} .pl-rec-record')", 25)
        fetched = len(asked2)
        with ctx2.expect_page() as new:
            page2.click(f"{whg} .pl-rec-record")
        tab = new.value
        tab.wait_for_load_state("domcontentloaded")
        self.assertEqual(urlparse(tab.url).path, f"/atlas/place/{self.place.id}/")
        page2.wait_for_timeout(1000)
        self.assertEqual(asked2[fetched:], [], "the Record link also opened the row's popup")
        self.assertEqual(self._count(page2, POPUP), 0, "the Record link also opened the row's popup")

    def test_place_list_rows_at_phone_width(self):
        ctx, page, console = self._page(self.beta, 390, 844)
        self._open(page, "/atlas/?gazetteer=clio", console)
        rows = _poll(page, "document.querySelectorAll('.placelist-row').length >= 4"
                           " && document.querySelectorAll('.placelist-row').length", 25)
        page.wait_for_timeout(400)
        got_it = page.locator("button:has-text('Got it')")   # the beta-tester notice covers the list
        if got_it.count():
            got_it.first.click()
        page.locator("#atlas_placelists_view").scroll_into_view_if_needed()
        page.locator("#atlas_placelists_view").screenshot(path=os.path.join(SHOT_DIR, "actions-placelist-390.png"))
        self.assertEqual(rows, 4, f"console tail: {console[-8:]}")
        geo = page.evaluate(f"""(() => {{
            const r = document.querySelector('{_row(1)}');
            const a = r.querySelector('.pl-rec-actions').getBoundingClientRect();
            const b = r.getBoundingClientRect();
            return {{ row: [b.left, b.right], acts: [a.left, a.right], vw: window.innerWidth,
                      scroll: document.documentElement.scrollWidth }};
        }})()""")
        self.assertLessEqual(geo["acts"][1], geo["row"][1], geo)
        self.assertLessEqual(geo["row"][1], geo["vw"], geo)
        print(f"\n[atlas actions] Place List row width at 390: {geo['row'][1] - geo['row'][0]:.0f}px")

    def test_anonymous_and_non_beta_get_none(self):
        """Same route for all three viewers (the debug popup hook, as a map click
        would open it): the beta popup has the actions, the others none."""
        seen = {}
        for label, user in (("beta", self.beta), ("normal", self.normal), ("anonymous", None)):
            ctx, page, console = self._page(user)
            self._open(page, "/atlas/", console)
            page.evaluate(f"window.__whgAtlas.openPlacePopup({CLIO['place_id']!r}, {CLIO['repr_point']!r})")
            html = self._popup_title(page, CLIO["title"])
            self.assertTrue(html, f"{label}: the popup never rendered; console tail: {console[-8:]}")
            seen[label] = self._count(page, f"{POPUP} .popup-rec-act")
            if label != "beta":
                self.assertEqual(self._count(page, ".pl-rec-act"), 0)
                page.screenshot(path=os.path.join(SHOT_DIR, f"actions-popup-{label}.png"))
            ctx.close()
        self.assertEqual(seen, {"beta": 2, "normal": 0, "anonymous": 0})

    def test_the_harness_can_fail(self):
        ctx, page, console = self._page(self.beta)
        page.goto("data:text/html,<title>no atlas here</title><p>nothing</p>")
        t0 = time.monotonic()
        self.assertIsNone(self._wait_for_searches(1, 2))
        self.assertIsNone(self._popup_title(page, CLIO["title"], timeout=1))
        self.assertIsNone(_poll(page, "window.__whgAtlas && window.__whgAtlas.booted", 1))
        self.assertLess(time.monotonic() - t0, 6, "the bounded waits overshot their budget")
