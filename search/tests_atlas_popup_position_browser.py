"""Browser test: the Atlas place popup opens clear of the floating controls (place#331).

On a phone the Atlas map popup used to open UNDER the floating search bar and
the date control (``#floating_search``), so its header row (title, share,
email, the Wikipedia mark and the Record / Cluster / Similar actions) was
covered and could not be tapped, and it could open partly off the right-hand
edge. ``GazetteerInteraction.openPopup`` now fits the popup into the clear part
of the map on screens narrower than 992 px (``_fitPopupClear``); wide screens
are left exactly as they were.

Headless Playwright (bundled chromium, throwaway profile, no window) against a
Django live server with the CRC gateway faked in-process, as
``search.tests_atlas_actions_browser``; the base map is a minimal local style.
Run locally:

    DB_NAME=<private> GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 WHG_BROWSER_TESTS=1 \\
        WHG_ATLAS_BUNDLE_DIR=<scratch build> WHG_ATLAS_BASELINE_BUNDLE_DIR=<pre-change build> \\
        WHG_SHOT_DIR=<dir> venv/bin/python manage.py test search.tests_atlas_popup_position_browser \\
        --settings=whg.settings_localtest --noinput

What it proves, at 390x844, 360x740 (portrait) and 844x390 (landscape), for an
authority (Cliopatria) popup and a contributed popup (with Record / Cluster /
Similar), opened (a) by a ``?gazetteer=&place=`` deep link, (b) by a Place List
row click and (c) by a real mouse click on a map feature (a layer the harness
adds, wired to the popup's click handler through the debug-gated
``window.__whgAttachPopupLayer``; placed just below the floating controls,
near the right edge, the worst case):

* the whole popup box (tip included) lies inside the visible map area (the map
  container clipped to the viewport);
* it does not intersect any floating control (each child of
  ``#floating_search`` and each MapLibre control group): rect intersection
  empty;
* ``elementFromPoint`` at the centre of EVERY header control (share, email,
  Wikipedia mark, Record / Cluster / Similar) and of the popup's close button
  returns that control (or a child of it);
* the deep link's ``&z=`` zoom still applies (the fit pans, it never zooms).

At 1280x800 the popup's box and the map centre are the same, within 1 px,
as the baseline build's (``WHG_ATLAS_BASELINE_BUNDLE_DIR``: the same source
with this change reverted), for all three paths.

How each check can fail: ``test_baseline_bundle_fails_on_a_phone`` runs the
deep-link check against the baseline build at 390x844 and asserts it FAILS
(header covered), so the phone checks are not checks of something always
true; the harness check reports "never" on a page with no Atlas. Skipped
loudly without Playwright, WHG_BROWSER_TESTS=1, or (the comparisons) a
baseline build.
"""
import os
import time
import unittest
from unittest.mock import patch

from django.conf import settings
from django.contrib.auth import get_user_model
from django.test import LiveServerTestCase, override_settings

from search.tests_atlas_actions import CLIO, Gateway
from search.tests_atlas_actions_browser import BASELINE_DIR, CLIO_FULL, _bundle_router
from search.tests_atlas_toponym_deeplink import BUNDLE_DIR
from search.tests_locus import GATEWAY, POST, _Resp
from search.tests_locus_browser import GL_ARGS, SHOT_DIR, _poll, _tiles_route

try:
    from playwright.sync_api import sync_playwright
    HAVE_PLAYWRIGHT = True
except ImportError:  # pragma: no cover
    HAVE_PLAYWRIGHT = False

PHONES = ((390, 844), (360, 740), (844, 390))
WIDE = (1280, 800)

# Everything the checks read, in one main-world call: the popup (whole
# container, tip included), the visible map area, the floating controls, and a
# hit test at the centre of each header control and of the close button.
# null until the fetched popup (with its header) is in.
MEASURE = """(() => {
    const pop = document.querySelector('.maplibregl-popup.whg-gazetteer-popup-anchor');
    if (!pop || !pop.querySelector('.whg-gazetteer-popup .popup-header')) return null;
    const box = (el) => { const b = el.getBoundingClientRect(); return [b.left, b.top, b.right, b.bottom]; };
    const shown = (el) => { const b = el.getBoundingClientRect(); const cs = getComputedStyle(el);
        return b.width > 0 && b.height > 0 && cs.display !== 'none' && cs.visibility !== 'hidden'; };
    const m = box(document.getElementById('hero_map'));
    const view = [Math.max(m[0], 0), Math.max(m[1], 0), Math.min(m[2], innerWidth), Math.min(m[3], innerHeight)];
    const obstacles = [];
    document.querySelectorAll('#floating_search > *, #hero_map .maplibregl-ctrl').forEach((el) => {
        if (shown(el)) obstacles.push({sel: el.id || el.className, box: box(el)});
    });
    const hit = (el) => {
        const b = el.getBoundingClientRect();
        const x = b.left + b.width / 2, y = b.top + b.height / 2;
        const e = document.elementFromPoint(x, y);
        return {cls: el.className, ok: !!e && (e === el || el.contains(e)),
                got: e ? (e.id || e.className || e.tagName) : null, at: [Math.round(x), Math.round(y)]};
    };
    const targets = Array.from(pop.querySelectorAll('.popup-header button, .popup-header a'));
    const close = pop.querySelector('.maplibregl-popup-close-button');
    const map = window.heroMapInstance;
    const c = map.getCenter();
    return {
        popup: box(pop), view, obstacles,
        hits: targets.map(hit), close: close ? hit(close) : null,
        actions: pop.querySelectorAll('.popup-rec-act').length,
        title: (pop.querySelector('.popup-title') || {}).textContent || null,
        moving: map.isMoving(), zoom: map.getZoom(), center: [c.lng, c.lat],
        vw: innerWidth, vh: innerHeight,
    };
})()"""


def _intersects(a, b):
    return min(a[2], b[2]) - max(a[0], b[0]) > 0.5 and min(a[3], b[3]) - max(a[1], b[1]) > 0.5


def _problems(m):
    """Every way the measured popup is not clear and usable ([] = all good)."""
    out = []
    p, v = m["popup"], m["view"]
    if p[0] < v[0] - 0.5 or p[1] < v[1] - 0.5 or p[2] > v[2] + 0.5 or p[3] > v[3] + 0.5:
        out.append(f"popup {p} not inside the visible map {v}")
    for o in m["obstacles"]:
        if _intersects(p, o["box"]):
            out.append(f"popup {p} overlaps {o['sel']} {o['box']}")
    if not m["hits"]:
        out.append("no header controls found")
    for h in m["hits"] + [m["close"]]:
        if not h or not h["ok"]:
            out.append(f"header control not hit: {h}")
    return out


@unittest.skipUnless(HAVE_PLAYWRIGHT, "playwright is not importable here")
@unittest.skipUnless(os.environ.get("WHG_BROWSER_TESTS") == "1", "set WHG_BROWSER_TESTS=1 to run browser tests")
@override_settings(CRC_GATEWAY_URL=GATEWAY, CRC_GATEWAY_TIMEOUT=2)
class AtlasPopupPositionTests(LiveServerTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._prev_async_unsafe = os.environ.get("DJANGO_ALLOW_ASYNC_UNSAFE")
        os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
        cls._pw = sync_playwright().start()
        cls.browser = cls._pw.chromium.launch(args=GL_ARGS)
        print(f"\n[popup position] atlas bundle from {BUNDLE_DIR or 'static/webpack/'}; "
              f"baseline {BASELINE_DIR or '(none)'}; shots in {SHOT_DIR}")

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
        from places.models import Place, PlaceName, PlaceType, PlaceWhen
        User = get_user_model()
        # A beta viewer: the popup carries the Record / Cluster / Similar actions.
        self.beta = User.objects.create_user(
            username="popup_pos_beta", email="ppb@example.org", password="pw",
            given_name="Bea", surname="Tester", role="beta_tester")
        self.assertTrue(self.beta.can_access_beta)
        ds = Dataset.objects.create(owner=self.beta, label="popup_pos_public", title="Persian towns",
                                    description="D", public=True, uri_base="https://example.org/place/")
        place = Place.objects.create(title="Isfahan", src_id="p1", dataset=ds, ccodes=["IR"])
        PlaceName.objects.create(place=place, toponym="Isfahan", jsonb={"toponym": "Isfahan", "lang": "en"})
        PlaceName.objects.create(place=place, toponym="Esfahan", jsonb={"toponym": "Esfahan", "lang": "fa"})
        PlaceType.objects.create(place=place, aat_id=300008389, fclass="P",
                                 jsonb={"identifier": "aat:300008389", "label": "cities", "sourceLabel": "city"})
        PlaceWhen.objects.create(place=place, minmax=[1050, 1722],
                                 jsonb={"timespans": [{"start": {"in": "1050"}, "end": {"in": "1722"}}]})
        self.gaz = f"whg:{ds.id}"
        self.whg = {"place_id": f"whg:{ds.id}:p1", "title": "Isfahan", "namespace": "whg",
                    "repr_point": [51.67, 32.65]}
        GazetteerRegistryEntry.objects.update_or_create(id="clio", defaults=dict(
            name="Cliopatria", namespace="clio", entry_class="authority", status="published",
            h3_coverage=[], temporal_extent=[], core=False, no_explore=False, gazetteer_type="standard"))
        GazetteerRegistryEntry.objects.update_or_create(id=self.gaz, defaults=dict(
            name="Persian towns", namespace="whg", entry_class="dataset", status="published",
            h3_coverage=[], temporal_extent=[], gazetteer_type="standard"))
        self.gateway = Gateway(hits=[CLIO_FULL, self.whg])
        for target, side in ((POST, self.gateway), ("api.crc_client.requests.get", lambda *a, **k: _Resp(200))):
            p = patch(target, side_effect=side)
            p.start()
            self.addCleanup(p.stop)
        os.makedirs(SHOT_DIR, exist_ok=True)
        # (label, place id, title, Place List row index): the authority popup and
        # the contributed one (which alone has the Record action).
        self.records = (("clio", CLIO["place_id"], CLIO["title"], 0),
                        ("contrib", self.whg["place_id"], "Isfahan", 1))

    # ── harness ────────────────────────────────────────────────────────────
    def _page(self, width, height, bundle_dir=None):
        ctx = self.browser.new_context(viewport={"width": width, "height": height})
        self.client.force_login(self.beta)
        ctx.add_cookies([{"name": settings.SESSION_COOKIE_NAME,
                          "value": self.client.cookies[settings.SESSION_COOKIE_NAME].value,
                          "url": self.live_server_url}])
        ctx.add_init_script("try { localStorage.setItem('whg.debug', '1');"
                            " localStorage.setItem('whg_atlas_tour_seen', 'true');"
                            " localStorage.setItem('whg_atlas_welcome_dismissed', 'true');"
                            # The beta consent bar (fixed, bottom) is dismissed once by a real tester.
                            " localStorage.setItem('whg-beta-diag-ack', '1'); } catch (e) {}")
        ctx.route("https://tiles.whgazetteer.org/**", _tiles_route)
        # The Categories type tree (/types/tree/) reads the live ES ``types``
        # index server side; nothing here needs it, so the browser never asks.
        ctx.route("**/types/tree/**", lambda route, request: route.fulfill(status=503, body=""))
        # Nor does a test page report errors or page views to the live services.
        for host in ("https://errors.whgazetteer.org/**", "https://analytics.whgazetteer.org/**"):
            ctx.route(host, lambda route, request: route.fulfill(status=204, body=""))
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
        booted = _poll(page, "window.__whgAtlas && (window.__whgAtlas.booted || window.__whgAtlas.error)"
                             " && window.__whgAtlas", 90)
        if not booted or not booted.get("booted"):
            self.fail(f"the Atlas never booted on {path}: {booted}; console tail: {console[-10:]}")
        # Screenshots only: the (pointer-events: none) loading veil never lifts
        # without tiles; it hides the popup in a shot, not from a hit test.
        page.add_style_tag(content="#map_loading_overlay { display: none !important; }")
        # The harness serves no tiles, so the Atlas toasts "the map layer for this
        # gazetteer could not be loaded" over the map: an artefact of the harness,
        # not a control a real visitor would see there.
        page.add_style_tag(content="#atlas_notice_container, .toast-container { display: none !important; }")

    def _settled(self, page, timeout=25):
        """The measurement once the popup is in, the map has stopped and the popup
        box has held still for two polls; None if that never happens."""
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            m = page.evaluate(MEASURE)
            if m and not m["moving"] and last and m["popup"] == last["popup"] and m["center"] == last["center"]:
                return m
            last = m
            page.wait_for_timeout(350)
        return None

    def _rows(self, page, console):
        n = _poll(page, "document.querySelectorAll('.placelist-row').length >= 2"
                        " && document.querySelectorAll('.placelist-row').length", 25)
        self.assertTrue(n, f"the Place List never filled; console tail: {console[-8:]}")

    # Each path opens the record's popup on a fresh page and returns the settled
    # measurement (or None).
    def _via_deep_link(self, page, console, pid, idx, zoom="9.00"):
        gaz = "clio" if pid.startswith("clio:") else self.gaz
        self._open(page, f"/atlas/?z={zoom}&gazetteer={gaz}&place={pid}", console)
        return self._settled(page)

    def _via_row(self, page, console, pid, idx):
        self._open(page, "/atlas/?gazetteer=clio", console)
        self._rows(page, console)
        page.wait_for_timeout(500)
        page.click(f'.placelist-row[data-idx="{idx}"] .pl-row-title')
        return self._settled(page)

    def _via_click(self, page, console, pid, idx):
        """A real mouse click on a feature just below the floating controls, near
        the right-hand edge of the map (the worst place for a popup to open)."""
        self._open(page, "/atlas/?gazetteer=clio", console)
        self._rows(page, console)
        page.wait_for_timeout(1500)   # let the gazetteer's own fit settle
        # A regional view, so the spot below is on the earth at every size (a
        # wide screen shows the whole globe, with space at its corners).
        page.evaluate("window.heroMapInstance.jumpTo({center: [51.4, 35.7], zoom: 5})")
        page.wait_for_timeout(500)
        spot = page.evaluate("""(pid) => {
            const map = window.heroMapInstance;
            const r = document.getElementById('hero_map').getBoundingClientRect();
            let low = r.top;
            document.querySelectorAll('#floating_search > *').forEach((el) => {
                const b = el.getBoundingClientRect(); if (b.height > 0) low = Math.max(low, b.bottom); });
            const bottom = Math.min(r.bottom, innerHeight);
            const y = (low + 24 < bottom - 10) ? low + 24 : (r.top + bottom) / 2;
            const x = r.left + Math.min(r.width, innerWidth - r.left) - 40;
            const ll = map.unproject([x - r.left, y - r.top]);
            map.addSource('pptest', {type: 'geojson', data: {type: 'FeatureCollection', features: [
                {type: 'Feature', geometry: {type: 'Point', coordinates: [ll.lng, ll.lat]},
                 properties: {place_id: pid}}]}});
            map.addLayer({id: 'pptest_circle', type: 'circle', source: 'pptest',
                          paint: {'circle-radius': 10, 'circle-color': '#e74c3c'}});
            window.__whgAttachPopupLayer('pptest');
            return {lngLat: [ll.lng, ll.lat], left: r.left, top: r.top};
        }""", pid)
        ready = _poll(page, """(() => { const m = window.heroMapInstance;
            return m.getLayer('pptest_circle') && m.queryRenderedFeatures({layers: ['pptest_circle']}).length > 0; })()""", 15)
        self.assertTrue(ready, f"the harness feature never rendered; console tail: {console[-8:]}")
        # Where the feature is NOW (the map may have moved), and that a tap there
        # reaches the map canvas, not a control above it.
        at = page.evaluate("""(s) => { const p = window.heroMapInstance.project(s.lngLat);
            const x = s.left + p.x, y = s.top + p.y; const e = document.elementFromPoint(x, y);
            return {x, y, canvas: !!e && e.classList.contains('maplibregl-canvas')}; }""", spot)
        self.assertTrue(at["canvas"], f"the harness feature is not tappable: {at}")
        page.mouse.click(at["x"], at["y"])
        return self._settled(page)

    def _shot(self, page, name):
        page.wait_for_timeout(200)
        page.screenshot(path=os.path.join(SHOT_DIR, f"popup-pos-{name}.png"))

    def _check_phones(self, path_name, opener):
        for width, height in PHONES:
            for label, pid, title, idx in self.records:
                with self.subTest(path=path_name, size=f"{width}x{height}", record=label):
                    ctx, page, console = self._page(width, height)
                    m = opener(page, console, pid, idx)
                    self._shot(page, f"{path_name}-{label}-{width}x{height}")
                    self.assertIsNotNone(m, f"no settled popup; console tail: {console[-8:]}")
                    self.assertEqual(m["title"], title)
                    # The contributed popup has all three actions; the authority two.
                    self.assertEqual(m["actions"], 3 if label == "contrib" else 2, m)
                    probs = _problems(m)
                    self.assertFalse(probs, f"{probs}\n{m}\nconsole tail: {console[-6:]}")
                    self.assertFalse([c for c in console if c.startswith("pageerror")], console)
                    ctx.close()

    # ── tests ──────────────────────────────────────────────────────────────
    def test_deep_link_popup_is_clear_on_a_phone(self):
        self._check_phones("deeplink", self._via_deep_link)

    def test_reopening_a_fitted_popup_moves_nothing(self):
        """Once fitted, the same popup opened again at the same place (a second
        tap on it, a Place List row for it) already fits: the map stays put."""
        for label, pid, title, idx in self.records:
            with self.subTest(record=label):
                ctx, page, console = self._page(390, 844)
                first = self._via_deep_link(page, console, pid, idx)
                self.assertIsNotNone(first, f"no settled popup; console tail: {console[-8:]}")
                point = CLIO["repr_point"] if label == "clio" else self.whg["repr_point"]
                page.evaluate(f"window.__whgAtlas.openPlacePopup({pid!r}, {point!r})")
                page.wait_for_timeout(300)
                again = self._settled(page)
                self.assertIsNotNone(again, f"no settled popup; console tail: {console[-8:]}")
                self.assertEqual(again["center"], first["center"])
                probs = _problems(again)
                self.assertFalse(probs, f"{probs}\n{again}")
                ctx.close()

    def test_deep_link_zoom_still_applies(self):
        """The shared view's ``&z=`` survives the fit (it pans, never zooms)."""
        for width, height in PHONES[:1] + (WIDE,):
            with self.subTest(size=f"{width}x{height}"):
                ctx, page, console = self._page(width, height)
                m = self._via_deep_link(page, console, CLIO["place_id"], 0, zoom="7.50")
                self.assertIsNotNone(m, f"no settled popup; console tail: {console[-8:]}")
                self.assertAlmostEqual(m["zoom"], 7.5, places=2)
                ctx.close()

    def test_place_list_row_popup_is_clear_on_a_phone(self):
        self._check_phones("row", self._via_row)

    def test_map_click_popup_is_clear_on_a_phone(self):
        self._check_phones("click", self._via_click)

    @unittest.skipUnless(BASELINE_DIR, "set WHG_ATLAS_BASELINE_BUNDLE_DIR to compare with a baseline build")
    def test_wide_screen_popup_is_unchanged(self):
        """At 1280x800 the popup box and the map centre are those of the baseline
        build (within 1 px / 1e-6 degree), for all three paths and both records."""
        def measure(bundle_dir, opener, pid, idx):
            ctx, page, console = self._page(*WIDE, bundle_dir=bundle_dir)
            m = opener(page, console, pid, idx)
            ctx.close()
            self.assertIsNotNone(m, f"no settled popup from {bundle_dir or 'static'}: {console[-8:]}")
            return m
        for path_name, opener in (("deeplink", self._via_deep_link), ("row", self._via_row),
                                  ("click", self._via_click)):
            for label, pid, title, idx in self.records:
                with self.subTest(path=path_name, record=label):
                    now = measure(BUNDLE_DIR, opener, pid, idx)
                    was = measure(BASELINE_DIR, opener, pid, idx)
                    for a, b in zip(now["popup"], was["popup"]):
                        self.assertAlmostEqual(a, b, delta=1.0, msg=f"{now['popup']} vs {was['popup']}")
                    for a, b in zip(now["center"], was["center"]):
                        self.assertAlmostEqual(a, b, delta=1e-6, msg=f"{now['center']} vs {was['center']}")
                    self.assertAlmostEqual(now["zoom"], was["zoom"], delta=1e-6)

    @unittest.skipUnless(BASELINE_DIR, "set WHG_ATLAS_BASELINE_BUNDLE_DIR to compare with a baseline build")
    def test_baseline_bundle_fails_on_a_phone(self):
        """Control: the same deep-link check against the pre-change build, at
        390x844, finds the popup covered (so the phone checks can fail)."""
        for label, pid, title, idx in self.records:
            with self.subTest(record=label):
                ctx, page, console = self._page(390, 844, bundle_dir=BASELINE_DIR)
                m = self._via_deep_link(page, console, pid, idx)
                self._shot(page, f"baseline-deeplink-{label}-390x844")
                self.assertIsNotNone(m, f"no settled popup; console tail: {console[-8:]}")
                self.assertEqual(m["title"], title)
                self.assertNotEqual(_problems(m), [], m)
                ctx.close()

    def test_the_harness_can_fail(self):
        ctx, page, console = self._page(390, 844)
        page.goto("data:text/html,<title>no atlas here</title><p>nothing</p>")
        t0 = time.monotonic()
        self.assertIsNone(_poll(page, "window.__whgAtlas && window.__whgAtlas.booted", 1))
        self.assertIsNone(self._settled(page, timeout=1.5))
        self.assertLess(time.monotonic() - t0, 6, "the bounded waits overshot their budget")
        # _problems() reports a covered header, an overlap and a box outside the map.
        bad = {"popup": [0, 0, 400, 300], "view": [0, 50, 390, 400],
               "obstacles": [{"sel": "search", "box": [8, 8, 382, 120]}],
               "hits": [{"ok": False}], "close": {"ok": True}}
        self.assertEqual(len(_problems(bad)), 3, _problems(bad))
