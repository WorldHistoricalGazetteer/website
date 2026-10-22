"""Browser test: the Atlas popup for a contributed record (place#329, place#330).

A contributed record's popup used to show only its header: the entity view
answered in LPF form and the renderer reads the flat popup shape. It now gets
the flat shape (``api.views_entity._contributed_popup_payload``), so the popup
shows country chips, type chip, "Also known as", the dates ("Active"), the
dated geometry, relations, description, depiction, the Source & licence footer
and the Attest footer. And the owner of a PRIVATE dataset now gets the popup for
their own record (it used to be a 404 behind the CSRF header: place#330).

Headless Playwright (bundled chromium, throwaway profile, no window) against a
Django live server with the CRC gateway faked in-process, as
``search.tests_atlas_actions_browser``. Run locally:

    DB_NAME=<private> GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 WHG_BROWSER_TESTS=1 \\
        WHG_ATLAS_BUNDLE_DIR=<scratch build> WHG_SHOT_DIR=<dir> \\
        venv/bin/python manage.py test search.tests_atlas_popup_contributed_browser \\
        --settings=whg.settings_localtest --noinput

How each check can fail: the same page, same route and same bundle are run
against the PRE-CHANGE server (the popup payload swapped back to the LPF form
and the viewer back to ``request.user``, patched in-process): there the
sections are asserted ABSENT and the owner's private popup is the error box, so
the presence checks are not checks of something always there.
"""
import os
import unittest
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import LiveServerTestCase, override_settings

from search.tests_atlas_actions import CLIO, Gateway
from search.tests_atlas_actions_browser import _bundle_router
from search.tests_atlas_toponym_deeplink import BUNDLE_DIR
from search.tests_locus import GATEWAY, POST, _Resp
from search.tests_locus_browser import GL_ARGS, SHOT_DIR, _poll, _tiles_route

try:
    from playwright.sync_api import sync_playwright
    HAVE_PLAYWRIGHT = True
except ImportError:  # pragma: no cover
    HAVE_PLAYWRIGHT = False

POPUP = ".whg-gazetteer-popup"

# Everything the checks read from the rendered popup, in one main-world call.
READ_POPUP = f"""(() => {{
    const p = document.querySelector('{POPUP}');
    if (!p) return null;
    const q = (s) => p.querySelector(s);
    const txt = (s) => (q(s) ? q(s).textContent.replace(/\\s+/g, ' ').trim() : null);
    return {{
        title: txt('.popup-title'),
        id: txt('.popup-place-id'),
        error: !p.querySelector('.popup-header'),
        cc: Array.from(p.querySelectorAll('.popup-chip-cc')).map(e => e.textContent.trim()),
        ccTip: q('.popup-chip-cc') && q('.popup-chip-cc').getAttribute('data-bs-title'),
        type: txt('.popup-chip-type'),
        typeHref: q('a.popup-chip-type') && q('a.popup-chip-type').getAttribute('href'),
        variants: txt('.popup-variants summary'),
        meta: txt('.popup-meta-list'),
        geoms: txt('.popup-geom-list'),
        relations: Array.from(p.querySelectorAll('details.popup-section summary')).map(s => s.textContent.trim()),
        description: txt('.popup-description'),
        depiction: q('.popup-depiction img') && q('.popup-depiction img').getAttribute('src'),
        footer: txt('.popup-attribution'),
        licence: txt('.whg-licence-code'),
        attest: !!q('.whg-attest-btn'),
        titleAttrs: p.querySelectorAll('[data-bs-toggle="tooltip"][title]').length,
    }};
}})()"""


def _old_popup_payload(place, place_id):
    """The PRE-CHANGE popup answer: the LPF Feature (control only)."""
    from api.serializers_api import PlaceFeatureSerializer
    from api.views_entity import _legacy_place_to_lpf
    from django.test import RequestFactory
    req = RequestFactory().get("/")
    return _legacy_place_to_lpf(PlaceFeatureSerializer(place, context={"request": req}).data, request=req)


@unittest.skipUnless(HAVE_PLAYWRIGHT, "playwright is not importable here")
@unittest.skipUnless(os.environ.get("WHG_BROWSER_TESTS") == "1", "set WHG_BROWSER_TESTS=1 to run browser tests")
@override_settings(CRC_GATEWAY_URL=GATEWAY, CRC_GATEWAY_TIMEOUT=2)
class ContributedPopupBrowserTests(LiveServerTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._prev_async_unsafe = os.environ.get("DJANGO_ALLOW_ASYNC_UNSAFE")
        os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
        cls._pw = sync_playwright().start()
        cls.browser = cls._pw.chromium.launch(args=GL_ARGS)
        print(f"\n[contributed popup] atlas bundle from {BUNDLE_DIR or 'static/webpack/'}; shots in {SHOT_DIR}")

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
        from api.tests_popup_contributed import make_full_record
        from licensing.models import License
        User = get_user_model()
        self.owner = User.objects.create_user(
            username="cpb_owner", email="cpbo@example.org", password="pw",
            given_name="Olive", surname="Owner", role="normal")
        self.other = User.objects.create_user(
            username="cpb_other", email="cpbx@example.org", password="pw",
            given_name="Otto", surname="Other", role="normal")
        # The live-server DB is flushed between tests, migration seeds and all.
        License.objects.get_or_create(spdx_id="CC-BY-4.0", defaults=dict(
            label="Creative Commons Attribution 4.0", url="https://creativecommons.org/licenses/by/4.0/",
            permits_commercial=True, attribution_required=True))
        self.ds, self.place, self.pid = make_full_record(self.owner, label="cpb_public")
        self.pds, self.pplace, self.ppid = make_full_record(self.owner, label="cpb_private", public=False)
        for ds in (self.ds, self.pds):
            GazetteerRegistryEntry.objects.update_or_create(id=f"whg:{ds.id}", defaults=dict(
                name=ds.title, namespace="whg", entry_class="dataset", status="published",
                h3_coverage=[], temporal_extent=[], gazetteer_type="standard"))
        self.point = [51.67, 32.65]
        for target, side in ((POST, Gateway(hits=[CLIO])),
                             ("api.crc_client.requests.get", lambda *a, **k: _Resp(200))):
            p = patch(target, side_effect=side)
            p.start()
            self.addCleanup(p.stop)
        os.makedirs(SHOT_DIR, exist_ok=True)

    def _page(self, user=None, width=1280, height=860):
        from django.conf import settings
        ctx = self.browser.new_context(viewport={"width": width, "height": height})
        if user is not None:
            self.client.force_login(user)
            ctx.add_cookies([{"name": settings.SESSION_COOKIE_NAME,
                              "value": self.client.cookies[settings.SESSION_COOKIE_NAME].value,
                              "url": self.live_server_url}])
        ctx.add_init_script("try { localStorage.setItem('whg.debug', '1');"
                            " localStorage.setItem('whg_atlas_tour_seen', 'true');"
                            " localStorage.setItem('whg_atlas_welcome_dismissed', 'true'); } catch (e) {}")
        ctx.route("https://tiles.whgazetteer.org/**", _tiles_route)
        if BUNDLE_DIR:
            ctx.route("**/static/webpack/atlas.bundle.*", _bundle_router(BUNDLE_DIR))
        self.addCleanup(ctx.close)
        page = ctx.new_page()
        console = []
        page.on("console", lambda m: None if "performance warning" in m.text
                else console.append(f"{m.type}: {m.text}"))
        page.on("pageerror", lambda e: console.append(f"pageerror: {e}"))
        return ctx, page, console

    def _popup(self, user, pid, shot, width=1280, height=860):
        ctx, page, console = self._page(user, width, height)
        page.goto(self.live_server_url + "/atlas/")
        booted = _poll(page, "window.__whgAtlas && (window.__whgAtlas.booted || window.__whgAtlas.error) && window.__whgAtlas", 90)
        self.assertTrue(booted and booted.get("booted"), f"the Atlas never booted: {booted}; {console[-8:]}")
        page.add_style_tag(content="#map_loading_overlay { display: none !important; }")
        page.evaluate(f"window.__whgAtlas.openPlacePopup({pid!r}, {self.point!r})")
        # Wait for the fetched popup (header or the error box), not the loading text.
        got = _poll(page, f"(() => {{ const p = document.querySelector('{POPUP}');"
                          f" return p && (p.querySelector('.popup-header') || /Failed to load/.test(p.textContent))"
                          f" ? true : null; }})()", 25)
        if width < 500:
            page.evaluate("window.heroMapInstance.panBy([0, -220], {duration: 0})")
        page.wait_for_timeout(600)
        page.screenshot(path=os.path.join(SHOT_DIR, f"contrib-popup-{shot}.png"))
        if page.locator(".maplibregl-popup").count():
            page.locator(".maplibregl-popup").first.screenshot(
                path=os.path.join(SHOT_DIR, f"contrib-popup-{shot}-box.png"))
        data = page.evaluate(READ_POPUP)
        errors = [c for c in console if c.startswith("pageerror")]
        ctx.close()
        self.assertTrue(got, f"{shot}: no popup and no error box; console tail: {console[-8:]}")
        self.assertEqual(errors, [], shot)
        return data

    def _assert_full(self, d, pid, label):
        self.assertIsNotNone(d, label)
        self.assertEqual(d["title"], "Isfahan", label)
        self.assertEqual(d["id"], pid, label)
        self.assertEqual(d["cc"], ["IR"], label)
        self.assertEqual(d["type"], "city", label)
        self.assertEqual(d["typeHref"], "https://vocab.getty.edu/page/aat/300008389", label)
        self.assertIn("Also known as", d["variants"] or "", label)
        self.assertIn("Esfahan", d["variants"] or "", label)
        self.assertIn("Active", d["meta"] or "", label)
        self.assertIn("1050–1722", d["meta"] or "", label)
        self.assertIn("Polygon", d["geoms"] or "", label)
        self.assertIn("1600–1700", d["geoms"] or "", label)
        self.assertIn("Relations (1)", d["relations"], label)
        self.assertEqual(d["description"], "Capital of Safavid Persia.", label)
        self.assertEqual(d["depiction"], "https://example.org/isfahan.jpg", label)
        self.assertIn("Source & licence", d["footer"] or "", label)
        self.assertIn("Persian towns", d["footer"] or "", label)
        self.assertEqual(d["licence"], "CC-BY-4.0", label)
        self.assertTrue(d["attest"], label)
        self.assertEqual(d["titleAttrs"], 0, label)   # tooltips via data-bs-title only

    def test_contributed_popup_is_full_for_every_viewer(self):
        for label, user in (("anonymous", None), ("other", self.other), ("owner", self.owner)):
            with self.subTest(viewer=label):
                self._assert_full(self._popup(user, self.pid, f"public-{label}"), self.pid, label)
        # Phone width, screenshot read by eye (layout), same content.
        self._assert_full(self._popup(None, self.pid, "public-anonymous-390", 390, 844), self.pid, "390")

    def test_owner_sees_their_private_record_and_others_do_not(self):
        self._assert_full(self._popup(self.owner, self.ppid, "private-owner"), self.ppid, "owner")
        for label, user in (("other", self.other), ("anonymous", None)):
            d = self._popup(user, self.ppid, f"private-{label}")
            self.assertTrue(d is None or d["error"], f"{label} saw the private record: {d}")

    def test_pre_change_server_shows_the_blank_popup_and_refuses_the_owner(self):
        """Control: the same checks against the pre-change server fail."""
        with patch("api.views_entity._contributed_popup_payload", side_effect=_old_popup_payload), \
                patch("api.views_entity._popup_viewer", side_effect=lambda request: request.user):
            d = self._popup(None, self.pid, "pre-change-public")
            owner = self._popup(self.owner, self.ppid, "pre-change-private-owner")
        self.assertEqual(d["title"], "Isfahan")            # the header renders (LPF fallback)
        for key in ("variants", "meta", "geoms", "footer"):
            self.assertIsNone(d[key], key)
        self.assertEqual(d["cc"], [])
        self.assertFalse(d["attest"])
        self.assertTrue(owner is None or owner["error"], owner)
