"""Browser test: the Atlas Place List for a CONTRIBUTED dataset (place#332).

Opening ``/atlas/?gazetteer=whg:<dataset_id>`` showed "No places to browse in
this gazetteer." because the gateway filtered ``whg:<dataset_id>`` against the
places' ``namespace``. The fake gateway here (``ScopedGateway``) implements the
fixed gateway (dataset scope + ``datasets_searched`` echo) or the old one, per
test. Same harness as ``search.tests_atlas_actions_browser``: headless
Playwright (its own chromium, throwaway profile), a Django live server, the
gateway faked in-process, a minimal local base style.

    DB_NAME=<private> GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 WHG_BROWSER_TESTS=1 \\
        WHG_ATLAS_BUNDLE_DIR=<scratch build> WHG_SHOT_DIR=<dir> \\
        venv/bin/python manage.py test search.tests_atlas_browse_contributed_browser --settings=whg.settings_localtest --noinput

What it proves, and how each check can fail:

* a contributed dataset's list fills with its places (and only its places),
  each row with a Record button to its own Record page; the status counts them;
* a private dataset (with a registry row, on purpose) lists for its owner
  through the deep link; a non-owner is not offered it in the Gazetteers panel
  (an existing rule, asserted beside a dataset that IS offered), and the
  list's own request sent from that page comes back empty without the gateway
  being asked, so its places never reach the page;
* an authority gazetteer (``clio``) still lists;
* empty states: a contributed dataset with no places says "No places to list
  in this dataset.", an authority with none keeps "No places to browse in this
  gazetteer.", and against a gateway WITHOUT the dataset scope the list says it
  cannot show the dataset instead of calling it empty. Run against the
  pre-change bundle (no ``WHG_ATLAS_BUNDLE_DIR``, i.e. ``static/webpack/``),
  those wording checks fail;
* the harness can fail: the bounded waits report "never" on a page with no Atlas.

Skipped loudly when Playwright is not importable or WHG_BROWSER_TESTS is unset.
"""
import copy
import os
import time
import unittest
from unittest.mock import patch

from django.conf import settings
from django.contrib.auth import get_user_model
from django.test import LiveServerTestCase, override_settings

from search.tests_atlas_actions_browser import _bundle_router
from search.tests_atlas_browse_contributed import CLIO_ROWS, ScopedGateway
from search.tests_atlas_toponym_deeplink import BUNDLE_DIR
from search.tests_locus import GATEWAY, POST, _Resp
from search.tests_locus_browser import GL_ARGS, SHOT_DIR, _poll, _tiles_route

try:
    from playwright.sync_api import sync_playwright
    HAVE_PLAYWRIGHT = True
except ImportError:  # pragma: no cover
    HAVE_PLAYWRIGHT = False

STATUS = "#atlas_placelists_view .placelist-status"
NOT_YET = "This list can't show a contributed dataset's places yet."


class BrowserGateway(ScopedGateway):
    """``ScopedGateway`` that also answers the page's other gateway calls
    (``/api/places`` for a popup), with ``raise_for_status``."""

    def __call__(self, url, json=None, **kwargs):
        if url.endswith("/api/search"):
            resp = super().__call__(url, json=json, **kwargs)
        elif url.endswith("/api/places"):
            ids = set((json or {}).get("ids") or [])
            resp = _Resp(200, {"places": [copy.deepcopy(h) for h in self.hits if h["place_id"] in ids]})
        else:
            resp = _Resp(404, {})
        resp.raise_for_status = lambda: None
        return resp


@unittest.skipUnless(HAVE_PLAYWRIGHT, "playwright is not importable here")
@unittest.skipUnless(os.environ.get("WHG_BROWSER_TESTS") == "1", "set WHG_BROWSER_TESTS=1 to run browser tests")
@override_settings(CRC_GATEWAY_URL=GATEWAY, CRC_GATEWAY_TIMEOUT=2)
class AtlasBrowseContributedTests(LiveServerTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._prev_async_unsafe = os.environ.get("DJANGO_ALLOW_ASYNC_UNSAFE")
        os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
        cls._pw = sync_playwright().start()
        cls.browser = cls._pw.chromium.launch(args=GL_ARGS)
        print(f"\n[browse contributed] atlas bundle from {BUNDLE_DIR or 'static/webpack/'}; shots in {SHOT_DIR}")

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
        self.owner = User.objects.create_user(
            username="plb_owner", email="plbo@example.org", password="pw",
            given_name="Olga", surname="Owner", role="beta_tester")
        self.other = User.objects.create_user(
            username="plb_other", email="plbx@example.org", password="pw",
            given_name="Otto", surname="Other", role="beta_tester")
        with patch("datasets.signals.doi"):
            def ds(label, title, public):
                return Dataset.objects.create(owner=self.owner, label=label, title=title,
                                              description="D", public=public)
            self.public = ds("plb_public", "Chinese Provinces", True)
            self.private = ds("plb_private", "Private notes", False)
            self.empty = ds("plb_empty", "Empty dataset", True)
        self.rows = {}
        hits = list(CLIO_ROWS)
        for d, titles in ((self.public, ["Anhui", "Hubei", "Hunan"]), (self.private, ["Secret A", "Secret B"])):
            for i, t in enumerate(titles):
                p = Place.objects.create(title=t, src_id=f"s{i}", dataset=d, ccodes=[])
                pid = f"whg:{d.pk}:s{i}"
                self.rows[pid] = p
                hits.append({"place_id": pid, "title": t, "namespace": "whg", "repr_point": [112.0 + i, 30.0]})
        self.hits = hits
        # The live-server DB is flushed between tests: the gazetteers the deep
        # links name are made here. The private dataset gets a registry row on
        # purpose, so the panel offers it to the non-owner too: the server rule,
        # not the registry, is what must keep its places out.
        reg = dict(h3_coverage=[], temporal_extent=[], gazetteer_type="standard")
        GazetteerRegistryEntry.objects.update_or_create(id="clio", defaults=dict(
            reg, name="Cliopatria", namespace="clio", entry_class="authority", status="published",
            core=False, no_explore=False))
        GazetteerRegistryEntry.objects.update_or_create(id="emptyauth", defaults=dict(
            reg, name="Empty authority", namespace="emptyauth", entry_class="authority", status="published",
            core=False, no_explore=False))
        for d in (self.public, self.private, self.empty):
            GazetteerRegistryEntry.objects.update_or_create(id=f"whg:{d.pk}", defaults=dict(
                reg, name=d.title, namespace="whg", entry_class="dataset", status="published"))
        self.gateway = BrowserGateway(self.hits)
        for target, side in ((POST, lambda *a, **k: self.gateway(*a, **k)),
                             ("api.crc_client.requests.get", lambda *a, **k: _Resp(200))):
            p = patch(target, side_effect=side)
            p.start()
            self.addCleanup(p.stop)
        os.makedirs(SHOT_DIR, exist_ok=True)

    # ── harness ────────────────────────────────────────────────────────────
    def _page(self, user=None, width=1280, height=860):
        ctx = self.browser.new_context(viewport={"width": width, "height": height})
        if user is not None:
            self.client.force_login(user)
            ctx.add_cookies([{
                "name": settings.SESSION_COOKIE_NAME,
                "value": self.client.cookies[settings.SESSION_COOKIE_NAME].value,
                "url": self.live_server_url,
            }])
        ctx.add_init_script("try { localStorage.setItem('whg.debug', '1');"
                            " localStorage.setItem('whg_atlas_tour_seen', 'true'); } catch (e) {}")
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

    def _open_list(self, page, gazetteer, console, shot=None):
        page.goto(f"{self.live_server_url}/atlas/?gazetteer={gazetteer}")
        booted = _poll(page, "window.__whgAtlas && (window.__whgAtlas.booted || window.__whgAtlas.error) && window.__whgAtlas", 90)
        if not booted or not booted.get("booted"):
            self.fail(f"the Atlas never booted: {booted}; console tail: {console[-10:]}")
        page.add_style_tag(content="#map_loading_overlay { display: none !important; }")
        # Settled: the status line has left "Loading…" (rows or an empty state).
        status = _poll(page, f"(() => {{ const s = document.querySelector({STATUS!r});"
                             f" const t = s && s.textContent.trim();"
                             f" return t && !t.startsWith('Loading') && t; }})()", 25)
        page.wait_for_timeout(300)
        if shot:
            page.screenshot(path=os.path.join(SHOT_DIR, f"browse-{shot}.png"))
        return status

    def _rows(self, page):
        return page.evaluate("""Array.from(document.querySelectorAll('.placelist-row')).map(r => ({
            title: r.querySelector('.pl-row-title').textContent,
            record: (r.querySelector('.pl-rec-record') || {getAttribute: () => null}).getAttribute('href'),
        }))""")

    def _asked_for(self, ns):
        return [b for b in self.gateway.bodies if ns in (b.get("namespaces") or [])]

    # ── tests ──────────────────────────────────────────────────────────────
    def test_contributed_dataset_lists_its_places_with_record_buttons(self):
        ctx, page, console = self._page(self.other)
        ns = f"whg:{self.public.pk}"
        status = self._open_list(page, ns, console, shot="contributed")
        rows = self._rows(page)
        self.assertEqual([r["title"] for r in rows], ["Anhui", "Hubei", "Hunan"],
                         f"status {status!r}; console tail: {console[-8:]}")
        for r, pid in zip(rows, (f"{ns}:s0", f"{ns}:s1", f"{ns}:s2")):
            self.assertEqual(r["record"], f"/atlas/place/{self.rows[pid].pk}/")
        self.assertEqual(status, "Showing 3")
        body = self._asked_for(ns)[0]
        self.assertTrue(body["browse"])
        self.assertEqual(body["namespaces"], [ns])
        self.assertFalse([c for c in console if c.startswith("pageerror")], console)

    def test_private_dataset_lists_for_its_owner_only(self):
        ns = f"whg:{self.private.pk}"
        ctx, page, console = self._page(self.owner)
        self._open_list(page, ns, console, shot="private-owner")
        self.assertEqual([r["title"] for r in self._rows(page)], ["Secret A", "Secret B"],
                         f"console tail: {console[-8:]}")
        self.assertEqual(len(self._asked_for(ns)), 1)

        # The non-owner: the Gazetteers panel does not offer the dataset (an
        # existing rule), so the deep link opens no list ...
        self.gateway.bodies.clear()
        ctx2, page2, console2 = self._page(self.other)
        self._open_list(page2, ns, console2, shot="private-other")
        offered = page2.evaluate("Array.from(document.querySelectorAll('#gazetteers_offcanvas .authority-cb'))"
                                 ".map(el => el.value)")
        self.assertIn(f"whg:{self.public.pk}", offered)    # the panel did list gazetteers
        self.assertNotIn(ns, offered)
        self.assertEqual(self._rows(page2), [])
        # ... and the list's own request, sent from the page as the panel sends
        # it, is answered empty without asking the gateway (the server rule).
        data = page2.evaluate("""async (ns) => {
            const csrf = (document.querySelector('[name=csrfmiddlewaretoken]') || {}).value
                || (document.cookie.match(/csrftoken=([^;]+)/) || [])[1] || '';
            const r = await fetch('/atlas/search/', {method: 'POST', credentials: 'same-origin',
                headers: {'Content-Type': 'application/json', 'X-CSRFToken': csrf},
                body: JSON.stringify({qstr: '', namespaces: [ns], mode: 'in', size: 100, offset: 0,
                                      browse: true, cluster: false, spatial: 'none'})});
            return {status: r.status, body: await r.json()};
        }""", ns)
        self.assertEqual(data["status"], 200, data)
        self.assertEqual((data["body"]["hits"], data["body"]["total"]), ([], 0))
        self.assertEqual(self._asked_for(ns), [], "the gateway was asked for a dataset this user cannot see")
        html = page2.content()
        for t in ("Secret A", "Secret B"):
            self.assertNotIn(t, html)

    def test_authority_gazetteer_still_lists(self):
        ctx, page, console = self._page(self.other)
        status = self._open_list(page, "clio", console, shot="clio")
        self.assertEqual([r["title"] for r in self._rows(page)], [r["title"] for r in CLIO_ROWS],
                         f"status {status!r}; console tail: {console[-8:]}")
        self.assertEqual(status, "Showing 2")
        self.assertEqual([r["record"] for r in self._rows(page)], [None, None])

    def test_empty_states(self):
        ctx, page, console = self._page(self.other)
        self.assertEqual(self._open_list(page, f"whg:{self.empty.pk}", console, shot="empty-dataset"),
                         "No places to list in this dataset.")
        self.assertEqual(self._open_list(page, "emptyauth", console, shot="empty-authority"),
                         "No places to browse in this gazetteer.")
        # A gateway without the dataset scope: the list says it cannot show the
        # dataset, not that the dataset is empty; an authority still lists.
        self.gateway.dataset_scope = False
        status = self._open_list(page, f"whg:{self.public.pk}", console, shot="old-gateway")
        self.assertTrue(status and status.startswith(NOT_YET), status)
        self.assertEqual(self._rows(page), [])
        self.assertEqual(self._open_list(page, "clio", console), "Showing 2")

    def test_the_harness_can_fail(self):
        ctx, page, console = self._page(self.other)
        page.goto("data:text/html,<title>no atlas here</title><p>nothing</p>")
        t0 = time.monotonic()
        self.assertIsNone(_poll(page, "window.__whgAtlas && window.__whgAtlas.booted", 1))
        self.assertIsNone(_poll(page, f"document.querySelector({STATUS!r})", 1))
        self.assertLess(time.monotonic() - t0, 6, "the bounded waits overshot their budget")
