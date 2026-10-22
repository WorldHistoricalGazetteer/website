"""place#325 Stage 5: the Record page in the shared two-column shell, in HEADLESS Chromium.

Opt-in, because it starts a browser and a live server:

    WHG_BROWSER_TESTS=1 GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test places.tests_record_layout_browser --settings=whg.settings_localtest

``WHG_RECORD_BUNDLE_DIR=<dir>`` serves ``atlas_record.bundle.js``/``.css`` from a scratch
build instead of ``static/webpack/`` (to test source not yet bundled; the committed bundle
is the lead's to rebuild). ``WHG_BROWSER_SHOTS=<dir>`` keeps screenshots, one per width.
Playwright's bundled chromium, fresh profile, never a visible window. The CRC gateway is
faked at ``api.crc_client.crc_fetch_places`` and EDOPS at
``api.edop_client._requests_transport``; base-map tiles are not served (the page's
plain-background fallback draws the points). Every absence is asserted beside a presence,
and ``test_harness_can_fail`` runs the checks with the page bundle blocked and requires
every one to fail.

The fixture: record "Larissa" cites a TGN-like record held in WHG (Postgres) and a
GeoNames record (gateway) that lie at the SAME point, a far Pleiades record, and a Wikidata
record at the record's own point, whose Wikipedia sitelinks the "Elsewhere" card shows. Two
co-citing records each cite two of the same identifiers.
"""
import json
import os
import unittest
from unittest.mock import patch

from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.gis.geos import Point
from django.test import Client, LiveServerTestCase, override_settings

RUN = os.environ.get("WHG_BROWSER_TESTS") == "1"
SHOTS = os.environ.get("WHG_BROWSER_SHOTS")
BUNDLE_DIR = os.environ.get("WHG_RECORD_BUNDLE_DIR") or ""
BLOCKED_HOSTS = ("analytics.whgazetteer.org", "errors.whgazetteer.org", "plausible", "computingplace.org")
LOCMEM = {**settings.CACHES, "default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache",
                                         "LOCATION": "rlb325"}}
RECORD_XY = (22.42, 39.64)
SHARED_XY = (22.0, 39.0)
GATEWAY_DB = {
    "gn:100": {"place_id": "gn:100", "title": "GeoNames Larissa", "namespace": "gn",
               "repr_point": list(SHARED_XY), "names": [{"label": "Larissa"}], "types": [], "ccodes": ["GR"],
               "geometries": [{"type": "Point", "coordinates": list(SHARED_XY)}]},
    "pl:500": {"place_id": "pl:500", "title": "Pleiades Larisa", "namespace": "pl",
               "repr_point": [25.0, 35.0], "names": [{"label": "Larisa"}], "types": [], "ccodes": ["GR"],
               "geometries": [{"type": "Point", "coordinates": [25.0, 35.0]}]},
    "wd:Q999": {"place_id": "wd:Q999", "title": "Wikidata Larissa", "namespace": "wd",
                "repr_point": list(RECORD_XY), "names": [{"label": "Larissa"}], "types": [], "ccodes": ["GR"],
                "geometries": [{"type": "Point", "coordinates": list(RECORD_XY)}],
                "links": [{"type": "seeAlso", "identifier": "https://el.wikipedia.org/wiki/Λάρισα"},
                          {"type": "seeAlso", "identifier": "https://en.wikipedia.org/wiki/Larissa"}]},
}


def fake_fetch(ids, user=None, allow_anonymous=False, meta=None):
    return {i: GATEWAY_DB[i] for i in ids if i in GATEWAY_DB}


def bundle_route(route, request):
    """Serve the page bundle from WHG_RECORD_BUNDLE_DIR when it has the file."""
    name = request.url.rsplit("/", 1)[-1].split("?", 1)[0]
    path = os.path.join(BUNDLE_DIR, name)
    if BUNDLE_DIR and os.path.isfile(path):
        ctype = "text/css" if name.endswith(".css") else "application/javascript"
        with open(path, "rb") as f:
            return route.fulfill(status=200, content_type=ctype, body=f.read())
    return route.continue_()


@unittest.skipUnless(RUN, "set WHG_BROWSER_TESTS=1 to drive the Record page layout in headless Chromium")
@override_settings(CACHES=LOCMEM, EDOP_ENABLED=True)
class RecordLayoutBrowserTests(LiveServerTestCase):
    maxDiff = None

    @classmethod
    def setUpClass(cls):
        os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
        super().setUpClass()
        from playwright.sync_api import sync_playwright
        cls._pw = sync_playwright().start()
        cls.browser = cls._pw.chromium.launch(headless=True)
        print(f"\n[record layout] atlas_record bundle from {BUNDLE_DIR or 'static/webpack/'}")

    @classmethod
    def tearDownClass(cls):
        try:
            cls.browser.close()
            cls._pw.stop()
        finally:
            super().tearDownClass()

    def setUp(self):
        from api.models import GazetteerRegistryEntry
        from datasets.models import Dataset
        from django.core.cache import cache
        from places.models import Place, PlaceGeom, PlaceLink, PlaceName

        cache.clear()
        self.beta = get_user_model().objects.create_user(
            username="rlb_beta", email="rlb@example.org", password="pw", given_name="R", surname="L",
            role="beta_tester")

        def ds(key, title):
            with patch("datasets.signals.doi"):
                return Dataset.objects.create(owner=self.beta, label=f"rlb_{key}", title=title, description="D",
                                              creator="X", public=True, ds_status="indexed",
                                              uri_base="https://example.org/places/")
        a, tgn, b = ds("a", "Dataset a"), ds("tgn", "TGN extract"), ds("b", "Dataset b")
        for ns, name in (("gn", "GeoNames"), ("pl", "Pleiades"), ("wd", "Wikidata")):
            GazetteerRegistryEntry.objects.update_or_create(
                id=ns, defaults=dict(name=name, namespace=ns, entry_class="authority", status="published"))

        def place(d, title, src, xy, links=()):
            p = Place.objects.create(title=title, src_id=src, dataset=d, ccodes=["GR"])
            PlaceGeom.objects.create(place=p, geom=Point(*xy, srid=4326), jsonb={"type": "Point", "coordinates": list(xy)})
            PlaceName.objects.create(place=p, toponym=title, jsonb={"toponym": title})
            for t, i in links:
                PlaceLink.objects.create(place=p, jsonb={"type": t, "identifier": i})
            return p
        # Held in WHG under its TGN id (its own self-link), at the GeoNames record's point.
        self.T = place(tgn, "TGN Larissa", "7000", SHARED_XY, links=[("exactMatch", "tgn:7000")])
        self.R = place(a, "Larissa", "R1", RECORD_XY, links=[
            ("exactMatch", "tgn:7000"), ("closeMatch", "gn:100"), ("closeMatch", "pl:500"),
            ("closeMatch", "wd:Q999"), ("seeAlso", "https://example.org/larissa")])
        # Co-citers, far away (never on the map): each cites two of R's identifiers.
        self.C = [place(b, f"Cociter {i}", f"C{i}", (120.0, -40.0),
                        links=[("closeMatch", "gn:100"), ("closeMatch", "pl:500")]) for i in (1, 2)]
        for p in (patch("api.crc_client.crc_clustering_params", return_value=(None, "unreachable")),
                  patch("api.crc_client.crc_fetch_places", side_effect=fake_fetch)):
            p.start()
            self.addCleanup(p.stop)

    # ---- harness ------------------------------------------------------------------
    def new_page(self, width=1280, block=None):
        ctx = self.browser.new_context(viewport={"width": width, "height": 1000})
        c = Client()
        c.force_login(self.beta)
        ctx.add_cookies([{"name": settings.SESSION_COOKIE_NAME,
                          "value": c.cookies[settings.SESSION_COOKIE_NAME].value, "url": self.live_server_url}])
        self.addCleanup(ctx.close)
        page = ctx.new_page()
        self.env_requests, self.errors = [], []

        def route(r):
            url = r.request.url
            if any(h in url for h in BLOCKED_HOSTS) or (block and block in url):
                return r.abort()
            if "/styles/" in url and url.endswith("style.json"):
                return r.abort()                       # no tiles offline: the plain-background fallback
            if "/static/webpack/atlas_record.bundle." in url:
                return bundle_route(r, r.request)
            return r.continue_()
        page.route("**/*", route)
        page.on("request", lambda r: self.env_requests.append(r.url) if "/environment/" in r.url else None)
        page.on("pageerror", lambda e: self.errors.append(str(e)))
        return page

    def wait_for(self, page, expr, ms):
        try:
            page.wait_for_function(expr, timeout=ms)
            return True
        except Exception:  # noqa: BLE001 — a timeout is a finding
            return False

    def shot(self, page, name):
        if SHOTS:
            os.makedirs(SHOTS, exist_ok=True)
            page.screenshot(path=os.path.join(SHOTS, f"{name}.png"), full_page=True)

    def open(self, page, wait_ms=45000):
        with patch("api.edop_client._requests_transport", side_effect=self.transport):
            page.goto(f"{self.live_server_url}/atlas/place/{self.R.pk}/")
            ready = self.wait_for(page, "window.__whgRecord && window.__whgRecord.concordance === 'ready' "
                                        "&& window.__whgRecord.mapDrawn >= 1", wait_ms)
            gloss = self.wait_for(page, "window.__whgEnrichment && window.__whgEnrichment.gloss === 'ready'",
                                  wait_ms if ready else 1000)
        return ready, gloss

    RECTS = """() => { const r = (s) => { const e = document.querySelector(s);
            if (!e || e.hidden) return null; const b = e.getBoundingClientRect();
            return {l: b.left, r: b.right, t: b.top + scrollY, b: b.bottom + scrollY, w: b.width, h: b.height}; };
        return {main: r('.rs-main'), side: r('.rs-side'), header: r('.rs-header'), map: r('#concordance_map_box'),
                notes: r('#concordance_map_notes'), record: r('#record'), conc: r('#concordance'),
                gloss: r('#record_gloss'), elsewhere: r('#record_elsewhere'), env: r('#record_environment'),
                canvas: window.__whgRecordMap ? (() => { const c = window.__whgRecordMap.getCanvas();
                    const box = document.getElementById('concordance_map');
                    return {w: c.clientWidth, h: c.clientHeight, boxW: box.clientWidth, boxH: box.clientHeight}; })()
                    : null,
                overflow: Array.from(document.querySelectorAll('#atlas_record *')).filter((e) =>
                    e.getBoundingClientRect().right > innerWidth + 1).slice(0, 6)
                    .map((e) => e.tagName + (e.id ? '#' + e.id : '') + '.' + String(e.className).slice(0, 40)),
                scrollW: document.documentElement.scrollWidth, innerW: innerWidth}; }"""

    def layout_checks(self, page, width):
        d = page.evaluate(self.RECTS)
        r = {}
        need = ("header", "map", "notes", "record", "conc", "gloss", "elsewhere", "env")
        r["all_present"] = all(d.get(k) for k in need)
        if not r["all_present"]:
            return r, d
        m = d["map"]
        # Nothing of THIS page wider than the window (the site's own navbar overflows at
        # 992 px on every page: not this page's, so not measured here).
        r["no_horizontal_overflow"] = d["overflow"] == []
        c = d["canvas"]
        r["canvas_fills_box"] = bool(c) and abs(c["w"] - c["boxW"]) <= 2 and abs(c["h"] - c["boxH"]) <= 2 \
            and abs(c["boxW"] - m["w"]) <= 5
        r["map_capped"] = m["h"] <= 560.5
        r["notes_under_map"] = 0 <= d["notes"]["t"] - m["b"] <= 12
        if width >= 992:
            r["two_columns"] = bool(d["main"] and d["side"]) and d["main"]["r"] <= d["side"]["l"] \
                and abs(d["main"]["t"] - d["side"]["t"]) <= 2
            r["map_tops_side"] = abs(m["t"] - d["side"]["t"]) <= 1
            r["map_square"] = abs(m["w"] - m["h"]) <= 1
            r["side_order"] = m["b"] <= d["elsewhere"]["t"] <= d["env"]["t"]
            r["main_order"] = d["header"]["b"] <= d["record"]["t"] <= d["conc"]["t"] <= d["gloss"]["t"]
            r["side_narrower"] = d["side"]["w"] < d["main"]["w"]
        else:
            # One column: header, map (with its notes), the left column's sections, then the cards.
            r["stacked_order"] = (d["header"]["b"] <= m["t"] and d["notes"]["b"] <= d["record"]["t"]
                                  and d["record"]["b"] <= d["conc"]["t"] <= d["gloss"]["t"]
                                  and d["gloss"]["b"] <= d["elsewhere"]["t"] <= d["env"]["t"])
            r["map_full_width"] = abs(m["w"] - d["record"]["w"]) <= 2
            r["map_square_or_capped"] = abs(m["w"] - m["h"]) <= 1 if m["w"] <= 560 else abs(m["h"] - 560) <= 1
        return r, d

    def setUp_transport(self):
        from api.tests_edop_client import AREA, FakeTransport
        return FakeTransport(AREA)

    @property
    def transport(self):
        if not hasattr(self, "_transport"):
            self._transport = self.setUp_transport()
        return self._transport

    # ---- tests --------------------------------------------------------------------
    def test_layout_at_four_widths(self):
        failures = {}
        for width in (1280, 992, 768, 390):
            page = self.new_page(width)
            ready, gloss = self.open(page)
            r, d = self.layout_checks(page, width)
            r.update(ready=ready, gloss=gloss)
            self.shot(page, f"record_layout_{width}")
            bad = {k: v for k, v in r.items() if not v}
            if bad:
                failures[width] = (bad, d)
        self.assertEqual(failures, {}, f"errors={self.errors}")
        self.assertEqual(self.errors, [])

    def test_map_tracks_the_column_switch(self):
        page = self.new_page(1280)
        ready, _g = self.open(page)
        self.assertTrue(ready)
        before = page.evaluate(self.RECTS)["map"]
        page.set_viewport_size({"width": 768, "height": 1000})
        tracked = self.wait_for(page, """() => { const b = document.getElementById('concordance_map_box');
            const c = window.__whgRecordMap.getCanvas();
            return b.clientWidth > 600 && Math.abs(c.clientWidth - b.clientWidth) <= 2
                && Math.abs(c.clientHeight - b.clientHeight) <= 2; }""", 5000)
        after = page.evaluate(self.RECTS)
        self.assertTrue(tracked, f"canvas did not follow the box: before={before} after={after}")
        self.assertGreater(after["map"]["w"], before["w"])                 # the box did change

    def test_summary_and_legend_wording(self):
        page = self.new_page(1280)
        ready, _g = self.open(page)
        d = page.evaluate("""() => ({summary: (document.getElementById('concordance_summary') || {}).innerText || '',
            note: (document.getElementById('concordance_map_note') || {}).innerText || '',
            inSide: !!document.querySelector('.rs-side #concordance_summary'),
            inMain: !!document.querySelector('.rs-main #concordance_summary'),
            state: window.__whgRecord})""")
        self.assertTrue(ready, d)
        summary = " ".join(d["summary"].split())
        self.assertIn("4 attested equivalents (exactMatch or closeMatch, from this record or about it), "
                      "4 on the map, plus this record.", summary)
        self.assertIn("this record", d["note"])
        self.assertIn("attested equivalents (exactMatch, closeMatch)", d["note"])
        self.assertIn("more than one record at the same point", d["note"])
        self.assertEqual((d["inSide"], d["inMain"]), (True, False))
        self.assertEqual((d["state"]["mapped"], d["state"]["stacks"]), (4, 2))

    def test_environment_is_fetched_only_on_click(self):
        page = self.new_page(1280)
        with patch("api.edop_client._requests_transport", side_effect=self.transport):
            ready, gloss = self.open(page)
            page.wait_for_timeout(1500)                 # time enough for any on-load request to go out
            before = page.evaluate("""() => ({env: window.__whgEnrichment.environment,
                fetches: window.__whgEnrichment.environmentFetches || 0,
                button: !!document.querySelector('#record_environment .enr-env-show'),
                modal: !!document.querySelector('.enr-env-modal')})""")
            sent_before = (list(self.env_requests), len(self.transport.calls))
            page.click("#record_environment .enr-env-show")
            loaded = self.wait_for(page, "window.__whgEnrichment.environment === 'ready'", 30000)
            shown = self.wait_for(page, "!!document.querySelector('.enr-env-modal.show')", 5000)
            body = page.evaluate("() => (document.querySelector('.enr-env-modal .modal-body') || {}).innerText || ''")
            self.shot(page, "record_environment_modal")
            page.click(".enr-env-modal .btn-close")
            self.wait_for(page, "!document.querySelector('.enr-env-modal.show')", 5000)
            page.click("#record_environment .enr-env-show")
            reshown = self.wait_for(page, "!!document.querySelector('.enr-env-modal.show')", 5000)
        self.assertTrue(ready and gloss)
        self.assertEqual(before, {"env": "idle", "fetches": 0, "button": True, "modal": False})
        self.assertEqual(sent_before, ([], 0))                              # nothing asked before the click
        self.assertTrue(loaded and shown and reshown)
        self.assertEqual(len(self.env_requests), 1, self.env_requests)      # one fetch; the reopen used the cache
        self.assertEqual(len(self.transport.calls), 1)
        self.assertIn("EDOPS", body)
        self.assertIn("research prototype", body.lower())

    def test_coincident_points_list_every_record(self):
        page = self.new_page(1280)
        ready, _g = self.open(page)
        self.assertTrue(ready, self.errors)

        def click(lnglat):
            page.evaluate("() => window.__whgRecordPopup && window.__whgRecordPopup.remove()")
            xy = page.evaluate("""(ll) => { const map = window.__whgRecordMap; const p = map.project(ll);
                const b = map.getCanvas().getBoundingClientRect(); return [b.left + p.x, b.top + p.y]; }""", lnglat)
            page.mouse.click(*xy)
            self.wait_for(page, "!!document.querySelector('.maplibregl-popup .conc-popup')", 3000)
            return page.evaluate("""() => { const p = document.querySelector('.maplibregl-popup .conc-popup');
                return p ? {count: p.dataset.count, text: p.innerText,
                            links: Array.from(p.querySelectorAll('a')).map(a => a.getAttribute('href'))} : null; }""")
        shared = click(list(SHARED_XY))
        self.shot(page, "record_coincident_popup")
        own = click(list(RECORD_XY))
        far = click([25.0, 35.0])
        badges = page.evaluate("""() => Array.from(document.querySelectorAll('.conc-stack-badge'))
            .map(b => b.textContent).sort()""")
        self.assertIsNotNone(shared)
        self.assertEqual(shared["count"], "2", shared)
        self.assertIn("TGN Larissa", shared["text"])
        self.assertIn("GeoNames Larissa", shared["text"])
        self.assertIn("2 records at this point", shared["text"])
        self.assertIn(f"/atlas/place/{self.T.pk}/", shared["links"])
        self.assertIn("/atlas/?place=gn%3A100", shared["links"])
        self.assertNotIn("Pleiades", shared["text"])                       # absence beside both presences
        self.assertEqual(own["count"], "2", own)                           # this record's point is in its group
        self.assertIn("(this record)", own["text"])
        self.assertIn("Wikidata Larissa", own["text"])
        self.assertEqual(far["count"], "1", far)
        self.assertIn("Pleiades Larisa", far["text"])
        self.assertNotIn("records at this point", far["text"])
        self.assertEqual(badges, ["2", "2"])

    def test_cocited_grouped_by_record(self):
        page = self.new_page(1280)
        ready, _g = self.open(page)
        d = page.evaluate("""() => ({
            records: Array.from(document.querySelectorAll('#concordance_tier3 .conc-cocited-record'))
                .map(e => ({id: e.dataset.placeId, idents: e.dataset.identifiers, text: e.innerText,
                            links: e.querySelectorAll('.conc-cocited-links li').length})),
            byIdentifier: document.querySelectorAll('#concordance_tier3 .conc-cocited-group').length,
            tier3: (document.getElementById('concordance_tier3') || {}).innerText || ''})""")
        self.assertTrue(ready)
        self.assertEqual([r["id"] for r in d["records"]], [str(c.pk) for c in self.C])
        for r in d["records"]:
            self.assertEqual(r["idents"], "gn:100 pl:500")
            self.assertEqual(r["links"], 2)
            self.assertIn("shares 2 identifiers with this record", r["text"])
        for c in self.C:
            self.assertEqual(d["tier3"].count(c.title), 1)                  # each co-citer once
        self.assertEqual(d["byIdentifier"], 0)
        self.assertIn("co-citation, not identity", d["tier3"])

    def test_elsewhere_card_shows_wikipedia(self):
        page = self.new_page(1280)
        ready, gloss = self.open(page)
        d = page.evaluate("""() => { const c = document.getElementById('record_elsewhere');
            return {hidden: c.hidden, text: c.innerText,
                    wp: Array.from(c.querySelectorAll('#record_wikipedia a')).map(a => a.getAttribute('href')),
                    web: Array.from(c.querySelectorAll('#record_outward_links a')).map(a => a.getAttribute('href')),
                    facets: Array.from(document.querySelectorAll('#record_gloss .rs-facet')).map(f => f.dataset.facet)}; }""")
        self.assertTrue(ready and gloss, self.errors)
        self.assertFalse(d["hidden"])
        self.assertEqual(d["wp"][0], "https://en.wikipedia.org/wiki/Larissa")          # English first
        self.assertEqual(len(d["wp"]), 2)
        self.assertIn("Links held in WHG's Wikidata record", d["text"])
        self.assertEqual(d["web"], ["https://example.org/larissa"])
        self.assertIn("sources", d["facets"])                                           # the gloss as cards

    def test_harness_can_fail(self):
        """The layout, wording and environment checks against a page whose bundle never
        loads must all fail (the server-rendered page is there: the presence)."""
        page = self.new_page(1280, block="atlas_record.bundle.js")
        ready, gloss = self.open(page, wait_ms=3000)
        r, d = self.layout_checks(page, 1280)
        r.update(ready=ready, gloss=gloss,
                 env_button=page.evaluate("!!document.querySelector('#record_environment .enr-env-show')"),
                 cocited=page.evaluate("document.querySelectorAll('.conc-cocited-record').length > 0"))
        passed = {k: v for k, v in r.items() if v}
        self.assertEqual(passed, {}, f"checks passed on a page with no bundle: {passed}; {d}")
        self.assertEqual(page.evaluate("document.querySelector('.rs-header h1').textContent"), "Larissa")
