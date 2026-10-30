"""place#337 (v4 reconciliation Phase 1): the Link sheet on the Record page, in HEADLESS Chromium.

Opt-in, because it starts a browser and a live server:

    WHG_BROWSER_TESTS=1 GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test places.tests_link_sheet_browser --settings=whg.settings_localtest

``WHG_RECORD_BUNDLE_DIR=<dir>`` serves ``atlas_record.bundle.js``/``.css`` from a scratch
build (the committed bundle predates attest.js until the lead rebuilds it). Playwright's
bundled chromium, fresh profile, never a visible window. ``/api/links`` is STUBBED with
``page.route`` in every test: nothing reaches the real endpoint, the gateway overlay or the
database. The CRC gateway is faked at ``api.crc_client.crc_fetch_places``.

Controls: the Link buttons are asserted ABSENT with the page's ``whg-link-enabled`` meta
stripped (after checking the strip removed exactly one tag) and PRESENT with it, in one
test, each beside a count of the Concordance entries they would sit on; exactMatch is
asserted hidden beside its being in the sheet, then shown once Advanced is opened; and
``test_harness_can_fail`` runs the checks with the page bundle blocked and requires every
one to fail.
"""
import json
import os
import unittest
from unittest.mock import patch

from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.gis.geos import Point
from django.test import Client, LiveServerTestCase, override_settings

from places.tests_record_layout_browser import BUNDLE_DIR, bundle_route

RUN = os.environ.get("WHG_BROWSER_TESTS") == "1"
BLOCKED_HOSTS = ("analytics.whgazetteer.org", "errors.whgazetteer.org", "plausible", "computingplace.org")
META = '<meta name="whg-link-enabled" content="1">'
LOCMEM = {**settings.CACHES, "default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache",
                                         "LOCATION": "link337"}}
FIXTURE_STYLE = {"version": 8, "name": "fixture", "sources": {}, "metadata": {"whg:backgroundcolour": "#ddeeff"},
                 "layers": [{"id": "fixture-bg", "type": "background", "paint": {"background-color": "#ddeeff"}}]}


def no_gateway(ids, user=None, allow_anonymous=False, meta=None):
    return {}


@unittest.skipUnless(RUN, "set WHG_BROWSER_TESTS=1 to drive the Link sheet in headless Chromium")
@override_settings(CACHES=LOCMEM)
class LinkSheetBrowserTests(LiveServerTestCase):
    @classmethod
    def setUpClass(cls):
        os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
        super().setUpClass()
        from playwright.sync_api import sync_playwright
        cls._pw = sync_playwright().start()
        cls.browser = cls._pw.chromium.launch(headless=True)  # bundled chromium, temp profile
        print(f"\n[link sheet] atlas_record bundle from {BUNDLE_DIR or 'static/webpack/'}")

    @classmethod
    def tearDownClass(cls):
        try:
            cls.browser.close()
            cls._pw.stop()
        finally:
            super().tearDownClass()

    def setUp(self):
        from datasets.models import Dataset
        from places.models import Place, PlaceGeom, PlaceLink

        User = get_user_model()
        self.beta = User.objects.create_user(username="ls_beta", email="ls_beta@example.org", password="pw",
                                             given_name="L", surname="Beta", role="beta_tester")

        def ds(key):
            return Dataset.objects.create(owner=self.beta, label=f"ls337_{key}", title=f"Dataset {key}",
                                          description="D", creator="X", public=True, ds_status="indexed",
                                          uri_base="https://example.org/places/")

        def place(d, title, src_id, xy, links=()):
            p = Place.objects.create(title=title, src_id=src_id, dataset=d, ccodes=[])
            PlaceGeom.objects.create(place=p, geom=Point(*xy, srid=4326),
                                     jsonb={"type": "Point", "coordinates": list(xy)})
            for t, ident in links:
                PlaceLink.objects.create(place=p, jsonb={"type": t, "identifier": ident})
            return p

        with patch("datasets.signals.doi"):
            a, b = ds("a"), ds("b")
        self.B = place(b, "Record B", "B1", (0.5, 0.5))
        # A cites B (a record WHG holds: resolved, linkable) and a placeholder (not linkable).
        self.A = place(a, "Record A", "A1", (0.0, 0.0), links=[("closeMatch", f"whg:{self.B.pk}"),
                                                               ("closeMatch", "wd:None")])
        from django.core.cache import cache
        cache.clear()
        pp = patch("api.crc_client.crc_clustering_params", return_value=(None, "unreachable"))
        pp.start()
        self.addCleanup(pp.stop)
        pf = patch("api.crc_client.crc_fetch_places", side_effect=no_gateway)
        pf.start()
        self.addCleanup(pf.stop)

    # ---- harness ----------------------------------------------------------
    def new_page(self, strip_meta=False, block_bundle=False, links_status=201):
        """A signed-in beta page. ``strip_meta`` removes the whg-link-enabled tag from the
        served HTML (``self.stripped`` counts what was removed); ``links_status`` is what the
        /api/links stub answers to POST. Every /api/links request is kept in ``self.calls``."""
        ctx = self.browser.new_context(viewport={"width": 1280, "height": 1000})
        c = Client()
        c.force_login(self.beta)
        ctx.add_cookies([{"name": settings.SESSION_COOKIE_NAME, "value": c.cookies[settings.SESSION_COOKIE_NAME].value,
                          "url": self.live_server_url}])
        page = ctx.new_page()
        self.calls, self.stripped, self.links_status = [], None, links_status
        self.page_errors = []
        page.on("pageerror", lambda e: self.page_errors.append(str(e)))

        def route_all(route):
            url = route.request.url
            if any(h in url for h in BLOCKED_HOSTS) or (block_bundle and "atlas_record.bundle.js" in url):
                return route.abort()
            if BUNDLE_DIR and "/atlas_record.bundle." in url:
                return bundle_route(route, route.request)
            return route.continue_()

        def route_record(route):
            if not strip_meta:
                return route.continue_()
            resp = route.fetch()
            html = resp.text()
            self.stripped = html.count(META)
            route.fulfill(response=resp, body=html.replace(META, ""))

        def route_links(route):
            req = route.request
            body = None
            if req.post_data:
                try:
                    body = json.loads(req.post_data)
                except ValueError:
                    body = req.post_data
            self.calls.append({"method": req.method, "url": req.url, "body": body,
                               "csrf": req.headers.get("x-csrftoken", "")})
            if req.method == "GET":
                return route.fulfill(status=200, content_type="application/json",
                                     body=json.dumps({"count": 0, "result": []}))
            st = self.links_status
            payload = {201: {"id": 7, "place_a": (body or {}).get("place_a"), "place_b": (body or {}).get("place_b"),
                             "relation_type": (body or {}).get("relation_type"), "status": "pending",
                             "asserted_at": "2026-10-29T12:00:00Z"},
                       404: {"error": "not found"}, 401: {"detail": "Authentication credentials were not provided."},
                       }.get(st, {"error": "stub"})
            route.fulfill(status=st, content_type="application/json", body=json.dumps(payload))

        page.route("**/*", route_all)
        page.route("**/styles/**/style.json", lambda route: route.fulfill(
            status=200, content_type="application/json", headers={"Access-Control-Allow-Origin": "*"},
            body=json.dumps(FIXTURE_STYLE)))
        page.route(f"**/atlas/place/{self.A.pk}/", route_record)
        page.route("**/api/links*", route_links)
        return page

    def wait_for(self, page, expr, timeout_ms):
        try:
            page.wait_for_function(expr, timeout=timeout_ms)
            return True
        except Exception:  # noqa: BLE001 — a timeout is a finding
            return False

    def open_record(self, page):
        page.goto(f"{self.live_server_url}/atlas/place/{self.A.pk}/#concordance")
        return self.wait_for(page, "window.__whgRecord && window.__whgRecord.concordance === 'ready'", 45000)

    def controls(self, page):
        """Concordance entries, Link buttons, and the one for record B, in ONE evaluation."""
        return page.evaluate("""(b) => ({
            entries: document.querySelectorAll('#concordance_body .conc-entry').length,
            buttons: document.querySelectorAll('#concordance_body .whg-link-btn').length,
            sameB: document.querySelectorAll(`#concordance_body .whg-link-btn[data-link-target="${b}"][data-link-relation="closeMatch"]`).length,
            notB: document.querySelectorAll(`#concordance_body .whg-link-btn[data-link-target="${b}"][data-link-relation="distinct"]`).length,
            placeholder: document.querySelectorAll('#concordance_body .whg-link-btn[data-link-target="wd:None"]').length,
        })""", f"whg:{self.B.pk}")

    def click(self, page, selector):
        # On the element itself: a lingering tooltip over its centre swallows a pointer click.
        return page.evaluate("(s) => { const e = document.querySelector(s); if (!e) return false; e.click(); return true; }",
                             selector)

    def sheet_state(self, page):
        return page.evaluate("""() => {
            const el = document.getElementById('whg_link_sheet');
            // checkVisibility, not client rects: a closed <details> keeps its content laid out
            // (content-visibility: hidden), so rects alone report a hidden radio as shown.
            const vis = (n) => !!n && n.checkVisibility({checkVisibilityCSS: true, contentVisibilityAuto: true});
            if (!el) return null;
            const checked = el.querySelector('input[name="whg_link_relation"]:checked');
            const st = el.querySelector('.whg-link-status');
            return {open: vis(el.querySelector('.modal-dialog')), checked: checked ? checked.value : null,
                    exactInSheet: !!el.querySelector('#whg_link_rel_exactMatch'),
                    exactVisible: vis(el.querySelector('#whg_link_rel_exactMatch')),
                    closeVisible: vis(el.querySelector('#whg_link_rel_closeMatch')),
                    distinctVisible: vis(el.querySelector('#whg_link_rel_distinct')),
                    pair: (el.querySelector('.whg-link-pair') || {}).textContent || '',
                    kind: st ? st.dataset.kind : null, status: st ? st.textContent : '',
                    signin: !!(st && st.querySelector('a[href^="/accounts/login/"]')),
                    text: el.textContent};
        }""")

    def wait_status(self, page):
        return self.wait_for(page, """() => { const s = document.querySelector('#whg_link_sheet .whg-link-status');
            return s && s.dataset.kind && s.dataset.kind !== ''; }""", 10000)

    def sheet_flow(self, page):
        """The checks of the sheet itself, as named results (used by the harness control too)."""
        r = {}
        r["ready"] = self.open_record(page)
        c = self.controls(page)
        r["link_buttons_on_B"] = c["sameB"] == 1 and c["notB"] == 1
        r["no_button_on_placeholder"] = c["entries"] >= 2 and c["placeholder"] == 0
        r["clicked"] = self.click(page, f'#concordance_body .whg-link-btn[data-link-target="whg:{self.B.pk}"]'
                                        f'[data-link-relation="closeMatch"]')
        r["sheet_open"] = self.wait_for(page, """() => { const d = document.querySelector('#whg_link_sheet .modal-dialog');
            return d && d.getClientRects().length > 0; }""", 10000)
        s = self.first_state = self.sheet_state(page) or {}
        r["default_closeMatch"] = s.get("checked") == "closeMatch"
        r["exact_hidden_until_advanced"] = (s.get("exactInSheet") is True and s.get("exactVisible") is False
                                            and s.get("closeVisible") is True and s.get("distinctVisible") is True)
        r["pair_named"] = "Record A" in s.get("pair", "") and "Record B" in s.get("pair", "")
        r["vocabulary"] = bool(s.get("text")) and not any(w in s.get("text", "").lower()
                                                          for w in ("attest", "accession", "reconcil"))
        self.click(page, "#whg_link_sheet details.whg-link-advanced > summary")
        s2 = self.sheet_state(page) or {}
        r["exact_shown_after_advanced"] = s2.get("exactVisible") is True and s2.get("checked") == "closeMatch"
        page.evaluate("""() => { const t = document.getElementById('whg_link_why');
            if (t) { t.value = 'Same coordinates, same name'; t.dispatchEvent(new Event('input')); } }""")
        self.click(page, "#whg_link_sheet .whg-link-submit")
        r["answered"] = self.wait_status(page)
        posts = [x for x in self.calls if x["method"] == "POST"]
        r["posted_exact_json"] = len(posts) == 1 and posts[0]["body"] == {
            "place_a": f"whg:{self.A.pk}", "place_b": f"whg:{self.B.pk}", "relation_type": "closeMatch",
            "justification": "Same coordinates, same name"}
        r["posted_csrf"] = len(posts) == 1 and len(posts[0]["csrf"]) > 10
        s3 = self.sheet_state(page) or {}
        r["saved_and_pending_said"] = (s3.get("kind") == "created" and "Saved" in s3.get("status", "")
                                       and "visible to you and the dataset owners until it is published" in s3.get("status", ""))
        return r

    # ---- tests ---------------------------------------------------------------
    def test_controls_follow_the_page_switch(self):
        """Absent without the meta tag, present with it: same user, same record."""
        off = self.new_page(strip_meta=True)
        self.assertTrue(self.open_record(off), "concordance never became ready (meta stripped)")
        self.assertEqual(self.stripped, 1, "the strip did not find exactly one whg-link-enabled tag to remove")
        c_off = self.controls(off)
        on = self.new_page()
        self.assertTrue(self.open_record(on), "concordance never became ready")
        c_on = self.controls(on)
        self.assertGreaterEqual(c_off["entries"], 2, c_off)       # the entries the buttons would sit on are there
        self.assertEqual(c_off["buttons"], 0, c_off)
        self.assertEqual(c_on["entries"], c_off["entries"], (c_on, c_off))
        self.assertGreater(c_on["buttons"], 0, c_on)
        self.assertEqual((c_on["sameB"], c_on["notB"], c_on["placeholder"]), (1, 1, 0), c_on)
        # Explore similar carries this record into the Atlas as the Link subject, only when on.
        hrefs = [p.evaluate("document.getElementById('explore_similar_header').getAttribute('href')") for p in (off, on)]
        self.assertNotIn("link_from", hrefs[0])
        self.assertIn(f"link_from=whg%3A{self.A.pk}", hrefs[1])
        self.assertEqual(self.page_errors, [])

    def test_sheet_defaults_and_post(self):
        page = self.new_page(links_status=201)
        results = self.sheet_flow(page)
        failed = {k: v for k, v in results.items() if not v}
        self.assertEqual(failed, {}, f"checks failed: {failed}; calls={self.calls}; first={self.first_state}; state={self.sheet_state(page)}; "
                                     f"errors={self.page_errors}")
        self.assertEqual(self.page_errors, [])

    def _refused(self, status):
        page = self.new_page(links_status=status)
        self.assertTrue(self.open_record(page))
        self.assertTrue(self.click(page, f'#concordance_body .whg-link-btn[data-link-target="whg:{self.B.pk}"]'
                                         f'[data-link-relation="distinct"]'))
        self.assertTrue(self.wait_for(page, "() => { const d = document.querySelector('#whg_link_sheet .modal-dialog');"
                                            " return d && d.getClientRects().length > 0; }", 10000))
        self.assertEqual((self.sheet_state(page) or {}).get("checked"), "distinct")   # the button preselects
        self.click(page, "#whg_link_sheet .whg-link-submit")
        self.assertTrue(self.wait_status(page), "no answer was shown")
        posts = [x for x in self.calls if x["method"] == "POST"]
        self.assertEqual(len(posts), 1, self.calls)
        self.assertEqual(posts[0]["body"]["relation_type"], "distinct")
        return self.sheet_state(page)

    def test_404_shows_refusal(self):
        s = self._refused(404)
        self.assertEqual(s["kind"], "notfound", s)
        self.assertIn("could not be found or is not visible to you", s["status"])
        self.assertFalse(s["signin"])

    def test_401_shows_sign_in(self):
        s = self._refused(401)
        self.assertEqual(s["kind"], "signin", s)
        self.assertIn("Sign in to link places", s["status"])
        self.assertTrue(s["signin"], s)
        self.assertNotIn("could not be found", s["status"])

    def test_attestation_entries_and_distinct_line(self):
        """A stubbed Concordance (the backend's shape, place#334 73649514b) carrying a link
        made with the sheet, in tier 1, and a `distinct` one: provenance, the pending note,
        and "Said not to be the same" kept out of the tiers."""
        att = {"id": 7, "place_a": f"whg:{self.A.pk}", "place_b": "wd:Q90", "relation_type": "closeMatch",
               "status": "pending", "asserted_at": "2026-10-29T12:00:00Z", "justification": "", "certainty": None,
               "mine": True, "attested_by": {"name": "Ada Lovelace", "orcid": "0000-0002-1825-0097"}}
        gw = {"status": "resolved", "via": "gateway", "has_geometry": False,
              "records": [{"place_id": "wd:Q90", "title": "Paris (Wikidata)", "namespace": "wd"}]}
        entry = {"tier": 1, "source": "attestation", "identifier": "wd:Q90", "namespace": "wd",
                 "link_type": "closeMatch", "aggregates": True, "target": gw, "attestation": att}
        dist = {**entry, "identifier": "gn:2988507", "link_type": "distinct", "aggregates": False,
                "target": {**gw, "records": [{"place_id": "gn:2988507", "title": "Paris (GeoNames)", "namespace": "gn"}]},
                "attestation": {**att, "id": 8, "place_b": "gn:2988507", "relation_type": "distinct",
                                "status": "active", "mine": False, "attested_by": {"name": "Bob", "orcid": ""}}}
        conc = {"public": True, "record": {"place_id": self.A.pk, "title": "Record A", "repr_point": None},
                "self_identifiers": [f"whg:{self.A.pk}"], "excluded_self_links": 0, "outbound": [entry],
                "inbound": [], "cocited": [], "aggregate": [], "distinct": [dist],
                "truncated": {"inbound": False, "cocited": False}}
        page = self.new_page()
        page.route(f"**/atlas/place/{self.A.pk}/concordance/**", lambda route: route.fulfill(
            status=200, content_type="application/json", body=json.dumps(conc)))
        self.assertTrue(self.open_record(page), "concordance never became ready")
        d = page.evaluate("""() => {
            const t = (s) => Array.from(document.querySelectorAll(s)).map(e => e.textContent.replace(/\\s+/g, ' '));
            return {tier1: t('#concordance_tier1 .conc-entry'), att: t('#concordance_tier1 .conc-attestation'),
                    distinct: t('#concordance_distinct .conc-entry'),
                    distinctHead: (document.querySelector('#concordance_distinct h3') || {}).textContent || '',
                    inTiers: t('#concordance_tier1 .conc-entry, #concordance_tier2 .conc-entry, #concordance_tier3 .conc-entry')
                        .filter(x => x.includes('GeoNames')).length,
                    orcid: Array.from(document.querySelectorAll('#concordance_distinct a[href*="orcid.org"]')).length,
                    change: document.querySelectorAll('#concordance_tier1 .conc-attestation .whg-link-btn').length};
        }""")
        self.assertEqual(len(d["tier1"]), 1, d)
        self.assertEqual(len(d["att"]), 1, d)                       # the entry is there, as a link
        self.assertIn("Paris (Wikidata)", d["att"][0])
        self.assertIn("Linked by you, 2026-10-29", d["att"][0])
        self.assertIn("Visible to you and the dataset owners until it is published", d["att"][0])
        self.assertEqual(d["change"], 1, d)                          # mine: Change or withdraw
        self.assertIn("Said not to be the same (1)", d["distinctHead"])
        self.assertEqual(len(d["distinct"]), 1, d)
        self.assertIn("Paris (GeoNames)", d["distinct"][0])
        self.assertIn("Linked by Bob, 2026-10-29", d["distinct"][0])
        self.assertNotIn("Visible to you", d["distinct"][0])
        self.assertEqual(d["inTiers"], 0, d)                         # never merged into the tiers
        self.assertEqual(d["orcid"], 0, d)                           # Bob gave no ORCID
        self.assertEqual(self.page_errors, [])

    def test_harness_can_fail(self):
        """The sheet checks with the page bundle blocked: every one must fail."""
        page = self.new_page(block_bundle=True)
        with patch.object(self, "wait_for", lambda p, e, t: LinkSheetBrowserTests.wait_for(self, p, e, 2000)):
            results = self.sheet_flow(page)
        passed = {k: v for k, v in results.items() if v}
        self.assertEqual(passed, {}, f"checks passed on a page with no Record bundle: {passed}")
        self.assertGreaterEqual(len(results), 14)
        self.assertEqual(page.evaluate("document.querySelector('#atlas_record h1').textContent"), "Record A")
