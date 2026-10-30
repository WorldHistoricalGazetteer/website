"""place#325 Stage 2: the Record page driven in HEADLESS Chromium (Playwright).

Opt-in, because it starts a browser and a live server:

    WHG_BROWSER_TESTS=1 GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test places.tests_record_page_browser --settings=whg.settings_localtest

Set ``WHG_BROWSER_SHOTS=<dir>`` to keep screenshots. Uses Playwright's own
bundled chromium with a fresh profile (never the user's Chrome, never a
visible window), and serves the COMMITTED ``static/webpack`` bundles, so it
tests what would be deployed. Analytics and error-reporting hosts are blocked
so a test run sends nothing to production services.

Recipe: ~/.claude/memory/playwright-maplibre-headless-testing.md. The page
raises ``window.__whgRecord`` (its own readiness flag); waits are bounded and
a timeout is reported as a failed check, not an exception. Every absence is
asserted beside a presence in the same evaluation, and
``test_harness_can_fail`` runs the same checks with the page's bundle blocked
and requires every one of them to fail.
"""
import json
import os
import time
import unittest
from unittest.mock import patch

from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.gis.geos import Point
from django.test import Client, LiveServerTestCase, override_settings

# place#325 Stage 5: WHG_RECORD_BUNDLE_DIR serves the page bundle from a scratch build.
from places.tests_record_layout_browser import BUNDLE_DIR, bundle_route

RUN = os.environ.get("WHG_BROWSER_TESTS") == "1"
SHOTS = os.environ.get("WHG_BROWSER_SHOTS")
BLOCKED_HOSTS = ("analytics.whgazetteer.org", "errors.whgazetteer.org", "plausible")

# whg_maplibre's load handler reads style.metadata, as the real tileboss styles carry it.
FIXTURE_STYLE = {"version": 8, "name": "fixture", "sources": {}, "metadata": {"whg:backgroundcolour": "#ddeeff"},
                 "layers": [{"id": "fixture-bg", "type": "background", "paint": {"background-color": "#ddeeff"}}]}

GATEWAY_DB = {
    "gn:100": {"place_id": "gn:100", "title": "Gateway Town", "namespace": "gn",
               "repr_point": [10.0, 20.0], "geometries": [{"repr_point": [10.0, 20.0]}]},
}


def fake_fetch(ids, user=None, allow_anonymous=False, meta=None):
    return {i: GATEWAY_DB[i] for i in ids if i in GATEWAY_DB}


def failing_fetch(ids, user=None, allow_anonymous=False, meta=None):
    if meta is not None:
        meta["error"] = "unreachable"
    return {}


# The default cache is file-based and shared with local dev runs; the live server
# here gets a private in-memory one (the other aliases unchanged).
LOCMEM = {**settings.CACHES,
          "default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache", "LOCATION": "rb325"}}


@unittest.skipUnless(RUN, "set WHG_BROWSER_TESTS=1 to drive the Record page in headless Chromium")
@override_settings(CACHES=LOCMEM)
class RecordPageBrowserTests(LiveServerTestCase):
    @classmethod
    def setUpClass(cls):
        # Playwright's sync API runs an event loop in this thread; the ORM calls
        # made here (fixtures, force_login) are ordinary synchronous ones.
        os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
        super().setUpClass()
        from playwright.sync_api import sync_playwright
        cls._pw = sync_playwright().start()
        cls.browser = cls._pw.chromium.launch(headless=True)  # bundled chromium, temp profile

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
        from places.models import Place, PlaceGeom, PlaceLink

        User = get_user_model()
        self.beta = User.objects.create_user(username="rb_beta", email="rb_beta@example.org", password="pw",
                                             given_name="R", surname="Beta", role="beta_tester")

        def ds(key):
            return Dataset.objects.create(owner=self.beta, label=f"rb325_{key}", title=f"Dataset {key}",
                                          description="D", creator="X", public=True, ds_status="indexed",
                                          uri_base="https://example.org/places/")

        def place(d, title, src_id, xy=None, links=()):
            p = Place.objects.create(title=title, src_id=src_id, dataset=d, ccodes=[])
            if xy:
                PlaceGeom.objects.create(place=p, geom=Point(*xy, srid=4326),
                                         jsonb={"type": "Point", "coordinates": list(xy)})
            for t, ident in links:
                PlaceLink.objects.create(place=p, jsonb={"type": t, "identifier": ident})
            return p

        with patch("datasets.signals.doi"):
            a, b = ds("a"), ds("b")
            priv = Dataset.objects.create(owner=self.beta, label="rb325_priv", title="Dataset priv",
                                          description="D", creator="X", public=False, ds_status="indexed",
                                          uri_base="https://example.org/places/")
        GazetteerRegistryEntry.objects.update_or_create(
            id="gn", defaults=dict(name="gn", namespace="gn", entry_class="authority", status="published"))
        GazetteerRegistryEntry.objects.filter(namespace="viaf").delete()
        # Dataset a is in the search index (its registry row was pushed); b is not.
        GazetteerRegistryEntry.objects.update_or_create(
            id=f"whg:{a.pk}", defaults=dict(name="Dataset a", namespace="whg", entry_class="dataset",
                                            status="published"))
        self.ds_a = a
        hidden = place(priv, "Hidden Thing", "H1", xy=(4.0, 4.0))
        self.A = place(a, "Record A", "A1", xy=(0.0, 0.0), links=[("exactMatch", "gn:100"),
                                                                   ("closeMatch", "viaf:55"),
                                                                   # private, and non-existent: one status
                                                                   ("closeMatch", f"whg:{hidden.pk}"),
                                                                   ("closeMatch", f"whg:{hidden.pk + 100000}"),
                                                                   # a placeholder (ruling 2026-10-21)
                                                                   ("closeMatch", "wd:None")])
        self.hidden_pk = hidden.pk
        # Record-page renders read the gateway's clustering_params for the
        # default-settings link. Never the real gateway from here: answer
        # "unreachable" unless a test opts into the real probe (self.real_probe),
        # which then reaches whatever fake requests.post that test installs.
        from django.core.cache import cache
        from api import crc_client
        cache.clear()
        self.real_probe = False
        real = crc_client.crc_clustering_params

        def params(user=None, timeout=None):
            return real(user=user, timeout=timeout) if self.real_probe else (None, "unreachable")
        pp = patch("api.crc_client.crc_clustering_params", side_effect=params)
        pp.start()
        self.addCleanup(pp.stop)
        self.B = place(b, "Record B", "B1", xy=(0.5, 0.5), links=[("closeMatch", f"whg:{self.A.pk}")])
        # The co-citer is far away: if tier 3 leaked into the extent, the map would show it.
        self.C = place(b, "Record C", "C1", xy=(120.0, -40.0), links=[("closeMatch", "gn:100")])
        self.Lonely = place(a, "Lonely Place", "L1", xy=(2.0, 2.0))
        # Placeholder carriers, far away: if wd:None co-cited them, tier 3 would gain a
        # 'wd:None' group (the groups check is exact) and the map extent would reach them.
        for i in range(3):
            place(b, f"Placeholder Carrier {i}", f"PC{i}", xy=(-60.0, 50.0), links=[("closeMatch", "wd:None")])
        # A 2024-migration record-to-record pair, direction unknown, no geometry.
        from places.models import CloseMatch
        self.F = place(b, "Record F", "F1")
        CloseMatch.objects.create(place_a=self.A, place_b=self.F, created_by=self.beta, basis="imported")

    # ---- harness ----------------------------------------------------------
    def new_page(self, block_bundle=False, base_style="fixture", anonymous=False):
        """``base_style``: "fixture" serves a minimal style for the tileserver's
        style.json (from a localhost origin the real one is CORS-blocked, and
        this tests our layers, not the tileserver); "abort" makes it fail.
        ``anonymous``: no session cookie (a signed-out visitor)."""
        ctx = self.browser.new_context(viewport={"width": 1280, "height": 1000})
        if not anonymous:
            c = Client()
            c.force_login(self.beta)
            ctx.add_cookies([{"name": settings.SESSION_COOKIE_NAME,
                              "value": c.cookies[settings.SESSION_COOKIE_NAME].value,
                              "url": self.live_server_url}])
        page = ctx.new_page()
        page.route("**/*", lambda route: route.abort()
                   if any(h in route.request.url for h in BLOCKED_HOSTS)
                   or (block_bundle and "atlas_record.bundle.js" in route.request.url)
                   else (bundle_route(route, route.request) if BUNDLE_DIR and "/atlas_record.bundle." in route.request.url
                         else route.continue_()))
        self.page_errors, self.console = [], []
        if base_style == "fixture":
            page.route("**/styles/**/style.json", lambda route: route.fulfill(
                status=200, content_type="application/json", headers={"Access-Control-Allow-Origin": "*"},
                body=json.dumps(FIXTURE_STYLE)))
        elif base_style == "abort":
            page.route("**/styles/**/style.json", lambda route: route.abort())
        page.on("pageerror", lambda e: self.page_errors.append(str(e)))
        page.on("console", lambda msg: self.console.append(f"{msg.type}: {msg.text}"[:300]))
        page.on("requestfailed", lambda r: self.console.append(f"requestfailed: {r.url[:200]}"))
        return page

    def wait_for(self, page, expr, timeout_ms):
        """True when ``expr`` became truthy within the bound; False (a result,
        not an exception) otherwise."""
        try:
            page.wait_for_function(expr, timeout=timeout_ms)
            return True
        except Exception:  # noqa: BLE001 — playwright TimeoutError: a finding
            return False

    def shot(self, page, name):
        if SHOTS:
            os.makedirs(SHOTS, exist_ok=True)
            page.screenshot(path=os.path.join(SHOTS, f"{name}.png"), full_page=True)

    def concordance_checks(self, page, expect_base="loaded"):
        """Named checks against a loaded Record page for record A."""
        results = {}
        results["ready"] = self.wait_for(page, "window.__whgRecord && window.__whgRecord.concordance === 'ready'",
                                         45000)
        dom = page.evaluate("""() => {
            const t = (sel) => Array.from(document.querySelectorAll(sel)).map(e => e.textContent);
            return {
              tier1: t('#concordance_tier1 .conc-entry'),
              tier2: t('#concordance_tier2 .conc-entry'),
              tier3: t('#concordance_tier3 .conc-entry'),
              // Stage 5: grouped by record, each with the identifiers it shares with this one.
              groups: Array.from(document.querySelectorAll('#concordance_tier3 .conc-cocited-record'))
                        .map(g => g.dataset.identifiers),
              tier3intro: (document.querySelector('#concordance_tier3') || {}).textContent || '',
            };
        }""")
        tier1 = " | ".join(dom["tier1"])
        results["tier1_typed_and_resolved"] = ("exactMatch" in tier1 and "Gateway Town" in tier1
                                               and "on the map" in tier1)
        results["tier1_not_held_reason"] = "viaf:55" in tier1 and "WHG does not hold records for" in tier1
        whg = page.evaluate("""(ids) => ids.map(id => {
            const e = Array.from(document.querySelectorAll('#concordance_tier1 .conc-entry'))
                .find(x => (x.querySelector('code') || {}).textContent === id);
            return e ? {status: e.dataset.status, text: e.textContent.replace(id, '<id>')} : null; })""",
                            [f"whg:{self.hidden_pk}", f"whg:{self.hidden_pk + 100000}"])
        results["tier1_whg_unavailable_same_for_private_and_missing"] = (
            all(w and w["status"] == "unavailable" for w in whg) and whg[0] == whg[1]
            and "may be private, or may no longer exist" in whg[0]["text"]
            and "WHG does not hold records for" not in whg[0]["text"]
            # textContent, not innerText: since round 2 the lists are closed folds, and an
            # absence read from rendered text only would hold for anything in a closed one.
            and "Hidden Thing" not in page.evaluate("document.body.textContent"))
        ph = page.evaluate("""() => { const e = Array.from(document.querySelectorAll('#concordance_tier1 .conc-entry'))
            .find(x => (x.querySelector('code') || {}).textContent === 'wd:None');
            return e ? {status: e.dataset.status, text: e.textContent,
                        badge: !!e.querySelector('.conc-placeholder-badge')} : null; }""")
        results["tier1_placeholder_flagged"] = bool(ph) and ph["status"] == "placeholder" and ph["badge"] \
            and "Placeholder, not a real identifier" in ph["text"] and "holds no record" not in ph["text"] \
            and "Placeholder Carrier" not in page.evaluate("document.body.textContent")
        tier2 = " | ".join(dom["tier2"])
        results["tier2_attributed"] = ("Linked by Dataset b" in tier2 and "Record B" in tier2
                                       and "Record C" not in tier2)
        results["tier2_imported_provenance"] = ("Record F" in tier2 and "2024 migration; provenance unknown" in tier2
                                                and "which side made it is not recorded" in tier2
                                                and "not mapped: no geometry" in tier2)
        results["tier3_cocitation"] = (dom["groups"] == ["gn:100"] and "Record C" in " ".join(dom["tier3"])
                                       and "co-citation, not identity" in dom["tier3intro"]
                                       and "Record B" not in " ".join(dom["tier3"]))
        results["map_drawn"] = self.wait_for(page, "window.__whgRecord && window.__whgRecord.mapDrawn >= 1", 60000)
        self.map_diag = page.evaluate("""() => {
            const map = window.__whgRecordMap;
            return {hasLib: !!window.whg_maplibre, hasMap: !!map, hidden: document.hidden,
                    styleLoaded: !!(map && map.isStyleLoaded && map.isStyleLoaded()),
                    loaded: !!(map && map.loaded && map.loaded()), state: window.__whgRecord};
        }""")
        m = page.evaluate("""() => {
            const map = window.__whgRecordMap;
            if (!map || !map.getSource('concordance')) return null;
            const titles = map.querySourceFeatures('concordance').map(f => f.properties.role + ':' + f.properties.title);
            const b = map.getBounds();
            const has = (lng, lat) => b.contains([lng, lat]);
            return {titles: Array.from(new Set(titles)).sort(),
                    rendered: map.queryRenderedFeatures({layers: ['concordance-members']}).map(f => f.properties.title),
                    a: has(0, 0), b: has(0.5, 0.5), gw: has(10, 20), c: has(120, -40), ph: has(-60, 50)};
        }""")
        results["base_map_loaded"] = bool(self.map_diag.get("state")) and \
            self.map_diag["state"].get("baseMap") == expect_base
        results["map_aggregate_only"] = bool(m) and m["titles"] == [
            "member:Gateway Town", "member:Record B", "record:Record A"]
        results["map_extent_excludes_tier3"] = bool(m) and m["a"] and m["b"] and m["gw"] and not m["c"] \
            and not m["ph"]
        # The canvas fills its container (dev walkthrough 2026-10-21: 400x300 in ~1294 px,
        # because the map was created while the section was still hidden).
        size = page.evaluate("""() => { const el = document.getElementById('concordance_map');
            const map = window.__whgRecordMap; if (!el || !map) return null;
            const c = map.getCanvas();
            return {box: el.clientWidth, boxH: el.clientHeight, css: c.clientWidth, cssH: c.clientHeight}; }""")
        self.map_size = size
        # Stage 5: the map is the right column's square box (~450 px at 1280 wide).
        results["map_canvas_fills_container"] = bool(size) and size["box"] > 300 \
            and abs(size["css"] - size["box"]) <= 2 and abs(size["cssH"] - size["boxH"]) <= 2
        results["map_rendered_members"] = bool(m) and {"Gateway Town", "Record B"} <= set(m["rendered"]) \
            and "Record C" not in m["rendered"]
        return results, m

    # ---- tests ---------------------------------------------------------------
    def test_concordance_section(self):
        page = self.new_page()
        with patch("api.crc_client.crc_fetch_places", side_effect=fake_fetch):
            page.goto(f"{self.live_server_url}/atlas/place/{self.A.pk}/#concordance")
            results, m = self.concordance_checks(page)
        self.shot(page, "record_with_concordance")
        failed = {k: v for k, v in results.items() if not v}
        self.assertEqual(failed, {}, f"checks failed: {failed}; map={m}; size={self.map_size}; "
                         f"diag={self.map_diag}; errors={self.page_errors}; console={self.console[-25:]}")
        self.assertEqual(self.page_errors, [])
        # ... and keeps tracking it when the container changes size later.
        # (Stage 5: narrower, still two columns; below 992 px the map takes the full width.)
        page.set_viewport_size({"width": 1100, "height": 1000})
        tracked = self._tracked(page, self.map_size["box"])
        after = page.evaluate("""() => [document.getElementById('concordance_map').clientWidth,
                                        window.__whgRecordMap.getCanvas().clientWidth]""")
        self.assertTrue(tracked, f"canvas did not follow its container: {after} (before: {self.map_size})")
        self.assertLess(after[0], self.map_size["box"])                   # the container did shrink

    def _tracked(self, page, before):
        try:
            page.wait_for_function("""(before) => { const el = document.getElementById('concordance_map');
                const c = window.__whgRecordMap.getCanvas();
                return el.clientWidth < before && Math.abs(c.clientWidth - el.clientWidth) <= 2; }""",
                                   arg=before, timeout=5000)
            return True
        except Exception:  # noqa: BLE001 — a timeout is a finding
            return False

    def test_unreachable_base_map_still_maps(self):
        """The base style failing outright falls back to a plain background, and
        the concordance is still drawn on it."""
        page = self.new_page(base_style="abort")
        with patch("api.crc_client.crc_fetch_places", side_effect=fake_fetch):
            page.goto(f"{self.live_server_url}/atlas/place/{self.A.pk}/#concordance")
            results, m = self.concordance_checks(page, expect_base="fallback")
        self.shot(page, "record_basemap_fallback")
        failed = {k: v for k, v in results.items() if not v}
        self.assertEqual(failed, {}, f"checks failed: {failed}; map={m}; diag={self.map_diag}; "
                         f"console={self.console[-25:]}")
        self.assertIn("base map could not be loaded", page.inner_text("#concordance_map_note"))

    def test_page_paints_before_concordance(self):
        """The record renders while the concordance is still being computed."""
        from places import concordance as conc_mod
        real = conc_mod.build_concordance

        def slow(*a, **kw):
            time.sleep(3)
            return real(*a, **kw)

        page = self.new_page()
        with patch("api.crc_client.crc_fetch_places", side_effect=fake_fetch), \
                patch("places.concordance.build_concordance", side_effect=slow):
            page.goto(f"{self.live_server_url}/atlas/place/{self.A.pk}/", wait_until="domcontentloaded")
            loading = self.wait_for(page, "window.__whgRecord && window.__whgRecord.concordance === 'loading'", 10000)
            early = page.evaluate("""() => ({
                title: document.querySelector('#atlas_record h1').textContent,
                spinner: !!document.querySelector('#concordance_status .spinner-border')
                         && !document.getElementById('concordance_status').hidden,
                body: document.getElementById('concordance_body').hidden})""")
            self.shot(page, "record_loading")
            ready = self.wait_for(page, "window.__whgRecord.concordance === 'ready'", 45000)
            spinner_after = page.evaluate("document.getElementById('concordance_status').hidden")
        self.assertTrue(loading)
        self.assertEqual(early, {"title": "Record A", "spinner": True, "body": True})
        self.assertTrue(ready)
        self.assertTrue(spinner_after)

    def test_empty_state_points_to_the_header_explore(self):
        """Round 2: the page's one Explore button is the header's; the empty state names it
        and links to it, and carries no button of its own."""
        page = self.new_page()
        with patch("api.crc_client.crc_fetch_places", side_effect=fake_fetch):
            page.goto(f"{self.live_server_url}/atlas/place/{self.Lonely.pk}/#concordance")
            ok = self.wait_for(page, "window.__whgRecord && window.__whgRecord.concordance === 'empty'", 45000)
        self.shot(page, "record_empty_concordance")
        d = page.evaluate("""() => {
            const e = document.getElementById('concordance_empty');
            const ref = e && e.querySelector('a');
            const header = document.getElementById('explore_similar_header');
            return {exists: !!e, ref: ref && ref.textContent.trim(), refHref: ref && ref.getAttribute('href'),
                    buttons: e ? e.querySelectorAll('.btn, button').length : null,
                    header: header && header.getAttribute('href'),
                    explore: Array.from(document.querySelectorAll('#atlas_record .btn'))
                        .filter(b => /Explore similar records/.test(b.textContent)).length,
                    map: !!document.getElementById('concordance_map'),
                    tiers: document.querySelectorAll('.conc-tier').length};
        }""")
        self.assertTrue(ok)
        self.assertEqual(d, {"exists": True, "ref": "Explore similar records", "refHref": "#explore_similar_header",
                             "buttons": 0,
                             # The Explore link carries the record it came from, so the Atlas
                             # results can offer to link back to it (place#337).
                             "header": f"/atlas/Lonely%20Place?link_from=whg%3A{self.Lonely.pk}&link_from_label=Lonely+Place",
                             "explore": 1, "map": False,
                             "tiers": 0})

    def test_gateway_unreachable_is_not_absence(self):
        page = self.new_page()
        with patch("api.crc_client.crc_fetch_places", side_effect=failing_fetch):
            page.goto(f"{self.live_server_url}/atlas/place/{self.A.pk}/#concordance")
            ok = self.wait_for(page, "window.__whgRecord && window.__whgRecord.concordance === 'ready'", 45000)
        self.shot(page, "record_gateway_unreachable")
        d = page.evaluate("""() => Object.fromEntries(Array.from(
            document.querySelectorAll('#concordance_tier1 .conc-entry[data-source="place_link"]'))
            .map(e => [e.querySelector('code').textContent, e.dataset.status + ' :: ' + e.textContent]))""")
        self.assertTrue(ok)
        self.assertIn("gateway_unavailable", d["gn:100"])
        self.assertIn("not a finding that it is absent", d["gn:100"])
        self.assertNotIn("holds no record", d["gn:100"])
        self.assertIn("not_held", d["viaf:55"])

    def _gn100_entry(self, page):
        return page.evaluate("""() => { const e = Array.from(
            document.querySelectorAll('#concordance_tier1 .conc-entry[data-source="place_link"]'))
            .find(x => (x.querySelector('code') || {}).textContent === 'gn:100');
            return e ? e.dataset.status + ' :: ' + e.textContent : null; }""")

    @override_settings(V4_LANDING_ENABLED=True, CRC_GATEWAY_URL="http://gateway.invalid")
    def test_signed_out_visitor_is_not_told_not_configured(self):
        """Review 2026-10-21: the gateway IS configured; a signed-out visitor is simply
        not looked up for. The real crc_fetch_places decides (and calls nothing)."""
        page = self.new_page(anonymous=True)
        with patch("requests.post", side_effect=AssertionError("no network from tests")):
            page.goto(f"{self.live_server_url}/atlas/place/{self.A.pk}/#concordance")
            ok = self.wait_for(page, "window.__whgRecord && window.__whgRecord.concordance === 'ready'", 45000)
        self.shot(page, "record_signed_out_gateway")
        text = self._gn100_entry(page)
        self.assertTrue(ok)
        self.assertIn("gateway_disabled", text)
        self.assertIn("Not looked up for signed-out visitors", text)
        self.assertIn("Sign in to see it", text)
        self.assertNotIn("not configured", text)
        self.assertIn("Record B", page.text_content("#concordance_tier2"))     # the rest still renders

    @override_settings(V4_LANDING_ENABLED=True, CRC_GATEWAY_URL="http://gateway.invalid")
    def test_signed_out_visitor_sees_open_namespaces_resolved(self):
        """Ruling 2026-10-21: an openly redistributable namespace (licence recorded,
        redistributable, published) IS looked up for a signed-out visitor; the same page
        with the licence unknown is not (the test above)."""
        from api.models import GazetteerRegistryEntry
        from licensing.models import License
        lic, _ = License.objects.get_or_create(spdx_id="CC-BY-4.0", defaults={"label": "CC BY 4.0"})
        GazetteerRegistryEntry.objects.filter(id="gn").update(license=lic, redistributable=True)
        asked = []

        def fetch(ids, user=None, allow_anonymous=False, meta=None):
            asked.append((tuple(ids), allow_anonymous))
            return fake_fetch(ids) if allow_anonymous else {}
        page = self.new_page(anonymous=True)
        with patch("api.crc_client.crc_fetch_places", side_effect=fetch):
            page.goto(f"{self.live_server_url}/atlas/place/{self.A.pk}/#concordance")
            ok = self.wait_for(page, "window.__whgRecord && window.__whgRecord.concordance === 'ready'", 45000)
            mapped = self.wait_for(page, "window.__whgRecord.mapDrawn >= 1", 30000)
        text = self._gn100_entry(page)
        self.assertTrue(ok)
        self.assertIn("resolved", text)
        self.assertIn("Gateway Town", text)
        self.assertNotIn("signed-out", text)
        # Every ask is for the open namespace only, always with the anonymous opt-in.
        # Not exactly one: with the switch on the page's public gloss/environment
        # sections (ruling 2026-10-21) read the group's gateway details too, under
        # the same anonymous rule (places.record_group.gateway_members).
        self.assertTrue(asked)
        self.assertEqual(set(asked), {(("gn:100",), True)})
        self.assertTrue(mapped)
        self.assertEqual(page.evaluate("window.__whgRecord.mapped"), 2)       # Gateway Town + Record B

    @override_settings(CRC_GATEWAY_URL="")
    def test_unconfigured_server_says_not_configured(self):
        page = self.new_page()
        with patch("requests.post", side_effect=AssertionError("no network from tests")):
            page.goto(f"{self.live_server_url}/atlas/place/{self.A.pk}/#concordance")
            ok = self.wait_for(page, "window.__whgRecord && window.__whgRecord.concordance === 'ready'", 45000)
        text = self._gn100_entry(page)
        self.assertTrue(ok)
        self.assertIn("gateway_disabled", text)
        self.assertIn("not configured on this server", text)
        self.assertNotIn("signed-out", text)

    @override_settings(CRC_GATEWAY_URL="http://gateway.invalid", CRC_GATEWAY_TIMEOUT=2)
    def test_default_cluster_link_resolves(self):
        """The secondary header link opens the locus page for this record at the
        Atlas defaults, and that page re-runs it and selects the record's cluster.
        A record whose dataset is not in the index has no such link (but keeps
        the primary one)."""
        from search.tests_locus import PARAMS, STOPLIST, FakeGateway, _Resp
        self.real_probe = True   # the probe reaches the fake gateway, which ships PARAMS
        member = f"whg:{self.ds_a.pk}:L1"
        gw = FakeGateway(hits=[
            {"place_id": member, "title": "Lonely Place", "namespace": "whg", "repr_point": [2.0, 2.0],
             "names": [{"label": "Lonely Place"}]},
            {"place_id": "gn:9", "title": "Lonely Place", "namespace": "gn", "repr_point": [100.0, -30.0],
             "names": [{"label": "Lonely Place"}]},
        ])
        page = self.new_page()
        page.add_init_script("try { localStorage.setItem('whg.debug', '1'); } catch (e) {}")
        with patch("api.crc_client.requests.post", side_effect=gw), \
                patch("api.crc_client.requests.get", side_effect=lambda *a, **k: _Resp(200)), \
                patch("api.crc_client.crc_fetch_places", side_effect=fake_fetch):
            page.goto(f"{self.live_server_url}/atlas/place/{self.B.pk}/")
            on_b = page.evaluate("""() => ({primary: !!document.getElementById('explore_similar_header'),
                                           secondary: !!document.getElementById('default_cluster_link')})""")
            page.goto(f"{self.live_server_url}/atlas/place/{self.Lonely.pk}/")
            link = page.evaluate("""() => { const a = document.getElementById('default_cluster_link');
                return a && {text: a.textContent.trim(), href: a.getAttribute('href'),
                             primary: !!document.getElementById('explore_similar_header')}; }""")
            self.assertIsNotNone(link, f"no default-settings link; console={self.console[-10:]}")
            page.click("#default_cluster_link")
            ok = self.wait_for(page, "window.__whgLocus && window.__whgLocus.resolved !== undefined", 45000)
            state = page.evaluate("window.__whgLocus || null")
            self.shot(page, "record_default_cluster_locus")
        self.assertEqual(on_b, {"primary": True, "secondary": False})
        self.assertEqual(link["text"], "See this record's cluster at gateway-calibrated settings")
        self.assertTrue(link["primary"])
        self.assertTrue(link["href"].startswith("/atlas/locus/?r="), link["href"])
        self.assertTrue(ok, f"the locus page never resolved; console={self.console[-10:]}")
        self.assertIs(state["resolved"], True, state)
        self.assertEqual(state["surviving"], [member])
        self.assertEqual(state["missing"], [])
        self.assertIn(member, state["selectedIds"])
        self.assertNotIn("gn:9", state["selectedIds"])      # the far namesake is another cluster
        self.assertEqual(state["stoplistCount"], len(STOPLIST))
        # The page re-clustered with the CALIBRATED weights the fake gateway shipped.
        self.assertEqual(state["params"]["weights"], PARAMS["weights"])
        # Bodies: ONE clustering_params probe (B has no link, so it asked nothing;
        # Lonely's render probed once) and the locus re-run.
        from api.crc_client import CLUSTERING_PARAMS_PROBE
        self.assertEqual(gw.bodies[0], CLUSTERING_PARAMS_PROBE)
        self.assertEqual(len(gw.bodies), 2, gw.bodies)
        self.assertEqual((gw.bodies[1]["query"], gw.bodies[1]["mode"], gw.bodies[1]["size"]),
                         ("Lonely Place", "in", 100))

    def test_default_cluster_link_falls_back_labelled(self):
        """Gateway params unreachable: the link still appears, says WHG fallback settings."""
        page = self.new_page()
        with patch("api.crc_client.crc_fetch_places", side_effect=fake_fetch):
            page.goto(f"{self.live_server_url}/atlas/place/{self.Lonely.pk}/")
            link = page.evaluate("""() => { const a = document.getElementById('default_cluster_link');
                return a && {text: a.textContent.trim(), settings: a.dataset.settings,
                             reason: a.dataset.fallbackReason}; }""")
        self.assertEqual(link, {"text": "See this record's cluster at WHG fallback settings",
                                "settings": "fallback", "reason": "unreachable"})

    def test_harness_can_fail(self):
        """The same checks, with the page's bundle blocked: every one must fail.
        A green run of test_concordance_section therefore means the page did it."""
        page = self.new_page(block_bundle=True)
        with patch("api.crc_client.crc_fetch_places", side_effect=fake_fetch), \
                patch.object(self, "wait_for", lambda p, e, t: RecordPageBrowserTests.wait_for(self, p, e, 3000)):
            page.goto(f"{self.live_server_url}/atlas/place/{self.A.pk}/")
            results, _m = self.concordance_checks(page)
        passed = {k: v for k, v in results.items() if v}
        self.assertEqual(passed, {}, f"checks passed on a page that cannot render: {passed}")
        self.assertGreaterEqual(len(results), 8)
        # and the subject did load: the server-rendered record is there
        self.assertEqual(page.evaluate("document.querySelector('#atlas_record h1').textContent"), "Record A")
