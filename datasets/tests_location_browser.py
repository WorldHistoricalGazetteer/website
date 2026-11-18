"""place#350 in HEADLESS Chromium: placing a location by hand never snaps, and adopting is offered
only for a safe source. Opt-in, like ``tests_feed_browser`` (same harness, same switches):

    WHG_BROWSER_TESTS=1 [WHG_FEED_BUNDLE_DIR=<dir>] GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test datasets.tests_location_browser --settings=whg.settings_localtest

Decision G's requirements, each checked on the rendered page: restricted markers are visible and
labelled as hints; there is no "place at this marker" control; markers cannot be dragged and take
no clicks while placing, so a click ON a restricted marker is stored where the pointer was (the
map's own ``unproject`` of the click), never at the marker's coordinates; the control starts empty;
the stored point is rounded to the zoom and has no source. ``test_harness_can_fail`` runs the same
checks with the feed bundle blocked and requires every one to fail.
"""
import os
import unittest
from unittest.mock import patch

from django.contrib.auth import get_user_model

from datasets import candidates
from datasets.models_matching import CandidateGeneration
from datasets.tests_candidates import GatewayFake, no_network
from datasets.tests_feed_browser import RUN, MapBrowserBase
from utils.livetest import stub_type_vocab

GN = (0.01, 51.76)
OSM = (0.03, 51.77)       # about 1.7 km away: two markers the map shows apart


@unittest.skipUnless(RUN, "set WHG_BROWSER_TESTS=1 to drive the contributor feed in headless Chromium")
class FeedLocationBrowserTests(MapBrowserBase):
    def setUp(self):
        stub_type_vocab(self)
        from api.models import GazetteerRegistryEntry
        from datasets.models import Dataset
        from datasets.tests_candidates import GN_TOWN, hit, response
        from licensing.models import License
        from places.models import Place
        self.fake = GatewayFake()
        for target, fn in (("api.crc_client.crc_search_status", self.fake),
                           ("api.crc_client.requests.post", no_network),
                           ("api.crc_client.requests.get", no_network)):
            p = patch(target, side_effect=fn)
            p.start()
            self.addCleanup(p.stop)
        # Each test sets up the terms it needs: a live-server flush may have removed the seeded rows.
        cc_by, _ = License.objects.get_or_create(spdx_id="CC-BY-4.0", defaults={
            "label": "CC BY 4.0", "permits_commercial": True, "share_alike": False, "no_derivatives": False})
        odbl, _ = License.objects.get_or_create(spdx_id="ODbL-1.0", defaults={
            "label": "ODbL 1.0", "permits_commercial": True, "share_alike": True, "no_derivatives": False})
        for ns, name, lic in (("gn", "GeoNames", cc_by), ("osm", "OpenStreetMap", odbl)):
            GazetteerRegistryEntry.objects.update_or_create(
                id=ns, defaults={"name": name, "namespace": ns, "entry_class": "authority",
                                 "status": "published", "license": lic})
        self.user = get_user_model().objects.create_user(username="mfl_beta", email="mfl@example.org", password="pw",
                                                         given_name="L", surname="Owner", role="beta_tester")
        with patch("datasets.signals.doi"):
            self.ds = Dataset.objects.create(owner=self.user, label="mfl350", title="Parishes 350", description="D",
                                             creator="X", public=False, ds_status="uploaded")
        Dataset.objects.filter(pk=self.ds.pk).update(license=cc_by)
        self.gen = CandidateGeneration.objects.create(dataset=self.ds, requested_by=self.user, status="finished",
                                                      recipe=candidates.build_recipe(self.ds))
        self.rec = Place.objects.create(title="Hoddesdon", src_id="h1", dataset=self.ds, ccodes=[])
        self.fake.by_query["Hoddesdon"] = response(hit("gn:8", "Hoddesdon", GN, types=[GN_TOWN]),
                                                   hit("osm:n8", "Hoddesdon", OSM, types=[GN_TOWN]))
        self.tier = candidates.record_candidates(self.rec, self.gen, self.user)

    # The page's map, read in the page's own world.
    def marker(self, page, cid):
        return page.evaluate("""(cid) => {
            const row = document.querySelector(`#mf_cands .mf-row[data-candidate="${cid}"]`);
            if (!row) return null;
            const el = document.querySelector(`#mf_map .mf-map-pin--cand[data-i="${row.dataset.i}"]`);
            const m = window.__whgMatchFeed.map.instance;
            if (!el || !m) return null;
            const ll = [Number(el.dataset.lng), Number(el.dataset.lat)];
            const box = m.getCanvas().getBoundingClientRect();   // MapLibre's pixels are the canvas's
            const p = m.project(ll);
            return { i: Number(row.dataset.i), ll, x: box.left + p.x, y: box.top + p.y, hint: el.dataset.hint || null,
                     pe: getComputedStyle(el).pointerEvents, draggable: el.getAttribute('draggable') };
        }""", cid)

    def flow(self, page):
        from places.models import PlaceGeom
        r = {}
        page.goto(f"{self.live_server_url}/datasets/{self.ds.pk}/matches/?tier={self.tier}")
        r["ready"] = self.wait_for(page, "window.__whgMatchFeed && window.__whgMatchFeed.ready === true")
        r["map_drawn"] = self.wait_for(page, "window.__whgMatchFeed.map.state === 'ok' && document.querySelectorAll('#mf_map .mf-map-pin--cand').length >= 2", 20000)
        gn, osm = self.marker(page, "gn:8"), self.marker(page, "osm:n8")
        # Restricted markers stay visible, labelled as hints; the safe one is not labelled.
        r["restricted_marker_is_a_labelled_hint"] = bool(osm and osm["hint"] == "hint only: location not adoptable (terms)")
        r["safe_marker_not_labelled"] = bool(gn and gn["hint"] is None)
        # No "place at this marker", no adopt for a suggestion not yet matched, nothing to drag.
        acts = page.evaluate("[...document.querySelectorAll('#match_feed [data-act]')].map((b) => b.dataset.act)")
        r["place_control_offered"] = "place-start" in acts
        r["no_place_at_marker_control"] = bool(acts) and not any(
            a in acts for a in ("adopt", "place-at", "place-marker", "snap", "use-marker"))
        r["markers_not_draggable"] = bool(gn and osm) and gn["draggable"] in (None, "false") and osm["draggable"] in (None, "false")
        # Start placing: the control starts empty, and says what it is.
        page.evaluate("document.querySelector('[data-act=place-start]').click()")
        r["placing"] = self.wait_for(page, "window.__whgMatchFeed.map.placing === true", 5000)
        r["starts_empty"] = page.evaluate("""() => window.__whgMatchFeed.map.placing === true
            && window.__whgMatchFeed.map.pending === null && !document.querySelector('#mf_map [data-role=pending]')
            && document.querySelector('[data-act=place-save]').disabled === true""")
        note = page.evaluate("(document.getElementById('mf_place_note') || {}).textContent || ''")
        r["says_so"] = "You are placing this location yourself." in note and "do not copy their positions" in note
        osm = self.marker(page, "osm:n8")
        r["markers_take_no_clicks_while_placing"] = bool(osm and osm["pe"] == "none")
        # Drag starting ON a marker: never a point, never a moved marker.
        if osm:
            page.mouse.move(osm["x"], osm["y"] - 10)
            page.mouse.down()
            page.mouse.move(osm["x"] + 60, osm["y"] + 40, steps=8)
            page.mouse.up()
        moved = self.marker(page, "osm:n8")
        r["no_drag_from_marker"] = bool(moved) and moved["ll"] == list(OSM) and page.evaluate(
            "window.__whgMatchFeed.map.pending === null")
        # A click on top of the restricted marker (on its body, 8 px above its point), once the
        # drag's pan has come to rest (MapLibre eases after a drag).
        self.wait_for(page, "!window.__whgMatchFeed.map.instance.isMoving()", 5000)
        page.wait_for_timeout(300)
        self.wait_for(page, "!window.__whgMatchFeed.map.instance.isMoving()", 5000)
        osm = self.marker(page, "osm:n8")
        clicked = None
        if osm:
            cx, cy = osm["x"], osm["y"] - 8
            clicked = page.evaluate("""([x, y]) => {
                const m = window.__whgMatchFeed.map.instance;
                const box = m.getCanvas().getBoundingClientRect();   // MapLibre's pixels are the canvas's
                const ll = m.unproject([x - box.left, y - box.top]);
                return { ll: [ll.lng, ll.lat], zoom: m.getZoom() };
            }""", [cx, cy])
            page.mouse.click(cx, cy)
        r["click_gives_a_point"] = self.wait_for(page, "Array.isArray(window.__whgMatchFeed.map.pending)", 5000)
        pending = page.evaluate("window.__whgMatchFeed.map.pending")
        # In screen pixels: the pointer's position is whole pixels, so compare to within one.
        px = page.evaluate("""([a, b]) => {
            const m = window.__whgMatchFeed.map.instance;
            const p = m.project(a), q = m.project(b);
            return Math.hypot(p.x - q.x, p.y - q.y);
        }""", [pending, clicked["ll"]]) if pending and clicked else None
        to_marker = page.evaluate("""([a, b]) => {
            const m = window.__whgMatchFeed.map.instance;
            const p = m.project(a), q = m.project(b);
            return Math.hypot(p.x - q.x, p.y - q.y);
        }""", [pending, list(OSM)]) if pending else None
        r["point_is_where_the_pointer_was"] = px is not None and px < 1.5
        if not r["point_is_where_the_pointer_was"]:
            print("[debug] pointer vs stored point, px:", px, pending, clicked)
        r["not_snapped_to_marker"] = to_marker is not None and to_marker > 6
        # Save: stored rounded to the zoom, with no source.
        page.evaluate("document.querySelector('[data-act=place-save]') && document.querySelector('[data-act=place-save]').click()")
        r["saved"] = self.wait_for(page, "window.__whgMatchFeed.saves === 1", 10000)
        g = PlaceGeom.objects.filter(place=self.rec).first()
        if g is not None and clicked:
            from datasets.location_adoption import decimals_for_zoom
            d = decimals_for_zoom(clicked["zoom"])
            r["stored_rounded_to_zoom"] = g.jsonb["coordinates"] == [round(pending[0], d), round(pending[1], d)]
            r["stored_without_source"] = g.geom_src_id is None and "osm" not in str(g.jsonb)
        else:
            r["stored_rounded_to_zoom"] = r["stored_without_source"] = False
        r["feed_marks_location"] = self.wait_for(
            page, "(document.getElementById('mf_location') || {}).dataset && document.getElementById('mf_location').dataset.kind === 'placed'", 5000)
        # Remove it; then match the GeoNames place and adopt its location: the safe source only.
        page.evaluate("document.querySelector('[data-act=loc-remove]') && document.querySelector('[data-act=loc-remove]').click()")
        r["removed"] = self.wait_for(page, "window.__whgMatchFeed.saves === 2", 10000) and not PlaceGeom.objects.filter(place=self.rec).exists()
        for cid in ("gn:8", "osm:n8"):
            m = self.marker(page, cid)
            if m:
                page.evaluate(f"document.querySelector('#mf_cands [data-act=set][data-i=\"{m['i']}\"][data-d=same]').click()")
        page.evaluate("document.querySelector('[data-act=save]') && document.querySelector('[data-act=save]').click()")
        r["matched"] = self.wait_for(page, "window.__whgMatchFeed.saves === 3", 10000)
        r["adopt_offered_for_safe_only"] = self.wait_for(
            page, "document.querySelectorAll('[data-act=adopt]').length === 1 && !!document.querySelector('[data-ref=\"osm:n8\"]')", 5000)
        r["keep_as_reference_offered"] = page.evaluate("!!document.querySelector('[data-act=keep-ref]')")
        page.evaluate("document.querySelector('[data-act=adopt]') && document.querySelector('[data-act=adopt]').click()")
        r["adopted"] = self.wait_for(page, "window.__whgMatchFeed.saves === 4", 10000)
        g = PlaceGeom.objects.filter(place=self.rec).first()
        r["adopted_from_gn"] = g is not None and g.geom_src_id == "gn" and g.jsonb["coordinates"] == list(GN)
        if os.environ.get("WHG_FEED_SHOT"):
            page.screenshot(path=os.environ["WHG_FEED_SHOT"], full_page=True)
        return r

    def test_placing_never_snaps_and_adopting_is_for_safe_sources_only(self):
        page = self.new_page()
        results = self.flow(page)
        failed = {k: v for k, v in results.items() if not v}
        self.assertEqual(failed, {}, results)

    def test_harness_can_fail(self):
        """The same checks with the feed bundle blocked: every one must fail."""
        page = self.new_page(block_bundle=True)
        with patch.object(self, "wait_for", lambda p, e, t=20000, arg=None: MapBrowserBase.wait_for(self, p, e, 1500, arg)):
            try:
                results = self.flow(page)
            except Exception as exc:  # noqa: BLE001
                results = {"flow_raised": False, "error": str(exc)[:80] and False}
        passed = {k: v for k, v in results.items() if v}
        self.assertEqual(passed, {}, f"checks passed on a page with no feed bundle: {passed}")
        self.assertIn("Match your places: Parishes 350", page.content())
