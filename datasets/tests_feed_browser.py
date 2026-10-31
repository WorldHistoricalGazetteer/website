"""place#338: the contributor feed ("Link your records") in HEADLESS Chromium.

Opt-in, because it starts a browser and a live server:

    WHG_BROWSER_TESTS=1 WHG_FEED_BUNDLE_DIR=<dir> GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test datasets.tests_feed_browser --settings=whg.settings_localtest

``WHG_FEED_BUNDLE_DIR`` serves ``match_feed.bundle.js``/``.css`` and its lazy chunks (the feed map:
``match-feed-map.<hash>.js`` and a vendors chunk) from a scratch build, because ``static/webpack``
is rebuilt only at promotion. Build one outside the repo with the entry alone, e.g. a config that
spreads ``webpack.config.js`` with ``watch: false``, ``cache: false``,
``entry: {match_feed: './whg/webpack/js/matchFeed.js'}`` and ``output.path`` set to the scratch dir,
run from the repo root (``.env/.env`` must exist, as for any bundle build). Without it the
committed static files are used, which predate the map, so the map checks fail.

``FeedMapBrowserTests`` aborts every host other than the live server (the basemap style too, so the
map must fall back to a plain background); ``WHG_FEED_SHOT=<png>`` saves a picture to look at.

The gateway answer is RECORDED: the CRC response for "Coventry" captured on 2026-10-21
(``scripts/fixtures/cluster_parity/real_coventry_2026-10-21.json``), trimmed to 20 hits and served
by the ``GatewayFake`` of ``tests_candidates`` in place of ``api.crc_client.crc_search_status``.
``requests`` inside the client fails the test, so nothing reaches the CRC. The Links are written for
real, through ``api.attestations.create``, into the test database. Playwright's bundled chromium,
fresh profile, never a visible window; the page's readiness flag (``__whgMatchFeed.ready``) is
waited on, not a timer. ``test_harness_can_fail`` runs the flow with the bundle blocked and requires
every check to fail.
"""
import json
import os
import unittest
from unittest.mock import patch

from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.gis.geos import Point
from django.test import Client, LiveServerTestCase

from datasets import candidates
from datasets.models_matching import CandidateGeneration, RecordReviewState
from datasets.tests_candidates import GatewayFake, no_network

RUN = os.environ.get("WHG_BROWSER_TESTS") == "1"
BUNDLE_DIR = os.environ.get("WHG_FEED_BUNDLE_DIR") or ""
BLOCKED_HOSTS = ("analytics.whgazetteer.org", "errors.whgazetteer.org", "plausible", "computingplace.org")
FIXTURE = os.path.join(settings.BASE_DIR, "scripts", "fixtures", "cluster_parity", "real_coventry_2026-10-21.json")
COVENTRY_GN = "gn:2652221"


def bundle_file(url):
    """The scratch-build file for a ``/static/webpack/`` URL (the entry and its lazy chunks: the
    feed map is ``match-feed-map.<hash>.js`` plus a vendors chunk), or None."""
    if not BUNDLE_DIR or "/static/webpack/" not in url:
        return None
    name = url.rsplit("/", 1)[-1].split("?", 1)[0]
    path = os.path.join(BUNDLE_DIR, name)
    return path if os.path.isfile(path) else None


def feed_route(route, block_bundle=False, block=(), external=None, local=None):
    """Serve the scratch bundle; abort the analytics hosts, anything named in ``block``, and (when
    ``local`` is given) every host that is not the live server, noting them in ``external``."""
    url = route.request.url
    if any(h in url for h in BLOCKED_HOSTS) or (block_bundle and "match_feed.bundle" in url) \
            or any(b in url for b in block):
        return route.abort()
    if local and not url.startswith(local) and not url.startswith("data:") and not url.startswith("blob:"):
        if external is not None:
            external.add(url.split("/")[2])
        return route.abort()
    path = bundle_file(url)
    if path:
        ctype = {"css": "text/css", "svg": "image/svg+xml"}.get(path.rsplit(".", 1)[-1], "application/javascript")
        with open(path, "rb") as f:
            return route.fulfill(status=200, body=f.read(), content_type=ctype)
    return route.continue_()


def recorded_coventry():
    with open(FIXTURE) as f:
        data = json.load(f)["data"]
    hits = data["hits"][:20]
    assert any(h["place_id"] == COVENTRY_GN for h in hits), "fixture no longer holds gn:2652221"
    return {**data, "hits": hits}


@unittest.skipUnless(RUN, "set WHG_BROWSER_TESTS=1 to drive the contributor feed in headless Chromium")
class FeedBrowserTests(LiveServerTestCase):
    # LiveServerTestCase flushes the database after each test, which drops the gazetteer registry
    # rows the migrations seed; a Link to gn: is then refused as an unknown namespace, so the
    # second test would fail for its neighbour's reason. Restore them.
    serialized_rollback = True
    @classmethod
    def setUpClass(cls):
        os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
        super().setUpClass()
        from playwright.sync_api import sync_playwright
        cls._pw = sync_playwright().start()
        cls.browser = cls._pw.chromium.launch(headless=True)
        print(f"\n[match feed] bundle from {BUNDLE_DIR or 'static/webpack/'}")

    @classmethod
    def tearDownClass(cls):
        try:
            cls.browser.close()
            cls._pw.stop()
        finally:
            super().tearDownClass()

    def setUp(self):
        from datasets.models import Dataset
        from places.models import Place, PlaceGeom
        self.fake = GatewayFake()
        for target, fn in (("api.crc_client.crc_search_status", self.fake),
                           ("api.crc_client.requests.post", no_network),
                           ("api.crc_client.requests.get", no_network)):
            p = patch(target, side_effect=fn)
            p.start()
            self.addCleanup(p.stop)
        self.recorded = recorded_coventry()
        gn = next(h for h in self.recorded["hits"] if h["place_id"] == COVENTRY_GN)
        self.fake.by_query["Coventry"] = self.recorded
        self.user = get_user_model().objects.create_user(username="mf_beta", email="mf@example.org", password="pw",
                                                         given_name="M", surname="F", role="beta_tester")
        with patch("datasets.signals.doi"):
            self.ds = Dataset.objects.create(owner=self.user, label="mf338", title="Parishes", description="D",
                                             creator="X", public=False, ds_status="uploaded")
        self.rec = Place.objects.create(title="Coventry", src_id="p1", dataset=self.ds, ccodes=[])
        PlaceGeom.objects.create(place=self.rec, geom=Point(*gn["repr_point"], srid=4326),
                                 jsonb={"type": "Point", "coordinates": gn["repr_point"]})
        self.gen = CandidateGeneration.objects.create(dataset=self.ds, requested_by=self.user, status="finished",
                                                      recipe=candidates.build_recipe(self.ds))
        self.tier = candidates.record_candidates(self.rec, self.gen, self.user)

    def new_page(self, block_bundle=False):
        ctx = self.browser.new_context(viewport={"width": 1280, "height": 1000})
        c = Client()
        c.force_login(self.user)
        ctx.add_cookies([{"name": settings.SESSION_COOKIE_NAME, "value": c.cookies[settings.SESSION_COOKIE_NAME].value,
                          "url": self.live_server_url}])
        page = ctx.new_page()

        page.route("**/*", lambda route: feed_route(route, block_bundle))
        return page

    def wait_for(self, page, expr, timeout_ms=20000):
        try:
            page.wait_for_function(expr, timeout=timeout_ms)
            return True
        except Exception:  # noqa: BLE001 — a timeout is a finding
            return False

    def flow(self, page):
        """The feed checks as named results (also run by the harness control)."""
        r = {}
        page.goto(f"{self.live_server_url}/datasets/{self.ds.pk}/matches/?tier={self.tier}")
        r["ready"] = self.wait_for(page, "window.__whgMatchFeed && window.__whgMatchFeed.ready === true")
        shown = page.evaluate("""(gn) => ({
            title: (document.getElementById('mf_record_title') || {}).textContent || '',
            cands: document.querySelectorAll('#mf_cands .mf-cand').length,
            gnIndex: [...document.querySelectorAll('#mf_cands .mf-cand')].findIndex((li) => li.dataset.candidate === gn),
            why: [...document.querySelectorAll('#mf_cands .mf-why')].map((x) => x.textContent).join(' | '),
            text: (document.getElementById('match_feed') || {}).textContent || '' })""", COVENTRY_GN)
        r["record_shown"] = shown["title"].startswith("Coventry")
        r["recorded_candidates_shown"] = shown["cands"] >= 10 and shown["gnIndex"] >= 0
        r["explained"] = "km" in shown["why"]
        r["vocabulary"] = bool(shown["text"]) and not any(w in shown["text"].lower()
                                                          for w in ("attest", "accession", "reconcil"))
        idx = shown["gnIndex"]
        if 0 <= idx < 9:
            page.keyboard.press(str(idx + 1))      # choose the GeoNames Coventry...
            page.keyboard.press("Enter")           # ...and say it is the same place
        r["saved"] = self.wait_for(page, "window.__whgMatchFeed.saves === 1", 20000)
        r["announced"] = "Saved." in (page.evaluate("(document.getElementById('mf_live') || {}).textContent || ''"))
        from api.models import ContributorAttestation
        rows = list(ContributorAttestation.objects.filter(user=self.user))
        r["link_written"] = (len(rows) == 1 and COVENTRY_GN in (rows[0].place_a, rows[0].place_b)
                             and rows[0].relation_type == "closeMatch" and rows[0].candidate_id is not None)
        page.keyboard.press("u")
        r["undone"] = self.wait_for(page, "(document.getElementById('mf_live') || {}).textContent === 'Undone.'", 10000)
        rows = list(ContributorAttestation.objects.filter(user=self.user))
        r["undo_withdrew"] = len(rows) == 1 and rows[0].status == "withdrawn"
        # Hand search: silence is said as silence, beside an answered search through the same box.
        self.fake.by_query["Coventry Cathedral"] = self.recorded
        page.fill("#mf_search_q", "Coventry Cathedral")
        page.keyboard.press("Enter")
        r["search_answered"] = self.wait_for(page, "document.querySelectorAll('#mf_search_results .mf-cand').length >= 10", 10000)
        self.fake.failures["Coventry Cathedral"] = "timeout"
        page.click("#mf_search [data-act=search]")
        r["search_silence_said"] = self.wait_for(page, "!!document.getElementById('mf_search_silent')", 10000)
        return r

    def test_link_undo_and_hand_search_with_the_keyboard(self):
        self.assertEqual(self.tier, "weak")       # the recorded response has no confidence (mode "in")
        self.assertEqual(RecordReviewState.objects.get(place=self.rec).candidate_count, 20)
        page = self.new_page()
        results = self.flow(page)
        failed = {k: v for k, v in results.items() if not v}
        self.assertEqual(failed, {}, results)

    def test_harness_can_fail(self):
        """The same checks with the page bundle blocked: every one must fail."""
        page = self.new_page(block_bundle=True)
        with patch.object(self, "wait_for", lambda p, e, t=20000: FeedBrowserTests.wait_for(self, p, e, 2000)):
            try:
                results = self.flow(page)
            except Exception as exc:  # noqa: BLE001 — a flow that cannot even run has failed every check
                results = {"flow_raised": False, "error": str(exc)[:80] and False}
        passed = {k: v for k, v in results.items() if v}
        self.assertEqual(passed, {}, f"checks passed on a page with no feed bundle: {passed}")
        self.assertIn("Link your records: Parishes", page.content())  # the page was served; only its bundle is missing


# The positions the map test reads: each marker's centre on screen against MapLibre's own
# projection of the coordinates it claims to show (data-lng / data-lat), inside the map box.
MARKERS_JS = """() => {
  const box = document.getElementById('mf_map');
  const st = (window.__whgMatchFeed || {}).map || {};
  const m = st.instance;
  if (!box || !m || typeof m.project !== 'function') return null;
  const br = box.getBoundingClientRect();
  return [...box.querySelectorAll('[data-role]')].map((el) => {
    const r = el.getBoundingClientRect();
    const ll = [Number(el.dataset.lng), Number(el.dataset.lat)];
    const p = m.project(ll);
    // A pin stands on its point (anchor bottom); the record's star is centred on it.
    const cx = r.left + r.width / 2 - br.left;
    const cy = (el.dataset.anchor === 'bottom' ? r.bottom : r.top + r.height / 2) - br.top;
    return { role: el.dataset.role, i: el.dataset.i == null ? null : Number(el.dataset.i), ll,
             dx: Math.abs(cx - p.x), dy: Math.abs(cy - p.y),
             inside: cx >= 0 && cy >= 0 && cx <= br.width && cy <= br.height };
  });
}"""


@unittest.skipUnless(RUN, "set WHG_BROWSER_TESTS=1 to drive the contributor feed in headless Chromium")
class FeedMapBrowserTests(LiveServerTestCase):
    """The feed map: the record (★) and the suggestion in focus, the others muted; it follows the
    person from suggestion to suggestion and record to record; a record with no location says so;
    a map that cannot load leaves the feed working. Also "Which one, if any?": nothing is linked
    until a suggestion is chosen, and "Not the same place" offers the second step.

    Every host other than the live server is aborted (and listed), so the basemap style cannot be
    fetched: the map must fall back to a plain background and still place its markers. Nothing
    leaves the machine."""
    serialized_rollback = True

    @classmethod
    def setUpClass(cls):
        os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
        super().setUpClass()
        from playwright.sync_api import sync_playwright
        cls._pw = sync_playwright().start()
        # Software GL kept as insurance (~/.claude/memory/playwright-maplibre-headless-testing.md).
        cls.browser = cls._pw.chromium.launch(
            headless=True, args=["--enable-unsafe-swiftshader", "--use-gl=angle", "--use-angle=swiftshader"])
        print(f"\n[feed map] bundle from {BUNDLE_DIR or 'static/webpack/'}")

    @classmethod
    def tearDownClass(cls):
        try:
            cls.browser.close()
            cls._pw.stop()
        finally:
            super().tearDownClass()

    def setUp(self):
        from datasets.models import Dataset
        from datasets.tests_candidates import GN_TOWN, hit, response
        from places.models import Place, PlaceGeom
        self.fake = GatewayFake()
        for target, fn in (("api.crc_client.crc_search_status", self.fake),
                           ("api.crc_client.requests.post", no_network),
                           ("api.crc_client.requests.get", no_network)):
            p = patch(target, side_effect=fn)
            p.start()
            self.addCleanup(p.stop)
        self.user = get_user_model().objects.create_user(username="mfm_beta", email="mfm@example.org", password="pw",
                                                         given_name="M", surname="M", role="beta_tester")
        with patch("datasets.signals.doi"):
            self.ds = Dataset.objects.create(owner=self.user, label="mfm338", title="Parishes map", description="D",
                                             creator="X", public=False, ds_status="uploaded")
        self.gen = CandidateGeneration.objects.create(dataset=self.ds, requested_by=self.user, status="finished",
                                                      recipe=candidates.build_recipe(self.ds))

        def rec(title, src, xy, hits):
            pl = Place.objects.create(title=title, src_id=src, dataset=self.ds, ccodes=[])
            if xy:
                PlaceGeom.objects.create(place=pl, geom=Point(*xy, srid=4326),
                                         jsonb={"type": "Point", "coordinates": list(xy)})
            self.fake.by_query[title] = response(*hits)
            return pl, candidates.record_candidates(pl, self.gen, self.user)

        # Two records in "Which one, if any?", far enough apart that the map must move.
        self.brox_xy, self.brox_c = (-0.02, 51.75), [(-0.02, 51.751), (-0.02, 51.756)]
        self.wark_xy, self.wark_c = (-1.59, 52.28), [(-1.59, 52.281), (-1.585, 52.284)]
        self.brox, t1 = rec("Broxbourne", "m1", self.brox_xy,
                            [hit("gn:6", "Broxbourne", self.brox_c[0], types=[GN_TOWN]),
                             hit("wd:Q7", "Broxbourne", self.brox_c[1], types=[GN_TOWN])])
        self.wark, t2 = rec("Warwick", "m2", self.wark_xy,
                            [hit("gn:16", "Warwick", self.wark_c[0], types=[GN_TOWN]),
                             hit("wd:Q17", "Warwick", self.wark_c[1], types=[GN_TOWN])])
        # A record with no location whose only suggestion has none either.
        self.nowhere, self.nowhere_tier = rec("Nowhere Parva", "m3", None, [hit("wd:Q99", "Nowhere Parva")])
        self.tiers = (t1, t2)
        # The list (and the map's numbers) follow the stored rank: put the expected points in it.
        from datasets.models_matching import MatchCandidate
        xy = {"gn:6": self.brox_c[0], "wd:Q7": self.brox_c[1], "gn:16": self.wark_c[0], "wd:Q17": self.wark_c[1]}
        for name, pl in (("brox_c", self.brox), ("wark_c", self.wark)):
            ids = list(MatchCandidate.objects.filter(place=pl, generation=self.gen).order_by("rank")
                       .values_list("candidate_id", flat=True))
            setattr(self, name, [xy[i] for i in ids])
            setattr(self, name + "_ids", ids)

    def new_page(self, block_bundle=False, block=()):
        ctx = self.browser.new_context(viewport={"width": 1400, "height": 1000})
        c = Client()
        c.force_login(self.user)
        ctx.add_cookies([{"name": settings.SESSION_COOKIE_NAME, "value": c.cookies[settings.SESSION_COOKIE_NAME].value,
                          "url": self.live_server_url}])
        page = ctx.new_page()
        self.external = set()
        page.route("**/*", lambda route: feed_route(route, block_bundle, block, self.external, self.live_server_url))
        return page

    def wait_for(self, page, expr, timeout_ms=30000, arg=None):
        try:
            page.wait_for_function(expr, arg=arg, timeout=timeout_ms)
            return True
        except Exception:  # noqa: BLE001 — a timeout is a finding
            return False

    def renders(self, page):
        return page.evaluate("(((window.__whgMatchFeed || {}).map || {}).renders) || 0")

    def placed(self, page, want):
        """True when every expected marker (role, i, [lon, lat]) is on the map, inside the box and
        where MapLibre projects its coordinates (within 3 px)."""
        ok = self.wait_for(page, f"""(want) => {{
            const ms = ({MARKERS_JS})();
            if (!ms) return false;
            return want.every(([role, i, ll]) => ms.some((m) => m.role === role && (i === null || m.i === i)
                && Math.abs(m.ll[0] - ll[0]) < 1e-6 && Math.abs(m.ll[1] - ll[1]) < 1e-6
                && m.inside && m.dx < 3 && m.dy < 3));
        }}""", arg=want, timeout_ms=20000)
        return ok

    def flow(self, page):
        r = {}
        page.goto(f"{self.live_server_url}/datasets/{self.ds.pk}/matches/?tier=ambiguous")
        r["ready"] = self.wait_for(page, "window.__whgMatchFeed && window.__whgMatchFeed.ready === true")
        r["map_drawn"] = self.wait_for(page, "window.__whgMatchFeed.map.state === 'ok' && window.__whgMatchFeed.map.renders >= 1")
        title = page.evaluate("(document.getElementById('mf_record_title') || {}).textContent || ''")
        r["first_record"] = title.startswith("Broxbourne")
        # Both points: the record and the suggestion in focus (the first), the other one muted.
        r["record_and_suggestion_shown"] = self.placed(page, [["record", None, list(self.brox_xy)],
                                                               ["focus", 0, list(self.brox_c[0])],
                                                               ["other", 1, list(self.brox_c[1])]])
        if os.environ.get("WHG_FEED_SHOT"):      # optional: a picture to look at, never compared
            page.screenshot(path=os.environ["WHG_FEED_SHOT"], full_page=True)
        # "Which one, if any?": Enter with nothing chosen links nothing and says why.
        page.keyboard.press("Enter")
        r["must_choose"] = self.wait_for(page, "(document.getElementById('mf_live') || {}).textContent.startsWith('Which one, if any?')", 5000)
        r["nothing_linked_unchosen"] = (r["must_choose"] and page.evaluate("window.__whgMatchFeed.saves") == 0)
        # Choose the second suggestion: the map follows.
        before = self.renders(page)
        page.keyboard.press("2")
        r["map_follows_choice"] = (self.wait_for(page, f"window.__whgMatchFeed.map.renders > {before}", 10000)
                                   and self.placed(page, [["focus", 1, list(self.brox_c[1])],
                                                          ["other", 0, list(self.brox_c[0])],
                                                          ["record", None, list(self.brox_xy)]]))
        # Next record: the record's own point moves.
        before = self.renders(page)
        page.keyboard.press("ArrowRight")
        r["next_record"] = self.wait_for(page, "(document.getElementById('mf_record_title') || {}).textContent.startsWith('Warwick')", 10000)
        r["map_follows_record"] = (self.wait_for(page, f"window.__whgMatchFeed.map.renders > {before}", 10000)
                                   and self.placed(page, [["record", None, list(self.wark_xy)],
                                                          ["focus", 0, list(self.wark_c[0])]])
                                   and not page.evaluate(f"""() => ({MARKERS_JS})().some((m) => m.role === 'record'
                                        && Math.abs(m.ll[1] - {self.brox_xy[1]}) < 1e-6)"""))
        # "Not the same place" in this tier: set aside, then the second step is offered.
        page.click("#mf_cands [data-act=reject-one][data-i='1']")
        r["second_step_offered"] = self.wait_for(page, "!!document.querySelector('.mf-distinct-ask')", 10000)
        page.click(".mf-distinct-ask [data-act=distinct-no]")
        r["second_step_declined"] = self.wait_for(page, "!document.querySelector('.mf-distinct-ask')", 5000)
        from api.models import ContributorAttestation
        from datasets.models_matching import MatchCandidate
        r["set_aside_not_said"] = (r["second_step_offered"]
                                   and MatchCandidate.objects.get(place=self.wark, candidate_id=self.wark_c_ids[1]).status == "rejected"
                                   and not ContributorAttestation.objects.filter(user=self.user).exists())
        # A record with no location at all: a clear "no location" state, never an empty box.
        if self.nowhere_tier == "ambiguous":     # the same tab, after Warwick (ordered by place id)
            page.keyboard.press("ArrowRight")
        else:
            page.goto(f"{self.live_server_url}/datasets/{self.ds.pk}/matches/?tier={self.nowhere_tier}")
            self.wait_for(page, "window.__whgMatchFeed && window.__whgMatchFeed.ready === true")
        r["no_location_record"] = self.wait_for(page, "(document.getElementById('mf_record_title') || {}).textContent.startsWith('Nowhere')", 10000)
        r["no_location_state"] = self.wait_for(page, """window.__whgMatchFeed.map.state === 'no-location'
            && !document.getElementById('mf_map_none').hidden && document.getElementById('mf_map').hidden
            && document.getElementById('mf_map_none').offsetHeight > 100
            && document.getElementById('mf_map_none').textContent.includes('No location')""", 10000)
        return r

    def test_map_shows_both_points_and_follows_the_person(self):
        self.assertEqual(self.tiers, ("ambiguous", "ambiguous"))
        page = self.new_page()
        results = self.flow(page)
        print(f"[feed map] basemap: {page.evaluate('window.__whgMatchFeed.map.basemap')}; "
              f"external hosts refused: {sorted(self.external)}")
        failed = {k: v for k, v in results.items() if not v}
        self.assertEqual(failed, {}, results)

    HAND_XY = [(-0.05, 51.74), (0.01, 51.77)]   # two search results with a point; a third has none

    def hand_flow(self, page):
        """Search by hand: the results join the map as muted pins (s1, s2); picking one focuses it
        and greys the suggestions; the result with no point is counted, not drawn."""
        from datasets.tests_candidates import GN_TOWN, hit, response
        self.fake.by_query["Brox Hand"] = response(
            hit("gn:70", "Brox Hand One", self.HAND_XY[0], types=[GN_TOWN]),
            hit("gn:71", "Brox Hand Two", self.HAND_XY[1], types=[GN_TOWN]),
            hit("wd:Q72", "Brox Hand Three"))
        r = {}
        page.goto(f"{self.live_server_url}/datasets/{self.ds.pk}/matches/?tier=ambiguous")
        r["ready"] = self.wait_for(page, "window.__whgMatchFeed && window.__whgMatchFeed.ready === true")
        r["map_drawn"] = self.wait_for(page, "window.__whgMatchFeed.map.state === 'ok' && window.__whgMatchFeed.map.renders >= 1")
        before = self.renders(page)
        page.fill("#mf_search_q", "Brox Hand")
        page.click("[data-act=search]")
        r["results_listed"] = self.wait_for(page, "document.querySelectorAll('#mf_search_results .mf-cand').length === 3", 15000)
        # Muted pins for the two results with a point, beside the record, the focused suggestion and the other.
        r["hand_pins_muted"] = (self.wait_for(page, f"window.__whgMatchFeed.map.renders > {before}", 10000)
                                and self.placed(page, [["hand", 0, list(self.HAND_XY[0])], ["hand", 1, list(self.HAND_XY[1])],
                                                       ["record", None, list(self.brox_xy)],
                                                       ["focus", 0, list(self.brox_c[0])], ["other", 1, list(self.brox_c[1])]]))
        r["no_point_counted"] = "search results not on the map (no location): s3" in page.evaluate(
            "document.getElementById('mf_map_caption').textContent")
        # Pick the second result: it is the focus, the suggestions are grey.
        before = self.renders(page)
        page.click("#mf_search_results .mf-cand[data-i='1'] .fw-semibold")
        r["picked_result_focused"] = (self.wait_for(page, f"window.__whgMatchFeed.map.renders > {before}", 10000)
                                      and self.placed(page, [["hand-focus", 1, list(self.HAND_XY[1])],
                                                             ["hand", 0, list(self.HAND_XY[0])],
                                                             ["other", 0, list(self.brox_c[0])],
                                                             ["other", 1, list(self.brox_c[1])],
                                                             ["record", None, list(self.brox_xy)]])
                                      and not page.evaluate(f"() => ({MARKERS_JS})().some((m) => m.role === 'focus')"))
        # Linking it still works from the list, and nothing was linked before.
        from api.models import ContributorAttestation
        r["nothing_linked_yet"] = not ContributorAttestation.objects.filter(user=self.user).exists()
        page.click("#mf_search_results .mf-cand[data-i='1'] [data-act=link-hand]")
        r["hand_link_saved"] = self.wait_for(page, "window.__whgMatchFeed.saves === 1", 10000)
        return r

    def test_hand_search_results_appear_on_the_map(self):
        page = self.new_page()
        results = self.hand_flow(page)
        failed = {k: v for k, v in results.items() if not v}
        self.assertEqual(failed, {}, results)

    def test_hand_search_works_when_the_map_cannot_load(self):
        """With the map chunk blocked, searching and linking by hand still work."""
        from datasets.tests_candidates import GN_TOWN, hit, response
        self.fake.by_query["Brox Hand"] = response(hit("gn:70", "Brox Hand One", self.HAND_XY[0], types=[GN_TOWN]))
        page = self.new_page(block=("match-feed-map.",))
        page.goto(f"{self.live_server_url}/datasets/{self.ds.pk}/matches/?tier=ambiguous")
        self.assertTrue(self.wait_for(page, "window.__whgMatchFeed.map.state === 'unavailable'", 20000))
        page.fill("#mf_search_q", "Brox Hand")
        page.click("[data-act=search]")
        self.assertTrue(self.wait_for(page, "document.querySelectorAll('#mf_search_results .mf-cand').length === 1", 15000))
        page.click("#mf_search_results [data-act=link-hand]")
        self.assertTrue(self.wait_for(page, "window.__whgMatchFeed.saves === 1", 10000))

    def test_hand_flow_harness_can_fail(self):
        page = self.new_page(block_bundle=True)
        with patch.object(self, "wait_for", lambda p, e, t=30000, arg=None: FeedMapBrowserTests.wait_for(self, p, e, 1500, arg)):
            try:
                results = self.hand_flow(page)
            except Exception:  # noqa: BLE001 — a flow that cannot even run has failed every check
                results = {}
        passed = {k: v for k, v in results.items() if v}
        self.assertEqual(passed, {}, f"hand-search checks passed with no feed bundle: {passed}")

    def test_feed_works_when_the_map_cannot_load(self):
        """The map chunk blocked: the panel says so, and a Link is still made from the keyboard."""
        page = self.new_page(block=("match-feed-map.",))
        page.goto(f"{self.live_server_url}/datasets/{self.ds.pk}/matches/?tier=ambiguous")
        self.assertTrue(self.wait_for(page, "window.__whgMatchFeed && window.__whgMatchFeed.ready === true"))
        self.assertTrue(self.wait_for(page, "window.__whgMatchFeed.map.state === 'unavailable' "
                                            "&& !document.getElementById('mf_map_failed').hidden", 20000))
        page.keyboard.press("1")
        page.keyboard.press("Enter")
        self.assertTrue(self.wait_for(page, "window.__whgMatchFeed.saves === 1", 10000))
        from api.models import ContributorAttestation
        self.assertEqual(ContributorAttestation.objects.filter(user=self.user).count(), 1)

    def test_harness_can_fail(self):
        """The same checks with the page bundle blocked: every one must fail."""
        page = self.new_page(block_bundle=True)
        with patch.object(self, "wait_for", lambda p, e, t=30000, arg=None: FeedMapBrowserTests.wait_for(self, p, e, 1500, arg)):
            try:
                results = self.flow(page)
            except Exception as exc:  # noqa: BLE001 — a flow that cannot even run has failed every check
                results = {"flow_raised": False, "error": str(exc)[:80] and False}
        passed = {k: v for k, v in results.items() if v}
        self.assertEqual(passed, {}, f"checks passed on a page with no feed bundle: {passed}")
        self.assertIn("Link your records: Parishes map", page.content())
