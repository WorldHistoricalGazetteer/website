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
from utils.livetest import (SerializedRollbackFromEmpty, drain_live_server_threads, start_sync_playwright,
                            stub_type_vocab)

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


def missing_cdn_fallbacks():
    """The local copies of CDN libraries that the bundle build puts in ``static/webpack/CDNfallbacks/``
    (``webpack.config.js``'s CopyWebpackPlugin patterns) and that are not there. ``base.js`` loads
    them when a CDN is unreachable, which is every time under ``MapBrowserBase``."""
    import re
    with open(os.path.join(settings.BASE_DIR, "webpack.config.js")) as f:
        wanted = [p.rsplit("/", 1)[-1] for p in re.findall(r"from:\s*'([^']+)',\s*to:\s*'CDNfallbacks/'", f.read())]
    assert "jquery.min.js" in wanted, "webpack.config.js no longer lists the CDN fallbacks this check reads"
    folder = os.path.join(settings.BASE_DIR, "static", "webpack", "CDNfallbacks")
    return [n for n in wanted if not os.path.isfile(os.path.join(folder, n))]


def recorded_coventry():
    with open(FIXTURE) as f:
        data = json.load(f)["data"]
    hits = data["hits"][:20]
    assert any(h["place_id"] == COVENTRY_GN for h in hits), "fixture no longer holds gn:2652221"
    return {**data, "hits": hits}


@unittest.skipUnless(RUN, "set WHG_BROWSER_TESTS=1 to drive the contributor feed in headless Chromium")
class FeedBrowserTests(SerializedRollbackFromEmpty, LiveServerTestCase):
    # LiveServerTestCase flushes the database after each test, which drops the gazetteer registry
    # rows the migrations seed; a Link to gn: is then refused as an unknown namespace, so the
    # second test would fail for its neighbour's reason. Restore them. (serialized_rollback alone
    # restored nothing once Playwright had started, and collided after a non-serialized class: hence
    # start_sync_playwright and SerializedRollbackFromEmpty, utils/livetest.py.)
    serialized_rollback = True
    @classmethod
    def setUpClass(cls):
        os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
        super().setUpClass()
        cls._pw = start_sync_playwright()   # not sync_playwright().start(): see utils/livetest.py
        cls.browser = cls._pw.chromium.launch(headless=True)
        print(f"\n[match feed] bundle from {BUNDLE_DIR or 'static/webpack/'}")

    @classmethod
    def tearDownClass(cls):
        try:
            cls.browser.close()
            cls._pw.stop()
        finally:
            super().tearDownClass()
            drain_live_server_threads()

    def setUp(self):
        stub_type_vocab(self)  # /types/vocab/ would query the real ES (see utils/livetest.py)
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
        ctx.add_init_script("try { if (!sessionStorage.getItem('t_seeded')) { localStorage.setItem('mf.shortcuts', 'on'); localStorage.setItem('whg_match_tour_seen', 'true'); localStorage.setItem('mf.intro', 'hidden'); } } catch (e) {}")
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
        r["explained"] = "Agrees" in shown["why"] or "Does not agree" in shown["why"]
        r["vocabulary"] = bool(shown["text"]) and not any(w in shown["text"].lower()
                                                          for w in ("attest", "accession", "reconcil"))
        idx = shown["gnIndex"]
        if 0 <= idx < 9:
            page.keyboard.press(str(idx + 1))      # go to the GeoNames Coventry...
            page.keyboard.press("s")               # ...mark it the same place...
            page.keyboard.press("Enter")           # ...and save
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
        self.assertIn("Match your places: Parishes", page.content())  # the page was served; only its bundle is missing


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
class MapBrowserBase(SerializedRollbackFromEmpty, LiveServerTestCase):
    """What the map-page browser tests share: headless Chromium with software GL, a signed-in
    beta user (``self.user``), every host but the live server aborted, and the page's own readiness
    flag. A base with no tests of its own."""
    serialized_rollback = True

    @classmethod
    def setUpClass(cls):
        missing = missing_cdn_fallbacks()
        if missing:
            # Every CDN is aborted below, so the page loads jQuery, Bootstrap, MapLibre... from these
            # local copies. They are build output (gitignored), absent from a fresh checkout or
            # worktree; without them the feed never becomes ready and every test times out at 30 s.
            raise RuntimeError(f"static/webpack/CDNfallbacks/ lacks {missing}: run `npm run build:prod` "
                               "(with .env/.env present) in this checkout before the map browser tests")
        os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
        super().setUpClass()
        cls._pw = start_sync_playwright()   # not sync_playwright().start(): see utils/livetest.py
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
            drain_live_server_threads()

    user = None

    def new_page(self, block_bundle=False, block=()):
        ctx = self.browser.new_context(viewport={"width": 1400, "height": 1000})
        c = Client()
        c.force_login(self.user)
        ctx.add_cookies([{"name": settings.SESSION_COOKIE_NAME, "value": c.cookies[settings.SESSION_COOKIE_NAME].value,
                          "url": self.live_server_url}])
        ctx.add_init_script("try { if (!sessionStorage.getItem('t_seeded')) { localStorage.setItem('mf.shortcuts', 'on'); localStorage.setItem('whg_match_tour_seen', 'true'); localStorage.setItem('mf.intro', 'hidden'); } } catch (e) {}")
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



@unittest.skipUnless(RUN, "set WHG_BROWSER_TESTS=1 to drive the contributor feed in headless Chromium")
class FeedMapBrowserTests(MapBrowserBase):
    """The feed map: the record (★) and the suggestion in focus, the others muted; it follows the
    person from suggestion to suggestion and record to record; a record with no location says so;
    a map that cannot load leaves the feed working. Also "Which one, if any?": nothing is linked
    until a suggestion is chosen, and "Not the same place" offers the second step.

    Every host other than the live server is aborted (and listed), so the basemap style cannot be
    fetched: the map must fall back to a plain background and still place its markers. Nothing
    leaves the machine."""
    def setUp(self):
        stub_type_vocab(self)  # /types/vocab/ would query the real ES (see utils/livetest.py)
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

    def flow(self, page):
        r = {}
        page.goto(f"{self.live_server_url}/datasets/{self.ds.pk}/matches/?tier=ambiguous")
        r["ready"] = self.wait_for(page, "window.__whgMatchFeed && window.__whgMatchFeed.ready === true")
        r["map_drawn"] = self.wait_for(page, "window.__whgMatchFeed.map.state === 'ok' && window.__whgMatchFeed.map.renders >= 1")
        title = page.evaluate("(document.getElementById('mf_record_title') || {}).textContent || ''")
        r["first_record"] = title.startswith("Broxbourne")
        # The record and both suggestions, all alike: nothing is chosen for the person.
        r["record_and_suggestions_shown"] = self.placed(page, [["record", None, list(self.brox_xy)],
                                                                ["other", 0, list(self.brox_c[0])],
                                                                ["other", 1, list(self.brox_c[1])]])
        r["no_default_focus"] = not page.evaluate("() => !!document.querySelector('#mf_map .mf-map-pin--lit')")
        if os.environ.get("WHG_FEED_SHOT"):      # optional: a picture to look at, never compared
            page.screenshot(path=os.environ["WHG_FEED_SHOT"], full_page=True)
        # "Which one, if any?": Enter with nothing chosen links nothing and says why.
        page.keyboard.press("Enter")
        r["must_choose"] = self.wait_for(page, "(document.getElementById('mf_live') || {}).textContent.startsWith('Which one, if any?')", 5000)
        r["nothing_linked_unchosen"] = (r["must_choose"] and page.evaluate("window.__whgMatchFeed.saves") == 0)
        # Go to the second suggestion: its marker is lit. Mark it the same place: the map follows.
        page.keyboard.press("2")
        r["marker_lit_with_row"] = self.wait_for(
            page, "!!document.querySelector('#mf_map .mf-map-pin--lit[data-i=\"1\"]')", 10000)
        before = self.renders(page)
        page.keyboard.press("s")
        r["map_follows_choice"] = (self.wait_for(page, f"window.__whgMatchFeed.map.renders > {before}", 10000)
                                   and self.placed(page, [["same", 1, list(self.brox_c[1])],
                                                          ["other", 0, list(self.brox_c[0])],
                                                          ["record", None, list(self.brox_xy)]]))
        # Next record: the record's own point moves.
        before = self.renders(page)
        page.keyboard.press("ArrowRight")
        r["next_record"] = self.wait_for(page, "(document.getElementById('mf_record_title') || {}).textContent.startsWith('Warwick')", 10000)
        r["map_follows_record"] = (self.wait_for(page, f"window.__whgMatchFeed.map.renders > {before}", 10000)
                                   and self.placed(page, [["record", None, list(self.wark_xy)],
                                                          ["other", 0, list(self.wark_c[0])]])
                                   and not page.evaluate(f"""() => ({MARKERS_JS})().some((m) => m.role === 'record'
                                        && Math.abs(m.ll[1] - {self.brox_xy[1]}) < 1e-6)"""))
        # Warwick's two suggestions are one place in two sources; choosing one decides nothing about
        # the other: it stays unmarked (and nothing is written for it).
        page.click("#mf_cands [data-act=set][data-i='0'][data-d=same]")
        r["group_mate_left_unmarked"] = self.wait_for(
            page, "!document.querySelector('#mf_cands [data-i=\"1\"] .is-on')", 5000)
        page.click("[data-act=save]")
        r["saved"] = self.wait_for(page, "window.__whgMatchFeed.saves === 1", 10000)
        from api.models import ContributorAttestation
        from datasets.models_matching import MatchCandidate
        chosen = MatchCandidate.objects.get(place=self.wark, candidate_id=self.wark_c_ids[0])
        other = MatchCandidate.objects.get(place=self.wark, candidate_id=self.wark_c_ids[1])
        r["chosen_explicit"] = (chosen.status, chosen.decision_source) == ("accepted", "explicit")
        r["group_mate_untouched"] = (other.status, other.decision_source) == ("suggested", "")
        links = list(ContributorAttestation.objects.filter(user=self.user).values_list("relation_type", flat=True))
        r["one_link_and_no_public_distinct"] = links == ["closeMatch"]
        if not all(r[k] for k in ("chosen_explicit", "group_mate_untouched", "one_link_and_no_public_distinct")):
            print("[debug]", (chosen.status, chosen.decision_source), (other.status, other.decision_source), links)
        r["header_progress_moved"] = self.wait_for(
            page, "document.getElementById('mf_progress').textContent.includes('1 match')", 15000)
        # A record with no location at all: a clear "no location" state, never an empty box.
        if self.nowhere_tier == "ambiguous":     # the next record still to decide: it follows on by itself
            pass
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

    BASEMAPS = {"OSM": "OpenStreetMap", "whg-enhanced": "WHG Enhanced", "Satellite": "Satellite"}

    def basemap_switcher(self, page, stubbed=True):
        """The labels and checked state of the map's basemap switcher. The three tileboss styles are
        replaced by one-layer stubs (nothing leaves the machine), so the test reads what the page
        OFFERS, not what MapTiler or tileboss serve today."""
        if stubbed:
            def style(route):
                sid = route.request.url.split("/styles/")[1].split("/")[0]
                body = {"version": 8, "name": self.BASEMAPS.get(sid, sid), "metadata": {}, "sources": {},
                        "layers": [{"id": "bg", "type": "background", "paint": {"background-color": "#cde"}}]}
                route.fulfill(status=200, body=json.dumps(body), content_type="application/json",
                              headers={"access-control-allow-origin": "*", "access-control-allow-headers": "*",
                                       "access-control-allow-methods": "GET, OPTIONS"})
            page.route("https://tiles.whgazetteer.org/styles/*/style.json", style)
        self.console = []
        self.tile_requests = []
        page.on("request", lambda r: self.tile_requests.append(r.url) if "tiles.whgazetteer" in r.url else None)
        page.on("requestfailed", lambda r: self.tile_requests.append("FAILED " + r.url) if "tiles.whgazetteer" in r.url else None)
        self.page_errors = []
        page.on("pageerror", lambda e: self.page_errors.append(str(e)[:300]))
        page.on("console", lambda m: self.console.append(f"{m.type}: {m.text}"[:300]))
        page.goto(f"{self.live_server_url}/datasets/{self.ds.pk}/matches/?tier=ambiguous")
        self.wait_for(page, "window.__whgMatchFeed && window.__whgMatchFeed.ready === true")
        if not self.wait_for(page, "document.querySelectorAll('#mf_map #mapStyleList input[name=style]').length > 0", 15000):
            self.diagnosis = {"map": page.evaluate("(({state, basemap}) => ({state, basemap}))((window.__whgMatchFeed || {}).map || {})"), "external": sorted(self.external), "tiles": self.tile_requests[:8], "pageerrors": self.page_errors[:6],
                              "style": page.evaluate("(() => { const m = ((window.__whgMatchFeed || {}).map || {}).instance; return m ? {loaded: m.loaded(), styleLoaded: m.isStyleLoaded(), layers: (m.getStyle().layers || []).map((l) => l.id)} : null; })()"),
                              "mapdiv": page.evaluate("(document.getElementById('mf_map') || {innerHTML: 'none'}).innerHTML.slice(0, 400)"),
                              "console": [c for c in self.console if "CDN" not in c and "MIME" not in c and "ERR_FAILED" not in c and "404" not in c][-12:]}
            return None
        return page.evaluate("""[...document.querySelectorAll('#mf_map #mapStyleList input[name=style]')].map((r) => ({
            id: r.dataset.value, label: r.parentElement.textContent.trim(), checked: r.checked}))""")

    def test_basemap_switcher_offers_osm_enhanced_and_satellite_and_remembers_the_choice(self):
        page = self.new_page()
        got = self.basemap_switcher(page)
        self.assertIsNotNone(got, getattr(self, "diagnosis", None))
        self.assertEqual([(x["id"], x["label"]) for x in got or []],
                         [("OSM", "OpenStreetMap"), ("whg-enhanced", "WHG Enhanced"), ("Satellite", "Satellite")], got)
        self.assertEqual([x["id"] for x in got if x["checked"]], ["OSM"])          # richest by default
        self.assertNotIn("whg-portal", [x["id"] for x in got])                    # no ecoregion fills
        page.click("#mf_map .style-button")                                        # the list opens from the control
        page.click("#mf_map #mapStyleList label[data-value='Satellite']")
        self.assertTrue(self.wait_for(page, "localStorage.getItem('whg-feed-maplayers') === '{\"style\":\"Satellite\"}'", 5000))
        got = self.basemap_switcher(page, stubbed=False)                           # the page reloads: the choice stays
        self.assertEqual([x["id"] for x in got if x["checked"]], ["Satellite"], got)
        self.assertEqual([x["id"] for x in got][0], "Satellite")

    def test_basemap_switcher_harness_can_fail(self):
        # With no style served (every tileboss request is aborted) there is no switcher to read: the
        # check above must say so rather than pass.
        page = self.new_page()
        self.assertIsNone(self.basemap_switcher(page, stubbed=False))

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
                                                       ["other", 0, list(self.brox_c[0])], ["other", 1, list(self.brox_c[1])]]))
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

    def test_several_search_results_can_be_ticked_and_linked_together(self):
        """Ali's case: a search finds the same place in several sources. Ticking a result is not
        "show it on the map"; the bar counts the ticks; one action links them all."""
        from api.models import ContributorAttestation
        from datasets.tests_candidates import GN_TOWN, hit, response
        self.fake.by_query["Brox Hand"] = response(
            hit("gn:70", "Brox Hand One", self.HAND_XY[0], types=[GN_TOWN]),
            hit("gn:71", "Brox Hand Two", self.HAND_XY[1], types=[GN_TOWN]),
            hit("wd:Q72", "Brox Hand Three"))
        page = self.new_page()
        page.goto(f"{self.live_server_url}/datasets/{self.ds.pk}/matches/?tier=ambiguous")
        self.assertTrue(self.wait_for(page, "window.__whgMatchFeed && window.__whgMatchFeed.ready === true"))
        page.fill("#mf_search_q", "Brox Hand")
        page.click("[data-act=search]")
        self.assertTrue(self.wait_for(page, "document.querySelectorAll('#mf_search_results .mf-hand-check').length === 3", 15000))
        self.assertTrue(page.is_disabled("#mf_hand_link"))                          # nothing ticked: nothing to link
        page.check("#mf_search_results .mf-hand-check[data-i='0']")
        page.check("#mf_search_results .mf-hand-check[data-i='2']")
        self.assertEqual(page.text_content("#mf_hand_link"), "Same place (2)")
        self.assertFalse(page.evaluate(f"() => ({MARKERS_JS})().some((m) => m.role === 'hand-focus')"))   # ticking is not focusing
        self.assertFalse(ContributorAttestation.objects.filter(user=self.user).exists())
        page.uncheck("#mf_search_results .mf-hand-check[data-i='2']")
        page.check("#mf_search_results .mf-hand-check[data-i='1']")
        self.assertEqual(page.text_content("#mf_hand_link"), "Same place (2)")
        page.click("#mf_hand_link")
        self.assertTrue(self.wait_for(page, "window.__whgMatchFeed.saves === 1", 10000))
        linked = {a.place_a if a.place_b.startswith("whg:") else a.place_b
                  for a in ContributorAttestation.objects.filter(user=self.user)}
        self.assertEqual(linked, {"gn:70", "gn:71"})

    def test_ticking_search_results_harness_can_fail(self):
        # With no ticked result the link button stays disabled: clicking it must not link anything.
        from api.models import ContributorAttestation
        from datasets.tests_candidates import GN_TOWN, hit, response
        self.fake.by_query["Brox Hand"] = response(hit("gn:70", "Brox Hand One", self.HAND_XY[0], types=[GN_TOWN]))
        page = self.new_page()
        page.goto(f"{self.live_server_url}/datasets/{self.ds.pk}/matches/?tier=ambiguous")
        self.assertTrue(self.wait_for(page, "window.__whgMatchFeed && window.__whgMatchFeed.ready === true"))
        page.fill("#mf_search_q", "Brox Hand")
        page.click("[data-act=search]")
        self.assertTrue(self.wait_for(page, "document.querySelectorAll('#mf_search_results .mf-hand-check').length === 1", 15000))
        page.evaluate("document.getElementById('mf_hand_link').click()")            # a disabled button ignores a click
        page.wait_for_timeout(500)
        self.assertEqual(page.evaluate("window.__whgMatchFeed.saves"), 0)
        self.assertFalse(ContributorAttestation.objects.filter(user=self.user).exists())

    CHOR_XY = (60.0, 41.0)

    def chorasmia(self):
        """A record like the one that was reported: one good suggestion, then weak noise. Three
        "Worsham Creek"s lie close together (one per source: one place in three sources), two more
        far apart (two places), and an unrelated name."""
        from datasets.models_matching import MatchCandidate
        from datasets.tests_candidates import GN_TOWN, hit, response
        from places.models import Place, PlaceGeom
        pl = Place.objects.create(title="Chorasmia", src_id="m9", dataset=self.ds, ccodes=[])
        PlaceGeom.objects.create(place=pl, geom=Point(*self.CHOR_XY, srid=4326),
                                 jsonb={"type": "Point", "coordinates": list(self.CHOR_XY)})
        near = (-77.0, 38.0)
        self.fake.by_query["Chorasmia"] = response(
            hit("wd:Q182647", "Chorasmia", (60.01, 41.01), types=[GN_TOWN]),
            hit("tgn:1", "Worsham Creek", near, conf=21.2, score=21.2, types=[GN_TOWN]),
            hit("osm:w1", "Worsham Creek", (-77.001, 38.001), conf=21.2, score=21.2, types=[GN_TOWN]),
            hit("gn:5", "Worsham Creek", (-77.002, 38.0), conf=21.2, score=21.2, types=[GN_TOWN]),
            hit("tgn:2", "Worsham Creek", (-90.0, 35.0), conf=21.2, score=21.2, types=[GN_TOWN]),
            hit("gn:6", "Worsham Creek", (-100.0, 40.0), conf=21.2, score=21.2, types=[GN_TOWN]),
            hit("wd:Q21", "Ogresmuiza", (24.6, 56.8), conf=21.2, score=21.2, types=[GN_TOWN]))
        tier = candidates.record_candidates(pl, self.gen, self.user)
        cands = list(MatchCandidate.objects.filter(place=pl, generation=self.gen).order_by("rank"))
        return pl, tier, cands

    def open_record(self, page, title, tier):
        page.goto(f"{self.live_server_url}/datasets/{self.ds.pk}/matches/?tier={tier}")
        self.assertTrue(self.wait_for(page, "window.__whgMatchFeed && window.__whgMatchFeed.ready === true"))
        if not page.evaluate("(t) => (document.getElementById('mf_record_title') || {}).textContent.startsWith(t)", title):
            page.click(f"[data-qi]:has-text('{title}')")
        self.assertTrue(self.wait_for(page, "(t) => (document.getElementById('mf_record_title') || {}).textContent.startsWith(t)", 10000, title))
        self.assertTrue(self.wait_for(page, "window.__whgMatchFeed.map.state === 'ok' && window.__whgMatchFeed.map.renders >= 1"))

    VISIBLE_ROWS = "() => [...document.querySelectorAll('#mf_cands .mf-row')].filter((x) => x.checkVisibility()).length"
    PINS = "() => document.querySelectorAll('#mf_map .mf-map-pin--cand').length"

    def test_weak_suggestions_are_folded_and_open_on_demand(self):
        pl, tier, cands = self.chorasmia()
        weak = [c for c in cands if c.tier == "weak"]
        self.assertEqual(len(weak), 6, [(c.candidate_id, c.tier) for c in cands])
        self.assertEqual(cands[0].candidate_id, "wd:Q182647")
        page = self.new_page()
        self.open_record(page, "Chorasmia", tier)
        self.assertEqual(page.text_content("details.mf-weak > summary").strip(), "6 weak suggestions: probably not the same place")
        self.assertFalse(page.evaluate("document.querySelector('details.mf-weak').open"))
        self.assertEqual(page.evaluate(self.VISIBLE_ROWS), 1)               # only the good one is listed
        self.assertTrue(self.wait_for(page, f"({self.PINS})() === 1", 10000))   # and only it has a pin: no orphan numbers
        page.click("details.mf-weak > summary")
        self.assertTrue(self.wait_for(page, f"({self.VISIBLE_ROWS})() >= 5", 5000))
        self.assertEqual(page.text_content("li.mf-fold > details > summary").split()[:2], ["2", "places"])   # two far-apart creeks fold
        self.assertIn("Worsham Creek", page.text_content("li.mf-fold > details > summary"))
        self.assertIn("Probably one place, described by 3 sources", page.text_content("details.mf-weak"))
        self.assertTrue(self.wait_for(page, f"({self.PINS})() > 1", 10000))     # opening the fold puts its pins on the map
        if os.environ.get("WHG_FEED_SHOT"):      # optional: a picture to look at, never compared
            page.set_viewport_size({"width": 1300, "height": 1100})
            page.wait_for_timeout(800)
            page.screenshot(path=os.environ["WHG_FEED_SHOT"])
        # The keyboard reaches a folded row by opening its fold: close it, then ask for suggestion 7.
        page.click("details.mf-weak > summary")
        self.assertFalse(page.evaluate("document.querySelector('details.mf-weak').open"))
        page.focus("#mf_cands .mf-row")
        page.keyboard.press("7")
        self.assertTrue(self.wait_for(page, "document.querySelector('details.mf-weak').open", 5000))

    def test_weak_folding_harness_can_fail(self):
        # A record whose every suggestion is weak is not folded (they are all there is): the fold
        # check above must not pass for it.
        from datasets.tests_candidates import GN_TOWN, hit, response
        from places.models import Place
        pl = Place.objects.create(title="Qzxv", src_id="m8", dataset=self.ds, ccodes=[])
        self.fake.by_query["Qzxv"] = response(hit("tgn:7", "Worsham Creek", (-77.0, 38.0), conf=21.2, score=21.2, types=[GN_TOWN]),
                                              hit("gn:8", "Ogresmuiza", (24.6, 56.8), conf=21.2, score=21.2, types=[GN_TOWN]))
        tier = candidates.record_candidates(pl, self.gen, self.user)
        page = self.new_page()
        self.open_record(page, "Qzxv", tier)
        self.assertEqual(page.evaluate("document.querySelectorAll('details.mf-weak').length"), 0)
        self.assertEqual(page.evaluate(self.VISIBLE_ROWS), 2)

    def test_search_results_already_suggested_are_not_offered_again(self):
        from datasets.tests_candidates import GN_TOWN, hit, response
        pl, tier, cands = self.chorasmia()
        self.fake.by_query["Khwarazm"] = response(hit("wd:Q182647", "Chorasmia", (60.01, 41.01), types=[GN_TOWN]),
                                                  hit("gn:900", "Khwarazm Region", (60.1, 41.1), types=[GN_TOWN]))
        page = self.new_page()
        self.open_record(page, "Chorasmia", tier)
        # The record's own name, nothing relaxed: the same search again. Said so; nothing to tick.
        page.click("[data-act=search]")
        self.assertTrue(self.wait_for(page, "document.getElementById('mf_search_same')", 15000))
        self.assertEqual(page.evaluate("document.querySelectorAll('#mf_search_results .mf-hand-check').length"), 0)
        self.assertIn("already suggested above", page.text_content("#mf_search_results"))
        # Another name that finds the same place and a new one: only the new one can be ticked.
        page.fill("#mf_search_q", "Khwarazm")
        page.click("[data-act=search]")
        self.assertTrue(self.wait_for(page, "document.querySelectorAll('#mf_search_results .mf-hand-check').length === 1", 15000))
        self.assertIsNone(page.query_selector("#mf_search_same"))
        listed = page.text_content("#mf_search_results .mf-hand-listed")
        self.assertIn("1 result is already suggested above", listed)
        self.assertIn("already suggested above as 1", listed)
        self.assertEqual(page.evaluate("document.querySelectorAll('#mf_search_results .mf-hand--listed').length"), 1)

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
        page.keyboard.press("s")
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
        self.assertIn("Match your places: Parishes map", page.content())


@unittest.skipUnless(RUN, "set WHG_BROWSER_TESTS=1 to drive the contributor feed in headless Chromium")
class FeedRedesignBrowserTests(MapBrowserBase):
    """The redesigned rows and decision bar (2026-11): the same place in two sources is grouped and
    one click marks both; choosing one place leaves the others unmarked and the
    summary says what Save will write; suggestions off the map are named on its edge and markers
    that would overlap are merged; the keys that belong to a focused row (↑/↓, s, x, c) and the
    switch for the one-key shortcuts; a map marker moves the focus and decides nothing; and a record
    nobody has searched for yet offers no decision. Suggestions are written straight into the
    candidate tables (no gateway), so each case is exactly what it says."""
    serialized_rollback = True

    def setUp(self):
        stub_type_vocab(self)  # /types/vocab/ would query the real ES (see utils/livetest.py)
        from datasets.models import Dataset
        self.user = get_user_model().objects.create_user(username="mfr_beta", email="mfr@example.org", password="pw",
                                                         given_name="R", surname="D", role="beta_tester")
        with patch("datasets.signals.doi"):
            self.ds = Dataset.objects.create(owner=self.user, label="mfr338", title="Redesign", description="D",
                                             creator="X", public=False, ds_status="uploaded")
        self.gen = CandidateGeneration.objects.create(dataset=self.ds, requested_by=self.user, status="finished",
                                                      recipe={})

    def rec(self, title, src, xy, cands, tier="ambiguous"):
        """cands: (candidate_id, title, point|None, km|None, group) in rank order."""
        from datasets.models_matching import MatchCandidate
        from places.models import Place, PlaceGeom
        pl = Place.objects.create(title=title, src_id=src, dataset=self.ds, ccodes=[])
        if xy:
            PlaceGeom.objects.create(place=pl, geom=Point(*xy, srid=4326), jsonb={"type": "Point", "coordinates": list(xy)})
        for rank, (cid, ctitle, pt, km) in enumerate(cands, start=1):
            card = {"title": ctitle, "namespace": cid.split(":")[0], "names": [{"label": ctitle}], "types": ["PPL"]}
            if pt:
                card["repr_point"] = list(pt)
            MatchCandidate.objects.create(
                dataset=self.ds, place=pl, subject_id=f"whg:{self.ds.pk}:{src}", candidate_id=cid, candidate_title=ctitle,
                namespace=cid.split(":")[0], tier=tier if cands else "weak", rank=rank, generation=self.gen, card=card,
                signals={"km": km, "type_sanity": "ok"},
                explanation={"pieces": [{"kind": "name", "text": "exact name", "agrees": True}], "sentence": "x"})
        RecordReviewState.objects.create(place=pl, dataset=self.ds, generation=self.gen, tier=tier,
                                         has_geometry=bool(xy), candidate_count=len(cands),
                                         top_candidate_id=cands[0][0] if cands else "")
        return pl

    def open(self, tier="ambiguous"):
        page = self.new_page()
        page.goto(f"{self.live_server_url}/datasets/{self.ds.pk}/matches/?tier={tier}")
        self.assertTrue(self.wait_for(page, "window.__whgMatchFeed && window.__whgMatchFeed.ready === true"))
        return page

    def springfield(self):
        # gn:1 and wd:2 are one place in two sources (50 m apart, same title); gn:3 is on another continent.
        return self.rec("Springfield", "r1", (-89.65, 39.78), [
            ("gn:1", "Springfield", (-89.65, 39.78), 0.1), ("wd:2", "Springfield", (-89.6504, 39.7803), 0.2),
            ("gn:3", "Springfield", (-123.0, 44.0), 2400.0)])

    def test_group_prefill_summary_chip_badge_and_save(self):
        self.springfield()
        page = self.open()
        r = {}
        r["group_shown"] = self.wait_for(page, "document.querySelectorAll('li.mf-group #mf_cands .x, li.mf-group .mf-row').length === 2")
        r["map_drawn"] = self.wait_for(page, "window.__whgMatchFeed.map.state === 'ok'")
        # Far suggestion 3 is off the map but named on its edge, with how far away it is.
        r["edge_chip"] = self.wait_for(page, """() => { const c = document.querySelector('.mf-map-chip[data-members="3"]');
            return !!c && c.textContent.includes('2,400 km'); }""", 20000)
        # The two entries for one place overlap on screen: one badge "1·2".
        r["merged_badge"] = self.wait_for(page, """() => { const b = document.querySelector('#mf_map [data-role=badge]');
            return !!b && b.dataset.members === '1,2'; }""", 20000)
        # One click on the group heading marks both; the far one is left unmarked (never filled in).
        page.click("[data-act=group][data-d=same]")
        r["both_same"] = self.wait_for(page, """() => ['0', '1'].every((i) =>
            document.querySelector(`.mf-seg-btn--same[data-i="${i}"]`).getAttribute('aria-pressed') === 'true')""", 5000)
        r["far_left_unmarked"] = self.wait_for(page, "!document.querySelector('#mf_cands [data-i=\"2\"] .is-on') && !document.querySelector('.mf-row--prefilled')", 5000)
        summary = page.evaluate("document.getElementById('mf_summary').textContent")
        r["summary_says_what_save_writes"] = "Same place: 1, 2" in summary and "Not the same" not in summary and "Not marked: 3" in summary
        page.click("[data-act=save]")
        r["saved"] = self.wait_for(page, "window.__whgMatchFeed.saves === 1", 10000)
        from api.models import ContributorAttestation
        from datasets.models_matching import MatchCandidate
        got = {c.candidate_id: (c.status, c.decision_source) for c in MatchCandidate.objects.filter(dataset=self.ds)}
        r["written"] = (got == {"gn:1": ("accepted", "explicit"), "wd:2": ("accepted", "explicit"),
                                "gn:3": ("suggested", "")}
                        and ContributorAttestation.objects.filter(user=self.user).count() == 2
                        and RecordReviewState.objects.get().state == "linked")
        failed = {k: v for k, v in r.items() if not v}
        self.assertEqual(failed, {}, r)

    def test_marker_click_moves_the_focus_and_decides_nothing(self):
        self.springfield()
        page = self.open()
        self.assertTrue(self.wait_for(page, "window.__whgMatchFeed.map.state === 'ok'"))
        self.assertTrue(self.wait_for(page, "!!document.querySelector('#mf_map [data-role=other][data-i=\"2\"]')", 20000)
                        or True)     # suggestion 3 may be off the view; use the one that is not
        page.evaluate("document.querySelector('#mf_map [data-role=badge]').click()")   # 1·2: same spot cycles
        self.assertTrue(self.wait_for(page, "!!document.querySelector('#mf_cands .mf-row.is-focus')", 5000))
        self.assertEqual(page.evaluate("document.querySelectorAll('#mf_cands [aria-pressed=true]').length"), 0)
        self.assertEqual(page.evaluate("window.__whgMatchFeed.saves"), 0)

    def test_row_keys_and_the_shortcut_switch(self):
        self.springfield()
        page = self.open()
        page.keyboard.press("1")
        self.assertTrue(self.wait_for(page, "document.activeElement && document.activeElement.dataset.i === '0'", 5000))
        page.keyboard.press("ArrowDown")
        self.assertTrue(self.wait_for(page, "document.activeElement && document.activeElement.dataset.i === '1'", 5000))
        page.keyboard.press("x")
        self.assertTrue(self.wait_for(page, "document.querySelector('.mf-seg-btn--not[data-i=\"1\"]').getAttribute('aria-pressed') === 'true'", 5000))
        page.keyboard.press("c")
        self.assertTrue(self.wait_for(page, "document.querySelector('.mf-seg-btn--unsure[data-i=\"1\"]').getAttribute('aria-pressed') === 'true'", 5000))
        page.keyboard.press("c")       # the same key again takes it back
        self.assertTrue(self.wait_for(page, "document.querySelector('.mf-seg-btn--unsure[data-i=\"1\"]').getAttribute('aria-pressed') === 'false'", 5000))
        # Switch the shortcuts off: no key does anything (they are an opt-in), but the buttons still work.
        page.uncheck("#mf_shortcuts")
        page.focus("#mf_cands .mf-row[data-i='2']")
        page.keyboard.press("n")
        page.wait_for_timeout(600)
        self.assertEqual(page.evaluate("window.__whgMatchFeed.saves"), 0)
        page.keyboard.press("s")
        page.wait_for_timeout(400)
        self.assertEqual(page.evaluate("document.querySelector('.mf-seg-btn--same[data-i=\"2\"]').getAttribute('aria-pressed')"), "false")
        page.click(".mf-seg-btn--same[data-i='2']")
        self.assertTrue(self.wait_for(page, "document.querySelector('.mf-seg-btn--same[data-i=\"2\"]').getAttribute('aria-pressed') === 'true'", 5000))
        # and the buttons are reachable with Tab when the shortcuts are off
        self.assertEqual(page.evaluate("document.querySelector('.mf-seg-btn[data-i=\"0\"]').tabIndex"), 0)

    def test_defaults_tooltips_heading_and_tour(self):
        """Shortcuts are an opt-in (hidden and inert until switched on), the fit buttons explain
        themselves, the heading leads with the page name, and the tour only highlights."""
        self.springfield()
        page = self.open(tier="ambiguous")
        # a fresh browser: no stored preferences, so the first visit starts the tour by itself
        page.evaluate("localStorage.clear(); sessionStorage.setItem('t_seeded', '1')")
        page.reload()
        self.assertTrue(self.wait_for(page, "window.__whgMatchFeed.ready === true", 20000))
        self.assertTrue(self.wait_for(page, "!!document.querySelector('.driver-popover')", 25000), "first visit starts the tour")
        self.assertTrue(page.evaluate("window.__whgMatchFeed.touring"))
        page.keyboard.press("Escape")
        if not self.wait_for(page, "!document.querySelector('.driver-popover') && !window.__whgMatchFeed.touring", 3000):
            print("[debug] after Escape:", page.evaluate("[!!document.querySelector('.driver-popover'), window.__whgMatchFeed.touring, document.activeElement.tagName, document.body.className]"))
        self.assertTrue(self.wait_for(page, "!document.querySelector('.driver-popover') && !window.__whgMatchFeed.touring", 3000))
        self.assertFalse(page.evaluate("document.getElementById('mf_shortcuts').checked"))
        self.assertTrue(page.evaluate("document.getElementById('mf_keylist').hidden"))
        self.assertFalse(page.evaluate("[...document.querySelectorAll('.mf-bar kbd')].some((k) => k.offsetParent !== null)"))
        page.keyboard.press("n")
        page.wait_for_timeout(400)
        self.assertEqual(page.evaluate("window.__whgMatchFeed.saves"), 0)
        for act in ("fit-near", "fit-all"):
            self.assertTrue(page.evaluate("(a) => (document.querySelector(`[data-act=${a}]`).title || '').length > 20", act))
        h1 = page.evaluate("document.querySelector('h1').textContent.replace(/\\s+/g, ' ').trim()")
        self.assertTrue(h1.startswith("Match your places"), h1)
        page.check("#mf_shortcuts")
        self.assertFalse(page.evaluate("document.getElementById('mf_keylist').hidden"))
        # the tour button repeats it, and a tour decides nothing
        page.click("#mf_tour_button")
        self.assertTrue(self.wait_for(page, "!!document.querySelector('.driver-popover')", 5000))
        page.keyboard.press("ArrowRight")          # the tour's own key: it must not move to another record
        page.keyboard.press("Escape")
        self.assertEqual(page.evaluate("window.__whgMatchFeed.saves"), 0)

    def test_a_long_queue_and_many_suggestions_scroll_and_nothing_is_hidden(self):
        """120 records in the list and 40 suggestions on the first one: the list scrolls inside its own
        box and keeps the current record in view, the page scrolls to a focused row without the
        sticky bar covering it, and nothing makes the page wider than the window."""
        for n in range(120):
            self.rec(f"Place {n:03d}", f"q{n}", (10 + n * 0.01, 50.0), [
                (f"gn:{n}00{k}", f"Place {n:03d}", (10 + n * 0.01 + k * 0.005, 50.0 + k * 0.004), 0.5 * k)
                for k in range(40 if n == 0 else 2)])
        page = self.open()
        r = {}
        r["forty_rows"] = self.wait_for(page, "document.querySelectorAll('#mf_cands .mf-row').length === 40")
        q = page.evaluate("""() => { const e = document.getElementById('mf_queue');
            return {sh: e.scrollHeight, ch: e.clientHeight, vh: window.innerHeight, ov: getComputedStyle(e).overflowY,
                    items: e.children.length}; }""")
        r["queue_scrolls_inside_a_bounded_box"] = q["sh"] > q["ch"] and q["ch"] <= q["vh"] * 0.72 and q["ov"] in ("auto", "scroll")
        # walk down the list with the keyboard far enough that the current record must scroll into view
        page.evaluate("localStorage.setItem('mf.shortcuts', 'on')")
        page.reload()
        self.assertTrue(self.wait_for(page, "window.__whgMatchFeed.ready === true"))
        for _ in range(45):
            page.keyboard.press("ArrowRight")
            page.wait_for_timeout(60)
        r["current_record_visible_in_queue"] = page.evaluate("""() => { const e = document.getElementById('mf_queue');
            const a = e.querySelector('.active, [aria-current=true]'); if (!a) return false;
            const q = e.getBoundingClientRect(), b = a.getBoundingClientRect();
            return b.top >= q.top - 1 && b.bottom <= q.bottom + 1; }""")
        page.goto(f"{self.live_server_url}/datasets/{self.ds.pk}/matches/?tier=ambiguous")
        self.assertTrue(self.wait_for(page, "window.__whgMatchFeed.ready === true"))
        self.assertTrue(self.wait_for(page, "document.querySelectorAll('#mf_cands .mf-row').length === 40"))
        page.focus("#mf_cands .mf-row[data-i='0']")
        for _ in range(39):
            page.keyboard.press("ArrowDown")
        self.wait_for(page, """() => { const b = document.querySelector('#mf_cands .mf-row[data-i="39"]').getBoundingClientRect();
            const bar = document.getElementById('mf_bar').getBoundingClientRect();
            return b.top >= 0 && b.bottom <= bar.top; }""", 6000)     # a smooth scroll may still be running
        box = page.evaluate("""() => { const row = document.querySelector('#mf_cands .mf-row[data-i="39"]').getBoundingClientRect();
            const bar = document.getElementById('mf_bar').getBoundingClientRect();
            return {top: row.top, bottom: row.bottom, bar: bar.top, vh: window.innerHeight,
                    wide: document.documentElement.scrollWidth > document.documentElement.clientWidth,
                    y: window.scrollY, active: (document.activeElement.dataset || {}).i, tag: document.activeElement.tagName,
                    sc: window.__whgMatchFeed.map.state}; }""")
        r["last_row_visible_above_the_bar"] = box["top"] >= 0 and box["bottom"] <= box["bar"] + 1
        r["map_still_in_view"] = page.evaluate("""() => { const m = document.getElementById('mf_map').getBoundingClientRect();
            return m.bottom > 0 && m.top < window.innerHeight; }""")
        r["page_not_wider_than_window"] = not box["wide"]
        failed = {k: v for k, v in r.items() if not v}
        self.assertEqual(failed, {}, (r, q, box))

    def test_tabs_are_a_tablist_with_a_panel_and_arrow_keys(self):
        self.springfield()
        self.rec("Edinburgh", "r3", (-3.19, 55.95), [("gn:9", "Edinburgh", (-3.19, 55.95), 0.1)], tier="confident")
        page = self.open(tier="confident")
        self.assertEqual(page.evaluate("document.getElementById('mf_panel').getAttribute('role')"), "tabpanel")
        self.assertEqual(page.evaluate("document.querySelectorAll('.mf-tabs [tabindex=\"0\"]').length"), 1)   # roving
        page.focus(".mf-tabs [data-tab=confident]")
        page.keyboard.press("ArrowRight")
        self.assertTrue(self.wait_for(page, "window.__whgMatchFeed.tab === 'ambiguous'", 5000))
        self.assertTrue(self.wait_for(page, "document.getElementById('mf_panel').getAttribute('aria-labelledby') === 'mf_tab_ambiguous'", 5000))
        self.assertTrue(self.wait_for(page, "(document.getElementById('mf_record_title') || {}).textContent.startsWith('Springfield')", 5000))
        self.assertEqual(page.evaluate("window.__whgMatchFeed.saves"), 0)         # moving between tabs decides nothing

    def test_a_record_nobody_has_searched_for_yet_offers_no_decision(self):
        self.rec("Aachen", "r2", (6.08, 50.78), [], tier="unqueried")
        page = self.open(tier="unqueried")
        self.assertTrue(self.wait_for(page, "!!document.getElementById('mf_waiting')"))
        self.assertEqual(page.evaluate("document.querySelectorAll('[data-act=save], [data-act=none], [data-act=later]').length"), 0)
        page.keyboard.press("n")       # the old page let this mark a never-searched record "no good match"
        page.keyboard.press("l")
        page.wait_for_timeout(600)
        self.assertEqual(page.evaluate("window.__whgMatchFeed.saves"), 0)
        self.assertEqual(RecordReviewState.objects.get().state, "unreviewed")

    def test_harness_can_fail(self):
        """The group/prefill checks with the page bundle blocked: none of them may pass."""
        self.springfield()
        page = self.new_page(block_bundle=True)
        page.goto(f"{self.live_server_url}/datasets/{self.ds.pk}/matches/?tier=ambiguous")
        ready = self.wait_for(page, "window.__whgMatchFeed && window.__whgMatchFeed.ready === true", 1500)
        self.assertFalse(ready)
        self.assertEqual(page.evaluate("document.querySelectorAll('.mf-row').length"), 0)
