"""``/api/spatial/`` counts its matches no further than ``LEGACY_DB_COUNT_CAP`` (2026-11-18), as
``/api/db/`` does (api/tests_legacy_db_count.py). Page and count come from ONE ``SELECT places.id
... LIMIT cap+1`` (``api.views.bounded_search``) instead of a page query plus an exact
``qs.count()``.

Why: for ``match=intersects`` the exact count re-ran the whole spatial match. A nearby circle is
``ST_DistanceSphere(geom, pt) <= m``, which no index serves, so the count was a second sequential
scan of ``place_geom`` on every request. Measured on a synthetic 2,005,000-row ``place_geom`` in the
throwaway local test DB (points only; prod's polygons cost more per row): the count was ~0.9 s of a
~3.3 s request.

Pins: exact ``count`` below the cap (``count_capped`` false); above it ``count`` is the cap and
``count_capped`` true; exactly the cap is not capped; the page is still filled; no ``COUNT(*)`` and
ONE distance scan per request; the visibility rule (place#310) and ``dataset`` still apply to the
count; the response keys are the old ones plus ``count_capped``.

    ENV_CONTEXT=local venv/bin/python manage.py test api.tests_spatial_count --settings=whg.settings_localtest
"""

from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.gis.geos import Point
from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext

URL = "/api/spatial/"
LON, LAT = -1.08, 53.96
NEARBY = {"type": "nearby", "lon": LON, "lat": LAT, "km": 10}
OLD_KEYS = {"deprecation", "type", "features", "parameters", "errors", "count", "pagesize", "pagesize_max",
            "geometry_mode", "match", "attribution"}


class _Fixture(TestCase):
    """Seven public places within 10 km of York, one private one there too, one public one far off."""

    @classmethod
    def setUpTestData(cls):
        from datasets.models import Dataset
        from places.models import Place, PlaceGeom
        User = get_user_model()
        cls.owner = User.objects.create_user(username="sc_owner", email="sc_owner@example.org", password="pw",
                                             given_name="SC", surname="Owner")
        with patch("datasets.signals.doi"):
            cls.ds = Dataset.objects.create(owner=cls.owner, label="sc_ds", title="Count", description="D",
                                            creator="C", public=True, ds_status="indexed",
                                            uri_base="https://example.org/")
            cls.private = Dataset.objects.create(owner=cls.owner, label="sc_private", title="P", description="D",
                                                 creator="C", public=False, ds_status="uploaded",
                                                 uri_base="https://example.org/")

        def place(title, ds, lon, lat):
            p = Place.objects.create(title=title, src_id=title, dataset=ds, ccodes=["GB"], fclasses=["P"])
            PlaceGeom.objects.create(place=p, geom=Point(lon, lat, srid=4326),
                                     jsonb={"type": "Point", "coordinates": [lon, lat]})
            return p

        for i in range(7):
            place(f"Near{i}", cls.ds, LON + 0.01 * i, LAT)
        place("Hidden", cls.private, LON, LAT + 0.01)
        place("Faraway", cls.ds, 10.0, 10.0)

    def setUp(self):
        from django.core.cache import caches
        for alias in ("default", "throttle"):
            try:
                caches[alias].clear()
            except Exception:
                pass

    def spatial(self, **params):
        with CaptureQueriesContext(connection) as ctx:
            r = self.client.get(URL, {**NEARBY, **params})
        self.assertEqual(r.status_code, 200, r.content[:300])
        sql = [q["sql"] for q in ctx.captured_queries]
        return r.json(), sql


class CappedSpatialCountTests(_Fixture):

    def test_exact_below_the_cap(self):
        for match in ("within", "intersects"):
            with self.subTest(match=match), patch("api.views.LEGACY_DB_COUNT_CAP", 10):
                body, sql = self.spatial(match=match, pagesize=2)
                self.assertEqual((body["count"], body["count_capped"], body["pagesize"]), (7, False, 2))
                # QuerySet.count() is `SELECT COUNT(*) AS "__count"` (the per-place geometry summary
                # has a COUNT of its own, per group, which is not this).
                self.assertEqual([q for q in sql if '"__count"' in q], [])
                self.assertTrue(any("COUNT(" in q.upper() for q in sql))  # the matcher can see a COUNT

    def test_above_the_cap_is_the_cap_and_says_so(self):
        for match in ("within", "intersects"):
            with self.subTest(match=match), patch("api.views.LEGACY_DB_COUNT_CAP", 5):
                body, _ = self.spatial(match=match, pagesize=2)
                self.assertEqual((body["count"], body["count_capped"], len(body["features"])), (5, True, 2))

    def test_exactly_the_cap_is_not_capped(self):
        with patch("api.views.LEGACY_DB_COUNT_CAP", 7):
            body, _ = self.spatial()
        self.assertEqual((body["count"], body["count_capped"]), (7, False))

    def test_a_page_larger_than_the_cap_is_still_filled(self):
        with patch("api.views.LEGACY_DB_COUNT_CAP", 3):
            body, _ = self.spatial(pagesize=6)
        self.assertEqual((len(body["features"]), body["count"], body["count_capped"]), (6, 6, True))

    def test_intersects_scans_place_geom_by_distance_once(self):
        # The old view ran the ST_DistanceSphere match twice: once for the page, once for count().
        body, sql = self.spatial(match="intersects")
        self.assertEqual(body["count"], 7)
        scans = [q for q in sql if "ST_DISTANCESPHERE" in q.upper()]
        self.assertEqual(len(scans), 1, scans)
        self.assertTrue(scans[0].rstrip().endswith("LIMIT 1001"), scans[0])

    def test_visibility_and_dataset_still_apply_to_the_count(self):
        anon, _ = self.spatial()
        self.assertEqual(anon["count"], 7)
        self.assertNotIn("Hidden", [f["properties"]["title"] for f in anon["features"]])
        # Presence control: the private dataset's owner is counted the hidden place.
        self.client.force_login(self.owner)
        mine, _ = self.spatial()
        self.assertEqual(mine["count"], 8)
        self.assertIn("Hidden", [f["properties"]["title"] for f in mine["features"]])
        only_private, _ = self.spatial(dataset=self.private.id)
        self.assertEqual((only_private["count"], [f["properties"]["title"] for f in only_private["features"]]),
                         (1, ["Hidden"]))

    def test_keys_are_the_old_ones_plus_count_capped(self):
        for params in ({}, {"match": "intersects"}, {"geometry": "full"}, {"geometry": "none"},
                       {"type": "bbox", "sw": "-2,53", "ne": "0,55"}):
            with self.subTest(params=params):
                body, _ = self.spatial(**params)
                self.assertEqual(set(body), OLD_KEYS | {"count_capped"})
                self.assertEqual((body["count"], body["count_capped"]), (7, False))

    def test_the_page_is_fetched_with_its_dataset(self):
        # select_related('dataset') as before: the serializers read place.dataset for every feature.
        body, sql = self.spatial(pagesize=7)
        self.assertEqual(len(body["features"]), 7)
        page = [q for q in sql if 'FROM "places"' in q and 'INNER JOIN "datasets"' in q and " IN (" in q]
        self.assertTrue(page, sql)
