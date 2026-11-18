"""``/api/db/`` counts its matches no further than ``LEGACY_DB_COUNT_CAP`` (SG 2026-11-18).
The exact ``COUNT(*)`` was a full scan of place_name on every request (``UPPER(toponym) LIKE
'%X%'`` has no index): ~1.3 s on prod for ``?name=York``. Page and count now come from ONE
``SELECT places.id ... LIMIT cap+1`` (``api.views.bounded_search``).

Pins: an exact ``count`` below the cap (``count_capped`` false); above it, ``count`` is the cap
and ``count_capped`` true; exactly the cap is not capped; one bounded search query and no
``COUNT(*)`` per request; the page is the first ``pagesize`` matches, duplicates of a place
with two matching names included, as before; the response keys are the old ones plus
``count_capped``; compat mode is untouched.

Run locally only:

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test api.tests_legacy_db_count --settings=whg.settings_localtest
"""

from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.db import connection
from django.test import TestCase, override_settings
from django.test.utils import CaptureQueriesContext

CAP = 5
OLD_KEYS = {"deprecation", "count", "pagesize", "parameters", "note", "type", "features", "attribution"}


class _Fixture(TestCase):
    """Seven places named Capville (one name each), one Oneville."""

    @classmethod
    def setUpTestData(cls):
        from api.models import APIToken
        from datasets.models import Dataset
        from places.models import Place, PlaceName
        User = get_user_model()
        cls.owner = User.objects.create_user(username="cc_owner", email="cc_owner@example.org", password="pw",
                                             given_name="CC", surname="Owner")
        APIToken.objects.create(user=cls.owner, key="cc-token")
        with patch("datasets.signals.doi"):
            cls.ds = Dataset.objects.create(
                owner=cls.owner, label="cc_ds", title="Count ds", description="D", creator="A Contributor",
                public=True, ds_status="indexed", uri_base="https://example.org/places/")
        for i in range(7):
            p = Place.objects.create(title=f"Capville {i}", src_id=f"cc{i}", dataset=cls.ds, fclasses=["P"], ccodes=["GB"])
            PlaceName.objects.create(place=p, toponym="Capville", jsonb={"toponym": "Capville"})
        p = Place.objects.create(title="Oneville", src_id="one", dataset=cls.ds, fclasses=["P"], ccodes=["GB"])
        PlaceName.objects.create(place=p, toponym="Oneville", jsonb={"toponym": "Oneville"})

    def setUp(self):
        from django.core.cache import caches
        for alias in ("default", "throttle"):
            try:
                caches[alias].clear()
            except Exception:
                pass

    def db(self, **params):
        with CaptureQueriesContext(connection) as ctx:
            r = self.client.get("/api/db/", params)
        self.assertEqual(r.status_code, 200, r.content[:300])
        sql = [q["sql"] for q in ctx.captured_queries]
        self.assertEqual([q for q in sql if "COUNT(" in q.upper()], [])
        # the name search itself: the queries that scan place_name with the LIKE
        searches = [q for q in sql if '"place_name"' in q and "LIKE" in q]
        return r.json(), searches


@patch("api.views.LEGACY_DB_COUNT_CAP", CAP)
class CappedCountTests(_Fixture):

    def test_exact_below_the_cap(self):
        # 7 matches > pagesize 2, cap raised to 10: exact, from one bounded query.
        with patch("api.views.LEGACY_DB_COUNT_CAP", 10):
            body, searches = self.db(name="capville", pagesize=2)
        self.assertEqual((body["count"], body["count_capped"], body["pagesize"]), (7, False, 2))
        self.assertEqual(len(searches), 1, searches)
        self.assertTrue(searches[0].rstrip().endswith("LIMIT 11"), searches[0])

    def test_capped_above_the_cap_with_the_flag(self):
        body, searches = self.db(name="capville", pagesize=2)
        self.assertEqual((body["count"], body["count_capped"], body["pagesize"]), (CAP, True, 2))
        self.assertEqual(len(body["features"]), 2)
        self.assertEqual(len(searches), 1, searches)
        self.assertTrue(searches[0].rstrip().endswith(f"LIMIT {CAP + 1}"), searches[0])

    def test_boundary_exactly_the_cap_is_not_capped(self):
        from places.models import Place
        Place.objects.filter(title__in=["Capville 5", "Capville 6"]).delete()
        body, _ = self.db(name="capville", pagesize=2)
        self.assertEqual((body["count"], body["count_capped"]), (CAP, False))

    def test_short_results_are_exact_with_one_search(self):
        for name, pagesize, n in (("capville", 20, 7), ("oneville", 20, 1), ("nowhereville", 20, 0)):
            body, searches = self.db(name=name, pagesize=pagesize)
            self.assertEqual((body["count"], body["count_capped"], len(body["features"]), body["pagesize"]),
                             (n, False, n, n), name)
            self.assertEqual(len(searches), 1, (name, searches))

    def test_a_place_with_two_matching_names_is_still_listed_and_counted_twice(self):
        from places.models import Place, PlaceName
        one = Place.objects.get(title="Oneville")
        PlaceName.objects.create(place=one, toponym="Oneville Abbey", jsonb={"toponym": "Oneville Abbey"})
        body, _ = self.db(name="oneville")
        self.assertEqual((body["count"], body["count_capped"]), (2, False))
        self.assertEqual([f["properties"]["title"] for f in body["features"]], ["Oneville", "Oneville"])

    def test_id_lookup_counts_too(self):
        from places.models import Place
        ids = ",".join(str(i) for i in Place.objects.filter(title__startswith="Capville")
                       .values_list("id", flat=True)[:3])
        body, _ = self.db(id=ids, pagesize=2)
        self.assertEqual((body["count"], body["count_capped"]), (3, False))

    def test_keys_are_the_old_ones_plus_count_capped(self):
        for params in ({"name": "capville", "pagesize": 2}, {"name": "oneville"}):
            body, _ = self.db(**params)
            self.assertEqual(set(body), OLD_KEYS | {"count_capped"}, params)
            self.assertEqual(next(iter(body)), "deprecation")
            self.assertEqual(body["type"], "FeatureCollection")
            self.assertIsInstance(body["count"], int)
            # built from the evaluated page now, not a re-run of the search
            self.assertEqual(list(body["attribution"].get("datasets", {})), ["cc_ds"], params)


@override_settings(LEGACY_API_MODE="compat")
@patch("api.views.LEGACY_DB_COUNT_CAP", CAP)
class CompatModeTests(_Fixture):

    def test_compat_name_search_is_served_by_reconcile_with_count_null(self):
        out = {"q0": {"result": [{"id": "place:gn:1", "name": "Capville", "score": 100, "match": True,
                                  "namespace": "gn", "ccodes": [], "repr_point": [0, 0]}]},
               "attribution": {"sources": {}}}
        with patch("api.reconcile.process_queries", return_value=out):
            r = self.client.get("/api/db/", {"name": "Capville", "token": "cc-token"})
        self.assertEqual(r.status_code, 200, r.content[:300])
        body = r.json()
        self.assertEqual(body["compat"]["served_by"], "compat")
        self.assertIsNone(body["count"])
        self.assertNotIn("count_capped", body)

    def test_inapp_passthrough_still_gets_the_capped_legacy_view(self):
        r = self.client.get("/api/db/", {"name": "capville", "pagesize": "2"},
                            HTTP_REFERER="http://testserver/places/1")
        self.assertEqual(r.status_code, 200, r.content[:300])
        body = r.json()
        self.assertNotIn("compat", body)
        self.assertEqual((body["count"], body["count_capped"]), (CAP, True))
