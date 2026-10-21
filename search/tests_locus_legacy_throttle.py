"""place#325 ruling 2026-10-21: a legacy ``/entity/locus:1NNNNNNN`` dereference writes a
permanent registry row the first time (mint on dereference, from the LegacyUnionRecord
snapshot), and legacy loci are public at release, so NEW rows are throttled per
caller: ``places.locus_registry.LEGACY_NEW_ROWS_PER_HOUR`` per IP for anonymous
callers, per user when signed in. Existing rows are served freely; an id not in the
snapshot is a 404 that writes nothing and costs nothing. Throttled: 429 with
Retry-After, and nothing written. Run locally:

    DB_NAME=whgv3beta_f4 GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test search.tests_locus_legacy_throttle --settings=whg.settings_localtest

Every absence (no row, 429) is asserted beside a presence (a row, a 200) in the same test.
"""
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.cache import cache
from django.test import TestCase, override_settings

from places import locus_registry as registry
from places.models import LegacyUnionRecord
from places.models_locus import LocusRegistryEntry
from places.tests_v4_landing import LOCMEM

LIMIT = 3          # patched down from LEGACY_NEW_ROWS_PER_HOUR (30) so a test is short
FIRST = 10000001


@override_settings(V4_LANDING_ENABLED=True, CACHES=LOCMEM)
class LegacyRowThrottleTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        from datasets.models import Dataset
        from places.models import Place
        User = get_user_model()
        cls.u1 = User.objects.create_user(username="lt_u1", email="lt_u1@example.org", password="pw",
                                          given_name="L", surname="One")
        cls.u2 = User.objects.create_user(username="lt_u2", email="lt_u2@example.org", password="pw",
                                          given_name="L", surname="Two")
        with patch("datasets.signals.doi"):
            ds = Dataset.objects.create(owner=cls.u1, label="lt_pub", title="Pub", description="D", public=True,
                                        ds_status="indexed")
        p = Place.objects.create(title="Legacy Town", src_id="lt1", dataset=ds, ccodes=[])
        for i in range(12):
            LegacyUnionRecord.objects.create(whg_id=FIRST + i, place_ids=[p.id], titles=["Legacy Town"])

    def setUp(self):
        cache.clear()
        self.real_limit = getattr(registry, "LEGACY_NEW_ROWS_PER_HOUR", None)
        p = patch.object(registry, "LEGACY_NEW_ROWS_PER_HOUR", LIMIT, create=True)
        p.start()
        self.addCleanup(p.stop)

    def get(self, whg_id, ip="203.0.113.1", accept="application/ld+json", user=None):
        self.client.logout()
        if user is not None:
            self.client.force_login(user)
        return self.client.get(f"/entity/locus:{whg_id}/", HTTP_ACCEPT=accept, REMOTE_ADDR=ip)

    def rows(self):
        return set(LocusRegistryEntry.objects.filter(kind="legacy").values_list("pid", flat=True))

    def test_the_constant(self):
        self.assertEqual(self.real_limit, 30)
        from api.views_locus import LegacyLocusRowThrottle
        self.assertEqual(LegacyLocusRowThrottle().get_rate(), f"{LIMIT}/hour")     # read at use, not import

    def test_new_rows_up_to_the_limit_then_429_and_nothing_written(self):
        for i in range(LIMIT):
            r = self.get(FIRST + i)
            self.assertEqual(r.status_code, 200, i)
        self.assertEqual(self.rows(), {str(FIRST + i) for i in range(LIMIT)})        # presence
        r = self.get(FIRST + LIMIT)
        self.assertEqual(r.status_code, 429)
        self.assertGreater(int(r["Retry-After"]), 0)
        self.assertLessEqual(int(r["Retry-After"]), 3600)
        self.assertEqual(r.json()["error"], "rate limited")
        self.assertNotIn(str(FIRST + LIMIT), self.rows())                              # absence, same test
        self.assertEqual(len(self.rows()), LIMIT)
        # The HTML representation is refused the same way and writes nothing either.
        h = self.get(FIRST + LIMIT, accept="text/html")
        self.assertEqual(h.status_code, 429)
        self.assertIn("Retry-After", h)
        self.assertNotIn(str(FIRST + LIMIT), self.rows())
        # An EXISTING row is still served at the limit, in both representations.
        self.assertEqual(self.get(FIRST).status_code, 200)
        self.assertEqual(self.get(FIRST, accept="text/html").status_code, 200)
        self.assertEqual(len(self.rows()), LIMIT)

    def test_ip_isolation(self):
        for i in range(LIMIT):
            self.assertEqual(self.get(FIRST + i, ip="203.0.113.1").status_code, 200)
        self.assertEqual(self.get(FIRST + LIMIT, ip="203.0.113.1").status_code, 429)
        r = self.get(FIRST + LIMIT, ip="198.51.100.7")                 # another IP: its own budget
        self.assertEqual(r.status_code, 200)
        self.assertIn(str(FIRST + LIMIT), self.rows())

    def test_signed_in_users_are_counted_per_user(self):
        for i in range(LIMIT):                                          # the IP's anonymous budget is spent
            self.assertEqual(self.get(FIRST + i).status_code, 200)
        self.assertEqual(self.get(FIRST + LIMIT).status_code, 429)
        # u1 from the same IP has a budget of their own ...
        for i in range(LIMIT):
            self.assertEqual(self.get(FIRST + LIMIT + i, user=self.u1).status_code, 200, i)
        r = self.get(FIRST + 2 * LIMIT, user=self.u1)                   # ... which runs out too
        self.assertEqual(r.status_code, 429)
        self.assertNotIn(str(FIRST + 2 * LIMIT), self.rows())
        # ... and is theirs alone: u2 (same IP) is not throttled by it.
        self.assertEqual(self.get(FIRST + 2 * LIMIT, user=self.u2).status_code, 200)
        self.assertIn(str(FIRST + 2 * LIMIT), self.rows())

    def test_ids_are_validated_against_the_snapshot_first_and_cost_nothing(self):
        for _ in range(LIMIT + 2):                                      # not in the snapshot: 404 every time
            self.assertEqual(self.get(19999999).status_code, 404)
        self.assertNotIn("19999999", self.rows())
        for i in range(LIMIT):                                          # ... and the budget is untouched
            self.assertEqual(self.get(FIRST + i).status_code, 200, i)
        self.assertEqual(self.get(FIRST + LIMIT).status_code, 429)
        self.assertEqual(self.get(19999998).status_code, 404)          # at the limit, still 404 (not 429)
        self.assertEqual(len(self.rows()), LIMIT)

    def test_a_portal_redirect_never_writes(self):
        """/places/<whg_id>/portal/ redirects to the locus without minting it (the
        snapshot is read, not the registry written); the dereference that follows is
        what is counted."""
        r = self.client.get(f"/places/{FIRST + 5}/portal/", REMOTE_ADDR="203.0.113.9")
        self.assertIn(r.status_code, (301, 302))
        self.assertTrue(r["Location"].endswith(f"/entity/locus:{FIRST + 5}/"))
        self.assertNotIn(str(FIRST + 5), self.rows())
        self.assertEqual(self.get(FIRST + 5, ip="203.0.113.9").status_code, 200)
        self.assertIn(str(FIRST + 5), self.rows())
