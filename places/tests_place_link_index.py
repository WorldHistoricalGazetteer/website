"""place#325 Stage 4: the place_link identifier index is usable by the
concordance's own query (places/migrations/0026_placelink_identifier_idx.py).

Run locally (never in a prod container):

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test places.tests_place_link_index --settings=whg.settings_localtest

The SQL explained is what ``build_concordance`` actually sends (captured,
not re-typed): one query for tier 2 (inbound) and one for tier 3 (co-cited),
each ordered by place id with its own LIMIT (review 2026-10-21), so a change to
``_LinkIdentifier`` or to the ORDER BY that the index no longer serves fails
here. The control drops the index in the same transaction and
requires the plan to change, so the assertion is shown to be able to fail.
"""
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.db import connection
from django.test import TestCase, TransactionTestCase
from django.test.utils import CaptureQueriesContext

INDEX = "place_link_identifier_idx"
SYNTHETIC_ROWS = 200_000


class PlaceLinkIdentifierIndexTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        from datasets.models import Dataset
        from places.models import Place, PlaceLink

        user = get_user_model().objects.create_user(
            username="ix_owner", email="ix_owner@example.org", password="pw", given_name="I", surname="X")
        with patch("datasets.signals.doi"):
            ds = Dataset.objects.create(owner=user, label="ix325", title="Ix", description="D", creator="X",
                                        public=True, ds_status="indexed", uri_base="https://example.org/p/")
        cls.A = Place.objects.create(title="Record A", src_id="A1", dataset=ds, ccodes=[])
        cls.B = Place.objects.create(title="Record B", src_id="B1", dataset=ds, ccodes=[])
        PlaceLink.objects.create(place=cls.A, jsonb={"type": "closeMatch", "identifier": "gn:42"})
        PlaceLink.objects.create(place=cls.B, jsonb={"type": "closeMatch", "identifier": "gn:42"})
        # Bulk filler, spread over many distinct identifiers like the real table.
        with connection.cursor() as cur:
            cur.execute(
                "INSERT INTO place_link (place_id, src_id, jsonb) "
                "SELECT %s, '', jsonb_build_object('type', 'closeMatch', 'identifier', 'syn:' || g) "
                "FROM generate_series(1, %s) g", [cls.B.pk, SYNTHETIC_ROWS])
            cur.execute("ANALYZE place_link")

    def _concordance_sql(self):
        from places.concordance import build_concordance
        with CaptureQueriesContext(connection) as ctx:
            conc = build_concordance(self.A, user=None, use_gateway=False)
        sqls = [q["sql"] for q in ctx.captured_queries if "->> 'identifier'" in q["sql"]]
        self.assertEqual(len(sqls), 2, sqls)            # inbound, then co-cited
        for sql in sqls:
            self.assertIn("ORDER BY", sql)
            self.assertIn("LIMIT", sql)
        return conc, sqls

    def _plan(self, sql):
        with connection.cursor() as cur:
            cur.execute("EXPLAIN " + sql)
            return "\n".join(r[0] for r in cur.fetchall())

    def test_index_exists_on_the_expression(self):
        with connection.cursor() as cur:
            cur.execute("SELECT indexdef FROM pg_indexes WHERE indexname = %s", [INDEX])
            row = cur.fetchone()
        self.assertIsNotNone(row)
        self.assertIn("(jsonb ->> 'identifier'::text)", row[0])

    def test_concordance_query_uses_the_index_and_not_without_it(self):
        conc, sqls = self._concordance_sql()
        # Presence: the query found B's co-citation (so it is the live tier-3 query).
        self.assertEqual([e["record"]["place_id"] for e in conc["cocited"]], [self.B.pk])

        for sql in sqls:
            plan = self._plan(sql)
            print(f"\n--- EXPLAIN with {INDEX} ({SYNTHETIC_ROWS:,} synthetic rows) ---\n{plan}")
            self.assertIn(INDEX, plan)
            self.assertNotIn("Seq Scan on place_link", plan)
            # Not a walk of the place_id / pk index to satisfy ORDER BY + LIMIT.
            self.assertNotIn("place_link_pkey", plan)
            self.assertNotIn("place_link_place_id", plan)

        # Control: without the index the same SQL must plan differently.
        with connection.cursor() as cur:
            cur.execute(f'DROP INDEX "{INDEX}"')
        for sql in sqls:
            plan_without = self._plan(sql)
            print(f"--- EXPLAIN without it ---\n{plan_without}")
            self.assertNotIn(INDEX, plan_without)
            self.assertIn("Seq Scan on place_link", plan_without)


class PlaceLinkIdentifierIndexReversibleTests(TransactionTestCase):
    """0026 unapplies (DROP INDEX CONCURRENTLY) and reapplies cleanly. Not in a
    transaction, as on prod: CONCURRENTLY refuses to run inside one."""

    def _exists(self):
        with connection.cursor() as cur:
            cur.execute("SELECT count(*) FROM pg_indexes WHERE indexname = %s", [INDEX])
            return cur.fetchone()[0] == 1

    def test_backwards_then_forwards(self):
        from django.core.management import call_command
        self.assertTrue(self._exists())
        try:
            call_command("migrate", "places", "0025_legacyunionrecord_and_more", verbosity=0)
            self.assertFalse(self._exists())
        finally:
            call_command("migrate", "places", verbosity=0)
        self.assertTrue(self._exists())
