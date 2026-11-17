"""Id sequences must not be left behind by explicit-pk inserts (2026-11-17).

`ds_insert` saves related rows with pks from Redis, so a table's own Postgres sequence stays put.
The v3 review screen then lets Postgres assign ids (`bulk_create`) and collides with rows that exist:
every accepted Wikidata match was a 500 (`place_geom_pkey`, `place_name_pkey`).

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test validation.tests_pk_sequences --settings=whg.settings_localtest

The failure is reproduced first (an unmoved sequence DOES collide), so the pass afterwards means
the resync did the work and not that the collision never arises.
"""
from io import StringIO
from unittest.mock import patch

from django.core.management import call_command
from django.db import IntegrityError, transaction
from django.test import TestCase

from datasets.models import Dataset
from places.models import Place, PlaceName
from validation.create_dataset import resync_pk_sequences


class SequenceResyncTests(TestCase):
    def setUp(self):
        from django.contrib.auth import get_user_model
        owner = get_user_model().objects.create_user(
            username="seq_owner", email="so@example.org", password="pw", given_name="S", surname="O")
        with patch("datasets.signals.doi"):
            ds = Dataset.objects.create(owner=owner, label="seqtest", title="T", description="D",
                                        creator="X", public=False, ds_status="uploaded")
        self.place = Place.objects.create(title="P", src_id="p1", dataset=ds, ccodes=[])
        # What ds_insert does: an explicit pk well ahead of the sequence.
        self.ahead = PlaceName.objects.order_by("-id").values_list("id", flat=True).first() or 0
        self.ahead += 1000
        PlaceName(id=self.ahead, place=self.place, src_id="p1", toponym="Ahead", jsonb={}).save()

    def _bulk(self, n=1):
        PlaceName.objects.bulk_create(
            [PlaceName(place=self.place, src_id="p1", toponym=f"B{i}", jsonb={}) for i in range(n)])

    def test_unmoved_sequence_collides(self):
        # Reaching the explicit pk needs the sequence to walk up to it; force it just below.
        with transaction.atomic(), self.assertRaises(IntegrityError):
            with connection_cursor() as cur:
                cur.execute("SELECT setval(pg_get_serial_sequence('place_name','id'), %s)", [self.ahead - 1])
            self._bulk()

    def test_resync_makes_bulk_create_safe(self):
        with connection_cursor() as cur:
            cur.execute("SELECT setval(pg_get_serial_sequence('place_name','id'), %s)", [self.ahead - 1])
        moved = resync_pk_sequences([PlaceName])
        self.assertEqual(moved, {"place_name": self.ahead})
        self._bulk(3)
        self.assertGreater(PlaceName.objects.filter(toponym__startswith="B").order_by("id").first().id, self.ahead)

    def test_never_lowers_a_sequence_and_second_run_is_a_noop(self):
        resync_pk_sequences([PlaceName])
        with connection_cursor() as cur:
            cur.execute("SELECT setval(pg_get_serial_sequence('place_name','id'), %s)", [self.ahead + 500])
        self.assertEqual(resync_pk_sequences([PlaceName]), {})
        with connection_cursor() as cur:
            cur.execute("SELECT last_value FROM place_name_id_seq")
            self.assertEqual(cur.fetchone()[0], self.ahead + 500)

    def test_management_command_reports(self):
        with connection_cursor() as cur:
            cur.execute("SELECT setval(pg_get_serial_sequence('place_name','id'), %s)", [self.ahead - 1])
        out = StringIO()
        call_command("resync_pk_sequences", stdout=out)
        self.assertIn("place_name", out.getvalue())
        out2 = StringIO()
        call_command("resync_pk_sequences", stdout=out2)
        self.assertIn("already level", out2.getvalue())


def connection_cursor():
    from django.db import connection
    return connection.cursor()


class DsInsertResyncsTests(TestCase):
    """The wiring: a real ds_insert leaves the sequences level, so a later bulk_create cannot collide."""

    def test_ds_insert_leaves_sequences_level(self):
        import json, tempfile
        from django.contrib.auth import get_user_model
        from validation.create_dataset import ds_insert
        owner = get_user_model().objects.create_user(
            username="seq_owner2", email="so2@example.org", password="pw", given_name="S", surname="O")
        with patch("datasets.signals.doi"):
            ds = Dataset.objects.create(owner=owner, label="seqins", title="T", description="D",
                                        creator="X", public=False, ds_status="uploaded", numrows=1)
        feat = {"@id": "f1", "type": "Feature", "properties": {"title": "Alpha", "src_id": "f1", "ccodes": ["GB"]},
                "names": [{"toponym": "Alpha"}],
                "geometry": {"type": "Point", "coordinates": [1.0, 51.0]}}
        with tempfile.NamedTemporaryFile("w", suffix=".jsonld", delete=False) as f:
            json.dump({"type": "FeatureCollection", "features": [feat]}, f)
        ds_insert(f.name, ds, "seq-test-task")
        row = PlaceName.objects.get(place__dataset=ds)
        with connection_cursor() as cur:
            cur.execute("SELECT last_value FROM place_name_id_seq")
            self.assertGreaterEqual(cur.fetchone()[0], row.id)
        PlaceName.objects.bulk_create([PlaceName(place=row.place, src_id="f1", toponym="Beta", jsonb={})])
