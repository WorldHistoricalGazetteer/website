"""The primary-key manager must hand out pks above what the database already holds (2026-11-17).

`ds_insert` initialised the counters under one key name and read them under another, so after a
Redis restart the first pk handed out was 1. A pk that already exists is UPDATED by `save()`,
not inserted."""
from django.test import TestCase

from datasets.models import Dataset
from places.models import Place, PlaceName
from validation.create_dataset import PrimaryKeyManager, get_redis_client


class PrimaryKeyManagerTests(TestCase):
    def setUp(self):
        self.r = get_redis_client()
        self.keys = ["PlaceName_max_pk", "Name_max_pk"]
        for k in self.keys:
            self.r.delete(k)
        self.addCleanup(lambda: [self.r.delete(k) for k in self.keys])

    def test_next_pk_is_above_the_database_maximum_whichever_name_is_used(self):
        from django.contrib.auth import get_user_model
        u = get_user_model().objects.create_user(username="pkm", password="x", email="pkm@example.org", given_name="P", surname="K")
        ds = Dataset.objects.create(owner=u, label="pkm_ds", title="pkm", description="d")
        pl = Place.objects.create(dataset=ds, src_id="a", title="A", ccodes=[])
        PlaceName.objects.create(pk=500, place=pl, src_id="a", toponym="A", jsonb={"toponym": "A"})
        m = PrimaryKeyManager()
        m.initialize_pks(["Name"])               # the spelling ds_insert uses to initialise ...
        self.assertGreater(m.get_next_pk("PlaceName"), 500)   # ... and the one it uses to read
        self.assertEqual(self.r.exists("Name_max_pk"), 0, "no second, orphan key")
