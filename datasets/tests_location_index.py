"""place#350 follow-on: an adopted or hand-placed location is labelled in the Atlas popup and
portal, and reaches (and leaves) the search index by the dataset's existing re-index.

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test datasets.tests_location_index --settings=whg.settings_localtest --noinput

Every refusal or absence is paired with the same request answering once the one thing that made
it refuse is put right. Nothing leaves the process: the gateway, Celery and requests are mocked,
and the cache is a local-memory one.
"""
import json
from datetime import timedelta
from unittest import mock

from django.conf import settings
from django.contrib.auth import get_user_model
from django.test import Client, override_settings
from django.utils import timezone

from datasets import location_index, tasks
from datasets.models import Dataset
from datasets.tests_location_adoption import AdoptionTestBase
from places.models import PlaceGeom

# Only the default cache (the debounce key) is swapped; the other aliases are the test settings'.
LOCMEM = {**settings.CACHES,
          "default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache", "LOCATION": "li350"}}
CFG = {"CRC_REINGEST_URL": "https://gateway.invalid/api/registry/reingest"}


def resp(status, body=None):
    r = mock.Mock()
    r.status_code = status
    r.json.return_value = body if body is not None else {}
    return r


@override_settings(CACHES=LOCMEM)
class LocationIndexBase(AdoptionTestBase):
    def setUp(self):
        super().setUp()
        from django.core.cache import cache
        cache.clear()
        a = mock.patch.object(tasks.index_dataset_on_publish, "apply_async")
        self.apply_async = a.start()
        self.addCleanup(a.stop)

    def adopt_now(self, cid, **kw):
        with self.captureOnCommitCallbacks(execute=True):
            r = self.adopt(cid, **kw)
        self.assertEqual(r.status_code, 201, r.content)
        return r

    def place_now(self, place):
        with self.captureOnCommitCallbacks(execute=True):
            r = self.loc({"action": "place", "place": place.pk, "lon": 0.02, "lat": 51.77, "zoom": 12})
        self.assertEqual(r.status_code, 201, r.content)
        return r

    def publish(self):
        Dataset.objects.filter(pk=self.ds.pk).update(lifecycle_state="published", public=True,
                                                     index_pending_since=None, index_job_id="")
        self.ds.refresh_from_db()

    def popup(self, place, client=None):
        return (client or self.c).get(f"/entity/place:{place.pk}/api", {"variant": "popup"})

    def beta_stranger(self, name="li_str"):
        u = get_user_model().objects.create_user(username=name, email=f"{name}@example.org", password="pw",
                                                 given_name="S", surname=name, role="beta_tester")
        c = Client()
        c.force_login(u)
        return u, c


class PopupLabelTests(LocationIndexBase):
    def test_popup_labels_an_adopted_location_with_source_date_and_person(self):
        r = self.popup(self.hodd)
        self.assertEqual(r.status_code, 200)
        self.assertNotIn("location_provenance", r.json())                 # the control: none yet
        self.adopt_now("gn:8")
        d = self.popup(self.hodd).json()
        [row] = d["location_provenance"]
        g = PlaceGeom.objects.get(place=self.hodd)
        self.assertEqual((row["kind"], row["source"], row["source_label"], row["terms"], row["by"]),
                         ("adopted", "gn", "GeoNames", "CC-BY-4.0", "C Owner"))
        self.assertEqual(row["label"], f"Location from GeoNames, adopted {g.create_date:%-d %b %Y} by C Owner (CC-BY-4.0)")
        self.assertNotIn("location_provenance", self.popup(self.ware).json())  # the contributor's own

    def test_popup_labels_a_placed_location_as_approximate(self):
        self.place_now(self.hodd)
        [row] = self.popup(self.hodd).json()["location_provenance"]
        self.assertEqual(row["kind"], "placed")
        self.assertIsNone(row["source"])
        self.assertTrue(row["label"].startswith("Location placed on the map "))
        self.assertTrue(row["label"].endswith(" by C Owner (approximate)"))

    def test_popup_and_record_page_say_the_same_sentence(self):
        self.adopt_now("gn:8")
        label = self.popup(self.hodd).json()["location_provenance"][0]["label"]
        page = self.c.get(f"/atlas/place/{self.hodd.pk}/").content.decode()
        self.assertIn(label, " ".join(page.split()))

    def test_undo_takes_the_label_away(self):
        self.adopt_now("gn:8")
        g = PlaceGeom.objects.get(place=self.hodd)
        self.assertIn("location_provenance", self.popup(self.hodd).json())
        with self.captureOnCommitCallbacks(execute=True):
            r = self.loc({"action": "remove", "place": self.hodd.pk, "geom": g.pk})
        self.assertIn(r.status_code, (200, 204), r.content)
        self.assertNotIn("location_provenance", self.popup(self.hodd).json())

    def test_a_private_dataset_says_nothing_to_a_stranger(self):
        self.adopt_now("gn:8")
        _u, c = self.beta_stranger()
        r = self.popup(self.hodd, c)
        self.assertEqual(r.status_code, 404)
        self.assertNotIn(b"GeoNames", r.content)
        self.assertNotIn(b"adopted", r.content)
        self.publish()                                                     # the pair: now public
        r = self.popup(self.hodd, c)
        self.assertEqual(r.status_code, 200, r.content)
        self.assertEqual(r.json()["location_provenance"][0]["source"], "gn")


class PortalLabelTests(LocationIndexBase):
    def setUp(self):
        super().setUp()
        self.pid = f"whg:{self.ds.pk}:f3"
        p = mock.patch("api.crc_client.crc_places",
                       side_effect=lambda ids, **kw: {"places": [{"place_id": ids[0], "title": "Hoddesdon",
                                                                  "namespace": "whg", "geometries": []}]})
        self.crc = p.start()
        self.addCleanup(p.stop)

    def portal(self, client=None):
        return (client or self.c).get("/atlas/place/", {"id": self.pid})

    def test_portal_labels_an_adopted_location(self):
        r = self.portal()
        self.assertEqual(r.status_code, 200, r.content)
        self.assertNotIn("location_provenance", r.json())                 # the control
        self.adopt_now("gn:8")
        row = self.portal().json()["location_provenance"][0]
        self.assertEqual((row["kind"], row["source_label"]), ("adopted", "GeoNames"))
        self.assertIn("adopted", row["label"])

    def test_portal_says_nothing_to_a_stranger_and_never_asks_the_gateway(self):
        self.adopt_now("gn:8")
        _u, c = self.beta_stranger("li_portal")
        self.crc.reset_mock()
        r = self.portal(c)
        self.assertEqual(r.status_code, 404)
        self.assertNotIn(b"GeoNames", r.content)
        self.crc.assert_not_called()
        self.publish()                                                     # the pair
        r = self.portal(c)
        self.assertEqual(r.status_code, 200, r.content)
        self.assertEqual(r.json()["location_provenance"][0]["source"], "gn")

    def test_the_label_helper_refuses_a_row_the_user_may_not_see(self):
        """Second gate, in case the dataset check and the row ever disagree."""
        from search.views import _location_provenance_for
        self.adopt_now("gn:8")
        u, _c = self.beta_stranger("li_helper")
        self.assertEqual(_location_provenance_for(self.pid, u), [])
        self.assertEqual(len(_location_provenance_for(self.pid, self.owner)), 1)   # the pair


class IndexRefreshTests(LocationIndexBase):
    def flag(self):
        return Dataset.objects.get(pk=self.ds.pk).index_pending_since

    def test_adopting_on_a_published_dataset_asks_for_one_re_index(self):
        self.publish()
        self.adopt_now("gn:8")
        self.assertIsNotNone(self.flag())
        self.apply_async.assert_called_once_with(args=[self.ds.pk], countdown=location_index.REFRESH_DELAY_S)

    def test_a_run_of_changes_costs_one_job_but_moves_the_flag(self):
        self.publish()
        self.adopt_now("gn:8")
        first = self.flag()
        Dataset.objects.filter(pk=self.ds.pk).update(index_pending_since=first - timedelta(minutes=1))
        lone = self.place("Nazeing", "f4")
        from datasets.models_matching import RecordReviewState
        RecordReviewState.objects.create(place=lone, dataset=self.ds, generation=self.gen, tier="none")
        self.place_now(lone)
        self.assertGreater(self.flag(), first - timedelta(minutes=1))     # moved forward
        self.assertEqual(self.apply_async.call_count, 1)                  # still one job

    def test_removing_it_asks_for_the_re_index_that_takes_it_out(self):
        self.publish()
        self.adopt_now("gn:8")
        g = PlaceGeom.objects.get(place=self.hodd)
        Dataset.objects.filter(pk=self.ds.pk).update(index_pending_since=None)
        from django.core.cache import cache
        cache.clear()
        self.apply_async.reset_mock()
        with self.captureOnCommitCallbacks(execute=True):
            r = self.loc({"action": "remove", "place": self.hodd.pk, "geom": g.pk})
        self.assertIn(r.status_code, (200, 204), r.content)
        self.assertIsNotNone(self.flag())
        self.apply_async.assert_called_once()

    def test_a_private_dataset_is_not_sent_to_the_index(self):
        self.adopt_now("gn:8")                                             # private, unpublished
        self.assertIsNone(self.flag())
        self.apply_async.assert_not_called()
        self.publish()                                                     # the pair
        self.assertTrue(location_index.location_changed(self.ds.pk))
        self.assertIsNotNone(self.flag())
        self.apply_async.assert_called_once()

    def test_a_published_but_private_dataset_is_not_sent_either(self):
        Dataset.objects.filter(pk=self.ds.pk).update(lifecycle_state="published", public=False)
        self.assertFalse(location_index.location_changed(self.ds.pk))
        self.apply_async.assert_not_called()
        Dataset.objects.filter(pk=self.ds.pk).update(public=True)          # the pair
        self.assertTrue(location_index.location_changed(self.ds.pk))


@override_settings(**CFG)
@mock.patch.dict("os.environ", {"REINGEST_DATASET_TOKEN": "tok"})
class SupersededJobTests(LocationIndexBase):
    """A job that started before the change cannot vouch for it."""

    def setUp(self):
        super().setUp()
        self.publish()
        g = mock.patch("requests.get", return_value=resp(200, {"status": "completed", "indexed": True}))
        self.get = g.start()
        self.addCleanup(g.stop)
        p = mock.patch("requests.post", return_value=resp(202, {"job_id": "1"}))
        self.post = p.start()
        self.addCleanup(p.stop)
        i = mock.patch.object(tasks, "index_to_pub")
        i.start()
        self.addCleanup(i.stop)

    def run_with(self, job_ms, pending):
        Dataset.objects.filter(pk=self.ds.pk).update(index_job_id=str(job_ms), index_pending_since=pending)
        return tasks.index_dataset_on_publish.run(self.ds.pk)

    def ds_now(self):
        return Dataset.objects.get(pk=self.ds.pk)

    def test_a_job_older_than_the_change_is_not_taken_as_indexing_it(self):
        pending = timezone.now()
        job_ms = int(pending.timestamp() * 1000) - 5000                   # started 5 s before
        out = self.run_with(job_ms, pending)
        self.assertEqual(out, {"status": "pending", "gateway": "superseded"})
        d = self.ds_now()
        self.assertIsNotNone(d.index_pending_since)
        self.assertEqual(d.index_job_id, "")
        self.apply_async.assert_called_once_with(args=[self.ds.pk], countdown=60)

    def test_a_job_that_started_after_the_change_clears_the_flag(self):
        pending = timezone.now()
        out = self.run_with(int(pending.timestamp() * 1000) + 5000, pending)
        self.assertEqual(out, {"status": "indexed"})
        self.assertIsNone(self.ds_now().index_pending_since)

    def test_a_job_id_that_is_not_a_start_time_behaves_as_before(self):
        out = self.run_with("J1", timezone.now())
        self.assertEqual(out, {"status": "indexed"})
        self.assertIsNone(self.ds_now().index_pending_since)

    def test_job_predates_reads_only_millisecond_start_times(self):
        now = timezone.now()
        ms = int(now.timestamp() * 1000)
        self.assertTrue(location_index.job_predates(str(ms - 1), now))
        self.assertFalse(location_index.job_predates(str(ms + 1), now))
        self.assertFalse(location_index.job_predates("10759393", now))    # a Slurm id proves nothing
        self.assertFalse(location_index.job_predates(str(ms - 1), None))


class LabelWordingTests(LocationIndexBase):
    def test_the_label_is_the_record_pages_wording(self):
        from datetime import datetime
        d = datetime(2026, 11, 8, 10, 0)
        self.assertEqual(location_index.location_label("adopted", "GeoNames", "CC-BY-4.0", d, "A B"),
                         "Location from GeoNames, adopted 8 Nov 2026 by A B (CC-BY-4.0)")
        self.assertEqual(location_index.location_label("placed", None, None, d, "A B"),
                         "Location placed on the map 8 Nov 2026 by A B (approximate)")
        self.assertEqual(json.dumps(location_index.location_labels(self.ware)), "[]")
