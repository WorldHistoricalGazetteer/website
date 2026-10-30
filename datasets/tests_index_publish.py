"""place#334 phase 4 (whg3 half): a published dataset is added to the search index.

All HTTP, ES and mail are mocked; nothing leaves the process.

    GDAL_LIBRARY_PATH=... venv/bin/python manage.py test datasets.tests_index_publish \
        --settings=whg.settings_localtest --noinput
"""
from unittest import mock

import requests
from django.test import override_settings

from datasets import lifecycle, tasks
from datasets.models import Dataset
from datasets.tests_lifecycle import LifecycleBase

CFG = {"CRC_REINGEST_URL": "https://gateway.invalid/api/registry/reingest"}
ENV = {"REINGEST_DATASET_TOKEN": "tok"}


def resp(status, body=None):
    r = mock.Mock()
    r.status_code = status
    r.json.return_value = body if body is not None else {}
    return r


class IndexOnPublishTests(LifecycleBase):
    def setUp(self):
        super().setUp()
        self.set_state("accepted")
        p = mock.patch("requests.post")
        self.post = p.start()
        self.addCleanup(p.stop)
        g = mock.patch("requests.get")
        self.get = g.start()
        self.addCleanup(g.stop)
        self.post.return_value = resp(202, {"job_id": "J1", "status": "queued"})
        # Celery is not eager under the test settings: run the task inline, and count the calls.
        d = mock.patch.object(tasks.index_dataset_on_publish, "delay",
                              side_effect=lambda pk: tasks.index_dataset_on_publish.run(pk))
        self.delay = d.start()
        self.addCleanup(d.stop)
        a = mock.patch.object(tasks.index_dataset_on_publish, "apply_async")
        self.apply_async = a.start()
        self.addCleanup(a.stop)
        rd = mock.patch.object(tasks.retract_dataset_from_index, "delay")
        self.retract_delay = rd.start()
        self.addCleanup(rd.stop)
        ra = mock.patch.object(tasks.retract_dataset_from_index, "apply_async")
        self.retract_async = ra.start()
        self.addCleanup(ra.stop)

    def flag(self):
        return Dataset.objects.get(pk=self.ds.pk).index_pending_since

    def job(self):
        return Dataset.objects.get(pk=self.ds.pk).index_job_id

    def publish(self):
        self.run_on_commit(lifecycle.publish, self.ds, self.owner)

    @override_settings(**CFG)
    def test_trigger_called_exactly_once_per_publish(self):
        with mock.patch.dict("os.environ", ENV):
            self.publish()
        self.assertEqual(self.delay.call_count, 1)
        self.assertEqual(self.post.call_count, 1)
        args, kwargs = self.post.call_args
        self.assertEqual(args[0], CFG["CRC_REINGEST_URL"])
        self.assertEqual(kwargs["json"], {"namespace": f"whg:{self.ds.pk}"})
        self.assertEqual(kwargs["headers"], {"Authorization": "Bearer tok"})
        self.assertEqual(self.job(), "J1")
        self.assertIsNotNone(self.flag())  # accepted for indexing is not indexed
        self.assertTrue(self.m["index"].called)

    @override_settings(**CFG)
    def test_failed_trigger_leaves_flag_and_still_indexes_to_pub(self):
        for outcome in (resp(503), resp(401), requests.ConnectionError("down")):
            Dataset.objects.filter(pk=self.ds.pk).update(index_pending_since=None, index_job_id="")
            self.m["index"].reset_mock()
            self.post.reset_mock()
            self.post.side_effect = outcome if isinstance(outcome, Exception) else None
            if not isinstance(outcome, Exception):
                self.post.return_value = outcome
            self.set_state("published", public=True)
            with mock.patch.dict("os.environ", ENV):
                tasks.index_dataset_on_publish(self.ds.pk)
            self.assertIsNotNone(self.flag(), outcome)
            self.assertEqual(self.job(), "")
            self.assertTrue(self.m["index"].called)

    @override_settings(**CFG)
    def test_409_keeps_flag_and_records_job(self):
        self.post.return_value = resp(409, {"detail": {"error": "running", "job_id": "J9"}})
        self.set_state("published", public=True)
        with mock.patch.dict("os.environ", ENV):
            tasks.index_dataset_on_publish(self.ds.pk)
        self.assertIsNotNone(self.flag())
        self.assertEqual(self.job(), "J9")

    @override_settings(**CFG)
    def test_flag_clears_only_when_confirmed_indexed(self):
        self.set_state("published", public=True)
        with mock.patch.dict("os.environ", ENV):
            tasks.index_dataset_on_publish(self.ds.pk)  # POST: job J1
            self.get.return_value = resp(200, {"job_id": "J1", "status": "completed", "indexed": False})
            tasks.index_dataset_on_publish(self.ds.pk)  # staged, not yet indexed
            self.assertIsNotNone(self.flag())
            self.get.return_value = resp(200, {"job_id": "J1", "status": "completed", "indexed": True})
            tasks.index_dataset_on_publish(self.ds.pk)
        self.assertIsNone(self.flag())
        self.assertEqual(self.job(), "")
        self.assertEqual(self.post.call_count, 1)  # polling never re-triggers

    @override_settings(**CFG)
    def test_a_job_that_failed_after_indexing_still_clears_the_flag(self):
        self.set_state("published", public=True)
        with mock.patch.dict("os.environ", ENV):
            tasks.index_dataset_on_publish(self.ds.pk)  # POST: job J1
            self.get.return_value = resp(200, {"job_id": "J1", "status": "failed", "indexed": False})
            tasks.index_dataset_on_publish(self.ds.pk)
            self.assertIsNotNone(self.flag())           # failed and not indexed: still waiting
            tasks.index_dataset_on_publish(self.ds.pk)  # fresh job J1 again (id cleared on failure)
            self.get.return_value = resp(200, {"job_id": "J1", "status": "failed", "indexed": True})
            tasks.index_dataset_on_publish(self.ds.pk)
        self.assertIsNone(self.flag())

    def test_unconfigured_makes_no_http_call(self):
        with override_settings(CRC_REINGEST_URL=""), mock.patch.dict("os.environ", ENV):
            self.publish()
        with override_settings(**CFG), mock.patch.dict("os.environ", {"REINGEST_DATASET_TOKEN": ""}):
            tasks.index_dataset_on_publish(self.ds.pk)
        self.post.assert_not_called()
        self.get.assert_not_called()
        self.assertTrue(self.m["index"].called)
        self.assertIsNotNone(self.flag())

    @override_settings(**CFG)
    def test_pending_or_private_never_triggers(self):
        cases = [("submitted", False), ("accepted", False), ("withdrawn", False),
                 ("published", False), (None, True), ("in_review", True)]
        with mock.patch.dict("os.environ", ENV):
            for state, public in cases:
                self.set_state(state, public=public)
                tasks.index_dataset_on_publish(self.ds.pk)
        self.post.assert_not_called()
        self.m["index"].assert_not_called()
        self.assertIsNone(self.flag())

    @override_settings(**CFG)
    def test_withdraw_clears_flag(self):
        with mock.patch.dict("os.environ", ENV):
            self.publish()
        self.assertIsNotNone(self.flag())
        self.run_on_commit(lifecycle.withdraw, self.ds, self.owner)
        self.assertIsNone(self.flag())
        self.assertEqual(self.job(), "")
        self.assertTrue(self.m["unindex"].delay.called)

    def test_status_line_wording(self):
        self.set_state("published", public=True)
        Dataset.objects.filter(pk=self.ds.pk).update(index_pending_since="2026-10-30 10:00")
        self.ds.refresh_from_db()
        src = open("datasets/templates/datasets/ds_status.html").read()
        i = src.rindex('id="ds_index_pending"')
        line = src[i:src.index("</p>", i)]
        self.assertIn("Waiting to be added to the search index", line)
        j = src.index('id="ds_index_pending"')
        failed = src[j:src.index("</p>", j)]
        self.assertIn("could not be added to the search index automatically", failed)
        for word in ("accession", "reconcil", "attest"):
            self.assertNotIn(word, line.lower())
            self.assertNotIn(word, failed.lower())
        self.assertIn("{% if ds.index_pending_since and ds.public %}", src)

    @override_settings(**CFG)
    def test_trigger_schedules_a_follow_up_poll(self):
        self.set_state("published", public=True)
        with mock.patch.dict("os.environ", ENV):
            tasks.index_dataset_on_publish(self.ds.pk)
        self.apply_async.assert_called_once_with(args=[self.ds.pk], countdown=60)

    @override_settings(**CFG)
    def test_failure_reason_is_kept_then_cleared_on_success(self):
        self.set_state("published", public=True)
        with mock.patch.dict("os.environ", ENV):
            tasks.index_dataset_on_publish(self.ds.pk)  # job J1
            self.get.return_value = resp(200, {"job_id": "J1", "status": "failed", "indexed": False,
                                               "reason": "stage_error"})
            tasks.index_dataset_on_publish(self.ds.pk)
            ds = Dataset.objects.get(pk=self.ds.pk)
            self.assertEqual(ds.index_failure_reason, "stage_error")
            self.assertEqual(ds.index_job_id, "")
            tasks.index_dataset_on_publish(self.ds.pk)  # fresh job
            self.get.return_value = resp(200, {"job_id": "J1", "status": "completed", "indexed": True})
            tasks.index_dataset_on_publish(self.ds.pk)
        ds = Dataset.objects.get(pk=self.ds.pk)
        self.assertEqual((ds.index_failure_reason, ds.index_job_id), ("", ""))
        self.assertIsNone(ds.index_pending_since)

    @override_settings(**CFG)
    def test_sweep_rechecks_only_recent_published_pending_datasets(self):
        from datetime import timedelta
        from django.utils import timezone
        self.set_state("published", public=True)
        now = timezone.now()
        Dataset.objects.filter(pk=self.ds.pk).update(index_pending_since=now - timedelta(hours=1))
        self.assertEqual(tasks.sweep_index_pending(), {"checked": 1})
        self.delay.assert_called_with(self.ds.pk)
        self.delay.reset_mock()
        Dataset.objects.filter(pk=self.ds.pk).update(index_pending_since=now - timedelta(hours=30))
        self.assertEqual(tasks.sweep_index_pending(), {"checked": 0})
        Dataset.objects.filter(pk=self.ds.pk).update(index_pending_since=now, public=False)
        self.assertEqual(tasks.sweep_index_pending(), {"checked": 0})
        self.delay.assert_not_called()

    @override_settings(**CFG)
    def test_409_for_another_datasets_job_is_not_adopted(self):
        self.post.return_value = resp(409, {"detail": {"error": "running", "job_id": "J9",
                                                       "namespace": "whg:99999"}})
        self.set_state("published", public=True)
        with mock.patch.dict("os.environ", ENV):
            tasks.index_dataset_on_publish(self.ds.pk)
        self.assertEqual(self.job(), "")
        self.post.return_value = resp(409, {"detail": {"error": "running", "job_id": "J8",
                                                       "namespace": f"whg:{self.ds.pk}"}})
        with mock.patch.dict("os.environ", ENV):
            tasks.index_dataset_on_publish(self.ds.pk)
        self.assertEqual(self.job(), "J8")

    @override_settings(**CFG)
    def test_retryable_failure_reasons_are_not_shown_as_a_problem(self):
        self.set_state("published", public=True)
        with mock.patch.dict("os.environ", ENV):
            tasks.index_dataset_on_publish(self.ds.pk)
            self.get.return_value = resp(200, {"job_id": "J1", "status": "failed", "indexed": False,
                                               "reason": "lock_conflict"})
            tasks.index_dataset_on_publish(self.ds.pk)
        self.assertEqual(Dataset.objects.get(pk=self.ds.pk).index_failure_reason, "")

    @override_settings(**CFG)
    def test_withdraw_queues_retraction_on_commit(self):
        with mock.patch.dict("os.environ", ENV):
            self.publish()
        self.retract_delay.assert_not_called()
        self.run_on_commit(lifecycle.withdraw, self.ds, self.owner)
        self.retract_delay.assert_called_once_with(self.ds.pk)

    @override_settings(**CFG)
    def test_retract_trigger_body_auth_and_follow_up(self):
        self.set_state("withdrawn", public=False)
        with mock.patch.dict("os.environ", ENV):
            tasks.retract_dataset_from_index.run(self.ds.pk)
        args, kwargs = self.post.call_args
        self.assertEqual(args[0], CFG["CRC_REINGEST_URL"])
        self.assertEqual(kwargs["json"], {"namespace": f"whg:{self.ds.pk}", "action": "retract"})
        self.assertEqual(kwargs["headers"], {"Authorization": "Bearer tok"})
        self.retract_async.assert_called_once_with(args=[self.ds.pk, "J1", 1], countdown=60)

    @override_settings(**CFG)
    def test_retract_poll_completes_or_gives_up(self):
        self.set_state("withdrawn", public=False)
        with mock.patch.dict("os.environ", ENV):
            self.get.return_value = resp(200, {"status": "completed", "indexed": False})
            out = tasks.retract_dataset_from_index.run(self.ds.pk, "J1", 1)
            self.assertEqual(out["status"], "retracted")
            self.retract_async.assert_not_called()
            self.get.return_value = resp(200, {"status": "running", "indexed": True})
            out = tasks.retract_dataset_from_index.run(self.ds.pk, "J1", 3)
            self.assertEqual(out["status"], "pending")
            self.assertEqual(self.retract_async.call_count, 1)
            out = tasks.retract_dataset_from_index.run(self.ds.pk, "J1", 9)
            self.assertEqual(out["status"], "unconfirmed")
            self.assertEqual(self.retract_async.call_count, 1)
        self.post.assert_not_called()

    @override_settings(**CFG)
    def test_retract_409_foreign_namespace_not_adopted(self):
        self.set_state("withdrawn", public=False)
        self.post.return_value = resp(409, {"detail": {"error": "running", "job_id": "J9",
                                                       "namespace": "whg:99999"}})
        with mock.patch.dict("os.environ", ENV):
            tasks.retract_dataset_from_index.run(self.ds.pk)
        self.retract_async.assert_not_called()
        self.post.return_value = resp(409, {"detail": {"error": "running", "job_id": "J8",
                                                       "namespace": f"whg:{self.ds.pk}"}})
        with mock.patch.dict("os.environ", ENV):
            tasks.retract_dataset_from_index.run(self.ds.pk)
        self.retract_async.assert_called_once_with(args=[self.ds.pk, "J8", 1], countdown=60)

    @override_settings(**CFG)
    def test_retract_gateway_failure_does_not_raise(self):
        self.set_state("withdrawn", public=False)
        with mock.patch.dict("os.environ", ENV):
            for outcome in (resp(503), resp(401)):
                self.post.return_value = outcome
                self.assertEqual(tasks.retract_dataset_from_index.run(self.ds.pk)["status"], "failed")
            self.post.side_effect = requests.ConnectionError("down")
            self.assertEqual(tasks.retract_dataset_from_index.run(self.ds.pk)["status"], "failed")
        self.retract_async.assert_not_called()

    def test_retract_unconfigured_makes_no_call(self):
        self.set_state("withdrawn", public=False)
        with override_settings(CRC_REINGEST_URL=""), mock.patch.dict("os.environ", ENV):
            tasks.retract_dataset_from_index.run(self.ds.pk)
        with override_settings(**CFG), mock.patch.dict("os.environ", {"REINGEST_DATASET_TOKEN": ""}):
            tasks.retract_dataset_from_index.run(self.ds.pk)
        self.post.assert_not_called()
        self.get.assert_not_called()

    @override_settings(**CFG)
    def test_retract_still_published_makes_no_call(self):
        self.set_state("published", public=True)
        with mock.patch.dict("os.environ", ENV):
            out = tasks.retract_dataset_from_index.run(self.ds.pk)
        self.assertEqual(out["status"], "skipped")
        self.post.assert_not_called()
        self.get.assert_not_called()
