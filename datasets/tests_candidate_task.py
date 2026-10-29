"""place#338: the ``generate_candidates`` task: resumable, idempotent, outage-aware, capped, paced.

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test datasets.tests_candidate_task --settings=whg.settings_localtest

``run_generation`` is driven directly with a fake ``sleep``/``schedule``/``clock``; the gateway is
the ``GatewayFake`` of ``tests_candidates`` (and ``requests`` in the client fails the test).
"""
import os
from unittest.mock import patch

from django.conf import settings
from django.test import override_settings

from datasets import candidates
from datasets import tasks_candidates as tc
from datasets.models_matching import CandidateGeneration, MatchCandidate, RecordReviewState
from datasets.tests_candidates import CandidateTestBase, GN_TOWN, hit, response

DEV = dict(ENV_CONTEXT="dev-whgazetteer-org", RECON_DEV_RECORD_LIMIT=50, RECON_BG_QPS=2)


class Clock:
    def __init__(self, step=0.1):
        self.t, self.step = 0.0, step

    def __call__(self):
        self.t += self.step
        return self.t


@override_settings(**DEV)
class GenerateTaskTests(CandidateTestBase):
    def setUp(self):
        super().setUp()
        self.recs = [self.place(f"Town {i}", f"t{i}", (0.1 * i, 51.0)) for i in range(5)]
        for i, r in enumerate(self.recs):
            self.fake.by_query[r.title] = response(hit(f"gn:{100 + i}", r.title, (0.1 * i, 51.0005), types=[GN_TOWN]))
        self.sleeps, self.scheduled = [], []

    def run_gen(self, gen):
        gen.refresh_from_db()
        return tc.run_generation(gen, sleep=self.sleeps.append,
                                 schedule=lambda pk, cd: self.scheduled.append((pk, cd)), clock=Clock())

    def new_gen(self, **kw):
        return CandidateGeneration.objects.create(dataset=self.ds, requested_by=self.owner,
                                                  recipe=candidates.build_recipe(self.ds), **kw)

    def test_runs_to_the_end_with_counts_and_pacing(self):
        gen = self.new_gen()
        self.assertEqual(self.run_gen(gen), "finished")
        gen.refresh_from_db()
        self.assertEqual(len(self.fake.calls), 5)
        self.assertEqual({k: gen.counts[k] for k in ("total", "done", "unqueried", "confident", "none")},
                         {"total": 5, "done": 5, "unqueried": 0, "confident": 5, "none": 0})
        self.assertEqual(gen.last_place_pk, self.recs[-1].pk)
        self.assertEqual(MatchCandidate.objects.filter(generation=gen).count(), 5)
        # 2 q/s with each call taking 0.1 s (fake clock): 0.4 s between calls.
        self.assertEqual(len(self.sleeps), 5)
        self.assertAlmostEqual(self.sleeps[0], 0.4, places=6)

    def test_idempotent_rerun_queries_nothing(self):
        gen = self.new_gen()
        self.run_gen(gen)
        self.assertEqual(len(self.fake.calls), 5)
        self.assertEqual(self.run_gen(gen), "finished")            # finished: a no-op
        gen.status = "running"                                     # re-delivered task, same generation
        gen.last_place_pk = None
        gen.save()
        self.assertEqual(self.run_gen(gen), "finished")
        self.assertEqual(len(self.fake.calls), 5)                  # nobody asked twice
        self.assertEqual(MatchCandidate.objects.filter(generation=gen).count(), 5)

    def test_resume_after_a_crash_does_not_repeat_answered_records(self):
        gen = self.new_gen()
        real = candidates.record_candidates
        seen = []

        def crash_on_fourth(place, generation, user):
            seen.append(place.pk)
            if len(seen) == 4:
                raise RuntimeError("worker killed")
            return real(place, generation, user)

        with patch("datasets.candidates.record_candidates", side_effect=crash_on_fourth):
            with self.assertRaises(RuntimeError):
                self.run_gen(gen)
        self.assertEqual(len(self.fake.calls), 3)
        self.assertEqual(self.run_gen(gen), "finished")            # the re-delivered task
        self.assertEqual([c["qstr"] for c in self.fake.calls], [r.title for r in self.recs])
        self.assertEqual(RecordReviewState.objects.filter(generation=gen, tier="confident").count(), 5)

    def test_outage_pauses_and_a_silent_record_later_becomes_none(self):
        """Silence is not absence, through the task: during an outage every record is
        ``unqueried`` and the run pauses; once the gateway answers (empty for Town 0), the same
        record is ``none``."""
        for r in self.recs:
            self.fake.failures[r.title] = "timeout"
        gen = self.new_gen()
        self.assertEqual(self.run_gen(gen), "paused")
        gen.refresh_from_db()
        self.assertEqual(gen.counts["unqueried"], 5)
        self.assertEqual(gen.counts["done"], 0)
        self.assertEqual(gen.counts.get("paused_reason"), "gateway")
        self.assertEqual(self.scheduled, [(gen.pk, tc.OUTAGE_BACKOFF_S)])
        st0 = RecordReviewState.objects.get(place=self.recs[0])
        self.assertEqual((st0.tier, st0.last_failure), ("unqueried", "timeout"))

        self.fake.failures.clear()
        self.fake.by_query["Town 0"] = response()                   # answered: nothing found
        status = None
        for _ in range(5):
            status = self.run_gen(gen)
            if status == "finished":
                break
        self.assertEqual(status, "finished")
        st0.refresh_from_db()
        self.assertEqual(st0.tier, "none")
        gen.refresh_from_db()
        self.assertEqual((gen.counts["unqueried"], gen.counts["none"], gen.counts["confident"]), (0, 1, 4))

    def test_one_bad_answer_does_not_pause(self):
        self.fake.failures["Town 2"] = "timeout"
        gen = self.new_gen()
        status = self.run_gen(gen)
        gen.refresh_from_db()
        self.assertEqual(status, "paused")                          # a retry round is scheduled...
        self.assertEqual(gen.counts.get("paused_reason"), "")       # ...not an outage pause
        self.assertEqual((gen.counts["unqueried"], gen.counts["confident"]), (1, 4))

    def test_decided_records_are_not_queried(self):
        RecordReviewState.objects.create(place=self.recs[1], dataset=self.ds, state="linked", tier="confident")
        gen = self.new_gen()
        self.run_gen(gen)
        asked = [c["qstr"] for c in self.fake.calls]
        self.assertEqual(len(asked), 4)
        self.assertNotIn("Town 1", asked)
        self.assertIn("Town 2", asked)

    @override_settings(RECON_DEV_RECORD_LIMIT=2)
    def test_dev_cap_limits_the_run(self):
        gen = self.new_gen()
        self.assertEqual(self.run_gen(gen), "finished")
        gen.refresh_from_db()
        self.assertEqual(len(self.fake.calls), 2)
        self.assertTrue(gen.counts["limited"])

    @override_settings(RECON_DEV_RECORD_LIMIT=None)
    def test_dev_without_a_cap_fails_before_any_call(self):
        gen = self.new_gen()
        self.assertEqual(self.run_gen(gen), "failed")
        gen.refresh_from_db()
        self.assertIn("RECON_DEV_RECORD_LIMIT", gen.error)
        self.assertEqual(self.fake.calls, [])
        with override_settings(RECON_DEV_RECORD_LIMIT=5):          # the same run, capped, does run
            gen2 = self.new_gen()
            self.assertEqual(self.run_gen(gen2), "finished")
        self.assertEqual(len(self.fake.calls), 5)

    def test_one_run_at_a_time(self):
        from django.utils import timezone
        busy = self.new_gen(status="running", heartbeat=timezone.now())
        gen = self.new_gen()
        self.assertEqual(self.run_gen(gen), "queued")
        self.assertEqual(self.fake.calls, [])
        self.assertEqual(self.scheduled, [(gen.pk, tc.BUSY_RETRY_S)])
        busy.heartbeat = timezone.now() - tc.STALE_HEARTBEAT * 2  # a dead worker's run does not block
        busy.save()
        self.assertEqual(self.run_gen(gen), "finished")
        self.assertEqual(len(self.fake.calls), 5)

    def test_start_generation_refuses_before_writing(self):
        with override_settings(RECON_DEV_RECORD_LIMIT=None):
            with self.assertRaises(candidates.GenerationRefused):
                tc.start_generation(self.ds, self.owner, enqueue=False)
        self.assertFalse(CandidateGeneration.objects.filter(dataset=self.ds).exists())
        gen = tc.start_generation(self.ds, self.owner, countries=["gb"], enqueue=False)
        self.assertEqual(gen.recipe["countries"], ["GB"])
        with self.assertRaises(candidates.GenerationRefused):        # one live run per dataset
            tc.start_generation(self.ds, self.owner, enqueue=False)


class RoutingTests(CandidateTestBase):
    def test_task_is_routed_to_the_recon_queue_and_the_worker_listens_to_it(self):
        from whg.celery import app
        self.assertEqual(tc.generate_candidates.name, "datasets.generate_candidates")
        self.assertIn("datasets.generate_candidates", app.tasks)
        route = app.amqp.router.route({}, "datasets.generate_candidates")
        self.assertEqual(route["queue"].name, "recon")
        other = app.amqp.router.route({}, "api.tasks.release_embargoes")   # the control
        self.assertNotEqual(other["queue"].name, "recon")
        with open(os.path.join(settings.BASE_DIR, "entrypoints", "entrypoint-celery.sh")) as f:
            self.assertIn('-Q "${CELERY_QUEUES:-celery,recon}"', f.read())
