"""place#338: suggestions prepared without being asked: the automatic start when a dataset becomes
ready for linking, the owner's email when a run finishes, and the daily refresh of stale runs.

    DB_NAME=whgv3beta_<you> GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test datasets.tests_suggestions_auto --settings=whg.settings_localtest

No gateway is called (``CandidateTestBase``'s fake, and ``requests`` fails the test); nothing is
queued (``_schedule`` and the task's ``delay`` are replaced); no mail leaves (``WHGmail`` is
patched). Every "nothing happened" is asserted next to the same path doing something.
"""
import re
from datetime import timedelta
from unittest.mock import patch

from django.template.loader import render_to_string
from django.test import override_settings
from django.utils import timezone

from datasets import candidates, lifecycle
from datasets import tasks_candidates as tc
from datasets.models import Dataset, DatasetLifecycleEvent
from datasets.models_matching import CandidateGeneration
from datasets.tests_candidates import CandidateTestBase, GN_TOWN, hit, response

DEV = dict(ENV_CONTEXT="dev-whgazetteer-org", RECON_DEV_RECORD_LIMIT=50, RECON_BG_QPS=2,
           V4_LANDING_ENABLED=False)
FORBIDDEN = re.compile(r"attest|accession|reconcil", re.I)


class Base(CandidateTestBase):
    def setUp(self):
        super().setUp()
        self.recs = [self.place(f"Town {i}", f"t{i}", (0.1 * i, 51.0)) for i in range(3)]
        for i, r in enumerate(self.recs):
            self.fake.by_query[r.title] = response(hit(f"gn:{100 + i}", r.title, (0.1 * i, 51.0005),
                                                       types=[GN_TOWN]))
        self.scheduled = []
        p = patch.object(tc, "_schedule", side_effect=lambda pk, cd: self.scheduled.append((pk, cd)))
        p.start()
        self.addCleanup(p.stop)
        # The on_commit hook calls .delay: run the task body in-process instead of queueing it.
        p = patch.object(tc.auto_generate_candidates, "delay",
                         side_effect=lambda pk: tc.auto_generate_candidates(pk))
        self.delay = p.start()
        self.addCleanup(p.stop)
        p = patch("whgmail.messaging.WHGmail", return_value=True)
        self.mail = p.start()
        self.addCleanup(p.stop)

    def set_state(self, state, ds=None, **fields):
        ds = ds or self.ds
        Dataset.objects.filter(pk=ds.pk).update(lifecycle_state=state, **fields)
        ds.refresh_from_db()

    def gens(self, ds=None):
        return list(CandidateGeneration.objects.filter(dataset=ds or self.ds).order_by("id"))

    def committed(self, fn, *a, **k):
        with self.captureOnCommitCallbacks(execute=True):
            return fn(*a, **k)

    def run_gen(self, gen):
        gen.refresh_from_db()
        return tc.run_generation(gen, sleep=lambda s: None,
                                 schedule=lambda pk, cd: self.scheduled.append((pk, cd)),
                                 clock=iter(range(10 ** 6)).__next__)

    def mails(self):
        return [c.kwargs["context"] for c in self.mail.call_args_list]


# ---------------------------------------------------------------------------
# (1) Automatic start
# ---------------------------------------------------------------------------

@override_settings(**DEV)
class AutoStartTests(Base):
    def test_start_queues_one_run_after_commit_and_a_refused_start_queues_none(self):
        stranger = type(self.owner).objects.create_user(username="s338", email="s@x.org", password="pw",
                                                               given_name="S", surname="T")
        with self.assertRaises(lifecycle.LifecycleRefused):        # a stranger may not start it
            self.committed(lifecycle.start, self.ds, stranger)
        self.assertEqual((self.gens(), self.delay.call_count), ([], 0))

        lifecycle.start(self.ds, self.owner)                       # no commit: nothing queued
        self.assertEqual((self.gens(), self.delay.call_count), ([], 0))
        self.set_state(None)

        self.committed(lifecycle.start, self.ds, self.owner)       # committed: one run
        (gen,) = self.gens()
        self.assertEqual((gen.status, gen.requested_by, gen.counts["trigger"]), ("queued", self.owner, "auto"))
        self.assertEqual(gen.counts["records_signature"], tc.records_signature(self.ds))
        self.assertEqual(self.scheduled, [(gen.pk, 0)])

    def test_validation_passed_starts_a_run_and_validation_failed_does_not(self):
        self.set_state("validating")
        self.committed(lifecycle.validation_failed, self.ds, ["bad"])
        self.assertEqual(self.gens(), [])
        self.set_state("validating")
        self.committed(lifecycle.validation_passed, self.ds)
        self.assertEqual([g.counts["trigger"] for g in self.gens()], ["auto"])

    def test_idempotent_until_the_records_change(self):
        self.set_state("draft")
        self.assertEqual(tc.auto_generate_candidates(self.ds.pk), "started")
        self.assertEqual(tc.auto_generate_candidates(self.ds.pk), "skipped")      # queued
        self.assertEqual(self.run_gen(self.gens()[0]), "finished")
        self.assertEqual(tc.auto_generate_candidates(self.ds.pk), "skipped")      # finished, same records
        self.assertEqual(len(self.gens()), 1)
        self.place("Town 9", "t9")                                                 # the records changed
        self.assertEqual(tc.auto_generate_candidates(self.ds.pk), "started")
        first, second = self.gens()
        self.assertEqual((first.status, second.status), ("superseded", "queued"))

    def test_a_failed_run_does_not_block_a_new_start(self):
        self.set_state("draft")
        CandidateGeneration.objects.create(dataset=self.ds, status="failed",
                                           counts={"records_signature": tc.records_signature(self.ds)})
        self.assertEqual(tc.auto_generate_candidates(self.ds.pk), "started")

    def test_never_for_a_dataset_off_the_lifecycle_or_before_validation(self):
        for state in (None, "uploaded", "validating", "validation_failed"):
            with self.subTest(state=state):
                self.set_state(state)
                self.assertEqual(tc.auto_generate_candidates(self.ds.pk), "skipped")
        self.assertEqual(self.gens(), [])
        self.set_state("draft")                                                    # control
        self.assertEqual(tc.auto_generate_candidates(self.ds.pk), "started")

    def test_beta_gated_on_the_owner(self):
        self.set_state("draft")
        type(self.owner).objects.filter(pk=self.owner.pk).update(role="normal")
        self.assertEqual(tc.auto_generate_candidates(self.ds.pk), "skipped")
        self.assertEqual(self.gens(), [])
        type(self.owner).objects.filter(pk=self.owner.pk).update(role="beta_tester")
        self.assertEqual(tc.auto_generate_candidates(self.ds.pk), "started")

    def test_the_environment_cap_still_refuses_outside_production(self):
        self.set_state("draft")
        with override_settings(RECON_DEV_RECORD_LIMIT=None):
            self.assertEqual(tc.auto_generate_candidates(self.ds.pk), "refused")
        self.assertEqual((self.gens(), self.scheduled), ([], []))
        with override_settings(RECON_DEV_RECORD_LIMIT=500):                       # over the ceiling
            self.assertEqual(tc.auto_generate_candidates(self.ds.pk), "refused")
        self.assertEqual(self.gens(), [])
        self.assertEqual(tc.auto_generate_candidates(self.ds.pk), "started")       # control (50)

    def test_manual_prepare_is_unchanged_and_blocks_the_automatic_start(self):
        self.set_state("draft")
        gen = tc.start_generation(self.ds, self.owner)
        self.assertEqual(gen.counts["trigger"], "manual")
        self.assertEqual(tc.auto_generate_candidates(self.ds.pk), "skipped")
        with self.assertRaises(candidates.GenerationRefused):                     # and vice versa
            tc.start_generation(self.ds, self.owner)


# ---------------------------------------------------------------------------
# (2) The owner's email
# ---------------------------------------------------------------------------

@override_settings(**DEV)
class CompletionMailTests(Base):
    def test_a_finished_run_mails_the_owner_once(self):
        gen = tc.start_generation(self.ds, self.owner, trigger="auto")
        self.assertEqual(self.run_gen(gen), "finished")
        (ctx,) = self.mails()
        self.assertEqual((ctx["template"], ctx["to_email"]), ("suggestions_ready", self.owner.email))
        self.assertNotIn("bcc", ctx)                         # the counts go to the owner only (Q11)
        self.assertNotIn("cc", ctx)
        self.assertTrue(ctx["feed_url"].endswith(f"/datasets/{self.ds.pk}/matches/"))
        self.assertEqual((ctx["records_checked"], ctx["with_suggestions"]), (3, 3))
        # The same run again: finished is a no-op; a re-delivered run finishes again, sends nothing.
        self.assertEqual(self.run_gen(gen), "finished")
        CandidateGeneration.objects.filter(pk=gen.pk).update(status="running", last_place_pk=None)
        self.assertEqual(self.run_gen(gen), "finished")
        self.assertEqual(len(self.mails()), 1)
        self.assertFalse(tc.notify_owner(gen))
        self.assertEqual(len(self.mails()), 1)

    def test_no_mail_when_a_run_fails_or_pauses(self):
        gen = tc.start_generation(self.ds, self.owner)
        with override_settings(RECON_DEV_RECORD_LIMIT=None):
            self.assertEqual(self.run_gen(gen), "failed")
        for r in self.recs:
            self.fake.failures[r.title] = "timeout"
        self.place("Town 3", "t3")
        self.place("Town 4", "t4")
        self.fake.failures.update({"Town 3": "timeout", "Town 4": "timeout"})
        gen2 = tc.start_generation(self.ds, self.owner)
        self.assertEqual(self.run_gen(gen2), "paused")
        self.assertEqual(self.mails(), [])
        self.fake.failures.clear()                                     # control: the gateway answers
        status = None
        for _ in range(5):
            status = self.run_gen(gen2)
            if status == "finished":
                break
        self.assertEqual(status, "finished")
        self.assertEqual(len(self.mails()), 1)

    def test_notify_refuses_a_run_that_has_not_finished(self):
        gen = tc.start_generation(self.ds, self.owner)
        for status in ("queued", "running", "paused", "failed"):
            CandidateGeneration.objects.filter(pk=gen.pk).update(status=status)
            self.assertFalse(tc.notify_owner(gen))
        self.assertEqual(self.mails(), [])
        CandidateGeneration.objects.filter(pk=gen.pk).update(status="finished")   # control
        self.assertTrue(tc.notify_owner(gen))
        self.assertEqual(len(self.mails()), 1)

    def test_a_mail_failure_does_not_undo_the_finish_and_is_not_retried(self):
        self.mail.side_effect = RuntimeError("smtp down")
        gen = tc.start_generation(self.ds, self.owner)
        self.assertEqual(self.run_gen(gen), "finished")
        gen.refresh_from_db()
        self.assertEqual(gen.status, "finished")
        self.assertIn("owner_notified", gen.counts)
        self.assertEqual(self.mail.call_count, 1)

    def test_a_refresh_mails_at_most_weekly_but_a_manual_run_always(self):
        first = tc.start_generation(self.ds, self.owner, trigger="auto")
        self.run_gen(first)
        refresh = tc.start_generation(self.ds, self.owner, trigger="refresh")
        self.run_gen(refresh)
        self.assertEqual(len(self.mails()), 1)                        # the refresh was quiet
        manual = tc.start_generation(self.ds, self.owner)
        self.run_gen(manual)
        self.assertEqual(len(self.mails()), 2)
        CandidateGeneration.objects.filter(dataset=self.ds).update(finished=timezone.now() - timedelta(days=8))
        later = tc.start_generation(self.ds, self.owner, trigger="refresh")
        self.run_gen(later)
        self.assertEqual(len(self.mails()), 3)
        self.assertTrue(self.mails()[-1]["refresh"])

    def test_the_template_keeps_to_the_vocabulary(self):
        for refresh, limited in ((False, False), (True, True)):
            html = render_to_string("suggestions_ready.html", {
                "greeting_name": "Olive", "dataset_title": "Clergy places", "dataset_label": "cp",
                "dataset_id": 7, "reply_to": "editor@example.org", "refresh": refresh,
                "records_checked": 40, "with_suggestions": 1, "limited": limited,
                "feed_url": "https://whgazetteer.org/datasets/7/matches/"})
            text = re.sub(r"<style.*?</style>", "", html, flags=re.S)
            self.assertIn("Clergy places", text)
            self.assertIn('href="https://whgazetteer.org/datasets/7/matches/"', text)
            self.assertIn("1 of them has at least one suggestion", text)
            self.assertIn("Link", text)
            self.assertIsNone(FORBIDDEN.search(text))
            self.assertEqual("Not every record was checked" in text, limited)
            self.assertEqual("looked again" in text, refresh)


# ---------------------------------------------------------------------------
# (3) Daily refresh
# ---------------------------------------------------------------------------

@override_settings(**DEV, RECON_REFRESH_PER_RUN=2, RECON_REFRESH_PER_DAY=3)
class RefreshSweepTests(Base):
    def setUp(self):
        super().setUp()
        self.t0 = timezone.now() - timedelta(days=3)

    def finished_gen(self, ds, started, **recipe):
        return CandidateGeneration.objects.create(
            dataset=ds, requested_by=self.owner, status="finished", started=started, created=started,
            finished=started + timedelta(minutes=5), record_limit=20,
            recipe=candidates.build_recipe(ds, **recipe), counts={"trigger": "auto"})

    def published(self, label, at, pending=False):
        with patch("datasets.signals.doi"):
            ds = Dataset.objects.create(owner=self.owner, label=label, title=label, description="D",
                                        creator="X", public=True)
        Dataset.objects.filter(pk=ds.pk).update(
            lifecycle_state="published", index_pending_since=timezone.now() if pending else None)
        DatasetLifecycleEvent.objects.create(dataset=ds, dataset_pk=ds.pk, dataset_label=label,
                                             from_state="accepted", to_state="published", at=at)
        return ds

    def draft(self, label):
        with patch("datasets.signals.doi"):
            ds = Dataset.objects.create(owner=self.owner, label=label, title=label, description="D",
                                        creator="X", public=False, lifecycle_state="draft")
        return ds

    def test_stale_only_after_a_newer_publication_that_is_in_the_index(self):
        self.set_state("draft")
        old = self.finished_gen(self.ds, self.t0, countries=["gb"])
        self.assertEqual(tc.stale_generations(), [])                          # nothing published
        pending = self.published("pend338", self.t0 + timedelta(hours=1), pending=True)
        self.published("long338", self.t0 - timedelta(days=2))                # well before the run
        self.assertEqual(tc.stale_generations(), [])
        Dataset.objects.filter(pk=pending.pk).update(index_pending_since=None)  # now in the index
        self.assertEqual(tc.stale_generations(), [old])

        out = tc.refresh_stale_suggestions()
        (new,) = [g for g in self.gens() if g.pk != old.pk]
        self.assertEqual(out["started"], [new.pk])
        self.assertEqual((new.counts["trigger"], new.recipe.get("countries"), new.record_limit),
                         ("refresh", ["GB"], 20))
        old.refresh_from_db()
        self.assertEqual(old.status, "superseded")
        self.assertEqual(tc.stale_generations(), [])                          # in flight: skipped
        self.assertEqual(tc.refresh_stale_suggestions()["started"], [])

    def test_a_publication_within_the_grace_before_the_run_still_counts(self):
        self.set_state("draft")
        old = self.finished_gen(self.ds, self.t0)
        self.published("grace338", self.t0 - timedelta(hours=2))
        self.assertEqual(tc.stale_generations(), [old])
        CandidateGeneration.objects.filter(pk=old.pk).update(started=self.t0 + timedelta(days=2))
        self.assertEqual(tc.stale_generations(), [])

    def test_never_touches_null_lifecycle_or_pre_validation_datasets(self):
        self.published("new338", self.t0 + timedelta(hours=1))
        old = self.finished_gen(self.ds, self.t0)
        for state in (None, "validating", "uploaded"):
            with self.subTest(state=state):
                self.set_state(state)
                self.assertEqual(tc.stale_generations(), [])
                self.assertEqual(tc.refresh_stale_suggestions()["started"], [])
        self.set_state("draft")                                               # control
        self.assertEqual(tc.stale_generations(), [old])

    def test_a_failed_latest_run_is_left_for_the_owner(self):
        self.set_state("draft")
        old = self.finished_gen(self.ds, self.t0)
        self.published("new338", self.t0 + timedelta(hours=1))
        self.assertEqual(tc.stale_generations(), [old])
        CandidateGeneration.objects.create(dataset=self.ds, status="failed", started=self.t0)
        self.assertEqual(tc.stale_generations(), [])

    def test_capped_per_sweep_and_per_day(self):
        self.published("new338", self.t0 + timedelta(hours=1))
        for i in range(5):
            self.finished_gen(self.draft(f"d338{i}"), self.t0 - timedelta(minutes=i))
        self.assertEqual(len(tc.stale_generations()), 5)
        self.assertEqual(len(tc.refresh_stale_suggestions()["started"]), 2)    # per sweep
        self.assertEqual(len(tc.refresh_stale_suggestions()["started"]), 1)    # 3 a day
        self.assertEqual(tc.refresh_stale_suggestions()["started"], [])
        self.assertEqual(len(tc.stale_generations()), 2)                       # still waiting
        CandidateGeneration.objects.filter(counts__trigger="refresh").update(
            created=timezone.now() - timedelta(hours=25))
        self.assertEqual(len(tc.refresh_stale_suggestions()["started"]), 2)    # the next day

    def test_beta_gate_and_dev_cap_skip_without_starting(self):
        self.set_state("draft")
        self.finished_gen(self.ds, self.t0)
        self.published("new338", self.t0 + timedelta(hours=1))
        type(self.owner).objects.filter(pk=self.owner.pk).update(role="normal")
        self.assertEqual(tc.refresh_stale_suggestions()["skipped"], [self.ds.pk])
        type(self.owner).objects.filter(pk=self.owner.pk).update(role="beta_tester")
        with override_settings(RECON_DEV_RECORD_LIMIT=None):
            self.assertEqual(tc.refresh_stale_suggestions()["skipped"], [self.ds.pk])
        self.assertEqual(len(self.gens()), 1)
        self.assertEqual(len(tc.refresh_stale_suggestions()["started"]), 1)    # control

    def test_on_the_beat_schedule_daily(self):
        from whg.celery import app
        entry = app.conf.beat_schedule["refresh-stale-suggestions-daily"]
        self.assertEqual(entry["task"], "datasets.refresh_stale_suggestions")
        self.assertEqual(entry["schedule"].hour, {4})
        self.assertIn("datasets.refresh_stale_suggestions", app.tasks)
