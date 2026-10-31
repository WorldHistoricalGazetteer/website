"""The editors' sample check (place#334 Phase 3, ``datasets/qc_sample.py``).

    DB_NAME=whgv3beta_<you> GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test datasets.tests_qc_sample --settings=whg.settings_localtest

No gateway is ever called: ``api.crc_client.crc_search_status`` is the ``GatewayFake`` of
``tests_candidates`` and ``requests`` inside the client fails the test. Every verdict is shown
both ways through the same call (an "ok" beside a "look closer", an answered sample beside an
unavailable one, the editor's report with the check beside the owner's without it).
"""
import json
import re
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import override_settings
from django.urls import reverse

from datasets import qc, qc_sample
from datasets.models import Dataset, DatasetLifecycleEvent
from datasets.tests_candidates import GN_TOWN, CandidateTestBase, hit, response
from datasets.tests_lifecycle_screens import ScreenBase, visible_text
from places.tests_vocabulary import BANNED

User = get_user_model()
# Dev context with an explicit small cap, as dev runs (dev shares production's gateway).
DEV = dict(ENV_CONTEXT="dev-whgazetteer-org", RECON_DEV_RECORD_LIMIT=50, RECON_BG_QPS=2,
           QC_SAMPLE_SIZE=12, RECON_QUERY_RATE=0)


class Clock:
    def __init__(self, step=0.1):
        self.t, self.step = 0.0, step

    def __call__(self):
        self.t += self.step
        return self.t


@override_settings(**DEV)
class SampleBase(CandidateTestBase):
    def setUp(self):
        super().setUp()
        self.editor = User.objects.create_user(username="qs_ed", email="qs_ed@example.org",
                                               password="pw", given_name="E", surname="Ed",
                                               is_staff=True)
        self.sleeps = []

    def towns(self, n, prefix="Town", answer=True):
        recs = [self.place(f"{prefix} {i}", f"{prefix[:2].lower()}{i}", (0.1 * i, 51.0)) for i in range(n)]
        if answer:
            for i, r in enumerate(recs):
                self.answer(r, i)
        return recs

    def answer(self, rec, i, pid=None, title=None):
        self.fake.by_query[rec.title] = response(
            hit(pid or f"gn:{1000 + i}", title or rec.title, (0.1 * i, 51.0005), types=[GN_TOWN]))

    def sample(self):
        return qc_sample.run_sample(self.ds, self.editor, sleep=self.sleeps.append, clock=Clock())

    def submitted(self, state="submitted"):
        Dataset.objects.filter(pk=self.ds.pk).update(lifecycle_state=state)
        self.ds.refresh_from_db()
        return DatasetLifecycleEvent.objects.create(
            dataset=self.ds, dataset_pk=self.ds.pk, dataset_label=self.ds.label,
            from_state="draft", to_state="submitted", actor=self.owner,
            payload={"qc": {"blocking": [], "checks": {}}})


class VerdictTests(SampleBase):
    def test_ok_and_look_closer_through_the_same_call(self):
        recs = self.towns(6)
        result = self.sample()
        check = qc_sample.check_from_result(result)
        self.assertEqual((result["status"], check["status"], check["verdict"]), ("done", "ok", "ok"))
        self.assertIn("6 of 6 records sampled", check["detail"])
        self.assertIn("6 of 6 sampled records have a confident suggestion", check["detail"])
        self.assertIn("6 of 6 sampled records have a location", check["detail"])
        self.assertEqual(len(self.fake.calls), 6)
        # The same dataset, four of whose records the gateway knows nothing about.
        for r in recs[:4]:
            self.fake.by_query.pop(r.title)
        check = qc_sample.check_from_result(self.sample())
        self.assertEqual((check["status"], check["verdict"]), ("warn", "look closer"))
        self.assertIn("2 of 6 sampled records have a confident suggestion", check["detail"])
        self.assertIn("4 nothing suggested", check["detail"])
        self.assertIn("fewer than half have a confident suggestion", check["detail"])
        self.assertEqual({e["place_id"] for e in check["examples"]}, {r.pk for r in recs[:4]})
        self.assertNotIn(check["key"], qc.qc_report(self.ds)["blocking"])  # advice, never a gate

    def test_records_without_a_name_are_counted_not_searched(self):
        recs = self.towns(5)
        result = self.sample()
        self.assertEqual((result["no_name"], qc_sample.verdict(result)[0]), (0, "ok"))
        blank = self.place("  ", "blank1", (1, 51))
        result = self.sample()
        check = qc_sample.check_from_result(result)
        self.assertEqual((result["no_name"], result["answered"], check["verdict"]), (1, 5, "look closer"))
        self.assertNotIn("", [c["qstr"].strip() for c in self.fake.calls])
        self.assertIn("1 have no name and were not searched", check["detail"])
        self.assertIn(blank.pk, [e["place_id"] for e in check["examples"]])
        self.assertTrue(recs)

    def test_many_records_with_one_top_suggestion(self):
        recs = self.towns(6)
        self.assertIsNone(self.sample()["shared_top"])
        for i, r in enumerate(recs[:3]):
            self.answer(r, i, pid="gn:42", title=r.title)
        result = self.sample()
        self.assertEqual(result["shared_top"]["n"], 3)
        check = qc_sample.check_from_result(result)
        self.assertEqual(check["verdict"], "look closer")
        self.assertIn("3 of 6 sampled records have the same top suggestion", check["detail"])

    def test_a_sample_stopped_early_counts_what_it_reached(self):
        result = {"status": "done", "records": 30, "size": 12, "reached": 9, "answered": 6,
                  "unanswered": 6, "no_name": 0, "with_location": 9,
                  "tiers": {"confident": 6, "ambiguous": 0, "weak": 0, "none": 0}}
        detail = qc_sample.check_from_result(result)["detail"]
        self.assertIn("9 of 9 sampled records have a location", detail)
        self.assertIn("6 got no answer", detail)
        self.assertIn("6 of 6 sampled records have a confident suggestion", detail)

    def test_no_link_counts_in_the_result(self):
        """Q11: suggestions only. The dataset's own links never enter the sample."""
        from places.models import PlaceLink
        recs = self.towns(5)
        PlaceLink.objects.create(place=recs[0], jsonb={"type": "closeMatch", "identifier": "wd:Q1"})
        blob = json.dumps(self.sample()) + json.dumps(qc_sample.check_from_result(self.sample()))
        self.assertIn("confident", blob)
        for word in ("links", "records_with_links", "links_public", "wd:Q1"):
            self.assertNotIn(word, blob)


class BoundsTests(SampleBase):
    def test_reproducible_bounded_and_paced(self):
        self.towns(30)
        first = self.sample()
        ids = list(self.fake.calls)
        self.assertEqual((first["size"], first["records"], len(ids)), (12, 30, 12))
        self.assertEqual(len(self.sleeps), 12)                       # one pause per call
        self.fake.calls.clear()
        self.sample()
        self.assertEqual(self.fake.calls, ids)                       # the same twelve, same order
        self.assertEqual(qc_sample.sample_place_ids(self.ds, 12, 30),
                         qc_sample.sample_place_ids(self.ds, 12, 30))
        # One more record: a different seed, a different sample.
        self.towns(1, prefix="Extra")
        self.assertNotEqual(qc_sample.sample_place_ids(self.ds, 12, 31),
                            qc_sample.sample_place_ids(self.ds, 12, 30))
        # 2 q/s, each call 0.1 s on the fake clock: 0.4 s between calls (minus the read).
        self.assertTrue(all(0 < s <= 0.5 for s in self.sleeps), self.sleeps)
        with override_settings(QC_SAMPLE_SIZE=500):
            self.fake.calls.clear()
            self.assertEqual(self.sample()["size"], 31)
            self.assertEqual(len(self.fake.calls), 31)
        self.assertEqual(qc_sample.configured_size(), 12)
        with override_settings(QC_SAMPLE_SIZE=500):
            self.assertEqual(qc_sample.configured_size(), qc_sample.MAX_SIZE)

    def test_dev_guard_is_respected(self):
        self.towns(10)
        with override_settings(RECON_DEV_RECORD_LIMIT=3):
            result = self.sample()
        self.assertEqual((result["status"], result["size"], len(self.fake.calls)), ("done", 3, 3))
        self.fake.calls.clear()
        with override_settings(RECON_DEV_RECORD_LIMIT=None):
            result = self.sample()
        self.assertEqual((result["status"], len(self.fake.calls)), ("unavailable", 0))
        check = qc_sample.check_from_result(result)
        self.assertEqual((check["status"], check["verdict"]), ("not_checked", "sample unavailable"))
        self.assertIn("switched off on this server", check["detail"])

    def test_query_budget_is_charged(self):
        self.towns(6)
        with override_settings(RECON_QUERY_RATE=100), \
                patch("api.throttling.consume_query_budget", return_value=(True, 0)) as budget:
            self.assertEqual(self.sample()["status"], "done")
        budget.assert_called_once_with(self.editor, 6)
        self.fake.calls.clear()
        with patch("api.throttling.consume_query_budget", return_value=(False, 30)):
            result = self.sample()
        self.assertEqual((result["status"], len(self.fake.calls)), ("unavailable", 0))
        self.assertIn("allowance", result["reason"])


class GatewayFailureTests(SampleBase):
    def test_silence_is_not_nothing_and_an_outage_is_unavailable(self):
        recs = self.towns(6)
        self.fake.failures[recs[0].title] = "timeout"
        result = self.sample()
        check = qc_sample.check_from_result(result)
        self.assertEqual((result["status"], result["answered"], result["unanswered"]), ("done", 5, 1))
        self.assertEqual(result["tiers"]["none"], 0)                 # the timeout is not "nothing"
        self.assertIn("1 got no answer from the search service", check["detail"])
        self.assertIn("5 of 5 sampled records have a confident suggestion", check["detail"])
        # The gateway down: three silences in a row end the sample; no verdict.
        for r in recs:
            self.fake.failures[r.title] = "unreachable"
        self.fake.calls.clear()
        result = self.sample()
        check = qc_sample.check_from_result(result)
        self.assertEqual(len(self.fake.calls), qc_sample.SILENCE_STREAK)
        self.assertEqual((result["reached"], result["unanswered"]), (3, 6))  # the rest not reached
        self.assertEqual((result["status"], check["status"], check["verdict"]),
                         ("unavailable", "not_checked", "sample unavailable"))
        self.assertTrue(check["detail"].startswith("Sample unavailable"))

    def test_too_few_answers_give_no_verdict(self):
        recs = self.towns(6)
        with patch.object(qc_sample, "SILENCE_STREAK", 99):         # silences need not be in a row
            self.fake.failures[recs[0].title] = "timeout"
            self.assertEqual(self.sample()["answered"], 5)
            self.assertEqual(self.sample()["status"], "done")        # 5 answered: enough
            self.fake.failures[recs[1].title] = "timeout"
            result = self.sample()
        self.assertEqual((result["answered"], result["status"]), (4, "unavailable"))
        self.assertEqual(qc_sample.check_from_result(result)["verdict"], "sample unavailable")

    def test_a_crash_is_stored_as_unavailable(self):
        self.towns(6)
        ev = self.submitted()
        self.assertEqual(qc_sample.take_and_store(ev.pk, self.editor.pk, sleep=self.sleeps.append), "done")
        with patch("datasets.qc_sample.run_sample", side_effect=RuntimeError("boom")):
            self.assertEqual(qc_sample.take_and_store(ev.pk, self.editor.pk), "unavailable")
        ev.refresh_from_db()
        self.assertEqual(ev.payload[qc_sample.PAYLOAD_KEY]["status"], "unavailable")
        self.assertIn("qc", ev.payload)                              # the submission summary kept


class StorageTests(SampleBase):
    def setUp(self):
        super().setUp()
        self.queued = []
        self.enqueue = lambda ev, user: self.queued.append((ev, user))

    def check(self, user="editor"):
        with self.captureOnCommitCallbacks(execute=True):
            return qc_sample.sample_check(self.ds, requested_by=self.editor if user == "editor" else user,
                                          enqueue=self.enqueue)

    def test_lazy_queued_once_then_read_and_rerun_when_records_change(self):
        self.towns(6)
        ev = self.submitted()
        first = self.check()
        self.assertEqual((first["status"], first["verdict"]), ("not_checked", "being prepared"))
        self.assertEqual(self.queued, [(ev.pk, self.editor.pk)])
        self.assertEqual(self.fake.calls, [])                        # the page never searches
        self.check()
        self.assertEqual(len(self.queued), 1)                        # running: not queued again
        qc_sample.take_and_store(*self.queued[0], sleep=self.sleeps.append)
        done = self.check()
        self.assertEqual((done["status"], done["verdict"]), ("ok", "ok"))
        self.assertEqual(len(self.queued), 1)
        calls = len(self.fake.calls)
        self.check()
        self.assertEqual(len(self.fake.calls), calls)                # cached: no new searches
        self.towns(1, prefix="Late")                                 # 7 records now
        self.assertEqual(self.check()["verdict"], "being prepared")
        self.assertEqual(len(self.queued), 2)

    def test_queued_only_for_an_editor_and_a_dataset_waiting_for_review(self):
        self.towns(6)
        self.assertIn("submitted for review", self.check()["detail"])  # no submission yet
        self.submitted(state="draft")
        self.assertEqual(self.check()["verdict"], "sample unavailable")
        self.assertEqual(self.queued, [])
        Dataset.objects.filter(pk=self.ds.pk).update(lifecycle_state="in_review")
        self.ds.refresh_from_db()
        self.assertEqual(self.check(user=None)["verdict"], "sample unavailable")
        self.assertEqual(self.queued, [])
        self.assertEqual(self.check()["verdict"], "being prepared")
        self.assertEqual(len(self.queued), 1)

    def test_a_stale_running_marker_is_queued_again(self):
        self.towns(6)
        ev = self.submitted()
        self.check()
        self.assertEqual(len(self.queued), 1)
        payload = dict(ev.__class__.objects.get(pk=ev.pk).payload)
        payload[qc_sample.PAYLOAD_KEY]["started_at"] = "2000-01-01T00:00:00"
        DatasetLifecycleEvent.objects.filter(pk=ev.pk).update(payload=payload)
        self.check()
        self.assertEqual(len(self.queued), 2)

    def test_only_one_claim_wins(self):
        """Two editors' pages at once: the second finds the sample already claimed."""
        self.towns(6)
        ev = self.submitted()
        self.assertTrue(qc_sample._claim(ev.pk, 6))
        self.assertFalse(qc_sample._claim(ev.pk, 6))
        self.assertFalse(qc_sample._claim(ev.pk, 7))                 # running wins until stale

    def test_broker_down_reads_as_unavailable(self):
        self.towns(6)
        ev = self.submitted()
        with patch("datasets.tasks_qc.qc_gateway_sample.apply_async", side_effect=OSError("no broker")):
            qc_sample._enqueue(ev.pk, self.editor.pk)
        ev.refresh_from_db()
        self.assertEqual(ev.payload[qc_sample.PAYLOAD_KEY]["status"], "unavailable")
        self.assertEqual(qc_sample.sample_check(self.ds, self.editor, enqueue=self.enqueue)["verdict"],
                         "sample unavailable")
        with patch("datasets.tasks_qc.qc_gateway_sample.apply_async") as ok:
            DatasetLifecycleEvent.objects.filter(pk=ev.pk).update(payload={})
            qc_sample._enqueue(ev.pk, self.editor.pk)
        ok.assert_called_once_with(args=[ev.pk, self.editor.pk], queue="recon")


@override_settings(**DEV)
class EditorsOnlyTests(ScreenBase):
    """The check is in the editors' report and page, never in the owner's (Q11 side: the
    owner's panel carries link counts; the sample is review advice for editors)."""

    def setUp(self):
        super().setUp()
        p = patch("datasets.qc_sample._enqueue")
        self.enqueue = p.start()
        self.addCleanup(p.stop)
        q = patch("datasets.qc_links._enqueue")              # the link check queues its own
        q.start()
        self.addCleanup(q.stop)

    def keys(self, user):
        self.client.force_login(user)
        r = self.client.get(reverse("datasets:lifecycle", args=[self.ds.pk]))
        self.assertEqual(r.status_code, 200, r.content)
        return [c["key"] for c in r.json()["checks"]["checks"]], r.json()["checks"]

    def test_editor_sees_the_sample_and_the_owner_does_not(self):
        self.set_state("submitted")
        DatasetLifecycleEvent.objects.create(dataset=self.ds, dataset_pk=self.ds.pk,
                                             dataset_label=self.ds.label, to_state="submitted",
                                             payload={})
        ed_keys, ed = self.keys(self.editor)
        own_keys, own = self.keys(self.owner)
        self.assertIn("gateway_sample", ed_keys)
        self.assertNotIn("gateway_sample", own_keys)
        self.assertEqual([k for k in ed_keys if k != "gateway_sample"], own_keys)
        self.assertIn("links", own)
        self.assertNotIn("links", ed)
        # Asked for directly, the owner's report refuses the sample too; the editors' has it.
        self.assertNotIn("gateway_sample", [c["key"] for c in qc.qc_report(
            self.ds, audience="owner", sample_for=self.editor)["checks"]])
        self.assertIn("gateway_sample", [c["key"] for c in qc.qc_report(
            self.ds, audience="editor", sample_for=self.editor)["checks"]])
        # The summary kept on a submission never carries the sample either.
        self.assertNotIn("gateway_sample", qc.summary(qc.qc_report(self.ds))["checks"])

    def test_review_page_shows_the_verdict_in_plain_words(self):
        self.set_state("in_review")
        ev = DatasetLifecycleEvent.objects.create(dataset=self.ds, dataset_pk=self.ds.pk,
                                                  dataset_label=self.ds.label, to_state="submitted",
                                                  payload={})
        html = self.queue()
        self.assertIn('data-check="gateway_sample"', html)
        self.assertIn("being prepared", html)
        result = {"status": "done", "records": 0, "size": 4, "reached": 4, "answered": 4,
                  "unanswered": 0, "no_name": 0, "with_location": 1,
                  "tiers": {"confident": 1, "ambiguous": 1, "weak": 1, "none": 1},
                  "shared_top": None, "examples_nothing": [], "examples_no_name": []}
        DatasetLifecycleEvent.objects.filter(pk=ev.pk).update(payload={qc_sample.PAYLOAD_KEY: result})
        html = self.queue()
        m = re.search(r'<li class="mb-1" data-check="gateway_sample".*?</li>', html, re.S)
        self.assertIsNotNone(m)
        text = visible_text(m.group(0))
        self.assertIn("look closer", text)
        self.assertIn("1 of 4 sampled records have a confident suggestion", text)
        self.assertIsNone(BANNED.search(text))
        self.assertIsNone(BANNED.search(visible_text(html)))
