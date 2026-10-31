"""The editors' link check (place#334 §4e, place#340; ``datasets/qc_links.py``).

    DB_NAME=whgv3beta_<you> GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test datasets.tests_qc_links --settings=whg.settings_localtest

No gateway is ever called: ``api.crc_client.crc_fetch_places`` is ``LinkGateway`` below and
``requests`` inside the client fails the test (``CandidateTestBase``). Every verdict is shown
both ways through the same call (an "ok" beside a "look closer", an answered sample beside an
unavailable one, a namespace asked about beside the same identifiers skipped, the editors' report
with the check beside the owner's with the placeholder).
"""
import json
import re
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import override_settings
from django.urls import reverse

from datasets import qc, qc_links, qc_sample
from datasets.models import Dataset, DatasetLifecycleEvent
from datasets.tests_candidates import CandidateTestBase
from datasets.tests_lifecycle_screens import ScreenBase, visible_text
from places.tests_vocabulary import BANNED

User = get_user_model()
DEV = dict(ENV_CONTEXT="dev-whgazetteer-org", RECON_DEV_RECORD_LIMIT=50, RECON_BG_QPS=2,
           QC_LINK_SAMPLE_SIZE=50, RECON_QUERY_RATE=0)
HELD = ("gn", "wd", "tgn", "pl")


class LinkGateway:
    """Answers ``crc_fetch_places`` from a set of known ids; records every batch."""

    def __init__(self):
        self.known, self.failing, self.calls, self.disabled = set(), set(), [], False

    def __call__(self, ids, user=None, allow_anonymous=False, meta=None):
        self.calls.append(list(ids))
        if self.disabled:
            meta["disabled"] = True
            return {}
        if self.failing & set(ids):
            meta["error"] = "timeout"
            return {}
        return {i: {"place_id": i, "title": i} for i in ids if i in self.known}


class Clock:
    def __init__(self, step=0.1):
        self.t, self.step = 0.0, step

    def __call__(self):
        self.t += self.step
        return self.t


def registry():
    from api.models import GazetteerRegistryEntry
    for ns in HELD:
        GazetteerRegistryEntry.objects.update_or_create(
            id=ns, defaults=dict(name=ns, namespace=ns, entry_class="authority", status="published"))
    GazetteerRegistryEntry.objects.filter(namespace="viaf").delete()


@override_settings(**DEV)
class LinkBase(CandidateTestBase):
    def setUp(self):
        super().setUp()
        registry()
        self.editor = User.objects.create_user(username="ql_ed", email="ql_ed@example.org",
                                               password="pw", given_name="E", surname="Ed",
                                               is_staff=True)
        self.gw = LinkGateway()
        p = patch("api.crc_client.crc_fetch_places", side_effect=self.gw)
        p.start()
        self.addCleanup(p.stop)
        self.sleeps = []
        self.n = 0

    def linked(self, *identifiers, known=True):
        """One record carrying ``identifiers``; the gateway knows them unless ``known`` is False."""
        from places.models import PlaceLink
        self.n += 1
        p = self.place(f"Town {self.n}", f"t{self.n}", (0.1, 51.0))
        for ident in identifiers:
            PlaceLink.objects.create(place=p, jsonb={"type": "closeMatch", "identifier": ident})
            if known:
                from places.concordance import normalise_identifier
                self.gw.known.add(normalise_identifier(ident)[0])
        return p

    def gn_links(self, n, start=1000, known=True):
        return [self.linked(f"gn:{start + i}", known=known) for i in range(n)]

    def sample(self):
        return qc_links.run_sample(self.ds, self.editor, sleep=self.sleeps.append, clock=Clock())

    def asked(self):
        return [i for batch in self.gw.calls for i in batch]

    def submitted(self, state="submitted"):
        Dataset.objects.filter(pk=self.ds.pk).update(lifecycle_state=state)
        self.ds.refresh_from_db()
        return DatasetLifecycleEvent.objects.create(
            dataset=self.ds, dataset_pk=self.ds.pk, dataset_label=self.ds.label,
            from_state="draft", to_state="submitted", actor=self.owner,
            payload={"qc": {"blocking": [], "checks": {}}})


class VerdictTests(LinkBase):
    def test_ok_and_look_closer_through_the_same_call(self):
        recs = self.gn_links(10)
        result = self.sample()
        check = qc_links.check_from_result(result)
        self.assertEqual((result["status"], check["status"], check["verdict"]), ("done", "ok", "ok"))
        self.assertIn("10 link identifiers sampled", check["detail"])
        self.assertIn("10 of 10 sampled link identifiers resolve", check["detail"])
        self.assertIn("not yet calibrated", check["detail"])
        self.assertEqual(check["examples"], [])
        self.assertEqual(sorted(self.asked()), sorted(f"gn:{1000 + i}" for i in range(10)))
        # The same dataset; the gateway no longer knows two of its identifiers: 8 of 10 < 90%.
        self.gw.known -= {"gn:1000", "gn:1001"}
        check = qc_links.check_from_result(self.sample())
        self.assertEqual((check["status"], check["verdict"]), ("warn", "look closer"))
        self.assertIn("8 of 10 sampled link identifiers resolve", check["detail"])
        self.assertIn("fewer than 90% resolve", check["detail"])
        self.assertEqual({e["identifier"] for e in check["examples"]}, {"gn:1000", "gn:1001"})
        self.assertEqual({e["place_id"] for e in check["examples"]}, {recs[0].pk, recs[1].pk})
        self.assertNotEqual(check["status"], "fail")
        self.assertNotIn(check["key"], qc.qc_report(self.ds)["blocking"])  # advice, never a gate

    def test_exactly_ninety_percent_is_ok_and_one_less_is_not(self):
        self.gn_links(10)
        self.gw.known.discard("gn:1000")
        self.assertEqual(qc_links.check_from_result(self.sample())["verdict"], "ok")      # 9 of 10
        self.gw.known.discard("gn:1001")
        self.assertEqual(qc_links.check_from_result(self.sample())["verdict"], "look closer")

    def test_placeholders_left_out_unheld_namespaces_counted_url_forms_merged(self):
        self.linked("gn:1", "https://www.wikidata.org/wiki/Q90")
        self.linked("wd:Q90", "tgn:7010731", "pleiades:579885")
        self.linked("viaf:123", "whg:5", "wd:None", "gn:null", "", known=False)
        result = self.sample()
        asked = set(self.asked())
        # Asked: the held namespaces, normalised (pleiades -> pl; the URL is wd:Q90, once).
        self.assertEqual(asked, {"gn:1", "wd:Q90", "tgn:7010731", "pl:579885"})
        self.assertEqual((result["size"], result["asked"], result["skipped"]), (6, 4, 2))
        self.assertEqual(result["skipped_namespaces"], ["viaf", "whg"])
        check = qc_links.check_from_result(result)
        self.assertIn("2 of 6 are in namespaces the gazetteer service does not hold (viaf, whg)",
                      check["detail"])
        self.assertIn("4 of 4 sampled link identifiers resolve", check["detail"])
        for placeholder in ("wd:None", "gn:null"):
            self.assertNotIn(placeholder, json.dumps(result))
        # The same identifiers ARE asked about once the service holds their namespace.
        from api.models import GazetteerRegistryEntry
        GazetteerRegistryEntry.objects.create(id="viaf", name="viaf", namespace="viaf",
                                              entry_class="authority", status="published")
        self.gw.calls.clear()
        result = self.sample()
        self.assertIn("viaf:123", self.asked())
        self.assertEqual((result["asked"], result["skipped"], result["resolved"]), (5, 1, 4))

    def test_nothing_to_check(self):
        check = qc_links.check_from_result(self.sample())
        self.assertEqual((check["status"], check["verdict"]), ("not_checked", "nothing to check"))
        self.assertEqual(self.gw.calls, [])
        self.linked("viaf:1", known=False)
        check = qc_links.check_from_result(self.sample())
        self.assertEqual(check["verdict"], "nothing to check")
        self.assertIn("1 of 1 are in namespaces", check["detail"])
        self.assertEqual(self.gw.calls, [])
        self.linked("gn:7")
        self.assertEqual(qc_links.check_from_result(self.sample())["verdict"], "ok")

    def test_no_link_totals_in_the_result_or_the_text(self):
        """Q11: the sample's own size is the only denominator; the dataset's 73 links are not
        stated anywhere, though they seed the sample."""
        self.gn_links(73)
        result = self.sample()
        check = qc_links.check_from_result(result)
        self.assertEqual(result["size"], 50)
        self.assertIn("50 of 50 sampled link identifiers resolve", check["detail"])
        blob = json.dumps({k: v for k, v in result.items() if k != "fingerprint"}) + json.dumps(check)
        self.assertNotIn("73", blob)
        for word in ("links_public", "records_with_links", "n_links", "total"):
            self.assertNotIn(word, blob)


class BoundsTests(LinkBase):
    def test_reproducible_bounded_batched_and_paced(self):
        self.gn_links(60)
        first = self.sample()
        asked = self.asked()
        self.assertEqual((first["size"], len(asked)), (50, 50))
        self.assertEqual([len(b) for b in self.gw.calls], [10] * 5)
        self.assertEqual(len(self.sleeps), 4)                    # between batches, not after
        # 2 identifiers/s, ten per request: ~5 s between requests, less the time taken.
        self.assertTrue(all(4.5 < s <= 5.0 for s in self.sleeps), self.sleeps)
        self.gw.calls.clear()
        self.sample()
        self.assertEqual(self.asked(), asked)                     # the same fifty, same order
        # One more link: a different seed, a different sample, a different fingerprint.
        fp = qc_links.fingerprint(self.ds)
        self.gn_links(1, start=5000)
        self.gw.calls.clear()
        self.sample()
        self.assertNotEqual(self.asked(), asked)
        self.assertNotEqual(qc_links.fingerprint(self.ds), fp)
        with override_settings(QC_LINK_SAMPLE_SIZE=500):
            self.assertEqual(qc_links.configured_size(), qc_links.MAX_SIZE)
        with override_settings(QC_LINK_SAMPLE_SIZE=7):
            self.gw.calls.clear()
            self.assertEqual(self.sample()["size"], 7)
            self.assertEqual(len(self.asked()), 7)

    def test_dev_guard_is_respected(self):
        self.gn_links(10)
        with override_settings(RECON_DEV_RECORD_LIMIT=3):
            result = self.sample()
        self.assertEqual((result["status"], result["size"], len(self.asked())), ("done", 3, 3))
        self.gw.calls.clear()
        with override_settings(RECON_DEV_RECORD_LIMIT=None):
            result = self.sample()
        self.assertEqual((result["status"], self.gw.calls), ("unavailable", []))
        check = qc_links.check_from_result(result)
        self.assertEqual((check["status"], check["verdict"]), ("not_checked", "sample unavailable"))
        self.assertIn("switched off on this server", check["detail"])

    def test_query_budget_is_charged_once_for_the_identifiers_asked(self):
        self.gn_links(6)
        self.linked("viaf:1", known=False)
        with patch("api.throttling.consume_query_budget", return_value=(True, 0)) as budget:
            self.assertEqual(self.sample()["status"], "done")
        budget.assert_called_once_with(self.editor, 6)            # the skipped one is not charged
        self.gw.calls.clear()
        with patch("api.throttling.consume_query_budget", return_value=(False, 30)):
            result = self.sample()
        self.assertEqual((result["status"], self.gw.calls), ("unavailable", []))
        self.assertIn("allowance", result["reason"])


class GatewayFailureTests(LinkBase):
    def test_silence_is_not_unresolved_and_an_outage_is_unavailable(self):
        self.gn_links(30)
        first = self.sample()
        self.assertEqual((first["answered"], first["unanswered"]), (30, 0))
        # One batch times out: its ten identifiers are unanswered, not "does not resolve".
        self.gw.failing = set(self.gw.calls[0])
        self.gw.calls.clear()
        result = self.sample()
        check = qc_links.check_from_result(result)
        self.assertEqual((result["status"], result["answered"], result["unanswered"],
                          result["resolved"]), ("done", 20, 10, 20))
        self.assertEqual(check["verdict"], "ok")
        self.assertIn("10 of the 30 asked about got no answer", check["detail"])
        self.assertIn("20 of 20 sampled link identifiers resolve", check["detail"])
        # The service down: two silent batches in a row end the sample; no verdict.
        self.gw.failing = {f"gn:{1000 + i}" for i in range(30)}
        self.gw.calls.clear()
        result = self.sample()
        check = qc_links.check_from_result(result)
        self.assertEqual(len(self.gw.calls), qc_links.SILENCE_STREAK)
        self.assertEqual((result["status"], result["unanswered"]), ("unavailable", 30))
        self.assertEqual((check["status"], check["verdict"]), ("not_checked", "sample unavailable"))
        self.assertTrue(check["detail"].startswith("Sample unavailable"))

    def test_service_not_configured_is_unavailable_not_unresolved(self):
        self.gn_links(6)
        self.assertEqual(self.sample()["status"], "done")
        self.gw.disabled = True
        result = self.sample()
        self.assertEqual(result["status"], "unavailable")
        self.assertIn("not available to this server", result["reason"])

    def test_a_crash_is_stored_as_unavailable(self):
        self.gn_links(6)
        ev = self.submitted()
        self.assertEqual(qc_links.take_and_store(ev.pk, self.editor.pk, sleep=self.sleeps.append), "done")
        ev.refresh_from_db()
        self.assertEqual(ev.payload[qc_links.PAYLOAD_KEY]["status"], "done")
        with patch("datasets.qc_links.run_sample", side_effect=RuntimeError("boom")):
            self.assertEqual(qc_links.take_and_store(ev.pk, self.editor.pk), "unavailable")
        ev.refresh_from_db()
        self.assertEqual(ev.payload[qc_links.PAYLOAD_KEY]["status"], "unavailable")
        self.assertIn("qc", ev.payload)                           # the submission summary kept


class StorageTests(LinkBase):
    def setUp(self):
        super().setUp()
        self.queued = []
        self.enqueue = lambda ev, user: self.queued.append((ev, user))

    def check(self, user="editor"):
        with self.captureOnCommitCallbacks(execute=True):
            return qc_links.link_check(self.ds, requested_by=self.editor if user == "editor" else user,
                                       enqueue=self.enqueue)

    def test_lazy_queued_once_then_read_and_rerun_when_links_change(self):
        self.gn_links(6)
        ev = self.submitted()
        first = self.check()
        self.assertEqual((first["status"], first["verdict"]), ("not_checked", "being prepared"))
        self.assertEqual(self.queued, [(ev.pk, self.editor.pk)])
        self.assertEqual(self.gw.calls, [])                       # the page never asks
        self.check()
        self.assertEqual(len(self.queued), 1)                     # running: not queued again
        qc_links.take_and_store(*self.queued[0], sleep=self.sleeps.append)
        done = self.check()
        self.assertEqual((done["status"], done["verdict"]), ("ok", "ok"))
        calls = len(self.gw.calls)
        self.check()
        self.assertEqual((len(self.gw.calls), len(self.queued)), (calls, 1))  # cached
        self.linked("gn:9999")                                    # one more link
        self.assertEqual(self.check()["verdict"], "being prepared")
        self.assertEqual(len(self.queued), 2)
        # The sample check's result on the same event is untouched by this one.
        qc_sample.store(ev.pk, {"status": "done", "marker": 1})
        qc_links.store(ev.pk, {"status": "unavailable", "reason": "x"})
        ev.refresh_from_db()
        self.assertEqual(ev.payload[qc_sample.PAYLOAD_KEY]["marker"], 1)

    def test_queued_only_for_an_editor_and_a_dataset_waiting_for_review(self):
        self.gn_links(6)
        self.assertIn("submitted for review", self.check()["detail"])
        self.submitted(state="draft")
        self.assertEqual(self.check()["verdict"], "sample unavailable")
        self.assertEqual(self.queued, [])
        Dataset.objects.filter(pk=self.ds.pk).update(lifecycle_state="in_review")
        self.ds.refresh_from_db()
        self.assertEqual(self.check(user=None)["verdict"], "sample unavailable")
        self.assertEqual(self.queued, [])
        self.assertEqual(self.check()["verdict"], "being prepared")
        self.assertEqual(len(self.queued), 1)

    def test_only_one_claim_wins_and_a_stale_one_is_retaken(self):
        self.gn_links(6)
        ev = self.submitted()
        fp = qc_links.fingerprint(self.ds)
        self.assertTrue(qc_links._claim(ev.pk, fp))
        self.assertFalse(qc_links._claim(ev.pk, fp))
        payload = dict(DatasetLifecycleEvent.objects.get(pk=ev.pk).payload)
        payload[qc_links.PAYLOAD_KEY]["started_at"] = "2000-01-01T00:00:00"
        DatasetLifecycleEvent.objects.filter(pk=ev.pk).update(payload=payload)
        self.assertTrue(qc_links._claim(ev.pk, fp))

    def test_broker_down_reads_as_unavailable(self):
        self.gn_links(6)
        ev = self.submitted()
        with patch("datasets.tasks_qc.qc_link_resolution.apply_async", side_effect=OSError("no broker")):
            qc_links._enqueue(ev.pk, self.editor.pk)
        ev.refresh_from_db()
        self.assertEqual(ev.payload[qc_links.PAYLOAD_KEY]["status"], "unavailable")
        self.assertEqual(qc_links.link_check(self.ds, self.editor, enqueue=self.enqueue)["verdict"],
                         "sample unavailable")
        with patch("datasets.tasks_qc.qc_link_resolution.apply_async") as ok:
            qc_links._enqueue(ev.pk, self.editor.pk)
        ok.assert_called_once_with(args=[ev.pk, self.editor.pk], queue="recon")


@override_settings(**DEV)
class EditorsOnlyTests(ScreenBase):
    """The real check is in the editors' report; the owner's keeps the placeholder."""

    def setUp(self):
        super().setUp()
        for target in ("datasets.qc_sample._enqueue", "datasets.qc_links._enqueue"):
            p = patch(target)
            p.start()
            self.addCleanup(p.stop)

    def report(self, user):
        self.client.force_login(user)
        with self.captureOnCommitCallbacks(execute=True):
            r = self.client.get(reverse("datasets:lifecycle", args=[self.ds.pk]))
        self.assertEqual(r.status_code, 200, r.content)
        return r.json()["checks"]

    def test_editor_gets_the_check_and_the_owner_the_placeholder(self):
        self.set_state("submitted")
        DatasetLifecycleEvent.objects.create(dataset=self.ds, dataset_pk=self.ds.pk,
                                             dataset_label=self.ds.label, to_state="submitted",
                                             payload={})
        ed, own = self.report(self.editor), self.report(self.owner)
        ed_link = next(c for c in ed["checks"] if c["key"] == "link_resolution")
        own_link = next(c for c in own["checks"] if c["key"] == "link_resolution")
        self.assertEqual(ed_link["verdict"], "being prepared")
        self.assertNotIn("verdict", own_link)
        self.assertEqual(own_link["detail"], "Not checked automatically yet.")
        self.assertEqual([c["key"] for c in ed["checks"] if c["key"] != "gateway_sample"],
                         [c["key"] for c in own["checks"]])
        self.assertEqual(qc_links._enqueue.call_count, 1)
        # Asked for directly with an editor, the owner's report still keeps the placeholder.
        for audience, verdict in (("editor", "being prepared"), ("owner", None)):
            link = next(c for c in qc.qc_report(self.ds, audience=audience, sample_for=self.editor)
                        ["checks"] if c["key"] == "link_resolution")
            self.assertEqual(link.get("verdict"), verdict, audience)
        # The summary kept on a submission keeps the placeholder.
        summ = qc.summary(qc.qc_report(self.ds))
        self.assertEqual(summ["checks"]["link_resolution"], ["not_checked", None])

    def test_review_page_shows_identifiers_and_the_verdict_in_plain_words(self):
        self.set_state("in_review")
        ev = DatasetLifecycleEvent.objects.create(dataset=self.ds, dataset_pk=self.ds.pk,
                                                  dataset_label=self.ds.label, to_state="submitted",
                                                  payload={})
        self.assertIn("being prepared", self.queue())
        place = self.ds.places.first() or self.make_place()
        result = {"status": "done", "fingerprint": qc_links.fingerprint(self.ds), "size": 10,
                  "asked": 10, "answered": 10, "resolved": 7, "unanswered": 0, "skipped": 0,
                  "skipped_namespaces": [],
                  "examples_unresolved": [{"identifier": "gn:404", "place_id": place.pk}]}
        DatasetLifecycleEvent.objects.filter(pk=ev.pk).update(payload={qc_links.PAYLOAD_KEY: result})
        html = self.queue()
        m = re.search(r'<li class="mb-1" data-check="link_resolution".*?</li>', html, re.S)
        self.assertIsNotNone(m)
        self.assertIn("<code>gn:404</code>", m.group(0))
        text = visible_text(m.group(0))
        self.assertIn("look closer", text)
        self.assertIn("7 of 10 sampled link identifiers resolve", text)
        self.assertIsNone(BANNED.search(text))
        self.assertIsNone(BANNED.search(visible_text(html)))

    def make_place(self):
        from places.models import Place
        return Place.objects.create(title="X", src_id="x1", dataset=self.ds, ccodes=[])
