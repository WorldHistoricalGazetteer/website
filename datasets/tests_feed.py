"""place#338: the contributor feed's JSON and the dataset page's Links section.

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test datasets.tests_feed --settings=whg.settings_localtest

The gateway is the ``GatewayFake`` of ``tests_candidates`` (``requests`` in the client fails the
test). Every Link goes through ``api.attestations.create``; the refusals are shown beside the same
request succeeding.
"""
import json

from django.contrib.auth import get_user_model
from django.template import Context, Template
from django.test import Client, RequestFactory, override_settings

from api.models import ContributorAttestation
from datasets import candidates
from datasets.models_matching import MatchCandidate, RecordReviewState
from datasets.tests_candidates import CandidateTestBase, GN_TOWN, hit, response


class FeedTestBase(CandidateTestBase):
    def setUp(self):
        super().setUp()
        self.gen = self.generation()
        self.ware = self.place("Ware", "f1", (0.03, 51.81))
        self.brox = self.place("Broxbourne", "f2", (-0.02, 51.75))
        self.fake.by_query["Ware"] = response(hit("gn:4", "Ware", (0.03, 51.811), types=[GN_TOWN]))
        self.fake.by_query["Broxbourne"] = response(
            hit("gn:6", "Broxbourne", (-0.02, 51.751), types=[GN_TOWN]),
            hit("wd:Q7", "Broxbourne", (-0.02, 51.752), types=[GN_TOWN]))
        candidates.record_candidates(self.ware, self.gen, self.owner)
        candidates.record_candidates(self.brox, self.gen, self.owner)
        self.c = Client()
        self.c.force_login(self.owner)
        self.base = f"/datasets/{self.ds.pk}/matches/"

    def post(self, body, client=None):
        return (client or self.c).post(self.base + "review/", json.dumps(body), content_type="application/json")

    def cand(self, place, cid):
        return MatchCandidate.objects.get(place=place, candidate_id=cid, generation=self.gen)


class FeedAccessTests(FeedTestBase):
    def test_owner_with_beta_sees_the_feed_and_others_get_404(self):
        User = get_user_model()
        self.assertEqual(self.c.get(self.base).status_code, 200)
        self.assertEqual(self.c.get(self.base + "candidates/?tier=confident").status_code, 200)
        stranger = User.objects.create_user(username="feed_str", email="fs@example.org", password="pw",
                                            given_name="F", surname="S", role="beta_tester")
        nobeta = User.objects.create_user(username="feed_nb", email="fnb@example.org", password="pw",
                                          given_name="F", surname="N")
        from datasets.models import DatasetUser
        DatasetUser.objects.create(dataset_id=self.ds, user_id=nobeta, role="member")   # in the circle, no beta
        self.assertTrue(self.ds.user_has_private_access(nobeta))
        for u in (stranger, nobeta):
            c = Client()
            c.force_login(u)
            for url in ("", "candidates/?tier=confident", "progress/"):
                self.assertEqual(c.get(self.base + url).status_code, 404, (u.username, url))
            self.assertEqual(self.post({"place": self.ware.pk, "action": "later"}, c).status_code, 404)
        self.assertEqual(RecordReviewState.objects.get(place=self.ware).state, "unreviewed")


class FeedListTests(FeedTestBase):
    def test_tabs_list_records_with_their_suggestions(self):
        d = self.c.get(self.base + "candidates/?tier=confident").json()
        self.assertEqual([r["title"] for r in d["records"]], ["Ware"])
        self.assertEqual(d["records"][0]["candidates"][0]["candidate_id"], "gn:4")
        self.assertIn("exact name", d["records"][0]["candidates"][0]["explanation"]["sentence"])
        d = self.c.get(self.base + "candidates/?tier=ambiguous").json()
        self.assertEqual([r["title"] for r in d["records"]], ["Broxbourne"])
        self.assertEqual([c["candidate_id"] for c in d["records"][0]["candidates"]], ["gn:6", "wd:Q7"])
        self.assertEqual((d["counts"]["confident"], d["counts"]["ambiguous"], d["counts"]["none"]), (1, 1, 0))
        self.assertEqual(self.c.get(self.base + "candidates/?tier=bogus").status_code, 400)

    def test_page_opens_on_the_first_non_empty_tab(self):
        r = self.c.get(self.base)
        self.assertEqual(r.context["tab"], "confident")
        self.assertContains(r, "Likely the same place")
        self.assertContains(r, "match_feed.bundle.js")
        feed = r.content.decode().split('id="match_feed"', 1)[1]
        for word in ("ttest", "ccession", "econcil"):      # no forbidden vocabulary in the feed
            self.assertNotIn(word, feed.lower())


class FeedReviewTests(FeedTestBase):
    def test_same_place_writes_a_link_through_the_attestation_rules(self):
        c = self.cand(self.ware, "gn:4")
        r = self.post({"place": self.ware.pk, "action": "link", "candidates": [c.pk], "note": "parish church"})
        self.assertEqual(r.status_code, 200, r.content)
        row = ContributorAttestation.objects.get(pk=r.json()["attestations"][0])
        self.assertEqual((row.relation_type, row.suggested_by, row.candidate_id, row.user_id),
                         ("closeMatch", "candidate", c.pk, self.owner.pk))
        self.assertEqual(row.status, "pending")          # the dataset is private: never forwarded
        self.assertEqual(row.evidence["tier"], "confident")
        self.assertEqual(row.justification, "parish church")
        self.assertEqual({row.place_a, row.place_b}, {"gn:4", f"whg:{self.ds.pk}:f1"})
        c.refresh_from_db()
        self.assertEqual(c.status, "accepted")
        self.assertEqual(RecordReviewState.objects.get(place=self.ware).state, "linked")
        self.assertEqual(r.json()["counts"]["confident"], 0)

    def test_exact_under_advanced_and_no_other_relation(self):
        c = self.cand(self.ware, "gn:4")
        r = self.post({"place": self.ware.pk, "action": "link", "candidates": [c.pk], "relation": "distinct"})
        self.assertEqual(r.status_code, 400)
        r = self.post({"place": self.ware.pk, "action": "link", "candidates": [c.pk], "relation": "exactMatch"})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(ContributorAttestation.objects.get().relation_type, "exactMatch")

    def test_a_link_the_rules_refuse_is_refused_here_too(self):
        """A suggestion naming a record of a dataset the user cannot see: 404 from
        ``attestations.create``, beside the same request for a public one succeeding."""
        other_user = get_user_model().objects.create_user(username="feed_o", email="fo@example.org",
                                                          password="pw", given_name="O", surname="U")
        type(self.hidden).objects.filter(pk=self.hidden.pk).update(owner=other_user)   # no save signals
        self.place("Ware", "x1", (0.03, 51.81), ds=self.hidden)
        self.place("Ware", "o1", (0.03, 51.81), ds=self.other)
        mk = lambda cid: MatchCandidate.objects.create(  # noqa: E731
            dataset=self.ds, place=self.ware, subject_id=f"whg:{self.ds.pk}:f1", candidate_id=cid,
            tier="ambiguous", rank=9, generation=self.gen)
        bad, good = mk(f"whg:{self.hidden.pk}:x1"), mk(f"whg:{self.other.pk}:o1")
        r = self.post({"place": self.ware.pk, "action": "link", "candidates": [bad.pk]})
        self.assertEqual(r.status_code, 404)
        self.assertFalse(ContributorAttestation.objects.exists())
        bad.refresh_from_db()
        self.assertEqual(bad.status, "suggested")
        r = self.post({"place": self.ware.pk, "action": "link", "candidates": [good.pk]})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(ContributorAttestation.objects.count(), 1)

    def test_daily_cap_applies(self):
        c = self.cand(self.ware, "gn:4")
        with override_settings(CONTRIBUTOR_ATTESTATION_DAILY_CAP=0):
            r = self.post({"place": self.ware.pk, "action": "link", "candidates": [c.pk]})
        self.assertEqual(r.status_code, 429)
        self.assertEqual(RecordReviewState.objects.get(place=self.ware).state, "unreviewed")
        r = self.post({"place": self.ware.pk, "action": "link", "candidates": [c.pk]})
        self.assertEqual(r.status_code, 200)

    def test_candidate_of_another_record_is_not_found(self):
        c = self.cand(self.brox, "gn:6")
        self.assertEqual(self.post({"place": self.ware.pk, "action": "link", "candidates": [c.pk]}).status_code, 404)
        self.assertEqual(self.post({"place": self.brox.pk, "action": "link", "candidates": [c.pk]}).status_code, 200)

    def test_not_the_same_place_rejects_and_only_says_so_when_asked(self):
        c6, q7 = self.cand(self.brox, "gn:6"), self.cand(self.brox, "wd:Q7")
        r = self.post({"place": self.brox.pk, "action": "reject", "candidates": [c6.pk]})
        self.assertEqual(r.status_code, 200)
        c6.refresh_from_db()
        self.assertEqual(c6.status, "rejected")
        self.assertFalse(ContributorAttestation.objects.exists())        # a rejection is not a statement
        r = self.post({"place": self.brox.pk, "action": "reject", "candidates": [q7.pk], "distinct": True})
        row = ContributorAttestation.objects.get()
        self.assertEqual((row.relation_type, row.candidate_id), ("distinct", q7.pk))
        self.assertEqual(RecordReviewState.objects.get(place=self.brox).state, "unreviewed")

    def test_none_later_and_undo(self):
        r = self.post({"place": self.brox.pk, "action": "none"})
        self.assertEqual(r.json()["state"], "no_good_match")
        self.assertEqual(set(MatchCandidate.objects.filter(place=self.brox).values_list("status", flat=True)),
                         {"rejected"})
        r = self.post({"place": self.ware.pk, "action": "later"})
        self.assertEqual(r.json()["counts"]["later"], 1)
        d = self.c.get(self.base + "candidates/?tier=later").json()
        self.assertEqual([x["title"] for x in d["records"]], ["Ware"])

        c = self.cand(self.ware, "gn:4")
        self.post({"place": self.ware.pk, "action": "link", "candidates": [c.pk]})
        row = ContributorAttestation.objects.get()
        self.assertEqual(row.status, "pending")
        r = self.post({"place": self.ware.pk, "action": "reopen"})
        row.refresh_from_db()
        c.refresh_from_db()
        self.assertEqual((row.status, c.status, r.json()["state"]), ("withdrawn", "suggested", "unreviewed"))
        self.assertEqual(r.json()["counts"]["confident"], 1)

    def test_bad_action(self):
        self.assertEqual(self.post({"place": self.ware.pk, "action": "accession"}).status_code, 400)


class ExportBase(FeedTestBase):
    def setUp(self):
        super().setUp()
        from unittest import mock
        # No test may reach the real gateway: by default it answers with nothing.
        p = mock.patch("api.crc_client.crc_fetch_places", lambda ids, user=None, meta=None, **k: {})
        p.start()
        self.addCleanup(p.stop)

    def rows(self, client=None):
        import csv, io
        r = (client or self.c).get(self.base + "export/")
        self.assertEqual(r.status_code, 200)
        self.assertIn("attachment", r["Content-Disposition"])
        return list(csv.DictReader(io.StringIO(r.content.decode("utf-8-sig"))))


class FeedExportTests(ExportBase):
    def test_earlier_decisions_are_exported_and_the_rest_say_where_they_stand(self):
        c = self.cand(self.ware, "gn:4")
        self.post({"place": self.ware.pk, "action": "link", "candidates": [c.pk], "note": "parish church"})
        self.post({"place": self.brox.pk, "action": "later"})
        rows = {r["src_id"]: r for r in self.rows()}
        self.assertEqual(set(rows), {"f1", "f2"})           # every record the feed holds, linked or not
        w = rows["f1"]
        self.assertEqual((w["review_state"], w["relation"], w["matched_id"], w["matched_title"],
                          w["matched_source"], w["note"]),
                         ("linked", "closeMatch", "gn:4", "Ware", "gn", "parish church"))
        self.assertEqual((float(w["matched_lon"]), float(w["matched_lat"])), (0.03, 51.811))
        b = rows["f2"]
        self.assertEqual((b["review_state"], b["relation"], b["matched_id"]), ("later", "", ""))

    def test_undone_links_are_not_exported_and_strangers_get_404(self):
        c = self.cand(self.ware, "gn:4")
        self.post({"place": self.ware.pk, "action": "link", "candidates": [c.pk]})
        self.post({"place": self.ware.pk, "action": "reopen"})
        self.assertEqual({r["matched_id"] for r in self.rows()}, {""})
        User = get_user_model()
        stranger = User.objects.create_user(username="feed_exp", email="fe@example.org", password="pw",
                                            given_name="F", surname="E", role="beta_tester")
        cl = Client()
        cl.force_login(stranger)
        self.assertEqual(cl.get(self.base + "export/").status_code, 404)
        self.assertEqual(Client().get(self.base + "export/").status_code, 302)


class FeedExportDetailTests(ExportBase):
    """The matched place's full record is read back from the gateway at download time, so a
    hand-search Link (which stores nothing but an id) is as complete as a suggestion."""
    PLACE = {"place_id": "gn:44", "namespace": "gn", "title": "Pazundaung",
             "names": [{"label": "Pazundaung", "lang": None, "timespans": []},
                       {"label": "Pazundaung Tsp", "lang": "my", "timespans": []}],
             "ccodes": ["MM"], "types": [{"identifier": "PPLX", "label": "P", "sourceLabel": "P.PPLX", "aat_ids": [300008347]}],
             "geometries": [{"repr_point": [96.17493, 16.79574]}], "repr_point": [96.17493, 16.79574],
             "links": [{"type": "seeAlso", "identifier": "https://en.wikipedia.org/wiki/Pazundaung"}],
             "descriptions": [], "relations": [], "population": 40690, "elevation": 19, "timespans": []}

    def export(self, fetch):
        from unittest import mock
        with mock.patch("api.crc_client.crc_fetch_places", fetch):
            return {r["src_id"]: r for r in self.rows()}

    def test_a_hand_search_link_carries_every_attribute(self):
        self.post({"place": self.ware.pk, "action": "link", "target": "gn:44", "note": "same town"})
        w = self.export(lambda ids, user=None, meta=None, **k: {"gn:44": self.PLACE})["f1"]
        self.assertEqual((w["matched_id"], w["matched_title"], w["matched_source"], w["matched_detail"]),
                         ("gn:44", "Pazundaung", "gn", "full"))
        self.assertEqual((w["matched_lon"], w["matched_lat"]), ("96.17493", "16.79574"))
        self.assertEqual((w["matched_ccodes"], w["matched_types"], w["matched_aat_ids"]), ("MM", "P.PPLX", "300008347"))
        self.assertEqual(w["matched_names"], "Pazundaung; Pazundaung Tsp (my)")
        self.assertIn("wikipedia.org/wiki/Pazundaung", w["matched_links"])
        self.assertEqual((w["matched_population"], w["matched_elevation"], w["note"]), ("40690", "19", "same town"))

    def test_a_silent_gateway_is_said_not_left_blank(self):
        self.post({"place": self.ware.pk, "action": "link", "target": "gn:44"})
        c = self.cand(self.brox, "gn:6")
        self.post({"place": self.brox.pk, "action": "link", "candidates": [c.pk]})

        def down(ids, user=None, meta=None, **k):
            meta["error"] = "timeout"
            return {}
        rows = self.export(down)
        self.assertTrue(rows["f1"]["matched_detail"].startswith("gateway unavailable"))
        self.assertTrue(rows["f2"]["matched_detail"].startswith("card only"))      # falls back to what was stored
        self.assertEqual(rows["f2"]["matched_title"], "Broxbourne")
        from datasets import match_export
        self.assertEqual(match_export.from_card({"temporal_range": [-814, 698]})["matched_dates"], "-814/698")

        def silent(ids, user=None, meta=None, **k):
            return {}
        self.assertTrue(self.export(silent)["f1"]["matched_detail"].startswith("not found"))

    def test_a_failing_gateway_is_asked_once_and_a_slow_one_for_a_minute_at_most(self):
        from unittest import mock
        from datasets import match_export
        pairs = [(f"gn:{n}", None) for n in range(5)]
        calls = []

        def down(ids, user=None, meta=None, **k):
            calls.append(ids)
            meta["error"] = "timeout"
            return {}
        with mock.patch.object(match_export, "CHUNK", 2):
            d = match_export.details_for(self.owner, pairs, fetch=down)
        self.assertEqual(len(calls), 1)                     # one failure, then no more asking
        self.assertTrue(all(r["matched_detail"].startswith("gateway unavailable") for r in d.values()))

        calls.clear()
        with mock.patch.object(match_export, "CHUNK", 2), mock.patch.object(match_export, "BUDGET_SECONDS", 0):
            d = match_export.details_for(self.owner, pairs, fetch=down)
        self.assertEqual(calls, [])                         # out of time: nothing asked
        self.assertTrue(all("time limit" in r["matched_detail"] for r in d.values()))

    def test_a_source_whose_licence_forbids_redistribution_is_withheld(self):
        from api.models import GazetteerRegistryEntry
        GazetteerRegistryEntry.objects.update_or_create(
            id="gn", defaults=dict(name="GeoNames", namespace="gn", entry_class="authority",
                                   status="published", redistributable=False, downloadable=False))
        self.post({"place": self.ware.pk, "action": "link", "target": "gn:44"})
        called = []

        def fetch(ids, user=None, meta=None, **k):
            called.append(ids)
            return {"gn:44": self.PLACE}
        w = self.export(fetch)["f1"]
        self.assertEqual((w["matched_id"], w["matched_source"], w["matched_source_name"]), ("gn:44", "gn", "GeoNames"))
        self.assertTrue(w["matched_detail"].startswith("withheld"))
        self.assertEqual((w["matched_lon"], w["matched_names"], w["matched_population"]), ("", "", ""))
        self.assertEqual(called, [])                         # nothing was even asked for


class FeedSearchTests(FeedTestBase):
    def test_hand_search_relaxes_chips_and_reports_silence_as_silence(self):
        self.gen.recipe = candidates.build_recipe(self.ds, countries=["GB"])
        self.gen.save()
        self.fake.by_query["Ware Priory"] = response(hit("gn:44", "Ware Priory", (0.03, 51.812), types=[GN_TOWN]))
        d = self.c.get(self.base + f"search/?place={self.ware.pk}&q=Ware+Priory").json()
        self.assertTrue(d["answered"])
        self.assertEqual([x["candidate_id"] for x in d["results"]], ["gn:44"])
        self.assertEqual(self.fake.calls[-1]["countries"], ["GB"])
        self.assertEqual([(c["id"], c["applied"]) for c in d["constraints"]], [("countries", True)])
        d = self.c.get(self.base + f"search/?place={self.ware.pk}&q=Ware+Priory&relax=countries").json()
        self.assertNotIn("countries", self.fake.calls[-1])
        self.assertEqual([(c["id"], c["applied"]) for c in d["constraints"]], [("countries", False)])

        self.fake.failures["Ware Priory"] = "timeout"
        d = self.c.get(self.base + f"search/?place={self.ware.pk}&q=Ware+Priory").json()
        self.assertEqual((d["answered"], d["failure"], d["results"]), (False, "timeout", []))

        r = self.post({"place": self.ware.pk, "action": "link", "target": "gn:44"})
        self.assertEqual(r.status_code, 200)
        row = ContributorAttestation.objects.get()
        self.assertEqual((row.suggested_by, row.candidate_id), ("manual", None))


class FeedSearchLocationTests(FeedTestBase):
    """Hand-search results carry ``location`` by the rules of the suggestions: a contributed
    result only if the viewer may see its dataset now (else left out entirely), authority results
    always, coordinates validated and rounded to 5 places."""

    SECRET = [12.34567, 45.67891]   # a point that must never appear in a refused response

    def search(self, q="Ware Priory"):
        r = self.c.get(self.base + f"search/?place={self.ware.pk}&q={q.replace(' ', '+')}")
        self.assertEqual(r.status_code, 200)
        return r.content.decode(), r.json()

    def test_hand_search_results_have_a_location_and_hidden_ones_are_absent(self):
        from datasets.models import Dataset
        other_user = get_user_model().objects.create_user(username="feed_hs", email="hs@example.org",
                                                          password="pw", given_name="H", surname="S")
        Dataset.objects.filter(pk=self.hidden.pk).update(owner=other_user)       # private, not ours
        self.place("Ware Priory", "o9", tuple(self.SECRET), ds=self.other)
        self.place("Ware Priory Zqx Secret", "x9", tuple(self.SECRET), ds=self.hidden)
        self.fake.by_query["Ware Priory"] = response(
            hit("gn:44", "Ware Priory", (0.0300123456, 51.8120987654), types=[GN_TOWN]),
            hit("wd:Q44", "Ware Priory", (400, 51.8)),                                   # bad coordinates
            hit("wd:Q45", "Ware Priory"),                                               # no point
            hit(f"whg:{self.other.pk}:o9", "Ware Priory Open Grange", tuple(self.SECRET), types=[GN_TOWN]),
            hit(f"whg:{self.hidden.pk}:x9", "Ware Priory Zqx Secret", tuple(self.SECRET), types=[GN_TOWN]))
        secret_id = f"whg:{self.hidden.pk}:x9"
        body, d = self.search()
        by_id = {x["candidate_id"]: x for x in d["results"]}
        # Present: authority rounded to 5 places, bad and missing points are null, a visible contribution served.
        self.assertEqual(by_id["gn:44"]["location"], [0.03001, 51.8121])
        self.assertEqual((by_id["wd:Q44"]["location"], by_id["wd:Q44"]["location_withheld"]), (None, False))
        self.assertEqual((by_id["wd:Q45"]["location"], by_id["wd:Q45"]["location_withheld"]), (None, False))
        open_id = f"whg:{self.other.pk}:o9"
        self.assertEqual((by_id[open_id]["location"], by_id[open_id]["location_withheld"]), (self.SECRET, False))
        # Absent: the private result, entirely (id, title, point), anywhere in the response.
        self.assertNotIn(secret_id, by_id)
        for needle in (secret_id, "Zqx Secret"):
            self.assertNotIn(needle, body)
        # Made public, the same request serves it with its point.
        Dataset.objects.filter(pk=self.hidden.pk).update(public=True)
        body, d = self.search()
        by_id = {x["candidate_id"]: x for x in d["results"]}
        self.assertEqual(by_id[secret_id]["location"], self.SECRET)
        self.assertIn("Zqx Secret", body)

    def test_the_feed_rule_holds_even_if_the_gateway_filter_is_loosened(self):
        """``candidates.filter_hits`` is the first line; the feed applies the suggestions' own rule
        again, so a private result that got past it is still not served."""
        from datasets.models import Dataset
        from unittest.mock import patch
        other_user = get_user_model().objects.create_user(username="feed_h2", email="h2@example.org",
                                                          password="pw", given_name="H", surname="T")
        Dataset.objects.filter(pk=self.hidden.pk).update(owner=other_user)
        self.place("Ware Priory Zqx Secret", "x9", tuple(self.SECRET), ds=self.hidden)
        self.fake.by_query["Ware Priory"] = response(
            hit("gn:44", "Ware Priory", (0.03, 51.812), types=[GN_TOWN]),
            hit(f"whg:{self.hidden.pk}:x9", "Ware Priory Zqx Secret", tuple(self.SECRET), types=[GN_TOWN]))
        with patch.object(candidates, "filter_hits", lambda hits, *a, **k: list(hits)):
            body, d = self.search()
            self.assertEqual([x["candidate_id"] for x in d["results"]], ["gn:44"])
            self.assertNotIn("Zqx Secret", body)
            self.assertNotIn("12.34567", body)
            Dataset.objects.filter(pk=self.hidden.pk).update(public=True)
            body, d = self.search()
            self.assertEqual(sorted(x["candidate_id"] for x in d["results"]),
                             sorted(["gn:44", f"whg:{self.hidden.pk}:x9"]))
            self.assertIn("Zqx Secret", body)


class LinksProgressSectionTests(FeedTestBase):
    TPL = Template("{% load match_feed_tags %}{% links_progress ds %}")

    def render_for(self, user):
        req = RequestFactory().get("/")
        req.user = user
        return self.TPL.render(Context({"ds": self.ds, "request": req}))

    def test_link_counts_are_shown_to_the_contributor_only(self):
        c = self.cand(self.ware, "gn:4")
        self.post({"place": self.ware.pk, "action": "link", "candidates": [c.pk]})
        html = self.render_for(self.owner)
        self.assertIn('data-testid="links-progress"', html)
        self.assertIn("1 match", html)
        self.assertIn("50% reviewed", html)
        from unittest.mock import patch
        self.ds.public = True
        with patch("datasets.signals.doi"):
            self.ds.save()
        stranger = get_user_model().objects.create_user(username="feed_pv", email="pv@example.org",
                                                         password="pw", given_name="P", surname="V",
                                                         role="beta_tester")
        html = self.render_for(stranger)
        self.assertNotIn("links-progress", html)
        self.assertNotIn("link", html.lower())


class UndoHandSearchLinkTests(FeedTestBase):
    """Undo withdraws the Links this user made for this record in this feed, including a
    hand-search one (no candidate); other people's Links and Links made outside the feed stay."""

    def test_undo_withdraws_the_hand_search_link_and_nothing_else(self):
        User = get_user_model()
        r = self.post({"place": self.ware.pk, "action": "link", "target": "gn:44"})
        self.assertEqual(r.status_code, 200)
        mine = ContributorAttestation.objects.get(user=self.owner)
        self.assertIsNone(mine.candidate_id)
        other = User.objects.create_user(username="feed_oth", email="fo@example.org", password="pw",
                                         given_name="O", surname="T", role="beta_tester")
        theirs = ContributorAttestation.objects.create(
            user=other, dataset_id=mine.dataset_id, relation_type="closeMatch", place_a=mine.place_a,
            place_b=mine.place_b, place_a_pk=mine.place_a_pk, place_b_pk=mine.place_b_pk,
            status=mine.status)
        outside = ContributorAttestation.objects.create(      # same user, made outside the feed
            user=self.owner, dataset_id=mine.dataset_id, relation_type="exactMatch",
            place_a=mine.place_a, place_b="wd:Q99", place_a_pk=mine.place_a_pk, status="active")
        self.assertEqual(mine.status in ("active", "pending"), True)   # present before Undo
        self.post({"place": self.ware.pk, "action": "reopen"})
        for row in (mine, theirs, outside):
            row.refresh_from_db()
        self.assertEqual(mine.status, "withdrawn")
        self.assertNotEqual(theirs.status, "withdrawn")
        self.assertEqual(outside.status, "active")


class FeedMapDataTests(FeedTestBase):
    """The data the feed map reads: ``location`` ([lon, lat] or null) on each record and each
    suggestion, through the existing candidates endpoint. A contributed suggestion whose dataset
    the person looking may not see NOW is left out entirely (title, id, point, number); every
    refusal is shown beside the same suggestion being served."""

    SECRET = [12.34567, 45.67891]   # a point that must never appear in a refused response

    def records(self, tier):
        r = self.c.get(self.base + f"candidates/?tier={tier}")
        self.assertEqual(r.status_code, 200)
        return r, {x["title"]: x for x in r.json()["records"]}

    def contributed(self, ds, src, rank, title="Ware", place=None):
        self.place(title, src, tuple(self.SECRET), ds=ds)
        place = place or self.ware
        return MatchCandidate.objects.create(
            dataset=self.ds, place=place, subject_id=f"whg:{self.ds.pk}:{place.src_id}",
            candidate_id=f"whg:{ds.pk}:{src}", candidate_title=title, tier="confident", rank=rank,
            generation=self.gen, card={"title": title, "namespace": "whg", "repr_point": list(self.SECRET)})

    def ware_cands(self):
        r, recs = self.records("confident")
        return r.content.decode(), recs["Ware"]["candidates"]

    def assertHidden(self, cand, body, cands):
        """Nothing of ``cand`` anywhere in the response: no id, no title, no number."""
        self.assertNotIn(cand.candidate_id, body)
        self.assertNotIn(cand.candidate_title, body)
        self.assertNotIn(cand.candidate_id, [c["candidate_id"] for c in cands])
        self.assertEqual([c["rank"] for c in cands], list(range(1, len(cands) + 1)))   # no gap

    def test_record_and_suggestion_points(self):
        _r, recs = self.records("confident")
        self.assertEqual(recs["Ware"]["location"], [0.03, 51.81])
        c = recs["Ware"]["candidates"][0]
        self.assertEqual((c["candidate_id"], c["location"], c["location_withheld"]), ("gn:4", [0.03, 51.811], False))
        _r, recs = self.records("ambiguous")
        self.assertEqual([c["location"] for c in recs["Broxbourne"]["candidates"]],
                         [[-0.02, 51.751], [-0.02, 51.752]])

    def test_record_without_a_location_and_a_suggestion_without_one(self):
        herts = self.place("Hertford", "f3")           # no geometry
        self.fake.by_query["Hertford"] = response(hit("gn:8", "Hertford", (-0.08, 51.79), types=[GN_TOWN]),
                                                  hit("wd:Q9", "Hertford"))   # no point at all
        candidates.record_candidates(herts, self.gen, self.owner)
        tier = RecordReviewState.objects.get(place=herts).tier
        _r, recs = self.records(tier)
        rec = recs["Hertford"]
        self.assertIsNone(rec["location"])
        self.assertFalse(rec["has_geometry"])
        by_id = {c["candidate_id"]: c for c in rec["candidates"]}
        self.assertEqual(by_id["gn:8"]["location"], [-0.08, 51.79])                   # present...
        self.assertEqual((by_id["wd:Q9"]["location"], by_id["wd:Q9"]["location_withheld"]), (None, False))  # ...absent

    def test_a_hit_with_only_point_lat_lon_is_turned_round(self):
        h = hit("gn:12", "Ware")
        h["point"] = [51.8105, 0.0301]                  # [lat, lon], no repr_point
        self.assertEqual(candidates.card(h)["repr_point"], [0.0301, 51.8105])
        self.assertNotIn("repr_point", candidates.card(hit("gn:13", "Ware")))

    def test_a_suggestion_the_viewer_may_not_see_now_is_left_out_entirely(self):
        from datasets.models import Dataset
        other_user = get_user_model().objects.create_user(username="feed_mo", email="mo@example.org",
                                                          password="pw", given_name="M", surname="O")
        Dataset.objects.filter(pk=self.hidden.pk).update(owner=other_user)       # private, not ours
        good = self.contributed(self.other, "o1", 2, title="Ware Open Grange")   # public
        bad = self.contributed(self.hidden, "x1", 3, title="Ware Zqx Secret Priory")
        body, cands = self.ware_cands()
        self.assertEqual([c["candidate_id"] for c in cands], ["gn:4", good.candidate_id])   # authority kept
        self.assertEqual(cands[1]["location"], self.SECRET)                          # served when visible
        self.assertIn("Ware Open Grange", body)
        self.assertHidden(bad, body, cands)
        # Made public, the same suggestion is served through the same request, numbered 3.
        Dataset.objects.filter(pk=self.hidden.pk).update(public=True)
        body, cands = self.ware_cands()
        self.assertEqual([(c["candidate_id"], c["rank"]) for c in cands][-1], (bad.candidate_id, 3))
        self.assertIn("Ware Zqx Secret Priory", body)
        self.assertEqual(cands[-1]["location"], self.SECRET)

    def test_hidden_suggestion_leaves_no_gap_in_the_numbers(self):
        from datasets.models import Dataset
        other_user = get_user_model().objects.create_user(username="feed_mg", email="mg@example.org",
                                                          password="pw", given_name="M", surname="G")
        Dataset.objects.filter(pk=self.hidden.pk).update(owner=other_user)
        MatchCandidate.objects.filter(place=self.ware, candidate_id="gn:4").update(rank=3)
        bad = self.contributed(self.hidden, "x2", 1, title="Ware Zqx Hidden First")
        good = self.contributed(self.other, "o2", 2, title="Ware Open Second")
        body, cands = self.ware_cands()
        self.assertEqual([(c["candidate_id"], c["rank"]) for c in cands], [(good.candidate_id, 1), ("gn:4", 2)])
        self.assertHidden(bad, body, cands)

    def test_embargoed_suggestion_is_hidden_from_outsiders_until_released(self):
        from datetime import timedelta
        from django.utils import timezone
        from api.models import GazetteerRegistryEntry
        from datasets.models import Dataset
        other_user = get_user_model().objects.create_user(username="feed_me", email="me@example.org",
                                                          password="pw", given_name="M", surname="E")
        Dataset.objects.filter(pk=self.hidden.pk).update(owner=other_user, public=True)
        cand = self.contributed(self.hidden, "e1", 5, title="Ware Zqx Embargoed Mill")
        body, cands = self.ware_cands()
        self.assertIn(cand.candidate_id, [c["candidate_id"] for c in cands])         # public: shown
        row = GazetteerRegistryEntry.objects.create(id=f"whg:{self.hidden.pk}", name="Embargoed", namespace="whg",
                                                    entry_class="dataset", status="embargoed")
        body, cands = self.ware_cands()
        self.assertHidden(cand, body, cands)
        self.assertNotIn("45.67891", body)                   # no coordinates anywhere either
        # An insider of an embargoed dataset (here its owner) still sees it.
        mine = self.contributed(self.other, "e2", 7, title="Ware Our Embargoed Barn")
        GazetteerRegistryEntry.objects.create(id=f"whg:{self.other.pk}", name="Ours", namespace="whg",
                                              entry_class="dataset", status="embargoed")
        body, cands = self.ware_cands()
        self.assertIn(mine.candidate_id, [c["candidate_id"] for c in cands])
        self.assertIn("Ware Our Embargoed Barn", body)
        self.assertHidden(cand, body, cands)
        row.embargo_release_at = timezone.now() - timedelta(days=1)       # a lifted embargo
        row.save()
        body, cands = self.ware_cands()
        self.assertIn(cand.candidate_id, [c["candidate_id"] for c in cands])

    def test_a_suggestion_from_a_deleted_dataset_is_left_out(self):
        good = self.contributed(self.other, "o1", 5)
        gone = MatchCandidate.objects.create(
            dataset=self.ds, place=self.ware, subject_id=f"whg:{self.ds.pk}:f1", candidate_id="whg:999999:z",
            candidate_title="Ware Zqx Gone Abbey", tier="confident", rank=6, generation=self.gen,
            card={"title": "Ware Zqx Gone Abbey", "repr_point": list(self.SECRET)})
        body, cands = self.ware_cands()
        self.assertIn(good.candidate_id, [c["candidate_id"] for c in cands])
        self.assertHidden(gone, body, cands)

    def test_a_suggestion_from_a_deleted_dataset_is_left_out_for_staff_too(self):
        """Staff see every EXISTING dataset, so their visibility filter is unconstrained and used
        to hand the ids back unchecked: a dataset that no longer exists counted as visible."""
        get_user_model().objects.filter(pk=self.owner.pk).update(is_staff=True)
        good = self.contributed(self.other, "o1", 5)
        gone = MatchCandidate.objects.create(
            dataset=self.ds, place=self.ware, subject_id=f"whg:{self.ds.pk}:f1", candidate_id="whg:999999:z",
            candidate_title="Ware Zqx Gone Abbey", tier="confident", rank=6, generation=self.gen,
            card={"title": "Ware Zqx Gone Abbey", "repr_point": list(self.SECRET)})
        body, cands = self.ware_cands()
        self.assertIn(good.candidate_id, [c["candidate_id"] for c in cands])
        self.assertHidden(gone, body, cands)

    def test_a_record_whose_only_suggestions_are_hidden_shows_none(self):
        from datasets.models import Dataset
        other_user = get_user_model().objects.create_user(username="feed_mn", email="mn@example.org",
                                                          password="pw", given_name="M", surname="N")
        Dataset.objects.filter(pk=self.hidden.pk).update(owner=other_user)
        hod = self.place("Hoddesdon", "f4", (-0.01, 51.76))
        candidates.record_candidates(hod, self.gen, self.owner)        # the gateway answers nothing
        tier = RecordReviewState.objects.get(place=hod).tier
        bad = self.contributed(self.hidden, "x3", 1, title="Hoddesdon Zqx Secret Hall", place=hod)
        r, recs = self.records(tier)
        self.assertEqual(recs["Hoddesdon"]["candidates"], [])           # the normal "no suggestions" state
        self.assertNotIn("Zqx", r.content.decode())
        Dataset.objects.filter(pk=self.hidden.pk).update(public=True)
        _r, recs = self.records(tier)
        self.assertEqual([c["candidate_id"] for c in recs["Hoddesdon"]["candidates"]], [bad.candidate_id])

    def test_bad_coordinates_are_no_location(self):
        from datasets.views_feed import lon_lat
        self.assertEqual(lon_lat([0.123456789, 51.5]), [0.12346, 51.5])
        for bad in (None, [], [1], ["x", 2], [200, 10], [10, 95], [float("nan"), 1], "0,1"):
            self.assertIsNone(lon_lat(bad), bad)

    def test_the_page_has_the_map_panel_and_its_no_location_state(self):
        r = self.c.get(self.base)
        for el in ('id="mf_map_panel"', 'id="mf_map"', 'id="mf_map_none"', 'id="mf_map_failed"'):
            self.assertContains(r, el)


class FeedDecideTests(FeedTestBase):
    """``decide``: every row of a record decided in one request, with how each decision arose."""

    def decide(self, place, decisions, **extra):
        return self.post({"place": place.pk, "action": "decide", "decisions": decisions, **extra})

    def d(self, place, cid, decision, source="explicit"):
        return {"candidate": self.cand(place, cid).pk, "decision": decision, "source": source}

    def test_one_same_leaves_the_other_suggestions_untouched(self):
        # choosing one place is not a judgement of the rest: only what is sent is written
        r = self.decide(self.brox, [self.d(self.brox, "gn:6", "same")])
        self.assertEqual(r.status_code, 200, r.content)
        gn, wd = self.cand(self.brox, "gn:6"), self.cand(self.brox, "wd:Q7")
        self.assertEqual((gn.status, gn.decision_source), ("accepted", "explicit"))
        self.assertEqual((wd.status, wd.decision_source), ("suggested", ""))
        self.assertEqual(RecordReviewState.objects.get(place=self.brox).state, "linked")
        self.assertEqual(ContributorAttestation.objects.count(), 1)       # a rejection is not a statement
        row = ContributorAttestation.objects.get()
        self.assertEqual((row.relation_type, row.candidate_id, row.evidence["decision_source"]),
                         ("closeMatch", gn.pk, "explicit"))

    def test_a_decision_the_page_filled_in_is_refused(self):
        r = self.decide(self.brox, [self.d(self.brox, "gn:6", "same"), self.d(self.brox, "wd:Q7", "not", "prefilled")])
        self.assertEqual(r.status_code, 400, r.content)
        self.assertEqual(ContributorAttestation.objects.count(), 0)
        self.assertEqual(self.cand(self.brox, "wd:Q7").status, "suggested")

    def test_a_public_not_the_same_statement_is_made_only_for_a_negative_the_person_marked(self):
        self.brox_states = self.decide(
            self.brox, [self.d(self.brox, "gn:6", "not", "explicit"),
                        self.d(self.brox, "wd:Q7", "not", "explicit")], distinct=True)
        self.assertEqual(self.brox_states.status_code, 200, self.brox_states.content)
        rows = list(ContributorAttestation.objects.all())
        self.assertEqual(sorted((r.relation_type, r.candidate_id) for r in rows),
                         sorted([("distinct", self.cand(self.brox, "gn:6").pk), ("distinct", self.cand(self.brox, "wd:Q7").pk)]))
        # Both were set aside, so nothing is left to choose: the record is "none fits".
        self.assertEqual(RecordReviewState.objects.get(place=self.brox).state, "no_good_match")
        # The same request without the box ticked makes no statement at all.
        MatchCandidate.objects.filter(place=self.brox).update(status="suggested", decision_source="")
        ContributorAttestation.objects.all().delete()
        self.decide(self.brox, [self.d(self.brox, "gn:6", "not")])
        self.assertFalse(ContributorAttestation.objects.exists())

    def test_unsure_leaves_the_suggestion_as_it_is_and_the_record_open(self):
        r = self.decide(self.brox, [self.d(self.brox, "gn:6", "not"), self.d(self.brox, "wd:Q7", "unsure")])
        self.assertEqual(r.status_code, 200, r.content)
        self.assertEqual(self.cand(self.brox, "wd:Q7").status, "suggested")
        self.assertEqual(self.cand(self.brox, "gn:6").status, "rejected")
        self.assertEqual(r.json()["state"], "unreviewed")                 # still to decide
        self.assertEqual(r.json()["counts"]["confident"], 1)

    def test_bad_input_is_refused(self):
        ok = self.d(self.brox, "gn:6", "same")
        for body in ([], None, "x"):
            self.assertEqual(self.post({"place": self.brox.pk, "action": "decide", "decisions": body}).status_code, 400)
        self.assertEqual(self.decide(self.brox, [{**ok, "decision": "maybe"}]).status_code, 400)
        self.assertEqual(self.decide(self.brox, [{**ok, "source": "guess"}]).status_code, 400)
        self.assertEqual(self.decide(self.brox, [{"decision": "same"}]).status_code, 400)
        self.assertEqual(self.decide(self.brox, [self.d(self.brox, "gn:6", "unsure")]).status_code, 400)
        self.assertEqual(self.decide(self.brox, [ok], relation="distinct").status_code, 400)
        # another record's suggestion, an unknown id, and the same row twice
        self.assertEqual(self.decide(self.brox, [self.d(self.ware, "gn:4", "same")]).status_code, 404)
        self.assertEqual(self.decide(self.brox, [{**ok, "candidate": 99999999}]).status_code, 404)
        self.assertEqual(self.decide(self.brox, [ok, ok]).status_code, 404)
        self.assertFalse(ContributorAttestation.objects.exists())
        self.assertEqual(set(MatchCandidate.objects.filter(place=self.brox).values_list("status", flat=True)),
                         {"suggested"})
        self.assertEqual(self.decide(self.brox, [ok]).status_code, 200)    # the same request, fixed

    def test_a_suggestion_set_aside_cannot_then_be_linked_without_undoing(self):
        gn = self.cand(self.brox, "gn:6")
        self.post({"place": self.brox.pk, "action": "reject", "candidates": [gn.pk]})
        r = self.post({"place": self.brox.pk, "action": "link", "candidates": [gn.pk]})
        self.assertEqual(r.status_code, 409)
        self.assertEqual(self.decide(self.brox, [self.d(self.brox, "gn:6", "same")]).status_code, 409)
        self.assertFalse(ContributorAttestation.objects.exists())
        self.post({"place": self.brox.pk, "action": "reopen"})
        self.assertEqual(self.post({"place": self.brox.pk, "action": "link", "candidates": [gn.pk]}).status_code, 200)
        # and the other way round: a linked suggestion cannot be set aside
        self.assertEqual(self.decide(self.brox, [self.d(self.brox, "gn:6", "not")]).status_code, 409)

    def test_none_records_its_source_and_undo_clears_it(self):
        self.post({"place": self.brox.pk, "action": "none"})
        self.assertEqual(set(MatchCandidate.objects.filter(place=self.brox).values_list("decision_source", flat=True)),
                         {"none_fits"})
        self.post({"place": self.brox.pk, "action": "reopen"})
        self.assertEqual(set(MatchCandidate.objects.filter(place=self.brox).values_list("decision_source", flat=True)),
                         {""})

    def test_every_review_reply_carries_the_progress_the_header_shows(self):
        r = self.post({"place": self.ware.pk, "action": "later"})
        p = r.json()["progress"]
        self.assertEqual((p["records"], p["reviewed"], p["links"]), (2, 1, 0))
        r = self.decide(self.brox, [self.d(self.brox, "gn:6", "same")])
        p = r.json()["progress"]
        self.assertEqual((p["reviewed"], p["links"]), (2, 1))


class FeedRecordDetailsAndGroupsTests(FeedTestBase):
    def test_the_record_is_described_by_its_other_names_types_and_countries(self):
        from places.models import PlaceName, PlaceType
        PlaceName.objects.create(place=self.ware, src_id="f1", toponym="Wara", jsonb={"toponym": "Wara"})
        PlaceName.objects.create(place=self.ware, src_id="f1", toponym="Ware", jsonb={"toponym": "Ware"})  # the title
        PlaceType.objects.create(place=self.ware, src_id="f1", fclass="P", jsonb={"sourceLabel": "market town"})
        type(self.ware).objects.filter(pk=self.ware.pk).update(ccodes=["GB"])
        rec = {x["title"]: x for x in self.c.get(self.base + "candidates/?tier=confident").json()["records"]}["Ware"]
        self.assertEqual((rec["names"], rec["types"], rec["ccodes"]), (["Wara"], ["market town"], ["GB"]))
        self.assertIn("decided_at", rec)

    def cand_json(self, rank, ns, title, point, names=()):
        return {"rank": rank, "namespace": ns, "title": title, "location": point,
                "card": {"title": title, "names": [{"label": n} for n in names]}}

    def test_the_same_place_in_two_sources_is_grouped_and_nothing_else(self):
        from datasets.views_feed import group_candidates
        cs = group_candidates([
            self.cand_json(1, "gn", "Cairo", [31.2497, 30.0626], ["القاهرة"]),
            self.cand_json(2, "wd", "القاهرة", [31.2357, 30.0444]),                 # 2.4 km: too far apart
            self.cand_json(3, "wd", "Cairo", [31.2500, 30.0630]),                   # 50 m, same name: one place
            self.cand_json(4, "gn", "Cairo", [31.2497, 30.0626]),                   # same source as #1: never grouped
            self.cand_json(5, "wd", "Cairo", [-89.17, 37.00]),                      # same name, other continent
            self.cand_json(6, "osm", "Caïro", [31.2501, 30.0627]),                  # accents and case ignored
            self.cand_json(7, "tgn", "Cairo", None)])                               # no location: never grouped
        self.assertEqual([c["group"] for c in cs], [1, None, 1, None, None, 1, None])

    def test_the_endpoint_serves_the_group(self):
        hert = self.place("Hertford", "f9", (-0.08, 51.80))
        self.fake.by_query["Hertford"] = response(
            hit("gn:41", "Hertford", (-0.08, 51.80), types=[GN_TOWN]),
            hit("wd:Q41", "Hertford", (-0.0801, 51.8001), types=[GN_TOWN]))
        candidates.record_candidates(hert, self.gen, self.owner)
        tier = RecordReviewState.objects.get(place=hert).tier
        recs = {x["title"]: x for x in self.c.get(self.base + f"candidates/?tier={tier}").json()["records"]}
        self.assertEqual({c["candidate_id"]: c["group"] for c in recs["Hertford"]["candidates"]},
                         {"gn:41": 1, "wd:Q41": 1})
