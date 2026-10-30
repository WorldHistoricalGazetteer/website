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
        self.assertIn("1 link", html)
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
