"""place#338: ``manage.py calibrate_match_tiers`` on SYNTHETIC data only.

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test datasets.tests_calibrate --settings=whg.settings_localtest

The fixture: 120 records, each with one suggestion whose label comes from a different source
(feed decision, reviewed v3 link, reviewed v3 hit, reviewed close match, a person's Link), with
expected counts written down before the run. The refusal (99 negatives) is shown beside the same
data with the 100th negative reporting.
"""
import io
import json
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase

from datasets.management.commands.calibrate_match_tiers import norm_id
from datasets.models_matching import CandidateGeneration, MatchCandidate


class CalibrateTests(TestCase):
    def setUp(self):
        from datasets.models import Dataset
        self.user = get_user_model().objects.create_user(username="cal", email="cal@example.org", password="pw",
                                                         given_name="C", surname="L")
        with patch("datasets.signals.doi"):
            self.ds = Dataset.objects.create(owner=self.user, label="cal338", title="T", description="D",
                                             creator="X", public=True, ds_status="indexed")
            self.ds2 = Dataset.objects.create(owner=self.user, label="cal338b", title="T2", description="D",
                                              creator="X", public=True, ds_status="indexed")
        self.gen = CandidateGeneration.objects.create(dataset=self.ds, status="finished")
        self.n = 0

    def cand(self, tier, cid, rank=1, status="suggested", demoted=False):
        from places.models import Place
        self.n += 1
        p = Place.objects.create(title=f"P{self.n}", src_id=f"s{self.n}", dataset=self.ds, ccodes=[])
        MatchCandidate.objects.create(dataset=self.ds, place=p, subject_id=f"whg:{self.ds.pk}:s{self.n}",
                                      candidate_id=cid, tier=tier, rank=rank, status=status, generation=self.gen,
                                      signals={"type_sanity": "demoted:S.SWT" if demoted else "ok"})
        return p

    def build(self, negatives):
        """Confident: 30 positives, 10 negatives (4 of them demoted by type sanity). Ambiguous: 10 positives, the rest negatives.
        Plus 5 unlabelled confident, and one conflicting pair."""
        from api.models import ContributorAttestation
        from datasets.models import Hit
        from places.models import CloseMatch, Place, PlaceLink
        for i in range(10):                                   # feed decisions
            self.cand("confident", f"gn:{i}", status="accepted")
        for i in range(10):                                   # reviewed v3 links (URI form)
            p = self.cand("confident", f"wd:Q{i}")
            PlaceLink.objects.create(place=p, task_id="t", reviewer=self.user,
                                     jsonb={"type": "closeMatch", "identifier": f"http://www.wikidata.org/entity/Q{i}"})
        for i in range(10):                                   # reviewed close matches
            o = Place.objects.create(title="O", src_id=f"o{i}", dataset=self.ds2, ccodes=[])
            p = self.cand("confident", f"whg:{self.ds2.pk}:o{i}")
            CloseMatch.objects.create(place_a=p, place_b=o, created_by=self.user, basis="reviewed")
        for i in range(10):                                   # reviewed hits, rejected: confident false positives
            p = self.cand("confident", f"tgn:{i}", demoted=(i < 4))
            Hit.objects.create(place=p, task_id="t", authority="tgn", dataset=self.ds, query_pass="pass1",
                               src_id="x", score=1, reviewed=True, matched=False, authrecord_id=str(i))
        for i in range(10):                                   # people's Links, positive
            p = self.cand("ambiguous", f"gn:a{i}")
            ContributorAttestation.objects.create(user=self.user, place_a=f"gn:a{i}", place_b=f"whg:{p.pk}",
                                                  relation_type="closeMatch")
        for i in range(negatives - 10):                       # feed rejections + distinct Links
            if i % 2:
                self.cand("ambiguous", f"gn:r{i}", status="rejected")
            else:
                p = self.cand("ambiguous", f"gn:d{i}")
                ContributorAttestation.objects.create(user=self.user, place_a=f"gn:d{i}",
                                                      place_b=f"whg:{self.ds.pk}:{p.src_id}", relation_type="distinct")
        for i in range(5):
            self.cand("confident", f"gn:u{i}")
        p = self.cand("weak", "gn:c1", status="rejected")      # conflicting: rejected here, linked in v3
        PlaceLink.objects.create(place=p, task_id="t", reviewer=self.user, jsonb={"identifier": "gn:c1"})

    def run_cmd(self, *args):
        out = io.StringIO()
        call_command("calibrate_match_tiers", *args, stdout=out)
        return out.getvalue()

    def test_refuses_below_100_negatives_and_reports_at_100(self):
        self.build(negatives=99)
        with self.assertRaises(CommandError) as cm:
            self.run_cmd()
        self.assertIn("99 labelled negatives", str(cm.exception))
        self.assertIn("40 positives", str(cm.exception))
        self.cand("ambiguous", "gn:last", status="rejected")   # the 100th negative
        report = json.loads(self.run_cmd("--json"))
        self.assertEqual((report["positives"], report["negatives"], report["conflicting"]), (40, 100, 1))
        conf = report["top_suggestion"]["confident"]
        self.assertEqual((conf["positives"], conf["negatives"], conf["unlabelled"]), (30, 10, 5))
        self.assertAlmostEqual(conf["precision"], 0.75)
        amb = report["top_suggestion"]["ambiguous"]
        self.assertEqual((amb["positives"], amb["negatives"]), (10, 90))
        self.assertEqual(report["type_sanity_demoted"], {"positives": 0, "negatives": 4, "unlabelled": 0})
        self.assertEqual(set(report["label_sources"]), {"feed", "link", "hit", "close", "statement"})
        text = self.run_cmd()
        self.assertIn("75.0%  = 30 / 40 labelled", text)

    def test_stale_suggestions_are_not_counted(self):
        self.build(negatives=100)
        self.assertEqual(json.loads(self.run_cmd("--json"))["negatives"], 100)
        # The rejected v3 hits label these whatever the suggestion's status; stale ones must not count.
        MatchCandidate.objects.filter(candidate_id__startswith="tgn:").update(status="stale")
        with self.assertRaises(CommandError) as cm:
            self.run_cmd()
        self.assertIn("90 labelled negatives", str(cm.exception))

    def test_norm_id(self):
        self.assertEqual(norm_id("http://www.wikidata.org/entity/Q90"), "wd:Q90")
        self.assertEqual(norm_id("Q90", "wd"), "wd:Q90")
        self.assertEqual(norm_id("7011781", "tgn"), "tgn:7011781")
        self.assertIsNone(norm_id("wd:None"))
        self.assertEqual(norm_id("gn:2988507"), "gn:2988507")
