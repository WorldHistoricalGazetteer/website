"""place#338 (v4 reconciliation Phase 2): candidate scoring, type sanity, tiers, own-dataset filter.

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test datasets.tests_candidates --settings=whg.settings_localtest

No gateway is ever called: ``api.crc_client.crc_search_status`` (the only door ``candidates``
uses) is replaced by a fake answering from the fixtures below, and ``requests`` inside the
client is replaced by a function that fails the test, so a path that slipped past the fake
would be caught rather than reach the CRC. Every refusal / absence is paired with the same case
succeeding through the same path.
"""
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.gis.geos import Point
from django.test import TestCase, override_settings

from datasets import candidates
from datasets.models_matching import CandidateGeneration, MatchCandidate, RecordReviewState

PARAMS = {"version": 1, "calibrated": True,
          "weights": {"name": 0.14045797658094575, "spatial": 0.2903334049072834,
                      "temporal": 0.3613341403053407, "type": 0.05787447820642999, "link": 0.15},
          "thresholds": {"theta_query": 0.21, "theta_bridge": 0.31, "theta_synth": 0.36,
                         "theta_synth_structural": 0.6, "tau_name": 0.75, "tau_link": 1.0}}
OXFORD = (-1.2577, 51.7520)


def hit(pid, title, xy=None, conf=100.0, types=(), years=None, score=100.0):
    ns = pid.split(":", 1)[0]
    h = {"place_id": pid, "title": title, "namespace": ns, "names": [{"label": title}],
         "types": list(types), "ccodes": ["GB"], "score": score, "confidence": conf,
         "query_match": {"name": title, "score": score}, "aat_paths": [], "temporal_range": years}
    if xy:
        h["repr_point"] = list(xy)
    return h


def near(xy, dkm):
    """A point ``dkm`` km north of ``xy`` (1 degree of latitude ~ 111.2 km)."""
    return (xy[0], xy[1] + dkm / 111.2)


GN_SEWAGE = {"identifier": "SWT", "label": "S", "sourceLabel": "S.SWT"}
GN_TOWN = {"identifier": "PPL", "label": "P", "sourceLabel": "P.PPL"}
WD_MEMORIAL = {"identifier": "Q575759", "label": "wikidata", "sourceLabel": "Q575759"}
WD_VILLAGE = {"identifier": "Q532", "label": "wikidata", "sourceLabel": "Q532"}


def response(*hits, edges=()):
    return {"hits": list(hits), "edges": list(edges), "clustering_params": PARAMS,
            "toponym_stoplist": [], "total": len(hits)}


def no_network(*a, **k):
    raise AssertionError("a test reached requests: the CRC gateway must never be called")


class GatewayFake:
    """Answers ``crc_search_status`` from a dict keyed by query string; records every call."""

    def __init__(self):
        self.by_query, self.failures, self.calls = {}, {}, []

    def __call__(self, options, user=None, meta=None):
        self.calls.append(dict(options))
        q = options.get("qstr")
        if q in self.failures:
            return None, self.failures[q]
        return self.by_query.get(q, response()), None


class CandidateTestBase(TestCase):
    def setUp(self):
        from datasets.models import Dataset
        User = get_user_model()
        self.owner = User.objects.create_user(username="cand_owner", email="co@example.org", password="pw",
                                              given_name="C", surname="Owner", role="beta_tester")
        with patch("datasets.signals.doi"):
            self.ds = Dataset.objects.create(owner=self.owner, label="cand338", title="Clergy", description="D",
                                             creator="X", public=False, ds_status="uploaded")
            self.other = Dataset.objects.create(owner=self.owner, label="cand338o", title="Other", description="D",
                                                creator="X", public=True, ds_status="indexed")
            self.hidden = Dataset.objects.create(owner=self.owner, label="cand338h", title="Hidden",
                                                 description="D", creator="X", public=False,
                                                 ds_status="uploaded")
        self.fake = GatewayFake()
        for target, fn in (("api.crc_client.crc_search_status", self.fake),
                           ("api.crc_client.requests.post", no_network),
                           ("api.crc_client.requests.get", no_network)):
            p = patch(target, side_effect=fn)
            p.start()
            self.addCleanup(p.stop)

    def place(self, title, src, xy=None, fclasses=None, minmax=None, ds=None):
        from places.models import Place, PlaceGeom
        p = Place.objects.create(title=title, src_id=src, dataset=ds or self.ds, ccodes=[],
                                 fclasses=fclasses, minmax=minmax)
        if xy:
            PlaceGeom.objects.create(place=p, geom=Point(*xy, srid=4326),
                                     jsonb={"type": "Point", "coordinates": list(xy)})
        return p

    def generation(self):
        return CandidateGeneration.objects.create(dataset=self.ds, requested_by=self.owner,
                                                  recipe=candidates.build_recipe(self.ds), status="running")


class TierRuleTests(CandidateTestBase):
    def assess(self, place, *hits, edges=()):
        tier, scored, _rec = candidates.assess(place, response(*hits, edges=edges), self.owner)
        return tier, scored

    def test_settlement_match_is_confident_but_a_sewage_works_of_the_same_name_is_not(self):
        """The CCEd case (place#273): "Oxford St Cross or Holywell" matched a sewage works at 100.
        Same name, same place on the map, same confidence: only the type differs."""
        rec = self.place("Oxford St Cross", "c1", OXFORD, fclasses=["A"])
        tier, scored = self.assess(rec, hit("gn:1", "Oxford St Cross", near(OXFORD, 0.5), types=[GN_TOWN]))
        self.assertEqual(tier, "confident")                      # the control: this fixture CAN be confident
        self.assertEqual(scored[0]["tier"], "confident")

        tier, scored = self.assess(rec, hit("gn:2", "Oxford St Cross", near(OXFORD, 0.5), types=[GN_SEWAGE]))
        self.assertEqual(len(scored), 1)                         # the candidate is still there, shown...
        self.assertEqual(scored[0]["confidence"], 100.0)
        self.assertFalse(scored[0]["sane"])                      # ...demoted by type sanity
        self.assertEqual(scored[0]["sanity_reason"], "S.SWT")
        self.assertNotEqual(tier, "confident")
        self.assertEqual(tier, "none")                           # every hit failed sanity
        self.assertIn("named after a place", scored[0]["explanation"]["sentence"])

    def test_sanity_demotes_below_a_real_settlement_candidate(self):
        rec = self.place("Bicester", "c2", (-1.15, 51.90), fclasses=["P"])
        tier, scored = self.assess(rec,
                                   hit("wd:Q99", "Bicester", near((-1.15, 51.90), 2), types=[WD_MEMORIAL]),
                                   hit("wd:Q100", "Bicester", near((-1.15, 51.90), 0.3), types=[WD_VILLAGE]))
        self.assertEqual([c["id"] for c in scored], ["wd:Q100", "wd:Q99"])
        self.assertEqual((tier, scored[0]["tier"], scored[1]["tier"]), ("confident", "confident", "weak"))

    def test_record_of_the_same_kind_is_not_demoted(self):
        """A lake dataset may match lakes: the rule applies to settlement records only."""
        lake = {"identifier": "LK", "label": "H", "sourceLabel": "H.LK"}
        rec_lake = self.place("Windermere", "c3", (-2.94, 54.35), fclasses=["H"])
        rec_town = self.place("Windermere", "c4", (-2.94, 54.35), fclasses=["P"])
        h = hit("gn:3", "Windermere", near((-2.94, 54.35), 1), types=[lake])
        self.assertEqual(self.assess(rec_lake, h)[0], "confident")
        self.assertEqual(self.assess(rec_town, h)[0], "none")

    def test_name_without_corroboration_is_ambiguous(self):
        """Name-confident but nothing else measured (the record has no point, no dates)."""
        rec = self.place("Ware", "c5")
        tier, scored = self.assess(rec, hit("gn:4", "Ware", (0.03, 51.81), types=[GN_TOWN]))
        self.assertEqual(tier, "ambiguous")
        rec2 = self.place("Ware", "c6", (0.03, 51.81))
        self.assertEqual(self.assess(rec2, hit("gn:4", "Ware", (0.03, 51.81), types=[GN_TOWN]))[0], "confident")

    def test_far_away_name_match_is_not_corroborated(self):
        rec = self.place("Ware", "c7", (0.03, 51.81))
        tier, scored = self.assess(rec, hit("gn:5", "Ware", (-71.0, 42.0), types=[GN_TOWN]))
        self.assertEqual(tier, "ambiguous")
        self.assertIn("apart", scored[0]["explanation"]["sentence"])

    def test_two_different_places_make_it_ambiguous_unless_linked(self):
        rec = self.place("Broxbourne", "c8", (-0.02, 51.75))
        a = hit("gn:6", "Broxbourne", near((-0.02, 51.75), 1), types=[GN_TOWN])
        b = hit("wd:Q7", "Broxbourne", near((-0.02, 51.75), 1.2), types=[WD_VILLAGE])
        self.assertEqual(self.assess(rec, a, b)[0], "ambiguous")
        same = {"a": "gn:6", "b": "wd:Q7", "relation_type": "sameAs"}
        self.assertEqual(self.assess(rec, a, b, edges=[same])[0], "confident")

    def test_low_confidence_is_weak(self):
        rec = self.place("Cheshunt", "c9", (-0.03, 51.70))
        tier, scored = self.assess(rec, hit("gn:8", "Chesham", near((-0.03, 51.70), 1), conf=20.0,
                                            types=[GN_TOWN]))
        self.assertEqual(tier, "weak")
        self.assertEqual(self.assess(rec, hit("gn:8", "Cheshunt", near((-0.03, 51.70), 1), conf=90.0,
                                              types=[GN_TOWN]))[0], "confident")

    def test_no_hits_is_none(self):
        rec = self.place("Nowhere", "c10", (0, 0))
        self.assertEqual(self.assess(rec)[0], "none")
        self.assertEqual(self.assess(rec, hit("gn:9", "Nowhere", (0, 0.001), types=[GN_TOWN]))[0], "confident")


class OwnDatasetFilterTests(CandidateTestBase):
    def test_own_dataset_and_hidden_records_are_dropped_in_django(self):
        rec = self.place("Hoddesdon", "h1", (-0.01, 51.76))
        sibling = self.place("Hoddesdon", "h2", (-0.01, 51.76))
        other = self.place("Hoddesdon", "o1", (-0.01, 51.76), ds=self.other)
        hidden = self.place("Hoddesdon", "x1", (-0.01, 51.76), ds=self.hidden)
        hits = [hit(f"whg:{self.ds.pk}:h1", "Hoddesdon", (-0.01, 51.76)),             # the record itself
                hit(f"whg:{self.ds.pk}:h2", "Hoddesdon", (-0.01, 51.76)),             # its own dataset
                hit(f"whg:{self.other.pk}:o1", "Hoddesdon", (-0.01, 51.76)),          # a public dataset
                hit(f"whg:{self.hidden.pk}:x1", "Hoddesdon", (-0.01, 51.76)),         # a private one
                hit("gn:10", "Hoddesdon", (-0.01, 51.761), types=[GN_TOWN])]
        stranger = get_user_model().objects.create_user(username="cand_str", email="s@example.org",
                                                         password="pw", given_name="S", surname="T")
        kept = [h["place_id"] for h in candidates.filter_hits(hits, self.ds.pk, candidates.subject_id(rec),
                                                              stranger)]
        self.assertEqual(kept, [f"whg:{self.other.pk}:o1", "gn:10"])
        # The owner of the hidden dataset may see it, through the same filter.
        kept_owner = [h["place_id"] for h in candidates.filter_hits(hits, self.ds.pk,
                                                                    candidates.subject_id(rec), self.owner)]
        self.assertEqual(kept_owner, [f"whg:{self.other.pk}:o1", f"whg:{self.hidden.pk}:x1", "gn:10"])
        self.assertTrue(sibling.pk and other.pk and hidden.pk)

    def test_recipe_carries_no_record_country_codes(self):
        rec = self.place("Ware", "r1", (0.03, 51.81))
        rec.ccodes = ["GB"]
        rec.save()
        opts = candidates.search_options(rec, candidates.build_recipe(self.ds))
        self.assertNotIn("countries", opts)
        opts = candidates.search_options(rec, candidates.build_recipe(self.ds, countries=["gb"]))
        self.assertEqual(opts["countries"], ["GB"])


class RecordCandidatesTests(CandidateTestBase):
    def test_silence_is_not_absence(self):
        """A timed-out record is ``unqueried`` with no candidates; the same record becomes
        ``none`` when the gateway answers with nothing."""
        rec = self.place("Ware", "s1", (0.03, 51.81))
        gen = self.generation()
        self.fake.failures["Ware"] = "timeout"
        self.assertEqual(candidates.record_candidates(rec, gen, self.owner), "unqueried")
        st = RecordReviewState.objects.get(place=rec)
        self.assertEqual((st.tier, st.last_failure, st.attempts), ("unqueried", "timeout", 1))
        self.assertEqual(MatchCandidate.objects.filter(place=rec).count(), 0)

        del self.fake.failures["Ware"]                        # the gateway answers: nothing found
        self.assertEqual(candidates.record_candidates(rec, gen, self.owner), "none")
        st.refresh_from_db()
        self.assertEqual((st.tier, st.last_failure, st.attempts), ("none", "", 2))

    def test_answered_record_is_not_queried_again_and_old_suggestions_go_stale(self):
        rec = self.place("Ware", "s2", (0.03, 51.81))
        self.fake.by_query["Ware"] = response(hit("gn:4", "Ware", (0.03, 51.811), types=[GN_TOWN]))
        gen = self.generation()
        self.assertEqual(candidates.record_candidates(rec, gen, self.owner), "confident")
        self.assertIsNone(candidates.record_candidates(rec, gen, self.owner))      # idempotent
        self.assertEqual(len(self.fake.calls), 1)
        c = MatchCandidate.objects.get(place=rec)
        self.assertEqual((c.tier, c.rank, c.status, c.candidate_id), ("confident", 1, "suggested", "gn:4"))
        self.assertIn("exact name", c.explanation["sentence"])
        self.assertEqual(c.signals["type_sanity"], "ok")

        gen2 = self.generation()
        self.assertEqual(candidates.record_candidates(rec, gen2, self.owner), "confident")
        self.assertEqual(MatchCandidate.objects.get(place=rec, generation=gen).status, "stale")
        self.assertEqual(MatchCandidate.objects.get(place=rec, generation=gen2).status, "suggested")
        self.assertEqual(gen.clustering_params_version, "1")


class RunLimitTests(TestCase):
    @override_settings(ENV_CONTEXT="dev-whgazetteer-org", RECON_DEV_RECORD_LIMIT=None)
    def test_dev_without_a_cap_is_refused(self):
        with self.assertRaises(candidates.GenerationRefused):
            candidates.run_limit()

    @override_settings(ENV_CONTEXT="dev-whgazetteer-org", RECON_DEV_RECORD_LIMIT=10)
    def test_dev_with_a_small_cap_runs_capped(self):
        self.assertEqual(candidates.run_limit(), 10)
        self.assertEqual(candidates.run_limit(5000), 10)

    @override_settings(ENV_CONTEXT="dev-whgazetteer-org", RECON_DEV_RECORD_LIMIT=5000)
    def test_dev_with_a_large_cap_is_refused(self):
        with self.assertRaises(candidates.GenerationRefused):
            candidates.run_limit()

    @override_settings(ENV_CONTEXT="whgazetteer-org", RECON_DEV_RECORD_LIMIT=None, RECON_MAX_RECORDS=1000)
    def test_production_needs_no_dev_cap_but_has_a_hard_one(self):
        self.assertEqual(candidates.run_limit(), 1000)
        self.assertEqual(candidates.run_limit(50), 50)
        self.assertEqual(candidates.run_limit(5000), 1000)
