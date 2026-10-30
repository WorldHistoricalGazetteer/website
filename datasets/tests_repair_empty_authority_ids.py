"""place#342: repair_empty_authority_ids classification and --execute."""
import io
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import SimpleTestCase, TestCase

from datasets.management.commands import repair_empty_authority_ids as cmd


def H(i, aid, **js):
    return {"id": i, "authrecord_id": aid, "json": js}


class ClassifierTests(SimpleTestCase):
    def test_link_single_hit_is_a(self):
        self.assertEqual(cmd.classify_link([H(1, "Q1")], [], "wd:"), ("a", "Q1"))

    def test_link_none_is_d(self):
        self.assertEqual(cmd.classify_link([], [], "wd:"), ("d", None))

    def test_link_own_id_among_other_links_is_b(self):
        wd = lambda i, a: H(i, a, dataset="wikidata")
        hits = [wd(1, "Q1"), wd(2, "Q2"), wd(3, "Q3")]
        self.assertEqual(cmd.classify_link(hits, ["wd:Q2", "gn:5"], "wd:"), ("b", "Q2"))
        # an un-evidenced GeoNames hit may have been the matched primary: not guessed
        hits.append(H(4, "77", dataset="geonames"))
        self.assertEqual(cmd.classify_link(hits, ["wd:Q2"], "wd:"), ("c", None))

    def test_link_two_evidenced_or_none_is_c(self):
        hits = [H(1, "Q1"), H(2, "Q2")]
        self.assertEqual(cmd.classify_link(hits, ["wd:Q1", "wd:Q2"], "wd:")[0], "c")
        self.assertEqual(cmd.classify_link(hits, [], "wd:")[0], "c")  # nothing singles one out

    def test_geom_matched_by_coordinates(self):
        g = {"type": "Point", "coordinates": [1, 2]}
        hits = [H(1, "Q1", geoms=[{"type": "Point", "coordinates": [9, 9]}]), H(2, "Q2", geoms=[g])]
        self.assertEqual(cmd.classify_geom(hits, g), ("b", "Q2"))
        self.assertEqual(cmd.classify_geom(hits, {"type": "Point", "coordinates": [5, 5]})[0], "c")

    def test_names_need_all_toponyms(self):
        hits = [H(1, "Q1", dataset="wikidata", variants=["A@fr", "B@de"]),
                H(2, "Q2", dataset="wikidata", variants=["A@fr"])]
        self.assertEqual(cmd.classify_names(hits, ["A", "B"]), ("b", "Q1"))
        self.assertEqual(cmd.classify_names(hits, ["A"])[0], "c")


class PlanExecuteTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        from datasets.models import Dataset, Hit
        from places.models import Place, PlaceGeom, PlaceLink, PlaceName
        user = get_user_model().objects.create_user(
            username="rep", email="rep@example.org", password="pw", given_name="R", surname="P")
        with patch("datasets.signals.doi"):
            ds = Dataset.objects.create(owner=user, label="rep342", title="R", description="D", creator="X",
                                        public=True, ds_status="indexed", uri_base="https://example.org/p/")
        mk = lambda s: Place.objects.create(title=s, src_id=s, dataset=ds, ccodes=[])
        cls.p1, cls.p2, cls.p3 = mk("one"), mk("two"), mk("three")
        hit = lambda p, aid, js: Hit.objects.create(place=p, task_id="T", authority="wikidata", dataset=ds,
                                                    query_pass="pass1", src_id="s", score=1, authrecord_id=aid, json=js)
        # p1: single hit; link + geom + name all bad
        hit(cls.p1, "Q1", {"dataset": "wikidata", "variants": ["Uno@it"],
                           "geoms": [{"type": "Point", "coordinates": [1, 1]}]})
        # p2: two hits, Q2 evidenced by its own extra link
        hit(cls.p2, "Q2", {"dataset": "wikidata"})
        hit(cls.p2, "Q3", {"dataset": "wikidata"})
        # p3: two hits, no evidence -> unrepairable
        hit(cls.p3, "Q4", {"dataset": "wikidata"})
        hit(cls.p3, "Q5", {"dataset": "wikidata"})
        mkl = lambda p, ident: PlaceLink.objects.create(place=p, task_id="T", reviewer=user, src_id=p.src_id,
                                                        jsonb={"type": "closeMatch", "identifier": ident})
        cls.l1, cls.l2, cls.l3 = mkl(cls.p1, "wd:None"), mkl(cls.p2, "wd:None"), mkl(cls.p3, "wd:None")
        mkl(cls.p2, "wd:Q2")
        cls.g1 = PlaceGeom.objects.create(place=cls.p1, task_id="T", reviewer=user, src_id="one",
                                          jsonb={"type": "Point", "citation": {"id": "wd:", "label": "Wikidata"},
                                                 "coordinates": [1, 1]})
        cls.n1 = PlaceName.objects.create(place=cls.p1, task_id="T", src_id="one", toponym="Uno",
                                          jsonb={"toponym": "Uno", "citations": [{"id": "wd:", "label": "Wikidata"}]})

    def run_cmd(self, *args):
        out = io.StringIO()
        from django.core.management import call_command
        call_command("repair_empty_authority_ids", *args, stdout=out)
        return out.getvalue()

    def test_dry_run_reports_and_writes_nothing(self):
        import os, tempfile, csv
        path = os.path.join(tempfile.mkdtemp(), "r.csv")
        out = self.run_cmd("--report", path)
        self.assertIn("DRY RUN", out)
        self.assertRegex(out, r"REPAIRABLE \(a\)\+\(b\)\s+2 of 3")
        self.assertRegex(out, r"delete \(duplicate of real link\)\s*1 of 2")
        self.l1.refresh_from_db()
        self.assertEqual(self.l1.jsonb["identifier"], "wd:None")
        rows = list(csv.reader(open(path)))
        self.assertEqual([r[0] for r in rows[1:]], ["link"])
        self.assertEqual(int(rows[1][3]), self.p3.id)

    def test_execute_refuses_without_backup(self):
        from django.core.management.base import CommandError
        with self.assertRaises(CommandError):
            self.run_cmd("--execute")

    def test_execute_rewrites_deletes_and_backs_up(self):
        import os, tempfile, json as _j
        from places.models import PlaceLink
        bk = os.path.join(tempfile.mkdtemp(), "bk.json")
        out = self.run_cmd("--execute", "--backup", bk)
        self.assertIn("EXECUTED", out)
        self.assertIn(bk, out)
        saved = _j.load(open(bk))
        self.assertEqual(saved["n_rows"], 4)  # l1, l2, g1, n1
        self.assertEqual({r["pk"] for r in saved["rows"] if r["model"] == "places.PlaceLink"}, {self.l1.pk, self.l2.pk})
        self.assertEqual([r for r in saved["rows"] if r["pk"] == self.l2.pk][0]["row"]["jsonb"]["identifier"], "wd:None")
        for o in (self.l1, self.g1, self.n1):
            o.refresh_from_db()
        self.assertEqual(self.l1.jsonb, {"type": "closeMatch", "identifier": "wd:Q1"})
        self.assertFalse(PlaceLink.objects.filter(pk=self.l2.pk).exists())   # duplicate of wd:Q2: deleted
        self.assertTrue(PlaceLink.objects.filter(place=self.p2, jsonb__identifier="wd:Q2").exists())
        self.assertEqual(PlaceLink.objects.get(pk=self.l3.pk).jsonb["identifier"], "wd:None")  # (c) never guessed
        self.assertEqual(self.g1.jsonb["citation"], {"id": "wd:Q1", "label": "Wikidata"})
        self.assertEqual(self.g1.jsonb["coordinates"], [1, 1])
        self.assertEqual(self.n1.jsonb["citations"][0]["id"], "wd:Q1")
        self.assertEqual(self.n1.toponym, "Uno")
        again = self.run_cmd("--execute", "--backup", bk + "2")
        self.assertIn("bad rows after:  {'place_link': 1, 'place_geom': 0, 'place_name': 0}", again)

    def test_sample_is_read_only_and_readable(self):
        out = self.run_cmd("--sample", "3", "--seed", "1")
        self.assertIn("PLANNED: DELETE", out)
        self.assertIn("PLANNED: rewrite", out)
        self.assertIn("**<- chosen**", out)
        self.l1.refresh_from_db()
        self.assertEqual(self.l1.jsonb["identifier"], "wd:None")


class GeoNamesRemapTests(TestCase):
    def test_geonames_hit_gets_gn_prefix_on_link_geom_and_name(self):
        from datasets.models import Dataset, Hit
        from places.models import Place, PlaceGeom, PlaceLink, PlaceName
        user = get_user_model().objects.create_user(
            username="rep2", email="rep2@example.org", password="pw", given_name="R", surname="P")
        with patch("datasets.signals.doi"):
            ds = Dataset.objects.create(owner=user, label="rep342b", title="R", description="D", creator="X",
                                        public=True, ds_status="indexed", uri_base="https://example.org/p/")
        p = Place.objects.create(title="g", src_id="g", dataset=ds, ccodes=[])
        Hit.objects.create(place=p, task_id="T2", authority="geonames", dataset=ds, query_pass="pass1",
                           src_id="s", score=1, authrecord_id="1504065",
                           json={"dataset": "geonames", "variants": ["Kaz"], "geoms": [{"type": "Point", "coordinates": [1, 1]}]})
        l = PlaceLink.objects.create(place=p, task_id="T2", reviewer=user, src_id="g",
                                     jsonb={"type": "closeMatch", "identifier": "wd:None"})
        g = PlaceGeom.objects.create(place=p, task_id="T2", reviewer=user, src_id="g",
                                     jsonb={"type": "Point", "citation": {"id": "wd:", "label": "Wikidata"}, "coordinates": [1, 1]})
        n = PlaceName.objects.create(place=p, task_id="T2", src_id="g", toponym="Kaz",
                                     jsonb={"toponym": "Kaz", "citations": [{"id": "wd:", "label": "Wikidata"}]})
        import os, tempfile
        from django.core.management import call_command
        call_command("repair_empty_authority_ids", "--execute", "--backup", os.path.join(tempfile.mkdtemp(), "b.json"),
                     stdout=io.StringIO())
        for o in (l, g, n):
            o.refresh_from_db()
        self.assertEqual(l.jsonb["identifier"], "gn:1504065")
        self.assertEqual(g.jsonb["citation"], {"id": "gn:1504065", "label": "GeoNames"})
        self.assertEqual(n.jsonb["citations"][0], {"id": "gn:1504065", "label": "GeoNames"})
