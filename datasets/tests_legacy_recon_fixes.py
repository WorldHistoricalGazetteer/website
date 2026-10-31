"""place#342 data fixes: legacy reconciliation authrecord_id read and whg_id allocation."""
from types import SimpleNamespace
from unittest import mock

from django.test import TestCase

from datasets import services, tasks


def _form(authrecord_id, hit_json):
    return SimpleNamespace(
        is_valid=lambda: True,
        cleaned_data={"match": "closeMatch", "json": hit_json, "authrecord_id": authrecord_id},
        instance=SimpleNamespace(id=7, reviewed=False),
    )


class AuthrecordIdTests(TestCase):
    def _run(self, forms, task_name="align_wdlocal", auth="wdlocal", **kw):
        place = mock.MagicMock(id=1, src_id="s1")
        task = mock.Mock(task_name=task_name, task_id="t1")
        made = {"link": [], "geom": [], "name": []}
        with mock.patch.object(services, "PlaceLink", side_effect=lambda **k: made["link"].append(k) or SimpleNamespace(place_id=1, **k)), \
                mock.patch.object(services, "PlaceGeom", side_effect=lambda **k: made["geom"].append(k) or SimpleNamespace(place_id=1, **k)), \
                mock.patch.object(services, "PlaceName", side_effect=lambda **k: made["name"].append(k) or SimpleNamespace(place_id=1, **k)), \
                mock.patch.object(services, "GEOSGeometry"), \
                mock.patch.object(services, "Hit"), mock.patch.object(services, "Dataset"), \
                mock.patch.object(services, "indexMatch"), mock.patch.object(services, "indexMultiMatch"):
            services._process_matching_decisions(
                mock.Mock(user=None), place, forms, task, auth, "Wikidata", kw, "review_wd", mock.MagicMock())
        return made

    def test_link_and_citations_use_the_hit_authority_id(self):
        # HitRecord JSON as stored by normalize(): the authority id is src_id, there is no authrecord_id key
        hit_json = {"src_id": "Q84", "dataset": "wikidata", "variants": ["Londres@fr"],
                    "geoms": [{"type": "Point", "coordinates": [0, 0]}]}
        made = self._run([_form("Q84", hit_json)], aug_geoms="on", aug_names="on")
        self.assertEqual(made["link"][0]["jsonb"]["identifier"], "wd:Q84")
        self.assertEqual(made["geom"][0]["jsonb"]["citation"]["id"], "wdlocal:Q84")
        self.assertEqual(made["name"][0]["jsonb"]["citations"][0]["id"], "wdlocal:Q84")

    def test_falls_back_to_json_src_id_when_form_field_empty(self):
        made = self._run([_form("", {"src_id": "Q5"})])
        self.assertEqual(made["link"][0]["jsonb"]["identifier"], "wd:Q5")

    def test_geonames_hit_under_wikidata_task_is_cited_as_gn(self):
        hit_json = {"src_id": "1504065", "dataset": "geonames", "variants": ["Kazakhstan"],
                    "geoms": [{"type": "Point", "coordinates": [66, 48]}]}
        form = _form("1504065", hit_json)
        form.cleaned_data["authority"] = "geonames"
        made = self._run([form], aug_geoms="on", aug_names="on")
        self.assertEqual(made["link"][0]["jsonb"]["identifier"], "gn:1504065")
        self.assertEqual(made["geom"][0]["jsonb"]["citation"], {"id": "gn:1504065", "label": "GeoNames"})
        self.assertEqual(made["name"][0]["jsonb"]["citations"][0], {"id": "gn:1504065", "label": "GeoNames"})

    def test_wikidata_hit_still_wd(self):
        form = _form("Q5", {"src_id": "Q5", "dataset": "wikidata"})
        form.cleaned_data["authority"] = "wikidata"
        self.assertEqual(self._run([form])["link"][0]["jsonb"]["identifier"], "wd:Q5")


    def test_whg_hits_write_no_link_or_citation(self):
        # _get_task_details gives auth = task_name[6:] minus 'local': 'whg' (align_whg) or 'idx'
        # (align_idx). Neither enters the wd/tgn branch, so the old json['place_id'] read for
        # whg was dead; prod has 0 place_link rows on the 103 tasks that own WHG hits.
        hit_json = {"whg_id": 12347684, "place_id": 82880, "pid": 82880,
                    "geoms": [{"type": "Point", "coordinates": [0, 0]}], "variants": ["X"]}
        for task_name, auth in (("align_whg", "whg"), ("align_idx", "idx")):
            with self.subTest(task_name):
                made = self._run([_form("12347684", hit_json)], task_name=task_name, auth=auth,
                                 aug_geoms="on", aug_names="on")
                self.assertEqual(made, {"link": [], "geom": [], "name": []})

    def test_link_uses_authrecord_id_not_json_place_id(self):
        made = self._run([_form("Q84", {"src_id": "Q84", "place_id": 999})], auth="wd")
        self.assertEqual(made["link"][0]["jsonb"]["identifier"], "wd:Q84")


class WhgIdAllocationTests(TestCase):
    def test_maxid_is_not_reused_for_first_new_seed(self):
        es = mock.MagicMock()
        es.search.return_value = {"aggregations": {"max_whg_id": {"value": 100.0}}}
        self.assertEqual(tasks.maxID(es, "whg"), 100)
        with mock.patch.object(tasks, "wait_until_es_ready", return_value=True), \
                mock.patch.object(tasks, "es") as es2, \
                mock.patch.object(tasks, "makeDoc", side_effect=lambda p: {"relation": {}}), \
                mock.patch.object(tasks.Place.objects, "get", return_value=mock.MagicMock(names=mock.MagicMock(all=lambda: []))), \
                mock.patch.object(tasks, "streaming_bulk", return_value=[]):
            actions = []
            es2.indices.exists.return_value = True
            with mock.patch.object(tasks, "streaming_bulk", side_effect=lambda es_, acts, **k: actions.extend(acts) or []):
                tasks.batch_new_seeds.run([11, 12], "off", start_id=101)
        ids = [a["_id"] for a in actions if a["_op_type"] == "index"]
        self.assertEqual(ids, ["101", "102"])

    def test_align_idx_new_seeds_do_not_overwrite_the_current_maximum(self):
        """End to end through align_idx -> batch_new_seeds with a fake ES holding whg_id 100:
        the two unmatched places must become 101 and 102, and doc 100 must survive.
        Before the fix align_idx passed start_id=maxID, so the first seed was indexed as _id 100."""

        class FakeES:
            def __init__(self):
                self.docs = {"100": {"whg_id": 100, "title": "existing parent"}}
                self.indices = mock.Mock(exists=mock.Mock(return_value=True))

            def search(self, index, body):
                return {"aggregations": {"max_whg_id": {"value": float(max(d["whg_id"] for d in self.docs.values()))}}}

        fake = FakeES()

        def bulk(es_, acts, **k):
            for a in acts:
                if a["_op_type"] == "index" and a["_index"] == "whg":
                    fake.docs[a["_id"]] = a["_source"]
                yield True, {}

        places = [SimpleNamespace(id=11), SimpleNamespace(id=12)]
        qs = mock.MagicMock(count=lambda: len(places), __iter__=lambda self: iter(places))
        Place = mock.MagicMock()
        Place.DoesNotExist = type("DoesNotExist", (Exception,), {})
        Place.objects.get.side_effect = lambda pk: SimpleNamespace(id=pk, names=mock.MagicMock(all=lambda: []))

        with self.settings(ES_CONN=fake, ES_WHG="whg", ES_PUB="pub"), \
                mock.patch.object(tasks, "es", fake), \
                mock.patch.object(tasks, "Place", Place), \
                mock.patch.object(tasks, "get_object_or_404"), \
                mock.patch.object(tasks, "get_place_queryset", return_value=qs), \
                mock.patch.object(tasks, "build_qobj", return_value={}), \
                mock.patch.object(tasks, "throttled_lookup", return_value={"hits": []}), \
                mock.patch.object(tasks, "finalise_summary", return_value={}), \
                mock.patch.object(tasks, "finalise_task"), \
                mock.patch.object(tasks, "wait_until_es_ready", return_value=True), \
                mock.patch.object(tasks, "makeDoc", side_effect=lambda p: {"relation": {}, "title": f"place {p.id}"}), \
                mock.patch.object(tasks, "streaming_bulk", side_effect=bulk), \
                mock.patch.object(tasks.batch_new_seeds, "delay",
                                  side_effect=lambda *a, **k: tasks.batch_new_seeds.run(*a, **k)):
            tasks.align_idx.run(ds=1, user=1, test="off", bounds=None)

        self.assertEqual(fake.docs["100"]["title"], "existing parent")
        self.assertEqual(sorted(fake.docs, key=int), ["100", "101", "102"])
        self.assertEqual([fake.docs[i]["whg_id"] for i in ("101", "102")], [101, 102])
