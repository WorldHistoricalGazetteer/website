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
    def _run(self, forms, **kw):
        place = mock.MagicMock(id=1, src_id="s1")
        task = mock.Mock(task_name="align_wdlocal", task_id="t1")
        made = {"link": [], "geom": [], "name": []}
        with mock.patch.object(services, "PlaceLink", side_effect=lambda **k: made["link"].append(k) or SimpleNamespace(place_id=1, **k)), \
                mock.patch.object(services, "PlaceGeom", side_effect=lambda **k: made["geom"].append(k) or SimpleNamespace(place_id=1, **k)), \
                mock.patch.object(services, "PlaceName", side_effect=lambda **k: made["name"].append(k) or SimpleNamespace(place_id=1, **k)), \
                mock.patch.object(services, "GEOSGeometry"), \
                mock.patch.object(services, "Hit"), mock.patch.object(services, "Dataset"):
            services._process_matching_decisions(
                mock.Mock(user=None), place, forms, task, "wdlocal", "Wikidata", kw, "review_wd", mock.MagicMock())
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

    def test_align_idx_passes_first_free_id(self):
        import inspect
        src = inspect.getsource(tasks.align_idx)
        self.assertIn("start_id=whg_id + 1", src)
