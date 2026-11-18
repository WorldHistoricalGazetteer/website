"""place#354: the legacy /search/ suggester must put exact-name matches first."""
from unittest import mock

from django.test import SimpleTestCase

from search.views import suggester


def _hit(i, title, children, searchy=None):
    return {"_id": str(i), "_index": "whg",
            "_source": {"title": title, "children": children, "searchy": searchy or [title]}}


class SuggesterExactFirstTests(SimpleTestCase):
    def _run(self, hits, qstr):
        es = mock.Mock()
        es.search.return_value = {"hits": {"hits": hits}}
        with mock.patch("search.views.settings") as st:
            st.ES_CONN = es
            return [s["hit"]["title"] for s in suggester({}, ["whg"], qstr=qstr)]

    def test_exact_beats_better_linked_fuzzy(self):
        hits = [_hit(1, "Helsingborg", list("abcde")), _hit(2, "Helsinki", ["a"])]
        self.assertEqual(self._run(hits, "helsinki"), ["Helsinki", "Helsingborg"])

    def test_variant_and_diacritics_count_as_exact(self):
        hits = [_hit(1, "Kochi", list("abc")),
                _hit(2, "Сочи", ["a"], searchy=["Сочи", "Sochi"]),
                _hit(3, "Sóchi", [], searchy=["Sóchi"])]
        self.assertEqual(self._run(hits, "SOCHI"), ["Сочи", "Sóchi", "Kochi"])

    def test_without_qstr_order_is_by_linkcount(self):
        hits = [_hit(1, "A", ["x"]), _hit(2, "B", list("xyz"))]
        self.assertEqual(self._run(hits, None), ["B", "A"])
