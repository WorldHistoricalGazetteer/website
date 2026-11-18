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
                _hit(3, "Sóchi", list("abcd"), searchy=["Sóchi"])]
        self.assertEqual(self._run(hits, "SOCHI"), ["Sóchi", "Сочи", "Kochi"])

    def test_title_match_beats_variant_match_beats_partial(self):
        # place#354 screenshots: "rome" -> Roman Empire (variant), "sydney" -> Opera House
        hits = [_hit(1, "Sydney Opera House", list("abc"), searchy=["Sydney Opera House", "Opéra de Sydney"]),
                _hit(2, "Sidney", list("ab"), searchy=["Sidney", "Sydney"]),
                _hit(3, "Sydney", ["a"])]
        self.assertEqual(self._run(hits, "sydney"), ["Sydney", "Sidney", "Sydney Opera House"])

    def test_query_has_unfuzzy_phrase_boost_that_does_not_filter(self):
        from search.views import SearchViewV3
        q = SearchViewV3.build_search_query({"qstr": "Rome"})["query"]["bool"]
        self.assertEqual(q["should"][0]["match_phrase"]["names.toponym.text"]["query"], "Rome")
        self.assertNotIn("minimum_should_match", q)

    def test_linked_variant_match_beats_unlinked_title_matches_when_no_linked_title(self):
        # place#354: Constantinople -> Istanbul (variant, 16 links) over three unlinked namesakes;
        # a fuzzy near-miss with more links (Constanta) must NOT be promoted.
        hits = [_hit(1, "Constantinople", []), _hit(2, "Constantinople", []),
                _hit(3, "Constanta", list("abcdefgh")),
                _hit(4, "Istanbul", list("abcdefghijklmnop"), searchy=["Istanbul", "Constantinople"])]
        self.assertEqual(self._run(hits, "constantinople"),
                         ["Istanbul", "Constantinople", "Constantinople", "Constanta"])

    def test_nothing_promoted_when_a_linked_title_match_exists(self):
        # "rome": Rome IT is linked, so the stray linked variant (Washington) stays below
        # the unlinked titles, as before.
        hits = [_hit(1, "Rome", list("abc")), _hit(2, "Rome", []),
                _hit(3, "Washington", list("ab"), searchy=["Washington", "Rome"])]
        self.assertEqual(self._run(hits, "rome"), ["Rome", "Rome", "Washington"])

    def test_without_qstr_order_is_by_linkcount(self):
        hits = [_hit(1, "A", ["x"]), _hit(2, "B", list("xyz"))]
        self.assertEqual(self._run(hits, None), ["B", "A"])
