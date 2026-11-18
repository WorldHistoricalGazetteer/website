from django.test import TestCase, RequestFactory
from unittest.mock import patch
from search.views import SearchViewNew  # Adjust the import path as necessary

"""
{'size': 100, 
    'query': {'bool': {'must': [
     {'exists': {'field': 'whg_id'}}, 
     {'multi_match': {'query': 'abydos', 'fields': ['title^3', 'names.toponym', 'searchy']}}
 ]}}
}
{'size': 100, 
    'query': {'bool': {'must': [
    {'exists': {'field': 'whg_id'}}, 
    {'multi_match': {'query': 'abydos', 'fields': ['title^3', 'names.toponym', 'searchy']}}, 
    {'terms': {'ccodes': ['AU', 'NZ']}}
    ]}}
}
v2 fclasses, countries
==
{'size': 100, 
    'query': {'bool': {'must': [
        {'exists': {'field': 'whg_id'}}, 
        {'multi_match': {'query': 'Allepo', 'fields': ['title^3', 'names.toponym', 'searchy']}}, 
        {'terms': {'fclasses': ['X', 'P']}}
    ], 
    'filter': {'geo_shape': {'geoms.location': {'shape': {'type': 'MultiPolygon', 'coordinates': [[]]}, 'relation': 'intersects'}}}}}}
"""
FIELDS = ["title^3", "names.toponym", "searchy"]
HAS_WHG_ID = {"exists": {"field": "whg_id"}}


def wrap(clause):
    """The full query SearchViewNew builds around one search clause (no filters)."""
    return {"size": 100, "query": {"bool": {"must": [HAS_WHG_ID, clause]}}}


# NB SearchViewNew is not routed in search/urls.py (it is only reachable from search.tests and
# from its own handle_request); these tests pin the query it would build.
class SearchViewNewTests(TestCase):

    def build(self, qstr, mode):
        return SearchViewNew.build_search_query({"qstr": qstr, "mode": mode}, mode)

    def test_build_search_query_exact(self):
        # 'exact' (and any unrecognised mode) is a plain multi_match
        self.assertEqual(self.build("Abydos", "exact"),
                         wrap({"multi_match": {"query": "Abydos", "fields": FIELDS}}))

    def test_build_search_query_starts(self):
        self.assertEqual(self.build("Aby", "starts"),
                         wrap({"bool": {"should": [{"prefix": {f: "Aby"}} for f in FIELDS]}}))

    def test_build_search_query_in(self):
        self.assertEqual(self.build("bydos", "in"),
                         wrap({"bool": {"should": [{"wildcard": {f: "*bydos*"}} for f in FIELDS]}}))

    def test_build_search_query_fuzzy(self):
        self.assertEqual(self.build("Abydos", "fuzzy"),
                         wrap({"multi_match": {"query": "Abydos", "fields": FIELDS, "fuzziness": 2}}))

    def test_handle_request_get(self):
        request = RequestFactory().get('/', {'qstr': 'test', 'mode': 'in'})
        request.session = {}
        with patch('search.views.suggester', return_value=[]) as suggester:
            response = SearchViewNew.handle_request(request)

        self.assertEqual(response.status_code, 200)
        import json
        data = json.loads(response.content)
        self.assertEqual(data['suggestions'], [])
        self.assertEqual(data['parameters']['qstr'], 'test')
        self.assertEqual(data['parameters']['mode'], 'in')
        # the query handed to the suggester is the 'in' (wildcard) query
        query = suggester.call_args.args[0]
        self.assertEqual(query['query']['bool']['must'][1],
                         {"bool": {"should": [{"wildcard": {f: "*test*"}} for f in FIELDS]}})
