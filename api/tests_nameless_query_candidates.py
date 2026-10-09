"""A name-less (pure-spatial) reconcile query must survive matching something.

`normalise_query_params` turns an absent or empty `query` into ``None``:

    query_text = params.get("query", "").strip() or None

…and `make_candidate` then did ``query_text.lower()`` to decide `match`. So the moment a
pure-spatial query produced a candidate, the whole POST raised
``AttributeError: 'NoneType' object has no attribute 'lower'`` and returned 500 — taking the
other queries in the batch with it, because `process_queries._run` catches only `ValueError`.

The shape is documented and supported: `api/schemas.py` says `contained_in` "may be used on its
own (a pure-spatial query, no `query` text)", and the validation in `normalise_query_params`
explicitly admits an empty query that carries `bounds`, a nearby circle, `contained_in`, or a
dataset filter. It had apparently never worked whenever it actually matched.

Found on prod 2026-09-08: an external client enumerating the contents of `ukhc:DVN` sent 22 such
queries and got intermittent 500s — intermittent because the crash needs a HIT, so the same query
looked fine whenever the gateway was timing out and returning nothing.
"""
from unittest.mock import patch

from django.test import SimpleTestCase

from api.reconcile import normalise_query_params, reconcile_place_es
from api.dataset_access import _NOTHING_HIDDEN
from api.reconcile_helpers import make_candidate

SCHEMA_SPACE = 'http://example.org/schema'


def _hit(place_id='kain_par:12345', title='Dartmouth St Petrox'):
    return {
        '_id': place_id,
        '_score': 42.0,
        '_source': {'place_id': place_id, 'title': title,
                    'names': [{'toponym': title}], 'ccodes': ['GB']},
    }


class MakeCandidateNamelessTests(SimpleTestCase):
    def test_a_candidate_survives_a_none_query_text(self):
        cand = make_candidate(_hit(), None, 100.0, SCHEMA_SPACE)
        self.assertEqual(cand['name'], 'Dartmouth St Petrox')

    def test_nothing_is_an_exact_match_when_nothing_was_asked_for(self):
        """`match: true` tells OpenRefine to auto-accept. A pure-spatial query supplied no
        toponym, so there is nothing for a candidate to be exactly equal to, and claiming a
        match would auto-confirm an arbitrary member of the container."""
        self.assertIs(make_candidate(_hit(), None, 100.0, SCHEMA_SPACE)['match'], False)

    def test_an_empty_query_string_is_also_not_a_match(self):
        self.assertIs(make_candidate(_hit(), '', 100.0, SCHEMA_SPACE)['match'], False)

    def test_a_real_query_still_matches_exactly(self):
        """Guard the guard: the fix must not flatten `match` for everyone."""
        cand = make_candidate(_hit(), 'dartmouth st petrox', 100.0, SCHEMA_SPACE)
        self.assertIs(cand['match'], True)


class NamelessSpatialQueryTests(SimpleTestCase):
    """End to end through the view's own helper, which is where the 500 was raised."""

    def _run(self, hits):
        def _fake(nq, user=None, namespaces=None, meta=None):
            if meta is not None:
                meta.update({'namespaces_searched': ['kain_par']})
            return list(hits)
        params = {'query': '', 'namespaces': 'kain_par', 'contained_in': ['ukhc:DVN'],
                  'containment': 'fuzzy', 'relation': 'intersects'}
        with patch('api.reconcile.crc_reconcile_search', _fake), \
                patch('api.reconcile.hidden_datasets', return_value=_NOTHING_HIDDEN):
            return reconcile_place_es(normalise_query_params(params))

    def test_enumerating_a_container_that_holds_places_does_not_raise(self):
        res = self._run([_hit()])
        self.assertEqual(len(res['result']), 1)
        self.assertEqual(res['result'][0]['name'], 'Dartmouth St Petrox')

    def test_the_empty_case_was_never_the_broken_one(self):
        """Why it looked intermittent: with no hits there is no candidate to build, so the
        same query came back clean whenever the gateway was failing."""
        self.assertEqual(self._run([])['result'], [])
