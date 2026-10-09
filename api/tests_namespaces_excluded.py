"""The exclusion the gateway applied reaches the /reconcile caller (place#294, whg3 half).

The gateway excludes `gb` by default — deliberately, 1.17M noisy records — and since indexing
`8901692` it says so in `namespaces_excluded`. Before this, whg3's per-query metadata had no
negative-scope field at all: `gb` was simply absent from `namespaces_searched`, which is also what
"searched and matched nothing" looks like, and a place#249 measurement banked 0/40 on the strength
of it.

The field is ADDITIVE. `namespaces_searched` keeps its meaning (it feeds the root `attribution`
block, place#157) and must not grow the excluded namespace; and the key is present only when the
gateway reported it, so an older gateway or a failed call changes nothing.
"""
from unittest.mock import patch

from django.test import SimpleTestCase

from api.reconcile import normalise_query_params, reconcile_place_es
from api.dataset_access import _NOTHING_HIDDEN


def _query(**params):
    params.setdefault('query', 'Kent')
    return normalise_query_params(params)


def _gateway(hits=(), **meta_updates):
    """Stand in for crc_reconcile_search: fills `meta` the way the real client does."""
    def _fake(normalised_query, user=None, namespaces=None, meta=None):
        if meta is not None:
            meta.update(meta_updates)
        return list(hits)
    return _fake


def _run(**meta):
    with patch('api.reconcile.crc_reconcile_search', _gateway(**meta)), \
            patch('api.reconcile.hidden_datasets', return_value=_NOTHING_HIDDEN):
        return reconcile_place_es(_query(namespaces='ukhc'))


class NamespacesExcludedTests(SimpleTestCase):

    def test_the_applied_exclusion_is_echoed(self):
        res = _run(namespaces_searched=['ukhc'], namespaces_excluded=['gb'])
        self.assertEqual(res['namespaces_excluded'], ['gb'])

    def test_an_overridden_exclusion_is_echoed_as_empty(self):
        """`[]` is information — an explicit scope switched the default off — not absence."""
        res = _run(namespaces_searched=['gb'], namespaces_excluded=[])
        self.assertIn('namespaces_excluded', res)
        self.assertEqual(res['namespaces_excluded'], [])

    def test_an_older_gateway_adds_no_key(self):
        res = _run(namespaces_searched=['ukhc'])
        self.assertNotIn('namespaces_excluded', res)

    def test_a_failed_gateway_adds_no_key(self):
        """Nothing was applied by a call that never answered; claiming an exclusion would be as
        wrong as claiming a search."""
        res = _run(error='timeout')
        self.assertNotIn('namespaces_excluded', res)

    def test_it_is_additive_to_namespaces_searched(self):
        """The whole reason the field exists apart: adding `gb` to `namespaces_searched` would
        break the "queried but matched nothing" signal and credit gb's terms in `attribution`."""
        res = _run(namespaces_searched=['ukhc'], namespaces_excluded=['gb'])
        self.assertNotIn('gb', res['namespaces_searched'])
        self.assertEqual(res['namespaces_searched'], ['ukhc'])

    def test_an_embargoed_namespace_is_not_named_even_as_excluded(self):
        """place#218: a caller who cannot see a gazetteer in /api/sources/ must not learn of it from
        a reconciliation response, whichever list would have carried the name."""
        with patch('api.reconcile.hidden_namespaces', return_value={'secret'}):
            res = _run(namespaces_searched=['ukhc'], namespaces_excluded=['gb', 'secret'])
        self.assertEqual(res['namespaces_excluded'], ['gb'])

    def test_the_existing_per_query_keys_are_unchanged(self):
        """A client validating the fields it already knows sees exactly what it saw before, plus
        one key it may ignore."""
        before = _run(namespaces_searched=['ukhc'], variants_used=[], derived_forms=[])
        after = _run(namespaces_searched=['ukhc'], variants_used=[], derived_forms=[],
                     namespaces_excluded=['gb'])
        self.assertEqual(set(after) - set(before), {'namespaces_excluded'})
        for key in before:
            self.assertEqual(before[key], after[key], key)
