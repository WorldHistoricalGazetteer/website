"""A gateway that never answered must not be reported as a gazetteer that found nothing.

`crc_client` records the reason on every failure path, but until now that reason only reached the
client through the `scope` block — i.e. only for a SPATIALLY SCOPED query. An unscoped query whose
gateway call timed out returned `{"result": [], "geojson": None, "namespaces_searched": [...]}`,
byte-identical to a genuine "no such place". Callers banked outages as honest misses, and because
the failure correlates with load it did so hardest on the largest runs.

Found 2026-09-08, when a 19,386-row run cached 78% of its queries as misses during a 12-minute
gateway saturation episode. See place#144 for the scoped half of the contract.
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


class UnscopedGatewayFailureTests(SimpleTestCase):
    """The hole this fixes: no spatial constraint, so the `scope` block never fired."""

    def _run(self, **meta):
        with patch('api.reconcile.crc_reconcile_search', _gateway(**meta)), \
                patch('api.reconcile.hidden_datasets', return_value=_NOTHING_HIDDEN):
            return reconcile_place_es(_query(namespaces='ukhc'))

    def test_timeout_is_reported_to_the_client(self):
        res = self._run(error='timeout')
        self.assertIn('gateway', res)
        self.assertIs(res['gateway']['answered'], False)
        self.assertEqual(res['gateway']['error'], 'timeout')

    def test_every_failure_reason_is_reported(self):
        for reason in ('timeout', 'connection', 'http', 'unexpected'):
            with self.subTest(reason=reason):
                self.assertEqual(self._run(error=reason)['gateway']['error'], reason)

    def test_the_message_does_not_call_it_a_miss(self):
        """The whole point: a human reading this must not conclude the place is absent."""
        message = self._run(error='timeout')['gateway']['message'].lower()
        self.assertIn('did not answer', message)
        self.assertIn('not evidence', message)

    def test_a_failed_gateway_does_not_claim_to_have_searched_anything(self):
        """`namespaces_searched` was filled in from what we ASKED for, asserting a search that never
        ran — and feeding those sources' terms into the root attribution block."""
        res = self._run(error='timeout')
        self.assertNotIn('ukhc', res.get('namespaces_searched', []))


class LiveGatewayTests(SimpleTestCase):
    """Presence of the key IS the failure, so a healthy call must never carry it."""

    def test_a_genuine_empty_result_carries_no_gateway_key(self):
        """The discriminator that did not exist before: the gateway answered, and the answer was
        'nothing'. That IS an honest miss and must stay distinguishable from an outage."""
        with patch('api.reconcile.hidden_datasets', return_value=_NOTHING_HIDDEN), \
                patch('api.reconcile.crc_reconcile_search',
                   _gateway(namespaces_searched=['ukhc'], variants_used=[])):
            res = reconcile_place_es(_query(namespaces='ukhc'))
        self.assertEqual(res['result'], [])
        self.assertNotIn('gateway', res)
        # …and it may honestly report what it searched.
        self.assertIn('ukhc', res['namespaces_searched'])

    def test_a_gateway_that_answered_is_believed_about_its_scope(self):
        with patch('api.reconcile.hidden_datasets', return_value=_NOTHING_HIDDEN), \
                patch('api.reconcile.crc_reconcile_search',
                   _gateway(namespaces_searched=['ukhc', 'kain_par'])):
            res = reconcile_place_es(_query(namespaces='ukhc,kain_par'))
        self.assertEqual(res['namespaces_searched'], ['kain_par', 'ukhc'])


class ScopedGatewayFailureTests(SimpleTestCase):
    """Regression guard: the place#144 behaviour for scoped queries must survive unchanged."""

    def _run(self, **meta):
        with patch('api.reconcile.crc_reconcile_search', _gateway(**meta)), \
                patch('api.reconcile.hidden_datasets', return_value=_NOTHING_HIDDEN):
            return reconcile_place_es(
                _query(namespaces='kain_par', contained_in=['ukhc:KEN'],
                       containment='fuzzy', relation='intersects'))

    def test_scope_is_still_reported_unapplied(self):
        res = self._run(error='timeout')
        self.assertIs(res['scope']['applied'], False)
        self.assertIs(res['scope']['requested'], True)

    def test_scoped_failures_also_carry_the_gateway_key(self):
        """Both signals, so a client can key on one thing regardless of the query's shape."""
        self.assertIs(self._run(error='timeout')['gateway']['answered'], False)
