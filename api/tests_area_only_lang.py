"""`area_only` (place#323) and `lang` (place#324) must reach the gateway body.

Both spellings are covered: the native query field and the OpenRefine-style `whg:*` property, which
`normalise_query_params` lifts into the same top-level key. Both are additive: absent, the body must
be exactly what it was before (nothing sent), because the gateway's own default is the old behaviour.
"""
from unittest.mock import patch

from django.test import SimpleTestCase

from api.crc_client import crc_reconcile_search
from api.reconcile import normalise_query_params, PROPERTY_FILTER_MAP


class _Resp:
    status_code = 200

    def raise_for_status(self):
        pass

    def json(self):
        return {'hits': []}


def _body_for(raw):
    nq = {'query_text': 'Blackburn', 'raw': raw, 'size': 10}
    with patch('api.crc_client._is_enabled', return_value=True), \
            patch('api.crc_client.requests.post', return_value=_Resp()) as post:
        crc_reconcile_search(nq, user=None)
    assert post.called, 'the gateway was never called'
    return post.call_args.kwargs['json']


def _body_via_normaliser(params):
    nq = normalise_query_params(dict(params))
    with patch('api.crc_client._is_enabled', return_value=True), \
            patch('api.crc_client.requests.post', return_value=_Resp()) as post:
        crc_reconcile_search(nq, user=None)
    assert post.called
    return post.call_args.kwargs['json']


class AreaOnlyForwarding(SimpleTestCase):
    def test_native_true(self):
        self.assertIs(_body_for({'area_only': True})['area_only'], True)

    def test_native_text_spellings(self):
        for v in ('true', 'True', ' 1 ', 'yes'):
            with self.subTest(v=v):
                self.assertIs(_body_for({'area_only': v})['area_only'], True)

    def test_false_and_absent_send_nothing(self):
        control = _body_for({'area_only': True})
        self.assertIn('area_only', control)           # positive control: the key CAN appear
        for raw in ({}, {'area_only': False}, {'area_only': 'false'}, {'area_only': 0},
                    {'area_only': None}):
            with self.subTest(raw=raw):
                self.assertNotIn('area_only', _body_for(raw))

    def test_whg_property_spelling(self):
        self.assertEqual(PROPERTY_FILTER_MAP['whg:area_only'], 'area_only')
        for v in (True, 'true'):
            body = _body_via_normaliser(
                {'query': 'Blackburn', 'properties': [{'pid': 'whg:area_only', 'v': v}]})
            self.assertIs(body['area_only'], True)
        body = _body_via_normaliser(
            {'query': 'Blackburn', 'properties': [{'pid': 'whg:area_only', 'v': 'false'}]})
        self.assertNotIn('area_only', body)

    def test_top_level_wins_over_property(self):
        body = _body_via_normaliser(
            {'query': 'Blackburn', 'area_only': True,
             'properties': [{'pid': 'whg:area_only', 'v': 'false'}]})
        self.assertIs(body['area_only'], True)


class LangForwarding(SimpleTestCase):
    def test_native(self):
        self.assertEqual(_body_for({'lang': 'en'})['lang'], 'en')

    def test_trimmed_but_not_judged(self):
        # Validation is the gateway's job (malformed -> und); we must not silently drop or fix.
        self.assertEqual(_body_for({'lang': '  EN '})['lang'], 'EN')

    def test_absent_or_blank_sends_nothing(self):
        self.assertIn('lang', _body_for({'lang': 'en'}))   # positive control
        for raw in ({}, {'lang': ''}, {'lang': '  '}, {'lang': None}, {'lang': 5}):
            with self.subTest(raw=raw):
                self.assertNotIn('lang', _body_for(raw))

    def test_whg_property_spelling(self):
        self.assertEqual(PROPERTY_FILTER_MAP['whg:lang'], 'lang')
        body = _body_via_normaliser(
            {'query': 'Blackburn', 'properties': [{'pid': 'whg:lang', 'v': 'cy'}]})
        self.assertEqual(body['lang'], 'cy')

    def test_client_vector_and_lang_travel_together(self):
        body = _body_for({'lang': 'en', 'embedding': [1] * 128, 'query_vector_model': 'v8'})
        self.assertEqual((body['lang'], body['query_vector_model'], len(body['query_vector'])),
                         ('en', 'v8', 128))


class AreaOnlySuppressesLegacyHits(SimpleTestCase):
    """The legacy WHG index has no per-geometry shape flag, so it cannot honour `area_only`; its hits
    would re-enter the ranking unfiltered. Like a spatial scope, the flag therefore suppresses that
    arm when the gateway is serving the query."""

    def _legacy_called(self, params):
        from api.dataset_access import _NOTHING_HIDDEN
        from api.reconcile import reconcile_place_es
        nq = normalise_query_params(dict(params))
        with patch('api.reconcile.crc_reconcile_search', return_value=[]), \
                patch('api.reconcile.es_search', return_value=[]) as legacy, \
                patch('api.reconcile.hidden_datasets', return_value=_NOTHING_HIDDEN):
            reconcile_place_es(nq, user=None)
        return legacy.called

    def test_default_still_searches_legacy(self):
        self.assertTrue(self._legacy_called({'query': 'Blackburn'}))      # control

    def test_area_only_skips_legacy(self):
        self.assertFalse(self._legacy_called({'query': 'Blackburn', 'area_only': True}))
        self.assertFalse(self._legacy_called(
            {'query': 'Blackburn', 'properties': [{'pid': 'whg:area_only', 'v': 'true'}]}))

    def test_area_only_false_string_does_not(self):
        self.assertTrue(self._legacy_called({'query': 'Blackburn', 'area_only': 'false'}))
