"""`undated` and `temporal_mode` must reach the gateway's /api/reconcile body with a year window.

Until 2026-10-12 crc_reconcile_search forwarded only start/end, so a year window dropped every
undated place on the gateway whatever the caller asked; Map your Data's "undated places included"
tick reached only the legacy index. Both are additive: without a window, or unset, nothing is sent.
"""
from unittest.mock import patch

from django.test import SimpleTestCase

from api.crc_client import crc_reconcile_search
from api.reconcile import normalise_query_params


class _Resp:
    status_code = 200

    def raise_for_status(self):
        pass

    def json(self):
        return {'hits': []}


def _body_for(raw):
    nq = {'query_text': 'Gayhurst', 'raw': raw, 'size': 10}
    with patch('api.crc_client._is_enabled', return_value=True), \
            patch('api.crc_client.requests.post', return_value=_Resp()) as post:
        crc_reconcile_search(nq, user=None)
    assert post.called, 'the gateway was never called'
    return post.call_args.kwargs['json']


class UndatedForwarding(SimpleTestCase):
    def test_with_a_window(self):
        body = _body_for({'start': 1000, 'end': 1100, 'undated': True})
        self.assertEqual((body['start_year'], body['end_year'], body['undated']), (1000, 1100, True))

    def test_text_spelling_and_open_window(self):
        for raw in ({'start': 1000, 'undated': 'true'}, {'end': 1100, 'undated': '1'}):
            with self.subTest(raw=raw):
                self.assertIs(_body_for(raw)['undated'], True)

    def test_nothing_sent_unless_set_and_windowed(self):
        self.assertIn('undated', _body_for({'start': 1000, 'undated': True}))   # positive control
        for raw in ({'start': 1000}, {'start': 1000, 'undated': False},
                    {'start': 1000, 'undated': 'false'}, {'undated': True}, {}):
            with self.subTest(raw=raw):
                self.assertNotIn('undated', _body_for(raw))

    def test_via_the_normaliser(self):
        nq = normalise_query_params({'query': 'Gayhurst', 'start': 1000, 'end': 1100, 'undated': True})
        with patch('api.crc_client._is_enabled', return_value=True), \
                patch('api.crc_client.requests.post', return_value=_Resp()) as post:
            crc_reconcile_search(nq, user=None)
        self.assertIs(post.call_args.kwargs['json']['undated'], True)


class TemporalModeForwarding(SimpleTestCase):
    def test_recognised_modes(self):
        for mode in ('possibly', 'definitely'):
            with self.subTest(mode=mode):
                self.assertEqual(_body_for({'start': 1000, 'temporal_mode': mode})['temporal_mode'], mode)

    def test_unrecognised_or_unwindowed_sends_nothing(self):
        self.assertIn('temporal_mode', _body_for({'start': 1000, 'temporal_mode': 'definitely'}))
        for raw in ({'start': 1000, 'temporal_mode': 'maybe'}, {'temporal_mode': 'definitely'}):
            with self.subTest(raw=raw):
                self.assertNotIn('temporal_mode', _body_for(raw))
