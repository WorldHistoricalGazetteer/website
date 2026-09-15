"""The browser must declare WHICH Symphonym generation built its query vector (place#285).

The in-browser encoder posts an int8 128-d vector on every reconcile query. When the index was
re-embedded to Symphonym v8 while whg3 still shipped the v7 encoder, those vectors were being KNN-ed
against v8 documents: the cosines were meaningless and nothing raised anywhere. The gateway now
honours a client vector only if the request also names the generation it has loaded, and otherwise
discards it and embeds server-side — correct results, no offload.

So the field has to survive the trip. These tests pin that it does, and that its ABSENCE is
preserved as absence: an unstated generation must reach the gateway unstated, because the gateway
treats that as a mismatch and that is the safe outcome. Defaulting it here would re-arm the bug.
"""
from unittest.mock import patch

from django.test import SimpleTestCase

from api.crc_client import crc_reconcile_search


class _Resp:
    status_code = 200

    def raise_for_status(self):
        pass

    def json(self):
        return {'hits': []}


VEC = [1] * 128


def _query(**raw_extra):
    raw = {'embedding': VEC}
    raw.update(raw_extra)
    return {'query_text': 'Exeter', 'raw': raw, 'size': 10}


class QueryVectorModelPassthroughTests(SimpleTestCase):
    def _body(self, query):
        with patch('api.crc_client._is_enabled', return_value=True), \
                patch('api.crc_client.requests.post', return_value=_Resp()) as post:
            crc_reconcile_search(query, user=None)
        self.assertTrue(post.called, 'the gateway was never called')
        return post.call_args.kwargs['json']

    def test_the_declared_generation_reaches_the_gateway(self):
        body = self._body(_query(query_vector_model='v8'))
        self.assertEqual(body['query_vector_model'], 'v8')
        # Control: the vector itself must still be there, or the assertion above is vacuous —
        # a body with no query_vector at all would make the model field meaningless.
        self.assertEqual(len(body['query_vector']), 128)
        self.assertEqual(body['mode'], 'phonetic')

    def test_an_unstated_generation_is_not_invented(self):
        """Absent must stay absent. The gateway reads a missing field as a mismatch and embeds
        server-side; supplying a default here would tell it a generation we cannot vouch for."""
        body = self._body(_query())
        self.assertNotIn('query_vector_model', body)
        self.assertEqual(len(body['query_vector']), 128)

    def test_a_blank_generation_is_treated_as_unstated(self):
        for blank in ('', '   ', None, 123):
            with self.subTest(blank=blank):
                body = self._body(_query(query_vector_model=blank))
                self.assertNotIn('query_vector_model', body)

    def test_the_generation_is_whitespace_trimmed(self):
        body = self._body(_query(query_vector_model='  v8  '))
        self.assertEqual(body['query_vector_model'], 'v8')

    def test_no_vector_means_no_model_field(self):
        """The field describes the vector. Without one it says nothing and must not be sent."""
        body = self._body({'query_text': 'Exeter', 'raw': {'query_vector_model': 'v8'}, 'size': 10})
        self.assertNotIn('query_vector_model', body)
        self.assertNotIn('query_vector', body)
