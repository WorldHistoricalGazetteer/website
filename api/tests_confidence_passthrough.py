"""The gateway's absolute `confidence` must survive the trip to the client (place#206).

`score` is normalised against the best candidate in a response, so the top one reads ~100 whether
the match is perfect or the best of a bad lot. `confidence` is the only field that says how good the
match actually is, and it was being dropped at two points on the way out.
"""
from django.test import TestCase

from api.crc_client import _adapt_hits
from api.reconcile_helpers import make_candidate

SCHEMA_SPACE = 'http://example.org/schema'


def _hit(place_id='gn:745044', title='Exeter', score=99.0, **extra):
    hit = {'place_id': place_id, 'title': title, 'score': score,
           'names': [{'label': title}], 'ccodes': ['GB']}
    hit.update(extra)
    return hit


class AdaptHitsConfidenceTests(TestCase):
    def test_confidence_is_carried_out_of_the_gateway_response(self):
        adapted = _adapt_hits({'hits': [_hit(confidence=40.9)]})
        self.assertEqual(adapted[0]['_confidence'], 40.9)
        # …and kept distinct from the relative score, which is what orders the list.
        self.assertEqual(adapted[0]['_score'], 99.0)

    def test_absent_confidence_is_none_not_zero(self):
        """An older gateway, or a non-fuzzy mode, reports nothing. Zero would read as
        'measured, and terrible' and would wrongly block auto-confirm."""
        adapted = _adapt_hits({'hits': [_hit()]})
        self.assertIsNone(adapted[0]['_confidence'])


class MakeCandidateConfidenceTests(TestCase):
    def _candidate(self, **kw):
        hit = _adapt_hits({'hits': [_hit(**kw)]})[0]
        return make_candidate(hit, 'Exeter', 100.0, SCHEMA_SPACE)

    def test_confidence_reaches_the_candidate(self):
        self.assertEqual(self._candidate(confidence=40.9)['confidence'], 40.9)

    def test_candidate_omits_confidence_when_unmeasured(self):
        """Omitted entirely, so a consumer can tell 'not measured' from 'measured badly'."""
        self.assertNotIn('confidence', self._candidate())

    def test_noise_keeps_a_top_score_of_100_but_low_confidence(self):
        """The place#198 shape: a response whose best candidate is junk still normalises to 100.
        Only `confidence` distinguishes it."""
        hit = _adapt_hits({'hits': [_hit(title='Shams solar power station', score=62.0,
                                           confidence=25.6)]})[0]
        cand = make_candidate(hit, 'Minster-in-Sheppy', 62.0, SCHEMA_SPACE)
        self.assertEqual(cand['score'], 100)
        self.assertEqual(cand['confidence'], 25.6)
