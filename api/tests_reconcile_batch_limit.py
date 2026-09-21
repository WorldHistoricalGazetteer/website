"""An oversized reconcile batch is rejected, not silently truncated (place#275).

`POST /reconcile` accepted at most 50 queries, kept the first 50, discarded the
rest and returned **200**. A client sending 100 got a successful-looking response
and lost half its rows with nothing in the status to say so.

🛑 The `messages` field was never a usable contract, for three independent
reasons, and the tests below pin the replacement rather than the old signal:

1. it is omitted entirely when empty, so absence is the normal case and presence
   cannot be tested for;
2. it sits at the top level alongside the query ids, so a client iterating
   response keys treats it as a query result;
3. the `period` path truncated and appended nothing at all.

Measured on prod before changing the contract (`reconciliation.log`, 12–21 Sep
2026): 325 POSTs, max batch 25, **zero** above 50 and none at exactly 50 — so no
observed client relies on truncation.

⚠️ Every rejection assertion has a companion asserting that a batch AT the limit
still succeeds. A change that rejected everything would pass a suite testing only
the rejection.
"""

import json
from unittest.mock import patch

from django.test import SimpleTestCase
from rest_framework.test import APIRequestFactory, force_authenticate


def _queries(n):
    return {f"q{i}": {"query": f"Place {i}", "mode": "fuzzy"} for i in range(n)}


class ReconcileBatchLimitTests(SimpleTestCase):

    LIMIT = 50

    def _post(self, queries, entity_type=None):
        """Drive the real view. `process_queries` is mocked so nothing reaches
        Elasticsearch or the gateway — the assertions here are about the
        request contract, not about matching."""
        from api.reconcile import ReconciliationView

        payload = {"queries": queries}
        if entity_type:
            for q in payload["queries"].values():
                q["type"] = entity_type
        request = APIRequestFactory().post(
            "/reconcile", data=json.dumps(payload), content_type="application/json")

        # `force_authenticate` rather than setting `request.user`: DRF
        # re-authenticates in `initial()` and would discard it. The view's POST
        # path reads only `is_authenticated`, so a stub avoids the database
        # entirely and keeps this a SimpleTestCase.
        class _U:
            is_authenticated = True
            is_anonymous = False
            is_active = True
            id = 1
            username = "probe"

            def __str__(self):
                return self.username
        force_authenticate(request, user=_U())

        def _fake_process(qs, batch_size=50, user=None):
            return {k: {"result": []} for k in qs}

        with patch("api.reconcile.process_queries", side_effect=_fake_process) as spy:
            response = ReconciliationView.as_view()(request)
        return response, spy

    # ---------- rejection ----------

    def test_over_the_limit_is_400(self):
        response, spy = self._post(_queries(self.LIMIT + 1))
        self.assertEqual(response.status_code, 400)
        spy.assert_not_called()

    def test_rejection_names_both_numbers(self):
        """A 400 that does not say what was received and what is allowed leaves
        the client guessing — which is most of what was wrong with `messages`."""
        response, _ = self._post(_queries(100))
        body = json.loads(response.content)
        text = json.dumps(body)
        self.assertIn("100", text, "must state the count received")
        self.assertIn("50", text, "must state the limit")

    def test_nothing_is_processed_on_rejection(self):
        """🛑 The whole point. Partial work behind an error is the same defect
        wearing a different status code."""
        response, spy = self._post(_queries(75))
        self.assertEqual(response.status_code, 400)
        spy.assert_not_called()
        self.assertNotIn("q0", json.loads(response.content))

    def test_period_path_is_rejected_too(self):
        """The `period` branch truncated with NO message at all — the one path
        with no signal whatsoever. One guard now covers both, so a client cannot
        learn one contract and be surprised by the other."""
        response, _ = self._post(_queries(self.LIMIT + 1), entity_type="period")
        self.assertEqual(response.status_code, 400)

    # ---------- the companion half: the limit still works ----------

    def test_exactly_at_the_limit_succeeds(self):
        """🛑 Stops every rejection test above passing vacuously. 50 is allowed;
        only 51 is not. An off-by-one here silently halves the advertised batch
        size for every integrator."""
        response, spy = self._post(_queries(self.LIMIT))
        self.assertEqual(response.status_code, 200)
        spy.assert_called_once()
        self.assertEqual(len(json.loads(response.content)), self.LIMIT)

    def test_an_ordinary_small_batch_succeeds(self):
        response, spy = self._post(_queries(8))
        self.assertEqual(response.status_code, 200)
        spy.assert_called_once()
        self.assertEqual(len(json.loads(response.content)), 8)

    def test_a_successful_response_carries_no_truncation_message(self):
        """With rejection in place, `messages` should never appear on the happy
        path — so a client that DID learn to look for it cannot be misled by a
        stale one."""
        response, _ = self._post(_queries(self.LIMIT))
        self.assertNotIn("messages", json.loads(response.content))
