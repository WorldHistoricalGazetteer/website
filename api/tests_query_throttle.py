"""The reconciliation limiter counts QUERIES, not requests (place#268).

WHG had no rate limiter at all. The design constraint is that a request-rate
limiter throttles exactly the batching our published guidance asks for:

    unbatched      20 req/min x  1 query  =  20 queries/min
    well-batched    2 req/min x 50 queries = 100 queries/min

So the well-behaved client imposes 5x the load at a tenth of the request rate. A
request limiter would punish it and ignore the other.

🛑 Two properties here are easy to get wrong in ways no happy-path test notices:

* the limiter must **fail OPEN** when the cache is unreachable. Failing closed
  turns a Redis blip into the whole reconciliation API returning 429 — a far
  worse incident than the overload being guarded against;
* a caller that ignores 429 must not be able to **reset its own window** by
  retrying, so the charge is applied even when it takes them over.

The cache is a locmem alias for these tests, so nothing touches Redis.
"""

from unittest.mock import patch

from django.core.cache import caches
from django.test import SimpleTestCase, override_settings

from api import throttling
from api.throttling import consume_query_budget


class _U:
    def __init__(self, pk):
        self.pk = pk


LOCMEM = {
    'default': {'BACKEND': 'django.core.cache.backends.locmem.LocMemCache'},
    'throttle': {'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
                 'LOCATION': 'throttle-tests'},
}


@override_settings(CACHES=LOCMEM, RECON_QUERY_RATE=100)
class QueryBudgetTests(SimpleTestCase):

    def setUp(self):
        caches['throttle'].clear()

    def test_a_request_within_budget_is_allowed(self):
        allowed, retry = consume_query_budget(_U(1), 50)
        self.assertTrue(allowed)
        self.assertEqual(retry, 0)

    def test_the_budget_is_spent_in_queries_not_requests(self):
        """🛑 The whole point of the issue. Two requests of 50 exhaust a
        100-query budget; a request-based limiter would see only two requests
        and allow far more."""
        u = _U(2)
        self.assertTrue(consume_query_budget(u, 50)[0])
        self.assertTrue(consume_query_budget(u, 50)[0])
        allowed, retry = consume_query_budget(u, 1)
        self.assertFalse(allowed)
        self.assertGreater(retry, 0)

    def test_many_tiny_requests_are_charged_far_less_than_one_big_one(self):
        """The companion assertion: the well-batched client is not punished. 20
        single-query requests cost 20, not 20 batches' worth."""
        u = _U(3)
        for _ in range(20):
            self.assertTrue(consume_query_budget(u, 1)[0])
        # 20 of 100 spent, so a full 50-query batch still fits.
        self.assertTrue(consume_query_budget(u, 50)[0])

    def test_budgets_are_per_identity(self):
        a, b = _U(10), _U(11)
        self.assertTrue(consume_query_budget(a, 100)[0])
        self.assertFalse(consume_query_budget(a, 1)[0])
        self.assertTrue(consume_query_budget(b, 100)[0],
                        "one caller must not throttle another")

    def test_retrying_does_not_reset_the_window(self):
        """A client ignoring 429 must not clear its own debt by hammering."""
        u = _U(4)
        consume_query_budget(u, 100)
        for _ in range(5):
            self.assertFalse(consume_query_budget(u, 1)[0])

    def test_sessions_without_a_pk_share_one_bucket_and_do_not_crash(self):
        """`CSRFUser` is an AnonymousUser subclass with `is_authenticated` True
        and NO pk. Keying on a missing pk would either crash or give every
        browser caller the same key as every other identity."""
        class _Anon:
            pk = None
        allowed, _ = consume_query_budget(_Anon(), 1)
        self.assertTrue(allowed)
        self.assertEqual(throttling._key(_Anon()), "rq:session")
        self.assertNotEqual(throttling._key(_Anon()), throttling._key(_U(1)))


@override_settings(CACHES=LOCMEM)
class ThrottleDisabledTests(SimpleTestCase):

    @override_settings(RECON_QUERY_RATE=0)
    def test_zero_means_unlimited_not_zero_allowance(self):
        """Matches the convention `UserAPIProfile.daily_limit` established. Read
        the other way, a misconfiguration would lock out every caller — which is
        precisely what place#213's permanent lockout was."""
        caches['throttle'].clear()
        for _ in range(10):
            self.assertTrue(consume_query_budget(_U(5), 10_000)[0])


@override_settings(CACHES=LOCMEM, RECON_QUERY_RATE=100)
class FailOpenTests(SimpleTestCase):
    """🛑 The most important test here, and the one a happy path cannot reach."""

    def test_cache_failure_allows_the_request(self):
        with patch.object(throttling, '_cache') as fake:
            fake.return_value.add.side_effect = RuntimeError("redis is down")
            allowed, retry = consume_query_budget(_U(6), 50)
        self.assertTrue(allowed, "a limiter that cannot count must not take the API down")
        self.assertEqual(retry, 0)

    def test_cache_failure_on_incr_also_allows(self):
        with patch.object(throttling, '_cache') as fake:
            fake.return_value.add.return_value = True
            fake.return_value.incr.side_effect = ValueError("key gone")
            allowed, _ = consume_query_budget(_U(7), 50)
        self.assertTrue(allowed)

    def test_the_fail_open_path_is_not_the_normal_path(self):
        """The control. If `_cache()` were broken in general, every test above
        would pass while the limiter did nothing at all."""
        caches['throttle'].clear()
        u = _U(8)
        self.assertTrue(consume_query_budget(u, 100)[0])
        self.assertFalse(consume_query_budget(u, 1)[0],
                         "with a WORKING cache the limiter must actually deny")


@override_settings(CACHES=LOCMEM, RECON_QUERY_RATE=100)
class ThrottleMessageTests(SimpleTestCase):

    def test_message_tells_the_client_to_keep_batching(self):
        """A client told only "too many requests" reduces its batch size, which
        is the opposite of what we want."""
        msg = throttling.throttle_message(30)
        self.assertIn("100", msg)
        self.assertIn("30", msg)
        self.assertIn("QUERIES", msg)
        self.assertIn("batching", msg)


@override_settings(CACHES=LOCMEM, RECON_QUERY_RATE=20)
class ReconcileViewThrottleTests(SimpleTestCase):
    """End to end through the view: the 429 and its Retry-After header."""

    def setUp(self):
        caches['throttle'].clear()

    def _post(self, n):
        import json
        from unittest.mock import patch as _patch
        from rest_framework.test import APIRequestFactory, force_authenticate
        from api.reconcile import ReconciliationView

        queries = {f"q{i}": {"query": f"P{i}", "mode": "fuzzy"} for i in range(n)}
        request = APIRequestFactory().post(
            "/reconcile", data=json.dumps({"queries": queries}),
            content_type="application/json")

        class _V:
            is_authenticated = True
            is_anonymous = False
            is_active = True
            pk = 99
            id = 99
            username = "throttled"

            def __str__(self):
                return self.username
        force_authenticate(request, user=_V())

        with _patch("api.reconcile.process_queries",
                    side_effect=lambda qs, batch_size=50, user=None: {k: {"result": []} for k in qs}):
            return ReconciliationView.as_view()(request)

    def test_within_budget_is_200(self):
        """The companion half — without it, a change that 429'd everything
        would pass the test below."""
        self.assertEqual(self._post(20).status_code, 200)

    def test_over_budget_is_429_with_retry_after(self):
        self.assertEqual(self._post(20).status_code, 200)
        response = self._post(1)
        self.assertEqual(response.status_code, 429)
        self.assertIn("Retry-After", response)
        self.assertGreater(int(response["Retry-After"]), 0)

    def test_an_oversized_batch_is_rejected_without_being_charged(self):
        """🛑 place#275 and place#268 interact. A 400-rejected batch must not
        spend budget it never used, or one malformed request could throttle a
        caller out of a whole window."""
        self.assertEqual(self._post(500).status_code, 400)
        self.assertEqual(self._post(20).status_code, 200,
                         "the rejected batch must not have been charged")


@override_settings(CACHES=LOCMEM, RECON_QUERY_RATE=100)
class IncrReturnsNoneTests(SimpleTestCase):
    """🛑 The production fail-open path, which the mocked-exception tests missed.

    `cache.incr()` can return None instead of raising:

    * `django_redis` with `IGNORE_EXCEPTIONS` (which the `throttle` alias sets)
      swallows a Redis outage and returns None, so no exception is raised;
    * a backend with no atomic counter can do the same.

    Before the guard, `None > rate` raised `TypeError` — so a Redis blip would
    have 500'd every reconcile POST, which is worse than failing open OR closed.

    ⚠️ Found only by running this file alongside the other API test files, where
    the `throttle` alias was absent and the fallback backend returned None. Every
    exception-based fail-open test passed while this path was wide open.
    """

    def test_none_from_incr_fails_open(self):
        with patch.object(throttling, '_cache') as fake:
            fake.return_value.add.return_value = True
            fake.return_value.incr.return_value = None
            allowed, retry = consume_query_budget(_U(20), 50)
        self.assertTrue(allowed)
        self.assertEqual(retry, 0)

    def test_a_working_backend_still_denies(self):
        """The control: the guard must not swallow real counts."""
        caches['throttle'].clear()
        u = _U(21)
        self.assertTrue(consume_query_budget(u, 100)[0])
        self.assertFalse(consume_query_budget(u, 1)[0])
