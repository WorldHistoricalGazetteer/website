"""A daily cap must expire at midnight, even for an account that hit it.

`api/authentication.py` used to read `profile.daily_count` directly, but the day rollover lives
inside `increment_usage()` — which the authenticator only reaches AFTER the cap check passes. Once
an account touched its limit, every later request raised before the rollover could run, so
`daily_reset` stayed frozen on the day of exhaustion and `daily_count` was never zeroed. The "daily"
limit became permanent: one account sat locked out for 16 days and 940k refused requests.
"""
from datetime import timedelta
from django.contrib.auth import get_user_model
from django.test import RequestFactory, TestCase
from django.utils import timezone
from rest_framework.exceptions import AuthenticationFailed
from rest_framework.test import APIClient

from api.authentication import DailyLimitExceeded, TokenQueryOrBearerAuthentication
from api.models import APIToken, UserAPIProfile

User = get_user_model()


class DailyLimitRolloverTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(
            username='recon-client', password='x', email='recon@example.org',
            given_name='Recon', surname='Client')
        self.token = APIToken.objects.create(user=self.user, key='k-rollover')
        self.factory = RequestFactory()

    def _authenticate(self):
        request = self.factory.get('/api/', HTTP_AUTHORIZATION=f'Bearer {self.token.key}')
        return TokenQueryOrBearerAuthentication().authenticate(request)

    def test_an_exhausted_account_is_refused_on_the_same_day(self):
        UserAPIProfile.objects.create(
            user=self.user, daily_limit=5000, daily_count=5000,
            daily_reset=timezone.now().date())
        with self.assertRaises(DailyLimitExceeded):
            self._authenticate()

    def test_an_exhausted_account_is_admitted_again_the_next_day(self):
        """The regression: yesterday's exhausted counter must not block today."""
        profile = UserAPIProfile.objects.create(
            user=self.user, daily_limit=5000, daily_count=5000, total_count=7460,
            daily_reset=timezone.now().date() - timedelta(days=1))

        user, _ = self._authenticate()
        self.assertEqual(user, self.user)

        profile.refresh_from_db()
        # The rollover is WRITTEN BACK, not merely computed — otherwise the account re-locks on the
        # next request and the stale date survives forever.
        self.assertEqual(profile.daily_reset, timezone.now().date())
        self.assertEqual(profile.daily_count, 1)
        self.assertEqual(profile.total_count, 7461)

    def test_a_long_stale_reset_date_does_not_survive(self):
        """The observed shape: `daily_reset` frozen 16 days back at limit/limit."""
        profile = UserAPIProfile.objects.create(
            user=self.user, daily_limit=5000, daily_count=5000,
            daily_reset=timezone.now().date() - timedelta(days=16))
        self._authenticate()
        profile.refresh_from_db()
        self.assertEqual(profile.daily_count, 1)

    def test_a_zero_daily_limit_is_unlimited_not_exhausted(self):
        """`remaining_today()` returns None for an uncapped account. `None == 0` is False, so the
        guard must fall through — reading None as 'no allowance' would lock out every staff and
        partner account that was deliberately set to 0."""
        UserAPIProfile.objects.create(user=self.user, daily_limit=0, daily_count=999999)
        user, _ = self._authenticate()
        self.assertEqual(user, self.user)

    def test_the_cap_still_bites_partway_through_a_day(self):
        profile = UserAPIProfile.objects.create(
            user=self.user, daily_limit=3, daily_count=2,
            daily_reset=timezone.now().date())
        self._authenticate()
        profile.refresh_from_db()
        self.assertEqual(profile.daily_count, 3)
        with self.assertRaises(DailyLimitExceeded):
            self._authenticate()

    def test_the_refusal_names_the_limit(self):
        UserAPIProfile.objects.create(
            user=self.user, daily_limit=5000, daily_count=5000,
            daily_reset=timezone.now().date())
        with self.assertRaises(DailyLimitExceeded) as caught:
            self._authenticate()
        self.assertIn('5000', str(caught.exception))


class DailyLimitResponseTests(TestCase):
    """The exhausted cap is a 429 with a machine-readable code and a Retry-After, so a client can
    tell it from a bad token (still 401) without parsing English (plato-tools #22)."""

    def setUp(self):
        self.user = User.objects.create_user(
            username='recon-client', password='x', email='recon@example.org',
            given_name='Recon', surname='Client')
        APIToken.objects.create(user=self.user, key='k-cap')
        UserAPIProfile.objects.create(user=self.user, daily_limit=5000, daily_count=5000,
                                      daily_reset=timezone.now().date())
        self.client = APIClient()

    def _get(self, key):
        return self.client.get('/reconcile', HTTP_AUTHORIZATION=f'Bearer {key}')

    def test_exhausted_cap_is_429_with_code_reset_and_retry_after(self):
        r = self._get('k-cap')
        self.assertEqual(r.status_code, 429)
        body = r.json()
        self.assertEqual(body['code'], 'daily_limit_exceeded')
        self.assertEqual(body['limit'], 5000)
        self.assertIn('5000', body['detail'])
        tomorrow = timezone.now().date() + timedelta(days=1)
        self.assertEqual(body['resets_at'], f'{tomorrow.isoformat()}T00:00:00Z')
        wait = int(r['Retry-After'])
        self.assertTrue(0 < wait <= 86401, wait)
        self.assertNotIn('WWW-Authenticate', r)

    def test_a_bad_token_is_still_401(self):
        self.assertEqual(self._get('k-cap').status_code, 429)          # positive control
        r = self._get('no-such-token')
        self.assertEqual(r.status_code, 401)
        self.assertNotEqual(r.json().get('code'), 'daily_limit_exceeded')
