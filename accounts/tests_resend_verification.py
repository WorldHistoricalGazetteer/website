"""The "Resend Verification Email" button must actually send one.

The template renders it as::

    <button type="submit" name="resend_verification" class="...">

with no ``value``. Per HTML, a submit button with a name and no value submits the **empty
string** — so ``request.POST.get('resend_verification')`` returned ``''``, which is falsy, and
the ``elif`` guarding the resend branch never fired. The view fell through to the GET-rendering
path: HTTP 200, no email, and no error message either. Entirely silent.

Reported 2026-09-10 by a user who had clicked it repeatedly across two days and received
nothing. His request log is the signature: every ``POST /profile/`` returning 200, never the
302 the resend branch always produces when it is reached.

These tests post the button exactly as a browser does — ``{'resend_verification': ''}`` — so a
regression to truthiness-testing fails here rather than in a support ticket.
"""
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.cache import caches
from django.test import TestCase
from django.urls import reverse

User = get_user_model()


# Creating a user fires the welcome-email signal, which calls WHGmail, which mirrors to Zulip —
# and `zulip_notification` posts to the REAL chat.whgazetteer.org even under the test runner.
# Django swaps the email backend to locmem for tests, so no mail escapes, but nothing intercepts
# the Zulip call. Patch it for the whole class so running these tests does not write to the
# production notification stream.
class ResendVerificationTests(TestCase):
    def setUp(self):
        # A class-level @patch decorates test METHODS, not setUp — creating a user here fires the
        # welcome-email signal, which mirrors to the REAL Zulip. Start the patcher in setUp.
        patcher = patch('whgmail.messaging.zulip_notification', return_value=True)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.user = User.objects.create_user(
            username='resend-tester',
            email='someone@example.org',
            password='x' * 20,
            given_name='Resend',
            surname='Tester',
        )
        self.user.email_confirmed = False
        self.user.save()
        self.client.force_login(self.user)
        # The send throttle lives in Redis, which a TestCase rollback does not reset.
        caches['property_cache'].delete_pattern(f"verify-mail*:{self.user.pk}*")

    def _post(self, data):
        with patch('accounts.views.WHGmail') as mail:
            response = self.client.post(reverse('profile-edit'), data)
        return response, mail

    def test_the_button_as_a_browser_sends_it_triggers_an_email(self):
        """The regression itself: name with no value == empty string."""
        response, mail = self._post({'resend_verification': ''})
        self.assertEqual(mail.call_count, 1)
        self.assertEqual(response.status_code, 302)

    def test_a_value_bearing_button_still_works(self):
        """Guard the guard — the fix must not depend on the value being absent."""
        _, mail = self._post({'resend_verification': '1'})
        self.assertEqual(mail.call_count, 1)

    def test_the_email_carries_a_confirmation_url(self):
        _, mail = self._post({'resend_verification': ''})
        context = mail.call_args[0][1]
        self.assertEqual(context['template'], 'email_verification')
        self.assertIn('confirm_url', context)
        self.assertIn('confirm_email=', context['confirm_url'])

    def test_an_unrelated_post_does_not_send(self):
        """Presence-testing must not fire on every POST to this view."""
        _, mail = self._post({'something_else': '1'})
        self.assertEqual(mail.call_count, 0)

    def test_no_email_when_the_address_is_already_confirmed(self):
        self.user.email_confirmed = True
        self.user.save()
        _, mail = self._post({'resend_verification': ''})
        self.assertEqual(mail.call_count, 0)

    def test_no_email_when_the_account_has_no_address(self):
        self.user.email = ''
        self.user.save()
        _, mail = self._post({'resend_verification': ''})
        self.assertEqual(mail.call_count, 0)


class VerificationThrottleTests(ResendVerificationTests):
    """One send per address per two minutes, five per user per hour (2026-10-11: two new users
    each got three verification emails within a minute from double-clicks and early resends)."""

    # Inheriting setUp only; do not re-run the parent's tests under this class.
    test_the_button_as_a_browser_sends_it_triggers_an_email = None
    test_a_value_bearing_button_still_works = None
    test_the_email_carries_a_confirmation_url = None
    test_an_unrelated_post_does_not_send = None
    test_no_email_when_the_address_is_already_confirmed = None
    test_no_email_when_the_account_has_no_address = None

    def test_a_second_resend_within_the_cooldown_is_not_sent(self):
        _, first = self._post({'resend_verification': ''})
        response, second = self._post({'resend_verification': ''})
        self.assertEqual(first.call_count, 1)
        self.assertEqual(second.call_count, 0)
        self.assertEqual(response.status_code, 302)

    def test_a_double_submitted_update_sends_once(self):
        data = {'email_action': 'update', 'email': 'new@example.org'}
        _, first = self._post(data)
        _, second = self._post(data)
        self.assertEqual((first.call_count, second.call_count), (1, 0))
        self.user.refresh_from_db()
        self.assertEqual(self.user.email, 'new@example.org')

    def test_correcting_a_typo_is_not_blocked_by_the_cooldown(self):
        _, first = self._post({'email_action': 'update', 'email': 'tpyo@example.org'})
        _, second = self._post({'email_action': 'update', 'email': 'typo@example.org'})
        self.assertEqual((first.call_count, second.call_count), (1, 1))

    def test_the_hourly_cap_stops_cycling_addresses(self):
        sent = 0
        for i in range(8):
            _, mail = self._post({'email_action': 'update', 'email': f'a{i}@example.org'})
            sent += mail.call_count
        self.assertEqual(sent, 5)
