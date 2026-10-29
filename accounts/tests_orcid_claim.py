"""Claiming a legacy account at the ORCiD claim step by emailed link (no password).

The properties that matter: the answer is identical whether or not an account matched (this page
is reachable by anyone who can obtain an ORCiD); only a CONFIRMED address is ever mailed; the link
completes the claim only inside the browser session that holds the same pending ORCiD; and it
never sets a password.
"""
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.cache import cache
from django.core.signing import TimestampSigner
from django.test import TestCase, override_settings
from django.urls import reverse

from accounts.views import CLAIM_SALT

User = get_user_model()
from accounts.orcid import build_orcid_profile

PROFILE = build_orcid_profile('0000-0001-2345-6789', {'person': {'name': {
    'given-names': {'value': 'Dan'}, 'family-name': {'value': 'Smith'}}}}, {})


class ClaimByEmailTests(TestCase):
    def setUp(self):
        p = patch('whgmail.messaging.zulip_notification', return_value=True)
        p.start()
        self.addCleanup(p.stop)
        cache.clear()
        self.addCleanup(cache.clear)
        self.legacy = User.objects.create_user(username='oldname', email='old@example.org',
                                               password='oldpass-' + 'x' * 12,
                                               given_name='Old', surname='Name')
        self.legacy.email_confirmed = True
        self.legacy.save()
        self._pending()

    def _pending(self, profile=PROFILE):
        s = self.client.session
        s['pending_orcid'] = dict(profile)
        s.save()

    def _ask(self, identifier):
        with patch('accounts.views.WHGmail') as mail:
            r = self.client.post(reverse('accounts:orcid_claim'),
                                 {'action': 'email', 'identifier': identifier}, follow=True)
        return r, mail

    def _token(self, pk=None, orcid=PROFILE['orcid_id']):
        return TimestampSigner(salt=CLAIM_SALT).sign(f"{pk or self.legacy.pk}:{orcid}")

    # ---- asking ---------------------------------------------------------

    def test_confirmed_account_is_mailed_a_link_by_username_or_email(self):
        for ident in ('oldname', 'OLD@example.org'):
            cache.clear()
            _, mail = self._ask(ident)
            self.assertEqual(mail.call_count, 1, ident)
            ctx = mail.call_args[0][1]
            self.assertEqual(ctx['to_email'], 'old@example.org')
            self.assertIn(reverse('accounts:orcid_claim_confirm') + '?token=', ctx['confirm_url'])
            self.assertFalse(ctx['mirror_to_zulip'])      # a live token must not reach Zulip

    def test_the_answer_is_the_same_whether_or_not_anything_was_sent(self):
        real, sent = self._ask('oldname')
        fake, none = self._ask('nobody-at-all')
        self.assertEqual(sent.call_count, 1)
        self.assertEqual(none.call_count, 0)              # the control: these two really differ
        self.assertEqual([m.message for m in real.context['messages']],
                         [m.message for m in fake.context['messages']])

    def test_an_unconfirmed_address_is_never_mailed(self):
        self.legacy.email_confirmed = False
        self.legacy.save()
        r, mail = self._ask('oldname')
        self.assertEqual(mail.call_count, 0)
        self.assertEqual(len(list(r.context['messages'])), 1)   # still told the same thing

    def test_an_account_already_on_orcid_is_not_mailed(self):
        self.legacy.orcid = '0000-0002-0000-0000'
        self.legacy.save()
        _, mail = self._ask('oldname')
        self.assertEqual(mail.call_count, 0)

    def test_requests_are_rate_limited_per_orcid(self):
        sends = sum(self._ask(f'nobody{i}' if i else 'oldname')[1].call_count for i in range(8))
        cache.clear()
        # the cap is on requests, so a sixth request for a real account is not mailed
        for i in range(5):
            self._ask('nobody')
        _, mail = self._ask('oldname')
        self.assertEqual(sends, 1)
        self.assertEqual(mail.call_count, 0)

    # ---- the link -------------------------------------------------------

    def test_the_link_completes_the_claim_and_sets_no_password(self):
        before = self.legacy.password
        r = self.client.get(reverse('accounts:orcid_claim_confirm'), {'token': self._token()})
        self.legacy.refresh_from_db()
        self.assertEqual(self.legacy.orcid, PROFILE['orcid_id'])
        self.assertTrue(self.legacy.legacy_login_retired)
        self.assertEqual(self.legacy.password, before)
        self.assertEqual(int(self.client.session['_auth_user_id']), self.legacy.pk)
        self.assertNotIn('pending_orcid', self.client.session)

    def test_the_link_does_nothing_without_the_pending_orcid_session(self):
        self.client.logout()
        s = self.client.session
        s.pop('pending_orcid', None)
        s.save()
        r = self.client.get(reverse('accounts:orcid_claim_confirm'), {'token': self._token()})
        self.legacy.refresh_from_db()
        self.assertFalse(self.legacy.orcid)
        self.assertNotIn('_auth_user_id', self.client.session)
        self.assertRedirects(r, reverse('accounts:login'), fetch_redirect_response=False)

    def test_the_link_is_bound_to_the_orcid_that_asked(self):
        self._pending({**PROFILE, 'orcid_id': '0000-0009-9999-9999'})
        self.client.get(reverse('accounts:orcid_claim_confirm'), {'token': self._token()})
        self.legacy.refresh_from_db()
        self.assertFalse(self.legacy.orcid)

    def test_a_tampered_or_expired_token_is_refused(self):
        bad = self._token()[:-3] + 'abc'
        self.client.get(reverse('accounts:orcid_claim_confirm'), {'token': bad})
        self.legacy.refresh_from_db()
        self.assertFalse(self.legacy.orcid)
        fresh = self._token()
        with patch('django.core.signing.time.time', return_value=__import__('time').time() + 2 * 3600):
            self.client.get(reverse('accounts:orcid_claim_confirm'), {'token': fresh})
        self.legacy.refresh_from_db()
        self.assertFalse(self.legacy.orcid)
        # control: the same token, unexpired, DOES claim
        self.client.get(reverse('accounts:orcid_claim_confirm'), {'token': fresh})
        self.legacy.refresh_from_db()
        self.assertEqual(self.legacy.orcid, PROFILE['orcid_id'])

    def test_a_replayed_link_cannot_relink_an_account_already_claimed(self):
        self.client.get(reverse('accounts:orcid_claim_confirm'), {'token': self._token()})
        self.client.logout()
        self._pending({**PROFILE, 'orcid_id': PROFILE['orcid_id']})
        self.client.get(reverse('accounts:orcid_claim_confirm'), {'token': self._token()})
        self.assertEqual(User.objects.filter(orcid=PROFILE['orcid_id']).count(), 1)


class PasswordResetTests(TestCase):
    def setUp(self):
        p = patch('whgmail.messaging.zulip_notification', return_value=True)
        p.start()
        self.addCleanup(p.stop)
        self.u = User.objects.create_user(username='someone', email='someone@example.org', password='p' * 16,
                                            given_name='Some', surname='One')

    def test_the_form_finds_a_user_by_the_hashed_email(self):
        """Control for the bug: Django's own lookup finds nobody against an encrypted column."""
        from django.contrib.auth.forms import PasswordResetForm
        from accounts.views import WHGPasswordResetForm
        self.assertEqual(list(PasswordResetForm().get_users('someone@example.org')), [])
        self.assertEqual(list(WHGPasswordResetForm().get_users('SOMEONE@example.org')), [self.u])

    @override_settings(ORCID_ENFORCED=False)
    def test_where_passwords_are_used_a_reset_really_mails(self):
        from django.core import mail
        with self.settings(EMAIL_BACKEND='django.core.mail.backends.locmem.EmailBackend'):
            self.client.post(reverse('accounts:password_reset'), {'email': 'someone@example.org'})
            self.assertEqual(len(mail.outbox), 1)
            self.client.post(reverse('accounts:password_reset'), {'email': 'nobody@example.org'})
            self.assertEqual(len(mail.outbox), 1)

    @override_settings(ORCID_ENFORCED=True)
    def test_where_orcid_is_enforced_the_reset_is_closed_and_says_so(self):
        r = self.client.get(reverse('accounts:password_reset'), follow=True)
        self.assertRedirects(r, reverse('accounts:login'))
        self.assertIn('no longer uses passwords', ' '.join(m.message for m in r.context['messages']))
        from django.core import mail
        self.client.post(reverse('accounts:password_reset'), {'email': 'someone@example.org'})
        self.assertEqual(len(mail.outbox), 0)
