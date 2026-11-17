"""The one-question prompt at first API-token issuance (place#349 C7)."""
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from api.models import APIToken, UserAPIProfile

User = get_user_model()


class DeclaredUseTests(TestCase):
    def setUp(self):
        # Creating a user fires the welcome-email signal, which mirrors to the REAL Zulip.
        p = patch('whgmail.messaging.zulip_notification', return_value=True)
        p.start()
        self.addCleanup(p.stop)
        self.user = User.objects.create_user(
            username='use-tester', email='use@example.org', password='x' * 20,
            given_name='Use', surname='Tester')
        self.client.force_login(self.user)
        self.url = reverse('accounts:profile-api-token')

    def test_first_token_requires_an_answer(self):
        r = self.client.post(self.url, {'action': 'generate'})
        self.assertEqual(r.status_code, 400)
        self.assertTrue(r.json().get('needs_use'))
        self.assertFalse(APIToken.objects.filter(user=self.user).exists())
        r = self.client.post(self.url, {'action': 'generate', 'declared_use': 'bogus'})
        self.assertEqual(r.status_code, 400)

    def test_answer_is_recorded_and_token_issued(self):
        r = self.client.post(self.url, {'action': 'generate', 'declared_use': 'research'})
        self.assertEqual(r.status_code, 200)
        self.assertIn('token', r.json())
        prof = UserAPIProfile.objects.get(user=self.user)
        self.assertEqual(prof.declared_use, 'research')
        self.assertIsNotNone(prof.declared_use_at)

    def test_regenerate_and_reissue_do_not_ask_again(self):
        self.client.post(self.url, {'action': 'generate', 'declared_use': 'commercial'})
        r = self.client.post(self.url, {'action': 'generate'})  # regenerate
        self.assertEqual(r.status_code, 200)
        self.client.post(self.url, {'action': 'delete'})
        r = self.client.post(self.url, {'action': 'generate'})  # already declared
        self.assertEqual(r.status_code, 200)
        self.assertEqual(UserAPIProfile.objects.get(user=self.user).declared_use, 'commercial')

    def test_existing_token_holder_can_regenerate_without_declaring(self):
        UserAPIProfile.objects.get_or_create(user=self.user)
        APIToken.objects.create(user=self.user, key='k' * 30)
        r = self.client.post(self.url, {'action': 'generate'})
        self.assertEqual(r.status_code, 200)
