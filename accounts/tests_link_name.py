"""The Profile page control for "Show my name on Links I make" (place#343)."""
from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings

URL = "/accounts/profile/link-name/"


class LinkNameOptInTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(
            username="ln_user", email="ln_user@example.org", password="x", given_name="Lin", surname="Ker")
        self.user.email_confirmed = True                              # the Preferences section needs it
        self.user.save()

    def fresh(self):
        self.user.refresh_from_db()
        return self.user.show_name_on_links

    def test_tick_and_untick(self):
        self.assertFalse(self.fresh())                                # default: not named
        self.client.force_login(self.user)
        r = self.client.post(URL, {"show_name_on_links": "on"})
        self.assertEqual(r.status_code, 302)
        self.assertTrue(self.fresh())
        self.client.post(URL, {})                                     # an unticked box sends nothing
        self.assertFalse(self.fresh())

    def test_signed_out_and_get_change_nothing(self):
        r = self.client.post(URL, {"show_name_on_links": "on"})
        self.assertEqual(r.status_code, 302)
        self.assertIn("login", r["Location"])
        self.client.force_login(self.user)
        self.assertEqual(self.client.get(URL, {"show_name_on_links": "on"}).status_code, 405)
        self.assertFalse(self.fresh())
        self.client.post(URL, {"show_name_on_links": "on"})           # control
        self.assertTrue(self.fresh())

    def test_profile_page_offers_the_control_where_linking_is_open(self):
        self.client.force_login(self.user)
        label = "Show my name on Links I make"
        with override_settings(V4_LANDING_ENABLED=True):
            self.assertIn(label, self.client.get("/profile/").content.decode())
        with override_settings(V4_LANDING_ENABLED=False):
            body = self.client.get("/profile/").content.decode()
            self.assertIn("Receive News and Updates", body)          # the Preferences section rendered
            self.assertNotIn(label, body)
