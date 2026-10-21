from django.test import TestCase, override_settings
from django.urls import reverse


class PlacePortalMissingIdTests(TestCase):
    """A portal URL naming a place id that does not exist must 404, not 500.

    Regression for 2026-10-05: PlacePortalView.get did a bare Place.objects.get(id=pid)
    and let DoesNotExist propagate as an Internal Server Error (seen on prod for
    /places/portal/6/, which alerted via Sentry and Zulip for an ordinary bad link).
    """

    # Assumes the v4 release switch is off: say so, clearing the alias too, so the test
    # holds where the env sets either (dev sets both; place#325 review #5).
    @override_settings(V4_LANDING_ENABLED=False, LOCUS_MINTING_ENABLED=False)
    def test_portal_by_pid_404s_for_unknown_place(self):
        url = reverse('places:place-portal-pid', args=[999999999])
        response = self.client.get(url)
        self.assertEqual(response.status_code, 404)
