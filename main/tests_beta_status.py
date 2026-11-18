"""The /development/ status page (main.views.beta_status_view).

Pins the gate (staff only until it is ungated), the required PLATO links, and the
shape of BETA_STATUS_SECTIONS, so an edit to the list cannot break the page quietly.
"""
import re

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from main import views


class BetaStatusPageTests(TestCase):
    def setUp(self):
        User = get_user_model()
        self.staff = User.objects.create_user(username="ds_staff", email="ds-staff@example.org", password="pw",
                                              given_name="Sam", surname="Staff", is_staff=True)
        self.beta = User.objects.create_user(username="ds_beta", email="ds-beta@example.org", password="pw",
                                             given_name="Bea", surname="Tester", role="beta_tester")
        self.url = reverse("beta_status")

    def test_url(self):
        self.assertEqual(self.url, "/development/")

    def test_anonymous_redirected_to_login(self):
        resp = self.client.get(self.url)
        self.assertEqual(resp.status_code, 302)
        self.assertIn("/accounts/login/", resp["Location"])

    def test_non_staff_gets_404(self):
        self.client.force_login(self.beta)
        self.assertEqual(self.client.get(self.url).status_code, 404)

    def test_staff_page_renders_plato_links_and_review_date(self):
        self.client.force_login(self.staff)
        resp = self.client.get(self.url)
        self.assertEqual(resp.status_code, 200)
        html = resp.content.decode()
        self.assertIn("<title>WHG :: Development status</title>", html)
        self.assertIn("last reviewed " + views.BETA_STATUS_UPDATED, html)
        self.assertIn('href="https://plato.pelagios.org/"', html)
        self.assertIn('href="https://plato.pelagios.org/tools/"', html)
        self.assertIn('href="https://plato.pelagios.org/guide/tools.html#map-your-data"', html)
        self.assertIn("Map your Data is moving to PLATO Tools", html)
        # The old GitHub link for PLATO is gone, and so is the old v3.x release targeting.
        self.assertNotIn("github.com/pelagios/place-attestation-ontology", html)
        self.assertIsNone(re.search(r">v3\.[3-6]<", html))
        # Staff see staff-flagged items, badged.
        self.assertIn("An experimental MCP server", html)

    def test_public_view_hides_staff_items(self):
        # What the page will show once ungated: the view drops staff-flagged items for non-staff.
        public = [it["name"] for sec in views.BETA_STATUS_SECTIONS for it in sec["items"] if not it.get("staff")]
        staff_only = [it["name"] for sec in views.BETA_STATUS_SECTIONS for it in sec["items"] if it.get("staff")]
        self.assertIn("Map your Data is moving to PLATO Tools", public)
        self.assertIn("WHG v4.0, and PLATO", public)
        self.assertIn("An experimental MCP server", staff_only)

    def test_sections_are_well_formed(self):
        allowed = {"name", "stage", "body", "version", "staff", "links"}
        self.assertTrue(re.fullmatch(r"\d{1,2} [A-Z][a-z]+ 20\d\d", views.BETA_STATUS_UPDATED))
        for sec in views.BETA_STATUS_SECTIONS:
            for it in sec["items"]:
                with self.subTest(item=it.get("name")):
                    self.assertLessEqual(set(it), allowed)
                    self.assertIn(it["stage"], views._BETA_STAGE_META)
                    self.assertTrue(it["body"].strip())
                    for link in it.get("links", []):
                        self.assertEqual(set(link), {"url", "label"})
                        self.assertTrue(link["url"].startswith(("https://", "/")), link["url"])
