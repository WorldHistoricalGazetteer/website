from django.template import Context, Template
from django.test import TestCase

from .models import License


class LicenseSeedTests(TestCase):
    """The 0002 data migration runs as part of test-DB setup, so the seeded
    rows are present without re-running anything here."""

    def test_seed_rows_present(self):
        self.assertEqual(License.objects.count(), 8)

    def test_flag_semantics(self):
        self.assertFalse(License.objects.get(spdx_id="CC0-1.0").attribution_required)
        self.assertTrue(License.objects.get(spdx_id="ODbL-1.0").share_alike)
        self.assertFalse(License.objects.get(spdx_id="CC-BY-NC-4.0").permits_commercial)

    def test_spdx_url(self):
        self.assertEqual(
            License.objects.get(spdx_id="CC-BY-4.0").spdx_url,
            "https://spdx.org/licenses/CC-BY-4.0.html",
        )
        self.assertIsNone(License.objects.get(spdx_id="custom-public-domain").spdx_url)

    def test_str(self):
        self.assertEqual(str(License.objects.get(spdx_id="CC-BY-4.0")), "CC-BY-4.0")


class LicenseBadgeTests(TestCase):
    TPL = '{% include "licensing/_license_badge.html" %}'

    def _render(self, **ctx):
        return Template(self.TPL).render(Context(ctx)).strip()

    def test_renders_link_for_license(self):
        lic = License.objects.get(spdx_id="CC-BY-4.0")
        html = self._render(license=lic, rights_statement="")
        self.assertIn("CC-BY-4.0", html)
        self.assertIn(lic.url, html)

    def test_renders_nothing_without_license(self):
        self.assertEqual(self._render(license=None), "")
