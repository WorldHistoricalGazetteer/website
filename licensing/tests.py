from django.template import Context, Template
from django.test import TestCase

from .models import License


class LicenseSeedTests(TestCase):
    """The 0002 + 0003 data migrations run as part of test-DB setup, so the
    seeded rows are present without re-running anything here."""

    def test_seed_rows_present(self):
        # 8 rows from 0002 + 7 from 0003_extend_licenses
        # + 6 from 0005_seed_authority_licences (place#157).
        self.assertEqual(License.objects.count(), 21)

    def test_extended_custom_and_nd_rows_present(self):
        """The four custom / NoDerivatives keys added by 0003 must resolve —
        the indexing-side inventory push depends on them at prod parity."""
        for spdx in ("CC-BY-NC-ND-3.0", "CC-BY-NC-ND-4.0",
                     "custom-all-rights-reserved", "custom-academic-use"):
            self.assertTrue(
                License.objects.filter(spdx_id=spdx).exists(),
                f"missing seeded licence row {spdx!r}",
            )
        # NoDerivatives / all-rights-reserved forbid commercial reuse.
        self.assertFalse(
            License.objects.get(spdx_id="CC-BY-NC-ND-4.0").permits_commercial)
        self.assertTrue(
            License.objects.get(spdx_id="custom-academic-use").custom)

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


class TriStateAndSelectableTests(TestCase):
    """place#157: the licence flags must be able to say "unknown" without that
    being read as either a permission or a prohibition."""

    def test_un_geodata_seeded_with_unknown_commercial(self):
        lic = License.objects.get(spdx_id="custom-un-geodata")
        self.assertIsNone(lic.permits_commercial)
        self.assertIsNone(lic.no_derivatives)
        self.assertTrue(lic.attribution_required)

    def test_unknown_commercial_is_not_categorised_as_noncommercial(self):
        """The bug this guards: `not None` is True, so an unstated grant would
        silently fall into the non-commercial bucket."""
        from licensing.catalog import _category
        self.assertEqual(_category(License.objects.get(spdx_id="custom-un-geodata")),
                         "unspecified")

    def test_no_derivatives_backfilled_for_existing_rows(self):
        self.assertTrue(License.objects.get(spdx_id="CC-BY-NC-ND-4.0").no_derivatives)
        self.assertFalse(License.objects.get(spdx_id="CC-BY-4.0").no_derivatives)
        self.assertIsNone(License.objects.get(spdx_id="custom-public-domain").no_derivatives)

    def test_source_specific_terms_are_not_offered_to_contributors(self):
        for spdx in ("custom-ukds-eul", "custom-chgis-academic",
                     "custom-nativeland-dst", "custom-historic-counties",
                     "custom-un-geodata"):
            self.assertFalse(License.objects.get(spdx_id=spdx).contributor_selectable,
                             f"{spdx} must not be offered as a contributor choice")

    def test_standard_licences_remain_selectable(self):
        for spdx in ("CC-BY-4.0", "CC0-1.0", "CC-BY-ND-4.0"):
            self.assertTrue(License.objects.get(spdx_id=spdx).contributor_selectable)

    def test_all_authority_declared_licences_exist(self):
        """Every id the indexing team's authority metadata sends must resolve —
        the inventory push silently drops any it can't find."""
        required = {"custom-nativeland-dst", "custom-historic-counties",
                    "custom-chgis-academic", "custom-ukds-eul",
                    "custom-un-geodata", "CC-BY-ND-4.0"}
        found = set(License.objects.filter(spdx_id__in=required)
                    .values_list("spdx_id", flat=True))
        self.assertEqual(found, required, f"missing: {required - found}")

    def test_catalog_entry_exposes_selectable_and_tristate(self):
        from licensing.catalog import license_entry
        e = license_entry(License.objects.get(spdx_id="custom-un-geodata"))
        self.assertIs(e["contributor_selectable"], False)
        self.assertIsNone(e["permits_commercial"])
        self.assertIsNone(e["no_derivatives"])
