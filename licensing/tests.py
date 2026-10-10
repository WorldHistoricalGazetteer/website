from django.template import Context, Template
from django.test import TestCase

from .models import License


# The rows THIS app's data migrations seed: 8 from 0002, 7 from 0003, 6 from
# 0005 (place#157), 1 from 0006 (place#158). Named rather than counted, deliberately.
#
# A bare `License.objects.count()` is not a claim about the licensing app — it is
# a claim about every app in the project, because the vocabulary is shared and
# other apps seed into it. `phonetics/0008_dual_licence_terms` adds MIT, with
# `contributor_selectable=False` so it stays out of the dataset picker. That is
# correct and intended, and it broke this assertion for the second time. A count
# checked against a hand-maintained comment rots on somebody else's commit; the
# set of ids this app is responsible for does not.
SEEDED_BY_LICENSING = frozenset({
    # 0002_seed_licenses
    "CC-BY-4.0", "CC-BY-3.0", "CC-BY-SA-4.0", "CC-BY-NC-4.0", "CC0-1.0",
    "ODbL-1.0", "ODC-By-1.0", "custom-public-domain",
    # 0003_extend_licenses
    "CC-BY-SA-3.0", "CC-BY-NC-SA-4.0", "CC-BY-NC-ND-3.0", "CC-BY-NC-ND-4.0",
    "CC-BY-3.0-IGO", "custom-all-rights-reserved", "custom-academic-use",
    # 0005_seed_authority_licences
    "custom-nativeland-dst", "custom-historic-counties", "custom-chgis-academic",
    "custom-ukds-eul", "custom-un-geodata", "CC-BY-ND-4.0",
    # 0006_seed_cc_by_nc_25 (place#158: OWTRAD's published terms)
    "CC-BY-NC-2.5",
    # 0007_seed_ogl_yukon (place#158: the Yukon gazetteer's published terms)
    "custom-ogl-yukon-2.0",
})


class LicenseSeedTests(TestCase):
    """The 0002 + 0003 + 0005 + 0006 + 0007 data migrations run as part of test-DB setup, so the
    seeded rows are present without re-running anything here."""

    def test_seed_rows_present(self):
        present = set(License.objects.values_list("spdx_id", flat=True))
        self.assertEqual(
            sorted(SEEDED_BY_LICENSING - present), [],
            "licensing seed rows missing from the vocabulary",
        )

    def test_seed_rows_did_not_collide(self):
        """Every seeded id resolves to its own row.

        `spdx_id` is unique and the seeds use `update_or_create`, so two entries
        sharing an id would silently merge into one row rather than fail — which
        is the failure the old count would genuinely have caught, kept here in a
        form that does not depend on what other apps put in the table.
        """
        self.assertEqual(
            License.objects.filter(spdx_id__in=SEEDED_BY_LICENSING).count(),
            len(SEEDED_BY_LICENSING),
        )

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

    def test_custom_licences_are_not_selectable_unless_explicitly_allowed(self):
        """Belt and braces with the indexing side, which now defaults every
        ``custom-*`` id it emits to non-selectable.

        A ``custom-*`` id is minted to record ONE named source's bespoke terms,
        so offering it to a contributor as a choice for their own data is
        almost always wrong — and the mistake is invisible until someone sees
        the wrong dropdown. Rather than flip the field default (which would
        silently drop the three generic rows below out of the picker), fail
        loudly here: a new custom licence must either be non-selectable or be
        added to this allow-list deliberately.
        """
        GENERICALLY_SELECTABLE = {
            # Not one institution's terms — any contributor might mean these.
            "custom-public-domain",
            "custom-all-rights-reserved",
            "custom-academic-use",
        }
        offered = set(License.objects
                      .filter(custom=True, contributor_selectable=True)
                      .values_list("spdx_id", flat=True))
        unexpected = offered - GENERICALLY_SELECTABLE
        self.assertEqual(
            unexpected, set(),
            "custom licence(s) offered in the contributor picker without being "
            f"declared generically choosable: {sorted(unexpected)}",
        )
