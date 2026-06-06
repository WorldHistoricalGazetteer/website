from django.template import Context, Template
from django.test import TestCase

from .models import License
from .rights import datacite_rights_list, rights_statement_text


class _Obj:
    """Lightweight stand-in for a Dataset/Collection (avoids the real models'
    save signals, which call DataCite). Only the attrs the helper reads."""
    def __init__(self, license=None, rights_statement=""):
        self.license = license
        self.rights_statement = rights_statement


class Dataset(_Obj):
    pass


class LicenseSeedTests(TestCase):
    """The 0002 data migration runs as part of test-DB setup, so the seeded
    rows are present without re-running anything here."""

    def test_seed_rows_present(self):
        # 8 from 0002 + 4 from 0003 (Phase 6: CC-BY-ND-4.0 + 3 custom rows).
        self.assertEqual(License.objects.count(), 12)

    def test_flag_semantics(self):
        self.assertFalse(License.objects.get(spdx_id="CC0-1.0").attribution_required)
        self.assertTrue(License.objects.get(spdx_id="ODbL-1.0").share_alike)
        self.assertFalse(License.objects.get(spdx_id="CC-BY-NC-4.0").permits_commercial)

    def test_phase6_rows(self):
        # The one genuine SPDX addition.
        nd = License.objects.get(spdx_id="CC-BY-ND-4.0")
        self.assertFalse(nd.custom)
        self.assertTrue(nd.permits_commercial)
        # The three bespoke non-SPDX rows.
        for key in ("custom-nativeland-dst", "custom-historic-counties",
                    "custom-chgis-academic"):
            self.assertTrue(License.objects.get(spdx_id=key).custom, key)
        # Non-commercial terms flagged as such; permissive one is not.
        self.assertFalse(License.objects.get(spdx_id="custom-nativeland-dst").permits_commercial)
        self.assertFalse(License.objects.get(spdx_id="custom-chgis-academic").permits_commercial)
        self.assertTrue(License.objects.get(spdx_id="custom-historic-counties").permits_commercial)

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


class _Person:
    def __init__(self, name, orcid=""):
        self._name = name
        self.orcid = orcid

    def __str__(self):
        return self._name


class _Contribution:
    """Stand-in for persons.Contribution for template-render tests."""
    def __init__(self, role, role_display, person, is_corresponding=False):
        self.role = role
        self._role_display = role_display
        self.person = person
        self.is_corresponding = is_corresponding

    def get_role_display(self):
        return self._role_display


class AttributionPartialTests(TestCase):
    """includes/_attribution.html — unified display partial (§4.4)."""
    TPL = '{% include "includes/_attribution.html" %}'

    def _render(self, **ctx):
        return Template(self.TPL).render(Context(ctx)).strip()

    def test_license_badge_without_contributors(self):
        lic = License.objects.get(spdx_id="CC-BY-4.0")
        html = self._render(license=lic)
        self.assertIn("CC-BY-4.0", html)
        self.assertNotIn("Credited contributors", html)

    def test_contributors_grouped_by_role(self):
        ed = _Person("A. Editor", orcid="0000-0001-2345-6789")
        wr = _Person("B. Writer")
        contribs = [
            _Contribution("writing-original-draft", "Writing – original draft", wr),
            _Contribution("data-curation", "Data curation", ed, is_corresponding=True),
        ]
        html = self._render(license=None, contributions=contribs)
        self.assertIn("Credited contributors", html)
        self.assertIn("Data curation", html)
        self.assertIn("Writing – original draft", html)
        self.assertIn("A. Editor", html)
        self.assertIn("orcid.org/0000-0001-2345-6789", html)
        self.assertIn("corresponding", html)

    def test_empty_when_nothing_supplied(self):
        self.assertNotIn("Credited contributors", self._render())


class RightsHelperTests(TestCase):
    """licensing.rights — truthful per-object rights (citations §4.2–4.3).
    The WHG overlay (settings.WHG_OVERLAY_LICENSE) is CC-BY-NC-4.0."""

    def test_datacite_source_then_overlay(self):
        lic = License.objects.get(spdx_id="ODbL-1.0")
        rl = datacite_rights_list(Dataset(license=lic))
        self.assertEqual(len(rl), 2)
        self.assertEqual(rl[0]["rightsIdentifier"], "ODbL-1.0")   # source first
        self.assertEqual(rl[1]["rightsIdentifier"], "CC-BY-NC-4.0")  # overlay

    def test_datacite_overlay_only_without_license(self):
        rl = datacite_rights_list(Dataset(license=None))
        self.assertEqual([r["rightsIdentifier"] for r in rl], ["CC-BY-NC-4.0"])

    def test_datacite_ignores_non_license_attr(self):
        # A plain string (e.g. the legacy Link.license CharField) is not a
        # License row and must not be emitted as a source licence.
        rl = datacite_rights_list(Dataset(license="some-string"))
        self.assertEqual([r["rightsIdentifier"] for r in rl], ["CC-BY-NC-4.0"])

    def test_statement_names_source_and_overlay(self):
        lic = License.objects.get(spdx_id="ODbL-1.0")
        txt = rights_statement_text(Dataset(license=lic,
                                           rights_statement="Extra terms."))
        self.assertIn("ODbL-1.0", txt)
        self.assertIn("Extra terms.", txt)
        self.assertIn("CC-BY-NC-4.0", txt)        # overlay still stated
        self.assertNotIn("noncommercial purposes only", txt)  # no false claim

    def test_statement_overlay_only_without_license(self):
        txt = rights_statement_text(Dataset(license=None))
        self.assertNotIn("licensed under", txt)   # no source-licence sentence
        self.assertIn("aggregation licence", txt)
