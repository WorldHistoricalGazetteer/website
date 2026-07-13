"""Phase-4 licence resolution in the gazetteer inventory push.

Exercises ``GazetteerInventoryView._upsert_one`` directly (no HTTP / token
auth needed — the method only touches ``self`` and the entry dict) to cover
the three licence behaviours the indexing-side push depends on:

* ``license_spdx`` resolves to a seeded ``License`` FK;
* an unknown ``license_spdx`` is logged and skipped, never fatal;
* ``license_url`` is stored as an optional deed override.
"""
from django.test import TestCase

from api.models import GazetteerRegistryEntry
from api.views_indexing import GazetteerInventoryView
from licensing.models import License


class InventoryLicenseResolutionTests(TestCase):
    """License rows are seeded by licensing migrations 0002 + 0003."""

    def _upsert(self, **extra):
        entry = {"id": "gn", "namespace": "gn", "name": "GeoNames", **extra}
        ok, err = GazetteerInventoryView()._upsert_one(entry)
        self.assertTrue(ok, err)
        return GazetteerRegistryEntry.objects.get(id="gn")

    def test_known_spdx_resolves_to_fk(self):
        row = self._upsert(license_spdx="CC-BY-4.0")
        self.assertIsNotNone(row.license)
        self.assertEqual(row.license.spdx_id, "CC-BY-4.0")

    def test_extended_spdx_from_0003_resolves(self):
        # Regression guard: the 0003-seeded custom/ND keys must resolve on the
        # push path (they only exist once 0003 is applied to prod).
        row = self._upsert(license_spdx="custom-academic-use")
        self.assertEqual(row.license.spdx_id, "custom-academic-use")

    def test_unknown_spdx_skipped_and_not_fatal(self):
        row = self._upsert(license_spdx="NOT-A-REAL-LICENSE-9.9")
        self.assertIsNone(row.license)  # left unset, entry still upserted

    def test_license_url_deed_override_stored(self):
        row = self._upsert(
            license_spdx="CC-BY-4.0",
            license_url="https://example.org/custom-deed",
        )
        self.assertEqual(row.license_url, "https://example.org/custom-deed")

    def test_citation_text_stored(self):
        row = self._upsert(citation_text="Cite me thus.")
        self.assertEqual(row.citation_text, "Cite me thus.")
