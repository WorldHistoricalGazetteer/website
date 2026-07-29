"""Dataset upload-form tests.

Focused on licence capture (place#158): both contribution routes — the legacy
upload page and Map your Data — funnel through ``DatasetUploadForm``, so this is
the single choke point where a contributor's chosen licence is accepted or
rejected before it reaches ``Dataset.license``.
"""
from django.test import TestCase

from datasets.forms import DatasetUploadForm
from licensing.models import License


class LicenceCaptureTests(TestCase):
    """The licence vocabulary is seeded by the licensing data migrations."""

    def _clean(self, value):
        form = DatasetUploadForm(data={'license': value})
        form.is_valid()          # other fields are irrelevant here
        return form.cleaned_data.get('license')

    def test_field_exists(self):
        self.assertIn('license', DatasetUploadForm().fields)

    def test_valid_spdx_id_passes_through(self):
        self.assertEqual(self._clean('CC-BY-4.0'), 'CC-BY-4.0')
        self.assertEqual(self._clean('CC-BY-NC-4.0'), 'CC-BY-NC-4.0')

    def test_blank_is_allowed(self):
        """No licence must never block a contribution — an unrecorded licence is
        recoverable, a failed upload is not."""
        self.assertEqual(self._clean(''), '')

    def test_unknown_id_is_dropped_not_raised(self):
        self.assertEqual(self._clean('NOT-A-LICENCE'), '')
        self.assertTrue(DatasetUploadForm(data={'license': 'NOT-A-LICENCE'}).is_valid()
                        or True)   # never raises; validity turns on other fields

    def test_non_selectable_licence_is_refused(self):
        """The picker never offers these, but `license` is a plain POST value —
        a contributor must not be able to license their own data under one named
        institution's bespoke terms."""
        lic = License.objects.get(spdx_id='custom-ukds-eul')
        self.assertFalse(lic.contributor_selectable)   # guards the premise
        self.assertEqual(self._clean('custom-ukds-eul'), '')

    def test_generic_custom_licence_is_still_accepted(self):
        self.assertEqual(self._clean('custom-public-domain'), 'custom-public-domain')

    def test_whitespace_is_tolerated(self):
        self.assertEqual(self._clean('  CC0-1.0  '), 'CC0-1.0')
