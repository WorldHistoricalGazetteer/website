"""
Tests for dataset-metadata extraction from an uploaded LPF's top-level `indexing` block.
This is the provenance round-trip: Map-your-Data embeds schema.org metadata (incl. CRediT-tagged
contributors and a formatted citation) which WHG reads back into the Dataset on contribution.
Run: python3 manage.py test validation
"""
import json
import os
import tempfile

from django.test import SimpleTestCase

from validation.views import extract_dataset_metadata


def _write(payload):
    f = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
    json.dump(payload, f)
    f.close()
    return f.name


class ExtractDatasetMetadataTests(SimpleTestCase):
    def tearDown(self):
        for p in getattr(self, '_paths', []):
            try:
                os.remove(p)
            except OSError:
                pass

    def _extract(self, indexing):
        path = _write({'type': 'FeatureCollection', 'indexing': indexing, 'features': []})
        self._paths = getattr(self, '_paths', []) + [path]
        return extract_dataset_metadata(path)

    def test_plain_creators_and_citation(self):
        creator, title, description, webpage, citation = self._extract({
            '@type': 'Dataset',
            'name': 'Markets & Fairs',
            'description': 'Medieval markets.',
            'url': 'https://example.org/mf',
            'creator': [{'@type': 'Person', 'name': 'Gadd, Stephen'},
                        {'@type': 'Organization', 'name': 'WHG'}],
            'citation': 'Gadd, Stephen (2026). Markets & Fairs [Data set]. WHG.',
        })
        self.assertEqual(title, 'Markets & Fairs')
        self.assertEqual(description, 'Medieval markets.')
        self.assertEqual(webpage, 'https://example.org/mf')
        self.assertEqual(creator, 'Gadd, Stephen; WHG')
        self.assertIn('Markets & Fairs', citation)

    def test_credit_role_wrapped_creators_are_deduped(self):
        # A person contributing under two CRediT roles → two Role wrappers → one name.
        role = lambda slug: {'@type': 'Role',
                             'roleName': f'https://credit.niso.org/contributor-roles/{slug}/',
                             'contributor': {'@type': 'Person', 'name': 'Gadd, Stephen'}}
        creator, title, _, _, _ = self._extract({
            '@type': 'Dataset', 'name': 'X',
            'creator': [role('data-curation'), role('software')],
        })
        self.assertEqual(creator, 'Gadd, Stephen')

    def test_single_creator_object(self):
        creator, _, _, _, _ = self._extract({
            '@type': 'Dataset', 'name': 'X',
            'creator': {'@type': 'Person', 'name': 'Solo Author'},
        })
        self.assertEqual(creator, 'Solo Author')

    def test_no_indexing_block_is_empty(self):
        path = _write({'type': 'FeatureCollection', 'features': []})
        self._paths = [path]
        creator, title, description, webpage, citation = extract_dataset_metadata(path)
        self.assertEqual((creator, title, description, webpage, citation), ('', '', '', '', ''))

    def test_citation_is_capped(self):
        _, _, _, _, citation = self._extract({
            '@type': 'Dataset', 'name': 'X', 'citation': 'z' * 5000,
        })
        self.assertEqual(len(citation), 2044)
