from django.test import TestCase

from api.attribution import attribution_for, attribution_block, namespaces_from_ids
from api.models import GazetteerRegistryEntry


class AttributionHelperTests(TestCase):
    """gn/wd/tgn etc. are seeded as authority rows by api migration 0003."""

    def test_namespaces_from_ids(self):
        self.assertEqual(
            namespaces_from_ids(['gn:745044', 'wd:Q84', '']), {'gn', 'wd'})

    def test_attribution_for_seeded_authorities(self):
        attr = attribution_for(['gn', 'wd'])
        self.assertIn('gn', attr)
        self.assertIn('wd', attr)
        self.assertIn('name', attr['gn'])
        self.assertIn('citation', attr['gn'])

    def test_citation_prefers_citation_text_over_description(self):
        """Phase 4: the structured citation_text wins over the legacy
        description blob once an upgraded inventory push populates it."""
        GazetteerRegistryEntry.objects.filter(namespace='gn').update(
            description='LEGACY description blob',
            citation_text='Structured Phase-4 citation.',
        )
        self.assertEqual(
            attribution_for(['gn'])['gn']['citation'],
            'Structured Phase-4 citation.',
        )

    def test_citation_falls_back_to_description(self):
        """Rows pushed before the upgrade (no citation_text) still surface
        their legacy description."""
        GazetteerRegistryEntry.objects.filter(namespace='gn').update(
            description='LEGACY description blob',
            citation_text=None,
        )
        self.assertEqual(
            attribution_for(['gn'])['gn']['citation'],
            'LEGACY description blob',
        )

    def test_unknown_namespace_omitted(self):
        self.assertNotIn('zz', attribution_for(['zz']))

    def test_block_has_whg_overlay(self):
        block = attribution_block(['gn'])
        self.assertIn('sources', block)
        self.assertEqual(block['whg']['spdx_id'], 'CC-BY-NC-4.0')


class AttributionEndpointTests(TestCase):
    def test_namespaces_param(self):
        r = self.client.get('/api/attribution/?namespaces=gn,wd')
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertIn('gn', data['sources'])
        self.assertIsNotNone(data['whg'])

    def test_ids_param(self):
        r = self.client.get('/api/attribution/?ids=gn:745044')
        self.assertEqual(r.status_code, 200)
        self.assertIn('gn', r.json()['sources'])

    def test_empty(self):
        r = self.client.get('/api/attribution/')
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()['sources'], {})
