from django.test import TestCase

from api.attribution import (
    attribution_for, attribution_block, namespaces_from_ids, safe_attribution_block,
)
from api.models import GazetteerRegistryEntry
from licensing.models import License


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

    # ── place#157: the aggregated block must state TERMS, not just names ──

    def test_attribution_for_includes_licence_object(self):
        """The point of the block: a consumer must be able to read the licence
        and the two flags it has to act on, without a second lookup."""
        GazetteerRegistryEntry.objects.filter(namespace='gn').update(
            license=License.objects.get(spdx_id='CC-BY-4.0'))
        lic = attribution_for(['gn'])['gn']['license']
        self.assertEqual(lic['spdx_id'], 'CC-BY-4.0')
        self.assertTrue(lic['permits_commercial'])
        self.assertFalse(lic['share_alike'])
        self.assertTrue(lic['attribution_required'])

    def test_share_alike_flag_surfaces(self):
        """ShareAlike is the flag with real downstream consequences, so it must
        come through truthfully rather than defaulting to False."""
        GazetteerRegistryEntry.objects.filter(namespace='gn').update(
            license=License.objects.get(spdx_id='ODbL-1.0'))
        lic = attribution_for(['gn'])['gn']['license']
        self.assertTrue(lic['permits_commercial'])
        self.assertTrue(lic['share_alike'])

    def test_licence_is_none_when_unresolved(self):
        """A source with no resolved licence must report `license: null` rather
        than silently look permissive."""
        GazetteerRegistryEntry.objects.filter(namespace='gn').update(license=None)
        self.assertIsNone(attribution_for(['gn'])['gn']['license'])

    def test_licence_url_override_wins(self):
        """Where a source deviates from the canonical deed, its own URL wins."""
        GazetteerRegistryEntry.objects.filter(namespace='gn').update(
            license=License.objects.get(spdx_id='CC-BY-4.0'),
            license_url='https://example.org/bespoke-deed')
        self.assertEqual(
            attribution_for(['gn'])['gn']['license']['url'],
            'https://example.org/bespoke-deed')

    def test_datasets_key_omitted_when_no_contributed_data(self):
        block = attribution_block(namespaces=['gn'])
        self.assertNotIn('datasets', block)

    def test_safe_block_survives_failure(self):
        """Attribution is supplementary — it must never break a result payload."""
        block = safe_attribution_block(namespaces=object())   # not iterable of str
        self.assertEqual(block['sources'], {})
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
