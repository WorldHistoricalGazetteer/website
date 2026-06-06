from django.test import TestCase

from api.attribution import attribution_for, attribution_block, namespaces_from_ids
from api.models import GazetteerRegistryEntry
from api.views_indexing import GazetteerInventoryView
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

    def test_unknown_namespace_omitted(self):
        self.assertNotIn('zz', attribution_for(['zz']))

    def test_block_has_whg_overlay(self):
        block = attribution_block(['gn'])
        self.assertIn('sources', block)
        self.assertEqual(block['whg']['spdx_id'], 'CC-BY-NC-4.0')

    # ── Phase 4 structured attribution ──────────────────────────────────

    def test_citation_text_preferred_over_description(self):
        entry = GazetteerRegistryEntry.objects.filter(
            namespace='gn', entry_class='authority').first()
        entry.description = 'legacy blob'
        entry.citation_text = 'Proper citation.'
        entry.save()
        self.assertEqual(attribution_for(['gn'])['gn']['citation'],
                         'Proper citation.')

    def test_description_fallback_when_no_citation_text(self):
        entry = GazetteerRegistryEntry.objects.filter(
            namespace='gn', entry_class='authority').first()
        entry.description = 'legacy blob'
        entry.citation_text = None
        entry.save()
        self.assertEqual(attribution_for(['gn'])['gn']['citation'],
                         'legacy blob')

    def test_structured_license_and_rights_emitted(self):
        lic = License.objects.get(spdx_id='CC-BY-4.0')
        entry = GazetteerRegistryEntry.objects.filter(
            namespace='gn', entry_class='authority').first()
        entry.license = lic
        entry.rights_holder = 'Unxos GmbH'
        entry.source_url = 'https://www.geonames.org/'
        entry.contributors_csl = [{'name': 'M. Wick', 'role': 'Resources'}]
        entry.save()
        attr = attribution_for(['gn'])['gn']
        self.assertEqual(attr['license']['spdx_id'], 'CC-BY-4.0')
        self.assertEqual(attr['license']['url'], lic.url)
        self.assertEqual(attr['rights_holder'], 'Unxos GmbH')
        self.assertEqual(attr['source_url'], 'https://www.geonames.org/')
        self.assertEqual(attr['contributors'][0]['name'], 'M. Wick')

    def test_license_url_override(self):
        lic = License.objects.get(spdx_id='CC-BY-4.0')
        entry = GazetteerRegistryEntry.objects.filter(
            namespace='gn', entry_class='authority').first()
        entry.license = lic
        entry.license_url = 'https://example.org/custom-deed'
        entry.save()
        self.assertEqual(attribution_for(['gn'])['gn']['license']['url'],
                         'https://example.org/custom-deed')

    def test_optional_keys_absent_when_unset(self):
        # A freshly-seeded row with no Phase 4 fields set omits the optional
        # keys entirely (graceful shape on un-upgraded data).
        entry = GazetteerRegistryEntry.objects.filter(
            namespace='wd', entry_class='authority').first()
        entry.license = None
        entry.license_url = None
        entry.rights_holder = None
        entry.source_url = None
        entry.contributors_csl = []
        entry.save()
        attr = attribution_for(['wd'])['wd']
        for key in ('license', 'rights_holder', 'source_url', 'contributors'):
            self.assertNotIn(key, attr)


class InventoryPushAttributionTests(TestCase):
    """The Batch 11 push (``GazetteerInventoryView._upsert_one``) accepting
    the Phase 4 attribution fields. Tested at the method level — token auth /
    HTTP handling are exercised by the auth class, not this logic."""

    def setUp(self):
        self.view = GazetteerInventoryView()

    def _push(self, **extra):
        entry = {'id': 'gn', 'namespace': 'gn', 'class': 'authority',
                 'name': 'GeoNames'}
        entry.update(extra)
        return self.view._upsert_one(entry)

    def test_resolves_known_license_spdx(self):
        ok, err = self._push(license_spdx='CC-BY-4.0',
                             citation_text='GeoNames database.',
                             rights_holder='Unxos GmbH')
        self.assertTrue(ok, err)
        row = GazetteerRegistryEntry.objects.get(id='gn')
        self.assertEqual(row.license.spdx_id, 'CC-BY-4.0')
        self.assertEqual(row.citation_text, 'GeoNames database.')
        self.assertEqual(row.rights_holder, 'Unxos GmbH')

    def test_unknown_license_spdx_skipped_not_fatal(self):
        ok, err = self._push(license_spdx='NO-SUCH-LICENCE-9.9',
                             citation_text='still stored')
        self.assertTrue(ok, err)
        row = GazetteerRegistryEntry.objects.get(id='gn')
        self.assertIsNone(row.license_id)
        self.assertEqual(row.citation_text, 'still stored')

    def test_contributors_alias_mapped_to_csl(self):
        ok, err = self._push(contributors=[{'name': 'X', 'role': 'Resources'}])
        self.assertTrue(ok, err)
        self.assertEqual(
            GazetteerRegistryEntry.objects.get(id='gn').contributors_csl,
            [{'name': 'X', 'role': 'Resources'}])

    def test_omitted_attribution_left_untouched(self):
        # Seed a value, then push without the field — it must survive.
        self._push(citation_text='keep me')
        ok, err = self._push(record_count=999)
        self.assertTrue(ok, err)
        row = GazetteerRegistryEntry.objects.get(id='gn')
        self.assertEqual(row.citation_text, 'keep me')
        self.assertEqual(row.record_count, 999)


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
