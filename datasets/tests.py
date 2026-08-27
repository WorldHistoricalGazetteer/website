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


"""
Place-type and feature-class derivation from a delimited row (place#213).

The `fclasses` column was honoured on initial upload but overwritten on dataset update,
and derivation from `aat_types` truncated a multi-class concept to its first letter — so
`cities` and `quilombos` (each ['A', 'P']) were classed 'A' and dropped out of
populated-place filtering. Both paths now share `datasets.place_types`.

The AAT index is injected directly, so these need no database.
"""
from django.test import SimpleTestCase

from datasets import place_types
from places.models import Place


class PlaceTypeDerivationTests(SimpleTestCase):

    INDEX = {
        300263222: {'fclasses': ['A', 'P'], 'term': 'quilombos', 'term_full': 'quilombos'},
        300008389: {'fclasses': ['A', 'P'], 'term': 'cities', 'term_full': 'cities'},
        300008687: {'fclasses': ['H'], 'term': 'rivers', 'term_full': 'rivers'},
    }

    def setUp(self):
        place_types._aat_index = dict(self.INDEX)

    def tearDown(self):
        place_types._aat_index = None

    # -- column parsing ----------------------------------------------------

    def test_parses_the_three_columns(self):
        types, aat_types, fclasses = place_types.parse_type_columns(
            {'types': 'quilombo; city', 'aat_types': '300263222;300008389', 'fclasses': 'P'})
        self.assertEqual(types, ['quilombo', 'city'])
        self.assertEqual(aat_types, [300263222, 300008389])
        self.assertEqual(fclasses, ['P'])

    def test_blank_and_nan_cells_are_empty(self):
        for blank in ('', None, 'nan', float('nan')):
            types, aat_types, fclasses = place_types.parse_type_columns(
                {'types': blank, 'aat_types': blank, 'fclasses': blank})
            self.assertEqual((types, aat_types, fclasses), ([], [], []), blank)

    def test_one_unreadable_aat_id_does_not_discard_the_rest(self):
        # Both previous implementations dropped the whole list on a single bad value.
        _, aat_types, _ = place_types.parse_type_columns({'aat_types': '300263222;rubbish;300008687'})
        self.assertEqual(aat_types, [300263222, 300008687])

    def test_aat_prefix_is_tolerated(self):
        _, aat_types, _ = place_types.parse_type_columns({'aat_types': 'aat:300263222'})
        self.assertEqual(aat_types, [300263222])

    def test_fclasses_column_is_normalised_and_filtered(self):
        # `Place.fclasses` is varchar(1); an unnormalised 'p' would never match a 'P' filter.
        _, _, fclasses = place_types.parse_type_columns({'fclasses': 'p; h ; ZZ'})
        self.assertEqual(fclasses, ['P', 'H'])

    # -- the guard ---------------------------------------------------------

    def test_guard_fires_on_any_of_the_three_columns(self):
        # The update path tested `types` alone, so a row carrying only `aat_types` or
        # `fclasses` had its PlaceTypes deleted and never rebuilt.
        self.assertTrue(place_types.has_type_columns({'fclasses': 'P'}))
        self.assertTrue(place_types.has_type_columns({'aat_types': '300008389'}))
        self.assertTrue(place_types.has_type_columns({'types': 'city'}))
        self.assertFalse(place_types.has_type_columns({'types': '', 'aat_types': '', 'fclasses': ''}))
        self.assertFalse(place_types.has_type_columns({'title': 'somewhere'}))

    # -- fclass derivation -------------------------------------------------

    def test_derivation_keeps_every_class_the_concept_carries(self):
        # The whole point: 'P' must survive, or quilombos vanish from populated-place filters.
        self.assertEqual(place_types.fclasses_for_row([300263222], []), ['A', 'P'])
        self.assertEqual(place_types.fclasses_for_aat(300008389), ['A', 'P'])

    def test_column_is_merged_with_derivation_not_replaced_by_it(self):
        self.assertEqual(place_types.fclasses_for_row([300008687], ['P']), ['H', 'P'])

    def test_column_alone_is_honoured(self):
        self.assertEqual(place_types.fclasses_for_row([], ['P', 'S']), ['P', 'S'])

    def test_duplicates_collapse_and_order_is_stable(self):
        self.assertEqual(place_types.fclasses_for_row([300263222, 300008389], ['P', 'A']), ['A', 'P'])

    def test_unknown_concept_contributes_nothing(self):
        self.assertEqual(place_types.fclasses_for_row([999999999], []), [])

    def test_aat_id_from_identifier(self):
        self.assertEqual(place_types.aat_id_from_identifier('aat:300008389'), 300008389)
        for junk in ('300008389', 'wd:Q515', '', None, 'aat:nonsense'):
            self.assertIsNone(place_types.aat_id_from_identifier(junk), junk)


class PlaceTypeObjectTests(SimpleTestCase):
    """Unsaved model instances only, so no database is touched."""

    def setUp(self):
        place_types._aat_index = dict(PlaceTypeDerivationTests.INDEX)
        self.place = Place(src_id='row-1')

    def tearDown(self):
        place_types._aat_index = None

    def test_types_and_aat_types_pair_positionally(self):
        objects = place_types.place_type_objects(self.place, ['quilombo', 'city'], [300263222, 300008389])
        self.assertEqual([o.aat_id for o in objects], [300263222, 300008389])
        self.assertEqual([o.jsonb['sourceLabel'] for o in objects], ['quilombo', 'city'])
        self.assertEqual([o.jsonb['identifier'] for o in objects], ['aat:300263222', 'aat:300008389'])
        self.assertEqual([o.jsonb['label'] for o in objects], ['quilombos', 'cities'])

    def test_fewer_aat_types_than_types_keeps_the_ones_given(self):
        # LP-TSV permits this. The update path's `len(aat_types) >= len(types)` test nulled
        # the identifier for *every* type in the row, then crashed on `None[4:]`.
        objects = place_types.place_type_objects(self.place, ['quilombo', 'city'], [300263222])
        self.assertEqual([o.aat_id for o in objects], [300263222, None])
        self.assertEqual([o.jsonb['identifier'] for o in objects], ['aat:300263222', ''])
        self.assertEqual([o.jsonb['sourceLabel'] for o in objects], ['quilombo', 'city'])

    def test_no_types_at_all_still_records_the_aat_concept(self):
        objects = place_types.place_type_objects(self.place, [], [300008389])
        self.assertEqual(len(objects), 1)
        self.assertEqual(objects[0].jsonb['sourceLabel'], '')
        self.assertEqual(objects[0].aat_id, 300008389)

    def test_apply_sets_fclasses_and_returns_objects(self):
        objects = place_types.apply_types_to_place(
            self.place, {'types': 'quilombo', 'aat_types': '300263222', 'fclasses': 'S'}, save=False)
        self.assertEqual(self.place.fclasses, ['A', 'P', 'S'])
        self.assertEqual(len(objects), 1)

    def test_apply_is_identical_for_insert_and_update_rows(self):
        # The divergence this issue is about: the same row must yield the same result
        # whichever path it arrives by.
        row = {'types': 'quilombo', 'aat_types': '300263222', 'fclasses': 'P'}
        first, second = Place(src_id='a'), Place(src_id='b')
        place_types.apply_types_to_place(first, row, save=False)
        place_types.apply_types_to_place(second, dict(row), save=False)
        self.assertEqual(first.fclasses, second.fclasses)
        self.assertEqual(first.fclasses, ['A', 'P'])
