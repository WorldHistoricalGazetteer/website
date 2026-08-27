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

from validation.views import extract_dataset_metadata, parse_to_LPF


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


"""
Coordinate parsing, range-checking and repair for delimited uploads (place#212).
A malformed `lon`/`lat` cell used to vanish silently, leaving a place with no geometry
and a contributor told the file was valid.
"""
from validation import coordinates
from validation.coordinates import resolve_lonlat, check_coordinate_pair, LON_LIMIT, LAT_LIMIT


class ResolveLonLatTests(SimpleTestCase):
    """`ccodes` is left out of every case here: these are the decisions that the ranges
    alone can make, with no database in play."""

    def test_clean_pair_passes_through_untouched(self):
        lon, lat, repairs, errors = resolve_lonlat('-41.363726', '-8.518098')
        self.assertEqual((lon, lat), (-41.363726, -8.518098))
        self.assertEqual((repairs, errors), ([], []))

    def test_numeric_input_is_accepted(self):
        lon, lat, repairs, errors = resolve_lonlat(-41.363726, -8.518098)
        self.assertEqual((lon, lat), (-41.363726, -8.518098))
        self.assertEqual(errors, [])

    def test_both_blank_is_not_an_error(self):
        # A place with no coordinates is legitimate; only a *malformed* one is not.
        self.assertEqual(resolve_lonlat('', ''), (None, None, [], []))
        self.assertEqual(resolve_lonlat(None, None), (None, None, [], []))

    def test_locale_thousands_separators_are_repaired(self):
        # The Brazilian quilombos case: a decimal comma reinterpreted by Excel as
        # thousands separators. Used to return None and be dropped without a word.
        lon, lat, repairs, errors = resolve_lonlat('-8,518,098', '-41,363,726')
        self.assertEqual((lon, lat), (-8.518098, -41.363726))
        self.assertEqual(errors, [])
        self.assertEqual(len(repairs), 2)
        self.assertIn('-8,518,098', repairs[0])

    def test_decimal_comma_is_repaired(self):
        lon, lat, repairs, errors = resolve_lonlat('12,5', '45,7')
        self.assertEqual((lon, lat), (12.5, 45.7))
        self.assertEqual(errors, [])
        self.assertEqual(len(repairs), 2)

    def test_non_breaking_spaces_do_not_defeat_parsing(self):
        lon, lat, _, errors = resolve_lonlat('\u00a0-41.363726 ', ' -8.518098\u00a0')
        self.assertEqual((lon, lat), (-41.363726, -8.518098))
        self.assertEqual(errors, [])

    def test_space_grouped_thousands_are_out_of_range_not_a_locale_artefact(self):
        # '-8 518,098' really is -8518.098; there is no reading of it that is a coordinate.
        _, _, _, errors = resolve_lonlat('-8 518,098', '45')
        self.assertEqual(len(errors), 1)
        self.assertIn('-8518.098', errors[0])
        self.assertIn('outside', errors[0])

    def test_unparseable_value_is_reported_not_dropped(self):
        # The most damaging of the three failure modes precisely because it was silent.
        lon, lat, _, errors = resolve_lonlat('not a number', '45.0')
        self.assertEqual((lon, lat), (None, None))
        self.assertEqual(len(errors), 1)
        self.assertIn('lon', errors[0])
        self.assertIn('not a number', errors[0])

    def test_out_of_range_values_are_rejected_naming_column_and_value(self):
        lon, lat, _, errors = resolve_lonlat('200', '45')
        self.assertEqual((lon, lat), (None, None))
        self.assertIn('lon', errors[0])
        self.assertIn('200', errors[0])

        # 200 is out of range on either axis, so transposition cannot rescue it.
        lon, lat, _, errors = resolve_lonlat('45', '200')
        self.assertEqual((lon, lat), (None, None))
        self.assertIn('lat', errors[0])
        self.assertIn('200', errors[0])

    def test_out_of_range_after_delocaling_is_reported_as_out_of_range(self):
        _, _, _, errors = resolve_lonlat('1,234.56', '12.3')
        self.assertEqual(len(errors), 1)
        self.assertIn('1234.56', errors[0])
        self.assertIn('outside', errors[0])

    def test_only_one_of_the_pair_is_rejected(self):
        # Used to produce a one-element `coordinates` array that blew up at GEOSGeometry,
        # long after the contributor had been told the file was valid.
        _, _, _, errors = resolve_lonlat('', '45.2')
        self.assertEqual(len(errors), 1)
        self.assertIn('lon', errors[0])

        _, _, _, errors = resolve_lonlat('45.2', '')
        self.assertEqual(len(errors), 1)
        self.assertIn('lat', errors[0])

    def test_certain_transposition_is_swapped(self):
        # |lon| <= 90 < |lat| can only be the columns the wrong way round.
        lon, lat, repairs, errors = resolve_lonlat('-8.518098', '-141.363726')
        self.assertEqual((lon, lat), (-141.363726, -8.518098))
        self.assertEqual(errors, [])
        self.assertIn('transposed', repairs[0])

    def test_delocaled_then_transposed_in_that_order(self):
        # De-locale first, then range-check, then test for transposition: a locale
        # artefact would otherwise masquerade as an out-of-range value.
        lon, lat, repairs, errors = resolve_lonlat('-8,518,098', '-141,363,726')
        self.assertEqual((lon, lat), (-141.363726, -8.518098))
        self.assertEqual(errors, [])
        self.assertEqual(len(repairs), 3)  # two de-locales plus the swap

    def test_ambiguous_transposition_is_left_alone_without_ccodes(self):
        # Both values are valid latitudes; nothing here can decide, and guessing would be
        # worse than leaving the contributor's own ordering in place.
        lon, lat, repairs, errors = resolve_lonlat('-8.5', '-41.3')
        self.assertEqual((lon, lat), (-8.5, -41.3))
        self.assertEqual((repairs, errors), ([], []))

    def test_both_wildly_out_of_range_is_two_errors(self):
        _, _, _, errors = resolve_lonlat('-41363726', '-8518098')
        self.assertEqual(len(errors), 2)


class TranspositionByCcodeTests(SimpleTestCase):
    """The ambiguous case: both values are valid latitudes, so only the row's country can
    say which way round they belong. The bbox cache is pre-seeded so no database is needed."""

    BRAZIL = (-73.97943, -33.74068, -34.79735, 5.26722)

    def setUp(self):
        coordinates._bbox_cache['BR'] = self.BRAZIL
        coordinates._bbox_cache['NO_BBOX'] = None

    def tearDown(self):
        coordinates._bbox_cache.clear()

    def test_swap_proposed_when_only_the_swap_falls_in_country(self):
        # The real quilombos row: -8.518098 / -41.363726 as given is in the South Atlantic;
        # swapped it is in Pernambuco.
        lon, lat, repairs, errors = resolve_lonlat('-8.518098', '-41.363726', ccodes=['BR'])
        self.assertEqual((lon, lat), (-41.363726, -8.518098))
        self.assertEqual(errors, [])
        self.assertIn('transposed', repairs[0])

    def test_no_swap_when_the_pair_as_given_is_in_country(self):
        lon, lat, repairs, errors = resolve_lonlat('-41.363726', '-8.518098', ccodes=['BR'])
        self.assertEqual((lon, lat), (-41.363726, -8.518098))
        self.assertEqual((repairs, errors), ([], []))

    def test_no_swap_when_neither_ordering_is_in_country(self):
        # Undecidable: flagging every such row would be noise, and guessing would be worse.
        lon, lat, repairs, errors = resolve_lonlat('10.0', '50.0', ccodes=['BR'])
        self.assertEqual((lon, lat), (10.0, 50.0))
        self.assertEqual((repairs, errors), ([], []))

    def test_missing_bbox_leaves_the_pair_alone(self):
        lon, lat, repairs, errors = resolve_lonlat('-8.518098', '-41.363726', ccodes=['NO_BBOX'])
        self.assertEqual((lon, lat), (-8.518098, -41.363726))
        self.assertEqual((repairs, errors), ([], []))

    def test_delocale_precedes_the_country_check(self):
        lon, lat, repairs, errors = resolve_lonlat('-8,518,098', '-41,363,726', ccodes=['BR'])
        self.assertEqual((lon, lat), (-41.363726, -8.518098))
        self.assertEqual(errors, [])
        self.assertEqual(len(repairs), 3)  # two de-locales plus the swap


class CheckCoordinatePairTests(SimpleTestCase):
    def test_good_position(self):
        self.assertIsNone(check_coordinate_pair([-41.36, -8.51]))

    def test_short_array(self):
        message = check_coordinate_pair([-41.36])
        self.assertIn('1 element', message)

    def test_out_of_range(self):
        message = check_coordinate_pair([-8518098, -41363726])
        self.assertIn('outside', message)

    def test_non_numeric(self):
        self.assertIn('not numbers', check_coordinate_pair(['a', 'b']))

    def test_limits(self):
        self.assertEqual((LON_LIMIT, LAT_LIMIT), (180.0, 90.0))


class ParseToLPFCoordinateTests(SimpleTestCase):
    """
    End-to-end over the delimited-to-LPF conversion. The regression that matters here is
    the silent one: a row whose `lon`/`lat` could not be parsed used to be written out with
    `geometry: null` and no word said about it.
    """

    TSV = "\n".join([
        "id\ttitle\ttitle_source\tlon\tlat\tstart",
        "1\tLocale\tIBGE\t-8,518,098\t-41,363,726\t1888",      # de-localed
        "2\tClean\tIBGE\t-41.363726\t-8.518098\t1888",         # untouched
        "3\tNo coords\tIBGE\t\t\t1888",                        # legitimately locationless
        "4\tJunk\tIBGE\tnorth\t-8.5\t1888",                    # rejected, was silent
        "5\tHalf\tIBGE\t-41.36\t\t1888",                       # rejected, was a 1-element array
        "6\tSwapped\tIBGE\t-8.518098\t-141.363726\t1888",      # transposed
        "7\tExcelInt\tIBGE\t-8518098\t-41363726\t1888",        # rejected as out of range
        "",
    ])

    def _convert(self):
        directory = tempfile.mkdtemp()
        path = os.path.join(directory, 'places.tsv')
        with open(path, 'w') as f:
            f.write(self.TSV)
        lpf_path, count, separator, header, report = parse_to_LPF(path, 'tsv')
        with open(lpf_path) as f:
            features = json.load(f)['features']
        return features, report

    def test_conversion_report_and_geometries(self):
        features, report = self._convert()
        geometries = {f['@id']: f['geometry'] for f in features}

        self.assertEqual(geometries['1']['coordinates'], [-8.518098, -41.363726])
        self.assertEqual(geometries['2']['coordinates'], [-41.363726, -8.518098])
        self.assertEqual(geometries['6']['coordinates'], [-141.363726, -8.518098])
        for src_id in ('3', '4', '5', '7'):
            self.assertIsNone(geometries[src_id], f"row {src_id} should have no geometry")

        self.assertEqual(report['fix_count'], 3)     # two de-locales and one swap
        self.assertEqual(report['error_count'], 4)   # 'north', the half pair, and both of row 7
        self.assertEqual(report['rows_without_geometry'], 4)

        # Every rejection names the row it came from: the contributor should not have to
        # hunt through the file for it.
        self.assertEqual({e['feature_id'] for e in report['errors']}, {'4', '5', '7'})

    def test_unparseable_coordinate_is_no_longer_silent(self):
        _, report = self._convert()
        junk = [e for e in report['errors'] if e['feature_id'] == '4']
        self.assertEqual(len(junk), 1)
        self.assertIn('north', junk[0]['description'])


"""
Feature-class derivation from an LPF feature (place#213).

LPF carries `types` at the FEATURE level, and that is where the delimited-to-LPF conversion
puts a row's `aat_types` — but `get_fclass_list` read `properties.types`, so `aat_types`
contributed no feature class at all on the live upload path.
"""
from datasets import place_types
from validation.create_dataset import get_fclass_list


class GetFclassListTests(SimpleTestCase):

    def setUp(self):
        place_types._aat_index = {
            300263222: {'fclasses': ['A', 'P'], 'term': 'quilombos', 'term_full': 'quilombos'},
            300008687: {'fclasses': ['H'], 'term': 'rivers', 'term_full': 'rivers'},
        }

    def tearDown(self):
        place_types._aat_index = None

    def test_feature_level_types_are_read(self):
        feature = {'properties': {'title': 'Conceição'},
                   'types': [{'identifier': 'aat:300263222'}]}
        self.assertEqual(get_fclass_list(feature), ['A', 'P'])

    def test_properties_fclasses_are_merged_with_derivation(self):
        feature = {'properties': {'fclasses': ['S']},
                   'types': [{'identifier': 'aat:300008687'}]}
        self.assertEqual(get_fclass_list(feature), ['H', 'S'])

    def test_types_nested_under_properties_still_work(self):
        feature = {'properties': {'types': [{'identifier': 'aat:300008687'}]}}
        self.assertEqual(get_fclass_list(feature), ['H'])

    def test_wikidata_identifier_still_maps(self):
        feature = {'properties': {}, 'types': [{'identifier': 'wd:Q515'}]}
        self.assertEqual(get_fclass_list(feature), ['P'])

    def test_no_types_and_no_column_is_empty(self):
        self.assertEqual(get_fclass_list({'properties': {}}), [])

    def test_malformed_type_entry_is_skipped_not_fatal(self):
        feature = {'properties': {}, 'types': ['not a dict', {'identifier': 'aat:300008687'}]}
        self.assertEqual(get_fclass_list(feature), ['H'])


class TypesWithoutAatTypesTests(SimpleTestCase):
    """
    LP-TSV permits a `types` value with no corresponding `aat_types`. The `aat_types`
    column becomes the feature's `types`, so such a row used to keep its labels in
    `additional_types` and end up with no `types` at all — failing the LPF schema's
    "fclasses or types" requirement and blocking legitimate input (place#213).
    """

    def _convert(self, tsv):
        directory = tempfile.mkdtemp()
        path = os.path.join(directory, 'places.tsv')
        with open(path, 'w') as f:
            f.write(tsv)
        lpf_path, *_ = parse_to_LPF(path, 'tsv')
        with open(lpf_path) as f:
            return {feat['@id']: feat for feat in json.load(f)['features']}

    def test_labels_become_types_when_there_are_no_aat_types(self):
        features = self._convert(
            "id\ttitle\ttitle_source\tlon\tlat\tstart\ttypes\taat_types\n"
            "1\tSerra\tIBGE\t-37.05\t-7.02\t1888\tquilombo;povoado\t\n")
        self.assertEqual(features['1']['types'],
                         [{'label': 'quilombo'}, {'label': 'povoado'}])
        self.assertNotIn('additional_types', features['1'])

    def test_labels_are_appended_when_aat_types_are_present(self):
        features = self._convert(
            "id\ttitle\ttitle_source\tlon\tlat\tstart\ttypes\taat_types\n"
            "1\tSerra\tIBGE\t-37.05\t-7.02\t1888\tquilombo\t300263222\n")
        self.assertEqual(features['1']['types'],
                         [{'identifier': 'aat:300263222'}, {'label': 'quilombo'}])
