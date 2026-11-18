"""`lang` and the toponym `name@lang` form must accept BCP 47 private-use subtags (place#351).

lpf_v2.0.jsonld listed `ar-Latn-x-ijmes` among its own `examples` for `lang`, yet its pattern
refused it: the extension group excluded the `x` singleton and there was no private-use branch.
So WHG's own example data (lp7_100: 5 of 100 places) failed WHG's own validator.

⚠️ Each acceptance has a refusal beside it. A pattern loosened to `.*` would pass every
acceptance; only the refusals show it is still a pattern.
"""

import json

from django.conf import settings
from django.test import SimpleTestCase
from jsonschema import Draft7Validator

with open(settings.LPF_SCHEMA_PATH, encoding='utf-8') as fh:
    _SCHEMA = json.load(fh)


def _validator(definition):
    return Draft7Validator(definition)


class LangPrivateUseTests(SimpleTestCase):

    def lang_ok(self, value):
        return not list(_validator(_SCHEMA['definitions']['lang']).iter_errors(value))

    def toponym_ok(self, value):
        node = _SCHEMA['definitions']['patterns']['definitions']['toponym']
        return not list(_validator(node).iter_errors(value))

    def test_the_schemas_own_example_is_accepted(self):
        for example in _SCHEMA['definitions']['lang']['examples']:
            self.assertTrue(self.lang_ok(example), example)

    def test_private_use_accepted(self):
        for tag in ('ar-Latn-x-ijmes', 'tr-x-a1', 'de-Arab-AR-x-abc-def', 'x-ijmes', 'X-ijmes'):
            self.assertTrue(self.lang_ok(tag), tag)

    def test_toponym_with_private_use_accepted(self):
        self.assertTrue(self.toponym_ok('Qanawat@ar-Latn-x-ijmes'))

    def test_ordinary_tags_still_accepted(self):
        for tag in ('en', 'de-Arab-AR', 'zh-Hans'):
            self.assertTrue(self.lang_ok(tag), tag)

    def test_malformed_tags_still_refused(self):
        for tag in ('ar-Latn-x-', 'ar-Latn-x-toolongtag', 'not a tag', '', 'ar-Latn-x-a b', 'x', 'x-', 'x-toolongtag1', 'xx-'):
            self.assertFalse(self.lang_ok(tag), tag)

    def test_standalone_private_use_toponym(self):
        self.assertTrue(self.toponym_ok('Qanawat@x-ijmes'))

    def test_malformed_toponym_language_still_refused(self):
        self.assertFalse(self.toponym_ok('Qanawat@ar-Latn-x-toolongtag'))
