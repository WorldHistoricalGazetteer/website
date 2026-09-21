"""`parent_name` without `parent_id` must not produce an invalid LPF relation (place#278).

`validation/tLPF_mappings.py` mapped the two columns into the *same* relation —
`parent_name` supplying `relationType` + `label`, `parent_id` supplying
`relationTo`. But `lpf_v2.0.jsonld` requires **both** `relationType` and
`relationTo`, so a contributor who filled in only `parent_name` got:

    INVALID — 'relationTo' is a required property at features[0].relations[0]

naming a field they had never supplied, and pointing at a column they had not
used. The natural reading is that something else is wrong with the file.

🛑 A contributor with a parent *name* and no stable parent *identifier* is the
normal case for historical data — the name is what the source records — so the
coupling is resolved in their favour: no `parent_id`, no relation.

⚠️ Each "must not emit" assertion has a companion asserting that the valid
combination still DOES emit. A change that dropped the relation entirely would
otherwise pass a suite testing only the first half.
"""

from django.test import SimpleTestCase

from validation.tLPF_mappings import tLPF_mappings


def _convert(column, value):
    """Run one column's converter exactly as the real mapping table does."""
    spec = tLPF_mappings[column]
    return spec['lpf'], spec['converter'](value)


class ParentNameRelationTests(SimpleTestCase):

    def test_parent_name_no_longer_targets_a_relation(self):
        """🛑 The fix. `parent_name` must not write into `relations[]` at all —
        anything it puts there is missing `relationTo` by construction."""
        target, _ = _convert('parent_name', 'Kent')
        self.assertFalse(
            target.startswith('relations'),
            f"parent_name still writes to {target!r}; alone it can only produce "
            f"a relation with no relationTo, which fails schema validation")

    def test_parent_name_value_is_preserved_somewhere(self):
        """The label must survive. Dropping it would trade an invalid file for
        silent data loss, which is the worse of the two."""
        target, value = _convert('parent_name', 'Kent')
        self.assertEqual(value, 'Kent')
        self.assertTrue(target, "parent_name must land somewhere")

    def test_parent_id_still_supplies_relationTo(self):
        """🛑 The companion. The valid combination must be untouched — this is
        what stops the fix being 'delete the feature'."""
        target, value = _convert('parent_id', 'gn:2647793')
        self.assertEqual(target, 'relations.0.relationTo')
        self.assertEqual(value, 'gn:2647793')

    def test_empty_parent_name_yields_nothing(self):
        for blank in ('', '   ', None):
            with self.subTest(blank=blank):
                _, value = _convert('parent_name', blank)
                self.assertIsNone(value)

    def test_the_two_columns_no_longer_share_a_relation_slot(self):
        """The root cause, asserted directly: they targeted the same
        `relations.0`, so one could complete what the other started. If a future
        change re-couples them this fails, whatever the schema says."""
        name_target, _ = _convert('parent_name', 'Kent')
        id_target, _ = _convert('parent_id', 'gn:1')
        self.assertNotEqual(
            name_target.split('.')[0], id_target.split('.')[0],
            "parent_name and parent_id must not write into the same structure")
