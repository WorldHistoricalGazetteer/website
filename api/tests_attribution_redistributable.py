"""The root attribution block must say whether a source can be dereferenced (place#296).

Since place#269, ``GET /entity/place:<id>/api`` answers **451** for a namespace whose
registry row says ``redistributable = False`` — live today for ``kain_par``, ``nl`` and
``chgis``. The root ``attribution`` block on ``/reconcile`` exists precisely so a
consumer knows *on what terms* a result set may be reused (place#157), and it did not
carry that flag.

🛑 The harm is the shape of a reconciliation workflow: **match, then fetch the matches.**
A client could reconcile a batch, read an attribution block naming the source and its
licence, conclude the terms were workable, and only then be refused on every single
dereference — having already committed to the matches.

⚠️ The licence is not a substitute signal. ``custom-ukds-eul`` does not tell a client
that **WHG specifically** will not re-serve the record. Only this flag does.

These tests hit the database deliberately. A source-level assertion that the string
``redistributable`` appears in ``attribution_for`` would have passed while the field was
absent from the ``.values()`` call — the two must agree, and only executing the query
proves they do. That failure mode cost five hollow tests in one day during the
place#214+ pass.
"""

from django.test import TestCase

from api.attribution import attribution_block, attribution_for


def _entry(namespace, *, redistributable=True, name=None, status='published'):
    """A minimal authority row. ``entry_class='authority'`` matters: the function
    filters on it, so a row created without it is invisible and every assertion
    below would pass vacuously against an empty dict."""
    from api.models import GazetteerRegistryEntry
    return GazetteerRegistryEntry.objects.create(
        namespace=namespace,
        name=name or namespace.upper(),
        entry_class='authority',
        status=status,
        redistributable=redistributable,
    )


class RedistributableInAttributionTests(TestCase):

    def test_a_refusing_source_is_flagged_false(self):
        """🛑 The fix. This is the case a consumer cannot currently see coming."""
        _entry('kain_par', redistributable=False)
        block = attribution_for(['kain_par'])
        self.assertIn('kain_par', block, "the row was not matched at all — check entry_class")
        self.assertIs(block['kain_par']['redistributable'], False)

    def test_a_permitted_source_is_flagged_true(self):
        """⚠️ The companion. Without it, a change that hardcoded ``False``, or dropped
        the field to a falsy default, would pass the test above on its own."""
        _entry('gn', redistributable=True)
        self.assertIs(attribution_for(['gn'])['gn']['redistributable'], True)

    def test_both_at_once_are_reported_independently(self):
        """The real case: a result set spanning sources with different answers. A
        single flag applied to the whole block would be worse than none."""
        _entry('kain_par', redistributable=False)
        _entry('gn', redistributable=True)
        block = attribution_for(['gn', 'kain_par'])
        self.assertEqual(
            {ns: block[ns]['redistributable'] for ns in ('gn', 'kain_par')},
            {'gn': True, 'kain_par': False})

    def test_the_key_is_always_present(self):
        """A consumer must be able to test the key rather than guess from its absence.
        ``.get('redistributable')`` returning None for a missing key and for a null
        value are indistinguishable, which is the place#157 tri-state trap."""
        _entry('tgn')
        self.assertIn('redistributable', attribution_for(['tgn'])['tgn'])

    def test_it_reaches_the_root_envelope_too(self):
        """🛑 Wiring, not just the helper. A correct helper nothing routes through is the
        place#260 failure — the badge function was right and was never called."""
        _entry('chgis', redistributable=False)
        block = attribution_block(namespaces=['chgis'])
        self.assertIs(block['sources']['chgis']['redistributable'], False)

    def test_the_licence_object_still_nests(self):
        """The shape is a PUBLIC contract and this change must not flatten it. The
        entity endpoint's block is flat with ``license__`` prefixes; this one nests.
        Code written against one cannot read the other, and the shared key name
        ``attribution`` is the whole trap."""
        _entry('gn')
        src = attribution_for(['gn'])['gn']
        for expected in ('name', 'citation', 'record_count', 'rights_holder',
                         'source_url', 'license', 'redistributable'):
            self.assertIn(expected, src)
        self.assertNotIn('license__spdx_id', src,
                         "the nested licence object has been flattened — that is the "
                         "entity endpoint's private shape, not this public one")

    def test_an_unknown_namespace_is_omitted_rather_than_guessed(self):
        """Unchanged behaviour, asserted so the addition did not make absence look
        like permission. A namespace with no registry row must not acquire a
        default ``redistributable: true``."""
        self.assertEqual(attribution_for(['nosuchns']), {})

    def test_a_non_authority_row_is_still_ignored(self):
        """``whg`` sub-namespaces are attributed from the dataset, not the registry.
        Asserted because widening the ``.values()`` call is exactly the kind of edit
        that quietly relaxes a filter."""
        _entry('whatever', redistributable=False)
        from api.models import GazetteerRegistryEntry
        GazetteerRegistryEntry.objects.filter(namespace='whatever').update(entry_class='dataset')
        self.assertEqual(attribution_for(['whatever']), {})
