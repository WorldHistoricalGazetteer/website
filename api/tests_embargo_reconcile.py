"""An embargoed gazetteer must not be reachable through reconciliation (place#218).

The place#162 embargo hid a gazetteer from every *discovery* surface —
`/api/sources/`, the Atlas Gazetteers and Regions offcanvas, the coverage
endpoint, all four via `visible_to()` — and left it **fully queryable** through
`POST /reconcile` and `GET /suggest/entity`, returning candidates carrying its
name and identifiers. `/api/attribution/` would then resolve that namespace to
its licence and rights holder.

**Latent, not live**: production has 0 `embargoed` rows, so nothing leaks today.
It would leak the first time the mechanism is used for its stated purpose — which
is the worst possible moment to find out.

🛑 Two failure modes the issue warns about specifically, and both are tested here
because either would be silent:

1. **The hybrid path.** Reconcile searches the gateway *and* the legacy index and
   merges. Filtering one arm still leaks.
2. **Failing open for namespaces with no registry row.** `whg` is a
   pseudo-namespace with no row at all; if the hidden set were computed as a naive
   complement of the visible one, `whg` — and every `draft`/`pending` row — would
   be hidden, and legacy reconciliation would stop working. ⚠️ Production carries
   a `pending` row today, so that mistake would have been live.
"""

from django.test import SimpleTestCase

from api.reconcile import apply_namespace_embargo


class ApplyNamespaceEmbargoTests(SimpleTestCase):
    """The subtraction itself. No database — this is set arithmetic with a
    contract, and the contract is where the leak was."""

    def test_no_embargo_is_a_no_op(self):
        """The overwhelmingly common case: 0 embargoed rows in production."""
        for requested in (None, {"gn", "tgn"}, set()):
            with self.subTest(requested=requested):
                out, narrowed = apply_namespace_embargo(requested, set())
                self.assertEqual(out, requested)
                self.assertFalse(narrowed)

    def test_an_embargoed_namespace_is_removed_from_an_explicit_request(self):
        out, narrowed = apply_namespace_embargo({"gn", "secret", "tgn"}, {"secret"})
        self.assertEqual(out, {"gn", "tgn"})
        self.assertTrue(narrowed)

    def test_requesting_ONLY_the_embargoed_namespace_yields_empty_not_an_error(self):
        """🛑 The issue is explicit: *'A caller who explicitly requests an
        embargoed namespace should get an empty result, not an error that
        confirms it exists.'* An error is a disclosure."""
        out, narrowed = apply_namespace_embargo({"secret"}, {"secret"})
        self.assertEqual(out, set())
        self.assertTrue(narrowed)

    def test_asking_for_everything_is_flagged_as_narrowed(self):
        """⚠️ `None` means 'all sources'. It cannot be expressed upstream as
        'all but these', so it must stay `None` — but it MUST still report as
        narrowed, or the `namespaces_searched` suppression is skipped exactly
        when the caller asked for everything, which is the common case."""
        out, narrowed = apply_namespace_embargo(None, {"secret"})
        self.assertIsNone(out)
        self.assertTrue(narrowed, "an unrestricted request must still be treated as narrowed")

    def test_whg_survives(self):
        """The pseudo-namespace for legacy places must never be hidden."""
        out, _ = apply_namespace_embargo({"whg", "gn"}, {"secret"})
        self.assertIn("whg", out)


class HiddenNamespacesFailOpenTests(SimpleTestCase):
    """`hidden_namespaces_for()` must be narrow. Source-level, because the risk is
    a future refactor to a naive complement — which is the obvious
    simplification and the wrong one."""

    def _models_src(self):
        import pathlib
        return (pathlib.Path(__file__).resolve().parent / "models.py").read_text()

    def test_it_filters_on_embargoed_status(self):
        """🛑 Restricted to `embargoed` rows. A complement over ALL rows would
        also hide `draft`, `submitted`, `rejected` and `pending` — and production
        has a `pending` row, so it would have silently stopped returning
        candidates from a namespace nobody embargoed."""
        src = self._models_src()
        self.assertIn("def hidden_namespaces_for", src)
        # ⚠ Scoped to the function BODY. `visible_to()` also contains
        # "status='embargoed'", so searching the whole file matched it there and the
        # assertion passed with the filter replaced by `self.all()` — caught by
        # mutation, and the third time today a source-level check has matched the
        # wrong occurrence.
        start = src.index("def hidden_namespaces_for")
        body = src[start:start + 2500]
        self.assertIn("status='embargoed'", body,
                      "hidden_namespaces_for no longer restricts to embargoed rows — a complement "
                      "over all rows would hide draft/pending/submitted namespaces too")

    def test_it_derives_from_visible_to_rather_than_reimplementing_it(self):
        """The beta rule, the lazy auto-release and the date comparison live in
        `visible_to()`. A second copy would drift, and drifting OPEN is a silent
        leak of the thing the embargo exists to hold back."""
        src = self._models_src()
        start = src.index("def hidden_namespaces_for")
        body = src[start:start + 2500]
        self.assertIn("self.visible_to(user)", body,
                      "hidden_namespaces_for must call visible_to(), not restate its rules")
        for restated in ("can_access_beta", "embargo_release_at__lte"):
            self.assertNotIn(restated, body,
                             f"{restated} is restated inside hidden_namespaces_for — it will drift "
                             f"from visible_to()")


class ReconcileWiringTests(SimpleTestCase):
    """Both arms, and the response. Asserted at source level because the leak was
    an *absence* — the gate existed and reconcile simply never called it."""

    def _src(self):
        import pathlib
        return (pathlib.Path(__file__).resolve().parent / "reconcile.py").read_text()

    def test_the_gate_is_called_on_both_endpoints(self):
        src = self._src()
        self.assertIn("_hidden_ns = hidden_namespaces(user)", src,
                      "reconcile_place_es does not resolve the embargoed set")
        self.assertIn("_suggest_hidden = hidden_namespaces(request.user, request)", src,
                      "SuggestEntityView does not resolve the embargoed set")

    def test_both_arms_are_filtered(self):
        """🛑 The hybrid path. Filtering only the gateway, or only the legacy
        index, still leaks — and each arm is a separate code path, so one can be
        fixed and the other forgotten."""
        src = self._src()
        self.assertEqual(
            src.count("not in _hidden_ns"), 2,
            "expected the legacy arm AND the gateway arm to be filtered in reconcile_place_es")
        self.assertEqual(
            src.count("not in _suggest_hidden"), 2,
            "expected both arms to be filtered in SuggestEntityView")

    def test_the_embargoed_namespace_is_not_reported_as_searched(self):
        """Naming it discloses its existence — the thing the embargo is for. And
        the root `attribution` block is built FROM this set (place#157), so it
        would otherwise resolve the namespace to its licence and rights holder."""
        src = self._src()
        self.assertIn("searched -= _hidden_ns", src)

    def test_the_result_is_cached_per_request(self):
        """A registry query per candidate is how a correctness fix becomes a
        performance incident."""
        src = self._src()
        self.assertIn("_whg_hidden_namespaces", src)
