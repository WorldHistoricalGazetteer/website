"""`/suggest/entity` must not rank two incomparable score scales against each other (place#214 defect 2).

`SuggestEntityView` normalises legacy ES hits against their own maximum and CRC
gateway hits against a **different** maximum, then concatenated the two lists and
sorted them together by `score`.

🛑 Both lists' top entries read **100 by construction**, so the merged order was
decided by list position and float ties rather than by quality — a mediocre
gateway hit normalised to 100 outranked a good legacy hit at 85.

There is no comparable quantity to sort by: `confidence` is absolute but the
legacy path emits none. So the lists are **interleaved by within-source rank**
instead — best-of-each, then second-of-each — which is fair, deterministic, and
claims nothing it cannot support.

⚠️ These tests exercise the merge helper directly. The view itself needs ES and
the gateway; the defect is in the merge, and testing it where it lives keeps this
a `SimpleTestCase`.
"""

from django.test import SimpleTestCase

from api.reconcile import _interleave_by_rank


def _c(name, scale, rank, score):
    return {"name": name, "score_scale": scale, "source_rank": rank, "score": score}


class InterleaveByRankTests(SimpleTestCase):

    def test_best_of_each_source_comes_first(self):
        """🛑 The fix. Previously both 100s landed adjacent and their order was
        decided by which list was concatenated first."""
        merged = _interleave_by_rank([
            _c("legacy-1", "legacy", 0, 100), _c("legacy-2", "legacy", 1, 85),
            _c("gw-1", "gateway", 0, 100), _c("gw-2", "gateway", 1, 40),
        ])
        self.assertEqual([c["name"] for c in merged],
                         ["legacy-1", "gw-1", "legacy-2", "gw-2"])

    def test_a_mediocre_hit_no_longer_outranks_a_good_one_from_another_scale(self):
        """The reported harm: `gw-1` is the best of a bad lot and normalises to
        100; `legacy-2` is a genuinely good match at 85. Under a score sort the
        gateway hit won. Now they are ordered by their own sources' ranking."""
        merged = _interleave_by_rank([
            _c("legacy-1", "legacy", 0, 100), _c("legacy-2", "legacy", 1, 85),
            _c("gw-1", "gateway", 0, 100),
        ])
        self.assertLess([c["name"] for c in merged].index("legacy-2"),
                        len(merged),
                        "legacy-2 must still be present")
        # gw-1 is drawn at depth 0 alongside legacy-1; legacy-2 follows at depth 1.
        self.assertEqual([c["name"] for c in merged], ["legacy-1", "gw-1", "legacy-2"])

    def test_uneven_lists_do_not_lose_candidates(self):
        """A source with more results must have its tail appended, not truncated."""
        merged = _interleave_by_rank(
            [_c(f"legacy-{i}", "legacy", i, 100 - i) for i in range(5)]
            + [_c("gw-1", "gateway", 0, 100)])
        self.assertEqual(len(merged), 6)
        self.assertEqual(merged[-1]["name"], "legacy-4")

    def test_a_single_source_is_unchanged(self):
        """The companion: with one source there is nothing to interleave, and the
        source's own order must survive untouched."""
        only = [_c(f"legacy-{i}", "legacy", i, 100 - i) for i in range(4)]
        self.assertEqual([c["name"] for c in _interleave_by_rank(only)],
                         ["legacy-0", "legacy-1", "legacy-2", "legacy-3"])

    def test_empty_input(self):
        self.assertEqual(_interleave_by_rank([]), [])

    def test_unmarked_candidates_are_their_own_source(self):
        """⚠️ An unmarked candidate must not be silently grouped with the legacy
        list — it would inherit a scale it was never normalised on, which is the
        defect wearing a different hat."""
        merged = _interleave_by_rank([
            _c("legacy-1", "legacy", 0, 100),
            _c("legacy-2", "legacy", 1, 90),
            # ⚠ `source_rank` 5 is what makes this test discriminate. With rank 0 the
            # candidate sorts early inside the legacy bucket TOO, so both arrangements
            # give the same order and the test passes either way — which it did, until
            # mutation testing showed it could not fail.
            {"name": "mystery", "score": 99, "source_rank": 5},
        ])
        names = [c["name"] for c in merged]
        self.assertEqual(len(merged), 3)
        # Its own source -> drawn at depth 0, i.e. SECOND overall.
        # Grouped with legacy -> rank 5 sends it to the END.
        self.assertEqual(names, ["legacy-1", "mystery", "legacy-2"],
                         "an unmarked candidate was grouped with the legacy list, so it "
                         "inherited a scale it was never normalised on")

    def test_ordering_is_deterministic(self):
        """Two runs over the same input must agree — the previous behaviour's
        dependence on float ties is exactly what made it unreproducible."""
        cands = [
            _c("a", "legacy", 0, 100), _c("b", "gateway", 0, 100),
            _c("c", "legacy", 1, 100), _c("d", "gateway", 1, 100),
        ]
        first = [c["name"] for c in _interleave_by_rank(list(cands))]
        second = [c["name"] for c in _interleave_by_rank(list(cands))]
        self.assertEqual(first, second)


class ProvenanceMarkerTests(SimpleTestCase):
    """The markers are what let a consumer see the two scales rather than infer a
    comparison that was never valid."""

    def test_the_view_marks_both_scales(self):
        import pathlib
        src = (pathlib.Path(__file__).resolve().parent / "reconcile.py").read_text()
        self.assertIn('candidate["score_scale"] = "legacy"', src)
        self.assertIn('candidate["score_scale"] = "gateway"', src)

    def test_the_merge_helper_is_actually_used(self):
        """🛑 A correct helper that nothing calls is the place#260 failure. Assert
        the wiring, not just the behaviour."""
        import pathlib
        src = (pathlib.Path(__file__).resolve().parent / "reconcile.py").read_text()
        self.assertIn("_interleave_by_rank(place_candidates)", src)
        self.assertNotIn(
            "combined_candidates.sort(key=lambda x: (x.get('score', 0)", src,
            "the score-based cross-scale sort is back")
