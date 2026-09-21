"""The place-identifier tooltip says the same thing in both places it lives (place#271).

A place's pid is surfaced on three public surfaces. Two of them share
`placeUriHTML()` in `whg/webpack/js/utilities.js`; the third is the
server-rendered `places/templates/places/place_detail.html`, which cannot import
it. So the wording exists twice.

🛑 A claim kept in two places drifts, and this project has already paid for that:
an `fclasses` caveat lived in two files in the documentation repo and only one
got corrected when it was disproved, so the retracted framing survived in the
other for as long as nobody looked. This test is the cheap defence.

⚠️ It asserts the *substance*, not byte-equality — the two files have different
escaping rules and a test that demanded identical bytes would fail for reasons
that do not matter and be edited into uselessness.
"""

import pathlib
import re

from django.test import SimpleTestCase

ROOT = pathlib.Path(__file__).resolve().parent.parent
JS = ROOT / "whg" / "webpack" / "js" / "utilities.js"
TPL = ROOT / "places" / "templates" / "places" / "place_detail.html"

# The claims the tooltip must make, wherever it appears. Each is here because
# leaving it out would mislead:
#   /api        — the machine-readable form is not the URI itself
#   licence     — the data reached is the contributor's, not ours to relicense
#   API token   — a WHG-hosted record returns 401 anonymously (measured on prod)
REQUIRED = ["/api", "licence", "API token"]


def _unescape(s):
    return s.replace("&#39;", "'").replace("&amp;", "&")


class PlaceUriTooltipTests(SimpleTestCase):

    def _js_tooltip(self):
        m = re.search(r"export const PLACE_URI_TOOLTIP\s*=\s*\n?\s*'([^']*)'", JS.read_text())
        self.assertIsNotNone(m, "PLACE_URI_TOOLTIP not found in utilities.js — renamed?")
        return _unescape(m.group(1))

    def _template_tooltip(self):
        text = TPL.read_text()
        m = re.search(r'title="(Opens this record[^"]*)"', text)
        self.assertIsNotNone(m, "the identifier tooltip was not found in place_detail.html")
        return _unescape(m.group(1))

    def test_both_surfaces_carry_the_tooltip(self):
        self.assertTrue(self._js_tooltip())
        self.assertTrue(self._template_tooltip())

    def test_the_two_do_not_drift(self):
        """🛑 The point of this file."""
        self.assertEqual(
            self._js_tooltip(), self._template_tooltip(),
            "the identifier tooltip differs between utilities.js and place_detail.html — "
            "one was edited and the other was not")

    def test_it_makes_every_claim_it_needs_to(self):
        for surface, tip in (("utilities.js", self._js_tooltip()),
                             ("place_detail.html", self._template_tooltip())):
            for claim in REQUIRED:
                with self.subTest(surface=surface, claim=claim):
                    self.assertIn(claim, tip,
                                  f"{surface}'s tooltip does not mention {claim!r}")

    def test_it_does_not_promise_anonymous_api_access(self):
        """⚠️ Measured on prod 2026-09-21: `place:gn:13274771` answers /api with
        200, `place:81010` with 401. A tooltip claiming the machine-readable view
        is simply available would be false for every WHG-hosted record — which is
        most of what a contributor cares about."""
        tip = self._js_tooltip().lower()
        for forbidden in ("freely available", "no authentication", "publicly available via /api"):
            self.assertNotIn(forbidden, tip)

    def test_the_helper_actually_uses_the_constant(self):
        """A constant nothing references is not a tooltip. Same failure as the
        place#260 badge, where the function was correct and never called."""
        js = JS.read_text()
        self.assertIn("title=\"${PLACE_URI_TOOLTIP}\"", js,
                      "placeUriHTML no longer interpolates PLACE_URI_TOOLTIP")
