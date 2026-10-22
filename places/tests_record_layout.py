"""place#325 Stage 5: the Record page in the shared two-column shell (template and view).

Run locally (never in a prod container):

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test places.tests_record_layout --settings=whg.settings_localtest

What the server renders: the columns and what is in each, in order; the "Elsewhere" card;
the gloss/environment gates (beta always, anyone with the release switch on, marked
public). The behaviour in a browser (columns, square map, click-to-load environment,
coincident points, co-cited by record) is places/tests_record_layout_browser.py. Every
absence sits beside a presence on the same response.
"""
import os
import re
from html.parser import HTMLParser
from unittest.mock import patch

from django.conf import settings
from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings

LOCMEM = {**settings.CACHES,
          "default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache", "LOCATION": "rl325"}}


class _Outline(HTMLParser):
    """The ids (and a few classes) inside .rs-main and .rs-side, in document order."""
    VOID = {"br", "img", "input", "meta", "link", "hr", "source", "wbr"}

    def __init__(self):
        super().__init__()
        self.stack = []          # (tag, column or None)
        self.cols = {"main": [], "side": []}
        self.side_children = []

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        cls = (a.get("class") or "").split()
        col = next((c for t, c in reversed(self.stack) if c), None)
        mine = "main" if "rs-main" in cls else ("side" if "rs-side" in cls else None)
        if col == "side" and self.stack and self.stack[-1][1] == "side":
            self.side_children.append(a.get("id") or ".".join(cls))
        if col and a.get("id"):
            self.cols[col].append(a["id"])
        if col and "rs-header" in cls:
            self.cols[col].append("<rs-header>")
        if tag not in self.VOID:
            self.stack.append((tag, mine))

    def handle_endtag(self, tag):
        for i in range(len(self.stack) - 1, -1, -1):
            if self.stack[i][0] == tag:
                del self.stack[i:]
                break


def outline(html):
    p = _Outline()
    p.feed(html)
    return p


@override_settings(CACHES=LOCMEM)
class RecordLayoutTemplateTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        from datasets.models import Dataset
        from places.models import Place, PlaceLink
        mk = get_user_model().objects.create_user
        cls.beta = mk(username="rl_beta", email="rl_beta@example.org", password="pw",
                      given_name="R", surname="L", role="beta_tester")
        cls.plain = mk(username="rl_plain", email="rl_plain@example.org", password="pw",
                       given_name="R", surname="P")
        with patch("datasets.signals.doi"):
            ds = Dataset.objects.create(owner=cls.beta, label="rl325", title="Dataset rl", description="D",
                                        creator="X", public=True, ds_status="indexed",
                                        uri_base="https://example.org/places/")
        cls.A = Place.objects.create(title="Layout Town", src_id="L1", dataset=ds, ccodes=[])
        PlaceLink.objects.create(place=cls.A, jsonb={"type": "closeMatch", "identifier": "gn:100"})
        PlaceLink.objects.create(place=cls.A, jsonb={"type": "seeAlso",
                                                     "identifier": "https://example.org/elsewhere/1"})
        PlaceLink.objects.create(place=cls.A, jsonb={"type": "seeAlso", "identifier": "javascript:alert(1)"})
        cls.Bare = Place.objects.create(title="Bare Town", src_id="B1", dataset=ds, ccodes=[])
        PlaceLink.objects.create(place=cls.Bare, jsonb={"type": "closeMatch", "identifier": "gn:200"})
        from places.models import PlaceRelated
        for label in ("Bigger Region", "Older Province"):
            PlaceRelated.objects.create(place=cls.A, jsonb={"relationType": "gvp:broaderPartitive",
                                                            "relationTo": "https://example.org/r/1", "label": label})
        cls.Empty = Place.objects.create(title="Empty Town", src_id="E1", dataset=ds, ccodes=[])

    def page(self, place, user=None):
        self.client.logout()
        if user is not None:
            self.client.force_login(user)
        with patch("api.crc_client.crc_clustering_params", return_value=(None, "unreachable")):
            r = self.client.get(f"/atlas/place/{place.pk}/")
        self.assertEqual(r.status_code, 200)
        return r.content.decode()

    def test_columns_and_their_order(self):
        o = outline(self.page(self.A, self.beta))
        main, side = o.cols["main"], o.cols["side"]
        order = [x for x in main if x in ("<rs-header>", "explore_similar_header", "record", "record_links",
                                          "concordance", "record_gloss")]
        # Round 2 (review 2026-10-22): Record, then About this group, then the Concordance.
        self.assertEqual(order, ["<rs-header>", "explore_similar_header", "record", "record_links", "record_gloss",
                                 "concordance"])
        # The right column: the map first, its notes directly under it, then the cards.
        self.assertEqual(o.side_children, ["concordance_map_box", "concordance_map_notes", "record_elsewhere",
                                           "record_environment"])
        for moved in ("concordance_map_box", "record_elsewhere", "record_environment"):
            self.assertNotIn(moved, main)                                   # absence beside the presence above
        self.assertNotIn("record_gloss", side)

    def test_map_box_is_the_shells_square_and_starts_hidden(self):
        html = self.page(self.A, self.beta)
        self.assertRegex(html, r'<div id="concordance_map_box" class="rs-map" hidden></div>')
        # The canvas is made by the page's JS once there is something to map, not before.
        self.assertNotIn('id="concordance_map"', html)
        self.assertIn('id="concordance_status"', html)                     # presence: the concordance section

    def test_old_stage3_sections_are_gone(self):
        html = self.page(self.A, self.beta)
        self.assertIn('id="record_gloss"', html)
        self.assertIn('id="record_environment"', html)
        self.assertNotIn("enr-section", html)
        self.assertNotIn("Environmental signature", html)                  # the old section heading
        self.assertEqual(len(re.findall(r'id="record_environment"[^>]*class="atlas-record-env"', html)), 1)
        self.assertNotIn('<section id="record_environment"', html)
        # The environment endpoint is named (for the click) but no EDOPS text is pre-rendered.
        self.assertIn(f'data-endpoint="/atlas/place/{self.A.pk}/environment/"', html)

    def test_elsewhere_card_holds_web_links_only(self):
        html = self.page(self.A, self.beta)
        card = html.split('id="record_elsewhere"', 1)[1].split('id="record_environment"', 1)[0]
        self.assertIn('href="https://example.org/elsewhere/1"', card)
        self.assertIn("Cited by this record", card)
        self.assertIn('id="record_wikipedia"', card)
        self.assertNotIn("gn:100", card)               # a CURIE is no web address (it is under Links)
        self.assertNotIn('href="javascript:alert', html)    # listed under Links as text, never an href
        self.assertIn("javascript:alert(1)", html)
        self.assertNotIn("javascript:", card)
        self.assertIn("gn:100", html)
        self.assertNotIn(' hidden', card.split(">", 1)[0])
        # A record with no web links: the card is there for Wikipedia but hidden until it has some.
        bare = self.page(self.Bare, self.beta)
        head = bare.split('id="record_elsewhere"', 1)[1].split(">", 1)[0]
        self.assertIn("hidden", head)
        self.assertNotIn("Cited by this record", bare)

    def test_nav_names_what_exists(self):
        html = self.page(self.A, self.beta)
        nav = html.split('atlas-record-tabs', 1)[1].split("</ul>", 1)[0]
        hrefs = re.findall(r'href="#([a-z_]+)"', nav)
        self.assertEqual(hrefs, ["record", "record_gloss", "concordance", "record_environment"])
        for h in hrefs:
            self.assertIn(f'id="{h}"', html)

    @override_settings(V4_LANDING_ENABLED=True)
    def test_public_visitors_get_both_marked_public(self):
        for user in (None, self.plain):
            html = self.page(self.A, user)
            o = outline(html)
            self.assertIn("record_gloss", o.cols["main"])
            self.assertIn("record_environment", o.cols["side"])
            self.assertEqual(html.count('data-enrichment="public"'), 2)
        beta = self.page(self.A, self.beta)
        self.assertIn('id="record_environment"', beta)
        self.assertNotIn("data-enrichment", beta)

    @override_settings(V4_LANDING_ENABLED=False, LOCUS_MINTING_ENABLED=False)
    def test_switch_off_is_unchanged(self):
        self.client.logout()
        self.assertEqual(self.client.get(f"/atlas/place/{self.A.pk}/").status_code, 302)
        self.client.force_login(self.plain)
        self.assertEqual(self.client.get(f"/atlas/place/{self.A.pk}/").status_code, 404)
        self.assertIn('id="record_gloss"', self.page(self.A, self.beta))

    # ---- round 2 (Stephen's review 2026-10-22) ---------------------------------------
    def _block(self, html, id_):
        """The opening tag of the element with this id, and its text up to the next block."""
        m = re.search(r'<(details|div)\b[^>]*\bid="%s"[^>]*>' % id_, html)
        self.assertIsNotNone(m, id_)
        return m.group(1), m.group(0), html[m.end():m.end() + 1500]

    def test_links_and_related_are_folds_with_totals(self):
        html = self.page(self.A, self.beta)
        tag, head, body = self._block(html, "record_links")
        self.assertEqual(tag, "details")
        self.assertNotIn(" open", head)                                     # closed at first
        self.assertIn('data-total="3"', head)
        self.assertIn('data-collapsed="true"', head)
        summary = body.split("</summary>", 1)[0]
        self.assertIn("<summary", summary)
        self.assertIn("rec-fold-chevron", summary)
        self.assertIn("Links, as contributed", summary)
        self.assertIn("(3)", summary)                                       # the total, in the header
        self.assertIn("gn:100", body.split("</summary>", 1)[1])             # the list, in the body
        tag, head, body = self._block(html, "record_related")
        self.assertEqual(tag, "details")
        self.assertIn('data-total="2"', head)
        self.assertIn("Related places <span class=\"rec-fold-count\">(2)</span>", body.split("</summary>", 1)[0])
        self.assertIn("Older Province", body)

    def test_zero_is_a_plain_block_not_a_fold(self):
        bare = self.page(self.Bare, self.beta)
        tag, head, body = self._block(bare, "record_related")
        self.assertEqual(tag, "div")
        self.assertIn('data-collapsed="none"', head)
        self.assertIn("Related places <span class=\"rec-fold-count\">(0)</span>", body)
        self.assertIn("This record names no related places.", body)
        self.assertEqual(self._block(bare, "record_links")[0], "details")    # presence: Bare's one link folds
        self.assertEqual(bare.count("<details"), 1)
        empty = self.page(self.Empty, self.beta)
        tag, head, body = self._block(empty, "record_links")
        self.assertEqual(tag, "div")
        self.assertIn("(0)", body)
        self.assertIn("This record cites no other identifiers.", body)
        self.assertNotIn("<details", empty)
        self.assertNotIn("<summary", empty)

    def test_sections_carry_the_divider(self):
        html = self.page(self.A, self.beta)
        for id_ in ("record", "record_gloss", "concordance"):
            m = re.search(r'<section id="%s" class="([^"]+)"' % id_, html)
            self.assertIsNotNone(m, id_)
            self.assertIn("atlas-record-divided", m.group(1).split(), id_)
        self.assertEqual(html.count("atlas-record-divided"), 3)

    def test_one_explore_button(self):
        html = self.page(self.A, self.beta)
        self.assertEqual(html.count("Explore similar records"), 1)
        self.assertIn('id="explore_similar_header"', html)
        self.assertNotIn("data-explore-url", html)                          # the second button's source
        path = os.path.join(settings.BASE_DIR, "whg", "webpack", "js", "atlasRecord.js")
        with open(path, encoding="utf-8") as f:
            src = f.read()
        self.assertNotIn("exploreButton", src)
        self.assertNotIn("conc-explore-foot", src)
        self.assertIn('href="#explore_similar_header"', src)                # the empty state points at it


class RecordLayoutSourceTests(TestCase):
    """The page script uses the shared enrichment API, not the Stage 3 wrapper."""

    def test_page_script_uses_the_contract(self):
        path = os.path.join(settings.BASE_DIR, "whg", "webpack", "js", "atlasRecord.js")
        with open(path, encoding="utf-8") as f:
            src = f.read()
        self.assertNotIn("initRecordEnrichment", src)
        for name in ("loadGlossCards", "mountEnvironmentCard", "recordFetcher", "renderWikipediaLinks"):
            self.assertIn(name, src)
