"""The Locus page's Stage 5 layout: the Record page's sibling shell, the recipe in a
modal, the beta-only "About this cluster" / Elsewhere / Environment slots (place#325).

Run locally only (never in the prod container):

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test search.tests_locus_layout --settings=whg.settings_localtest

Server-rendered markup only; the browser half (map square, recipe modal opening,
EDOPS only on click, no horizontal scroll) is ``search.tests_locus_layout_browser``.
The HTML is parsed (BeautifulSoup, already a requirement), so "inside the modal" is
an ancestor test, not a substring test. Every absence is paired with a presence in
the same test: the recipe text is asserted present AND every occurrence is inside
a modal; a variant without a recipe is asserted against the variant that has one.

Proved able to fail (2026-10-22): run against the pre-Stage-5 template
(``git show 7525cbd51:search/templates/search/locus.html``), every test in
``RecipeLayoutTests`` and ``VariantLayoutTests`` that names the shell or the
recipe modal fails (no ``.rs-layout``, no ``#locus_recipe_modal``, the recipe in a
visible ``<aside>``); the tombstone/legacy/suppressed tests still pass there, as
they should (those variants are unchanged).
"""
from unittest.mock import patch

from bs4 import BeautifulSoup
from django.contrib.auth.models import AnonymousUser
from django.template.loader import render_to_string
from django.test import RequestFactory, override_settings

from places import locus_recipe as lr
from places import locus_registry as registry
from places.models_locus import LocusRegistryEntry
from search.locus import describe_recipe
from search.tests_locus import FULL, POST, SIMPLE, LocusBase, recipe, token

# Literal recipe text the page shows, each of which must live in the recipe modal only.
RECIPE_PHRASES = ("Everything below is literal", "Members when the recipe was made",
                  "Generic-name stoplist", "the 250 best-scoring records", "500 BCE to 1500")


def soup(html):
    return BeautifulSoup(html, "html.parser")


def in_modal(node):
    """True if ``node`` (a tag or a string) sits inside a Bootstrap .modal."""
    return any("modal" in (p.get("class") or []) for p in node.parents if getattr(p, "get", None))


def shell(s):
    """The two columns of the shared shell, asserted to exist."""
    layout = s.select_one("#locus_page > .rs-layout")
    assert layout is not None, "no .rs-layout directly in #locus_page"
    main = layout.find("div", class_="rs-main", recursive=False)
    side = layout.find("div", class_="rs-side", recursive=False)
    assert main is not None and side is not None, "the shell lacks .rs-main or .rs-side"
    return layout, main, side


class LayoutBase(LocusBase):
    def setUp(self):
        super().setUp()
        self.client.force_login(self.beta)

    def html(self, tok):
        resp = self.page(tok)
        self.assertEqual(resp.status_code, 200)
        return resp.content.decode()

    def assert_recipe_only_in_modal(self, s, phrases=RECIPE_PHRASES):
        modal = s.select_one("#locus_recipe_modal")
        self.assertIsNotNone(modal, "no recipe modal")
        self.assertIn("modal", modal["class"])
        self.assertNotIn("show", modal["class"])                    # not shown by default
        self.assertEqual(modal.get("aria-hidden"), "true")
        self.assertIsNone(modal.find_parent(class_="rs-layout"))    # not inside the page's columns
        for el_id in ("locus_recipe_members", "locus_stoplist"):
            el = s.find(id=el_id)
            self.assertIsNotNone(el, el_id)
            self.assertIs(el.find_parent(id="locus_recipe_modal"), modal, f"#{el_id} is not in the recipe modal")
        for phrase in phrases:
            hits = s.find_all(string=lambda t, p=phrase: p in t)
            self.assertTrue(hits, f"{phrase!r} is not on the page at all")              # presence
            outside = [h for h in hits if not in_modal(h) and h.parent.name != "script"]
            self.assertEqual(outside, [], f"{phrase!r} is shown outside a modal")      # absence
        return modal


class RecipeLayoutTests(LayoutBase):
    # Assumes the v4 release switch is off: say so, clearing the alias too (review #5).
    @override_settings(V4_LANDING_ENABLED=False, LOCUS_MINTING_ENABLED=False)
    def test_unminted_page_uses_the_shell_and_hides_the_recipe_in_a_modal(self):
        s = soup(self.html(token(FULL, members=["wd:Q64", "gn:1", "tgn:7003712"])))
        layout, main, side = shell(s)
        header = main.find("header", class_="rs-header", recursive=False)
        self.assertIsNotNone(header)
        self.assertIn("shareable but not permanent", header.get_text())
        self.assertIn("unminted", header.select_one("h1").get_text())
        # The header carries every action, the new Recipe button among them.
        for el_id in ("locus_copy_link", "locus_mint", "locus_recipe_open"):
            self.assertIsNotNone(header.find(id=el_id), el_id)
        self.assertTrue(header.find(id="locus_mint").has_attr("disabled"))   # release switch off
        btn = header.find(id="locus_recipe_open")
        self.assertEqual((btn.get("data-bs-toggle"), btn.get("data-bs-target")), ("modal", "#locus_recipe_modal"))
        self.assertIn("Recipe", btn.get_text())
        # Left: the re-run's answer. Right: the map first, square box, canvas inside.
        self.assertIsNotNone(main.find(id="locus_result"))
        first = side.find(True, recursive=False)
        self.assertIn("rs-map", first["class"])
        self.assertIsNotNone(first.select_one(":scope > #locus_map.rs-map-canvas"))
        # The recipe: every literal present, all of it in the closed modal.
        self.assert_recipe_only_in_modal(s, RECIPE_PHRASES + ("tgn:7003712",))
        self.assertNotIn("Members when the recipe was made", layout.get_text())
        self.assertEqual(s.select(".locus-recipe-col, .locus-layout"), [])   # the old visible panel is gone
        # The members-when-made block keeps the hooks locus.js marks.
        self.assertEqual([m["data-member"] for m in s.select("#locus_recipe_members .locus-member-id")],
                         ["gn:1", "tgn:7003712", "wd:Q64"])

    @override_settings(V4_LANDING_ENABLED=False, LOCUS_MINTING_ENABLED=False)
    def test_beta_slots_are_rendered_closed_and_in_their_columns(self):
        s = soup(self.html(token()))
        _, main, side = shell(s)
        about = main.find(id="locus_about")
        self.assertIsNotNone(about)
        self.assertTrue(about.has_attr("hidden"))           # opened by locus.js once a cluster is selected
        title = about.find(id="locus_about_h").get_text(" ", strip=True)
        self.assertIn("About this cluster", title)
        self.assertIn("generated from the records' data", title)
        self.assertIsNotNone(about.select_one(".locus-about-body"))
        elsewhere = side.find(id="locus_elsewhere")
        self.assertTrue({"rs-card", "rs-icon-links"} <= set(elsewhere["class"]))
        self.assertTrue(elsewhere.has_attr("hidden"))       # no Wikipedia links: no card, no heading
        self.assertIsNotNone(elsewhere.select_one(".locus-wikipedia"))
        env = side.find(id="locus_environment_slot")
        self.assertTrue(env is not None and env.has_attr("hidden"))
        # Nothing of the Stage 3 sections is rendered (they were made by the old wrapper).
        self.assertEqual(s.select("#locus_gloss, #locus_environment, #locus_enrichment"), [])

    def test_the_beta_slots_follow_beta_access(self):
        """Recipe loci are beta-only today, so this renders the template directly as
        an anonymous visitor would see it once they are not: the map stays, the three
        beta slots (whose endpoints 403 without beta) are not rendered."""
        r = recipe(SIMPLE)
        ctx = {"recipe": r, "token": token(), "summary": describe_recipe(r),
               "resolve_url": "/atlas/locus/resolve/", "minting": {"enabled": False}}
        req = RequestFactory().get("/atlas/locus/")
        for user, expect in ((self.beta, True), (AnonymousUser(), False), (self.normal, False)):
            req.user = user
            s = soup(render_to_string("search/locus.html", ctx, request=req))
            self.assertIsNotNone(s.find(id="locus_map"))                       # presence
            self.assertIsNotNone(s.find(id="locus_recipe_modal"))
            for el_id in ("locus_about", "locus_elsewhere", "locus_environment_slot"):
                self.assertEqual(s.find(id=el_id) is not None, expect, (el_id, user))


@override_settings(LOCUS_MINTING_ENABLED=True)
class VariantLayoutTests(LayoutBase):
    def test_minted_recipe_locus(self):
        entry, _ = registry.mint_recipe(recipe(SIMPLE), self.beta)
        with patch(POST, side_effect=self.gateway) as post:
            resp = self.client.get(f"/entity/locus:{entry.pid}/", HTTP_ACCEPT="text/html")
            post.assert_not_called()
        s = soup(resp.content.decode())
        _, main, side = shell(s)
        header = main.find("header", class_="rs-header", recursive=False)
        text = header.get_text(" ", strip=True)
        self.assertIn(f"Locus {entry.pid}", text)
        self.assertIn("minted", header.select_one(".locus-minted-badge").get_text())
        self.assertIn("This identifier is permanent; its membership is not", text)
        self.assertIn("Minted ", text)
        self.assertIsNotNone(header.find(id="locus_copy_pid"))
        self.assertIsNotNone(header.find(id="locus_recipe_open"))
        self.assertIsNone(s.find(id="locus_mint"))
        self.assertIsNotNone(side.find(id="locus_map"))
        modal = self.assert_recipe_only_in_modal(s, RECIPE_PHRASES[:3])
        self.assertIsNotNone(modal.select_one('[data-member="gn:1"]'))

    def test_unminted_with_minting_on_keeps_the_mint_modal_and_its_recipe(self):
        tok = token(FULL, members=["wd:Q64", "gn:1", "tgn:7003712"])
        s = soup(self.html(tok))
        _, main, _ = shell(s)
        mint = main.find(id="locus_mint")
        self.assertFalse(mint.has_attr("disabled"))
        mint_modal = s.find(id="locus_mint_modal")
        self.assertIsNotNone(mint_modal)
        # The confirmation still lists the recipe being minted, as before.
        listed = mint_modal.select_one(".locus-mint-recipe").get_text(" ", strip=True)
        for phrase in ("Retrieval", "Clustering", "the 250 best-scoring records", "tgn:7003712"):
            self.assertIn(phrase, listed)
        self.assert_recipe_only_in_modal(s)

    def test_existing_pid_badge_links_to_it(self):
        tok = token()
        self.assertEqual(self.client.post("/atlas/locus/mint/", data={"r": tok, "confirm": True},
                                          content_type="application/json").status_code, 201)
        pid = lr.recipe_pid(recipe(SIMPLE))
        s = soup(self.html(tok))
        _, main, _ = shell(s)
        badge = main.select_one("h1 a.locus-minted-badge")
        self.assertEqual((badge["href"], badge.get_text(strip=True)), (f"/entity/locus:{pid}/", f"minted as {pid}"))
        already = main.find(id="locus_mint")
        self.assertEqual(already.name, "a")
        self.assertIn(f"Already minted: {pid}", already.get_text())
        self.assertIsNotNone(main.find(id="locus_recipe_open"))
        self.assertIsNone(s.find(id="locus_mint_modal"))

    def test_collision_state(self):
        r = recipe(SIMPLE)
        other = lr.canonical_bytes(recipe(SIMPLE, members=("gn:3",)))
        LocusRegistryEntry.objects.create(pid=lr.recipe_pid(r), kind="recipe", version=1, canonical=other,
                                          digest=LocusRegistryEntry.digest_of(other), token="t",
                                          members=["gn:3"], minted_by=self.normal, minted_via="modal")
        s = soup(self.html(token()))
        _, main, _ = shell(s)
        btn = main.find(id="locus_mint")
        self.assertTrue(btn.has_attr("disabled"))
        self.assertIn("Cannot mint (collision)", btn.get_text())
        self.assertIsNotNone(main.find(id="locus_recipe_open"))

    def test_tombstone_and_suppressed_link_have_no_recipe_and_no_shell(self):
        entry, _ = registry.mint_recipe(recipe(SIMPLE), self.beta)
        live = soup(self.client.get(f"/entity/locus:{entry.pid}/", HTTP_ACCEPT="text/html").content.decode())
        self.assertIsNotNone(live.find(id="locus_recipe_modal"))                     # presence first
        entry.withdraw("gone (test)", by=self.beta)
        s = soup(self.client.get(f"/entity/locus:{entry.pid}/", HTTP_ACCEPT="text/html").content.decode())
        self.assertIn("withdrawn", s.select_one(".locus-frozen-badge").get_text())
        self.assertIsNotNone(s.find(id="locus_copy_pid"))
        self.assertEqual(s.select("#locus_recipe_modal, #locus_recipe_open, .rs-layout, #locus_about"), [])
        link = soup(self.html(token()))
        self.assertIn("This recipe is no longer shown", link.get_text())
        self.assertEqual(link.select("#locus_recipe_modal, #locus_recipe_open, .rs-layout"), [])

    def test_legacy_frozen_locus_is_unchanged(self):
        from datasets.models import Dataset
        from places.models import LegacyUnionRecord, Place
        ds = Dataset.objects.create(owner=self.beta, label="layout_legacy", title="Mine", description="D", public=False)
        p = Place.objects.create(title="Layout Hamlet", src_id="lh1", dataset=ds, ccodes=[])
        LegacyUnionRecord.objects.create(whg_id=10000055, place_ids=[p.id], titles=["Layout Hamlet"])
        s = soup(self.client.get("/entity/locus:10000055/", HTTP_ACCEPT="text/html").content.decode())
        self.assertIn("legacy, frozen", s.get_text())
        self.assertEqual([a.get_text(strip=True) for a in s.select("#locus_legacy_members .locus-record-link")],
                         ["Layout Hamlet"])
        self.assertEqual(s.select("#locus_recipe_modal, #locus_recipe_open, #locus_recipe"), [])
