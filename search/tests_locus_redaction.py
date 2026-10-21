"""Take-down and attribution on /entity/locus:<PID> (place#325 rulings
2026-10-21, after the dev walkthrough). Run locally only:

    DB_NAME=whgv3beta_f2 GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test search.tests_locus_redaction --settings=whg.settings_localtest

* A WITHDRAWN locus, and a REDACTED one (staff; active or not), is served as
  a tombstone: PID, status, reason (withdrawal only), dates. No recipe text in
  the HTML or the JSON: not the token, not the recipe, not its members, not
  the recipe page link, not ``current``.
* The minter is named only when they opted in at mint time; otherwise
  "a WHG contributor". ``minted_by`` stays in the database for staff.

Every absence is paired with the same string PRESENT on an otherwise
identical active, unredacted locus, so a page that never rendered the recipe
at all could not pass (a-check-that-cannot-fail.md).

Proved able to fail (2026-10-21): with ``entry.is_suppressed`` forced False in
``locus_document``, ``test_withdrawn_json_carries_no_recipe_text`` fails; with
the name rule in ``public_minter_name`` returning the name unconditionally,
``test_name_only_when_opted_in`` fails.
"""
import json
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import override_settings

from places import locus_registry as registry
from places.models_locus import LocusRegistryEntry
from search.tests_locus import POST, SIMPLE, LocusBase, recipe, token

MEMBERS = ("gn:7777777", "wd:Q98765")
QUERY = {**SIMPLE, "query": "Xanadurecipe"}
SECRETS = ("Xanadurecipe", "Q98765", "gn:7777777")


@override_settings(V4_LANDING_ENABLED=True, LOCUS_MINTING_ENABLED=True)
class TombstoneTests(LocusBase):
    def setUp(self):
        super().setUp()
        self.client.force_login(self.beta)
        self.staff = get_user_model().objects.create_user(
            username="locus_staff", email="ls@example.org", password="pw", given_name="Staff",
            surname="Person", role="beta_tester", is_staff=True)
        self.entry, _ = registry.mint_recipe(recipe(QUERY, members=MEMBERS), self.beta, minter_public=True)
        self.control, _ = registry.mint_recipe(recipe({**QUERY, "size": 99}, members=MEMBERS), self.beta,
                                               minter_public=True)

    def get(self, pid, accept="text/html"):
        with patch(POST, side_effect=self.gateway):
            return self.client.get(f"/entity/locus:{pid}/", HTTP_ACCEPT=accept)

    def assert_suppressed_html(self, entry, header):
        resp = self.get(entry.pid)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp["X-WHG-Locus-Status"], header)
        html = resp.content.decode()
        self.assertIn(f"Locus {entry.pid}", html)
        self.assertIn('data-withdrawn="1"', html)
        self.assertNotIn(entry.token, html)
        self.assertNotIn('id="locus_recipe"', html)
        self.assertNotIn("data-token=\"" + entry.token, html)
        for s in SECRETS:
            self.assertNotIn(s, html, s)
        # The minter is not shown either (the signed-in nav names the viewer, so look for the byline).
        self.assertNotIn("by " + (self.beta.name or self.beta.username), html)
        self.assertNotIn("a WHG contributor", html)
        # The control: the same strings ARE on the active, unredacted page.
        ctl = self.get(self.control.pid).content.decode()
        self.assertIn(self.control.token, ctl)
        self.assertIn('id="locus_recipe"', ctl)
        self.assertIn("by " + (self.beta.name or self.beta.username), ctl)
        for s in SECRETS:
            self.assertIn(s, ctl, s)
        return html

    def assert_suppressed_json(self, entry, header):
        resp = self.get(entry.pid, accept="application/ld+json")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp["X-WHG-Locus-Status"], header)
        body = resp.content.decode()
        doc = json.loads(body)
        for key in ("recipe", "recipe_token", "recipe_page", "current", "legacy"):
            self.assertNotIn(key, doc, key)
        self.assertNotIn(entry.token, body)
        for s in SECRETS:
            self.assertNotIn(s, body, s)
        self.assertNotIn(self.beta.name or self.beta.username, body)
        self.assertEqual(doc["pid"], entry.pid)
        self.assertIn("at", doc["minted"])
        ctl = self.get(self.control.pid, accept="application/ld+json")
        cdoc = json.loads(ctl.content)
        for key in ("recipe", "recipe_token", "recipe_page", "current"):
            self.assertIn(key, cdoc, key)
        self.assertIn(self.control.token, ctl.content.decode())
        self.assertNotIn("X-WHG-Locus-Status", ctl)
        return doc

    def test_withdrawn_html_carries_no_recipe_text(self):
        self.entry.withdraw("Withdrawn at the minter's request (test).", by=self.beta)
        html = self.assert_suppressed_html(self.entry, "withdrawn")
        self.assertIn("Withdrawn", html)
        self.assertIn("minter&#x27;s request (test)", html)

    def test_withdrawn_json_carries_no_recipe_text(self):
        self.entry.withdraw("Withdrawn at the minter's request (test).", by=self.beta)
        doc = self.assert_suppressed_json(self.entry, "withdrawn")
        self.assertEqual(doc["status"], "withdrawn")
        self.assertEqual(doc["withdrawn"]["reason"], "Withdrawn at the minter's request (test).")
        self.assertTrue(doc["withdrawn"]["at"])

    def test_redacted_active_locus_is_suppressed_and_the_reason_stays_with_staff(self):
        self.entry.redact("contains a living person's home address", by=self.staff)
        html = self.assert_suppressed_html(self.entry, "redacted")
        self.assertIn("redacted", html.lower())
        self.assertNotIn("home address", html)
        doc = self.assert_suppressed_json(self.entry, "redacted")
        self.assertEqual(doc["status"], "active")
        self.assertTrue(doc["redacted"]["at"])
        self.assertNotIn("home address", json.dumps(doc))
        # The bytes are still in the registry (the PID is their hash), only not shown.
        row = LocusRegistryEntry.objects.get(pk=self.entry.pk)
        self.assertIn(b"Xanadurecipe", row.canonical_bytes)

    def test_withdrawn_legacy_locus_hides_its_captured_membership(self):
        from datasets.models import Dataset
        from places.models import LegacyUnionRecord, Place
        ds = Dataset.objects.create(owner=self.beta, label="red_legacy", title="Mine", description="D", public=False)
        p = Place.objects.create(title="Hidden Hamlet", src_id="h1", dataset=ds, ccodes=[])
        LegacyUnionRecord.objects.create(whg_id=10000077, place_ids=[p.id], titles=["Hidden Hamlet"])
        live = self.get("10000077").content.decode()
        self.assertIn("Hidden Hamlet", live)                       # presence first
        LocusRegistryEntry.objects.get(pk="10000077").withdraw("snapshot row withdrawn (test)", by=self.staff)
        resp = self.get("10000077")
        self.assertEqual(resp["X-WHG-Locus-Status"], "withdrawn")
        self.assertNotIn("Hidden Hamlet", resp.content.decode())
        jbody = self.get("10000077", accept="application/json").content.decode()
        self.assertNotIn("Hidden Hamlet", jbody)
        self.assertNotIn("places_id", jbody)

    def test_suppressed_loci_are_not_offered_as_overlaps(self):
        other = recipe(QUERY, members=("gn:7777777", "gn:3"))
        before = registry.overlapping(other["members"])
        self.assertIn(self.entry.pid, [o["pid"] for o in before])
        self.entry.withdraw("gone (test)", by=self.beta)
        self.control.redact("gone (test)", by=self.staff)
        after = registry.overlapping(other["members"])
        self.assertNotIn(self.entry.pid, [o["pid"] for o in after])
        self.assertNotIn(self.control.pid, [o["pid"] for o in after])


@override_settings(V4_LANDING_ENABLED=True, LOCUS_MINTING_ENABLED=True)
class AttributionTests(LocusBase):
    def setUp(self):
        super().setUp()
        self.client.force_login(self.beta)
        self.name = self.beta.name or self.beta.username

    def mint(self, tok, **extra):
        body = {"r": tok, "confirm": True, **extra}
        return self.client.post("/atlas/locus/mint/", data=json.dumps(body), content_type="application/json")

    def get(self, pid, accept="text/html"):
        with patch(POST, side_effect=self.gateway):
            return self.client.get(f"/entity/locus:{pid}/", HTTP_ACCEPT=accept)

    def test_modal_offers_the_name_checkbox_unticked(self):
        html = self.page(token()).content.decode()
        self.assertIn('id="locus_mint_public"', html)
        self.assertNotRegex(html, r'id="locus_mint_public"[^>]*checked')
        self.assertIn("a WHG contributor", html)

    def test_name_only_when_opted_in(self):
        shy = self.mint(token())
        self.assertEqual(shy.status_code, 201)
        named = self.mint(token(members=("gn:5",)), minter_public=True)
        self.assertEqual(named.status_code, 201)
        # "yes" is not true: only a JSON boolean opts in.
        odd = self.mint(token(members=("gn:6",)), minter_public="yes")
        self.assertFalse(LocusRegistryEntry.objects.get(pk=odd.json()["pid"]).minter_public)
        shy_pid, named_pid = shy.json()["pid"], named.json()["pid"]
        self.assertEqual(shy.json()["minted"]["minted_by"], "a WHG contributor")
        self.assertEqual(named.json()["minted"]["minted_by"], self.name)

        shy_html = self.get(shy_pid).content.decode()
        named_html = self.get(named_pid).content.decode()
        self.assertIn("by a WHG contributor", shy_html)
        self.assertNotIn(f"by {self.name}", shy_html)
        self.assertIn(f"by {self.name}", named_html)
        self.assertNotIn("by a WHG contributor", named_html)
        shy_doc = json.loads(self.get(shy_pid, accept="application/json").content)
        named_doc = json.loads(self.get(named_pid, accept="application/json").content)
        self.assertEqual(shy_doc["minted"]["by"], "a WHG contributor")
        self.assertEqual(named_doc["minted"]["by"], self.name)
        # minted_by is recorded for staff either way.
        self.assertEqual(LocusRegistryEntry.objects.get(pk=shy_pid).minted_by, self.beta)

    def test_minter_can_retract_their_name_and_nobody_else_can(self):
        pid = self.mint(token(), minter_public=True).json()["pid"]
        url = "/atlas/locus/attribution/"
        self.client.force_login(self.normal)
        self.assertEqual(self.client.post(url, {"pid": pid, "public": "0"}).status_code, 403)
        self.assertTrue(LocusRegistryEntry.objects.get(pk=pid).minter_public)
        self.client.force_login(self.beta)
        self.assertEqual(self.client.post(url, {"pid": pid, "public": "1"}).status_code, 400)
        html = self.get(pid).content.decode()
        self.assertIn('action="/atlas/locus/attribution/"', html)      # the minter is offered the retraction
        resp = self.client.post(url, {"pid": pid, "public": "0"})
        self.assertEqual(resp.status_code, 303)
        self.assertEqual(resp["Location"], f"/entity/locus:{pid}/")
        self.assertFalse(LocusRegistryEntry.objects.get(pk=pid).minter_public)
        html = self.get(pid).content.decode()
        self.assertIn("by a WHG contributor", html)
        self.assertNotIn('action="/atlas/locus/attribution/"', html)
