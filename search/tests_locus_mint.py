"""Minting from the locus page and dereferencing /entity/locus:<PID>
(place#172, place#325 Stage 4). Run locally only:

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test search.tests_locus_mint --settings=whg.settings_localtest

The feature is OFF by default (``LOCUS_MINTING_ENABLED`` False): the first
class proves that with it off nothing public exists, even for a row already
in the registry. The rest run with it on and are beta-gated as the locus page
is. The gateway is faked at ``api.crc_client.requests.post`` (same seam as
``search.tests_locus``), so the JSON representation's ``current`` block comes
from the REAL retrieval path.

Every refusal is paired with the case that is let through on the same route;
every "not in the page" with something that is.

Proved able to fail (2026-10-21): with ``confirm`` not required in
``atlas_locus_mint``, ``test_post_mints_once_and_needs_confirmation`` fails;
with the flag check removed from ``locus_entity``,
``test_everything_is_404_when_off`` fails.
"""
import base64
import json
from unittest.mock import patch

from django.conf import settings
from django.test import override_settings

from places import locus_recipe as lr
from places import locus_registry as registry
from places.models import LegacyUnionRecord
from places.models_locus import LocusRegistryEntry
from search.tests_locus import (FULL, HITS, POST, SIMPLE, FakeGateway, LocusBase, recipe, token)

PID_RE = r"\b[0-9A-HJKMNP-TV-Y]{4}-[0-9A-HJKMNP-TV-Y]{4}-[0-9A-HJKMNP-TV-Y]{4}\b"


class MintBase(LocusBase):
    def preflight(self, tok):
        return self.client.get("/atlas/locus/mint/", {"r": tok})

    def mint(self, tok, confirm=True):
        body = {"r": tok}
        if confirm is not None:
            body["confirm"] = confirm
        return self.client.post("/atlas/locus/mint/", data=json.dumps(body), content_type="application/json")

    def entity(self, pid, accept="text/html", api=False):
        return self.client.get(f"/entity/locus:{pid}/" + ("api" if api else ""), HTTP_ACCEPT=accept)


class OffByDefaultTests(MintBase):
    def test_everything_is_404_when_off(self):
        self.assertFalse(registry.minting_enabled())
        e, _ = registry.mint_recipe(recipe(SIMPLE), self.beta)      # a row exists, nothing serves it
        self.client.force_login(self.beta)
        self.assertEqual(self.preflight(token()).status_code, 404)
        self.assertEqual(self.mint(token()).status_code, 404)
        self.assertEqual(self.entity(e.pid).status_code, 404)
        self.assertEqual(self.entity(e.pid, accept="application/ld+json").status_code, 404)
        self.assertEqual(self.entity(e.pid, api=True).status_code, 404)
        page = self.page(token())
        self.assertEqual(page.status_code, 200)
        html = page.content.decode()
        self.assertIn('id="locus_mint" disabled', html)
        self.assertNotIn("data-mint-url", html)
        self.assertNotIn("locus_mint_modal", html)
        self.assertNotRegex(html, PID_RE)
        self.assertEqual(LocusRegistryEntry.objects.count(), 1)
        # ...and with the flag on, the same requests serve (the pairing that makes this test mean something).
        with override_settings(LOCUS_MINTING_ENABLED=True), patch(POST, side_effect=self.gateway):
            self.assertEqual(self.preflight(token()).status_code, 200)
            self.assertEqual(self.entity(e.pid).status_code, 200)


class OffMatchesPreBranchTests(MintBase):
    """Review #8: with both flags off, /entity/locus:X/ and its /api must answer
    EXACTLY as before the branch, when they fell through to the generic entity views
    (anonymous: DRF's 401; signed in: 404 "unsupported type"; HEAD as GET). The
    control is another unsupported type, zzz:, which takes that same path today.
    Paired with flag on, where the locus route answers its own (different) 401."""

    def _answers(self, path, method):
        # A private cache, cleared per call: the anonymous entity throttle (60/min per
        # IP) would otherwise answer 429 part-way through, and leave counts behind.
        from django.core.cache import cache
        cache.clear()
        call = self.client.head if method == "HEAD" else self.client.get
        out = []
        for accept in ("text/html", "application/json", "application/ld+json"):
            r = call(path, HTTP_ACCEPT=accept)
            out.append((r.status_code, r.get("WWW-Authenticate"), r.content))
        return out

    @override_settings(LOCUS_MINTING_ENABLED=False, V4_LANDING_ENABLED=False, CACHES={
        **settings.CACHES, "default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache",
                                       "LOCATION": "locus-off-review8"}})
    def test_off_is_the_generic_entity_answer(self):
        e, _ = registry.mint_recipe(recipe(SIMPLE), self.beta)
        for who in (None, self.normal, self.beta):
            self.client.logout()
            if who is not None:
                self.client.force_login(who)
            for tail in ("", "api"):
                for method in ("GET", "HEAD"):
                    for pid in (e.pid, "ABCD-EFGH-JKMN"):
                        got = self._answers(f"/entity/locus:{pid}/{tail}", method)
                        # The 404 text names the type ("Unsupported object type: locus").
                        want = [(st, auth, body.replace(b"zzz", b"locus"))
                                for st, auth, body in self._answers(f"/entity/zzz:{pid}/{tail}", method)]
                        self.assertEqual(got, want, (who, tail, method, pid))
                        status = {a[0] for a in got}
                        self.assertEqual(status, {401} if who is None else {404}, (who, tail, method, got))
        # Presence: flag on, the locus route answers its OWN 401, unlike the control.
        self.client.logout()
        with override_settings(LOCUS_MINTING_ENABLED=True):
            on = self.client.get(f"/entity/locus:{e.pid}/", HTTP_ACCEPT="application/json")
            ctl = self.client.get(f"/entity/zzz:{e.pid}/", HTTP_ACCEPT="application/json")
        self.assertEqual((on.status_code, on.json()["error"]), (401, "authentication required"))
        self.assertNotEqual(on.content, ctl.content)


@override_settings(LOCUS_MINTING_ENABLED=True)
class MintGateTests(MintBase):
    def test_mint_routes_are_beta_gated(self):
        tok = token()
        self.assertEqual(self.preflight(tok).status_code, 403)
        self.assertEqual(self.mint(tok).status_code, 403)
        self.client.force_login(self.normal)
        self.assertEqual(self.preflight(tok).status_code, 403)
        self.assertEqual(self.mint(tok).status_code, 403)
        self.assertEqual(LocusRegistryEntry.objects.count(), 0)
        self.client.force_login(self.beta)
        self.assertEqual(self.preflight(tok).status_code, 200)
        self.assertEqual(self.mint(tok).status_code, 201)
        self.assertEqual(LocusRegistryEntry.objects.count(), 1)

    def test_entity_route_is_beta_gated(self):
        e, _ = registry.mint_recipe(recipe(SIMPLE), self.beta)
        resp = self.entity(e.pid)
        self.assertEqual(resp.status_code, 302)
        self.assertTrue(resp["Location"].startswith("/accounts/login/?next=/entity/locus"), resp["Location"])
        resp = self.entity(e.pid, accept="application/ld+json")
        self.assertEqual(resp.status_code, 401)
        self.assertEqual(resp.json()["error"], "authentication required")
        self.client.force_login(self.normal)
        self.assertEqual(self.entity(e.pid).status_code, 404)
        self.assertEqual(self.entity(e.pid, accept="application/json").status_code, 404)
        self.client.force_login(self.beta)
        with patch(POST, side_effect=self.gateway):
            self.assertEqual(self.entity(e.pid).status_code, 200)
            self.assertEqual(self.entity(e.pid, accept="application/json").status_code, 200)

    def test_place_entity_routes_are_untouched(self):
        """locus: is routed ahead of the generic entity routes; place: behaves as before."""
        resp = self.client.get("/entity/place:gn:1/", HTTP_ACCEPT="text/html")
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(resp["Location"], "/entity/place:gn:1/api")
        # An unknown object type still meets EntityDetailView's own gates (401 anonymously,
        # 404 "Unsupported object type" signed in), exactly as before this route existed.
        self.assertEqual(self.client.get("/entity/nonsense:1/", HTTP_ACCEPT="application/json").status_code, 401)
        self.client.force_login(self.beta)
        self.assertEqual(self.client.get("/entity/nonsense:1/", HTTP_ACCEPT="application/json").status_code, 404)


@override_settings(LOCUS_MINTING_ENABLED=True)
class MintFlowTests(MintBase):
    def setUp(self):
        super().setUp()
        self.client.force_login(self.beta)

    def test_page_offers_the_modal_and_says_what_is_minted(self):
        tok = token(FULL, members=["wd:Q64", "gn:1", "tgn:7003712"])
        pid = lr.recipe_pid(recipe(FULL, members=["wd:Q64", "gn:1", "tgn:7003712"]))
        html = self.page(tok).content.decode()
        self.assertIn(f'data-mint-url="/atlas/locus/mint/"', html)
        self.assertIn(f'data-mint-pid="{pid}"', html)
        self.assertIn('id="locus_mint"', html)
        self.assertNotIn('id="locus_mint" disabled', html)
        self.assertIn('id="locus_mint_modal"', html)
        self.assertIn(f"/entity/locus:{pid}", html)
        self.assertIn("names this recipe, not the records shown now", html)
        self.assertIn("re-run against the", html)
        self.assertIn('id="locus_mint_confirm"', html)
        self.assertIn("tgn:7003712", html)                 # the recipe is listed in the modal
        self.assertIn("starts with", html)
        self.assertIn("shareable but not permanent", html) # still the unminted page
        self.assertIn("unminted", html)
        self.assertNotIn('class="locus-minted-badge"', html)
        # Once minted, the page points at the locus instead of offering to mint again.
        self.assertEqual(self.mint(tok).status_code, 201)
        html = self.page(tok).content.decode()
        self.assertIn(f"Already minted: {pid}", html)
        self.assertIn(f'href="/entity/locus:{pid}/"', html)
        self.assertNotIn('id="locus_mint_modal"', html)

    def test_preflight_reports_pid_existing_and_overlap(self):
        tok = token()
        pid = lr.recipe_pid(recipe(SIMPLE))
        other, _ = registry.mint_recipe(recipe(SIMPLE, members=("gn:1", "gn:2")), self.normal)
        data = self.preflight(tok).json()
        self.assertEqual(data["pid"], pid)
        self.assertIsNone(data["minted"])
        self.assertEqual([o["pid"] for o in data["overlapping"]], [other.pid])
        self.assertEqual(data["overlapping"][0]["shared"], ["gn:1"])
        self.assertEqual(data["allowance"], {"hour": registry.MINT_LIMIT_PER_HOUR, "day": registry.MINT_LIMIT_PER_DAY})
        self.assertIn("re-run", data["statement"])
        self.assertEqual(LocusRegistryEntry.objects.count(), 1)      # a preflight mints nothing
        self.mint(tok)
        data = self.preflight(tok).json()
        self.assertEqual(data["minted"]["pid"], pid)
        self.assertEqual(data["minted"]["path"], f"/entity/locus:{pid}/")
        self.assertEqual(data["allowance"]["hour"], registry.MINT_LIMIT_PER_HOUR - 1)

    def test_post_mints_once_and_needs_confirmation(self):
        tok = token()
        pid = lr.recipe_pid(recipe(SIMPLE))
        self.assertEqual(self.mint(tok, confirm=None).status_code, 400)
        self.assertEqual(self.mint(tok, confirm=False).status_code, 400)
        self.assertEqual(self.mint(tok, confirm="yes").status_code, 400)
        self.assertEqual(LocusRegistryEntry.objects.count(), 0)
        resp = self.mint(tok)
        self.assertEqual(resp.status_code, 201)
        data = resp.json()
        self.assertEqual(data["pid"], pid)
        self.assertTrue(data["created"])
        self.assertEqual(data["path"], f"/entity/locus:{pid}/")
        self.assertTrue(data["url"].endswith(f"/entity/locus:{pid}/"))
        self.assertEqual(data["minted"]["minted_by"], "a WHG contributor")   # the name is opt-in
        again = self.mint(tok)
        self.assertEqual(again.status_code, 200)
        self.assertFalse(again.json()["created"])
        self.assertEqual(again.json()["pid"], pid)
        self.assertEqual(LocusRegistryEntry.objects.count(), 1)
        row = LocusRegistryEntry.objects.get(pid=pid)
        self.assertEqual(row.minted_by, self.beta)
        self.assertEqual(row.canonical_bytes, lr.canonical_bytes(recipe(SIMPLE)))

    def test_bad_tokens_and_forbidden_keys_are_400(self):
        self.assertEqual(self.mint("not-a-token").status_code, 400)
        self.assertEqual(self.mint("").status_code, 400)
        bad = recipe(SIMPLE)
        bad["edges"] = [["gn:1", "wd:Q64"]]
        tok = base64.urlsafe_b64encode(json.dumps(bad).encode()).decode().rstrip("=")
        resp = self.mint(tok)
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json()["path"], "edges")
        self.assertIn("place#306", resp.json()["message"])
        self.assertEqual(self.preflight(tok).status_code, 400)
        self.assertEqual(LocusRegistryEntry.objects.count(), 0)
        self.assertEqual(self.mint(token()).status_code, 201)

    def test_rate_limit_is_429_with_retry_after(self):
        with patch.object(registry, "MINT_LIMIT_PER_HOUR", 1):
            self.assertEqual(self.mint(token()).status_code, 201)
            resp = self.mint(token(members=("gn:2",)))
            self.assertEqual(resp.status_code, 429)
            self.assertEqual(resp.json()["error"], "rate limited")
            self.assertTrue(int(resp["Retry-After"]) >= 1)
            self.assertEqual(self.mint(token()).status_code, 200)   # the idempotent re-mint is free
            self.assertEqual(self.preflight(token(members=("gn:2",))).json()["allowance"]["hour"], 0)
        self.assertEqual(LocusRegistryEntry.objects.count(), 1)

    def test_collision_is_409(self):
        r = recipe(SIMPLE)
        pid = lr.recipe_pid(r)
        other = lr.canonical_bytes(recipe(SIMPLE, members=("gn:3",)))
        LocusRegistryEntry.objects.create(pid=pid, kind="recipe", version=1, canonical=other,
                                          digest=LocusRegistryEntry.digest_of(other), token="t",
                                          members=["gn:3"], minted_by=self.normal, minted_via="modal")
        self.assertEqual(self.mint(token()).status_code, 409)
        self.assertEqual(self.preflight(token()).status_code, 409)
        html = self.page(token()).content.decode()
        self.assertIn("Cannot mint (collision)", html)
        self.assertEqual(LocusRegistryEntry.objects.get(pid=pid).canonical_bytes, other)


@override_settings(LOCUS_MINTING_ENABLED=True)
class EntityLocusTests(MintBase):
    def setUp(self):
        super().setUp()
        self.client.force_login(self.beta)
        self.entry, _ = registry.mint_recipe(recipe(SIMPLE), self.beta)
        self.pid = self.entry.pid

    def test_html_is_the_locus_page_at_the_canonical_uri(self):
        with patch(POST, side_effect=self.gateway) as post:
            resp = self.entity(self.pid)
            post.assert_not_called()                        # the page's script resolves; the view does not
        self.assertEqual(resp.status_code, 200)
        self.assertIn("text/html", resp["Content-Type"])
        html = resp.content.decode()
        self.assertIn(f"Locus {self.pid}", html)
        self.assertIn('class="locus-minted-badge"', html)
        self.assertIn("This identifier is permanent; its membership is not", html)
        self.assertNotIn("shareable but not permanent", html)
        self.assertIn(f'data-pid="{self.pid}"', html)
        self.assertIn(f'data-token="{self.entry.token}"', html)
        self.assertIn('data-withdrawn="0"', html)
        self.assertIn('id="locus_recipe"', html)            # the same script re-runs the same recipe
        self.assertIn('data-member="gn:1"', html)
        self.assertIn("Minted ", html)
        self.assertNotIn('id="locus_mint"', html)           # nothing to mint on a minted page
        self.assertIn('id="locus_copy_pid"', html)
        # Browser-style Accept and the alternative spellings land on the same page.
        for spelt in (self.pid.lower(), self.pid.replace("-", "")):
            resp = self.client.get(f"/entity/locus:{spelt}/", HTTP_ACCEPT="text/html,application/xhtml+xml,*/*;q=0.8")
            self.assertEqual(resp.status_code, 200)
            self.assertIn(f"Locus {self.pid}", resp.content.decode())

    def test_json_is_honest_about_what_it_resolves(self):
        for accept, ctype in (("application/ld+json", "application/ld+json"), ("application/json", "application/json"),
                              ("*/*", "application/ld+json"), ("", "application/ld+json")):
            with patch(POST, side_effect=self.gateway):
                resp = self.entity(self.pid, accept=accept)
            self.assertEqual(resp.status_code, 200, accept)
            self.assertTrue(resp["Content-Type"].startswith(ctype), (accept, resp["Content-Type"]))
            self.assertIn("Accept", [v.strip() for v in resp["Vary"].split(",")])
        doc = json.loads(resp.content)
        self.assertTrue(doc["@id"].endswith(f"/entity/locus:{self.pid}/"))
        self.assertEqual(doc["pid"], self.pid)
        self.assertEqual(doc["kind"], "recipe")
        self.assertEqual(doc["status"], "active")
        self.assertEqual(doc["recipe"], lr.parse_recipe(recipe(SIMPLE)))
        self.assertEqual(doc["recipe_token"], self.entry.token)
        self.assertEqual(doc["minted"]["by"], "a WHG contributor")   # not opted in (rulings 2026-10-21)
        cur = doc["current"]
        self.assertFalse(cur["frozen"])
        self.assertIn("re-run", cur["statement"])
        self.assertIn("resolved_at", cur)
        self.assertEqual(cur["candidates"], [h["place_id"] for h in HITS])
        self.assertEqual(cur["candidate_total"], 3)
        self.assertEqual(cur["recipe_members"], {"present": ["gn:1", "wd:Q64"], "absent": []})
        # The server-side clusterer (search.clustering, parity-tested twin of clustering.js)
        # selects the cluster the recipe meant: HITS at the recipe's theta 0.33 put the two
        # Berlins together and the New Hampshire one apart (the shape_berlin_three parity fixture).
        self.assertEqual(cur["cluster_members"], ["gn:1", "wd:Q64"])
        self.assertEqual(cur["cluster"]["reason"], "plurality")
        self.assertEqual((cur["cluster"]["score"], cur["cluster"]["candidates"], cur["cluster"]["cluster_count"]), (2, 1, 2))
        self.assertEqual(cur["cluster"]["stoplist_count"], 500)   # the response's list, not the recipe's
        self.assertEqual(cur["cluster"]["params"]["theta"], 0.33)
        self.assertIn("clustering.js", cur["note"])
        self.assertEqual(len(self.gateway.bodies), 4)          # one real retrieval per JSON dereference
        self.assertEqual(self.gateway.bodies[-1]["query"], "Berlin")
        # /api forces JSON whatever the browser says.
        with patch(POST, side_effect=self.gateway):
            resp = self.entity(self.pid, accept="text/html", api=True)
        self.assertTrue(resp["Content-Type"].startswith("application/ld+json"))
        self.assertEqual(json.loads(resp.content)["pid"], self.pid)
        # ...and a recipe none of whose members survive says so instead of guessing.
        entry2, _ = registry.mint_recipe(recipe(SIMPLE, members=("gn:404",)), self.beta)
        with patch(POST, side_effect=self.gateway):
            cur2 = json.loads(self.entity(entry2.pid, accept="application/json").content)["current"]
        self.assertIsNone(cur2["cluster_members"])
        self.assertEqual(cur2["cluster"]["reason"], "no_members_survive")
        self.assertEqual(cur2["recipe_members"], {"present": [], "absent": ["gn:404"]})
        self.assertEqual(len(self.gateway.bodies), 6)          # still one retrieval per dereference

    def test_json_reports_a_gateway_failure_without_pretending(self):
        import requests
        with patch(POST, side_effect=requests.exceptions.ConnectionError("down")):
            resp = self.entity(self.pid, accept="application/json")
        self.assertEqual(resp.status_code, 200)               # the locus resolves; its answer says why it is empty
        cur = json.loads(resp.content)["current"]
        self.assertNotIn("candidates", cur)
        self.assertEqual(cur["error"]["status"], 503)

    def test_bad_and_unminted_pids(self):
        digits = lr.PID_ALPHABET
        wrong = self.pid[:-1] + digits[(digits.index(self.pid[-1]) + 1) % len(digits)]
        resp = self.entity(wrong, accept="application/json")
        self.assertEqual(resp.status_code, 400)
        self.assertIn("check digit", resp.json()["detail"])
        resp = self.entity(wrong)
        self.assertEqual(resp.status_code, 400)
        self.assertIn("not a well-formed locus identifier", resp.content.decode())
        unminted = lr.recipe_pid(recipe(SIMPLE, members=("gn:99",)))
        resp = self.entity(unminted, accept="application/json")
        self.assertEqual(resp.status_code, 404)
        self.assertIn("no locus has been minted", resp.json()["detail"])
        resp = self.entity(unminted)
        self.assertEqual(resp.status_code, 404)
        self.assertIn("No locus has this identifier", resp.content.decode())
        self.assertEqual(LocusRegistryEntry.objects.count(), 1)     # a dereference never mints a recipe PID

    def test_withdrawn_locus_is_a_tombstone_not_a_404(self):
        self.entry.withdraw("Withdrawn on request of the minter (test).", by=self.beta)
        with patch(POST, side_effect=self.gateway) as post:
            resp = self.entity(self.pid, accept="application/json")
            html_resp = self.entity(self.pid)
            post.assert_not_called()                            # a tombstone is not re-run
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp["X-WHG-Locus-Status"], "withdrawn")
        doc = json.loads(resp.content)
        self.assertEqual(doc["status"], "withdrawn")
        self.assertEqual(doc["withdrawn"]["reason"], "Withdrawn on request of the minter (test).")
        # Rulings 2026-10-21: the recipe text is suppressed (search.tests_locus_redaction pairs
        # each absence with its presence on an active locus).
        self.assertNotIn("recipe", doc)
        self.assertNotIn("current", doc)
        self.assertEqual(html_resp.status_code, 200)
        self.assertEqual(html_resp["X-WHG-Locus-Status"], "withdrawn")
        html = html_resp.content.decode()
        self.assertIn("Withdrawn", html)
        self.assertIn("request of the minter", html)
        self.assertIn('data-withdrawn="1"', html)
        self.assertIn("no longer shown or re-run", html)
        self.assertNotIn('id="locus_recipe"', html)
        # Still minted, still refused a re-bind, still the same PID.
        e, created = registry.mint_recipe(recipe(SIMPLE), self.normal)
        self.assertFalse(created)
        self.assertEqual(e.status, "withdrawn")

    def test_legacy_whg_id_mints_a_frozen_locus_on_first_dereference(self):
        from datasets.models import Dataset
        from places.models import Place
        mine = Dataset.objects.create(owner=self.beta, label="legacy_mine", title="Mine", description="D", public=False)
        theirs = Dataset.objects.create(owner=self.normal, label="legacy_theirs", title="Theirs", description="D",
                                        public=False)
        p1 = Place.objects.create(title="Mine 1", src_id="m1", dataset=mine, ccodes=[])
        p2 = Place.objects.create(title="Theirs 1", src_id="t1", dataset=theirs, ccodes=[])
        LegacyUnionRecord.objects.create(whg_id=10000001, place_ids=[p1.id, p2.id, 99999999],
                                         titles=["Mine 1", "Theirs 1", ""])
        self.assertEqual(LocusRegistryEntry.objects.filter(pid="10000001").count(), 0)
        resp = self.entity("10000001", accept="application/ld+json")
        self.assertEqual(resp.status_code, 200)
        doc = json.loads(resp.content)
        self.assertEqual((doc["pid"], doc["kind"], doc["minted"]["via"]), ("10000001", "legacy", "dereference"))
        self.assertIsNone(doc["minted"]["by"])
        self.assertNotIn("recipe", doc)
        cur = doc["current"]
        self.assertTrue(cur["frozen"])
        self.assertIn("carries no recipe", cur["statement"])
        self.assertEqual(cur["captured_count"], 3)
        self.assertEqual([m["places_id"] for m in cur["members"]], [p1.id])         # theirs is withheld; 99999999 gone
        self.assertEqual(cur["members"][0]["record"], f"/atlas/place/{p1.id}/")
        self.assertEqual(cur["members"][0]["place_id"], f"whg:{mine.id}:m1")
        self.assertEqual(cur["withheld"]["count"], 2)
        row = LocusRegistryEntry.objects.get(pid="10000001")
        self.assertEqual(row.members, [f"whg:{p1.id}", f"whg:{p2.id}", "whg:99999999"])
        # Second dereference: same row, no second mint; HTML lists the visible member.
        html = self.entity("10000001").content.decode()
        self.assertIn("legacy, frozen", html)
        self.assertIn("Mine 1", html)
        self.assertNotIn("Theirs 1", html)
        self.assertIn("2 records withheld", html)
        self.assertEqual(LocusRegistryEntry.objects.filter(kind="legacy").count(), 1)
        # Not in the snapshot: 404, and nothing minted.
        self.assertEqual(self.entity("10000002", accept="application/json").status_code, 404)
        self.assertEqual(self.entity("10000002").status_code, 404)
        self.assertEqual(LocusRegistryEntry.objects.filter(kind="legacy").count(), 1)
