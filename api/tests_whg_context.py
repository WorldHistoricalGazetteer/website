"""place#325 ruling 2026-10-21 (release-behaviour interview): the locus and record
JSON-LD documents name a REAL, dereferenceable, minimal WHG context, served at
``/ns/whg-context.jsonld`` (provisional until PLATO), instead of the placeholder
``@vocab`` IRIs ``https://whgazetteer.org/ns/locus#`` and ``.../record#``. Run locally:

    DB_NAME=whgv3beta_f4 GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test api.tests_whg_context --settings=whg.settings_localtest

* the context is served always (switch on AND off), GET, ``application/ld+json``,
  with a SHORT ``Cache-Control`` (it is provisional); CORS is nginx's (see the view);
* every key either document emits is a term the context defines (no ``@vocab``, so an
  undefined key cannot pass by falling through to a catch-all vocabulary). The walker
  is proved able to fail on a planted key, beside the real documents that pass;
* each document expands under a real JSON-LD 1.1 processor (pyld), with the context
  fetched from this app, so the context is valid and its IRIs are what we say.

The gateway is faked at ``api.crc_client.requests.post`` (search.tests_locus), so a
recipe locus's ``current`` block is the real retrieval + server clustering output.
"""
import json
from unittest.mock import patch

from django.test import Client, override_settings

from places import locus_registry as registry
from places.models import LegacyUnionRecord
from places.tests_v4_landing import LOCMEM, ON, _Fixture
from search.tests_locus import HITS, POST, SIMPLE, recipe
from search.tests_locus_mint import MintBase

CONTEXT_PATH = "/ns/whg-context.jsonld"


def load_context(client=None):
    resp = (client or Client()).get(CONTEXT_PATH)
    assert resp.status_code == 200, resp.status_code
    return json.loads(resp.content)


def undefined_keys(doc, context: dict, path="$") -> list:
    """Every key in ``doc`` that ``context`` does not define, as JSON paths. Keywords
    (``@id`` ...) are JSON-LD's own. A term typed ``@json`` is a JSON literal: what is
    inside it is a value, not a set of terms, so it is not walked."""
    ctx = context["@context"]
    out = []
    if isinstance(doc, dict):
        for k, v in doc.items():
            if k.startswith("@"):
                continue
            if k not in ctx:
                out.append(f"{path}.{k}")
                continue
            d = ctx[k]
            if isinstance(d, dict) and d.get("@type") == "@json":
                continue
            out += undefined_keys(v, context, f"{path}.{k}")
    elif isinstance(doc, list):
        for i, v in enumerate(doc):
            out += undefined_keys(v, context, f"{path}[{i}]")
    return out


def expand(doc, client):
    """pyld expansion with the context fetched from THIS app (never the network)."""
    from pyld import jsonld

    def loader(url, options=None):
        assert url.endswith(CONTEXT_PATH), url        # nothing else may be fetched
        resp = client.get(CONTEXT_PATH)
        return {"contentType": "application/ld+json", "contextUrl": None, "documentUrl": url,
                "document": json.loads(resp.content)}
    return jsonld.expand(doc, {"documentLoader": loader})


class ContextDocumentTests(_Fixture):
    def check_served(self):
        resp = self.client.get(CONTEXT_PATH)
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp["Content-Type"].startswith("application/ld+json"), resp["Content-Type"])
        # CORS comes from nginx on every GET (both hosts); a second copy from Django
        # would make it "*, *", which browsers reject (measured on prod /schema/).
        self.assertNotIn("Access-Control-Allow-Origin", resp)
        cc = resp["Cache-Control"]
        self.assertIn("public", cc)
        max_age = int(cc.split("max-age=")[1].split(",")[0])
        self.assertLessEqual(max_age, 3600)             # provisional: may change, so short
        self.assertGreater(max_age, 0)
        body = json.loads(resp.content)
        self.assertIn("@context", body)
        self.assertNotIn("@vocab", body["@context"])    # no catch-all: every term is explicit
        self.assertEqual(body["@context"]["@version"], 1.1)
        self.assertTrue(body["provisional"])
        self.assertIn("PLATO", body["note"])
        return resp

    @ON
    def test_served_with_the_switch_on(self):
        self.check_served()

    @override_settings(V4_LANDING_ENABLED=False, CACHES=LOCMEM)
    def test_served_with_the_switch_off_too(self):
        """A context reveals nothing, so it is not behind the release switch."""
        self.check_served()
        self.assertEqual(self.anon(CONTEXT_PATH).status_code, 200)

    def test_get_and_head_only(self):
        self.assertEqual(self.client.head(CONTEXT_PATH).status_code, 200)
        self.assertEqual(self.client.post(CONTEXT_PATH, {}).status_code, 405)
        self.assertEqual(self.client.get("/ns/whg-context.json").status_code, 404)

    def test_walker_can_fail(self):
        """Paired with the real documents below: a planted key, nested anywhere outside
        a JSON literal, is reported; the same key inside an @json literal is not."""
        ctx = load_context(self.client)
        good = {"@context": "x", "@id": "y", "title": "T", "minted": {"at": "now"}}
        self.assertEqual(undefined_keys(good, ctx), [])
        bad = {"title": "T", "minted": {"at": "now", "planted": 1}, "concordance": {"outbound": [{"bogus": 1}]}}
        self.assertEqual(undefined_keys(bad, ctx), ["$.minted.planted", "$.concordance.outbound[0].bogus"])
        self.assertEqual(undefined_keys({"recipe": {"anything": {"goes": 1}}}, ctx), [])


@ON
class RecordDocumentContextTests(_Fixture):
    def setUp(self):
        super().setUp()
        from places.models import PlaceLink
        PlaceLink.objects.create(place=self.Pub, jsonb={"type": "closeMatch", "identifier": "gn:2950159"})
        PlaceLink.objects.create(place=self.Pub, jsonb={"type": "exactMatch", "identifier": "wd:None"})
        PlaceLink.objects.create(place=self.Pub2, jsonb={"type": "closeMatch", "identifier": f"whg:{self.Pub.pk}"})
        p = patch("api.crc_client.crc_fetch_places", return_value={})
        p.start()
        self.addCleanup(p.stop)

    def docs(self):
        pub = json.loads(self.anon(f"/entity/place:{self.Pub.pk}/", HTTP_ACCEPT="application/ld+json").content)
        mine = json.loads(self.as_user(self.owner, f"/entity/place:{self.Priv.pk}/",
                                       HTTP_ACCEPT="application/ld+json").content)
        return pub, mine

    def test_record_documents_name_the_served_context_and_use_only_its_terms(self):
        ctx = load_context(self.client)
        pub, mine = self.docs()
        for doc in (pub, mine):
            self.assertEqual(doc["@context"], f"http://testserver{CONTEXT_PATH}")
            self.assertEqual(undefined_keys(doc, ctx), [], doc)
        # The documents exercise the parts of the shape the context must cover:
        conc = pub["concordance"]
        self.assertTrue(conc["outbound"] and conc["inbound"])
        self.assertTrue(any(e.get("placeholder") for e in conc["outbound"]))
        self.assertTrue(any(e.get("records") for e in conc["inbound"]))
        self.assertIsNone(mine["concordance"])
        self.assertIn("concordance_note", mine)
        self.assertNotIn("ns/record#", json.dumps(pub))

    def test_record_document_expands_under_a_json_ld_processor(self):
        pub, _ = self.docs()
        (node,) = expand(pub, self.client)
        self.assertEqual(node["@id"], f"http://testserver/entity/place:{self.Pub.pk}/")
        self.assertEqual(node["@type"], ["https://whgazetteer.org/ns/whg-context.jsonld#Record"])
        self.assertEqual(node["http://purl.org/dc/terms/title"], [{"@value": "Public Town"}])
        self.assertIn("http://www.w3.org/2000/01/rdf-schema#seeAlso", node)    # lpf, typed @id
        self.assertEqual(node["http://www.w3.org/2000/01/rdf-schema#seeAlso"][0]["@id"],
                         f"http://testserver/entity/place:{self.Pub.pk}/api")


@override_settings(V4_LANDING_ENABLED=True, CACHES=LOCMEM)
class LocusDocumentContextTests(MintBase):
    def setUp(self):
        super().setUp()
        self.client.force_login(self.beta)

    def get(self, pid, client=None):
        with patch(POST, side_effect=self.gateway):
            resp = (client or self.client).get(f"/entity/locus:{pid}/", HTTP_ACCEPT="application/ld+json")
        self.assertEqual(resp.status_code, 200, resp.content[:300])
        return json.loads(resp.content)

    def locus_docs(self):
        from datasets.models import Dataset
        from places.models import Place
        active, _ = registry.mint_recipe(recipe(SIMPLE), self.beta, minter_public=True)
        gone, _ = registry.mint_recipe(recipe(SIMPLE, members=("gn:404",)), self.beta)
        withdrawn, _ = registry.mint_recipe(recipe(SIMPLE, members=("gn:2",)), self.beta)
        withdrawn.withdraw("Withdrawn on request (test).", by=self.beta)
        redacted, _ = registry.mint_recipe(recipe(SIMPLE, members=("wd:Q64",)), self.beta)
        redacted.redact("staff reason (test)", by=self.beta)
        ds = Dataset.objects.create(owner=self.beta, label="ctx_legacy", title="L", description="D", public=True)
        p1 = Place.objects.create(title="Legacy 1", src_id="l1", dataset=ds, ccodes=[])
        LegacyUnionRecord.objects.create(whg_id=10000001, place_ids=[p1.id, 99999999], titles=["Legacy 1", ""])
        docs = {"active": self.get(active.pid), "no_members_survive": self.get(gone.pid),
                "withdrawn": self.get(withdrawn.pid), "redacted": self.get(redacted.pid),
                "legacy": self.get("10000001")}
        with patch("api.views_locus.CLUSTER_MAX_HITS", 1):
            docs["skipped"] = self.get(active.pid)
        import requests
        with patch(POST, side_effect=requests.exceptions.ConnectionError("down")):
            docs["gateway_down"] = json.loads(self.client.get(f"/entity/locus:{active.pid}/",
                                                              HTTP_ACCEPT="application/ld+json").content)
        return docs

    def test_locus_documents_name_the_served_context_and_use_only_its_terms(self):
        ctx = load_context(self.client)
        docs = self.locus_docs()
        for name, doc in docs.items():
            self.assertEqual(doc["@context"], f"http://testserver{CONTEXT_PATH}", name)
            self.assertEqual(undefined_keys(doc, ctx), [], (name, doc))
            self.assertNotIn("ns/locus#", json.dumps(doc))
        # Presence: each variant really has the shape it is here for.
        self.assertEqual(docs["active"]["current"]["cluster_members"], ["gn:1", "wd:Q64"])
        self.assertIn("params", docs["active"]["current"]["cluster"])
        self.assertTrue(docs["active"]["minted"]["by"])
        self.assertEqual(docs["no_members_survive"]["current"]["cluster"]["reason"], "no_members_survive")
        self.assertEqual(docs["skipped"]["current"]["cluster"]["skipped"], "too_many_hits")
        self.assertIn("error", docs["gateway_down"]["current"])
        self.assertIn("withdrawn", docs["withdrawn"])
        self.assertIn("redacted", docs["redacted"])
        self.assertTrue(docs["legacy"]["current"]["frozen"])
        self.assertTrue(docs["legacy"]["current"]["members"])
        self.assertTrue(docs["legacy"]["current"]["withheld"])
        self.assertEqual(len(HITS), docs["active"]["current"]["candidate_total"])

    def test_locus_documents_expand_under_a_json_ld_processor(self):
        docs = self.locus_docs()
        for name, doc in docs.items():
            (node,) = expand(doc, self.client)
            self.assertEqual(node["@type"], ["https://whgazetteer.org/ns/whg-context.jsonld#Locus"], name)
            self.assertTrue(node["@id"].endswith(f"/entity/locus:{doc['pid']}/"), name)
            # pid is NOT a second @id (the old context mapped it so, which collides with
            # @id): it is the identifier, Dublin Core's term.
            self.assertEqual(node["http://purl.org/dc/terms/identifier"], [{"@value": doc["pid"]}], name)
        active = expand(docs["active"], self.client)[0]
        recipe_iri = "https://whgazetteer.org/ns/whg-context.jsonld#recipe"
        self.assertEqual(active[recipe_iri][0]["@type"], "@json")          # the recipe is carried verbatim
        self.assertEqual(active[recipe_iri][0]["@value"], docs["active"]["recipe"])
