"""place#325 Stage 5: the gloss's structured ``facets`` (places.gloss) and the Wikipedia
links held on a group's wd: members (places.wikipedia_links, places.record_group, the
gloss endpoints). The gateway is never called: ``crc_fetch_places`` is patched.

    DB_NAME=<private> GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test places.tests_gloss_facets --settings=whg.settings_localtest --noinput
"""
import json
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.cache import caches
from django.test import SimpleTestCase, TestCase, override_settings

from places.gloss import FACET_KEYS, build_gloss, member_from_hit
from places.tests_enrichment_views import LOCMEM, _PublicFixture, honest_fetch
from places.tests_gloss import m
from places.wikipedia_links import (MAX_WIKIPEDIA_LINKS, member_wikipedia, parse_wikipedia_url,
                                    wikipedia_links, wikipedia_payload)


def lamberhurst():
    return [
        m("whg:1", "Lamberhurst", "Kain & Oliver", names=["Lamberherst"], types=["parish"], ccodes=["GB"],
          years=[1659, 1851], part_of=["Kent"], is_record=True),
        m("gn:2", "Lamberhurst", "GeoNames", types=["populated place"], ccodes=["GB"]),
        m("wd:3", "Lamberhurst", "Wikidata", types=["parish"], ccodes=["GB"], years=[1700, 1900]),
    ]


class GlossFacetTests(SimpleTestCase):
    def test_facets_carry_the_sentences_verbatim_and_add_nothing(self):
        g = build_gloss(lamberhurst())
        facets = {f["key"]: f for f in g["facets"]}
        self.assertEqual([f["key"] for f in g["facets"]], [k for k in FACET_KEYS if k in facets])   # fixed order
        self.assertEqual(set(facets), {"sources", "names", "type", "countries", "dates", "unknown"})
        # Every sentence is in exactly one facet, and the facets say nothing else.
        said = [f["text"] for f in g["facets"] if f["key"] != "unknown"]
        self.assertEqual(" ".join(said), " ".join(g["sentences"]))
        self.assertEqual(facets["type"]["text"],
                         "Typed as parish (2) and populated place (1); the records do not all use the same type.")
        self.assertEqual(facets["countries"]["text"], "Country code GB in every record. Part of Kent (according to 1 record).")
        self.assertEqual(facets["dates"]["text"], "Dated 1659 to 1900 across 2 of 3 records.")
        self.assertEqual(facets["sources"]["items"], [{"label": "Kain & Oliver", "count": 1},
                                                      {"label": "GeoNames", "count": 1},
                                                      {"label": "Wikidata", "count": 1}])
        self.assertEqual(facets["names"]["items"][0], {"label": "Lamberhurst", "count": 3})
        self.assertEqual(facets["type"]["items"], [{"label": "parish", "count": 2},
                                                   {"label": "populated place", "count": 1}])
        self.assertEqual(facets["unknown"]["items"], [{"label": u} for u in g["unknown"]])
        self.assertEqual(facets["unknown"]["title"], "Not known from these records")
        self.assertIn({"label": "1 of 3 records is undated."}, facets["unknown"]["items"])

    def test_old_keys_unchanged(self):
        g = build_gloss(lamberhurst())
        self.assertEqual(set(g) - {"facets"}, {"version", "kind", "n_records", "sentences", "unknown", "limits", "facts"})
        self.assertEqual(g["version"], 1)

    def test_an_aspect_with_nothing_said_has_no_facet_but_its_unknown_is_said(self):
        g = build_gloss([m("a:1", "X", "S1"), m("a:2", "X", "S1")], kind="locus")
        keys = [f["key"] for f in g["facets"]]
        self.assertEqual(keys, ["sources", "names", "unknown"])      # untyped, no codes, undated
        unknown = [i["label"] for i in g["facets"][-1]["items"]]
        self.assertIn("No record carries a place type.", unknown)
        self.assertIn("No record is dated.", unknown)

    def test_empty_group_has_only_unknown(self):
        g = build_gloss([])
        self.assertEqual(g["facets"], [{"key": "unknown", "title": "Not known from these records", "text": "",
                                        "items": [{"label": "There are no records to describe."}]}])

    def test_deterministic(self):
        self.assertEqual(json.dumps(build_gloss(lamberhurst())), json.dumps(build_gloss(lamberhurst())))


def wd(key, links, offsite=True):
    return {"key": key, "place_id": key, "namespace": "wd", "offsite": offsite,
            "wikipedia": member_wikipedia(links)}


def sl(lang, title):
    return {"type": "seeAlso", "identifier": f"https://{lang}.wikipedia.org/wiki/{title}"}


class WikipediaLinkTests(SimpleTestCase):
    def test_parse_reads_only_wikipedia_article_urls(self):
        self.assertEqual(parse_wikipedia_url("https://en.wikipedia.org/wiki/Lamberhurst_(village)"),
                         {"lang": "en", "url": "https://en.wikipedia.org/wiki/Lamberhurst_(village)",
                          "title": "Lamberhurst (village)"})
        self.assertEqual(parse_wikipedia_url("http://zh-min-nan.m.wikipedia.org/wiki/Ti%C3%A2u")["url"],
                         "https://zh-min-nan.wikipedia.org/wiki/Ti%C3%A2u")
        self.assertEqual(parse_wikipedia_url("https://de.wikipedia.org/wiki/K%C3%B6ln")["title"], "Köln")
        for bad in ("https://www.wikidata.org/wiki/Q64", "https://en.wikipedia.org/w/index.php?title=X",
                    "javascript:alert(1)//en.wikipedia.org/wiki/X", "https://en.wikipedia.org.evil.example/wiki/X",
                    "https://evil.example/?u=https://en.wikipedia.org/wiki/X", "wd:Q64", None, 42):
            self.assertIsNone(parse_wikipedia_url(bad), bad)

    def test_dedup_english_first_then_language(self):
        members = [wd("wd:Q2", [sl("fr", "Paris"), sl("en", "Paris"), sl("de", "Paris")]),
                   wd("wd:Q1", [sl("en", "Paris"), sl("ar", "باريس")]),
                   {"key": "gn:9", "namespace": "gn", "offsite": True,      # inherited sitelinks: not read
                    "wikipedia": member_wikipedia([sl("es", "París")])}]
        out = wikipedia_links(members)
        self.assertEqual([w["lang"] for w in out], ["en", "ar", "de", "fr"])
        self.assertEqual(out[0], {"lang": "en", "url": "https://en.wikipedia.org/wiki/Paris", "title": "Paris",
                                  "from": "wd:Q1"})                     # lowest key wins a shared URL
        self.assertNotIn("es", [w["lang"] for w in out])

    def test_a_member_the_viewer_cannot_see_contributes_nothing(self):
        members = [wd("wd:Q1", [sl("en", "Seen")]), wd("wd:Q2", [sl("en", "Hidden")], offsite=False),
                   {**wd("wd:Q3", [sl("en", "Unmarked")]), "offsite": None}]
        self.assertEqual([w["title"] for w in wikipedia_links(members)], ["Seen"])          # fails closed
        self.assertEqual([w["title"] for w in wikipedia_links(members, include_withheld=True)],
                         ["Hidden", "Seen", "Unmarked"])                                      # beta's own view

    def test_never_made_from_a_qid(self):
        self.assertEqual(wikipedia_links([{"key": "wd:Q64", "namespace": "wd", "offsite": True}]), [])

    def test_cap_and_total(self):
        members = [wd(f"wd:Q{i}", [sl("en", f"T{i}")]) for i in range(MAX_WIKIPEDIA_LINKS + 5)]
        p = wikipedia_payload(members)
        self.assertEqual((len(p["wikipedia"]), p["wikipedia_total"]), (MAX_WIKIPEDIA_LINKS, MAX_WIKIPEDIA_LINKS + 5))

    def test_member_from_hit_reads_links_only_on_wd(self):
        hit = {"place_id": "wd:Q64", "namespace": "wd", "title": "Berlin",
               "links": [sl("en", "Berlin"), {"type": "seeAlso", "identifier": "https://evil.example/x"}]}
        self.assertEqual(member_from_hit(hit)["wikipedia"],
                         [{"lang": "en", "url": "https://en.wikipedia.org/wiki/Berlin", "title": "Berlin"}])
        self.assertNotIn("wikipedia", member_from_hit({**hit, "place_id": "gn:1", "namespace": "gn"}))
        self.assertNotIn("wikipedia", member_from_hit({k: v for k, v in hit.items() if k != "links"}))


# ---------------------------------------------------------------------------------------
# The Record page's gloss endpoint carries `wikipedia`, from the group's wd: members.
# ---------------------------------------------------------------------------------------
WD_REC = {"place_id": "wd:Q1", "title": "Record A", "namespace": "wd", "repr_point": [-0.12, 51.5],
          "names": [{"label": "Record A"}], "types": [], "ccodes": ["GB"],
          "geometries": [{"type": "Point", "coordinates": [-0.12, 51.5]}],
          "links": [sl("fr", "Record_A"), sl("en", "Record_A"), {"type": "closeMatch", "identifier": "gn:100"}]}


def wd_fetch(ids, user=None, allow_anonymous=False, meta=None):
    extra = {"wd:Q1": WD_REC}
    out = honest_fetch([i for i in ids if i not in extra], user=user, allow_anonymous=allow_anonymous, meta=meta)
    if getattr(user, "is_authenticated", False) or allow_anonymous:
        out.update({i: extra[i] for i in ids if i in extra})
    return out


class _WikipediaFixture(_PublicFixture):
    def setUp(self):
        super().setUp()
        from api.models import GazetteerRegistryEntry
        from licensing.models import License
        from places.models import PlaceLink
        lic = License.objects.get(spdx_id="CC-BY-4.0")
        self.wd_row, _ = GazetteerRegistryEntry.objects.update_or_create(
            id="wd", defaults=dict(name="Wikidata", namespace="wd", entry_class="authority", status="published",
                                   redistributable=True, license=lic))
        PlaceLink.objects.create(place=self.A, jsonb={"type": "exactMatch", "identifier": "wd:Q1"})


@override_settings(CACHES=LOCMEM, EDOP_ENABLED=True, V4_LANDING_ENABLED=True)
class RecordGlossWikipediaTests(_WikipediaFixture):
    def test_anonymous_and_beta_get_the_wd_members_links(self):
        for user in (None, self.beta):
            caches["default"].clear()
            r = self.gloss(self.A.pk, user=user, fetch=wd_fetch)
            self.assertEqual(r.status_code, 200)
            body = r.json()
            self.assertIn("wd:Q1", {mm["key"] for mm in body["members"]})
            self.assertEqual(body["wikipedia"], [
                {"lang": "en", "url": "https://en.wikipedia.org/wiki/Record_A", "title": "Record A", "from": "wd:Q1"},
                {"lang": "fr", "url": "https://fr.wikipedia.org/wiki/Record_A", "title": "Record A", "from": "wd:Q1"}])
            self.assertEqual(body["wikipedia_total"], 2)
            self.assertIn("facets", body["gloss"])

    def test_without_links_in_the_answer_there_are_none(self):
        def no_links(ids, **kw):
            out = wd_fetch(ids, **kw)
            return {k: {kk: vv for kk, vv in v.items() if kk != "links"} for k, v in out.items()}
        r = self.gloss(self.A.pk, user=self.beta, fetch=no_links).json()
        self.assertIn("wd:Q1", {mm["key"] for mm in r["members"]})      # presence: the member is there
        self.assertEqual(r["wikipedia"], [])                             # ... but no link was held


@override_settings(CACHES=LOCMEM, EDOP_ENABLED=True, V4_LANDING_ENABLED=True)
class RecordGlossWikipediaVisibilityTests(_WikipediaFixture):
    """Wikidata embargoed (hypothetically): a beta user sees the wd: member and its
    links; an anonymous visitor sees neither."""

    def setUp(self):
        super().setUp()
        self.wd_row.status = "embargoed"
        self.wd_row.save()

    def test_a_link_from_a_member_the_viewer_cannot_see_is_never_returned(self):
        beta = self.gloss(self.A.pk, user=self.beta, fetch=wd_fetch).json()
        self.assertEqual([w["from"] for w in beta["wikipedia"]], ["wd:Q1", "wd:Q1"])       # presence
        caches["default"].clear()
        anon = self.gloss(self.A.pk, fetch=wd_fetch).json()
        self.assertEqual(anon["gloss"]["n_records"], 3)                                     # it was served
        self.assertNotIn("wd:Q1", {mm["key"] for mm in anon["members"]})
        self.assertEqual((anon["wikipedia"], anon["wikipedia_total"]), ([], 0))
        self.assertNotIn("wikipedia.org", json.dumps(anon))


@override_settings(CACHES=LOCMEM, EDOP_ENABLED=True)
class LocusGlossWikipediaTests(TestCase):
    def test_posted_wd_hit_links_are_read(self):
        User = get_user_model()
        beta = User.objects.create_user(username="wf_beta", email="wf_beta@example.org", password="pw",
                                        given_name="W", surname="Beta", role="beta_tester")
        self.client.force_login(beta)
        r = self.client.post("/atlas/locus/gloss/", json.dumps({"members": [
            {"place_id": "gn:1", "namespace": "gn", "title": "Berlin", "links": [sl("de", "Berlin")]},
            {"place_id": "wd:Q64", "namespace": "wd", "title": "Berlin", "links": [sl("de", "Berlin"), sl("en", "Berlin")]},
        ]}), content_type="application/json").json()
        self.assertEqual([(w["lang"], w["from"]) for w in r["wikipedia"]], [("en", "wd:Q64"), ("de", "wd:Q64")])
        self.assertEqual([f["key"] for f in r["gloss"]["facets"]][:2], ["sources", "names"])
