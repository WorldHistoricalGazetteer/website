"""place#325 Stage 1 — the concordance helper (places/concordance.py).

Run locally (never in a prod container):

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test places.tests_concordance --settings=whg.settings_localtest

Every absence asserted here sits beside a presence asserted in the same test,
on the same structure, so a helper that returned nothing would fail (see
~/.claude/memory/a-check-that-cannot-fail.md). The CRC gateway is faked at
``api.crc_client.crc_fetch_places``.
"""
from datetime import timedelta
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.gis.geos import Point
from django.test import TestCase
from django.utils import timezone

from places.concordance import (
    build_concordance, is_self_link, normalise_identifier, normalise_link_type, resolve_targets,
)

GATEWAY_DB = {
    "gn:100": {"place_id": "gn:100", "title": "Gateway Town", "namespace": "gn",
               "repr_point": [10.0, 20.0], "geometries": [{"repr_point": [10.0, 20.0]}]},
    "gn:200": {"place_id": "gn:200", "title": "Pointless", "namespace": "gn",
               "repr_point": None, "geometries": []},
}


def fake_fetch(ids, user=None, allow_anonymous=False, meta=None):
    fake_fetch.calls.append(list(ids))
    if user is None or not getattr(user, "is_authenticated", False):
        if meta is not None:
            meta["disabled"] = True
        return {}
    return {i: GATEWAY_DB[i] for i in ids if i in GATEWAY_DB}


fake_fetch.calls = []


def failing_fetch(ids, user=None, allow_anonymous=False, meta=None):
    if meta is not None:
        meta["error"] = "timeout"
    return {}


class NormalisationTests(TestCase):
    def test_identifiers(self):
        self.assertEqual(normalise_identifier("https://www.wikidata.org/wiki/Q90"), ("wd:Q90", "wd", "Q90"))
        self.assertEqual(normalise_identifier("http://sws.geonames.org/745044/"), ("gn:745044", "gn", "745044"))
        self.assertEqual(normalise_identifier("http://vocab.getty.edu/page/tgn/7010731"),
                         ("tgn:7010731", "tgn", "7010731"))
        self.assertEqual(normalise_identifier("wikidata:q90"), ("wd:Q90", "wd", "Q90"))
        self.assertEqual(normalise_identifier("whg:81991"), ("whg:81991", "whg", "81991"))
        # unrecognised URL: verbatim, no namespace
        self.assertEqual(normalise_identifier("https://example.org/x/1"), ("https://example.org/x/1", None, None))

    def test_link_types(self):
        self.assertEqual(normalise_link_type("sameAs"), ("exactMatch", True))
        self.assertEqual(normalise_link_type("owl:sameAs"), ("exactMatch", True))
        self.assertEqual(normalise_link_type("skos:closeMatch"), ("closeMatch", True))
        self.assertEqual(normalise_link_type("exactMatch"), ("exactMatch", True))
        self.assertEqual(normalise_link_type("related"), ("related", False))
        self.assertEqual(normalise_link_type("seeAlso"), ("seeAlso", False))
        self.assertEqual(normalise_link_type(None), (None, False))

    def test_self_link(self):
        self.assertTrue(is_self_link("tgn:7010731", 5, "7010731"))
        self.assertTrue(is_self_link("whg:5", 5, "abc"))
        self.assertTrue(is_self_link("https://example.org/p/9", 5, "https://example.org/p/9"))
        self.assertFalse(is_self_link("tgn:7010732", 5, "7010731"))
        self.assertFalse(is_self_link("whg:6", 5, "abc"))
        self.assertFalse(is_self_link("gn:1", 5, ""))


class ConcordanceTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        from api.models import GazetteerRegistryEntry
        from datasets.models import Dataset
        from django_celery_results.models import TaskResult
        from places.models import CloseMatch, Place, PlaceGeom, PlaceLink

        User = get_user_model()
        cls.user = User.objects.create_user(username="c_owner", email="c_owner@example.org", password="pw",
                                            given_name="C", surname="Owner")
        cls.beta = User.objects.create_user(username="c_beta", email="c_beta@example.org", password="pw",
                                            given_name="C", surname="Beta", role="beta_tester")

        def ds(key, public=True):
            return Dataset.objects.create(owner=cls.user, label=f"c325_{key}", title=f"Dataset {key}",
                                          description="D", creator="X", public=public, ds_status="indexed",
                                          uri_base="https://example.org/places/")

        def place(d, title, src_id, xy=None, links=()):
            p = Place.objects.create(title=title, src_id=src_id, dataset=d, ccodes=[])
            if xy:
                PlaceGeom.objects.create(place=p, geom=Point(*xy, srid=4326),
                                         jsonb={"type": "Point", "coordinates": list(xy)})
            for t, ident in links:
                PlaceLink.objects.create(place=p, jsonb={"type": t, "identifier": ident})
            return p

        with patch("datasets.signals.doi"):
            cls.ds_a = ds("a")
            cls.ds_b = ds("b")
            cls.ds_tgn = ds("tgn")
            cls.ds_priv = ds("priv", public=False)
            cls.ds_emb = ds("emb")
        GazetteerRegistryEntry.objects.create(id=f"whg:{cls.ds_emb.pk}", name="Emb", namespace="whg",
                                              entry_class="dataset", status="embargoed")
        # Migrations seed some authority rows; pin the ones this test relies on.
        for ns in ("gn", "wd"):
            GazetteerRegistryEntry.objects.update_or_create(
                id=ns, defaults=dict(name=ns, namespace=ns, entry_class="authority", status="published"))
        GazetteerRegistryEntry.objects.update_or_create(
            id="tm", defaults=dict(name="tm", namespace="tm", entry_class="authority", status="embargoed",
                                   embargo_release_at=timezone.now() + timedelta(days=30)))
        GazetteerRegistryEntry.objects.filter(namespace__in=["viaf", "tgn", "xx"]).delete()

        # An authority record WHG holds in Postgres: TGN 7000, self-linked by src_id.
        cls.T = place(cls.ds_tgn, "Tgn Place", "7000", xy=(1.0, 2.0), links=[("closeMatch", "tgn:7000")])
        cls.P = place(cls.ds_priv, "Secret Private", "p1", xy=(3.0, 3.0))

        cls.A = place(cls.ds_a, "Record A", "A1", xy=(0.0, 0.0), links=[
            ("closeMatch", "xx:A1"),                 # self (local == src_id)
            ("closeMatch", "tgn:7000"),              # -> Postgres (T)
            ("exactMatch", "gn:100"),                # -> gateway, with geometry
            ("sameAs", "https://www.wikidata.org/wiki/Q1"),  # alias; gateway: not found
            ("related", "wd:Q2"),                    # listed, not aggregated
            ("closeMatch", "viaf:55"),               # not held anywhere
            ("closeMatch", "tm:5"),                  # embargoed namespace: never asked
            ("closeMatch", "gn:200"),                # gateway, no geometry
        ])
        PlaceLink.objects.create(place=cls.A, jsonb={"type": "closeMatch", "identifier": f"whg:{cls.A.pk}"})
        PlaceLink.objects.create(place=cls.A, jsonb={"type": "closeMatch", "identifier": f"whg:{cls.P.pk}"})

        # tier 2 (inbound place_link) — public B, and private D / embargoed E that must not render
        cls.B = place(cls.ds_b, "Record B", "B1", xy=(0.1, 0.1), links=[("closeMatch", f"whg:{cls.A.pk}")])
        cls.D = place(cls.ds_priv, "Secret D", "D1", links=[("closeMatch", f"whg:{cls.A.pk}")])
        cls.Bseealso = place(cls.ds_b, "Record B2", "B2", links=[("seeAlso", f"whg:{cls.A.pk}")])
        # tier 3 (co-citation of gn:100, stored as a URL) — public C, embargoed E
        cls.C = place(cls.ds_b, "Record C", "C1", links=[("closeMatch", "https://www.geonames.org/100")])
        cls.E = place(cls.ds_emb, "Embargoed E", "E1", links=[("closeMatch", "gn:100")])
        # also co-cites tgn:7000 (not T itself, which self-links it)
        cls.C2 = place(cls.ds_b, "Record C2", "C2", links=[("closeMatch", "http://vocab.getty.edu/tgn/7000")])

        # close_matches: F imported (direction unknown), G reviewed from A's side, H private
        cls.F = place(cls.ds_b, "Record F", "F1", xy=(0.2, 0.2))
        cls.G = place(cls.ds_b, "Record G", "G1")
        cls.H = place(cls.ds_priv, "Secret H", "H1")
        task = TaskResult.objects.create(task_id="t-325", task_name="align_idx",
                                         task_kwargs=repr({"ds": cls.ds_a.pk, "user": cls.user.pk}))
        CloseMatch.objects.create(place_a=cls.A, place_b=cls.F, created_by=cls.user, basis="imported")
        CloseMatch.objects.create(place_a=cls.A, place_b=cls.G, created_by=cls.user, basis="reviewed", task=task)
        CloseMatch.objects.create(place_a=cls.A, place_b=cls.H, created_by=cls.user, basis="imported")

    def setUp(self):
        fake_fetch.calls = []
        # premises
        self.assertTrue(self.ds_emb.public)
        self.assertFalse(self.ds_priv.public)
        self.assertFalse(self.user.can_access_beta)
        self.assertTrue(self.beta.can_access_beta)

    def build(self, place=None, user="default"):
        with patch("api.crc_client.crc_fetch_places", side_effect=fake_fetch):
            return build_concordance(place or self.A, user=self.user if user == "default" else user)

    @staticmethod
    def by_ident(entries):
        return {e["identifier"]: e for e in entries}

    @staticmethod
    def record_ids(entries):
        return {e["record"]["place_id"] for e in entries if e.get("record")}

    def test_private_record_has_no_concordance(self):
        self.assertIsNotNone(self.build(self.A))
        self.assertIsNone(self.build(self.D))
        self.assertIsNone(self.build(self.E))

    def test_self_links_excluded_everywhere(self):
        c = self.build()
        out = self.by_ident(c["outbound"])
        self.assertIn("tgn:7000", out)              # presence
        self.assertNotIn("xx:A1", out)              # self by src_id
        self.assertNotIn(f"whg:{self.A.pk}", out)   # self by pk
        self.assertEqual(c["excluded_self_links"], 2)
        self.assertIn("xx:A1", c["self_identifiers"])
        # T's own self-link to tgn:7000 does not make T a co-citer
        cocited = self.record_ids(c["cocited"])
        self.assertIn(self.C2.id, cocited)
        self.assertNotIn(self.T.id, cocited)

    def test_outbound_types_and_resolution(self):
        c = self.build()
        out = self.by_ident(c["outbound"])
        t = out["tgn:7000"]["target"]
        self.assertEqual((t["status"], t["via"], t["has_geometry"]), ("resolved", "postgres", True))
        self.assertEqual(t["records"][0]["place_id"], self.T.id)
        self.assertEqual(t["records"][0]["repr_point"], [1.0, 2.0])

        g = out["gn:100"]
        self.assertEqual((g["link_type"], g["aggregates"]), ("exactMatch", True))
        self.assertEqual((g["target"]["status"], g["target"]["via"], g["target"]["has_geometry"]),
                         ("resolved", "gateway", True))
        self.assertEqual(out["gn:200"]["target"]["status"], "resolved")
        self.assertFalse(out["gn:200"]["target"]["has_geometry"])

        w = out["wd:Q1"]
        self.assertEqual((w["link_type"], w["link_type_raw"], w["aggregates"]), ("exactMatch", "sameAs", True))
        self.assertEqual(w["target"]["status"], "not_found")

        r = out["wd:Q2"]
        self.assertEqual((r["link_type"], r["aggregates"]), ("related", False))
        self.assertEqual(out["viaf:55"]["target"]["status"], "not_held")
        self.assertEqual(out["tm:5"]["target"]["status"], "not_held")
        asked = {i for call in fake_fetch.calls for i in call}
        self.assertIn("gn:100", asked)
        self.assertNotIn("tm:5", asked)      # embargoed namespace never asked
        self.assertNotIn("viaf:55", asked)
        self.assertNotIn("tgn:7000", asked)  # held in Postgres: no gateway call
        for e in c["outbound"]:
            self.assertIn(e["provenance"]["basis"], ("contributed", "reviewed", "imported", "authid", "unknown"))

    def test_embargoed_namespace_asked_only_for_beta(self):
        """A namespace embargo hides it from non-beta users only (registry visible_to)."""
        self.build(user=self.beta)
        asked = {i for call in fake_fetch.calls for i in call}
        self.assertIn("tm:5", asked)
        self.assertIn("gn:100", asked)

    def test_private_target_does_not_render(self):
        c = self.build()
        out = self.by_ident(c["outbound"])
        # the link is the record's own data and is listed ...
        self.assertIn(f"whg:{self.P.pk}", out)
        # ... but the private record behind it is not resolved or named anywhere
        self.assertEqual(out[f"whg:{self.P.pk}"]["target"]["status"], "unavailable")
        self.assertEqual(out[f"whg:{self.P.pk}"]["target"]["records"], [])
        self.assertIn("Record B", repr(c))
        self.assertNotIn("Secret", repr(c))

    def test_inbound_attributed_and_private_excluded(self):
        c = self.build()
        inbound = {e["record"]["place_id"]: e for e in c["inbound"]}
        self.assertIn(self.B.id, inbound)
        b = inbound[self.B.id]
        self.assertEqual((b["tier"], b["source"], b["link_type"], b["aggregates"]),
                         (2, "place_link", "closeMatch", True))
        self.assertEqual(b["attested_by"]["dataset"], self.ds_b.label)
        self.assertNotIn(self.D.id, inbound)           # private attester
        s = inbound[self.Bseealso.id]                  # seeAlso: listed, not aggregated
        self.assertFalse(s["aggregates"])

    def test_close_matches_direction_and_provenance(self):
        c = self.build()
        inbound = {e["record"]["place_id"]: e for e in c["inbound"] if e["source"] == "close_matches"}
        outbound_cm = [e for e in c["outbound"] if e["source"] == "close_matches"]
        self.assertIn(self.F.id, inbound)
        f = inbound[self.F.id]
        self.assertEqual((f["direction"], f["provenance"]["basis"]), ("unknown", "imported"))
        self.assertIn("unknown", f["provenance"]["label"])
        self.assertEqual(len(outbound_cm), 1)
        g = outbound_cm[0]
        self.assertEqual(g["target"]["records"][0]["place_id"], self.G.id)
        self.assertEqual((g["tier"], g["direction"], g["provenance"]["basis"]), (1, "outbound", "reviewed"))
        self.assertNotIn(self.H.id, inbound)  # private partner
        self.assertNotIn(self.H.id, {r["place_id"] for e in outbound_cm for r in e["target"]["records"]})

    def test_cocited_listed_never_aggregated(self):
        c = self.build()
        cocited = {e["record"]["place_id"]: e for e in c["cocited"]}
        self.assertIn(self.C.id, cocited)
        self.assertEqual(cocited[self.C.id]["cocited_identifier"], "gn:100")
        self.assertNotIn(self.E.id, cocited)  # embargoed dataset
        self.assertTrue(all(e["aggregates"] is False and e["tier"] == 3 for e in c["cocited"]))
        agg_ids = {m["place_id"] for m in c["aggregate"]}
        self.assertIn(self.B.id, agg_ids)
        self.assertNotIn(self.C.id, agg_ids)
        self.assertNotIn(self.C2.id, agg_ids)

    def test_aggregate_subset(self):
        c = self.build()
        agg = {m["place_id"]: m for m in c["aggregate"]}
        for pid in (self.T.id, "gn:100", "gn:200", self.B.id, self.F.id, self.G.id):
            self.assertIn(pid, agg)
        self.assertTrue(agg["gn:100"]["has_geometry"])
        self.assertFalse(agg["gn:200"]["has_geometry"])
        self.assertEqual(agg[self.T.id]["tiers"], [1])
        self.assertEqual(agg[self.B.id]["tiers"], [2])
        # related, seeAlso, not-found, not-held, co-cited, private: none aggregate
        for absent in ("wd:Q2", "wd:Q1", "viaf:55", self.Bseealso.id, self.C.id, self.P.id, self.H.id, self.A.id):
            self.assertNotIn(absent, agg)

    def test_no_closure(self):
        """B attests A; another record attests B. That record is two steps from A."""
        from places.models import Place, PlaceLink
        with patch("datasets.signals.doi"):
            far = Place.objects.create(title="Far Record", src_id="Z1", dataset=self.ds_b, ccodes=[])
        PlaceLink.objects.create(place=far, jsonb={"type": "closeMatch", "identifier": f"whg:{self.B.pk}"})
        c = self.build()
        ids = self.record_ids(c["inbound"]) | self.record_ids(c["cocited"]) | {m["place_id"] for m in c["aggregate"]}
        self.assertIn(self.B.id, ids)
        self.assertNotIn(far.id, ids)
        c_b = self.build(self.B)
        self.assertIn(far.id, self.record_ids(c_b["inbound"]))  # it IS one step from B

    def test_gateway_failure_is_not_absence(self):
        with patch("api.crc_client.crc_fetch_places", side_effect=failing_fetch):
            c = build_concordance(self.A, user=self.user)
        out = self.by_ident(c["outbound"])
        self.assertEqual(out["gn:100"]["target"]["status"], "gateway_unavailable")
        self.assertEqual(out["tgn:7000"]["target"]["status"], "resolved")  # Postgres unaffected
        agg = {m["place_id"] for m in c["aggregate"]}
        self.assertIn(self.T.id, agg)
        self.assertNotIn("gn:100", agg)

    def test_anonymous_gateway_disabled(self):
        c = self.build(user=None)
        out = self.by_ident(c["outbound"])
        self.assertEqual(out["gn:100"]["target"]["status"], "gateway_disabled")
        self.assertEqual(out["tgn:7000"]["target"]["status"], "resolved")

    def test_gateway_disabled_says_which_cause(self):
        """Review 2026-10-21: a signed-out visitor was told the index is "not
        configured on this server". The REAL crc_fetch_places decides here (it calls
        nothing in either case: anonymous, or no CRC_GATEWAY_URL)."""
        from django.test import override_settings
        with override_settings(CRC_GATEWAY_URL="http://gw.invalid"), \
                patch("requests.post", side_effect=AssertionError("network")):
            anon = resolve_targets(["gn:100", "tgn:7000"], user=None)
        with override_settings(CRC_GATEWAY_URL=""), patch("requests.post", side_effect=AssertionError("network")):
            unconf = resolve_targets(["gn:100"], user=self.user)
        self.assertEqual((anon["gn:100"]["status"], anon["gn:100"]["reason"]), ("gateway_disabled", "anonymous"))
        self.assertEqual((unconf["gn:100"]["status"], unconf["gn:100"]["reason"]),
                         ("gateway_disabled", "not_configured"))
        self.assertEqual(anon["tgn:7000"]["status"], "resolved")            # presence: Postgres still answers

    def test_resolve_targets_every_input_answered(self):
        with patch("api.crc_client.crc_fetch_places", side_effect=fake_fetch):
            r = resolve_targets(["tgn:7000", "gn:100", "viaf:1", f"whg:{self.F.pk}"], user=self.user)
        self.assertEqual(set(r), {"tgn:7000", "gn:100", "viaf:1", f"whg:{self.F.pk}"})
        self.assertEqual(r[f"whg:{self.F.pk}"]["records"][0]["place_id"], self.F.id)

    def test_unresolved_whg_target_is_unavailable_and_reveals_nothing(self):
        """A private, an embargoed and a non-existent whg: target get the IDENTICAL
        resolution, distinct from not_held; a public one still resolves."""
        from places.models import Place
        missing = (Place.objects.order_by("-id").values_list("id", flat=True).first() or 0) + 1000
        ids = [f"whg:{self.P.pk}", f"whg:{self.E.pk}", f"whg:{missing}", f"whg:{self.F.pk}", "viaf:1"]
        with patch("api.crc_client.crc_fetch_places", side_effect=fake_fetch):
            r = resolve_targets(ids, user=self.user)
        self.assertEqual(r[f"whg:{self.F.pk}"]["status"], "resolved")                 # presence
        self.assertEqual(r[f"whg:{self.F.pk}"]["records"][0]["place_id"], self.F.id)
        private, embargoed, absent = r[f"whg:{self.P.pk}"], r[f"whg:{self.E.pk}"], r[f"whg:{missing}"]
        self.assertEqual(private, {"status": "unavailable", "via": None, "records": [], "has_geometry": False})
        self.assertEqual(private, absent)          # the answer cannot tell private from non-existent
        self.assertEqual(embargoed, absent)
        self.assertEqual(r["viaf:1"]["status"], "not_held")                             # the other meaning
        self.assertNotIn("Secret", repr(r))
        self.assertNotIn("Embargoed", repr(r))

    def test_place_matches_still_one_step(self):
        m = self.A.matches
        ids = {p.id for p in m}
        self.assertEqual(ids, {self.A.id, self.F.id, self.G.id, self.H.id})


class HubTruncationTests(TestCase):
    """Review 2026-10-21: tiers 2 and 3 shared one LIMIT with no ORDER BY, so the
    survivors were arbitrary and co-citers could crowd every inbound attester out;
    and the group did not know it was partial. The caps are patched small (5 and 3)
    so the hub is cheap; the co-citers are created FIRST, so a shared, unordered
    LIMIT returns them before any inbound row."""

    @classmethod
    def setUpTestData(cls):
        from datasets.models import Dataset
        from places.models import Place, PlaceGeom, PlaceLink

        user = get_user_model().objects.create_user(username="hub_owner", email="hub@example.org", password="pw",
                                                    given_name="H", surname="Ub")
        with patch("datasets.signals.doi"):
            ds = Dataset.objects.create(owner=user, label="hub325", title="Hub ds", description="D", creator="X",
                                        public=True, ds_status="indexed", uri_base="https://example.org/p/")

        def place(title, src):
            p = Place.objects.create(title=title, src_id=src, dataset=ds, ccodes=[])
            PlaceGeom.objects.create(place=p, geom=Point(1.0, 2.0, srid=4326),
                                     jsonb={"type": "Point", "coordinates": [1.0, 2.0]})
            return p
        cls.Hub = place("Hub", "H0")
        PlaceLink.objects.create(place=cls.Hub, jsonb={"type": "closeMatch", "identifier": "gn:4242"})
        cls.Small = place("Small", "S0")
        PlaceLink.objects.create(place=cls.Small, jsonb={"type": "closeMatch", "identifier": "gn:4243"})
        cls.cociters = []
        for i in range(12):                      # > INBOUND + COCITED + 1 (5 + 3 + 1)
            p = place(f"Cociter {i}", f"C{i}")
            PlaceLink.objects.create(place=p, jsonb={"type": "closeMatch", "identifier": "gn:4242"})
            cls.cociters.append(p.pk)
        cls.inbound = []
        for i in range(8):                       # > INBOUND_LIMIT (5)
            p = place(f"Attester {i}", f"A{i}")
            PlaceLink.objects.create(place=p, jsonb={"type": "exactMatch", "identifier": f"whg:{cls.Hub.pk}"})
            cls.inbound.append(p.pk)
        p = place("Small attester", "SA")
        PlaceLink.objects.create(place=p, jsonb={"type": "exactMatch", "identifier": f"whg:{cls.Small.pk}"})
        cls.small_attester = p.pk

    def setUp(self):
        for name, val in (("INBOUND_LIMIT", 5), ("COCITED_LIMIT", 3)):
            p = patch(f"places.concordance.{name}", val)
            p.start()
            self.addCleanup(p.stop)

    def conc(self, place):
        return build_concordance(place, user=None, use_gateway=False)

    def test_inbound_capped_deterministically_and_never_crowded_out(self):
        runs = [self.conc(self.Hub) for _ in range(2)]
        for c in runs:
            self.assertEqual([e["record"]["place_id"] for e in c["inbound"]], sorted(self.inbound)[:5])
            self.assertEqual([e["record"]["place_id"] for e in c["cocited"]], sorted(self.cociters)[:3])
            self.assertEqual(c["truncated"], {"inbound": True, "cocited": True})
        self.assertEqual(runs[0]["inbound"], runs[1]["inbound"])
        # Control: a record under both caps is complete and says so.
        small = self.conc(self.Small)
        self.assertEqual([e["record"]["place_id"] for e in small["inbound"]], [self.small_attester])
        self.assertEqual(small["truncated"], {"inbound": False, "cocited": False})

    def test_partial_group_is_said_in_gloss_and_environment_payload(self):
        from places.gloss import build_gloss
        from places.record_group import build_record_group
        hub = build_record_group(self.Hub, user=None)
        small = build_record_group(self.Small, user=None)
        self.assertEqual((hub["truncated"], small["truncated"]), (True, False))
        self.assertEqual(hub["n_equivalents"], 5)
        partial = "describes part of the group"
        g = build_gloss(hub["members"], truncated=hub["truncated"])
        self.assertTrue(any(partial in u and "first 5" in u for u in g["unknown"]), g["unknown"])
        g2 = build_gloss(small["members"], truncated=small["truncated"])
        self.assertFalse(any(partial in u for u in g2["unknown"]), g2["unknown"])
        self.assertTrue(g2["sentences"])                                     # presence beside the absence
        # The environment endpoint carries the flag for the page to say so (enrichment.js).
        from django.test import override_settings
        from api.tests_edop_client import BASIN, FakeTransport
        from places.tests_enrichment_views import LOCMEM
        beta = get_user_model().objects.create_user(username="hub_beta", email="hub_beta@example.org",
                                                    password="pw", given_name="H", surname="B", role="beta_tester")
        self.client.force_login(beta)
        with override_settings(CACHES=LOCMEM, EDOP_ENABLED=True), \
                patch("api.edop_client._requests_transport", side_effect=FakeTransport(BASIN)):
            env = {p.pk: self.client.get(f"/atlas/place/{p.pk}/environment/").json() for p in (self.Hub, self.Small)}
        self.assertEqual((env[self.Hub.pk]["truncated"], env[self.Small.pk]["truncated"]), (True, False))
        self.assertEqual(env[self.Small.pk]["status"], "ok")
