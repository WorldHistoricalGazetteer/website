"""place#350: the dataset owner adopts a matched place's location, or places one by hand.

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test datasets.tests_location_adoption --settings=whg.settings_localtest

Decisions A-G are in the issue body. Every refusal here is paired with the same request
succeeding once the one thing that made it refuse is put right, so a refusal can never pass
because the request was broken for some other reason. The gateway is the ``GatewayFake`` of
``tests_candidates``; nothing reaches the CRC.
"""
import json

from django.contrib.auth import get_user_model
from django.contrib.gis.geos import Point
from django.test import Client

from api.models import ContributorAttestation, GazetteerRegistryEntry
from datasets import candidates
from datasets.models import Dataset, DatasetUser
from datasets.models_matching import MatchCandidate, RecordReviewState
from datasets.tests_candidates import GN_TOWN, hit, response
from datasets.tests_feed import FeedTestBase
from licensing.models import License
from places.models import PlaceGeom

HODD = (0.01, 51.76)          # where the suggestions for the record with no location lie
HODD_OSM = (0.0102, 51.7601)  # the OpenStreetMap one, a few metres off


def lic(spdx):
    return License.objects.get(spdx_id=spdx)


class AdoptionTestBase(FeedTestBase):
    def setUp(self):
        super().setUp()
        # The registry's terms for the sources the suggestions come from.
        for ns, spdx in (("gn", "CC-BY-4.0"), ("wd", "CC0-1.0"), ("osm", "ODbL-1.0"),
                         ("ohm", "CC0-1.0"), ("tm", "CC-BY-NC-4.0")):
            GazetteerRegistryEntry.objects.filter(id=ns).update(license=lic(spdx), web_item=None)
        GazetteerRegistryEntry.objects.filter(id="gn").update(web_item="https://www.geonames.org/<id>",
                                                              citation_text="GeoNames, geonames.org")
        GazetteerRegistryEntry.objects.update_or_create(
            id="dgsd", defaults={"name": "DGSD", "namespace": "dgsd", "entry_class": "authority",
                                 "status": "published", "license": lic("CC-BY-ND-4.0")})
        # The dataset's own licence (place#158): one is needed before anything can be adopted.
        Dataset.objects.filter(pk=self.ds.pk).update(license=lic("CC-BY-4.0"),
                                                     uri_base="https://whgazetteer.org/api/db/?id=")
        self.ds.refresh_from_db()
        # A record with NO location, and suggestions from a safe source, share-alike, NC, ND
        # and a safe boundary source.
        self.hodd = self.place("Hoddesdon", "f3")
        self.fake.by_query["Hoddesdon"] = response(
            hit("gn:8", "Hoddesdon", HODD, types=[GN_TOWN]),
            hit("wd:Q8", "Hoddesdon", (0.0101, 51.7602), types=[GN_TOWN]),
            hit("osm:n8", "Hoddesdon", HODD_OSM, types=[GN_TOWN]),
            hit("tm:8", "Hoddesdon", (0.0103, 51.7603), types=[GN_TOWN]),
            hit("dgsd:8", "Hoddesdon", (0.0104, 51.7604), types=[GN_TOWN]),
            hit("ohm:r8", "Hoddesdon", (0.0105, 51.7605), types=[GN_TOWN]))
        candidates.record_candidates(self.hodd, self.gen, self.owner)
        self.loc_url = self.base + "location/"

    # helpers -------------------------------------------------------------------------------
    def loc(self, body, client=None):
        return (client or self.c).post(self.loc_url, json.dumps(body), content_type="application/json")

    def link(self, place, cid, client=None):
        c = self.cand(place, cid)
        r = self.post({"place": place.pk, "action": "link", "candidates": [c.pk]}, client)
        self.assertEqual(r.status_code, 200, r.content)
        return c

    def adopt(self, cid, place=None, client=None, link=True, **extra):
        place = place or self.hodd
        c = self.link(place, cid) if link else self.cand(place, cid)
        return self.loc({"action": "adopt", "place": place.pk, "candidate": c.pk, **extra}, client)

    def geoms(self, place=None):
        return list(PlaceGeom.objects.filter(place=place or self.hodd))

    def snapshot_own_geoms(self):
        """Every geometry the contributor loaded, as stored, to show none is touched."""
        return sorted((g.pk, g.place_id, json.dumps(g.jsonb, sort_keys=True), g.geom.wkt, g.task_id,
                       g.geom_src_id) for g in PlaceGeom.objects.filter(place__dataset=self.ds.label))

    def record_json(self, place, client=None, tier=None):
        st = RecordReviewState.objects.get(place=place)
        tab = tier or (st.state == "later" and "later") or st.tier
        if st.state not in ("unreviewed", "later"):
            # a decided record has left its tab; read it back through the record=... route
            st_state = st.state
            RecordReviewState.objects.filter(pk=st.pk).update(state="unreviewed")
            try:
                d = (client or self.c).get(self.base + f"candidates/?tier={st.tier}").json()
            finally:
                RecordReviewState.objects.filter(pk=st.pk).update(state=st_state)
        else:
            d = (client or self.c).get(self.base + f"candidates/?tier={tab}").json()
        return next(r for r in d["records"] if r["place"] == place.pk)

    def stranger(self, role, name):
        u = get_user_model().objects.create_user(username=name, email=f"{name}@example.org", password="pw",
                                                 given_name="S", surname=name, role="beta_tester")
        DatasetUser.objects.create(dataset_id=self.ds, user_id=u, role=role)
        c = Client()
        c.force_login(u)
        return u, c


class AdoptTests(AdoptionTestBase):
    def test_owner_adopts_a_safe_point_location_as_one_separate_row(self):
        before = self.snapshot_own_geoms()
        r = self.adopt("gn:8")
        self.assertEqual(r.status_code, 201, r.content)
        rows = self.geoms()
        self.assertEqual(len(rows), 1)
        g = rows[0]
        c = self.cand(self.hodd, "gn:8")
        self.assertEqual(g.geom_src_id, "gn")                         # the source gazetteer
        self.assertEqual(g.task_id, f"gen:{c.generation_id}")         # the candidate's generation
        self.assertEqual(g.reviewer_id, self.owner.pk)                # the owner
        self.assertEqual(g.jsonb["type"], "Point")
        self.assertEqual(g.jsonb["coordinates"], list(HODD))
        self.assertEqual((g.geom.x, g.geom.y), HODD)
        prov = g.jsonb["provenance"]
        self.assertEqual((prov["kind"], prov["match"], prov["source"], prov["terms"]),
                         ("adopted", "gn:8", "gn", "CC-BY-4.0"))
        self.assertEqual(prov["candidate"], c.pk)
        att = ContributorAttestation.objects.get(candidate=c)
        self.assertEqual(prov["attestation"], att.pk)
        self.assertEqual(g.jsonb["citation"], {"id": "gn:8", "label": "GeoNames"})
        # The contributor's own geometries (other records) are exactly as they were.
        after = [x for x in self.snapshot_own_geoms() if x[0] != g.pk]
        self.assertEqual(after, before)
        self.assertTrue(before)                                       # there WERE some to compare

    def test_the_point_is_the_stored_suggestions_never_one_sent_by_the_browser(self):
        r = self.adopt("gn:8", lon=10.0, lat=10.0, coordinates=[10.0, 10.0])
        self.assertEqual(r.status_code, 201, r.content)
        self.assertEqual(self.geoms()[0].jsonb["coordinates"], list(HODD))

    def test_refused_when_the_record_has_its_own_geometry(self):
        r = self.adopt("gn:4", place=self.ware)                     # Ware has a location
        self.assertEqual(r.status_code, 409, r.content)
        self.assertEqual(r.json()["reason"], "has_location")
        self.assertEqual(len(self.geoms(self.ware)), 1)
        self.assertEqual(self.adopt("gn:8").status_code, 201)       # the control: a record without one

    def test_refused_when_the_record_already_has_an_adopted_location(self):
        self.assertEqual(self.adopt("gn:8").status_code, 201)
        r = self.adopt("wd:Q8")
        self.assertEqual(r.status_code, 409, r.content)
        self.assertEqual(r.json()["reason"], "has_location")
        self.assertEqual(len(self.geoms()), 1)

    def test_refused_for_share_alike_non_commercial_and_no_derivatives_sources(self):
        for cid in ("osm:n8", "tm:8", "dgsd:8"):
            r = self.adopt(cid)
            self.assertEqual(r.status_code, 409, (cid, r.content))
            self.assertEqual(r.json()["reason"], "terms", cid)
        self.assertEqual(self.geoms(), [])
        self.assertEqual(self.adopt("gn:8").status_code, 201)        # a safe source on the same record

    def test_registry_terms_are_checked_not_only_the_namespace(self):
        """gn is in the safe group, but if its registered terms became share-alike, NC or ND the
        location would no longer be adoptable."""
        self.link(self.hodd, "gn:8")
        c = self.cand(self.hodd, "gn:8")
        for spdx in ("CC-BY-SA-4.0", "CC-BY-NC-4.0", "CC-BY-ND-4.0"):
            GazetteerRegistryEntry.objects.filter(id="gn").update(license=lic(spdx))
            r = self.loc({"action": "adopt", "place": self.hodd.pk, "candidate": c.pk})
            self.assertEqual(r.status_code, 409, (spdx, r.content))
            self.assertEqual(r.json()["reason"], "terms")
        GazetteerRegistryEntry.objects.filter(id="gn").update(license=None)
        r = self.loc({"action": "adopt", "place": self.hodd.pk, "candidate": c.pk})
        self.assertEqual((r.status_code, r.json()["reason"]), (409, "no_terms"))
        GazetteerRegistryEntry.objects.filter(id="gn").update(license=lic("CC-BY-4.0"))
        r = self.loc({"action": "adopt", "place": self.hodd.pk, "candidate": c.pk})
        self.assertEqual(r.status_code, 201, r.content)

    def test_points_only_a_boundary_source_is_a_reference(self):
        r = self.adopt("ohm:r8")
        self.assertEqual(r.status_code, 409, r.content)
        self.assertEqual(r.json()["reason"], "not_point_source")
        self.assertEqual(self.adopt("wd:Q8").status_code, 201)

    def test_refused_when_the_location_is_withheld(self):
        """A contributed suggestion from a dataset the owner may not see: no coordinates and no
        offer of any kind (constraint 5)."""
        other_user = get_user_model().objects.create_user(username="lo_o", email="lo@example.org",
                                                          password="pw", given_name="O", surname="U")
        Dataset.objects.filter(pk=self.hidden.pk).update(owner=other_user)
        self.place("Hoddesdon", "x8", HODD, ds=self.hidden)
        c = MatchCandidate.objects.create(
            dataset=self.ds, place=self.hodd, subject_id=f"whg:{self.ds.pk}:f3",
            candidate_id=f"whg:{self.hidden.pk}:x8", tier="ambiguous", rank=9, generation=self.gen,
            status="accepted", card={"title": "Hoddesdon", "namespace": "whg", "repr_point": list(HODD)})
        r = self.loc({"action": "adopt", "place": self.hodd.pk, "candidate": c.pk})
        self.assertEqual(r.status_code, 409, r.content)
        self.assertEqual(r.json()["reason"], "withheld")
        self.assertEqual(self.geoms(), [])
        self.assertEqual(self.adopt("gn:8").status_code, 201)

    def test_refused_for_anyone_but_the_dataset_owner(self):
        self.link(self.hodd, "gn:8")
        c = self.cand(self.hodd, "gn:8")
        body = {"action": "adopt", "place": self.hodd.pk, "candidate": c.pk}
        for role in ("member", "owner"):          # a collaborator, and a co-owner: not THE owner
            _u, cl = self.stranger(role, f"lo_{role}")
            r = self.loc(body, cl)
            self.assertEqual(r.status_code, 403, (role, r.content))
            self.assertEqual(r.json()["reason"], "owner_only")
        self.assertEqual(self.geoms(), [])
        self.assertEqual(self.loc(body).status_code, 201)

    def test_refused_until_the_suggestion_is_marked_same_place(self):
        r = self.adopt("gn:8", link=False)
        self.assertEqual(r.status_code, 409, r.content)
        self.assertEqual(r.json()["reason"], "not_matched")
        self.assertEqual(self.geoms(), [])
        self.assertEqual(self.adopt("gn:8").status_code, 201)

    def test_refused_for_a_dataset_declared_sensitive(self):
        for answer in ("yes", "unsure"):
            Dataset.objects.filter(pk=self.ds.pk).update(sensitivity_restricted=answer)
            r = self.adopt("gn:8")
            self.assertEqual(r.status_code, 409, (answer, r.content))
            self.assertEqual(r.json()["reason"], "sensitive")
            self.post({"place": self.hodd.pk, "action": "reopen", "adopted_location": "keep"})
        self.assertEqual(self.geoms(), [])
        Dataset.objects.filter(pk=self.ds.pk).update(sensitivity_restricted="no")
        self.assertEqual(self.adopt("gn:8").status_code, 201)

    def test_refused_while_the_dataset_has_no_licence(self):
        Dataset.objects.filter(pk=self.ds.pk).update(license=None)
        r = self.adopt("gn:8")
        self.assertEqual((r.status_code, r.json()["reason"]), (409, "dataset_licence"))
        Dataset.objects.filter(pk=self.ds.pk).update(license=lic("CC0-1.0"))
        c = self.cand(self.hodd, "gn:8")
        r = self.loc({"action": "adopt", "place": self.hodd.pk, "candidate": c.pk})
        self.assertEqual(r.status_code, 201, r.content)

    def test_same_place_alone_writes_no_geometry(self):
        before = PlaceGeom.objects.count()
        self.link(self.hodd, "gn:8")
        self.assertEqual(PlaceGeom.objects.count(), before)
        self.assertEqual(self.geoms(), [])
        r = self.post({"place": self.hodd.pk, "action": "decide",
                       "decisions": [{"candidate": self.cand(self.hodd, "wd:Q8").pk, "decision": "same"}]})
        self.assertEqual(r.status_code, 200, r.content)
        self.assertEqual(PlaceGeom.objects.count(), before)
        self.assertEqual(self.adopt("wd:Q8", link=False).status_code, 201)   # it is a second act
        self.assertEqual(PlaceGeom.objects.count(), before + 1)

    def test_adopting_does_not_change_the_match(self):
        c = self.link(self.hodd, "gn:8")
        att = ContributorAttestation.objects.get(candidate=c)
        st = RecordReviewState.objects.get(place=self.hodd)
        keep_att = (att.status, att.certainty, att.relation_type, json.dumps(att.evidence, sort_keys=True))
        c.refresh_from_db()
        keep_c = (c.status, c.tier, c.rank, json.dumps(c.signals, sort_keys=True))
        keep_st = (st.tier, st.state, st.has_geometry, st.generation_id)
        self.assertEqual(self.loc({"action": "adopt", "place": self.hodd.pk, "candidate": c.pk}).status_code, 201)
        att.refresh_from_db()
        c.refresh_from_db()
        st.refresh_from_db()
        self.assertEqual((att.status, att.certainty, att.relation_type,
                          json.dumps(att.evidence, sort_keys=True)), keep_att)
        self.assertEqual((c.status, c.tier, c.rank, json.dumps(c.signals, sort_keys=True)), keep_c)
        self.assertEqual((st.tier, st.state, st.has_geometry, st.generation_id), keep_st)


class AdoptUndoTests(AdoptionTestBase):
    def test_undo_removes_exactly_the_adopted_row(self):
        self.assertEqual(self.adopt("gn:8").status_code, 201)
        g = self.geoms()[0]
        before = self.snapshot_own_geoms()
        # Removing a geometry the contributor loaded is refused, whoever's record it is.
        own = PlaceGeom.objects.get(place=self.ware)
        r = self.loc({"action": "remove", "place": self.ware.pk, "geom": own.pk})
        self.assertEqual(r.status_code, 404, r.content)
        r = self.loc({"action": "remove", "place": self.hodd.pk, "geom": own.pk})
        self.assertEqual(r.status_code, 404, r.content)
        self.assertTrue(PlaceGeom.objects.filter(pk=own.pk).exists())
        r = self.loc({"action": "remove", "place": self.hodd.pk, "geom": g.pk})
        self.assertEqual(r.status_code, 200, r.content)
        self.assertFalse(PlaceGeom.objects.filter(pk=g.pk).exists())
        self.assertEqual(self.snapshot_own_geoms(), [x for x in before if x[0] != g.pk])

    def test_a_non_owner_cannot_remove_it(self):
        self.assertEqual(self.adopt("gn:8").status_code, 201)
        g = self.geoms()[0]
        _u, cl = self.stranger("member", "lo_rm")
        r = self.loc({"action": "remove", "place": self.hodd.pk, "geom": g.pk}, cl)
        self.assertEqual(r.status_code, 403, r.content)
        self.assertTrue(PlaceGeom.objects.filter(pk=g.pk).exists())

    def test_undo_of_the_match_asks_about_the_adopted_location_and_never_keeps_it_silently(self):
        self.assertEqual(self.adopt("gn:8").status_code, 201)
        g = self.geoms()[0]
        r = self.post({"place": self.hodd.pk, "action": "reopen"})
        self.assertEqual(r.status_code, 409, r.content)
        self.assertEqual(r.json()["reason"], "adopted_location")
        self.assertEqual(r.json()["adopted"][0]["geom"], g.pk)
        # Nothing was undone while the question was open.
        self.assertEqual(RecordReviewState.objects.get(place=self.hodd).state, "linked")
        self.assertTrue(PlaceGeom.objects.filter(pk=g.pk).exists())
        self.assertEqual(ContributorAttestation.objects.filter(status__in=("active", "pending")).count(), 1)
        r = self.post({"place": self.hodd.pk, "action": "reopen", "adopted_location": "remove"})
        self.assertEqual(r.status_code, 200, r.content)
        self.assertFalse(PlaceGeom.objects.filter(pk=g.pk).exists())
        self.assertEqual(RecordReviewState.objects.get(place=self.hodd).state, "unreviewed")
        self.assertEqual(ContributorAttestation.objects.filter(status__in=("active", "pending")).count(), 0)

    def test_undo_of_the_match_may_keep_the_location_when_asked_to(self):
        self.assertEqual(self.adopt("gn:8").status_code, 201)
        r = self.post({"place": self.hodd.pk, "action": "reopen", "adopted_location": "keep"})
        self.assertEqual(r.status_code, 200, r.content)
        self.assertEqual(len(self.geoms()), 1)

    def test_undo_of_a_match_with_no_adopted_location_asks_nothing(self):
        self.link(self.hodd, "gn:8")
        r = self.post({"place": self.hodd.pk, "action": "reopen"})
        self.assertEqual(r.status_code, 200, r.content)


class OfferJsonTests(AdoptionTestBase):
    def cands(self, rec):
        return {c["candidate_id"]: c for c in rec["candidates"]}

    def test_the_owner_is_offered_a_matched_safe_location_with_its_terms(self):
        self.link(self.hodd, "gn:8")
        rec = self.record_json(self.hodd)
        self.assertIsNone(rec["location_info"])
        self.assertTrue(rec["can_place"])
        gn = self.cands(rec)["gn:8"]["adopt"]
        self.assertTrue(gn["offer"], gn)
        self.assertEqual(gn["source_label"], "GeoNames")
        self.assertEqual(gn["terms"]["id"], "CC-BY-4.0")
        self.assertEqual(gn["link"], "https://www.geonames.org/8")
        self.assertEqual(gn["citation"], "GeoNames, geonames.org")
        # Not matched yet: adoptable source, but no offer until "Same place".
        wd = self.cands(rec)["wd:Q8"]["adopt"]
        self.assertTrue(wd["adoptable"])
        self.assertFalse(wd["offer"])
        self.assertEqual(wd["reason"], "not_matched")

    def test_reviewers_see_the_location_as_a_reference_only(self):
        self.link(self.hodd, "gn:8")
        _u, cl = self.stranger("member", "lo_ref")
        rec = self.record_json(self.hodd, client=cl)
        gn = self.cands(rec)["gn:8"]["adopt"]
        self.assertFalse(gn["offer"])
        self.assertEqual(gn["reason"], "owner_only")
        self.assertEqual(gn["link"], "https://www.geonames.org/8")    # the reference is still there
        self.assertFalse(rec["can_place"])
        self.assertTrue(self.cands(self.record_json(self.hodd))["gn:8"]["adopt"]["offer"])  # owner: yes

    def test_restricted_marker_is_drawn_as_a_hint_and_never_offered(self):
        self.link(self.hodd, "osm:n8")
        rec = self.record_json(self.hodd)
        osm = self.cands(rec)["osm:n8"]
        self.assertEqual(osm["location"], list(HODD_OSM))              # still drawn on the map
        self.assertFalse(osm["adopt"]["adoptable"])
        self.assertFalse(osm["adopt"]["offer"])
        self.assertEqual(osm["adopt"]["reason"], "terms")
        self.assertEqual(osm["adopt"]["hint"], "hint only: location not adoptable (terms)")
        self.assertTrue(self.cands(rec)["gn:8"]["adopt"]["adoptable"])  # the control

    def test_withheld_candidates_are_not_drawn_and_offer_nothing(self):
        other_user = get_user_model().objects.create_user(username="lo_w", email="lw@example.org",
                                                          password="pw", given_name="O", surname="U")
        Dataset.objects.filter(pk=self.hidden.pk).update(owner=other_user)
        Dataset.objects.filter(pk=self.other.pk).update(owner=other_user)
        self.place("Hoddesdon", "x8", HODD, ds=self.hidden)
        self.place("Hoddesdon", "o8", HODD, ds=self.other)
        for n, ds in ((10, self.hidden), (11, self.other)):
            MatchCandidate.objects.create(
                dataset=self.ds, place=self.hodd, subject_id=f"whg:{self.ds.pk}:f3",
                candidate_id=f"whg:{ds.pk}:{'x8' if ds == self.hidden else 'o8'}", tier="weak", rank=n,
                generation=self.gen, card={"title": "Hoddesdon", "namespace": "whg", "repr_point": list(HODD)})
        rec = self.record_json(self.hodd)
        ids = self.cands(rec)
        self.assertIn(f"whg:{self.other.pk}:o8", ids)                   # a visible one IS drawn...
        self.assertEqual(ids[f"whg:{self.other.pk}:o8"]["location"], list(HODD))
        self.assertNotIn(f"whg:{self.hidden.pk}:x8", ids)                # ...a withheld one is not there at all

    def test_a_record_with_its_own_location_is_offered_nothing(self):
        self.link(self.ware, "gn:4")
        rec = self.record_json(self.ware)
        self.assertEqual(rec["location_info"]["kind"], "own")
        self.assertFalse(rec["can_place"])
        self.assertEqual(self.cands(rec)["gn:4"]["adopt"]["reason"], "has_location")
        self.assertFalse(self.cands(rec)["gn:4"]["adopt"]["offer"])

    def test_the_feed_marks_a_record_that_now_has_an_adopted_location(self):
        self.assertEqual(self.adopt("gn:8").status_code, 201)
        rec = self.record_json(self.hodd)
        info = rec["location_info"]
        self.assertEqual((info["kind"], info["source_label"], info["match"]), ("adopted", "GeoNames", "gn:8"))
        self.assertEqual(info["by"], "C Owner")
        self.assertEqual(rec["location"], list(HODD))
        self.assertFalse(rec["can_place"])


class PlaceByHandTests(AdoptionTestBase):
    def place_at(self, lon, lat, zoom, place=None, client=None, **extra):
        return self.loc({"action": "place", "place": (place or self.hodd).pk, "lon": lon, "lat": lat,
                         "zoom": zoom, **extra}, client)

    def test_owner_places_a_location_that_is_their_own_data(self):
        r = self.place_at(0.0123456789, 51.7654321, 17)
        self.assertEqual(r.status_code, 201, r.content)
        (g,) = self.geoms()
        self.assertIsNone(g.geom_src_id)                       # never a source
        self.assertEqual(g.reviewer_id, self.owner.pk)
        self.assertEqual(g.jsonb["type"], "Point")
        self.assertEqual(g.jsonb["coordinates"], [0.01235, 51.76543])
        self.assertEqual(g.jsonb["certainty"], "less-certain")
        prov = g.jsonb["provenance"]
        self.assertEqual((prov["kind"], prov["method"], prov["approximate"]),
                         ("placed", "placed on the map", True))
        self.assertNotIn("citation", g.jsonb)
        # Nothing of any suggestion is recorded with it: no id, no namespace.
        blob = json.dumps(g.jsonb)
        for c in MatchCandidate.objects.filter(place=self.hodd):
            self.assertNotIn(c.candidate_id, blob)
        for ns in ("osm", "gn", "wd", "tm", "dgsd", "ohm"):
            self.assertNotIn(f'"{ns}', blob)

    def test_no_snap_a_click_one_metre_from_a_marker_is_stored_as_clicked(self):
        # About a metre east and north of the OpenStreetMap suggestion (share-alike).
        lon, lat = HODD_OSM[0] + 0.0000147, HODD_OSM[1] + 0.0000090
        r = self.place_at(lon, lat, 17)
        self.assertEqual(r.status_code, 201, r.content)
        stored = self.geoms()[0].jsonb["coordinates"]
        self.assertEqual(stored, [round(lon, 5), round(lat, 5)])
        self.assertNotEqual(stored, list(HODD_OSM))
        self.assertIsNone(self.geoms()[0].geom_src_id)

    def test_a_click_exactly_on_a_restricted_marker_is_allowed_and_not_attributed_to_it(self):
        """Decision G: on top of a restricted marker is fine; it is still the owner's own point."""
        r = self.place_at(*HODD_OSM, 17)
        self.assertEqual(r.status_code, 201, r.content)
        g = self.geoms()[0]
        self.assertIsNone(g.geom_src_id)
        self.assertNotIn("osm", json.dumps(g.jsonb))

    def test_stored_precision_follows_the_zoom(self):
        lon, lat = 0.0123456789, 51.7654321
        for zoom, decimals in ((0, 0), (3, 1), (6, 2), (9, 3), (10, 3), (13, 4), (14, 4), (17, 5)):
            PlaceGeom.objects.filter(place=self.hodd).delete()
            r = self.place_at(lon, lat, zoom)
            self.assertEqual(r.status_code, 201, (zoom, r.content))
            g = self.geoms()[0]
            self.assertEqual(g.jsonb["coordinates"], [round(lon, decimals), round(lat, decimals)], zoom)
            self.assertEqual(g.jsonb["provenance"]["decimals"], decimals, zoom)
            self.assertEqual(g.jsonb["provenance"]["zoom"], zoom)
            self.assertEqual((g.geom.x, g.geom.y), (round(lon, decimals), round(lat, decimals)))

    def test_there_is_no_place_at_this_marker_request(self):
        c = self.cand(self.hodd, "osm:n8")
        for extra in ({"candidate": c.pk}, {"marker": c.pk}, {"source": "osm"}, {"snap": True}):
            r = self.place_at(0.02, 51.77, 12, **extra)
            self.assertEqual(r.status_code, 400, (extra, r.content))
        self.assertEqual(self.geoms(), [])
        self.assertEqual(self.place_at(0.02, 51.77, 12).status_code, 201)

    def test_bad_input_is_refused(self):
        for body in ({"lon": 200, "lat": 51, "zoom": 10}, {"lon": 0, "lat": 95, "zoom": 10},
                     {"lon": "x", "lat": 51, "zoom": 10}, {"lon": 0, "lat": 51},
                     {"lon": 0, "lat": 51, "zoom": "near"}, {"lon": float("nan"), "lat": 51, "zoom": 3}):
            r = self.client_post_raw(body)
            self.assertEqual(r.status_code, 400, (body, r.content))
        self.assertEqual(self.geoms(), [])
        self.assertEqual(self.place_at(0, 51, 10).status_code, 201)

    def client_post_raw(self, body):
        return self.c.post(self.loc_url, json.dumps({"action": "place", "place": self.hodd.pk, **body}),
                           content_type="application/json")

    def test_owner_only(self):
        for role in ("member", "owner"):
            _u, cl = self.stranger(role, f"lp_{role}")
            r = self.place_at(0.02, 51.77, 12, client=cl)
            self.assertEqual(r.status_code, 403, (role, r.content))
        self.assertEqual(self.geoms(), [])
        self.assertEqual(self.place_at(0.02, 51.77, 12).status_code, 201)

    def test_refused_for_a_record_that_already_has_a_location(self):
        r = self.place_at(0.03, 51.8, 12, place=self.ware)
        self.assertEqual(r.status_code, 409, r.content)
        self.assertEqual(r.json()["reason"], "has_location")
        self.assertEqual(len(self.geoms(self.ware)), 1)
        self.assertEqual(self.place_at(0.02, 51.77, 12).status_code, 201)
        r = self.place_at(0.02, 51.77, 12)                      # and now this one has one too
        self.assertEqual((r.status_code, r.json()["reason"]), (409, "has_location"))

    def test_refused_for_a_dataset_declared_sensitive(self):
        Dataset.objects.filter(pk=self.ds.pk).update(sensitivity_indigenous="yes")
        r = self.place_at(0.02, 51.77, 12)
        self.assertEqual((r.status_code, r.json()["reason"]), (409, "sensitive"))
        Dataset.objects.filter(pk=self.ds.pk).update(sensitivity_indigenous="no")
        self.assertEqual(self.place_at(0.02, 51.77, 12).status_code, 201)

    def test_independent_of_the_match_decision(self):
        st0 = RecordReviewState.objects.get(place=self.hodd)
        self.assertEqual(st0.state, "unreviewed")
        n_att = ContributorAttestation.objects.count()
        self.assertEqual(self.place_at(0.02, 51.77, 12).status_code, 201)
        st = RecordReviewState.objects.get(place=self.hodd)
        self.assertEqual((st.state, st.tier), (st0.state, st0.tier))
        self.assertEqual(ContributorAttestation.objects.count(), n_att)
        self.assertFalse(MatchCandidate.objects.filter(place=self.hodd).exclude(status="suggested").exists())

    def test_undo_removes_exactly_the_placed_row(self):
        self.assertEqual(self.place_at(0.02, 51.77, 12).status_code, 201)
        g = self.geoms()[0]
        before = self.snapshot_own_geoms()
        r = self.loc({"action": "remove", "place": self.hodd.pk, "geom": g.pk})
        self.assertEqual(r.status_code, 200, r.content)
        self.assertEqual(self.geoms(), [])
        self.assertEqual(self.snapshot_own_geoms(), [x for x in before if x[0] != g.pk])

    def test_the_feed_marks_a_placed_location(self):
        self.assertEqual(self.place_at(0.02, 51.77, 12).status_code, 201)
        info = self.record_json(self.hodd)["location_info"]
        self.assertEqual(info["kind"], "placed")
        self.assertIsNone(info["source_label"])


class LaterGenerationTests(AdoptionTestBase):
    """Decision D: an adopted location is the record's location in later generations, except
    that agreeing with the very place it came from is not corroboration."""

    def test_agreement_with_the_source_of_an_adopted_location_is_not_counted(self):
        # The control: with a location of the record's own, both suggestions are measured.
        own = self.place("Hoddesdon", "f9", HODD)
        hits = (hit("gn:8", "Hoddesdon", HODD, types=[GN_TOWN]),
                hit("wd:Q8", "Hoddesdon", HODD, types=[GN_TOWN]))
        _t, scored, _r = candidates.assess(own, response(*hits), self.owner)
        by = {c["id"]: c for c in scored}
        self.assertIn("spatial", by["gn:8"]["signals"])
        self.assertIsNotNone(by["gn:8"]["km"])
        # The same point, adopted from gn:8.
        self.assertEqual(self.adopt("gn:8").status_code, 201)
        self.hodd.refresh_from_db()
        _t, scored, _r = candidates.assess(self.hodd, response(*hits), self.owner)
        by = {c["id"]: c for c in scored}
        self.assertNotIn("spatial", by["gn:8"]["signals"])       # not compared with itself
        self.assertIsNone(by["gn:8"]["km"])
        self.assertIn("spatial", by["wd:Q8"]["signals"])          # every other candidate: normally
        self.assertAlmostEqual(by["wd:Q8"]["km"], 0.0, places=3)

    def test_a_later_generation_counts_the_adopted_location_as_the_records(self):
        self.assertEqual(self.adopt("gn:8").status_code, 201)
        gen2 = self.generation()
        candidates.record_candidates(self.hodd, gen2, self.owner)
        st = RecordReviewState.objects.get(place=self.hodd)
        self.assertTrue(st.has_geometry)
        wd = MatchCandidate.objects.get(place=self.hodd, generation=gen2, candidate_id="wd:Q8")
        gn = MatchCandidate.objects.get(place=self.hodd, generation=gen2, candidate_id="gn:8")
        self.assertIsNotNone(wd.signals["km"])
        self.assertIsNone(gn.signals["km"])

    def test_a_placed_location_counts_with_no_guard(self):
        r = self.loc({"action": "place", "place": self.hodd.pk, "lon": HODD[0], "lat": HODD[1], "zoom": 17})
        self.assertEqual(r.status_code, 201, r.content)
        self.hodd.refresh_from_db()
        _t, scored, _r = candidates.assess(self.hodd, response(hit("gn:8", "Hoddesdon", HODD, types=[GN_TOWN])),
                                           self.owner)
        self.assertIn("spatial", scored[0]["signals"])


class ProvenanceTests(AdoptionTestBase):
    def test_record_page_labels_an_adopted_location_with_its_source_date_and_person(self):
        url = f"/atlas/place/{self.hodd.pk}/"
        page = self.c.get(url)
        self.assertEqual(page.status_code, 200)
        self.assertNotContains(page, "adopted")                            # the control
        self.assertEqual(self.adopt("gn:8").status_code, 201)
        page = self.c.get(url).content.decode()
        g = self.geoms()[0]
        self.assertIn("from GeoNames, adopted", page)
        self.assertIn(g.create_date.strftime("%-d %b %Y"), page)
        self.assertIn("by C Owner", page)
        self.assertNotIn("1 geometry", page)                               # never as the contributor's own

    def test_record_page_labels_a_placed_location(self):
        r = self.loc({"action": "place", "place": self.hodd.pk, "lon": 0.02, "lat": 51.77, "zoom": 12})
        self.assertEqual(r.status_code, 201)
        page = self.c.get(f"/atlas/place/{self.hodd.pk}/").content.decode()
        self.assertIn("placed on the map", page)
        self.assertIn("by C Owner", page)

    def download(self, filetype):
        import zlib
        from django.test import RequestFactory
        from api.download_file import stream_live
        req = RequestFactory().get("/")
        req.user = self.owner
        raw = b"".join(stream_live("dataset", Dataset.objects.get(pk=self.ds.pk), req, filetype=filetype))
        return zlib.decompressobj(31).decompress(raw).decode()

    def test_downloads_state_the_terms_of_an_adopted_location(self):
        self.assertEqual(self.adopt("gn:8").status_code, 201)
        lpf = json.loads(self.download("lpf"))
        feats = {f["properties"]["src_id"]: f for f in lpf["features"]}
        geo = feats["f3"]["geometry"]
        self.assertEqual(geo["coordinates"], list(HODD))
        self.assertEqual((geo["provenance"]["terms"], geo["provenance"]["source_label"]), ("CC-BY-4.0", "GeoNames"))
        self.assertIn("adopted from GeoNames under CC-BY-4.0", geo["provenance"]["statement"])
        self.assertEqual(geo["citation"]["id"], "gn:8")
        self.assertNotIn("provenance", feats["f1"]["geometry"])          # the contributor's own: as loaded
        tsv = self.download("tsv").splitlines()
        head = tsv[0].split("\t")
        rows = {r.split("\t")[0]: dict(zip(head, r.split("\t"))) for r in tsv[1:] if r}
        hodd = next(r for r in rows.values() if r["title"] == "Hoddesdon")
        ware = next(r for r in rows.values() if r["title"] == "Ware")
        self.assertEqual(hodd["geo_source"], "GeoNames (CC-BY-4.0), adopted")
        self.assertEqual(hodd["geo_id"], "gn:8")
        self.assertEqual(ware["geo_source"], "")                           # the control

    def test_downloads_say_a_placed_location_is_the_owners_own(self):
        r = self.loc({"action": "place", "place": self.hodd.pk, "lon": 0.02, "lat": 51.77, "zoom": 12})
        self.assertEqual(r.status_code, 201)
        tsv = self.download("tsv").splitlines()
        head = tsv[0].split("\t")
        hodd = next(dict(zip(head, r.split("\t"))) for r in tsv[1:] if "\tHoddesdon\t" in r)
        self.assertEqual(hodd["geo_source"], "placed on the map by the dataset owner (approximate)")

    def test_qc_report_counts_adopted_and_placed_locations_per_source(self):
        from datasets import qc
        self.assertEqual(self.adopt("gn:8").status_code, 201)
        r = self.loc({"action": "place", "place": self.brox.pk, "lon": 0.02, "lat": 51.77, "zoom": 12})
        self.assertEqual(r.status_code, 409)                               # Broxbourne has its own
        lone = self.place("Nazeing", "f4")
        RecordReviewState.objects.create(place=lone, dataset=self.ds, generation=self.gen, tier="none")
        r = self.loc({"action": "place", "place": lone.pk, "lon": 0.02, "lat": 51.77, "zoom": 12})
        self.assertEqual(r.status_code, 201, r.content)
        check = next(c for c in qc.qc_report(self.ds)["checks"] if c["key"] == "locations_added")
        self.assertEqual(check["count"], 2)
        self.assertEqual(check["by_source"], {"gn": 1})
        self.assertEqual(check["placed"], 1)
        self.assertIn("GeoNames 1", check["detail"])
