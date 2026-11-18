"""place#350 follow-on, constrained by place#343: what happens to an adopted (or hand-placed)
location when its dataset is made private, withdrawn, re-published or deleted, and when the Link
it was adopted through is withdrawn, rejected or superseded.

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test datasets.tests_location_lifecycle --settings=whg.settings_localtest

The rules are in ``datasets/location_lifecycle.py``. In short: an adopted location goes back
with its Link. A Link that goes ``pending`` (dataset private or withdrawn) takes the location
out of public view with the dataset, row kept, and re-publication brings both back; a Link that
stops standing (withdrawn, rejected, superseded) takes the row off the record into
``RetractedLocation``, and nothing brings it back on its own. A hand-placed location is the
owner's own data and follows the dataset only.

Every negative assertion is paired with the same surface showing the location first, so an
absence cannot pass because the surface never showed anything.
"""
import json
import zlib
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.db import connection
from django.test import Client
from django.test.utils import CaptureQueriesContext

from api import attestations
from api.models import ContributorAttestation, GazetteerRegistryEntry
from datasets.models import Dataset
from datasets.models_matching import RecordReviewState, RetractedLocation
from datasets.tests_location_adoption import HODD, AdoptionTestBase, lic
from places.models import Place, PlaceGeom

HODD_COORDS = [HODD[0], HODD[1]]


def _has_coords(data, coords=HODD_COORDS):
    """True when ``coords`` appear as a coordinate pair anywhere in a JSON value."""
    if isinstance(data, dict):
        return any(_has_coords(v, coords) for v in data.values())
    if isinstance(data, list):
        if len(data) == 2 and all(isinstance(x, (int, float)) for x in data):
            return [round(x, 6) for x in data] == coords
        return any(_has_coords(v, coords) for v in data)
    return False


class LifecycleTestBase(AdoptionTestBase):
    def setUp(self):
        super().setUp()
        for target in ("datasets.signals.doi",):
            p = patch(target)
            p.start()
            self.addCleanup(p.stop)
        # Index side effects are asserted where they matter; never reach ES or the gateway.
        self.refresh = patch("datasets.tasks.refresh_index_after_location_change.delay")
        self.refresh_mock = self.refresh.start()
        self.addCleanup(self.refresh.stop)
        self.lone = self.place("Nazeing", "f4")           # a second record with no location
        RecordReviewState.objects.create(place=self.lone, dataset=self.ds, generation=self.gen, tier="none")
        User = get_user_model()
        self.str_user = User.objects.create_user(username="lc_str", email="lcs@example.org", password="pw",
                                                 given_name="S", surname="Stranger", role="beta_tester")
        self.str_client = Client()
        self.str_client.force_login(self.str_user)

    # helpers -------------------------------------------------------------------------------
    def make_public(self):
        """Public without the v3 side effects of a save (set before any Link is made, so the
        Links are created active, as for a published dataset)."""
        Dataset.objects.filter(pk=self.ds.pk).update(public=True)
        self.ds.refresh_from_db()

    def adopted(self):
        r = self.adopt("gn:8")
        self.assertEqual(r.status_code, 201, r.content)
        return PlaceGeom.objects.get(place=self.hodd, task_id__startswith="gen:")

    def placed(self, place=None):
        r = self.loc({"action": "place", "place": (place or self.lone).pk, "lon": 0.02, "lat": 51.77,
                      "zoom": 12})
        self.assertEqual(r.status_code, 201, r.content)
        return PlaceGeom.objects.get(place=place or self.lone, task_id="feed-placed")

    def link_row(self):
        return ContributorAttestation.objects.get(candidate=self.cand(self.hodd, "gn:8"), user=self.owner)

    def own_geoms(self):
        return self.snapshot_own_geoms()

    def surfaces(self, client):
        """What each public surface shows ``client`` of Hoddesdon's adopted location:
        ``{surface: True | False | 404}``."""
        out = {}
        r = client.get(f"/atlas/place/{self.hodd.pk}/")
        out["record_page"] = 404 if r.status_code == 404 else "from GeoNames, adopted" in r.content.decode()
        r = client.get(f"/entity/place:{self.hodd.pk}/api")
        out["entity_api"] = 404 if r.status_code == 404 else _has_coords(json.loads(r.content))
        r = client.get(f"/api/place/{self.hodd.pk}/")
        out["api_place"] = 404 if r.status_code == 404 else _has_coords(json.loads(r.content))
        r = client.get(f"/api/geojson/?id={self.ds.pk}")
        out["api_geojson"] = 404 if r.status_code == 404 else _has_coords(json.loads(r.content))
        with patch("api.download_file.FileCache.is_cached", return_value=False), \
                patch("api.download_file.FileCache.acquire_build_lock", return_value=False):
            r = client.get(f"/entity/dataset:{self.ds.pk}/api?filetype=lpf")
            if r.status_code == 404:
                out["download"] = 404
            else:
                raw = b"".join(r.streaming_content)
                out["download"] = _has_coords(json.loads(zlib.decompressobj(31).decompress(raw)))
        return out


# ---------------------------------------------------------------------------------------------
# Private / withdrawn: the location goes out of view with the Link, row kept; re-publication
# brings both back.
# ---------------------------------------------------------------------------------------------

class PrivateAndWithdrawnTests(LifecycleTestBase):
    def test_made_private_hides_the_adopted_location_on_every_surface_and_keeps_the_row(self):
        self.make_public()
        g = self.adopted()
        self.assertEqual(self.link_row().status, "active")
        everywhere = {"record_page": True, "entity_api": True, "api_place": True,
                      "api_geojson": True, "download": True}
        self.assertEqual(self.surfaces(self.str_client), everywhere)          # the control
        ds = Dataset.objects.get(pk=self.ds.pk)
        ds.public = False
        ds.save()                                                             # the v3 route
        self.assertEqual(self.link_row().status, "pending")
        self.assertEqual(set(self.surfaces(self.str_client).values()), {404})
        self.assertEqual(self.surfaces(self.c), everywhere)                   # the owner still does
        self.assertTrue(PlaceGeom.objects.filter(pk=g.pk).exists())           # not deleted
        self.assertFalse(RetractedLocation.objects.exists())

    def test_anonymous_never_sees_an_adopted_location_of_a_private_dataset(self):
        g = self.adopted()
        anon = Client()
        self.assertEqual(anon.get(f"/api/place/{self.hodd.pk}/").status_code, 404)
        r = anon.get(f"/entity/dataset:{self.ds.pk}/api?filetype=lpf")
        self.assertIn(r.status_code, (401, 403, 404))
        self.assertNotEqual(anon.get(f"/atlas/place/{self.hodd.pk}/").status_code, 200)
        # the control: the owner, same requests
        self.assertTrue(_has_coords(json.loads(self.c.get(f"/api/place/{self.hodd.pk}/").content)))
        self.assertTrue(PlaceGeom.objects.filter(pk=g.pk).exists())

    def test_lifecycle_withdraw_then_publish_hides_then_restores_exactly(self):
        from datasets import lifecycle
        self.make_public()
        Dataset.objects.filter(pk=self.ds.pk).update(lifecycle_state="published")
        g = self.adopted()
        placed = self.placed()
        before = PlaceGeom.objects.filter(pk__in=(g.pk, placed.pk)).order_by("pk")
        before = [(x.pk, json.dumps(x.jsonb, sort_keys=True)) for x in before]
        lifecycle.withdraw(self.ds.pk, self.owner)
        self.assertEqual(self.link_row().status, "pending")
        self.assertEqual(self.surfaces(self.str_client)["api_place"], 404)
        Dataset.objects.filter(pk=self.ds.pk).update(lifecycle_state="accepted")
        lifecycle.publish(self.ds.pk, self.owner)
        self.assertEqual(self.link_row().status, "active")
        after = [(x.pk, json.dumps(x.jsonb, sort_keys=True))
                 for x in PlaceGeom.objects.filter(pk__in=(g.pk, placed.pk)).order_by("pk")]
        self.assertEqual(after, before)                                       # same rows, untouched
        self.assertEqual(self.surfaces(self.str_client)["api_place"], True)

    def test_hand_placed_location_follows_the_dataset(self):
        self.make_public()
        placed = self.placed()
        r = self.str_client.get(f"/api/place/{self.lone.pk}/")
        self.assertTrue(_has_coords(json.loads(r.content), [0.02, 51.77]))   # the control
        ds = Dataset.objects.get(pk=self.ds.pk)
        ds.public = False
        ds.save()
        self.assertEqual(self.str_client.get(f"/api/place/{self.lone.pk}/").status_code, 404)
        self.assertTrue(PlaceGeom.objects.filter(pk=placed.pk).exists())
        ds.public = True
        ds.save()
        r = self.str_client.get(f"/api/place/{self.lone.pk}/")
        self.assertTrue(_has_coords(json.loads(r.content), [0.02, 51.77]))

    def test_republication_retracts_a_location_whose_source_terms_no_longer_allow_it(self):
        self.make_public()
        g = self.adopted()
        ds = Dataset.objects.get(pk=self.ds.pk)
        ds.public = False
        ds.save()
        GazetteerRegistryEntry.objects.filter(id="gn").update(license=lic("CC-BY-NC-4.0"))
        ds = Dataset.objects.get(pk=self.ds.pk)
        ds.public = True
        ds.save()
        self.assertFalse(PlaceGeom.objects.filter(pk=g.pk).exists())
        arch = RetractedLocation.objects.get(geom_pk=g.pk)
        self.assertEqual(arch.reason, "source terms")
        self.assertEqual(self.surfaces(self.str_client)["api_place"], False)

    def test_republication_keeps_a_location_whose_terms_still_allow_it(self):
        self.make_public()
        g = self.adopted()
        for public in (False, True):
            ds = Dataset.objects.get(pk=self.ds.pk)
            ds.public = public
            ds.save()
        self.assertTrue(PlaceGeom.objects.filter(pk=g.pk).exists())
        self.assertFalse(RetractedLocation.objects.exists())


# ---------------------------------------------------------------------------------------------
# The Link stops standing: the adopted row goes back with it, exactly that row.
# ---------------------------------------------------------------------------------------------

class LinkRetractionTests(LifecycleTestBase):
    def setUp(self):
        super().setUp()
        # Another dataset of the same owner with its own adopted location, which must not move.
        self.make_public()
        self.g = self.adopted()
        self.placed_row = self.placed()
        self.other_place = Place.objects.create(title="Elsewhere", src_id="o1", dataset=self.other, ccodes=[])
        self.other_geom = PlaceGeom.objects.create(
            place=self.other_place, src_id="o1", task_id="gen:999999", reviewer=self.owner,
            jsonb={"type": "Point", "coordinates": [1.0, 1.0],
                   "provenance": {"kind": "adopted", "source": "gn", "match": "gn:8",
                                  "attestation": self.link_row().pk, "candidate": self.cand(self.hodd, "gn:8").pk}})
        self.own_before = [x for x in self.own_geoms() if x[0] not in (self.g.pk, self.placed_row.pk)]
        self.assertTrue(self.own_before)

    def assert_only_the_adopted_row_went(self, reason):
        self.assertFalse(PlaceGeom.objects.filter(pk=self.g.pk).exists())
        arch = RetractedLocation.objects.get(geom_pk=self.g.pk)
        self.assertEqual(arch.reason, reason)
        self.assertEqual(arch.place_id, self.hodd.pk)
        self.assertEqual(arch.dataset_id, self.ds.pk)
        self.assertEqual(arch.jsonb["coordinates"], HODD_COORDS)
        self.assertEqual((arch.geom_src, arch.task_id, arch.reviewer_id), ("gn", self.g.task_id, self.owner.pk))
        self.assertEqual(RetractedLocation.objects.count(), 1)
        self.assertTrue(PlaceGeom.objects.filter(pk=self.placed_row.pk).exists())   # hand-placed: stays
        self.assertTrue(PlaceGeom.objects.filter(pk=self.other_geom.pk).exists())   # other dataset: stays
        own_after = [x for x in self.own_geoms() if x[0] not in (self.g.pk, self.placed_row.pk)]
        self.assertEqual(own_after, self.own_before)                                # contributor's own: same

    def test_withdrawing_the_link_retracts_exactly_its_adopted_location(self):
        self.assertEqual(self.surfaces(self.str_client)["api_place"], True)           # the control
        attestations.withdraw(self.link_row(), by=self.owner)
        self.assert_only_the_adopted_row_went("link withdrawn")
        self.assertEqual(RetractedLocation.objects.get().retracted_by_id, self.owner.pk)
        self.assertEqual(self.surfaces(self.str_client),
                         {"record_page": False, "entity_api": False, "api_place": False,
                          "api_geojson": False, "download": False})
        self.assertEqual(self.surfaces(self.c)["api_place"], False)                   # owner too

    def test_withdrawing_through_the_links_api_retracts_it(self):
        r = self.c.delete(f"/api/links?id={self.link_row().pk}")
        self.assertEqual(r.status_code, 200, r.content)
        self.assert_only_the_adopted_row_went("link withdrawn")

    def test_editorial_rejection_and_supersession_retract_it(self):
        row = self.link_row()
        row.status = "rejected"
        row.save()
        self.assert_only_the_adopted_row_went("link rejected")

    def test_superseded_link_retracts_it(self):
        row = self.link_row()
        row.status = "superseded"
        row.save()
        self.assert_only_the_adopted_row_went("link superseded")

    def test_going_pending_or_active_retracts_nothing(self):
        row = self.link_row()
        for status in ("pending", "active"):
            row.status = status
            row.save()
        self.assertTrue(PlaceGeom.objects.filter(pk=self.g.pk).exists())
        self.assertFalse(RetractedLocation.objects.exists())

    def test_another_standing_link_on_the_same_suggestion_keeps_it(self):
        u, cl = self.stranger("member", "lc_col")
        c = self.cand(self.hodd, "gn:8")
        att = ContributorAttestation.objects.create(
            user=u, place_a=self.link_row().place_a, place_b=self.link_row().place_b,
            relation_type="sameAs", status="active", candidate=c, suggested_by="candidate",
            place_a_pk=self.link_row().place_a_pk, place_b_pk=self.link_row().place_b_pk)
        attestations.withdraw(self.link_row(), by=self.owner)
        self.assertTrue(PlaceGeom.objects.filter(pk=self.g.pk).exists())           # still matched
        attestations.withdraw(att, by=u)                                             # the last one
        self.assertFalse(PlaceGeom.objects.filter(pk=self.g.pk).exists())

    def test_idempotent(self):
        row = self.link_row()
        attestations.withdraw(row, by=self.owner)
        row.refresh_from_db()
        row.status = "rejected"
        row.save()                                                                  # again, by another route
        attestations.withdraw(row, by=self.owner)
        self.assertEqual(RetractedLocation.objects.count(), 1)
        self.assertEqual(RetractedLocation.objects.get().reason, "link withdrawn")

    def test_relinking_does_not_bring_the_location_back_on_its_own(self):
        attestations.withdraw(self.link_row(), by=self.owner)
        c = self.cand(self.hodd, "gn:8")
        r = self.post({"place": self.hodd.pk, "action": "reopen"})
        self.assertEqual(r.status_code, 200, r.content)                            # nothing to ask about now
        self.link(self.hodd, "gn:8")
        self.assertIn(self.link_row().status, ("active", "pending"))
        self.assertFalse(PlaceGeom.objects.filter(place=self.hodd).exists())        # "Same place" writes none
        # ...and the owner can adopt it again, as a separate act
        r = self.loc({"action": "adopt", "place": self.hodd.pk, "candidate": c.pk})
        self.assertEqual(r.status_code, 201, r.content)

    def test_restore_puts_the_row_back_exactly(self):
        from datasets import location_lifecycle
        attestations.withdraw(self.link_row(), by=self.owner)
        arch = RetractedLocation.objects.get()
        # Refused while the Link does not stand.
        with self.assertRaises(location_lifecycle.RestoreRefused):
            location_lifecycle.restore(arch, by=self.owner)
        self.link(self.hodd, "gn:8")                                                # the same Link again
        g = location_lifecycle.restore(arch, by=self.owner)
        self.assertEqual((g.jsonb, g.geom_src_id, g.task_id, g.reviewer_id, g.src_id),
                         (self.g.jsonb, "gn", self.g.task_id, self.owner.pk, self.g.src_id))
        self.assertEqual((g.geom.x, g.geom.y), HODD)
        arch.refresh_from_db()
        self.assertEqual(arch.restored_geom_pk, g.pk)
        self.assertIsNotNone(arch.restored_at)
        with self.assertRaises(location_lifecycle.RestoreRefused):                 # idempotent: once only
            location_lifecycle.restore(arch, by=self.owner)
        self.assertEqual(PlaceGeom.objects.filter(place=self.hodd).count(), 1)

    def test_restore_refused_when_the_record_has_a_location_again(self):
        from datasets import location_lifecycle
        attestations.withdraw(self.link_row(), by=self.owner)
        self.link(self.hodd, "gn:8")
        self.placed(self.hodd)
        with self.assertRaises(location_lifecycle.RestoreRefused):
            location_lifecycle.restore(RetractedLocation.objects.get(), by=self.owner)

    def test_retraction_on_a_public_dataset_refreshes_its_index_and_download(self):
        with patch("api.download_file.FileCache.delete_cache") as dc, \
                patch("utils.mapdata.delete_mapdata_cache.delay") as md:
            with self.captureOnCommitCallbacks(execute=True):
                attestations.withdraw(self.link_row(), by=self.owner)
        self.refresh_mock.assert_called_once_with(self.ds.pk, [self.hodd.pk])
        self.assertTrue(any(c.args[:2] == ("dataset", self.ds.pk) for c in dc.call_args_list))
        md.assert_any_call("datasets", self.ds.pk, refresh=True)

    def test_retraction_on_a_private_dataset_queues_no_indexing(self):
        Dataset.objects.filter(pk=self.ds.pk).update(public=False)
        with self.captureOnCommitCallbacks(execute=True):
            attestations.withdraw(self.link_row(), by=self.owner)
        self.assertFalse(PlaceGeom.objects.filter(pk=self.g.pk).exists())          # it did retract
        self.refresh_mock.assert_not_called()


class FeedUndoTests(LifecycleTestBase):
    def test_undo_keep_is_the_owners_choice_and_survives_later_checks(self):
        from datasets import location_lifecycle
        g = self.adopted()
        r = self.post({"place": self.hodd.pk, "action": "reopen", "adopted_location": "keep"})
        self.assertEqual(r.status_code, 200, r.content)
        self.assertEqual(self.link_row().status, "withdrawn")
        g.refresh_from_db()
        self.assertEqual(g.jsonb["provenance"]["kept"]["by"], self.owner.pk)
        self.assertEqual(g.jsonb["provenance"]["kept"]["link"], self.link_row().pk)
        self.assertEqual(location_lifecycle.audit(self.ds), [])
        self.assertFalse(RetractedLocation.objects.exists())

    def test_undo_remove_deletes_and_archives_nothing_twice(self):
        g = self.adopted()
        r = self.post({"place": self.hodd.pk, "action": "reopen", "adopted_location": "remove"})
        self.assertEqual(r.status_code, 200, r.content)
        self.assertFalse(PlaceGeom.objects.filter(pk=g.pk).exists())
        self.assertFalse(RetractedLocation.objects.exists())   # the owner removed it; not a retraction


# ---------------------------------------------------------------------------------------------
# Deletion
# ---------------------------------------------------------------------------------------------

class DeletionTests(LifecycleTestBase):
    def test_deleting_the_dataset_takes_its_adopted_and_placed_rows_and_nothing_else(self):
        self.make_public()
        g = self.adopted()
        placed = self.placed()
        other_place = Place.objects.create(title="Elsewhere", src_id="o1", dataset=self.other, ccodes=[])
        other = PlaceGeom.objects.create(place=other_place, src_id="o1", jsonb={"type": "Point", "coordinates": [1, 1]})
        with patch("datasets.views.dataset_file_delete"):
            r = self.c.post(f"/datasets/{self.ds.pk}/delete")
        self.assertIn(r.status_code, (302, 200), r.content[:300])
        self.assertFalse(Dataset.objects.filter(pk=self.ds.pk).exists())
        self.assertFalse(PlaceGeom.objects.filter(pk__in=(g.pk, placed.pk)).exists())
        self.assertTrue(PlaceGeom.objects.filter(pk=other.pk).exists())
        self.assertFalse(RetractedLocation.objects.exists())                     # nothing left behind
        self.assertEqual(set(ContributorAttestation.objects.values_list("status", flat=True)), {"withdrawn"})


# ---------------------------------------------------------------------------------------------
# QC report and audit
# ---------------------------------------------------------------------------------------------

class QCTests(LifecycleTestBase):
    def check(self):
        from datasets import qc
        return next(c for c in qc.qc_report(self.ds)["checks"] if c["key"] == "locations_added")

    def test_private_dataset_counts_and_says_who_sees_them(self):
        self.adopted()
        self.placed()
        c = self.check()
        self.assertEqual((c["status"], c["count"], c["by_source"], c["placed"]), ("ok", 2, {"gn": 1}, 1))
        self.assertEqual(c["visibility"], "private")
        self.assertIn("shown only to the dataset's owner and collaborators", c["detail"])
        self.make_public()
        c = self.check()
        self.assertEqual(c["visibility"], "public")
        self.assertNotIn("shown only to", c["detail"])

    def test_retracted_locations_are_counted_not_lost(self):
        self.adopted()
        attestations.withdraw(self.link_row(), by=self.owner)
        c = self.check()
        self.assertEqual((c["count"], c["retracted"]), (0, 1))
        self.assertIn("1 went back with its match", c["detail"])

    def test_flags_an_adopted_location_whose_link_no_longer_stands(self):
        g = self.adopted()
        self.assertEqual(self.check()["status"], "ok")                            # the control
        # A bulk update fires no signal: the state a missed hook would leave behind.
        ContributorAttestation.objects.filter(pk=self.link_row().pk).update(status="withdrawn")
        c = self.check()
        self.assertEqual(c["status"], "fail")
        self.assertEqual(c["should_not_be_visible"], {"link no longer stands": 1})
        self.assertEqual(c["examples"][0]["place_id"], self.hodd.pk)
        self.assertTrue(PlaceGeom.objects.filter(pk=g.pk).exists())               # QC only reports

    def test_flags_an_adopted_location_whose_source_terms_changed(self):
        self.adopted()
        GazetteerRegistryEntry.objects.filter(id="gn").update(license=lic("CC-BY-SA-4.0"))
        c = self.check()
        self.assertEqual((c["status"], c["should_not_be_visible"]), ("fail", {"source terms": 1}))
        GazetteerRegistryEntry.objects.filter(id="gn").update(license=lic("CC-BY-4.0"))
        self.assertEqual(self.check()["status"], "ok")


class QueryBoundTests(LifecycleTestBase):
    """Large datasets: the cost of the checks and of a retraction does not grow with rows."""

    def bulk_adopted(self, n):
        rows = []
        att = ContributorAttestation.objects.create(
            user=self.owner, place_a=f"gn:bulk{n}", place_b=f"whg:{self.ds.pk}:bulk{n}", relation_type="sameAs",
            status="active")
        for i in range(n):
            p = Place.objects.create(title=f"B{n}-{i}", src_id=f"b{n}-{i}", dataset=self.ds, ccodes=[])
            rows.append(PlaceGeom(place=p, src_id=p.src_id, task_id="gen:1", reviewer=self.owner,
                                  jsonb={"type": "Point", "coordinates": [0.5, 51.5],
                                         "provenance": {"kind": "adopted", "source": "gn", "match": f"gn:{i}",
                                                        "attestation": att.pk, "candidate": None}}))
        PlaceGeom.objects.bulk_create(rows)
        return att

    def count(self, fn):
        with CaptureQueriesContext(connection) as ctx:
            fn()
        return len(ctx.captured_queries)

    def test_audit_qc_and_retraction_are_constant_in_the_number_of_rows(self):
        from datasets import location_lifecycle, qc
        a3 = self.bulk_adopted(3)
        small = (self.count(lambda: location_lifecycle.audit(self.ds)),
                 self.count(lambda: qc.check_locations_added(self.ds)))
        a40 = self.bulk_adopted(40)
        large = (self.count(lambda: location_lifecycle.audit(self.ds)),
                 self.count(lambda: qc.check_locations_added(self.ds)))
        self.assertEqual(small, large)
        self.assertLessEqual(large[0], 4)

        def rows_of(att):
            return list(PlaceGeom.objects.filter(place__dataset=self.ds.label,
                                                 jsonb__provenance__attestation=att.pk))
        r3, r40 = rows_of(a3), rows_of(a40)
        self.assertEqual((len(r3), len(r40)), (3, 40))
        n3 = self.count(lambda: location_lifecycle.retract(r3, "source terms"))
        n40 = self.count(lambda: location_lifecycle.retract(r40, "source terms"))
        self.assertEqual(n3, n40)
        self.assertLessEqual(n40, 10)
        self.assertEqual(RetractedLocation.objects.count(), 43)
        self.assertFalse(PlaceGeom.objects.filter(pk__in=[r.pk for r in r3 + r40]).exists())

    def test_a_link_going_pending_or_active_costs_no_query(self):
        from datasets import location_lifecycle
        att = self.bulk_adopted(5)
        # Point the Link at one of its records, so that only the early return can avoid a query
        # (the control: the same call for a withdrawn Link does query, and retracts).
        att.place_b_pk = PlaceGeom.objects.filter(jsonb__provenance__attestation=att.pk).first().place_id
        att.status = "withdrawn"
        ContributorAttestation.objects.filter(pk=att.pk).update(status="withdrawn")
        with CaptureQueriesContext(connection) as ctx:
            self.assertEqual(location_lifecycle.on_link_saved(att), 1)
        self.assertGreater(len(ctx.captured_queries), 0)
        for status in ("pending", "active"):
            att.status = status
            with self.assertNumQueries(0):
                location_lifecycle.on_link_saved(att)

    def test_making_a_large_dataset_private_touches_no_location(self):
        self.make_public()
        self.bulk_adopted(30)
        ds = Dataset.objects.get(pk=self.ds.pk)
        ds.public = False
        ds.save()
        self.assertEqual(PlaceGeom.objects.filter(place__dataset=self.ds.label, task_id="gen:1").count(), 30)
        self.assertFalse(RetractedLocation.objects.exists())


class CommandTests(LifecycleTestBase):
    def run_cmd(self, *args):
        from io import StringIO
        from django.core.management import call_command
        out = StringIO()
        call_command("location_lifecycle", *args, stdout=out)
        return out.getvalue()

    def test_dry_run_reports_and_changes_nothing_then_apply_retracts_once(self):
        g = self.adopted()
        out = self.run_cmd("--dry-run")
        self.assertIn("0 adopted location(s) to retract", out)                   # the control
        ContributorAttestation.objects.filter(pk=self.link_row().pk).update(status="withdrawn")
        out = self.run_cmd("--dry-run")
        self.assertIn("1 adopted location(s) to retract", out)
        self.assertIn(f"dataset {self.ds.pk}", out)
        self.assertTrue(PlaceGeom.objects.filter(pk=g.pk).exists())
        self.run_cmd()
        self.assertFalse(PlaceGeom.objects.filter(pk=g.pk).exists())
        self.assertEqual(RetractedLocation.objects.get().reason, "link no longer stands")
        out = self.run_cmd()
        self.assertIn("0 adopted location(s) retracted", out)                    # idempotent
        self.assertEqual(RetractedLocation.objects.count(), 1)

    def test_restore_by_id(self):
        g = self.adopted()
        attestations.withdraw(self.link_row(), by=self.owner)
        self.link(self.hodd, "gn:8")
        arch = RetractedLocation.objects.get(geom_pk=g.pk)
        out = self.run_cmd("--restore", str(arch.pk), "--dry-run")
        self.assertIn("would restore", out)
        self.assertFalse(PlaceGeom.objects.filter(place=self.hodd).exists())
        self.run_cmd("--restore", str(arch.pk))
        self.assertEqual(PlaceGeom.objects.filter(place=self.hodd).count(), 1)
