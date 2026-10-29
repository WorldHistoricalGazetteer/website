"""Attestations in the Record page's Concordance (place#334 Phase 1).

An attestation is a statement by a WHG user, so it enters the Concordance as a source of its own
beside place_link and close_matches: an attester in the record's dataset circle speaks outbound
(tier 1), anyone else inbound (tier 2), ``distinct`` is listed apart and never aggregates, and a
pending row (a private dataset is involved) reaches only its author and that dataset's circle.

    GDAL_LIBRARY_PATH=... venv/bin/python manage.py test places.tests_concordance_attestations \
        --settings=whg.settings_localtest
"""
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase

from api.models import ContributorAttestation
from places.concordance import build_concordance, cached_concordance, decorate_for_user


class AttestationConcordanceTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        from datasets.models import Dataset
        from places.models import Place

        User = get_user_model()
        cls.owner = User.objects.create_user(username="ac_owner", email="ac_owner@example.org", password="pw",
                                             given_name="Ada", surname="Owner")
        cls.stranger = User.objects.create_user(username="ac_str", email="ac_str@example.org", password="pw",
                                                given_name="Sam", surname="Stranger")

        def ds(key, public=True):
            return Dataset.objects.create(owner=cls.owner, label=f"ac334_{key}", title=f"Dataset {key}",
                                          description="D", creator="X", public=public, ds_status="indexed",
                                          uri_base="https://example.org/places/")

        with patch("datasets.signals.doi"):
            cls.ds_a, cls.ds_b, cls.ds_priv = ds("a"), ds("b"), ds("priv", public=False)
        cls.A = Place.objects.create(title="Alpha", src_id="a1", dataset=cls.ds_a, ccodes=[])
        cls.B = Place.objects.create(title="Beta", src_id="b1", dataset=cls.ds_b, ccodes=[])
        cls.C = Place.objects.create(title="Gamma", src_id="c1", dataset=cls.ds_b, ccodes=[])
        cls.P = Place.objects.create(title="Hidden", src_id="p1", dataset=cls.ds_priv, ccodes=[])

    def setUp(self):
        for t in ("forward_attestation_post", "forward_attestation_revoke"):
            p = patch(f"api.signals.{t}")
            p.start()
            self.addCleanup(p.stop)

    def gid(self, place):
        return f"whg:{place.dataset.pk}:{place.src_id}"

    def attest(self, user, a, b, relation="closeMatch", status="active"):
        ids = sorted([(self.gid(a), a.id), (self.gid(b), b.id)])
        return ContributorAttestation.objects.create(
            user=user, dataset_id=f"whg:{a.dataset.pk}", place_a=ids[0][0], place_b=ids[1][0],
            place_a_pk=ids[0][1], place_b_pk=ids[1][1], relation_type=relation, status=status)

    def build(self, place=None):
        return build_concordance(place or self.A, user=self.owner, use_gateway=False)

    @staticmethod
    def attested(entries):
        return [e for e in entries if e.get("source") == "attestation"]

    def test_owner_attestation_is_tier_1_and_aggregates_its_target(self):
        self.attest(self.owner, self.A, self.B)
        conc = self.build()
        (e,) = self.attested(conc["outbound"])
        self.assertEqual((e["tier"], e["identifier"], e["aggregates"]), (1, self.gid(self.B), True))
        self.assertEqual(e["target"]["records"][0]["place_id"], self.B.id)
        self.assertEqual([m["place_id"] for m in conc["aggregate"]], [self.B.id])
        self.assertEqual(self.attested(conc["inbound"]), [])

    def test_stranger_attestation_is_tier_2_with_its_target(self):
        self.attest(self.stranger, self.A, self.B)
        conc = self.build()
        (e,) = self.attested(conc["inbound"])
        self.assertEqual((e["tier"], e["attestation"]["attested_by"]["name"]), (2, "Sam Stranger"))
        self.assertEqual([(m["place_id"], m["tiers"]) for m in conc["aggregate"]], [(self.B.id, [2])])
        self.assertEqual(self.attested(conc["outbound"]), [])

    def test_two_attesters_of_the_same_target_make_one_aggregate_member(self):
        self.attest(self.owner, self.A, self.B)
        self.attest(self.stranger, self.A, self.B)
        conc = self.build()
        self.assertEqual([(m["place_id"], m["tiers"]) for m in conc["aggregate"]], [(self.B.id, [1, 2])])

    def test_distinct_is_listed_apart_and_never_aggregates(self):
        self.attest(self.stranger, self.A, self.C, relation="distinct")
        conc = self.build()
        self.assertEqual(len(conc["distinct"]), 1)
        self.assertEqual(conc["distinct"][0]["identifier"], self.gid(self.C))
        self.assertEqual((conc["outbound"], conc["inbound"], conc["aggregate"]), ([], [], []))
        # control: the same pair as closeMatch does aggregate
        ContributorAttestation.objects.all().delete()
        self.attest(self.stranger, self.A, self.C, relation="closeMatch")
        self.assertEqual([m["place_id"] for m in self.build()["aggregate"]], [self.C.id])

    def test_withdrawn_and_rejected_rows_do_not_appear(self):
        self.attest(self.owner, self.A, self.B, status="withdrawn")
        self.attest(self.stranger, self.A, self.B, status="rejected")
        conc = self.build()
        self.assertEqual(self.attested(conc["outbound"] + conc["inbound"]), [])

    def test_pending_row_reaches_its_author_and_not_other_users_or_the_cache(self):
        self.attest(self.stranger, self.A, self.P, status="pending")
        base = self.build()
        self.assertEqual(self.attested(base["outbound"] + base["inbound"]), [])  # shared build: none
        mine = decorate_for_user(self.build(), self.A, self.stranger)
        (e,) = self.attested(mine["inbound"])
        self.assertTrue(e["attestation"]["mine"])
        self.assertEqual(e["attestation"]["status"], "pending")
        others = decorate_for_user(self.build(), self.B, self.owner)
        self.assertEqual(self.attested(others["outbound"] + others["inbound"]), [])

    def test_pending_row_reaches_the_owner_of_the_unpublished_dataset(self):
        """The dataset's circle sees it too; a stranger who is not in it does not."""
        self.attest(self.stranger, self.A, self.P, status="pending")
        owner_view = decorate_for_user(self.build(), self.A, self.owner)
        self.assertEqual(len(self.attested(owner_view["inbound"] + owner_view["outbound"])), 1)
        self.assertFalse((self.attested(owner_view["inbound"] + owner_view["outbound"])[0]
                          ["attestation"]["mine"]))

    def test_mine_is_per_user_and_the_internal_user_id_never_leaves(self):
        self.attest(self.owner, self.A, self.B)
        self.attest(self.stranger, self.A, self.C)
        as_owner = decorate_for_user(self.build(), self.A, self.owner)
        flags = {e["identifier"]: e["attestation"]["mine"]
                 for e in self.attested(as_owner["outbound"] + as_owner["inbound"])}
        self.assertEqual(flags, {self.gid(self.B): True, self.gid(self.C): False})
        for e in self.attested(as_owner["outbound"] + as_owner["inbound"]):
            self.assertNotIn("_uid", e["attestation"])

    def test_a_write_shows_on_the_next_cached_read(self):
        with patch("places.concordance.gateway_namespaces", return_value=set()):
            first = cached_concordance(self.A, user=self.owner)
            self.assertEqual(self.attested(first["outbound"] + first["inbound"]), [])
            cached_concordance(self.A, user=self.owner)  # warm the cache
            self.attest(self.owner, self.A, self.B)  # post_save drops the cached entries
            after = cached_concordance(self.A, user=self.owner)
        self.assertEqual(len(self.attested(after["outbound"])), 1)
