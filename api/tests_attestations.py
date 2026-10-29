"""Phase 0 of place#334: who may write to the gateway's hard-link overlay, and what.

Each refusal is paired with the same request succeeding through the same code path, so a
check that passes because the path was never reached (or always refuses) would fail its pair.

    GDAL_LIBRARY_PATH=... venv/bin/python manage.py test api.tests_attestations \
        --settings=whg.settings_localtest
"""
from unittest import mock

from django.contrib.auth import get_user_model
from django.test import SimpleTestCase, TestCase, override_settings

from api import attestations
from api.crc_client import crc_delete_link, crc_post_link
from api.models import ContributorAttestation

LINK = {"place_a": "wd:Q90", "place_b": "gn:2988507", "relation_type": "closeMatch",
        "source_category": "contributor", "source_id": "contributor:1"}
KEY = {"place_a": "wd:Q90", "place_b": "gn:2988507", "relation_type": "closeMatch",
       "source_id": "contributor:1"}


@override_settings(CRC_GATEWAY_URL="http://gateway.invalid:9200")
class GatewayWriteRailTests(SimpleTestCase):
    """N3: dev shares the gateway with prod, so only prod may write the overlay."""

    def _post(self, context):
        with override_settings(ENV_CONTEXT=context), \
                mock.patch("api.crc_client.requests.post") as post:
            post.return_value.status_code = 200
            return crc_post_link(LINK), post

    def _delete(self, context):
        with override_settings(ENV_CONTEXT=context), \
                mock.patch("api.crc_client.requests.request") as req:
            req.return_value.status_code = 200
            return crc_delete_link(KEY), req

    def test_production_context_posts(self):
        ok, post = self._post("whgazetteer-org")
        self.assertTrue(ok)
        post.assert_called_once()

    def test_dev_context_does_not_post(self):
        ok, post = self._post("dev-whgazetteer-org")
        self.assertFalse(ok)
        post.assert_not_called()

    def test_local_context_does_not_post(self):
        ok, post = self._post("local")
        self.assertFalse(ok)
        post.assert_not_called()

    def test_production_context_deletes(self):
        ok, req = self._delete("whgazetteer-org")
        self.assertTrue(ok)
        req.assert_called_once()

    def test_dev_context_does_not_delete(self):
        ok, req = self._delete("dev-whgazetteer-org")
        self.assertFalse(ok)
        req.assert_not_called()


class VisibilityTests(SimpleTestCase):
    """N4: both sides must be visible, and the refusal is the same whichever side hides."""

    BODY = {"dataset_id": "whg:5", "place_a": "whg:5:a1", "place_b": "wd:Q90",
            "relation_type": "closeMatch"}

    def _validate(self, body=None, hidden=(), dataset_ok=True):
        def vis(pid, user):
            return mock.Mock(allowed=pid not in hidden)
        with mock.patch("api.attestations.dataset_visibility", side_effect=vis), \
                mock.patch("api.attestations._namespace_visible", return_value=True), \
                mock.patch("api.attestations._dataset_visible",
                           side_effect=None if dataset_ok else attestations._not_found()):
            return attestations.validate(mock.Mock(), body or self.BODY)

    def test_public_pair_is_accepted(self):
        self.assertEqual(self._validate()["relation_type"], "closeMatch")

    def test_hidden_contributed_place_refused_as_404(self):
        with self.assertRaises(attestations.AttestationRefused) as cm:
            self._validate(hidden={"whg:5:a1"})
        self.assertEqual(cm.exception.status, 404)

    def test_hidden_on_the_other_side_refused_identically(self):
        body = dict(self.BODY, place_a="wd:Q90", place_b="whg:5:a1")
        self._validate(body)  # the same pair, visible: accepted
        with self.assertRaises(attestations.AttestationRefused) as cm:
            self._validate(body, hidden={"whg:5:a1"})
        self.assertEqual((cm.exception.status, cm.exception.message), (404, "not found"))

    def test_hidden_dataset_refused(self):
        with self.assertRaises(attestations.AttestationRefused) as cm:
            self._validate(dataset_ok=False)
        self.assertEqual(cm.exception.status, 404)

    def test_unknown_authority_namespace_refused(self):
        with mock.patch("api.attestations._dataset_visible"), \
                mock.patch("api.attestations._namespace_visible", return_value=False):
            with self.assertRaises(attestations.AttestationRefused) as cm:
                attestations.validate(mock.Mock(), dict(self.BODY, place_a="zz:1", place_b="wd:Q90"))
        self.assertEqual(cm.exception.status, 404)

    def test_bad_relation_type_refused(self):
        self._validate()
        with self.assertRaises(attestations.AttestationRefused) as cm:
            self._validate(dict(self.BODY, relation_type="isTheSameAsEverything"))
        self.assertEqual(cm.exception.status, 400)

    def test_self_link_refused(self):
        with self.assertRaises(attestations.AttestationRefused):
            self._validate(dict(self.BODY, place_b="whg:5:a1"))

    def test_malformed_id_refused(self):
        for bad in ("nocolon", "whg:5", "WD:Q90 ", "x" * 200 + ":1"):
            with self.subTest(bad=bad), self.assertRaises(attestations.AttestationRefused):
                self._validate(dict(self.BODY, place_b=bad))


@mock.patch("api.signals.crc_delete_link")
@mock.patch("api.signals.crc_post_link")
@mock.patch("api.attestations._place_visible")
@mock.patch("api.attestations._dataset_visible")
class WriteTests(TestCase):
    BODY = {"dataset_id": "whg:5", "place_a": "wd:Q90", "place_b": "gn:2988507",
            "relation_type": "closeMatch"}

    def setUp(self):
        self.user = get_user_model().objects.create_user(
            username="att", email="att@example.org", password="x", given_name="A", surname="T")

    def test_client_status_and_legacy_flag_are_ignored(self, *_):
        row, created = attestations.create(
            self.user, dict(self.BODY, status="pending", legacy_v3_2=True))
        self.assertTrue(created)
        self.assertEqual((row.status, row.legacy_v3_2), ("active", False))

    def test_withdraw_keeps_row_and_revokes(self, ds_vis, place_vis, post, delete):
        row, _ = attestations.create(self.user, self.BODY)
        post.assert_called_once()
        delete.assert_not_called()
        attestations.withdraw(row)
        row.refresh_from_db()
        self.assertEqual(row.status, "withdrawn")
        self.assertIsNotNone(row.withdrawn_at)
        delete.assert_called_once()
        self.assertEqual(ContributorAttestation.objects.count(), 1)

    def test_reattest_after_withdraw_reactivates_the_same_row(self, ds_vis, place_vis, post, delete):
        row, _ = attestations.create(self.user, self.BODY)
        attestations.withdraw(row)
        again, created = attestations.create(self.user, self.BODY)
        self.assertTrue(created)
        self.assertEqual(again.pk, row.pk)
        self.assertEqual((again.status, again.withdrawn_at), ("active", None))

    def test_repeat_of_an_active_edge_is_idempotent(self, *_):
        row, _created = attestations.create(self.user, self.BODY)
        again, created = attestations.create(self.user, dict(self.BODY, place_a="gn:2988507", place_b="wd:Q90"))
        self.assertFalse(created)
        self.assertEqual(again.pk, row.pk)

    @override_settings(CONTRIBUTOR_ATTESTATION_DAILY_CAP=2)
    def test_daily_cap(self, *_):
        for i in range(2):
            attestations.create(self.user, dict(self.BODY, place_b=f"gn:{i}"))
        with self.assertRaises(attestations.AttestationRefused) as cm:
            attestations.create(self.user, dict(self.BODY, place_b="gn:99"))
        self.assertEqual(cm.exception.status, 429)

    def test_dataset_deletion_withdraws_its_attestations_only(self, ds_vis, place_vis, post, delete):
        mine, _ = attestations.create(self.user, dict(self.BODY, dataset_id="whg:5"))
        touching, _ = attestations.create(
            self.user, dict(self.BODY, dataset_id="whg:7", place_a="whg:5:x", place_b="wd:Q1"))
        other, _ = attestations.create(self.user, dict(self.BODY, dataset_id="whg:7", place_b="gn:1"))
        # whg:55 must not match the whg:5 prefix
        sibling, _ = attestations.create(
            self.user, dict(self.BODY, dataset_id="whg:55", place_a="whg:55:x", place_b="wd:Q2"))
        self.assertEqual(attestations.withdraw_for_dataset(5), 2)
        status = {r.pk: r.status for r in ContributorAttestation.objects.all()}
        self.assertEqual(status[mine.pk], "withdrawn")
        self.assertEqual(status[touching.pk], "withdrawn")
        self.assertEqual(status[other.pk], "active")
        self.assertEqual(status[sibling.pk], "active")
        self.assertEqual(delete.call_count, 2)
