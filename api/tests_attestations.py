"""Phases 0-1 of place#334: who may write to the gateway's hard-link overlay, what a write
stores, and how an attestation reaches the Record page's Concordance.

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

    BODY = {"place_a": "whg:5:a1", "place_b": "wd:Q90", "relation_type": "closeMatch"}

    def _validate(self, body=None, hidden=(), missing=(), public=True, namespace_ok=True):
        def vis(pid, user):
            return mock.Mock(allowed=pid not in hidden, dataset=mock.Mock(public=public))
        with mock.patch("api.attestations.dataset_visibility", side_effect=vis), \
                mock.patch("api.attestations._place_pk",
                           side_effect=lambda ds, src: None if src in missing else 100 + ds), \
                mock.patch("api.attestations.is_dataset_embargoed", return_value=False), \
                mock.patch("api.attestations._namespace_visible", return_value=namespace_ok):
            return attestations.validate(mock.Mock(), body or self.BODY)

    def test_public_pair_is_accepted_and_describes_both_sides(self):
        clean = self._validate()
        self.assertEqual(clean["relation_type"], "closeMatch")
        self.assertEqual(clean["dataset_id"], "whg:5")
        self.assertEqual([x["place_pk"] for x in clean["_sides"]], [105, None])

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

    def test_a_record_that_does_not_exist_is_the_same_404(self):
        self._validate()
        with self.assertRaises(attestations.AttestationRefused) as cm:
            self._validate(missing={"a1"})
        self.assertEqual((cm.exception.status, cm.exception.message), (404, "not found"))

    def test_unknown_authority_namespace_refused(self):
        body = dict(self.BODY, place_a="zz:1")
        self._validate(body)
        with self.assertRaises(attestations.AttestationRefused) as cm:
            self._validate(body, namespace_ok=False)
        self.assertEqual(cm.exception.status, 404)

    def test_a_django_pk_id_is_canonicalised_to_the_gateway_form(self):
        """The Record page names its own record as whg:<pk>; it must reach the same Place
        as the gateway form, or the same link would be stored twice under two names."""
        body = {"place_a": "whg:105", "place_b": "wd:Q90", "relation_type": "closeMatch"}
        with mock.patch("api.attestations._place_ref", return_value=(5, "a1")):
            via_pk = self._validate(body)
        via_gateway = self._validate()
        self.assertEqual((via_pk["place_a"], via_pk["dataset_id"]), ("whg:5:a1", "whg:5"))
        self.assertEqual((via_pk["place_a"], via_pk["place_b"]), (via_gateway["place_a"], via_gateway["place_b"]))

    def test_a_django_pk_that_names_no_record_is_the_same_404(self):
        body = {"place_a": "whg:105", "place_b": "wd:Q90", "relation_type": "closeMatch"}
        with mock.patch("api.attestations._place_ref", return_value=(5, "a1")):
            self._validate(body)  # control: it resolves
        with mock.patch("api.attestations._place_ref", return_value=None):
            with self.assertRaises(attestations.AttestationRefused) as cm:
                self._validate(body)
        self.assertEqual((cm.exception.status, cm.exception.message), (404, "not found"))

    def test_the_two_forms_of_one_record_are_a_self_link(self):
        body = {"place_a": "whg:105", "place_b": "whg:5:a1", "relation_type": "closeMatch"}
        with mock.patch("api.attestations._place_ref", return_value=(5, "a1")):
            with self.assertRaises(attestations.AttestationRefused):
                self._validate(body)

    def test_dataset_id_is_blank_when_neither_side_is_contributed(self):
        self.assertEqual(self._validate(dict(self.BODY, place_a="gn:1"))["dataset_id"], "")

    def test_client_dataset_id_is_ignored(self):
        self.assertEqual(self._validate(dict(self.BODY, dataset_id="whg:999"))["dataset_id"], "whg:5")

    def test_bad_relation_type_refused(self):
        self._validate()
        with self.assertRaises(attestations.AttestationRefused) as cm:
            self._validate(dict(self.BODY, relation_type="isTheSameAsEverything"))
        self.assertEqual(cm.exception.status, 400)

    def test_bad_certainty_refused(self):
        self._validate(dict(self.BODY, certainty="probable"))
        with self.assertRaises(attestations.AttestationRefused):
            self._validate(dict(self.BODY, certainty="certain"))

    def test_self_link_refused(self):
        with self.assertRaises(attestations.AttestationRefused):
            self._validate(dict(self.BODY, place_b="whg:5:a1"))

    def test_malformed_id_refused(self):
        for bad in ("nocolon", "whg:", "WD:Q90 ", "x" * 200 + ":1"):
            with self.subTest(bad=bad), self.assertRaises(attestations.AttestationRefused):
                self._validate(dict(self.BODY, place_b=bad))

    def test_can_attest_mirrors_validate(self):
        with mock.patch("api.attestations._place_side") as side, \
                mock.patch("api.attestations._check_cap"):
            self.assertTrue(attestations.can_attest(mock.Mock(), "wd:Q1", "gn:2"))
            side.side_effect = attestations._not_found()
            self.assertFalse(attestations.can_attest(mock.Mock(), "wd:Q1", "gn:2"))


def fake_side(private=(), places=None):
    """A stand-in for ``attestations._place_side``: contributed ids ``whg:<ds>:<src>`` get
    pk ``places[pid]`` (default 1000 + len), and are non-public when listed in ``private``."""
    places = places or {}

    def side(pid, user):
        if pid.startswith("whg:"):
            ds = int(pid.split(":")[1])
            return {"pid": pid, "dataset_pk": ds, "place_pk": places.get(pid, 1000 + len(pid)),
                    "public": pid not in private}
        return {"pid": pid, "dataset_pk": None, "place_pk": None, "public": True}
    return side


@mock.patch("api.signals.forward_attestation_revoke")
@mock.patch("api.signals.forward_attestation_post")
class WriteTests(TestCase):
    BODY = {"place_a": "wd:Q90", "place_b": "gn:2988507", "relation_type": "closeMatch"}

    def setUp(self):
        self.user = get_user_model().objects.create_user(
            username="att", email="att@example.org", password="x", given_name="A", surname="T")
        p = mock.patch("api.attestations._place_side", side_effect=fake_side())
        p.start()
        self.addCleanup(p.stop)

    def create(self, body=None, user=None):
        with self.captureOnCommitCallbacks(execute=True):
            return attestations.create(user or self.user, body or self.BODY)

    def test_client_status_and_legacy_flag_are_ignored(self, post, revoke):
        row, created = self.create(dict(self.BODY, status="pending", legacy_v3_2=True))
        self.assertTrue(created)
        self.assertEqual((row.status, row.legacy_v3_2, row.suggested_by), ("active", False, "manual"))

    def test_active_row_is_forwarded_once_after_commit(self, post, revoke):
        row, _ = self.create()
        post.delay.assert_called_once_with(row.pk)
        revoke.delay.assert_not_called()

    def test_row_on_an_unpublished_dataset_is_pending_and_never_forwarded(self, post, revoke):
        body = {"place_a": "whg:5:a1", "place_b": "wd:Q90", "relation_type": "closeMatch"}
        with mock.patch("api.attestations._place_side", side_effect=fake_side(private={"whg:5:a1"})):
            row, _ = self.create(body)
        self.assertEqual((row.status, row.dataset_id), ("pending", "whg:5"))
        post.delay.assert_not_called()
        revoke.delay.assert_not_called()

    def test_same_body_on_a_public_dataset_is_active_and_forwarded(self, post, revoke):
        """Control for the pending test above: only publicness differs."""
        row, _ = self.create({"place_a": "whg:5:a1", "place_b": "wd:Q90", "relation_type": "closeMatch"})
        self.assertEqual(row.status, "active")
        post.delay.assert_called_once_with(row.pk)

    def test_place_pks_follow_canonical_order(self, post, revoke):
        places = {"whg:5:a1": 77}
        body = {"place_a": "whg:5:a1", "place_b": "wd:Q90", "relation_type": "closeMatch"}
        with mock.patch("api.attestations._place_side", side_effect=fake_side(places=places)):
            row, _ = self.create(body)
        # canonical order is plain string order: 'wd:...' < 'whg:...', so the authority id is place_a
        self.assertEqual((row.place_a, row.place_b), ("wd:Q90", "whg:5:a1"))
        self.assertEqual((row.place_a_pk, row.place_b_pk), (None, 77))

    def test_withdraw_keeps_row_revokes_and_records_the_event(self, post, revoke):
        row, _ = self.create()
        with self.captureOnCommitCallbacks(execute=True):
            attestations.withdraw(row, by=self.user)
        row.refresh_from_db()
        self.assertEqual(row.status, "withdrawn")
        self.assertEqual(row.withdrawn_by_id, self.user.id)
        revoke.delay.assert_called_once_with(row.gateway_key())
        self.assertEqual(ContributorAttestation.objects.count(), 1)
        self.assertEqual(list(row.events.values_list("action", flat=True)), ["created", "withdrawn"])

    def test_reattest_after_withdraw_reactivates_the_same_row(self, post, revoke):
        row, _ = self.create()
        attestations.withdraw(row)
        again, created = self.create()
        self.assertTrue(created)
        self.assertEqual(again.pk, row.pk)
        self.assertEqual((again.status, again.withdrawn_at), ("active", None))
        self.assertEqual(list(row.events.values_list("action", flat=True)),
                         ["created", "withdrawn", "reactivated"])

    def test_repeat_of_an_active_edge_is_idempotent(self, post, revoke):
        row, _created = self.create()
        again, created = self.create(dict(self.BODY, place_a="gn:2988507", place_b="wd:Q90"))
        self.assertFalse(created)
        self.assertEqual(again.pk, row.pk)

    @override_settings(CONTRIBUTOR_ATTESTATION_DAILY_CAP=2)
    def test_daily_cap(self, post, revoke):
        for i in range(2):
            self.create(dict(self.BODY, place_b=f"gn:{i}"))
        with self.assertRaises(attestations.AttestationRefused) as cm:
            self.create(dict(self.BODY, place_b="gn:99"))
        self.assertEqual(cm.exception.status, 429)

    def test_dataset_deletion_withdraws_its_attestations_only(self, post, revoke):
        mine, _ = self.create({"place_a": "whg:5:x", "place_b": "wd:Q1", "relation_type": "closeMatch"})
        touching, _ = self.create({"place_a": "wd:Q9", "place_b": "whg:5:y", "relation_type": "closeMatch"})
        other, _ = self.create(dict(self.BODY, place_b="gn:1"))
        sibling, _ = self.create({"place_a": "whg:55:x", "place_b": "wd:Q2", "relation_type": "closeMatch"})
        with self.captureOnCommitCallbacks(execute=True):
            self.assertEqual(attestations.withdraw_for_dataset(5), 2)  # whg:55 must not match
        status = {r.pk: r.status for r in ContributorAttestation.objects.all()}
        self.assertEqual(status[mine.pk], "withdrawn")
        self.assertEqual(status[touching.pk], "withdrawn")
        self.assertEqual(status[other.pk], "active")
        self.assertEqual(status[sibling.pk], "active")
        self.assertEqual(revoke.delay.call_count, 2)

    def test_a_write_drops_the_cached_concordances_it_changes(self, post, revoke):
        from django.core.cache import cache
        body = {"place_a": "whg:5:a1", "place_b": "wd:Q90", "relation_type": "closeMatch"}
        key = "concordance:v1:{}:{}"
        pk = 4242
        with mock.patch("api.attestations._place_side", side_effect=fake_side(places={"whg:5:a1": pk})):
            cache.set(key.format(pk, "beta"), {"stale": True}, 60)
            cache.set(key.format(pk + 1, "beta"), {"other": True}, 60)
            self.create(body)
        self.assertIsNone(cache.get(key.format(pk, "beta")))
        self.assertEqual(cache.get(key.format(pk + 1, "beta")), {"other": True})  # control
        cache.delete(key.format(pk + 1, "beta"))


class ForwardTaskTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(
            username="fw", email="fw@example.org", password="x", given_name="F", surname="W")
        self.row = ContributorAttestation.objects.create(
            user=self.user, dataset_id="", place_a="gn:1", place_b="wd:Q1",
            relation_type="closeMatch", status="active")

    def test_task_sends_an_active_row(self):
        from api import tasks
        with mock.patch("api.crc_client.crc_post_link", return_value=True) as post:
            self.assertTrue(tasks.forward_attestation_post(self.row.pk))
        self.assertEqual(post.call_args[0][0]["source_id"], f"contributor:{self.user.id}")

    def test_task_does_not_send_a_row_withdrawn_after_it_was_queued(self):
        from api import tasks
        ContributorAttestation.objects.filter(pk=self.row.pk).update(status="withdrawn")
        with mock.patch("api.crc_client.crc_post_link") as post:
            self.assertFalse(tasks.forward_attestation_post(self.row.pk))
        post.assert_not_called()


class ApiViewTests(TestCase):
    """The HTTP surface, with the place checks stubbed (they are covered above)."""

    def setUp(self):
        User = get_user_model()
        self.user = User.objects.create_user(username="v1", email="v1@example.org", password="x",
                                             given_name="V", surname="One")
        self.other = User.objects.create_user(username="v2", email="v2@example.org", password="x",
                                              given_name="V", surname="Two")
        self.client.force_login(self.user)
        p = mock.patch("api.attestations._place_side", side_effect=fake_side())
        p.start()
        self.addCleanup(p.stop)
        for t in ("forward_attestation_post", "forward_attestation_revoke"):
            q = mock.patch(f"api.signals.{t}")
            q.start()
            self.addCleanup(q.stop)

    def post(self, body):
        return self.client.post("/api/links", body, content_type="application/json")

    def test_post_get_about_patch_round_trip(self):
        r = self.post({"place_a": "wd:Q90", "place_b": "gn:2988507", "relation_type": "closeMatch"})
        self.assertEqual(r.status_code, 201)
        self.assertEqual(r.json()["status"], "active")
        again = self.post({"place_a": "wd:Q90", "place_b": "gn:2988507", "relation_type": "closeMatch"})
        self.assertEqual(again.status_code, 200)
        got = self.client.get("/api/links", {"about": "wd:Q90"}).json()
        self.assertEqual(got["count"], 1)
        self.assertTrue(got["result"][0]["mine"])
        self.assertEqual(got["result"][0]["attested_by"]["name"], "V One")
        rid = r.json()["id"]
        self.assertEqual(self.client.patch("/api/links", {"id": rid, "status": "active"},
                                           content_type="application/json").status_code, 400)
        self.assertEqual(self.client.patch("/api/links", {"id": rid, "status": "withdrawn"},
                                           content_type="application/json").status_code, 200)
        self.assertEqual(self.client.get("/api/links", {"about": "wd:Q90"}).json()["count"], 0)

    def test_another_user_sees_it_but_not_as_theirs_and_cannot_withdraw_it(self):
        rid = self.post({"place_a": "wd:Q90", "place_b": "gn:1", "relation_type": "distinct"}).json()["id"]
        self.client.force_login(self.other)
        got = self.client.get("/api/links", {"about": "wd:Q90"}).json()
        self.assertEqual((got["count"], got["result"][0]["mine"]), (1, False))
        self.assertEqual(self.client.delete("/api/links", {"id": rid},
                                            content_type="application/json").status_code, 404)

    def test_pending_row_is_visible_to_its_author_only(self):
        with mock.patch("api.attestations._place_side", side_effect=fake_side(private={"whg:5:a1"})):
            r = self.post({"place_a": "whg:5:a1", "place_b": "wd:Q90", "relation_type": "closeMatch"})
            self.assertEqual(r.json()["status"], "pending")
            mine = self.client.get("/api/links", {"about": "wd:Q90"}).json()
            self.client.force_login(self.other)
            theirs = self.client.get("/api/links", {"about": "wd:Q90"}).json()
        self.assertEqual((mine["count"], theirs["count"]), (1, 0))

    def test_anonymous_is_401(self):
        self.client.logout()
        self.assertEqual(self.client.get("/api/links", {"about": "wd:Q90"}).status_code, 401)
        self.assertEqual(self.post({}).status_code, 401)
