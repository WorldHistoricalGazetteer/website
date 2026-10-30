"""The v4 dataset lifecycle (place#334 Phase 3): the state machine, its refusals, and its
writes to GRACE.

Every refusal is driven through the same function that, from the right state or by the
right person, succeeds; the table test below runs every transition from every state, so a
machine that refused everything (or allowed everything) fails it. Mail, the gateway push,
the DOI call and the pub index are all patched: nothing leaves the test.

    GDAL_LIBRARY_PATH=... venv/bin/python manage.py test datasets.tests_lifecycle \
        --settings=whg.settings_localtest
"""
import datetime
from unittest import mock

from django.contrib.auth import get_user_model
from django.test import TestCase

from datasets import lifecycle
from datasets.models import LIFECYCLE_STATES, Dataset, DatasetLifecycleEvent

User = get_user_model()
ALL_STATES = [None] + [s for s, _ in LIFECYCLE_STATES]


def patch_side_effects(testcase):
    """Patch everything a transition (or a Dataset save) could send out of the process."""
    targets = {
        "doi": "datasets.signals.doi",
        "mail": "datasets.lifecycle.WHGmail",
        "v3mail": "whgmail.messaging.WHGmail",
        "index": "datasets.tasks.index_to_pub",
        "unindex": "datasets.tasks.unindex_from_pub",
        "cache": "api.signals.invalidate_and_rebuild_cache",
        "post": "api.signals.forward_attestation_post",
        "revoke": "api.signals.forward_attestation_revoke",
    }
    mocks = {}
    for key, target in targets.items():
        p = mock.patch(target)
        mocks[key] = p.start()
        testcase.addCleanup(p.stop)
    mocks["mail"].return_value = True
    return mocks


class LifecycleBase(TestCase):
    @classmethod
    def setUpTestData(cls):
        from licensing.models import License
        cls.owner = User.objects.create_user(username="lc_owner", email="lc_owner@example.org",
                                             password="pw", given_name="Olive", surname="Owner",
                                             name="Olive Owner")
        cls.editor = User.objects.create_user(username="lc_ed", email="lc_ed@example.org",
                                              password="pw", given_name="Eda", surname="Editor",
                                              name="Eda Editor", is_staff=True)
        cls.stranger = User.objects.create_user(username="lc_str", email="lc_str@example.org",
                                                password="pw", given_name="Sam",
                                                surname="Stranger", name="Sam Stranger")
        cls.licence, _ = License.objects.get_or_create(spdx_id="CC-BY-4.0",
                                                       defaults={"label": "CC BY 4.0"})

    def setUp(self):
        self.m = patch_side_effects(self)
        with mock.patch("datasets.signals.doi"):
            self.ds = Dataset.objects.create(
                owner=self.owner, label="lc334", title="Lifecycle test", description="D",
                creator="X", public=False, license=self.licence,
                uri_base="https://example.org/places/")

    def set_state(self, state, public=None):
        fields = {"lifecycle_state": state}
        if public is not None:
            fields["public"] = public
        Dataset.objects.filter(pk=self.ds.pk).update(**fields)
        self.ds.refresh_from_db()

    def state(self):
        return Dataset.objects.values_list("lifecycle_state", flat=True).get(pk=self.ds.pk)

    def run_on_commit(self, fn, *args, **kwargs):
        with self.captureOnCommitCallbacks(execute=True):
            return fn(*args, **kwargs)


class FullPathTests(LifecycleBase):
    def test_submission_to_publication_and_back(self):
        self.run_on_commit(lifecycle.submit, self.ds, self.owner)
        self.run_on_commit(lifecycle.start_review, self.ds, self.editor)
        self.run_on_commit(lifecycle.accept, self.ds, self.editor)
        self.run_on_commit(lifecycle.publish, self.ds, self.owner)
        self.assertTrue(Dataset.objects.get(pk=self.ds.pk).public)
        self.run_on_commit(lifecycle.withdraw, self.ds, self.owner, "taking it back")
        self.assertFalse(Dataset.objects.get(pk=self.ds.pk).public)
        # A withdrawn dataset is reviewed again before it can be published (2026-10-30).
        with self.assertRaises(lifecycle.LifecycleRefused) as cm:
            self.run_on_commit(lifecycle.publish, self.ds, self.owner)
        self.assertEqual((cm.exception.status, self.state()), (409, "withdrawn"))
        self.run_on_commit(lifecycle.submit, self.ds, self.owner)
        self.run_on_commit(lifecycle.start_review, self.ds, self.editor)
        self.run_on_commit(lifecycle.accept, self.ds, self.editor)
        self.run_on_commit(lifecycle.publish, self.ds, self.owner)
        steps = list(DatasetLifecycleEvent.objects.filter(dataset_pk=self.ds.pk)
                     .values_list("from_state", "to_state", "actor_id"))
        self.assertEqual(steps, [
            (None, "submitted", self.owner.id), ("submitted", "in_review", self.editor.id),
            ("in_review", "accepted", self.editor.id), ("accepted", "published", self.owner.id),
            ("published", "withdrawn", self.owner.id), ("withdrawn", "submitted", self.owner.id),
            ("submitted", "in_review", self.editor.id), ("in_review", "accepted", self.editor.id),
            ("accepted", "published", self.owner.id)])
        self.assertEqual(self.state(), "published")
        # ds_status, which the v3 pages read, is untouched throughout.
        self.assertIsNone(Dataset.objects.get(pk=self.ds.pk).ds_status)

    def test_changes_requested_then_resubmitted(self):
        self.run_on_commit(lifecycle.submit, self.ds, self.owner)
        self.run_on_commit(lifecycle.start_review, self.ds, self.editor)
        self.run_on_commit(lifecycle.request_changes, self.ds, self.editor, "Add a licence note")
        self.assertEqual(self.state(), "changes_requested")
        self.run_on_commit(lifecycle.submit, self.ds, self.owner)
        self.run_on_commit(lifecycle.start_review, self.ds, self.editor)  # a second round
        from grace.models import Review
        self.assertEqual(Review.objects.filter(dataset__dataset=self.ds).count(), 2)


class TransitionTableTests(LifecycleBase):
    """Every transition from every state: allowed exactly where TRANSITIONS says, refused
    (409, nothing written) everywhere else, through one call path."""

    def call(self, name):
        from grace.models import Review
        from grace.vocabularies import ReviewType
        ds = self.ds
        if name in ("accept", "request_changes", "decline"):
            # exactly one open review, as start_review would have left it
            Review.objects.all().delete()
            Review.objects.create(dataset=lifecycle._track(ds), sent_on=datetime.date.today(),
                                  review_type=lifecycle._term(
                                      ReviewType, "pre-publication-qc", "Pre-publication QC", 15))
        if name == "start_review":
            Review.objects.all().delete()
        fn = {
            "upload": lambda: lifecycle.record_upload(ds, self.owner),
            "validate": lambda: lifecycle.begin_validation(ds),
            "fail_validation": lambda: lifecycle.validation_failed(ds, ["bad row"]),
            "pass_validation": lambda: lifecycle.validation_passed(ds),
            "start": lambda: lifecycle.start(ds, self.owner),
            "submit": lambda: lifecycle.submit(ds, self.owner),
            "start_review": lambda: lifecycle.start_review(ds, self.editor),
            "accept": lambda: lifecycle.accept(ds, self.editor),
            "request_changes": lambda: lifecycle.request_changes(ds, self.editor, "why"),
            "decline": lambda: lifecycle.decline(ds, self.editor, "why"),
            "publish": lambda: lifecycle.publish(ds, self.owner),
            "withdraw": lambda: lifecycle.withdraw(ds, self.owner),
        }[name]
        return self.run_on_commit(fn)

    def test_every_transition_from_every_state(self):
        allowed_count = refused_count = 0
        for name, (froms, to) in lifecycle.TRANSITIONS.items():
            for state in ALL_STATES:
                with self.subTest(transition=name, state=state):
                    self.set_state(state, public=(state in ("published",)))
                    before = DatasetLifecycleEvent.objects.count()
                    if state in froms:
                        self.call(name)
                        self.assertEqual(self.state(), to)
                        self.assertEqual(DatasetLifecycleEvent.objects.count(), before + 1)
                        allowed_count += 1
                    else:
                        with self.assertRaises(lifecycle.LifecycleRefused) as cm:
                            self.call(name)
                        self.assertEqual(cm.exception.status, 409)
                        self.assertEqual(self.state(), state)
                        self.assertEqual(DatasetLifecycleEvent.objects.count(), before)
                        refused_count += 1
        # Both arms ran, and as often as the table says they should.
        expected_allowed = sum(len(f) for f, _ in lifecycle.TRANSITIONS.values())
        self.assertEqual(allowed_count, expected_allowed)
        self.assertEqual(allowed_count + refused_count,
                         len(lifecycle.TRANSITIONS) * len(ALL_STATES))


class PermissionTests(LifecycleBase):
    def test_only_the_owner_submits(self):
        with self.assertRaises(lifecycle.LifecycleRefused) as cm:
            lifecycle.submit(self.ds, self.stranger)
        self.assertEqual((cm.exception.status, self.state()), (403, None))
        self.run_on_commit(lifecycle.submit, self.ds, self.owner)
        self.assertEqual(self.state(), "submitted")

    def test_only_an_editor_reviews(self):
        self.set_state("submitted")
        with self.assertRaises(lifecycle.LifecycleRefused) as cm:
            lifecycle.start_review(self.ds, self.owner)
        self.assertEqual((cm.exception.status, self.state()), (403, "submitted"))
        self.run_on_commit(lifecycle.start_review, self.ds, self.editor)
        self.assertEqual(self.state(), "in_review")

    def test_only_an_editor_concludes(self):
        self.set_state("submitted")
        self.run_on_commit(lifecycle.start_review, self.ds, self.editor)
        with self.assertRaises(lifecycle.LifecycleRefused) as cm:
            lifecycle.accept(self.ds, self.owner)
        self.assertEqual((cm.exception.status, self.state()), (403, "in_review"))
        self.run_on_commit(lifecycle.accept, self.ds, self.editor)
        self.assertEqual(self.state(), "accepted")

    def test_only_the_owner_publishes(self):
        self.set_state("accepted")
        with self.assertRaises(lifecycle.LifecycleRefused) as cm:
            lifecycle.publish(self.ds, self.stranger)
        self.assertEqual((cm.exception.status, self.state()), (403, "accepted"))
        self.run_on_commit(lifecycle.publish, self.ds, self.owner)
        self.assertEqual(self.state(), "published")


class RefusalRollsBackTests(LifecycleBase):
    def test_publish_without_a_licence_changes_nothing(self):
        self.set_state("accepted")
        Dataset.objects.filter(pk=self.ds.pk).update(license=None)
        with self.assertRaises(lifecycle.LifecycleRefused) as cm:
            self.run_on_commit(lifecycle.publish, self.ds, self.owner)
        self.assertEqual(cm.exception.status, 409)
        ds = Dataset.objects.get(pk=self.ds.pk)
        self.assertEqual((ds.lifecycle_state, ds.public), ("accepted", False))
        self.assertFalse(DatasetLifecycleEvent.objects.exists())
        self.m["index"].delay.assert_not_called()
        # the same call with a licence goes through
        Dataset.objects.filter(pk=self.ds.pk).update(license=self.licence)
        self.run_on_commit(lifecycle.publish, self.ds, self.owner)
        ds.refresh_from_db()
        self.assertEqual((ds.lifecycle_state, ds.public), ("published", True))
        self.m["index"].delay.assert_called_once_with(self.ds.pk)  # v3 pub indexing still runs

    def test_start_is_the_owners_explicit_opt_in_for_a_private_v3_dataset(self):
        self.set_state(None, public=True)
        with self.assertRaises(lifecycle.LifecycleRefused) as cm:
            lifecycle.start(self.ds, self.owner)
        self.assertEqual((cm.exception.status, self.state()), (409, None))
        self.set_state(None, public=False)
        with self.assertRaises(lifecycle.LifecycleRefused) as cm:
            lifecycle.start(self.ds, self.stranger)
        self.assertEqual((cm.exception.status, self.state()), (403, None))
        self.run_on_commit(lifecycle.start, self.ds, self.owner)
        self.assertEqual(self.state(), "draft")
        self.assertEqual(list(DatasetLifecycleEvent.objects.filter(dataset_pk=self.ds.pk)
                              .values_list("from_state", "to_state", "actor_id")),
                         [(None, "draft", self.owner.id)])

    def test_a_public_v3_dataset_cannot_be_submitted(self):
        self.set_state(None, public=True)
        with self.assertRaises(lifecycle.LifecycleRefused):
            lifecycle.submit(self.ds, self.owner)
        self.assertIsNone(self.state())
        self.set_state(None, public=False)
        self.run_on_commit(lifecycle.submit, self.ds, self.owner)
        self.assertEqual(self.state(), "submitted")

    def test_decline_and_changes_need_a_reason(self):
        self.set_state("submitted")
        self.run_on_commit(lifecycle.start_review, self.ds, self.editor)
        for fn in (lifecycle.decline, lifecycle.request_changes):
            with self.assertRaises(lifecycle.LifecycleRefused) as cm:
                fn(self.ds, self.editor, "  ")
            self.assertEqual(cm.exception.status, 400)
        self.assertEqual(self.state(), "in_review")
        self.run_on_commit(lifecycle.decline, self.ds, self.editor, "Out of scope")
        self.assertEqual(self.state(), "declined")

    def test_withdraw_refused_for_a_dataset_in_the_shared_index(self):
        self.set_state("published", public=True)
        with mock.patch.object(Dataset, "has_accessioned_places", new_callable=mock.PropertyMock,
                               return_value=True):
            with self.assertRaises(lifecycle.LifecycleRefused):
                lifecycle.withdraw(self.ds, self.owner)
        self.assertTrue(Dataset.objects.get(pk=self.ds.pk).public)
        self.run_on_commit(lifecycle.withdraw, self.ds, self.owner)
        self.assertFalse(Dataset.objects.get(pk=self.ds.pk).public)
