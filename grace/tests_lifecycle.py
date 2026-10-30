"""GRACE's side of the v4 dataset lifecycle (place#334 Appendix E, slices G0, G1, G3).

GRACE gains rows for human acts only: a TrackedDataset when a contributor submits, a Review
when an editor starts and concludes one. Everything else is read through. Each "nothing was
written" assertion is paired with the same path writing it under the right conditions.

    GDAL_LIBRARY_PATH=... venv/bin/python manage.py test grace.tests_lifecycle \
        --settings=whg.settings_localtest
"""
import datetime
import importlib

from django.apps import apps as django_apps
from django.contrib.auth import get_user_model
from django.test import TestCase

from datasets import lifecycle
from datasets.models import Dataset, DatasetLifecycleEvent
from datasets.tests_lifecycle import LifecycleBase

from .models import Person, Review, TrackedDataset, add_working_days, overdue_review_q

User = get_user_model()


class AutoTrackTests(LifecycleBase):
    def test_submission_tracks_the_upload_once_with_no_person_and_no_copied_count(self):
        Dataset.objects.filter(pk=self.ds.pk).update(numrows=321)
        self.assertFalse(TrackedDataset.objects.exists())
        people = Person.objects.count()
        self.run_on_commit(lifecycle.submit, self.ds, self.owner)
        tracked = TrackedDataset.objects.get()
        self.assertEqual((tracked.dataset_id, tracked.title), (self.ds.pk, "Lifecycle test"))
        self.assertIsNone(tracked.expected_record_count)  # read through, not "as told"
        self.assertEqual(Person.objects.count(), people)  # nothing for the Art. 14 queue
        self.assertEqual(tracked.whg_dataset_id, self.ds.pk)
        # A second round finds the same record.
        self.set_state("changes_requested")
        self.run_on_commit(lifecycle.submit, self.ds, self.owner)
        self.assertEqual(TrackedDataset.objects.count(), 1)

    def test_a_record_tracked_by_hand_earlier_is_found_and_linked(self):
        from . import tracking
        hand = TrackedDataset.objects.create(title="Prospect",
                                             notes=tracking.marker(self.ds.pk) + " by hand")
        self.assertIsNone(hand.dataset_id)
        self.run_on_commit(lifecycle.submit, self.ds, self.owner)
        hand.refresh_from_db()
        self.assertEqual(hand.dataset_id, self.ds.pk)
        self.assertEqual(TrackedDataset.objects.count(), 1)


class ReviewRowTests(LifecycleBase):
    def setUp(self):
        super().setUp()
        self.run_on_commit(lifecycle.submit, self.ds, self.owner)

    def test_start_review_writes_one_gate_review_with_an_internal_reviewer(self):
        self.run_on_commit(lifecycle.start_review, self.ds, self.editor)
        review = Review.objects.get()
        self.assertEqual(review.review_type.slug, "pre-publication-qc")
        self.assertEqual(review.sent_on, datetime.date.today())
        self.assertEqual(review.reviewer.user_id, self.editor.id)
        self.assertTrue(review.reviewer.role.is_internal)
        self.assertNotIn(review.reviewer, Person.objects.owed_privacy_notice())
        self.assertTrue(review.is_outstanding)

    def test_a_second_open_review_is_refused(self):
        tracked = TrackedDataset.objects.get()
        self.run_on_commit(lifecycle.start_review, self.ds, self.editor)  # control: it works
        Review.objects.filter(dataset=tracked).update(returned_on=None)
        self.set_state("submitted")  # e.g. a state edited by hand behind an open review
        with self.assertRaises(lifecycle.LifecycleRefused) as cm:
            lifecycle.start_review(self.ds, self.editor)
        self.assertEqual(cm.exception.status, 409)
        self.assertEqual(Review.objects.count(), 1)

    def test_outcome_is_recorded_and_shared_only_when_the_mail_went(self):
        self.run_on_commit(lifecycle.start_review, self.ds, self.editor)
        self.m["mail"].return_value = False  # the send fails
        self.run_on_commit(lifecycle.request_changes, self.ds, self.editor, "Fix the dates")
        review = Review.objects.get()
        self.assertEqual((review.returned_on, review.recommendation.slug, review.comments),
                         (datetime.date.today(), "changes-requested", "Fix the dates"))
        self.assertIsNone(review.shared_with_author_on)
        self.assertTrue(review.awaiting_share)  # GRACE's "author not told" alarm
        # The same path with the mail going out sets it.
        self.m["mail"].return_value = True
        self.run_on_commit(lifecycle.submit, self.ds, self.owner)
        self.run_on_commit(lifecycle.start_review, self.ds, self.editor)
        self.run_on_commit(lifecycle.accept, self.ds, self.editor)
        second = Review.objects.exclude(pk=review.pk).get()
        self.assertEqual((second.recommendation.slug, second.shared_with_author_on),
                         ("approved", datetime.date.today()))

    def test_a_refused_conclusion_leaves_the_review_open(self):
        self.run_on_commit(lifecycle.start_review, self.ds, self.editor)
        with self.assertRaises(lifecycle.LifecycleRefused):
            lifecycle.decline(self.ds, self.editor, "")
        self.assertTrue(Review.objects.get().is_outstanding)
        self.run_on_commit(lifecycle.decline, self.ds, self.editor, "Not a gazetteer")
        self.assertEqual(Review.objects.get().recommendation.slug, "rejected")

    def test_publish_keeps_the_tracked_record(self):
        self.run_on_commit(lifecycle.start_review, self.ds, self.editor)
        self.run_on_commit(lifecycle.accept, self.ds, self.editor)
        self.run_on_commit(lifecycle.publish, self.ds, self.owner)
        self.assertEqual(TrackedDataset.objects.get().dataset_id, self.ds.pk)


class TimelineTests(LifecycleBase):
    """One function, two renderings: the contributor's and GRACE's read-out."""

    def test_one_transition_shows_in_both_and_strangers_see_nothing(self):
        from .admin import TrackedDatasetAdmin
        from .admin_site import grace_admin_site
        self.run_on_commit(lifecycle.submit, self.ds, self.owner)
        admin = TrackedDatasetAdmin(TrackedDataset, grace_admin_site)
        tracked = TrackedDataset.objects.get()
        before_owner = lifecycle.timeline(self.ds, self.owner)
        before_grace = str(admin.whg_readout(tracked))
        self.assertNotIn("Review started", before_grace)
        self.run_on_commit(lifecycle.start_review, self.ds, self.editor)
        after_owner = lifecycle.timeline(self.ds, self.owner)
        after_grace = str(admin.whg_readout(tracked))
        self.assertEqual([s["state"] for s in after_owner], ["submitted", "in_review"])
        self.assertEqual(len(after_owner), len(before_owner) + 1)
        self.assertIn("Review started", after_grace)
        self.assertIn("Eda Editor", after_grace)
        self.assertEqual(lifecycle.timeline(self.ds, self.stranger), [])  # same dataset, a stranger
        self.assertIsNone(lifecycle.status_summary(self.ds, self.stranger))

    def test_the_readout_escapes_what_users_typed(self):
        from .admin import TrackedDatasetAdmin
        from .admin_site import grace_admin_site
        Dataset.objects.filter(pk=self.ds.pk).update(label="<b>x</b>")
        self.ds.refresh_from_db()
        self.run_on_commit(lifecycle.submit, self.ds, self.owner)
        html = str(TrackedDatasetAdmin(TrackedDataset, grace_admin_site)
                   .whg_readout(TrackedDataset.objects.get()))
        self.assertIn("&lt;b&gt;x&lt;/b&gt;", html)
        self.assertNotIn("<b>x</b>", html)

    def test_service_level_is_computed_from_the_submission(self):
        self.run_on_commit(lifecycle.submit, self.ds, self.owner)
        summary = lifecycle.status_summary(self.ds, self.owner)
        today = datetime.date.today()
        self.assertEqual(summary["submitted_on"], today)
        self.assertEqual(summary["usually_by"], add_working_days(today, 10))

    def test_a_deleted_upload_leaves_its_history(self):
        self.run_on_commit(lifecycle.submit, self.ds, self.owner)
        pk = self.ds.pk
        Dataset.objects.filter(pk=pk).delete()
        tracked = TrackedDataset.objects.get()
        self.assertIsNone(tracked.dataset_id)
        self.assertEqual(DatasetLifecycleEvent.objects.filter(dataset_pk=pk).count(), 1)
        from .admin import TrackedDatasetAdmin
        from .admin_site import grace_admin_site
        self.assertIn(f"Upload #{pk} has been deleted",
                      str(TrackedDatasetAdmin(TrackedDataset, grace_admin_site).whg_readout(tracked)))


class DueDateTests(TestCase):
    def test_working_days_skip_weekends(self):
        monday = datetime.date(2026, 11, 2)
        self.assertEqual(add_working_days(monday, 10), datetime.date(2026, 11, 16))
        friday = datetime.date(2026, 11, 6)
        self.assertEqual(add_working_days(friday, 1), datetime.date(2026, 11, 9))

    def test_overdue_filter_agrees_with_the_property(self):
        tracked = TrackedDataset.objects.create(title="T")
        today = datetime.date.today()
        reviews = [Review.objects.create(dataset=tracked, sent_on=today - datetime.timedelta(days=d))
                   for d in range(0, 40)]
        Review.objects.create(dataset=tracked, sent_on=today - datetime.timedelta(days=60),
                              returned_on=today)  # returned: never overdue
        by_property = {r.pk for r in Review.objects.all() if r.is_overdue}
        by_filter = set(Review.objects.filter(overdue_review_q()).values_list("pk", flat=True))
        self.assertTrue(by_property)                      # some are overdue ...
        self.assertTrue(len(by_property) < len(reviews))  # ... and some are not
        self.assertEqual(by_filter, by_property)


class MarkerMigrationTests(TestCase):
    """grace.0006 fills the new link from the two older ones."""

    def test_markers_and_register_ids_become_links(self):
        from unittest import mock
        from api.models import GazetteerRegistryEntry
        owner = User.objects.create_user(username="mm", email="mm@example.org", password="pw",
                                         given_name="M", surname="M")
        with mock.patch("datasets.signals.doi"), \
                mock.patch("api.signals.invalidate_and_rebuild_cache"):
            a, b, c = (Dataset.objects.create(owner=owner, label=f"mm{i}", title=f"mm{i}",
                                              description="d") for i in range(3))
        by_marker = TrackedDataset.objects.create(title="m", notes=f"[whg-dataset:{a.pk}] x")
        entry = GazetteerRegistryEntry.objects.create(id=f"whg:{b.pk}", name="b", namespace="whg",
                                                      entry_class="dataset", record_count=1)
        by_register = TrackedDataset.objects.create(title="r", registry=entry)
        dangling = TrackedDataset.objects.create(title="d", notes="[whg-dataset:999999]")
        prospect = TrackedDataset.objects.create(title="p")
        already = TrackedDataset.objects.create(title="c", dataset=c)
        second_for_c = TrackedDataset.objects.create(title="c2", notes=f"[whg-dataset:{c.pk}]")
        mig = importlib.import_module("grace.migrations.0006_trackeddataset_dataset")
        mig.link_uploads(django_apps, None)
        got = {t.pk: t.dataset_id for t in TrackedDataset.objects.all()}
        self.assertEqual(got[by_marker.pk], a.pk)
        self.assertEqual(got[by_register.pk], b.pk)
        self.assertIsNone(got[dangling.pk])
        self.assertIsNone(got[prospect.pk])
        self.assertEqual(got[already.pk], c.pk)
        self.assertIsNone(got[second_for_c.pk])  # one upload, one tracked record

    def test_review_terms_are_seeded_by_slug(self):
        from .vocabularies import ReviewRecommendation, ReviewType
        self.assertTrue(ReviewType.objects.filter(slug="pre-publication-qc").exists())
        self.assertEqual(set(ReviewRecommendation.objects.filter(
            slug__in=["approved", "changes-requested", "rejected"]).values_list("slug", flat=True)),
            {"approved", "changes-requested", "rejected"})
