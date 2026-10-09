"""place#322 — an accessioned dataset stays public, and accession needs a public dataset.

* ``refuse_private_if_accessioned`` (used by ``DatasetDetailModelForm`` and the admin's
  ``DatasetAdminForm``) refuses public -> private once any place is in the ``whg`` index;
  a dataset with no accessioned places, and a private -> public change, pass.
* ``review`` refuses a POST on an ``align_idx`` task of a non-public dataset before any
  decision is processed; GET, a public dataset and a non-idx task are the controls.
* the beat schedule rebuilds the toponym table daily.

Run locally (never in the prod container):

    venv/bin/python manage.py test datasets.tests_accession_guard --settings=whg.settings_localtest
"""

from unittest import mock

from django import forms
from django.contrib.auth import get_user_model
from django.test import TestCase

from datasets.admin import DatasetAdmin
from datasets.forms import (
    ACCESSIONED_PRIVATE_MESSAGE, DatasetAdminForm, DatasetDetailModelForm, refuse_private_if_accessioned,
)
from datasets.models import Dataset
from datasets.tests_private_access import OWNER_ID, _Reached, _WriteGateBase, make_user
from places.models import Place


class RefusePrivateTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        owner = get_user_model().objects.create_user(username="o322", email="o322@example.org", password="pw",
                                                       given_name="O", surname="Wner")
        cls.accessioned = Dataset.objects.create(owner=owner, label="g322_acc", title="A", public=True,
                                                 description="D", ds_status="indexed")
        Place.objects.create(title="Here", src_id="1", dataset=cls.accessioned, indexed=True, ccodes=[])
        Place.objects.create(title="There", src_id="2", dataset=cls.accessioned, indexed=False, ccodes=[])
        cls.unaccessioned = Dataset.objects.create(owner=owner, label="g322_not", title="N", public=True,
                                                   description="D", ds_status="reconciling")
        Place.objects.create(title="Elsewhere", src_id="1", dataset=cls.unaccessioned, indexed=False, ccodes=[])

    def test_accessioned_public_to_private_refused(self):
        self.assertTrue(self.accessioned.has_accessioned_places)
        with self.assertRaisesMessage(forms.ValidationError, ACCESSIONED_PRIVATE_MESSAGE):
            refuse_private_if_accessioned(self.accessioned, False)

    def test_unaccessioned_may_go_private(self):
        self.assertFalse(self.unaccessioned.has_accessioned_places)
        self.assertIs(refuse_private_if_accessioned(self.unaccessioned, False), False)

    def test_staying_public_and_going_public_pass(self):
        self.assertIs(refuse_private_if_accessioned(self.accessioned, True), True)
        self.accessioned.public = False  # already private (legacy rows): re-saving private passes
        self.assertIs(refuse_private_if_accessioned(self.accessioned, False), False)
        self.assertIs(refuse_private_if_accessioned(self.accessioned, True), True)

    def test_both_forms_apply_the_rule(self):
        for form_class in (DatasetDetailModelForm, DatasetAdminForm):
            with self.subTest(form=form_class.__name__):
                form = form_class(instance=self.accessioned)
                form.cleaned_data = {"public": False}
                with self.assertRaises(forms.ValidationError):
                    form.clean_public()
                form.cleaned_data = {"public": True}
                self.assertIs(form.clean_public(), True)

    def test_admin_uses_the_guarded_form(self):
        self.assertIs(DatasetAdmin.form, DatasetAdminForm)


class ReviewAccessionGateTests(_WriteGateBase):
    def _call(self, ds, task_name, method):
        from datasets import views
        task = mock.Mock(task_name=task_name)
        r = self.req(method, f"/datasets/1/review/{self.TID}/pass1", make_user(OWNER_ID))
        with self.with_ds(ds), \
                mock.patch("datasets.views._get_task_details",
                           return_value=(task, "idx", "WHG", {}, "off")), \
                mock.patch("datasets.views._filter_unreviewed_places", side_effect=_Reached), \
                mock.patch("datasets.views.messages") as msgs:
            try:
                return views.review(r, dsid=1, tid=self.TID, passnum="pass1"), msgs
            except _Reached:
                return "reached", msgs

    def test_post_on_private_idx_task_refused(self):
        resp, msgs = self._call(self.dataset(public=False), "align_idx", "post")
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(resp.url, f"/datasets/1/review/{self.TID}/pass1")
        msgs.error.assert_called_once()

    def test_controls_reach_the_review(self):
        cases = [
            ("public idx POST", True, "align_idx", "post"),
            ("private idx GET", False, "align_idx", "get"),
            ("private wdlocal POST", False, "align_wdlocal", "post"),
        ]
        for label, public, task_name, method in cases:
            with self.subTest(label):
                resp, msgs = self._call(self.dataset(public=public), task_name, method)
                self.assertEqual(resp, "reached")
                msgs.error.assert_not_called()


class ToponymScheduleTests(TestCase):
    def test_populate_toponyms_is_scheduled(self):
        from whg.celery import app
        import sitemap.tasks
        tasks = {e["task"] for e in app.conf.beat_schedule.values()}
        self.assertIn(sitemap.tasks.populate_toponyms.name, tasks)
