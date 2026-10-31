"""Two rulings of 2026-10-31: a declined dataset may be submitted again, and a dataset cannot
be submitted until both sensitivity questions are answered. Each refusal is paired, in the
same test, with the same call succeeding once the condition is met.

    GDAL_LIBRARY_PATH=... venv/bin/python manage.py test datasets.tests_lifecycle_resubmit \
        --settings=whg.settings_localtest
"""
import re

from django.urls import reverse

from datasets import lifecycle
from datasets.models import Dataset, DatasetLifecycleEvent
from datasets.templatetags.lifecycle_tags import panel_context
from datasets.tests_lifecycle_screens import ScreenBase
from datasets.views_lifecycle import allowed_actions


class ResubmissionTests(ScreenBase):
    def decline_it(self):
        self.run_on_commit(lifecycle.submit, self.ds, self.owner)
        self.run_on_commit(lifecycle.start_review, self.ds, self.editor)
        self.run_on_commit(lifecycle.decline, self.ds, self.editor, "Out of scope, for now")
        self.assertEqual(self.state(), "declined")

    def test_declined_can_be_submitted_again_and_goes_round_again(self):
        from grace.models import Review
        self.decline_it()
        self.assertTrue(allowed_actions(self.ds, self.owner)["submit"])
        # a stranger still cannot (control: the refusal is about who, not about the state)
        with self.assertRaises(lifecycle.LifecycleRefused) as cm:
            lifecycle.submit(self.ds, self.stranger)
        self.assertEqual(cm.exception.status, 403)
        self.run_on_commit(lifecycle.submit, self.ds, self.owner, "Revised")
        self.assertEqual(self.state(), "submitted")
        self.run_on_commit(lifecycle.start_review, self.ds, self.editor)
        self.run_on_commit(lifecycle.accept, self.ds, self.editor)
        self.assertEqual(self.state(), "accepted")
        # one GRACE record, a fresh review per round, each returned
        self.assertEqual(Review.objects.filter(dataset__dataset=self.ds).count(), 2)
        self.assertFalse(Review.objects.filter(dataset__dataset=self.ds,
                                               returned_on__isnull=True).exists())
        self.assertEqual(list(DatasetLifecycleEvent.objects.filter(dataset_pk=self.ds.pk)
                              .values_list("to_state", flat=True)),
                         ["submitted", "in_review", "declined", "submitted", "in_review",
                          "accepted"])

    def test_other_states_still_refuse_submit(self):
        for state in ("submitted", "in_review", "accepted", "uploaded", "validation_failed"):
            self.set_state(state)
            with self.assertRaises(lifecycle.LifecycleRefused) as cm:
                lifecycle.submit(self.ds, self.owner)
            self.assertEqual((cm.exception.status, self.state()), (409, state))
        self.set_state("declined")
        self.run_on_commit(lifecycle.submit, self.ds, self.owner)  # control
        self.assertEqual(self.state(), "submitted")

    def test_panel_wording_note_and_email(self):
        self.decline_it()
        html = self.status_page()
        p = self.panel(html)
        self.assertIn("Submit again for review", p)
        self.assertIn("Out of scope, for now", p)  # the editor's note stays visible
        self.assertIn("submit it for review again", p)
        self.set_state("draft")
        p = self.panel(self.status_page())
        self.assertIn("Submit for review", p)
        self.assertNotIn("Submit again for review", p)
        mail = (lifecycle.__file__.rsplit("/datasets/", 1)[0]
                + "/whgmail/templates/dataset_review_outcome.html")
        text = open(mail).read()
        self.assertIn("edit the dataset and submit it for review again", text)
        self.assertNotIn("upload a revised version", text)

    def test_json_can_for_declined(self):
        self.decline_it()
        self.client.force_login(self.owner)
        r = self.client.get(f"/datasets/{self.ds.pk}/lifecycle/")
        self.assertTrue(r.json()["can"]["submit"])
        self.set_state("in_review")
        r = self.client.get(f"/datasets/{self.ds.pk}/lifecycle/")
        self.assertFalse(r.json()["can"]["submit"])


class SensitivityRequiredTests(ScreenBase):
    def blank(self, **fields):
        Dataset.objects.filter(pk=self.ds.pk).update(
            **{"sensitivity_indigenous": None, "sensitivity_restricted": None,
               "sensitivity_note": "", **fields})
        self.ds.refresh_from_db()

    def test_submit_refused_until_both_answered_then_accepted(self):
        self.set_state("draft")
        self.client.force_login(self.owner)
        url = f"/datasets/{self.ds.pk}/lifecycle/submit/"
        cases = [  # (fields, must be refused)
            ({}, True),
            ({"sensitivity_indigenous": "no"}, True),
            ({"sensitivity_restricted": "no"}, True),
            ({"sensitivity_indigenous": "", "sensitivity_restricted": "no"}, True),
            ({"sensitivity_indigenous": "yes", "sensitivity_restricted": "no"}, True),  # no note
            ({"sensitivity_indigenous": "no", "sensitivity_restricted": "no"}, False),
            ({"sensitivity_indigenous": "unsure", "sensitivity_restricted": "no"}, False),
            ({"sensitivity_indigenous": "yes", "sensitivity_restricted": "no",
              "sensitivity_note": "Burial sites"}, False),
        ]
        for fields, refused in cases:
            with self.subTest(fields=fields):
                self.set_state("draft")
                self.blank(**fields)
                with self.captureOnCommitCallbacks(execute=True):
                    r = self.client.post(url, {}, content_type="application/json")
                if refused:
                    self.assertEqual(r.status_code, 409, r.content)
                    self.assertRegex(r.json()["error"], r"sensitiv|questions")
                    self.assertEqual(self.state(), "draft")
                    self.assertFalse(DatasetLifecycleEvent.objects.filter(
                        dataset_pk=self.ds.pk).exists())
                    self.assertFalse(self.m["mail"].called)
                else:
                    self.assertEqual(r.status_code, 200, r.content)
                    self.assertEqual(self.state(), "submitted")
                DatasetLifecycleEvent.objects.filter(dataset_pk=self.ds.pk).delete()

    def test_unanswered_is_not_no(self):
        self.set_state("draft")
        self.blank()
        with self.assertRaises(lifecycle.LifecycleRefused) as cm:
            lifecycle.submit(self.ds, self.owner)
        self.assertEqual(cm.exception.status, 409)
        self.blank(sensitivity_indigenous="no", sensitivity_restricted="no")
        self.run_on_commit(lifecycle.submit, self.ds, self.owner)
        self.assertEqual(self.state(), "submitted")

    def test_panel_explains_and_disables_until_answered(self):
        self.set_state("draft")
        self.blank()
        p = self.panel(self.status_page())
        self.assertRegex(p, r'data-testid="lifecycle-submit-button"[^>]*\bdisabled\b')
        self.assertIn('data-testid="lifecycle-submit-blocked"', p)
        self.assertIn("Answer the two questions", p)
        self.blank(sensitivity_indigenous="no", sensitivity_restricted="unsure")
        p = self.panel(self.status_page())
        self.assertNotRegex(p, r'data-testid="lifecycle-submit-button"[^>]*\bdisabled\b')
        self.assertNotIn("lifecycle-submit-blocked", p)
        self.assertIn('data-testid="lifecycle-submit-button"', p)

    def test_answers_through_the_form_endpoint_unblock_submit(self):
        self.set_state("draft")
        self.blank()
        self.client.force_login(self.owner)
        base = f"/datasets/{self.ds.pk}/lifecycle/"
        self.assertEqual(self.client.post(base + "submit/", {}, content_type="application/json")
                         .status_code, 409)
        r = self.client.post(base + "sensitivity/", {"sensitivity_indigenous": "no",
                                                      "sensitivity_restricted": "no"},
                             content_type="application/json")
        self.assertEqual(r.status_code, 200)
        with self.captureOnCommitCallbacks(execute=True):
            self.assertEqual(self.client.post(base + "submit/", {}, content_type="application/json")
                             .status_code, 200)

    def test_saved_answers_survive_resubmission(self):
        for state in ("changes_requested", "declined", "withdrawn"):
            with self.subTest(state=state):
                self.blank(sensitivity_indigenous="yes", sensitivity_restricted="no",
                           sensitivity_note="Sacred site")
                self.set_state(state)
                self.assertEqual(panel_context(self.ds, self.owner)["submit_blocked"], "")
                self.run_on_commit(lifecycle.submit, self.ds, self.owner)
                self.assertEqual(self.state(), "submitted")
                self.assertEqual(Dataset.objects.get(pk=self.ds.pk).sensitivity_note, "Sacred site")

    def test_checks_report_agrees(self):
        from datasets import qc
        self.blank()
        item = {c["key"]: c for c in qc.qc_report(self.ds, audience="owner")["checks"]}["sensitivity"]
        self.assertEqual(item["status"], "warn")
        self.assertIn("cannot be submitted", item["detail"])
