"""Mail for the lifecycle transitions (place#334 Phase 3). WHGmail is patched throughout
(``patch_side_effects``); the templates are rendered for real but never sent.

    GDAL_LIBRARY_PATH=... venv/bin/python manage.py test datasets.tests_lifecycle_mail \
        --settings=whg.settings_localtest
"""
import re

from django.conf import settings
from django.template.loader import render_to_string

from datasets import lifecycle
from datasets.models import Dataset
from datasets.tests_lifecycle import LifecycleBase

FORBIDDEN = re.compile(r"attest|accession|reconcil", re.I)  # never on screen (Appendix B)


class LifecycleMailTests(LifecycleBase):
    def sent(self):
        return [c.kwargs["context"] for c in self.m["mail"].call_args_list]

    def test_submission_mails_the_owner_with_an_editorial_copy_after_commit(self):
        lifecycle.submit(self.ds, self.owner)  # no commit inside a TestCase: nothing sent
        self.assertEqual(self.sent(), [])
        Dataset.objects.filter(pk=self.ds.pk).update(lifecycle_state=None)
        self.run_on_commit(lifecycle.submit, self.ds, self.owner)
        (ctx,) = self.sent()
        self.assertEqual((ctx["template"], ctx["to_email"], ctx["bcc"]),
                         ("dataset_submitted", self.owner.email, [settings.DEFAULT_FROM_EDITORIAL]))
        self.assertEqual(ctx["sla_days"], 10)

    def test_a_refused_transition_sends_nothing(self):
        with self.assertRaises(lifecycle.LifecycleRefused):
            self.run_on_commit(lifecycle.submit, self.ds, self.stranger)
        self.assertEqual(self.sent(), [])
        self.run_on_commit(lifecycle.submit, self.ds, self.owner)  # control
        self.assertEqual(len(self.sent()), 1)

    def test_each_outcome_mails_the_owner_with_the_reason(self):
        for outcome, comments in (("request_changes", "Fix the dates"),
                                  ("decline", "Out of scope"), ("accept", "")):
            with self.subTest(outcome=outcome):
                self.set_state("submitted")
                self.run_on_commit(lifecycle.start_review, self.ds, self.editor)
                self.m["mail"].reset_mock()
                self.run_on_commit(lifecycle.conclude_review, self.ds, self.editor, outcome, comments)
                (ctx,) = self.sent()
                self.assertEqual((ctx["template"], ctx["outcome"], ctx["comments"], ctx["to_email"]),
                                 ("dataset_review_outcome", outcome, comments, self.owner.email))

    def test_a_mail_failure_does_not_undo_the_transition(self):
        self.m["mail"].side_effect = RuntimeError("smtp down")
        self.run_on_commit(lifecycle.submit, self.ds, self.owner)
        self.assertEqual(self.state(), "submitted")


class TemplateTests(LifecycleBase):
    def render(self, template, **extra):
        ctx = {"greeting_name": "Olive", "dataset_title": "Clergy places", "dataset_label": "cp",
               "dataset_id": 7, "reply_to": "editor@example.org", "sla_days": 10,
               "usually_by": "16 November 2026", **extra}
        return render_to_string(f"{template}.html", ctx)

    def test_templates_render_and_keep_to_the_vocabulary(self):
        pages = [self.render("dataset_submitted")] + [
            self.render("dataset_review_outcome", outcome=o, comments="Because <b>reasons</b>")
            for o in ("accept", "request_changes", "decline")]
        for html in pages:
            self.assertIn("Clergy places", html)  # it rendered the dataset
            self.assertIsNone(FORBIDDEN.search(re.sub(r"<style.*?</style>", "", html, flags=re.S)))
        self.assertIn("10 working days", pages[0])
        self.assertIn("Because &lt;b&gt;reasons&lt;/b&gt;", pages[2])  # escaped
        self.assertIn("<b>accepted</b>", pages[1])
