"""The lifecycle's JSON endpoints (place#334 Phase 3): who reaches them and what they say.
Each refusal is paired with the same request succeeding for the right caller.

    GDAL_LIBRARY_PATH=... venv/bin/python manage.py test datasets.tests_lifecycle_views \
        --settings=whg.settings_localtest
"""
from django.contrib.auth import get_user_model

from datasets.models import Dataset
from datasets.tests_lifecycle import LifecycleBase

User = get_user_model()


class LifecycleViewTests(LifecycleBase):
    def setUp(self):
        super().setUp()
        User.objects.filter(pk=self.owner.pk).update(role="beta_tester")
        self.owner.refresh_from_db()

    def post(self, path, body=None):
        with self.captureOnCommitCallbacks(execute=True):
            return self.client.post(f"/datasets/{self.ds.pk}/lifecycle/{path}", body or {},
                                    content_type="application/json")

    def test_anonymous_is_401(self):
        self.assertEqual(self.client.get(f"/datasets/{self.ds.pk}/lifecycle/").status_code, 401)
        self.client.force_login(self.owner)
        self.assertEqual(self.client.get(f"/datasets/{self.ds.pk}/lifecycle/").status_code, 200)

    def test_beta_gate(self):
        User.objects.filter(pk=self.owner.pk).update(role="normal")
        self.client.force_login(User.objects.get(pk=self.owner.pk))
        self.assertEqual(self.post("submit/").status_code, 404)
        self.assertIsNone(Dataset.objects.get(pk=self.ds.pk).lifecycle_state)
        User.objects.filter(pk=self.owner.pk).update(role="beta_tester")
        self.client.force_login(User.objects.get(pk=self.owner.pk))
        r = self.post("submit/")
        self.assertEqual(r.status_code, 200, r.content)
        self.assertEqual(r.json()["summary"]["state"], "submitted")

    def test_a_beta_stranger_gets_404(self):
        User.objects.filter(pk=self.stranger.pk).update(role="beta_tester")
        self.client.force_login(User.objects.get(pk=self.stranger.pk))
        self.assertEqual(self.client.get(f"/datasets/{self.ds.pk}/lifecycle/").status_code, 404)
        self.assertEqual(self.post("submit/").status_code, 404)

    def test_owner_and_editor_through_to_publication(self):
        self.client.force_login(self.owner)
        self.assertEqual(self.post("publish/").status_code, 409)  # not accepted yet
        self.assertEqual(self.post("submit/").status_code, 200)
        self.client.force_login(self.editor)
        self.assertEqual(self.post("review/start/").status_code, 200)
        self.assertEqual(self.post("review/conclude/", {"outcome": "decline"}).status_code, 400)
        r = self.post("review/conclude/", {"outcome": "accept"})
        self.assertEqual(r.json()["summary"]["state"], "accepted")
        self.assertEqual(self.post("publish/").status_code, 200)  # staff manage every dataset
        # (that publish went through: undo it so the owner's toggle can be shown)
        self.client.force_login(self.owner)
        r = self.post("withdraw/")
        self.assertEqual(r.status_code, 200)
        # withdrawn: back through review, never straight to published (2026-10-30)
        self.assertEqual((r.json()["can"]["publish"], r.json()["can"]["submit"]), (False, True))
        self.assertEqual(self.post("publish/").status_code, 409)
        self.assertFalse(Dataset.objects.get(pk=self.ds.pk).public)
        self.assertEqual(self.post("submit/").status_code, 200)
        self.client.force_login(self.editor)
        self.post("review/start/")
        self.post("review/conclude/", {"outcome": "accept"})
        self.client.force_login(self.owner)
        r = self.post("publish/")
        self.assertEqual(r.status_code, 200)
        self.assertTrue(Dataset.objects.get(pk=self.ds.pk).public)
        self.assertEqual([s["state"] for s in r.json()["timeline"]][-1], "published")

    def test_start_moves_a_private_v3_dataset_to_draft(self):
        self.client.force_login(self.owner)
        self.assertTrue(self.client.get(f"/datasets/{self.ds.pk}/lifecycle/").json()["can"]["start"])
        self.assertEqual(self.post("start/").status_code, 200)
        self.assertEqual(Dataset.objects.get(pk=self.ds.pk).lifecycle_state, "draft")
        self.assertEqual(self.post("start/").status_code, 409)  # only from NULL

    def test_link_counts_reach_the_owner_and_not_the_editor(self):
        self.client.force_login(self.owner)
        mine = self.client.get(f"/datasets/{self.ds.pk}/lifecycle/").json()
        self.assertIn("links", mine["checks"])
        self.client.force_login(self.editor)
        theirs = self.client.get(f"/datasets/{self.ds.pk}/lifecycle/").json()
        self.assertNotIn("links", theirs["checks"])
        self.assertEqual(len(theirs["checks"]["checks"]), len(mine["checks"]["checks"]))

    def test_sensitivity_saved_only_when_valid(self):
        # the base dataset is answered "no"/"no" so it can be submitted; start from unanswered
        Dataset.objects.filter(pk=self.ds.pk).update(sensitivity_indigenous=None,
                                                     sensitivity_restricted=None)
        self.client.force_login(self.owner)
        r = self.post("sensitivity/", {"sensitivity_indigenous": "yes",
                                        "sensitivity_restricted": "no"})
        self.assertEqual(r.status_code, 400)
        self.assertIsNone(Dataset.objects.get(pk=self.ds.pk).sensitivity_indigenous)
        r = self.post("sensitivity/", {"sensitivity_indigenous": "yes",
                                        "sensitivity_restricted": "no",
                                        "sensitivity_note": "Burial sites"})
        self.assertEqual(r.status_code, 200)
        ds = Dataset.objects.get(pk=self.ds.pk)
        self.assertEqual((ds.sensitivity_indigenous, ds.sensitivity_note), ("yes", "Burial sites"))
