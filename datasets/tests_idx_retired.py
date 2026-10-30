"""place#342: the WHG-index (idx) matching target is retired; ds_recon refuses it server-side."""
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase

from datasets.models import Dataset


class IdxRefusedTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(
            username="idx342", email="idx342@example.org", password="pw", given_name="I", surname="X")
        with patch("datasets.signals.doi"):
            self.ds = Dataset.objects.create(owner=self.user, label="idx342", title="T", description="D",
                                             creator="X", public=True, ds_status="uploaded")
        self.client.force_login(self.user)

    def test_idx_refused_and_no_task_started(self):
        with patch("datasets.views.align_idx") as idx:
            r = self.client.post(f"/datasets/{self.ds.id}/recon/", {"recon": "idx", "test": "on"})
        self.assertEqual(r.status_code, 302)
        self.assertTrue(r.url.endswith("/addtask"))
        idx.delay.assert_not_called()

    def test_wdlocal_still_dispatched(self):
        with patch("datasets.views.align_wdlocal") as wd:
            wd.delay.return_value.id = "x"
            self.client.post(f"/datasets/{self.ds.id}/recon/", {"recon": "wdlocal"})
        wd.delay.assert_called_once()
