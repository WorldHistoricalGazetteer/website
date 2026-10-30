"""The indexing side reads /reconcile/authority-datasets to decide what to ingest (place#334).

A dataset published under the v4 lifecycle must be listed and marked ``published`` even though it
is not an authority; a v3-era dataset (lifecycle_state NULL) keeps the old rule; a v4 dataset in
any other state is never ``published``.
"""
from datasets.models import Dataset
from datasets.tests_lifecycle import LifecycleBase


class AuthorityDatasetsLifecycleTests(LifecycleBase):
    def listing(self, **params):
        self.client.force_login(self.editor)
        r = self.client.get("/reconcile/authority-datasets", params)
        self.assertEqual(r.status_code, 200, r.content[:200])
        return {d["id"]: d for d in r.json()["result"]}

    def test_published_v4_dataset_is_listed_and_published_without_authority(self):
        self.set_state("published", public=True)
        self.assertFalse(Dataset.objects.get(pk=self.ds.pk).authority)
        entry = self.listing()[self.ds.pk]
        self.assertEqual((entry["dataset_status"], entry["lifecycle_state"]), ("published", "published"))

    def test_other_v4_states_and_legacy_rule(self):
        for state in ("submitted", "accepted", "withdrawn"):
            self.set_state(state, public=(state == "accepted"))
            self.assertNotIn(self.ds.pk, self.listing(), state)          # not authority: absent
            entry = self.listing(include_pending=True).get(self.ds.pk)   # but never "published"
            self.assertEqual(entry["dataset_status"], "pending", state)
        # legacy: lifecycle NULL, authority + public + accessioning = published as before
        Dataset.objects.filter(pk=self.ds.pk).update(lifecycle_state=None, authority=True,
                                                     public=True, ds_status="accessioning")
        entry = self.listing()[self.ds.pk]
        self.assertEqual((entry["dataset_status"], entry["lifecycle_state"]), ("published", None))
