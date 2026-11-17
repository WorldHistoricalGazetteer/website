"""The place-collection builders add one WHG-owned sample dataset to every non-superuser's
"add places from a dataset" dropdown. A missing sample dataset used to raise Dataset.DoesNotExist
and take the whole builder page down with a 500."""
from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from collection.models import Collection
from collection.views import SAMPLE_DATASET_LABEL_CREATE, SAMPLE_DATASET_LABEL_UPDATE
from datasets.models import Dataset

User = get_user_model()


class SampleDatasetInBuilderTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.user = User.objects.create_user(
            username='builder', email='builder@example.org', password='pw',
            given_name='Bea', surname='Builder')
        cls.admin = User.objects.create_user(
            username='builder_admin', email='builder_admin@example.org', password='pw',
            given_name='Ada', surname='Admin', is_superuser=True, is_staff=True)
        cls.own = Dataset.objects.create(owner=cls.user, label='builder_own', title='Mine',
                                         description='D')
        cls.coll = Collection.objects.create(owner=cls.user, title='Builder collection',
                                             collection_class='place', description='D')

    def _pages(self):
        return {
            'create': (reverse('collection:place-collection-create'), SAMPLE_DATASET_LABEL_CREATE),
            'update': (reverse('collection:place-collection-update', args=[self.coll.id]),
                       SAMPLE_DATASET_LABEL_UPDATE),
        }

    def _ds_labels(self, url, user):
        self.client.force_login(user)
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200, url)
        return [d.label for d in response.context['ds_select']]

    def test_pages_load_when_the_sample_dataset_is_missing(self):
        for name, (url, label) in self._pages().items():
            with self.subTest(page=name):
                self.assertFalse(Dataset.objects.filter(label=label).exists())
                labels = self._ds_labels(url, self.user)
                self.assertEqual(labels, ['builder_own'])

    def test_sample_dataset_is_offered_to_non_superusers_when_present(self):
        for name, (url, label) in self._pages().items():
            with self.subTest(page=name):
                sample = Dataset.objects.create(owner=self.admin, label=label,
                                                title='Sample', description='D')
                try:
                    self.assertIn(label, self._ds_labels(url, self.user))
                finally:
                    sample.delete()

    def test_superusers_are_not_given_the_sample_twice(self):
        for name, (url, label) in self._pages().items():
            with self.subTest(page=name):
                sample = Dataset.objects.create(owner=self.admin, label=label,
                                                title='Sample', description='D')
                try:
                    labels = self._ds_labels(url, self.admin)
                    self.assertEqual(labels.count(label), 1)
                finally:
                    sample.delete()
