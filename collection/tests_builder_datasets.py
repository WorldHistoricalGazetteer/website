"""The "add places from a dataset" dropdown in the place-collection builders.

It lists the datasets the user owns or co-owns (superusers: all). It used to also slip one private
WHG-owned "sample" dataset (label owt10 / owt10_foo) into every ordinary user's list, which
add_dataset_places then refused with a 404 (a private dataset cannot be pulled into a collection),
and a missing sample was a 500 on the whole builder page. The explanation now lives in a tooltip.
"""
from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from collection.models import Collection
from datasets.models import Dataset, DatasetUser

User = get_user_model()


class BuilderDatasetDropdownTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        def user(name, **kw):
            return User.objects.create_user(
                username=name, email=f'{name}@example.org', password='pw',
                given_name=name.title(), surname='Test', **kw)
        cls.user = user('builder')
        cls.other = user('someone_else')
        cls.admin = user('builder_admin', is_superuser=True, is_staff=True)
        cls.coll = Collection.objects.create(owner=cls.user, title='Builder collection',
                                             collection_class='place', description='D')

        def ds(label, owner, public=False):
            return Dataset.objects.create(owner=owner, label=label, title=f'T {label}',
                                          description='D', public=public)
        cls.mine = ds('b_mine', cls.user)
        cls.co_owned = ds('b_coowned', cls.other)
        DatasetUser.objects.create(dataset_id=cls.co_owned, user_id=cls.user, role='owner')
        cls.member_only = ds('b_member', cls.other)
        DatasetUser.objects.create(dataset_id=cls.member_only, user_id=cls.user, role='member')
        cls.theirs = ds('b_theirs', cls.other, public=True)
        # the old injected samples: private, WHG-owned
        cls.sample_a = ds('owt10', cls.admin)
        cls.sample_b = ds('owt10_foo', cls.admin)

    def pages(self):
        return {'create': reverse('collection:place-collection-create'),
                'update': reverse('collection:place-collection-update', args=[self.coll.id])}

    def labels(self, url, user):
        self.client.force_login(user)
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200, url)
        return response, [d.label for d in response.context['ds_select']]

    def test_lists_only_owned_and_co_owned_datasets(self):
        for name, url in self.pages().items():
            with self.subTest(page=name):
                _, labels = self.labels(url, self.user)
                self.assertEqual(sorted(labels), ['b_coowned', 'b_mine'])

    def test_no_private_sample_dataset_is_offered(self):
        for name, url in self.pages().items():
            with self.subTest(page=name):
                _, labels = self.labels(url, self.user)
                self.assertFalse({'owt10', 'owt10_foo'} & set(labels))

    def test_every_offered_dataset_can_actually_be_added(self):
        """What the dropdown lists must be accepted by add_dataset_places (no 404 surprises)."""
        self.client.force_login(self.user)
        _, labels = self.labels(self.pages()['update'], self.user)
        for label in labels:
            with self.subTest(dataset=label):
                ds = Dataset.objects.get(label=label)
                response = self.client.get(reverse('collection:add-dsplaces',
                                                   kwargs={'coll_id': self.coll.id, 'ds_id': ds.id}),
                                           HTTP_REFERER='/')
                self.assertEqual(response.status_code, 302)

    def test_superuser_sees_every_dataset(self):
        _, labels = self.labels(self.pages()['create'], self.admin)
        self.assertEqual(len(labels), Dataset.objects.count())

    def test_dropdown_is_one_query_not_one_per_dataset(self):
        self.client.force_login(self.user)
        from django.db import connection
        from django.test.utils import CaptureQueriesContext
        from collection.views import datasets_for_place_collection_builder
        with CaptureQueriesContext(connection) as ctx:
            datasets_for_place_collection_builder(self.user)
        self.assertEqual(len(ctx.captured_queries), 1)

    def test_explanation_is_a_tooltip_and_an_empty_state_points_to_the_dashboard(self):
        response, labels = self.labels(self.pages()['update'], self.user)
        self.assertContains(response, 'data-bs-toggle="tooltip"')
        self.assertContains(response, 'Lists the datasets you own or collaborate on')
        self.assertNotContains(response, 'id="no_datasets"')   # this user has datasets

        nobody = User.objects.create_user(username='nobody', email='nobody@example.org', password='pw',
                                          given_name='No', surname='Body')
        coll = Collection.objects.create(owner=nobody, title='Empty', collection_class='place',
                                         description='D')
        self.client.force_login(nobody)
        response = self.client.get(reverse('collection:place-collection-update', args=[coll.id]))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'id="no_datasets"')
        self.assertContains(response, reverse('dashboard'))
