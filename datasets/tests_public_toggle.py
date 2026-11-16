"""place#344 — the v3 metadata form must not let a non-editor make a dataset public (or reassign
its owner / featured rank), and must not publish a dataset that is in the v4 review process.

Each refusal is paired with the same input accepted from someone allowed (so the test can fail).

    venv/bin/python manage.py test datasets.tests_public_toggle --settings=whg.settings_localtest
"""
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.test import TestCase

from datasets.forms import DatasetDetailModelForm
from datasets.models import Dataset


def _post(ds, **over):
    data = {'owner': ds.owner_id, 'title': ds.title, 'description': 'D', 'label': ds.label, 'id': ds.pk,
            'datatype': 'place', 'format': 'delimited', 'uri_base': '', 'public': 'on', 'featured': ''}
    data.update(over)
    return data


class PublicToggleTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        U = get_user_model()
        mk = lambda n: U.objects.create_user(username=n, email=f'{n}@example.org', password='pw',
                                             given_name=n, surname='T')
        cls.owner, cls.editor, cls.staff, cls.other = mk('o344'), mk('e344'), mk('s344'), mk('x344')
        cls.editor.groups.add(Group.objects.get_or_create(name='editorial')[0])
        cls.staff.is_staff = True
        cls.staff.save()
        cls.legacy = Dataset.objects.create(owner=cls.owner, label='g344_legacy', title='L', description='D',
                                            public=False)
        cls.draft = Dataset.objects.create(owner=cls.owner, label='g344_draft', title='D', description='D',
                                           public=False, lifecycle_state='draft')

    def form(self, ds, user, **over):
        ds = Dataset.objects.get(pk=ds.pk)       # a fresh instance: is_valid() writes the form onto it
        f = DatasetDetailModelForm(_post(ds, **over), instance=ds, user=user)
        f.is_valid()
        return f

    def test_owner_cannot_set_public_but_an_editor_can(self):
        self.assertIs(self.form(self.legacy, self.editor).cleaned_data['public'], True)   # control: accepted
        self.assertIs(self.form(self.legacy, self.owner).cleaned_data['public'], False)   # refused: unchanged

    def test_no_user_means_locked(self):
        f = DatasetDetailModelForm(_post(self.legacy), instance=self.legacy)
        f.is_valid()
        self.assertIs(f.cleaned_data['public'], False)

    def test_owner_and_featured_cannot_be_changed_by_a_non_staff_user(self):
        over = {'owner': self.other.pk, 'featured': '3'}
        allowed = self.form(self.legacy, self.staff, **over).cleaned_data                  # control
        self.assertEqual(allowed['owner'], self.other)
        self.assertEqual(allowed['featured'], 3)
        refused = self.form(self.legacy, self.editor, **over).cleaned_data
        self.assertEqual(refused['owner'], self.owner)
        self.assertNotEqual(refused.get('featured'), 3)

    def test_a_dataset_in_review_cannot_be_published_through_this_form_even_by_an_editor(self):
        ok = self.form(self.legacy, self.editor)                                           # control: same input, no lifecycle
        self.assertNotIn('public', ok.errors)
        refused = self.form(self.draft, self.editor)
        self.assertIn('public', refused.errors)
        self.assertIn('review process', refused.errors['public'][0])

    def test_a_published_lifecycle_dataset_may_be_made_public_again(self):
        ds = Dataset.objects.create(owner=self.owner, label='g344_pub', title='P', description='D', public=False,
                                    lifecycle_state='published')
        self.assertNotIn('public', self.form(ds, self.editor).errors)
