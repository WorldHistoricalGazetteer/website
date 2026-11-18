"""The admin-dashboard list views (/main/<name>_list/<sort>/<order>/) show each user their
OWN objects only; whg_admins see everything (security fix 2026-11-18).

Before the fix the four views had no login check and collection/area/group lists started
from ``.objects.all()``, so an anonymous request listed every sandbox collection.

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 venv/bin/python manage.py test \
        main.tests_list_visibility --settings=whg.settings_localtest

Every absence asserted here is paired with a presence in the same response (the user's own
object, or an admin seeing the other user's), so a list that rendered nothing, or a page that
never reached the view, fails its pair instead of passing.
"""
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.test import TestCase
from django.urls import reverse

from areas.models import Area
from collection.models import Collection, CollectionGroup, CollectionGroupUser, CollectionUser
from datasets.models import Dataset, DatasetUser
from places.models import Place

User = get_user_model()

LISTS = {
    # name: (url name, default sort, order, context key)
    'datasets': ('main:dataset-list', 'last_modified', 'desc', 'datasets'),
    'collections': ('main:collection-list', 'create_date', 'desc', 'collections'),
    'areas': ('main:area-list', 'created', 'desc', 'areas'),
    'groups': ('main:group-list', 'title', 'asc', 'groups'),
}

SQUARE = {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]}


class ListVisibilityBase(TestCase):
    @classmethod
    def setUpTestData(cls):
        def user(name, **kw):
            return User.objects.create_user(
                username=name, email=f'{name}@example.org', password='pw',
                given_name=name.title(), surname='Test', **kw)

        cls.a = user('lv_alice')
        cls.b = user('lv_bob')
        cls.admin = user('lv_admin')
        cls.admin.groups.add(Group.objects.get_or_create(name='whg_admins')[0])

        def coll(title, owner, status='sandbox'):
            return Collection.objects.create(owner=owner, title=title, collection_class='place',
                                             description='D', status=status)

        cls.c_a = coll('LVCOLL-alice-own', cls.a)
        cls.c_b = coll('LVCOLL-bob-private', cls.b)
        cls.c_b_collab = coll('LVCOLL-bob-shared-with-alice', cls.b)
        CollectionUser.objects.create(collection=cls.c_b_collab, user=cls.a, role='member')

        def ds(label, owner):
            d = Dataset.objects.create(owner=owner, label=label, title=f'LVDS-{label}',
                                       description='D', public=False, numrows=1)
            # dataset_list.html only renders a row when the dataset has places
            Place.objects.create(title=f'P {label}', src_id=f'{label}-1', dataset=d, ccodes=[])
            return d

        cls.d_a = ds('lv_alice_own', cls.a)
        cls.d_b = ds('lv_bob_private', cls.b)
        cls.d_b_collab = ds('lv_bob_shared', cls.b)
        DatasetUser.objects.create(dataset_id=cls.d_b_collab, user_id=cls.a, role='member')

        def area(title, owner):
            return Area.objects.create(owner=owner, title=title, type='drawn', description='D',
                                       geojson=SQUARE)

        cls.ar_a = area('LVAREA-alice-own', cls.a)
        cls.ar_b = area('LVAREA-bob-private', cls.b)

        def group(title, owner):
            return CollectionGroup.objects.create(owner=owner, title=title, description='D',
                                                  type='class')

        cls.g_a = group('LVGROUP-alice-leads', cls.a)
        cls.g_b = group('LVGROUP-bob-private', cls.b)
        cls.g_b_member = group('LVGROUP-bob-alice-member', cls.b)
        CollectionGroupUser.objects.create(collectiongroup=cls.g_b_member, user=cls.a,
                                           role='member')

    def get_list(self, name, user=None, **params):
        url_name, sort, order, _ = LISTS[name]
        if user is not None:
            self.client.force_login(user)
        return self.client.get(reverse(url_name, args=[sort, order]), params)

    def listed(self, name, user, **params):
        """(set of pks in the rendered context list, HTML) for ``user``."""
        r = self.get_list(name, user, **params)
        self.assertEqual(r.status_code, 200)
        key = LISTS[name][3]
        return {o.pk for o in r.context[key]}, r.content.decode()


class AnonymousTests(ListVisibilityBase):
    def test_anonymous_redirected_to_login_for_all_four_lists(self):
        for name in LISTS:
            with self.subTest(name=name):
                r = self.get_list(name)
                self.assertEqual(r.status_code, 302)
                self.assertTrue(r['Location'].startswith(settings.LOGIN_URL), r['Location'])
                self.assertNotIn('LV', r.content.decode())

    def test_same_lists_render_for_a_logged_in_user(self):
        # pair for the redirects above: the same URLs reach the view and render rows
        for name in LISTS:
            with self.subTest(name=name):
                pks, html = self.listed(name, self.a)
                self.assertTrue(pks, f'{name}: alice sees nothing at all')
                self.assertIn('LV', html)


class CollectionListTests(ListVisibilityBase):
    def test_user_sees_own_and_collaborated_not_others(self):
        pks, html = self.listed('collections', self.a)
        self.assertEqual(pks, {self.c_a.pk, self.c_b_collab.pk})
        self.assertIn('LVCOLL-alice-own', html)
        self.assertIn('LVCOLL-bob-shared-with-alice', html)
        self.assertNotIn('LVCOLL-bob-private', html)

    def test_other_user_sees_own_and_not_alices(self):
        pks, html = self.listed('collections', self.b)
        self.assertEqual(pks, {self.c_b.pk, self.c_b_collab.pk})
        self.assertIn('LVCOLL-bob-private', html)
        self.assertNotIn('LVCOLL-alice-own', html)

    def test_admin_sees_all(self):
        pks, html = self.listed('collections', self.admin)
        self.assertTrue({self.c_a.pk, self.c_b.pk, self.c_b_collab.pk} <= pks)
        self.assertIn('LVCOLL-bob-private', html)

    def test_filters_and_sorts_stay_scoped(self):
        # a title search matching bob's private collection must not reveal it to alice
        pks, html = self.listed('collections', self.a, title='LVCOLL', status='sandbox')
        self.assertEqual(pks, {self.c_a.pk, self.c_b_collab.pk})
        self.assertNotIn('LVCOLL-bob-private', html)
        for sort in ('title', 'owner', 'status', 'create_date'):
            with self.subTest(sort=sort):
                self.client.force_login(self.a)
                r = self.client.get(reverse('main:collection-list', args=[sort, 'asc']))
                self.assertEqual({o.pk for o in r.context['collections']},
                                 {self.c_a.pk, self.c_b_collab.pk})


class DatasetListTests(ListVisibilityBase):
    def test_user_sees_own_and_collaborated_not_others(self):
        pks, html = self.listed('datasets', self.a)
        self.assertEqual(pks, {self.d_a.pk, self.d_b_collab.pk})
        self.assertIn('LVDS-lv_alice_own', html)
        self.assertNotIn('LVDS-lv_bob_private', html)

    def test_admin_sees_all(self):
        pks, html = self.listed('datasets', self.admin)
        self.assertTrue({self.d_a.pk, self.d_b.pk, self.d_b_collab.pk} <= pks)
        self.assertIn('LVDS-lv_bob_private', html)


class AreaListTests(ListVisibilityBase):
    def test_user_sees_own_not_others(self):
        pks, html = self.listed('areas', self.a)
        self.assertEqual(pks, {self.ar_a.pk})
        self.assertIn('LVAREA-alice-own', html)
        self.assertNotIn('LVAREA-bob-private', html)

    def test_title_filter_stays_scoped(self):
        pks, html = self.listed('areas', self.a, title='LVAREA', type='all')
        self.assertEqual(pks, {self.ar_a.pk})
        self.assertNotIn('LVAREA-bob-private', html)

    def test_admin_sees_all(self):
        pks, html = self.listed('areas', self.admin)
        self.assertTrue({self.ar_a.pk, self.ar_b.pk} <= pks)
        self.assertIn('LVAREA-bob-private', html)


class GroupListTests(ListVisibilityBase):
    def test_user_sees_led_and_member_groups_not_others(self):
        pks, html = self.listed('groups', self.a)
        self.assertEqual(pks, {self.g_a.pk, self.g_b_member.pk})
        self.assertIn('LVGROUP-alice-leads', html)
        self.assertIn('LVGROUP-bob-alice-member', html)
        self.assertNotIn('LVGROUP-bob-private', html)

    def test_admin_sees_all(self):
        pks, html = self.listed('groups', self.admin)
        self.assertTrue({self.g_a.pk, self.g_b.pk, self.g_b_member.pk} <= pks)
        self.assertIn('LVGROUP-bob-private', html)

    def test_category_filter_is_echoed_back_from_type(self):
        r = self.get_list('groups', self.a, type='workshop')
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.context['filters']['type'], 'workshop')
        self.assertEqual({o.pk for o in r.context['groups']}, set())   # none are workshops
        r = self.get_list('groups', self.a, type='class')
        self.assertEqual(r.context['filters']['type'], 'class')
        self.assertEqual({o.pk for o in r.context['groups']}, {self.g_a.pk, self.g_b_member.pk})


class DashboardAdminContextTests(ListVisibilityBase):
    def context_for(self, user):
        self.client.force_login(user)
        r = self.client.get(reverse('dashboard-admin'))
        self.assertEqual(r.status_code, 200)
        return r.context

    def test_non_admin_context_holds_only_own_objects(self):
        ctx = self.context_for(self.a)
        self.assertEqual({o.pk for o in ctx['collections']}, {self.c_a.pk, self.c_b_collab.pk})
        self.assertEqual({o.pk for o in ctx['datasets']}, {self.d_a.pk, self.d_b_collab.pk})
        self.assertEqual({o.pk for o in ctx['areas']}, {self.ar_a.pk})

    def test_admin_context_holds_everything(self):
        ctx = self.context_for(self.admin)
        self.assertTrue({self.c_a.pk, self.c_b.pk} <= {o.pk for o in ctx['collections']})
        self.assertTrue({self.d_a.pk, self.d_b.pk} <= {o.pk for o in ctx['datasets']})
        self.assertTrue({self.ar_a.pk, self.ar_b.pk} <= {o.pk for o in ctx['areas']})
