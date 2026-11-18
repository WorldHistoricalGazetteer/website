"""/collections/group/<id>/detail is visible only to the group's leader (owner), its members
and staff/whg_admins; a logged-in stranger gets 404, anonymous is sent to login (existence not confirmed).
The public face of a group is its gallery (/group/<id>/gallery, reviewed/published
collections only), which stays open. Security fix 2026-11-18.

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 venv/bin/python manage.py test \
        collection.tests_group_detail_access --settings=whg.settings_localtest

Each 404 is paired with a 200 for an allowed user on the same URL showing the group's
title, so a URL that never reached the view cannot pass.
"""
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.test import TestCase
from django.urls import reverse

from collection.models import CollectionGroup, CollectionGroupUser

User = get_user_model()
TITLE = 'GDA-private-class-group'


class GroupDetailAccessTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        def user(name, **kw):
            return User.objects.create_user(
                username=name, email=f'{name}@example.org', password='pw',
                given_name=name.title(), surname='Test', **kw)

        cls.leader = user('gda_leader')
        cls.member = user('gda_member')
        cls.stranger = user('gda_stranger')
        cls.admin = user('gda_admin')
        cls.admin.groups.add(Group.objects.get_or_create(name='whg_admins')[0])
        cls.staff = user('gda_staff', is_staff=True)
        cls.group = CollectionGroup.objects.create(owner=cls.leader, title=TITLE,
                                                   description='D', type='class')
        CollectionGroupUser.objects.create(collectiongroup=cls.group, user=cls.member,
                                           role='member')

    def url(self, pk=None):
        return reverse('collection:collection-group-detail', args=[pk or self.group.pk])

    def get(self, user=None, pk=None):
        if user is not None:
            self.client.force_login(user)
        return self.client.get(self.url(pk))

    def test_allowed_users_see_the_group(self):
        for who in ('leader', 'member', 'admin', 'staff'):
            with self.subTest(who=who):
                r = self.get(getattr(self, who))
                self.assertEqual(r.status_code, 200)
                self.assertContains(r, TITLE)

    def test_stranger_gets_404(self):
        r = self.get(self.stranger)
        self.assertEqual(r.status_code, 404)
        self.assertNotIn(TITLE, r.content.decode())

    def test_anonymous_is_sent_to_login(self):
        r = self.get()
        self.assertEqual(r.status_code, 302)
        self.assertTrue(r['Location'].startswith(settings.LOGIN_URL), r['Location'])
        self.assertNotIn(TITLE, r.content.decode())
        # same response for a group that does not exist: existence is not confirmed
        missing = CollectionGroup.objects.order_by('-pk').first().pk + 1000
        self.assertEqual(self.get(pk=missing).status_code, 302)

    def test_stranger_404_is_the_same_as_a_missing_group(self):
        missing = CollectionGroup.objects.order_by('-pk').first().pk + 1000
        self.assertEqual(self.get(self.stranger, pk=missing).status_code, 404)
        self.assertEqual(self.get(self.stranger).status_code, 404)
