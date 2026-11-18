"""/api/users/ and /api/user/<pk>/ serialise every account's name and email; they had no
permission class, so anonymous requests listed all users (found 2026-11-18). Staff only now.

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 venv/bin/python manage.py test \
        api.tests_user_endpoints_access --settings=whg.settings_localtest

Each denial is paired with a staff request to the same URL that does return the email, so a
URL that never reached the view (or never contained the data) cannot pass.
"""
from django.contrib.auth import get_user_model
from django.test import TestCase

User = get_user_model()
SECRET = 'uea-target@example.org'


class UserEndpointAccessTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.target = User.objects.create_user(username='uea_target', email=SECRET, password='pw',
                                              given_name='Target', surname='Test')
        cls.plain = User.objects.create_user(username='uea_plain', email='uea-plain@example.org',
                                             password='pw', given_name='Plain', surname='Test')
        cls.staff = User.objects.create_user(username='uea_staff', email='uea-staff@example.org',
                                             password='pw', given_name='Staff', surname='Test',
                                             is_staff=True)

    def urls(self):
        return ['/api/users/', f'/api/user/{self.target.pk}/']

    def test_staff_sees_the_data(self):
        self.client.force_login(self.staff)
        for url in self.urls():
            with self.subTest(url=url):
                r = self.client.get(url)
                self.assertEqual(r.status_code, 200)
                self.assertIn(SECRET, r.content.decode())

    def test_anonymous_refused(self):
        for url in self.urls():
            with self.subTest(url=url):
                r = self.client.get(url)
                self.assertIn(r.status_code, (401, 403))
                self.assertNotIn(SECRET, r.content.decode())

    def test_ordinary_user_refused(self):
        self.client.force_login(self.plain)
        for url in self.urls():
            with self.subTest(url=url):
                r = self.client.get(url)
                self.assertEqual(r.status_code, 403)
                self.assertNotIn(SECRET, r.content.decode())
