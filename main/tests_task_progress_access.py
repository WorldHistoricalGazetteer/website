"""/task_progress/<taskid>/ requires login: anonymous gets 401 JSON (as validation's
task_status does), a logged-in user gets the progress JSON. Security fix 2026-11-18.

A Celery task id cannot be tied to its requester here (the result backend holds no owner
while a task runs), so this is login-only, not per-user scoping.

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 venv/bin/python manage.py test \
        main.tests_task_progress_access --settings=whg.settings_localtest
"""
from unittest import mock

from django.contrib.auth import get_user_model
from django.test import TestCase

User = get_user_model()
TASK = '0b1c2d3e-0000-4000-8000-00000000abcd'


def fake_result(*args, **kwargs):
    r = mock.Mock()
    r.backend = object()          # not a DisabledBackend
    r.state = 'PROGRESS'
    r.result = {'current': 3, 'total': 7}
    return r


@mock.patch('main.views.AsyncResult', side_effect=fake_result)
class TaskProgressAccessTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.user = User.objects.create_user(username='tpa_user', email='tpa@example.org',
                                            password='pw', given_name='Tpa', surname='Test')

    def test_logged_in_user_gets_progress(self, _ar):
        self.client.force_login(self.user)
        r = self.client.get(f'/task_progress/{TASK}/')
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json(), {'state': 'PROGRESS', 'progress': {'current': 3, 'total': 7}})

    def test_anonymous_gets_401_json_and_no_progress(self, ar):
        r = self.client.get(f'/task_progress/{TASK}/')
        self.assertEqual(r.status_code, 401)
        self.assertEqual(r['Content-Type'], 'application/json')
        self.assertNotIn('PROGRESS', r.content.decode())
        ar.assert_not_called()


class CeleryProgressNotRoutedTests(TestCase):
    """django-celery-progress is in INSTALLED_APPS but its urls ('/celery-progress/<id>/', which
    answers anyone with any task's state and result) have not been mounted since 1f44c336e
    (2024-02-12). Pin that, so re-adding the include fails here rather than reopening an
    unauthenticated task-result endpoint. The presence half (task_progress resolves) proves the
    resolver is the real one, not one that resolves nothing."""

    def test_celery_progress_is_not_routed_but_task_progress_is(self):
        from django.urls import Resolver404, resolve
        self.assertEqual(resolve(f'/task_progress/{TASK}/').url_name, 'task-progress')
        with self.assertRaises(Resolver404):
            resolve(f'/celery-progress/{TASK}/')

    @mock.patch('celery.result.AsyncResult')
    def test_anonymous_celery_progress_gets_404_without_touching_celery(self, ar):
        r = self.client.get(f'/celery-progress/{TASK}/')
        self.assertEqual(r.status_code, 404)
        ar.assert_not_called()
