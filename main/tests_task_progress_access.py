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
        self.assertEqual(r.json(), {'state': 'PROGRESS',
                                    'progress': {'current': 3, 'total': 7, 'percent': 42.86, 'pending': False},
                                    'complete': False, 'success': None, 'result': None})

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


def _task(state, result=None):
    def make(*args, **kwargs):
        r = mock.Mock()
        r.backend = object()
        r.state = state
        r.result = result
        return r
    return make


class TaskProgressCeleryProgressBarShapeTests(TestCase):
    """/task_progress/ is what CeleryProgressBar (whg/static/celery_progress/celery_progress.js)
    polls on the dataset builder (builders-dataset.js) and collection-group download
    (collection_group_detail.html) since /celery-progress/ went unrouted. That class reads
    `complete` (missing -> "Data Error" and polling stops), `success`, `result`, and
    `progress.percent/pending/current/total`; the page's onResult links `'/' + result.filename`.
    The fields the older poller reads (`state`, `progress.current/total`, mapFunctions.js) stay."""

    @classmethod
    def setUpTestData(cls):
        cls.user = User.objects.create_user(username='tpa_shape', email='tpa_shape@example.org',
                                            password='pw', given_name='Tpa', surname='Shape')

    def _get(self, state, result=None):
        self.client.force_login(self.user)
        with mock.patch('main.views.AsyncResult', side_effect=_task(state, result)):
            r = self.client.get(f'/task_progress/{TASK}/')
        self.assertEqual(r.status_code, 200)
        return r.json()

    def test_pending_keeps_polling(self):
        body = self._get('PENDING')
        self.assertIs(body['complete'], False)
        self.assertIs(body['progress']['pending'], True)
        self.assertEqual(body['progress']['current'], 0)

    def test_success_hands_over_only_a_site_relative_filename(self):
        import os
        from django.conf import settings
        fn = os.path.join(settings.MEDIA_ROOT, 'downloads', '12_ds_20261118.json')
        body = self._get('SUCCESS', {'msg': 'lpf written', 'filename': fn, 'secret': 'x'})
        self.assertEqual((body['state'], body['complete'], body['success']), ('SUCCESS', True, True))
        self.assertEqual(body['progress']['percent'], 100)
        # '/' + filename is a site path (it was '//app/media/...', a host named "app").
        self.assertEqual(body['result'], {'filename': 'media/downloads/12_ds_20261118.json'})

    def test_success_outside_media_root_hands_over_no_path(self):
        body = self._get('SUCCESS', {'filename': '/etc/passwd'})
        self.assertEqual(body['result'], {'filename': None})

    def test_failure_is_complete_and_unsuccessful_without_the_exception_text(self):
        body = self._get('FAILURE', ValueError('/app/secret/path exploded'))
        self.assertEqual((body['complete'], body['success']), (True, False))
        self.assertIn('did not complete', body['result'])  # presence: a message was sent
        self.assertNotIn('secret', str(body))


class DownloadPollersPointAtTaskProgressTests(TestCase):
    """The collection-group page's download poller is rendered with /task_progress/, and the
    page itself is login-gated, as /task_progress/ is (so the poll never meets its 401)."""

    def test_collection_group_detail_polls_task_progress(self):
        from collection.models import CollectionGroup
        owner = User.objects.create_user(username='tpa_cg', email='tpa_cg@example.org', password='pw',
                                         given_name='Tpa', surname='Cg')
        cg = CollectionGroup.objects.create(owner=owner, title='TPA group', description='d')
        url = f'/collections/group/{cg.id}/detail'
        self.assertEqual(self.client.get(url).status_code, 302)  # anonymous -> login
        self.client.force_login(owner)
        r = self.client.get(url)
        self.assertEqual(r.status_code, 200)
        html = r.content.decode()
        self.assertIn('"/task_progress/" + task_id + "/"', html)
        self.assertIn('TPA group', html)
        self.assertNotIn('"/celery-progress/"', html)
