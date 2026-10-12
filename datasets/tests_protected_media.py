"""place#310: uploaded dataset files and exports under /media/ are served only to those allowed.

nginx used to serve /media/ straight from disk, so a private dataset's uploaded file
(``user_<name>/<file>``) or an owner's export (``downloads/<user id>_…``) was readable by anyone
who could guess its path. Each refusal here is paired with an insider reading the same file
through the same route, so a test cannot pass because the route always 404s.

    python3 manage.py test datasets.tests_protected_media --settings=whg.settings_localtest
"""
import os
import shutil
import tempfile

from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings

from datasets.models import Dataset, DatasetFile, DatasetUser

User = get_user_model()


class ProtectedMediaTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.media = tempfile.mkdtemp()
        cls._settings = override_settings(MEDIA_ROOT=cls.media, PROTECTED_MEDIA_ACCEL=False)
        cls._settings.enable()
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        cls._settings.disable()
        shutil.rmtree(cls.media, ignore_errors=True)

    @classmethod
    def setUpTestData(cls):
        def user(name, **kw):
            return User.objects.create_user(username=name, email=f'{name}@example.org', password='pw',
                                            given_name=name.title(), surname='Test', **kw)

        cls.owner, cls.collab, cls.stranger = user('pmowner'), user('pmcollab'), user('pmstranger')
        cls.staff = user('pmstaff', is_staff=True)
        for key, public in (('priv', False), ('pub', True)):
            ds = Dataset.objects.create(owner=cls.owner, label=f'pm310_{key}', title=key, description='D',
                                        creator='C', public=public, ds_status='indexed',
                                        uri_base='https://example.org/places/')
            DatasetFile.objects.create(dataset_id=ds, file=f'user_pmowner/{key}.tsv')
            setattr(cls, key, ds)
        DatasetUser.objects.create(dataset_id=cls.priv, user_id=cls.collab, role='member')

    def setUp(self):
        for rel in ('user_pmowner/priv.tsv', 'user_pmowner/pub.tsv', 'user_pmowner/notes.txt',
                    f'downloads/{self.owner.id}_pm310_priv_20261012_120000.tsv',
                    'downloads/anon_pm310_pub_20261012_120000.tsv',
                    f'downloads/whg_dataset_{self.priv.id}.lpf.gz', 'help/page.html'):
            full = os.path.join(self.media, rel)
            os.makedirs(os.path.dirname(full), exist_ok=True)
            with open(full, 'w') as fh:
                fh.write(f'content of {rel}')

    def _get(self, user, path):
        if user:
            self.client.force_login(user)
        else:
            self.client.logout()
        return self.client.get('/media/' + path)

    def assertServed(self, user, path):
        r = self._get(user, path)
        self.assertEqual(r.status_code, 200, (user, path))
        self.assertEqual(b''.join(r.streaming_content), f'content of {path}'.encode())
        self.assertIn('no-store', r['Cache-Control'])

    def assertWithheld(self, user, path):
        self.assertEqual(self._get(user, path).status_code, 404, (user, path))

    def test_private_dataset_upload(self):
        path = 'user_pmowner/priv.tsv'
        for insider in (self.owner, self.collab, self.staff):
            self.assertServed(insider, path)
        for outsider in (self.stranger, None):
            self.assertWithheld(outsider, path)

    def test_public_dataset_upload_stays_public(self):
        self.assertServed(None, 'user_pmowner/pub.tsv')
        self.assertServed(self.stranger, 'user_pmowner/pub.tsv')

    def test_unattached_file_in_a_user_directory(self):
        self.assertServed(self.owner, 'user_pmowner/notes.txt')
        self.assertWithheld(self.stranger, 'user_pmowner/notes.txt')
        self.assertWithheld(None, 'user_pmowner/notes.txt')

    def test_an_export_belongs_to_its_requester(self):
        path = f'downloads/{self.owner.id}_pm310_priv_20261012_120000.tsv'
        self.assertServed(self.owner, path)
        self.assertServed(self.staff, path)
        # A collaborator may see the dataset, but this file was written for someone else.
        for other in (self.collab, self.stranger, None):
            self.assertWithheld(other, path)

    def test_an_anonymous_export_is_readable_by_anyone(self):
        self.assertServed(None, 'downloads/anon_pm310_pub_20261012_120000.tsv')

    def test_the_shared_export_caches_are_staff_only(self):
        path = f'downloads/whg_dataset_{self.priv.id}.lpf.gz'
        self.assertServed(self.staff, path)
        for other in (self.owner, None):
            self.assertWithheld(other, path)

    def test_missing_and_refused_look_the_same(self):
        self.assertWithheld(self.owner, 'user_pmowner/absent.tsv')
        self.assertWithheld(self.stranger, 'user_pmowner/priv.tsv')

    def test_no_escape_from_the_media_root(self):
        self.assertServed(self.owner, 'user_pmowner/notes.txt')                 # positive control
        for path in ('user_pmowner/../help/page.html', 'downloads/../user_pmowner/priv.tsv',
                     'user_pmowner/%2e%2e/%2e%2e/etc/passwd'):
            self.assertWithheld(self.staff, path)

    @override_settings(PROTECTED_MEDIA_ACCEL=True)
    def test_nginx_serves_the_bytes(self):
        r = self._get(self.owner, 'user_pmowner/priv.tsv')
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r['X-Accel-Redirect'], '/protected-media/user_pmowner/priv.tsv')
        self.assertEqual(r.content, b'')
        self.assertEqual(self._get(None, 'user_pmowner/priv.tsv').status_code, 404)
        gz = self._get(self.staff, f'downloads/whg_dataset_{self.priv.id}.lpf.gz')
        self.assertEqual(gz['Content-Type'], 'application/gzip')
        self.assertNotIn('Content-Encoding', gz)
