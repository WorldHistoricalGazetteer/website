"""Workbench API bodies and history for PLATO Tools (A1): gzip request bodies, U+0000, and the kept
versions. Each refusal is paired with the same request made acceptable, so a test cannot pass
because the route refuses everything.

Run: venv/bin/python manage.py test workbench.tests_bearer_body --settings=whg.settings_localtest
"""
import gzip
import json

from django.test import override_settings
from django.urls import reverse

from .models import ProjectSnapshot, TeamMember, ROLE_VIEWER
from .tests_bearer import BearerBase


class GzipBodyTests(BearerBase):
    def post_raw(self, raw, encoding=None, token=None):
        headers = self.bearer(token or self.alice_token)
        if encoding:
            headers['HTTP_CONTENT_ENCODING'] = encoding
        return self.anon.post(reverse('workbench:projects'), data=raw,
                              content_type='application/json', **headers)

    def body(self, snapshot=None):
        return json.dumps({'doc_type': 'plato', 'title': 'gz',
                           'snapshot': snapshot or {'payload': 'x' * 1000}}).encode()

    def test_a_gzip_body_is_decoded(self):
        r = self.post_raw(gzip.compress(self.body()), 'gzip')
        self.assertEqual(r.status_code, 201, r.content)
        pid = r.json()['id']
        got = self.anon.get(reverse('workbench:project-detail', args=[pid]), **self.bearer(self.alice_token))
        self.assertEqual(got.json()['snapshot'], {'payload': 'x' * 1000})

    def test_gzip_sent_without_the_header_is_not_guessed(self):
        self.assertEqual(self.post_raw(gzip.compress(self.body()), 'gzip').status_code, 201)
        self.assertEqual(self.post_raw(gzip.compress(self.body())).status_code, 400)

    @override_settings(WORKBENCH_BODY_MAX_BYTES=4096)
    def test_the_cap_applies_after_decompression(self):
        small = self.body({'payload': 'x' * 1000})
        self.assertEqual(self.post_raw(gzip.compress(small), 'gzip').status_code, 201)
        bomb = gzip.compress(self.body({'payload': 'x' * 100_000}))
        self.assertLess(len(bomb), 4096)                    # small on the wire...
        r = self.post_raw(bomb, 'gzip')
        self.assertEqual(r.status_code, 413)                # ...too large once decoded
        self.assertEqual(r.json()['code'], 'body_too_large')

    def test_broken_gzip_and_other_encodings_are_refused(self):
        self.assertEqual(self.post_raw(gzip.compress(self.body()), 'gzip').status_code, 201)
        r = self.post_raw(b'not gzip at all', 'gzip')
        self.assertEqual((r.status_code, r.json()['code']), (400, 'bad_encoding'))
        r = self.post_raw(gzip.compress(self.body())[:-12], 'gzip')     # truncated stream
        self.assertEqual((r.status_code, r.json()['code']), (400, 'bad_encoding'))
        r = self.post_raw(self.body(), 'br')
        self.assertEqual((r.status_code, r.json()['code']), (415, 'unsupported_encoding'))

    def test_identity_is_plain(self):
        self.assertEqual(self.post_raw(self.body(), 'identity').status_code, 201)


class NulCharacterTests(BearerBase):
    def test_u0000_is_a_json_400_not_a_500(self):
        self.assertEqual(self.create(self.alice_token, snapshot={'a': 'fine'}).status_code, 201)
        for snapshot in ({'a': 'x\x00y'}, {'k\x00': 1}, {'a': [{'b': '\x00'}]}):
            with self.subTest(snapshot=snapshot):
                r = self.create(self.alice_token, snapshot=snapshot)
                self.assertEqual(r.status_code, 400)
                self.assertEqual(r.json()['code'], 'nul_character')

    def test_an_escaped_backslash_u0000_is_ordinary_text(self):
        # The six characters \u0000 as TEXT (JSON "\\u0000") are storable and must not be refused.
        r = self.create(self.alice_token, snapshot={'a': '\\u0000'})
        self.assertEqual(r.status_code, 201, r.content)

    def test_a_put_with_u0000_is_refused_and_changes_nothing(self):
        pid = self.create(self.alice_token).json()['id']
        r = self.put(pid, {'snapshot': {'a': '\x00'}, 'base_version': 1}, self.alice_token)
        self.assertEqual((r.status_code, r.json()['code']), (400, 'nul_character'))
        ok = self.put(pid, {'snapshot': {'a': 'b'}, 'base_version': 1}, self.alice_token)
        self.assertEqual(ok.status_code, 200)
        self.assertEqual(ok.json()['version'], 2)


class VersionHistoryTests(BearerBase):
    def setUp(self):
        super().setUp()
        self.pid = self.create(self.alice_token, snapshot={'n': 1}).json()['id']
        self.put(self.pid, {'snapshot': {'n': 2}, 'base_version': 1}, self.alice_token)
        self.put(self.pid, {'snapshot': {'n': 3}, 'base_version': 2}, self.alice_token)

    def versions(self, token):
        return self.anon.get(reverse('workbench:project-versions', args=[self.pid]), **self.bearer(token))

    def version(self, n, token):
        return self.anon.get(reverse('workbench:project-version-detail', args=[self.pid, n]),
                             **self.bearer(token))

    def test_the_list_is_newest_first_with_who_and_when(self):
        j = self.versions(self.alice_token).json()
        self.assertEqual(j['current_version'], 3)
        self.assertEqual([v['version'] for v in j['versions']], [3, 2, 1])
        self.assertEqual(j['versions'][0]['saved_by']['username'], self.alice.username)
        self.assertTrue(j['versions'][0]['created'])
        self.assertNotIn('snapshot', j['versions'][0])

    def test_an_older_version_is_returned_whole(self):
        j = self.version(1, self.alice_token).json()
        self.assertEqual((j['version'], j['snapshot'], j['current_version']), (1, {'n': 1}, 3))

    def test_a_viewer_may_read_history_and_a_stranger_may_not(self):
        p = ProjectSnapshot.objects.get(project_id=self.pid, version=1).project
        self.assertEqual(self.versions(self.bob_token).status_code, 403)
        self.assertEqual(self.version(1, self.bob_token).status_code, 403)
        TeamMember.objects.create(team=p.team, user=self.bob, role=ROLE_VIEWER)
        self.assertEqual(self.versions(self.bob_token).status_code, 200)
        self.assertEqual(self.version(1, self.bob_token).json()['snapshot'], {'n': 1})

    def test_a_pruned_or_unknown_version_is_a_json_404(self):
        self.assertEqual(self.version(2, self.alice_token).status_code, 200)
        ProjectSnapshot.objects.filter(project_id=self.pid, version=2).delete()
        r = self.version(2, self.alice_token)
        self.assertEqual((r.status_code, r.json()['code']), (404, 'not_found'))
        self.assertEqual(self.version(99, self.alice_token).status_code, 404)

    def test_the_saver_may_be_gone(self):
        v = ProjectSnapshot.objects.get(project_id=self.pid, version=1)
        v.created_by = None
        v.save(update_fields=['created_by'])
        self.assertIsNone(self.version(1, self.alice_token).json()['saved_by'])
