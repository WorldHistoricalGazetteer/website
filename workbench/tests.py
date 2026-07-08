"""
Tests for the collaborative Workbench (place#112): three-way merge + the Phase 0/1 API.
Run: python3 manage.py test workbench
"""
import json

from django.contrib.auth import get_user_model
from django.test import TestCase, Client
from django.urls import reverse

from .merge import merge_snapshots
from .models import Team, TeamMember, WorkbenchProject, ProjectSnapshot, ROLE_VIEWER, ROLE_EDITOR

User = get_user_model()


def make_user(username, beta=True):
    u = User.objects.create_user(username=username, email=f'{username}@example.org',
                                 password='pw', given_name=username.title(), surname='Test',
                                 role='beta_tester' if beta else 'normal')
    return u


def snap(**kw):
    base = {'columns': [], 'rows': [], 'matches': {}, 'decisions': {}, 'geom': {}, 'rowTypes': {},
            'scope': {}, 'fileName': 'demo.csv'}
    base.update(kw)
    return base


# ── merge unit tests ────────────────────────────────────────────────────────
class MergeTests(TestCase):
    def test_disjoint_keyed_edits_automerge(self):
        base = snap(decisions={})
        mine = snap(decisions={'0:1': {'status': 'accepted', 'place_id': 'A'}})
        theirs = snap(decisions={'0:2': {'status': 'accepted', 'place_id': 'B'}})
        merged, conflicts = merge_snapshots(base, mine, theirs)
        self.assertEqual(conflicts, [])
        self.assertIn('0:1', merged['decisions'])
        self.assertIn('0:2', merged['decisions'])

    def test_same_key_divergence_conflicts(self):
        base = snap(decisions={'0:1': {'status': 'pending'}})
        mine = snap(decisions={'0:1': {'status': 'accepted', 'place_id': 'A'}})
        theirs = snap(decisions={'0:1': {'status': 'accepted', 'place_id': 'B'}})
        merged, conflicts = merge_snapshots(base, mine, theirs)
        self.assertEqual(len(conflicts), 1)
        self.assertEqual(conflicts[0]['kind'], 'decisions')
        self.assertEqual(conflicts[0]['key'], '0:1')
        # server value kept in merged
        self.assertEqual(merged['decisions']['0:1']['place_id'], 'B')

    def test_identical_edit_is_not_conflict(self):
        base = snap(decisions={})
        val = {'status': 'accepted', 'place_id': 'A'}
        mine = snap(decisions={'0:1': val})
        theirs = snap(decisions={'0:1': dict(val)})
        _, conflicts = merge_snapshots(base, mine, theirs)
        self.assertEqual(conflicts, [])

    def test_struct_field_mine_only(self):
        base = snap(scope={'where': []})
        mine = snap(scope={'where': ['GB']})
        theirs = snap(scope={'where': []})
        merged, conflicts = merge_snapshots(base, mine, theirs)
        self.assertEqual(conflicts, [])
        self.assertEqual(merged['scope'], {'where': ['GB']})

    def test_struct_field_conflict(self):
        base = snap(title='x')
        mine = snap(title='mine')
        theirs = snap(title='theirs')
        merged, conflicts = merge_snapshots(base, mine, theirs)
        self.assertEqual([c['key'] for c in conflicts], ['title'])
        self.assertEqual(merged['title'], 'theirs')  # server kept

    def test_deletion_on_my_side(self):
        base = snap(decisions={'0:1': {'status': 'accepted'}})
        mine = snap(decisions={})  # removed the key
        theirs = snap(decisions={'0:1': {'status': 'accepted'}})
        merged, conflicts = merge_snapshots(base, mine, theirs)
        self.assertEqual(conflicts, [])
        self.assertNotIn('0:1', merged['decisions'])


# ── API tests ───────────────────────────────────────────────────────────────
class ApiTests(TestCase):
    def setUp(self):
        self.owner = make_user('alice')
        self.other = make_user('bob')
        self.client = Client()
        self.client.force_login(self.owner)

    def _create(self, **kw):
        r = self.client.post(reverse('workbench:projects'),
                             data=json.dumps({'snapshot': snap(**kw)}),
                             content_type='application/json')
        self.assertEqual(r.status_code, 201, r.content)
        return r.json()

    def test_create_makes_personal_team_and_v1(self):
        j = self._create()
        p = WorkbenchProject.objects.get(pk=j['id'])
        self.assertEqual(p.version, 1)
        self.assertTrue(p.team.is_personal)
        self.assertEqual(p.role_for(self.owner), 'owner')
        self.assertTrue(ProjectSnapshot.objects.filter(project=p, version=1).exists())

    def test_beta_gate_blocks_non_beta(self):
        normal = make_user('carol', beta=False)
        c = Client()
        c.force_login(normal)
        r = c.get(reverse('workbench:projects'))
        self.assertEqual(r.status_code, 404)

    def test_fast_forward_push(self):
        j = self._create()
        url = reverse('workbench:project-detail', args=[j['id']])
        r = self.client.put(url, data=json.dumps(
            {'base_version': 1, 'snapshot': snap(decisions={'0:1': {'status': 'accepted'}})}),
            content_type='application/json')
        self.assertEqual(r.status_code, 200, r.content)
        self.assertEqual(r.json()['status'], 'ok')
        self.assertEqual(r.json()['version'], 2)
        self.assertTrue(ProjectSnapshot.objects.filter(project_id=j['id'], version=2).exists())

    def test_stale_disjoint_automerges(self):
        j = self._create()
        url = reverse('workbench:project-detail', args=[j['id']])
        # server advances to v2 (adds 0:2)
        self.client.put(url, data=json.dumps(
            {'base_version': 1, 'snapshot': snap(decisions={'0:2': {'status': 'accepted'}})}),
            content_type='application/json')
        # a client still on v1 pushes a disjoint edit (0:1) → should auto-merge
        r = self.client.put(url, data=json.dumps(
            {'base_version': 1, 'snapshot': snap(decisions={'0:1': {'status': 'accepted'}})}),
            content_type='application/json')
        self.assertEqual(r.status_code, 200, r.content)
        self.assertEqual(r.json()['status'], 'merged')
        merged = r.json()['snapshot']
        self.assertIn('0:1', merged['decisions'])
        self.assertIn('0:2', merged['decisions'])

    def test_stale_same_key_conflicts(self):
        j = self._create(decisions={'0:1': {'status': 'pending'}})
        url = reverse('workbench:project-detail', args=[j['id']])
        self.client.put(url, data=json.dumps(
            {'base_version': 1, 'snapshot': snap(decisions={'0:1': {'status': 'accepted', 'id': 'B'}})}),
            content_type='application/json')
        r = self.client.put(url, data=json.dumps(
            {'base_version': 1, 'snapshot': snap(decisions={'0:1': {'status': 'accepted', 'id': 'A'}})}),
            content_type='application/json')
        self.assertEqual(r.status_code, 409)
        self.assertEqual(r.json()['status'], 'conflict')
        self.assertEqual(r.json()['conflicts'][0]['key'], '0:1')

    def test_viewer_cannot_push(self):
        # owner creates a real team + project, adds bob as viewer
        team = Team.objects.create(owner=self.owner, title='T', slug='t')
        TeamMember.objects.create(team=team, user=self.owner, role='owner')
        TeamMember.objects.create(team=team, user=self.other, role=ROLE_VIEWER)
        p = WorkbenchProject.objects.create(team=team, title='P', created_by=self.owner,
                                            snapshot=snap(), version=1)
        ProjectSnapshot.objects.create(project=p, version=1, snapshot=snap())
        c = Client()
        c.force_login(self.other)
        url = reverse('workbench:project-detail', args=[p.id])
        # can read
        self.assertEqual(c.get(url).status_code, 200)
        # cannot write
        r = c.put(url, data=json.dumps({'base_version': 1, 'snapshot': snap()}),
                  content_type='application/json')
        self.assertEqual(r.status_code, 403)

    def test_share_roundtrip(self):
        j = self._create()
        r = self.client.post(reverse('workbench:project-share', args=[j['id']]),
                             content_type='application/json')
        self.assertEqual(r.status_code, 200)
        token = r.json()['token']
        # anonymous fetch works (token is the capability)
        anon = Client()
        r2 = anon.get(reverse('workbench:shared', args=[token]))
        self.assertEqual(r2.status_code, 200)
        self.assertTrue(r2.json()['read_only'])
        self.assertIn('snapshot', r2.json())

    def test_non_member_forbidden(self):
        j = self._create()
        c = Client()
        c.force_login(self.other)
        r = c.get(reverse('workbench:project-detail', args=[j['id']]))
        self.assertEqual(r.status_code, 403)

    def test_team_create_and_invite_by_username(self):
        r = self.client.post(reverse('workbench:teams'),
                             data=json.dumps({'title': 'Markets & Fairs'}),
                             content_type='application/json')
        self.assertEqual(r.status_code, 201)
        tid = r.json()['id']
        r2 = self.client.post(reverse('workbench:team-members', args=[tid]),
                             data=json.dumps({'identifier': 'bob', 'role': ROLE_EDITOR}),
                             content_type='application/json')
        self.assertEqual(r2.status_code, 200, r2.content)
        self.assertTrue(TeamMember.objects.filter(team_id=tid, user=self.other,
                                                  role=ROLE_EDITOR).exists())

    def test_collab_token(self):
        from django.test import override_settings
        import jwt as _jwt
        j = self._create()
        url = reverse('workbench:collab-token', args=[j['id']])
        # No secret configured → 501 (client falls back to REST sync)
        with override_settings(HOCUSPOCUS_SECRET=''):
            self.assertEqual(self.client.post(url).status_code, 501)
        # Configured → signed JWT scoped to this document + role
        with override_settings(HOCUSPOCUS_SECRET='test-secret'):
            r = self.client.post(url)
            self.assertEqual(r.status_code, 200, r.content)
            payload = _jwt.decode(r.json()['token'], 'test-secret', algorithms=['HS256'])
            self.assertEqual(payload['project_id'], j['id'])
            self.assertEqual(payload['role'], 'owner')
            # Non-member is refused a token
            c = Client(); c.force_login(self.other)
            self.assertEqual(c.post(url).status_code, 403)

    def test_gsheet_rejects_non_google_url(self):
        # Invalid (non-Google) URL → 400, without any network call (SSRF-safe validation).
        r = self.client.post(reverse('workbench:gsheet'),
                             data=json.dumps({'url': 'https://evil.example.com/x'}),
                             content_type='application/json')
        self.assertEqual(r.status_code, 400)
        self.assertIn('Google Sheets', r.json()['error'])

    def test_gsheet_beta_gated(self):
        normal = make_user('dave', beta=False)
        c = Client(); c.force_login(normal)
        r = c.post(reverse('workbench:gsheet'),
                   data=json.dumps({'url': 'https://docs.google.com/spreadsheets/d/abc/edit'}),
                   content_type='application/json')
        self.assertEqual(r.status_code, 404)

    def test_non_owner_cannot_invite(self):
        team = Team.objects.create(owner=self.owner, title='T', slug='t2')
        TeamMember.objects.create(team=team, user=self.owner, role='owner')
        TeamMember.objects.create(team=team, user=self.other, role=ROLE_EDITOR)
        c = Client()
        c.force_login(self.other)
        r = c.post(reverse('workbench:team-members', args=[team.id]),
                   data=json.dumps({'identifier': 'alice'}), content_type='application/json')
        self.assertEqual(r.status_code, 403)
