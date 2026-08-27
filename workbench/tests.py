"""
Tests for the collaborative Workbench (place#112): three-way merge + the Phase 0/1 API.
Run: python3 manage.py test workbench
"""
import json

from django.contrib.auth import get_user_model
from django.test import TestCase, Client
from django.urls import reverse

from .merge import merge_snapshots
from . import extraction, views
from .models import (Team, TeamMember, WorkbenchProject, ProjectSnapshot,
                     ROLE_VIEWER, ROLE_EDITOR, ROLE_OWNER)

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

    def test_gdoc_rejects_non_google_url(self):
        r = self.client.post(reverse('workbench:gdoc'),
                             data=json.dumps({'url': 'https://evil.example.com/x'}),
                             content_type='application/json')
        self.assertEqual(r.status_code, 400)
        self.assertIn('Google Docs', r.json()['error'])

    def test_ner_beta_gated(self):
        normal = make_user('erin', beta=False)
        c = Client(); c.force_login(normal)
        r = c.post(reverse('workbench:ner'), data=json.dumps({'text': 'Rome'}),
                   content_type='application/json')
        self.assertEqual(r.status_code, 404)

    def test_ner_empty_text_rejected(self):
        # Blank text → 400 without any call to the extractor.
        r = self.client.post(reverse('workbench:ner'), data=json.dumps({'text': '   '}),
                             content_type='application/json')
        self.assertEqual(r.status_code, 400)

    def test_ner_service_unconfigured(self):
        # No OLLAMA_URL configured → 503 (graceful), no network call.
        from django.test import override_settings
        with override_settings(OLLAMA_URL=''):
            r = self.client.post(reverse('workbench:ner'),
                                 data=json.dumps({'text': 'Rome and Venice'}),
                                 content_type='application/json')
            self.assertEqual(r.status_code, 503)

    def test_ner_success_shape(self):
        """The endpoint returns the extractor's names even when reconciliation is unavailable."""
        from unittest.mock import patch
        with patch('workbench.extraction.extract_places') as extract, \
                patch('workbench.views._ner_reconcile_disambiguate', return_value={}):
            extract.return_value = [
                {'name': 'Duxford', 'label': 'LLM', 'count': 1, 'context': 'in Duxford',
                 'verbatim': True}]
            r = self.client.post(reverse('workbench:ner'),
                                 data=json.dumps({'text': 'a tenement in Duxford'}),
                                 content_type='application/json')
        self.assertEqual(r.status_code, 200, r.content)
        self.assertEqual([e['name'] for e in r.json()['entities']], ['Duxford'])

    def test_ner_model_failure_is_502_not_500(self):
        """A model fault is not the user's problem — it must not surface as a server error page."""
        from unittest.mock import patch
        with patch('workbench.extraction.extract_places', side_effect=ValueError('boom')):
            r = self.client.post(reverse('workbench:ner'),
                                 data=json.dumps({'text': 'Rome and Venice'}),
                                 content_type='application/json')
        self.assertEqual(r.status_code, 502)

    def test_cannot_add_yourself_and_keep_owner_role(self):
        """place#203 — self-add used to overwrite the owner's own role with the default
        'editor', locking them out of managing the team they created."""
        team = Team.objects.create(owner=self.owner, title='T', slug='t-self')
        TeamMember.objects.create(team=team, user=self.owner, role=ROLE_OWNER)
        url = reverse('workbench:team-members', args=[team.id])
        for identifier in ('alice', self.owner.email):
            r = self.client.post(url, data=json.dumps({'identifier': identifier, 'role': ROLE_EDITOR}),
                                 content_type='application/json')
            self.assertEqual(r.status_code, 400, r.content)
            self.assertIn('cannot add yourself', r.json()['error'])
            self.assertEqual(team.role_for(self.owner), ROLE_OWNER)

    def test_add_member_cannot_demote_an_owner(self):
        team = Team.objects.create(owner=self.owner, title='T', slug='t-own')
        TeamMember.objects.create(team=team, user=self.owner, role=ROLE_OWNER)
        TeamMember.objects.create(team=team, user=self.other, role=ROLE_OWNER)  # co-owner
        r = self.client.post(reverse('workbench:team-members', args=[team.id]),
                             data=json.dumps({'identifier': 'bob', 'role': ROLE_VIEWER}),
                             content_type='application/json')
        self.assertEqual(r.status_code, 400, r.content)
        self.assertEqual(team.role_for(self.other), ROLE_OWNER)

    def test_existing_member_role_can_still_be_changed(self):
        team = Team.objects.create(owner=self.owner, title='T', slug='t-chg')
        TeamMember.objects.create(team=team, user=self.owner, role=ROLE_OWNER)
        TeamMember.objects.create(team=team, user=self.other, role=ROLE_EDITOR)
        r = self.client.post(reverse('workbench:team-members', args=[team.id]),
                             data=json.dumps({'identifier': 'bob', 'role': ROLE_VIEWER}),
                             content_type='application/json')
        self.assertEqual(r.status_code, 200, r.content)
        self.assertEqual(team.role_for(self.other), ROLE_VIEWER)

    def test_non_owner_cannot_invite(self):
        team = Team.objects.create(owner=self.owner, title='T', slug='t2')
        TeamMember.objects.create(team=team, user=self.owner, role='owner')
        TeamMember.objects.create(team=team, user=self.other, role=ROLE_EDITOR)
        c = Client()
        c.force_login(self.other)
        r = c.post(reverse('workbench:team-members', args=[team.id]),
                   data=json.dumps({'identifier': 'alice'}), content_type='application/json')
        self.assertEqual(r.status_code, 403)


# ── doc-type registry (unit; no DB) ───────────────────────────────────────────
class DocTypeRegistryTests(TestCase):
    def test_creatable_excludes_v4_placeholders(self):
        from workbench import doctypes
        keys = [d.key for d in doctypes.creatable()]
        self.assertIn('place_collection', keys)
        self.assertIn('itinerary', keys)
        self.assertIn('gazetteer_group', keys)
        self.assertNotIn('route', keys)
        self.assertNotIn('network', keys)

    def test_route_network_reserved_but_disabled(self):
        from workbench import doctypes
        self.assertIsNotNone(doctypes.get('route'))
        self.assertFalse(doctypes.get('route').enabled)
        self.assertFalse(doctypes.get('network').enabled)

    def test_snapshot_validation(self):
        from workbench import doctypes
        self.assertTrue(doctypes.validate_snapshot('nope', {}))              # unknown → error
        self.assertTrue(doctypes.validate_snapshot('place_collection', {}))  # missing title/places
        self.assertEqual([], doctypes.validate_snapshot(
            'place_collection', {'title': 'X', 'places': [{'id': 'whg:5'}]}))
        self.assertTrue(doctypes.validate_snapshot(
            'gazetteer_group', {'title': 'G', 'gazetteers': [{}]}))          # gazetteer missing id


# ── doc-type project creation gate ────────────────────────────────────────────
class DocTypeCreateTests(TestCase):
    def setUp(self):
        self.owner = make_user('frank')
        self.client = Client()
        self.client.force_login(self.owner)

    def _post(self, **body):
        return self.client.post(reverse('workbench:projects'),
                                data=json.dumps(body), content_type='application/json')

    def test_default_doc_type_is_reconciliation(self):
        r = self._post(snapshot=snap())
        self.assertEqual(r.status_code, 201, r.content)
        self.assertEqual(r.json()['doc_type'], 'reconciliation')

    def test_create_place_collection(self):
        r = self._post(doc_type='place_collection',
                       snapshot={'title': 'My places', 'places': [{'id': 'whg:1'}]})
        self.assertEqual(r.status_code, 201, r.content)
        p = WorkbenchProject.objects.get(pk=r.json()['id'])
        self.assertEqual(p.doc_type, 'place_collection')
        self.assertEqual(p.title, 'My places')

    def test_route_creation_gated_off(self):
        r = self._post(doc_type='route', snapshot={'title': 'Silk Road'})
        self.assertEqual(r.status_code, 403)

    def test_unknown_doc_type_rejected(self):
        r = self._post(doc_type='banana', snapshot={'title': 'x'})
        self.assertEqual(r.status_code, 400)

    def test_invalid_place_collection_snapshot_rejected(self):
        # missing title + non-list places
        r = self._post(doc_type='place_collection', snapshot={'places': 'nope'})
        self.assertEqual(r.status_code, 400)


# ── publish + checkout into collection.Collection ─────────────────────────────
def make_place(title, label='wb-test-ds'):
    """Minimal Dataset + Place fixture. Place.dataset FKs Dataset.label (not pk)."""
    from datasets.models import Dataset
    from places.models import Place
    ds, _ = Dataset.objects.get_or_create(
        label=label, defaults={'owner': User.objects.filter(is_superuser=False).first(),
                               'title': 'WB test ds', 'description': 'x'})
    return Place.objects.create(title=title, src_id='', dataset=ds, ccodes=[])


class PublishCheckoutTests(TestCase):
    def setUp(self):
        self.owner = make_user('grace')
        self.client = Client()
        self.client.force_login(self.owner)

    def _create_pc(self, snapshot):
        r = self.client.post(reverse('workbench:projects'),
                             data=json.dumps({'doc_type': 'place_collection', 'snapshot': snapshot}),
                             content_type='application/json')
        self.assertEqual(r.status_code, 201, r.content)
        return WorkbenchProject.objects.get(pk=r.json()['id'])

    def test_publish_place_collection_creates_collection(self):
        from collection.models import Collection, CollPlace
        from traces.models import TraceAnnotation
        p1, p2 = make_place('Rome'), make_place('Venice')
        proj = self._create_pc({
            'title': 'Italian cities', 'description': 'demo',
            'keywords': ['italy', 'cities'],
            'places': [{'id': f'whg:{p1.id}', 'note': 'the capital'},
                       {'id': f'whg:{p2.id}'},
                       {'id': 'gn:745044'}]})  # a CRC id — must be reported unresolved
        r = self.client.post(reverse('workbench:project-publish', args=[proj.id]),
                             content_type='application/json')
        self.assertEqual(r.status_code, 200, r.content)
        body = r.json()
        self.assertEqual(body['added'], 2)
        self.assertEqual(body['unresolved'], ['gn:745044'])
        coll = Collection.objects.get(pk=body['collection_id'])
        self.assertEqual(coll.collection_class, 'place')
        self.assertEqual(coll.title, 'Italian cities')
        self.assertEqual(sorted(coll.keywords), ['cities', 'italy'])
        self.assertEqual(CollPlace.objects.filter(collection=coll).count(), 2)
        self.assertTrue(TraceAnnotation.objects.filter(collection=coll, place=p1,
                                                       note='the capital').exists())
        # project now points at the published collection + is marked published
        proj.refresh_from_db()
        self.assertEqual(proj.published_collection_id, coll.pk)
        self.assertEqual(proj.status, 'published')

    def test_republish_updates_in_place(self):
        from collection.models import Collection, CollPlace
        p1, p2 = make_place('Rome'), make_place('Venice')
        proj = self._create_pc({'title': 'C', 'places': [{'id': f'whg:{p1.id}'}]})
        r1 = self.client.post(reverse('workbench:project-publish', args=[proj.id]),
                              content_type='application/json')
        cid = r1.json()['collection_id']
        # edit snapshot to two places, re-publish → same collection, membership rebuilt
        proj.refresh_from_db()
        proj.snapshot = {'title': 'C', 'places': [{'id': f'whg:{p1.id}'}, {'id': f'whg:{p2.id}'}]}
        proj.save(update_fields=['snapshot'])
        r2 = self.client.post(reverse('workbench:project-publish', args=[proj.id]),
                              content_type='application/json')
        self.assertEqual(r2.json()['collection_id'], cid)      # same collection
        self.assertEqual(Collection.objects.filter(collection_class='place').count(), 1)
        self.assertEqual(CollPlace.objects.filter(collection_id=cid).count(), 2)

    def test_publish_beta_gated(self):
        proj = self._create_pc({'title': 'C', 'places': []})
        normal = make_user('heidi', beta=False)
        c = Client(); c.force_login(normal)
        r = c.post(reverse('workbench:project-publish', args=[proj.id]),
                   content_type='application/json')
        self.assertEqual(r.status_code, 404)

    def test_checkout_roundtrip(self):
        # publish, then check out the published collection → new project whose snapshot matches
        p1 = make_place('Rome')
        proj = self._create_pc({'title': 'RT', 'places': [{'id': f'whg:{p1.id}', 'note': 'hi'}]})
        r = self.client.post(reverse('workbench:project-publish', args=[proj.id]),
                             content_type='application/json')
        cid = r.json()['collection_id']
        r2 = self.client.post(reverse('workbench:checkout-collection', args=[cid]),
                              content_type='application/json')
        self.assertEqual(r2.status_code, 201, r2.content)
        new = WorkbenchProject.objects.get(pk=r2.json()['id'])
        self.assertEqual(new.doc_type, 'place_collection')
        self.assertEqual(new.published_collection_id, cid)
        self.assertEqual(new.snapshot['places'][0]['id'], f'whg:{p1.id}')
        self.assertEqual(new.snapshot['places'][0]['note'], 'hi')


class NerScopeTests(TestCase):
    """place#211 items 1 and 2 — caller-supplied scope, and the residence formula."""

    def setUp(self):
        self.client = Client()
        self.client.force_login(make_user('scoper'))

    def test_person_of_place_yields_the_toponym(self):
        """_NER_CAND_RE swallows "Robert Heard of Stifford" whole and it matches nothing; the tail is
        the place and must be offered separately, while the full span survives too."""
        got = views._ner_candidates('Robert Heard of Stifford, husbandman.', [])
        self.assertIn('Stifford', got)
        self.assertIn('Robert Heard of Stifford', got)

    def test_of_split_takes_the_last_and_most_specific_tail(self):
        got = views._ner_candidates('Master of the Hospital of St Mary Magdalen in Bath.', [])
        self.assertIn('St Mary Magdalen', got)

    def test_contained_in_is_passed_through_and_skips_mode_seeking(self):
        """With a container the caller already knows where the row is; the global 250 km cluster is
        wrong for a table whose rows sit in different counties, so it must not run."""
        from unittest.mock import patch
        captured = {}

        def fake(queries, batch_size=None, user=None):
            captured['queries'] = queries
            return {'q0': {'result': [
                # A far-apart pair the geographic pass would have had to choose between; inside the
                # container, score alone decides.
                {'id': 'place:osm:n1', 'name': 'Dovercourt', 'score': 91, 'repr_point': [1.2, 51.9]},
                {'id': 'place:osm:n2', 'name': 'Dovercourt', 'score': 40, 'repr_point': [-79.0, 43.0]},
            ]}}

        with patch('api.reconcile.process_queries', fake):
            out = views._ner_reconcile_disambiguate({'Dovercourt': 1}, None,
                                                    contained_in=['ukhc:ESE'])
        self.assertEqual(captured['queries']['q0']['contained_in'], ['ukhc:ESE'])
        self.assertEqual(out['Dovercourt']['id'], 'place:osm:n1')
        self.assertTrue(out['Dovercourt']['ambiguous'])         # two distinct locations survived

    def test_names_only_skips_reconciliation_entirely(self):
        from unittest.mock import patch
        with patch('workbench.extraction.extract_places') as extract, \
                patch('workbench.views._ner_reconcile_disambiguate') as recon:
            extract.return_value = [{'name': 'Duxford', 'label': 'LLM', 'count': 1,
                                     'context': '', 'verbatim': True}]
            r = self.client.post(reverse('workbench:ner'),
                                 data=json.dumps({'text': 'lands in Duxford', 'names_only': True}),
                                 content_type='application/json')
        self.assertEqual(r.status_code, 200, r.content)
        self.assertTrue(r.json()['names_only'])
        recon.assert_not_called()

    def test_scope_accepts_a_bare_string_and_is_bounded(self):
        from unittest.mock import patch
        with patch('workbench.extraction.extract_places', return_value=[]), \
                patch('workbench.views._ner_reconcile_disambiguate', return_value={}) as recon:
            self.client.post(reverse('workbench:ner'),
                             data=json.dumps({'text': 'x', 'contained_in': 'ukhc:ESE'}),
                             content_type='application/json')
            self.assertEqual(recon.call_args.kwargs['contained_in'], ['ukhc:ESE'])
            self.client.post(reverse('workbench:ner'),
                             data=json.dumps({'text': 'x',
                                              'contained_in': [f'r{i}' for i in range(50)]}),
                             content_type='application/json')
            self.assertEqual(len(recon.call_args.kwargs['contained_in']), views.NER_SCOPE_MAX)


class NerPerRowTests(TestCase):
    """place#211 items 3-6 — the per-row column operation's server half."""

    def setUp(self):
        self.user = make_user('rowrunner')
        self.client = Client()
        self.client.force_login(self.user)

    def _post(self, payload):
        return self.client.post(reverse('workbench:ner-rows'), data=json.dumps(payload),
                                content_type='application/json')

    def test_each_row_is_scoped_by_its_own_container(self):
        """The whole point: a table's rows sit in different counties, so one global region is wrong."""
        from unittest.mock import patch
        seen = []

        def recon(mentions, user, contained_in=None):
            seen.append(contained_in)
            return {}

        with patch('workbench.extraction.extract_places',
                   return_value=[{'name': 'Dovercourt', 'count': 1, 'context': 'c',
                                  'verbatim': True, 'label': 'LLM'}]), \
                patch('workbench.views._ner_reconcile_disambiguate', side_effect=recon):
            r = self._post({'rows': [
                {'key': 'a', 'text': 'lands in Dovercourt', 'contained_in': ['ukhc:ESE']},
                {'key': 'b', 'text': 'lands in Dovercourt', 'contained_in': ['ukhc:DEV']},
            ]})
        self.assertEqual(r.status_code, 200, r.content)
        self.assertEqual(seen, [['ukhc:ESE'], ['ukhc:DEV']])

    def test_rows_yielding_nothing_survive_with_a_flag(self):
        """23 of 90 sampled REQ 2 cases produced no place. Dropping them makes every denominator
        wrong, so they must come back, flagged."""
        from unittest.mock import patch
        with patch('workbench.extraction.extract_places', return_value=[]), \
                patch('workbench.views._ner_reconcile_disambiguate', return_value={}):
            r = self._post({'rows': [{'key': 'a', 'text': 'a bond and nothing else'},
                                     {'key': 'b', 'text': '   '}]})
        out = r.json()['results']
        self.assertEqual([x['key'] for x in out], ['a', 'b'])
        self.assertTrue(all(x['no_places_found'] for x in out))

    def test_mentions_are_deduplicated_within_the_row(self):
        """"Great Easton" twice in one description is one place mentioned twice, not two rows."""
        from unittest.mock import patch
        text = 'Great Easton, and further lands at Great Easton in the same parish.'
        with patch('workbench.extraction.extract_places',
                   return_value=[{'name': 'Great Easton', 'count': 2, 'context': 'c',
                                  'verbatim': True, 'label': 'LLM'}]), \
                patch('workbench.views._ner_reconcile_disambiguate', return_value={}):
            r = self._post({'rows': [{'key': 'a', 'text': text}]})
        places = r.json()['results'][0]['places']
        self.assertEqual([(p['name'], p['mentions']) for p in places], [('Great Easton', 2)])

    def test_out_of_container_hits_are_marked_never_silently_accepted(self):
        from unittest.mock import patch
        calls = []

        def recon(mentions, user, contained_in=None):
            calls.append(contained_in)
            if contained_in == ['ohm:r2694795']:
                return {'London': {'id': 'place:wd:Q84', 'title': 'London', 'score': 100,
                                   'ccodes': ['GB'], 'lng': -0.1, 'lat': 51.5, 'ambiguous': False}}
            return {}

        with patch('workbench.extraction.extract_places',
                   return_value=[{'name': 'London', 'count': 1, 'context': 'c',
                                  'verbatim': True, 'label': 'LLM'}]), \
                patch('workbench.views._ner_reconcile_disambiguate', side_effect=recon):
            r = self._post({'rows': [{'key': 'a', 'text': 'a merchant of London',
                                      'contained_in': ['ukhc:ESE']}],
                            'fallback_contained_in': ['ohm:r2694795']})
        place = r.json()['results'][0]['places'][0]
        self.assertEqual(calls, [['ukhc:ESE'], ['ohm:r2694795']])
        self.assertTrue(place['outside_container'])

    def test_batch_is_capped_and_charged_per_row(self):
        from unittest.mock import patch
        from api.models import UserAPIProfile
        with patch('workbench.extraction.extract_places', return_value=[]), \
                patch('workbench.views._ner_reconcile_disambiguate', return_value={}):
            r = self._post({'rows': [{'key': str(i), 'text': 'x'} for i in range(40)]})
        j = r.json()
        self.assertEqual(j['capped'], views.NER_ROWS_MAX)
        self.assertEqual(len(j['results']), views.NER_ROWS_MAX)
        self.assertEqual(UserAPIProfile.objects.get(user=self.user).daily_count, views.NER_ROWS_MAX)

    def test_exhausted_allowance_is_refused_before_any_model_work(self):
        from unittest.mock import patch
        from api.models import UserAPIProfile
        UserAPIProfile.objects.create(user=self.user, daily_limit=1, daily_count=1)
        with patch('workbench.extraction.extract_places') as extract:
            r = self._post({'rows': [{'key': 'a', 'text': 'x'}, {'key': 'b', 'text': 'y'}]})
        self.assertEqual(r.status_code, 429)
        extract.assert_not_called()

    def test_a_zero_daily_limit_means_unlimited_not_blocked(self):
        """api/authentication.py reads a falsy daily_limit as UNLIMITED (`if profile.daily_limit and
        ...`), and accounts are set to 0 deliberately. Reading it as an exhausted quota locks exactly
        those users out — which is what happened to a real account before this was pinned."""
        from unittest.mock import patch
        from api.models import UserAPIProfile
        UserAPIProfile.objects.create(user=self.user, daily_limit=0, daily_count=9999)
        with patch('workbench.extraction.extract_places', return_value=[]), \
                patch('workbench.views._ner_reconcile_disambiguate', return_value={}):
            r = self._post({'rows': [{'key': 'a', 'text': 'x'}]})
        self.assertEqual(r.status_code, 200, r.content)
        self.assertIsNone(r.json()['remaining_today'])

    def test_bare_appellatives_are_dropped_only_when_unvouched(self):
        """The model returns "Hospital" and "Cathedral" because the surrounding phrase is place-like.
        A common noun the gazetteer cannot vouch for is noise; one it CAN vouch for is a real place
        that happens to be called Mill, and must survive."""
        from unittest.mock import patch
        ents = [{'name': n, 'count': 1, 'context': 'c', 'verbatim': True, 'label': 'LLM'}
                for n in ('Hospital', 'Mill', 'Holloway')]
        match = {'id': 'place:x:1', 'title': 'Mill', 'score': 100, 'ccodes': [], 'lng': 0, 'lat': 0,
                 'ambiguous': False}
        with patch('workbench.extraction.extract_places', return_value=ents), \
                patch('workbench.views._ner_reconcile_disambiguate', return_value={'Mill': match}):
            r = self._post({'rows': [{'key': 'a', 'text': 'the Hospital and the Mill at Holloway'}]})
        names = [p['name'] for p in r.json()['results'][0]['places']]
        self.assertNotIn('Hospital', names)     # common noun, nothing vouches for it
        self.assertNotIn('Mill', names)         # matched, but UNSCOPED — the match is not evidence
        self.assertIn('Holloway', names)        # unmatched, but not a common noun — kept and flagged

    def test_a_vouched_appellative_survives_inside_a_container(self):
        """Inside a county polygon a match IS evidence, so a real place called Mill comes through;
        outside one the index will happily match "Master" and present a title as a location."""
        from unittest.mock import patch
        ents = [{'name': n, 'count': 1, 'context': 'c', 'verbatim': True, 'label': 'LLM'}
                for n in ('Mill', "King's")]
        match = {'id': 'place:x:1', 'title': 'Mill', 'score': 100, 'ccodes': [], 'lng': 0, 'lat': 0,
                 'ambiguous': False}
        with patch('workbench.extraction.extract_places', return_value=ents), \
                patch('workbench.views._ner_reconcile_disambiguate',
                      return_value={'Mill': match, "King's": match}):
            r = self._post({'rows': [{'key': 'a', 'text': "the Mill on the King's land",
                                      'contained_in': ['ukhc:ESE']}]})
        names = [p['name'] for p in r.json()['results'][0]['places']]
        self.assertIn('Mill', names)
        self.assertNotIn("King's", names)       # a possessive does not escape the stop list

    def test_invented_and_lowercase_names_are_refused(self):
        """On a row naming no place the model falls back on the prompt's own vocabulary — "towns,
        villages, parishes, counties, rivers, mountains, seas" — and returns it as findings. Nothing
        it invented is a mention of anything. A word present but never capitalised is a common noun,
        not a toponym; a capitalised one is kept even unmatched, because that is how a genuine minor
        place the gazetteer lacks reaches the user."""
        from unittest.mock import patch
        text = 'obligation touching a chain of pearls, and lands at Honyngforde'
        ents = [{'name': n, 'count': 1, 'context': 'c', 'verbatim': True, 'label': 'LLM'}
                for n in ('towns', 'villages', 'chain', 'pearls', 'Honyngforde')]
        with patch('workbench.extraction.extract_places', return_value=ents), \
                patch('workbench.views._ner_reconcile_disambiguate', return_value={}):
            r = self._post({'rows': [{'key': 'a', 'text': text}]})
        names = [p['name'] for p in r.json()['results'][0]['places']]
        self.assertEqual(names, ['Honyngforde'])

    def test_one_bad_row_does_not_lose_the_batch(self):
        from unittest.mock import patch
        with patch('workbench.extraction.extract_places',
                   side_effect=[ValueError('boom'),
                                [{'name': 'Duxford', 'count': 1, 'context': 'c',
                                  'verbatim': True, 'label': 'LLM'}]]), \
                patch('workbench.views._ner_reconcile_disambiguate', return_value={}):
            # the second row's text must really contain the name — an unmatched name that is not in
            # the source is treated as invented, which is the point of the test above.
            r = self._post({'rows': [{'key': 'a', 'text': 'x'},
                                     {'key': 'b', 'text': 'lands in Duxford'}]})
        out = r.json()['results']
        self.assertTrue(out[0]['failed'])
        self.assertEqual([p['name'] for p in out[1]['places']], ['Duxford'])


class ExtractionUnitTests(TestCase):
    """workbench.extraction — the parts that do not need a model (place#211)."""

    def test_compound_span_split_only_when_parts_are_real(self):
        """qwen3 returns "Kingsbridge, Devon" as one span; it must become two names — but a comma
        in a name whose parts are NOT separately in the text has to survive intact."""
        text = 'John Joyce, shoemaker of Kingsbridge, Devon, v Robert Toly of East Allington, Devon.'
        self.assertEqual(extraction._split_compound('Kingsbridge, Devon', text),
                         ['Kingsbridge', 'Devon'])
        # "Toly, Cornwall" — Cornwall never appears, so splitting would invent a place.
        self.assertEqual(extraction._split_compound('Kingsbridge, Cornwall', text),
                         ['Kingsbridge, Cornwall'])
        self.assertEqual(extraction._split_compound('Saffron Walden', text), ['Saffron Walden'])

    def test_names_are_grounded_in_the_source_text(self):
        """A name the model invented is kept but flagged, so the caller can rank or drop it; a real
        one carries its true occurrence count."""
        from unittest.mock import patch
        text = 'A messuage in Duxford, and lands in Duxford, county Cambridge.'
        with patch('workbench.extraction._generate', return_value=['Duxford', 'Atlantis']):
            ents = extraction.extract_places(text)
        by_name = {e['name']: e for e in ents}
        self.assertEqual(by_name['Duxford']['count'], 2)
        self.assertTrue(by_name['Duxford']['verbatim'])
        self.assertIn('Duxford', by_name['Duxford']['context'])
        self.assertEqual(by_name['Atlantis']['count'], 0)
        self.assertFalse(by_name['Atlantis']['verbatim'])

    def test_unconfigured_host_raises_rather_than_calling_out(self):
        from django.test import override_settings
        with override_settings(OLLAMA_URL=''):
            with self.assertRaises(extraction.ExtractionUnavailable):
                extraction.extract_places('Rome and Venice')
