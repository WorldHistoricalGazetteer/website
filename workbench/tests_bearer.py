"""
The Workbench project API from another origin with a Bearer token (place#314).

What these prove, and the way each could fail:

  * the CORS preflight answers PUT and DELETE for an allow-listed origin only, and never offers
    credentials — a wildcard, a missing method or an ``Allow-Credentials`` header fails here;
  * the auth matrix: anonymous → JSON 401 (not a redirect); bad token → 401 even beside a valid
    session cookie; good token → the token's user, with no charge to the daily API limit;
  * CSRF stays enforced for the cookie path once the view is ``csrf_exempt`` for the token path;
  * ownership is unchanged: a token is exactly its user, no more;
  * the body cap is a JSON 413; the opaque doc-type is stored byte-for-byte, refused whole when
    stale, and never gets a live-editing token.

Run: venv/bin/python manage.py test workbench.tests_bearer --settings=whg.settings_localtest
"""
import json

from django.core.cache import caches
from django.test import Client, TestCase, override_settings
from django.urls import reverse

from api.models import APIToken, UserAPIProfile

from .models import WorkbenchProject, ProjectSnapshot, Team, TeamMember, ROLE_VIEWER
from .tests import make_user, snap

PELAGIOS = 'https://pelagios.org'
EVIL = 'https://evil.example'

LOCMEM = {
    'default': {'BACKEND': 'django.core.cache.backends.locmem.LocMemCache'},
    'throttle': {'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
                 'LOCATION': 'workbench-bearer-tests'},
}


@override_settings(WORKBENCH_CORS_ORIGINS=[PELAGIOS], WORKBENCH_TOKEN_RATE=1000,
                   WORKBENCH_BODY_MAX_BYTES=2 * 1024 * 1024, CACHES=LOCMEM)
class BearerBase(TestCase):
    def setUp(self):
        caches['throttle'].clear()
        self.alice = make_user('alice')
        self.bob = make_user('bob')
        self.alice_token = APIToken.objects.create(user=self.alice, key='alice-key')
        self.bob_token = APIToken.objects.create(user=self.bob, key='bob-key')
        self.anon = Client()  # no session, no CSRF enforcement

    # ── helpers ───────────────────────────────────────────────────────────────
    def bearer(self, token, origin=PELAGIOS):
        h = {'HTTP_AUTHORIZATION': f'Bearer {token.key}'}
        if origin:
            h['HTTP_ORIGIN'] = origin
        return h

    def create(self, token=None, client=None, doc_type='plato', snapshot=None, title=None, **extra):
        client = client or self.anon
        body = {'snapshot': snapshot if snapshot is not None else {'record': {'x': 1}},
                'doc_type': doc_type}
        if title:
            body['title'] = title
        headers = self.bearer(token) if token else {}
        headers.update(extra)
        return client.post(reverse('workbench:projects'), data=json.dumps(body),
                           content_type='application/json', **headers)

    def put(self, pid, body, token=None, client=None, **extra):
        client = client or self.anon
        headers = self.bearer(token) if token else {}
        headers.update(extra)
        return client.put(reverse('workbench:project-detail', args=[pid]), data=json.dumps(body),
                          content_type='application/json', **headers)


class PreflightTests(BearerBase):
    def test_preflight_from_an_allowed_origin_offers_put_and_delete(self):
        r = self.anon.options(reverse('workbench:projects'), HTTP_ORIGIN=PELAGIOS,
                              HTTP_ACCESS_CONTROL_REQUEST_METHOD='PUT',
                              HTTP_ACCESS_CONTROL_REQUEST_HEADERS='authorization,content-type')
        self.assertEqual(r.status_code, 204)
        self.assertEqual(r['Access-Control-Allow-Origin'], PELAGIOS)
        methods = {m.strip() for m in r['Access-Control-Allow-Methods'].split(',')}
        self.assertIn('POST', methods)
        self.assertIn('OPTIONS', methods)
        self.assertIn('Vary', r)
        self.assertIn('Origin', r['Vary'])
        headers = {h.strip().lower() for h in r['Access-Control-Allow-Headers'].split(',')}
        self.assertEqual(headers, {'authorization', 'content-type'})
        self.assertNotIn('Access-Control-Allow-Credentials', r)

    def test_preflight_on_the_detail_endpoint_offers_put_and_delete(self):
        j = self.create(self.alice_token).json()
        r = self.anon.options(reverse('workbench:project-detail', args=[j['id']]),
                              HTTP_ORIGIN=PELAGIOS, HTTP_ACCESS_CONTROL_REQUEST_METHOD='DELETE')
        self.assertEqual(r.status_code, 204)
        methods = {m.strip() for m in r['Access-Control-Allow-Methods'].split(',')}
        self.assertTrue({'GET', 'PUT', 'DELETE', 'OPTIONS'} <= methods, methods)
        self.assertNotIn('POST', methods)

    def test_preflight_from_another_origin_gets_no_cors_headers(self):
        r = self.anon.options(reverse('workbench:projects'), HTTP_ORIGIN=EVIL,
                              HTTP_ACCESS_CONTROL_REQUEST_METHOD='PUT')
        self.assertEqual(r.status_code, 204)
        self.assertNotIn('Access-Control-Allow-Origin', r)
        self.assertNotIn('Access-Control-Allow-Methods', r)

    def test_preflight_never_needs_a_token(self):
        # Browsers send no Authorization on a preflight; the answer must not depend on one.
        r = self.anon.options(reverse('workbench:teams'), HTTP_ORIGIN=PELAGIOS,
                              HTTP_ACCESS_CONTROL_REQUEST_METHOD='POST')
        self.assertEqual(r.status_code, 204)
        self.assertEqual(r['Access-Control-Allow-Origin'], PELAGIOS)

    def test_endpoints_outside_the_scope_answer_no_cors(self):
        # publish/ is session-only: the allow-listed origin gets nothing there.
        j = self.create(self.alice_token).json()
        r = self.anon.options(reverse('workbench:project-publish', args=[j['id']]),
                              HTTP_ORIGIN=PELAGIOS, HTTP_ACCESS_CONTROL_REQUEST_METHOD='POST')
        self.assertNotIn('Access-Control-Allow-Origin', r)

    def test_actual_responses_carry_the_origin_and_vary(self):
        r = self.anon.get(reverse('workbench:projects'), **self.bearer(self.alice_token))
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r['Access-Control-Allow-Origin'], PELAGIOS)
        self.assertIn('Origin', r['Vary'])
        self.assertIn('Retry-After', r['Access-Control-Expose-Headers'])
        self.assertNotIn('Access-Control-Allow-Credentials', r)

    def test_error_responses_carry_cors_too(self):
        # A 401 the browser cannot read is indistinguishable from a network failure.
        r = self.anon.get(reverse('workbench:projects'), HTTP_ORIGIN=PELAGIOS,
                          HTTP_AUTHORIZATION='Bearer nope')
        self.assertEqual(r.status_code, 401)
        self.assertEqual(r['Access-Control-Allow-Origin'], PELAGIOS)

    def test_shared_snapshot_is_readable_cross_origin(self):
        j = self.create(self.alice_token).json()
        r = self.anon.post(reverse('workbench:project-share', args=[j['id']]),
                           **self.bearer(self.alice_token))
        self.assertEqual(r.status_code, 200, r.content)
        tok = r.json()['token']
        r = self.anon.get(reverse('workbench:shared', args=[tok]), HTTP_ORIGIN=PELAGIOS)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r['Access-Control-Allow-Origin'], PELAGIOS)
        self.assertTrue(r.json()['read_only'])
        # ...and only from an allow-listed origin
        r = self.anon.get(reverse('workbench:shared', args=[tok]), HTTP_ORIGIN=EVIL)
        self.assertEqual(r.status_code, 200)
        self.assertNotIn('Access-Control-Allow-Origin', r)

    @override_settings(WORKBENCH_CORS_ORIGINS=[])
    def test_an_empty_allow_list_turns_cors_off(self):
        r = self.anon.options(reverse('workbench:projects'), HTTP_ORIGIN=PELAGIOS,
                              HTTP_ACCESS_CONTROL_REQUEST_METHOD='PUT')
        self.assertNotIn('Access-Control-Allow-Origin', r)


class AuthMatrixTests(BearerBase):
    def test_anonymous_gets_a_json_401_not_a_redirect(self):
        r = self.anon.get(reverse('workbench:projects'))
        self.assertEqual(r.status_code, 401)
        self.assertEqual(r['Content-Type'], 'application/json')
        self.assertEqual(r.json()['code'], 'unauthenticated')
        self.assertTrue(r['WWW-Authenticate'].startswith('Bearer'))

    def test_a_bad_token_is_401(self):
        r = self.anon.get(reverse('workbench:projects'), HTTP_AUTHORIZATION='Bearer wrong')
        self.assertEqual(r.status_code, 401)

    def test_the_token_must_be_in_the_header_not_the_query_string(self):
        r = self.anon.get(reverse('workbench:projects') + f'?token={self.alice_token.key}')
        self.assertEqual(r.status_code, 401)

    def test_a_bad_token_never_falls_back_to_the_session_cookie(self):
        c = Client()
        c.force_login(self.alice)
        self.assertEqual(c.get(reverse('workbench:projects')).status_code, 200)  # cookie works
        r = c.get(reverse('workbench:projects'), HTTP_AUTHORIZATION='Bearer wrong')
        self.assertEqual(r.status_code, 401)

    def test_the_token_decides_the_user_not_the_cookie(self):
        c = Client()
        c.force_login(self.alice)
        r = self.create(self.bob_token, client=c)
        self.assertEqual(r.status_code, 201, r.content)
        p = WorkbenchProject.objects.get(pk=r.json()['id'])
        self.assertEqual(p.created_by, self.bob)
        self.assertEqual(p.team.owner, self.bob)

    def test_a_good_token_is_not_charged_to_the_daily_api_limit(self):
        profile = UserAPIProfile.objects.create(user=self.alice, daily_limit=5000, daily_count=4999)
        r = self.anon.get(reverse('workbench:projects'), **self.bearer(self.alice_token))
        self.assertEqual(r.status_code, 200)
        profile.refresh_from_db()
        self.assertEqual(profile.daily_count, 4999)
        self.assertEqual(profile.total_count, 0)

    def test_an_exhausted_daily_limit_does_not_lock_the_workbench(self):
        UserAPIProfile.objects.create(user=self.alice, daily_limit=5000, daily_count=5000)
        r = self.anon.get(reverse('workbench:projects'), **self.bearer(self.alice_token))
        self.assertEqual(r.status_code, 200)

    def test_a_non_beta_token_holder_is_told_so(self):
        carol = make_user('carol', beta=False)
        tok = APIToken.objects.create(user=carol, key='carol-key')
        r = self.anon.get(reverse('workbench:projects'), **self.bearer(tok))
        self.assertEqual(r.status_code, 403)
        self.assertEqual(r.json()['code'], 'beta_required')

    def test_the_session_path_beta_gate_is_unchanged(self):
        carol = make_user('carol', beta=False)
        c = Client()
        c.force_login(carol)
        self.assertEqual(c.get(reverse('workbench:projects')).status_code, 404)

    def test_a_token_request_with_a_forged_disallowed_origin_is_refused(self):
        r = self.anon.get(reverse('workbench:projects'), **self.bearer(self.alice_token, origin=EVIL))
        self.assertEqual(r.status_code, 403)
        self.assertEqual(r.json()['code'], 'origin_not_allowed')

    def test_a_token_request_without_an_origin_is_fine(self):
        # curl and other non-browser clients send no Origin.
        r = self.anon.get(reverse('workbench:projects'), **self.bearer(self.alice_token, origin=None))
        self.assertEqual(r.status_code, 200)

    def test_an_inactive_user_token_is_refused(self):
        self.alice.is_active = False
        self.alice.save()
        r = self.anon.get(reverse('workbench:projects'), **self.bearer(self.alice_token))
        self.assertEqual(r.status_code, 401)

    def test_a_missing_project_is_a_json_404(self):
        import uuid
        r = self.anon.get(reverse('workbench:project-detail', args=[uuid.uuid4()]),
                          **self.bearer(self.alice_token))
        self.assertEqual(r.status_code, 404)
        self.assertEqual(r.json()['code'], 'not_found')

    def test_collab_token_is_token_reachable_and_json_on_refusal(self):
        r = self.anon.post(reverse('workbench:collab-token', args=['00000000-0000-0000-0000-000000000000']))
        self.assertEqual(r.status_code, 401)

    def test_last_used_is_recorded(self):
        self.assertIsNone(self.alice_token.last_used)
        self.anon.get(reverse('workbench:projects'), **self.bearer(self.alice_token))
        self.alice_token.refresh_from_db()
        self.assertIsNotNone(self.alice_token.last_used)


class CsrfTests(BearerBase):
    """The views are csrf_exempt so that the token path can skip CSRF; the cookie path must still
    enforce it. ``Client(enforce_csrf_checks=True)`` is the only way to prove that."""

    def test_a_cookie_post_without_a_csrf_token_is_refused(self):
        c = Client(enforce_csrf_checks=True)
        c.force_login(self.alice)
        r = self.create(client=c, doc_type='reconciliation', snapshot=snap())
        self.assertEqual(r.status_code, 403)
        self.assertEqual(r.json()['code'], 'csrf_failed')

    def test_a_cookie_post_with_a_csrf_token_succeeds(self):
        c = Client(enforce_csrf_checks=True)
        c.force_login(self.alice)
        # Obtain a CSRF cookie the way the page would, then echo it in the header.
        c.get(reverse('workbench:projects'))
        token = c.cookies.get('csrftoken')
        if token is None:
            from django.middleware.csrf import get_token
            from django.test import RequestFactory
            req = RequestFactory().get('/')
            req.session = {}
            token_value = get_token(req)
            c.cookies['csrftoken'] = token_value
        else:
            token_value = token.value
        r = self.create(client=c, doc_type='reconciliation', snapshot=snap(),
                        HTTP_X_CSRFTOKEN=token_value)
        self.assertEqual(r.status_code, 201, r.content)

    def test_a_cookie_get_is_still_fine_without_csrf(self):
        c = Client(enforce_csrf_checks=True)
        c.force_login(self.alice)
        self.assertEqual(c.get(reverse('workbench:projects')).status_code, 200)

    def test_a_bearer_post_needs_no_csrf_token(self):
        c = Client(enforce_csrf_checks=True)
        r = self.create(self.alice_token, client=c)
        self.assertEqual(r.status_code, 201, r.content)

    def test_a_cookie_request_from_another_origin_is_refused_even_for_get(self):
        # Belt and braces: the CORS answer would never allow credentials anyway, but a cookie must
        # not authenticate a request that says it comes from elsewhere.
        c = Client()
        c.force_login(self.alice)
        r = c.get(reverse('workbench:projects'), HTTP_ORIGIN=PELAGIOS)
        self.assertEqual(r.status_code, 401)
        r = c.get(reverse('workbench:projects'), HTTP_ORIGIN='http://testserver')
        self.assertEqual(r.status_code, 200)


class OwnershipTests(BearerBase):
    def test_another_users_token_cannot_read_or_write_or_delete(self):
        j = self.create(self.alice_token).json()
        url = reverse('workbench:project-detail', args=[j['id']])
        self.assertEqual(self.anon.get(url, **self.bearer(self.bob_token)).status_code, 403)
        r = self.put(j['id'], {'base_version': 1, 'snapshot': {'record': {}}}, token=self.bob_token)
        self.assertEqual(r.status_code, 403)
        self.assertEqual(self.anon.delete(url, **self.bearer(self.bob_token)).status_code, 403)
        self.assertTrue(WorkbenchProject.objects.filter(pk=j['id']).exists())

    def test_a_viewer_token_can_read_but_not_write(self):
        team = Team.objects.create(owner=self.alice, title='T', slug='t')
        TeamMember.objects.create(team=team, user=self.alice, role='owner')
        TeamMember.objects.create(team=team, user=self.bob, role=ROLE_VIEWER)
        r = self.anon.post(reverse('workbench:projects'),
                           data=json.dumps({'snapshot': {'a': 1}, 'doc_type': 'plato', 'team': team.id}),
                           content_type='application/json', **self.bearer(self.alice_token))
        self.assertEqual(r.status_code, 201, r.content)
        pid = r.json()['id']
        url = reverse('workbench:project-detail', args=[pid])
        self.assertEqual(self.anon.get(url, **self.bearer(self.bob_token)).status_code, 200)
        self.assertEqual(self.put(pid, {'base_version': 1, 'snapshot': {'a': 2}},
                                  token=self.bob_token).status_code, 403)
        self.assertEqual(self.anon.delete(url, **self.bearer(self.bob_token)).status_code, 403)

    def test_the_owner_token_can_delete(self):
        j = self.create(self.alice_token).json()
        url = reverse('workbench:project-detail', args=[j['id']])
        self.assertEqual(self.anon.delete(url, **self.bearer(self.alice_token)).status_code, 200)
        self.assertFalse(WorkbenchProject.objects.filter(pk=j['id']).exists())

    def test_team_members_need_the_owner_token(self):
        team = Team.objects.create(owner=self.alice, title='T', slug='t')
        TeamMember.objects.create(team=team, user=self.alice, role='owner')
        url = reverse('workbench:team-members', args=[team.id])
        r = self.anon.post(url, data=json.dumps({'identifier': 'bob'}),
                           content_type='application/json', **self.bearer(self.bob_token))
        self.assertEqual(r.status_code, 403)
        r = self.anon.post(url, data=json.dumps({'identifier': 'bob'}),
                           content_type='application/json', **self.bearer(self.alice_token))
        self.assertEqual(r.status_code, 200, r.content)
        self.assertTrue(TeamMember.objects.filter(team=team, user=self.bob).exists())


class RateLimitTests(BearerBase):
    @override_settings(WORKBENCH_TOKEN_RATE=2)
    def test_the_third_request_in_a_minute_is_429_with_retry_after(self):
        url = reverse('workbench:projects')
        self.assertEqual(self.anon.get(url, **self.bearer(self.alice_token)).status_code, 200)
        self.assertEqual(self.anon.get(url, **self.bearer(self.alice_token)).status_code, 200)
        r = self.anon.get(url, **self.bearer(self.alice_token))
        self.assertEqual(r.status_code, 429)
        self.assertGreater(int(r['Retry-After']), 0)
        self.assertEqual(r.json()['code'], 'rate_limited')
        self.assertEqual(r['Access-Control-Allow-Origin'], PELAGIOS)  # the browser can read it

    @override_settings(WORKBENCH_TOKEN_RATE=2)
    def test_budgets_are_per_user(self):
        url = reverse('workbench:projects')
        for _ in range(2):
            self.anon.get(url, **self.bearer(self.alice_token))
        self.assertEqual(self.anon.get(url, **self.bearer(self.alice_token)).status_code, 429)
        self.assertEqual(self.anon.get(url, **self.bearer(self.bob_token)).status_code, 200)

    @override_settings(WORKBENCH_TOKEN_RATE=2)
    def test_the_session_path_is_not_rate_limited(self):
        c = Client()
        c.force_login(self.alice)
        for _ in range(4):
            self.assertEqual(c.get(reverse('workbench:projects')).status_code, 200)


class SizeLimitTests(BearerBase):
    @override_settings(WORKBENCH_BODY_MAX_BYTES=500)
    def test_an_oversize_put_is_a_json_413(self):
        j = self.create(self.alice_token).json()
        r = self.put(j['id'], {'base_version': 1, 'snapshot': {'blob': 'x' * 600}},
                     token=self.alice_token)
        self.assertEqual(r.status_code, 413)
        self.assertEqual(r.json()['code'], 'body_too_large')
        self.assertEqual(r.json()['limit'], 500)
        self.assertEqual(r['Access-Control-Allow-Origin'], PELAGIOS)
        self.assertEqual(WorkbenchProject.objects.get(pk=j['id']).version, 1)

    @override_settings(WORKBENCH_BODY_MAX_BYTES=500)
    def test_an_oversize_create_is_a_json_413(self):
        r = self.create(self.alice_token, snapshot={'blob': 'x' * 600})
        self.assertEqual(r.status_code, 413)
        self.assertEqual(WorkbenchProject.objects.count(), 0)

    @override_settings(WORKBENCH_BODY_MAX_BYTES=500)
    def test_a_body_under_the_cap_is_accepted(self):
        r = self.create(self.alice_token, snapshot={'blob': 'x' * 100})
        self.assertEqual(r.status_code, 201)

    @override_settings(WORKBENCH_BODY_MAX_BYTES=500)
    def test_the_cap_applies_to_the_session_path_too(self):
        c = Client()
        c.force_login(self.alice)
        r = self.create(client=c, doc_type='reconciliation', snapshot=snap(blob='x' * 600))
        self.assertEqual(r.status_code, 413)


class PlatoDocTypeTests(BearerBase):
    SNAP = {'record': {'id': 'r1', 'steps': [{'n': 1, 'done': None}, {'n': 2.5}]},
            'work': {'decisions': {'c1': {'ok': True}}, 'notes': {}, 'empty': [], 'z': 0},
            'unicode': 'Κρίσις — ἀλήθεια'}

    def test_create_and_round_trip_byte_for_byte(self):
        r = self.create(self.alice_token, snapshot=self.SNAP, title='My workflow')
        self.assertEqual(r.status_code, 201, r.content)
        self.assertEqual(r.json()['doc_type'], 'plato')
        p = WorkbenchProject.objects.get(pk=r.json()['id'])
        self.assertEqual(p.title, 'My workflow')
        g = self.anon.get(reverse('workbench:project-detail', args=[p.id]),
                          **self.bearer(self.alice_token)).json()
        self.assertEqual(g['snapshot'], self.SNAP)  # nulls, floats, empties, unicode all intact
        self.assertEqual(g['doc_type'], 'plato')

    def test_the_server_does_not_read_a_title_out_of_the_snapshot(self):
        r = self.create(self.alice_token, snapshot={'title': 'inside', 'fileName': 'f.csv'})
        p = WorkbenchProject.objects.get(pk=r.json()['id'])
        # Create: no title sent → the default, never the one inside the snapshot...
        self.assertEqual(p.title, 'Untitled project')
        # ...but a PUT never derives one from the snapshot; an explicit title is honoured.
        self.put(p.id, {'base_version': 1, 'snapshot': {'title': 'changed'}}, token=self.alice_token)
        p.refresh_from_db()
        self.assertNotEqual(p.title, 'changed')
        self.put(p.id, {'base_version': 2, 'snapshot': {'title': 'x'}, 'title': 'explicit'},
                 token=self.alice_token)
        p.refresh_from_db()
        self.assertEqual(p.title, 'explicit')

    def test_fast_forward_put_commits(self):
        j = self.create(self.alice_token, snapshot=self.SNAP).json()
        r = self.put(j['id'], {'base_version': 1, 'snapshot': {'record': 2}}, token=self.alice_token)
        self.assertEqual(r.status_code, 200, r.content)
        self.assertEqual(r.json(), {'status': 'ok', 'version': 2, 'snapshot': None})
        self.assertEqual(WorkbenchProject.objects.get(pk=j['id']).snapshot, {'record': 2})
        self.assertTrue(ProjectSnapshot.objects.filter(project_id=j['id'], version=2).exists())

    def test_a_stale_put_is_refused_whole_with_the_current_snapshot(self):
        j = self.create(self.alice_token, snapshot=self.SNAP).json()
        self.put(j['id'], {'base_version': 1, 'snapshot': {'record': 'theirs'}}, token=self.alice_token)
        # A second client, still on version 1, pushes a disjoint edit that a merge WOULD accept.
        r = self.put(j['id'], {'base_version': 1, 'snapshot': {**self.SNAP, 'extra': 'mine'}},
                     token=self.alice_token)
        self.assertEqual(r.status_code, 409)
        body = r.json()
        self.assertEqual(body['status'], 'conflict')
        self.assertEqual(body['reason'], 'opaque')
        self.assertEqual(body['version'], 2)
        self.assertEqual(body['snapshot'], {'record': 'theirs'})
        self.assertNotIn('merged', body)
        p = WorkbenchProject.objects.get(pk=j['id'])
        self.assertEqual(p.version, 2)                       # nothing committed
        self.assertEqual(p.snapshot, {'record': 'theirs'})   # nothing merged in
        # The client merges and retries with the new base_version.
        r = self.put(j['id'], {'base_version': 2, 'snapshot': {'record': 'theirs', 'extra': 'mine'}},
                     token=self.alice_token)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()['version'], 3)

    def test_a_reconciliation_project_still_merges(self):
        """The companion: the opaque path must not have replaced the merge for every type."""
        j = self.create(self.alice_token, doc_type='reconciliation', snapshot=snap()).json()
        self.put(j['id'], {'base_version': 1, 'snapshot': snap(decisions={'a': 1})}, token=self.alice_token)
        r = self.put(j['id'], {'base_version': 1, 'snapshot': snap(decisions={'b': 2})}, token=self.alice_token)
        self.assertEqual(r.status_code, 200, r.content)
        self.assertEqual(r.json()['status'], 'merged')

    def test_a_non_object_snapshot_is_rejected(self):
        r = self.create(self.alice_token, snapshot=[1, 2, 3])
        self.assertEqual(r.status_code, 400)

    def test_live_editing_is_off(self):
        j = self.create(self.alice_token).json()
        with override_settings(HOCUSPOCUS_SECRET='test-secret'):
            r = self.anon.post(reverse('workbench:collab-token', args=[j['id']]),
                               **self.bearer(self.alice_token))
        self.assertEqual(r.status_code, 403)
        self.assertEqual(r.json()['code'], 'live_editing_disabled')
        # ...and stays on for the types that had it.
        k = self.create(self.alice_token, doc_type='reconciliation', snapshot=snap()).json()
        with override_settings(HOCUSPOCUS_SECRET='test-secret'):
            r = self.anon.post(reverse('workbench:collab-token', args=[k['id']]),
                               **self.bearer(self.alice_token))
        self.assertEqual(r.status_code, 200, r.content)

    def test_a_share_link_for_an_opaque_project_is_a_token_not_a_map_your_data_url(self):
        """SG, 2026-10-09: PLATO builds its own link from ``token``. The ``url`` the endpoint
        returns opens Map your Data, which cannot open a PLATO blob, so for an opaque type there is
        none; the anonymous fetch says what type it is, so a recipient's page can refuse it."""
        j = self.create(self.alice_token, snapshot=self.SNAP).json()
        r = self.anon.post(reverse('workbench:project-share', args=[j['id']]), **self.bearer(self.alice_token))
        self.assertEqual(r.status_code, 200, r.content)
        body = r.json()
        self.assertTrue(body['shared'])
        self.assertIsNone(body['url'])
        self.assertEqual(body['doc_type'], 'plato')
        g = self.anon.get(reverse('workbench:shared', args=[body['token']]), HTTP_ORIGIN=PELAGIOS)
        self.assertEqual(g.status_code, 200)
        self.assertEqual(g.json()['doc_type'], 'plato')
        self.assertEqual(g.json()['snapshot'], self.SNAP)
        # The companion: a Map your Data project still gets the page URL, and says so.
        k = self.create(self.alice_token, doc_type='reconciliation', snapshot=snap()).json()
        r = self.anon.post(reverse('workbench:project-share', args=[k['id']]), **self.bearer(self.alice_token))
        body = r.json()
        self.assertIn(f"/reconciliation/?shared={body['token']}", body['url'])
        self.assertEqual(body['doc_type'], 'reconciliation')
        g = self.anon.get(reverse('workbench:shared', args=[body['token']]))
        self.assertEqual(g.json()['doc_type'], 'reconciliation')

    def test_opaque_projects_are_listed_only_when_asked_for(self):
        self.create(self.alice_token, snapshot=self.SNAP)
        self.create(self.alice_token, doc_type='reconciliation', snapshot=snap())
        url = reverse('workbench:projects')
        default = self.anon.get(url, **self.bearer(self.alice_token)).json()['projects']
        self.assertEqual([p['doc_type'] for p in default], ['reconciliation'])
        plato = self.anon.get(url + '?doc_type=plato', **self.bearer(self.alice_token)).json()['projects']
        self.assertEqual([p['doc_type'] for p in plato], ['plato'])
        self.assertEqual(self.anon.get(url + '?doc_type=nonsense',
                                       **self.bearer(self.alice_token)).status_code, 400)

    def test_an_invite_does_not_deep_link_an_opaque_project_into_map_your_data(self):
        from unittest.mock import patch
        self.bob.email_confirmed = True   # has_verified_email() → the notify path runs
        self.bob.save()
        team = Team.objects.create(owner=self.alice, title='T', slug='t')
        TeamMember.objects.create(team=team, user=self.alice, role='owner')
        r = self.anon.post(reverse('workbench:projects'),
                           data=json.dumps({'snapshot': {'a': 1}, 'doc_type': 'plato', 'team': team.id}),
                           content_type='application/json', **self.bearer(self.alice_token))
        pid = r.json()['id']
        k = self.create(self.alice_token, doc_type='reconciliation', snapshot=snap()).json()
        url = reverse('workbench:team-members', args=[team.id])
        with patch('workbench.views._notify_team_member', return_value=True) as notify:
            r = self.anon.post(url, data=json.dumps({'identifier': 'bob', 'project_id': pid}),
                               content_type='application/json', **self.bearer(self.alice_token))
            self.assertEqual(r.status_code, 200, r.content)
            self.assertTrue(notify.called)
            self.assertIsNone(notify.call_args.args[4])       # no ?open=<plato id> link
            # The companion: a Map-your-Data project in the same team still deep-links.
            TeamMember.objects.filter(team=team, user=self.bob).delete()
            WorkbenchProject.objects.filter(pk=k['id']).update(team=team)
            self.anon.post(url, data=json.dumps({'identifier': 'bob', 'project_id': k['id']}),
                           content_type='application/json', **self.bearer(self.alice_token))
            self.assertEqual(notify.call_args.args[4], k['id'])
