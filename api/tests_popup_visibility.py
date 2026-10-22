"""Who the Atlas popup serves a contributed record to (place#330).

The popup fetches ``/entity/place:whg:<ds>:<src>/api?variant=popup`` with an
``X-CSRFToken`` header, so ``TokenQueryOrBearerAuthentication`` answers first
with a ``CSRFUser`` (anonymous, ``is_authenticated`` True) and DRF's
``request.user`` is NOT the signed-in session user: an owner was refused their
own private record (404) while the same session WITHOUT the header got it.

The fix (``api.views_entity._popup_viewer``) decides the popup for the session
user behind a CSRFUser, and changes nothing else. The reference rule is the
Record page's: ``get_visible_place_or_404(user, pk=...)`` for the signed-in
user. These tests hold the popup to it for every viewer class, dataset state
and header combination, and hold every anonymous response byte-identical to the
pre-change rule.

Run locally (never in the prod container):

    DB_NAME=<private> GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test api.tests_popup_visibility --settings=whg.settings_localtest --noinput

How these can fail:

* ``test_matrix_matches_the_record_page_rule`` compares every cell with the
  Record page's rule; without the fix the owner / collaborator / staff cells
  on private and embargoed data WITH the header are 404 (the matrix reports
  them: ``test_pre_change_rule_refuses_the_owner`` runs the same matrix with the
  pre-change viewer and asserts exactly those cells differ);
* ``test_a_leaky_viewer_is_caught`` runs the matrix with a helper mutated to
  treat any signed-in user as the owner and asserts the "other signed-in user"
  and "beta" cells are reported, so the matrix can see a leak;
* ``test_anonymous_is_byte_identical`` compares bodies and statuses with the
  pre-change viewer, and pairs them with a response that differs (the owner);
* the CSRFUser control proves the header really does turn ``request.user`` into
  a ``CSRFUser`` in this test client (it does not send the header by default).
"""
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.http import Http404
from django.test import TestCase

CSRF = {"HTTP_X_CSRFTOKEN": "test"}   # the test client does not enforce CSRF, so any value passes


class PopupVisibilityBase(TestCase):
    @classmethod
    def setUpTestData(cls):
        from api.models import GazetteerRegistryEntry
        from datasets.models import Dataset, DatasetUser
        from places.models import Place
        User = get_user_model()

        def user(name, **kw):
            return User.objects.create_user(username=f"pv_{name}", email=f"pv_{name}@example.org",
                                            password="pw", given_name=name.title(), surname="T",
                                            role=kw.pop("role", "normal"), **kw)
        cls.owner = user("owner")
        cls.collab = user("collab")
        cls.other = user("other")
        cls.beta = user("beta", role="beta_tester")
        cls.staff = user("staff", is_staff=True)
        cls.ds = {
            "public": Dataset.objects.create(owner=cls.owner, label="pv_public", title="Public",
                                             description="D", public=True),
            "private": Dataset.objects.create(owner=cls.owner, label="pv_private", title="Private",
                                              description="D", public=False),
            # Public by its flag, withheld by an unlifted embargo (place#162).
            "embargoed": Dataset.objects.create(owner=cls.owner, label="pv_embargoed", title="Embargoed",
                                                description="D", public=True),
        }
        GazetteerRegistryEntry.objects.update_or_create(id=f"whg:{cls.ds['embargoed'].id}", defaults=dict(
            name="Embargoed", namespace="whg", entry_class="dataset", status="embargoed",
            h3_coverage=[], temporal_extent=[], gazetteer_type="standard"))
        cls.place, cls.pid = {}, {}
        for state, ds in cls.ds.items():
            DatasetUser.objects.create(dataset_id=ds, user_id=cls.collab, role="member")
            cls.place[state] = Place.objects.create(title=f"{state} place", src_id=f"{state[0]}1",
                                                    dataset=ds, ccodes=[])
            cls.pid[state] = f"whg:{ds.id}:{state[0]}1"
        cls.viewers = {"anonymous": None, "other": cls.other, "beta": cls.beta,
                       "owner": cls.owner, "collaborator": cls.collab, "staff": cls.staff}

    def setUp(self):
        self.assertTrue(self.beta.can_access_beta)
        self.assertFalse(self.other.can_access_beta)

    def _get(self, viewer, state, header, popup=True):
        self.client.logout()
        if viewer is not None:
            self.client.force_login(viewer)
        params = {"variant": "popup"} if popup else {}
        return self.client.get(f"/entity/place:{self.pid[state]}/api", params, **(CSRF if header else {}))

    def _record_page_rule(self, viewer, state):
        """The Record page's answer for this viewer: may they see the record?"""
        from api.dataset_access import get_visible_place_or_404
        from django.contrib.auth.models import AnonymousUser
        try:
            get_visible_place_or_404(viewer or AnonymousUser(), pk=self.place[state].pk)
            return True
        except Http404:
            return False

    def _matrix(self):
        """``[(viewer, state, header, status, expected)]`` for every cell that
        disagrees with the Record page rule (signed-in viewers), or with the
        anonymous rules (no session)."""
        bad = []
        for vname, viewer in self.viewers.items():
            for state in self.ds:
                for header in (True, False):
                    status = self._get(viewer, state, header).status_code
                    if viewer is None:
                        # No session: with the header, a CSRFUser sees public data only;
                        # without it, a contributed id needs authentication at all.
                        expected = (200 if state == "public" else 404) if header else 401
                    else:
                        expected = 200 if self._record_page_rule(viewer, state) else 404
                    if status != expected:
                        bad.append((vname, state, header, status, expected))
        return bad


def _pre_change_viewer(request):
    """The rule before place#330: whatever DRF authenticated."""
    return request.user


class PopupVisibilityMatrixTests(PopupVisibilityBase):
    def test_the_header_makes_request_user_a_csrfuser(self):
        """Control: in this test client the header really does reach
        ``TokenQueryOrBearerAuthentication``, and a signed-in session request
        WITH it arrives as a CSRFUser (without it, as the session user)."""
        import api.views_entity as ve
        from api.authentication import CSRFUser
        seen = {}   # one entry per request (the beta check calls the helper again)
        real = ve._popup_viewer

        def spy(request):
            out = real(request)
            seen.setdefault(id(request), (request.user, out))
            return out
        with patch("api.views_entity._popup_viewer", side_effect=spy):
            self._get(self.owner, "public", header=True)
            self._get(self.owner, "public", header=False)
            self._get(None, "public", header=True)
        seen = list(seen.values())
        self.assertEqual(len(seen), 3, seen)
        self.assertIsInstance(seen[0][0], CSRFUser)
        self.assertEqual(seen[0][1], self.owner)          # the session user behind it
        self.assertNotIsInstance(seen[1][0], CSRFUser)    # SessionAuthentication's user
        self.assertEqual(seen[1][0], self.owner)
        self.assertEqual(seen[1][1], self.owner)
        self.assertIsInstance(seen[2][0], CSRFUser)       # no session: stays a CSRFUser
        self.assertIsInstance(seen[2][1], CSRFUser)

    def test_matrix_matches_the_record_page_rule(self):
        # Sanity: the reference rule itself separates the viewers.
        self.assertTrue(self._record_page_rule(self.owner, "private"))
        self.assertTrue(self._record_page_rule(self.collab, "embargoed"))
        self.assertTrue(self._record_page_rule(self.staff, "private"))
        self.assertFalse(self._record_page_rule(self.other, "private"))
        self.assertFalse(self._record_page_rule(self.beta, "embargoed"))
        self.assertTrue(self._record_page_rule(self.other, "public"))
        self.assertEqual(self._matrix(), [])

    def test_owner_gets_their_private_record_with_the_header(self):
        """The issue's own repro, as the Atlas asks (session + header)."""
        for state in ("private", "embargoed"):
            resp = self._get(self.owner, state, header=True)
            self.assertEqual(resp.status_code, 200, state)
            self.assertEqual(resp.json()["place_id"], self.pid[state])
            # ...and the other signed-in user, same request: the same 404 as a missing record.
            self.assertEqual(self._get(self.other, state, header=True).status_code, 404, state)

    def test_pre_change_rule_refuses_the_owner(self):
        """The matrix sees the bug: with the pre-change viewer, exactly the
        insiders' private/embargoed cells WITH the header disagree."""
        with patch("api.views_entity._popup_viewer", side_effect=_pre_change_viewer):
            bad = self._matrix()
        self.assertEqual(sorted((v, s, h) for v, s, h, _st, _ex in bad), sorted(
            (v, s, True) for v in ("owner", "collaborator", "staff") for s in ("private", "embargoed")))
        self.assertTrue(all(st == 404 and ex == 200 for *_x, st, ex in bad), bad)

    def test_a_leaky_viewer_is_caught(self):
        """Mutation: a helper that treats any signed-in session user as the owner.
        The matrix must report the outsiders' private/embargoed cells."""
        from django.contrib.auth import get_user
        owner = self.owner

        def leaky(request):
            u = get_user(request._request)
            return owner if getattr(u, "is_authenticated", False) else request.user
        with patch("api.views_entity._popup_viewer", side_effect=leaky):
            bad = self._matrix()
        cells = {(v, s, h) for v, s, h, _st, _ex in bad}
        for v in ("other", "beta"):
            for s in ("private", "embargoed"):
                self.assertIn((v, s, True), cells)
        self.assertTrue(all(st == 200 and ex == 404 for *_x, st, ex in bad), bad)

    def test_anonymous_is_byte_identical(self):
        """No session: status AND body equal to the pre-change rule's, for every
        dataset state, header or not. Paired with the owner, whose header response
        on private data does differ, so the comparison can fail."""
        cases = [(state, header) for state in self.ds for header in (True, False)]
        now = {c: self._get(None, *c) for c in cases}
        with patch("api.views_entity._popup_viewer", side_effect=_pre_change_viewer):
            before = {c: self._get(None, *c) for c in cases}
            owner_before = self._get(self.owner, "private", True)
        for c in cases:
            self.assertEqual((now[c].status_code, now[c].content), (before[c].status_code, before[c].content), c)
        self.assertEqual(now[("public", True)].status_code, 200)
        self.assertEqual(now[("private", True)].status_code, 404)
        owner_now = self._get(self.owner, "private", True)
        self.assertNotEqual((owner_now.status_code, owner_now.content),
                            (owner_before.status_code, owner_before.content))

    def test_non_popup_requests_are_unchanged(self):
        """Only ``variant=popup`` reads the session behind a CSRFUser: the plain
        LPF request with the header keeps today's answer (404 for the owner's
        private record), so no other caller's visibility moved."""
        self.assertEqual(self._get(self.owner, "private", True, popup=False).status_code, 404)
        self.assertEqual(self._get(self.owner, "private", False, popup=False).status_code, 200)
        self.assertEqual(self._get(self.owner, "private", True, popup=True).status_code, 200)

    def test_a_token_user_stays_the_token_user(self):
        """A Bearer token identifies its own user, whatever session rides along:
        the owner's session does not lend the token user the owner's records."""
        from api.models import APIToken
        APIToken.objects.create(user=self.other, key="pv-other-token")
        self.client.force_login(self.owner)
        url = f"/entity/place:{self.pid['private']}/api"
        resp = self.client.get(url, {"variant": "popup"}, HTTP_AUTHORIZATION="Bearer pv-other-token")
        self.assertEqual(resp.status_code, 404)
        # Control: the owner's own session, same request without the token.
        self.assertEqual(self.client.get(url, {"variant": "popup"}, **CSRF).status_code, 200)


class PopupViewerHelperTests(PopupVisibilityBase):
    def test_helper_cases(self):
        from django.test import RequestFactory
        from api.authentication import CSRFUser
        from api.views_entity import _popup_beta_viewer, _popup_viewer

        class Req:   # the DRF Request surface the helper reads
            def __init__(self, user, django_request):
                self.user, self._request = user, django_request

        def django_request(user=None):
            from importlib import import_module
            from django.conf import settings
            from django.contrib.auth import login
            r = RequestFactory().get("/")
            r.session = import_module(settings.SESSION_ENGINE).SessionStore()
            if user is not None:
                login(r, user, backend="django.contrib.auth.backends.ModelBackend")
            return r
        # CSRFUser + signed-in session: the session user.
        self.assertEqual(_popup_viewer(Req(CSRFUser(), django_request(self.owner))), self.owner)
        # CSRFUser + no session user: the CSRFUser itself.
        self.assertIsInstance(_popup_viewer(Req(CSRFUser(), django_request())), CSRFUser)
        # A real (token or session) user is never swapped for the session's.
        self.assertEqual(_popup_viewer(Req(self.other, django_request(self.owner))), self.other)
        # An inactive session user does not count.
        self.owner.is_active = False
        self.owner.save(update_fields=["is_active"])
        try:
            self.assertIsInstance(_popup_viewer(Req(CSRFUser(), django_request(self.owner))), CSRFUser)
        finally:
            self.owner.is_active = True
            self.owner.save(update_fields=["is_active"])
        # Beta check on top: beta session -> beta; non-beta -> None.
        self.assertEqual(_popup_beta_viewer(Req(CSRFUser(), django_request(self.beta))), self.beta)
        self.assertIsNone(_popup_beta_viewer(Req(CSRFUser(), django_request(self.other))))
        self.assertIsNone(_popup_beta_viewer(Req(CSRFUser(), django_request())))


class PlaceModalPreviewTests(PopupVisibilityBase):
    """The Atlas place modal (``/entity/place:<id>/preview``, gazetteerInteraction.js
    ``openPlaceModal``) sends the same header: same symptom, same fix, same rule."""

    def _preview(self, viewer, state, header):
        self.client.logout()
        if viewer is not None:
            self.client.force_login(viewer)
        return self.client.get(f"/entity/place:{self.pid[state]}/preview", **(CSRF if header else {}))

    def test_preview_follows_the_record_page_rule(self):
        bad = []
        for vname, viewer in self.viewers.items():
            if viewer is None:
                continue
            for state in self.ds:
                for header in (True, False):
                    status = self._preview(viewer, state, header).status_code
                    expected = 200 if self._record_page_rule(viewer, state) else 404
                    if status != expected:
                        bad.append((vname, state, header, status, expected))
        self.assertEqual(bad, [])
        resp = self._preview(self.owner, "private", True)
        self.assertIn(b"private place", resp.content)

    def test_preview_before_the_fix_refused_the_owner_and_anonymous_is_unchanged(self):
        with patch("api.views_entity._popup_viewer", side_effect=_pre_change_viewer):
            self.assertEqual(self._preview(self.owner, "private", True).status_code, 404)
            before = {(s, h): self._preview(None, s, h) for s in self.ds for h in (True, False)}
        for (s, h), resp in before.items():
            now = self._preview(None, s, h)
            self.assertEqual((now.status_code, now.content), (resp.status_code, resp.content), (s, h))
        self.assertEqual(before[("public", True)].status_code, 200)
        self.assertEqual(before[("private", True)].status_code, 404)
        self.assertEqual(self._preview(self.other, "private", True).status_code, 404)
