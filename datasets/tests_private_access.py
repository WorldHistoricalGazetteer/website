"""Private datasets are private (decision 2026-09-30), and user-derived map attribution
is HTML-escaped (maplibre GHSA-jrc7-96c5-q579).

DB-free: every test is a SimpleTestCase and the ORM is mocked, so this module runs on a
machine with no reachable Postgres:

    python3 manage.py test datasets.tests_private_access

Each denial test is paired with an allowed case through the same code path, so a test
that passes because the path was never reached (or always 404s) would fail its pair.
"""
import json
from unittest import mock

from django.contrib.auth.models import AnonymousUser
from django.http import Http404
from django.test import RequestFactory, SimpleTestCase

from datasets.models import Dataset

OWNER_ID = 10
COLLAB_ID = 20
CO_OWNER_ID = 30
STRANGER_ID = 99


def make_user(uid, staff=False, superuser=False, admin_group=False):
    user = mock.Mock()
    user.id = uid
    user.pk = uid
    user.is_authenticated = True
    user.is_anonymous = False
    user.is_staff = staff
    user.is_superuser = superuser
    user.groups.filter.return_value.exists.return_value = admin_group
    return user


def id_set_manager(ids):
    """A stand-in for a User queryset: .filter(id=x).exists() is True iff x in ids."""
    qs = mock.Mock()
    qs.filter.side_effect = lambda **kw: mock.Mock(exists=mock.Mock(return_value=kw.get("id") in ids))
    return qs


class _DatasetRolesMixin:
    """Patch Dataset.owners / Dataset.collaborators (DB-backed properties) for each test."""

    def setUp(self):
        super().setUp()
        p_owners = mock.patch.object(Dataset, "owners", new_callable=mock.PropertyMock,
                                     return_value=id_set_manager({OWNER_ID, CO_OWNER_ID}))
        p_collabs = mock.patch.object(Dataset, "collaborators", new_callable=mock.PropertyMock,
                                      return_value=id_set_manager({COLLAB_ID}))
        p_owners.start()
        p_collabs.start()
        self.addCleanup(p_owners.stop)
        self.addCleanup(p_collabs.stop)

    @staticmethod
    def dataset(public, pk=1):
        return Dataset(id=pk, label="ds_test", title="T", public=public, owner_id=OWNER_ID)


class UserCanViewTests(_DatasetRolesMixin, SimpleTestCase):
    def test_truth_table(self):
        cases = [
            # (who, user, public, expected)
            ("anon", AnonymousUser(), True, True),
            ("anon", AnonymousUser(), False, False),
            ("None", None, False, False),
            ("owner (FK)", make_user(OWNER_ID), False, True),
            ("co-owner", make_user(CO_OWNER_ID), False, True),
            ("collaborator", make_user(COLLAB_ID), False, True),
            ("staff", make_user(STRANGER_ID, staff=True), False, True),
            ("superuser", make_user(STRANGER_ID, superuser=True), False, True),
            ("whg_admins", make_user(STRANGER_ID, admin_group=True), False, True),
            ("stranger", make_user(STRANGER_ID), False, False),
            ("stranger", make_user(STRANGER_ID), True, True),
        ]
        for who, user, public, expected in cases:
            with self.subTest(who=who, public=public):
                self.assertIs(self.dataset(public).user_can_view(user), expected)

    def test_owner_found_via_owners_not_only_fk(self):
        # A co-owner is not the FK owner; allowed only because owners contains them.
        ds = self.dataset(False)
        self.assertNotEqual(ds.owner_id, CO_OWNER_ID)
        self.assertTrue(ds.user_can_view(make_user(CO_OWNER_ID)))


class MapdataGateTests(_DatasetRolesMixin, SimpleTestCase):
    def setUp(self):
        super().setUp()
        self.rf = RequestFactory()

    def _call(self, ds, user, category="datasets", **kw):
        from utils import mapdata as md
        req = self.rf.get(f"/mapdata/{category}/1/")
        req.user = user
        with mock.patch.object(md.Dataset.objects, "filter") as flt, \
                mock.patch.object(md, "generate_mapdata", return_value={"table": [], "metadata": {}}) as gen:
            flt.return_value.first.return_value = ds
            resp = md.mapdata(req, category, 1, **kw)
        return resp, gen

    def test_anon_private_404_and_no_generation(self):
        resp, gen = self._call(self.dataset(False), AnonymousUser())
        self.assertEqual(resp.status_code, 404)
        gen.assert_not_called()

    def test_anon_private_carousel_404(self):
        resp, gen = self._call(self.dataset(False), AnonymousUser(), carousel=True)
        self.assertEqual(resp.status_code, 404)
        gen.assert_not_called()

    def test_missing_dataset_same_404(self):
        resp, gen = self._call(None, AnonymousUser())
        self.assertEqual(resp.status_code, 404)
        gen.assert_not_called()

    def test_stranger_private_404(self):
        resp, gen = self._call(self.dataset(False), make_user(STRANGER_ID))
        self.assertEqual(resp.status_code, 404)
        gen.assert_not_called()

    def test_anon_public_served(self):
        resp, gen = self._call(self.dataset(True), AnonymousUser())
        self.assertEqual(resp.status_code, 200)
        gen.assert_called_once()

    def test_owner_private_served(self):
        resp, gen = self._call(self.dataset(False), make_user(OWNER_ID))
        self.assertEqual(resp.status_code, 200)
        gen.assert_called_once()

    def test_collections_not_gated(self):
        # Collections stay viewable by link: no dataset lookup, generation proceeds.
        resp, gen = self._call(self.dataset(False), AnonymousUser(), category="collections")
        self.assertEqual(resp.status_code, 200)
        gen.assert_called_once()


class DatasetViewGateTests(_DatasetRolesMixin, SimpleTestCase):
    def setUp(self):
        super().setUp()
        self.rf = RequestFactory()

    def _get_object(self, ds, user):
        from datasets import views
        view = views.DatasetPlacesView()
        req = self.rf.get("/datasets/1/places")
        req.user = user
        view.setup(req, id=1)
        with mock.patch("datasets.utils.get_object_or_404", return_value=ds):
            return view.get_object()

    def test_places_view_anon_private_404(self):
        with self.assertRaises(Http404):
            self._get_object(self.dataset(False), AnonymousUser())

    def test_places_view_full_dispatch_anon_private_404(self):
        # Through as_view()/get(), not just get_object(), so nothing before it can serve.
        from datasets import views
        req = self.rf.get("/datasets/1/places")
        req.user = AnonymousUser()
        with mock.patch("datasets.utils.get_object_or_404", return_value=self.dataset(False)):
            with self.assertRaises(Http404):
                views.DatasetPlacesView.as_view()(req, id=1)

    def test_places_view_anon_public_allowed(self):
        ds = self.dataset(True)
        self.assertIs(self._get_object(ds, AnonymousUser()), ds)

    def test_places_view_collaborator_private_allowed(self):
        ds = self.dataset(False)
        self.assertIs(self._get_object(ds, make_user(COLLAB_ID)), ds)

    def test_citation_anon_private_404(self):
        from datasets import views
        req = self.rf.get("/datasets/1/citation")
        req.user = AnonymousUser()
        with mock.patch("datasets.utils.get_object_or_404", return_value=self.dataset(False)):
            with self.assertRaises(Http404):
                views.dataset_citation(req, id=1)

    def test_ds_list_anon_private_404(self):
        from datasets import views
        req = self.rf.get("/datasets/ds_test/places/")
        req.user = AnonymousUser()
        with mock.patch("datasets.utils.get_object_or_404", return_value=self.dataset(False)):
            with self.assertRaises(Http404):
                views.ds_list(req, label="ds_test")

    def test_download_file_anon_private_404(self):
        from datasets import utils
        req = self.rf.get("/datasets/1/file/")
        req.user = AnonymousUser()
        with mock.patch("datasets.utils.get_object_or_404", return_value=self.dataset(False)):
            with self.assertRaises(Http404):
                utils.download_file(req, id=1)

    def test_status_view_logged_in_stranger_private_404(self):
        from datasets import views
        view = views.DatasetStatusView()
        req = self.rf.get("/datasets/1/status")
        req.user = make_user(STRANGER_ID)
        view.setup(req, id=1)
        with mock.patch("datasets.utils.get_object_or_404", return_value=self.dataset(False)):
            with self.assertRaises(Http404):
                view.get_object()


class AttributionEscapingTests(SimpleTestCase):
    PAYLOAD = '<img src=x onerror=alert(1)>'

    def test_title_and_authors_escaped(self):
        from utils.mapdata import attribution_from_csl
        out = attribution_from_csl({
            "title": self.PAYLOAD,
            "author": [{"literal": self.PAYLOAD}],
            "issued": {"date-parts": [["<b>2020</b>"]]},
        })
        self.assertNotIn("<", out)
        self.assertNotIn(">", out)
        self.assertIn("&lt;img src=x onerror=alert(1)&gt;", out)
        self.assertIn("&lt;b&gt;2020&lt;/b&gt;", out)
        self.assertIs(type(out), str)

    def test_family_given_escaped(self):
        from utils.mapdata import attribution_from_csl
        out = attribution_from_csl({
            "title": "Ordinary & Title",
            "author": [{"family": '"><script>x</script>', "given": "<A"}],
        })
        self.assertNotIn("<script>", out)
        self.assertIn("&quot;&gt;&lt;script&gt;", out)
        self.assertIn("&lt;.", out)             # given-name initial, escaped
        self.assertIn("Ordinary &amp; Title", out)

    def test_plain_text_unchanged(self):
        from utils.mapdata import attribution_from_csl
        out = attribution_from_csl({
            "title": "Gazetteer of Ireland",
            "author": [{"family": "Southall", "given": "Humphrey"}],
            "issued": {"date-parts": [[2021]]},
        })
        self.assertEqual(out, "Southall H., 2021 – Gazetteer of Ireland")

    def test_truncation_does_not_split_an_entity(self):
        from utils.mapdata import attribution_from_csl
        # "&" at the cut boundary: truncation happens before escaping.
        title = "x" * 58 + "&&&&&"
        out = attribution_from_csl({"title": title}, max_title_length=60)
        self.assertTrue(out.endswith("x" * 58 + "&amp;…"), out)


class DatasetDeleteGateTests(_DatasetRolesMixin, SimpleTestCase):
    """Deleting a dataset: owners and staff/admins only (it had no check at all before
    2026-09-30), and the cleanup runs on a normal POST (Django 4 routes POST via form_valid)."""

    def test_user_can_manage_truth_table(self):
        ds = self.dataset(public=True)
        self.assertFalse(ds.user_can_manage(AnonymousUser()))
        self.assertFalse(ds.user_can_manage(None))
        self.assertFalse(ds.user_can_manage(make_user(STRANGER_ID)))
        self.assertFalse(ds.user_can_manage(make_user(COLLAB_ID)))  # collaborators can't delete
        self.assertTrue(ds.user_can_manage(make_user(OWNER_ID)))
        self.assertTrue(ds.user_can_manage(make_user(CO_OWNER_ID)))
        self.assertTrue(ds.user_can_manage(make_user(STRANGER_ID, staff=True)))
        self.assertTrue(ds.user_can_manage(make_user(STRANGER_ID, admin_group=True)))

    def _view(self, user, ds):
        from datasets.views import DatasetDeleteView
        view = DatasetDeleteView()
        view.request = RequestFactory().post(f"/datasets/{ds.id}/delete")
        view.request.user = user
        view.kwargs = {"pk": ds.id}
        return view

    def test_get_object_404_for_stranger_even_on_public_dataset(self):
        ds = self.dataset(public=True)
        with mock.patch("django.views.generic.detail.SingleObjectMixin.get_object", return_value=ds):
            with self.assertRaises(Http404):
                self._view(make_user(STRANGER_ID), ds).get_object()
            with self.assertRaises(Http404):
                self._view(make_user(COLLAB_ID), ds).get_object()

    def test_get_object_allowed_for_owner(self):
        ds = self.dataset(public=False)
        with mock.patch("django.views.generic.detail.SingleObjectMixin.get_object", return_value=ds):
            self.assertIs(self._view(make_user(OWNER_ID), ds).get_object(), ds)

    def test_form_valid_runs_cleanup_then_deletes(self):
        ds = self.dataset(public=True)
        ds.ds_status = "indexed"
        view = self._view(make_user(OWNER_ID), ds)
        view.object = ds
        with mock.patch("datasets.views.dataset_file_delete") as files, \
             mock.patch("datasets.views.removePlacesFromIndex") as unindex, \
             mock.patch.object(Dataset, "placeids", new_callable=mock.PropertyMock, return_value=[1, 2]), \
             mock.patch.object(Dataset, "delete") as dsdel, \
             mock.patch("datasets.views.django_reverse", return_value="/dashboard/"):
            resp = view.form_valid(form=None)
        files.assert_called_once_with(ds)
        unindex.assert_called_once()
        self.assertEqual(unindex.call_args[0][2], [1, 2])
        dsdel.assert_called_once()
        self.assertEqual(resp.status_code, 302)


# ---------------------------------------------------------------------------
# Dataset WRITE endpoints (2026-09-30). Rules: "edit" = Dataset.user_can_edit (owners,
# collaborators, staff/admins); "manage" = Dataset.user_can_manage (NOT collaborators).
# Pages: anonymous -> login redirect, not permitted -> 404. AJAX: anonymous -> 401 JSON,
# not permitted -> 404 JSON (same as missing). Each denial has an allowed pair through the
# same code path, asserting the write (or the next step) actually happens.
# ---------------------------------------------------------------------------

class _Reached(Exception):
    """Raised by a patched internal to prove the gate let the request through."""


def _tasks_manager(task_ids, task_name="align_wdlocal"):
    """Stand-in for Dataset.tasks: .filter(task_id=x) -> exists()/first() iff x in task_ids."""
    def _filter(**kw):
        hit = kw.get("task_id") in task_ids
        task = mock.Mock(task_id=kw.get("task_id"), task_name=task_name)
        return mock.Mock(exists=mock.Mock(return_value=hit),
                         first=mock.Mock(return_value=task if hit else None))
    qs = mock.Mock()
    qs.filter.side_effect = _filter
    return qs


class _WriteGateBase(_DatasetRolesMixin, SimpleTestCase):
    TID = "task-abc"

    def setUp(self):
        super().setUp()
        self.rf = RequestFactory()
        p_tasks = mock.patch.object(Dataset, "tasks", new_callable=mock.PropertyMock,
                                    return_value=_tasks_manager({self.TID}))
        p_tasks.start()
        self.addCleanup(p_tasks.stop)

    def req(self, method, path, user, data=None, **extra):
        r = getattr(self.rf, method)(path, data or {}, **extra)
        r.user = user
        return r

    def with_ds(self, ds):
        """Patch the dataset lookup behind every get_*_dataset_or_404 helper."""
        return mock.patch("datasets.utils.get_object_or_404", return_value=ds)

    def assertLoginRedirect(self, resp):
        self.assertEqual(resp.status_code, 302)
        self.assertIn("login", resp.url)

    def assertJsonStatus(self, resp, status):
        self.assertEqual(resp.status_code, status)
        json.loads(resp.content)  # is JSON, not an HTML login page


class UserCanEditTests(_DatasetRolesMixin, SimpleTestCase):
    def test_truth_table(self):
        ds = self.dataset(public=True)
        cases = [
            ("anon", AnonymousUser(), False),
            ("None", None, False),
            ("stranger", make_user(STRANGER_ID), False),
            ("collaborator", make_user(COLLAB_ID), True),
            ("owner", make_user(OWNER_ID), True),
            ("co-owner", make_user(CO_OWNER_ID), True),
            ("staff", make_user(STRANGER_ID, staff=True), True),
            ("whg_admins", make_user(STRANGER_ID, admin_group=True), True),
        ]
        for who, user, expected in cases:
            with self.subTest(who=who):
                self.assertIs(ds.user_can_edit(user), expected)


class ReviewGateTests(_WriteGateBase):
    def _call(self, user, ds, tid=None, method="get"):
        from datasets import views
        tid = tid or self.TID
        r = self.req(method, f"/datasets/1/review/{tid}/pass1", user)
        with self.with_ds(ds), \
                mock.patch("datasets.views._get_task_details", side_effect=_Reached) as details:
            try:
                return views.review(r, dsid=1, tid=tid, passnum="pass1"), details
            except _Reached:
                return "reached", details

    def test_anon_redirected_to_login(self):
        resp, details = self._call(AnonymousUser(), self.dataset(True))
        self.assertLoginRedirect(resp)
        details.assert_not_called()

    def test_stranger_on_public_dataset_404_get_and_post(self):
        for method in ("get", "post"):
            with self.subTest(method=method), self.assertRaises(Http404):
                self._call(make_user(STRANGER_ID), self.dataset(True), method=method)

    def test_task_of_another_dataset_404_even_for_owner(self):
        with self.assertRaises(Http404):
            self._call(make_user(OWNER_ID), self.dataset(False), tid="someone-elses-task")

    def test_collaborator_reaches_review(self):
        resp, details = self._call(make_user(COLLAB_ID), self.dataset(False), method="post")
        self.assertEqual(resp, "reached")
        details.assert_called_once_with(self.TID)


class DsReconGateTests(_WriteGateBase):
    def _call(self, user, ds):
        from datasets import views
        with self.with_ds(ds):
            return views.ds_recon(self.req("get", "/datasets/1/recon/", user), pk=1)

    def test_anon_redirected_to_login(self):
        self.assertLoginRedirect(self._call(AnonymousUser(), self.dataset(True)))

    def test_stranger_404(self):
        with self.assertRaises(Http404):
            self._call(make_user(STRANGER_ID), self.dataset(True))

    def test_collaborator_allowed(self):
        # GET (non-POST) is the view's own "back to the reconcile tab" redirect.
        resp = self._call(make_user(COLLAB_ID), self.dataset(False))
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(resp.url, "/datasets/1/reconcile")


class TaskDeleteGateTests(_WriteGateBase):
    def _call(self, user, ds, method="post"):
        from datasets import views
        tr = mock.Mock(task_args='"(1,)"')
        with self.with_ds(ds), \
                mock.patch.object(views.TaskResult, "objects") as trs, \
                mock.patch("datasets.tasks.delete_reconciliation_task") as deleter:
            trs.get.return_value = tr
            deleter.delay.return_value = mock.Mock(id="del-1")
            resp = views.task_delete(self.req(method, f"/datasets/task-delete/{self.TID}/task", user),
                                     tid=self.TID, scope="task")
        return resp, deleter, tr

    def test_get_not_allowed(self):
        resp, deleter, tr = self._call(make_user(OWNER_ID), self.dataset(True), method="get")
        self.assertEqual(resp.status_code, 405)
        deleter.delay.assert_not_called()

    def test_anon_401(self):
        resp, deleter, tr = self._call(AnonymousUser(), self.dataset(True))
        self.assertJsonStatus(resp, 401)
        deleter.delay.assert_not_called()
        tr.save.assert_not_called()

    def test_stranger_and_collaborator_404(self):
        for uid in (STRANGER_ID, COLLAB_ID):
            with self.subTest(uid=uid):
                resp, deleter, tr = self._call(make_user(uid), self.dataset(True))
                self.assertJsonStatus(resp, 404)
                deleter.delay.assert_not_called()
                tr.save.assert_not_called()

    def test_owner_starts_deletion(self):
        resp, deleter, tr = self._call(make_user(OWNER_ID), self.dataset(False))
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(json.loads(resp.content)["status"], "started")
        deleter.delay.assert_called_once()
        self.assertEqual(deleter.delay.call_args.kwargs["dsid"], 1)
        tr.save.assert_called_once()


class MatchUndoGateTests(_WriteGateBase):
    def _call(self, user, ds, tid=None):
        from datasets import views
        tid = tid or self.TID
        place = mock.Mock()
        r = self.req("get", f"/datasets/match-undo/1/{tid}/5", user, HTTP_REFERER="/back/")
        with self.with_ds(ds), \
                mock.patch("datasets.views.get_object_or_404", return_value=place) as place_get, \
                mock.patch("datasets.views.PlaceGeom") as pg, \
                mock.patch("datasets.views.PlaceLink") as pl, \
                mock.patch("datasets.views.Hit") as hit:
            resp = views.match_undo(r, ds=1, tid=tid, pid=5)
        return resp, pg, place, place_get

    def test_anon_redirected_to_login(self):
        resp, pg, place, _ = self._call(AnonymousUser(), self.dataset(True))
        self.assertLoginRedirect(resp)
        pg.objects.filter.assert_not_called()

    def test_stranger_404_before_any_delete(self):
        with mock.patch("datasets.views.PlaceGeom") as pg:
            with self.assertRaises(Http404):
                self._call(make_user(STRANGER_ID), self.dataset(True))

    def test_task_of_another_dataset_404(self):
        with self.assertRaises(Http404):
            self._call(make_user(OWNER_ID), self.dataset(True), tid="someone-elses-task")

    def test_collaborator_undoes(self):
        resp, pg, place, place_get = self._call(make_user(COLLAB_ID), self.dataset(False))
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(resp.url, "/back/")
        pg.objects.filter.assert_called_once_with(task_id=self.TID, place_id=5)
        place.save.assert_called_once()
        self.assertEqual(place.review_wd, 0)
        # the place lookup is tied to this dataset
        self.assertEqual(place_get.call_args.kwargs.get("dataset_id"), "ds_test")


class CollabGateTests(_WriteGateBase):
    def _delete(self, user, ds):
        from datasets import views
        du = mock.Mock()
        with self.with_ds(ds), mock.patch("datasets.views.get_object_or_404", return_value=du):
            resp = views.collab_delete(self.req("get", "/datasets/collab-delete/20/1/1", user),
                                       uid=COLLAB_ID, dsid=1, v="1")
        return resp, du

    def test_delete_anon_redirected(self):
        resp, du = self._delete(AnonymousUser(), self.dataset(True))
        self.assertLoginRedirect(resp)
        du.delete.assert_not_called()

    def test_delete_collaborator_and_stranger_404(self):
        for uid in (COLLAB_ID, STRANGER_ID):
            with self.subTest(uid=uid), self.assertRaises(Http404):
                self._delete(make_user(uid), self.dataset(True))

    def test_delete_owner_allowed(self):
        resp, du = self._delete(make_user(CO_OWNER_ID), self.dataset(False))
        du.delete.assert_called_once()
        self.assertEqual(resp.url, "/datasets/1/collab")

    def _add(self, user, ds):
        from datasets import views
        r = self.req("post", "/datasets/collab-add/1/1/", user, {"username": "bob", "role": "member"})
        with self.with_ds(ds), \
                mock.patch("datasets.views.messages"), \
                mock.patch.object(views.User, "objects") as users, \
                mock.patch.object(views.DatasetUser, "objects") as dus:
            dus.get_or_create.return_value = (mock.Mock(), True)
            resp = views.collab_add(r, dsid=1, v="1")
        return resp, dus

    def test_add_stranger_and_collaborator_404(self):
        for uid in (COLLAB_ID, STRANGER_ID):
            with self.subTest(uid=uid), self.assertRaises(Http404):
                self._add(make_user(uid), self.dataset(True))

    def test_add_owner_allowed(self):
        resp, dus = self._add(make_user(OWNER_ID), self.dataset(True))
        dus.get_or_create.assert_called_once()
        self.assertEqual(resp.url, "/datasets/1/collab")


class AjaxManageGateTests(_WriteGateBase):
    """update_vis_parameters, update_volunteers_text, toggle_volunteers: manage, JSON errors."""

    def _post(self, fn, user, ds, data):
        with self.with_ds(ds), mock.patch.object(Dataset, "save") as save:
            resp = fn(self.req("post", "/x/", user, data))
        return resp, save

    def endpoints(self):
        from datasets import views, utils
        return [
            ("update_vis_parameters", views.update_vis_parameters, {"ds_id": "1", "checked": "true"}),
            ("update_volunteers_text", views.update_volunteers_text,
             {"dataset_id": "1", "volunteers_text": "help wanted"}),
            ("toggle_volunteers", utils.toggle_volunteers, {"dataset_id": "1", "is_checked": "true"}),
        ]

    def test_anon_401(self):
        for name, fn, data in self.endpoints():
            with self.subTest(name):
                resp, save = self._post(fn, AnonymousUser(), self.dataset(True), data)
                self.assertJsonStatus(resp, 401)
                save.assert_not_called()

    def test_stranger_and_collaborator_404(self):
        for name, fn, data in self.endpoints():
            for uid in (STRANGER_ID, COLLAB_ID):
                with self.subTest(name, uid=uid):
                    resp, save = self._post(fn, make_user(uid), self.dataset(True), data)
                    self.assertJsonStatus(resp, 404)
                    save.assert_not_called()

    def test_owner_writes(self):
        for name, fn, data in self.endpoints():
            with self.subTest(name):
                ds = self.dataset(False)
                resp, save = self._post(fn, make_user(OWNER_ID), ds, data)
                self.assertEqual(resp.status_code, 200)
                save.assert_called_once()
        # spot-check the values actually written
        ds = self.dataset(False)
        from datasets import views
        self._post(views.update_volunteers_text, make_user(OWNER_ID), ds,
                   {"dataset_id": "1", "volunteers_text": "help wanted"})
        self.assertEqual(ds.volunteers_text, "help wanted")

    def test_get_not_allowed(self):
        from datasets import utils, views
        for fn in (utils.toggle_volunteers, views.update_volunteers_text, views.update_vis_parameters):
            with self.subTest(fn.__name__):
                self.assertEqual(fn(self.req("get", "/x/", make_user(OWNER_ID))).status_code, 405)

    def test_update_volunteers_text_no_longer_csrf_exempt(self):
        from datasets import views
        self.assertFalse(getattr(views.update_volunteers_text, "csrf_exempt", False))


class DsUpdateGateTests(_WriteGateBase):
    def _call(self, user, ds, tempfn="/etc/passwd", filename_new="x.tsv"):
        from datasets import views
        data = {"dsid": "1", "format": "delimited", "keepg": "true", "keepl": "true",
                "compare_data": json.dumps({"compare_result": {}, "tempfn": tempfn,
                                            "filename_new": filename_new})}
        with self.with_ds(ds), mock.patch("datasets.views.copyfile") as cp:
            resp = views.ds_update(self.req("post", "/datasets/update/", user, data))
        return resp, cp

    def test_anon_401(self):
        resp, cp = self._call(AnonymousUser(), self.dataset(True))
        self.assertJsonStatus(resp, 401)
        cp.assert_not_called()

    def test_collaborator_404(self):
        resp, cp = self._call(make_user(COLLAB_ID), self.dataset(True))
        self.assertJsonStatus(resp, 404)
        cp.assert_not_called()

    def test_owner_passes_gate_but_path_escape_refused(self):
        # Reaching the 400 proves the owner got past the permission gate.
        for tempfn, fn_new in (("/etc/passwd", "x.tsv"), ("/tmp/ok.tsv", "../whg/settings.py")):
            with self.subTest(tempfn=tempfn, fn_new=fn_new):
                resp, cp = self._call(make_user(OWNER_ID), self.dataset(False), tempfn, fn_new)
                self.assertJsonStatus(resp, 400)
                cp.assert_not_called()


class WritePass0GateTests(_WriteGateBase):
    def _call(self, user, ds):
        from datasets import services
        task = mock.Mock(task_kwargs="{'ds': 1, 'aug_geoms': 'on'}", task_name="align_wdlocal")
        r = self.req("get", f"/datasets/wd_pass0/{self.TID}", user, HTTP_REFERER="/datasets/1/status")
        with mock.patch("datasets.services.get_object_or_404", return_value=task), \
                mock.patch.object(services.Dataset, "objects") as dss, \
                mock.patch("datasets.services.Hit") as hit:
            dss.get.return_value = ds
            hit.objects.filter.return_value = []
            resp = services.write_wd_pass0(r, tid=self.TID)
        return resp, hit

    def test_anon_redirected(self):
        resp, hit = self._call(AnonymousUser(), self.dataset(True))
        self.assertLoginRedirect(resp)
        hit.objects.filter.assert_not_called()

    def test_stranger_404(self):
        with self.assertRaises(Http404):
            self._call(make_user(STRANGER_ID), self.dataset(True))

    def test_collaborator_allowed(self):
        resp, hit = self._call(make_user(COLLAB_ID), self.dataset(False))
        self.assertEqual(resp.status_code, 302)
        hit.objects.filter.assert_called_once()


class UpdateViewPostGateTests(_WriteGateBase):
    """DatasetStatusView / DatasetMetadataView: GET is view-gated, POST (which saves the
    DatasetDetailModelForm) needs edit — a stranger could POST it to a PUBLIC dataset before."""

    def _post(self, cls, user, ds):
        from django.views.generic import UpdateView
        with self.with_ds(ds), mock.patch.object(UpdateView, "post", side_effect=_Reached):
            try:
                return cls.as_view()(self.req("post", "/datasets/1/metadata", user), id=1)
            except _Reached:
                return "reached"

    def test_stranger_post_on_public_dataset_404(self):
        from datasets import views
        for cls in (views.DatasetMetadataView, views.DatasetStatusView):
            with self.subTest(cls.__name__), self.assertRaises(Http404):
                self._post(cls, make_user(STRANGER_ID), self.dataset(True))

    def test_collaborator_post_reaches_form_handling(self):
        from datasets import views
        for cls in (views.DatasetMetadataView, views.DatasetStatusView):
            with self.subTest(cls.__name__):
                self.assertEqual(self._post(cls, make_user(COLLAB_ID), self.dataset(False)), "reached")

    def test_anon_post_redirected(self):
        from datasets import views
        resp = self._post(views.DatasetMetadataView, AnonymousUser(), self.dataset(True))
        self.assertLoginRedirect(resp)


class UpdateCountsGateTests(_WriteGateBase):
    def test_anon_private_404_public_served(self):
        from datasets.utils import UpdateCountsView
        with self.with_ds(self.dataset(False)):
            with self.assertRaises(Http404):
                UpdateCountsView.get(self.req("get", "/datasets/updatecounts/", AnonymousUser(),
                                              {"ds_id": "1"}))
        ds = self.dataset(True)
        with self.with_ds(ds), mock.patch.object(Dataset, "tasks", new_callable=mock.PropertyMock) as t:
            t.return_value.filter.return_value = []
            resp = UpdateCountsView.get(self.req("get", "/datasets/updatecounts/", AnonymousUser(),
                                                 {"ds_id": "1"}))
        self.assertEqual(resp.status_code, 200)


class DeferReviewGateTests(_WriteGateBase):
    """places.views.defer_review: sets Place.review_* = 2; was open to anonymous GETs."""

    def _call(self, user, ds):
        from places import views as pviews
        place = mock.Mock(dataset=ds)
        r = self.req("get", "/places/defer/5/wd/3", user,
                     HTTP_REFERER="/datasets/1/review/t/pass1?page=1")
        with mock.patch("places.views.get_object_or_404", return_value=place):
            resp = pviews.defer_review(r, pid=5, auth="wd", last="3")
        return resp, place

    def test_anon_redirected(self):
        resp, place = self._call(AnonymousUser(), self.dataset(True))
        self.assertLoginRedirect(resp)
        place.save.assert_not_called()

    def test_stranger_404(self):
        with self.assertRaises(Http404):
            self._call(make_user(STRANGER_ID), self.dataset(True))

    def test_collaborator_defers(self):
        resp, place = self._call(make_user(COLLAB_ID), self.dataset(False))
        place.save.assert_called_once()
        self.assertEqual(place.review_wd, 2)
        self.assertEqual(resp.status_code, 302)


class RemoveDatasetFromIndexRouteTests(SimpleTestCase):
    """/elastic/remove_dataset/<dsid> is staff-only at the URL layer. The wrapped function
    is patched out, so nothing here can touch the (shared, production) ES index."""

    def _call(self, user):
        from django.urls import resolve
        match = resolve("/elastic/remove_dataset/7")
        req = RequestFactory().post("/elastic/remove_dataset/7")
        req.user = user
        with mock.patch("datasets.models.Dataset.objects") as dss:
            dss.get.side_effect = _Reached
            try:
                return match.func(req, *match.args, **match.kwargs)
            except _Reached:
                return "reached"

    def test_anon_and_non_staff_redirected_without_running(self):
        for user in (AnonymousUser(), make_user(OWNER_ID)):
            with self.subTest(user=user):
                resp = self._call(user)
                self.assertEqual(resp.status_code, 302)

    def test_staff_reaches_the_function(self):
        self.assertEqual(self._call(make_user(STRANGER_ID, staff=True)), "reached")
