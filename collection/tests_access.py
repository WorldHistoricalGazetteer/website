"""Collection write endpoints are restricted (decision 2026-09-30); reads stay open by link.

DB-free: every test is a SimpleTestCase and the ORM is mocked, so this module runs on a
machine with no reachable Postgres:

    python3 manage.py test collection.tests_access

Each denial is paired with an allowed case through the same code path, so a test that passes
only because the path was never reached (or always 404s) would fail its pair. The write each
endpoint performs is mocked and asserted called / not called, so "denied" means "nothing was
written", not just "a 404 came back".
"""
import json
from unittest import mock

from django.contrib.auth.models import AnonymousUser
from django.core.exceptions import PermissionDenied
from django.http import Http404
from django.test import RequestFactory, SimpleTestCase
from django.urls import resolve

from collection import views
from collection.models import Collection, CollectionGroup
from datasets.models import Dataset
from places.models import Place

OWNER_ID = 10       # Collection.owner FK
CO_OWNER_ID = 20    # CollectionUser role 'owner'
MEMBER_ID = 30      # CollectionUser role 'member'
LEADER_ID = 40      # CollectionGroup.owner (group leader)
GROUP_MEMBER_ID = 50
STRANGER_ID = 99


def make_user(uid, staff=False, superuser=False, groups=(), role='normal'):
    user = mock.Mock()
    user.id = user.pk = uid
    user.is_authenticated = True
    user.is_anonymous = False
    user.is_staff = staff
    user.is_superuser = superuser
    user.role = role
    user.name = user.username = f"user{uid}"
    user.email = f"u{uid}@example.org"
    groups = set(groups)

    def _filter(**kw):
        hit = kw.get('name') in groups or bool(set(kw.get('name__in', ())) & groups)
        return mock.Mock(exists=mock.Mock(return_value=hit))

    user.groups.filter.side_effect = _filter
    return user


def id_set_manager(ids):
    """Stand-in for a User queryset: .filter(id=x).exists() is True iff x in ids."""
    qs = mock.Mock()
    qs.filter.side_effect = lambda **kw: mock.Mock(exists=mock.Mock(return_value=kw.get('id') in ids))
    return qs


def owner():
    return make_user(OWNER_ID)


def co_owner():
    return make_user(CO_OWNER_ID)


def member():
    return make_user(MEMBER_ID)


def leader():
    return make_user(LEADER_ID)


def stranger():
    return make_user(STRANGER_ID)


def staff():
    return make_user(STRANGER_ID, staff=True)


class _Roles:
    """Patch the DB-backed Collection.owners / Collection.collaborators properties."""

    def setUp(self):
        super().setUp()
        self.rf = RequestFactory()
        for name, ids in (('owners', {OWNER_ID, CO_OWNER_ID}),
                          ('collaborators', {CO_OWNER_ID, MEMBER_ID})):
            p = mock.patch.object(Collection, name, new_callable=mock.PropertyMock,
                                  return_value=id_set_manager(ids))
            p.start()
            self.addCleanup(p.stop)

    @staticmethod
    def group(pk=5):
        return CollectionGroup(id=pk, owner_id=LEADER_ID, title='G')

    def coll(self, in_group=False):
        c = Collection(id=1, owner_id=OWNER_ID, title='C', collection_class='place')
        if in_group:
            c.group = self.group()
        return c

    def post(self, user, data=None, path='/x/'):
        req = self.rf.post(path, data or {})
        req.user = user
        return req

    def get(self, user, path='/x/'):
        req = self.rf.get(path)
        req.user = user
        return req

    def gate(self, obj):
        """The object every collection/access get_*_or_404 fetches."""
        return mock.patch('collection.access.get_object_or_404', return_value=obj)


# --------------------------------------------------------------------------------------
# Model predicates
# --------------------------------------------------------------------------------------

class CollectionPredicateTests(_Roles, SimpleTestCase):
    def test_truth_table(self):
        cases = [
            # who, user, manage, edit
            ('anon', AnonymousUser(), False, False),
            ('None', None, False, False),
            ('owner FK', owner(), True, True),
            ('co-owner', co_owner(), True, True),
            ('member', member(), False, True),
            ('whg_team', make_user(STRANGER_ID, groups={'whg_team'}), False, True),
            ('editorial', make_user(STRANGER_ID, groups={'editorial'}), False, True),
            ('staff', staff(), True, True),
            ('superuser', make_user(STRANGER_ID, superuser=True), True, True),
            ('whg_admins', make_user(STRANGER_ID, groups={'whg_admins'}), True, True),
            ('group leader', leader(), False, False),
            ('stranger', stranger(), False, False),
        ]
        c = self.coll(in_group=True)
        for who, user, manage, edit in cases:
            with self.subTest(who=who):
                self.assertIs(c.user_can_manage(user), manage)
                self.assertIs(c.user_can_edit(user), edit)

    def test_review_is_the_groups_leader_not_the_owner(self):
        c = self.coll(in_group=True)
        self.assertTrue(c.user_can_review(leader()))
        self.assertTrue(c.user_can_review(staff()))
        self.assertFalse(c.user_can_review(owner()))
        self.assertFalse(c.user_can_review(stranger()))
        self.assertFalse(c.user_can_review(AnonymousUser()))
        # No group: nobody but staff reviews.
        self.assertFalse(self.coll().user_can_review(leader()))
        self.assertTrue(self.coll().user_can_review(staff()))


class GroupPredicateTests(_Roles, SimpleTestCase):
    def test_manage(self):
        g = self.group()
        self.assertTrue(g.user_can_manage(leader()))
        self.assertTrue(g.user_can_manage(staff()))
        self.assertTrue(g.user_can_manage(make_user(STRANGER_ID, groups={'whg_admins'})))
        self.assertFalse(g.user_can_manage(stranger()))
        self.assertFalse(g.user_can_manage(make_user(GROUP_MEMBER_ID)))
        self.assertFalse(g.user_can_manage(AnonymousUser()))

    def test_create(self):
        self.assertTrue(CollectionGroup.user_can_create(make_user(1, groups={'group_leaders'})))
        self.assertTrue(CollectionGroup.user_can_create(make_user(1, role='group_leader')))
        self.assertTrue(CollectionGroup.user_can_create(staff()))
        self.assertFalse(CollectionGroup.user_can_create(stranger()))
        self.assertFalse(CollectionGroup.user_can_create(AnonymousUser()))


# --------------------------------------------------------------------------------------
# Collaborators (manage)
# --------------------------------------------------------------------------------------

class CollabTests(_Roles, SimpleTestCase):
    def _add(self, user, role='member'):
        req = self.post(user, {'username': 'newbie', 'role': role})
        target = mock.Mock(id=77)
        with self.gate(self.coll()), \
                mock.patch.object(views, 'get_object_or_404', return_value=target), \
                mock.patch.object(views.CollectionUser, 'objects') as cu:
            cu.filter.return_value.exists.return_value = False
            cu.create.return_value = 'collab'
            resp = views.collab_add(req, cid=1)
        return resp, cu.create

    def test_anon_401(self):
        resp, create = self._add(AnonymousUser())
        self.assertEqual(resp.status_code, 401)
        create.assert_not_called()

    def test_stranger_404(self):
        resp, create = self._add(stranger())
        self.assertEqual(resp.status_code, 404)
        create.assert_not_called()

    def test_member_cannot_add(self):
        resp, create = self._add(member())
        self.assertEqual(resp.status_code, 404)
        create.assert_not_called()

    def test_owner_adds(self):
        resp, create = self._add(owner())
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(json.loads(resp.content)['status'], 'ok')
        create.assert_called_once_with(user_id=77, collection_id=1, role='member')

    def test_co_owner_adds(self):
        resp, create = self._add(co_owner(), role='owner')
        self.assertEqual(json.loads(resp.content)['status'], 'ok')
        create.assert_called_once()

    def test_bogus_role_refused(self):
        resp, create = self._add(owner(), role='superuser')
        self.assertNotEqual(json.loads(resp.content)['status'], 'ok')
        create.assert_not_called()

    def _remove(self, user):
        req = self.post(user)
        with self.gate(self.coll()), \
                mock.patch.object(views, 'get_object_or_404') as g404:
            resp = views.collab_remove(req, uid=MEMBER_ID, cid=1)
        return resp, g404.return_value.delete

    def test_remove_stranger_404(self):
        resp, delete = self._remove(stranger())
        self.assertEqual(resp.status_code, 404)
        delete.assert_not_called()

    def test_remove_member_404(self):
        resp, delete = self._remove(member())
        self.assertEqual(resp.status_code, 404)
        delete.assert_not_called()

    def test_remove_owner_ok(self):
        resp, delete = self._remove(owner())
        self.assertEqual(resp.status_code, 200)
        delete.assert_called_once()

    def test_remove_get_refused(self):
        req = self.get(owner())
        self.assertEqual(views.collab_remove(req, uid=1, cid=1).status_code, 405)


# --------------------------------------------------------------------------------------
# Datasets into / out of a collection (edit + dataset view)
# --------------------------------------------------------------------------------------

class DatasetMembershipTests(_Roles, SimpleTestCase):
    def setUp(self):
        super().setUp()
        self.datasets = mock.MagicMock()
        self.datasets.filter.return_value.exists.return_value = False
        p = mock.patch.object(Collection, 'datasets', new_callable=mock.PropertyMock,
                              return_value=self.datasets)
        p.start()
        self.addCleanup(p.stop)
        p2 = mock.patch.object(Dataset, 'owners', new_callable=mock.PropertyMock,
                               return_value=id_set_manager({OWNER_ID}))
        p3 = mock.patch.object(Dataset, 'collaborators', new_callable=mock.PropertyMock,
                               return_value=id_set_manager(set()))
        p4 = mock.patch.object(Dataset, 'places', new_callable=mock.PropertyMock)
        for p in (p2, p3, p4):
            p.start()
            self.addCleanup(p.stop)

    @staticmethod
    def ds(public, owner_id=12345):
        return Dataset(id=2, label='d', title='D', description='x' * 200, public=public,
                       owner_id=owner_id)

    def _add_ds(self, user, ds):
        req = self.get(user)
        self.datasets.count.return_value = 1
        Dataset.places.count.return_value = 0  # PropertyMock's return value
        with self.gate(self.coll()), \
                mock.patch('datasets.utils.get_object_or_404', return_value=ds), \
                mock.patch.object(views, 'update_collection_components'):
            return views.add_dataset(req, coll_id=1, ds_id=2)

    def test_add_ds_anon_401(self):
        self.assertEqual(self._add_ds(AnonymousUser(), self.ds(True)).status_code, 401)
        self.datasets.add.assert_not_called()

    def test_add_ds_stranger_404(self):
        self.assertEqual(self._add_ds(stranger(), self.ds(True)).status_code, 404)
        self.datasets.add.assert_not_called()

    def test_add_ds_editor_private_dataset_404(self):
        resp = self._add_ds(member(), self.ds(False))
        self.assertEqual(resp.status_code, 404)
        self.datasets.add.assert_not_called()

    def test_add_ds_editor_public_dataset_added(self):
        resp = self._add_ds(member(), self.ds(True))
        self.assertEqual(json.loads(resp.content)['status'], 'success')
        self.datasets.add.assert_called_once()

    def test_add_ds_editor_own_private_dataset_added(self):
        resp = self._add_ds(owner(), self.ds(False, owner_id=OWNER_ID))
        self.assertEqual(json.loads(resp.content)['status'], 'success')

    def _add_dsplaces(self, user, ds):
        req = self.get(user)
        req.META['HTTP_REFERER'] = '/back/'
        with self.gate(self.coll()), \
                mock.patch('datasets.utils.get_object_or_404', return_value=ds):
            Dataset.places.all.return_value = []  # PropertyMock's return value
            return views.add_dataset_places(req, coll_id=1, ds_id=2)

    def test_add_dsplaces_anon_redirects_to_login(self):
        resp = self._add_dsplaces(AnonymousUser(), self.ds(True))
        self.assertEqual(resp.status_code, 302)
        self.assertIn('/accounts/login/', resp['Location'])
        self.datasets.add.assert_not_called()

    def test_add_dsplaces_stranger_404(self):
        with self.assertRaises(Http404):
            self._add_dsplaces(stranger(), self.ds(True))
        self.datasets.add.assert_not_called()

    def test_add_dsplaces_private_dataset_404(self):
        with self.assertRaises(Http404):
            self._add_dsplaces(member(), self.ds(False))
        self.datasets.add.assert_not_called()

    def test_add_dsplaces_editor_ok(self):
        resp = self._add_dsplaces(member(), self.ds(True))
        self.assertEqual(resp.status_code, 302)
        self.datasets.add.assert_called_once()

    def _remove_ds(self, user):
        req = self.get(user)
        req.META['HTTP_REFERER'] = '/back/'
        c = self.coll()
        with self.gate(c), \
                mock.patch.object(views, 'get_object_or_404', return_value=self.ds(True)), \
                mock.patch.object(Dataset, 'placeids', new_callable=mock.PropertyMock,
                                  return_value=[7, 8]), \
                mock.patch.object(Collection, 'traces', new_callable=mock.PropertyMock), \
                mock.patch.object(views.CollPlace, 'objects') as cp:
            resp = views.remove_dataset(req, coll_id=1, ds_id=2)
        return resp, cp, c

    def test_remove_ds_stranger_404_nothing_deleted(self):
        with self.assertRaises(Http404):
            self._remove_ds(stranger())
        self.datasets.remove.assert_not_called()

    def test_remove_ds_is_scoped_to_this_collection(self):
        resp, cp, c = self._remove_ds(member())
        self.assertEqual(resp.status_code, 302)
        # The scope fix: CollPlace rows are filtered by THIS collection, not only place ids.
        cp.filter.assert_called_once_with(collection=c, place_id__in=[7, 8])
        cp.filter.return_value.delete.assert_called_once()
        self.datasets.remove.assert_called_once()


class DatasetFormFieldTests(SimpleTestCase):
    def _form(self, viewer, chosen):
        from collection.forms import CollectionModelForm
        form = CollectionModelForm.__new__(CollectionModelForm)
        form.viewer = viewer
        form.instance = Collection()  # unsaved: nothing already in it
        form.cleaned_data = {'datasets': chosen}
        return form

    def test_private_dataset_refused(self):
        from django import forms
        private = Dataset(id=3, public=False, owner_id=12345)
        with mock.patch.object(Dataset, 'owners', new_callable=mock.PropertyMock,
                               return_value=id_set_manager(set())), \
                mock.patch.object(Dataset, 'collaborators', new_callable=mock.PropertyMock,
                                  return_value=id_set_manager(set())):
            with self.assertRaises(forms.ValidationError):
                self._form(stranger(), [private]).clean_datasets()

    def test_public_dataset_accepted(self):
        public = Dataset(id=3, public=True)
        self.assertEqual(self._form(stranger(), [public]).clean_datasets(), [public])


# --------------------------------------------------------------------------------------
# Places, traces, sequence, display options (edit)
# --------------------------------------------------------------------------------------

class PlaceEditTests(_Roles, SimpleTestCase):
    def _places(self, public=True):
        ds = Dataset(id=2, public=public, owner_id=12345)
        return [Place(id=7, title='p', dataset=ds)]

    def _add_places(self, user, public=True):
        req = self.post(user, {'collection': '1', 'place_list': '7'})
        with self.gate(self.coll()), \
                mock.patch.object(views.Place, 'objects') as pl, \
                mock.patch.object(views.TraceAnnotation, 'objects') as ta, \
                mock.patch.object(views.CollPlace, 'objects') as cp, \
                mock.patch.object(Dataset, 'owners', new_callable=mock.PropertyMock,
                                  return_value=id_set_manager(set())), \
                mock.patch.object(Dataset, 'collaborators', new_callable=mock.PropertyMock,
                                  return_value=id_set_manager(set())), \
                mock.patch.object(views, 'seq', return_value=0):
            pl.filter.return_value.select_related.return_value = self._places(public)
            ta.filter.return_value = []
            resp = views.add_places(req)
        return resp, cp.create

    def test_add_places_anon_401(self):
        resp, create = self._add_places(AnonymousUser())
        self.assertEqual(resp.status_code, 401)
        create.assert_not_called()

    def test_add_places_stranger_404(self):
        resp, create = self._add_places(stranger())
        self.assertEqual(resp.status_code, 404)
        create.assert_not_called()

    def test_add_places_private_dataset_404(self):
        resp, create = self._add_places(member(), public=False)
        self.assertEqual(resp.status_code, 404)
        create.assert_not_called()

    def test_add_places_member_ok(self):
        resp, create = self._add_places(member())
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(json.loads(resp.content)['msg']['added'], [7])
        create.assert_called_once()

    def _archive(self, user):
        req = self.post(user, {'collection': '1', 'place_list': '7'})
        with self.gate(self.coll()), \
                mock.patch.object(views.Place, 'objects') as pl, \
                mock.patch.object(Collection, 'places', new_callable=mock.PropertyMock) as places, \
                mock.patch.object(views.TraceAnnotation, 'objects'), \
                mock.patch.object(views.CollPlace, 'objects') as cp:
            cp.filter.return_value.order_by.return_value = []
            resp = views.archive_traces(req)
            return resp, places.return_value.all

    def test_archive_stranger_404(self):
        resp, touched = self._archive(stranger())
        self.assertEqual(resp.status_code, 404)
        touched.assert_not_called()

    def test_archive_member_ok(self):
        resp, touched = self._archive(member())
        self.assertEqual(resp.status_code, 200)
        touched.assert_called()

    def _sequence(self, user):
        req = self.post(user, {'coll_id': '1', 'seq': json.dumps({'7': 0})})
        cp_row = mock.Mock(place_id=7)
        with self.gate(self.coll()), mock.patch.object(views.CollPlace, 'objects') as cp:
            cp.filter.return_value = [cp_row]
            resp = views.update_sequence(req)
        return resp, cp_row.save

    def test_sequence_stranger_404(self):
        resp, save = self._sequence(stranger())
        self.assertEqual(resp.status_code, 404)
        save.assert_not_called()

    def test_sequence_member_ok(self):
        resp, save = self._sequence(member())
        self.assertEqual(resp.status_code, 200)
        save.assert_called_once()

    def _vis(self, user):
        req = self.post(user, {'coll_id': '1', 'checked': 'true'})
        with self.gate(self.coll()), mock.patch.object(Collection, 'save') as save:
            resp = views.update_vis_parameters(req)
        return resp, save

    def test_vis_stranger_404(self):
        resp, save = self._vis(stranger())
        self.assertEqual(resp.status_code, 404)
        save.assert_not_called()

    def test_vis_member_ok(self):
        resp, save = self._vis(member())
        self.assertEqual(resp.status_code, 200)
        save.assert_called_once()


class AddCollectionPlacesTests(_Roles, SimpleTestCase):
    def _call(self, user, coll_id='1'):
        req = self.post(user, {'collection': coll_id, 'primarySource': '7', 'title': 't'})
        ds = Dataset(id=2, public=True)
        with self.gate(self.coll()), \
                mock.patch.object(views.Place, 'objects') as pl, \
                mock.patch.object(views.Collection, 'objects') as co, \
                mock.patch.object(views.TraceAnnotation, 'objects') as ta, \
                mock.patch.object(views.CollPlace, 'objects') as cp, \
                mock.patch.object(views, 'seq', return_value=0):
            pl.filter.return_value.select_related.return_value = [Place(id=7, dataset=ds)]
            pl.get.return_value = Place(id=7, dataset=ds)
            co.get.return_value = mock.Mock(collection_class='place', id=1, title='C',
                                            description='d', places=mock.Mock(count=mock.Mock(return_value=1)))
            ta.filter.return_value = []
            resp = views.add_collection_places(req)
        return resp, cp.create, co.create

    def test_anon_401(self):
        resp, create, _ = self._call(AnonymousUser())
        self.assertEqual(resp.status_code, 401)
        create.assert_not_called()

    def test_stranger_existing_collection_404(self):
        resp, create, _ = self._call(stranger())
        self.assertEqual(resp.status_code, 404)
        create.assert_not_called()

    def test_member_existing_collection_ok(self):
        resp, create, _ = self._call(member())
        self.assertEqual(json.loads(resp.content)['status'], 'success')
        create.assert_called_once()


# --------------------------------------------------------------------------------------
# Group review / submission / join codes / links
# --------------------------------------------------------------------------------------

class ReviewTests(_Roles, SimpleTestCase):
    def _status(self, user, status='reviewed'):
        req = self.post(user, {'coll': '1', 'status': status})
        with self.gate(self.coll(in_group=True)), mock.patch.object(Collection, 'save') as save:
            resp = views.status_update(req)
        return resp, save

    def test_status_owner_cannot_review_own(self):
        resp, save = self._status(owner())
        self.assertEqual(resp.status_code, 404)
        save.assert_not_called()

    def test_status_stranger_404(self):
        resp, save = self._status(stranger())
        self.assertEqual(resp.status_code, 404)
        save.assert_not_called()

    def test_status_leader_ok(self):
        resp, save = self._status(leader())
        self.assertEqual(resp.status_code, 200)
        save.assert_called_once()

    def test_status_leader_cannot_publish(self):
        resp, save = self._status(leader(), status='published')
        self.assertEqual(resp.status_code, 400)
        save.assert_not_called()

    def test_status_staff_can_publish(self):
        resp, save = self._status(staff(), status='published')
        self.assertEqual(resp.status_code, 200)
        save.assert_called_once()

    def _nominate(self, user):
        from django.contrib.auth import get_user_model
        req = self.post(user, {'coll': '1', 'nominated': 'true'})
        c = self.coll(in_group=True)
        c.owner = get_user_model()(id=OWNER_ID, username='o')
        with self.gate(c), \
                mock.patch.object(Collection, 'save') as save, \
                mock.patch.object(views, 'WHGmail') as mail:
            resp = views.nominator(req)
        return resp, save, mail

    def test_nominate_owner_404(self):
        resp, save, mail = self._nominate(owner())
        self.assertEqual(resp.status_code, 404)
        save.assert_not_called()
        mail.assert_not_called()

    def test_nominate_leader_ok(self):
        resp, save, mail = self._nominate(leader())
        self.assertEqual(resp.status_code, 200)
        save.assert_called_once()


class GroupConnectTests(_Roles, SimpleTestCase):
    def _connect(self, user, is_group_member):
        req = self.post(user, {'coll': '1', 'group': '5', 'action': 'submit'})
        g = self.group()
        with self.gate(self.coll()), \
                mock.patch.object(views.CollectionGroup, 'objects') as cgo, \
                mock.patch.object(views.CollectionGroupUser, 'objects') as cgu, \
                mock.patch.object(CollectionGroup, 'collections', new_callable=mock.PropertyMock) as cols, \
                mock.patch.object(Collection, 'save'):
            cgo.get.return_value = g
            cgu.filter.return_value.exists.return_value = is_group_member
            resp = views.group_connect(req)
            return resp, cols.return_value.add

    def test_stranger_404(self):
        resp, add = self._connect(stranger(), True)
        self.assertEqual(resp.status_code, 404)
        add.assert_not_called()

    def test_member_collaborator_cannot_submit(self):
        resp, add = self._connect(member(), True)
        self.assertEqual(resp.status_code, 404)
        add.assert_not_called()

    def test_owner_not_in_group_404(self):
        resp, add = self._connect(owner(), False)
        self.assertEqual(resp.status_code, 404)
        add.assert_not_called()

    def test_owner_in_group_ok(self):
        resp, add = self._connect(owner(), True)
        self.assertEqual(resp.status_code, 200)
        add.assert_called_once()


class JoincodeTests(_Roles, SimpleTestCase):
    def _set(self, user):
        req = self.post(user)
        with self.gate(self.group()), mock.patch.object(CollectionGroup, 'save') as save:
            resp = views.set_joincode(req, cgid=5, join_code='SwiftFox')
        return resp, save

    def test_anon_401(self):
        resp, save = self._set(AnonymousUser())
        self.assertEqual(resp.status_code, 401)
        save.assert_not_called()

    def test_stranger_404(self):
        resp, save = self._set(stranger())
        self.assertEqual(resp.status_code, 404)
        save.assert_not_called()

    def test_leader_ok(self):
        resp, save = self._set(leader())
        self.assertEqual(json.loads(resp.content)['join_code'], 'SwiftFox')
        save.assert_called_once()

    def test_join_group_anon_401(self):
        req = self.post(AnonymousUser(), {'join_code': 'x'})
        with mock.patch.object(views.CollectionGroupUser, 'objects') as cgu:
            self.assertEqual(views.join_group(req).status_code, 401)
        cgu.create.assert_not_called()


class InactiveTests(_Roles, SimpleTestCase):
    def _call(self, user):
        req = self.post(user, {'id': '1'})
        with self.gate(self.coll()), mock.patch.object(Collection, 'save') as save:
            resp = views.inactive(req)
        return resp, save

    def test_member_404(self):
        resp, save = self._call(member())
        self.assertEqual(resp.status_code, 404)
        save.assert_not_called()

    def test_owner_ok(self):
        resp, save = self._call(owner())
        self.assertEqual(resp.status_code, 200)
        save.assert_called_once()


class RemoveLinkTests(_Roles, SimpleTestCase):
    def _call(self, user, on_group=False):
        from main.models import Link
        link = Link(id=3)
        if on_group:
            link.collection_group = self.group()
        else:
            link.collection = self.coll()
        req = self.get(user)
        req.META['HTTP_REFERER'] = '/back/'
        with mock.patch.object(views, 'get_object_or_404', return_value=link), \
                mock.patch.object(Link, 'delete') as delete:
            resp = views.remove_link(req, id=3)
        return resp, delete

    def test_anon_login(self):
        resp, delete = self._call(AnonymousUser())
        self.assertEqual(resp.status_code, 302)
        self.assertIn('/accounts/login/', resp['Location'])
        delete.assert_not_called()

    def test_stranger_404(self):
        with mock.patch.object(__import__('main.models', fromlist=['Link']).Link, 'delete') as d:
            with self.assertRaises(Http404):
                self._call(stranger())

    def test_member_removes_collection_link(self):
        resp, delete = self._call(member())
        self.assertEqual(resp.status_code, 302)
        delete.assert_called_once()

    def test_member_cannot_remove_group_link(self):
        with self.assertRaises(Http404):
            self._call(member(), on_group=True)

    def test_leader_removes_group_link(self):
        resp, delete = self._call(leader(), on_group=True)
        delete.assert_called_once()


class CreateLinkTests(_Roles, SimpleTestCase):
    def _call(self, user, model='Collection', target=None):
        from main import views as mviews
        req = self.post(user, {'model': model, 'objectid': '1', 'uri': 'https://example.org/',
                               'label': 'l', 'link_type': 'webpage'})
        target = target or self.coll()
        fake_model = mock.Mock()
        fake_model.objects.get.return_value = target
        rel = mock.patch.object(type(target), 'related_links', new_callable=mock.PropertyMock)
        with mock.patch.object(mviews.apps, 'get_model', return_value=fake_model), rel as rl, \
                mock.patch.object(mviews.Link, 'objects') as lo:
            rl.return_value.filter.return_value = []
            lo.create.return_value = mock.Mock(uri='u', label='l', link_type='webpage', id=1,
                                               get_link_type_display=mock.Mock(return_value='w'))
            resp = mviews.create_link(req)
        return resp, lo.create

    def test_anon_401(self):
        resp, create = self._call(AnonymousUser())
        self.assertEqual(resp.status_code, 401)
        create.assert_not_called()

    def test_stranger_404(self):
        resp, create = self._call(stranger())
        self.assertEqual(resp.status_code, 404)
        create.assert_not_called()

    def test_other_model_404(self):
        resp, create = self._call(owner(), model='CollectionUser')
        self.assertEqual(resp.status_code, 404)
        create.assert_not_called()

    def test_member_links_collection(self):
        resp, create = self._call(member())
        self.assertEqual(json.loads(resp.content)['status'], 'ok')
        create.assert_called_once()

    def test_member_cannot_link_group(self):
        resp, create = self._call(member(), model='CollectionGroup', target=self.group())
        self.assertEqual(resp.status_code, 404)
        create.assert_not_called()

    def test_leader_links_group(self):
        resp, create = self._call(leader(), model='CollectionGroup', target=self.group())
        self.assertEqual(json.loads(resp.content)['status'], 'ok')
        create.assert_called_once()


# --------------------------------------------------------------------------------------
# Class-based views
# --------------------------------------------------------------------------------------

class ClassViewTests(_Roles, SimpleTestCase):
    def _get_object(self, view_cls, user, obj, **kw):
        view = view_cls()
        req = self.get(user)
        view.setup(req, id=1, **kw)
        with self.gate(obj):
            return view.get_object()

    def test_delete_view(self):
        with self.assertRaises(Http404):
            self._get_object(views.CollectionDeleteView, member(), self.coll())
        with self.assertRaises(Http404):
            self._get_object(views.CollectionDeleteView, stranger(), self.coll())
        self.assertIsNotNone(self._get_object(views.CollectionDeleteView, co_owner(), self.coll()))
        self.assertIsNotNone(self._get_object(views.CollectionDeleteView, staff(), self.coll()))

    def test_update_views_need_edit(self):
        for cls in (views.DatasetCollectionUpdateView, views.PlaceCollectionUpdateView):
            with self.subTest(view=cls.__name__):
                with self.assertRaises(Http404):
                    self._get_object(cls, stranger(), self.coll())
                self.assertIsNotNone(self._get_object(cls, member(), self.coll()))

    def test_group_update_and_delete_need_group_manage(self):
        for cls in (views.CollectionGroupUpdateView, views.CollectionGroupDeleteView):
            with self.subTest(view=cls.__name__):
                with self.assertRaises(Http404):
                    self._get_object(cls, make_user(GROUP_MEMBER_ID), self.group())
                self.assertIsNotNone(self._get_object(cls, leader(), self.group()))

    def test_anonymous_redirected_to_login(self):
        for cls in (views.CollectionDeleteView, views.DatasetCollectionUpdateView,
                    views.PlaceCollectionUpdateView, views.CollectionGroupUpdateView,
                    views.CollectionGroupDeleteView, views.CollectionGroupCreateView):
            with self.subTest(view=cls.__name__), self.gate(self.coll()):
                resp = cls.as_view()(self.get(AnonymousUser()), id=1)
                self.assertEqual(resp.status_code, 302)
                self.assertIn('/accounts/login/', resp['Location'])

    def test_group_create_needs_leader(self):
        view = views.CollectionGroupCreateView.as_view()
        with self.assertRaises(PermissionDenied):
            view(self.get(stranger()))
        with mock.patch('django.views.generic.edit.ProcessFormView.get', return_value='rendered') as g:
            self.assertEqual(view(self.get(make_user(1, groups={'group_leaders'}))), 'rendered')
        g.assert_called_once()


# --------------------------------------------------------------------------------------
# traces.annotate
# --------------------------------------------------------------------------------------

class AnnotateTests(_Roles, SimpleTestCase):
    def test_route_is_not_csrf_exempt(self):
        func = resolve('/collections/1/annotate').func
        self.assertFalse(getattr(func, 'csrf_exempt', False))

    def test_anno_form_renders_a_csrf_token(self):
        from django.template.loader import render_to_string
        req = self.get(member())
        from types import SimpleNamespace
        # Not a Mock: Django templates never call a Mock (do_not_call_in_templates is truthy).
        place = SimpleNamespace(id=7, title='p', names=SimpleNamespace(all=lambda: []))
        html = render_to_string('../templates/traceanno_form.html', context={
            'form': mock.MagicMock(), 'place': place,
            'collection': self.coll(), 'rel_keywords': [], 'existing': None,
        }, request=req)
        self.assertIn('csrfmiddlewaretoken', html)

    def _call(self, user, body_collection='1', public=True):
        from traces import views as tviews
        req = self.post(user, {'place': '7', 'collection': body_collection})
        place = Place(id=7, dataset=Dataset(id=2, public=public, owner_id=12345))
        with self.gate(self.coll()), \
                mock.patch.object(tviews.Place, 'objects') as po, \
                mock.patch.object(Dataset, 'owners', new_callable=mock.PropertyMock,
                                  return_value=id_set_manager(set())), \
                mock.patch.object(Dataset, 'collaborators', new_callable=mock.PropertyMock,
                                  return_value=id_set_manager(set())), \
                mock.patch.object(tviews, 'TraceAnnotationModelForm') as form_cls:
            po.select_related.return_value.get.return_value = place
            form_cls.return_value.is_valid.return_value = True
            resp = tviews.annotate(req, id=1)
        return resp, form_cls.return_value.save

    def test_anon_401(self):
        resp, save = self._call(AnonymousUser())
        self.assertEqual(resp.status_code, 401)
        save.assert_not_called()

    def test_stranger_404(self):
        resp, save = self._call(stranger())
        self.assertEqual(resp.status_code, 404)
        save.assert_not_called()

    def test_body_cannot_redirect_to_another_collection(self):
        resp, save = self._call(member(), body_collection='2')
        self.assertEqual(resp.status_code, 404)
        save.assert_not_called()

    def test_private_dataset_place_404(self):
        resp, save = self._call(member(), public=False)
        self.assertEqual(resp.status_code, 404)
        save.assert_not_called()

    def test_member_saves(self):
        resp, save = self._call(member())
        self.assertEqual(resp.status_code, 200)
        save.assert_called_once()


# --------------------------------------------------------------------------------------
# validation task_status
# --------------------------------------------------------------------------------------

class TaskStatusTests(SimpleTestCase):
    def _call(self, user, owner_on_task=str(OWNER_ID)):
        from validation import tasks
        rf = RequestFactory()
        req = rf.get('/validation/task_status/validation_task_x/')
        req.user = user
        h = {b'status': b'in_progress', b'total_features': b'10', b'queued_features': b'5',
             b'start_time': b'2026-09-30T00:00:00'}
        if owner_on_task is not None:
            h[b'owner_id'] = owner_on_task.encode()
        redis = mock.Mock()
        redis.hgetall.return_value = h
        redis.hget.return_value = None
        redis.lrange.return_value = []
        with mock.patch.object(tasks, 'get_redis_client', return_value=redis), \
                mock.patch.object(tasks, 'cleanup') as cleanup:
            resp = tasks.get_task_status(req, 'validation_task_x')
        return resp, redis

    def test_anon_401_and_no_redis_touch(self):
        resp, redis = self._call(AnonymousUser())
        self.assertEqual(resp.status_code, 401)
        redis.hgetall.assert_not_called()

    def test_other_user_404_and_no_write(self):
        resp, redis = self._call(stranger())
        self.assertEqual(resp.status_code, 404)
        redis.hset.assert_not_called()

    def test_uploader_ok(self):
        resp, redis = self._call(owner())
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(json.loads(resp.content)['status'], 'success')

    def test_staff_ok(self):
        resp, _ = self._call(staff())
        self.assertEqual(resp.status_code, 200)

    def test_owner_falls_back_to_metadata(self):
        from validation import tasks
        redis = mock.Mock()
        redis.hget.return_value = str(OWNER_ID).encode()
        self.assertEqual(tasks._task_owner_id(redis, 't', {}), OWNER_ID)
        redis.hget.return_value = None
        self.assertIsNone(tasks._task_owner_id(redis, 't', {}))
