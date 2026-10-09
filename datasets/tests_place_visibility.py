"""place#310 — places of a non-public or embargoed contributed dataset are not
readable, one at a time or in a list, by anyone outside the dataset's circle,
on ANY surface.

One rule (``api.dataset_access``: ``visible_places_q`` for the ORM,
``HiddenDatasets`` for Elasticsearch and gateway hits) applied to every
surface that can return a place, its geometry, its names or its links. This
module is the matrix:

    surface  ×  (owner, collaborator, staff | beta stranger, normal stranger, anonymous)
             ×  (public, private, embargoed)

Run locally (never in the prod container — see reference_never_run_suite_on_prod):

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test datasets.tests_place_visibility --settings=whg.settings_localtest

Discipline, so nothing passes vacuously (a-check-that-cannot-fail):

* every surface is probed for EVERY cell of the matrix, and the public dataset
  row is the positive control for every outsider: a surface that 404s or
  returns nothing for everyone fails on that row;
* the insiders are the positive control for the private / embargoed rows: a
  surface that withholds those datasets from everyone fails there;
* Elasticsearch is faked at the client handle and the fake EVALUATES the
  query's ``must_not terms dataset`` clause against a fixed corpus, so a view
  that forgot the clause, or addressed the wrong field, returns the private
  doc and fails here;
* the gateway is faked at the HTTP seam and returns contributed hits for all
  three datasets, so a view that forgot to drop them fails here;
* ``visible_datasets_q`` and ``hidden_datasets`` are checked against
  ``Dataset.user_can_view`` / ``dataset_visible_to`` cell by cell, so the batch
  forms cannot drift from the single-row rule.

Mutation-checked when written (``visible_datasets_q`` returning ``Q()``): see
the commit message for the counts.
"""

import json
from datetime import timedelta
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.auth.models import AnonymousUser, Group
from django.contrib.gis.geos import Point
from django.test import TestCase, override_settings
from django.utils import timezone

from api.dataset_access import (
    dataset_visible_to, hidden_datasets, visible_dataset_pks, visible_datasets_q, visible_places_q,
)

INSIDERS = ("owner", "collab", "staff")
OUTSIDERS = ("beta", "normal", "anon")
WHO = INSIDERS + OUTSIDERS
# Routes that refuse anonymous callers before any dataset rule runs (401): the
# matrix for them is the signed-in rows, and the refusal is asserted separately.
AUTHENTICATED = ("owner", "collab", "staff", "beta", "normal")
DATASETS = ("pub", "priv", "emb")
GATEWAY = "http://gateway.test"


def expected(who, ds_key):
    """The truth table every surface must reproduce."""
    return ds_key == "pub" or who in INSIDERS


# ---------------------------------------------------------------------------
# A fake legacy Elasticsearch that honours the one clause under test
# ---------------------------------------------------------------------------

def _must_not_dataset_labels(body):
    """The labels a query body excludes via ``bool.filter[].bool.must_not[].terms.dataset``
    (the clause ``es_apply_visibility`` writes). Empty when the clause is absent."""
    out = set()
    q = (body or {}).get("query") or {}
    b = q.get("bool") or {}
    flt = b.get("filter")
    clauses = flt if isinstance(flt, list) else ([flt] if flt else [])
    for c in clauses:
        for mn in ((c.get("bool") or {}).get("must_not") or []):
            terms = (mn.get("terms") or {}).get("dataset")
            if terms:
                out.update(terms)
    return out


class FakeES:
    """Serves ``docs`` (legacy ``whg``-index documents) that satisfy the body's
    ``bool`` query under Elasticsearch's own semantics — ``must`` / ``filter``
    all match, ``must_not`` none, and ``should`` at least ``minimum_should_match``
    of them, which ES defaults to 1 when the bool has no ``must``/``filter`` and
    to 0 once it has. That last rule is the one a visibility clause can trip
    over: adding a ``filter`` to a should-only bool silently turns the shoulds
    into optional boosts. Records every body it was given."""

    def __init__(self, docs):
        self.docs = docs
        self.bodies = []

    @staticmethod
    def _as_list(x):
        return x if isinstance(x, list) else ([x] if x else [])

    def _clause(self, clause, doc):
        src = doc["_source"]
        if "bool" in clause:
            return self._bool(clause["bool"], doc)
        if "match" in clause:
            (field, val), = clause["match"].items()
            if field == "_id":
                return doc["_id"] == val
            return str(val).lower() in str(src.get(field, "")).lower()
        if "parent_id" in clause:
            return (src.get("relation") or {}).get("parent") == clause["parent_id"]["id"]
        if "exists" in clause:
            return clause["exists"]["field"] in src
        if "terms" in clause:
            (field, vals), = clause["terms"].items()
            have = src.get(field)
            return bool(set(have) & set(vals)) if isinstance(have, list) else have in vals
        if "multi_match" in clause:
            q = str(clause["multi_match"]["query"]).lower()
            return (q in str(src.get("title", "")).lower()
                    or any(q in str(n.get("toponym", "")).lower() for n in src.get("names", [])))
        return True  # match_all, geo_shape, has_child: not under test here

    def _bool(self, b, doc):
        musts = self._as_list(b.get("must"))
        filters = self._as_list(b.get("filter"))
        if any(not self._clause(c, doc) for c in musts + filters):
            return False
        if any(self._clause(c, doc) for c in self._as_list(b.get("must_not"))):
            return False
        shoulds = self._as_list(b.get("should"))
        if shoulds:
            msm = b.get("minimum_should_match")
            if msm is None:
                msm = 0 if (musts or filters) else 1
            if sum(self._clause(c, doc) for c in shoulds) < int(msm):
                return False
        return True

    def _select(self, body):
        query = (body or {}).get("query") or body or {}
        return [d for d in self.docs if self._clause(query, d)]

    def search(self, index=None, body=None, size=None, **kw):
        self.bodies.append(body)
        hits = [dict(h, inner_hits={"child": {"hits": {"hits": []}}}) for h in self._select(body)]
        return {"hits": {"hits": hits, "total": {"value": len(hits)}}}

    def count(self, index=None, body=None, **kw):
        self.bodies.append(body)
        return {"count": len(self._select(body))}


def mapdata_titles(data):
    """Titles across every layer of a /mapdata/ payload (keyed by geometry type
    plus ``table``), whatever shape the layers take."""
    out = set()
    for key, layer in (data or {}).items():
        if isinstance(layer, dict):
            for f in layer.get("features") or []:
                props = f.get("properties") or {}
                out.add(props.get("title") or f.get("title"))
    return out - {None}


class _Resp:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.text = json.dumps(self._payload)

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class VisibilityMatrixBase(TestCase):
    @classmethod
    def setUpTestData(cls):
        from datasets.models import Dataset, DatasetUser
        from places.models import Place, PlaceGeom, PlaceLink, PlaceName
        from api.models import GazetteerRegistryEntry
        User = get_user_model()

        def user(name, role="normal", **kw):
            u = User.objects.create_user(username=name, email=f"{name}@example.org", password="pw",
                                         given_name=name.title(), surname="Test", role=role)
            for k, v in kw.items():
                setattr(u, k, v)
            if kw:
                u.save()
            return u

        cls.users = {
            "owner": user("v_owner", role="beta_tester"),
            "collab": user("v_collab", role="beta_tester"),
            "staff": user("v_staff", is_staff=True),
            "beta": user("v_beta", role="beta_tester"),
            "normal": user("v_normal"),
            "anon": None,
        }
        Group.objects.get_or_create(name="whg_admins")
        Group.objects.get_or_create(name="whg_team")

        cls.datasets, cls.places = {}, {}
        coords = {"pub": (-1.0, 51.0), "priv": (-1.1, 51.1), "emb": (-1.2, 51.2)}
        with patch("datasets.signals.doi"):
            for key, public in (("pub", True), ("priv", False), ("emb", True)):
                ds = Dataset.objects.create(
                    owner=cls.users["owner"], label=f"v310_{key}", title=f"Dataset {key}",
                    description="D", creator="A Contributor", public=public, ds_status="indexed",
                    uri_base="https://example.org/places/")
                cls.datasets[key] = ds
                lon, lat = coords[key]
                p = Place.objects.create(title=f"Placeville-{key}", src_id="1", dataset=ds, ccodes=["GB"],
                                         minmax=[1500, 1600], timespans=[[1500, 1600]], fclasses=["P"])
                PlaceName.objects.create(place=p, toponym=f"Placeville-{key}",
                                         jsonb={"toponym": f"Placeville-{key}"})
                PlaceName.objects.create(place=p, toponym=f"Variant-{key}", jsonb={"toponym": f"Variant-{key}"})
                PlaceGeom.objects.create(place=p, geom=Point(lon, lat, srid=4326),
                                         jsonb={"type": "Point", "coordinates": [lon, lat]})
                PlaceLink.objects.create(place=p, jsonb={"type": "closeMatch", "identifier": f"wd:Q{key}"})
                cls.places[key] = p
                DatasetUser.objects.create(dataset_id=ds, user_id=cls.users["collab"], role="member")
        GazetteerRegistryEntry.objects.create(
            id=f"whg:{cls.datasets['emb'].pk}", name="Embargoed", namespace="whg",
            entry_class="dataset", status="embargoed")
        # A released embargo is public again: the lazy release of place#162.
        GazetteerRegistryEntry.objects.create(
            id=f"whg:{cls.datasets['pub'].pk}", name="Released", namespace="whg",
            entry_class="dataset", status="embargoed",
            embargo_release_at=timezone.now() - timedelta(days=1))

    def setUp(self):
        # Establish the premises, do not inherit them.
        self.assertTrue(self.users["beta"].can_access_beta)
        self.assertFalse(self.users["normal"].can_access_beta)
        self.assertTrue(self.users["staff"].is_staff)
        self.assertTrue(self.datasets["emb"].public)
        self.assertFalse(self.datasets["priv"].public)

    def login(self, who):
        self.client.logout()
        if self.users[who] is not None:
            self.client.force_login(self.users[who])

    def user(self, who):
        return self.users[who] or AnonymousUser()

    def title(self, key):
        return self.places[key].title

    def check_matrix(self, sees, whos=WHO, keys=DATASETS, surface=""):
        """``sees(who, key) -> bool`` is probed for every cell and compared with the
        truth table. Failures are collected so one run reports the whole matrix."""
        wrong = []
        for who in whos:
            self.login(who)
            for key in keys:
                got = bool(sees(who, key))
                if got != expected(who, key):
                    wrong.append(f"{surface} {who}/{key}: {'visible' if got else 'withheld'}, "
                                 f"expected {'visible' if expected(who, key) else 'withheld'}")
        self.assertEqual(wrong, [], "\n".join(wrong))


# ---------------------------------------------------------------------------
# The rule itself, in all its forms
# ---------------------------------------------------------------------------

class RuleTests(VisibilityMatrixBase):
    def test_batch_q_agrees_with_the_single_row_rule_cell_by_cell(self):
        from datasets.models import Dataset
        for who in WHO:
            u = self.user(who)
            q_visible = set(Dataset.objects.filter(visible_datasets_q(u)).values_list("id", flat=True))
            hidden = hidden_datasets(u)
            for key, ds in self.datasets.items():
                with self.subTest(who=who, key=key):
                    single = dataset_visible_to(ds, u)
                    self.assertEqual(single, expected(who, key))
                    self.assertEqual(ds.pk in q_visible, single)
                    self.assertEqual(ds.pk in hidden.pks, not single)
                    self.assertEqual(ds.label in hidden.labels, not single)
                    self.assertEqual(hidden.hides_contributed_id(f"whg:{ds.pk}:1"), not single)
            # The two-part fallback id names no dataset: withheld whenever anything is.
            if hidden:
                self.assertTrue(hidden.hides_contributed_id(f"whg:{self.datasets['pub'].pk}"))

    def test_embargo_overrides_public_and_releases_lazily(self):
        anon = AnonymousUser()
        self.assertFalse(dataset_visible_to(self.datasets["emb"], anon))
        self.assertTrue(dataset_visible_to(self.datasets["pub"], anon))  # released row
        self.assertTrue(dataset_visible_to(self.datasets["emb"], self.users["collab"]))

    def test_visible_places_q_and_visible_dataset_pks(self):
        from places.models import Place
        for who in WHO:
            u = self.user(who)
            got = set(Place.objects.filter(visible_places_q(u)).values_list("id", flat=True))
            want = {p.id for k, p in self.places.items() if expected(who, k)}
            self.assertEqual(got & set(p.id for p in self.places.values()), want, who)
            pks = visible_dataset_pks(u, [d.pk for d in self.datasets.values()])
            self.assertEqual(pks, {d.pk for k, d in self.datasets.items() if expected(who, k)}, who)

    def test_staff_like_roles_see_everything(self):
        u = self.users["normal"]
        self.assertTrue(hidden_datasets(u))
        u.groups.add(Group.objects.get(name="whg_team"))
        self.assertFalse(hidden_datasets(u))

    def test_hidden_datasets_is_one_query_per_request(self):
        from django.db import connection
        from django.test.utils import CaptureQueriesContext
        with CaptureQueriesContext(connection) as ctx:
            hidden_datasets(self.users["beta"])
        # groups lookup + embargo rows + the hidden set: bounded, independent of place count
        self.assertLessEqual(len(ctx.captured_queries), 3, [q["sql"] for q in ctx.captured_queries])
        self.assertEqual(sum('auth_group' in q['sql'] for q in ctx.captured_queries), 1)


# ---------------------------------------------------------------------------
# Place pages, place APIs, entity API, reconcile EXTEND
# ---------------------------------------------------------------------------

class PlaceRecordSurfaceTests(VisibilityMatrixBase):
    def test_place_detail_page(self):
        self.check_matrix(lambda who, k: self.client.get(f"/places/{self.places[k].id}/detail").status_code == 200,
                          surface="/places/<pk>/detail")

    def test_place_portal_single_pid(self):
        # One place with no close matches: the portal 302s to the detail page for
        # an insider and 404s otherwise.
        def sees(who, k):
            r = self.client.get(f"/places/portal/{self.places[k].id}/")
            return r.status_code == 302
        self.check_matrix(sees, surface="/places/portal/<pid>/")

    def test_place_portal_multi_pid_omits_withheld_members(self):
        ids = ",".join(str(self.places[k].id) for k in DATASETS)
        def sees(who, k):
            r = self.client.get(f"/places/portal/{ids}/")
            self.assertEqual(r.status_code, 200, who)  # the public member keeps it a portal
            return self.title(k) in r.content.decode()
        self.check_matrix(sees, surface="/places/portal/<a,b,c>/")

    def test_place_portal_all_withheld_is_404(self):
        self.login("beta")
        r = self.client.get(f"/places/portal/{self.places['priv'].id},{self.places['emb'].id}/")
        self.assertEqual(r.status_code, 404)
        self.login("owner")
        self.assertEqual(self.client.get(
            f"/places/portal/{self.places['priv'].id},{self.places['emb'].id}/").status_code, 200)

    def test_place_portal_by_whg_id_filters_the_index_bundle(self):
        ids = [self.places[k].id for k in DATASETS]
        def sees(who, k):
            with patch("places.views.findPortalPlaces", return_value=ids):
                r = self.client.get("/places/999/portal/")
            return r.status_code == 200 and self.title(k) in r.content.decode()
        self.check_matrix(sees, surface="/places/<whg_id>/portal/")

    def test_api_place_single(self):
        self.check_matrix(lambda who, k: self.client.get(f"/api/place/{self.places[k].id}/").status_code == 200,
                          surface="/api/place/<pk>/")

    def test_api_place_multi_omits_withheld(self):
        ids = "-".join(str(self.places[k].id) for k in DATASETS)
        def sees(who, k):
            r = self.client.get(f"/api/place/{ids}/")
            self.assertEqual(r.status_code, 200, who)
            titles = {n["toponym"] for n in r.json()["names"]} | set(r.json()["title"].split("|"))
            return self.title(k) in titles
        self.check_matrix(sees, surface="/api/place/<a-b-c>/")

    def test_api_place_by_label_and_src_id(self):
        self.check_matrix(
            lambda who, k: self.client.get(f"/api/place/{self.datasets[k].label}/1/").status_code == 200,
            surface="/api/place/<label>/<src_id>/")

    def test_api_place_compare(self):
        self.check_matrix(
            lambda who, k: self.client.get(f"/api/place_compare/{self.places[k].id}/").status_code == 200,
            surface="/api/place_compare/<pk>/")

    def test_api_spatial_bbox(self):
        def sees(who, k):
            r = self.client.get("/api/spatial/", {"type": "bbox", "sw": "-2,50", "ne": "0,52", "pagesize": "50"})
            self.assertEqual(r.status_code, 200, who)
            return any(f["properties"]["title"] == self.title(k) for f in r.json()["features"])
        self.check_matrix(sees, surface="/api/spatial/")

    def test_api_spatial_dataset_filter_does_not_confirm_a_private_id(self):
        self.login("beta")
        r = self.client.get("/api/spatial/", {"type": "bbox", "sw": "-2,50", "ne": "0,52",
                                              "dataset": str(self.datasets["priv"].pk)})
        self.assertEqual(r.status_code, 400)
        self.assertIn("does not exist", r.content.decode())
        r = self.client.get("/api/spatial/", {"type": "bbox", "sw": "-2,50", "ne": "0,52",
                                              "dataset": str(self.datasets["pub"].pk)})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(len(r.json()["features"]), 1)

    def test_api_db(self):
        def sees(who, k):
            r = self.client.get("/api/db/", {"name": self.title(k)})
            self.assertEqual(r.status_code, 200, who)
            return any(f["properties"]["title"] == self.title(k) for f in r.json().get("features", []))
        self.check_matrix(sees, surface="/api/db/")

    def test_api_datasets_list(self):
        def sees(who, k):
            r = self.client.get("/api/datasets/", {"id": str(self.datasets[k].pk)})
            self.assertEqual(r.status_code, 200, who)
            return any(d["id"] == self.datasets[k].pk for d in r.json()["features"])
        self.check_matrix(sees, surface="/api/datasets/")

    def test_search_db(self):
        def sees(who, k):
            r = self.client.get("/search/db/", {"name": self.title(k), "fclasses": "P"})
            self.assertEqual(r.status_code, 200, who)
            items = r.json().get("suggestions") or []
            return any(i.get("name") == self.title(k) for i in items)
        self.check_matrix(sees, surface="/search/db/")

    def test_entity_api_and_preview(self):
        # Anonymous callers are refused outright for contributed ids (401); the
        # matrix for them is "withheld" by that route.
        for suffix in ("api", "preview"):
            def sees(who, k):
                r = self.client.get(f"/entity/place:{self.places[k].id}/{suffix}")
                return r.status_code == 200
            self.check_matrix(sees, whos=AUTHENTICATED, surface=f"/entity/place:<pk>/{suffix}")
        self.login("anon")
        self.assertEqual(self.client.get(f"/entity/place:{self.places['pub'].id}/api").status_code, 401)

    def test_entity_dataset_stream(self):
        def sees(who, k):
            r = self.client.get(f"/entity/dataset:{self.datasets[k].pk}/api")
            return r.status_code == 200
        self.check_matrix(sees, whos=AUTHENTICATED, surface="/entity/dataset:<id>/api")

    def test_reconcile_extend(self):
        def sees(who, k):
            body = {"extend": {"ids": [f"place:{self.places[k].id}"],
                               "properties": [{"id": "whg:names_canonical"}, {"id": "whg:geometry_wkt"}]}}
            r = self.client.post("/reconcile", data=json.dumps(body), content_type="application/json")
            self.assertEqual(r.status_code, 200, (who, r.content[:200]))
            rows = r.json().get("rows", {})
            return f"place:{self.places[k].id}" in rows and self.title(k) in json.dumps(rows)
        self.check_matrix(sees, whos=AUTHENTICATED, surface="/reconcile EXTEND")

    def test_annotation_form_and_elastic_fetch(self):
        from collection.models import Collection
        coll = Collection.objects.create(owner=self.users["owner"], title="C", collection_class="place",
                                         status="sandbox")
        def sees_form(who, k):
            r = self.client.get("/collections/annoform/", {"p": self.places[k].id, "c": coll.id})
            return r.status_code == 200 and self.title(k) in r.json()["form"]
        self.check_matrix(sees_form, surface="/collections/annoform/")

        docs = []
        def sees_fetch(who, k):
            fake = FakeES([{"_id": "w1", "_source": {"place_id": self.places[k].id, "title": self.title(k),
                                                    "dataset": self.datasets[k].label, "whg_id": "w1",
                                                    "relation": {"name": "parent"}, "children": [],
                                                    "ccodes": ["GB"]}}])
            with patch("elastic.es_utils.es", fake):
                r = self.client.post("/elastic/fetch/", {"pid": self.places[k].id})
            return r.status_code == 200 and r.json()["dbplace"]["title"] == self.title(k)
        self.check_matrix(sees_fetch, surface="/elastic/fetch/")

    def test_comments_do_not_confirm_a_withheld_place(self):
        self.login("beta")
        r = self.client.post("/comment/", {"commentText": "x", "tag": "t", "placeId": self.places["priv"].id})
        self.assertFalse(r.json().get("success"), r.content)
        self.assertEqual(self.places["priv"].comments.count() if hasattr(self.places["priv"], "comments") else 0, 0)
        r = self.client.post("/comment/", {"commentText": "x", "tag": "t", "placeId": self.places["pub"].id})
        self.assertTrue(r.json().get("success"), r.content)

    def test_workbench_record_routes(self):
        def sees_suggest(who, k):
            r = self.client.get(f"/reconciliation/suggestions/for-place/{self.places[k].id}/")
            return r.status_code == 200
        # beta-gated: only beta users reach the dataset rule; normal/anon are refused earlier
        self.check_matrix(sees_suggest, whos=("owner", "collab", "staff", "beta"),
                          surface="/reconciliation/suggestions/for-place/")

        def sees_checkout(who, k):
            r = self.client.post(f"/reconciliation/checkout/place/{self.places[k].id}/",
                                 data="{}", content_type="application/json")
            return r.status_code == 201
        self.check_matrix(sees_checkout, whos=("owner", "collab", "staff", "beta"),
                          surface="/reconciliation/checkout/place/")


# ---------------------------------------------------------------------------
# Collections: viewable by link, private members withheld
# ---------------------------------------------------------------------------

class CollectionSurfaceTests(VisibilityMatrixBase):
    @classmethod
    def setUpTestData(cls):
        super().setUpTestData()
        from collection.models import Collection, CollDataset, CollPlace
        from traces.models import TraceAnnotation
        with patch("datasets.signals.doi"):
            cls.ds_coll = Collection.objects.create(owner=cls.users["owner"], title="DS coll",
                                                    collection_class="dataset", status="sandbox",
                                                    keywords=["k"])
            for ds in cls.datasets.values():
                CollDataset.objects.create(collection=cls.ds_coll, dataset=ds)
            cls.pl_coll = Collection.objects.create(owner=cls.users["owner"], title="Place coll",
                                                    collection_class="place", status="sandbox",
                                                    keywords=["k"])
            for i, p in enumerate(cls.places.values()):
                CollPlace.objects.create(collection=cls.pl_coll, place=p, sequence=i)
                TraceAnnotation.objects.create(collection=cls.pl_coll, place=p, owner=cls.users["owner"],
                                               anno_type="place", motivation="locating", relation=["waypoint"],
                                               saved=True)

    def test_collection_model_helpers(self):
        for who in WHO:
            u = self.user(who)
            want = {self.datasets[k].pk for k in DATASETS if not expected(who, k)}
            self.assertEqual(set(self.ds_coll.withheld_dataset_pks(u)), want, who)
            self.assertEqual(set(self.pl_coll.withheld_dataset_pks(u)), want, who)
            self.assertEqual({p.id for p in self.pl_coll.visible_places(u)},
                             {self.places[k].id for k in DATASETS if expected(who, k)}, who)
            self.assertEqual({d["id"] for d in self.ds_coll.visible_ds_list(u)},
                             {self.datasets[k].pk for k in DATASETS if expected(who, k)}, who)
            self.assertEqual(self.pl_coll.visible_num_places(u), sum(expected(who, k) for k in DATASETS), who)

    def _mapdata_caches(self):
        from django.conf import settings
        caches = dict(settings.CACHES)
        caches["default"] = {"BACKEND": "django.core.cache.backends.locmem.LocMemCache", "LOCATION": "v310"}
        return caches

    def test_mapdata_collections(self):
        from django.conf import settings
        with override_settings(CACHES=self._mapdata_caches()):
            for coll, label in ((self.ds_coll, "dataset"), (self.pl_coll, "place")):
                def sees(who, k):
                    r = self.client.get(f"/mapdata/collections/{coll.id}/")
                    self.assertEqual(r.status_code, 200, (who, r.content[:300]))
                    return self.title(k) in mapdata_titles(r.json())
                # An insider first: their (unfiltered) view is the one that gets cached,
                # and the outsider probes after it must NOT be served from that cache.
                self.check_matrix(sees, whos=("owner", "beta", "anon", "collab", "normal", "staff"),
                                  surface=f"/mapdata/collections/ ({label})")

    def test_mapdata_filtered_view_is_not_cached_and_shared_view_is(self):
        from django.core.cache import cache
        with override_settings(CACHES=self._mapdata_caches()):
            cache.clear()
            import itertools
            ticks = itertools.count(0, 10)
            with patch("utils.mapdata.time.time", side_effect=lambda: float(next(ticks))):
                self.login("beta")
                self.client.get(f"/mapdata/collections/{self.pl_coll.id}/")
                self.assertIsNone(cache.get(f"collections_{self.pl_coll.id}"))
                self.login("owner")
                self.client.get(f"/mapdata/collections/{self.pl_coll.id}/")
                cached = cache.get(f"collections_{self.pl_coll.id}")
            self.assertIsNotNone(cached)
            self.assertEqual(len(mapdata_titles(cached)), 3)

    def test_collection_apis(self):
        def feature_titles(r):
            data = r.json()
            feats = data.get("features") if isinstance(data, dict) else data
            feats = feats or (data.get("results") if isinstance(data, dict) else [])
            out = set()
            for f in feats or []:
                props = f.get("properties") or {}
                out.add(props.get("title") or f.get("title"))
            return out

        probes = {
            "/api/placetable_coll/ (dataset)": lambda: self.client.get("/api/placetable_coll/", {"id": self.ds_coll.id}),
            "/api/placetable_coll/ (place)": lambda: self.client.get("/api/placetable_coll/", {"id": self.pl_coll.id}),
            "/api/geoms/?coll": lambda: self.client.get("/api/geoms/", {"coll": self.pl_coll.id}),
            "/api/geojson/?coll": lambda: self.client.get("/api/geojson/", {"coll": self.pl_coll.id}),
            "/api/featureCollection/?coll (place)": lambda: self.client.get("/api/featureCollection/", {"coll": self.pl_coll.id}),
            "/search/collgeom/ (dataset)": lambda: self.client.get("/search/collgeom/", {"coll_id": self.ds_coll.id}),
            "/collections/<id>/geojson/": lambda: self.client.get(f"/collections/{self.pl_coll.id}/geojson/"),
        }
        for name, probe in probes.items():
            def sees(who, k, probe=probe, name=name):
                r = probe()
                self.assertEqual(r.status_code, 200, (name, who, r.content[:200]))
                body = r.content.decode()
                if name.startswith("/api/featureCollection") or name.startswith("/api/geoms"):
                    # geometry only: tell the members apart by their coordinates
                    lon = {"pub": -1.0, "priv": -1.1, "emb": -1.2}[k]
                    return json.dumps(lon) in body or f"{lon}," in body or f"[{lon}" in body
                return self.title(k) in body
            self.check_matrix(sees, surface=name)

    def test_collection_browse_pages_withhold_member_datasets(self):
        def sees(who, k):
            r = self.client.get(f"/collections/{self.ds_coll.id}/browse_ds")
            self.assertEqual(r.status_code, 200, who)
            return any(d["id"] == self.datasets[k].pk for d in r.context["datasets"])
        self.check_matrix(sees, surface="/collections/<id>/browse_ds")

        def sees_pl(who, k):
            r = self.client.get(f"/collections/{self.pl_coll.id}/browse_pl")
            self.assertEqual(r.status_code, 200, who)
            return any(d["id"] == self.datasets[k].pk for d in r.context["ds_list"])
        self.check_matrix(sees_pl, surface="/collections/<id>/browse_pl")

    def test_collection_entity_stream(self):
        import gzip
        def sees(who, k):
            r = self.client.get(f"/entity/collection:{self.pl_coll.id}/api")
            self.assertEqual(r.status_code, 200, who)
            body = gzip.decompress(b"".join(r.streaming_content)).decode()
            return self.title(k) in body
        self.check_matrix(sees, whos=AUTHENTICATED, surface="/entity/collection:<id>/api")

    def test_collection_download_task_filters_by_viewer(self):
        from utils.tasks import make_download
        with patch("utils.tasks.make_download.delay") as delay:
            delay.return_value.task_id = "t"
            self.login("beta")
            r = self.client.post("/dlcelery/", {"collid": self.pl_coll.id, "format": "json"},
                                 HTTP_X_REQUESTED_WITH="XMLHttpRequest")
            self.assertEqual(r.status_code, 200, r.content)
            self.assertEqual(delay.call_args.kwargs["viewer_id"], self.users["beta"].id)
            self.client.logout()
            self.client.post("/dlcelery/", {"collid": self.pl_coll.id, "format": "json"},
                             HTTP_X_REQUESTED_WITH="XMLHttpRequest")
            self.assertIsNone(delay.call_args.kwargs["viewer_id"])

    def test_workbench_publish_cannot_add_a_withheld_place_but_keeps_an_existing_member(self):
        from workbench.publish import _resolve_places
        refs = [{"id": f"whg:{self.places[k].id}"} for k in DATASETS]
        resolved, unresolved = _resolve_places(refs, user=self.users["beta"])
        self.assertEqual({p.id for p, _ in resolved}, {self.places["pub"].id})
        self.assertEqual(set(unresolved), {str(self.places["priv"].id), str(self.places["emb"].id)})
        resolved, unresolved = _resolve_places(refs, user=self.users["beta"],
                                               keep_pks=[self.places["priv"].id])
        self.assertEqual({p.id for p, _ in resolved}, {self.places["pub"].id, self.places["priv"].id})
        resolved, unresolved = _resolve_places(refs, user=self.users["owner"])
        self.assertEqual(len(resolved), 3)

    def test_workbench_checkout_withholds_titles_and_coordinates(self):
        from workbench.checkout import checkout_place_collection
        snapshot, _ = checkout_place_collection(self.pl_coll, user=self.users["beta"])
        by_id = {ref["id"]: ref for ref in snapshot["places"]}
        self.assertEqual(len(by_id), 3)  # membership and order survive the round trip
        priv = by_id[f"whg:{self.places['priv'].id}"]
        self.assertTrue(priv.get("withheld"))
        self.assertEqual(priv["title"], "")
        self.assertNotIn("lng", priv)
        self.assertEqual(by_id[f"whg:{self.places['pub'].id}"]["title"], self.title("pub"))
        snapshot, _ = checkout_place_collection(self.pl_coll, user=self.users["owner"])
        self.assertTrue(all(ref["title"] for ref in snapshot["places"]))


# ---------------------------------------------------------------------------
# Search backed by the legacy index: the clause is written AND addressed right
# ---------------------------------------------------------------------------

class LegacyIndexSurfaceTests(VisibilityMatrixBase):
    def docs(self):
        return [{"_id": f"w{k}", "_index": "whg", "_score": 1.0,
                 "_source": {"place_id": self.places[k].id, "title": self.title(k),
                             "dataset": self.datasets[k].label, "whg_id": f"w{k}",
                             "names": [{"toponym": self.title(k)}], "searchy": [self.title(k)],
                             "children": [], "links": [], "types": [], "ccodes": ["GB"], "timespans": [],
                             "fclasses": ["P"], "descriptions": [], "depictions": [], "relations": [],
                             "minmax": [], "uri": "", "src_id": "1",
                             "geoms": [{"location": {"type": "Point", "coordinates": [-1.0, 51.0]}}],
                             "relation": {"name": "parent"}}}
                for k in DATASETS]

    def run_matrix(self, probe, surface):
        fake = FakeES(self.docs())
        with override_settings(ES_CONN=fake), patch("api.reconcile_helpers.es", fake), \
                patch("elastic.es_utils.es", fake):
            def sees(who, k):
                r = probe()
                self.assertEqual(r.status_code, 200, (surface, who, r.content[:200]))
                return self.title(k) in r.content.decode()
            self.check_matrix(sees, surface=surface)
        # The fake was consulted: an absence above is an absence from a real answer.
        self.assertGreaterEqual(len(fake.bodies), len(WHO) * len(DATASETS))
        return fake

    def test_search_index(self):
        fake = self.run_matrix(lambda: self.client.post(
            "/search/index/", data=json.dumps({"qstr": "Placeville", "mode": "fuzzy"}),
            content_type="application/json"), "/search/index/")
        # The owner's query carries no exclusion; an outsider's names both withheld labels.
        self.login("anon")
        self.assertEqual(_must_not_dataset_labels(fake.bodies[-1]) & {d.label for d in self.datasets.values()},
                         {self.datasets["priv"].label, self.datasets["emb"].label})

    def test_search_suggestions(self):
        self.run_matrix(lambda: self.client.get("/search/suggestions/", {"q": "Placeville"}),
                        "/search/suggestions/")

    def test_api_index_by_name(self):
        self.run_matrix(lambda: self.client.get("/api/index/", {"name": "Placeville"}), "/api/index/?name")

    def test_search_context(self):
        self.run_matrix(lambda: self.client.get("/search/context/", {
            "idx": "whg", "doc_type": "place", "task": "features",
            "extent": json.dumps([[[-2, 50], [0, 50], [0, 52], [-2, 52], [-2, 50]]])}), "/search/context/")

    def test_search_tracegeom(self):
        # The trace body names all three places; the geometry fetch must still exclude.
        fake = FakeES(self.docs())
        trace_doc = {"_source": {"body": [{"place_id": self.places[k].id} for k in DATASETS]}}
        calls = {"n": 0}
        real_search = fake.search
        def search(index=None, body=None, **kw):
            if index == "trace":
                return {"hits": {"hits": [trace_doc]}}
            return real_search(index=index, body=body, **kw)
        fake.search = search
        with override_settings(ES_CONN=fake):
            def sees(who, k):
                r = self.client.get("/search/tracegeom/", {"idx": "trace", "search": "t1", "doc_type": "trace"})
                self.assertEqual(r.status_code, 200, who)
                return self.title(k) in r.content.decode()
            self.check_matrix(sees, surface="/search/tracegeom/")

    def test_sitemap_toponyms_come_from_public_places_only(self):
        from sitemap.models import Toponym
        from sitemap.tasks import populate_toponyms
        populate_toponyms()
        names = set(Toponym.objects.values_list("name", flat=True))
        self.assertIn(self.title("pub"), names)
        self.assertNotIn(self.title("priv"), names)
        self.assertNotIn(self.title("emb"), names)


# ---------------------------------------------------------------------------
# Gateway-backed surfaces: contributed hits are dropped by their dataset id
# ---------------------------------------------------------------------------

@override_settings(CRC_GATEWAY_URL=GATEWAY, CRC_GATEWAY_TIMEOUT=1, CRC_GATEWAY_API_KEY="k")
class GatewaySurfaceTests(VisibilityMatrixBase):
    def gateway_hits(self):
        return [{"place_id": f"whg:{self.datasets[k].pk}:1", "title": self.title(k), "namespace": "whg",
                 "names": [{"toponym": self.title(k)}], "ccodes": ["GB"], "score": 90 - i,
                 "geometries": [{"type": "Point", "coordinates": [-1.0, 51.0]}], "types": [], "links": []}
                for i, k in enumerate(DATASETS)]

    def fake_post(self, url, json=None, **kw):
        hits = self.gateway_hits()
        if url.endswith("/api/search"):
            return _Resp(200, {"hits": hits, "total": len(hits),
                               "edges": [{"a": hits[0]["place_id"], "b": hits[1]["place_id"],
                                          "relation_type": "same_as"}],
                               "clustering_params": None, "toponym_stoplist": []})
        if url.endswith("/api/reconcile"):
            return _Resp(200, {"hits": hits, "total": len(hits), "namespaces_searched": ["whg"]})
        return _Resp(404, {})

    def test_atlas_search(self):
        def sees(who, k):
            with patch("api.crc_client.requests.post", side_effect=self.fake_post):
                r = self.client.post("/atlas/search/", data=json.dumps({"qstr": "Placeville"}),
                                     content_type="application/json")
            self.assertEqual(r.status_code, 200, (who, r.content[:200]))
            return any(h["place_id"].endswith(f":{self.datasets[k].pk}:1") for h in r.json()["hits"])
        self.check_matrix(sees, whos=("owner", "collab", "staff", "beta"), surface="/atlas/search/")
        # and the edge that touched a withheld hit went with it
        self.login("beta")
        with patch("api.crc_client.requests.post", side_effect=self.fake_post):
            r = self.client.post("/atlas/search/", data=json.dumps({"qstr": "Placeville"}),
                                 content_type="application/json")
        self.assertEqual(r.json()["edges"], [])
        self.assertEqual(r.json()["total"], 1)
        self.login("owner")
        with patch("api.crc_client.requests.post", side_effect=self.fake_post):
            r = self.client.post("/atlas/search/", data=json.dumps({"qstr": "Placeville"}),
                                 content_type="application/json")
        self.assertEqual(len(r.json()["edges"]), 1)

    def test_reconcile_queries(self):
        fake = FakeES([])
        def sees(who, k):
            body = {"queries": {"q0": {"query": "Placeville"}}}
            with patch("api.crc_client.requests.post", side_effect=self.fake_post), \
                    patch("api.reconcile_helpers.es", fake):
                r = self.client.post("/reconcile", data=json.dumps(body), content_type="application/json")
            self.assertEqual(r.status_code, 200, (who, r.content[:300]))
            cands = r.json()["q0"]["result"]
            return any(c["id"].endswith(f":{self.datasets[k].pk}:1") for c in cands)
        self.check_matrix(sees, whos=AUTHENTICATED, surface="/reconcile queries")

    def test_suggest_entity(self):
        fake = FakeES([])
        def sees(who, k):
            with patch("api.crc_client.requests.post", side_effect=self.fake_post), \
                    patch("api.reconcile_helpers.es", fake):
                r = self.client.get("/suggest/entity", {"prefix": "Placeville"})
            self.assertEqual(r.status_code, 200, (who, r.content[:300]))
            return any(c["id"].endswith(f":{self.datasets[k].pk}:1") for c in r.json()["result"])
        self.check_matrix(sees, whos=AUTHENTICATED, surface="/suggest/entity")

    def test_authority_datasets_pending_list(self):
        def sees(who, k):
            r = self.client.get("/reconcile/authority-datasets", {"include_pending": "true"})
            self.assertEqual(r.status_code, 200, (who, r.content[:200]))
            return any(d["id"] == self.datasets[k].pk for d in r.json()["result"])
        self.check_matrix(sees, whos=AUTHENTICATED, surface="/reconcile/authority-datasets")

    def test_atlas_page_specialist_rows(self):
        from api.models import GazetteerRegistryEntry
        for k in ("pub", "priv"):
            GazetteerRegistryEntry.objects.update_or_create(
                id=f"whg:{self.datasets[k].pk}",
                defaults={"name": f"Registry {k}", "namespace": "whg", "entry_class": "dataset",
                          "status": "published"})
        def sees(who, k):
            r = self.client.get("/atlas/")
            self.assertEqual(r.status_code, 200, who)
            return any(row["id"] == f"whg:{self.datasets[k].pk}" for row in r.context["specialist_gazetteers"])
        # (the pub row's released embargo was replaced by a published row above)
        self.check_matrix(sees, keys=("pub", "priv"), surface="/atlas/ specialist gazetteers")


# ---------------------------------------------------------------------------
# Pre-ship review of f0965975a: four more places the rule had to reach
# ---------------------------------------------------------------------------

class ReviewFixTests(VisibilityMatrixBase):
    @classmethod
    def setUpTestData(cls):
        super().setUpTestData()
        from collection.models import Collection, CollPlace
        from traces.models import TraceAnnotation
        from django.contrib.gis.geos import Polygon
        with patch("datasets.signals.doi"):
            cls.pl_coll = Collection.objects.create(
                owner=cls.users["owner"], title="Place coll", collection_class="place",
                status="sandbox", keywords=["k"],
                bbox=Polygon.from_bbox((-1.2, 51.0, -1.0, 51.2)))  # stored over ALL members
            for i, p in enumerate(cls.places.values()):
                CollPlace.objects.create(collection=cls.pl_coll, place=p, sequence=i)
                TraceAnnotation.objects.create(collection=cls.pl_coll, place=p, owner=cls.users["owner"],
                                               anno_type="place", motivation="locating",
                                               relation=["waypoint"], saved=True)

    def legacy_docs(self):
        base = LegacyIndexSurfaceTests.docs(self)
        # a second PUBLIC doc: the one a should-less bundle query must NOT return
        other = json.loads(json.dumps(base[0]))
        other.update({"_id": "wother"})
        other["_source"].update({"place_id": 999999, "title": "Otherville", "whg_id": "wother",
                                 "names": [{"toponym": "Otherville"}], "searchy": ["Otherville"]})
        return base + [other]

    def test_api_index_whgid_bundle_is_the_asked_bundle_only(self):
        """A visibility filter added to a should-only bool must not turn the
        shoulds into optional boosts (ES minimum_should_match 1 → 0): the
        response is the asked parent and its children, never other visible docs."""
        fake = FakeES(self.legacy_docs())
        with override_settings(ES_CONN=fake):
            wrong = []
            for who in WHO:
                self.login(who)
                for k in DATASETS:
                    r = self.client.get("/api/index/", {"whgid": f"w{k}"})
                    self.assertEqual(r.status_code, 200, (who, k, r.content[:200]))
                    titles = {f["properties"]["title"] for f in r.json()["features"]}
                    want = {self.title(k)} if expected(who, k) else set()
                    if titles != want:
                        wrong.append(f"{who}/{k}: got {sorted(titles)}, expected {sorted(want)}")
            self.assertEqual(wrong, [], "\n".join(wrong))

    def test_portal_extent_and_centroid_cover_visible_members_only(self):
        ids = ",".join(str(self.places[k].id) for k in DATASETS)
        self.login("beta")
        r = self.client.get(f"/places/portal/{ids}/")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.context["extent"], [-1.0, 51.0, -1.0, 51.0])
        self.assertEqual(r.context["centroid"], [-1.0, 51.0])
        self.login("owner")
        r = self.client.get(f"/places/portal/{ids}/")
        self.assertEqual(r.context["extent"], [-1.2, 51.0, -1.0, 51.2])

    def test_mapdata_bounds_cover_visible_members_only(self):
        caches = CollectionSurfaceTests._mapdata_caches(self)
        with override_settings(CACHES=caches):
            def minx(r):
                self.assertEqual(r.status_code, 200, r.content[:200])
                return min(x for x, _y in r.json()["metadata"]["bounds"]["coordinates"][0])
            self.login("beta")
            self.assertEqual(minx(self.client.get(f"/mapdata/collections/{self.pl_coll.id}/")), -1.0)
            self.login("owner")
            self.assertEqual(minx(self.client.get(f"/mapdata/collections/{self.pl_coll.id}/")), -1.2)

    def test_attribution_does_not_resolve_a_withheld_place_or_dataset(self):
        """``/api/attribution/?ids=`` names a contributed place's dataset (label,
        citation, licence) for the ``whg:<ds>:<src>``, ``whg:<pk>`` and bare-pk
        forms; a withheld one answers exactly as an absent id."""
        forms = {
            "bare pk": lambda k: str(self.places[k].id),
            "whg:<pk>": lambda k: f"whg:{self.places[k].id}",
            "whg:<ds>:<src>": lambda k: f"whg:{self.datasets[k].pk}:1",
        }
        wrong = []
        for who in WHO:
            self.login(who)
            for k in DATASETS:
                for form, mk in forms.items():
                    r = self.client.get("/api/attribution/", {"ids": mk(k)})
                    self.assertEqual(r.status_code, 200, (who, k, form))
                    got = self.datasets[k].label in (r.json().get("datasets") or {})
                    if got != expected(who, k):
                        wrong.append(f"{who}/{k} [{form}]: {'named' if got else 'absent'}")
        self.assertEqual(wrong, [], "\n".join(wrong))
        # the control for the control: an absent id names nothing
        self.login("owner")
        self.assertNotIn("datasets", self.client.get("/api/attribution/", {"ids": "999999999"}).json())

    def test_republish_by_an_outsider_leaves_their_annotation_on_a_withheld_member_alone(self):
        from collection.models import CollPlace
        from traces.models import TraceAnnotation
        from workbench.checkout import checkout_place_collection
        from workbench.models import Team, WorkbenchProject
        from workbench.publish import publish_place_collection
        beta = self.users["beta"]
        priv, pub = self.places["priv"], self.places["pub"]
        mine = TraceAnnotation.objects.create(
            collection=self.pl_coll, place=priv, owner=beta, anno_type="place", motivation="locating",
            relation=["waypoint"], note="my note on a place I can no longer see", saved=True)
        TraceAnnotation.objects.create(
            collection=self.pl_coll, place=pub, owner=beta, anno_type="place", motivation="locating",
            relation=["waypoint"], note="old note", saved=True)
        snapshot, base_version = checkout_place_collection(self.pl_coll, user=beta)
        for ref in snapshot["places"]:
            if ref["id"] == f"whg:{pub.id}":
                ref["note"] = "new note"
        project = WorkbenchProject.objects.create(
            team=Team.personal_for(beta), title="pc", created_by=beta, snapshot=snapshot, version=1,
            status="draft", doc_type="place_collection", published_collection=self.pl_coll,
            source_published_id=str(self.pl_coll.pk), base_version=base_version)
        out = publish_place_collection(project, beta)
        self.assertEqual(out["collection_id"], self.pl_coll.pk)
        self.assertEqual(CollPlace.objects.filter(collection=self.pl_coll).count(), 3)   # membership kept
        mine.refresh_from_db()                                                           # row untouched
        self.assertEqual(mine.note, "my note on a place I can no longer see")
        self.assertEqual(TraceAnnotation.objects.filter(collection=self.pl_coll, owner=beta, place=priv).count(), 1)
        self.assertEqual(TraceAnnotation.objects.get(collection=self.pl_coll, owner=beta, place=pub).note, "new note")
        # the owner's annotations were never the publisher's to touch
        self.assertEqual(TraceAnnotation.objects.filter(collection=self.pl_coll, owner=self.users["owner"]).count(), 3)
