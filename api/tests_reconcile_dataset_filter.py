"""`POST /reconcile` with `dataset`: only the places of that one WHG dataset (2026-11-18).

Until then `dataset` was accepted (it let a name-less query through) and filtered NOTHING: neither the
legacy ES query (`reconcile_helpers.build_es_query`) nor the gateway body (`crc_client
.crc_reconcile_search`) carried it, and `reconcile_place_es` never looked at it. The legacy-API compat
layer worked round that by narrowing to `whg` and filtering the candidate ids itself.

These tests drive the REAL view, the REAL `reconcile_place_es` and the REAL gateway client; only
`requests.post` (a fake gateway that honours `namespaces`, as the real one does) and the legacy
`es_search` (a fake that IGNORES the dataset label, so the Python-side filter is what is tested) are
replaced. Nothing reaches the CRC gateway or Elasticsearch. Datasets, users and the place#310
visibility rule are real rows in the test DB.

Every "filtered out" assertion sits beside a "still there" one in the same test: the same hits, the
same fake, without `dataset` (or as a user who may see the dataset), come back.

    ENV_CONTEXT=local venv/bin/python manage.py test api.tests_reconcile_dataset_filter \\
        --settings=whg.settings_localtest
"""

import json
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import SimpleTestCase, TestCase
from rest_framework.test import APIRequestFactory, force_authenticate


class _Resp:
    status_code = 200

    def __init__(self, data):
        self._data = data

    def raise_for_status(self):
        pass

    def json(self):
        return self._data


class InlinePool:
    """process_queries runs each query on a worker thread with its own DB connection, which cannot
    see a TestCase's uncommitted rows (every dataset would look missing, and the filter would pass
    by returning nothing). Run the workers inline on the test's connection instead; the worker's
    `db_connection.close()` is patched out alongside, as closing inside the test transaction would
    break it."""

    def __init__(self, *a, **kw):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def map(self, fn, items):
        return [fn(i) for i in items]


def inline_reconcile():
    """Patches that make process_queries run its workers on the test's own DB connection."""
    from unittest.mock import MagicMock
    return [patch("api.reconcile.ThreadPoolExecutor", InlinePool),
            patch("api.reconcile.db_connection", MagicMock())]


def _gw_hit(place_id, title):
    return {"place_id": place_id, "title": title, "names": [{"label": title}], "ccodes": ["GB"],
            "score": 0, "geometries": [{"repr_point": [-1.0, 51.0]}]}


class FakeGateway:
    """The gateway's `/api/reconcile`: returns its fixed hits, narrowed to `namespaces` when sent, and
    nothing for a query with no `query`, `contained_in` or `bounds` (its early return; see
    tests_reconcile_circle_only)."""

    def __init__(self, hits):
        self.hits = hits
        self.bodies = []

    def __call__(self, url, json=None, **kwargs):
        body = json or {}
        self.bodies.append(body)
        if not (body.get("query") or "").strip() and not body.get("contained_in") and not body.get("bounds"):
            return _Resp({"hits": [], "namespaces_searched": []})
        ns = body.get("namespaces")
        hits = [h for h in self.hits if not ns or h["place_id"].split(":", 1)[0] in ns]
        return _Resp({"hits": hits, "namespaces_searched": sorted(ns or [])})


class FakeLegacy:
    """`es_search` for the legacy index. Ignores `dataset_label` on purpose (records it), so what is
    filtered is filtered by reconcile_place_es, not by this fake."""

    def __init__(self, hits):
        self.hits = hits
        self.queries = []

    def __call__(self, query=None, hidden=None, **kwargs):
        self.queries.append(query)
        return [dict(h) for h in self.hits]


class DatasetFilterThroughTheView(TestCase):

    @classmethod
    def setUpTestData(cls):
        from datasets.models import Dataset
        User = get_user_model()
        cls.owner = User.objects.create_user(username="dsf_owner", email="dsf_owner@example.org", password="pw",
                                             given_name="DSF", surname="Owner")
        cls.outsider = User.objects.create_user(username="dsf_out", email="dsf_out@example.org", password="pw",
                                                given_name="DSF", surname="Outsider")
        with patch("datasets.signals.doi"):
            cls.a = Dataset.objects.create(owner=cls.owner, label="dsf_a", title="A", description="D",
                                           creator="C", public=True, ds_status="indexed",
                                           uri_base="https://example.org/a/")
            cls.b = Dataset.objects.create(owner=cls.owner, label="dsf_b", title="B", description="D",
                                           creator="C", public=True, ds_status="indexed",
                                           uri_base="https://example.org/b/")
            cls.private = Dataset.objects.create(owner=cls.owner, label="dsf_private", title="P", description="D",
                                                 creator="C", public=False, ds_status="uploaded",
                                                 uri_base="https://example.org/p/")

    def setUp(self):
        # reconcile_helpers caches the dataset label -> pk map; one built by another test DB is stale.
        from django.core.cache import cache
        cache.clear()

    def gateway_hits(self):
        return [_gw_hit("gn:2643743", "London"),
                _gw_hit(f"whg:{self.a.id}:a1", "Aville"),
                _gw_hit(f"whg:{self.b.id}:b1", "Bville"),
                _gw_hit(f"whg:{self.private.id}:p1", "Pville")]

    def legacy_hits(self):
        def doc(pk, label, src_id, title):
            return {"_id": pk, "_score": 5.0,
                    "_source": {"place_id": pk, "dataset": label, "src_id": src_id, "title": title,
                                "names": [{"toponym": title}], "ccodes": ["GB"]}}
        return [doc("9000001", "dsf_a", "a2", "Aford"), doc("9000002", "dsf_b", "b2", "Bford"),
                doc("9000003", "dsf_private", "p2", "Pford")]

    def post(self, q, user=None):
        from api.reconcile import ReconciliationView
        request = APIRequestFactory().post("/reconcile", data=json.dumps({"queries": {"q0": q}}),
                                           content_type="application/json")
        force_authenticate(request, user=user or self.outsider)
        gateway, legacy = FakeGateway(self.gateway_hits()), FakeLegacy(self.legacy_hits())
        pool, conn = inline_reconcile()
        with pool, conn, patch("api.crc_client._is_enabled", return_value=True), \
                patch("api.crc_client._headers", return_value={}), \
                patch("api.crc_client.requests.post", side_effect=gateway), \
                patch("api.reconcile.es_search", side_effect=legacy), \
                patch("api.reconcile._attribution_for_results", return_value={}), \
                patch("api.reconcile.consume_query_budget", return_value=(True, 0)):
            response = ReconciliationView.as_view()(request)
        self.assertEqual(response.status_code, 200, response.content[:300])
        return json.loads(response.content)["q0"], gateway, legacy

    def ids(self, res):
        return sorted(c["id"] for c in res["result"])

    # --- the filter ---------------------------------------------------------------------------------

    def test_no_dataset_returns_every_visible_source_presence_control(self):
        res, gateway, _ = self.post({"query": "ville"})
        a, b = self.a.id, self.b.id
        self.assertEqual(self.ids(res), sorted([
            "place:gn:2643743", f"place:whg:{a}:a1", f"place:whg:{b}:b1",
            f"place:whg:{a}:a2", f"place:whg:{b}:b2"]))
        # place#310 without `dataset`: the outsider never sees the private dataset's places.
        self.assertFalse(any(f"whg:{self.private.id}:" in i for i in self.ids(res)))
        self.assertNotIn("namespaces", gateway.bodies[0])

    def test_dataset_keeps_only_that_datasets_places_on_both_arms(self):
        a = self.a.id
        for value in (a, str(a), f"whg:{a}", "dsf_a", " dsf_a "):
            with self.subTest(dataset=value):
                res, _, _ = self.post({"query": "ville", "dataset": value})
                self.assertNotIn("error", res)
                # a1 from the gateway, a2 from the legacy index: both arms kept their dataset-A hits ...
                self.assertEqual(self.ids(res), [f"place:whg:{a}:a1", f"place:whg:{a}:a2"])

    def test_the_other_dataset_is_selected_just_as_well(self):
        # ... and it is not "dataset A always wins": B selects B's places.
        b = self.b.id
        res, _, _ = self.post({"query": "ville", "dataset": "dsf_b"})
        self.assertEqual(self.ids(res), [f"place:whg:{b}:b1", f"place:whg:{b}:b2"])

    def test_gateway_is_asked_for_whg_only_and_as_many_as_it_gives(self):
        from api.crc_client import GATEWAY_MAX_SIZE
        _, gateway, legacy = self.post({"query": "ville", "dataset": self.a.id, "limit": 5})
        self.assertEqual(gateway.bodies[0]["namespaces"], ["whg"])
        self.assertEqual(gateway.bodies[0]["size"], GATEWAY_MAX_SIZE)
        self.assertEqual(legacy.queries[0]["dataset_label"], "dsf_a")
        # Presence control: without `dataset` the body asks for what the caller asked for.
        _, gateway, legacy = self.post({"query": "ville", "limit": 5})
        self.assertEqual(gateway.bodies[0]["size"], 5)
        self.assertNotIn("dataset_label", legacy.queries[0])

    def test_limit_still_trims_the_filtered_list(self):
        res, _, _ = self.post({"query": "ville", "dataset": self.a.id, "limit": 1})
        self.assertEqual(len(res["result"]), 1)

    def test_namespaces_without_whg_leave_nothing(self):
        res, gateway, legacy = self.post({"query": "ville", "dataset": self.a.id, "namespaces": "gn"})
        self.assertEqual(res["result"], [])
        self.assertEqual((gateway.bodies, legacy.queries), ([], []))
        # Presence control: the same namespaces without `dataset` do return the gn place.
        res, _, _ = self.post({"query": "ville", "namespaces": "gn"})
        self.assertEqual(self.ids(res), ["place:gn:2643743"])

    # --- visibility (place#310) ----------------------------------------------------------------------

    def test_a_dataset_the_requester_may_not_see_answers_as_one_that_does_not_exist(self):
        hidden, _, _ = self.post({"query": "ville", "dataset": self.private.id})
        by_label, _, _ = self.post({"query": "ville", "dataset": "dsf_private"})
        missing, _, _ = self.post({"query": "ville", "dataset": 987654321})
        missing_label, _, _ = self.post({"query": "ville", "dataset": "no_such_dataset"})
        self.assertEqual(hidden["result"], [])
        self.assertNotIn("error", hidden)
        self.assertEqual(hidden, missing)
        self.assertEqual(by_label, missing_label)
        self.assertEqual(hidden, by_label)

    def test_the_owner_does_see_the_private_dataset_presence_control(self):
        p = self.private.id
        res, _, _ = self.post({"query": "ville", "dataset": self.private.id}, user=self.owner)
        self.assertEqual(self.ids(res), [f"place:whg:{p}:p1", f"place:whg:{p}:p2"])

    def test_the_legacy_arm_is_not_asked_about_a_dataset_it_may_not_name(self):
        _, gateway, legacy = self.post({"query": "ville", "dataset": self.private.id})
        self.assertEqual(legacy.queries, [])
        self.assertEqual(len(gateway.bodies), 1)  # still consulted: the response shape must not differ
        _, _, legacy = self.post({"query": "ville", "dataset": self.private.id}, user=self.owner)
        self.assertEqual(legacy.queries[0]["dataset_label"], "dsf_private")

    # --- parameter handling ---------------------------------------------------------------------------

    def test_malformed_dataset_is_a_per_query_error(self):
        for bad in (["dsf_a"], True, {"id": 1}, "whg:x", 0, "whg:0"):
            with self.subTest(dataset=bad):
                res, gateway, legacy = self.post({"query": "ville", "dataset": bad})
                self.assertIn("`dataset` must name one WHG dataset", res["error"])
                self.assertEqual((gateway.bodies, legacy.queries), ([], []))

    def test_dataset_alone_is_a_query_and_an_empty_dataset_is_not(self):
        res, _, legacy = self.post({"dataset": "dsf_a"})
        self.assertNotIn("error", res)
        self.assertIsNone(legacy.queries[0]["query_text"])
        self.assertEqual(legacy.queries[0]["dataset_label"], "dsf_a")
        self.assertEqual(self.ids(res), [f"place:whg:{self.a.id}:a2"])
        # `"dataset": ""` constrains nothing, so it may not license a name-less query either.
        res, _, _ = self.post({"dataset": "", "type": "https://whgazetteer.org/static/whg_schema.jsonld#Place"})
        self.assertIn("Empty query", res["error"])


class LegacyEsQueryCarriesTheDataset(SimpleTestCase):
    """The ES body the legacy arm sends: a term filter on the `dataset` label, and match_all for a
    name-less dataset query (a multi_match with a null query is an ES 400)."""

    def body(self, raw, query_text, label):
        from api import reconcile_helpers
        captured = {}

        class _ES:
            def search(self, index=None, body=None, **kw):
                captured["body"] = body
                return {"hits": {"hits": []}}

        with patch.object(reconcile_helpers, "es", _ES()):
            reconcile_helpers.es_search(query={"raw": raw, "query_text": query_text, "size": 10,
                                               "dataset_label": label})
        return captured["body"]

    def test_term_filter_on_the_label(self):
        b = self.body({"query": "x"}, "x", "dsf_a")
        self.assertIn({"term": {"dataset": "dsf_a"}}, b["query"]["bool"]["filter"])
        self.assertIn("multi_match", json.dumps(b["query"]["bool"]["must"]))

    def test_no_label_no_filter_presence_control(self):
        b = self.body({"query": "x"}, "x", None)
        self.assertNotIn("dataset", json.dumps(b))

    def test_a_caller_cannot_supply_the_label_through_raw_params(self):
        b = self.body({"query": "x", "dataset_label": "dsf_private"}, "x", None)
        self.assertNotIn("dsf_private", json.dumps(b))

    def test_nameless_dataset_query_is_match_all_plus_the_filter(self):
        b = self.body({"dataset": "dsf_a"}, None, "dsf_a")
        self.assertIn({"match_all": {}}, b["query"]["bool"]["must"])
        self.assertIn({"term": {"dataset": "dsf_a"}}, b["query"]["bool"]["filter"])
