"""Server-side clustering on /entity/locus:<PID> JSON has a budget (place#325 review #2).
Run locally only:

    DB_NAME=whgv3beta_f2b GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test search.tests_locus_budget --settings=whg.settings_localtest

The reviewer measured the Python clusterer at 0.15 s for 100 hits, 2.35 s for 400 and
14.9 s for 1000 (O(n^2) pairs, O(C) cannot-link checks per merge), with recipe sizes up
to 10000 and no limit. Three guards, each paired with the unguarded case it must not
disturb: a hit cap, a wall-clock budget checked inside the clusterer's loops (a clean
"skipped", never a 500), and a throttle on JSON dereference. With no deadline the
clusterer's output is identical (parity: search.tests_cluster_parity stays green).

Proved able to fail (2026-10-21): with the cap check removed,
``test_too_many_hits_skips_clustering`` fails; with the deadline never checked,
``test_budget_aborts_cleanly`` and ``test_deadline_raises_in_the_pair_loop`` fail; with
the throttle removed, ``test_json_dereference_is_throttled`` fails.
"""
import json
import time
from unittest.mock import patch

from django.conf import settings
from django.test import TestCase, override_settings

from places import locus_registry as registry
from search import clustering
from search.tests_locus import HITS, POST, SIMPLE, FakeGateway, LocusBase, recipe

LOCMEM = {**settings.CACHES,
          "default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache", "LOCATION": "locus-budget"}}


def many_hits(n):
    out = []
    for i in range(n):
        h = json.loads(json.dumps(HITS[i % 3]))
        h["place_id"] = f"gn:{i + 1}"
        h["repr_point"] = [13.4 + i * 1e-3, 52.5]
        out.append(h)
    return out


class DeadlineUnitTests(TestCase):
    def test_no_deadline_is_the_unguarded_result(self):
        hits = many_hits(40)
        a = clustering.cluster_hits(hits=hits, stoplist=[])
        b = clustering.cluster_hits(hits=hits, stoplist=[], deadline=time.monotonic() + 60)
        self.assertEqual(a["clusters"], b["clusters"])
        self.assertEqual(a["assignments"], b["assignments"])

    def test_deadline_raises_in_the_pair_loop(self):
        with self.assertRaises(clustering.ClusterBudgetExceeded):
            clustering.cluster_hits(hits=many_hits(40), stoplist=[], deadline=time.monotonic() - 1)


@override_settings(V4_LANDING_ENABLED=True, CACHES=LOCMEM)
class EntityBudgetTests(LocusBase):
    def setUp(self):
        super().setUp()
        from django.core.cache import cache
        cache.clear()
        self.client.force_login(self.beta)
        self.entry, _ = registry.mint_recipe(recipe(SIMPLE, members=("gn:1",)), self.beta)

    def current(self, gateway):
        with patch(POST, side_effect=gateway):
            resp = self.client.get(f"/entity/locus:{self.entry.pid}/api")
        self.assertEqual(resp.status_code, 200)
        return resp.json()["current"]

    def test_too_many_hits_skips_clustering(self):
        from api import views_locus
        with patch("search.clustering.resolve_recipe", wraps=clustering.resolve_recipe) as run:
            cur = self.current(FakeGateway(hits=many_hits(views_locus.CLUSTER_MAX_HITS + 1)))
            self.assertFalse(run.called)
        self.assertIsNone(cur["cluster_members"])
        self.assertEqual(cur["cluster"], {"skipped": "too_many_hits", "hits": views_locus.CLUSTER_MAX_HITS + 1,
                                          "cap": views_locus.CLUSTER_MAX_HITS})
        self.assertEqual(len(cur["candidates"]), views_locus.CLUSTER_MAX_HITS + 1)   # retrieval still reported
        # At the cap it clusters.
        cur = self.current(FakeGateway(hits=many_hits(30)))
        self.assertNotIn("skipped", cur["cluster"])
        self.assertIn("gn:1", cur["cluster_members"])

    def test_budget_aborts_cleanly(self):
        with patch("api.views_locus.CLUSTER_TIME_BUDGET_S", -1):
            cur = self.current(FakeGateway(hits=many_hits(30)))
        self.assertIsNone(cur["cluster_members"])
        self.assertEqual(cur["cluster"]["skipped"], "time_budget")
        self.assertEqual(cur["cluster"]["budget_s"], -1)
        cur = self.current(FakeGateway(hits=many_hits(30)))           # the default budget: clustered
        self.assertIn("gn:1", cur["cluster_members"])

    def test_anonymous_throttle_buckets_by_forwarded_client_not_by_proxy(self):
        """Behind nginx REMOTE_ADDR is the proxy for everyone: the bucket must follow
        get_ident (X-Forwarded-For), or all anonymous callers share one allowance."""
        import uuid
        from django.contrib.auth.models import AnonymousUser
        from django.test import RequestFactory
        from api import views_locus
        a, b = (f"203.0.113.{uuid.uuid4().int % 250}", f"198.51.100.{uuid.uuid4().int % 250}")
        if a == b:
            b = "192.0.2.77"

        def req(xff):
            r = RequestFactory().get("/entity/locus:X/api", HTTP_X_FORWARDED_FOR=xff, REMOTE_ADDR="172.18.0.2")
            r.user = AnonymousUser()
            return r
        with patch.object(views_locus, "JSON_DEREFERENCE_RATE", (1, 3600)):
            self.assertIsNone(views_locus._json_dereference_throttled(req(a)))      # first client: allowed
            self.assertIsNone(views_locus._json_dereference_throttled(req(b)))      # a different client: its own bucket
            self.assertEqual(views_locus._json_dereference_throttled(req(a)).status_code, 429)  # the first again: limited

    def test_json_dereference_is_throttled(self):
        from api import views_locus
        with patch.object(views_locus, "JSON_DEREFERENCE_RATE", (2, 60)):
            gw = FakeGateway()
            codes = []
            for _ in range(3):
                with patch(POST, side_effect=gw):
                    codes.append(self.client.get(f"/entity/locus:{self.entry.pid}/api").status_code)
            self.assertEqual(codes, [200, 200, 429])
            r = self.client.get(f"/entity/locus:{self.entry.pid}/api")
            self.assertTrue(r["Retry-After"])
            self.assertEqual(len(gw.bodies), 2)                       # the refused ones did no work
            # The HTML page does no server-side work and is not counted.
            self.assertEqual(self.client.get(f"/entity/locus:{self.entry.pid}/", HTTP_ACCEPT="text/html").status_code,
                             200)
