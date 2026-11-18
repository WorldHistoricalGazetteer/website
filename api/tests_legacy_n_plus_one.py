"""The legacy LPF endpoints (``/api/spatial/``, ``/api/db/``) serialise a page in a CONSTANT
number of SQL queries, and their responses are byte-identical to what they were before.

Why (2026-11-18): ``LPFSerializer`` read six child relations per place (types, links, related,
whens, descriptions, depictions) and, under ``geometry=full``, the geometries too: one query per
relation per place. A 20-place ``/api/spatial/`` page ran ~125 queries. The page is now
prefetched once per relation (``api.views.prefetch_lpf``), so the count no longer grows with
the page.

Two pins:

* ``GoldenOutputTests``: the response BYTES of a set of requests (geometry point/full/none,
  type bbox/nearby, match within/intersects, ``dataset`` filter, ``count``/``count_capped``,
  private-dataset visibility for anonymous and owner, and ``/api/db/``) equal the golden file
  ``api/testdata/legacy_n_plus_one_golden.json``, which was captured from the code BEFORE the
  prefetch. Every primary key in the fixture is fixed, so the bytes do not depend on sequences.
  Regenerate only on purpose: ``WHG_WRITE_GOLDEN=1`` (the test then fails, so a regenerated run
  is never mistaken for a passing one).
* ``QueryCountTests``: a 5-place page and a 25-place page (20 under ``geometry=full``, its cap)
  cost the same number of queries, and that number is at most ``CEILING`` (13 anonymous, 16
  signed in; before the prefetch a 25-place point page was 157, a 20-place full page 146).

    ENV_CONTEXT=local venv/bin/python manage.py test api.tests_legacy_n_plus_one \\
        --settings=whg.settings_localtest
"""

import json
import os
import re
from collections import Counter
from pathlib import Path
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.gis.geos import Point, Polygon
from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext

GOLDEN = Path(__file__).parent / "testdata" / "legacy_n_plus_one_golden.json"
SPATIAL, DB = "/api/spatial/", "/api/db/"
# All fixture places lie in this box (lon 10..11, lat 45..46).
BOX = {"type": "bbox", "sw": "9.9,44.9", "ne": "11.1,46.1"}
# Measured 2026-11-18 after the prefetch, anonymous /api/spatial/: 2 spatial-match queries, 1
# embargo lookup, 1 bounded id scan, 1 page fetch, 6 child relations, 1 geometry query (the
# summaries, or the geoms prefetch under full), 1 attribution = 13 (/api/db/: 11, no spatial
# match). Signed in adds the session, the user and one visibility lookup: 16.
CEILING = 13
CEILING_SIGNED_IN = 16

DS_A, DS_B, DS_P = 970001, 970002, 970003
USER_OWNER, USER_OTHER = 970101, 970102


def _ring(x, y, r):
    return [(x - r, y - r), (x + r, y - r), (x + r, y + r), (x - r, y + r), (x - r, y - r)]


class _Fixture(TestCase):
    """30 places: 20 in public dataset A (one point each), 6 in public dataset B (a point AND a
    polygon each), 4 in private dataset P. Every place has types and links (some links
    augmented, i.e. with a task_id); some have related, whens, descriptions, depictions."""

    @classmethod
    def setUpTestData(cls):
        from datasets.models import Dataset
        from places.models import (Place, PlaceDepiction, PlaceDescription, PlaceGeom, PlaceLink,
                                   PlaceName, PlaceRelated, PlaceType, PlaceWhen)
        User = get_user_model()
        cls.owner = User.objects.create_user(id=USER_OWNER, username="np1_owner", email="np1_owner@example.org",
                                             password="pw", given_name="NP", surname="Owner")
        cls.other = User.objects.create_user(id=USER_OTHER, username="np1_other", email="np1_other@example.org",
                                             password="pw", given_name="NP", surname="Other")
        with patch("datasets.signals.doi"):
            mk = dict(owner=cls.owner, description="D", creator="A Contributor", ds_status="indexed",
                      uri_base="https://example.org/places/")
            def dataset(pk, label, title, public):
                # Created without a pk (a pre_save signal looks a set pk up), then renumbered.
                ds = Dataset.objects.create(label=label, title=title, public=public, **mk)
                Dataset.objects.filter(pk=ds.pk).update(id=pk)
                return Dataset.objects.get(pk=pk)
            cls.ds_a = dataset(DS_A, "np1_a", "Dataset A", True)
            cls.ds_b = dataset(DS_B, "np1_b", "Dataset B", True)
            cls.ds_p = dataset(DS_P, "np1_p", "Dataset P", False)

        pid = 9700000
        child = 9800000

        def nxt():
            nonlocal child
            child += 1
            return child

        def place(ds, i, geoms):
            nonlocal pid
            pid += 1
            title = f"{ds.label}_place_{i:02d} Ünïcode"
            p = Place.objects.create(id=pid, title=title, src_id=f"src{i}", dataset=ds, ccodes=["IT"],
                                     minmax=[1500 + i, 1600 + i], timespans=[[1500 + i, 1600 + i]],
                                     fclasses=["P", "S"] if i % 2 else ["P"])
            PlaceName.objects.create(id=nxt(), place=p, toponym=title, jsonb={"toponym": title})
            for k in range(2):
                PlaceType.objects.create(id=nxt(), place=p, src_id=p.src_id, fclass="P", aat_id=300008347 + k,
                                         jsonb={"identifier": f"aat:{300008347 + k}", "label": f"type {k}",
                                                "sourceLabel": f"src type {k}"})
            PlaceLink.objects.create(id=nxt(), place=p, src_id=p.src_id,
                                     jsonb={"type": "closeMatch", "identifier": f"wd:Q{1000 + i}"})
            if i % 3 == 0:
                PlaceLink.objects.create(id=nxt(), place=p, src_id=p.src_id, task_id=f"task-{i}",
                                         reviewer=cls.owner,
                                         jsonb={"type": "closeMatch", "identifier": f"gn:{5000 + i}"})
            if i % 2 == 0:
                PlaceWhen.objects.create(id=nxt(), place=p, src_id=p.src_id, minmax=[1500, 1600],
                                         jsonb={"timespans": [{"start": {"in": "1500"}, "end": {"in": "1600"}}],
                                                "label": "16th c."})
            if i % 4 == 0:
                PlaceRelated.objects.create(id=nxt(), place=p, src_id=p.src_id,
                                            jsonb={"relationType": "gvp:broaderPartitive",
                                                   "relationTo": "wd:Q38", "label": "Italy"})
                PlaceDescription.objects.create(id=nxt(), place=p, src_id=p.src_id,
                                                jsonb={"id": f"d{i}", "value": f"About {title}", "lang": "en"})
                PlaceDepiction.objects.create(id=nxt(), place=p, src_id=p.src_id,
                                              jsonb={"@id": f"https://example.org/img/{i}.png",
                                                     "title": "pic", "license": "CC0"})
            for g in geoms:
                PlaceGeom.objects.create(id=nxt(), place=p, src_id=p.src_id, geom=g,
                                         jsonb=json.loads(g.geojson) | {"citation": {"label": "c"}})
            return p

        for i in range(20):
            x, y = 10.0 + 0.04 * i, 45.0 + 0.04 * i
            place(cls.ds_a, i, [Point(x, y, srid=4326)])
        for i in range(6):
            x, y = 10.2 + 0.1 * i, 45.8 - 0.1 * i
            place(cls.ds_b, i, [Point(x, y, srid=4326), Polygon(_ring(x, y, 0.01), srid=4326)])
        for i in range(4):
            place(cls.ds_p, i, [Point(10.5 + 0.01 * i, 45.5, srid=4326)])

    def setUp(self):
        from django.core.cache import caches
        for alias in ("default", "throttle"):
            try:
                caches[alias].clear()
            except Exception:
                pass


# (case name, user or None, url, params, patched LEGACY_DB_COUNT_CAP or None)
CASES = [
    ("spatial_bbox_point_anon", None, SPATIAL, {**BOX, "pagesize": 40}, None),
    ("spatial_bbox_point_owner", "owner", SPATIAL, {**BOX, "pagesize": 40}, None),
    ("spatial_bbox_point_other_user", "other", SPATIAL, {**BOX, "pagesize": 40}, None),
    ("spatial_bbox_none", None, SPATIAL, {**BOX, "geometry": "none", "pagesize": 40}, None),
    ("spatial_bbox_full_anon", None, SPATIAL, {**BOX, "geometry": "full"}, None),
    ("spatial_bbox_full_owner", "owner", SPATIAL, {**BOX, "geometry": "full"}, None),
    ("spatial_bbox_full_ds_b", None, SPATIAL, {**BOX, "geometry": "full", "dataset": DS_B}, None),
    ("spatial_bbox_point_ds_b", None, SPATIAL, {**BOX, "dataset": DS_B}, None),
    ("spatial_bbox_private_ds_anon", None, SPATIAL, {**BOX, "dataset": DS_P}, None),
    ("spatial_bbox_private_ds_owner", "owner", SPATIAL, {**BOX, "dataset": DS_P, "geometry": "full"}, None),
    ("spatial_bbox_intersects_full", None, SPATIAL, {**BOX, "match": "intersects", "geometry": "full"}, None),
    ("spatial_nearby_within_point", None, SPATIAL,
     {"type": "nearby", "lon": 10.4, "lat": 45.4, "km": 40, "pagesize": 40}, None),
    ("spatial_nearby_intersects_full", None, SPATIAL,
     {"type": "nearby", "lon": 10.4, "lat": 45.4, "km": 40, "match": "intersects", "geometry": "full"}, None),
    ("spatial_count_capped", None, SPATIAL, {**BOX, "pagesize": 5}, 10),
    ("spatial_count_capped_full", "owner", SPATIAL, {**BOX, "pagesize": 5, "geometry": "full"}, 10),
    ("db_name_contains_anon", None, DB, {"name_contains": "place_", "pagesize": 40}, None),
    ("db_name_contains_owner", "owner", DB, {"name_contains": "place_", "pagesize": 40}, None),
    ("db_name", None, DB, {"name": "np1_b_place", "pagesize": 40}, None),
    ("db_dataset", None, DB, {"name_contains": "place_", "dataset": "np1_b"}, None),
    ("db_ids_with_private_anon", None, DB, {"id": "9700001,9700021,9700027"}, None),
    ("db_ids_with_private_owner", "owner", DB, {"id": "9700001,9700021,9700027"}, None),
    ("db_count_capped", None, DB, {"name_contains": "place_", "pagesize": 5}, 10),
]


class GoldenOutputTests(_Fixture):

    def _run(self, who, url, params, cap):
        self.client.logout()
        if who:
            self.client.force_login(getattr(self, who))
        if cap is None:
            return self.client.get(url, params)
        with patch("api.views.LEGACY_DB_COUNT_CAP", cap):
            return self.client.get(url, params)

    def test_responses_are_byte_identical_to_the_golden_file(self):
        got = {}
        for name, who, url, params, cap in CASES:
            r = self._run(who, url, params, cap)
            got[name] = {"status": r.status_code, "body": r.content.decode("utf-8")}

        # The cases exercise what they claim (a-check-that-cannot-fail): presence beside absence.
        def body(n):
            return json.loads(got[n]["body"])
        anon = {f["properties"]["src_id"] + f["properties"]["dataset_label"]
                for f in body("spatial_bbox_point_anon")["features"]}
        owner = {f["properties"]["src_id"] + f["properties"]["dataset_label"]
                 for f in body("spatial_bbox_point_owner")["features"]}
        self.assertEqual(len(anon), 26)
        self.assertEqual(len(owner), 30)
        self.assertFalse(any(k.endswith("np1_p") for k in anon))
        self.assertEqual(got["spatial_bbox_private_ds_anon"]["status"], 400)
        self.assertEqual(got["spatial_bbox_private_ds_owner"]["status"], 200)
        self.assertEqual(len(body("spatial_bbox_private_ds_owner")["features"]), 4)
        self.assertIs(body("spatial_count_capped")["count_capped"], True)
        self.assertIs(body("spatial_bbox_point_anon")["count_capped"], False)
        self.assertEqual(body("spatial_bbox_point_anon")["count"], 26)
        self.assertIs(body("db_count_capped")["count_capped"], True)
        full = body("spatial_bbox_full_ds_b")["features"]
        self.assertEqual(len(full), 6)
        self.assertTrue(all(f["geometry"]["type"] == "GeometryCollection" for f in full))
        self.assertTrue(any(lk["aug"] for f in full for lk in f["links"]))
        self.assertTrue(any(f["related"] and f["descriptions"] and f["depictions"] and f["whens"]
                            for f in body("spatial_bbox_full_anon")["features"]))
        self.assertIn("bbox", body("spatial_bbox_point_anon")["features"][0])
        self.assertNotIn("bbox", body("spatial_bbox_none")["features"][0])
        self.assertEqual(len(body("db_ids_with_private_anon")["features"]), 2)
        self.assertEqual(len(body("db_ids_with_private_owner")["features"]), 3)

        if os.environ.get("WHG_WRITE_GOLDEN") == "1":
            GOLDEN.parent.mkdir(exist_ok=True)
            GOLDEN.write_text(json.dumps(got, indent=1, ensure_ascii=False, sort_keys=True) + "\n",
                              encoding="utf-8")
            self.fail(f"golden file rewritten ({GOLDEN}); unset WHG_WRITE_GOLDEN and re-run")

        want = json.loads(GOLDEN.read_text(encoding="utf-8"))
        self.assertEqual(sorted(got), sorted(want))
        for name in want:
            with self.subTest(case=name):
                self.assertEqual(got[name]["status"], want[name]["status"])
                # Byte equality of the body, not equality of the parsed JSON.
                self.assertEqual(got[name]["body"], want[name]["body"])


def _shape(sql):
    """A query with its literals removed, for grouping."""
    sql = re.sub(r"'(?:[^']|'')*'", "?", sql)
    sql = re.sub(r"\b\d+\b", "N", sql)
    sql = re.sub(r"IN \((?:N|\?)(?:, (?:N|\?))*\)", "IN (...)", sql)
    return sql


class QueryCountTests(_Fixture):

    def _count(self, url, params, who=None):
        self.client.logout()
        if who:
            self.client.force_login(getattr(self, who))
        with CaptureQueriesContext(connection) as ctx:
            r = self.client.get(url, params)
        self.assertEqual(r.status_code, 200, r.content[:300])
        return len(json.loads(r.content)["features"]), ctx

    def _report(self, ctx):
        shapes = Counter(_shape(q["sql"]) for q in ctx.captured_queries)
        return "\n".join(f"{n:4d}  {s[:220]}" for s, n in shapes.most_common())

    def _assert_flat(self, url, small, large, n_small, n_large, who=None):
        got_small, ctx_small = self._count(url, small, who)
        got_large, ctx_large = self._count(url, large, who)
        self.assertEqual((got_small, got_large), (n_small, n_large))  # the pages really differ in size
        self.assertEqual(len(ctx_small), len(ctx_large),
                         f"\n{n_small} places:\n{self._report(ctx_small)}\n{n_large} places:\n"
                         f"{self._report(ctx_large)}")
        self.assertLessEqual(len(ctx_large), CEILING_SIGNED_IN if who else CEILING, self._report(ctx_large))
        return len(ctx_large)

    def test_spatial_point(self):
        self._assert_flat(SPATIAL, {**BOX, "pagesize": 5}, {**BOX, "pagesize": 25}, 5, 25)

    def test_spatial_none(self):
        self._assert_flat(SPATIAL, {**BOX, "pagesize": 5, "geometry": "none"},
                          {**BOX, "pagesize": 25, "geometry": "none"}, 5, 25)

    def test_spatial_full(self):
        self._assert_flat(SPATIAL, {**BOX, "pagesize": 5, "geometry": "full"},
                          {**BOX, "pagesize": 20, "geometry": "full"}, 5, 20)

    def test_spatial_nearby_signed_in(self):
        q = {"type": "nearby", "lon": 10.4, "lat": 45.4, "km": 80}
        self._assert_flat(SPATIAL, {**q, "pagesize": 5}, {**q, "pagesize": 25}, 5, 25, who="owner")

    def test_db(self):
        self._assert_flat(DB, {"name_contains": "place_", "pagesize": 5},
                          {"name_contains": "place_", "pagesize": 25}, 5, 25)
