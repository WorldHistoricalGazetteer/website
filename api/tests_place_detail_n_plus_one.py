"""``PlacesDetailAPIView`` (``/places/<ids>/panel/``, ``/api/place/<id>/``, ``/api/place/<a-b-c>/``)
serialises its places in a CONSTANT number of SQL queries, and its responses are byte-identical
to what they were before.

Why (2026-11-18): ``PlaceSerializer`` read eight child relations per place (names, types, geoms,
links, related, whens, descriptions, depictions), the dataset, an ``Extent`` aggregate and a
geometry count per place, and, with ``?cid=``, the place's trace annotations: ~13 queries per
place. The view now prefetches the relations once for the whole id list, takes every extent in
one grouped query, and the traces in one query.

Two pins, on the pattern of ``api.tests_legacy_n_plus_one``:

* ``GoldenOutputTests``: the response BYTES of a set of requests (single id, multi id up to 50,
  private-dataset visibility for anonymous / owner / other user, ``cid`` with trace annotations
  (an archived one and another collection's excluded), an id list containing a missing id, an
  invalid ``cid``, the 404 / 400 traps) equal ``api/testdata/place_detail_n_plus_one_golden.json``,
  captured from the code BEFORE the change. All primary keys and timestamps are fixed. The one
  exception to byte equality is the top-level ``title`` of a multi-place response: the view joins
  a Python ``set`` of titles, whose order follows the per-process string hash, so it is compared
  as the sorted set (the rest of that body is still compared byte for byte).
  Regenerate only on purpose: ``WHG_WRITE_GOLDEN=1`` (the test then fails).
* ``QueryCountTests``: 1, 5, 25 and 50 ids cost the same number of queries, with and without
  ``cid``, and that number is at most ``CEILING``.

    ENV_CONTEXT=local venv/bin/python manage.py test api.tests_place_detail_n_plus_one \\
        --settings=whg.settings_localtest

``WHG_MEASURE=1`` also prints query counts and wall time per size.
"""

import datetime
import json
import os
import re
import time
from collections import Counter
from pathlib import Path
from unittest.mock import patch

from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.gis.geos import Point, Polygon
from django.db import connection
from django.test import TestCase, override_settings
from django.test.utils import CaptureQueriesContext

GOLDEN = Path(__file__).parent / "testdata" / "place_detail_n_plus_one_golden.json"
# The view reads aliases.json from STATIC_ROOT (collected); point it at the source copy.
ALIASES_DIR = str(Path(settings.BASE_DIR) / "places" / "static")

# Measured 2026-11-18 after the change, anonymous: 1 visibility lookup (registry entries), 1
# place fetch, 9 prefetches (dataset + 8 child relations), 1 grouped extent, 1 dataset titles
# = 13, at 1, 5, 25 and 50 ids. Signed in adds the session, the user and a group check (3);
# ?cid= adds the trace query (1): 17. Before: 16 for one id and ~12 more per extra id
# anonymous (50 ids: 603); 20 and ~13 per extra id signed in with cid (50 ids: 656).
CEILING = 13
CEILING_CID_SIGNED_IN = 17

DS_A, DS_B, DS_P = 971001, 971002, 971003
USER_OWNER, USER_OTHER = 971101, 971102
COLL, COLL_OTHER = 971201, 971202
A0, B0, P0 = 9710000, 9710050, 9710056  # A: +1..+50, B: +1..+6, P: +1..+4
NOGEOM = A0 + 8  # the A place with no geometry
CREATED = datetime.datetime(2026, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)


def _ring(x, y, r):
    return [(x - r, y - r), (x + r, y - r), (x + r, y + r), (x - r, y + r), (x - r, y - r)]


def a(*ks):
    return [A0 + k for k in ks]


def ids(lst):
    return "-".join(map(str, lst))


@override_settings(STATIC_ROOT=ALIASES_DIR)
class _Fixture(TestCase):
    """60 places: 50 in public dataset A (one point each, except NOGEOM which has none), 6 in
    public dataset B (a point AND a polygon each), 4 in private dataset P. Every place has names,
    types and links; some have related, whens, descriptions, depictions, attestation years.
    Collection COLL annotates every 4th A place (plus one archived trace); COLL_OTHER annotates
    A+4 too, which a COLL request must not show."""

    @classmethod
    def setUpTestData(cls):
        from collection.models import Collection
        from datasets.models import Dataset
        from places.models import (Place, PlaceDepiction, PlaceDescription, PlaceGeom, PlaceLink,
                                   PlaceName, PlaceRelated, PlaceType, PlaceWhen)
        from traces.models import TraceAnnotation
        User = get_user_model()
        cls.owner = User.objects.create_user(id=USER_OWNER, username="pd_owner", email="pd_owner@example.org",
                                             password="pw", given_name="PD", surname="Owner")
        cls.other = User.objects.create_user(id=USER_OTHER, username="pd_other", email="pd_other@example.org",
                                             password="pw", given_name="PD", surname="Other")
        with patch("datasets.signals.doi"):
            mk = dict(owner=cls.owner, description="D", creator="A Contributor", ds_status="indexed",
                      uri_base="https://example.org/places/")

            def dataset(pk, label, title, public):
                ds = Dataset.objects.create(label=label, title=title, public=public, **mk)
                Dataset.objects.filter(pk=ds.pk).update(id=pk)
                return Dataset.objects.get(pk=pk)
            cls.ds_a = dataset(DS_A, "pd_a", "Dataset A", True)
            cls.ds_b = dataset(DS_B, "pd_b", "Dataset B", True)
            cls.ds_p = dataset(DS_P, "pd_p", "Dataset P", False)

        child = 9810000

        def nxt():
            nonlocal child
            child += 1
            return child

        ccodes = [["IT"], ["FR"], ["IT", "DE"], []]

        def place(pid, ds, i, geoms):
            title = f"{ds.label}_place_{i:02d} Ünïcode"
            p = Place.objects.create(id=pid, title=title, src_id=f"src{i}", dataset=ds, ccodes=ccodes[i % 4],
                                     minmax=None if i % 7 == 3 else [1500 + i, 1600 + 2 * i],
                                     timespans=[[1500 + i, 1600 + i]],
                                     fclasses=["P", "S"] if i % 2 else ["P"],
                                     attestation_year=1700 + i if i % 5 == 0 else None)
            PlaceName.objects.create(id=nxt(), place=p, toponym=title, jsonb={"toponym": title})
            if i % 3 == 0:
                PlaceName.objects.create(id=nxt(), place=p, toponym=f"Alt {i}",
                                         jsonb={"toponym": f"Alt {i}", "when": {"timespans": []}})
            for k in range(2):
                PlaceType.objects.create(id=nxt(), place=p, src_id=p.src_id, fclass="P", aat_id=300008347 + k,
                                         jsonb={"identifier": f"aat:{300008347 + k}", "label": f"type {k}",
                                                "sourceLabel": f"src type {k}"})
            if i % 5 == 1:
                PlaceType.objects.create(id=nxt(), place=p, src_id=p.src_id, fclass="P", aat_id=300008389,
                                         jsonb={"identifier": "wd:Q515", "label": "city"})
            PlaceLink.objects.create(id=nxt(), place=p, src_id=p.src_id,
                                     jsonb={"type": "closeMatch", "identifier": f"wd:Q{1000 + i}"})
            if i % 3 == 0:
                PlaceLink.objects.create(id=nxt(), place=p, src_id=p.src_id, task_id=f"task-{i}",
                                         reviewer=cls.owner,
                                         jsonb={"type": "closeMatch", "identifier": f"gn:{5000 + i}"})
            if i % 6 == 1:
                PlaceLink.objects.create(id=nxt(), place=p, src_id=p.src_id,
                                         jsonb={"type": "seeAlso", "identifier": "https://example.org/x"})
            if i % 2 == 0:
                PlaceWhen.objects.create(id=nxt(), place=p, src_id=p.src_id, minmax=[1500, 1600],
                                         jsonb={"timespans": [{"start": {"in": "1500"}, "end": {"in": "1600"}}],
                                                "label": "16th c."})
            if i % 4 == 0:
                PlaceRelated.objects.create(id=nxt(), place=p, src_id=p.src_id,
                                            jsonb={"relationType": "gvp:broaderPartitive",
                                                   "relationTo": "wd:Q38", "label": "Italy"})
                PlaceDescription.objects.create(id=nxt(), place=p, src_id=p.src_id,
                                                jsonb={"id": f"wp:Place_{i}", "value": f"About {title}",
                                                       "lang": "en"})
                PlaceDepiction.objects.create(id=nxt(), place=p, src_id=p.src_id,
                                              jsonb={"@id": f"https://example.org/img/{i}.png",
                                                     "title": "pic", "license": "CC0"})
            for g in geoms:
                PlaceGeom.objects.create(id=nxt(), place=p, src_id=p.src_id, geom=g,
                                         jsonb=json.loads(g.geojson) | {"citation": {"label": "c"}})
            return p

        for i in range(50):
            x, y = 10.0 + 0.013 * i, 45.0 + 0.011 * i
            place(A0 + 1 + i, cls.ds_a, i, [] if A0 + 1 + i == NOGEOM else [Point(x, y, srid=4326)])
        for i in range(6):
            x, y = 10.2 + 0.1 * i, 45.8 - 0.1 * i
            place(B0 + 1 + i, cls.ds_b, i, [Point(x, y, srid=4326), Polygon(_ring(x, y, 0.01), srid=4326)])
        for i in range(4):
            place(P0 + 1 + i, cls.ds_p, i, [Point(12.5 + 0.01 * i, 41.9, srid=4326)])

        with patch("collection.signals.doi"):
            for pk, title in ((COLL, "Trace coll"), (COLL_OTHER, "Other coll")):
                Collection.objects.create(id=pk, owner=cls.owner, title=title, collection_class="place",
                                          description="D")
        tid = 9910000
        for k in range(1, 51, 4):  # A+1, A+5, ..., A+49
            tid += 1
            TraceAnnotation.objects.create(id=tid, collection_id=COLL, place_id=A0 + k, owner=cls.owner,
                                           note=f"note {k}", relation=["waypoint"], start="1500",
                                           end="1600", sequence=k, saved=True)
        tid += 1
        TraceAnnotation.objects.create(id=tid, collection_id=COLL, place_id=A0 + 2, owner=cls.owner,
                                       note="archived", archived=True)
        tid += 1
        TraceAnnotation.objects.create(id=tid, collection_id=COLL_OTHER, place_id=A0 + 5, owner=cls.owner,
                                       note="other collection")
        TraceAnnotation.objects.update(created=CREATED)

    def setUp(self):
        from django.core.cache import caches
        for alias in ("default", "throttle"):
            try:
                caches[alias].clear()
            except Exception:
                pass


def panel(lst, cid=None):
    return f"/places/{ids(lst)}/panel/" + (f"?cid={cid}" if cid is not None else "")


# (case name, user or None, url)
CASES = [
    ("single_a_anon", None, panel(a(1))),
    ("single_a_owner", "owner", panel(a(1))),
    ("single_b_polygon", None, panel([B0 + 3])),
    ("single_nogeom", None, panel([NOGEOM])),
    ("single_api_route", None, f"/api/place/{A0 + 4}/"),
    ("multi_api_route", None, f"/api/place/{ids(a(1, 2, 3))}/"),
    ("multi_5", None, panel(a(1, 2, 3, 4, 5))),
    ("multi_25", None, panel(a(*range(1, 26)))),
    ("multi_50", None, panel(a(*range(1, 51)))),
    ("multi_a_and_b", None, panel(a(4, 8, 12) + [B0 + 1, B0 + 6])),
    ("private_mix_anon", None, panel(a(1) + [P0 + 1, P0 + 2] + [B0 + 2])),
    ("private_mix_owner", "owner", panel(a(1) + [P0 + 1, P0 + 2] + [B0 + 2])),
    ("private_mix_other", "other", panel(a(1) + [P0 + 1, P0 + 2] + [B0 + 2])),
    ("private_only_anon", None, panel([P0 + 3])),
    ("private_only_owner", "owner", panel([P0 + 3])),
    ("cid_single_traced", "owner", panel(a(5), COLL)),
    ("cid_single_untraced", "owner", panel(a(3), COLL)),
    ("cid_multi", "owner", panel(a(1, 2, 3, 4, 5, 6, 9, 13), COLL)),
    ("cid_50_anon", None, panel(a(*range(1, 51)), COLL)),
    ("cid_other_collection", "owner", panel(a(1, 5), COLL_OTHER)),
    ("cid_invalid", None, panel(a(1)) + "?cid=abc"),
    ("missing_id_in_list", None, panel(a(1, 2) + [99999999])),
    ("missing_only", None, panel([99999999])),
    ("too_many", None, panel(list(range(A0 + 1, A0 + 52)))),
]


def _canonical_title(body):
    """Byte body with the top-level multi-place ``title`` (a set-ordered join) put in sorted order.
    Returns the body unchanged when it has no top-level title."""
    try:
        doc = json.loads(body)
    except ValueError:
        return body
    if not isinstance(doc, dict) or "title" not in doc or "id" not in doc:
        return body
    # The top-level key is the one followed by "names" (each geometry also carries a title).
    raw = '"title": ' + json.dumps(doc["title"]) + ', "names": '
    assert body.count(raw) == 1, (raw, body[:300])
    return body.replace(raw, '"title": ' + json.dumps("|".join(sorted(doc["title"].split("|")))) + ', "names": ')


class GoldenOutputTests(_Fixture):

    def _get(self, who, url):
        self.client.logout()
        if who:
            self.client.force_login(getattr(self, who))
        return self.client.get(url)

    def test_responses_are_byte_identical_to_the_golden_file(self):
        got = {}
        for name, who, url in CASES:
            r = self._get(who, url)
            got[name] = {"status": r.status_code, "body": _canonical_title(r.content.decode("utf-8"))}

        # The cases exercise what they claim (a-check-that-cannot-fail): presence beside absence.
        def body(n):
            return json.loads(got[n]["body"])
        self.assertEqual(got["multi_50"]["status"], 200)
        self.assertEqual(len(body("multi_50")["names"]), 50 + 17)  # 50 titles + 17 "Alt" names
        self.assertEqual(len(body("multi_50")["geoms"]), 49)  # NOGEOM has none
        self.assertEqual(len(body("multi_50")["title"].split("|")), 50)
        self.assertIsNone(body("single_nogeom")["extent"])
        self.assertIsNotNone(body("single_a_anon")["extent"])
        ext = body("multi_a_and_b")["extent"]
        self.assertTrue(ext[0] < 10.2 - 0.009 and ext[2] > 10.7 + 0.009, ext)  # the B polygons widen it
        self.assertEqual([d["title"] for d in body("multi_a_and_b")["datasets"]], ["Dataset A", "Dataset B"])
        self.assertEqual(body("multi_50")["minmax"], [1500, 1698])
        self.assertEqual(body("multi_50")["attestation_years"], [1700 + i for i in range(0, 50, 5)])
        self.assertIn({"ccode": "IT", "label": "Italy"}, body("multi_5")["countries"])
        self.assertIn({"ccode": "DE", "label": "Germany"}, body("multi_5")["countries"])
        self.assertTrue(any(t.get("url") == "https://www.wikidata.org/wiki/Q515" for t in body("multi_5")["types"]))
        self.assertTrue(any(lk.get("url") == "http://www.geonames.org/5000" for lk in body("multi_5")["links"]))
        self.assertTrue(any(lk["aug"] for lk in body("multi_5")["links"]))
        self.assertTrue(any(d.get("url") == "https://en.wikipedia.org/wiki/Place_0"
                            for d in body("multi_5")["descriptions"]))
        self.assertTrue(body("multi_5")["related"] and body("multi_5")["depictions"])
        self.assertEqual(body("multi_5")["geoms"][0]["ds"], DS_A)
        # private visibility
        self.assertEqual(body("private_mix_anon")["id"], ids(a(1) + [P0 + 1, P0 + 2] + [B0 + 2]))
        self.assertEqual(len(body("private_mix_anon")["geoms"]), 1 + 2)
        self.assertEqual(len(body("private_mix_owner")["geoms"]), 1 + 2 + 2)
        self.assertEqual(len(body("private_mix_other")["geoms"]), 1 + 2)
        self.assertEqual(got["private_only_anon"]["status"], 404)
        self.assertEqual(got["private_only_owner"]["status"], 200)
        # traces
        self.assertEqual([t["fields"]["note"] for t in body("cid_single_traced")["traces"]], ["note 5"])
        self.assertEqual(body("cid_single_untraced")["traces"], [])
        self.assertEqual([t["fields"]["place"] for t in body("cid_multi")["traces"]], a(1, 5, 9, 13))
        self.assertEqual(len(body("cid_50_anon")["traces"]), 13)
        self.assertEqual([t["fields"]["note"] for t in body("cid_other_collection")["traces"]],
                         ["other collection"])
        self.assertEqual(body("single_a_anon")["traces"], [])
        self.assertEqual(got["cid_invalid"]["status"], 500)
        # missing ids and traps
        self.assertEqual(got["missing_id_in_list"]["status"], 200)
        self.assertEqual(len(body("missing_id_in_list")["geoms"]), 2)
        self.assertEqual(got["missing_only"]["status"], 404)
        self.assertEqual(got["too_many"]["status"], 400)

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
    sql = re.sub(r"'(?:[^']|'')*'", "?", sql)
    sql = re.sub(r"\b\d+\b", "N", sql)
    sql = re.sub(r"IN \((?:N|\?)(?:, (?:N|\?))*\)", "IN (...)", sql)
    return sql


SIZES = (1, 5, 25, 50)


class QueryCountTests(_Fixture):

    def _count(self, url, who=None):
        self.client.logout()
        if who:
            self.client.force_login(getattr(self, who))
        with CaptureQueriesContext(connection) as ctx:
            r = self.client.get(url)
        self.assertEqual(r.status_code, 200, r.content[:300])
        return json.loads(r.content), ctx

    def _report(self, ctx):
        shapes = Counter(_shape(q["sql"]) for q in ctx.captured_queries)
        return "\n".join(f"{n:4d}  {s[:200]}" for s, n in shapes.most_common())

    def _assert_flat(self, cid, who, ceiling):
        counts, reports = {}, {}
        for n in SIZES:
            body, ctx = self._count(panel(a(*range(1, n + 1)), cid), who)
            self.assertEqual(len(body["names"]) - sum(1 for k in range(n) if k % 3 == 0), n)  # n places
            if cid is not None:
                self.assertEqual(len(body["traces"]), len(range(1, n + 1, 4)))
            counts[n], reports[n] = len(ctx), self._report(ctx)
            if os.environ.get("WHG_MEASURE") == "1":
                reps = 5
                t0 = time.perf_counter()
                for _ in range(reps):
                    self.client.get(panel(a(*range(1, n + 1)), cid))
                ms = (time.perf_counter() - t0) / reps * 1000
                print(f"\nMEASURE cid={cid} who={who} n={n}: {counts[n]} queries, {ms:.1f} ms/request")
        detail = "\n".join(f"--- {n} places: {counts[n]}\n{reports[n]}" for n in SIZES)
        self.assertEqual(len(set(counts.values())), 1, f"{counts}\n{detail}")
        self.assertLessEqual(counts[SIZES[-1]], ceiling, detail)

    def test_flat_anonymous(self):
        self._assert_flat(None, None, CEILING)

    def test_flat_with_cid_signed_in(self):
        self._assert_flat(COLL, "owner", CEILING_CID_SIGNED_IN)
