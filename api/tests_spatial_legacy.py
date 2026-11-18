"""api.spatial_legacy.to_reconcile_query: the old /api/spatial/ parameters mapped to
the POST /reconcile body that replaces them at the v4 cutover. Pure function, no DB.

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test api.tests_spatial_legacy --settings=whg.settings_localtest
"""

from django.test import SimpleTestCase

from api.spatial import SpatialParamError
from api.spatial_legacy import DATASET_NOTE, NO_EQUIVALENT, to_reconcile_query


class LegacyMappingTests(SimpleTestCase):

    def q(self, **params):
        out = to_reconcile_query(params)
        self.assertEqual((out["url"], out["method"]), ("/reconcile", "POST"))
        return out, out["body"]["queries"]["q0"]

    def test_nearby(self):
        out, q = self.q(type="nearby", lon="-1.08", lat="53.96", km="10")
        self.assertEqual(q, {"lat": 53.96, "lng": -1.08, "radius": 10.0, "relation": "within",
                             "containment": "fuzzy", "limit": 20})
        self.assertNotIn("query", q)  # spatial-only
        self.assertTrue(out["notes"])  # the H3-disc approximation is stated

    def test_nearby_intersects(self):
        _, q = self.q(type="nearby", lon="0", lat="0", km="5", match="intersects")
        self.assertEqual(q["relation"], "intersects")

    def test_bbox_becomes_bounds_polygon(self):
        _, q = self.q(type="bbox", sw="-2,50", ne="0,52")
        self.assertEqual(q["bounds"], {"type": "Polygon",
                                       "coordinates": [[[-2.0, 50.0], [0.0, 50.0], [0.0, 52.0], [-2.0, 52.0],
                                                        [-2.0, 50.0]]]})
        self.assertEqual((q["relation"], q["containment"]), ("within", "exact"))
        self.assertNotIn("lat", q)

    def test_filters_and_limit(self):
        out, q = self.q(type="bbox", sw="-2,50", ne="0,52", fc="p,a,P", dataset="42", pagesize="5000")
        self.assertEqual(q["fclasses"], ["A", "P"])
        self.assertEqual(q["dataset"], 42)
        # /reconcile does not filter on `dataset`: narrow to WHG's places and say so.
        self.assertEqual(q["namespaces"], ["whg"])
        self.assertIn(DATASET_NOTE, out["notes"])
        self.assertEqual(q["limit"], 1000)
        self.assertEqual(out["dropped"], {})
        self.assertEqual(self.q(type="bbox", sw="-2,50", ne="0,52", pagesize="7")[1]["limit"], 7)

    def test_what_has_no_equivalent_is_said(self):
        out, q = self.q(type="nearby", lon="0", lat="0", km="5", collection="9", geometry="full")
        self.assertEqual(set(out["dropped"]), {"collection", "geometry"})
        self.assertNotIn("collection", q)
        self.assertTrue({"collection", "population", "format", "geometry", "count", "auth"}
                        <= set(out["no_equivalent"]))
        self.assertIs(out["no_equivalent"], NO_EQUIVALENT)
        # geometry=point maps to the candidates' repr_point: nothing dropped.
        self.assertEqual(self.q(type="nearby", lon="0", lat="0", km="5")[0]["dropped"], {})

    def test_query_string_lists_take_the_last_value(self):
        _, q = self.q(type=["nearby"], lon=["1"], lat=["2"], km=["3", "4"])
        self.assertEqual(q["radius"], 4.0)

    def test_refusals(self):
        for bad in ({"type": "circle"}, {"type": "nearby", "lon": "0", "lat": "0"},
                    {"type": "nearby", "lon": "200", "lat": "0", "km": "1"},
                    {"type": "nearby", "lon": "x", "lat": "0", "km": "1"},
                    {"type": "bbox", "sw": "-2,50"}, {"type": "bbox", "sw": "-2,50", "ne": "0,95"},
                    {"type": "bbox", "sw": "-2,50", "ne": "0,52", "match": "overlaps"},
                    {"type": "bbox", "sw": "-2,50", "ne": "0,52", "geometry": "polygon"},
                    {"type": "bbox", "sw": "-2,50", "ne": "0,52", "dataset": "abc"}):
            with self.assertRaises(SpatialParamError, msg=bad):
                to_reconcile_query(bad)
