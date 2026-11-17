"""/api/spatial/: lightweight geometry by default, full geometry on request, and a
contained-in (``match=within``) nearby circle by default.

Measured on prod 2026-11-17: ``type=nearby&lon=-1.08&lat=53.96&km=10&pagesize=20``
took 20-30 s and returned ~108 MB, 99% geometry ('(British Empire)' alone is a
71 MB GeometryCollection of 384,503 vertices). This module pins:

* ``geometry=point`` (default): each Feature's ``geometry`` is ONE Point that lies
  on the place's geometry (ST_PointOnSurface, not the centroid), the Feature
  carries an RFC 7946 ``bbox`` member covering ALL of its geometries, and
  ``properties.geometry_mode`` / ``geometry_url`` say it was reduced;
* ``geometry=none``: ``geometry`` is null and there is no ``bbox``;
* ``geometry=full``: the features are exactly what the serializer produced
  before this change (compared against ``LPFSerializer`` directly);
* bad values of ``geometry`` / ``match`` and over-cap ``km``, bbox and
  ``pagesize`` are refused (400) or clamped, as documented;
* ``match=within`` (default for nearby): a place matches only if its geometry
  lies entirely inside the circle; ``match=intersects`` is the old
  within-distance behaviour, unchanged.

Discipline (a-check-that-cannot-fail): every absence is asserted beside a
presence in the same response (the big polygon is ABSENT under ``within`` only
where the in-circle point is PRESENT; the full response is asserted LARGE where
the default one is asserted small).

Run locally only (never in the prod container):

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test api.tests_spatial_lightweight --settings=whg.settings_localtest
"""

import json
import math
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.gis.geos import GEOSGeometry, Point, Polygon
from django.test import TestCase

URL = "/api/spatial/"
# York, the prod measurement's centre.
LON, LAT = -1.08, 53.96


def _ring(cx, cy, r_deg, n):
    """A closed ring of n vertices (a many-vertex 'circle' in degrees)."""
    pts = [(cx + r_deg * math.cos(2 * math.pi * i / n), cy + r_deg * math.sin(2 * math.pi * i / n))
           for i in range(n)]
    return pts + [pts[0]]


def _geojson(geom):
    return json.loads(geom.geojson)


class SpatialLightweightTests(TestCase):

    @classmethod
    def setUpTestData(cls):
        from datasets.models import Dataset
        from places.models import Place, PlaceGeom, PlaceName
        User = get_user_model()
        cls.owner = User.objects.create_user(username="sp_owner", email="sp_owner@example.org", password="pw",
                                             given_name="Sp", surname="Owner")
        with patch("datasets.signals.doi"):
            cls.ds = Dataset.objects.create(
                owner=cls.owner, label="sp_light", title="Spatial lightweight", description="D",
                creator="A Contributor", public=True, ds_status="indexed",
                uri_base="https://example.org/places/")

        def place(title, *geoms):
            p = Place.objects.create(title=title, src_id=title, dataset=cls.ds, ccodes=["GB"],
                                     minmax=[1500, 1600], timespans=[[1500, 1600]], fclasses=["P"])
            PlaceName.objects.create(place=p, toponym=title, jsonb={"toponym": title})
            for g in geoms:
                PlaceGeom.objects.create(place=p, geom=g, jsonb=_geojson(g))
            return p

        # A point 3 km east of the centre: inside a 10 km circle.
        cls.near = place("Nearpoint", Point(LON + 0.045, LAT, srid=4326))
        # A point ~25 km north: outside a 10 km circle, inside a 50 km one.
        cls.far = place("Farpoint", Point(LON, LAT + 0.225, srid=4326))
        # An empire: a 20,000-vertex polygon of radius ~3 degrees CONTAINING the
        # centre and reaching far beyond any 10 km circle.
        cls.empire = place("Empire", Polygon(_ring(LON, LAT, 3.0, 20000), srid=4326))
        # A C-shaped (concave) polygon 2 km wide whose centroid lies in its notch,
        # i.e. OUTSIDE the polygon, plus a separate outlying point: the bbox must
        # cover both, and the representative point must lie ON the polygon.
        x0, y0 = LON - 0.03, LAT - 0.02
        c_shape = Polygon([(x0, y0), (x0 + 0.03, y0), (x0 + 0.03, y0 + 0.004), (x0 + 0.004, y0 + 0.004),
                           (x0 + 0.004, y0 + 0.026), (x0 + 0.03, y0 + 0.026), (x0 + 0.03, y0 + 0.03),
                           (x0, y0 + 0.03), (x0, y0)], srid=4326)
        cls.c_shape_geom = c_shape
        cls.outlier = Point(LON + 0.02, LAT + 0.02, srid=4326)
        cls.cplace = place("Cshape", c_shape, cls.outlier)

    # -- helpers ---------------------------------------------------------------

    def get(self, **params):
        base = {"type": "nearby", "lon": LON, "lat": LAT, "km": 10}
        base.update(params)
        return self.client.get(URL, base)

    def features(self, r):
        self.assertEqual(r.status_code, 200, r.content[:500])
        return {f["properties"]["title"]: f for f in r.json()["features"]}

    # -- geometry=point (default) ---------------------------------------------

    def test_default_is_point_with_bbox_and_mode(self):
        r = self.get()
        feats = self.features(r)
        self.assertIn("Nearpoint", feats)
        self.assertIn("Cshape", feats)
        body = r.json()
        self.assertEqual(body["geometry_mode"], "point")
        for title, f in feats.items():
            self.assertEqual(f["type"], "Feature")
            self.assertEqual(f["geometry"]["type"], "Point", title)
            self.assertEqual(len(f["geometry"]["coordinates"]), 2, title)
            self.assertEqual(len(f["bbox"]), 4, title)
            props = f["properties"]
            self.assertEqual(props["geometry_mode"], "point")
            self.assertEqual(props["geometry_url"], f"https://whgazetteer.org/api/place/{props['place_id']}/")
        self.assertEqual(feats["Cshape"]["properties"]["geometry_count"], 2)
        self.assertEqual(sorted(feats["Cshape"]["properties"]["geometry_types"]), ["POINT", "POLYGON"])

    def test_explicit_point_equals_default(self):
        self.assertEqual(self.features(self.get()), self.features(self.get(geometry="point")))

    def test_bbox_member_covers_all_geometries(self):
        f = self.features(self.get())["Cshape"]
        w, s, e, n = f["bbox"]
        xs = [x for x, _ in self.c_shape_geom.coords[0]] + [self.outlier.x]
        ys = [y for _, y in self.c_shape_geom.coords[0]] + [self.outlier.y]
        # Rounded outward to 6 decimals, never inward.
        for got, want in ((w, min(xs)), (s, min(ys))):
            self.assertLessEqual(got, want + 1e-9)  # float noise in the fixture
            self.assertAlmostEqual(got, want, delta=2e-6)
        for got, want in ((e, max(xs)), (n, max(ys))):
            self.assertGreaterEqual(got, want - 1e-9)
            self.assertAlmostEqual(got, want, delta=2e-6)

    def test_point_lies_on_the_geometry_not_the_centroid(self):
        f = self.features(self.get())["Cshape"]
        pt = Point(*f["geometry"]["coordinates"], srid=4326)
        # The positive control: the centroid of this C-shape is OUTSIDE it, so a
        # centroid implementation fails this test.
        self.assertFalse(self.c_shape_geom.covers(self.c_shape_geom.centroid))
        self.assertTrue(self.c_shape_geom.covers(pt), f["geometry"])
        e = self.features(self.get(match="intersects"))["Empire"]
        empire = self.empire.geoms.get().geom
        self.assertTrue(empire.covers(Point(*e["geometry"]["coordinates"], srid=4326)))

    def test_default_response_is_small_where_full_is_large(self):
        light = self.get(match="intersects")
        full = self.get(match="intersects", geometry="full")
        self.assertIn("Empire", self.features(light))
        self.assertIn("Empire", self.features(full))
        # 20,000 vertices: the full response is hundreds of KB (positive control) ...
        self.assertGreater(len(full.content), 500_000)
        # ... the default one is a few KB for the same four places.
        self.assertLess(len(light.content), 10_000)

    def test_compact_json(self):
        r = self.get()
        self.assertEqual(r.status_code, 200)
        self.assertNotIn(b"\n  ", r.content)
        self.assertIn(b'"Nearpoint"', r.content)  # the control: the body is the real answer

    # -- geometry=none ---------------------------------------------------------

    def test_none_has_no_geometry_and_no_bbox(self):
        feats = self.features(self.get(geometry="none"))
        self.assertIn("Nearpoint", feats)
        for f in feats.values():
            self.assertIsNone(f["geometry"])
            self.assertNotIn("bbox", f)
            self.assertEqual(f["properties"]["geometry_mode"], "none")

    # -- geometry=full ---------------------------------------------------------

    def test_full_features_identical_to_the_old_serializer(self):
        from api.serializers import LPFSerializer
        from places.models import Place
        r = self.get(geometry="full", match="intersects")
        feats = self.features(r)
        self.assertEqual(set(feats), {"Nearpoint", "Empire", "Cshape"})
        self.assertEqual(r.json()["geometry_mode"], "full")
        old = LPFSerializer(Place.objects.filter(id__in=[self.near.id, self.empire.id, self.cplace.id]),
                            many=True).data
        old = {f["properties"]["title"]: json.loads(json.dumps(f)) for f in old}
        self.assertEqual(feats, old)
        self.assertNotIn("bbox", feats["Empire"])
        self.assertEqual(feats["Empire"]["geometry"]["type"], "Polygon")

    def test_full_pagesize_capped_at_20(self):
        r = self.get(geometry="full", pagesize=500)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["pagesize_max"], 20)
        r = self.get(pagesize=500)
        self.assertEqual(r.json()["pagesize_max"], 200)

    # -- validation ------------------------------------------------------------

    def test_invalid_geometry_value(self):
        r = self.get(geometry="polygon")
        self.assertEqual(r.status_code, 400)
        self.assertIn("geometry", r.json()["error"])
        self.assertEqual(self.get(geometry="point").status_code, 200)  # the control

    def test_invalid_match_value(self):
        r = self.get(match="overlaps")
        self.assertEqual(r.status_code, 400)
        self.assertIn("match", r.json()["error"])

    def test_km_cap(self):
        self.assertEqual(self.get(km=501).status_code, 400)
        self.assertEqual(self.get(km=0).status_code, 400)
        self.assertEqual(self.get(km=500).status_code, 200)

    def test_bbox_span_cap(self):
        self.assertEqual(self.client.get(URL, {"type": "bbox", "sw": "-20,40", "ne": "11,55"}).status_code, 400)
        self.assertEqual(self.client.get(URL, {"type": "bbox", "sw": "-19,40", "ne": "11,55"}).status_code, 200)

    # -- match=within (default) vs match=intersects -----------------------------

    def test_within_is_the_default_and_excludes_the_containing_polygon(self):
        feats = self.features(self.get())
        self.assertEqual(self.get().json()["match"], "within")
        self.assertIn("Nearpoint", feats)          # inside the circle
        self.assertNotIn("Farpoint", feats)        # outside the circle
        self.assertNotIn("Empire", feats)          # contains the centre, extends beyond
        self.assertIn("Cshape", feats)             # entirely inside (both geometries)

    def test_within_larger_circle_takes_the_far_point(self):
        feats = self.features(self.get(km=50))
        self.assertIn("Farpoint", feats)
        self.assertNotIn("Empire", feats)

    def test_intersects_includes_the_containing_polygon(self):
        feats = self.features(self.get(match="intersects"))
        self.assertIn("Nearpoint", feats)
        self.assertIn("Empire", feats)
        self.assertNotIn("Farpoint", feats)

    def test_intersects_is_the_old_distance_query(self):
        """match=intersects selects exactly what geom__distance_lte selected."""
        from django.contrib.gis.measure import D
        from places.models import PlaceGeom
        for km in (1, 10, 30, 400):
            old = set(PlaceGeom.objects.filter(
                geom__distance_lte=(Point(LON, LAT, srid=4326), D(km=km))).values_list("place_id", flat=True))
            got = {f["properties"]["place_id"] for f in self.features(self.get(match="intersects", km=km)).values()}
            self.assertEqual(got, old, km)
        self.assertTrue(old)

    def test_place_with_one_geometry_outside_is_excluded_under_within(self):
        # Cshape's outlying point is ~2.4 km from the centre; at 2 km the place
        # is only partly inside, so 'contained-in' excludes it.
        feats = self.features(self.get(km=2))
        self.assertNotIn("Cshape", feats)
        self.assertIn("Cshape", self.features(self.get(km=2, match="intersects")))

    def test_bbox_is_contained_in_already(self):
        r = self.client.get(URL, {"type": "bbox", "sw": "-1.5,53.5", "ne": "-0.5,54.5"})
        feats = self.features(r)
        self.assertIn("Nearpoint", feats)
        self.assertNotIn("Empire", feats)
        self.assertEqual(r.json()["match"], "within")
        feats = self.features(self.client.get(URL, {"type": "bbox", "sw": "-1.5,53.5", "ne": "-0.5,54.5",
                                                    "match": "intersects"}))
        self.assertIn("Empire", feats)

    def test_antimeridian_circle(self):
        from places.models import Place, PlaceGeom, PlaceName
        p = Place.objects.create(title="Dateline", src_id="dl", dataset=self.ds, ccodes=[], fclasses=["P"])
        PlaceGeom.objects.create(place=p, geom=Point(-179.95, 0, srid=4326),
                                 jsonb={"type": "Point", "coordinates": [-179.95, 0]})
        feats = self.features(self.client.get(URL, {"type": "nearby", "lon": 179.95, "lat": 0, "km": 20}))
        self.assertIn("Dateline", feats)
        feats = self.features(self.client.get(URL, {"type": "nearby", "lon": 179.95, "lat": 0, "km": 5}))
        self.assertNotIn("Dateline", feats)
        # 11.1 km away across the antimeridian: inside 15 km.
        self.assertIn("Dateline", self.features(self.client.get(
            URL, {"type": "nearby", "lon": 179.95, "lat": 0, "km": 15})))


class CircleTests(TestCase):
    def test_circle_vertices_lie_on_the_sphere_at_radius(self):
        from api.spatial import circle, EARTH_RADIUS_KM
        poly = circle(LON, LAT, 10)
        ring = poly.coords[0]
        self.assertGreaterEqual(len(ring), 129)
        lam1, phi1 = math.radians(LON), math.radians(LAT)
        for x, y in ring:
            lam2, phi2 = math.radians(x), math.radians(y)
            d = 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(
                math.sin((phi2 - phi1) / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin((lam2 - lam1) / 2) ** 2))
            self.assertAlmostEqual(d, 10, delta=1e-6)

    def test_polar_circle_refused(self):
        from api.spatial import circle, SpatialParamError
        with self.assertRaises(SpatialParamError):
            circle(0, 89.99, 10)


class SpatialSchemaTests(TestCase):
    def test_swagger_documents_the_parameters(self):
        r = self.client.get("/api/schema/", {"format": "json"})
        self.assertEqual(r.status_code, 200)
        op = r.json()["paths"]["/api/spatial/"]["get"]
        names = {p["name"] for p in op["parameters"]}
        self.assertTrue({"type", "lon", "lat", "km", "sw", "ne", "geometry", "match", "pagesize"} <= names, names)
        geom = next(p for p in op["parameters"] if p["name"] == "geometry")
        self.assertEqual(sorted(geom["schema"]["enum"]), ["full", "none", "point"])
        self.assertEqual(geom["schema"]["default"], "point")


class SpatialDeprecationTests(TestCase):
    """SG 2026-11-17: /api/spatial/ is deprecated (retired at the v4 switch)."""

    def test_headers_and_body_on_success_and_on_error(self):
        ok = self.client.get(URL, {"type": "bbox", "sw": "-2,50", "ne": "0,52"})
        bad = self.client.get(URL, {"type": "circle"})
        bad_param = self.client.get(URL, {"type": "bbox", "sw": "-2,50", "ne": "0,52", "geometry": "x"})
        bad_value = self.client.get(URL, {"type": "nearby", "lon": "x", "lat": "0", "km": "5"})
        self.assertEqual((ok.status_code, bad.status_code, bad_param.status_code, bad_value.status_code),
                         (200, 400, 400, 400))
        for r in (ok, bad, bad_param, bad_value):
            self.assertEqual(r["Deprecation"], "@1794873600")
            self.assertIn('<https://whgazetteer.org/reconcile>; rel="successor-version"', r["Link"])
            self.assertNotIn("Sunset", r)  # no date is known; none is invented
            body = r.json()
            self.assertEqual(next(iter(body)), "deprecation")  # first key at the root
            dep = body["deprecation"]
            self.assertIs(dep["deprecated"], True)
            self.assertIs(dep["token_required"], True)
            self.assertIsNone(dep["sunset"])
            self.assertIn("/reconcile", dep["replacement"])
            self.assertIn("API token", dep["message"])
            self.assertIn("#api-tokens", dep["message"])
        self.assertIn("error", bad.json())
        self.assertIn("features", ok.json())

    def test_each_call_is_logged(self):
        with self.assertLogs("api.deprecation", level="INFO") as logs:
            self.client.get(URL, {"type": "nearby", "lon": "0", "lat": "0", "km": "5"},
                            HTTP_USER_AGENT="probe-agent/1.0")
        line = next(m for m in logs.output if "legacy api call" in m)
        self.assertIn("endpoint=spatial", line)
        self.assertIn("origin=external", line)
        self.assertIn("probe-agent/1.0", line)

    def test_swagger_marks_it_deprecated(self):
        op = self.client.get("/api/schema/", {"format": "json"}).json()["paths"]["/api/spatial/"]["get"]
        self.assertIs(op.get("deprecated"), True)
        self.assertIn("/reconcile", op["description"])

    def test_notice_on_the_404_path(self):
        # The view's only 404 is its Place.DoesNotExist handler, which no real query
        # reaches; force it so the notice is proven on that path too.
        from places.models import Place
        with patch("api.views.spatial_api.geometry_summaries", side_effect=Place.DoesNotExist):
            r = self.client.get(URL, {"type": "bbox", "sw": "-2,50", "ne": "0,52"})
        self.assertEqual(r.status_code, 404)
        self.assertEqual(next(iter(r.json())), "deprecation")
        self.assertIs(r.json()["deprecation"]["token_required"], True)
        self.assertEqual(r["Deprecation"], "@1794873600")
