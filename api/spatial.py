"""Helpers for /api/spatial/ (SpatialAPIView): parameter limits, the contained-in
circle for ``type=nearby&match=within``, and the lightweight per-place geometry
summary for ``geometry=point``.

Why (measured on prod 2026-11-17): a 10 km nearby query round York took 20-30 s and
returned ~108 MB for 20 places, 99% of it geometry: Cliopatria polity polygons of up
to 384,503 vertices, serialised twice per geometry (``coordinates`` and the ``geom``
passthrough) and pretty-printed. The old nearby filter, ``geom__distance_lte`` on a
4326 geometry column, compiles to ``ST_DistanceSphere(geom, pt) <= m``, which no
index can serve: a sequential scan of ``place_geom`` (EXPLAIN in the commit message).
``geom__within`` a polygon is served by the GiST index (``geom @ polygon``).
"""

import math

from django.contrib.gis.db.models import Collect, Extent
from django.contrib.gis.db.models.functions import NumPoints, PointOnSurface
from django.contrib.gis.geos import MultiPolygon, Polygon
from django.contrib.postgres.aggregates import ArrayAgg
from django.db.models import CharField, Count, F, Func, Sum

GEOMETRY_MODES = ("point", "none", "full")
MATCH_MODES = ("within", "intersects")

MAX_KM = 500.0              # nearby radius
MAX_BBOX_SPAN_DEG = 30.0    # bbox width and height, each
MAX_PAGESIZE = 200          # geometry=point / none
MAX_PAGESIZE_FULL = 20      # geometry=full
DEFAULT_PAGESIZE = 20

# Mean Earth radius (IUGG), the sphere the circle is built on.
EARTH_RADIUS_KM = 6371.0088
# 128 vertices: the inscribed polygon falls short of the true circle by at most
# r * (1 - cos(pi/128)) = 0.03% of the radius (3 m at 10 km). Being inscribed, it
# never admits a geometry that is outside the circle.
CIRCLE_VERTICES = 128


class SpatialParamError(ValueError):
    """A parameter the endpoint refuses (400)."""


def circle(lon, lat, km, n=CIRCLE_VERTICES):
    """The circle of radius ``km`` round (lon, lat) on the sphere, as a 4326 polygon
    whose vertices lie exactly at that great-circle distance.

    A circle crossing the antimeridian is returned as a MultiPolygon of the circle
    and its copy shifted by 360 degrees, so a geometry on either side can lie within
    it. A circle that contains a pole cannot be drawn as a lon/lat polygon and is
    refused.
    """
    d = km / EARTH_RADIUS_KM
    phi1, lam1 = math.radians(lat), math.radians(lon)
    if d >= math.radians(90.0 - abs(lat)):
        raise SpatialParamError("A 'nearby' circle may not contain a pole; use type=bbox, or match=intersects.")
    pts = []
    for i in range(n):
        brg = 2 * math.pi * i / n
        phi2 = math.asin(math.sin(phi1) * math.cos(d) + math.cos(phi1) * math.sin(d) * math.cos(brg))
        lam2 = lam1 + math.atan2(math.sin(brg) * math.sin(d) * math.cos(phi1),
                                 math.cos(d) - math.sin(phi1) * math.sin(phi2))
        pts.append((math.degrees(lam2), math.degrees(phi2)))  # longitudes left unwrapped
    pts.append(pts[0])
    xs = [x for x, _ in pts]
    rings = [pts]
    if min(xs) < -180:
        rings.append([(x + 360, y) for x, y in pts])
    if max(xs) > 180:
        rings.append([(x - 360, y) for x, y in pts])
    polys = [Polygon(r, srid=4326) for r in rings]
    return polys[0] if len(polys) == 1 else MultiPolygon(*polys, srid=4326)


def _outward(v, up):
    """Round a bbox edge to 6 decimals without moving it inward."""
    f = math.ceil if up else math.floor
    return f(round(v * 1e6, 3)) / 1e6


def geometry_summaries(place_ids):
    """{place_id: summary} for the given places, computed in ONE grouped query, so no
    geometry is ever loaded into Python or serialised:

    * ``point``: ST_PointOnSurface of all the place's geometries collected, a point
      guaranteed to lie on them (a centroid may fall outside a concave polygon or
      between the parts of a multi-geometry);
    * ``bbox``: ST_Extent of all of them, [west, south, east, north];
    * ``types``, ``count``, ``vertices``: what the full geometry would hold.
    """
    from places.models import PlaceGeom
    rows = (PlaceGeom.objects.filter(place_id__in=list(place_ids), geom__isnull=False)
            .values("place_id")
            .annotate(extent=Extent("geom"),
                      point=PointOnSurface(Collect("geom")),
                      count=Count("id"),
                      vertices=Sum(NumPoints("geom")),
                      types=ArrayAgg(Func(F("geom"), function="GeometryType", output_field=CharField()),
                                     distinct=True)))
    out = {}
    for r in rows:
        w, s, e, n = r["extent"]
        out[r["place_id"]] = {
            "point": [r["point"].x, r["point"].y] if r["point"] else None,
            "bbox": [_outward(w, False), _outward(s, False), _outward(e, True), _outward(n, True)],
            "types": sorted(r["types"]),
            "count": r["count"],
            "vertices": r["vertices"],
        }
    return out
