# datasets/geometry_utils.py
"""
Geometry utilities that require Django's GIS components.
"""
import json
import logging
from django.contrib.gis.geos import GEOSGeometry, GeometryCollection, Polygon
from django.contrib.gis.db.models import Extent

from .core_utils import flatten

logger = logging.getLogger(__name__)


def _simplify_geometry(geom):
    """
    Reduce a GeoJSON geometry to its simplest valid form.

    Rules:
    - If the input is not a GeometryCollection, return it unchanged.
    - If a GeometryCollection contains a single geometry, unwrap and return that geometry.
    - If a GeometryCollection contains multiple geometries and they are all the same type,
      convert to the corresponding Multi* geometry (e.g., MultiPoint, MultiLineString,
      MultiPolygon) and return its GeoJSON.
    - If mixed geometry types are present, return the original GeometryCollection.
    - On any error, log and return the original geometry to avoid losing data.
    """
    # Quick guard for non-collection inputs
    if not isinstance(geom, dict) or geom.get("type") != "GeometryCollection":
        return geom

    geoms = geom.get("geometries", [])
    if not geoms:
        return geom

    try:
        # Convert subgeometries to GEOSGeometry objects for type inspection and unioning
        geos_list = [GEOSGeometry(json.dumps(g)) for g in geoms]
    except Exception as e:
        logger.exception("Failed to parse subgeometries in _simplify_geometry: %s", e)
        return geom

    # If only one geometry present, unwrap it
    if len(geos_list) == 1:
        try:
            return json.loads(geos_list[0].geojson)
        except Exception:
            return geom

    # Inspect geometry types
    types = {g.geom_type for g in geos_list}

    # If all geometries are the same type, attempt to create a Multi* geometry
    if len(types) == 1:
        geom_type = next(iter(types))
        try:
            # Make a GeometryCollection and then use unary_union or appropriate combine
            coll = GeometryCollection(geos_list)
            unioned = coll.union if hasattr(coll, "union") else coll.unary_union
            # unioned may be a single geometry or a Multi* geometry depending on inputs
            if unioned is None:
                # fallback: return the original collection
                return geom
            return json.loads(unioned.geojson)
        except Exception as e:
            logger.exception("Failed to create Multi geometry in _simplify_geometry: %s", e)
            return geom

    # Mixed types remain a GeometryCollection
    return geom


def patch_geos_signatures():
    """
    Patch GEOS to function on macOS arm64 and presumably
    other odd architectures by ensuring that call signatures
    are explicit, and that Django 4 bugfixes are backported.

    Should work on Django 2.2+, minimally tested, caveat emptor.
    """
    from ctypes import POINTER, c_uint, c_int
    from django.contrib.gis.geos import Polygon
    from django.contrib.gis.geos import prototypes as capi
    from django.contrib.gis.geos.prototypes import GEOM_PTR
    from django.contrib.gis.geos.prototypes.geom import GeomOutput
    from django.contrib.gis.geos.libgeos import geos_version, lgeos
    from django.contrib.gis.geos.linestring import LineString

    _geos_version = geos_version()
    logger.debug("GEOS: %s %s", _geos_version, repr(lgeos))

    # Backport https://code.djangoproject.com/ticket/30274
    def new_linestring_iter(self):
        for i in range(len(self)):
            yield self[i]

    LineString.__iter__ = new_linestring_iter

    # macOS arm64 requires that we have explicit argtypes for cffi calls.
    capi.create_polygon = GeomOutput(
        "GEOSGeom_createPolygon", argtypes=[GEOM_PTR, POINTER(GEOM_PTR), c_uint]
    )

    capi.create_collection = GeomOutput(
        "GEOSGeom_createCollection", argtypes=[c_int, POINTER(GEOM_PTR), c_uint]
    )

    def new_create_polygon(self, length, items):
        if not length:
            return capi.create_empty_polygon()

        rings = []
        for r in items:
            if isinstance(r, GEOM_PTR):
                rings.append(r)
            else:
                rings.append(self._construct_ring(r))

        shell = self._clone(rings.pop(0))

        n_holes = length - 1
        if n_holes:
            holes = (GEOM_PTR * n_holes)(*[self._clone(r) for r in rings])
            holes_param = holes
        else:
            holes_param = None

        return capi.create_polygon(shell, holes_param, c_uint(n_holes))

    Polygon._create_polygon = new_create_polygon

    def new_create_collection(self, length, items):
        geoms = (GEOM_PTR * length)(
            *[
                capi.geom_clone(getattr(g, "ptr", g))
                for g in items
            ]
        )
        return capi.create_collection(c_int(self._typeid), geoms, c_uint(length))

    GeometryCollection._create_collection = new_create_collection


def hullify(g_list):
    """
    Create convex hull from list of geometries with buffering.

    Args:
        g_list: List of geometry dicts (GeoJSON format)

    Returns:
        dict: GeoJSON geometry of convex hull, or empty list on error
    """
    # Apply GEOS patch if needed
    patch_geos_signatures()

    # 1 point -> Point; 2 points -> LineString; >2 -> Polygon
    try:
        mp = [GEOSGeometry(json.dumps(g)) for g in g_list]
        hull = GeometryCollection(mp).convex_hull
    except Exception as e:
        logger.exception(f'hullify() failed on g_list {g_list}')
        return []

    if hull.geom_type in ['Point', 'LineString', 'Polygon']:
        # buffer hull, but only a little if near meridian
        coll = GeometryCollection([GEOSGeometry(json.dumps(g)) for g in g_list]).simplify()
        longs = list(c[0] for c in flatten(coll.coords))

        try:
            if len([i for i in longs if i >= 175]) == 0:
                hull = hull.buffer(1.4)  # ~100km radius
            else:
                hull = hull.buffer(0.1)
        except Exception as e:
            logger.exception(f'hullify buffer error longs: {longs}')

    return json.loads(hull.geojson) if hull.geojson is not None else []


def ccodesFromGeom(geom):
    """
    Extract country codes from geometry by spatial intersection.
    Requires Country model - use lazy import to avoid circular dependencies.

    Args:
        geom: GeoJSON geometry dict

    Returns:
        list: ISO country codes
    """
    # Lazy import to avoid circular dependency
    from areas.models import Country

    if geom['type'] == 'Point' and geom['coordinates'] == []:
        return []

    g = GEOSGeometry(str(geom))

    if g.geom_type == 'GeometryCollection':
        # just hull them all
        qs = Country.objects.filter(mpoly__intersects=g.convex_hull)
    else:
        qs = Country.objects.filter(mpoly__intersects=g)

    ccodes = [c.iso for c in qs]
    return ccodes


def compute_dataset_bbox(label):
    """
    Compute bounding box for all geometries in a dataset.
    Requires PlaceGeom model - use lazy import.

    Args:
        label: Dataset label

    Returns:
        Polygon: Bounding box polygon, or None if no geometries
    """
    # Lazy import to avoid circular dependency
    from places.models import PlaceGeom

    dsgeoms = PlaceGeom.objects.filter(place__dataset=label)
    extent = dsgeoms.aggregate(Extent("geom"))["geom__extent"]

    if extent:
        return Polygon.from_bbox(extent)
    return None