# datasets/geometry_utils.py
"""
Geometry utilities that require Django's GIS components.
"""
import json
import logging
from django.contrib.gis.db.models import Extent
from django.contrib.gis.geos import GEOSGeometry, GeometryCollection, Polygon

from shapely.geometry import shape, GeometryCollection, mapping
from shapely.ops import transform
from pyproj import Proj, Transformer, CRS

logger = logging.getLogger(__name__)


def hullify(g_list, buffer_km=10):
    """
    Creates a convex hull from a list of geometries, buffered by a specified distance (km)
    using a dynamic Azimuthal Equidistant projection.

    Args:
        g_list (list): List of geometry dicts (GeoJSON format).
        buffer_km (float): Distance in kilometers to buffer the hull (default: 10 km).

    Returns:
        dict: GeoJSON geometry of buffered convex hull, or empty dict on error.
    """

    # 1. Convert buffer distance to meters
    buffer_m = buffer_km * 1000

    # Convert GEOS dicts to Shapely objects for easier processing
    try:
        shapes = [shape(g) for g in g_list]
        geom_collection = GeometryCollection(shapes)

        if not geom_collection.geoms:
            return {}

        # Find the center point for the projection (centroid of the collection)
        # Use a safe centroid, converting it to a basic Point first
        safe_centroid = geom_collection.centroid
        lon, lat = safe_centroid.x, safe_centroid.y

    except Exception as e:
        logger.exception(f'hullify() initial setup failed on g_list {g_list}')
        return {}

    # 2. Define dynamic Azimuthal Equidistant projection (meters)
    # CRS based on WGS84, centered at the centroid of the geometry.
    local_azimuthal = Proj(
        proj='aeqd',
        ellps='WGS84',
        datum='WGS84',
        lon_0=lon,
        lat_0=lat
    )

    # Transformer for conversion: WGS84 (EPSG:4326) <-> Local AEQD (meters)
    transformer_to_local = Transformer.from_proj(
        CRS.from_epsg(4326), local_azimuthal, always_xy=True
    )
    transformer_to_wgs84 = Transformer.from_proj(
        local_azimuthal, CRS.from_epsg(4326), always_xy=True
    )

    # 3. Transform to Local Projection, Calculate Hull, Apply Buffer
    try:
        # Transform geometries to the local AEQD projection (in meters)
        projected_shapes = [
            transform(transformer_to_local.transform, s) for s in shapes
        ]

        # Calculate the convex hull in the projected (meter-based) system
        projected_hull = GeometryCollection(projected_shapes).convex_hull

        # Apply the buffer in meters
        buffered_projected_hull = projected_hull.buffer(buffer_m)

        # 4. Transform back to WGS84 (degrees)
        final_hull = transform(transformer_to_wgs84.transform, buffered_projected_hull)

        return mapping(final_hull)

    except Exception as e:
        logger.exception(f'hullify() processing failed: {e}')
        return {}


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
