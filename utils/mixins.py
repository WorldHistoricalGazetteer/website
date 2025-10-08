"""
Geospatial computation utilities for Collection models.

This mixin provides methods for computing and caching unified geometries
of collection places, with proper geodesic buffering and validation.
"""
import logging
from typing import Optional

import pyproj
from django.contrib.gis.geos import GEOSGeometry, MultiPolygon, Polygon
from geojson import loads
from shapely.geometry import shape
from shapely.ops import transform, unary_union

from places.models import PlaceGeom

logger = logging.getLogger(__name__)

# --- Module-Level Constants ---

# Standard WGS84 projection
WGS84_PROJ = pyproj.Proj(proj='latlong', datum='WGS84')

# Buffer distance for points/lines in meters (1km = 1000m)
# This converts arbitrary point/line data into polygons for unioning
DEFAULT_POINT_LINE_BUFFER_M = 1000


class CollectionGeospatialMixin:
    """
    Provides methods and properties for computing, caching, and accessing
    geometries related to a Collection's overall spatial extent.

    Expects the following attributes on the model:
    - places_all: QuerySet of all places in the collection
    - unioned_geometries: MultiPolygonField for cached geometries
    - unioned_hulls: MultiPolygonField for cached hulls
    - save(): Standard Django model save method
    """

    def _validate_shapely(self, geom):
        """
        Ensures a shapely geometry is valid by applying buffer(0) if necessary.

        Args:
            geom: Shapely geometry

        Returns:
            Valid shapely geometry
        """
        if not geom.is_valid:
            return geom.buffer(0)
        return geom

    def _get_aeqd_transformers(self, centroid):
        """
        Sets up the Azimuthal Equidistant (AEQD) projection centered on the centroid
        and returns the transformation functions to and from AEQD.

        Args:
            centroid: Shapely Point object

        Returns:
            tuple: (project_to_aeqd, project_to_wgs84) transformer functions
        """
        lat, lon = centroid.y, centroid.x

        # Define Azimuthal Equidistant projection centered on centroid
        aeqd_proj = pyproj.Proj(proj='aeqd', lat_0=lat, lon_0=lon, datum='WGS84')

        # Create transformation functions
        project_to_aeqd = pyproj.Transformer.from_proj(WGS84_PROJ, aeqd_proj, always_xy=True).transform
        project_to_wgs84 = pyproj.Transformer.from_proj(aeqd_proj, WGS84_PROJ, always_xy=True).transform

        return project_to_aeqd, project_to_wgs84

    def _buffer_geometry_to_polygon(self, geom):
        """
        Convert a point or line geometry to a polygon by buffering to 1km.
        Uses Azimuthal Equidistant projection centered on the geometry's centroid.

        Args:
            geom: Django GEOS geometry (Point, LineString, MultiPoint, or MultiLineString)

        Returns:
            Shapely Polygon in WGS84
        """
        try:
            # Convert to shapely and validate
            shapely_geom = self._validate_shapely(shape(loads(geom.geojson)))

            # Setup projection transformers
            project_to_aeqd, project_to_wgs84 = self._get_aeqd_transformers(shapely_geom.centroid)

            # Transform to AEQD, buffer, transform back to WGS84
            geom_aeqd = transform(project_to_aeqd, shapely_geom)
            buffered_aeqd = geom_aeqd.buffer(DEFAULT_POINT_LINE_BUFFER_M)
            buffered_wgs84 = transform(project_to_wgs84, buffered_aeqd)

            return buffered_wgs84
        except Exception as e:
            logger.warning(f"Failed to buffer geometry: {e}")
            return None

    def _shapely_to_multipolygon(self, shapely_geom):
        """
        Convert a validated shapely geometry to a Django MultiPolygon.
        Handles Polygon, MultiPolygon, and GeometryCollection types.

        Args:
            shapely_geom: Shapely geometry (should be valid)

        Returns:
            GEOSGeometry MultiPolygon or None
        """
        if shapely_geom.is_empty:
            return None

        if shapely_geom.geom_type == 'Polygon':
            return GEOSGeometry(MultiPolygon([shapely_geom]).wkt, srid=4326)
        elif shapely_geom.geom_type == 'MultiPolygon':
            return GEOSGeometry(shapely_geom.wkt, srid=4326)
        elif shapely_geom.geom_type == 'GeometryCollection':
            # Extract only polygons from collection
            polygons = [g for g in shapely_geom.geoms if g.geom_type in ('Polygon', 'MultiPolygon')]
            if not polygons:
                return None
            # Flatten any MultiPolygons
            flat_polygons = []
            for g in polygons:
                if g.geom_type == 'Polygon':
                    flat_polygons.append(g)
                else:  # MultiPolygon
                    flat_polygons.extend(g.geoms)
            if flat_polygons:
                return GEOSGeometry(MultiPolygon(flat_polygons).wkt, srid=4326)
        return None

    def _compute_unioned_geometries(self):
        """
        Compute and cache both unioned_geometries and unioned_hulls.
        All geometries are validated and converted to polygons where necessary.
        Points and lines are buffered to 1km using appropriate projections.
        """
        try:
            place_ids = self.places_all.values_list('id', flat=True)
            place_geoms = PlaceGeom.objects.filter(place_id__in=place_ids, geom__isnull=False)

            if not place_geoms.exists():
                self.unioned_geometries = None
                self.unioned_hulls = None
                self.save(update_fields=['unioned_geometries', 'unioned_hulls'])
                logger.info(f"No geometries found for collection {self.pk}")
                return

            polygons_for_union = []
            polygons_for_hulls = []

            for pg in place_geoms:
                geom = pg.geom
                if not geom or not geom.valid:
                    continue

                try:
                    # Convert to shapely and validate
                    shapely_geom = self._validate_shapely(shape(loads(geom.geojson)))

                    if shapely_geom.geom_type in ['Point', 'MultiPoint', 'LineString', 'MultiLineString']:
                        # Buffer points and lines to 1km polygons
                        buffered = self._buffer_geometry_to_polygon(geom)
                        if buffered and buffered.is_valid:
                            polygons_for_union.append(buffered)
                            hull = self._validate_shapely(buffered.convex_hull)
                            if hull.geom_type in ['Polygon', 'MultiPolygon']:
                                polygons_for_hulls.append(hull)

                    elif shapely_geom.geom_type in ['Polygon', 'MultiPolygon']:
                        polygons_for_union.append(shapely_geom)
                        hull = self._validate_shapely(shapely_geom.convex_hull)
                        if hull.geom_type in ['Polygon', 'MultiPolygon']:
                            polygons_for_hulls.append(hull)

                except Exception as e:
                    logger.warning(f"Failed to process geometry for place {pg.place_id}: {e}")
                    continue

            # Compute unions with proper geometry type handling
            if polygons_for_union:
                unioned = self._validate_shapely(unary_union(polygons_for_union))
                self.unioned_geometries = self._shapely_to_multipolygon(unioned)
            else:
                self.unioned_geometries = None

            if polygons_for_hulls:
                unioned_hulls = self._validate_shapely(unary_union(polygons_for_hulls))
                self.unioned_hulls = self._shapely_to_multipolygon(unioned_hulls)
            else:
                self.unioned_hulls = None

            self.save(update_fields=['unioned_geometries', 'unioned_hulls'])
            logger.info(f"Successfully computed unioned geometries for collection {self.pk}")

        except Exception as e:
            logger.error(f"Error computing unioned geometries for collection {self.pk}: {e}")
            self.unioned_geometries = None
            self.unioned_hulls = None
            self.save(update_fields=['unioned_geometries', 'unioned_hulls'])

    @property
    def unioned_geometries_cached(self):
        """
        Get unioned geometries, computing and caching if not already present.
        Also computes unioned_hulls for efficiency.
        """
        if self.unioned_geometries is None:
            self._compute_unioned_geometries()
        return self.unioned_geometries

    @property
    def unioned_hulls_cached(self):
        """
        Get unioned hulls, computing and caching if not already present.
        Also computes unioned_geometries for efficiency.
        """
        if self.unioned_hulls is None:
            self._compute_unioned_geometries()
        return self.unioned_hulls

    def get_hull_buffered(self, buffer_m: float = DEFAULT_POINT_LINE_BUFFER_M) -> Optional[MultiPolygon]:
        """
        Returns a buffered version of the unioned_hulls_cached.

        Calculates the buffer by projecting and buffering each constituent polygon 
        individually for geodesic accuracy, then computing the final union.

        Args:
            buffer_m: Buffer distance in meters (default: 1000m/1km)

        Returns:
            MultiPolygon or None if no hull exists
        """
        hull_geom = self.unioned_hulls_cached

        if not hull_geom:
            return None

        try:
            # Extract constituent polygons
            if hull_geom.geom_type == 'Polygon':
                polygons = [hull_geom]
            else:  # MultiPolygon
                polygons = list(hull_geom)

            buffered_shapely_geoms = []

            for django_polygon in polygons:
                try:
                    # 1. Convert to Shapely and validate
                    shapely_polygon = self._validate_shapely(shape(loads(django_polygon.geojson)))

                    # 2. Setup projection transformers centered on this polygon
                    project_to_aeqd, project_to_wgs84 = self._get_aeqd_transformers(shapely_polygon.centroid)

                    # 3. Transform to local projection, buffer, validate, transform back
                    local_geom = transform(project_to_aeqd, shapely_polygon)
                    buffered_local = self._validate_shapely(local_geom.buffer(buffer_m))
                    buffered_wgs84 = transform(project_to_wgs84, buffered_local)

                    buffered_shapely_geoms.append(buffered_wgs84)

                except Exception as e:
                    logger.warning(f"Failed to buffer polygon in collection {self.pk}: {e}")
                    continue

            # 4. Compute the final union
            if not buffered_shapely_geoms:
                return None

            final_union = self._validate_shapely(unary_union(buffered_shapely_geoms))

            # 5. Convert to MultiPolygon using helper method
            return self._shapely_to_multipolygon(final_union)

        except Exception as e:
            logger.error(f"Error buffering hull for collection {self.pk}: {e}")
            return None