import pyproj
from django.contrib.gis.geos import GEOSGeometry, MultiPolygon
from shapely.geometry import shape
from shapely.ops import transform, unary_union
from geojson import loads, dumps
from places.models import PlaceGeom

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
    """

    def _validate_shapely(self, geom):
        """Ensures a shapely geometry is valid by applying buffer(0) if necessary."""
        if not geom.is_valid:
            return geom.buffer(0)
        return geom

    def _get_aeqd_transformers(self, centroid):
        """
        Sets up the Azimuthal Equidistant (AEQD) projection centered on the centroid
        and returns the transformation functions to and from AEQD.
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
        """
        # Convert to shapely and validate
        shapely_geom = self._validate_shapely(shape(loads(geom.geojson)))

        # Setup projection transformers
        project_to_aeqd, project_to_wgs84 = self._get_aeqd_transformers(shapely_geom.centroid)

        # Transform to AEQD, buffer using the global constant, transform back to WGS84
        geom_aeqd = transform(project_to_aeqd, shapely_geom)
        buffered_aeqd = geom_aeqd.buffer(DEFAULT_POINT_LINE_BUFFER_M)
        buffered_wgs84 = transform(project_to_wgs84, buffered_aeqd)

        return buffered_wgs84

    def _compute_unioned_geometries(self):
        """
        Compute and cache both unioned_geometries and unioned_hulls.
        """
        # Note: self.places_all must be defined on Collection
        place_ids = self.places_all.values_list('id', flat=True)
        place_geoms = PlaceGeom.objects.filter(place_id__in=place_ids, geom__isnull=False)

        if not place_geoms.exists():
            self.unioned_geometries = None
            self.unioned_hulls = None
            self.save(update_fields=['unioned_geometries', 'unioned_hulls'])
            return

        polygons_for_union = []
        polygons_for_hulls = []

        for pg in place_geoms:
            geom = pg.geom
            if not geom or not geom.valid: continue

            # Convert to shapely and validate
            shapely_geom = self._validate_shapely(shape(loads(geom.geojson)))

            if shapely_geom.geom_type in ['Point', 'MultiPoint', 'LineString', 'MultiLineString']:
                # Buffer uses _buffer_geometry_to_polygon, which now uses the global constant
                buffered = self._buffer_geometry_to_polygon(geom)
                if buffered.is_valid:
                    polygons_for_union.append(buffered)
                    hull = self._validate_shapely(buffered.convex_hull)
                    if hull.geom_type in ['Polygon', 'MultiPolygon']:
                        polygons_for_hulls.append(hull)

            elif shapely_geom.geom_type in ['Polygon', 'MultiPolygon']:
                if shapely_geom.is_valid:
                    polygons_for_union.append(shapely_geom)
                    hull = self._validate_shapely(shapely_geom.convex_hull)
                    if hull.geom_type in ['Polygon', 'MultiPolygon']:
                        polygons_for_hulls.append(hull)

        # Compute unions and final validation
        if polygons_for_union:
            unioned = self._validate_shapely(unary_union(polygons_for_union))
            if unioned.geom_type == 'Polygon': unioned = MultiPolygon([unioned])
            self.unioned_geometries = GEOSGeometry(unioned.wkt, srid=4326)
        else:
            self.unioned_geometries = None

        if polygons_for_hulls:
            unioned_hulls = self._validate_shapely(unary_union(polygons_for_hulls))
            if unioned_hulls.geom_type == 'Polygon': unioned_hulls = MultiPolygon([unioned_hulls])
            self.unioned_hulls = GEOSGeometry(unioned_hulls.wkt, srid=4326)
        else:
            self.unioned_hulls = None

        self.save(update_fields=['unioned_geometries', 'unioned_hulls'])

    @property
    def unioned_geometries_obj(self):
        """Get unioned geometries, computing and caching if not already present."""
        if self.unioned_geometries is None:
            self._compute_unioned_geometries()
        return self.unioned_geometries

    @property
    def unioned_hulls_obj(self):
        """Get unioned hulls, computing and caching if not already present."""
        if self.unioned_hulls is None:
            self._compute_unioned_geometries()
        return self.unioned_hulls

    def get_hull_buffered(self, buffer_m: float = DEFAULT_POINT_LINE_BUFFER_M ) -> MultiPolygon | None:
        """
        Returns a buffered version of the unioned_hulls_obj, calculating the buffer
        by projecting and buffering each constituent polygon individually for geodesic accuracy,
        then computing the final union.

        :param buffer_m: buffer distance in metres
        """
        hull_geom = self.unioned_hulls_obj

        if not hull_geom:
            return None

        # Extract constituent polygons
        polygons = hull_geom if hull_geom.geom_type == 'Polygon' else [p for p in hull_geom]

        buffered_shapely_geoms = []

        for django_polygon in polygons:
            # 1. Convert to Shapely and validate
            shapely_polygon = self._validate_shapely(shape(loads(django_polygon.geojson)))

            # 2. Setup projection transformers
            project_to_aeqd, project_to_wgs84 = self._get_aeqd_transformers(shapely_polygon.centroid)

            # 3. Transform, Buffer, Reproject
            local_geom = transform(project_to_aeqd, shapely_polygon)
            buffered_local = local_geom.buffer(buffer_m)

            # 4. Correction step after buffer
            buffered_local = self._validate_shapely(buffered_local)

            buffered_wgs84 = transform(project_to_wgs84, buffered_local)

            buffered_shapely_geoms.append(buffered_wgs84)

        # 5. Compute the final union
        if not buffered_shapely_geoms:
            return None

        final_union = self._validate_shapely(unary_union(buffered_shapely_geoms))

        # Final MultiPolygon conversion
        if final_union.geom_type == "Polygon":
            final_geom = MultiPolygon(GEOSGeometry(final_union.wkt, srid=4326))
        elif final_union.geom_type == "MultiPolygon":
            final_geom = GEOSGeometry(final_union.wkt, srid=4326)
        else:
            # Handle GeometryCollections by extracting Polygons
            final_geom = MultiPolygon([GEOSGeometry(g.wkt, srid=4326)
                                       for g in final_union.geoms if g.geom_type in ('Polygon', 'MultiPolygon')])

        return final_geom