import pyproj
from django.contrib.gis.geos import GEOSGeometry, MultiPolygon
from shapely.geometry import shape
from shapely.ops import transform, unary_union
from geojson import loads, dumps
from places.models import PlaceGeom

class CollectionGeospatialMixin:
    """Methods and properties for computing and accessing cached geometries."""

    def _buffer_geometry_to_polygon(self, geom):
        """
        Convert a point or line geometry to a polygon by buffering to 1km.
        Uses Azimuthal Equidistant projection centered on the geometry's centroid.
        """
        # Convert to shapely
        shapely_geom = shape(loads(geom.geojson))

        # Get centroid for projection center
        centroid = shapely_geom.centroid

        # Define Azimuthal Equidistant projection centered on centroid
        aeqd_proj = pyproj.Proj(proj='aeqd', lat_0=centroid.y, lon_0=centroid.x, datum='WGS84')
        wgs84_proj = pyproj.Proj(proj='latlong', datum='WGS84')

        # Create transformation functions
        project_to_aeqd = pyproj.Transformer.from_proj(wgs84_proj, aeqd_proj, always_xy=True).transform
        project_to_wgs84 = pyproj.Transformer.from_proj(aeqd_proj, wgs84_proj, always_xy=True).transform

        # Transform to AEQD, buffer 1000m, transform back to WGS84
        geom_aeqd = transform(project_to_aeqd, shapely_geom)
        buffered_aeqd = geom_aeqd.buffer(1000)  # 1km buffer
        buffered_wgs84 = transform(project_to_wgs84, buffered_aeqd)

        return buffered_wgs84

    def _compute_unioned_geometries(self):
        """
        Compute and cache both unioned_geometries and unioned_hulls.
        All geometries are validated and converted to polygons where necessary.
        Points and lines are buffered to 1km using appropriate projections.
        """
        # Get all place geometries
        # Note: self.places_all must be defined on Collection (or another mixin)
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

            # Skip invalid geometries
            if not geom or not geom.valid:
                continue

            # Convert to shapely for processing
            shapely_geom = shape(loads(geom.geojson))

            # Validate and fix if needed
            if not shapely_geom.is_valid:
                shapely_geom = shapely_geom.buffer(0)

            # Handle different geometry types
            if shapely_geom.geom_type in ['Point', 'MultiPoint', 'LineString', 'MultiLineString']:
                # Buffer points and lines to 1km polygons
                buffered = self._buffer_geometry_to_polygon(geom)
                if buffered.is_valid:
                    polygons_for_union.append(buffered)
                    # For hulls, use the convex hull of the buffered geometry
                    hull = buffered.convex_hull
                    if hull.is_valid and hull.geom_type in ['Polygon', 'MultiPolygon']:
                        polygons_for_hulls.append(hull)

            elif shapely_geom.geom_type in ['Polygon', 'MultiPolygon']:
                # Already polygons, use directly
                if shapely_geom.is_valid:
                    polygons_for_union.append(shapely_geom)
                    # Compute convex hull for hull union
                    hull = shapely_geom.convex_hull
                    if hull.is_valid and hull.geom_type in ['Polygon', 'MultiPolygon']:
                        polygons_for_hulls.append(hull)

        # Compute unions
        if polygons_for_union:
            unioned = unary_union(polygons_for_union)
            # Ensure result is valid
            if not unioned.is_valid:
                unioned = unioned.buffer(0)
            # Convert to MultiPolygon if it's a single Polygon
            if unioned.geom_type == 'Polygon':
                unioned = MultiPolygon([unioned])
            elif unioned.geom_type == 'MultiPolygon':
                pass
            else:
                unioned = MultiPolygon([g for g in unioned.geoms if g.geom_type == 'Polygon'])

            # Convert back to Django geometry
            self.unioned_geometries = GEOSGeometry(unioned.wkt, srid=4326)
        else:
            self.unioned_geometries = None

        if polygons_for_hulls:
            unioned_hulls = unary_union(polygons_for_hulls)
            # Ensure result is valid
            if not unioned_hulls.is_valid:
                unioned_hulls = unioned_hulls.buffer(0)
            # Convert to MultiPolygon if it's a single Polygon
            if unioned_hulls.geom_type == 'Polygon':
                unioned_hulls = MultiPolygon([unioned_hulls])
            elif unioned_hulls.geom_type == 'MultiPolygon':
                pass
            else:
                unioned_hulls = MultiPolygon([g for g in unioned_hulls.geoms if g.geom_type == 'Polygon'])

            # Convert back to Django geometry
            self.unioned_hulls = GEOSGeometry(unioned_hulls.wkt, srid=4326)
        else:
            self.unioned_hulls = None

        # Save both fields
        self.save(update_fields=['unioned_geometries', 'unioned_hulls'])

    @property
    def unioned_geometries_obj(self):
        """
        Get unioned geometries, computing and caching if not already present.
        (Needs self.unioned_geometries field)
        """
        if self.unioned_geometries is None:
            self._compute_unioned_geometries()
        return self.unioned_geometries

    @property
    def unioned_hulls_obj(self):
        """
        Get unioned hulls, computing and caching if not already present.
        (Needs self.unioned_hulls field)
        """
        if self.unioned_hulls is None:
            self._compute_unioned_geometries()
        return self.unioned_hulls