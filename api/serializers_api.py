# api/serializers_api.py
import logging
from functools import reduce

from pyproj import CRS, Transformer
from rest_framework import serializers
from shapely.geometry import shape
from shapely.ops import transform

from api.serializers import PlaceNameSerializer, PlaceTypeSerializer, PlaceWhenSerializer, PlaceLinkSerializer, \
    PlaceRelatedSerializer, PlaceDescriptionSerializer, PlaceDepictionSerializer
from areas.models import Area
from collection.models import Collection
from datasets.models import Dataset
from periods.models import Period
from places.models import Place, PlaceGeom

logger = logging.getLogger('reconciliation')


def normalize_timespans(data):
    """
    Iterate through when/timespans and normalise into dicts:
    {begin, end, circa, note}.
    """
    timespans = []
    for when in data.get("when", []):  # or "whens" if that's your serializer field
        for ts in when.get("timespans", []):
            start = ts.get("start") or {}
            end = ts.get("end") or {}

            timespan = {
                "begin": start.get("earliest") or start.get("latest"),
                "end": end.get("latest") or end.get("earliest"),
                "circa": ts.get("circa"),
                "note": ts.get("note"),
            }
            # drop empty keys
            timespan = {k: v for k, v in timespan.items() if v is not None}
            if timespan:
                timespans.append(timespan)
    return timespans


class APIPlaceGeomSerializer(serializers.ModelSerializer):
    """
    PlaceGeom serializer with computed geometry helpers:
    - geojson: full geometry object from jsonb
    - geowkt: WKT string derived from jsonb
    - bbox: [minLng, minLat, maxLng, maxLat]
    - centroid: [lng, lat] in WGS84
    """

    ds = serializers.SerializerMethodField()
    geom = serializers.ReadOnlyField(source="jsonb")

    type = serializers.ReadOnlyField(source="jsonb.type")
    coordinates = serializers.ReadOnlyField(source="jsonb.coordinates")
    citation = serializers.ReadOnlyField(source="jsonb.citation")
    when = serializers.ReadOnlyField(source="jsonb.when")
    certainty = serializers.ReadOnlyField(source="jsonb.certainty")

    geojson = serializers.SerializerMethodField()
    geowkt = serializers.SerializerMethodField()
    bbox = serializers.SerializerMethodField()
    centroid = serializers.SerializerMethodField()

    def get_ds(self, obj):
        return obj.place.dataset.id

    def _shapely_geom(self, obj):
        jb = getattr(obj, "jsonb", None)
        if not jb:
            return None
        try:
            return shape(jb)
        except Exception:
            return None

    def get_geojson(self, obj):
        geojson = getattr(obj, "jsonb", None)
        if not geojson:
            return None
        return {k: v for k, v in geojson.items() if k != "geowkt"}

    def get_geowkt(self, obj):
        geojson = getattr(obj, "jsonb", None)
        if not geojson:
            return None
        return geojson.get("geowkt")

    def get_bbox(self, obj):
        g = self._shapely_geom(obj)
        if not g:
            return None
        minx, miny, maxx, maxy = g.bounds
        return [minx, miny, maxx, maxy]

    def get_centroid(self, obj):
        g = self._shapely_geom(obj)
        if not g:
            return None
        try:
            # pick a local equal-area projection centred on bbox midpoint
            minx, miny, maxx, maxy = g.bounds
            cx = (minx + maxx) / 2
            cy = (miny + maxy) / 2
            crs_wgs84 = CRS.from_epsg(4326)
            local_crs = CRS.from_proj4(
                f"+proj=aea +lat_0={cy} +lon_0={cx} "
                "+lat_1={0} +lat_2={1}".format(cy - 10, cy + 10)
            )
            transformer_to_local = Transformer.from_crs(crs_wgs84, local_crs, always_xy=True).transform
            transformer_to_wgs = Transformer.from_crs(local_crs, crs_wgs84, always_xy=True).transform

            g_local = transform(transformer_to_local, g)
            c_local = g_local.centroid
            c_wgs = transform(transformer_to_wgs, c_local)
            return [c_wgs.x, c_wgs.y]
        except Exception:
            # fallback: shapely centroid in WGS84
            c = g.centroid
            return [c.x, c.y]

    class Meta:
        model = PlaceGeom
        fields = (
            "place_id",
            "src_id",
            "type",
            "geowkt",
            "coordinates",
            "geom_src",
            "citation",
            "when",
            "title",
            "ds",
            "certainty",
            "geom",
            "geojson",
            "bbox",
            "centroid",
        )


class AreaPreviewSerializer(serializers.ModelSerializer):
    """
    Lightweight serializer for an Area preview snippet.
    Intended for reconciliation preview (HTML/JSON summary).
    """

    class Meta:
        model = Area
        fields = [
            "id",
            "title",
            "type",
            "description",
            "created",
            "ccodes",
        ]


class AreaFeatureSerializer(serializers.ModelSerializer):
    """
    Serializer that returns an Area as a single GeoJSON Feature.
    """

    class Meta:
        model = Area
        fields = [
            "id",
            "type",
            "title",
            "description",
            "ccodes",
            "geojson",
            "created",
        ]

    def to_representation(self, instance):
        # Use geojson if present, otherwise bbox as fallback
        geometry = instance.geojson or (
            instance.bbox.geojson if instance.bbox else None
        )

        return {
            "type": "Feature",
            "geometry": geometry,
            "properties": {
                "id": instance.id,
                "type": instance.type,
                "title": instance.title,
                "description": instance.description,
                "ccodes": instance.ccodes,
                "created": instance.created.isoformat() if instance.created else None,
            },
        }


class CollectionPreviewSerializer(serializers.ModelSerializer):
    """
    Lightweight serializer for a Collection preview snippet.
    Intended for reconciliation preview (HTML/JSON summary).
    """
    datasets = serializers.SlugRelatedField(
        many=True,
        read_only=True,
        slug_field="title"
    )

    class Meta:
        model = Collection
        fields = [
            "id",
            "title",
            "creator",
            "collection_class",
            "datasets",
            "description",
            "create_date",
        ]


class DatasetPreviewSerializer(serializers.ModelSerializer):
    """
    Minimal serializer for reconciliation preview snippets.
    Provides title, description, and summary metadata.
    """

    class Meta:
        model = Dataset
        fields = [
            "id",
            "title",
            "creator",
            "description",
            "numrows",
            "create_date",
        ]


class PeriodFeatureSerializer(serializers.ModelSerializer):
    """
    Serialize a Period as a GeoJSON Feature for LPF-compatible mapping.
    Geometry is taken from bbox or merged spatialCoverage geometries.
    """
    authority = serializers.SerializerMethodField()
    chrononyms = serializers.SerializerMethodField()
    spatial_coverage = serializers.SerializerMethodField()
    temporal_bounds = serializers.SerializerMethodField()
    canonical_label = serializers.SerializerMethodField()
    geometry = serializers.SerializerMethodField()

    class Meta:
        model = Period
        fields = [
            'id', 'chrononym', 'canonical_label', 'type', 'language', 'languageTag',
            'authority', 'chrononyms', 'spatial_coverage', 'temporal_bounds',
            'editorialNote', 'note', 'broader', 'narrower', 'sameAs', 'url',
            'script', 'derivedFrom', 'geometry'
        ]

    def get_canonical_label(self, obj):
        return obj.chrononym or (obj.chrononyms.first().label if obj.chrononyms.exists() else None)

    def get_authority(self, obj):
        if obj.authority:
            return {
                'id': obj.authority.id,
                'label': getattr(obj.authority, 'label', None),
                'type': obj.authority.type,
                'editorialNote': obj.authority.editorialNote,
                'sameAs': obj.authority.sameAs,
                'source': obj.authority.source,
            }
        return None

    def get_chrononyms(self, obj):
        return [
            {
                'id': c.id,
                'label': c.label,
                'languageTag': c.languageTag,
                'isPrimary': (c.label == obj.chrononym and c.languageTag == obj.languageTag),
            }
            for c in obj.chrononyms.all()
        ]

    def get_spatial_coverage(self, obj):
        return [
            {
                'uri': se.uri,
                'label': se.label,
                'gazetteer_source': se.gazetteer_source,
                'geometry': se.geometry.geojson if se.geometry else None,
                'bbox': se.bbox.geojson if se.bbox else None,
            }
            for se in obj.spatialCoverage.all()
        ]

    def get_temporal_bounds(self, obj):
        bounds = {}
        for tb in obj.bounds.all():
            bounds[tb.kind] = {
                'label': tb.label,
                'year': tb.year,
                'earliestYear': tb.earliestYear,
                'latestYear': tb.latestYear,
                'interval': f"{tb.earliestYear}/{tb.latestYear}" if tb.earliestYear and tb.latestYear else None,
            }
        return bounds

    def get_geometry(self, obj):
        # Prefer bbox; if missing, union all spatialCoverage geometries
        if obj.bbox:
            return obj.bbox.geojson

        geoms = [se.geometry for se in obj.spatialCoverage.all() if se.geometry]
        if not geoms:
            return None

        # Use union of geometries if multiple
        union_geom = reduce(lambda a, b: a.union(b), geoms)
        return union_geom.geojson

    def to_representation(self, obj):
        """Wrap as GeoJSON Feature"""
        feature = super().to_representation(obj)
        return {
            "type": "Feature",
            "geometry": feature.pop('geometry', None),
            "properties": feature
        }


class PeriodPreviewSerializer(serializers.ModelSerializer):
    """
    Serializer for Period preview (lighter version for HTML preview)
    """
    authority_label = serializers.SerializerMethodField()
    chrononyms_preview = serializers.SerializerMethodField()
    spatial_preview = serializers.SerializerMethodField()
    date_range = serializers.SerializerMethodField()

    class Meta:
        model = Period
        fields = [
            'id', 'chrononym', 'languageTag', 'editorialNote', 'note',
            'authority_label', 'chrononyms_preview', 'spatial_preview', 'date_range'
        ]

    def get_authority_label(self, obj):
        return obj.authority.id if obj.authority else 'Unknown'

    def get_chrononyms_preview(self, obj):
        chrononyms = list(obj.chrononyms.all()[:3])  # show a few
        return [f"{c.label} ({c.languageTag})" for c in chrononyms]

    def get_spatial_preview(self, obj):
        spatial = list(obj.spatialCoverage.all()[:3])
        return [se.label or se.uri for se in spatial]

    @staticmethod
    def format_year(year: int) -> str:
        """Convert a numeric year to a readable BCE/CE string."""
        if year is None:
            return "Unknown"
        return f"{abs(year)} BCE" if year < 0 else f"{year} CE"

    def get_date_range(self, obj):
        bounds = {b.kind: b for b in obj.bounds.all()}
        result = {}

        for kind in ("start", "stop"):
            b = bounds.get(kind)
            if not b:
                continue
            if b.earliestYear == b.latestYear:
                result[kind if kind == "stop" else "begin"] = self.format_year(b.earliestYear)
            else:
                result[kind if kind == "stop" else "begin"] = f"{self.format_year(b.earliestYear)} / {self.format_year(b.latestYear)}"

        return result if result else "Unknown"


class PlacePreviewSerializer(serializers.ModelSerializer):
    """
    Minimal serializer for reconciliation preview snippets.
    Provides name, description, and key locational fields.
    """
    names = PlaceNameSerializer(many=True, read_only=True)
    types = PlaceTypeSerializer(many=True, read_only=True)
    year_ranges = serializers.SerializerMethodField()

    class Meta:
        model = Place
        fields = ["id", "title", "names", "types", "ccodes", "fclasses", "year_ranges", "dataset"]

    def get_year_ranges(self, obj):
        ranges = []
        for when in obj.whens.all():
            data = getattr(when, "jsonb", {}) or {}
            timespans = data.get("timespans", [])

            for ts in timespans:
                start_data = ts.get("start", {})
                end_data = ts.get("end", {})

                start = start_data.get("earliest") or start_data.get("latest")
                end = end_data.get("latest") or end_data.get("earliest")

                if start and end:
                    ranges.append(f"{start}-{end}")
                elif start:
                    ranges.append(f"{start}-")
                elif end:
                    ranges.append(f"-{end}")

        return ranges


class OptimizedPlaceSerializer(serializers.ModelSerializer):
    """
    Optimized serializer that only includes requested fields.
    """
    dataset = serializers.ReadOnlyField(source="dataset.title")
    dataset_id = serializers.ReadOnlyField(source="dataset.id")

    names = PlaceNameSerializer(many=True, read_only=True)
    types = PlaceTypeSerializer(many=True, read_only=True)
    geoms = APIPlaceGeomSerializer(many=True, read_only=True)
    whens = PlaceWhenSerializer(many=True, read_only=True)
    links = PlaceLinkSerializer(many=True, read_only=True)
    related = PlaceRelatedSerializer(many=True, read_only=True)
    descriptions = PlaceDescriptionSerializer(many=True, read_only=True)
    depictions = PlaceDepictionSerializer(many=True, read_only=True)

    def __init__(self, *args, **kwargs):
        # Remove fields parameter from kwargs if it exists
        fields = kwargs.pop('fields', None)
        super().__init__(*args, **kwargs)

        if fields is not None:
            # Drop any fields that are not specified in the `fields` argument
            allowed = set(fields)
            existing = set(self.fields)
            for field_name in existing - allowed:
                self.fields.pop(field_name)

    class Meta:
        model = Place
        fields = (
            "id", "title", "ccodes", "fclasses",
            "names", "types", "geoms", "extent", "whens",
            "links", "related", "descriptions", "depictions",
            "dataset", "dataset_id"
        )


class PlaceFeatureSerializer(OptimizedPlaceSerializer):
    """
    Full serializer for place feature representations.
    Includes identifiers, dataset metadata, names, types,
    geometries, temporal extents, relationships, and media.
    Suitable for machine-readable exchange.

    This is a wrapper around OptimizedPlaceSerializer that includes all fields.
    """

    def __init__(self, *args, **kwargs):
        # Don't pass fields parameter - we want all fields for this serializer
        kwargs.pop('fields', None)
        super().__init__(*args, **kwargs)

    class Meta:
        model = Place
        fields = (
            "url",
            "id",
            "title",
            "src_id",
            "dataset",
            "dataset_id",
            "ccodes",
            "fclasses",
            "names",
            "types",
            "geoms",
            "extent",
            "links",
            "related",
            "whens",
            "descriptions",
            "depictions",
            "minmax",
        )
