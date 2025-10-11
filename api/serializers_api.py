# api/serializers_api.py
import json
import logging
from functools import reduce

from pyproj import CRS, Transformer
from rest_framework import serializers
from shapely.geometry import shape
from shapely.geometry.collection import GeometryCollection
from shapely.geometry.geo import mapping
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon
from shapely.ops import transform, unary_union

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


class OptimizedPeriodSerializer(serializers.ModelSerializer):
    """
    Optimized serializer for Periods, only includes requested fields.
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
            'script', 'derivedFrom', 'geometry', 'ccodes', 'bbox', 'source'
        ]

    def __init__(self, *args, **kwargs):
        fields = kwargs.pop('fields', None)
        super().__init__(*args, **kwargs)
        if fields is not None:
            allowed = set(fields)
            for field_name in set(self.fields) - allowed:
                self.fields.pop(field_name)

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
        # Prefer bbox; else union all spatialCoverage geometries
        if obj.bbox:
            return obj.bbox.geojson
        geoms = [se.geometry for se in obj.spatialCoverage.all() if se.geometry]
        if not geoms:
            return None
        union_geom = reduce(lambda a, b: a.union(b), geoms)
        return union_geom.geojson


class PeriodFeatureSerializer(serializers.ModelSerializer):
    """
    LPF-compliant serializer for Period objects as GeoJSON Features.
    Follows Linked Places Format v2.0 specification.
    """

    def to_representation(self, obj):
        # Build LPF Feature structure
        feature = {
            "@context": "https://whgazetteer.org/schema/lpo_v2.0.jsonld",
            "type": "Feature",
            "@id": f"https://whgazetteer.org/period/{obj.id}",
            "properties": {
                "title": obj.chrononym or "Untitled Period",
                "ccodes": self.get_country_codes(obj),
                "fclasses": ["L"]  # Area
            },
            "names": self.get_names(obj),
            "geometry": self.get_geometry(obj),
            "when": self.get_when(obj),
            "links": self.get_links(obj),
            "descriptions": self.get_descriptions(obj),
        }

        # Remove empty optional fields
        return {k: v for k, v in feature.items() if (v is not None or k == "geometry")}

    def get_names(self, obj):
        """Build LPF names array from chrononyms"""
        names = []

        # Add canonical chrononym as preferred name
        if obj.chrononym:
            names.append({
                "toponym": obj.chrononym,
                "lang": obj.languageTag if obj.languageTag else None
            })

        # Add variant chrononyms
        for chrononym in obj.chrononyms.all():
            # Skip if it's the same as canonical
            if chrononym.label != obj.chrononym:
                name_obj = {
                    "toponym": chrononym.label,
                    "lang": chrononym.languageTag if chrononym.languageTag else None
                }
                names.append(name_obj)

        # Remove None values from lang fields
        for name in names:
            if name.get("lang") is None:
                del name["lang"]

        return names if names else [{"toponym": "Untitled Period"}]

    def get_geometry(self, obj):
        """Get geometry from computed bbox or spatial coverage, merged as a single MultiPolygon"""

        # Gather geometries
        spatial_entities = obj.spatialCoverage.filter(geometry__isnull=False)
        polygons = []

        for entity in spatial_entities:
            if entity.geometry:
                geom = entity.geometry.geojson
                if isinstance(geom, str):
                    geom = json.loads(geom)
                shapely_geom = shape(geom)

                # Extract polygons recursively
                def extract_polygons(g):
                    if isinstance(g, Polygon):
                        polygons.append(g)
                    elif isinstance(g, MultiPolygon):
                        for p in g.geoms:
                            polygons.append(p)
                    elif isinstance(g, GeometryCollection):
                        for sub in g.geoms:
                            extract_polygons(sub)

                extract_polygons(shapely_geom)

        if not polygons:
            return None

        # Merge all polygons into a single MultiPolygon
        merged = unary_union(polygons)

        # Ensure result is Polygon or MultiPolygon
        if isinstance(merged, Polygon):
            merged = MultiPolygon([merged])
        elif isinstance(merged, GeometryCollection):
            # Flatten any leftover collections
            flat_polygons = []
            for g in merged.geoms:
                if isinstance(g, Polygon):
                    flat_polygons.append(g)
                elif isinstance(g, MultiPolygon):
                    flat_polygons.extend(list(g.geoms))
            merged = MultiPolygon(flat_polygons)

        return mapping(merged)

    def get_when(self, obj):
        """Build LPF when object from temporal bounds"""
        bounds = obj.bounds.all()
        start_bound = bounds.filter(kind='start').first()
        stop_bound = bounds.filter(kind='stop').first()

        if not (start_bound or stop_bound):
            return None

        when_obj = {
            "timespans": []
        }

        # Create timespan from bounds
        timespan = {}

        if start_bound:
            start_obj = {}
            if start_bound.earliestYear == start_bound.latestYear:
                start_obj["in"] = str(start_bound.earliestYear)
            else:
                if start_bound.earliestYear:
                    start_obj["earliest"] = str(start_bound.earliestYear)
                if start_bound.latestYear:
                    start_obj["latest"] = str(start_bound.latestYear)

            if start_obj:
                timespan["start"] = start_obj

        if stop_bound:
            end_obj = {}
            if stop_bound.earliestYear == stop_bound.latestYear:
                end_obj["in"] = str(stop_bound.earliestYear)
            else:
                if stop_bound.earliestYear:
                    end_obj["earliest"] = str(stop_bound.earliestYear)
                if stop_bound.latestYear:
                    end_obj["latest"] = str(stop_bound.latestYear)

            if end_obj:
                timespan["end"] = end_obj

        if timespan:
            when_obj["timespans"].append(timespan)

            # Add label if available
            if start_bound and start_bound.label:
                when_obj["label"] = start_bound.label
            elif stop_bound and stop_bound.label:
                when_obj["label"] = stop_bound.label

            return when_obj

        return None

    def get_links(self, obj):
        """Build LPF links array"""
        links = []

        # Add PeriodO reference if available
        if obj.sameAs:
            links.append({
                "type": "exactMatch",
                "identifier": obj.sameAs
            })

        # Add broader/narrower relationships
        if obj.broader:
            links.append({
                "type": "seeAlso",
                "identifier": f"https://whgazetteer.org/period/{obj.broader}"
            })

        return links if links else None

    def get_descriptions(self, obj):
        """Build LPF descriptions array"""
        descriptions = []

        if obj.editorialNote:
            descriptions.append({
                "value": obj.editorialNote
            })

        if obj.note:
            descriptions.append({
                "value": obj.note
            })

        return descriptions if descriptions else None

    def get_country_codes(self, obj):
        return obj.ccodes

    class Meta:
        model = Period
        fields = []  # All data comes from to_representation


class PeriodPreviewSerializer(serializers.ModelSerializer):
    """
    Serializer for Period preview (lighter version for HTML preview)
    """
    authority_label = serializers.SerializerMethodField()
    chrononyms_preview = serializers.SerializerMethodField()
    spatial_preview = serializers.SerializerMethodField()
    start = serializers.SerializerMethodField()
    stop = serializers.SerializerMethodField()

    class Meta:
        model = Period
        fields = [
            'id', 'chrononym', 'languageTag', 'editorialNote', 'note',
            'authority_label', 'chrononyms_preview', 'spatial_preview', 'start', 'stop', 'ccodes'
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
                result[kind] = self.format_year(b.earliestYear)
            else:
                result[kind] = f"{self.format_year(b.earliestYear)} / {self.format_year(b.latestYear)}"

        return result

    def get_start(self, obj):
        return self.get_date_range(obj).get("start")

    def get_stop(self, obj):
        return self.get_date_range(obj).get("stop")


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
