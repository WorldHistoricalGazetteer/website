# /api/reconcile_helpers.py

import json
import logging
import os
import urllib
from datetime import datetime
from pathlib import Path

from drf_spectacular.utils import extend_schema_serializer
from rest_framework import serializers

from api.serializers_api import OptimizedPlaceSerializer, OptimizedPeriodSerializer, PeriodFeatureSerializer
from areas.models import Area
from main.choices import FEATURE_CLASSES
from whg import settings

logger = logging.getLogger('reconciliation')

ELASTIC_INDICES = "whg,pub,wdgn"  # or options from "whg,pub,wdgn"

# TODO: Replace ElasticSearch with Vespa backend when ready
es = settings.ES_CONN

ALLOWED_TYPES = {"place", "period"}

# Property ID to required serializer fields mapping
PROPERTY_FIELD_MAP = {
    # Place properties
    "whg:id_short": ["id"],
    "whg:id_object": ["id", "title"],
    "whg:names_canonical": ["title"],
    "whg:names_array": ["names", "title"],
    "whg:names_summary": ["names", "title"],
    "whg:geometry_wkt": ["geoms"],
    "whg:geometry_geojson": ["geoms"],
    "whg:geometry_centroid": ["geoms"],
    "whg:geometry_bbox": ["geoms"],
    "whg:temporal_objects": ["whens"],
    "whg:temporal_years": ["whens"],
    "whg:countries_codes": ["ccodes"],
    "whg:countries_objects": ["ccodes"],
    "whg:classes_codes": ["fclasses"],
    "whg:classes_objects": ["fclasses"],
    "whg:types_objects": ["types"],
    "whg:dataset": ["dataset", "dataset_id"],
    "whg:lpf_feature": ["id", "title", "names", "geoms", "extent", "whens", "types", "ccodes", "fclasses", "dataset",
                        "dataset_id", "links", "related", "descriptions", "depictions"],

    # Period properties
    "whg:chrononym_canonical": ["canonical_label"],
    "whg:chrononym_variants_array": ["chrononyms"],
    "whg:chrononym_variants_summary": ["chrononyms"],
    "whg:period_notes_editorial": ["editorialNote"],
    "whg:period_authority_object": ["authority"],
    "whg:periodo_identifier": ["id"],
    "whg:spatial_coverage_geometry": ["spatial_coverage"],
    "whg:spatial_coverage_objects": ["spatial_coverage"],
    "whg:temporal_bounds_objects": ["temporal_bounds"],
    "whg:temporal_bounds_years": ["temporal_bounds"],
    "whg:lpf_period_feature": ["all"],
}

FCLASS_MAP = {
    code: {
        "code": code,
        "label": label,
        "reference": "https://www.geonames.org/source-code/javadoc/org/geonames/FeatureClass.html#{}".format(code)
    }
    for code, label in FEATURE_CLASSES
}

with open(Path("media/data/regions_countries.json"), "r", encoding="utf-8") as f:
    COUNTRY_LABELS = {}
    for section in json.load(f):
        for item in section.get("children", []):
            if "ccodes" in item:
                # region with multiple codes
                for c in item["ccodes"]:
                    COUNTRY_LABELS[c] = item["text"]
            else:
                # single country
                COUNTRY_LABELS[item["id"]] = item["text"]


def get_canonical_name(src, fallback_id):
    if src.get("title"):
        return src["title"]
    elif src.get("names"):
        return src["names"][0]["toponym"]
    elif src.get("searchy"):
        return src["searchy"][0]
    else:
        return f"Unknown ({fallback_id})"


def get_alternative_names(src, canonical_name):
    alt_names = []
    if src.get("names"):
        alt_names = [n["toponym"] for n in src["names"] if n.get("toponym") and n["toponym"] != canonical_name]
    alt_names += [s for s in src.get("searchy", []) if s not in alt_names and s != canonical_name]
    return alt_names


def normalize_score(raw_score, max_score):
    return int((raw_score / max_score) * 100) if max_score else 0


def geoms_to_geojson(src):
    place_id = str(src.get("place_id") or "unknown")
    name = get_canonical_name(src, place_id)
    features = []
    for geom in src.get("geoms", []):
        geometry = geom.get("location", geom)
        features.append({
            "type": "Feature",
            "geometry": geometry,
            "properties": {
                "id": place_id,
                "name": name,
            }
        })
    return {"type": "FeatureCollection", "features": features} if features else None


def make_candidate(hit, query_text, max_score, schema_space):
    src = hit["_source"]
    name = get_canonical_name(src, hit["_id"])
    alt_names = get_alternative_names(src, name)
    score = normalize_score(hit["_score"], max_score)
    is_exact = name.lower() == query_text.lower()
    ccodes = src.get("ccodes", [])
    return {
        "id": "place:" + str(src.get("place_id")),  # or hit.get("whg_id") or hit["_id"]),
        "name": name,
        "score": score,
        "match": is_exact,
        "alt_names": alt_names,
        "description": f"Country: {', '.join(ccodes)}",
        "type": [
            {
                "id": schema_space + "#Place",
                "name": "Place"
            }
        ]
    }


def build_es_query(params, size=100):
    qstr = params.get("qstr")
    fields = ["title^3", "names.toponym", "searchy"]

    # Search mode handling (default, starts, in, fuzzy)
    search_mode = params.get("mode", "fuzzy")

    # Handle "prefix|fuzziness" mode
    if "|" in search_mode:
        mode_parts = search_mode.split("|")
        if len(mode_parts) != 2:
            raise ValueError(f"Invalid fuzzy mode: {search_mode}. Expected format 'prefix|fuzziness'.")
        prefix_length, fuzziness = mode_parts
        if prefix_length.isdigit() and (
                fuzziness == "AUTO" or (fuzziness.isdigit() and int(fuzziness) >= 0 and int(fuzziness) <= 2)):
            search_query = {
                "multi_match": {
                    "query": qstr,
                    "fields": fields,
                    "type": "best_fields",
                    "fuzziness": fuzziness if fuzziness == "AUTO" else int(fuzziness),
                    "prefix_length": int(prefix_length)
                }
            }
        else:
            raise ValueError(f"Invalid fuzzy mode: {search_mode}")
    elif search_mode == "starts":
        search_query = {"bool": {"should": [{"prefix": {field: qstr}} for field in fields]}}
    elif search_mode == "in":
        search_query = {"bool": {"should": [{"wildcard": {field: f"*{qstr}*"}} for field in fields]}}
    elif search_mode == "fuzzy":
        search_query = {
            "multi_match": {
                "query": qstr,
                "fields": fields,
                "type": "best_fields",
                "fuzziness": "AUTO",
                "prefix_length": 2
            }
        }
    else:  # "exact" or any other
        search_query = {"multi_match": {"query": qstr, "fields": fields}}

    q = {
        "size": size,
        "query": {
            "bool": {
                "must": [
                    {"exists": {"field": "whg_id"}},
                    search_query
                ]
            }
        }
    }

    # fclasses
    fclasses = params.get("fclasses")
    if fclasses:
        if isinstance(fclasses, str):
            fclasses = fclasses.split(",")
        fclasses.append("X")
        q["query"]["bool"]["must"].append({"terms": {"fclasses": fclasses}})

    # temporal
    if params.get("temporal"):
        current_year = datetime.now().year
        start_year = str(params.get("start")) if params.get("start") else None
        end_year = str(params.get("end", current_year))
        if start_year:
            timespan_filter = {"range": {"timespans": {"gte": start_year, "lte": end_year}}}
            if params.get("undated"):
                q["query"]["bool"]["must"].append({
                    "bool": {"should": [timespan_filter, {"bool": {"must_not": {"exists": {"field": "timespans"}}}}]}
                })
            else:
                q["query"]["bool"]["must"].append(timespan_filter)

    # countries
    countries = params.get("countries")
    if countries:
        if isinstance(countries, str):
            countries = json.loads(countries)
        q["query"]["bool"]["must"].append({"terms": {"ccodes": countries}})

    # spatial filters (bounds + userareas)
    geometry_filters = []

    bounds = params.get("bounds")
    if bounds:
        if isinstance(bounds, str):
            bounds = json.loads(bounds)
        if bounds.get("geometries"):
            for geometry in bounds["geometries"]:
                geometry_filters.append({
                    "geo_shape": {
                        "geoms.location": {
                            "shape": {
                                "type": geometry["type"],
                                "coordinates": geometry["coordinates"]
                            },
                            "relation": "intersects"
                        }
                    }
                })

    userareas = params.get("userareas")
    if userareas:
        for userarea_id in userareas:
            user_area = Area.objects.filter(id=userarea_id).values("geojson").first()
            if user_area:
                geometry_filters.append({
                    "geo_shape": {
                        "geoms.location": {
                            "shape": user_area["geojson"],
                            "relation": "intersects"
                        }
                    }
                })

    if geometry_filters:
        q["query"]["bool"]["must"].append({"bool": {"should": geometry_filters, "minimum_should_match": 1}})

    # handle unlocated (default: true)
    unlocated = params.get("unlocated")
    if unlocated in [False, "false", "False", "0"]:  # explicitly false
        q["query"]["bool"]["must"].append({"exists": {"field": "geoms.location"}})

    return q


def es_search(index=ELASTIC_INDICES, query=None, ids=None):
    """
    Execute an Elasticsearch search.

    query: dict from normalise_query_params
    ids: optional list of document IDs to fetch directly
    """

    if ids:
        body = {
            "query": {
                "terms": {
                    "place_id": ids
                }
            },
            "_source": True,
            "size": len(ids),
        }
    elif query:
        params = dict(query["raw"])  # shallow copy
        params["qstr"] = query["query_text"]
        body = build_es_query(params, size=query["size"])
    else:
        return []

    resp = es.search(index=index, body=body)
    return resp.get("hits", {}).get("hits", [])


def get_required_fields(properties):
    """Get required serializer fields for any entity type."""
    required_fields = set()
    for prop in properties:
        pid = prop.get("id") if isinstance(prop, dict) else prop
        fields = PROPERTY_FIELD_MAP.get(pid, [])
        required_fields.update(fields)
    return list(required_fields)


def format_extend_row(entity, properties, request=None):
    """
    Build the property values dict for an OpenRefine extend row.
    Handles both Place and Period entities with type-specific logic.
    """
    # Determine entity type
    entity_type = "period" if hasattr(entity, 'chrononym') else "place"

    # Check if we need full LPF serialization for periods
    needs_lpf_period = any(
        (prop.get("id") if isinstance(prop, dict) else prop) == "whg:lpf_period_feature"
        for prop in properties
    )

    if entity_type == "period" and needs_lpf_period:
        # Use full LPF serializer for period
        lpf_serializer = PeriodFeatureSerializer(entity, context={"request": request})
        lpf_data = lpf_serializer.data

        # Also get regular serialized data for other properties
        required_fields = get_required_fields(properties)
        regular_serializer = OptimizedPeriodSerializer(
            entity, context={"request": request}, fields=required_fields
        )
        regular_data = regular_serializer.data

        # Build row with both LPF and regular extractors
        extractors = get_period_extractors()
        row = {}

        for prop in properties:
            pid = prop.get("id") if isinstance(prop, dict) else prop

            if pid == "whg:lpf_period_feature":
                row[pid] = wrap_value(lpf_data)
            else:
                extractor = extractors.get(pid)
                if extractor:
                    try:
                        value = extractor(regular_data, entity)
                        row[pid] = wrap_value(value)
                    except Exception as e:
                        logger.warning(f"Error extracting {pid} for period {entity.id}: {e}")
                        row[pid] = []
                else:
                    row[pid] = []

        return row

    # Get appropriate serializer and field mapping
    required_fields = get_required_fields(properties)
    if entity_type == "place":
        serializer = OptimizedPlaceSerializer(
            entity, context={"request": request}, fields=required_fields
        )
        extractors = get_place_extractors()
    else:  # period
        serializer = OptimizedPeriodSerializer(
            entity, context={"request": request}, fields=required_fields
        )
        extractors = get_period_extractors()

    data = serializer.data
    row = {}

    # Process each property
    for prop in properties:
        pid = prop.get("id") if isinstance(prop, dict) else prop
        extractor = extractors.get(pid)

        if extractor:
            try:
                value = extractor(data, entity)
                row[pid] = wrap_value(value)
            except Exception as e:
                logger.warning(f"Error extracting {pid} for {entity_type} {entity.id}: {e}")
                row[pid] = []
        else:
            row[pid] = []

    return row


def get_place_extractors():
    """Get property extractors for place entities."""

    def prepend_if_missing(names_list, title):
        if title and not any(n.get("toponym") == title for n in names_list):
            return [{"toponym": title, "jsonb": {"status": "preferred"}}] + names_list
        return names_list

    def geom_wkt_list(data):
        return [g.get("geowkt") for g in data.get("geoms", []) if g.get("geowkt")]

    def geom_geojson_list(data):
        return [g.get("geojson") for g in data.get("geoms", []) if g.get("geojson")]

    def geom_centroid_list(data):
        return [f"{g['centroid'][1]}, {g['centroid'][0]}" for g in data.get("geoms", []) if g.get("centroid")]

    def geom_bbox_list(data):
        return [", ".join(map(str, g["bbox"])) for g in data.get("geoms", []) if g.get("bbox")]

    def temporal_objects(data):
        timespans_list = []
        for when in data.get("when", []):
            for ts in when.get("timespans", []):
                timespan = {}
                start = ts.get("start", {})
                end = ts.get("end", {})
                if start:
                    timespan["begin"] = start.get("earliest") or start.get("latest")
                if end:
                    timespan["end"] = end.get("latest") or end.get("earliest")
                if ts.get("circa"):
                    timespan["circa"] = ts["circa"]
                if ts.get("note"):
                    timespan["note"] = ts["note"]
                if timespan:
                    timespans_list.append(timespan)
        return timespans_list

    def temporal_years(data):
        ranges = []
        for when in data.get("when", []):
            for ts in when.get("timespans", []):
                start = ts.get("start", {}).get("earliest")
                end = ts.get("end", {}).get("latest")
                if start and end:
                    ranges.append(f"{start}-{end}")
        return ranges

    return {
        # Names
        "whg:names_canonical": lambda data, entity: data.get("title"),
        "whg:names_array": lambda data, entity: prepend_if_missing(data.get("names", []), data.get("title")),
        "whg:names_summary": lambda data, entity: [n["toponym"] for n in
                                                   prepend_if_missing(data.get("names", []), data.get("title"))],

        # Identifiers
        "whg:id_short": lambda data, entity: f"https://whgazetteer.org/place/{entity.id}",
        "whg:id_object": lambda data, entity: {"id": f"https://whgazetteer.org/place/{entity.id}",
                                               "label": data.get("title", "")},

        # Geometry
        "whg:geometry_wkt": lambda data, entity: geom_wkt_list(data),
        "whg:geometry_geojson": lambda data, entity: geom_geojson_list(data),
        "whg:geometry_centroid": lambda data, entity: geom_centroid_list(data),
        "whg:geometry_bbox": lambda data, entity: geom_bbox_list(data),

        # Temporal
        "whg:temporal_objects": lambda data, entity: temporal_objects(data),
        "whg:temporal_years": lambda data, entity: temporal_years(data),

        # Countries
        "whg:countries_codes": lambda data, entity: data.get("ccodes", []),
        "whg:countries_objects": lambda data, entity: [
            {"code": code, "label": COUNTRY_LABELS.get(code, code)}
            for code in data.get("ccodes", [])
        ],

        # Feature classes
        "whg:classes_codes": lambda data, entity: data.get("fclasses", []),
        "whg:classes_objects": lambda data, entity: [
            FCLASS_MAP.get(fc, {"code": fc, "label": "Unknown", "reference": ""})
            for fc in data.get("fclasses", [])
        ],

        # Types
        "whg:types_objects": lambda data, entity: data.get("types", []),

        # Dataset
        "whg:dataset": lambda data, entity: {"name": data.get("dataset"), "id": data.get("dataset_id")} if data.get(
            "dataset") else None,

        # LPF feature
        "whg:lpf_feature": lambda data, entity: build_lpf_feature(entity, data),
    }


def get_period_extractors():
    """Get property extractors for period entities."""

    def format_year_range(temporal_bound):
        earliest = temporal_bound.get("earliestYear")
        latest = temporal_bound.get("latestYear")

        if earliest == latest and earliest is not None:
            return str(earliest)
        elif earliest and latest:
            return f"{earliest} / {latest}"
        elif earliest:
            return f"From {earliest}"
        elif latest:
            return f"Until {latest}"
        return ""

    return {
        "whg:chrononym_canonical": lambda data, entity: data.get("canonical_label"),
        "whg:chrononym_variants_array": lambda data, entity: data.get("chrononyms", []),
        "whg:chrononym_variants_summary": lambda data, entity: [c.get("label") for c in data.get("chrononyms", [])],
        "whg:period_notes_editorial": lambda data, entity: data.get("editorialNote"),
        "whg:period_authority_object": lambda data, entity: data.get("authority"),
        "whg:periodo_identifier": lambda data, entity: data.get("id"),
        "whg:spatial_coverage_geometry": lambda data, entity: [
            sc.get("geometry") for sc in data.get("spatial_coverage", [])
            if sc.get("geometry")
        ],
        "whg:spatial_coverage_objects": lambda data, entity: data.get("spatial_coverage", []),
        "whg:temporal_bounds_objects": lambda data, entity: data.get("temporal_bounds", {}),
        "whg:temporal_bounds_years": lambda data, entity: [
            format_year_range(tb) for tb in data.get("temporal_bounds", {}).values()
            if tb.get("earliestYear") or tb.get("latestYear")
        ],
    }


def wrap_value(value):
    """
    Wrap values in OpenRefine's expected format.
    Handles strings, lists, objects, and None values.
    """
    if value is None:
        return []

    if isinstance(value, str):
        return [{"str": value}]

    if isinstance(value, list):
        # Handle list of strings vs list of objects differently
        if value and isinstance(value[0], str):
            return [{"str": item} for item in value]
        else:
            return [{"str": json.dumps(value)}] if value else []

    if isinstance(value, dict):
        return [{"str": json.dumps(value)}]

    return [{"str": str(value)}]


def build_lpf_feature(place, serialized_data):
    """
    Build a complete Linked Places Format GeoJSON Feature
    Based on LPF v1.1 specification and existing WHG serializer patterns
    """
    lpf_feature = {
        "@context": "https://raw.githubusercontent.com/LinkedPasts/linked-places/master/linkedplaces-context-v1.1.jsonld",
        "type": "Feature",
        "properties": {
            "id": str(place.id),
            "title": serialized_data.get("title", ""),
            "ccodes": serialized_data.get("ccodes", [])
        }
    }

    # Add geometry - handle multiple geometries as GeometryCollection
    geoms = serialized_data.get("geoms", [])
    if geoms:
        if len(geoms) == 1:
            # Single geometry
            geom_data = geoms[0]
            if geom_data.get("geojson"):
                lpf_feature["geometry"] = geom_data["geojson"]
        else:
            # Multiple geometries - create GeometryCollection
            geometries = []
            for geom in geoms:
                if geom.get("geojson"):
                    geometries.append(geom["geojson"])
            if geometries:
                lpf_feature["geometry"] = {
                    "type": "GeometryCollection",
                    "geometries": geometries
                }

    # Add names array - LPF format
    names = serialized_data.get("names", [])
    if names:
        lpf_names = []
        for name in names:
            lpf_name = {"toponym": name.get("toponym", "")}

            # Add language info if available
            if name.get("lang"):
                lpf_name["lang"] = name["lang"]

            # Add citations/attestations if available
            if name.get("jsonb", {}).get("citation"):
                lpf_name["citation"] = name["jsonb"]["citation"]

            # Add when info if available
            if name.get("jsonb", {}).get("when"):
                lpf_name["when"] = name["jsonb"]["when"]

            lpf_names.append(lpf_name)

        lpf_feature["properties"]["names"] = lpf_names

    # Add types array - LPF format
    types = serialized_data.get("types", [])
    if types:
        lpf_types = []
        for ptype in types:
            lpf_type = {
                "identifier": ptype.get("identifier", ""),
                "label": ptype.get("label", "")
            }

            # Add source label if different from label
            if ptype.get("src_label") and ptype["src_label"] != ptype.get("label"):
                lpf_type["sourceLabel"] = ptype["src_label"]

            # Add AAT ID if available
            if ptype.get("aat_id"):
                lpf_type["aat_id"] = ptype["aat_id"]

            lpf_types.append(lpf_type)

        lpf_feature["properties"]["types"] = lpf_types

    # Add when/temporal data - LPF format
    whens = serialized_data.get("whens", [])
    if whens:
        lpf_when = []
        for when in whens:
            when_obj = {}

            # Handle timespans
            timespans = when.get("timespans", [])
            if timespans:
                when_obj["timespans"] = []
                for ts in timespans:
                    timespan = {}
                    if ts.get("start"):
                        timespan["start"] = ts["start"]
                    if ts.get("end"):
                        timespan["end"] = ts["end"]
                    when_obj["timespans"].append(timespan)

            # Handle periods
            periods = when.get("periods", [])
            if periods:
                when_obj["periods"] = periods

            # Add label and duration if available
            if when.get("label"):
                when_obj["label"] = when["label"]
            if when.get("duration"):
                when_obj["duration"] = when["duration"]

            if when_obj:  # Only add if not empty
                lpf_when.append(when_obj)

        if lpf_when:
            lpf_feature["properties"]["when"] = lpf_when

    # Add links - LPF format
    links = serialized_data.get("links", [])
    if links:
        lpf_links = []
        for link in links:
            lpf_link = {
                "type": link.get("type", ""),
                "identifier": link.get("identifier", "")
            }

            # Add label if available
            if link.get("label"):
                lpf_link["label"] = link["label"]

            lpf_links.append(lpf_link)

        lpf_feature["properties"]["links"] = lpf_links

    # Add relations - LPF format
    related = serialized_data.get("related", [])
    if related:
        lpf_relations = []
        for rel in related:
            lpf_relation = {
                "relationType": rel.get("relation_type", ""),
                "relationTo": rel.get("relation_to", ""),
                "label": rel.get("label", "")
            }

            # Add when info if available
            if rel.get("when"):
                lpf_relation["when"] = rel["when"]

            lpf_relations.append(lpf_relation)

        lpf_feature["properties"]["relations"] = lpf_relations

    # Add descriptions - LPF format
    descriptions = serialized_data.get("descriptions", [])
    if descriptions:
        lpf_descriptions = []
        for desc in descriptions:
            lpf_desc = {
                "value": desc.get("value", "")
            }

            # Add identifier if available
            if desc.get("identifier"):
                lpf_desc["@id"] = desc["identifier"]

            # Add language if available
            if desc.get("lang"):
                lpf_desc["lang"] = desc["lang"]

            lpf_descriptions.append(lpf_desc)

        lpf_feature["properties"]["descriptions"] = lpf_descriptions

    # Add depictions - LPF format
    depictions = serialized_data.get("depictions", [])
    if depictions:
        lpf_depictions = []
        for dep in depictions:
            lpf_depiction = {
                "@id": dep.get("identifier", ""),
                "title": dep.get("title", ""),
                "license": dep.get("license", "")
            }
            lpf_depictions.append(lpf_depiction)

        lpf_feature["properties"]["depictions"] = lpf_depictions

    # Add dataset information
    if serialized_data.get("dataset"):
        lpf_feature["properties"]["dataset"] = {
            "id": serialized_data.get("dataset_id"),
            "label": serialized_data.get("dataset")
        }

    # Add extent if available (not standard LPF but useful)
    if serialized_data.get("extent"):
        lpf_feature["properties"]["extent"] = serialized_data["extent"]

    return lpf_feature


@extend_schema_serializer(component_name=None)
class ReconciliationRequestSerializer(serializers.Serializer):
    queries = serializers.DictField(
        required=False,
        child=serializers.DictField()
    )
    extend = serializers.DictField(
        required=False,
    )


def parse_schema(schema_file):
    """
    Parses a WHG schema from a local file and constructs the PROPOSE_PROPERTIES list and VALID_FCLASSES.
    """
    import json

    if not os.path.exists(schema_file):
        logger.error("Schema file not found: %s", schema_file)
        return [], []

    try:
        with open(schema_file, encoding="utf-8") as f:
            schema = json.load(f)
    except Exception as e:
        logger.error("Error loading schema JSON: %s", e)
        return [], []

    def valid_domain(domains):
        """Return 'Place' or 'Period' if present in rdfs:domain, else None."""
        if isinstance(domains, str):
            domains = [domains]
        for d in domains or []:
            if d.endswith("Place"):
                return "Place"
            if d.endswith("Period"):
                return "Period"
        return None

    propose_properties, valid_fclasses = [], []

    # Iterate through the @graph array
    for item in schema.get('@graph', []):
        if item.get("@type") == "rdf:Property":
            domain = valid_domain(item.get("rdfs:domain"))
            if not domain:
                continue

            api_views = item.get("whg:apiView", [])
            if isinstance(api_views, dict):
                api_views = [api_views]

            for view in api_views:
                if isinstance(view, dict) and {"id", "name", "description"} <= view.keys():
                    propose_properties.append({
                        "id": view["id"],
                        "name": f"{domain}: {view['name']}",
                        "description": view["description"],
                        "type": "string"
                    })

        if item.get('@id') == 'whg:classes':
            valid_fclasses = [val.get('code') for val in item.get('whg:allowedValues', [])]

    # Add special properties
    propose_properties.extend([
        {
            "id": "whg:lpf_feature",
            "name": "Place: LPF Feature (object)",
            "description": "Complete place record as a Linked Places Format GeoJSON Feature, including full properties, names, geometry, and links",
            "type": "string"
        },
        {
            "id": "whg:lpf_period_feature",
            "name": "Period: LPF Feature (object)",
            "description": "Complete period record as a Linked Places Format GeoJSON Feature, including chrononyms, temporal bounds, and spatial coverage",
            "type": "string"
        }
    ])

    return propose_properties, valid_fclasses


def extract_entity_type(source, from_queries=False):
    """
    Extract and validate entity type + ids from either:
      - extend ids: ["place:123", "place:456"]
      - queries: {"q1": {"type": "...#Place"}, ...}

    Returns (entity_type, ids) where ids may be [] for queries.
    """
    if from_queries:
        types = {
            urllib.parse.unquote(q["type"]).split("#")[-1].lower()
            for q in source.values()
            if isinstance(q.get("type"), str)
        }
        if not types:
            return None, None
        if not types.issubset(ALLOWED_TYPES):
            raise ValueError(f"Unsupported entity type(s): {', '.join(sorted(types))}")
        if len(types) > 1:
            raise ValueError("All queries in a batch must be for the same entity type")
        return types.pop(), []

    else:
        parsed = []
        obj_types = set()
        for full_id in source:
            try:
                obj_type, raw_id = full_id.split(":", 1)
            except ValueError:
                raise ValueError(f"Invalid id format: {full_id}. Expected format 'type:id'")
            if obj_type not in ALLOWED_TYPES:
                raise ValueError(
                    f"Unsupported type in id: {full_id}. "
                    f"Supported types are {', '.join(sorted(ALLOWED_TYPES))}"
                )
            parsed.append(raw_id)
            obj_types.add(obj_type)

        if len(obj_types) > 1:
            raise ValueError("All ids must be of the same type")

        return obj_types.pop(), parsed


def create_type_guessing_dummies(SERVICE_METADATA):
    """
    Returns a dictionary of high-score, dummy candidates for all default types
    defined in SERVICE_METADATA.
    """

    # List to hold candidates for all types
    all_candidates = []

    # Iterate over the defaultTypes list from the SERVICE_METADATA constant
    for type_obj in SERVICE_METADATA.get("defaultTypes", []):
        type_id = type_obj.get("id")
        type_name = type_obj.get("name")
        type_slug = type_name.lower()

        if type_id and type_name:
            candidate_type_list = [{"id": type_id, "name": type_name}]

            candidate = {
                # Prepend 'dummy:' to avoid collision with real IDs
                "id": f"dummy:{type_slug}_1",
                "name": f"Guessing Result: {type_name}",
                "score": 100,
                "match": True,
                "type": candidate_type_list
            }
            all_candidates.append(candidate)

    return all_candidates
