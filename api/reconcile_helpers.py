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

# The pseudo-namespace for legacy WHG places (numeric IDs stored in local ES/DB).
WHG_NAMESPACE = "whg"


def get_namespace(place_id: str) -> str:
    """
    Extract the namespace from a place identifier.

    - WHG places (e.g. ``"whg:1234:19799"``, ``"whg:90687"``) → ``"whg"``
    - Namespaced CRC IDs (e.g. ``"gn:745044"``) → ``"gn"``
    - Bare numeric IDs (e.g. ``"12345"``) → ``"whg"`` (the pre-namespacing form,
      still accepted on input)
    """
    place_id = str(place_id)
    if place_id.isdigit():
        return WHG_NAMESPACE
    if ":" in place_id:
        return place_id.split(":", 1)[0].lower()
    return WHG_NAMESPACE


# ── WHG place identifiers ────────────────────────────────────────────────────
# Every place this service returns carries its gazetteer namespace, so an id says
# where it came from without a lookup: ``gn:745044``, ``wd:Q90`` from the gateway,
# and ``whg:<dataset_id>:<src_id>`` for a WHG-contributed place — the dataset that
# contributed it, then that dataset's OWN id for the record. The dataset leaf
# matches the ``whg:<dataset_id>`` form the indexing and attestation APIs already
# use for datasets (see api/views_indexing.py).
#
# `src_id` is the contributor's identifier and survives re-ingestion, which the
# Postgres primary key does not; where a dataset row or src_id is missing we fall
# back to the two-part ``whg:<place_pk>``. Both WHG forms — and the bare numeric
# ids emitted before namespacing — resolve through resolve_legacy_place_pk(), so
# identifiers already held by API consumers keep working.

_DS_LABEL_PK_CACHE_KEY = 'recon:dataset_label_pk_map:v1'


def dataset_pk_for_label(label) -> int | None:
    """Primary key of the dataset with this label, or None.

    The ES documents carry the dataset LABEL; the identifier carries the numeric
    id (stable across a rename). Cached for 10 minutes — the map only changes
    when a dataset is created or renamed, and this runs once per candidate.
    """
    if not label:
        return None
    from django.core.cache import cache
    from datasets.models import Dataset

    mapping = cache.get(_DS_LABEL_PK_CACHE_KEY)
    if mapping is None:
        mapping = {lbl: pk for pk, lbl in Dataset.objects.values_list('id', 'label') if lbl}
        cache.set(_DS_LABEL_PK_CACHE_KEY, mapping, 600)
    pk = mapping.get(str(label))
    if pk is None:
        # A dataset created since the map was cached: check once, and refresh.
        pk = Dataset.objects.filter(label=str(label)).values_list('id', flat=True).first()
        if pk is not None:
            cache.delete(_DS_LABEL_PK_CACHE_KEY)
    return pk


def whg_place_id(src: dict) -> str:
    """The namespaced identifier for one ES hit's ``_source``.

    Gateway hits arrive already namespaced and are passed through unchanged.
    """
    place_id = str(src.get("place_id") or "").strip()
    if not place_id.isdigit():
        return place_id                      # gn:745044, wd:Q90, … — already namespaced
    ds_pk = dataset_pk_for_label(src.get("dataset"))
    src_id = str(src.get("src_id") or "").strip()
    if ds_pk and src_id:
        return f"{WHG_NAMESPACE}:{ds_pk}:{src_id}"
    return f"{WHG_NAMESPACE}:{place_id}"     # dataset unresolved — namespaced by pk


def resolve_legacy_place_pks(raw_ids) -> dict:
    """Map WHG place ids to Postgres ``Place`` pks, in any form this service has
    emitted: ``whg:<dataset_id>:<src_id>``, ``whg:<place_pk>``, or a bare numeric
    ``<place_pk>``. Ids that resolve to nothing — and gateway ids (``gn:…``) — are
    absent from the result. The indexing side disambiguates a duplicate src_id by
    appending the place key (``whg:20:20155:91040``); such an id resolves to exactly
    that place, while the bare ``whg:20:20155`` resolves to the lowest-pk sibling.

    Batched by dataset: one query per dataset named, not one per id. ``Place`` is
    keyed on the dataset LABEL (``to_field='label'``), so the dataset leaf is
    matched by traversing the relation rather than by the raw column.
    """
    from places.models import Place

    # {dataset_pk: {src_id: [(raw_id, exact_place_pk_or_None), …]}} — a src_id can be
    # wanted by more than one raw id (the bare form and a disambiguated one), so this
    # has to be a multimap, not a plain dict.
    resolved, by_dataset = {}, {}
    for raw_id in raw_ids:
        raw = str(raw_id or "").strip()
        if raw.isdigit():
            resolved[raw_id] = int(raw)
            continue
        parts = raw.split(":")
        if len(parts) < 2 or parts[0].lower() != WHG_NAMESPACE or not parts[1].isdigit():
            continue
        if len(parts) == 2:
            resolved[raw_id] = int(parts[1])
            continue
        ds = by_dataset.setdefault(int(parts[1]), {})
        # src_id may itself contain colons — only the dataset leaf is delimited.
        ds.setdefault(":".join(parts[2:]), []).append((raw_id, None))
        # The indexing side disambiguates a duplicate src_id by appending the place
        # key (`whg:20:20155:91040`). Register that reading too; the literal src_id
        # above is tried first, so a src_id that genuinely ends in `:<digits>` wins.
        if len(parts) >= 4 and parts[-1].isdigit():
            ds.setdefault(":".join(parts[2:-1]), []).append((raw_id, int(parts[-1])))

    for ds_pk, wanted in by_dataset.items():
        pks_by_src = {}
        for pk, src_id in (Place.objects
                           .filter(dataset__id=ds_pk, src_id__in=list(wanted))
                           .order_by('src_id', 'id')
                           .values_list('id', 'src_id')):
            pks_by_src.setdefault(src_id, []).append(pk)

        # Pass 1 — the id names a src_id and nothing more. src_id is the
        # contributor's identifier and is NOT unique within a dataset (4,298 legacy
        # places across 11 datasets share one with a sibling, 0.16% of the table),
        # so resolve to the LOWEST pk: an ambiguous id then always dereferences to
        # the same place rather than varying with row order. See place#172.
        for src_id, entries in wanted.items():
            for raw_id, exact_pk in entries:
                if exact_pk is None and raw_id not in resolved and pks_by_src.get(src_id):
                    resolved[raw_id] = pks_by_src[src_id][0]
        # Pass 2 — a trailing place key names WHICH sibling is meant.
        for src_id, entries in wanted.items():
            for raw_id, exact_pk in entries:
                if exact_pk is not None and raw_id not in resolved \
                        and exact_pk in pks_by_src.get(src_id, ()):
                    resolved[raw_id] = exact_pk
    return resolved


def resolve_legacy_place_pk(raw_id) -> int | None:
    """Postgres ``Place`` pk for a single WHG place id — see resolve_legacy_place_pks()."""
    return resolve_legacy_place_pks([raw_id]).get(raw_id)


def parse_namespaces(namespaces_param) -> set[str] | None:
    """
    Parse a comma-delimited namespace string into a set of lowercase codes.

    Returns ``None`` when the parameter is absent/empty, meaning *all*
    namespaces (no filtering).  Accepts a string (``"gn,tgn"``) or a
    list (``["gn", "tgn"]``).
    """
    if not namespaces_param:
        return None
    if isinstance(namespaces_param, (list, tuple)):
        codes = {ns.strip().lower() for ns in namespaces_param if isinstance(ns, str) and ns.strip()}
    else:
        codes = {ns.strip().lower() for ns in str(namespaces_param).split(",") if ns.strip()}
    return codes or None


def filter_hits_by_namespace(hits: list[dict], namespaces: set[str] | None) -> list[dict]:
    """
    Filter ES-style ``hits.hits[]`` dicts by namespace.

    If *namespaces* is ``None`` every hit passes through (no filtering).
    """
    if namespaces is None:
        return hits
    return [
        hit for hit in hits
        if get_namespace(str(hit.get("_source", {}).get("place_id", ""))) in namespaces
    ]


def parse_delimited_param(value, upper=False) -> list[str] | None:
    """
    Normalise a filter parameter that may arrive as a JSON list, a
    comma-delimited string, or ``None``.

    Returns a list of stripped strings (optionally upper-cased), or
    ``None`` when nothing was provided.
    """
    if value is None:
        return None
    if isinstance(value, str):
        items = [v.strip() for v in value.split(",") if v.strip()]
    elif isinstance(value, (list, tuple)):
        items = [str(v).strip() for v in value if str(v).strip()]
    else:
        return None
    if upper:
        items = [v.upper() for v in items]
    return items or None


def is_crc_place_id(raw_id: str) -> bool:
    """
    Return True if raw_id is a CRC-namespaced place identifier (e.g. ``gn:745044``).

    WHG's own places are ``whg:<dataset_id>:<src_id>`` (or, from before
    namespacing, a plain integer) and live in the local index/DB, not the gateway.
    """
    raw = str(raw_id or "")
    return not raw.isdigit() and not raw.lower().startswith(WHG_NAMESPACE + ":")

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


def wikipedia_links(links):
    """Extract Wikipedia article links from a place's ``links`` list.

    The indexing pipeline stores Wikidata sitelinks as ``{type: "seeAlso",
    identifier: "https://<lang>.wikipedia.org/wiki/<title>"}``. Return a compact
    ``[{lang, url}]`` list so the Workbench (and other clients) can offer Wikipedia
    enrichment for Wikidata-backed candidates. Empty when the place carries none.
    """
    out = []
    for link in links or []:
        ident = link.get("identifier", "") if isinstance(link, dict) else ""
        if ".wikipedia.org/wiki/" in ident:
            try:
                lang = ident.split("//", 1)[1].split(".wikipedia.org", 1)[0]
            except (IndexError, AttributeError):
                lang = ""
            out.append({"lang": lang, "url": ident})
    return out


def _first_lonlat(coords):
    """Descend nested GeoJSON coordinate arrays (Point/Line/Polygon/Multi*) to the first [lon, lat]."""
    while isinstance(coords, (list, tuple)) and coords and isinstance(coords[0], (list, tuple)):
        coords = coords[0]
    if isinstance(coords, (list, tuple)) and len(coords) >= 2 and all(isinstance(x, (int, float)) for x in coords[:2]):
        return [float(coords[0]), float(coords[1])]
    return None


def _geo_point_lonlat(v):
    """An Elasticsearch geo_point → [lon, lat]. Accepts [lon, lat] array, {lat, lon[/lng]} object, or
    the ES "lat,lon" string form."""
    if isinstance(v, dict):
        lat = v.get("lat")
        lon = v.get("lon", v.get("lng"))
        if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
            return [float(lon), float(lat)]
        return None
    if isinstance(v, str) and "," in v:
        try:
            lat, lon = (float(x) for x in v.split(",")[:2])   # geo_point string is "lat,lon"
            return [lon, lat]
        except ValueError:
            return None
    return _first_lonlat(v)  # [lon, lat] array


def repr_point(src):
    """The place's authoritative representative point as [lng, lat]. The index stores one per geometry
    (``geometries[].repr_point``, a geo_point); prefer it (and any centroid) over deriving a point from
    a polygon. Reads both the raw index field (``geometries``) and the reconcile-adapted (``geoms``)."""
    geom_lists = [src.get("geometries") or [], src.get("geoms") or []]
    # 1. Authoritative representative point / centroid.
    for geoms in geom_lists:
        for g in geoms:
            if not isinstance(g, dict):
                continue
            for key in ("repr_point", "centroid", "h3_centroid"):
                p = _geo_point_lonlat(g.get(key))
                if p:
                    return p
    # 2. Last resort: a vertex of whatever geometry is present.
    for geoms in geom_lists:
        for g in geoms:
            if not isinstance(g, dict):
                continue
            p = _first_lonlat((g.get("location") or {}).get("coordinates")) or _first_lonlat(g.get("coordinates"))
            if p:
                return p
    return None


def make_candidate(hit, query_text, max_score, schema_space):
    src = hit["_source"]
    name = get_canonical_name(src, hit["_id"])
    alt_names = get_alternative_names(src, name)
    score = normalize_score(hit["_score"], max_score)
    is_exact = name.lower() == query_text.lower()
    ccodes = src.get("ccodes", [])
    # has_geom: does this place have a full POLYGON geometry (i.e. usable as a `contained_in`
    # region)? Prefer the explicit flag forwarded from the gateway; otherwise infer it from the
    # geometry types (legacy ES path), falling back to False when no geometry is present.
    has_geom = src.get("has_geom")
    if has_geom is None:
        has_geom = any((g.get("location") or {}).get("type") in ("Polygon", "MultiPolygon")
                       for g in src.get("geoms", []))
    return {
        # `place:` is the OpenRefine entity-type prefix (the protocol's opaque id);
        # what follows is the gazetteer identifier we surface and export.
        "id": "place:" + whg_place_id(src),
        "name": name,
        "score": score,
        "match": is_exact,
        # Which source this candidate came from (place#157). A reconciliation
        # response blends candidates from many differently-licensed gazetteers;
        # without this the root `attribution` block would tell a consumer WHICH
        # licences the response spans but not which candidate falls under which.
        # Resolve against `attribution.sources[namespace]` (or, for "whg",
        # `attribution.datasets`).
        "namespace": get_namespace(whg_place_id(src)),
        "alt_names": alt_names,
        "ccodes": ccodes,
        "repr_point": repr_point(src),  # [lng, lat] or None — enables map preview + geo-disambiguation
        "description": f"Country: {', '.join(ccodes)}",
        "has_geom": bool(has_geom),
        # Wikipedia article links (from Wikidata sitelinks in the index) — empty unless the place
        # carries them. Additive: lets the Workbench enrich Wikidata-backed matches with Wikipedia.
        "wikipedia": wikipedia_links(src.get("links")),
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

    # AAT place types (types.identifier)
    types = params.get("types")
    if types:
        if isinstance(types, str):
            types = [t.strip() for t in types.split(",") if t.strip()]
        q["query"]["bool"]["must"].append({"terms": {"types.identifier": types}})

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

        # Collect geometries from either plain GeoJSON or GeometryCollection
        geometries = []
        if bounds.get("geometries"):
            geometries = bounds["geometries"]
        elif bounds.get("type") in ("Polygon", "MultiPolygon"):
            geometries = [bounds]

        for geometry in geometries:
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

    # Tolerate an index in ELASTIC_INDICES that is absent from the cluster
    # (e.g. dropped/renamed during reindexing): skip it rather than 500 the
    # whole search. Without this a single missing index (wdgn) raised
    # index_not_found_exception and took down every reconcile/suggest call.
    resp = es.search(index=index, body=body, ignore_unavailable=True, allow_no_indices=True)
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

    # Add filter properties (used as query constraints, not data extension)
    propose_properties.extend([
        {
            "id": "whg:namespaces",
            "name": "Place: Namespaces (filter)",
            "description": "Comma-separated namespace prefixes to search (e.g. 'wd,gn'). Limits results to the specified data sources.",
            "type": "string"
        },
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
