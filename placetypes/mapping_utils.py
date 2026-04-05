# placetypes/mapping_utils.py
"""
Elasticsearch query helpers for the type mapping UI.

All data is read from and written to the production ES `types` index
via `settings.ES_CONN`.  No Django models are used for mapping storage.

Functions:
    get_current_mappings()     — build reverse lookups from ES types index
    get_geonames_types()       — GeoNames feature codes + counts from ES places
    get_wikidata_types()       — Wikidata Q-items + counts from ES places
    get_osm_types()            — OSM tag values from static JSON + mappings
    get_ohm_types()            — OHM tag values from static JSON + mappings
    search_aat_types()         — search AAT concepts in the types index
    save_mapping()             — add a source→AAT mapping in ES
    remove_mapping()           — remove a source→AAT mapping in ES
    copy_osm_to_ohm()         — bulk-copy OSM mappings to OHM
    get_mapping_stats()        — mapping coverage statistics
"""

import json
import logging
from pathlib import Path

from django.conf import settings

logger = logging.getLogger(__name__)

# Map from vocabulary name to ES field on AAT type documents
VOCAB_FIELD_MAP = {
    "geonames": "gn_fcodes",
    "wikidata": "wd_qids",
    "osm": "osm_tags",
    "ohm": "ohm_tags",
}

# GeoNames feature class letters
GN_FCLASSES = {"A", "H", "L", "P", "R", "S", "T", "U", "V"}

# GeoNames feature codes URL (for label lookup)
GN_FEATURE_CODES_URL = "https://download.geonames.org/export/dump/featureCodes_en.txt"


# ---------------------------------------------------------------------------
# Current mappings from ES types index
# ---------------------------------------------------------------------------

def get_current_mappings():
    """
    Query the ES `types` index for all documents with cross-vocabulary
    mapping fields populated.  Build four reverse-lookup dicts:

        gn_map:  "P.PPL"        → {"aat_id": 300008347, "aat_term": "inhabited places"}
        wd_map:  "Q515"         → {"aat_id": 300008389, "aat_term": "cities"}
        osm_map: "place=city"   → {"aat_id": 300008389, "aat_term": "cities"}
        ohm_map: "place=city"   → {"aat_id": 300008389, "aat_term": "cities"}

    Returns (gn_map, wd_map, osm_map, ohm_map).
    """
    es = settings.ES_CONN
    gn_map = {}
    wd_map = {}
    osm_map = {}
    ohm_map = {}

    try:
        resp = es.search(
            index="types",
            query={
                "bool": {
                    "should": [
                        {"exists": {"field": "gn_fcodes"}},
                        {"exists": {"field": "wd_qids"}},
                        {"exists": {"field": "osm_tags"}},
                        {"exists": {"field": "ohm_tags"}},
                    ],
                    "minimum_should_match": 1,
                }
            },
            _source=["aat_id", "term", "gn_fcodes", "wd_qids", "osm_tags", "ohm_tags"],
            size=10000,
        )
    except Exception as e:
        logger.exception("Error fetching current mappings from ES")
        return gn_map, wd_map, osm_map, ohm_map

    for hit in resp["hits"]["hits"]:
        src = hit["_source"]
        aat_id = src["aat_id"]
        aat_term = src.get("term", "")
        info = {"aat_id": aat_id, "aat_term": aat_term}

        for fc in src.get("gn_fcodes") or []:
            gn_map[fc] = info
        for qid in src.get("wd_qids") or []:
            wd_map[qid] = info
        for tag in src.get("osm_tags") or []:
            osm_map[tag] = info
        for tag in src.get("ohm_tags") or []:
            ohm_map[tag] = info

    return gn_map, wd_map, osm_map, ohm_map


# ---------------------------------------------------------------------------
# GeoNames feature codes
# ---------------------------------------------------------------------------

def _load_geonames_labels():
    """
    Load GeoNames feature code labels from a local cache file,
    or return an empty dict if unavailable.

    The file format (featureCodes_en.txt) is tab-separated:
        A.ADM1<TAB>first-order administrative division<TAB>description...
    """
    cache_path = Path(__file__).parent / "data" / "featureCodes_en.txt"
    labels = {}
    if cache_path.exists():
        with open(cache_path, encoding="utf-8") as f:
            for line in f:
                parts = line.strip().split("\t")
                if len(parts) >= 2:
                    code = parts[0].strip()
                    label = parts[1].strip()
                    desc = parts[2].strip() if len(parts) >= 3 else ""
                    labels[code] = {"label": label, "description": desc}
    return labels


def get_geonames_types():
    """
    Return a list of GeoNames feature codes with counts and AAT mappings.

    Tries to aggregate from ES `places` index first; if that fails,
    returns only the codes that have existing AAT mappings.
    """
    gn_map, _, _, _ = get_current_mappings()
    gn_labels = _load_geonames_labels()
    es = settings.ES_CONN

    # Try to get counts from ES places index
    code_counts = {}
    try:
        resp = es.search(
            index="places",
            size=0,
            query={"match_all": {}},
            aggs={
                "fcode_counts": {
                    "terms": {
                        "field": "fclasses",
                        "size": 10,
                    }
                },
                "source_labels": {
                    "nested": {"path": "types"},
                    "aggs": {
                        "by_label": {
                            "terms": {
                                "field": "types.sourceLabel",
                                "size": 2000,
                            }
                        }
                    },
                },
            },
        )
        for bucket in resp["aggregations"]["source_labels"]["by_label"]["buckets"]:
            sl = bucket["key"]
            # GeoNames sourceLabels look like "P.PPL" or "A.ADM1"
            if "." in sl and sl.split(".")[0] in GN_FCLASSES:
                code_counts[sl] = bucket["doc_count"]
    except Exception as e:
        logger.warning("Could not aggregate GeoNames types from ES places: %s", e)

    # Build result list from the union of known codes + mapped codes
    all_codes = set(code_counts.keys()) | set(gn_map.keys()) | set(gn_labels.keys())
    items = []
    for code in sorted(all_codes):
        fclass = code.split(".")[0] if "." in code else ""
        if fclass and fclass not in GN_FCLASSES:
            continue
        info = gn_labels.get(code, {})
        items.append({
            "source_id": code,
            "fclass": fclass,
            "label": info.get("label", code),
            "description": info.get("description", ""),
            "count": code_counts.get(code, 0),
            "mapping": gn_map.get(code),
        })

    # Sort by count descending, then by code
    items.sort(key=lambda x: (-x["count"], x["source_id"]))
    return items


# ---------------------------------------------------------------------------
# Wikidata Q-items
# ---------------------------------------------------------------------------

def get_wikidata_types():
    """
    Return a list of Wikidata Q-items with counts and AAT mappings.

    Tries to aggregate from ES `places` index; falls back to just
    listing currently-mapped Q-items.
    """
    _, wd_map, _, _ = get_current_mappings()
    es = settings.ES_CONN

    qid_counts = {}
    qid_labels = {}
    try:
        resp = es.search(
            index="places",
            size=0,
            query={"match_all": {}},
            aggs={
                "qid_agg": {
                    "nested": {"path": "types"},
                    "aggs": {
                        "by_ident": {
                            "terms": {
                                "field": "types.identifier",
                                "include": "Q.*",
                                "size": 10000,
                            },
                            "aggs": {
                                "label": {
                                    "terms": {
                                        "field": "types.sourceLabel",
                                        "size": 1,
                                    }
                                }
                            },
                        }
                    },
                }
            },
        )
        for bucket in resp["aggregations"]["qid_agg"]["by_ident"]["buckets"]:
            qid = bucket["key"]
            if qid.startswith("Q"):
                qid_counts[qid] = bucket["doc_count"]
                label_buckets = bucket.get("label", {}).get("buckets", [])
                if label_buckets:
                    qid_labels[qid] = label_buckets[0]["key"]
    except Exception as e:
        logger.warning("Could not aggregate Wikidata types from ES places: %s", e)

    # Build result from union of aggregated + mapped Q-items
    all_qids = set(qid_counts.keys()) | set(wd_map.keys())
    items = []
    for qid in sorted(all_qids):
        items.append({
            "source_id": qid,
            "label": qid_labels.get(qid, qid),
            "description": "",
            "count": qid_counts.get(qid, 0),
            "mapping": wd_map.get(qid),
        })

    items.sort(key=lambda x: (-x["count"], x["source_id"]))
    return items


# ---------------------------------------------------------------------------
# OSM tag values
# ---------------------------------------------------------------------------

def get_osm_types(tag_key_filter=None):
    """
    Load OSM tag values from `placetypes/data/osm.json` and merge
    with current AAT mappings.

    Args:
        tag_key_filter: Optional tag key to filter by (e.g. "place", "natural")

    Returns:
        List of dicts with source_id, tag_key, label, description, count,
        in_wiki, mapping.
    """
    data_path = Path(__file__).parent / "data" / "osm.json"
    try:
        with open(data_path, encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.warning("Could not load OSM data file: %s", e)
        return []

    _, _, osm_map, _ = get_current_mappings()

    items = []
    for tag_key, tag_data in data.items():
        if not isinstance(tag_data, dict):
            continue
        if tag_key_filter and tag_key != tag_key_filter:
            continue
        for entry in tag_data.get("values", []):
            source_id = f"{tag_key}={entry['value']}"
            items.append({
                "source_id": source_id,
                "tag_key": tag_key,
                "label": entry.get("value", ""),
                "description": entry.get("description", ""),
                "count": entry.get("count", 0),
                "in_wiki": entry.get("in_wiki", False),
                "mapping": osm_map.get(source_id),
            })

    # Sort: mapped items first, then by count descending
    items.sort(key=lambda x: (-x["count"], x["source_id"]))
    return items


# ---------------------------------------------------------------------------
# OHM tag values
# ---------------------------------------------------------------------------

def get_ohm_types(tag_key_filter=None):
    """
    Load OHM tag values from `placetypes/data/ohm.json` and merge
    with current AAT mappings.
    """
    data_path = Path(__file__).parent / "data" / "ohm.json"
    try:
        with open(data_path, encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.warning("Could not load OHM data file: %s", e)
        return []

    _, _, _, ohm_map = get_current_mappings()

    items = []
    for tag_key, tag_data in data.items():
        if not isinstance(tag_data, dict):
            continue
        if tag_key_filter and tag_key != tag_key_filter:
            continue
        for entry in tag_data.get("values", []):
            source_id = f"{tag_key}={entry['value']}"
            items.append({
                "source_id": source_id,
                "tag_key": tag_key,
                "label": entry.get("value", ""),
                "description": entry.get("description", ""),
                "count": entry.get("count", 0),
                "in_wiki": entry.get("in_wiki", False),
                "mapping": ohm_map.get(source_id),
            })

    items.sort(key=lambda x: (-x["count"], x["source_id"]))
    return items


# ---------------------------------------------------------------------------
# AAT concept search
# ---------------------------------------------------------------------------

def search_aat_types(query, limit=20):
    """
    Search the ES `types` index for AAT concepts matching the query.

    Uses a boosted bool/should query across term.keyword, term.folded,
    and note fields.  Only returns is_place_type=True documents.

    Returns a list of dicts: aat_id, term, note (truncated), fclasses, path, score.
    """
    es = settings.ES_CONN
    clean = query.replace("_", " ")

    try:
        resp = es.search(
            index="types",
            size=limit,
            query={
                "bool": {
                    "must": [{"term": {"is_place_type": True}}],
                    "should": [
                        {"term": {"term.keyword": {"value": clean, "boost": 30}}},
                        {"match_phrase_prefix": {"term.folded": {"query": clean, "boost": 10}}},
                        {"match": {"term.folded": {"query": clean, "operator": "and", "boost": 5}}},
                        {
                            "multi_match": {
                                "query": clean,
                                "fields": ["term.folded^3", "note"],
                                "type": "most_fields",
                                "boost": 1,
                            }
                        },
                    ],
                    "minimum_should_match": 1,
                }
            },
            _source=["aat_id", "term", "note", "fclasses", "path"],
        )
    except Exception as e:
        logger.exception("Error searching AAT types in ES")
        return []

    results = []
    for hit in resp["hits"]["hits"]:
        src = hit["_source"]
        results.append({
            "aat_id": src["aat_id"],
            "term": src.get("term", ""),
            "note": (src.get("note") or "")[:200],
            "fclasses": src.get("fclasses", []),
            "path": src.get("path", ""),
            "score": hit["_score"],
        })
    return results


# ---------------------------------------------------------------------------
# Save / remove mappings
# ---------------------------------------------------------------------------

def save_mapping(source_vocab, source_id, aat_id):
    """
    Save a source type → AAT concept mapping in the ES types index.

    1. Remove source_id from any other AAT concept's array (if re-mapping).
    2. Add source_id to the target AAT concept's array.
    3. Return {"status": "ok", "aat_id": ..., "aat_term": ...}.
    """
    es = settings.ES_CONN
    field = VOCAB_FIELD_MAP[source_vocab]

    # 1. Find and remove from any existing AAT concept
    try:
        old_resp = es.search(
            index="types",
            query={"term": {field: source_id}},
            _source=["aat_id"],
            size=10,
        )
        for hit in old_resp["hits"]["hits"]:
            old_aat_id = hit["_source"]["aat_id"]
            if old_aat_id != aat_id:
                es.update(
                    index="types",
                    id=hit["_id"],
                    script={
                        "source": f"""
                            if (ctx._source.{field} != null) {{
                                ctx._source.{field}.remove(
                                    ctx._source.{field}.indexOf(params.val)
                                );
                            }}
                        """,
                        "params": {"val": source_id},
                    },
                )
    except Exception as e:
        logger.warning("Error removing old mapping for %s: %s", source_id, e)

    # 2. Add to the target AAT concept
    es.update(
        index="types",
        id=f"aat:{aat_id}",
        script={
            "source": f"""
                if (ctx._source.{field} == null) ctx._source.{field} = [];
                if (!ctx._source.{field}.contains(params.val)) {{
                    ctx._source.{field}.add(params.val);
                }}
            """,
            "params": {"val": source_id},
        },
    )

    # 3. Fetch the updated doc to return the term
    doc = es.get(index="types", id=f"aat:{aat_id}", _source=["aat_id", "term"])
    return {
        "status": "ok",
        "aat_id": aat_id,
        "aat_term": doc["_source"].get("term", ""),
    }


def remove_mapping(source_vocab, source_id, aat_id):
    """
    Remove a source type → AAT concept mapping from the ES types index.
    """
    es = settings.ES_CONN
    field = VOCAB_FIELD_MAP[source_vocab]

    es.update(
        index="types",
        id=f"aat:{aat_id}",
        script={
            "source": f"""
                if (ctx._source.{field} != null) {{
                    ctx._source.{field}.remove(
                        ctx._source.{field}.indexOf(params.val)
                    );
                }}
            """,
            "params": {"val": source_id},
        },
    )
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Copy OSM → OHM
# ---------------------------------------------------------------------------

def copy_osm_to_ohm():
    """
    Copy all OSM tag mappings to OHM for tag values that appear in both
    vocabularies.

    Returns {"status": "ok", "copied": N, "skipped": N}.
    """
    # Load OHM tag values to build a set of valid OHM source_ids
    ohm_data_path = Path(__file__).parent / "data" / "ohm.json"
    try:
        with open(ohm_data_path, encoding="utf-8") as f:
            ohm_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"status": "error", "error": "Could not load OHM data file"}

    ohm_source_ids = set()
    for tag_key, tag_data in ohm_data.items():
        if not isinstance(tag_data, dict):
            continue
        for entry in tag_data.get("values", []):
            ohm_source_ids.add(f"{tag_key}={entry['value']}")

    # Get current mappings
    _, _, osm_map, ohm_map = get_current_mappings()

    es = settings.ES_CONN
    copied = 0
    skipped = 0

    for source_id, osm_info in osm_map.items():
        if source_id not in ohm_source_ids:
            skipped += 1
            continue
        if source_id in ohm_map:
            skipped += 1
            continue

        aat_id = osm_info["aat_id"]
        try:
            es.update(
                index="types",
                id=f"aat:{aat_id}",
                script={
                    "source": """
                        if (ctx._source.ohm_tags == null) ctx._source.ohm_tags = [];
                        if (!ctx._source.ohm_tags.contains(params.val)) {
                            ctx._source.ohm_tags.add(params.val);
                        }
                    """,
                    "params": {"val": source_id},
                },
            )
            copied += 1
        except Exception as e:
            logger.warning("Error copying OSM→OHM for %s: %s", source_id, e)
            skipped += 1

    return {"status": "ok", "copied": copied, "skipped": skipped}


# ---------------------------------------------------------------------------
# Mapping statistics
# ---------------------------------------------------------------------------

def get_mapping_stats():
    """
    Return mapping coverage statistics for all four vocabularies.

    Uses a single call to get_current_mappings() for mapped counts,
    and counts totals from data files / cached data to avoid redundant
    ES queries.
    """
    gn_map, wd_map, osm_map, ohm_map = get_current_mappings()

    # Count total source types from data files (cheap, no ES needed)
    osm_total = 0
    osm_path = Path(__file__).parent / "data" / "osm.json"
    try:
        with open(osm_path, encoding="utf-8") as f:
            osm_data = json.load(f)
        for tag_key, tag_data in osm_data.items():
            if isinstance(tag_data, dict):
                osm_total += len(tag_data.get("values", []))
    except Exception:
        pass

    ohm_total = 0
    ohm_path = Path(__file__).parent / "data" / "ohm.json"
    try:
        with open(ohm_path, encoding="utf-8") as f:
            ohm_data = json.load(f)
        for tag_key, tag_data in ohm_data.items():
            if isinstance(tag_data, dict):
                ohm_total += len(tag_data.get("values", []))
    except Exception:
        pass

    # GeoNames and Wikidata totals include both mapped codes and any from ES
    # For stats, we'll use mapped count as a lower bound for total
    gn_mapped = len(gn_map)
    wd_mapped = len(wd_map)
    osm_mapped = len(osm_map)
    ohm_mapped = len(ohm_map)

    # For GN/WD, we don't have reliable totals without ES — use mapped as minimum
    gn_total = max(gn_mapped, 680)  # ~680 known GeoNames feature codes
    wd_total = max(wd_mapped, 0)  # Unknown without ES aggregation

    return {
        "geonames": {"total": gn_total, "mapped": gn_mapped, "unmapped": gn_total - gn_mapped},
        "wikidata": {"total": wd_total, "mapped": wd_mapped, "unmapped": wd_total - wd_mapped},
        "osm": {"total": osm_total, "mapped": osm_mapped, "unmapped": max(0, osm_total - osm_mapped)},
        "ohm": {"total": ohm_total, "mapped": ohm_mapped, "unmapped": max(0, ohm_total - ohm_mapped)},
    }


