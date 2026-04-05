# placetypes/mapping_utils.py
"""
Elasticsearch query helpers for the type mapping UI.

All mapping data is read from and written to the ES `types` index
via `settings.ES_CONN`.  Source type inventories come from static
JSON/TXT data files in placetypes/data/.

Key design: get_current_mappings() is cached via Django's cache
framework (shared across workers) for 60 s so that multiple AJAX
calls on the same page load don't each wait for ES.
"""

import json
import logging
import time as _time
from pathlib import Path

from django.conf import settings
from django.core.cache import cache as django_cache

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / "data"

# Map from vocabulary name to ES field on AAT type documents
VOCAB_FIELD_MAP = {
    "geonames": "gn_fcodes",
    "wikidata": "wd_qids",
    "osm": "osm_tags",
    "ohm": "ohm_tags",
}

# GeoNames feature class letters
GN_FCLASSES = {"A", "H", "L", "P", "R", "S", "T", "U", "V"}


# ---------------------------------------------------------------------------
# Mappings cache (Django cache framework — shared across workers)
# ---------------------------------------------------------------------------
_MAPPINGS_CACHE_KEY = "placetypes:current_mappings"
_MAPPINGS_TTL = 60  # seconds


def _invalidate_mappings_cache():
    """Force the next get_current_mappings() call to re-query ES."""
    django_cache.delete(_MAPPINGS_CACHE_KEY)


def get_current_mappings():
    """
    Query the ES `types` index for all documents with cross-vocabulary
    mapping fields populated.  Results are cached for 60 s via Django's
    cache framework (shared across all workers).

    Returns (gn_map, wd_map, osm_map, ohm_map) — four dicts mapping
    source IDs to {"aat_id": int, "aat_term": str}.
    """
    cached = django_cache.get(_MAPPINGS_CACHE_KEY)
    if cached is not None:
        return cached

    # Query ES
    gn_map, wd_map, osm_map, ohm_map = {}, {}, {}, {}
    try:
        es = settings.ES_CONN
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
            request_timeout=8,
        )
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
    except Exception as e:
        logger.warning("get_current_mappings ES error (returning empty): %s", e)

    result = (gn_map, wd_map, osm_map, ohm_map)
    django_cache.set(_MAPPINGS_CACHE_KEY, result, _MAPPINGS_TTL)
    return result


# ---------------------------------------------------------------------------
# GeoNames feature codes
# ---------------------------------------------------------------------------

# Module-level cache for the labels file (loaded once)
_gn_labels_cache = None


def _load_geonames_labels():
    """
    Load GeoNames feature code labels from featureCodes_en.txt.

    File format (tab-separated):
        A.ADM1<TAB>first-order administrative division<TAB>long description…
    """
    global _gn_labels_cache
    if _gn_labels_cache is not None:
        return _gn_labels_cache

    labels = {}
    fpath = DATA_DIR / "featureCodes_en.txt"
    if fpath.exists():
        with open(fpath, encoding="utf-8") as f:
            for line in f:
                parts = line.strip().split("\t")
                if len(parts) >= 2:
                    code = parts[0].strip()
                    label = parts[1].strip()
                    desc = parts[2].strip() if len(parts) >= 3 else ""
                    labels[code] = {"label": label, "description": desc}
    _gn_labels_cache = labels
    return labels


def get_geonames_types():
    """
    Return a list of GeoNames feature codes with labels, descriptions,
    and any existing AAT mappings.

    Source of truth for the code inventory: featureCodes_en.txt
    (downloaded from geonames.org).  ES `places` aggregation is attempted
    for place counts but is optional.
    """
    gn_map = get_current_mappings()[0]
    gn_labels = _load_geonames_labels()

    # Try to get counts from ES places index (optional — may timeout)
    code_counts = {}
    try:
        es = settings.ES_CONN
        resp = es.search(
            index="places",
            size=0,
            query={"match_all": {}},
            aggs={
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
            request_timeout=8,
        )
        for bucket in resp["aggregations"]["source_labels"]["by_label"]["buckets"]:
            sl = bucket["key"]
            if "." in sl and sl.split(".")[0] in GN_FCLASSES:
                code_counts[sl] = bucket["doc_count"]
    except Exception as e:
        logger.info("GeoNames counts from ES unavailable: %s", e)

    # Build from the labels file (authoritative inventory), plus any mapped codes
    all_codes = set(gn_labels.keys()) | set(gn_map.keys()) | set(code_counts.keys())
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

    items.sort(key=lambda x: (-x["count"], x["source_id"]))
    return items


# ---------------------------------------------------------------------------
# Wikidata Q-items
# ---------------------------------------------------------------------------

_WD_LABELS_CACHE_FILE = DATA_DIR / "wikidata_labels.json"


def _load_wikidata_label_cache():
    """Load cached Wikidata labels from a JSON file."""
    if _WD_LABELS_CACHE_FILE.exists():
        try:
            with open(_WD_LABELS_CACHE_FILE, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_wikidata_label_cache(cache):
    """Save Wikidata labels to a JSON file."""
    try:
        with open(_WD_LABELS_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=1)
    except Exception as e:
        logger.warning("Could not save Wikidata label cache: %s", e)


def _fetch_wikidata_labels(qids, batch_size=50):
    """
    Fetch English labels + descriptions for a list of Wikidata Q-IDs
    from the Wikidata API (wbgetentities), in batches of 50.

    Returns a dict: { "Q515": {"label": "city", "description": "large settlement"}, ... }
    """
    import requests as http_requests

    result = {}
    qid_list = list(qids)

    for i in range(0, len(qid_list), batch_size):
        batch = qid_list[i:i + batch_size]
        try:
            resp = http_requests.get(
                "https://www.wikidata.org/w/api.php",
                params={
                    "action": "wbgetentities",
                    "ids": "|".join(batch),
                    "props": "labels|descriptions",
                    "languages": "en",
                    "format": "json",
                },
                timeout=10,
                headers={"User-Agent": "WHG/3.5 (https://whgazetteer.org)"},
            )
            resp.raise_for_status()
            data = resp.json()
            for qid, entity in data.get("entities", {}).items():
                label = entity.get("labels", {}).get("en", {}).get("value", qid)
                desc = entity.get("descriptions", {}).get("en", {}).get("value", "")
                result[qid] = {"label": label, "description": desc}
        except Exception as e:
            logger.warning("Wikidata API error for batch %d: %s", i, e)
            for qid in batch:
                if qid not in result:
                    result[qid] = {"label": qid, "description": ""}

        # Brief pause between batches to be polite
        if i + batch_size < len(qid_list):
            _time.sleep(0.2)

    return result


def get_wikidata_types():
    """
    Return a list of Wikidata Q-items with labels, descriptions,
    and any existing AAT mappings.

    Labels are fetched from Wikidata API and cached to a local JSON file.
    To avoid blocking requests, at most MAX_FETCH_PER_REQUEST labels are
    fetched per call; subsequent page loads progressively fill the cache.
    """
    MAX_FETCH_PER_REQUEST = 200  # 4 batches of 50

    wd_map = get_current_mappings()[1]

    # Try to get Q-item counts from ES places index
    qid_counts = {}
    try:
        es = settings.ES_CONN
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
                        }
                    },
                }
            },
            request_timeout=8,
        )
        for bucket in resp["aggregations"]["qid_agg"]["by_ident"]["buckets"]:
            qid = bucket["key"]
            if qid.startswith("Q"):
                qid_counts[qid] = bucket["doc_count"]
    except Exception as e:
        logger.info("Wikidata counts from ES unavailable: %s", e)

    # All known Q-IDs: from ES aggregation + from existing mappings
    all_qids = set(qid_counts.keys()) | set(wd_map.keys())
    if not all_qids:
        return []

    # Load cached labels, fetch a limited batch of missing ones
    label_cache = _load_wikidata_label_cache()
    missing = [q for q in all_qids if q not in label_cache]
    if missing:
        # Prioritise high-count items so the most important labels arrive first
        missing.sort(key=lambda q: -qid_counts.get(q, 0))
        fetch_batch = missing[:MAX_FETCH_PER_REQUEST]
        logger.info(
            "Fetching %d of %d missing Wikidata labels from API...",
            len(fetch_batch), len(missing),
        )
        new_labels = _fetch_wikidata_labels(fetch_batch)
        label_cache.update(new_labels)
        _save_wikidata_label_cache(label_cache)

    items = []
    for qid in sorted(all_qids):
        info = label_cache.get(qid, {})
        items.append({
            "source_id": qid,
            "label": info.get("label", qid),
            "description": info.get("description", ""),
            "count": qid_counts.get(qid, 0),
            "mapping": wd_map.get(qid),
        })

    items.sort(key=lambda x: (-x["count"], x["source_id"]))
    return items


# ---------------------------------------------------------------------------
# OSM / OHM tag values
# ---------------------------------------------------------------------------

# Module-level caches for static JSON data files
_osm_data_cache = None
_ohm_data_cache = None


def _load_osm_data():
    """Load and cache OSM tag data from the JSON file."""
    global _osm_data_cache
    if _osm_data_cache is not None:
        return _osm_data_cache
    try:
        with open(DATA_DIR / "osm.json", encoding="utf-8") as f:
            _osm_data_cache = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.warning("Could not load OSM data file: %s", e)
        _osm_data_cache = {}
    return _osm_data_cache


def _load_ohm_data():
    """Load and cache OHM tag data from the JSON file."""
    global _ohm_data_cache
    if _ohm_data_cache is not None:
        return _ohm_data_cache
    try:
        with open(DATA_DIR / "ohm.json", encoding="utf-8") as f:
            _ohm_data_cache = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.warning("Could not load OHM data file: %s", e)
        _ohm_data_cache = {}
    return _ohm_data_cache


def _count_tag_values(data):
    """Count total tag values in an OSM/OHM data dict."""
    total = 0
    for tag_data in data.values():
        if isinstance(tag_data, dict):
            total += len(tag_data.get("values", []))
    return total


def _build_tag_items(data, mapping_dict, tag_key_filter=None):
    """Build the items list from an OSM/OHM data dict + mapping dict."""
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
                "mapping": mapping_dict.get(source_id),
            })
    items.sort(key=lambda x: (-x["count"], x["source_id"]))
    return items


def get_osm_types(tag_key_filter=None):
    """Return all OSM tag values with current AAT mappings."""
    data = _load_osm_data()
    osm_map = get_current_mappings()[2]
    return _build_tag_items(data, osm_map, tag_key_filter)


def get_ohm_types(tag_key_filter=None):
    """Return all OHM tag values with current AAT mappings."""
    data = _load_ohm_data()
    ohm_map = get_current_mappings()[3]
    return _build_tag_items(data, ohm_map, tag_key_filter)


# ---------------------------------------------------------------------------
# AAT concept search
# ---------------------------------------------------------------------------

def search_aat_types(query, limit=20):
    """
    Search the ES `types` index for AAT concepts matching the query.

    Uses a boosted bool/should query across term, term.folded, and note.
    Only returns is_place_type=True documents.
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
            request_timeout=8,
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
    Save a source type -> AAT concept mapping in the ES types index.

    1. Remove source_id from any other AAT concept's array (if re-mapping).
    2. Add source_id to the target AAT concept's array.
    3. Invalidate the mappings cache.
    4. Return {"status": "ok", "aat_id": ..., "aat_term": ...}.
    """
    es = settings.ES_CONN
    field = VOCAB_FIELD_MAP[source_vocab]

    # 1. Remove from any existing AAT concept
    try:
        old_resp = es.search(
            index="types",
            query={"term": {field: source_id}},
            _source=["aat_id"],
            size=10,
            request_timeout=8,
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
                    refresh=True,
                    request_timeout=8,
                )
    except Exception as e:
        logger.warning("Error removing old mapping for %s: %s", source_id, e)

    # 2. Add to target AAT concept (refresh=True so the change is
    #    immediately visible to subsequent search queries)
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
        refresh=True,
        request_timeout=8,
    )

    # 3. Invalidate cache
    _invalidate_mappings_cache()

    # 4. Return updated info
    doc = es.get(index="types", id=f"aat:{aat_id}", _source=["aat_id", "term"])
    return {
        "status": "ok",
        "aat_id": aat_id,
        "aat_term": doc["_source"].get("term", ""),
    }


def remove_mapping(source_vocab, source_id, aat_id):
    """Remove a source type -> AAT concept mapping from ES."""
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
        refresh=True,
        request_timeout=8,
    )
    _invalidate_mappings_cache()
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Copy OSM -> OHM
# ---------------------------------------------------------------------------

def copy_osm_to_ohm():
    """Copy all OSM tag mappings to OHM for matching tag values."""
    ohm_data = _load_ohm_data()
    ohm_source_ids = set()
    for tag_key, tag_data in ohm_data.items():
        if isinstance(tag_data, dict):
            for entry in tag_data.get("values", []):
                ohm_source_ids.add(f"{tag_key}={entry['value']}")

    _, _, osm_map, ohm_map = get_current_mappings()

    es = settings.ES_CONN
    copied = 0
    skipped = 0

    for source_id, osm_info in osm_map.items():
        if source_id not in ohm_source_ids or source_id in ohm_map:
            skipped += 1
            continue
        try:
            es.update(
                index="types",
                id=f"aat:{osm_info['aat_id']}",
                script={
                    "source": """
                        if (ctx._source.ohm_tags == null) ctx._source.ohm_tags = [];
                        if (!ctx._source.ohm_tags.contains(params.val)) {
                            ctx._source.ohm_tags.add(params.val);
                        }
                    """,
                    "params": {"val": source_id},
                },
                request_timeout=8,
            )
            copied += 1
        except Exception as e:
            logger.warning("Error copying OSM->OHM for %s: %s", source_id, e)
            skipped += 1

    _invalidate_mappings_cache()

    # Flush so all updates are immediately searchable
    try:
        es.indices.refresh(index="types")
    except Exception as e:
        logger.warning("Index refresh after copy_osm_to_ohm failed: %s", e)

    return {"status": "ok", "copied": copied, "skipped": skipped}


# ---------------------------------------------------------------------------
# Mapping statistics
# ---------------------------------------------------------------------------

def get_mapping_stats():
    """
    Return mapping coverage statistics.

    Totals come from local data files (instant); mapped counts from
    the cached get_current_mappings() call.
    """
    gn_map, wd_map, osm_map, ohm_map = get_current_mappings()

    gn_labels = _load_geonames_labels()
    gn_total = len(gn_labels) if gn_labels else 680
    gn_mapped = len(gn_map)

    osm_total = _count_tag_values(_load_osm_data())
    ohm_total = _count_tag_values(_load_ohm_data())
    osm_mapped = len(osm_map)
    ohm_mapped = len(ohm_map)

    wd_mapped = len(wd_map)

    # Count total Wikidata Q-items from ES places aggregation
    wd_total = 0
    try:
        es = settings.ES_CONN
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
                        }
                    },
                }
            },
            request_timeout=8,
        )
        wd_total = len(resp["aggregations"]["qid_agg"]["by_ident"]["buckets"])
    except Exception as e:
        logger.info("Wikidata total count from ES unavailable: %s", e)
    # Ensure total is at least as large as the mapped count
    wd_total = max(wd_total, wd_mapped)

    return {
        "geonames": {"total": gn_total, "mapped": gn_mapped, "unmapped": gn_total - gn_mapped},
        "wikidata": {"total": wd_total, "mapped": wd_mapped, "unmapped": max(0, wd_total - wd_mapped)},
        "osm": {"total": osm_total, "mapped": osm_mapped, "unmapped": max(0, osm_total - osm_mapped)},
        "ohm": {"total": ohm_total, "mapped": ohm_mapped, "unmapped": max(0, ohm_total - ohm_mapped)},
    }

