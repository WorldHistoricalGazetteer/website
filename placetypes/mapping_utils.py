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
        if "." in code:
            fclass = code.split(".")[0]
            if fclass not in GN_FCLASSES:
                continue
        elif code in GN_FCLASSES:
            # Standalone fclass code (A, H, L, P, R, S, T, U, V)
            fclass = code
        else:
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


def get_osm_ohm_types(tag_key_filter=None):
    """
    Return a merged list of OSM + OHM tag values.

    Tags that appear in both vocabularies are unified into a single row.
    Descriptions are taken from OSM (which has wiki-sourced descriptions)
    when the OHM entry lacks one.  For OHM-only tags whose *value*
    appears in OSM under a different key, the OSM description for that
    value is used as a fallback (OHM reuses the OSM tagging scheme).

    A ``sources`` list on each item indicates whether the tag comes from
    OSM, OHM, or both.  Mappings are resolved from *either* ``osm_tags``
    or ``ohm_tags`` on the AAT document (they are kept in sync by
    dual-write in save_mapping).
    """
    osm_data = _load_osm_data()
    ohm_data = _load_ohm_data()
    _, _, osm_map, ohm_map = get_current_mappings()

    # Build a cross-key description lookup: value → first non-empty
    # description found in any OSM key.  This lets OHM-only tags like
    # "shop=bakery" pick up the description from "amenity=bakery" etc.
    _osm_desc_by_value = {}
    for tag_key, tag_data in osm_data.items():
        if not isinstance(tag_data, dict):
            continue
        for entry in tag_data.get("values", []):
            v = entry.get("value", "")
            d = entry.get("description", "")
            if d and v not in _osm_desc_by_value:
                _osm_desc_by_value[v] = d

    # Build lookup dicts keyed by source_id → entry dict
    # Each entry: {tag_key, label, description, osm_count, ohm_count, sources}
    merged = {}  # source_id → dict

    for tag_key, tag_data in osm_data.items():
        if not isinstance(tag_data, dict):
            continue
        if tag_key_filter and tag_key != tag_key_filter:
            continue
        for entry in tag_data.get("values", []):
            sid = f"{tag_key}={entry['value']}"
            merged[sid] = {
                "source_id": sid,
                "tag_key": tag_key,
                "label": entry.get("value", ""),
                "description": entry.get("description", ""),
                "osm_count": entry.get("count", 0),
                "ohm_count": 0,
                "sources": ["osm"],
                "in_wiki": entry.get("in_wiki", False),
            }

    for tag_key, tag_data in ohm_data.items():
        if not isinstance(tag_data, dict):
            continue
        if tag_key_filter and tag_key != tag_key_filter:
            continue
        for entry in tag_data.get("values", []):
            sid = f"{tag_key}={entry['value']}"
            if sid in merged:
                # Exists in OSM already — add OHM count and mark both sources
                merged[sid]["ohm_count"] = entry.get("count", 0)
                merged[sid]["sources"].append("ohm")
                # Use OHM description if OSM is blank
                if not merged[sid]["description"] and entry.get("description"):
                    merged[sid]["description"] = entry["description"]
            else:
                # OHM-only tag — use its own description, falling back to
                # the cross-key OSM lookup (same value under a different key)
                desc = entry.get("description", "")
                if not desc:
                    desc = _osm_desc_by_value.get(entry.get("value", ""), "")
                merged[sid] = {
                    "source_id": sid,
                    "tag_key": tag_key,
                    "label": entry.get("value", ""),
                    "description": desc,
                    "osm_count": 0,
                    "ohm_count": entry.get("count", 0),
                    "sources": ["ohm"],
                    "in_wiki": entry.get("in_wiki", False),
                }

    # Attach mapping (check both maps — they should be kept in sync)
    items = []
    for sid, info in merged.items():
        mapping = osm_map.get(sid) or ohm_map.get(sid)
        items.append({
            **info,
            "count": info["osm_count"] + info["ohm_count"],
            "mapping": mapping,
        })

    items.sort(key=lambda x: (-x["count"], x["source_id"]))
    return items


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

    For ``source_vocab='osm_ohm'``, writes to both ``osm_tags`` *and*
    ``ohm_tags`` so the two ES fields stay in sync.
    """
    es = settings.ES_CONN

    # Determine which ES fields to write to
    if source_vocab == "osm_ohm":
        fields = ["osm_tags", "ohm_tags"]
    else:
        fields = [VOCAB_FIELD_MAP[source_vocab]]

    for field in fields:
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
            logger.warning("Error removing old mapping for %s in %s: %s", source_id, field, e)

        # 2. Add to target AAT concept
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
    """
    Remove a source type -> AAT concept mapping from ES.

    For ``source_vocab='osm_ohm'``, removes from both ``osm_tags``
    and ``ohm_tags``.
    """
    es = settings.ES_CONN

    if source_vocab == "osm_ohm":
        fields = ["osm_tags", "ohm_tags"]
    else:
        fields = [VOCAB_FIELD_MAP[source_vocab]]

    for field in fields:
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
# Mapping statistics
# ---------------------------------------------------------------------------

def get_mapping_stats():
    """
    Return mapping coverage statistics.

    Each vocabulary returns both **term-based** counts (how many distinct
    source codes are mapped) and a **record-count-based** coverage
    percentage (``count_pct``).  The latter answers: "what fraction of
    actual place records in the ES ``places`` index are covered by the
    types we have already mapped?"

    Picking off high-count types first means ``count_pct`` can reach
    95 % long before every obscure code is mapped.
    """
    gn_map, wd_map, osm_map, ohm_map = get_current_mappings()

    # ------------------------------------------------------------------
    # GeoNames — counts from ES places aggregation on types.sourceLabel
    # ------------------------------------------------------------------
    gn_labels = _load_geonames_labels()
    gn_total_terms = len(gn_labels) if gn_labels else 680
    gn_mapped_terms = len(gn_map)

    gn_total_records = 0
    gn_mapped_records = 0
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
                cnt = bucket["doc_count"]
                gn_total_records += cnt
                if sl in gn_map:
                    gn_mapped_records += cnt
            elif sl in GN_FCLASSES:
                cnt = bucket["doc_count"]
                gn_total_records += cnt
                if sl in gn_map:
                    gn_mapped_records += cnt
    except Exception as e:
        logger.info("GeoNames record counts from ES unavailable: %s", e)

    # ------------------------------------------------------------------
    # Wikidata — counts from ES places aggregation on types.identifier
    # ------------------------------------------------------------------
    wd_mapped_terms = len(wd_map)
    wd_total_terms = 0
    wd_total_records = 0
    wd_mapped_records = 0
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
                cnt = bucket["doc_count"]
                wd_total_terms += 1
                wd_total_records += cnt
                if qid in wd_map:
                    wd_mapped_records += cnt
    except Exception as e:
        logger.info("Wikidata counts from ES unavailable: %s", e)
    wd_total_terms = max(wd_total_terms, wd_mapped_terms)

    # ------------------------------------------------------------------
    # OSM / OHM (merged) — counts from static JSON data files
    # ------------------------------------------------------------------
    osm_data = _load_osm_data()
    ohm_data = _load_ohm_data()

    # Build a merged dict: source_id → total count (same logic as
    # get_osm_ohm_types but we only need the counts)
    osm_ohm_counts = {}  # source_id → int
    for tag_key, tag_data in osm_data.items():
        if isinstance(tag_data, dict):
            for entry in tag_data.get("values", []):
                sid = f"{tag_key}={entry['value']}"
                osm_ohm_counts[sid] = osm_ohm_counts.get(sid, 0) + (entry.get("count") or 0)
    for tag_key, tag_data in ohm_data.items():
        if isinstance(tag_data, dict):
            for entry in tag_data.get("values", []):
                sid = f"{tag_key}={entry['value']}"
                osm_ohm_counts[sid] = osm_ohm_counts.get(sid, 0) + (entry.get("count") or 0)

    osm_ohm_total_terms = len(osm_ohm_counts)
    mapped_ids = set(osm_map.keys()) | set(ohm_map.keys())
    osm_ohm_mapped_terms = len(mapped_ids)

    osm_ohm_total_records = sum(osm_ohm_counts.values())
    osm_ohm_mapped_records = sum(
        osm_ohm_counts.get(sid, 0) for sid in mapped_ids
    )

    # ------------------------------------------------------------------
    # Build response
    # ------------------------------------------------------------------
    def _pct(num, denom):
        return round(100 * num / denom, 1) if denom else 0

    return {
        "geonames": {
            "total": gn_total_terms,
            "mapped": gn_mapped_terms,
            "unmapped": gn_total_terms - gn_mapped_terms,
            "pct": _pct(gn_mapped_terms, gn_total_terms),
            "total_records": gn_total_records,
            "mapped_records": gn_mapped_records,
            "count_pct": _pct(gn_mapped_records, gn_total_records),
        },
        "wikidata": {
            "total": wd_total_terms,
            "mapped": wd_mapped_terms,
            "unmapped": max(0, wd_total_terms - wd_mapped_terms),
            "pct": _pct(wd_mapped_terms, wd_total_terms),
            "total_records": wd_total_records,
            "mapped_records": wd_mapped_records,
            "count_pct": _pct(wd_mapped_records, wd_total_records),
        },
        "osm_ohm": {
            "total": osm_ohm_total_terms,
            "mapped": osm_ohm_mapped_terms,
            "unmapped": max(0, osm_ohm_total_terms - osm_ohm_mapped_terms),
            "pct": _pct(osm_ohm_mapped_terms, osm_ohm_total_terms),
            "total_records": osm_ohm_total_records,
            "mapped_records": osm_ohm_mapped_records,
            "count_pct": _pct(osm_ohm_mapped_records, osm_ohm_total_records),
        },
    }

