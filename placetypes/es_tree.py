# placetypes/es_tree.py
"""
ES-backed AAT type-tree, search and descendant-expansion — reads the **curated**
`types` Elasticsearch index (proper is_place_type, place-type entry-point roots)
instead of the PostgreSQL Type table, which on prod holds the full uncurated AAT.

A synchronous port of the indexing repo's ``typesystem/tree_api.py`` (which was
async/FastAPI). Queries go through ``settings.ES_CONN`` (elasticsearch8), the same
client placetypes/mapping_utils.py already uses for index="types".

Node shape is identical to the old Postgres tree (get_type_tree_json), so the
reconciliation Scope picker and the mapping UI need no changes:
    {id:"aat:300008347", aat_id, text, fclasses:[...], guide:bool, children:True|[]}
"""

import re
import unicodedata

from django.conf import settings

from placetypes.aat_config import (
    AAT_ENTRY_POINTS,
    AAT_TREE_PROMOTE_TO_ROOT,
    AAT_TREE_SKIP_NODES,
)

TYPES_INDEX = "types"
_skip_and_promote = AAT_TREE_SKIP_NODES | AAT_TREE_PROMOTE_TO_ROOT
# The genuine place-type roots. `is_place_type` is marked True on the generic AAT facets too
# (Objects/Agents/Concepts…, as ancestors of place types), so `depth:0` would surface those. Rooting
# the browse tree — and gating search — on these explicit entry points keeps it place-focused.
_PLACE_ROOTS = set(AAT_ENTRY_POINTS) | AAT_TREE_PROMOTE_TO_ROOT
_ES_TIMEOUT = 8


def _es():
    return settings.ES_CONN


def _search(query, sort=None, source=None, size=1000):
    """Run a query against the types index; return the list of _source dicts."""
    kwargs = {"index": TYPES_INDEX, "query": query, "size": size, "request_timeout": _ES_TIMEOUT}
    if sort is not None:
        kwargs["sort"] = sort
    if source is not None:
        kwargs["_source"] = source
    resp = _es().search(**kwargs)
    return [hit["_source"] for hit in resp["hits"]["hits"]]


def _count(query):
    resp = _es().count(index=TYPES_INDEX, query=query, request_timeout=_ES_TIMEOUT)
    return resp.get("count", 0)


# ── AAT vocabulary bulk export (place#122) ──────────────────────────────────
# The Atlas popup annotates each AAT-mapped type chip with its ``aat:<id>``
# identifier and scope-note description. Rather than a lookup per chip, the whole
# place-type vocabulary is shipped once and cached client-side in IndexedDB
# (keyed by ``place_type_count`` as the version), mirroring the registry-coverage
# cache. Reusable for any other AAT-label/description need.
_VOCAB_QUERY = {"term": {"is_place_type": True}}


def place_type_count():
    """Cheap cache-version signal for the AAT vocab — the number of place-type
    concepts. Bumps whenever the curated vocabulary grows/shrinks."""
    return _count(_VOCAB_QUERY)


def get_type_vocab():
    """The full place-type AAT vocabulary as ``{"aat:<id>": {label, desc}}``.
    ``label`` = the AAT term; ``desc`` = the English scope note (trimmed)."""
    rows = _search(_VOCAB_QUERY, source=["aat_id", "term", "note"], size=8000)
    by_id = {}
    for r in rows:
        aid = r.get("aat_id")
        if aid is None:
            continue
        note = r.get("note") or ""
        by_id[f"aat:{aid}"] = {
            "label": r.get("term") or "",
            "desc": note[:600],  # trim long scope notes for a tooltip
        }
    return by_id


def _strip_diacritics(text):
    if not text:
        return text or ""
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in nfkd if unicodedata.category(ch)[0] != "M")


def _clean_label(term, parent_label=None):
    """Clean an AAT term for tree-widget display. Returns (label, is_guide)."""
    label = (term or "").strip()
    is_guide = False
    if re.fullmatch(r'aat:\d+', label):
        return None, False
    cleaned = re.sub(r'\s*\(hierarchy name\s*\)\s*$', '', label)
    if cleaned != label:
        label = cleaned.strip().lower()
    if label.startswith('<') and label.endswith('>'):
        is_guide = True
        inner = label[1:-1].strip()
        by_match = re.search(r'\b(by\b.+)', inner)
        label = by_match.group(1) if by_match else inner
    if parent_label:
        stripped = re.sub(r'\s*\(' + re.escape(parent_label) + r'\)\s*$', '', label, flags=re.IGNORECASE).strip()
        if stripped:
            label = stripped
    return label, is_guide


def _children_query(parent_path, parent_depth, exclude_ids=None):
    must = [
        {"prefix": {"path": f"{parent_path}."}},
        {"term": {"depth": parent_depth + 1}},
        {"term": {"is_place_type": True}},
    ]
    q = {"bool": {"must": must}}
    if exclude_ids:
        q["bool"]["must_not"] = [{"terms": {"aat_id": list(exclude_ids)}}]
    return q


def _has_children(path, depth):
    """Does this node have at least one child visible in the restructured tree?"""
    if _count(_children_query(path, depth, exclude_ids=_skip_and_promote)) > 0:
        return True
    # A skip-node child counts if IT has (non-promoted) children.
    skip_kids = _search(
        {"bool": {"must": [
            {"prefix": {"path": f"{path}."}},
            {"term": {"depth": depth + 1}},
            {"term": {"is_place_type": True}},
            {"terms": {"aat_id": list(AAT_TREE_SKIP_NODES)}},
        ]}},
        source=["aat_id", "path", "depth"], size=10,
    )
    for skip in skip_kids:
        if _count(_children_query(skip["path"], skip["depth"], exclude_ids=AAT_TREE_PROMOTE_TO_ROOT)) > 0:
            return True
    return False


def _to_node(doc, has_kids, parent_label=None):
    text, guide = _clean_label(doc.get("term", ""), parent_label)
    if text is None:
        note = doc.get("note")
        if note:
            first_sentence = re.split(r'[.;]', note, maxsplit=1)[0].strip()
            text = first_sentence[:80] or f"[unnamed type aat:{doc['aat_id']}]"
        else:
            text = f"[unnamed type aat:{doc['aat_id']}]"
    return {
        "id": f"aat:{doc['aat_id']}",
        "aat_id": doc["aat_id"],
        "text": text,
        "fclasses": doc.get("fclasses", []) or [],
        "guide": guide,
        "children": True if has_kids else [],
    }


_NODE_SOURCE = ["aat_id", "term", "fclasses", "path", "depth", "note"]


def get_type_tree(root_aat_id=None):
    """Root-level place-type nodes (root_aat_id=None) or a node's visible children."""
    if root_aat_id is None:
        roots = _search(
            {"bool": {"must": [{"terms": {"aat_id": list(_PLACE_ROOTS)}}, {"term": {"is_place_type": True}}]}},
            source=_NODE_SOURCE, size=50,
        )
        nodes = [_to_node(d, _has_children(d["path"], d["depth"])) for d in roots]
        nodes.sort(key=lambda n: n["text"].lower())
        return nodes

    parents = _search({"term": {"aat_id": root_aat_id}}, source=_NODE_SOURCE, size=1)
    if not parents:
        return []
    parent = parents[0]
    parent_label, _ = _clean_label(parent.get("term", ""))
    direct = _search(
        {"bool": {"must": [
            {"prefix": {"path": f"{parent['path']}."}},
            {"term": {"depth": parent["depth"] + 1}},
            {"term": {"is_place_type": True}},
        ]}},
        sort=[{"term.keyword": "asc"}], source=_NODE_SOURCE,
    )
    visible = []
    for child in direct:
        if child["aat_id"] in AAT_TREE_PROMOTE_TO_ROOT:
            continue  # shown at the root
        if child["aat_id"] in AAT_TREE_SKIP_NODES:
            # Reparent this node's (non-promoted) children up to the grandparent.
            for gc in _search(
                {"bool": {"must": [
                    {"prefix": {"path": f"{child['path']}."}},
                    {"term": {"depth": child["depth"] + 1}},
                    {"term": {"is_place_type": True}},
                ]}},
                sort=[{"term.keyword": "asc"}], source=_NODE_SOURCE,
            ):
                if gc["aat_id"] not in AAT_TREE_PROMOTE_TO_ROOT:
                    visible.append(gc)
            continue
        visible.append(child)
    return [_to_node(d, _has_children(d["path"], d["depth"]), parent_label) for d in visible]


def search_types(query, limit=30):
    """Search place types by label (ES relevance). Returns [{aat_id, text, ancestors}]."""
    q = (query or "").strip()
    if len(q) < 2:
        return []
    # Over-fetch a wide pool, not just `limit`: the score-ranked window can fill with matches that are
    # is_place_type=True but not geographic places (e.g. art-style "… region styles"), which the ancestor
    # gate below then drops — starving genuine place types that match the query only via prefix and so score
    # lower. A prefix like "region" hit exactly this: its exact-token matches ("… region style") outranked
    # the real "regions (geographic)" types, filling a 30-doc window that was then wholly discarded. The
    # types index is small (~5.8k docs), so fetching a generous pool and truncating after filtering is cheap.
    pool = max(limit * 10, 300)
    docs = _search(
        {"bool": {
            # match_phrase_prefix → typeahead (a word beginning with the query, e.g. "mon" → monastery);
            # match → whole-word relevance (e.g. "temple" → "temple complexes"). Either can satisfy.
            "must": [
                {"bool": {"should": [
                    {"match_phrase_prefix": {"term": {"query": q, "max_expansions": 50}}},
                    {"match": {"term": q}},
                ], "minimum_should_match": 1}},
                {"term": {"is_place_type": True}},
            ],
            "must_not": [
                {"prefix": {"term.keyword": "<"}},          # guide terms
                {"regexp": {"term.keyword": "aat:[0-9]+"}},  # raw ID terms
            ],
        }},
        sort=["_score"], source=["aat_id", "term", "ancestors", "path"], size=pool,
    )
    ql = q.lower()
    results = []
    for doc in docs:
        text, guide = _clean_label(doc.get("term", ""))
        if guide or text is None:
            continue
        ancestors = doc.get("ancestors")
        if not ancestors:
            ancestors = [int(x) for x in doc["path"].split(".")] if doc.get("path") else [doc["aat_id"]]
        # Keep only genuine place types: a node with a real hierarchy path must descend from one of the
        # place-type entry points. Drops object/concept matches (e.g. "city maps", "city noise") that are
        # is_place_type=True only because they share a facet ancestor. Lenient when there is no path.
        anc_set = set(ancestors)
        if len(anc_set) > 1 and not (_PLACE_ROOTS & anc_set):
            continue
        results.append({"aat_id": doc["aat_id"], "text": text, "ancestors": ancestors,
                        "_tier": _match_tier(text, ql)})
    # Re-rank exact/leading matches to the top. ES _score alone buries the term you typed under longer
    # compounds that happen to score higher (e.g. "town" → "town halls" above "towns"). A stable sort by
    # tier keeps ES relevance order *within* each tier, so "towns"/"town" lead, then "town …" phrases.
    results.sort(key=lambda r: r["_tier"])
    for r in results:
        del r["_tier"]
    return results[:limit]


def _match_tier(text, ql):
    """Rank bucket for a label against a lower-cased query — lower is better. Handles simple
    English plural/singular so "town" matches the canonical AAT plural "towns"."""
    base = re.sub(r"\s*\([^)]*\)\s*$", "", text).strip().lower()  # drop a trailing "(qualifier)"
    parts = base.split()
    first = parts[0] if parts else base

    def eqish(a):
        return a == ql or a == ql + "s" or a == ql + "es" or a + "s" == ql or a + "es" == ql

    if eqish(base):
        return 0                       # whole label is (a plural/singular of) the query
    if eqish(first):
        return 1                       # label's first word is the query — "town halls" for "town"
    if base.startswith(ql):
        return 2                       # label begins with the query string
    if re.search(r"\b" + re.escape(ql), base):
        return 3                       # query begins some later word
    return 4                           # prefix-only / mid-word match


def get_descendant_ids(aat_id, include_self=True):
    """All descendant aat_id integers of a concept (via the materialized path)."""
    roots = _search({"term": {"aat_id": aat_id}}, source=["aat_id", "path"], size=1)
    if not roots:
        return [aat_id] if include_self else []
    path = roots[0].get("path")
    if not path:
        return [aat_id] if include_self else []
    docs = _search(
        {"bool": {"must": [{"prefix": {"path": f"{path}."}}, {"term": {"is_place_type": True}}]}},
        source=["aat_id"], size=10000,
    )
    ids = {d["aat_id"] for d in docs}
    if include_self:
        ids.add(aat_id)
    return sorted(ids)


def expand_type_identifiers(aat_identifiers):
    """Expand ['aat:NNN', ...] to include all descendants; returns a list of 'aat:NNN'."""
    expanded = set()
    for ident in aat_identifiers:
        if ident.startswith("aat:"):
            try:
                aid = int(ident[4:])
            except ValueError:
                expanded.add(ident)
                continue
            for did in get_descendant_ids(aid, include_self=True):
                expanded.add(f"aat:{did}")
        else:
            expanded.add(ident)
    return list(expanded)
