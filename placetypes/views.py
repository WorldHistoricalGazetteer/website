# placetypes/views.py
import logging

from django.http import JsonResponse

from placetypes import es_tree
from placetypes import aat_utils  # PostgreSQL fallback if the curated ES index is unavailable

logger = logging.getLogger(__name__)


def type_tree(request, aat_id=None):
    """
    JSON endpoint for the hierarchical type selector widget.

    GET /types/tree/          → top-level place-type roots
    GET /types/tree/300008347 → children of aat:300008347

    Reads the curated `types` ES index; falls back to the PostgreSQL Type table
    if ES is unavailable. Returns a list of jstree-compatible node dicts.
    """
    try:
        nodes = es_tree.get_type_tree(root_aat_id=aat_id)
    except Exception as e:
        logger.warning("type_tree: ES unavailable (%s); falling back to Postgres", e)
        nodes = aat_utils.get_type_tree_json(root_aat_id=aat_id)
    return JsonResponse(nodes, safe=False)


def type_tree_search(request):
    """
    Search the type hierarchy by label.

    GET /types/tree/search/?q=city

    Reads the curated `types` ES index (relevance-ranked); falls back to the
    PostgreSQL Type table. Returns [{aat_id, text, ancestors}].
    """
    q = request.GET.get('q', '').strip()
    try:
        results = es_tree.search_types(q)
    except Exception as e:
        logger.warning("type_tree_search: ES unavailable (%s); falling back to Postgres", e)
        results = aat_utils.search_types(q)
    return JsonResponse(results, safe=False)


def type_vocab(request):
    """Bulk AAT place-type vocabulary for client-side annotation (place#122).

    GET /types/vocab/ → {"version": "<count>", "byId": {"aat:<id>": {label, desc}}}

    Powers the Atlas popup type-chip tooltips (aat:<id> + scope-note description).
    ~5,800 concepts; cached in IndexedDB keyed by ``version`` (place-type count),
    fetched only when the page's ``aat_vocab_version`` global differs. Reads the
    curated `types` ES index; returns an empty vocab (not an error) if ES is down.
    """
    try:
        by_id = es_tree.get_type_vocab()
        # Version = the same count the page ships as aat_vocab_version, so the
        # client's IndexedDB cache validates without a refetch.
        version = str(es_tree.place_type_count())
    except Exception as e:
        logger.warning("type_vocab: ES unavailable (%s)", e)
        return JsonResponse({'version': '0', 'byId': {}})
    return JsonResponse({'version': version, 'byId': by_id})


def type_expand(request):
    """
    Expand a set of AAT identifiers to include all of their descendants.

    GET /types/expand/?ids=aat:300008347,aat:300008389
      → {"ids": ["aat:300008347", ...], "count": N}

    Used by the reconciliation Scope picker: a user selects a branch (e.g.
    "inhabited places") and we expand it to every descendant id, because the
    reconcile `types.identifier` filter matches exact ids only. Reads the curated
    `types` ES index; falls back to the PostgreSQL Type table.
    """
    raw = request.GET.get('ids', '')
    idents = [s.strip() for s in raw.split(',') if s.strip()]
    if not idents:
        return JsonResponse({'ids': [], 'count': 0})
    try:
        expanded = es_tree.expand_type_identifiers(idents)
    except Exception as e:
        logger.warning("type_expand: ES unavailable (%s); falling back to Postgres", e)
        expanded = aat_utils.expand_type_identifiers(idents)
    return JsonResponse({'ids': expanded, 'count': len(expanded)})
