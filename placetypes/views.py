# placetypes/views.py
from django.http import JsonResponse

from placetypes.aat_utils import get_type_tree_json, search_types, expand_type_identifiers


def type_tree(request, aat_id=None):
    """
    JSON endpoint for the hierarchical type selector widget.

    GET /types/tree/          → top-level root categories
    GET /types/tree/300008347 → children of aat:300008347

    Returns a list of jstree-compatible node dicts.
    """
    nodes = get_type_tree_json(root_aat_id=aat_id)
    return JsonResponse(nodes, safe=False)


def type_tree_search(request):
    """
    JSON endpoint for searching the type hierarchy.

    GET /types/tree/search/?q=city

    Returns a list of matching types with ancestor paths for
    the tree widget to expand-to-node.
    """
    q = request.GET.get('q', '').strip()
    results = search_types(q)
    return JsonResponse(results, safe=False)


def type_expand(request):
    """
    Expand a set of AAT identifiers to include all of their descendants.

    GET /types/expand/?ids=aat:300008347,aat:300008389
      → {"ids": ["aat:300008347", ...], "count": N}

    Used by the reconciliation Scope picker: a user selects a branch (e.g.
    "inhabited places") and we expand it to every descendant id, because the
    reconcile `types.identifier` filter matches exact ids only.
    """
    raw = request.GET.get('ids', '')
    idents = [s.strip() for s in raw.split(',') if s.strip()]
    expanded = expand_type_identifiers(idents) if idents else []
    return JsonResponse({'ids': expanded, 'count': len(expanded)})


