# placetypes/aat_utils.py
"""
Utilities for working with the AAT place-type hierarchy.

Provides cached functions for descendant expansion (used at search query
time) and category identifier lookups (used to build the search UI).
"""

import logging

from django.core.cache import cache

from placetypes.aat_config import (
    AAT_FCLASS_MAP,
    CATEGORY_LABELS,
)

logger = logging.getLogger(__name__)

# Cache TTL: 1 hour (the hierarchy rarely changes)
_CACHE_TTL = 3600


def get_descendants_for_aat_id(aat_id, include_self=True):
    """
    Return a list of all aat_id integers that are descendants of the
    given aat_id, using the materialized path in the Type table.

    Results are cached for 1 hour.
    """
    cache_key = f"aat_descendants:{aat_id}:{include_self}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    from placetypes.models import Type

    try:
        root = Type.objects.get(aat_id=aat_id)
    except Type.DoesNotExist:
        logger.warning("AAT type %s not found in Type table", aat_id)
        return [aat_id] if include_self else []

    descendants = list(
        root.get_descendants(include_self=include_self)
        .values_list('aat_id', flat=True)
    )

    cache.set(cache_key, descendants, _CACHE_TTL)
    return descendants


def get_descendant_identifiers(aat_id, include_self=True):
    """
    Like get_descendants_for_aat_id but returns prefixed strings:
    ['aat:300008347', 'aat:300008389', ...]
    """
    return [f"aat:{aid}" for aid in get_descendants_for_aat_id(aat_id, include_self)]


def get_category_identifiers(fclass_code):
    """
    Return all AAT identifier strings for a given fclass category.

    Queries the Type table for all concepts that have this fclass
    in their fclasses array.  Results are cached for 1 hour.
    """
    cache_key = f"aat_category:{fclass_code}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    from placetypes.models import Type

    aat_ids = list(
        Type.objects.filter(
            fclasses__contains=[fclass_code],
            is_place_type=True,
        ).values_list('aat_id', flat=True)
    )

    identifiers = [f"aat:{aid}" for aid in aat_ids]
    cache.set(cache_key, identifiers, _CACHE_TTL)
    return identifiers


def expand_type_identifiers(aat_identifiers):
    """
    Given a list of AAT identifier strings (e.g. ['aat:300008347']),
    expand each to include all descendants and return the full list.

    Used by the search view to expand user-selected type filters
    to include all child types.
    """
    expanded = set()
    for ident in aat_identifiers:
        if ident.startswith("aat:"):
            try:
                aat_id = int(ident[4:])
            except ValueError:
                expanded.add(ident)
                continue
            for desc_id in get_descendants_for_aat_id(aat_id, include_self=True):
                expanded.add(f"aat:{desc_id}")
        else:
            expanded.add(ident)
    return list(expanded)


def build_adv_filters():
    """
    Build the adv_filters structure for the search template context.

    Returns a list of [fclass_code, category_label, [aat_identifiers]]
    matching the format expected by the search.html template and JS.
    """
    filters = []
    for fclass_code in sorted(CATEGORY_LABELS.keys()):
        label = CATEGORY_LABELS[fclass_code]
        identifiers = get_category_identifiers(fclass_code)
        filters.append([fclass_code, label, identifiers])
    return filters


def get_type_tree_json(root_aat_id=None):
    """
    Return a JSON-serialisable tree structure for the type selector widget.

    If root_aat_id is None, returns the top-level nodes (depth == 1 in the
    walk, i.e. the immediate children of the entry points).
    Otherwise returns the children of the given node using the materialized
    path field.

    Each node dict has:
        id, aat_id, text, fclasses, children (True | [])
    """
    from placetypes.models import Type
    from placetypes.aat_config import AAT_ENTRY_POINTS

    def _to_node(t, has_kids):
        return {
            "id": f"aat:{t.aat_id}",
            "aat_id": t.aat_id,
            "text": t.term,
            "fclasses": t.fclasses or [],
            "children": True if has_kids else [],
        }

    if root_aat_id is None:
        # Top-level: depth-1 concepts under any entry point.
        # These are the nodes whose path is "<entry_point>.<self>"
        # (i.e. exactly one dot in the path).
        nodes = []
        for t in Type.objects.filter(
            depth=1, is_place_type=True,
        ).order_by('term'):
            has_children = Type.objects.filter(
                path__startswith=f"{t.path}.",
                is_place_type=True,
            ).exists()
            nodes.append(_to_node(t, has_children))
        return nodes

    # Children of a specific node: find concepts whose path is
    # exactly "<parent_path>.<child_aat_id>" — i.e. one level deeper.
    try:
        parent = Type.objects.get(aat_id=root_aat_id)
    except Type.DoesNotExist:
        return []

    parent_path = parent.path
    parent_depth = parent.depth

    children_qs = Type.objects.filter(
        path__startswith=f"{parent_path}.",
        depth=parent_depth + 1,
        is_place_type=True,
    ).order_by('term')

    nodes = []
    for t in children_qs:
        has_children = Type.objects.filter(
            path__startswith=f"{t.path}.",
            is_place_type=True,
        ).exists()
        nodes.append(_to_node(t, has_children))

    return nodes


def invalidate_caches():
    """Clear all AAT-related caches. Call after sync_aat_types runs."""
    for fclass_code in CATEGORY_LABELS:
        cache.delete(f"aat_category:{fclass_code}")
    logger.info("AAT caches invalidated for category lookups.")
