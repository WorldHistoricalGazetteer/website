# placetypes/aat_utils.py
"""
Utilities for working with the AAT place-type hierarchy.

Provides cached functions for descendant expansion (used at search query
time) and category identifier lookups (used to build the search UI).
"""

import logging

from django.core.cache import cache

from placetypes.aat_config import (
    CATEGORY_LABELS,
    FCLASS_TO_ROOTS,
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

    This expands all root nodes for that fclass plus all their descendants.
    Results are cached for 1 hour.
    """
    cache_key = f"aat_category:{fclass_code}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    root_ids = FCLASS_TO_ROOTS.get(fclass_code, [])
    identifiers = []
    for root_id in root_ids:
        identifiers.extend(get_descendant_identifiers(root_id, include_self=True))

    # Deduplicate while preserving order
    seen = set()
    unique = []
    for ident in identifiers:
        if ident not in seen:
            seen.add(ident)
            unique.append(ident)

    cache.set(cache_key, unique, _CACHE_TTL)
    return unique


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
    for fclass_code in ["A", "P", "S", "R", "L", "T", "H"]:
        label = CATEGORY_LABELS.get(fclass_code, fclass_code)
        identifiers = get_category_identifiers(fclass_code)
        filters.append([fclass_code, label, identifiers])
    return filters


def get_type_tree_json(root_aat_id=None):
    """
    Return a JSON-serialisable tree structure for the type selector widget.

    If root_aat_id is None, returns all top-level categories.
    Otherwise returns the children of the given node.

    Returns a list of dicts:
    [
        {
            "id": "aat:300008347",
            "aat_id": 300008347,
            "text": "inhabited places",
            "fclass": "P",
            "children": [...]  or True (for lazy-loading)
        },
        ...
    ]
    """
    from placetypes.models import Type

    if root_aat_id is None:
        # Return top-level categories
        from placetypes.aat_config import AAT_PLACE_TYPE_ROOTS
        nodes = []
        for aat_id, fclass, label, desc in AAT_PLACE_TYPE_ROOTS:
            has_children = Type.objects.filter(
                parent_id=aat_id, is_place_type=True
            ).exists()
            nodes.append({
                "id": f"aat:{aat_id}",
                "aat_id": aat_id,
                "text": label,
                "fclass": fclass,
                "description": desc,
                "children": True if has_children else [],
            })
        return nodes

    # Return children of a specific node
    children_qs = Type.objects.filter(
        parent_id=root_aat_id,
        is_place_type=True,
    ).order_by('term')

    nodes = []
    for t in children_qs:
        has_children = Type.objects.filter(
            parent_id=t.aat_id, is_place_type=True
        ).exists()
        nodes.append({
            "id": f"aat:{t.aat_id}",
            "aat_id": t.aat_id,
            "text": t.term,
            "fclass": t.fclass or '',
            "children": True if has_children else [],
        })

    return nodes


def invalidate_caches():
    """Clear all AAT-related caches. Call after sync_aat_types runs."""
    for fclass_code in CATEGORY_LABELS:
        cache.delete(f"aat_category:{fclass_code}")
    logger.info("AAT caches invalidated for category lookups.")

