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


def _clean_label(term):
    """
    Clean an AAT term for tree-widget display.

    Returns (label, is_guide) where:
    - ``label`` has "(hierarchy name)" stripped, is lowercased if it was
      a hierarchy-name term, and has angle-bracket guide-term boilerplate
      reduced (e.g. ``<barns by form>`` → ``by form``).
    - ``is_guide`` is True for AAT guide terms (angle-bracket labels)
      which are organisational grouping nodes, not real place types.
    - If the term is a raw AAT identifier (e.g. ``aat:300391509``), the
      label is returned as ``None`` so the caller can supply a fallback.
    """
    import re

    label = term.strip()
    is_guide = False

    # Detect raw AAT identifiers stored as labels (sync fallback artefact)
    if re.fullmatch(r'aat:\d+', label):
        return None, False

    # Strip trailing "(hierarchy name)" / "(hierarchy name )"
    cleaned = re.sub(r'\s*\(hierarchy name\s*\)\s*$', '', label)
    was_hierarchy_name = cleaned != label
    if was_hierarchy_name:
        label = cleaned.strip().lower()

    # Handle angle-bracket guide terms: <barns by form> → "by form"
    if label.startswith('<') and label.endswith('>'):
        is_guide = True
        inner = label[1:-1].strip()
        by_match = re.search(r'\b(by\b.+)', inner)
        if by_match:
            label = by_match.group(1)
        else:
            label = inner

    return label, is_guide


def get_type_tree_json(root_aat_id=None):
    """
    Return a JSON-serialisable tree structure for the type selector widget.

    If root_aat_id is None, returns the root-level nodes (entry-point
    concepts at depth 0 **plus** any nodes configured in
    ``AAT_TREE_PROMOTE_TO_ROOT``).

    Otherwise returns the immediate *visible* children of the given node,
    accounting for ``AAT_TREE_SKIP_NODES`` (whose children are reparented
    to the grandparent) and ``AAT_TREE_PROMOTE_TO_ROOT`` (excluded from
    child lists since they appear at the root).

    Each node dict has:
        id, aat_id, text, fclasses, guide, children (True | [])
    """
    from placetypes.models import Type
    from placetypes.aat_config import (
        AAT_TREE_PROMOTE_TO_ROOT,
        AAT_TREE_SKIP_NODES,
    )

    _skip_and_promote = AAT_TREE_SKIP_NODES | AAT_TREE_PROMOTE_TO_ROOT

    def _to_node(t, has_kids, parent_label=None):
        text, guide = _clean_label(t.term)
        # If _clean_label returned None the stored term was a raw AAT
        # identifier — fall back to the scope note or a placeholder.
        if text is None:
            if t.note:
                # Use the first sentence / clause of the note as a label.
                import re
                first_sentence = re.split(r'[.;]', t.note, maxsplit=1)[0].strip()
                text = first_sentence[:80] or f"[unnamed type aat:{t.aat_id}]"
            else:
                text = f"[unnamed type aat:{t.aat_id}]"
        # Strip redundant parenthesised qualifiers that just repeat the
        # parent's label, e.g. "countries (sovereign states)" → "countries"
        # when displayed under the "sovereign states" parent node.
        if parent_label:
            import re
            stripped = re.sub(
                r'\s*\(' + re.escape(parent_label) + r'\)\s*$',
                '', text, flags=re.IGNORECASE,
            ).strip()
            if stripped:
                text = stripped
        return {
            "id": f"aat:{t.aat_id}",
            "aat_id": t.aat_id,
            "text": text,
            "fclasses": t.fclasses or [],
            "guide": guide,
            "children": True if has_kids else [],
        }

    def _has_visible_children(t):
        """Does *t* have at least one child visible in the restructured tree?"""
        # Fast path: any normal (non-skip, non-promote) direct children?
        if Type.objects.filter(
            path__startswith=f"{t.path}.",
            depth=t.depth + 1,
            is_place_type=True,
        ).exclude(aat_id__in=_skip_and_promote).exists():
            return True

        # Slower path: check children of any skip-node children.
        for skip in Type.objects.filter(
            path__startswith=f"{t.path}.",
            depth=t.depth + 1,
            is_place_type=True,
            aat_id__in=AAT_TREE_SKIP_NODES,
        ):
            if Type.objects.filter(
                path__startswith=f"{skip.path}.",
                depth=skip.depth + 1,
                is_place_type=True,
            ).exclude(aat_id__in=AAT_TREE_PROMOTE_TO_ROOT).exists():
                return True

        return False

    def _visible_children_of(parent_type):
        """
        Yield Type instances that should appear as children of *parent_type*
        in the display tree.
        """
        direct = Type.objects.filter(
            path__startswith=f"{parent_type.path}.",
            depth=parent_type.depth + 1,
            is_place_type=True,
        )
        collected = []
        for child in direct:
            if child.aat_id in AAT_TREE_PROMOTE_TO_ROOT:
                continue          # shown at root level
            if child.aat_id in AAT_TREE_SKIP_NODES:
                # Reparent this node's children (minus promoted ones)
                for gc in Type.objects.filter(
                    path__startswith=f"{child.path}.",
                    depth=child.depth + 1,
                    is_place_type=True,
                ).exclude(aat_id__in=AAT_TREE_PROMOTE_TO_ROOT):
                    collected.append(gc)
                continue
            collected.append(child)
        collected.sort(key=lambda t: t.term.lower())
        return collected

    # ------------------------------------------------------------------

    if root_aat_id is None:
        # Root level: depth-0 entry points + promoted nodes.
        nodes = []
        seen = set()

        for t in Type.objects.filter(
            depth=0, is_place_type=True,
        ).order_by('term'):
            nodes.append(_to_node(t, _has_visible_children(t)))
            seen.add(t.aat_id)

        for t in Type.objects.filter(
            aat_id__in=AAT_TREE_PROMOTE_TO_ROOT,
            is_place_type=True,
        ):
            if t.aat_id not in seen:
                nodes.append(_to_node(t, _has_visible_children(t)))
                seen.add(t.aat_id)

        nodes.sort(key=lambda n: n['text'].lower())
        return nodes

    # Children of a specific node.
    try:
        parent = Type.objects.get(aat_id=root_aat_id)
    except Type.DoesNotExist:
        return []

    parent_label, _ = _clean_label(parent.term)
    nodes = []
    for t in _visible_children_of(parent):
        nodes.append(_to_node(t, _has_visible_children(t), parent_label=parent_label))
    return nodes


def _strip_diacritics(text):
    """Remove combining diacritical marks so e.g. 'châteaux' → 'chateaux'."""
    import unicodedata
    if not text:
        return text or ""
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in nfkd if unicodedata.category(ch)[0] != "M")


def search_types(query, limit=30):
    """
    Search the Type table for terms matching *query* (case-insensitive,
    accent-insensitive).

    Returns a list of dicts with ``aat_id``, ``text``, and ``ancestors``
    (the materialized-path AAT ids from root to the matched node,
    inclusive).  Guide terms are excluded.
    """
    from placetypes.models import Type

    if not query or len(query) < 2:
        return []

    query_folded = _strip_diacritics(query).lower()

    # Fetch candidates using the DB icontains (catches most matches)
    # plus a broader pass with the accent-stripped form, then filter
    # in Python to cover accented terms the DB lookup would miss.
    candidates = (
        Type.objects
        .filter(is_place_type=True)
        .exclude(term__startswith='<')
        .exclude(term__regex=r'^aat:\d+$')
        .order_by('term')
    )

    results = []
    for m in candidates.iterator():
        term_folded = _strip_diacritics(m.term).lower()
        if query_folded not in term_folded:
            continue
        text, guide = _clean_label(m.term)
        if guide or text is None:
            continue
        ancestors = (
            [int(x) for x in m.path.split('.')]
            if m.path else [m.aat_id]
        )
        results.append({
            'aat_id': m.aat_id,
            'text': text,
            'ancestors': ancestors,
        })
        if len(results) >= limit:
            break
    return results


def invalidate_caches():
    """Clear all AAT-related caches. Call after sync_aat_types runs."""
    for fclass_code in CATEGORY_LABELS:
        cache.delete(f"aat_category:{fclass_code}")
    logger.info("AAT caches invalidated for category lookups.")
