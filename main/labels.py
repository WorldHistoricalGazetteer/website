"""
Centralised user-facing terminology map (plan-collaborativeCollections §2 & §11).

v3.3 introduces a display-layer relabel ONLY: "Dataset" → "Gazetteer", "Dataset Collection" →
"Gazetteer Group", while "Place Collection" is kept. **No DB values, model names, URLs, or API
fields change** — those are a later, separate migration (§14). Every *user-facing* string that names
one of these concepts should route through this map (server: ``{% label 'gazetteer' %}``; client:
the mirrored ``LABELS`` constant in whg/webpack/js/wb-labels.js) so the eventual deep rename — and
any future wording tweak — is a one-line change here.

Keep this file and wb-labels.js in lock-step: they are two views of one source of truth.
"""

# key → user-facing label (singular). Add plural/verb variants only when a template needs them.
LABELS = {
    # legacy datasets.Dataset — the code/DB name stays "Dataset"; users see "Gazetteer".
    'gazetteer': 'Gazetteer',
    'gazetteer_plural': 'Gazetteers',
    # Collection(collection_class='dataset') — was "Dataset collection".
    'gazetteer_group': 'Gazetteer Group',
    'gazetteer_group_plural': 'Gazetteer Groups',
    # Collection(collection_class='place') — unchanged, already public-facing.
    'place_collection': 'Place Collection',
    'place_collection_plural': 'Place Collections',
    # sequenced Place Collection (seq set).
    'itinerary': 'Itinerary',
    'itinerary_plural': 'Itineraries',
    # the reconciliation doc-type (Map your Data).
    'map_your_data': 'Map your Data',
    # v4 placeholders.
    'route': 'Route',
    'network': 'Network',
    # the tool itself.
    'workbench': 'Collaborative Workbench',
}


def label(key):
    """Return the user-facing label for ``key`` (falls back to the key itself if unknown)."""
    return LABELS.get(key, key)
