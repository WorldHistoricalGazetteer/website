"""
Centralised user-facing terminology map (plan-collaborativeCollections §2 & §11).

NAMING RULE (decided 2026-09-02, adopted site-wide 2026-11-17; supersedes the unconditional
"Dataset → Gazetteer" relabel of 2026-07-09): a contributor brings a **dataset**; once it is
reconciled and published it is a **gazetteer**. So "gazetteer" and "Gazetteer Group" below are only
for PUBLISHED material; anything still being uploaded, matched or reviewed is a "dataset". The
authorities WHG matches against (GeoNames, Wikidata ...) are "source gazetteers" in user copy.
"Place Collection" is kept. **No DB values, model names, URLs, or API fields change**; there is
no planned deep rename. Every *user-facing* string that names
one of these concepts should route through this map (server: ``{% label 'gazetteer' %}``; client:
the mirrored ``LABELS`` constant in whg/webpack/js/wb-labels.js) so the eventual deep rename — and
any future wording tweak — is a one-line change here.

Keep this file and wb-labels.js in lock-step: they are two views of one source of truth.
"""

# key → user-facing label (singular). Add plural/verb variants only when a template needs them.
LABELS = {
    # datasets.Dataset once PUBLISHED — the code/DB name stays "Dataset"; unpublished it is "dataset".
    'dataset': 'Dataset',
    'dataset_plural': 'Datasets',
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
    # single-record correction (record-level check-out).
    'place_record': 'Record correction',
    # gazetteer records correction (dataset check-out: subset / whole-that-fits).
    'dataset_edit': 'Gazetteer records correction',
    # v4 placeholders.
    'route': 'Route',
    'network': 'Network',
    # the tool itself.
    'workbench': 'Collaborative Workbench',
}


def label(key):
    """Return the user-facing label for ``key`` (falls back to the key itself if unknown)."""
    return LABELS.get(key, key)
