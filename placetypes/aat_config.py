# placetypes/aat_config.py
"""
AAT (Art & Architecture Thesaurus) configuration for WHG place types.

Top-down approach: we walk the AAT hierarchy from a small number of broad
entry points and EXCLUDE the few subtrees that are clearly not place types.
Every other descendant is a valid WHG place type.

fclass assignment is multi-valued: each concept inherits fclass letters
from ALL mapped ancestor nodes reachable via any path (polyhierarchy).

AAT data source:
  https://vocab.getty.edu/dataset/aat/
  Explicit N-Triples dump: explicit.zip (~93 MB compressed)
  See: https://vocab.getty.edu/doc/#Export_Files  §6.6.1
"""

# ---------------------------------------------------------------------------
# AAT bulk download URL  (explicit export — separate .nt files)
# ---------------------------------------------------------------------------
AAT_EXPLICIT_DUMP_URL = "http://aatdownloads.getty.edu/VocabData/explicit.zip"

# Local path (relative to BASE_DIR) where the downloaded dump is cached
AAT_DUMP_CACHE_DIR = "data/aat"

# Filename for the metadata file that tracks last-modified / etag
AAT_DUMP_META_FILE = "aat_dump_meta.json"

# ---------------------------------------------------------------------------
# Files inside explicit.zip that we parse
# ---------------------------------------------------------------------------
AAT_NT_HIERARCHICAL_RELS = "AATOut_HierarchicalRels.nt"
AAT_NT_TERMS = "AATOut_2Terms.nt"
AAT_NT_SCOPE_NOTES = "AATOut_ScopeNotes.nt"

# ---------------------------------------------------------------------------
# RDF predicates used when parsing the AAT N-Triples
# ---------------------------------------------------------------------------
GVP_BROADER_PREFERRED = "http://vocab.getty.edu/ontology#broaderPreferred"
GVP_BROADER_GENERIC = "http://vocab.getty.edu/ontology#broaderGeneric"
SKOSXL_PREF_LABEL = "http://www.w3.org/2008/05/skos-xl#prefLabel"
SKOSXL_LITERAL_FORM = "http://www.w3.org/2008/05/skos-xl#literalForm"
SKOS_SCOPE_NOTE = "http://www.w3.org/2004/02/skos/core#scopeNote"
RDF_VALUE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#value"

# AAT URI prefixes
AAT_URI_PREFIX = "http://vocab.getty.edu/aat/"
AAT_TERM_URI_PREFIX = "http://vocab.getty.edu/aat/term/"
AAT_SCOPE_NOTE_URI_PREFIX = "http://vocab.getty.edu/aat/scopeNote/"

# ---------------------------------------------------------------------------
# Entry points: broad AAT nodes whose descendants we walk
# ---------------------------------------------------------------------------
AAT_ENTRY_POINTS = [
    300264550,   # Built Environment (hierarchy name)
    300182722,   # geographic regions (Associated Concepts Facet)
    300232420,   # sovereign states (Agents Facet)
]

# ---------------------------------------------------------------------------
# Excluded subtrees: NOT place types — skip these and all descendants
# ---------------------------------------------------------------------------
AAT_EXCLUDED_SUBTREES = {
    300266061,   # vegetation (1,083 concepts — plant species/communities)
    300266157,   # extraterrestrial bodies (23 concepts — stars, galaxies)
}

# ---------------------------------------------------------------------------
# fclass assignment map
# ---------------------------------------------------------------------------
# Intermediate AAT nodes that define fclass category boundaries.
# Each concept accumulates fclass letters from ALL mapped ancestors
# reachable via any broader path (polyhierarchy).
#
AAT_FCLASS_MAP = {
    # A: Administrative / political
    300232420: 'A',   # sovereign states
    300120579: 'A',   # <settlements by function: administrative>
    300387506: 'A',   # countries (sovereign states)
    300261086: 'A',   # political administrative bodies

    # P: Populated places
    300008347: 'P',   # inhabited places

    # S: Structures, sites, buildings
    300004790: 'S',   # single built works (built environment)
    300000202: 'S',   # complexes (buildings and sites)
    300078073: 'S',   # site elements

    # R: Transportation
    300120693: 'R',   # transportation structures

    # L: Regions, landscape areas, open spaces
    300008072: 'L',   # open spaces
    300008932: 'L',   # cultural landscapes
    300182722: 'L',   # geographic regions
    300000705: 'L',   # districts

    # T: Terrestrial landforms
    300266060: 'T',   # landforms (terrestrial)

    # H: Water bodies
    300266059: 'H',   # bodies of water (natural)

    # U: Undersea features
    300387581: 'U',   # undersea landforms
}

# ---------------------------------------------------------------------------
# Tree widget display overrides
# ---------------------------------------------------------------------------
# Nodes promoted to the root level of the type-tree widget.
AAT_TREE_PROMOTE_TO_ROOT = {
    300008347,   # inhabited places — extracted as a top-level entry
    300132294,   # natural landscapes — extracted as a top-level entry
}

# Nodes removed from the tree display; their children (minus promoted ones)
# are reparented to the grandparent node.
AAT_TREE_SKIP_NODES = {
    300008346,   # Settlements and Landscapes (hierarchy name)
}

# ---------------------------------------------------------------------------
# Friendly labels for the search UI filter categories
# ---------------------------------------------------------------------------
CATEGORY_LABELS = {
    "A": "Administrative entities",
    "P": "Cities, towns, hamlets",
    "S": "Sites, buildings, complexes",
    "R": "Roads, routes, rail",
    "L": "Regions, landscape areas",
    "T": "Terrestrial landforms",
    "H": "Water bodies",
    "U": "Undersea features",
}
