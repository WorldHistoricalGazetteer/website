# placetypes/aat_config.py
"""
AAT (Art & Architecture Thesaurus) configuration for WHG place types.

Defines the root nodes of the AAT hierarchy that correspond to place-type
concepts. All descendants of these root nodes are considered valid WHG
place types.

Each root maps to a legacy GeoNames fclass letter for backward compatibility.

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

# Hierarchical parent — the explicit export has direct triples for both:
#   <child> gvp:broaderPreferred <parent> .   (canonical single parent)
#   <child> gvp:broaderGeneric   <parent> .   (additional poly-hierarchy)
GVP_BROADER_PREFERRED = "http://vocab.getty.edu/ontology#broaderPreferred"
GVP_BROADER_GENERIC = "http://vocab.getty.edu/ontology#broaderGeneric"

# SKOS-XL labels  (two-hop: concept → term URI → literal)
#   <concept> skos-xl:prefLabel <term-URI> .
#   <term-URI> skos-xl:literalForm "label"@en .
SKOSXL_PREF_LABEL = "http://www.w3.org/2008/05/skos-xl#prefLabel"
SKOSXL_LITERAL_FORM = "http://www.w3.org/2008/05/skos-xl#literalForm"

# Scope notes  (two-hop: concept → note URI → text)
#   <concept> skos:scopeNote <note-URI> .
#   <note-URI> rdf:value "text"@en .
SKOS_SCOPE_NOTE = "http://www.w3.org/2004/02/skos/core#scopeNote"
RDF_VALUE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#value"

# AAT URI prefixes
AAT_URI_PREFIX = "http://vocab.getty.edu/aat/"
AAT_TERM_URI_PREFIX = "http://vocab.getty.edu/aat/term/"
AAT_SCOPE_NOTE_URI_PREFIX = "http://vocab.getty.edu/aat/scopeNote/"

# ---------------------------------------------------------------------------
# Place-type root nodes
# ---------------------------------------------------------------------------
# Each entry: (aat_id, fclass, label, description)
#
# These are the top-level AAT concepts whose descendants form the WHG
# place-type vocabulary. The fclass letter maps to the legacy GeoNames
# feature class for backward compatibility.
#
# Verify any ID at: http://vocab.getty.edu/page/aat/<id>
#
# NOTE: Multiple roots can map to the same fclass. When the UI groups
# by fclass it will merge them automatically.
#
AAT_PLACE_TYPE_ROOTS = [
    # --- A: Administrative / political ---
    (300387506, "A", "political divisions",
     "Administrative and political entities: countries, provinces, counties, municipalities, etc."),

    # --- P: Populated places ---
    (300008347, "P", "inhabited places",
     "Cities, towns, villages, hamlets, and other populated places."),

    # --- S: Sites, structures, buildings ---
    (300004792, "S", "single built works",
     "Individual buildings, structures, monuments, bridges, and other built works."),

    (300000745, "S", "complexes",
     "Built complexes, districts, campuses, and compound structures."),

    (300006891, "S", "fortifications",
     "Defensive works: castles, forts, walls, and military structures."),

    (300000810, "S", "archaeological sites",
     "Sites of archaeological significance: ruins, tells, excavation sites."),

    (300007391, "S", "religious buildings",
     "Churches, mosques, temples, monasteries, and other religious structures."),

    (300004895, "S", "agricultural structures",
     "Farms, granaries, mills, irrigation works, and agricultural buildings."),

    (300121918, "S", "industrial structures",
     "Factories, mines, workshops, kilns, and industrial complexes."),

    # --- R: Routes / transportation ---
    (300007836, "R", "transportation structures",
     "Roads, routes, bridges, railways, canals, and other transportation infrastructure."),

    # --- L: Regions / landscape areas ---
    (300008178, "L", "open spaces and site elements",
     "Regions, landscape areas, parks, gardens, fields, and other open areas."),

    (300182722, "L", "geographic regions",
     "Named regions: continents, sub-regions, cultural areas, biomes."),

    # --- T: Terrestrial landforms ---
    (300266060, "T", "landforms (terrestrial)",
     "Terrestrial landforms: mountains, hills, valleys, plains, deserts, islands, etc."),

    # --- H: Water bodies ---
    (300008680, "H", "bodies of water",
     "Water features: rivers, lakes, seas, oceans, bays, springs, etc."),
]

# ---------------------------------------------------------------------------
# Derived lookup tables (built at import time from AAT_PLACE_TYPE_ROOTS)
# ---------------------------------------------------------------------------

FCLASS_TO_ROOTS = {}   # fclass letter -> list of root aat_ids
ROOT_TO_FCLASS = {}    # root aat_id -> fclass letter
ROOT_AAT_IDS = set()   # set of all root aat_ids

for _aat_id, _fclass, _label, _desc in AAT_PLACE_TYPE_ROOTS:
    ROOT_TO_FCLASS[_aat_id] = _fclass
    ROOT_AAT_IDS.add(_aat_id)
    FCLASS_TO_ROOTS.setdefault(_fclass, []).append(_aat_id)

# Friendly labels for the top-level filter categories shown in the search UI.
# Keyed by fclass letter for backward compatibility with the search template.
CATEGORY_LABELS = {
    "A": "Administrative entities",
    "P": "Cities, towns, hamlets",
    "S": "Sites, buildings, complexes",
    "R": "Roads, routes, rail",
    "L": "Regions, landscape areas",
    "T": "Terrestrial landforms",
    "H": "Water bodies",
}


