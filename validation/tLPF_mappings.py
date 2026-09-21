# tLPF_mappings.py

import re
import logging

logger = logging.getLogger('validation')

# Regex to capture toponyms and RFC 5646 language tags
pattern = re.compile(r'''
    ^(?P<toponym>[^\s@]+)
    (?:@(?P<language>
        (?:(?P<language_code>[A-Za-z]{2,3})(?:-(?P<language_code_ext>[A-Za-z]{3}(-[A-Za-z]{3}){0,2}))?|[A-Za-z]{4}|[A-Za-z]{5,8})
        (-(?P<script>[A-Za-z]{4}))?
        (-(?P<region>[A-Za-z]{2}|[0-9]{3}))?
        (-(?P<variant>[A-Za-z0-9]{5,8}|[0-9][A-Za-z0-9]{3}))*
        (-(?P<extension>[0-9A-WY-Za-wy-z](-[A-Za-z0-9]{2,8})+))?
    )?)$
''', re.VERBOSE)


def variant_conversion(x):
    if not x:
        return []

    variants = []
    for variant in (v.strip() for v in x.split(';') if v.strip()):
        match = pattern.match(variant)
        if match:
            groups = match.groupdict()
            toponym = groups.get('toponym', '')
            variant_entry = {
                'toponym': toponym
            }

            lang_data = {k: v for k, v in groups.items() if k != 'toponym' and v}
            if lang_data:
                variant_entry['bcp47'] = lang_data

            lang = lang_data.get('language', None)
            if lang:
                variant_entry['lang'] = lang

            variants.append(variant_entry)
        else:
            variants.append({
                'toponym': variant.strip()
            })

    logger.debug(variants)

    return variants


def safe_float_conversion(x):
    """Safely convert input to float, handling empty strings and None."""
    if x is None or str(x).strip() == "":
        return None
    try:
        return float(str(x).strip())
    except ValueError:
        return None


def _has_value(x):
    """True when a cell actually holds something (place#278).

    `str_x` stringifies before testing, so `None` and `float('nan')` survive it as
    the literal strings 'None' and 'nan'. Anything relying on `str_x(x) or None`
    to mean "empty" is therefore wrong for a missing cell. Tested explicitly rather
    than assumed, because the strings are truthy and pass the `pd.notna` guard
    applied downstream.
    """
    if x is None:
        return False
    if isinstance(x, float) and x != x:   # NaN is the only value unequal to itself
        return False
    return True


def str_x(x, split=False):
    """
    Convert to string and remove '.0' if it's a float ending with .0.
    pandas read_excel is very uncooperative with regard to forcing dtype, and infers type regardless of configuration
    """
    stripped = re.sub(r'\.0$', '', str(x).strip())  # Remove any trailing '.0'
    if not stripped:
        if split:
            return []
        return None
    elif split:
        return stripped.split(';')
    else:
        return stripped


tLPF_mappings = {
    'id': {
        'lpf': '@id',
        'converter': lambda x: str_x(x)
    },
    'title': {
        'lpf': 'names.0.toponym',
        'converter': lambda x: str_x(x)
    },
    'title_source': {
        'lpf': 'names.0.citations.0.label',
        'converter': lambda x: str_x(x)
    },
    'fclasses': {
        'lpf': 'properties.fclasses',
        'converter': lambda x: [item.strip() for item in str_x(x, True) if item.strip()] or None
    },
    'aat_types': {
        'lpf': 'types',
        'converter': lambda x: [{'identifier': f'aat:{item.strip()}'} for item in str_x(x, True) if
                                item.strip()] or None
    },
    'attestation_year': {
        'lpf': 'names.0.citations.0.year',
        'converter': lambda x: str_x(x)
    },
    'start': {
        'lpf': 'when.timespans.0.start.in',
        'converter': lambda x: str_x(x)
    },
    'end': {
        'lpf': 'when.timespans.0.end.in',
        'converter': lambda x: str_x(x)
    },
    'title_uri': {
        'lpf': 'names.0.citations.0.@id',
        'converter': lambda x: str_x(x)
    },
    'ccodes': {
        'lpf': 'properties.ccodes',
        'converter': lambda x: [item.strip() for item in str_x(x, True) if item.strip()] or None
    },
    'matches': {
        'lpf': 'links',
        'converter': lambda x: [{'type': 'exactMatch', 'identifier': item.strip()} for item in str_x(x, True) if
                                item.strip()] or None
    },
    'variants': {
        'lpf': 'additional_names',
        'converter': lambda x: variant_conversion(x)
    },
    'types': {
        'lpf': 'additional_types',
        'converter': lambda x: [{'label': item.strip()} for item in str_x(x, True) if item.strip()] or None
    },
    # place#278 — `parent_name` alone must NOT emit a relation.
    #
    # `relations[]` requires BOTH `relationType` and `relationTo` (lpf_v2.0.jsonld,
    # definitions.relations.items.allOf[1].required). Emitting the label without a
    # `relationTo` produced a file that failed validation naming `relationTo` — a
    # field the contributor had never filled in — so the error pointed at a column
    # they had not used and the natural reading was that something else was wrong.
    #
    # A contributor with a parent NAME and no stable parent IDENTIFIER is the normal
    # case for historical data: the name is what the source records. So the coupling
    # is resolved in their favour — the relation is emitted only when `parent_id`
    # supplies the `relationTo` that makes it valid.
    #
    # The label is carried to `parent_name_unresolved` instead of being discarded, so
    # the value survives into the record rather than being silently dropped. Prefer
    # MyD's containment chain, which handles arbitrary depth as plain columns.
    'parent_name': {
        'lpf': 'properties.parent_name_unresolved',
        # ⚠️ Guarded rather than relying on `str_x` alone: `str_x` is `str(x).strip()`,
        # so a missing cell becomes the LITERAL string 'None' or 'nan' — truthy, and
        # non-null to the `pd.notna` guard at the assignment site, so it would be
        # written into the record as if the contributor had typed it. Scoped to this
        # column; `str_x` is shared by many others and is not changed here.
        'converter': lambda x: (str_x(x) or None) if _has_value(x) else None
    },
    'parent_id': {
        'lpf': 'relations.0.relationTo',
        'converter': lambda x: str_x(x)
    },
    'lon': {
        'lpf': 'geometry.coordinates.0',
        'converter': lambda x: safe_float_conversion(x)
    },
    'lat': {
        'lpf': 'geometry.coordinates.1',
        'converter': lambda x: safe_float_conversion(x)
    },
    'geowkt': {
        'lpf': 'geowkt',
        'converter': lambda x: str_x(x)
    },
    'geo_source': {
        'lpf': 'geometry.citations.0.label',
        'converter': lambda x: str_x(x)
    },
    'geo_id': {
        'lpf': 'geometry.citations.0.@id',
        'converter': lambda x: str_x(x)
    },
    'description': {
        'lpf': 'descriptions.0.value',
        'converter': lambda x: str_x(x)
    },
    'approximation': {
        'lpf': 'geometry.approximation',
        'converter': lambda x: (
            {'type': 'geo:hasSpatialAccuracy', 'tolerance': safe_float_conversion(x)}
            if safe_float_conversion(x) is not None
            else {'type': str_x(x)} if str_x(x) else None
        )
    }
}
