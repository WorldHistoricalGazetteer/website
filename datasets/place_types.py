# datasets/place_types.py
"""
Shared derivation of PlaceTypes and feature classes from a delimited (LP-TSV) row.

The insert path (`datasets.insert.process_types`) and the update path
(`datasets.views.update_rels_tsv`) each carried their own copy of this logic and drifted:
the update path never read the `fclasses` column and overwrote what insert had merged, so a
contributor lost the column the first time they re-uploaded a corrected file. One function
now serves both. See place#213.

Derivation uses the whole `Type.fclasses` array. The back-compat `Type.fclass` property
returns `fclasses[0]` of a *sorted* list, so both `cities` and `quilombos` (each ['A', 'P'])
derived 'A' — administrative — and dropped out of populated-place filtering entirely.
583 of the ~59,000 AAT concepts carry more than one feature class.
"""

import logging
from itertools import zip_longest

logger = logging.getLogger(__name__)

# The three columns any of which means "this row says something about place type".
TYPE_COLUMNS = ('types', 'aat_types', 'fclasses')

# Values pandas and the CSV readers leave behind for an empty cell.
_BLANKS = {'', 'nan', 'none', 'null'}

_aat_index = None


def aat_index(refresh=False):
    """
    `{aat_id: {'fclasses': [...], 'term': str, 'term_full': str}}`, built once per process.

    Replaces both a full `Type` table scan at `datasets.insert` import time and — far worse —
    a `Type.objects.values_list('aat_id', flat=True)` issued once per type per row inside the
    update loop. Pass `refresh=True` after an AAT sync in a long-lived process.
    """
    global _aat_index
    if _aat_index is None or refresh:
        from places.models import Type
        _aat_index = {
            t.aat_id: {
                'fclasses': [fc for fc in (t.fclasses or []) if fc],
                'term': t.term,
                'term_full': t.term_full,
            }
            for t in Type.objects.only('aat_id', 'fclasses', 'term', 'term_full')
        }
        logger.debug(f"Built AAT index of {len(_aat_index)} concepts.")
    return _aat_index


def valid_fclasses():
    """The feature-class letters the `Place.fclasses` column accepts."""
    from main.choices import FEATURE_CLASSES
    return {code for code, _ in FEATURE_CLASSES}


def _is_blank(value):
    if value is None:
        return True
    if isinstance(value, float) and value != value:  # NaN
        return True
    return str(value).strip().lower() in _BLANKS


def has_type_columns(row):
    """
    True when the row says anything about place type.

    The insert path has always used this three-column test; the update path tested `types`
    alone, so a row carrying `aat_types`/`fclasses` but no `types` had its PlaceTypes deleted
    and never rebuilt.
    """
    return any(not _is_blank(row.get(column)) for column in TYPE_COLUMNS)


def parse_type_columns(row):
    """
    Split a row's `types`, `aat_types` and `fclasses` cells.

    :return: (types, aat_types, fclasses_from_row). An `aat_types` entry that is not an
             integer is dropped with a warning rather than discarding the whole list, which
             is what both previous implementations did.
    """
    def split(column):
        value = row.get(column)
        if _is_blank(value):
            return []
        return [part.strip() for part in str(value).split(';') if part.strip()]

    types = split('types')

    aat_types = []
    for token in split('aat_types'):
        try:
            aat_types.append(int(token.removeprefix('aat:')))
        except ValueError:
            logger.warning(f"Ignoring unreadable aat_types value {token!r} in row "
                           f"{row.get('id', '(no id)')!r}.")

    allowed = valid_fclasses()
    fclasses_from_row = []
    for token in split('fclasses'):
        letter = token.upper()[:1]
        if letter in allowed:
            fclasses_from_row.append(letter)
        else:
            logger.warning(f"Ignoring unrecognised fclasses value {token!r} in row "
                           f"{row.get('id', '(no id)')!r}.")

    return types, aat_types, fclasses_from_row


def aat_id_from_identifier(identifier):
    """`'aat:300008389'` -> `300008389`; anything else -> None."""
    if not isinstance(identifier, str) or not identifier.startswith('aat:'):
        return None
    try:
        return int(identifier[4:])
    except ValueError:
        return None


def fclasses_for_aat(aat_id):
    """Every feature class the AAT concept carries — not just the first."""
    if aat_id is None:
        return []
    return list(aat_index().get(aat_id, {}).get('fclasses') or [])


def fclasses_for_row(aat_types, fclasses_from_row):
    """
    The row's full feature-class set: everything its AAT types carry, plus its own
    `fclasses` column. Order preserved, de-duplicated.
    """
    resolved = []
    for aat_id in aat_types:
        for fclass in fclasses_for_aat(aat_id):
            if fclass not in resolved:
                resolved.append(fclass)
    for fclass in fclasses_from_row:
        if fclass not in resolved:
            resolved.append(fclass)
    return resolved


def place_type_objects(place, types, aat_types):
    """
    Unsaved PlaceType rows for the place.

    `types` and `aat_types` are paired positionally with `zip_longest`, so a row with fewer
    `aat_types` than `types` — which LP-TSV explicitly permits — keeps the identifiers it
    does have. The update path's `len(aat_types) >= len(types)` test was all-or-nothing and
    then crashed on `None[4:]`.
    """
    from places.models import PlaceType

    index = aat_index()
    objects = []
    for type_, aat_id in zip_longest(types, aat_types, fillvalue=None):
        entry = index.get(aat_id, {}) if aat_id is not None else {}
        objects.append(PlaceType(
            place=place,
            src_id=place.src_id,
            aat_id=aat_id,
            jsonb={
                'sourceLabel': type_ or '',
                'identifier': f'aat:{aat_id}' if aat_id is not None else '',
                'label': entry.get('term_full') or entry.get('term') or '',
            },
        ))
    return objects


def apply_types_to_place(place, row, save=True):
    """
    Set `place.fclasses` from the row and return its unsaved PlaceType objects.

    The single entry point for both the insert and the update path — the duplication is what
    let them drift.
    """
    types, aat_types, fclasses_from_row = parse_type_columns(row)
    place.fclasses = fclasses_for_row(aat_types, fclasses_from_row)
    if save:
        place.save()
    return place_type_objects(place, types, aat_types)
