# validation/coordinates.py
"""
Coordinate parsing, range-checking and repair for delimited (LP-TSV/CSV/Excel) uploads.

Malformed `lon`/`lat` cells used to vanish silently: `float()` raised, the converter
returned `None`, the nested `coordinates` array was never assigned, and the feature was
written out with `geometry: null`. The contributor was told the file was valid and only
discovered the loss from the map preview — or never. See place#212.

Order of operations matters:

    de-locale  ->  range-check  ->  test for transposition

A locale artefact ('-8,518,098', a decimal comma reinterpreted by Excel as thousands
separators) otherwise masquerades as an out-of-range value, and a transposed pair
otherwise masquerades as an out-of-range latitude.

Nothing here fails quietly: every cell either yields a coordinate, a reported repair, or
a reported error naming the column and the offending value.
"""

import logging
import re

logger = logging.getLogger('validation')

LON_LIMIT = 180.0
LAT_LIMIT = 90.0

# A number whose commas are grouped as thousands separators: '-8,518,098', '1,234'.
_THOUSANDS_GROUPED = re.compile(r'^[+-]?\d{1,3}(?:,\d{3})+$')

# Anything we are willing to strip separators out of: digits, sign, comma, dot, spaces.
_NUMERIC_ISH = re.compile(r'^[+-]?[\d\s,.  ]+$')

# Unicode dashes that spreadsheets and word processors substitute for a hyphen-minus.
_MINUS_VARIANTS = str.maketrans({'−': '-', '‒': '-', '–': '-', '—': '-'})

# Degrees of slop allowed when testing a point against a country bounding box: coastlines,
# generalised outlines and offshore places all sit a little outside the box.
_BBOX_PAD = 1.0

_bbox_cache = {}


def is_blank(value):
    """True for None, empty/whitespace strings and NaN (pandas leaves both about)."""
    if value is None:
        return True
    if isinstance(value, float) and value != value:  # NaN
        return True
    return str(value).strip() == ''


def _normalise(text):
    """Tidy a raw cell into something `float()` has a fair chance with."""
    text = str(text).strip().translate(_MINUS_VARIANTS)
    if _NUMERIC_ISH.match(text):
        # Space-grouped thousands ('-8 518 098') and stray non-breaking spaces.
        text = re.sub(r'[\s  ]', '', text)
    return text


def _candidate_values(text):
    """
    Reinterpretations of a cell that `float()` refused, in no particular order.

    Only ever generated for values containing a comma — the locale artefact this is
    written for. Which candidate (if any) is correct is decided by the range check in
    `_parse_cell`, not here.
    """
    candidates = []

    def add(rewritten):
        try:
            candidates.append(float(rewritten))
        except ValueError:
            pass

    if ',' not in text:
        return candidates

    if '.' in text:
        if text.rindex(',') > text.rindex('.'):
            add(text.replace('.', '').replace(',', '.'))  # '1.234,56' — dots group, comma decimal
        else:
            add(text.replace(',', ''))  # '1,234.56' — commas group, dot decimal
        return candidates

    add(text.replace(',', '.'))  # a single comma acting as the decimal separator
    if _THOUSANDS_GROUPED.match(text):
        add(text.replace(',', ''))  # commas genuinely grouping thousands
    head, _, tail = text.partition(',')
    add(f"{head}.{tail.replace(',', '')}")  # '-8,518,098' -> '-8.518098'

    return candidates


def _fmt(value):
    """Format a coordinate for a message without %g's six-significant-figure truncation."""
    text = f"{value:.10g}"
    return text


def _parse_cell(column, raw):
    """
    Parse one `lon`/`lat` cell.

    :return: (value, repair, error) — exactly one of `value`/`error` is meaningful;
             `repair` is a description string when the value had to be reinterpreted.

    A blank cell is not an error here: `(None, None, None)`. Both axes blank means the
    place simply has no point, which is legitimate.
    """
    if is_blank(raw):
        return None, None, None

    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        return float(raw), None, None

    text = _normalise(raw)
    try:
        return float(text), None, None
    except ValueError:
        pass

    # De-locale. A candidate is only credible if it lands within the widest coordinate
    # range there is; the axis-specific check follows, once transposition is settled.
    candidates = _candidate_values(text)
    seen, plausible = set(), []
    for value in candidates:
        if abs(value) <= LON_LIMIT:
            key = round(value, 10)
            if key not in seen:
                seen.add(key)
                plausible.append(value)

    if len(plausible) == 1:
        value = plausible[0]
        return value, (f"Column <code>{column}</code>: read {_show(raw)} as {_fmt(value)} "
                       f"(comma treated as a decimal separator)."), None

    if len(plausible) > 1:
        readings = ', '.join(_fmt(v) for v in plausible)
        return None, None, (f"Column <code>{column}</code>: {_show(raw)} is ambiguous - it could be read as "
                            f"{readings}. Please correct it in the source file.")

    if candidates:
        readings = ', '.join(_fmt(v) for v in candidates)
        return None, None, (f"Column <code>{column}</code>: {_show(raw)} reads as {readings}, which is outside "
                            f"the valid coordinate range.")

    return None, None, (f"Column <code>{column}</code>: {_show(raw)} could not be read as a number.")


def _show(raw):
    """Render a raw cell for a message without letting it break out of the report HTML."""
    text = str(raw).strip()
    if len(text) > 40:
        text = text[:37] + '…'
    text = (text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;'))
    return f"'{text}'"


def _country_bbox(ccodes):
    """
    Union of the bounding boxes of the row's countries, or None when unavailable.

    Used only to break a transposition tie that the ranges cannot. Any failure here
    (no database, no `Country` row, no bbox) simply means the tie is left unbroken.
    """
    boxes = []
    for ccode in ccodes or []:
        code = str(ccode).strip().upper()
        if not code:
            continue
        if code not in _bbox_cache:
            extent = None
            try:
                from areas.models import Country
                country = Country.objects.filter(iso__iexact=code).first()
                if country and country.bbox:
                    extent = tuple(country.bbox.extent)
            except Exception as e:  # database unavailable, table missing, etc.
                logger.debug(f"Country bbox lookup failed for '{code}': {e}")
            _bbox_cache[code] = extent
        if _bbox_cache[code]:
            boxes.append(_bbox_cache[code])

    if not boxes:
        return None
    return (min(b[0] for b in boxes), min(b[1] for b in boxes),
            max(b[2] for b in boxes), max(b[3] for b in boxes))


def _in_box(lon, lat, box):
    minx, miny, maxx, maxy = box
    return (minx - _BBOX_PAD) <= lon <= (maxx + _BBOX_PAD) and (miny - _BBOX_PAD) <= lat <= (maxy + _BBOX_PAD)


def resolve_lonlat(lon_raw, lat_raw, ccodes=None):
    """
    Turn a row's raw `lon`/`lat` cells into a checked coordinate pair.

    :param ccodes: the row's country codes, used only to break an otherwise undecidable
                   transposition; may be None.
    :return: (lon, lat, repairs, errors). `lon`/`lat` are both floats or both None;
             `repairs` and `errors` are lists of description strings for the report.
    """
    repairs, errors = [], []

    lon, lon_repair, lon_error = _parse_cell('lon', lon_raw)
    lat, lat_repair, lat_error = _parse_cell('lat', lat_raw)

    for repair in (lon_repair, lat_repair):
        if repair:
            repairs.append(repair)
    for error in (lon_error, lat_error):
        if error:
            errors.append(error)

    if errors:
        return None, None, repairs, errors

    if lon is None and lat is None:
        return None, None, repairs, errors

    # A one-element `coordinates` array used to reach GEOSGeometry and blow up on insert,
    # long after the contributor had been told the file was valid.
    if lon is None or lat is None:
        missing, present = ('lon', 'lat') if lon is None else ('lat', 'lon')
        errors.append(f"Column <code>{missing}</code> is empty but <code>{present}</code> is not — "
                      f"a point needs both.")
        return None, None, repairs, errors

    lon_in_range = abs(lon) <= LON_LIMIT
    lat_in_range = abs(lat) <= LAT_LIMIT

    if lon_in_range and lat_in_range:
        # Both plausible as given. Only a country bounding box can show that the columns
        # are nonetheless transposed, and only when the swap is unambiguously better.
        if abs(lon) <= LAT_LIMIT:
            box = _country_bbox(ccodes)
            if box and not _in_box(lon, lat, box) and _in_box(lat, lon, box):
                repairs.append(f"Columns <code>lon</code> and <code>lat</code> appear transposed: "
                               f"({_fmt(lon)}, {_fmt(lat)}) falls outside the country given in "
                               f"<code>ccodes</code>, whereas ({_fmt(lat)}, {_fmt(lon)}) falls within it. "
                               f"Swapped.")
                return lat, lon, repairs, errors
        return lon, lat, repairs, errors

    # Transposed: a latitude out of range that is a valid longitude, paired with a
    # longitude that is a valid latitude, is certainly the columns the wrong way round.
    if abs(lat) <= LON_LIMIT and abs(lon) <= LAT_LIMIT:
        repairs.append(f"Columns <code>lon</code> and <code>lat</code> were transposed "
                       f"(lat {_fmt(lat)} is outside the range -{_fmt(LAT_LIMIT)} to {_fmt(LAT_LIMIT)}): "
                       f"swapped to lon={_fmt(lat)}, lat={_fmt(lon)}.")
        return lat, lon, repairs, errors

    if not lon_in_range:
        errors.append(f"Column <code>lon</code>: {_fmt(lon)} is outside the valid range "
                      f"-{_fmt(LON_LIMIT)} to {_fmt(LON_LIMIT)}.")
    if not lat_in_range:
        errors.append(f"Column <code>lat</code>: {_fmt(lat)} is outside the valid range "
                      f"-{_fmt(LAT_LIMIT)} to {_fmt(LAT_LIMIT)}.")
    return None, None, repairs, errors


def check_coordinate_pair(coordinates):
    """
    Guard a GeoJSON position before it reaches GEOSGeometry.

    :return: an error description, or None when the position is a usable [lon, lat].
    """
    if not isinstance(coordinates, (list, tuple)):
        return "Geometry coordinates are missing."
    if len(coordinates) < 2:
        return (f"Geometry coordinates has {len(coordinates)} element"
                f"{'' if len(coordinates) == 1 else 's'}; a position needs both a longitude "
                f"and a latitude.")
    lon, lat = coordinates[0], coordinates[1]
    if not isinstance(lon, (int, float)) or not isinstance(lat, (int, float)):
        return "Geometry coordinates are not numbers."
    if abs(lon) > LON_LIMIT or abs(lat) > LAT_LIMIT:
        return (f"Geometry coordinates ({_fmt(lon)}, {_fmt(lat)}) are outside the valid range "
                f"(longitude +/-{_fmt(LON_LIMIT)}, latitude +/-{_fmt(LAT_LIMIT)}).")
    return None
