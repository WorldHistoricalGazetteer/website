# datasets/core_utils.py
"""
Core utilities with NO Django model imports.
Safe to import anywhere without circular dependency issues.
"""
import time
import datetime
import json
import logging

logger = logging.getLogger(__name__)


def makeNow():
    """Generate timestamp string for current time"""
    ts = time.time()
    sttime = datetime.datetime.fromtimestamp(ts).strftime('%Y%m%d_%H%M%S')
    return sttime


def elapsed(delta):
    """Format timedelta as MM:SS string"""
    minutes, seconds = divmod(delta.seconds, 60)
    return '{:02}:{:02}'.format(int(minutes), int(seconds))


def parse_wkt(g):
    """
    Parse WKT string to GeoJSON feature.

    Args:
        g: WKT string representation of geometry

    Returns:
        dict: GeoJSON feature

    Raises:
        ValueError: If coordinates are outside valid range
    """
    from shapely.geometry import mapping
    from shapely.wkt import loads as wkt_loads

    # Load the geometry from the WKT string
    gw = wkt_loads(g)

    # Get the bounding box of the geometry
    minx, miny, maxx, maxy = gw.bounds

    # Check if the bounding box's coordinates are within the valid range
    if not (-180 <= minx <= 180 and -90 <= miny <= 90 and -180 <= maxx <= 180 and -90 <= maxy <= 90):
        raise ValueError("Invalid coordinates in WKT geometry")

    # Convert the geometry to a GeoJSON feature
    feature = json.loads(json.dumps(mapping(gw)))

    return feature


def flatten(l):
    """
    Flattens nested tuple list.

    Args:
        l: Nested list/tuple structure

    Yields:
        Flattened elements
    """
    for el in l:
        if isinstance(el, tuple) and any(isinstance(sub, tuple) for sub in el):
            for sub in flatten(el):
                yield sub
        else:
            yield el


class HitRecord(object):
    """
    Lightweight data structure for reconciliation hits.
    No Django dependencies - safe for tasks and models.
    """

    def __init__(self, place_id, dataset, src_id, title):
        self.place_id = place_id
        self.src_id = src_id
        self.title = title
        self.dataset = dataset

    def __str__(self):
        return json.dumps(str(self.__dict__))

    def toJSON(self):
        return json.loads(json.dumps(self.__dict__, indent=2))


class PlaceMapper(object):
    """Mapper object for place data without Django dependencies"""

    def __init__(self, id, src_id, title):
        self.id = id
        self.src_id = src_id
        self.title = title

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def __getitem__(self, key):
        return getattr(self, key)

    def __str__(self):
        return json.dumps(str(self.__dict__))

    def toJSON(self):
        return json.loads(json.dumps(self.__dict__, indent=2))


def makeCoords(lonstr, latstr):
    """
    Convert longitude/latitude strings to coordinate array.

    Args:
        lonstr: Longitude as string
        latstr: Latitude as string

    Returns:
        list: [lon, lat] or empty list if invalid
    """
    lon = float(lonstr) if lonstr not in ['', 'nan', None] else ''
    lat = float(latstr) if latstr not in ['', 'nan', None] else ''
    coords = [] if (lonstr == '' or latstr == '') else [lon, lat]
    return coords


def roundy(x, direct="up", base=10):
    """
    Round number to nearest base value.

    Args:
        x: Number to round
        direct: "up" or "down"
        base: Base to round to (default 10)

    Returns:
        int: Rounded value
    """
    import math
    if direct == "down":
        return int(math.ceil(x / 10.0)) * 10 - base
    else:
        return int(math.ceil(x / 10.0)) * 10


def fixName(toponym):
    """
    Fix common toponym formatting issues.

    Args:
        toponym: Place name string

    Returns:
        str: Corrected toponym
    """
    import re
    search_name = toponym
    r1 = re.compile(r"(.*?), Gulf of")
    r2 = re.compile(r"(.*?), Sea of")
    r3 = re.compile(r"(.*?), Cape")
    r4 = re.compile(r"^'")

    if bool(re.search(r1, toponym)):
        search_name = "Gulf of " + re.search(r1, toponym).group(1)
    if bool(re.search(r2, toponym)):
        search_name = "Sea of " + re.search(r2, toponym).group(1)
    if bool(re.search(r3, toponym)):
        search_name = "Cape " + re.search(r3, toponym).group(1)
    if bool(re.search(r4, toponym)):
        search_name = toponym[1:]

    return search_name if search_name != toponym else toponym


def parsedates_tsv(dates):
    """
    Parse date tuple into timespans object.

    Args:
        dates: Tuple of (start, end, attestation_year)

    Returns:
        dict: Object with timespans and minmax, or None if invalid
    """
    s, e, attestation_year = dates

    if s and e:
        s_yr = s.year
        e_yr = e.year
        timespans = {
            "start": {"earliest": s.isoformat()},
            "end": {"latest": e.isoformat()}
        }
        minmax = [s_yr, e_yr]
    elif s and not e:
        s_yr = s.year
        timespans = {"start": {"in": s.isoformat()}}
        minmax = [s_yr, s_yr]
    elif attestation_year:
        s_yr = attestation_year
        timespans = {"start": {"in": str(attestation_year)}}
        minmax = [attestation_year, attestation_year]
    else:
        return None

    return {"timespans": [timespans], "minmax": minmax}