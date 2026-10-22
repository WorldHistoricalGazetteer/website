"""Canonical locus recipe: parse, canonicalise, encode, hash (place#172, place#325 Stage 1).

A *locus* (place#172) is a frozen recipe: the retrieval spec that fetched a set of
candidates, the literal clustering parameters the browser clusterer ran over them,
and the member ids of the one cluster that was meant. Resolving a locus re-runs the
recipe against the index as it currently stands; the members only settle *which*
cluster, they are not the answer.

This module is the ONE serialisation that serves both

(a) the unminted soft-cluster URL (shareable, not permanent, re-run on every visit),
    carried as a URL-safe token of the canonical bytes; and
(b) the locus PID (``/entity/locus:<PID>``), which is a hash of exactly those bytes.

It is pure Python: no Django, no models, no I/O. Minting and the registry are
Stage 4 and are not here. The browser twin is ``whg/webpack/js/locus_recipe.js``;
``scripts/fixtures/locus_recipe_vectors.json`` holds the cross-language vectors
and ``scripts/check-locus-recipe.mjs`` proves the twin agrees byte for byte.

Recipe, version 1
-----------------
::

    {
      "v": 1,
      "retrieval": {                         # the gateway POST /api/search body, as sent
        "query": "berlin",                   # optional; NFC, whitespace-collapsed, NOT case-folded
        "mode": "in",                        # required: exact | starts | in | fuzzy | phonetic
        "size": 100,                         # required: the candidate limit, 1..10000
        "ccodes": ["DE"],                    # optional; ISO-3166 alpha-2, upper, sorted, unique
        "types": ["aat:300008347"],          # optional; source type identifiers, sorted, unique
        "aat_types": [300008347],            # optional; AAT concept ids (int), sorted, unique
        "namespaces": ["gn", "wd"],          # optional; lower, sorted, unique
        "bounds": {GeoJSON geometry},        # optional; Polygon | MultiPolygon | GeometryCollection
        "contained_in": ["osm:r62422"],      # optional; place ids, sorted, unique
        "containment": "exact",              # required iff contained_in: fuzzy | exact
        "relation": "intersects",            # optional, only with bounds/contained_in: intersects | within
        "temporal": {                        # optional
          "start": -500, "end": 1500,        # ints, at least one, start <= end
          "mode": "possibly",                # required: possibly | definitely
          "undated": true                    # omitted when false
        }
      },
      "clustering": {                        # every value literal and required (never a params version)
        "weights": {"link": 0.15, "name": 0.35, "spatial": 0.2, "temporal": 0.15, "type": 0.15},
        "theta": 0.65,
        "tau_name": 0.75,
        "tau_link": 1,
        "same_ns_penalty": 0.15,
        "respect_distinct": true
      },                                     # NO stoplist: see "The stoplist is not in the recipe"
      "members": ["gn:2950159", "wd:Q64"]    # >= 1; sorted by code point, unique
    }

Canonical form
~~~~~~~~~~~~~~
* JSON with no whitespace; object keys sorted by Unicode code point; UTF-8.
* Strings are NFC-normalised; the whitespace class ``[\\t\\n\\v\\f\\r \\u00a0\\u1680
  \\u2000-\\u200a\\u2028\\u2029\\u202f\\u205f\\u3000\\ufeff]`` is collapsed to one
  space and trimmed; any remaining control character (Cc) or lone surrogate is an error.
  Strings are emitted as JSON (``json.dumps(ensure_ascii=False)`` == ``JSON.stringify``).
* Numbers: clustering scalars are quantised to 4 decimal places and coordinates to 6,
  rounding half-up on the SHORTEST decimal representation of the double (``repr`` in
  Python, ``String(x)`` in JS), so both languages decide ties identically. They are
  emitted as the minimal decimal: no exponent, no trailing zeros, no ``+``, ``-0`` is
  ``0``, integral values have no ``.0``. Integers (``size``, years, ``aat_types``)
  are emitted as integers. NaN/Infinity and booleans-as-numbers are errors.
* Lists whose order carries no meaning (``members``, ``ccodes``,
  ``types``, ``aat_types``, ``namespaces``, ``contained_in``, GeometryCollection
  members) are de-duplicated and sorted by code point (ints numerically; geometries
  by their canonical JSON). Polygon ring and vertex order IS kept: two rings with the
  same vertices in a different order are different recipes (geometric equivalence
  is not normalised; see "Edge cases").
* Absent means "no filter". An optional retrieval field equal to its no-filter value
  is stripped: ``query`` empty after cleaning, an empty list, ``bounds`` that is an
  empty GeometryCollection, ``undated: false``. A GeometryCollection holding one
  geometry is unwrapped to that geometry. Nothing in ``clustering`` is ever defaulted:
  the calibrated defaults are recalibrated offline, so a recipe that leaned on "the
  default θ" would change meaning (place#172: literal values, never a params version).
* Enumerations, namespaces and the namespace segment of a place id
  are lower-cased; country codes are upper-cased. The local segment of a place id
  (after the first colon) is kept verbatim (``wd:Q64`` is not ``wd:q64``). A leading
  ``place:`` on an id is stripped.
* Unknown keys anywhere are rejected. In particular nothing that could carry a user's
  acceptance or rejection of a merge (``edges``, ``merges``, ``accepted``,
  ``must_link`` ...) has a place in a recipe: user judgements are attestations, never
  forced merges (place#306). Authority hard links reach the clusterer from the index
  at resolution time, not from the recipe.

The stoplist is not in the recipe
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Ruled by SG on 2026-10-21 (place#325 Stage 1, ruling 3: "take it out of the hash").
``clustering.stoplist`` is REJECTED as an unknown key, with a message naming this
ruling, so a client still sending it fails loudly instead of hashing differently.

* A locus PID names a RECIPE that is re-run on every visit, not a frozen member list.
  What the recipe retrieves already depends on the index as it stands at resolve time.
* The gateway's ``toponym_stoplist`` (500 generic high-frequency names, e.g. "село",
  "Mill Creek", that scale the name signal x0.2 in ``clustering.js``) is gateway DATA,
  like the index itself. At resolve time it is a fixed resolver contract: the
  resolver takes it from the gateway's search response
  (``whg/webpack/js/locus_resolve.js`` ``clusteringArgs(recipe, stoplist)``), exactly
  as the Atlas does when it renders the cluster the recipe was made from.
* The parameters a user TUNES (``weights``, ``theta``, ``tau_*``, ``same_ns_penalty``,
  ``respect_distinct``) stay in the hash as literals.
* Measured: with the 500-entry list inside it, a cluster-card token was ~10,488
  characters and would 414 under nginx's default 8k request-line limit (gunicorn's
  own default, 4094 bytes, is tighter still and sits behind it). Without it: 366
  characters for a plain name search (the browser harness's card), 711 for the
  ``atlas_default`` vector (viewport polygon + temporal window + four members).
  ``places.tests_locus_recipe``, ``search.tests_locus`` and the browser test assert a
  2000-character ceiling beside a response carrying the real 500-entry list.
* What can still make a token long is ``bounds``: an Atlas area selection that is
  not all gateway-exact regions sends the region polygons themselves, and those
  vertices go into the recipe. Such a token can exceed 4094 bytes; that is not
  addressed here.
* No locus had been minted when this was changed, so the change was made in place at
  ``"v":1`` rather than as a version 2. Any PID or token computed before 2026-10-21
  with a ``stoplist`` member is void (none was ever published or registered).

Hash and PID
~~~~~~~~~~~~
``digest = sha256(canonical_bytes)``. The version is inside the bytes (``"v":1`` is
the first member), so a v2 recipe with identical content hashes differently.

``PID``: the first 54 bits of the digest, as an integer, written as 11 base-31 digits
over Crockford's alphabet minus ``Z`` (``0-9 A-Z`` minus ``I L O U Z``, so 31
symbols), plus one check digit, grouped ``XXXX-XXXX-XXXX``. Check digit =
``sum((i+1) * d_i for i in 0..10) mod 31``, from the same alphabet. Because the
modulus is prime, the position weights 1..11 are non-zero mod 31, and no two digits
differ by 31, EVERY single-character substitution and EVERY adjacent transposition of
unequal digits is detected: a typo is a hard error, never a different real locus.
(Plain base32 with a mod-31 check would miss ``0``/``Z`` in one position; Crockford's
own mod-37 check needs five symbols outside the alphabet.) Input is case-insensitive,
hyphens optional, ``I``/``L`` read as ``1`` and ``O`` as ``0``. PIDs are disjoint
from legacy ``whg_id``s (``1[0-9]{7}``) by length and shape.

Token (unminted URL)
~~~~~~~~~~~~~~~~~~~~
``base64url(canonical_bytes)`` without padding. Decoding re-parses and
re-canonicalises, so a hand-edited token still yields a canonical recipe; the hash is
always taken from the re-canonicalised bytes, never from the token as received.

Edge cases, documented
~~~~~~~~~~~~~~~~~~~~~~
* ``tau_name`` and ``tau_link`` are shipped in ``clustering_params`` but not read by
  ``clusterHits`` today. They are frozen as literals anyway (place#172 lists them),
  so two recipes differing only there resolve identically under today's clusterer.
* Geometric equivalence is not identity: a ring starting at a different vertex, or
  the same box as Polygon vs MultiPolygon, hash differently. Coordinates are
  quantised to 6 dp (~0.1 m), which collapses float noise from the viewport.
* Longitudes are accepted in [-540, 540] (not wrapped) because a map panned across
  the antimeridian reports them that way and the gateway is sent them as-is.
* A ``query``-less recipe is allowed when ``bounds`` or ``contained_in`` is present
  (a spatial sweep). A recipe with none of the three is rejected as unbounded.
* ``browse`` and ``offset`` (Place List pagination) are rejected: a page of a browse
  is not a cluster recipe.
* Lower-casing uses ``str.lower`` / ``String.prototype.toLowerCase``; the two agree
  on everything the vectors exercise. A difference would be a parity bug, not a
  spec choice.
* 54 hash bits: ~2.8e-5 probability of any collision among a million loci. The
  Stage 4 registry must still refuse to mint a PID already bound to different bytes.
"""

from __future__ import annotations

import base64
import hashlib
import json
import math
import re
import unicodedata
from decimal import Decimal, ROUND_HALF_UP

__all__ = [
    "RECIPE_VERSION", "RecipeError",
    "parse_recipe", "canonical_json", "canonical_bytes", "recipe_digest", "recipe_pid",
    "pid_from_digest", "normalise_pid", "is_legacy_locus_id", "PID_RE",
    "encode_recipe_token", "decode_recipe_token", "retrieval_from_search_body",
    "PID_ALPHABET",
]

RECIPE_VERSION = 1

SCALAR_DP = 4
COORD_DP = 6
SIZE_MAX = 10000
YEAR_ABS_MAX = 1_000_000
TOKEN_MAX_BYTES = 65536
PID_HASH_BITS = 54      # 31**11 > 2**54, so 11 base-31 digits hold them exactly
PID_DIGITS = 11

MODES = ("exact", "starts", "in", "fuzzy", "phonetic")
CONTAINMENTS = ("fuzzy", "exact")
RELATIONS = ("intersects", "within")
TEMPORAL_MODES = ("possibly", "definitely")
WEIGHT_KEYS = ("link", "name", "spatial", "temporal", "type")

PID_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXY"   # Crockford base32 minus Z: 31 symbols
PID_RE = re.compile(r"^[0-9A-HJKMNP-TV-Y]{4}-[0-9A-HJKMNP-TV-Y]{4}-[0-9A-HJKMNP-TV-Y]{4}$")
_LEGACY_RE = re.compile(r"^1[0-9]{7}$")

# Keys that would let a user's merge judgement into the recipe (place#306). They are
# unknown keys like any other; the dedicated message says why they will never exist.
FORBIDDEN_KEYS = frozenset({
    "edges", "edge", "links", "hard_links", "merges", "merge", "forced_merges",
    "accept", "accepted", "acceptances", "reject", "rejected", "rejections",
    "must_link", "cannot_link", "pins", "pinned", "attestations",
})

_TOP_KEYS = frozenset({"v", "retrieval", "clustering", "members"})
_RETRIEVAL_KEYS = frozenset({
    "query", "mode", "size", "ccodes", "types", "aat_types", "namespaces",
    "bounds", "contained_in", "containment", "relation", "temporal",
})
_TEMPORAL_KEYS = frozenset({"start", "end", "mode", "undated"})
_CLUSTERING_KEYS = frozenset({
    "weights", "theta", "tau_name", "tau_link", "same_ns_penalty", "respect_distinct",
})
# Keys that used to be in a recipe and were ruled out. Rejected like any unknown key,
# but the message says why, so an old client fails with a reason.
RETIRED_CLUSTERING_KEYS = {
    "stoplist": ("is not part of a recipe: the generic-name stoplist is gateway data, taken "
                 "from the gateway response at resolve time (place#325 ruling 3, 2026-10-21)"),
}

# Exactly JavaScript's \s, written out so both languages collapse the same characters.
_WS_RE = re.compile("[\t\n\x0b\x0c\r \u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]+")
_CCODE_RE = re.compile(r"^[A-Z]{2}$")
_NAMESPACE_RE = re.compile(r"^[a-z0-9_]+$")
_PLACE_ID_RE = re.compile(r"^([A-Za-z0-9_]+):(\S+)$")
_TOKEN_RE = re.compile(r"^[A-Za-z0-9_-]+$")


class RecipeError(ValueError):
    """A recipe that is malformed, unknown, or outside the specification.

    ``path`` names the offending member (``retrieval.ccodes[1]``); ``message`` says why.
    """

    def __init__(self, path: str, message: str):
        self.path = path
        self.message = message
        super().__init__(f"{path}: {message}" if path else message)


# --------------------------------------------------------------------------- scalars

def _is_int(x) -> bool:
    return isinstance(x, int) and not isinstance(x, bool)


def _is_number(x) -> bool:
    return (_is_int(x) or isinstance(x, float)) and not isinstance(x, bool)


def _quantise(x: float, dp: int) -> float:
    """Round half-up on the shortest decimal repr of the double, to ``dp`` places."""
    d = Decimal(repr(float(x))).quantize(Decimal(1).scaleb(-dp), rounding=ROUND_HALF_UP)
    if d == 0:
        return 0.0
    return float(d)


def _number(x, path: str, dp: int, lo: float, hi: float) -> float:
    if not _is_number(x):
        raise RecipeError(path, "must be a number")
    if not math.isfinite(x):
        raise RecipeError(path, "must be finite")
    if x < lo or x > hi:
        raise RecipeError(path, f"must be between {lo} and {hi}")
    q = _quantise(x, dp)
    if q < lo or q > hi:
        raise RecipeError(path, f"must be between {lo} and {hi}")
    return q


def _integer(x, path: str, lo: int, hi: int) -> int:
    if isinstance(x, float) and math.isfinite(x) and x.is_integer():
        x = int(x)
    if not _is_int(x):
        raise RecipeError(path, "must be an integer")
    if x < lo or x > hi:
        raise RecipeError(path, f"must be between {lo} and {hi}")
    return x


def _boolean(x, path: str) -> bool:
    if not isinstance(x, bool):
        raise RecipeError(path, "must be true or false")
    return x


def _num_text(v) -> str:
    """Canonical JSON text of an already-quantised number."""
    if _is_int(v):
        return str(v)
    d = Decimal(repr(float(v)))
    if d == 0:
        return "0"
    return format(d.normalize(), "f")


# --------------------------------------------------------------------------- strings

def _text(x, path: str, *, lower: bool = False, allow_empty: bool = False) -> str:
    if not isinstance(x, str):
        raise RecipeError(path, "must be a string")
    s = unicodedata.normalize("NFC", x)
    s = _WS_RE.sub(" ", s).strip()
    for ch in s:
        o = ord(ch)
        if 0xD800 <= o <= 0xDFFF:
            raise RecipeError(path, "contains a lone surrogate")
        if unicodedata.category(ch) == "Cc":
            raise RecipeError(path, "contains a control character")
    if lower:
        s = unicodedata.normalize("NFC", s.lower())
    if not s and not allow_empty:
        raise RecipeError(path, "must not be empty")
    return s


def _enum(x, path: str, allowed) -> str:
    s = _text(x, path, lower=True)
    if s not in allowed:
        raise RecipeError(path, "must be one of " + ", ".join(allowed))
    return s


def _place_id(x, path: str) -> str:
    s = _text(x, path)
    if s.startswith("place:"):
        s = s[len("place:"):]
    m = _PLACE_ID_RE.match(s)
    if not m:
        raise RecipeError(path, "must be a place id of the form <namespace>:<id>")
    return m.group(1).lower() + ":" + m.group(2)


def _sorted_unique_strings(items, path: str, each) -> list:
    if not isinstance(items, list):
        raise RecipeError(path, "must be a list")
    return sorted({each(v, f"{path}[{i}]") for i, v in enumerate(items)})


# --------------------------------------------------------------------------- geometry

def _position(p, path: str) -> list:
    if not isinstance(p, list) or len(p) != 2:
        raise RecipeError(path, "must be a [longitude, latitude] pair")
    lon = _number(p[0], f"{path}[0]", COORD_DP, -540.0, 540.0)
    lat = _number(p[1], f"{path}[1]", COORD_DP, -90.0, 90.0)
    return [lon, lat]


def _ring(r, path: str) -> list:
    if not isinstance(r, list) or len(r) < 4:
        raise RecipeError(path, "a ring needs at least four positions")
    ring = [_position(p, f"{path}[{i}]") for i, p in enumerate(r)]
    if ring[0] != ring[-1]:
        raise RecipeError(path, "a ring must be closed (first position == last)")
    return ring


def _polygon_coords(c, path: str) -> list:
    if not isinstance(c, list) or not c:
        raise RecipeError(path, "a Polygon needs at least one ring")
    return [_ring(r, f"{path}[{i}]") for i, r in enumerate(c)]


def _geometry(g, path: str, *, nested: bool = False):
    """Returns the canonical geometry dict, or None for an empty collection."""
    if not isinstance(g, dict):
        raise RecipeError(path, "must be a GeoJSON geometry object")
    gtype = g.get("type")
    if not isinstance(gtype, str):
        raise RecipeError(f"{path}.type", "must be a string")
    if gtype == "GeometryCollection":
        _only_keys(g, path, {"type", "geometries"})
        if nested:
            raise RecipeError(path, "a GeometryCollection may not contain a GeometryCollection")
        geoms = g.get("geometries")
        if not isinstance(geoms, list):
            raise RecipeError(f"{path}.geometries", "must be a list")
        parsed = []
        for i, m in enumerate(geoms):
            pm = _geometry(m, f"{path}.geometries[{i}]", nested=True)
            if pm is not None:
                parsed.append(pm)
        if not parsed:
            return None
        if len(parsed) == 1:
            return parsed[0]
        uniq = {_dumps(pm): pm for pm in parsed}
        return {"geometries": [uniq[k] for k in sorted(uniq)], "type": "GeometryCollection"}
    if gtype in ("Polygon", "MultiPolygon"):
        _only_keys(g, path, {"type", "coordinates"})
        coords = g.get("coordinates")
        cpath = f"{path}.coordinates"
        if gtype == "Polygon":
            return {"coordinates": _polygon_coords(coords, cpath), "type": "Polygon"}
        if not isinstance(coords, list) or not coords:
            raise RecipeError(cpath, "a MultiPolygon needs at least one polygon")
        return {"coordinates": [_polygon_coords(pc, f"{cpath}[{i}]") for i, pc in enumerate(coords)],
                "type": "MultiPolygon"}
    raise RecipeError(f"{path}.type", "must be Polygon, MultiPolygon or GeometryCollection")


# --------------------------------------------------------------------------- blocks

def _only_keys(obj: dict, path: str, allowed) -> None:
    for k in obj:
        if not isinstance(k, str):
            raise RecipeError(path, "object keys must be strings")
        if k in allowed:
            continue
        where = f"{path}.{k}" if path else k
        if k in FORBIDDEN_KEYS:
            raise RecipeError(where, "user acceptances and rejections cannot enter a recipe (place#306)")
        raise RecipeError(where, "unknown key")


def _object(x, path: str, allowed) -> dict:
    if not isinstance(x, dict):
        raise RecipeError(path, "must be an object")
    _only_keys(x, path, allowed)
    return x


def _parse_temporal(t, path: str) -> dict:
    t = _object(t, path, _TEMPORAL_KEYS)
    out = {}
    if "start" in t and t["start"] is not None:
        out["start"] = _integer(t["start"], f"{path}.start", -YEAR_ABS_MAX, YEAR_ABS_MAX)
    if "end" in t and t["end"] is not None:
        out["end"] = _integer(t["end"], f"{path}.end", -YEAR_ABS_MAX, YEAR_ABS_MAX)
    if "start" not in out and "end" not in out:
        raise RecipeError(path, "needs a start year, an end year, or both")
    if "start" in out and "end" in out and out["start"] > out["end"]:
        raise RecipeError(path, "start must not be after end")
    if "mode" not in t:
        raise RecipeError(f"{path}.mode", "is required")
    out["mode"] = _enum(t["mode"], f"{path}.mode", TEMPORAL_MODES)
    if "undated" in t and t["undated"] is not None and _boolean(t["undated"], f"{path}.undated"):
        out["undated"] = True
    return out


def _parse_retrieval(r, path: str) -> dict:
    r = _object(r, path, _RETRIEVAL_KEYS)
    out: dict = {}

    if "query" in r and r["query"] is not None:
        q = _text(r["query"], f"{path}.query", allow_empty=True)
        if q:
            out["query"] = q

    if "mode" not in r:
        raise RecipeError(f"{path}.mode", "is required")
    out["mode"] = _enum(r["mode"], f"{path}.mode", MODES)

    if "size" not in r:
        raise RecipeError(f"{path}.size", "is required")
    out["size"] = _integer(r["size"], f"{path}.size", 1, SIZE_MAX)

    if r.get("ccodes") is not None:
        def ccode(v, p):
            s = _text(v, p, lower=False).upper()
            if not _CCODE_RE.match(s):
                raise RecipeError(p, "must be a two-letter country code")
            return s
        vals = _sorted_unique_strings(r["ccodes"], f"{path}.ccodes", ccode)
        if vals:
            out["ccodes"] = vals

    if r.get("types") is not None:
        vals = _sorted_unique_strings(r["types"], f"{path}.types", lambda v, p: _text(v, p))
        if vals:
            out["types"] = vals

    if r.get("aat_types") is not None:
        if not isinstance(r["aat_types"], list):
            raise RecipeError(f"{path}.aat_types", "must be a list")
        vals = sorted({_integer(v, f"{path}.aat_types[{i}]", 1, 10**12) for i, v in enumerate(r["aat_types"])})
        if vals:
            out["aat_types"] = vals

    if r.get("namespaces") is not None:
        def ns(v, p):
            s = _text(v, p, lower=True)
            if not _NAMESPACE_RE.match(s):
                raise RecipeError(p, "must be a namespace code ([a-z0-9_])")
            return s
        vals = _sorted_unique_strings(r["namespaces"], f"{path}.namespaces", ns)
        if vals:
            out["namespaces"] = vals

    if r.get("bounds") is not None:
        g = _geometry(r["bounds"], f"{path}.bounds")
        if g is not None:
            out["bounds"] = g

    if r.get("contained_in") is not None:
        vals = _sorted_unique_strings(r["contained_in"], f"{path}.contained_in", _place_id)
        if vals:
            out["contained_in"] = vals

    if "contained_in" in out:
        if r.get("containment") is None:
            raise RecipeError(f"{path}.containment", "is required with contained_in")
        out["containment"] = _enum(r["containment"], f"{path}.containment", CONTAINMENTS)
    elif r.get("containment") is not None:
        raise RecipeError(f"{path}.containment", "is meaningless without contained_in")

    if r.get("relation") is not None:
        if "bounds" not in out and "contained_in" not in out:
            raise RecipeError(f"{path}.relation", "is meaningless without bounds or contained_in")
        out["relation"] = _enum(r["relation"], f"{path}.relation", RELATIONS)

    if r.get("temporal") is not None:
        out["temporal"] = _parse_temporal(r["temporal"], f"{path}.temporal")

    if not ("query" in out or "bounds" in out or "contained_in" in out):
        raise RecipeError(path, "needs a query, bounds or contained_in: the retrieval would be unbounded")

    return out


def _parse_clustering(c, path: str) -> dict:
    if isinstance(c, dict):
        for k, why in RETIRED_CLUSTERING_KEYS.items():
            if k in c:
                raise RecipeError(f"{path}.{k}", why)
    c = _object(c, path, _CLUSTERING_KEYS)
    for k in sorted(_CLUSTERING_KEYS):
        if k not in c or c[k] is None:
            raise RecipeError(f"{path}.{k}", "is required (clustering parameters are always literal)")
    w = _object(c["weights"], f"{path}.weights", set(WEIGHT_KEYS))
    weights = {}
    for k in WEIGHT_KEYS:
        if k not in w or w[k] is None:
            raise RecipeError(f"{path}.weights.{k}", "is required")
        weights[k] = _number(w[k], f"{path}.weights.{k}", SCALAR_DP, 0.0, 1.0)
    return {
        "respect_distinct": _boolean(c["respect_distinct"], f"{path}.respect_distinct"),
        "same_ns_penalty": _number(c["same_ns_penalty"], f"{path}.same_ns_penalty", SCALAR_DP, 0.0, 1.0),
        "tau_link": _number(c["tau_link"], f"{path}.tau_link", SCALAR_DP, 0.0, 1.0),
        "tau_name": _number(c["tau_name"], f"{path}.tau_name", SCALAR_DP, 0.0, 1.0),
        "theta": _number(c["theta"], f"{path}.theta", SCALAR_DP, 0.0, 1.0),
        "weights": weights,
    }


def parse_recipe(obj) -> dict:
    """Validate ``obj`` and return its canonical form as a plain dict.

    Raises :class:`RecipeError` on anything unknown, malformed or out of range. The
    result is idempotent: ``parse_recipe(parse_recipe(x)) == parse_recipe(x)``.
    """
    obj = _object(obj, "", _TOP_KEYS)
    if "v" not in obj:
        raise RecipeError("v", "is required")
    v = obj["v"]
    if isinstance(v, float) and math.isfinite(v) and v.is_integer():
        v = int(v)   # JSON "1.0" is the integer 1, as it is in JavaScript
    if not _is_int(v) or v != RECIPE_VERSION:
        raise RecipeError("v", f"unsupported recipe version (this code speaks {RECIPE_VERSION})")
    for k in ("retrieval", "clustering", "members"):
        if k not in obj or obj[k] is None:
            raise RecipeError(k, "is required")
    members = _sorted_unique_strings(obj["members"], "members", _place_id)
    if not members:
        raise RecipeError("members", "needs at least one member id")
    return {
        "clustering": _parse_clustering(obj["clustering"], "clustering"),
        "members": members,
        "retrieval": _parse_retrieval(obj["retrieval"], "retrieval"),
        "v": RECIPE_VERSION,
    }


# --------------------------------------------------------------------------- canonical bytes

def _dumps(v) -> str:
    """Canonical JSON text of an already-parsed value (sorted keys, no whitespace)."""
    if v is True:
        return "true"
    if v is False:
        return "false"
    if v is None:
        raise RecipeError("", "null has no canonical form (absent means absent)")
    if _is_number(v):
        return _num_text(v)
    if isinstance(v, str):
        return json.dumps(v, ensure_ascii=False)
    if isinstance(v, list):
        return "[" + ",".join(_dumps(x) for x in v) + "]"
    if isinstance(v, dict):
        return "{" + ",".join(json.dumps(k, ensure_ascii=False) + ":" + _dumps(v[k]) for k in sorted(v)) + "}"
    raise RecipeError("", f"cannot serialise {type(v).__name__}")


def canonical_json(recipe) -> str:
    """The canonical JSON text of ``recipe`` (parsed first, so any valid input works)."""
    return _dumps(parse_recipe(recipe))


def canonical_bytes(recipe) -> bytes:
    """The bytes that are hashed and tokenised: canonical JSON as UTF-8."""
    return canonical_json(recipe).encode("utf-8")


def recipe_digest(recipe) -> bytes:
    """``sha256`` over exactly :func:`canonical_bytes`."""
    return hashlib.sha256(canonical_bytes(recipe)).digest()


def recipe_pid(recipe) -> str:
    """The locus PID of ``recipe``: ``XXXX-XXXX-XXXX`` (Crockford base32, check digit)."""
    return pid_from_digest(recipe_digest(recipe))


# --------------------------------------------------------------------------- PID

def _pid_digits(digest: bytes) -> list:
    """The first ``PID_HASH_BITS`` bits of ``digest`` as ``PID_DIGITS`` base-31 digits, most significant first."""
    n_bytes = (PID_HASH_BITS + 7) // 8
    value = int.from_bytes(digest[:n_bytes], "big") >> (8 * n_bytes - PID_HASH_BITS)
    digits = []
    for _ in range(PID_DIGITS):
        value, d = divmod(value, 31)
        digits.append(d)
    return digits[::-1]


def _check_digit(digits) -> int:
    return sum((i + 1) * d for i, d in enumerate(digits)) % 31


def _format_pid(digits) -> str:
    s = "".join(PID_ALPHABET[d] for d in digits) + PID_ALPHABET[_check_digit(digits)]
    return f"{s[0:4]}-{s[4:8]}-{s[8:12]}"


def pid_from_digest(digest: bytes) -> str:
    if not isinstance(digest, (bytes, bytearray)) or len(digest) < 8:
        raise ValueError("digest must be at least 8 bytes")
    return _format_pid(_pid_digits(bytes(digest)))


def normalise_pid(s: str) -> str:
    """Canonical ``XXXX-XXXX-XXXX`` for a PID typed in any accepted form.

    Case-insensitive; hyphens optional; ``I``/``L`` read as ``1`` and ``O`` as ``0``.
    Raises ``ValueError`` when the shape is wrong or the check digit does not match.
    """
    if not isinstance(s, str):
        raise ValueError("PID must be a string")
    raw = s.strip().upper().replace("-", "").replace("I", "1").replace("L", "1").replace("O", "0")
    if len(raw) != PID_DIGITS + 1:
        raise ValueError("PID must be twelve characters")
    try:
        digits = [PID_ALPHABET.index(ch) for ch in raw]
    except ValueError:
        raise ValueError("PID contains a character outside the locus alphabet") from None
    if digits[PID_DIGITS] != _check_digit(digits[:PID_DIGITS]):
        raise ValueError("PID check digit does not match")
    return _format_pid(digits[:PID_DIGITS])


def is_legacy_locus_id(s) -> bool:
    """True for a legacy ``whg_id`` (``1[0-9]{7}``), which is a frozen locus (place#172)."""
    return isinstance(s, str) and bool(_LEGACY_RE.match(s.strip()))


# --------------------------------------------------------------------------- token

def encode_recipe_token(recipe) -> str:
    """URL-safe token for the unminted locus URL: base64url of the canonical bytes, unpadded."""
    return base64.urlsafe_b64encode(canonical_bytes(recipe)).decode("ascii").rstrip("=")


def _reject_constant(name):
    raise RecipeError("", f"{name} is not a valid number")


def _reject_duplicate_keys(pairs):
    d = {}
    for k, v in pairs:
        if k in d:
            raise RecipeError(k, "duplicate key")
        d[k] = v
    return d


def decode_recipe_token(token: str) -> dict:
    """Parse a token back into a canonical recipe dict (see :func:`parse_recipe`)."""
    if not isinstance(token, str):
        raise RecipeError("", "token must be a string")
    token = token.strip()
    if not token or not _TOKEN_RE.match(token):
        raise RecipeError("", "token is not base64url")
    if len(token) > TOKEN_MAX_BYTES:
        raise RecipeError("", "token is too long")
    try:
        raw = base64.urlsafe_b64decode(token + "=" * (-len(token) % 4))
        text = raw.decode("utf-8")
        obj = json.loads(text, parse_constant=_reject_constant, object_pairs_hook=_reject_duplicate_keys)
    except RecipeError:
        raise
    except (ValueError, UnicodeDecodeError):
        raise RecipeError("", "token does not decode to JSON") from None
    return parse_recipe(obj)


# --------------------------------------------------------------------------- adapter

_BODY_FIXED = frozenset({"geom", "include_hard_links", "include_clustering_fields", "include_embeddings", "include_links"})
_BODY_DIRECT = frozenset({"query", "mode", "size", "ccodes", "types", "aat_types", "namespaces",
                          "bounds", "contained_in", "containment", "relation"})
_BODY_TEMPORAL = {"start_year": "start", "end_year": "end", "undated": "undated", "temporal_mode": "mode"}
_BODY_REJECT = frozenset({"browse", "offset"})


def retrieval_from_search_body(body: dict) -> dict:
    """The ``retrieval`` block from the gateway ``POST /api/search`` body that
    ``api.crc_client.crc_search_status`` sends.

    The fixed-contract flags (``geom``, ``include_*``) are dropped: they are the
    resolver's business, not the recipe's. ``browse``/``offset`` are rejected. The
    result is not yet validated: pass it inside a recipe to :func:`parse_recipe`.
    """
    if not isinstance(body, dict):
        raise RecipeError("body", "must be an object")
    out: dict = {}
    temporal: dict = {}
    for k, v in body.items():
        if k in _BODY_FIXED or v is None:
            continue
        if k in _BODY_REJECT:
            raise RecipeError(f"body.{k}", "a browse page is not a cluster recipe")
        if k in _BODY_DIRECT:
            out[k] = v
        elif k in _BODY_TEMPORAL:
            temporal[_BODY_TEMPORAL[k]] = v
        else:
            raise RecipeError(f"body.{k}", "unknown search body key")
    if temporal:
        out["temporal"] = temporal
    return out
