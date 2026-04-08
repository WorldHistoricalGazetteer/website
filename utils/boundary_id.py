"""
Encode / decode boundary feature IDs for MVT (vector tile) generation.

tippecanoe requires GeoJSON top-level ``id`` to be an integer, and
MapLibre GL JS uses it as the MVT feature ID for operations like
``querySourceFeatures`` and ``feature-state``.

We pack *namespace* + *relation_id* into a single integer that fits
within JavaScript's ``Number.MAX_SAFE_INTEGER`` (2**53 − 1).

Layout (53 usable bits)::

    bits [52..49]  — 4-bit namespace code  (0–15)
    bits [48..0]   — 49-bit relation_id    (0 – 562,949,953,421,311)

This gives room for 16 namespaces and relation IDs up to ~563 trillion —
more than enough for OSM / OHM / M49 identifiers.

The same encoding is implemented in JavaScript in
``whg/webpack/js/boundaryId.js`` so that the tile-generation pipeline
and the browser client agree on the format.

Usage in tile-generation scripts
--------------------------------

.. code-block:: python

    from utils.boundary_id import encode_feature_id, decode_feature_id

    # When writing GeoJSON for tippecanoe:
    feature = {
        "type": "Feature",
        "id": encode_feature_id("osm", 12345),
        "properties": { ... },
        "geometry": { ... },
    }

    # To decode back:
    ns, rid = decode_feature_id(feature["id"])
    # ns == "osm", rid == 12345
"""

from __future__ import annotations

# Namespace string → 4-bit code.
# Keep in sync with whg/webpack/js/boundaryId.js NAMESPACE_CODES.
NAMESPACE_CODES: dict[str, int] = {
    "osm": 1,
    "ohm": 2,
    "m49": 3,
}

# Reverse lookup: code → namespace string.
CODE_TO_NAMESPACE: dict[int, str] = {v: k for k, v in NAMESPACE_CODES.items()}

# Number of bits reserved for the relation_id.
_ID_BITS = 49
_ID_MULTIPLIER = 2 ** _ID_BITS  # 562_949_953_421_312


def encode_feature_id(namespace: str, relation_id: int) -> int:
    """
    Encode a namespace + relation_id into a single safe integer.

    Parameters
    ----------
    namespace : str
        One of ``'osm'``, ``'ohm'``, ``'m49'``.
    relation_id : int
        Non-negative integer (e.g. an OSM relation ID).

    Returns
    -------
    int
        Packed integer suitable for the GeoJSON ``"id"`` field.

    Raises
    ------
    ValueError
        If *namespace* is unknown or *relation_id* is out of range.
    """
    ns_code = NAMESPACE_CODES.get(namespace)
    if ns_code is None:
        raise ValueError(f"Unknown boundary namespace: {namespace!r}")
    if not isinstance(relation_id, int) or relation_id < 0:
        raise ValueError(f"relation_id must be a non-negative integer, got {relation_id!r}")
    if relation_id >= _ID_MULTIPLIER:
        raise ValueError(
            f"relation_id {relation_id} exceeds maximum ({_ID_MULTIPLIER - 1})"
        )
    return (ns_code << _ID_BITS) | relation_id


def decode_feature_id(packed_id: int) -> tuple[str, int]:
    """
    Decode a packed boundary ID back to (namespace, relation_id).

    Parameters
    ----------
    packed_id : int
        The integer from the GeoJSON ``"id"`` field / MVT feature ID.

    Returns
    -------
    tuple[str, int]
        ``(namespace, relation_id)``

    Raises
    ------
    ValueError
        If the namespace code portion is unknown.
    """
    if not isinstance(packed_id, int) or packed_id < 0:
        raise ValueError(f"Invalid packed boundary ID: {packed_id!r}")
    ns_code = packed_id >> _ID_BITS
    relation_id = packed_id & (_ID_MULTIPLIER - 1)
    namespace = CODE_TO_NAMESPACE.get(ns_code)
    if namespace is None:
        raise ValueError(
            f"Unknown namespace code {ns_code} in packed ID {packed_id}"
        )
    return namespace, relation_id

