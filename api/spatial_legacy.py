"""Translate a legacy ``GET /api/spatial/`` query into its ``POST /reconcile``
equivalent, for the v4 cutover (when /api/spatial/ is retired).

``POST /reconcile`` takes a JSON body (``{"queries": {"q0": {...}}}``) and an API
token, so an HTTP redirect from the old GET cannot carry the translated query: a
301/307 keeps the method (GET) and the query string, and the old callers send no
token. The redirection is therefore a server-side shim at the old URL,
``api.legacy_compat`` (``LEGACY_API_MODE``): in ``compat`` mode it runs this
translation and answers with the reshaped result; in ``gone`` mode it refuses with
a 410 naming the replacement and the translated body. ``to_reconcile_query`` is
pure (no I/O) so both can use it.

What the replacement searches differs: /api/spatial/ searches only the places in
WHG's own database (contributed datasets); /reconcile searches the gazetteer index
(tens of millions of places from GeoNames, Wikidata, OSM, ...) plus WHG's index.
"""

from api import spatial as spatial_api

RECONCILE_URL = "/reconcile"
RECONCILE_MAX_LIMIT = 1000

# Old parameters / behaviours the new API has no equivalent for. Returned with
# every translation so nobody assumes parity.
NO_EQUIVALENT = {
    "collection": "No collection filter on /reconcile. Filter the candidates by the collection's place "
                  "ids client-side.",
    "population": "/api/spatial/ searched only WHG's own database (contributed datasets); /reconcile "
                  "searches the gazetteer index as well, so the result set is larger and different.",
    "format": "/reconcile returns reconciliation candidates (id, name, score, repr_point, ccodes, "
              "namespace, ...), not LPF Features in a FeatureCollection; no names/links/whens arrays.",
    "geometry": "Candidates carry repr_point ([lon, lat], lies on the place) but no bbox and no full "
                "geometry. For the geometry use GET /entity/place:<id>/api or the gateway's "
                "GET /api/geometry/<place_id>.",
    "count": "No total count of matches; page with limit/offset.",
    "auth": "/reconcile requires an API token (401 without); /api/spatial/ was anonymous.",
}

def to_reconcile_query(params):
    """Map old /api/spatial/ query parameters (a dict of strings, as in a query
    string) to ``{"body": <POST /reconcile JSON>, "dropped": {param: why},
    "no_equivalent": NO_EQUIVALENT, "notes": [...]}``.

    Raises ``spatial_api.SpatialParamError`` for a query the old endpoint itself
    would refuse (bad type, missing or out-of-range coordinates, bad geometry/match).
    """
    p = {k: (v[-1] if isinstance(v, (list, tuple)) else v) for k, v in dict(params).items()}
    qtype = p.get("type")
    match = p.get("match", "within")
    geometry = p.get("geometry", "point")
    if match not in spatial_api.MATCH_MODES:
        raise spatial_api.SpatialParamError(f"'match' must be one of {', '.join(spatial_api.MATCH_MODES)}.")
    if geometry not in spatial_api.GEOMETRY_MODES:
        raise spatial_api.SpatialParamError(
            f"'geometry' must be one of {', '.join(spatial_api.GEOMETRY_MODES)}.")

    q, dropped, notes = {}, {}, []
    try:
        if qtype == "nearby":
            lon, lat, km = float(p["lon"]), float(p["lat"]), float(p["km"])
            if not (-180 <= lon <= 180 and -90 <= lat <= 90) or km <= 0:
                raise spatial_api.SpatialParamError("'lon'/'lat'/'km' out of range.")
            q.update({"lat": lat, "lng": lon, "radius": km, "relation": match})
            # The gateway resolves a circle as an H3 disc and builds no polygon, so
            # 'exact' would degrade to fuzzy anyway: say so rather than ask for it.
            q["containment"] = "fuzzy"
            notes.append("The circle is matched against an H3 cell disc that covers it (fuzzy), so places "
                         "just outside km can match; /api/spatial/ used an exact spherical circle.")
        elif qtype == "bbox":
            w, s = map(float, p["sw"].split(","))
            e, n = map(float, p["ne"].split(","))
            if not (-180 <= w <= 180 and -180 <= e <= 180 and -90 <= s <= 90 and -90 <= n <= 90):
                raise spatial_api.SpatialParamError("Bounding box coordinates are out of bounds.")
            q["bounds"] = {"type": "Polygon", "coordinates": [[[w, s], [e, s], [e, n], [w, n], [w, s]]]}
            q.update({"relation": match, "containment": "exact"})
        else:
            raise spatial_api.SpatialParamError("'type' must be 'nearby' or 'bbox'.")
    except (KeyError, ValueError, AttributeError) as exc:
        if isinstance(exc, spatial_api.SpatialParamError):
            raise
        raise spatial_api.SpatialParamError(f"Missing or malformed spatial parameter: {exc}")

    if p.get("fc"):
        q["fclasses"] = sorted({x.strip().upper() for x in p["fc"].split(",") if x.strip()})
    if p.get("dataset"):
        if not str(p["dataset"]).isdigit():
            raise spatial_api.SpatialParamError("'dataset' must be an integer id.")
        # /reconcile keeps only that dataset's places (since 2026-11-18; until then it
        # accepted `dataset` and filtered nothing, and this mapping narrowed to `whg` and
        # left the caller to filter the candidate ids).
        q["dataset"] = int(p["dataset"])
    if p.get("collection"):
        dropped["collection"] = NO_EQUIVALENT["collection"]
    raw = str(p.get("pagesize", spatial_api.DEFAULT_PAGESIZE))
    q["limit"] = max(1, min(int(raw) if raw.isdigit() else spatial_api.DEFAULT_PAGESIZE, RECONCILE_MAX_LIMIT))
    if geometry != "point":
        dropped["geometry"] = NO_EQUIVALENT["geometry"]

    return {"url": RECONCILE_URL, "method": "POST", "body": {"queries": {"q0": q}},
            "dropped": dropped, "no_equivalent": NO_EQUIVALENT, "notes": notes}
