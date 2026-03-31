# search/views_crc.py
"""
Search views — thin proxies that forward requests to the CRC Gateway
``/api/search`` and ``/api/suggest`` endpoints.
"""

import logging

import simplejson as json
from django.http import JsonResponse
from django.views import View

from api.crc_client import crc_search, crc_suggest

logger = logging.getLogger(__name__)


def _has_geometries(bounds: dict) -> bool:
    """Return True only if the GeoJSON geometry contains actual geometry data."""
    if not bounds:
        return False
    geom_type = bounds.get("type", "")
    if geom_type == "GeometryCollection":
        return bool(bounds.get("geometries"))
    # Any other valid GeoJSON type (Polygon, MultiPolygon, etc.) is usable
    return bool(geom_type)


def _matches_type_filter(suggestion: dict, expanded_ids: set) -> bool:
    """
    Return True if the suggestion has at least one type whose AAT
    identifier is in the *expanded_ids* set.

    Falls back to True when the suggestion carries no type identifiers
    at all (so untyped records are never silently dropped).
    """
    types_full = suggestion.get("types_full") or []
    if not types_full:
        return True  # no type info → don't filter out
    return any(
        t.get("identifier") in expanded_ids
        for t in types_full
        if t.get("identifier")
    )


def _adapt_gateway_hit(hit):
    """
    Convert a single CRC Gateway hit into the ``suggestionItem`` shape
    expected by the browser-side ``renderResults()`` JS function.

    Includes both legacy fields (for backward-compatible filters) and
    rich fields (for the enhanced result display).
    """
    place_id = hit.get("place_id", "")
    title = hit.get("title", "")

    # Variants: all name labels except the title itself
    names_raw = hit.get("names") or []
    variants = list({
        n.get("label", "") for n in names_raw
        if n.get("label") and n.get("label") != title
    })

    # Rich name objects: [{toponym, lang}, ...]
    # Gateway names use "label" key; JS expects "toponym"
    names = []
    for n in names_raw:
        label = n.get("label", "")
        if label:
            name_obj = {"toponym": label}
            lang = n.get("lang") or n.get("language")
            if lang:
                name_obj["lang"] = lang
            names.append(name_obj)

    # Geometries — prefer full geometry objects, fall back to repr_point
    #
    # The gateway may return any of:
    #   • geometries: [{type, coordinates, ...}, ...]   — full GeoJSON
    #   • geometries: [{location: {type, coordinates}}, ...]  — ES-style
    #   • repr_point: [lon, lat]  — centroid only (current gateway default)
    #
    # We pass through whatever geometry types are available (Point,
    # Polygon, LineString, MultiPolygon, GeometryCollection, etc.)
    # so that the JS ``geomsGeoJSON()`` helper can render lines and
    # polygons on the map, not just point markers.
    geom = []
    for g in (hit.get("geometries") or []):
        if isinstance(g, dict):
            # Full GeoJSON geometry object (has "type" and "coordinates")
            if g.get("type") and g.get("coordinates"):
                geom.append({
                    "type": g["type"],
                    "coordinates": g["coordinates"],
                    "properties": {"pid": place_id},
                })
            # ES-style wrapper: {location: {type, coordinates}}
            elif isinstance(g.get("location"), dict):
                loc = g["location"]
                if loc.get("type") and loc.get("coordinates"):
                    geom.append({
                        "type": loc["type"],
                        "coordinates": loc["coordinates"],
                        "properties": {"pid": place_id},
                    })
            # Legacy repr_point inside a geometries entry
            else:
                grp = g.get("repr_point")
                if grp and len(grp) == 2:
                    geom.append({
                        "type": "Point",
                        "coordinates": grp,
                        "properties": {"pid": place_id},
                    })

    # Fall back to the top-level repr_point when no full geometries exist
    if not geom:
        rp = hit.get("repr_point")
        if rp and len(rp) == 2:
            geom.append({
                "type": "Point",
                "coordinates": rp,
                "properties": {"pid": place_id},
            })

    # Types — gateway may return dicts with 'label' or plain strings
    raw_types = hit.get("types") or []
    types = []
    types_full = []
    for t in raw_types:
        if isinstance(t, dict):
            label = t.get("label", "")
            if label:
                types.append(label)
            type_obj = {}
            if label:
                type_obj["label"] = label
            if t.get("identifier"):
                type_obj["identifier"] = t["identifier"]
            if t.get("sourceLabel") or t.get("src_label"):
                type_obj["sourceLabel"] = t.get("sourceLabel") or t.get("src_label", "")
            if type_obj:
                types_full.append(type_obj)
        elif isinstance(t, str) and t:
            types.append(t)
            types_full.append({"label": t})

    # Timespans
    timespans = []
    for span in (hit.get("timespans") or []):
        if isinstance(span, dict):
            gte = span.get("gte") or span.get("start")
            lte = span.get("lte") or span.get("end")
            if gte is not None and lte is not None:
                timespans.append([gte, lte])
        elif isinstance(span, (list, tuple)) and len(span) == 2:
            timespans.append(list(span))

    # External links: [{identifier, type}, ...]
    links = []
    for lnk in (hit.get("links") or []):
        link_obj = {}
        if isinstance(lnk, dict):
            if "identifier" in lnk:
                link_obj["identifier"] = lnk["identifier"]
            if "type" in lnk:
                link_obj["type"] = lnk["type"]
        if link_obj:
            links.append(link_obj)

    # Descriptions: [{value, lang}, ...]
    descriptions = []
    for d in (hit.get("descriptions") or []):
        if isinstance(d, dict):
            desc_obj = {}
            if "value" in d:
                desc_obj["value"] = d["value"]
            if "lang" in d:
                desc_obj["lang"] = d["lang"]
            if desc_obj:
                descriptions.append(desc_obj)

    children = hit.get("children") or []

    return {
        "whg_id": hit.get("whg_id", ""),
        "pid": place_id,
        "index": "crc",
        "children": children,
        "linkcount": len(set(children)) if children else 0,
        "title": title,
        "variants": variants,
        "ccodes": hit.get("ccodes") or [],
        # Flat type labels for backward compatibility (filters, etc.)
        "types": types,
        "geom": geom,
        "timespans": timespans,
        # Rich fields for enhanced display
        "names": names,
        "types_full": types_full,
        "links": links,
        "descriptions": descriptions,
        "depictions": hit.get("depictions") or [],
        "relations": hit.get("relations") or [],
        "dataset": hit.get("dataset", ""),
        "fclasses": hit.get("fclasses") or [],
        "src_id": hit.get("src_id", ""),
        "uri": hit.get("uri", ""),
    }


class SearchView(View):
    """
    POST /search/index/ — CRC Gateway proxy.

    Translates the browser's POST payload into the CRC Gateway
    ``/api/search`` request shape, then adapts the response into the
    legacy ``{parameters, suggestions}`` format so the existing
    browser-side ``renderResults()`` JS works unchanged.
    """

    def post(self, request):
        try:
            data = json.loads(request.body.decode("utf-8"))
        except json.JSONDecodeError:
            return JsonResponse({"error": "Invalid JSON"}, status=400)

        qstr = data.get("qstr", "").strip()

        # Determine whether any non-text filters are present
        has_fclasses = bool(data.get("fclasses", "").strip())
        has_temporal = (
            data.get("temporal") not in (None, False, "false", "")
            and (data.get("start") is not None or data.get("end") is not None)
        )
        has_countries = bool(data.get("countries"))
        bounds_raw = data.get("bounds")
        has_bounds = bool(bounds_raw) and _has_geometries(
            bounds_raw if isinstance(bounds_raw, dict) else {}
        )
        has_filters = has_fclasses and (has_temporal or has_countries or has_bounds)

        if not qstr and not has_filters:
            return JsonResponse(
                {"parameters": data, "suggestions": []},
                safe=False,
            )

        # Build the gateway request body
        gateway_params = {
            "mode": data.get("mode", "fuzzy"),
            "size": 100,
        }
        if qstr:
            gateway_params["query"] = qstr

        # Country codes — may arrive as JSON string or list
        countries = data.get("countries")
        if countries:
            if isinstance(countries, str):
                try:
                    countries = json.loads(countries)
                except (json.JSONDecodeError, ValueError):
                    countries = [countries]
            gateway_params["ccodes"] = countries

        # Spatial bounds — GeoJSON geometry
        # The browser always sends bounds; skip when the GeometryCollection
        # is empty (no actual geometries drawn / selected).
        bounds = data.get("bounds")
        if bounds:
            if isinstance(bounds, str):
                try:
                    bounds = json.loads(bounds)
                except (json.JSONDecodeError, ValueError):
                    bounds = None
            if bounds and _has_geometries(bounds):
                gateway_params["bounds"] = bounds

        # Temporal range
        temporal = data.get("temporal")
        if temporal and temporal != "false":
            start = data.get("start")
            end = data.get("end")
            if start is not None:
                try:
                    gateway_params["start_year"] = int(start)
                except (ValueError, TypeError):
                    pass
            if end is not None:
                try:
                    gateway_params["end_year"] = int(end)
                except (ValueError, TypeError):
                    pass
            undated = data.get("undated")
            if undated and undated != "false":
                gateway_params["undated"] = True

        # Store params in session for the search page template
        request.session["search_params"] = data

        gateway_result = crc_search(gateway_params, request=request)

        # Adapt gateway response into legacy {parameters, suggestions} shape
        suggestions = [
            _adapt_gateway_hit(hit)
            for hit in gateway_result.get("hits", [])
        ]

        # --- Type filtering (fclasses) ----------------------------------
        # fclasses contains comma-separated AAT identifiers (e.g.
        # "aat:300008347,aat:300120579") from either the category
        # checkboxes or the type tree widget.  When present, expand
        # each identifier to include all AAT descendants, then keep
        # only suggestions whose type identifiers intersect.
        fclasses_raw = data.get("fclasses", "")
        if fclasses_raw and isinstance(fclasses_raw, str):
            fclass_ids = [s.strip() for s in fclasses_raw.split(",") if s.strip()]
            if fclass_ids:
                try:
                    from placetypes.aat_utils import expand_type_identifiers
                    expanded = set(expand_type_identifiers(fclass_ids))
                    if expanded:
                        suggestions = [
                            s for s in suggestions
                            if _matches_type_filter(s, expanded)
                        ]
                except Exception as exc:
                    logger.warning(
                        "Type filter expansion failed, skipping filter: %s",
                        exc,
                    )

        # Sort by linkcount descending (matching legacy behaviour)
        suggestions.sort(key=lambda s: s["linkcount"], reverse=True)

        result = {"parameters": data, "suggestions": suggestions}
        return JsonResponse(result, safe=False)

    def get(self, request):
        return JsonResponse(
            {"error": "GET requests are not allowed for this endpoint"},
            status=400,
        )


def TypeaheadSuggestions(request):
    """
    GET /search/suggestions/ — CRC Gateway proxy.

    Forwards the prefix to the CRC Gateway ``/api/suggest`` endpoint
    and returns a list of name strings for the typeahead dropdown.
    """
    q = request.GET.get("q", "").strip()
    if not q or len(q) < 2:
        return JsonResponse([], safe=False)

    result = crc_suggest(q, size=10, request=request)
    # Return just the name strings for backward compatibility with
    # the existing typeahead JS that expects a flat list
    names = [s.get("name", "") for s in result.get("suggestions", [])]
    return JsonResponse(names, safe=False)
