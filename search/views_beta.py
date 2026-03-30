# search/views_beta.py
"""
Beta search views — thin proxies that forward requests to the CRC Gateway
``/api/search`` and ``/api/suggest`` endpoints.

Wired via ``version_dispatch()`` in ``urls.py`` so that:
  - stable users → ``SearchViewV3`` (legacy ES query building in Django)
  - beta users   → ``SearchViewBeta`` (CRC Gateway proxy)
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


def _adapt_gateway_hit(hit):
    """
    Convert a single CRC Gateway hit into the ``suggestionItem`` shape
    expected by the browser-side ``renderResults()`` JS function.

    Legacy suggestion format::

        {
            "whg_id": "",
            "pid": "gn:745044",
            "index": "crc",
            "children": [],
            "linkcount": 0,
            "title": "Istanbul",
            "variants": ["Constantinople", ...],
            "ccodes": ["TR"],
            "types": ["inhabited place"],
            "geom": [{"type": "Point", "coordinates": [...], "properties": {"pid": ...}}],
            "timespans": [[800, 1800]],
        }
    """
    place_id = hit.get("place_id", "")
    title = hit.get("title", "")

    # Variants: all name labels except the title itself
    names_raw = hit.get("names") or []
    variants = list({
        n.get("label", "") for n in names_raw
        if n.get("label") and n.get("label") != title
    })

    # Geometry in legacy format
    geom = []
    for g in (hit.get("geometries") or []):
        rp = g.get("repr_point")
        if rp and len(rp) == 2:
            geom.append({
                "type": "Point",
                "coordinates": rp,
                "properties": {"pid": place_id},
            })

    # Types — gateway may return dicts with 'label' or plain strings
    raw_types = hit.get("types") or []
    types = []
    for t in raw_types:
        if isinstance(t, dict):
            label = t.get("label", "")
            if label:
                types.append(label)
        elif isinstance(t, str) and t:
            types.append(t)

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
        "types": types,
        "geom": geom,
        "timespans": timespans,
    }


class SearchViewBeta(View):
    """
    POST /search/index/ — beta proxy.

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
        if not qstr:
            return JsonResponse(
                {"parameters": data, "suggestions": []},
                safe=False,
            )

        # Build the gateway request body
        gateway_params = {
            "query": qstr,
            "mode": data.get("mode", "fuzzy"),
            "size": 100,
        }

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
        # Sort by linkcount descending (matching legacy behaviour)
        suggestions.sort(key=lambda s: s["linkcount"], reverse=True)

        result = {"parameters": data, "suggestions": suggestions}
        return JsonResponse(result, safe=False)

    def get(self, request):
        return JsonResponse(
            {"error": "GET requests are not allowed for this endpoint"},
            status=400,
        )


def TypeaheadSuggestionsBeta(request):
    """
    GET /search/suggestions/ — beta proxy.

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

