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


class SearchViewBeta(View):
    """
    POST /search/index/ — beta proxy.

    Translates the browser's POST payload into the CRC Gateway
    ``/api/search`` request shape and returns the gateway response
    directly.  The browser-side JS will need to understand the new
    response format (hits/facets/total instead of parameters/suggestions).
    """

    def post(self, request):
        try:
            data = json.loads(request.body.decode("utf-8"))
        except json.JSONDecodeError:
            return JsonResponse({"error": "Invalid JSON"}, status=400)

        qstr = data.get("qstr", "").strip()
        if not qstr:
            return JsonResponse(
                {"hits": [], "total": 0, "max_score": 0,
                 "facets": {"types": [], "countries": []}},
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
        bounds = data.get("bounds")
        if bounds:
            if isinstance(bounds, str):
                try:
                    bounds = json.loads(bounds)
                except (json.JSONDecodeError, ValueError):
                    bounds = None
            if bounds:
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

        result = crc_search(gateway_params, request=request)
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

