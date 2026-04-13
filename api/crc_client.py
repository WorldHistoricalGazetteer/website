# api/crc_client.py

"""
HTTP client for the CRC Gateway reconciliation endpoint.

Provides a synchronous interface for the WHG Django app to search the CRC
`places` and `toponyms` ES indexes via the FastAPI gateway running on the
Pitt CRC instance.

Results are adapted into the same shape as legacy ES hits so that existing
`make_candidate()` logic works unchanged.

All calls are fail-safe: on timeout, connection error, or HTTP error,
a warning is logged and an empty list is returned.  Legacy results are
always returned regardless of CRC availability.

Access is gated by the configured APP_VERSION in settings:
  - version < 3.5  → never call the gateway (legacy behaviour)
  - version >= 3.5 → call the gateway for all authenticated users
"""

import logging

import requests
from django.conf import settings

logger = logging.getLogger("reconciliation")


def _is_enabled_for_request(request) -> bool:
    """
    Check whether CRC gateway integration should fire for *this* request.

    The gateway is enabled when:
    1. ``CRC_GATEWAY_URL`` is configured in settings.
    2. The user is authenticated.
    3. The configured ``APP_VERSION`` is >= 3.5.
    """
    # Must have a gateway URL configured at all
    gateway_url = getattr(settings, "CRC_GATEWAY_URL", "")
    if not gateway_url:
        logger.warning("CRC gateway disabled: CRC_GATEWAY_URL is empty")
        return False

    if request is None:
        logger.warning("CRC gateway disabled: request is None")
        return False

    user = getattr(request, "user", None)
    if user is None or not user.is_authenticated:
        logger.warning("CRC gateway disabled: user not authenticated (user=%s)", user)
        return False

    # Determine the version from settings
    version_str = getattr(settings, "APP_VERSION", "0")
    try:
        version_num = float(version_str.split("-")[0])
    except (ValueError, AttributeError):
        logger.warning("CRC gateway disabled: cannot parse version %r", version_str)
        return False

    if version_num < 3.5:
        logger.warning("CRC gateway disabled: version %.1f < 3.5", version_num)
        return False

    logger.info("CRC gateway enabled: url=%s, user=%s, version=%s", gateway_url, user, version_str)
    return True


def _gateway_url() -> str:
    return settings.CRC_GATEWAY_URL.rstrip("/")


def _timeout() -> int:
    return getattr(settings, "CRC_GATEWAY_TIMEOUT", 10)


def _headers() -> dict:
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    api_key = getattr(settings, "CRC_GATEWAY_API_KEY", None)
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    return headers


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def crc_reconcile_search(normalised_query: dict, request=None) -> list[dict]:
    """
    Call the CRC gateway ``/api/reconcile`` endpoint.

    Args:
        normalised_query: The dict returned by ``normalise_query_params()``
            in reconcile.py.  Keys used: ``query_text``, ``raw`` (original
            params dict), ``bounds``, ``size``.
        request: The Django HttpRequest (used for version-based access check).

    Returns:
        List of dicts in the same shape as ES ``hits.hits`` entries, i.e.
        ``{"_id": ..., "_score": ..., "_source": {...}}``, ready for
        ``make_candidate()``.  Returns ``[]`` on any error.
    """
    if not _is_enabled_for_request(request):
        return []

    # Build the gateway request body from the normalised query
    raw = normalised_query.get("raw", {})
    body = {
        "query": normalised_query.get("query_text"),
        "mode": raw.get("mode", "fuzzy"),
        "size": normalised_query.get("size", 50),
    }

    # Country codes
    countries = raw.get("countries")
    if countries:
        if isinstance(countries, str):
            import json
            countries = json.loads(countries)
        body["ccodes"] = countries

    # Namespace filter (e.g. ["wd", "gn"])
    namespaces = raw.get("namespaces")
    if namespaces:
        if isinstance(namespaces, str):
            namespaces = [n.strip() for n in namespaces.split(",") if n.strip()]
        body["namespaces"] = namespaces

    # Feature class filter (e.g. ["P", "A"])
    fclasses = normalised_query.get("fclasses")
    if fclasses:
        body["fclasses"] = fclasses

    # Spatial bounds
    bounds = normalised_query.get("bounds")
    if bounds:
        body["bounds"] = bounds

    # Temporal range
    start = raw.get("start")
    end = raw.get("end")
    if start is not None:
        body["start_year"] = int(start)
    if end is not None:
        body["end_year"] = int(end)

    try:
        resp = requests.post(
            f"{_gateway_url()}/api/reconcile",
            json=body,
            headers=_headers(),
            timeout=_timeout(),
        )
        resp.raise_for_status()
        data = resp.json()
    except requests.Timeout:
        logger.warning("CRC gateway timeout after %ss", _timeout())
        return []
    except requests.ConnectionError as e:
        logger.warning("CRC gateway connection error: %s", e)
        return []
    except requests.HTTPError as e:
        logger.warning("CRC gateway HTTP error: %s", e)
        return []
    except Exception as e:
        logger.warning("CRC gateway unexpected error: %s", e)
        return []

    return _adapt_hits(data)


def crc_suggest_search(prefix: str, mode: str = "starts", limit: int = 10, request=None) -> list[dict]:
    """
    Call the CRC gateway for suggest/typeahead results.

    Uses the same ``/api/reconcile`` endpoint with a small size.

    Returns:
        List of adapted ES-style hit dicts.
    """
    if not _is_enabled_for_request(request):
        return []

    body = {
        "query": prefix,
        "mode": mode,
        "size": limit,
    }

    try:
        resp = requests.post(
            f"{_gateway_url()}/api/reconcile",
            json=body,
            headers=_headers(),
            timeout=_timeout(),
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning("CRC gateway suggest error: %s", e)
        return []

    return _adapt_hits(data)


# ---------------------------------------------------------------------------
# Beta search proxy — calls the new /api/search and /api/suggest endpoints
# ---------------------------------------------------------------------------

def crc_search(params: dict, request=None) -> dict:
    """
    Call the CRC gateway ``/api/search`` endpoint (beta only).

    Args:
        params: Dict with keys matching the gateway SearchRequest schema:
            query, mode, ccodes, bounds, start_year, end_year, undated,
            size, exclude_namespaces, geom.

            ``geom`` controls geometry detail level:
              - ``"full"`` (default) — complete GeoJSON geometries + repr_point
              - ``"repr_point"`` — centroids only (lighter payloads)
        request: Django HttpRequest (for version-based access check).

    Returns:
        The full gateway response dict (hits, total, max_score, facets),
        or an empty-result dict on any error.
    """
    if not _is_enabled_for_request(request):
        return {"hits": [], "total": 0, "max_score": 0, "facets": {"types": [], "countries": []}}

    try:
        url = f"{_gateway_url()}/api/search"
        logger.info("CRC gateway POST %s with params: %s", url, params)
        resp = requests.post(
            url,
            json=params,
            headers=_headers(),
            timeout=_timeout(),
        )
        logger.info("CRC gateway /api/search response: %s %s", resp.status_code, resp.text[:500] if resp.text else "")
        resp.raise_for_status()
        return resp.json()
    except requests.Timeout:
        logger.warning("CRC gateway /api/search timeout after %ss", _timeout())
    except requests.ConnectionError as e:
        logger.warning("CRC gateway /api/search connection error: %s", e)
    except requests.HTTPError as e:
        logger.warning("CRC gateway /api/search HTTP error: %s", e)
    except Exception as e:
        logger.warning("CRC gateway /api/search unexpected error: %s", e)

    return {"hits": [], "total": 0, "max_score": 0, "facets": {"types": [], "countries": []}}


def crc_suggest(prefix: str, size: int = 10, request=None) -> dict:
    """
    Call the CRC gateway ``GET /api/suggest`` endpoint (beta only).

    Returns:
        The gateway response dict (suggestions, total), or an empty-result
        dict on any error.
    """
    if not _is_enabled_for_request(request):
        return {"suggestions": [], "total": 0}

    try:
        url = f"{_gateway_url()}/api/suggest"
        logger.info("CRC gateway GET %s?q=%s&size=%s", url, prefix, size)
        resp = requests.get(
            url,
            params={"q": prefix, "size": size},
            headers=_headers(),
            timeout=_timeout(),
        )
        logger.info("CRC gateway /api/suggest response: %s %s", resp.status_code, resp.text[:500] if resp.text else "")
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning("CRC gateway /api/suggest error: %s", e)

    return {"suggestions": [], "total": 0}


# ---------------------------------------------------------------------------
# Adapter: CRC response → legacy ES hit shape
# ---------------------------------------------------------------------------

def _adapt_hits(data: dict) -> list[dict]:
    """
    Convert the CRC gateway response into a list of dicts that look like
    Elasticsearch ``hits.hits[]`` entries, so ``make_candidate()`` can
    process them without changes.

    Legacy hit shape::

        {
            "_id": "...",
            "_score": 87.0,
            "_source": {
                "place_id": 12345,          # int for legacy, str for CRC
                "title": "Istanbul",
                "names": [{"toponym": "Constantinople"}, ...],
                "searchy": ["constantinople", ...],
                "ccodes": ["TR"],
                "whg_id": ...,              # absent for CRC hits
                "geoms": [{"location": {"type": "Point", "coordinates": [...]}}],
            }
        }
    """
    hits = data.get("hits", [])
    adapted = []

    for hit in hits:
        place_id = hit.get("place_id", "")
        title = hit.get("title", "")
        names_raw = hit.get("names", [])
        ccodes = hit.get("ccodes", [])
        score = hit.get("score", 0)
        geometries = hit.get("geometries", [])

        # Build names in legacy format
        names = [{"toponym": n.get("label", "")} for n in names_raw if n.get("label")]

        # Build searchy (lowercased name list for text matching)
        searchy = list({n.get("label", "").lower() for n in names_raw if n.get("label")})
        if title and title.lower() not in searchy:
            searchy.append(title.lower())

        # Build geoms in legacy format — prefer full geometries,
        # fall back to repr_point.  The gateway may return any of:
        #   • {type, coordinates}  — full GeoJSON geometry
        #   • {location: {type, coordinates}}  — ES-style wrapper
        #   • {repr_point: [lon, lat]}  — centroid only
        geoms = []
        for g in geometries:
            if isinstance(g, dict):
                # Full GeoJSON geometry object
                if g.get("type") and g.get("coordinates"):
                    geoms.append({
                        "location": {
                            "type": g["type"],
                            "coordinates": g["coordinates"],
                        }
                    })
                # ES-style wrapper
                elif isinstance(g.get("location"), dict):
                    loc = g["location"]
                    if loc.get("type") and loc.get("coordinates"):
                        geoms.append({"location": loc})
                # Legacy repr_point inside a geometries entry
                else:
                    rp = g.get("repr_point")
                    if rp and len(rp) == 2:
                        geoms.append({
                            "location": {
                                "type": "Point",
                                "coordinates": rp,
                            }
                        })

        # Fall back to top-level repr_point when no full geometries exist
        if not geoms:
            rp = hit.get("repr_point")
            if rp and len(rp) == 2:
                geoms.append({
                    "location": {
                        "type": "Point",
                        "coordinates": rp,
                    }
                })

        adapted.append({
            "_id": f"crc_{place_id}",
            "_score": score,
            "_source": {
                "place_id": place_id,  # Namespaced, e.g. "gn:745044"
                "title": title,
                "names": names,
                "searchy": searchy,
                "ccodes": ccodes,
                "geoms": geoms,
                # Mark as CRC-sourced so make_candidate can optionally tag it
                "_crc_source": True,
            },
        })

    return adapted

