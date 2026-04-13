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

Access is environment-controlled:
  - ``CRC_GATEWAY_URL`` must be set (e.g. in env vars or local_settings).
  - The requesting user must be authenticated.
  - To test before going live, configure the URL only on the dev server.
"""

import logging

import requests
from django.conf import settings

logger = logging.getLogger("reconciliation")


def _is_enabled(user=None) -> bool:
    """
    Check whether CRC gateway integration is active.

    The gateway is enabled when:
    1. ``CRC_GATEWAY_URL`` is configured in settings (env var or local_settings).
    2. The user is authenticated.

    This keeps gating purely environment-based: configure the URL on dev
    for testing, leave it unset on production until ready to go live.
    """
    gateway_url = getattr(settings, "CRC_GATEWAY_URL", "")
    if not gateway_url:
        return False

    if user is None or not user.is_authenticated:
        return False

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

def crc_reconcile_search(normalised_query: dict, user=None, namespaces: set[str] | None = None) -> list[dict]:
    """
    Call the CRC gateway ``/api/reconcile`` endpoint.

    Args:
        normalised_query: The dict returned by ``normalise_query_params()``
            in reconcile.py.  Keys used: ``query_text``, ``raw`` (original
            params dict), ``bounds``, ``size``.
        user: The Django User instance from ``request.user``.
        namespaces: Optional set of CRC namespace codes to restrict
            results to (e.g. ``{"gn", "tgn"}``).  ``None`` means no
            filtering.  The set should *not* contain ``"whg"`` (legacy
            places are handled separately).

    Returns:
        List of dicts in the same shape as ES ``hits.hits`` entries, i.e.
        ``{"_id": ..., "_score": ..., "_source": {...}}``, ready for
        ``make_candidate()``.  Returns ``[]`` on any error.
    """
    if not _is_enabled(user):
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

    # Feature classes (e.g. ["A", "P"])
    fclasses = normalised_query.get("fclasses") or raw.get("fclasses")
    if fclasses:
        if isinstance(fclasses, str):
            fclasses = [f.strip() for f in fclasses.split(",") if f.strip()]
        body["fclasses"] = fclasses

    # AAT place types (e.g. ["aat:300008347"])
    types = normalised_query.get("types") or raw.get("types")
    if types:
        if isinstance(types, str):
            types = [t.strip() for t in types.split(",") if t.strip()]
        body["types"] = types

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
        url = f"{_gateway_url()}/api/reconcile"
        logger.info("CRC gateway POST %s", url)
        resp = requests.post(
            url,
            json=body,
            headers=_headers(),
            timeout=_timeout(),
        )
        logger.info("CRC gateway /api/reconcile response: %s", resp.status_code)
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


def crc_extend(entity_ids: list[str], properties: list, user=None) -> dict:
    """
    Call the CRC gateway ``POST /api/extend`` endpoint for data extension.

    Forwards an OpenRefine-style extend request for CRC-sourced place records
    (namespaced IDs like ``"place:wd:Q16202"``) to the gateway, which looks up
    records and extracts the requested properties.

    Args:
        entity_ids: Full entity IDs including the type prefix,
            e.g. ``["place:wd:Q16202", "place:gn:745044"]``.
        properties: Property list in OpenRefine extend format,
            e.g. ``[{"id": "whg:geometry_geojson"}, {"id": "whg:names_canonical"}]``.
        user: Django User instance (for access check).

    Returns:
        Dict mapping entity_id → property values dict, e.g.::

            {
                "place:wd:Q16202": {
                    "whg:geometry_geojson": [{"str": "..."}],
                    "whg:names_canonical": [{"str": "Istanbul"}]
                }
            }

        Returns ``{}`` on any error.
    """
    if not _is_enabled(user):
        return {}

    if not entity_ids:
        return {}

    body = {
        "ids": entity_ids,
        "properties": properties,
    }

    try:
        url = f"{_gateway_url()}/api/extend"
        logger.info("CRC gateway POST %s  ids=%s", url, entity_ids)
        resp = requests.post(
            url,
            json=body,
            headers=_headers(),
            timeout=_timeout(),
        )
        logger.info("CRC gateway /api/extend response: %s", resp.status_code)
        resp.raise_for_status()
        data = resp.json()
    except requests.Timeout:
        logger.warning("CRC gateway /api/extend timeout after %ss", _timeout())
        return {}
    except requests.ConnectionError as e:
        logger.warning("CRC gateway /api/extend connection error: %s", e)
        return {}
    except requests.HTTPError as e:
        logger.warning("CRC gateway /api/extend HTTP error: %s", e)
        return {}
    except Exception as e:
        logger.warning("CRC gateway /api/extend unexpected error: %s", e)
        return {}

    return data.get("rows", {})


def crc_suggest_search(prefix: str, mode: str = "starts", limit: int = 10, user=None,
                       namespaces: set[str] | None = None,
                       ccodes: list[str] | None = None,
                       fclasses: list[str] | None = None,
                       types: list[str] | None = None) -> list[dict]:
    """
    Call the CRC gateway for suggest/typeahead results.

    Uses the same ``/api/reconcile`` endpoint with a small size.

    Args:
        prefix: The search prefix text.
        mode: Search mode (``"starts"`` or ``"fuzzy"``).
        limit: Maximum number of results.
        user: Django User instance.
        namespaces: Optional set of CRC namespace codes to filter by.
        ccodes: Optional list of ISO country codes to filter by.
        fclasses: Optional list of GeoNames feature classes to filter by.
        types: Optional list of AAT type identifiers to filter by.

    Returns:
        List of adapted ES-style hit dicts.
    """
    if not _is_enabled(user):
        return []

    body = {
        "query": prefix,
        "mode": mode,
        "size": limit,
    }

    # Namespace filter (e.g. ["gn", "tgn"])
    if namespaces:
        body["namespaces"] = sorted(namespaces)

    # Country codes (e.g. ["US", "GB"])
    if ccodes:
        body["ccodes"] = ccodes

    # Feature classes (e.g. ["A", "P"])
    if fclasses:
        body["fclasses"] = fclasses

    # AAT place types (e.g. ["aat:300008347"])
    if types:
        body["types"] = types

    try:
        url = f"{_gateway_url()}/api/reconcile"
        logger.info("CRC gateway suggest POST %s", url)
        resp = requests.post(
            url,
            json=body,
            headers=_headers(),
            timeout=_timeout(),
        )
        logger.info("CRC gateway suggest response: %s", resp.status_code)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning("CRC gateway suggest error: %s", e)
        return []

    return _adapt_hits(data)


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
