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

Access is gated by the ``SiteSetting.crc_gateway_mode`` admin setting:
  - ``disabled``   → never call the gateway
  - ``admin_only`` → only for staff / superuser requests (useful for testing)
  - ``all_users``  → every authenticated request
"""

import logging

import requests
from django.conf import settings

logger = logging.getLogger("reconciliation")


def _is_enabled_for_user(user) -> bool:
    """
    Check whether CRC gateway integration should fire for *this* user.

    Reads the ``SiteSetting.crc_gateway_mode`` value from the database
    (auto-created on first access) and compares against the user object.
    """
    # Must have a gateway URL configured at all
    if not getattr(settings, "CRC_GATEWAY_URL", ""):
        return False

    # Import here to avoid circular imports at module level
    from main.models import SiteSetting

    try:
        mode = SiteSetting.load().crc_gateway_mode
    except Exception:
        # DB not migrated yet, table missing, etc. — fail safe
        return False

    if mode == "disabled":
        return False

    if mode == "admin_only":
        return user is not None and user.is_authenticated and (user.is_staff or user.is_superuser)

    if mode == "all_users":
        return user is not None and user.is_authenticated

    return False


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

def crc_reconcile_search(normalised_query: dict, user=None) -> list[dict]:
    """
    Call the CRC gateway ``/api/reconcile`` endpoint.

    Args:
        normalised_query: The dict returned by ``normalise_query_params()``
            in reconcile.py.  Keys used: ``query_text``, ``raw`` (original
            params dict), ``bounds``, ``size``.
        user: The Django User instance from ``request.user``.

    Returns:
        List of dicts in the same shape as ES ``hits.hits`` entries, i.e.
        ``{"_id": ..., "_score": ..., "_source": {...}}``, ready for
        ``make_candidate()``.  Returns ``[]`` on any error.
    """
    if not _is_enabled_for_user(user):
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


def crc_suggest_search(prefix: str, mode: str = "starts", limit: int = 10, user=None) -> list[dict]:
    """
    Call the CRC gateway for suggest/typeahead results.

    Uses the same ``/api/reconcile`` endpoint with a small size.

    Returns:
        List of adapted ES-style hit dicts.
    """
    if not _is_enabled_for_user(user):
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

        # Build geoms in legacy format
        geoms = []
        for g in geometries:
            rp = g.get("repr_point")
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

