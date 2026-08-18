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


def crc_health(user=None) -> bool:
    """Quick reachability probe of the CRC gateway (the Pitt CRC VM).

    Returns ``True`` when the gateway answers at all (any HTTP status — even a
    404 means the VM/service is reachable), ``False`` when it is unconfigured,
    the user isn't eligible, or the host is unreachable (connection refused /
    timeout — i.e. the VM is down). Uses a SHORT fixed timeout so a down VM
    doesn't stall the Atlas page: this is a liveness check, not a query.
    """
    if not _is_enabled(user):
        return False
    try:
        # A cheap GET to the gateway root; any HTTP response = reachable.
        requests.get(_gateway_url() + "/", headers=_headers(), timeout=3)
        return True
    except (requests.Timeout, requests.ConnectionError):
        return False
    except Exception:  # pragma: no cover — defensive; treat as reachable-unknown
        return False


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def crc_places(ids: list, user=None) -> dict | None:
    """Call the CRC gateway ``POST /api/places`` and return the full response.

    Fetches complete ``PlaceDetail`` records (names, types, geometries, links,
    descriptions, relations, timespans, …) by namespaced place id — the data
    source for the dynamic Atlas portal. Returns ``None`` when the gateway is
    unconfigured or the call fails.
    """
    if not _is_enabled(user):
        return None
    if isinstance(ids, str):
        ids = [ids]
    ids = [str(i) for i in (ids or []) if i]
    if not ids:
        return None
    try:
        url = f"{_gateway_url()}/api/places"
        resp = requests.post(url, json={"ids": ids}, headers=_headers(), timeout=_timeout())
        if 200 <= resp.status_code < 300:
            return resp.json()
        logger.warning("CRC gateway POST /api/places %s: %s",
                       resp.status_code, resp.text[:200])
        return None
    except (requests.Timeout, requests.ConnectionError) as exc:
        logger.warning("CRC gateway /api/places network error: %s", exc)
    except Exception as exc:  # pragma: no cover — defensive
        logger.warning("CRC gateway /api/places unexpected: %s", exc)
    return None


def crc_search(options: dict, user=None) -> dict | None:
    """Fail-safe wrapper over :func:`crc_search_status` — the response, or None.

    Keeps the "any failure is None" contract every caller but the Atlas view
    relies on. Use ``crc_search_status`` when the *kind* of failure matters.
    """
    data, _failure = crc_search_status(options, user=user)
    return data


def crc_search_status(options: dict, user=None) -> tuple[dict | None, str | None]:
    """Call the CRC gateway ``POST /api/search`` and return the FULL response.

    Unlike ``crc_reconcile_search`` (which adapts hits for the reconciliation
    make_candidate() path), this returns the gateway's complete SearchResponse
    dict — ``hits``, ``edges``, ``clustering_params``, ``toponym_stoplist``,
    ``facets`` — so the browser Atlas clusterer
    (``whg/webpack/js/clustering.js``) receives all the fuel.

    ``options`` is the Atlas search payload (atlas.js ``gatherToponymOptions``):
    ``qstr``, ``types`` (AAT ids), ``bounds`` (GeoJSON), ``start`` / ``end`` /
    ``temporal_mode`` / ``undated``, ``exact``, ``countries``, ``namespaces``,
    ``size``.

    Always opts into the clustering fuel (include_hard_links /
    include_clustering_fields / include_embeddings) and full geometry so the
    map can plot results.

    Returns ``(response, failure)``. ``failure`` is None on success and
    otherwise names *why* nothing came back, because the two reasons deserve
    different words to the user:

    ``"unreachable"``
        Connection refused / DNS / the VM is down. Search really is offline.
    ``"timeout"``
        The gateway answered too slowly. It is **up**; this one query was slow.
        Search latency is genuinely variable (median ~1.9 s, occasional
        multi-second spikes against a 10 s ``CRC_GATEWAY_TIMEOUT``), so telling
        the user the service is "offline" is both wrong and sticky — the Atlas
        banner then stays up over a service that is answering fine.
    ``"disabled"``
        Gateway unconfigured, or the user isn't eligible.

    A read timeout is retried **once**: the retry usually returns in about the
    median, so most would-be failures become a merely-slow search. Connection
    errors are not retried — a refused connection will be refused again, and
    retrying only doubles the wait before the user is told.
    """
    if not _is_enabled(user):
        return None, "disabled"

    # Search mode: an explicit, valid `mode` wins (e.g. the Place List sends
    # "in" for substring/"contains" matching); otherwise fall back to the
    # exact/fuzzy toggle used by the main toponym search.
    _valid_modes = {"exact", "starts", "in", "fuzzy", "phonetic"}
    _mode = options.get("mode")
    if _mode not in _valid_modes:
        _mode = "exact" if options.get("exact") else "fuzzy"

    body = {
        "query": (options.get("qstr") or "").strip() or None,
        "mode": _mode,
        "size": int(options.get("size") or 100),
        "geom": "full",
        "include_hard_links": True,
        "include_clustering_fields": True,
        "include_embeddings": True,
    }

    # Browse mode (Atlas Place List): enumerate a whole gazetteer with no query
    # — the gateway returns a namespace-filtered, alphabetically-ordered match-all
    # with a real total and offset pagination. Ignored by the gateway when a query
    # is present.
    if options.get("browse"):
        body["browse"] = True

    # Offset pagination (browse list + any paged consumer).
    offset = options.get("offset")
    if offset:
        try:
            body["offset"] = max(0, int(offset))
        except (TypeError, ValueError):
            pass

    countries = options.get("countries")
    if countries:
        if isinstance(countries, str):
            countries = [c.strip().upper() for c in countries.split(",") if c.strip()]
        if countries:
            body["ccodes"] = countries

    types = options.get("types")
    if types:
        if isinstance(types, str):
            types = [t.strip() for t in types.split(",") if t.strip()]
        if types:
            body["types"] = types

    # AAT type facet filter — hierarchical (concept + descendants), list of ints.
    aat_types = options.get("aat_types")
    if aat_types:
        if isinstance(aat_types, str):
            aat_types = aat_types.split(",")
        ids = []
        for t in aat_types:
            s = str(t).strip()
            if s.isdigit():
                ids.append(int(s))
        if ids:
            body["aat_types"] = ids

    namespaces = options.get("namespaces")
    if namespaces:
        if isinstance(namespaces, str):
            namespaces = [n.strip() for n in namespaces.split(",") if n.strip()]
        if namespaces:
            body["namespaces"] = namespaces

    bounds = options.get("bounds")
    if isinstance(bounds, dict) and bounds.get("type") and (
        bounds.get("coordinates") or bounds.get("geometries")
    ):
        body["bounds"] = bounds

    if options.get("temporal"):
        start = options.get("start")
        end = options.get("end")
        if start is not None:
            body["start_year"] = int(start)
        if end is not None:
            body["end_year"] = int(end)
        if options.get("undated"):
            body["undated"] = True
        # Which of the four temporal bounds the window is tested against
        # (place#164/#169): "possibly" admits anything the source's bounds do not
        # rule out, "definitely" requires an attested core. The gateway rejects
        # anything else with a 422, so only pass a recognised value on.
        mode = options.get("temporal_mode")
        if mode in ("possibly", "definitely"):
            body["temporal_mode"] = mode

    url = f"{_gateway_url()}/api/search"
    # attempt 0, then one retry reserved for a read timeout (see docstring).
    for attempt in (0, 1):
        try:
            logger.info("CRC gateway POST %s  q=%r%s", url, body.get("query"),
                        "  (retry)" if attempt else "")
            resp = requests.post(url, json=body, headers=_headers(), timeout=_timeout())
            if 200 <= resp.status_code < 300:
                return resp.json(), None
            logger.warning("CRC gateway POST /api/search %s: %s",
                           resp.status_code, resp.text[:200])
            return None, "error"
        except requests.Timeout as exc:
            logger.warning("CRC gateway /api/search timed out (attempt %d): %s",
                           attempt + 1, exc)
            if attempt == 0:
                continue
            return None, "timeout"
        except requests.ConnectionError as exc:
            logger.warning("CRC gateway /api/search unreachable: %s", exc)
            return None, "unreachable"
        except Exception as exc:  # pragma: no cover — defensive
            logger.warning("CRC gateway /api/search unexpected: %s", exc)
            return None, "error"
    return None, "timeout"  # pragma: no cover — loop always returns


def crc_reconcile_search(normalised_query: dict, user=None, namespaces: set[str] | None = None,
                         meta: dict | None = None) -> list[dict]:
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
        meta: Optional dict, populated in place with the gateway's
            response-level metadata (``scope``, ``variants_used``) so the
            caller can propagate it to the client.  Left untouched when the
            gateway predates those fields or the call failed — callers MUST
            treat a missing ``scope`` as "old gateway, previous behaviour"
            and never infer ``applied: False`` from its absence (place#144).

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

    # Client-supplied Symphonym query embedding (int8, 128-d, language-conditioned in the browser).
    # Forward it as query_vector and force phonetic mode so the gateway ranks by it directly, skipping
    # its own server-side embed. Harmless on older gateways (they ignore the unknown field).
    embedding = raw.get("embedding")
    if isinstance(embedding, (list, tuple)) and len(embedding) == 128:
        body["query_vector"] = list(embedding)
        body["mode"] = "phonetic"

    # Name variants (alt_names, issue #143): alternative spellings for this row, forwarded so the gateway
    # tries them alongside the primary toponym when ranking candidates. Accept a list or ';'-delimited
    # string. Harmless on older gateways that don't yet read the field (they ignore unknown keys).
    variants = raw.get("variants")
    if variants:
        if isinstance(variants, str):
            variants = [v.strip() for v in variants.split(";") if v.strip()]
        variants = [str(v).strip() for v in variants if str(v).strip()]
        if variants:
            body["variants"] = variants
            # Client-computed int8 128-d Symphonym embeddings, one per variant, POSITIONALLY aligned
            # with `variants` — lets the gateway skip the server-side embed. A null entry means "embed
            # this one yourself". Only forwarded when the lengths agree, since a mismatch would
            # silently pair vectors with the wrong spellings.
            vecs = raw.get("variant_vectors")
            if isinstance(vecs, (list, tuple)) and len(vecs) == len(variants):
                body["variant_vectors"] = [
                    list(v) if isinstance(v, (list, tuple)) and len(v) == 128 else None
                    for v in vecs
                ]

    # Country codes – accept list ["US","GB"] or comma-delimited string "US,GB"
    countries = raw.get("countries")
    if countries:
        if isinstance(countries, str):
            countries = [c.strip().upper() for c in countries.split(",") if c.strip()]
        body["ccodes"] = countries

    # Namespace filter (e.g. ["wd", "gn"]). Prefer the caller-computed CRC set: it has the legacy
    # "whg" pseudo-namespace stripped out, which the gateway doesn't know — forwarding it verbatim
    # would narrow the gateway query to an unknown source. Fall back to whatever the client sent.
    ns = namespaces if namespaces else raw.get("namespaces")
    if ns:
        if isinstance(ns, str):
            ns = [n.strip() for n in ns.split(",") if n.strip()]
        elif isinstance(ns, (set, frozenset)):
            ns = sorted(ns)
        body["namespaces"] = list(ns)

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

    # Spatial: a RADIAL filter goes to the gateway as lat/lng/radius so it can be
    # resolved as an H3 disc — a terms match on the covers already in the index.
    # Converting it to a polygon here (which normalise_query_params does, for the
    # legacy path's benefit) would send it back through Shapely union + polyfill +
    # prepared-geometry for no gain. See place#184.
    lat, lng, radius = (raw.get("lat"), raw.get("lng"), raw.get("radius"))
    if lat is not None and lng is not None and radius:
        body["lat"] = float(lat)
        body["lng"] = float(lng)
        body["radius"] = float(radius)
    else:
        bounds = normalised_query.get("bounds")
        if bounds:
            body["bounds"] = bounds

    # Spatial containment by reference place_ids, with optional mode + relation.
    contained_in = raw.get("contained_in")
    if contained_in:
        if isinstance(contained_in, str):
            contained_in = [c.strip() for c in contained_in.split(",") if c.strip()]
        body["contained_in"] = contained_in
    containment = raw.get("containment")
    if containment in ("fuzzy", "exact"):
        body["containment"] = containment
    relation = raw.get("relation")
    if relation in ("intersects", "within"):
        body["relation"] = relation

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
        try:
            resp = requests.post(url, json=body, headers=_headers(), timeout=_timeout())
        except requests.ConnectionError as first:
            # Retry ONCE on a dropped connection. A gateway worker that dies mid-flight
            # (2026-08-18: GEOS segfaults under concurrent containment) drops every
            # request it was holding, and the fail-safe below turns that into an empty
            # result — so the row records "no match" and the user is told nothing. The
            # supervisor has already replaced the worker by the time we get here, so a
            # single retry lands on a live one. Safe to repeat: a reconcile search is a
            # read. NOT retried on timeout — that request may still be running, and
            # repeating it would double the load that caused it.
            logger.warning("CRC gateway connection dropped (%s) — retrying once", first)
            resp = requests.post(url, json=body, headers=_headers(), timeout=_timeout())
        logger.info("CRC gateway /api/reconcile response: %s", resp.status_code)
        resp.raise_for_status()
        data = resp.json()
    # On every failure path record the reason in `meta`. The caller needs to tell "the gateway had
    # nothing to say about scope" (older gateway → keep previous behaviour) apart from "the gateway
    # never answered", because with an active scope it suppresses the legacy hits and would otherwise
    # return an unexplained empty result. See place#144.
    except requests.Timeout:
        logger.warning("CRC gateway timeout after %ss", _timeout())
        if meta is not None:
            meta["error"] = "timeout"
        return []
    except requests.ConnectionError as e:
        logger.warning("CRC gateway connection error: %s", e)
        if meta is not None:
            meta["error"] = "connection"
        return []
    except requests.HTTPError as e:
        logger.warning("CRC gateway HTTP error: %s", e)
        if meta is not None:
            meta["error"] = "http"
        return []
    except Exception as e:
        logger.warning("CRC gateway unexpected error: %s", e)
        if meta is not None:
            meta["error"] = "unexpected"
        return []

    # Response-level metadata (place#144). `scope` is present only when the query carried
    # contained_in/bounds; `variants_used` echoes the alt_names actually queried after dedupe and
    # the gateway's 10-form cap. Both are absent on gateways predating the change — we simply leave
    # `meta` unset in that case so the caller keeps the previous behaviour.
    if meta is not None and isinstance(data, dict):
        if data.get("scope") is not None:
            meta["scope"] = data["scope"]
        if data.get("variants_used") is not None:
            meta["variants_used"] = data["variants_used"]
        # Source namespaces echoed by the gateway (place#157). `namespaces` is the
        # set actually present in the hits; `namespaces_searched` is the request's
        # positive scope — echoed on EVERY return path including the empty ones,
        # which is precisely the case deriving namespaces from result ids cannot
        # reach (a source searched that contributed nothing still has terms the
        # consumer should see). `[]` for `namespaces_searched` means unrestricted.
        # Both absent on a gateway predating the change; the caller then falls
        # back to id-derivation.
        if data.get("namespaces") is not None:
            meta["namespaces"] = data["namespaces"]
        if data.get("namespaces_searched") is not None:
            meta["namespaces_searched"] = data["namespaces_searched"]

    return _adapt_hits(data)


def crc_fetch_places(place_ids: list[str], user=None) -> dict[str, dict]:
    """
    Fetch full place data from the CRC gateway by namespaced IDs.

    Calls ``POST /api/places`` with body ``{"ids": [...]}``.

    Args:
        place_ids: List of namespaced CRC place IDs, e.g. ``["gn:745044", "tgn:7010731"]``.
        user: Django User instance.

    Returns:
        Dict mapping each place_id to its data dict (with keys like
        ``title``, ``names``, ``ccodes``, ``geometries``, etc.).
        Missing/errored IDs are omitted.  Returns ``{}`` on any error.
    """
    if not _is_enabled(user):
        return {}

    if not place_ids:
        return {}

    try:
        url = f"{_gateway_url()}/api/places"
        logger.info("CRC gateway GET %s  ids=%s", url, place_ids)
        resp = requests.post(
            url,
            json={"ids": place_ids},
            headers=_headers(),
            timeout=_timeout(),
        )
        logger.info("CRC gateway /api/places response: %s", resp.status_code)
        resp.raise_for_status()
        data = resp.json()
    except requests.Timeout:
        logger.warning("CRC gateway /api/places timeout after %ss", _timeout())
        return {}
    except requests.ConnectionError as e:
        logger.warning("CRC gateway /api/places connection error: %s", e)
        return {}
    except requests.HTTPError as e:
        logger.warning("CRC gateway /api/places HTTP error: %s — extend unavailable for CRC entities", e)
        return {}
    except Exception as e:
        logger.warning("CRC gateway /api/places unexpected error: %s", e)
        return {}

    # Expected response: {"places": [{"place_id": "gn:745044", "title": ..., ...}, ...]}
    result = {}
    for place in data.get("places", data.get("hits", [])):
        pid = str(place.get("place_id", ""))
        if pid:
            result[pid] = place

    return result


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
                entry = {}
                # Full GeoJSON geometry object
                if g.get("type") and g.get("coordinates"):
                    entry["location"] = {"type": g["type"], "coordinates": g["coordinates"]}
                # ES-style wrapper
                elif isinstance(g.get("location"), dict):
                    loc = g["location"]
                    if loc.get("type") and loc.get("coordinates"):
                        entry["location"] = loc
                # Preserve the indexed representative point ALWAYS (not only when a full geometry is
                # absent) — it's the authoritative point for the place; the polygon isn't a substitute.
                rp = g.get("repr_point")
                if rp and len(rp) == 2:
                    entry["repr_point"] = rp
                    entry.setdefault("location", {"type": "Point", "coordinates": rp})
                if entry:
                    geoms.append(entry)

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
                # True iff the place has a full polygon geometry (usable as a contained_in
                # region). The gateway flags this per geometry; surface it for the candidate.
                "has_geom": any(isinstance(g, dict) and g.get("has_geom") for g in geometries),
                # Forward the gateway's authority/Wikipedia links so make_candidate can surface
                # Wikipedia enrichment for Wikidata-backed candidates (previously dropped here).
                "links": hit.get("links", []) or [],
                # The place's own AAT types, so a client can enrich with what the record IS
                # rather than the protocol's entity type ("Place"). Dropped here previously,
                # which left every gateway candidate typeless. See place#184.
                "types": hit.get("types", []) or [],
                "aat_ids": hit.get("aat_ids", []) or [],
                # Mark as CRC-sourced so make_candidate can optionally tag it
                "_crc_source": True,
            },
        })

    return adapted


# ---------------------------------------------------------------------------
# Hard-link forwarding to the gateway (Master Plan §2a)
# ---------------------------------------------------------------------------
#
# When a contributor creates / revokes an attestation in this Django app,
# the row is forwarded to the gateway's /api/links endpoint so the Pitt
# SQLite hard-link overlay stays in sync without waiting for the next
# Batch 12 contributor_replay run. Forwarding is best-effort: a failure
# logs a warning and is reconciled by the next batch run (the SQLite
# is rebuildable from DO PG at any time).


def crc_post_link(payload: dict) -> bool:
    """POST a single hard-link assertion to the gateway.

    Payload shape (matches ``processing/staging_contract.HARD_LINK_REQUIRED_FIELDS``)::

        {
          "place_a":         "<ns>:<id>",     # canonical-ordered: place_a < place_b
          "place_b":         "<ns>:<id>",
          "relation_type":   "sameAs"|"exactMatch"|"closeMatch"|"distinct",
          "source_category": "contributor",
          "source_id":       "contributor:<user_id>[:legacy_v3_2]",
          "asserted_at":     "<ISO 8601>"|null,
          "justification":   "..."|null
        }

    Returns ``True`` on a 2xx response; ``False`` on any error (logged).
    Always returns ``False`` when no gateway is configured — the next
    Batch 12 run will pick the row up.
    """
    if not getattr(settings, "CRC_GATEWAY_URL", ""):
        return False
    try:
        url = f"{_gateway_url()}/api/links"
        resp = requests.post(
            url, json=payload, headers=_headers(), timeout=_timeout(),
        )
        if 200 <= resp.status_code < 300:
            return True
        logger.warning("CRC gateway POST /api/links %s: %s",
                       resp.status_code, resp.text[:200])
        return False
    except (requests.Timeout, requests.ConnectionError) as exc:
        logger.warning("CRC gateway POST /api/links network error: %s", exc)
    except Exception as exc:  # pragma: no cover — defensive
        logger.warning("CRC gateway POST /api/links unexpected: %s", exc)
    return False


def crc_delete_link(payload: dict) -> bool:
    """DELETE a single hard-link assertion on the gateway.

    Payload identifies the assertion by the ``UNIQUE`` overlay key
    ``(place_a, place_b, relation_type, source_id)``::

        {
          "place_a":       "<ns>:<id>",
          "place_b":       "<ns>:<id>",
          "relation_type": "...",
          "source_id":     "contributor:<user_id>[:legacy_v3_2]"
        }

    Returns ``True`` on 2xx; ``False`` otherwise. Reconciled by the next
    Batch 12 run on failure.
    """
    if not getattr(settings, "CRC_GATEWAY_URL", ""):
        return False
    try:
        url = f"{_gateway_url()}/api/links"
        # The gateway accepts identification via JSON body on DELETE so we
        # don't have to URL-encode 4 fields.
        resp = requests.request(
            "DELETE", url, json=payload, headers=_headers(),
            timeout=_timeout(),
        )
        if 200 <= resp.status_code < 300:
            return True
        logger.warning("CRC gateway DELETE /api/links %s: %s",
                       resp.status_code, resp.text[:200])
        return False
    except (requests.Timeout, requests.ConnectionError) as exc:
        logger.warning("CRC gateway DELETE /api/links network error: %s", exc)
    except Exception as exc:  # pragma: no cover
        logger.warning("CRC gateway DELETE /api/links unexpected: %s", exc)
    return False
