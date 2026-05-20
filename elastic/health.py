"""Shared helpers for detecting an unreachable Elasticsearch indexing server.

The whole site depends on the indexing server; when it is down, search, place
lookups and reconciliation cannot run. This module centralises:

  * `index_available()` — a cheap, never-raising liveness probe used by the
    `/search/health/` endpoint and the client-side sniffer.
  * `INDEX_DOWN_EXCEPTIONS` — the `settings.ES_CONN` exceptions that mean the
    server is unreachable/unresponsive (as opposed to `ApiError`/
    `BadRequestError`, which mean a healthy server rejected the query).
  * Standard JSON error payload/response helpers so every endpoint reports a
    down index the same way.
"""

import logging

import requests
from django.conf import settings
from django.http import JsonResponse
from elasticsearch8.exceptions import ConnectionError as ESConnectionError
from elasticsearch8.exceptions import ConnectionTimeout

logger = logging.getLogger(__name__)

# Raised by settings.ES_CONN when the indexing server cannot be reached or does
# not respond in time. Deliberately excludes ApiError/BadRequestError, which
# indicate a query problem on an otherwise-healthy server.
INDEX_DOWN_EXCEPTIONS = (ESConnectionError, ConnectionTimeout)

INDEX_DOWN_MESSAGE = (
    "The indexing server is currently unavailable, so search and place "
    "lookups cannot be completed. Please try again shortly."
)


def index_available(timeout=3):
    """Return True if the indexing server reports a usable cluster status.

    Never raises: logs and returns False on any connection/timeout/parse error.
    """
    url = f"{settings.ES_SCHEME}://{settings.ES_HOST}:{settings.ES_PORT}/_cluster/health"
    try:
        resp = requests.get(url, auth=('elastic', settings.ELASTIC_PASSWORD), timeout=timeout)
        if resp.ok:
            return resp.json().get('status') in ('green', 'yellow')
    except requests.RequestException as e:
        logger.warning(f"Index health check failed: {e}")
    return False


def index_unavailable_payload():
    """Standard JSON body returned by API endpoints when the index is down."""
    return {'error': 'index_unavailable', 'message': INDEX_DOWN_MESSAGE}


def index_unavailable_response(status=503):
    """Django `JsonResponse` for the index-down case (for non-DRF views)."""
    return JsonResponse(index_unavailable_payload(), status=status)
