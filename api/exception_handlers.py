"""Custom DRF exception handling for the API.

Translates an unreachable Elasticsearch indexing server into a clear
503 JSON response instead of an opaque 500, for every DRF endpoint (e.g. the
reconciliation views). Plain Django views guard themselves via
`elastic.health.index_unavailable_response`.

Also renders `GatewayUnavailable` (place#272) and `DailyLimitExceeded`, which
must not go through DRF's default rendering — see the notes on those classes.
"""

import logging

from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import exception_handler as drf_default_handler

from api.authentication import DailyLimitExceeded
from api.gateway_errors import GatewayUnavailable
from elastic.health import INDEX_DOWN_EXCEPTIONS, index_unavailable_payload

logger = logging.getLogger(__name__)


def api_exception_handler(exc, context):
    """Map index-down errors to 503; delegate everything else to DRF."""
    if isinstance(exc, GatewayUnavailable):
        # Built here rather than from `exc.detail`: DRF coerces every leaf of a
        # structured detail to `ErrorDetail(str)`, so `False` would ship as the
        # string "False" — truthy in JavaScript, which inverts the meaning of
        # `gateway.answered` for exactly the clients this field exists for
        # (place#272).
        return Response(exc.payload(),
                        status=status.HTTP_503_SERVICE_UNAVAILABLE,
                        headers={"Retry-After": str(exc.wait)})
    if isinstance(exc, DailyLimitExceeded):
        return Response(exc.payload(), status=status.HTTP_429_TOO_MANY_REQUESTS,
                        headers={"Retry-After": str(exc.wait)})
    if isinstance(exc, INDEX_DOWN_EXCEPTIONS):
        view = context.get('view').__class__.__name__ if context.get('view') else '?'
        logger.warning("Indexing server unavailable during API request (%s): %s", view, exc)
        return Response(index_unavailable_payload(), status=status.HTTP_503_SERVICE_UNAVAILABLE)
    return drf_default_handler(exc, context)
