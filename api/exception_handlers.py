"""Custom DRF exception handling for the API.

Translates an unreachable Elasticsearch indexing server into a clear
503 JSON response instead of an opaque 500, for every DRF endpoint (e.g. the
reconciliation views). Plain Django views guard themselves via
`elastic.health.index_unavailable_response`.
"""

import logging

from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import exception_handler as drf_default_handler

from elastic.health import INDEX_DOWN_EXCEPTIONS, index_unavailable_payload

logger = logging.getLogger(__name__)


def api_exception_handler(exc, context):
    """Map index-down errors to 503; delegate everything else to DRF."""
    if isinstance(exc, INDEX_DOWN_EXCEPTIONS):
        view = context.get('view').__class__.__name__ if context.get('view') else '?'
        logger.warning("Indexing server unavailable during API request (%s): %s", view, exc)
        return Response(index_unavailable_payload(), status=status.HTTP_503_SERVICE_UNAVAILABLE)
    return drf_default_handler(exc, context)
