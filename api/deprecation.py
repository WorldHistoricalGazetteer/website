"""One deprecation notice for the legacy v3 JSON endpoints (SG, 2026-11-17: all
retired when WHG moves to v4; no date set, so no ``Sunset``).

``deprecated_endpoint(key)`` wraps a view callable in ``api/urls.py``, so the views
themselves are untouched. Every response it wraps gets:

* ``Deprecation: @1794873600`` (RFC 9745 date form: 2026-11-17) and ``Link:
  <successor>; rel="successor-version"`` plus ``<docs>; rel="deprecation"``;
* when the body is a JSON object, a ``deprecation`` member inserted as its FIRST
  key, on errors too. A bare JSON list, HTML or a file cannot carry a member
  without breaking its consumers, so those get the headers only. The member is
  spliced into the serialised text rather than re-encoding the body, so every
  existing field and its formatting stay exactly as they were;
* one ``logger.info`` line ("legacy api call endpoint=... origin=in-app|external
  ...") so retirement can be measured on external use. ``in-app`` means the
  request carried a same-origin Referer or a session cookie.

The replacements are the ones content/technical/apis.md documents. Where the docs
name none, the notice says "replacement to be confirmed" rather than guess.
"""

import json
import logging
from functools import wraps
from urllib.parse import urlparse

from django.conf import settings

logger = logging.getLogger(__name__)

DEPRECATION_HEADER = "@1794873600"
DOCS = "https://docs.whgazetteer.org/content/technical/apis.html"
DOCS_SECTION = DOCS + "#deprecated-legacy-endpoints"
TOKEN_DOCS = DOCS + "#api-tokens"
PROFILE = "https://whgazetteer.org/profile/"
SITE = "https://whgazetteer.org"

_LEAD = "This endpoint is deprecated and will be retired when WHG moves to v4. "
_TOKEN_TAIL = (": generate one on your profile page (" + PROFILE + "); see " + TOKEN_DOCS + ".")

# key: (switch sentence up to "which require(s) an API token", replacement, successor URL, docs URL)
_ENDPOINTS = {
    "spatial": (
        "Switch to the Reconciliation API (POST /reconcile) spatial filters (lat/lng/radius or bounds), "
        "which require an API token",
        "POST https://whgazetteer.org/reconcile (spatial-only query: lat/lng/radius or bounds)",
        SITE + "/reconcile",
        DOCS + "#mapping-legacy-spatial-queries",
    ),
    "db": (
        "Switch to the Reconciliation API (POST /reconcile) to search by name, or the Entity API "
        "(GET /entity/place:<id>/api) to fetch one place by id, which require an API token",
        "POST https://whgazetteer.org/reconcile (name search); "
        "GET https://whgazetteer.org/entity/place:<id>/api (one place by id)",
        SITE + "/reconcile",
        DOCS_SECTION,
    ),
    "index": (
        "Switch to the Reconciliation API (POST /reconcile), which searches the WHG index and the "
        "gazetteer index and requires an API token",
        "POST https://whgazetteer.org/reconcile",
        SITE + "/reconcile",
        DOCS_SECTION,
    ),
    "place": (
        "Switch to the Entity API (GET /entity/place:<id>/api, one place per call), which requires an "
        "API token for contributed places",
        "GET https://whgazetteer.org/entity/place:<id>/api",
        SITE + "/entity/",
        DOCS_SECTION,
    ),
    "place_src": (
        "Its replacement is to be confirmed: no documented API yet addresses a place by dataset label and "
        "the contributor's own src_id. The Entity API (GET /entity/place:<id>/api) serves a place by its "
        "WHG id, and requires an API token",
        "to be confirmed (by WHG id: GET https://whgazetteer.org/entity/place:<id>/api)",
        SITE + "/entity/",
        DOCS_SECTION,
    ),
    "geojson": (
        "For a dataset, switch to the Entity API (GET /entity/dataset:<id>/api); the replacement for a "
        "collection (?coll=) is to be confirmed. The Entity API requires an API token",
        "GET https://whgazetteer.org/entity/dataset:<id>/api (datasets); collections: to be confirmed",
        SITE + "/entity/",
        DOCS_SECTION,
    ),
}


def notice(key):
    """The ``deprecation`` member for one endpoint."""
    switch, replacement, _successor, docs = _ENDPOINTS[key]
    return {
        "deprecated": True,
        "message": _LEAD + switch + _TOKEN_TAIL,
        "replacement": replacement,
        "docs": docs,
        "token_docs": TOKEN_DOCS,
        "token_required": True,
        "sunset": None,
    }


def link_header(key):
    return f'<{_ENDPOINTS[key][2]}>; rel="successor-version", <{_ENDPOINTS[key][3]}>; rel="deprecation"'


def request_origin(request):
    """``in-app`` for a same-origin Referer or a session cookie, else ``external``."""
    referer = request.META.get("HTTP_REFERER") or ""
    if referer and urlparse(referer).netloc == request.get_host():
        return "in-app"
    if settings.SESSION_COOKIE_NAME in request.COOKIES:
        return "in-app"
    return "external"


def _insert_member(content, member):
    """Splice ``"deprecation": {...}`` in as the first key of a serialised JSON
    object. Returns None when the body is not a JSON object."""
    stripped = content.lstrip()
    if not stripped.startswith(b"{"):
        return None
    lead = len(content) - len(stripped)
    rest = stripped[1:]
    encoded = json.dumps(member, ensure_ascii=False).encode()
    sep = b"" if rest.lstrip().startswith(b"}") else b","
    return content[:lead] + b'{"deprecation":' + encoded + sep + rest


def deprecated_endpoint(key):
    """Wrap a view callable (``View.as_view()`` or a function) as a deprecated
    legacy endpoint. See the module docstring."""
    member = notice(key)
    link = link_header(key)

    def decorator(view):
        @wraps(view)
        def wrapped(request, *args, **kwargs):
            logger.info("legacy api call endpoint=%s path=%s origin=%s auth=%s ua=%r",
                        key, request.path, request_origin(request),
                        bool(getattr(getattr(request, "user", None), "is_authenticated", False)),
                        (request.META.get("HTTP_USER_AGENT") or "")[:120])
            response = view(request, *args, **kwargs)
            if hasattr(response, "render") and not getattr(response, "is_rendered", True):
                response.render()
            response["Deprecation"] = DEPRECATION_HEADER
            response["Link"] = link
            if (not getattr(response, "streaming", False)
                    and "json" in (response.get("Content-Type") or "")):
                body = _insert_member(response.content, member)
                if body is not None:
                    response.content = body
            return response
        return wrapped
    return decorator
