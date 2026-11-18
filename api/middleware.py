# api/middleware.py
import logging
import threading
import requests
from django.conf import settings
from django.utils.deprecation import MiddlewareMixin

APPLICABLE_PATHS = ["/reconcile/", "/suggest"]

logger = logging.getLogger('reconciliation')

# Where an API call comes from, WITHOUT asking callers to do anything new: a browser sends Origin (or
# Referer) by itself, and every HTTP client sends a User-Agent. Only coarse, allow-listed labels are ever
# recorded (never the raw header, a token, an IP or a user), so the event stays low-cardinality and
# carries no personal data. Both are self-reported and spoofable: indicative, not authoritative.
KNOWN_ORIGINS = {
    "plato.pelagios.org": "plato-tools",
    "pelagios.org": "pelagios",
    "whgazetteer.org": "whg-site",
    "dev.whgazetteer.org": "whg-site",
    "docs.whgazetteer.org": "whg-docs",
}


def is_applicable_path(path):
    """True for the reconciliation and suggest endpoints. `/reconcile` itself (no trailing slash, the
    route OpenRefine and PLATO Tools POST to) must match: it used to fall through the prefix test."""
    return path == "/reconcile" or any(path.startswith(p) for p in APPLICABLE_PATHS)


def classify_origin(request):
    """`plato-tools`, `pelagios`, `whg-site`, `whg-docs`, `other-web` (a browser page on another site) or
    `none` (no Origin/Referer: a script, OpenRefine, curl ...)."""
    from urllib.parse import urlsplit
    raw = request.META.get("HTTP_ORIGIN") or request.META.get("HTTP_REFERER") or ""
    host = (urlsplit(raw).hostname or "").lower() if raw and raw != "null" else ""
    if not host:
        return "none"
    return KNOWN_ORIGINS.get(host, "other-web")


def classify_client(request):
    """Coarse software class from the User-Agent: whg-mcp, openrefine, python, curl, postman, browser,
    other, or none."""
    ua = (request.META.get("HTTP_USER_AGENT") or "").lower()
    if not ua:
        return "none"
    for needle, label in (("whg-mcp", "whg-mcp"), ("openrefine", "openrefine"), ("python-requests", "python"),
                          ("python-httpx", "python"), ("aiohttp", "python"), ("curl/", "curl"),
                          ("postman", "postman"), ("mozilla/", "browser")):
        if needle in ua:
            return label
    return "other"


def send_to_plausible(request, response):
    api_key = getattr(settings, "PLAUSIBLE_API_KEY", None)
    base_url = getattr(settings, "PLAUSIBLE_BASE_URL", "https://analytics.whgazetteer.org")
    site = getattr(settings, "PLAUSIBLE_SITE", "whgazetteer.org")

    if not api_key:
        logger.debug("No PLAUSIBLE_API_KEY set when required by middleware")
        return

    url = f"{base_url}/api/event"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "name": "API_Call",
        "url": f"https://{request.get_host()}{request.path}",
        "domain": site,
        "props": {
            "method": request.method,
            "status_code": response.status_code,
            "authenticated": getattr(request.user, "is_authenticated", False),
            "origin": classify_origin(request),
            "client": classify_client(request),
        },
    }

    try:
        requests.post(url, json=payload, headers=headers, timeout=1)
    except requests.RequestException:
        logger.exception("Failed to send event to Plausible")
        pass


class PlausibleAPIMiddleware(MiddlewareMixin):
    def process_response(self, request, response):
        if is_applicable_path(request.path):
            threading.Thread(
                target=send_to_plausible, args=(request, response), daemon=True
            ).start()

        return response
