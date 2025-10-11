# api/middleware.py
import logging
import threading
import requests
from django.conf import settings
from django.utils.deprecation import MiddlewareMixin

APPLICABLE_PATHS = ["/reconcile/", "/reconcile?", "/suggest"]

logger = logging.getLogger('reconciliation')


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
        },
    }

    try:
        requests.post(url, json=payload, headers=headers, timeout=1)
    except requests.RequestException:
        logger.exception("Failed to send event to Plausible")
        pass


class PlausibleAPIMiddleware(MiddlewareMixin):
    def process_response(self, request, response):
        if any(request.path.startswith(p) for p in APPLICABLE_PATHS):
            threading.Thread(
                target=send_to_plausible, args=(request, response), daemon=True
            ).start()

        return response
