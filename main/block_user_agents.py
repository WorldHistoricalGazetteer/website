from django.conf import settings
from django.http import HttpResponseForbidden

BLOCKED_USER_AGENT_SUBSTRINGS = getattr(settings, 'BLOCKED_USER_AGENT_SUBSTRINGS', [])
HEALTHCHECK_PATHS = {'/health', '/health/'}


def _has_valid_api_token(request):
    """
    True if the request presents a recognised API token, via either the
    ``Authorization: Bearer <key>`` header or a ``?token=<key>`` query param
    (the same two credential channels DRF's TokenQueryOrBearerAuthentication
    accepts).

    Legitimate API clients frequently send a default library User-Agent
    (python-requests, Go-http-client, node-fetch, curl, …) that the bot filter
    would otherwise reject, forcing them to spoof a browser UA. A genuine token
    proves the caller is an authenticated API consumer, not a scraper, so it
    exempts them from the block. Quota/limits are still enforced downstream by
    DRF authentication.
    """
    key = None
    auth = request.headers.get('Authorization', '')
    if auth.lower().startswith('bearer '):
        key = auth.split(' ', 1)[1].strip()
    if not key:
        key = request.GET.get('token')
    if not key:
        return False

    # Imported lazily to avoid pulling in app models at middleware-import time.
    try:
        from api.models import APIToken
        return APIToken.objects.filter(key=key).exists()
    except Exception:
        # Never let an auth-lookup error convert a normal request into a 500;
        # fall back to treating it as un-exempt (i.e. the UA rule applies).
        return False


class BlockUserAgentsMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        # Always allow health probes regardless of User-Agent.
        if request.path in HEALTHCHECK_PATHS:
            return self.get_response(request)

        user_agent = request.META.get('HTTP_USER_AGENT', '')
        if any(blocked in user_agent for blocked in BLOCKED_USER_AGENT_SUBSTRINGS):
            # A valid API token means this is an authenticated API client using
            # a library-default User-Agent, not a bot — let it through. The DB
            # lookup only runs on the (rare) blocked-UA path.
            if not _has_valid_api_token(request):
                return HttpResponseForbidden("Forbidden: Bot access denied.")
        return self.get_response(request)
