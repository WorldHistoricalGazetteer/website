"""
Bearer-token access, CORS and limits for the Workbench project API (place#314).

A browser client on another origin — PLATO Tools at https://pelagios.org, which is taking on
Map your Data — keeps a project in the Workbench with the user's existing WHG API token. The
session-cookie path used by WHG's own pages is unchanged; this module adds a second, token path to
the SAME views and keeps the two apart:

  * **Bearer** (``Authorization: Bearer <key>``, nowhere else — never ``?token=``): the token is
    looked up in ``api.models.APIToken``; a bad or missing key is a JSON ``401`` with a
    ``WWW-Authenticate`` challenge, never a redirect. The token decides the user; a session cookie
    on the same request is ignored, so a token can never fall back to a cookie. These calls are
    **not** charged to ``UserAPIProfile.daily_limit`` (SG, 2026-10-05): saves are frequent and
    would lock the user out of reconcile. They get their own per-minute budget
    (``WORKBENCH_TOKEN_RATE``) and a body-size cap (``WORKBENCH_BODY_MAX_BYTES``) instead.
  * **Session** (cookie): CSRF is enforced exactly as before — the views are ``csrf_exempt`` only
    so that the token path can skip it, and this module runs Django's own check by hand for every
    cookie-authenticated unsafe request. A cookie request that arrives cross-origin is refused
    outright (``401``): cross-origin callers must use a token.
  * **Anonymous**: JSON ``401``. (``shared/<token>/`` stays anonymous — its capability is the link.)

CORS is answered here, per view, for the exact origins in ``WORKBENCH_CORS_ORIGINS`` and for
nothing else: the preflight (``OPTIONS``) is handled before authentication (browsers send no
``Authorization`` on a preflight), allows ``PUT`` and ``DELETE``, and NEVER sets
``Access-Control-Allow-Credentials`` — so a cookie can never ride a cross-origin request even from
an allow-listed origin. The host's nginx must not add wildcard CORS headers on these paths (see
``server-admin/nginx-template.j2``): two ``Access-Control-Allow-Origin`` values make a browser reject
the response.

Why not django-cors-headers: the pinned 2.4.0 does not import on Python 3.10 and was never in
``MIDDLEWARE``; a site-wide middleware would also widen CORS to every view, and the point here is
to scope it to these endpoints only.
"""
import logging
from functools import wraps

from django.conf import settings
from django.http import Http404, JsonResponse
from django.middleware.csrf import CsrfViewMiddleware
from django.utils import timezone
from django.utils.cache import patch_vary_headers
from django.views.decorators.csrf import csrf_exempt

from api.models import APIToken
from api.throttling import consume_budget

logger = logging.getLogger(__name__)

# Defaults; each has a settings.py line reading the environment (WORKBENCH_*).
DEFAULT_TOKEN_RATE = 120            # token-authenticated requests per minute per user
DEFAULT_BODY_MAX_BYTES = 2 * 1024 * 1024   # 2 MiB request body on POST/PUT

ALLOWED_REQUEST_HEADERS = 'Authorization, Content-Type'
EXPOSED_RESPONSE_HEADERS = 'Retry-After'
PREFLIGHT_MAX_AGE = '86400'


def cors_origins():
    return set(getattr(settings, 'WORKBENCH_CORS_ORIGINS', None) or [])


def token_rate():
    return int(getattr(settings, 'WORKBENCH_TOKEN_RATE', DEFAULT_TOKEN_RATE) or 0)


def body_max_bytes():
    return int(getattr(settings, 'WORKBENCH_BODY_MAX_BYTES', DEFAULT_BODY_MAX_BYTES) or 0)


def _json_error(message, status, code=None, **extra):
    body = {'error': message}
    if code:
        body['code'] = code
    body.update(extra)
    return JsonResponse(body, status=status)


def _own_origin(request):
    return f'{request.scheme}://{request.get_host()}'


def _origin_allowed(request, origin):
    """An origin may call cross-origin if it is allow-listed; the site's own origin is always fine
    (browsers send ``Origin`` on same-origin unsafe requests too)."""
    if not origin:
        return True
    return origin in cors_origins() or origin == _own_origin(request)


def _add_cors(response, origin):
    response['Access-Control-Allow-Origin'] = origin
    response['Access-Control-Expose-Headers'] = EXPOSED_RESPONSE_HEADERS
    patch_vary_headers(response, ('Origin',))
    # Deliberately no Access-Control-Allow-Credentials: cookies never cross origins here.
    return response


def _preflight(request, methods, origin):
    """Answer a CORS preflight. 204 either way; the CORS headers appear only for an allow-listed
    origin, so a disallowed one gets a bare 204 the browser treats as a refusal."""
    response = JsonResponse({}, status=204)
    response['Allow'] = ', '.join(methods)
    if origin:
        _add_cors(response, origin)
        response['Access-Control-Allow-Methods'] = ', '.join(methods)
        response['Access-Control-Allow-Headers'] = ALLOWED_REQUEST_HEADERS
        response['Access-Control-Max-Age'] = PREFLIGHT_MAX_AGE
    return response


def _bearer_key(request):
    auth = request.headers.get('Authorization', '')
    if auth.lower().startswith('bearer '):
        return auth.split(' ', 1)[1].strip()
    return None


def _unauthenticated(message='authentication required: send a WHG API token as '
                             '"Authorization: Bearer <token>", or sign in'):
    response = _json_error(message, 401, code='unauthenticated')
    response['WWW-Authenticate'] = 'Bearer realm="workbench"'
    return response


def _authenticate_token(request, key):
    """Resolve the Bearer key to a user, or return the error response. Updates ``request.user``."""
    token = APIToken.objects.select_related('user').filter(key=key).first()
    if token is None:
        return _unauthenticated('invalid API token')
    user = token.user
    if not user.is_active:
        return _unauthenticated('invalid API token')
    origin = request.headers.get('Origin')
    if not _origin_allowed(request, origin):
        # The browser's preflight would already have refused this origin; a non-browser caller
        # sending a forged Origin gets the same answer, so the allow-list is enforced twice.
        return _json_error('this origin may not use the Workbench API', 403, code='origin_not_allowed')
    allowed, retry_after = consume_budget(f'wb:u{user.pk}', token_rate())
    if not allowed:
        response = _json_error(
            f'rate limit exceeded: at most {token_rate()} Workbench requests per minute per '
            f'token; retry after {retry_after}s', 429, code='rate_limited',
            retry_after=retry_after)
        response['Retry-After'] = str(retry_after)
        return response
    if not user.can_access_beta:
        # The Workbench is a beta feature; a token caller is told so (a 404 would read as a
        # wrong URL to a client).
        return _json_error('the Workbench is in beta and this account is not enrolled', 403,
                           code='beta_required')
    now = timezone.now()
    if not token.last_used or (now - token.last_used).total_seconds() > 60:
        token.last_used = now
        token.save(update_fields=['last_used'])
    request.user = user
    request.workbench_token = token
    return None


def _authenticate_session(request):
    """The cookie path: unchanged semantics (CSRF, beta → 404), run by hand because the view is
    ``csrf_exempt`` for the token path's sake."""
    origin = request.headers.get('Origin')
    if origin and origin != _own_origin(request):
        return _unauthenticated('cross-origin requests must use a Bearer token')
    reason = CsrfViewMiddleware(lambda r: None).process_view(request, None, (), {})
    if reason is not None:
        return _json_error('CSRF verification failed', 403, code='csrf_failed')
    if not request.user.can_access_beta:
        raise Http404()
    request.workbench_token = None
    return None


def _check_body_size(request):
    cap = body_max_bytes()
    if not cap or request.method not in ('POST', 'PUT', 'PATCH'):
        return None
    try:
        declared = int(request.META.get('CONTENT_LENGTH') or 0)
    except (TypeError, ValueError):
        declared = 0
    size = max(declared, len(request.body)) if declared <= cap else declared
    if size > cap:
        return _json_error(f'request body too large: {size} bytes; the limit is {cap} bytes',
                           413, code='body_too_large', limit=cap, size=size)
    return None


def workbench_api(methods, auth=True):
    """Decorate a Workbench JSON view: CORS for the allow-listed origins (preflight included),
    Bearer-or-session authentication (``auth=False`` for the anonymous share endpoint), the
    body-size cap, and JSON for every refusal (404 included)."""
    methods = [m.upper() for m in methods]
    if 'OPTIONS' not in methods:
        methods = methods + ['OPTIONS']

    def decorator(view):
        @csrf_exempt
        @wraps(view)
        def wrapped(request, *args, **kwargs):
            origin = request.headers.get('Origin')
            cors_origin = origin if origin and origin in cors_origins() else None

            if request.method == 'OPTIONS':
                return _preflight(request, methods, cors_origin)

            response = _dispatch(request, view, methods, auth, args, kwargs)
            if cors_origin:
                _add_cors(response, cors_origin)
            return response

        wrapped.workbench_api = True
        return wrapped
    return decorator


def _dispatch(request, view, methods, auth, args, kwargs):
    if request.method not in methods:
        response = _json_error('method not allowed', 405, code='method_not_allowed')
        response['Allow'] = ', '.join(methods)
        return response
    try:
        if auth:
            key = _bearer_key(request)
            if key is not None:
                refused = _authenticate_token(request, key)
            elif getattr(request, 'user', None) is not None and request.user.is_authenticated:
                refused = _authenticate_session(request)
            else:
                refused = _unauthenticated()
            if refused is not None:
                return refused
        too_big = _check_body_size(request)
        if too_big is not None:
            return too_big
        return view(request, *args, **kwargs)
    except Http404:
        return _json_error('not found', 404, code='not_found')
