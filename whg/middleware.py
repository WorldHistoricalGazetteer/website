"""
Project-wide middleware.

``SentryBetaContextMiddleware`` (plan-beta-diagnostics): for requests from beta testers, tag the
GlitchTip/Sentry scope with the tester's identity, role, and the client-supplied per-session
correlation id (``X-WHG-Beta-Session``). This makes a server-side error during a beta action join up
with the tester's client-side events and the snag they file — the whole point of the beta diagnostics.

Strictly best-effort and gated on ``can_access_beta``: it never raises, and does nothing for
non-beta/anonymous requests, so ordinary users are unaffected.
"""
import logging

logger = logging.getLogger(__name__)


class SentryBetaContextMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        try:
            user = getattr(request, 'user', None)
            if user is not None and user.is_authenticated and getattr(user, 'can_access_beta', False):
                import sentry_sdk
                sentry_sdk.set_user({'id': user.id, 'username': getattr(user, 'username', '')})
                sentry_sdk.set_tag('beta', 'true')
                sentry_sdk.set_tag('user_role', 'staff' if user.is_staff else 'beta')
                session = request.headers.get('X-WHG-Beta-Session')
                if session:
                    sentry_sdk.set_tag('beta_session', session[:64])
        except Exception:  # noqa: BLE001 — diagnostics must never affect the response
            logger.debug('SentryBetaContextMiddleware: skipped', exc_info=True)
        return self.get_response(request)
