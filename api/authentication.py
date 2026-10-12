# api/authentication.py
from datetime import datetime, timedelta

from django.contrib.auth.models import AnonymousUser
from django.middleware.csrf import CsrfViewMiddleware
from django.utils import timezone
from rest_framework.authentication import BaseAuthentication, SessionAuthentication
from rest_framework import status
from rest_framework.exceptions import APIException, AuthenticationFailed
from rest_framework.permissions import IsAuthenticated
from rest_framework.views import APIView

from .models import APIToken, UserAPIProfile

class CSRFUser(AnonymousUser):
    @property
    def is_authenticated(self):
        return True

def enforce_csrf(request):
    """
    Run Django's CSRF validation against the incoming request.
    Raise AuthenticationFailed if invalid.
    """
    check = CsrfViewMiddleware(lambda req: None)  # dummy get_response
    reason = check.process_view(request, None, (), {})
    if reason:  # if not None, validation failed
        raise AuthenticationFailed(f"CSRF Failed: {reason}")

class DailyLimitExceeded(APIException):
    """429 for a token that has used up today's allowance.

    It used to be an `AuthenticationFailed`, which DRF answers with 401 and a Bearer challenge: the
    same response as a mistyped token, so a client could tell "wait until tomorrow" from "your token
    is wrong" only by parsing the English message, and a browser client could not read when to try
    again. Not a subclass of `AuthenticationFailed` deliberately: DRF rewrites those to 401/403.
    Rendered by `api.exception_handlers` so that `limit` stays an integer.
    """
    status_code = status.HTTP_429_TOO_MANY_REQUESTS
    default_code = 'daily_limit_exceeded'

    def __init__(self, limit):
        self.limit = limit
        # The allowance rolls over when `timezone.now().date()` changes (UserAPIProfile), i.e. at
        # the next midnight in settings.TIME_ZONE (UTC).
        now = timezone.now()
        self.resets_at = datetime.combine(now.date() + timedelta(days=1), datetime.min.time())
        if timezone.is_aware(now):
            self.resets_at = self.resets_at.replace(tzinfo=now.tzinfo)
        self.wait = max(1, int((self.resets_at - now).total_seconds()) + 1)
        super().__init__(f"Daily API limit ({limit} calls) exceeded")

    def payload(self):
        return {'code': self.default_code, 'detail': str(self.detail), 'limit': self.limit,
                'resets_at': self.resets_at.strftime('%Y-%m-%dT%H:%M:%SZ')}


class TokenQueryOrBearerAuthentication(BaseAuthentication):
    """
    Token authentication via URL parameter (?token=...) or Bearer header
    Integrates with existing APIToken and UserAPIProfile models
    """

    def authenticate(self, request):
        """
        Authenticate using the same logic as AuthenticatedViewMixin
        """
        key = None

        # 1. Check Authorization header
        auth = request.headers.get("Authorization", "")
        if auth.lower().startswith("bearer "):
            key = auth.split(" ", 1)[1].strip()

        # 2. Check token from URL query param
        if not key:
            key = request.GET.get("token")

        if key:
            try:
                token = APIToken.objects.select_related("user").get(key=key)
            except APIToken.DoesNotExist:
                raise AuthenticationFailed("Invalid API token")

            user = token.user
            profile, _ = UserAPIProfile.objects.get_or_create(user=user)
            # Ask `remaining_today()`, not `daily_count` directly: the day rollover lives inside
            # `increment_usage()`, which this branch never reaches once the cap is hit. Reading the
            # raw counter therefore turned a DAILY cap into a PERMANENT lockout — the account stayed
            # at limit/limit with `daily_reset` frozen on the day it was exhausted, and no amount of
            # waiting cleared it. `remaining_today()` applies the rollover; `increment_usage()` below
            # then writes it back. None means UNLIMITED, so compare against 0 explicitly.
            if profile.remaining_today() == 0:
                raise DailyLimitExceeded(profile.daily_limit)
            profile.increment_usage()

            now = timezone.now()
            if not token.last_used or (now - token.last_used).total_seconds() > 60:
                token.last_used = now
                token.save(update_fields=["last_used"])

            return (user, key)

        # 3. If no token but CSRF header present, accept request as "public user"
        csrf_token = request.headers.get("X-CSRFToken") or request.headers.get("X-CSRF-Token")
        if csrf_token:
            enforce_csrf(request)
            return (CSRFUser(), None)

        # Otherwise, no credentials
        return None

    def authenticate_header(self, request):
        """
        Indicate that this endpoint uses Bearer auth for 401 challenges.
        """
        return 'Bearer realm="api"'


class AuthenticatedAPIView(APIView):
    """
    Base API view with token/session authentication
    """
    authentication_classes = [
        TokenQueryOrBearerAuthentication,
        SessionAuthentication,
    ]
    permission_classes = [IsAuthenticated]
