# api/authentication.py
from django.contrib.auth.models import AnonymousUser
from django.middleware.csrf import CsrfViewMiddleware
from django.utils import timezone
from rest_framework.authentication import BaseAuthentication, SessionAuthentication
from rest_framework.exceptions import AuthenticationFailed
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
            if profile.daily_limit and profile.daily_count >= profile.daily_limit:
                raise AuthenticationFailed(
                    f"Daily API limit ({profile.daily_limit} calls) exceeded"
                )
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
