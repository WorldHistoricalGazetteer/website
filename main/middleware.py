# main/middleware.py

"""
Dev-server access restriction middleware.

When ``ENV_CONTEXT`` is ``'dev-whgazetteer-org'``, only logged-in superusers
may access the site.  All other users (anonymous or non-superuser) are
redirected to the Django **admin** login page (ORCiD is not available on
dev because the redirect URI differs from the registered production one).

Requests carrying an ``Authorization: Bearer …`` header bypass the gate so
DRF's own token-auth class (``api.authentication.TokenQueryOrBearerAuthentication``)
can validate the credential at the view layer. This keeps machine-to-machine
API endpoints (e.g. the ingestion-pipeline inventory push) working on dev
without exposing the rest of the site to anonymous traffic.

On any other environment (production, local, etc.) this middleware is a
transparent no-op.
"""

import os

from django.http import HttpResponseForbidden
from django.shortcuts import redirect


class DevServerAccessMiddleware:
    """Restrict the dev server to authenticated superusers."""

    # URL prefixes that are always accessible (login flow, static assets, etc.)
    ALLOWED_PREFIXES = (
        '/admin/',
        '/static/',
        '/media/',
        '/CDNfallbacks/',
        '/health',
        '/leads/suggest/',  # public dataset-suggestion form is anonymous by design
    )

    def __init__(self, get_response):
        self.get_response = get_response
        self.is_dev = os.environ.get('ENV_CONTEXT', '') == 'dev-whgazetteer-org'

    def __call__(self, request):
        if not self.is_dev:
            return self.get_response(request)

        # Always allow the whitelisted URL prefixes
        if any(request.path.startswith(p) for p in self.ALLOWED_PREFIXES):
            return self.get_response(request)

        # Bearer-token API requests bypass the session gate and let DRF's
        # own auth class validate the credential. This is checked BEFORE
        # ``request.user`` because session middleware reports anonymous for
        # bearer-only requests, which would otherwise redirect them to the
        # admin login.
        auth_header = request.META.get('HTTP_AUTHORIZATION', '')
        if auth_header.lower().startswith('bearer '):
            return self.get_response(request)

        # Require authentication
        if not request.user.is_authenticated:
            return redirect(f'/admin/login/?next={request.path}')

        # Require superuser
        if not request.user.is_superuser:
            return HttpResponseForbidden(
                '<h2>Access Restricted</h2>'
                '<p>The development server is accessible to superusers only.</p>'
                '<p>Please use the <a href="https://whgazetteer.org">production site</a> instead.</p>'
            )

        return self.get_response(request)



