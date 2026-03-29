# main/middleware.py

"""
Dev-server access restriction middleware.

When ``ENV_CONTEXT`` is ``'dev-whgazetteer-org'``, only logged-in superusers
may access the site.  All other users (anonymous or non-superuser) are
redirected to the Django **admin** login page (ORCiD is not available on
dev because the redirect URI differs from the registered production one).

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



