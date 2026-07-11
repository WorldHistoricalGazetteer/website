import os

from django.conf import settings


def environment(request):
    env = os.getenv('ENV_CONTEXT', 'default')
    return {
        'environment': env,
        'ENV_CONTEXT': env,
        # True only on the dev server. Used to steer the "Login/Register"
        # nav + login page to the Django admin login (dev auth is admin
        # creds, not ORCiD — the ORCiD redirect URI only matches prod), while
        # leaving the ORCiD *sandbox* flow reachable for deliberate testing.
        'WHG_DEV_SERVER': env == 'dev-whgazetteer-org',
        'GLITCHTIP_DSN': settings.GLITCHTIP_DSN,
        'GLITCHTIP_RELEASE': settings.GLITCHTIP_RELEASE,
    }

def app_version(request):
    return {'APP_VERSION': getattr(settings, 'APP_VERSION', 'dev')}


def asset_version(request):
    """A per-deploy cache-busting token appended (?v=) to webpack bundle URLs.

    GLITCHTIP_RELEASE is regenerated as the current git short-hash by server-admin/load_env.py on
    every deploy, so it changes exactly when the code (and thus the bundles) change. Falls back to
    APP_VERSION when unset (e.g. a bare local checkout)."""
    v = getattr(settings, 'GLITCHTIP_RELEASE', None) or getattr(settings, 'APP_VERSION', 'dev')
    return {'asset_version': v}