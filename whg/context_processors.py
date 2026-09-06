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


def registry_version(request):
    """A cheap authority-registry change-stamp emitted on every page (base template
    ``<meta name="registry-version">``), so the shared IndexedDB caches — Atlas
    coverage maps AND the AAT place-type vocabulary (place#134) — are keyed
    consistently across Atlas, Map-your-Data and the Workbench. Mirrors
    ``search.views._registry_version`` (latest ``updated_at`` + row count, both
    bumped by any inventory push). Cached briefly so a page load costs no DB work
    on the hot path; on any error the meta is emitted empty (client then always
    fetches the vocab fresh — correct, just uncached)."""
    from django.core.cache import cache
    ckey = 'registry_version_stamp'
    try:
        v = cache.get(ckey)
        if v is None:
            from django.db.models import Max, Count
            from api.models import GazetteerRegistryEntry
            agg = (GazetteerRegistryEntry.objects
                   .filter(entry_class='authority')
                   .aggregate(m=Max('updated_at'), n=Count('id')))
            v = f"{agg['m'].isoformat() if agg['m'] else '0'}|{agg['n']}"
            cache.set(ckey, v, 300)  # 5 min; the stamp only moves on inventory pushes
    except Exception:
        v = ''
    return {'registry_version': v}

def overlay_license(request):
    """WHG's own curation/aggregation licence, for surfaces that must name it.

    Exposed so that no template hard-codes it: every place it appears has to
    move together if it changes, and it is asserted ALONGSIDE a source's terms,
    never instead of them (place#157/#158).
    """
    return {'WHG_OVERLAY_LICENSE': getattr(settings, 'WHG_OVERLAY_LICENSE', None)}


def phonetics_visible(request):
    """Whether to offer the phonetic-rule-review link in the Data menu.

    Asks the same predicate the view's gate uses, so the menu can never offer a
    link that 404s — while unlaunched the app is staff/beta only, and its
    existence is not disclosed to anyone else.
    """
    from phonetics.visibility import is_visible
    return {'PHONETICS_VISIBLE': is_visible(getattr(request, 'user', None))}
