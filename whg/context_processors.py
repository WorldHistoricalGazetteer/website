import os

from django.conf import settings


def environment(request):
    return {
        'environment': os.getenv('ENV_CONTEXT', 'default'),
        'ENV_CONTEXT': os.getenv('ENV_CONTEXT', 'default'),
        'GLITCHTIP_DSN': settings.GLITCHTIP_DSN,
        'GLITCHTIP_RELEASE': settings.GLITCHTIP_RELEASE,
    }

def app_version(request):
    beta = getattr(settings, 'BETA_VERSION', '')
    selected = request.session.get('whg_version', settings.APP_VERSION)
    # Validate: selected must be either the current APP_VERSION or the BETA_VERSION
    if selected not in (settings.APP_VERSION, beta):
        selected = settings.APP_VERSION
    return {
        'APP_VERSION': getattr(settings, 'APP_VERSION', 'dev'),
        'BETA_VERSION': beta,
        'SELECTED_VERSION': selected,
        'IS_BETA': bool(beta and selected == beta),
    }
