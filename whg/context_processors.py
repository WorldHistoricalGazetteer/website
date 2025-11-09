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
    return {'APP_VERSION': getattr(settings, 'APP_VERSION', 'dev')}