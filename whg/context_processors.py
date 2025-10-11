import os

from django.conf import settings


def environment(request):
    return {
        'environment': os.getenv('ENV_CONTEXT', 'default'),
    }

def app_version(request):
    return {'APP_VERSION': getattr(settings, 'APP_VERSION', 'dev')}