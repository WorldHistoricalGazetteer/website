from whg.settings import *  # noqa
DATABASES = {'default': {'ENGINE': 'django.contrib.gis.db.backends.postgis',
                         'HOST': '127.0.0.1', 'NAME': 'test', 'USER': 'test',
                         'PASSWORD': 'test', 'PORT': 55432}}
EMAIL_BACKEND = 'django.core.mail.backends.locmem.EmailBackend'
