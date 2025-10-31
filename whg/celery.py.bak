# whg/celery.py

from __future__ import absolute_import, unicode_literals
import os
import logging
from celery import Celery
from celery.schedules import crontab
from django.conf import settings

# set the default Django settings module for the 'celery' program.
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "whg.settings")

import django
django.setup()  # Ensure Django is fully set up before importing anything else

app = Celery('whg')

# Load configuration from Django settings with the CELERY_ namespace
app.config_from_object('django.conf:settings', namespace='CELERY')

# Configure broker URL explicitly if needed; otherwise, use CELERY_BROKER_URL from settings
#app.conf.broker_url = 'redis://localhost:6379'

# override Beat default daily cleanup task
app.conf.result_expires = None

app.conf.beat_schedule = {
    'clean-tmp-files-every-hour': {
        'task': 'validation.tasks.clean_tmp_files',
        'schedule': crontab(minute=0, hour='*'),  # Runs every hour
    },
}

# Load task modules from all registered Django app configs.
app.autodiscover_tasks()

# Ensure Celery uses Django's logging configuration
from logging.config import dictConfig
dictConfig(settings.LOGGING)

@app.task(bind=True)
def debug_task(self):
    logger = logging.getLogger(__name__)
    logger.info('Request: {0!r}'.format(self.request))
