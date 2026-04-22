# placetypes/apps.py
from django.apps import AppConfig


class TypesConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'placetypes'
    verbose_name = 'AAT Place Types'
