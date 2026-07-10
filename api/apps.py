from django.apps import AppConfig


class ApiConfig(AppConfig):
    name = 'api'

    def ready(self):
        import api.schema_extensions
        # Register ContributorAttestation → CRC gateway forwarding and the
        # dataset/collection cache-invalidation handlers (cf. datasets/apps.py).
        import api.signals  # noqa: F401
