from django.apps import AppConfig

class PlacesConfig(AppConfig):
    name = 'places'
    def ready(self):
        # Import the signals module to ensure signal handlers are registered.
        import places.signals
        # The locus registry (place#172) lives in its own module; importing it
        # here registers the model with this app without touching models.py.
        import places.models_locus  # noqa: F401

