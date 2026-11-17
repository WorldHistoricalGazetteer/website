from django.core.management.base import BaseCommand

from places.models import (Place, PlaceGeom, PlaceWhen, PlaceLink, PlaceRelated, PlaceDescription,
                           PlaceDepiction, PlaceName, PlaceType)
from validation.create_dataset import resync_pk_sequences

MODELS = [Place, PlaceGeom, PlaceWhen, PlaceLink, PlaceRelated, PlaceDescription, PlaceDepiction,
          PlaceName, PlaceType]


class Command(BaseCommand):
    help = ("Raise each place_* id sequence to max(id) of its table (never lowers one). Safe to re-run; "
            "use after any bulk load that supplied explicit pks.")

    def handle(self, *args, **opts):
        moved = resync_pk_sequences(MODELS)
        self.stdout.write(f"moved: {moved}" if moved else "all sequences already level")
