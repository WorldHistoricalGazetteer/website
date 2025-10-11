# regions/management/commands/clear_regions.py
from django.core.management.base import BaseCommand
from regions.models import Region, RegionLabel

class Command(BaseCommand):
    help = "Delete all entries from Region and RegionLabel, without dropping tables."

    def handle(self, *args, **options):
        self.stdout.write("Deleting all RegionLabel entries...")
        labels_deleted, _ = RegionLabel.objects.all().delete()
        self.stdout.write(f"Deleted {labels_deleted} labels.")

        self.stdout.write("Deleting all Region entries...")
        regions_deleted, _ = Region.objects.all().delete()
        self.stdout.write(f"Deleted {regions_deleted} regions.")

        self.stdout.write(self.style.SUCCESS("✓ All region data cleared."))
