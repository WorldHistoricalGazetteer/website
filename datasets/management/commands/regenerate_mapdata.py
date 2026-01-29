# datasets/management/commands/regenerate_mapdata.py

from django.core.management.base import BaseCommand
from django.core.cache import cache
from datasets.models import Dataset
from collection.models import Collection
from utils.mapdata import generate_mapdata


class Command(BaseCommand):
    help = 'Clear cached mapdata and optionally regenerate for all datasets and collections'

    def add_arguments(self, parser):
        parser.add_argument(
            '--clear-only',
            action='store_true',
            help='Only clear the cache without regenerating mapdata',
        )
        parser.add_argument(
            '--datasets',
            action='store_true',
            help='Process only datasets',
        )
        parser.add_argument(
            '--collections',
            action='store_true',
            help='Process only collections',
        )
        parser.add_argument(
            '--id',
            type=int,
            help='Process only a specific dataset or collection by ID',
        )

    def handle(self, *args, **options):
        clear_only = options['clear_only']
        process_datasets = options['datasets']
        process_collections = options['collections']
        specific_id = options['id']

        # If neither datasets nor collections specified, do both
        if not process_datasets and not process_collections:
            process_datasets = True
            process_collections = True

        total_cleared = 0
        total_regenerated = 0

        # Process datasets
        if process_datasets:
            if specific_id:
                datasets = Dataset.objects.filter(id=specific_id)
            else:
                datasets = Dataset.objects.all()

            self.stdout.write(f"Processing {datasets.count()} dataset(s)...")

            for ds in datasets:
                cache_key = f"datasets_{ds.id}"

                # Clear cache
                if cache.delete(cache_key):
                    self.stdout.write(f"  Cleared cache for dataset {ds.id}: {ds.title}")
                    total_cleared += 1

                # Regenerate if not clear-only
                if not clear_only:
                    try:
                        self.stdout.write(f"  Regenerating mapdata for dataset {ds.id}...")
                        generate_mapdata('datasets', ds.id, refresh=True)
                        self.stdout.write(self.style.SUCCESS(f"    ✓ Regenerated dataset {ds.id}"))
                        total_regenerated += 1
                    except Exception as e:
                        self.stdout.write(self.style.ERROR(f"    ✗ Error regenerating dataset {ds.id}: {e}"))

        # Process collections
        if process_collections:
            if specific_id:
                collections = Collection.objects.filter(id=specific_id)
            else:
                collections = Collection.objects.all()

            self.stdout.write(f"Processing {collections.count()} collection(s)...")

            for coll in collections:
                cache_key = f"collections_{coll.id}"

                # Clear cache
                if cache.delete(cache_key):
                    self.stdout.write(f"  Cleared cache for collection {coll.id}: {coll.title}")
                    total_cleared += 1

                # Regenerate if not clear-only
                if not clear_only:
                    try:
                        self.stdout.write(f"  Regenerating mapdata for collection {coll.id}...")
                        generate_mapdata('collections', coll.id, refresh=True)
                        self.stdout.write(self.style.SUCCESS(f"    ✓ Regenerated collection {coll.id}"))
                        total_regenerated += 1
                    except Exception as e:
                        self.stdout.write(self.style.ERROR(f"    ✗ Error regenerating collection {coll.id}: {e}"))

        # Summary
        self.stdout.write(self.style.SUCCESS(f"\nSummary:"))
        self.stdout.write(f"  Cache entries cleared: {total_cleared}")
        if not clear_only:
            self.stdout.write(f"  Mapdata regenerated: {total_regenerated}")
        else:
            self.stdout.write(f"  Mapdata will be regenerated on first access")
