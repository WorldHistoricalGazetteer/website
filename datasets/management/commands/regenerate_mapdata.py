# datasets/management/commands/regenerate_mapdata.py

import gc
# datasets/management/commands/regenerate_mapdata.py

import gc
import logging
import traceback
from django.core.management.base import BaseCommand
from django.core.cache import cache
from django.db import connection
from datasets.models import Dataset
from collection.models import Collection
from utils.mapdata import generate_mapdata

logger = logging.getLogger('mapdata')


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
        parser.add_argument(
            '--skip-to',
            type=int,
            help='Skip to a specific ID (useful for resuming after interruption)',
        )
        parser.add_argument(
            '--stop-on-error',
            action='store_true',
            help='Stop processing if an individual item fails (default is to continue)',
        )
        parser.add_argument(
            '--include-large',
            action='store_true',
            help='Include very large datasets (>500k places) which may cause out-of-memory errors',
        )

    def handle(self, *args, **options):
        clear_only = options['clear_only']
        process_datasets = options['datasets']
        process_collections = options['collections']
        specific_id = options['id']
        skip_to = options.get('skip_to')
        stop_on_error = options.get('stop_on_error', False)
        include_large = options.get('include_large', False)

        # If neither datasets nor collections specified, do both
        if not process_datasets and not process_collections:
            process_datasets = True
            process_collections = True

        total_cleared = 0
        total_regenerated = 0
        total_failed = 0
        total_skipped_large = 0
        failed_items = []
        skipped_large = []

        LARGE_DATASET_THRESHOLD = 500000  # 500k places

        # Process datasets
        if process_datasets:
            if specific_id:
                datasets = Dataset.objects.filter(id=specific_id)
            else:
                datasets = Dataset.objects.all().order_by('id')

            total_datasets = datasets.count()
            self.stdout.write(f"\nProcessing {total_datasets} dataset(s)...")

            for idx, ds in enumerate(datasets, 1):
                # Skip if we're resuming from a specific ID
                if skip_to and ds.id < skip_to:
                    continue

                cache_key = f"datasets_{ds.id}"

                try:
                    self.stdout.write(f"\n[{idx}/{total_datasets}] Dataset {ds.id}: {ds.title[:50]}...")

                    # Check if this is a very large dataset (unless specific ID or --include-large)
                    num_places = ds.places.count()
                    is_large = num_places > LARGE_DATASET_THRESHOLD

                    if is_large and not specific_id and not include_large:
                        self.stdout.write(self.style.WARNING(
                            f"  ⚠ Skipping: Very large dataset ({num_places:,} places > {LARGE_DATASET_THRESHOLD:,})"
                        ))
                        self.stdout.write(f"    Use --include-large to process, or use --clear-only for this dataset")
                        total_skipped_large += 1
                        skipped_large.append(('dataset', ds.id, ds.title, num_places))
                        continue

                    # Clear cache
                    if cache.delete(cache_key):
                        self.stdout.write(f"  ✓ Cleared cache")
                        total_cleared += 1
                    else:
                        self.stdout.write(f"  - No cache to clear")

                    # Regenerate if not clear-only
                    if not clear_only:
                        self.stdout.write(f"  Regenerating mapdata...")
                        self.stdout.flush()  # Force output before potentially long operation

                        try:
                            result = generate_mapdata('datasets', ds.id, refresh=True)
                            feature_count = result.get('metadata', {}).get('num_places', 'unknown')
                            self.stdout.write(self.style.SUCCESS(
                                f"  ✓ Regenerated ({feature_count} features)"
                            ))
                            total_regenerated += 1

                            # Cleanup memory after each dataset
                            gc.collect()
                            connection.close()

                        except Exception as e:
                            error_msg = f"{type(e).__name__}: {str(e)}"
                            self.stdout.write(self.style.ERROR(f"  ✗ Error: {error_msg}"))
                            logger.error(f"Error regenerating dataset {ds.id}: {error_msg}\n{traceback.format_exc()}")
                            total_failed += 1
                            failed_items.append(('dataset', ds.id, ds.title, error_msg))

                            if stop_on_error:
                                self.stdout.write(self.style.ERROR(
                                    f"\nStopping due to error (--stop-on-error flag is set)."
                                ))
                                self.stdout.write(f"To resume, use: --skip-to {ds.id + 1}")
                                raise

                except KeyboardInterrupt:
                    self.stdout.write(self.style.WARNING(
                        f"\n\nInterrupted! To resume, use: --skip-to {ds.id}"
                    ))
                    raise
                except Exception as e:
                    error_msg = f"{type(e).__name__}: {str(e)}"
                    self.stdout.write(self.style.ERROR(f"  ✗ Unexpected error: {error_msg}"))
                    logger.error(f"Unexpected error processing dataset {ds.id}: {error_msg}\n{traceback.format_exc()}")
                    total_failed += 1
                    failed_items.append(('dataset', ds.id, ds.title, error_msg))

                    if stop_on_error:
                        self.stdout.write(self.style.ERROR(
                            f"\nStopping due to error (--stop-on-error flag is set)."
                        ))
                        self.stdout.write(f"To resume, use: --skip-to {ds.id + 1}")
                        raise

        # Process collections
        if process_collections:
            if specific_id:
                collections = Collection.objects.filter(id=specific_id)
            else:
                collections = Collection.objects.all().order_by('id')

            total_collections = collections.count()
            self.stdout.write(f"\nProcessing {total_collections} collection(s)...")

            for idx, coll in enumerate(collections, 1):
                # Skip if we're resuming from a specific ID
                if skip_to and coll.id < skip_to:
                    continue

                cache_key = f"collections_{coll.id}"

                try:
                    self.stdout.write(f"\n[{idx}/{total_collections}] Collection {coll.id}: {coll.title[:50]}...")

                    # Clear cache
                    if cache.delete(cache_key):
                        self.stdout.write(f"  ✓ Cleared cache")
                        total_cleared += 1
                    else:
                        self.stdout.write(f"  - No cache to clear")

                    # Regenerate if not clear-only
                    if not clear_only:
                        self.stdout.write(f"  Regenerating mapdata...")
                        self.stdout.flush()

                        try:
                            result = generate_mapdata('collections', coll.id, refresh=True)
                            feature_count = result.get('metadata', {}).get('num_places', 'unknown')
                            self.stdout.write(self.style.SUCCESS(
                                f"  ✓ Regenerated ({feature_count} features)"
                            ))
                            total_regenerated += 1

                            # Cleanup memory after each collection
                            gc.collect()
                            connection.close()

                        except Exception as e:
                            error_msg = f"{type(e).__name__}: {str(e)}"
                            self.stdout.write(self.style.ERROR(f"  ✗ Error: {error_msg}"))
                            logger.error(f"Error regenerating collection {coll.id}: {error_msg}\n{traceback.format_exc()}")
                            total_failed += 1
                            failed_items.append(('collection', coll.id, coll.title, error_msg))

                            if stop_on_error:
                                self.stdout.write(self.style.ERROR(
                                    f"\nStopping due to error (--stop-on-error flag is set)."
                                ))
                                self.stdout.write(f"To resume, use: --skip-to {coll.id + 1}")
                                raise

                except KeyboardInterrupt:
                    self.stdout.write(self.style.WARNING(
                        f"\n\nInterrupted! To resume, use: --skip-to {coll.id}"
                    ))
                    raise
                except Exception as e:
                    error_msg = f"{type(e).__name__}: {str(e)}"
                    self.stdout.write(self.style.ERROR(f"  ✗ Unexpected error: {error_msg}"))
                    logger.error(f"Unexpected error processing collection {coll.id}: {error_msg}\n{traceback.format_exc()}")
                    total_failed += 1
                    failed_items.append(('collection', coll.id, coll.title, error_msg))

                    if stop_on_error:
                        self.stdout.write(self.style.ERROR(
                            f"\nStopping due to error (--stop-on-error flag is set)."
                        ))
                        self.stdout.write(f"To resume, use: --skip-to {coll.id + 1}")
                        raise

        # Summary
        self.stdout.write("\n" + "="*60)
        self.stdout.write(self.style.SUCCESS("SUMMARY:"))
        self.stdout.write(f"  Cache entries cleared: {total_cleared}")
        if not clear_only:
            self.stdout.write(f"  Mapdata regenerated: {total_regenerated}")
            if total_skipped_large > 0:
                self.stdout.write(self.style.WARNING(f"  Skipped (large): {total_skipped_large}"))
            if total_failed > 0:
                self.stdout.write(self.style.ERROR(f"  Failed: {total_failed}"))
        else:
            self.stdout.write(f"  Mapdata will be regenerated on first access")

        # List skipped large datasets
        if skipped_large:
            self.stdout.write("\n" + self.style.WARNING("SKIPPED LARGE DATASETS (>500k places):"))
            for item_type, item_id, title, num_places in skipped_large:
                self.stdout.write(f"  {item_type} {item_id}: {title[:40]} ({num_places:,} places)")
            self.stdout.write("  Use --include-large to process these, or --clear-only to just clear cache")

        # List failed items
        if failed_items:
            self.stdout.write("\n" + self.style.ERROR("FAILED ITEMS:"))
            for item_type, item_id, title, error in failed_items:
                self.stdout.write(f"  {item_type} {item_id}: {title[:40]} - {error[:60]}")

        self.stdout.write("="*60 + "\n")

