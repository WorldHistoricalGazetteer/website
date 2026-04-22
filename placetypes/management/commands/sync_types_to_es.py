"""
Django management command to check and sync AAT types between DB and ES index.

Usage:
    python manage.py sync_types_to_es                 # Check for missing types
    python manage.py sync_types_to_es --fix --dry-run # Show what would be synced
    python manage.py sync_types_to_es --fix           # Actually sync missing types

This command:
1. Queries all is_place_type=True from the Django DB (via Type model)
2. Queries all documents from the ES types index
3. Identifies types in DB but not in ES
4. Optionally bulk-adds missing types to ES without destroying existing data
"""

import logging
from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from placetypes.models import Type

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Check and sync AAT types from Django DB to ES index."

    def add_arguments(self, parser):
        parser.add_argument(
            '--fix',
            action='store_true',
            help='Actually add missing types to ES (requires explicit flag for safety).',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be added without writing to ES.',
        )

    def get_es_types(self):
        """Query ES types index for all AAT IDs currently indexed."""
        self.stdout.write("Querying ES types index...")

        es = settings.ES_CONN
        es_aats = set()

        try:
            # Use point_in_time for large result sets
            pit_resp = es.open_point_in_time(index="types", keep_alive="1m")
            pit_id = pit_resp["id"]
            search_after = None
            total_fetched = 0

            try:
                while True:
                    search_body = {
                        "pit": {"id": pit_id, "keep_alive": "1m"},
                        "query": {"match_all": {}},
                        # _id sort can require fielddata; _shard_doc is the recommended
                        # tiebreaker for PIT + search_after deep pagination.
                        "sort": [{"_shard_doc": "asc"}],
                        "_source": ["aat_id"],
                        "size": 10000,
                    }
                    if search_after:
                        search_body["search_after"] = search_after

                    batch_resp = es.search(
                        body=search_body,
                        request_timeout=30,
                    )

                    batch_hits = batch_resp["hits"]["hits"]
                    if not batch_hits:
                        break

                    for hit in batch_hits:
                        aat_id = hit["_source"].get("aat_id")
                        if aat_id:
                            es_aats.add(aat_id)

                    total_fetched += len(batch_hits)
                    pit_id = batch_resp.get("pit_id", pit_id)
                    # With _shard_doc sort, ES always returns a stable sort token.
                    search_after = batch_hits[-1].get("sort", [])

                    self.stdout.write(f"  ... fetched {total_fetched} documents")

                    if len(batch_hits) < 10000:
                        break

            finally:
                es.close_point_in_time(id=pit_id)

        except Exception as e:
            raise CommandError(f"Error querying ES types index: {e}")

        self.stdout.write(f"  ✓ ES index: {len(es_aats)} types")
        return es_aats

    def get_db_types(self):
        """Query Django DB for all place types."""
        self.stdout.write("Querying Django DB for place types...")

        try:
            db_aats = set(
                Type.objects.filter(is_place_type=True).values_list(
                    'aat_id', flat=True
                )
            )
        except Exception as e:
            raise CommandError(f"Error querying Django DB: {e}")

        self.stdout.write(f"  ✓ Django DB: {len(db_aats)} place types")
        return db_aats

    def build_documents(self, missing_aat_ids):
        """Build ES documents for missing AAT types from Django DB."""
        self.stdout.write(f"\nBuilding ES documents for {len(missing_aat_ids)} types...")

        try:
            missing_types = Type.objects.filter(
                aat_id__in=missing_aat_ids
            ).values(
                'aat_id', 'term', 'term_full', 'note', 'fclasses',
                'path', 'depth', 'is_place_type'
            )

            documents = []
            for t in missing_types:
                doc = {
                    "aat_id": t["aat_id"],
                    "term": t["term"],
                    "term_full": t["term_full"],
                    "note": t["note"],
                    "fclasses": t["fclasses"] or [],
                    "path": t["path"],
                    "depth": t["depth"],
                    "is_place_type": t["is_place_type"],
                    # Mapping fields initialized empty (mappings are added via UI)
                    "gn_fcodes": None,
                    "wd_qids": None,
                    "osm_tags": None,
                    "ohm_tags": None,
                    "mapping_conf": {},
                }
                documents.append(doc)

            return documents

        except Exception as e:
            raise CommandError(f"Error building documents: {e}")

    def bulk_add_to_es(self, documents, dry_run=False):
        """Bulk add documents to ES types index without deleting existing data."""
        if not documents:
            self.stdout.write("No documents to add.")
            return 0
        
        self.stdout.write(f"\nBulk indexing {len(documents)} documents...")
        
        if dry_run:
            self.stdout.write(self.style.WARNING("(DRY RUN MODE — not writing to ES)"))
            for i, doc in enumerate(documents[:20], 1):
                self.stdout.write(f"  [{i}] aat:{doc['aat_id']}: {doc['term']}")
            if len(documents) > 20:
                self.stdout.write(f"  ... and {len(documents) - 20} more")
            return 0
        
        es = settings.ES_CONN
        added = 0
        errors = []
        
        try:
            from elasticsearch8.helpers import bulk as es_bulk
            
            actions = []
            for doc in documents:
                action = {
                    "_index": "types",
                    "_id": f"aat:{doc['aat_id']}",
                    "_source": doc,
                }
                actions.append(action)
            
            # helpers.bulk returns (success_count, errors)
            added, errors = es_bulk(
                es,
                actions,
                request_timeout=30,
                chunk_size=500,
                raise_on_error=False,
                stats_only=False,
            )

            self.stdout.write(f"  ✓ Successfully added: {added}")
            
            if errors:
                self.stdout.write(self.style.WARNING(f"  Errors: {len(errors)}"))
                for err in errors[:5]:
                    self.stdout.write(f"    {err}")
                if len(errors) > 5:
                    self.stdout.write(f"    ... and {len(errors) - 5} more")
        
        except Exception as e:
            raise CommandError(f"Error during bulk indexing: {e}")
        
        return added

    def handle(self, *args, **options):
        fix = options['fix']
        dry_run = options['dry_run']

        self.stdout.write(self.style.SUCCESS("\n" + "=" * 70))
        self.stdout.write(self.style.SUCCESS("AAT Types Index Sync"))
        self.stdout.write(self.style.SUCCESS("=" * 70 + "\n"))

        # Query ES
        es_aats = self.get_es_types()

        # Query Django DB
        db_aats = self.get_db_types()

        # Compare
        self.stdout.write("\nComparing...")
        missing = db_aats - es_aats

        if not missing:
            self.stdout.write(self.style.SUCCESS(
                "✓ All place types in Django DB are present in ES index."
            ))
            return

        # Report missing types
        self.stdout.write(self.style.WARNING(
            f"\n⚠ MISSING TYPES: {len(missing)} types in Django DB but NOT in ES index"
        ))
        self.stdout.write(f"  ES index: {len(es_aats)} types")
        self.stdout.write(f"  Django DB: {len(db_aats)} place types")
        self.stdout.write(f"  Missing: {len(missing)} types")

        # Show first 20 missing types
        missing_sorted = sorted(missing)[:20]
        self.stdout.write("\n  Missing AAT IDs (first 20):")
        for aat_id in missing_sorted:
            try:
                t = Type.objects.get(aat_id=aat_id)
                self.stdout.write(f"    aat:{aat_id}: {t.term}")
            except Type.DoesNotExist:
                self.stdout.write(f"    aat:{aat_id}: (not found in DB)")

        if len(missing) > 20:
            self.stdout.write(f"    ... and {len(missing) - 20} more")

        if not fix:
            self.stdout.write(self.style.WARNING(
                "\nTo add these missing types to the ES index, run:"
            ))
            self.stdout.write("  python manage.py sync_types_to_es --fix --dry-run")
            self.stdout.write("  python manage.py sync_types_to_es --fix")
            return

        # Build documents
        documents = self.build_documents(missing)
        self.stdout.write(f"  ✓ Built {len(documents)} documents")

        # Add to ES
        added = self.bulk_add_to_es(documents, dry_run=dry_run)

        if dry_run:
            self.stdout.write(self.style.WARNING(
                f"\n(DRY RUN) Would have added {len(documents)} documents."
            ))
            self.stdout.write("Run again without --dry-run to actually index the documents.")
        else:
            self.stdout.write(self.style.SUCCESS(
                f"\n✓ Successfully synced {added} missing types to ES index."
            ))

