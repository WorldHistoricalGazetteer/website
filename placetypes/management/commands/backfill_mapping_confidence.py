# placetypes/management/commands/backfill_mapping_confidence.py
"""
One-time management command to backfill the ``mapping_conf`` field on
all AAT documents in the ES ``types`` index that have cross-vocabulary
mapping arrays (gn_fcodes, wd_qids, osm_tags, ohm_tags).

Every existing mapped source ID is given confidence = "exact" (the
default), so the new field is populated for all pre-existing data.

Also updates the ES index mapping to add ``mapping_conf`` with
``enabled: false`` (stored in _source but not indexed).

Usage:
    python manage.py backfill_mapping_confidence
    python manage.py backfill_mapping_confidence --dry-run
"""

import logging

from django.conf import settings
from django.core.management.base import BaseCommand

logger = logging.getLogger(__name__)

MAPPING_FIELDS = ["gn_fcodes", "wd_qids", "osm_tags", "ohm_tags"]


class Command(BaseCommand):
    help = "Backfill mapping_conf on ES types index documents (set existing mappings to 'exact')."

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would be updated without writing to ES.",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        es = settings.ES_CONN

        # Step 1: Ensure the mapping_conf field exists in the index mapping
        if not dry_run:
            self.stdout.write("Updating index mapping to add mapping_conf field...")
            try:
                es.indices.put_mapping(
                    index="types",
                    body={
                        "properties": {
                            "mapping_conf": {"enabled": False, "type": "object"},
                        }
                    },
                )
                self.stdout.write(self.style.SUCCESS("  Index mapping updated."))
            except Exception as e:
                self.stdout.write(self.style.WARNING(
                    f"  Could not update index mapping (may already exist): {e}"
                ))

        # Step 2: Find all documents with any mapping field populated
        self.stdout.write("Scanning types index for documents with mappings...")
        query = {
            "bool": {
                "should": [{"exists": {"field": f}} for f in MAPPING_FIELDS],
                "minimum_should_match": 1,
            }
        }

        resp = es.search(
            index="types",
            query=query,
            _source=MAPPING_FIELDS + ["aat_id", "mapping_conf"],
            size=10000,
            request_timeout=15,
        )
        hits = resp["hits"]["hits"]
        self.stdout.write(f"  Found {len(hits)} documents with mapping fields.")

        updated = 0
        skipped = 0

        for hit in hits:
            doc_id = hit["_id"]
            src = hit["_source"]
            existing_conf = src.get("mapping_conf") or {}

            # Build the new mapping_conf, merging with any existing data
            new_conf = {}
            needs_update = False

            for field in MAPPING_FIELDS:
                values = src.get(field) or []
                if not values:
                    continue
                field_conf = dict(existing_conf.get(field) or {})
                for val in values:
                    if val not in field_conf:
                        field_conf[val] = "exact"
                        needs_update = True
                if field_conf:
                    new_conf[field] = field_conf

            if not needs_update:
                skipped += 1
                continue

            if dry_run:
                aat_id = src.get("aat_id", "?")
                total_entries = sum(len(v) for v in new_conf.values())
                self.stdout.write(
                    f"  [DRY RUN] Would update {doc_id} (aat:{aat_id}): "
                    f"{total_entries} confidence entries"
                )
                updated += 1
                continue

            # Write to ES
            try:
                es.update(
                    index="types",
                    id=doc_id,
                    doc={"mapping_conf": new_conf},
                    request_timeout=8,
                )
                updated += 1
            except Exception as e:
                self.stdout.write(self.style.ERROR(
                    f"  Error updating {doc_id}: {e}"
                ))

        # Refresh the index once at the end
        if not dry_run and updated > 0:
            es.indices.refresh(index="types")

        prefix = "[DRY RUN] " if dry_run else ""
        self.stdout.write(self.style.SUCCESS(
            f"\n{prefix}Done. Updated: {updated}, Already up-to-date: {skipped}"
        ))

