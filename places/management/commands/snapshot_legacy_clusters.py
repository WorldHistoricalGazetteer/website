"""
Capture the legacy Elasticsearch `whg` union index into Postgres (place#170).

`whg_id` values are in public circulation — carried by Wikidata property P13061,
and dispensed for years by the portal's Permalink button — but they exist only in
an Elasticsearch index that is being retired. Once it goes, the mapping from a
whg_id to the places it united is unrecoverable. This command freezes it first.

The result backs the `/entity/locus:<PID>` endpoint: a legacy identifier with no
registry entry is resolved from here and minted as a frozen locus on demand.

Read-only against Elasticsearch. Safe to re-run: rows are upserted on whg_id, so
an interrupted run is resumed simply by running it again.

    python manage.py snapshot_legacy_clusters --limit 500 --dry-run
    python manage.py snapshot_legacy_clusters
"""

import logging

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from elasticsearch8.helpers import scan

from places.models import LegacyUnionRecord, Place

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Freeze the legacy ES `whg` union index into Postgres before it is retired."

    def add_arguments(self, parser):
        parser.add_argument(
            '--batch-size', type=int, default=2000,
            help="Union records held in memory per write (default 2000).",
        )
        parser.add_argument(
            '--limit', type=int, default=None,
            help="Stop after this many union records. For smoke-testing.",
        )
        parser.add_argument(
            '--dry-run', action='store_true',
            help="Read and report, but write nothing.",
        )

    def handle(self, *args, **opts):
        es = settings.ES_CONN
        idx = settings.ES_WHG
        batch_size = opts['batch_size']
        limit = opts['limit']
        dry_run = opts['dry_run']

        try:
            total = es.count(index=idx)['count']
        except Exception as e:
            raise CommandError(f"Cannot reach Elasticsearch index '{idx}': {e}")

        self.stdout.write(
            f"Index '{idx}': {total:,} union records"
            + (f" (limited to {limit:,})" if limit else "")
            + (" — DRY RUN, nothing will be written" if dry_run else "")
        )

        cursor = scan(
            es,
            index=idx,
            query={'query': {'match_all': {}}},
            _source=['whg_id', 'place_id', 'children'],
            size=batch_size,
            preserve_order=False,
        )

        seen = written = skipped = multi = 0
        batch = []

        for doc in cursor:
            src = doc.get('_source', {})
            whg_id = src.get('whg_id')
            place_id = src.get('place_id')
            if whg_id is None or place_id is None:
                # A union record without an id or a head place cannot be resolved
                # later; count it so the discrepancy is visible rather than silent.
                skipped += 1
                continue

            ids = [int(place_id)] + [int(c) for c in (src.get('children') or [])]
            # De-duplicate while preserving order: the head place occasionally
            # reappears among its own children.
            ids = list(dict.fromkeys(ids))
            if len(ids) > 1:
                multi += 1

            batch.append((int(whg_id), ids))
            seen += 1

            if len(batch) >= batch_size:
                written += self._flush(batch, dry_run)
                self._progress(seen, total, limit)
                batch = []

            if limit and seen >= limit:
                break

        if batch:
            written += self._flush(batch, dry_run)

        self.stdout.write(self.style.SUCCESS(
            f"\nRead {seen:,} union records "
            f"({multi:,} uniting more than one place, "
            f"{seen - multi:,} singletons)."
        ))
        if skipped:
            self.stdout.write(self.style.WARNING(
                f"Skipped {skipped:,} record(s) lacking whg_id or place_id."
            ))
        self.stdout.write(
            f"{'Would have written' if dry_run else 'Wrote'} {written:,} rows to "
            f"{LegacyUnionRecord._meta.db_table}."
        )

    def _flush(self, batch, dry_run):
        """Resolve titles for one batch and upsert it. Returns rows written."""
        wanted = {pid for _, ids in batch for pid in ids}
        titles = dict(
            Place.objects.filter(id__in=wanted).values_list('id', 'title')
        )

        rows = [
            LegacyUnionRecord(
                whg_id=whg_id,
                place_ids=ids,
                # Titles are positionally aligned with place_ids. A place already
                # deleted from Postgres yields '' rather than shortening the list,
                # which would silently break the alignment invariant.
                titles=[titles.get(pid, '') or '' for pid in ids],
            )
            for whg_id, ids in batch
        ]

        if dry_run:
            return len(rows)

        with transaction.atomic():
            LegacyUnionRecord.objects.bulk_create(
                rows,
                update_conflicts=True,
                update_fields=['place_ids', 'titles'],
                unique_fields=['whg_id'],
                batch_size=len(rows),
            )
        return len(rows)

    def _progress(self, seen, total, limit):
        target = min(total, limit) if limit else total
        pct = (seen / target * 100) if target else 0
        self.stdout.write(f"  {seen:,} / {target:,} ({pct:.1f}%)", ending='\r')
        self.stdout.flush()
