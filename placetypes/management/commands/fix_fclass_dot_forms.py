# placetypes/management/commands/fix_fclass_dot_forms.py
"""
One-time management command to rename bare-letter GeoNames fclass entries
(e.g. "S", "H", "A") to dot form (e.g. "S.", "H.", "A.") in the ES
``types`` index.

The ``places`` index stores fclass-only entries as "S.", "H." etc.
(with a trailing dot to distinguish fclasses from feature codes).
This command updates the ``gn_fcodes`` array and ``mapping_conf.gn_fcodes``
object on each affected AAT document to match.

Usage:
    python manage.py fix_fclass_dot_forms
    python manage.py fix_fclass_dot_forms --dry-run
"""

import logging

from django.conf import settings
from django.core.management.base import BaseCommand

logger = logging.getLogger(__name__)

GN_FCLASSES = {"A", "H", "L", "P", "R", "S", "T", "U", "V"}


class Command(BaseCommand):
    help = (
        "Rename bare-letter GeoNames fclass entries (S, H, A …) to dot form "
        "(S., H., A. …) in the ES types index gn_fcodes arrays and mapping_conf."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would be updated without writing to ES.",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        es = settings.ES_CONN

        # Find all documents that contain any bare fclass letter in gn_fcodes
        should_clauses = [{"term": {"gn_fcodes": fc}} for fc in sorted(GN_FCLASSES)]
        resp = es.search(
            index="types",
            query={
                "bool": {
                    "should": should_clauses,
                    "minimum_should_match": 1,
                }
            },
            _source=["aat_id", "term", "gn_fcodes", "mapping_conf"],
            size=100,
            request_timeout=15,
        )

        hits = resp["hits"]["hits"]
        self.stdout.write(f"Found {len(hits)} document(s) with bare-letter fclass entries.")

        if not hits:
            self.stdout.write(self.style.SUCCESS("Nothing to do."))
            return

        updated = 0
        for hit in hits:
            doc_id = hit["_id"]
            src = hit["_source"]
            aat_id = src.get("aat_id")
            term = src.get("term", "")
            gn_fcodes = src.get("gn_fcodes") or []
            conf_data = src.get("mapping_conf") or {}
            gn_conf = conf_data.get("gn_fcodes") or {}

            # Find bare letters that need renaming
            bare_letters = [fc for fc in gn_fcodes if fc in GN_FCLASSES]
            if not bare_letters:
                continue

            self.stdout.write(
                f"  {doc_id} (aat:{aat_id} \"{term}\"): "
                f"renaming {bare_letters} → {[f'{fc}.' for fc in bare_letters]}"
            )

            if dry_run:
                updated += 1
                continue

            # Build the painless script to do the rename atomically
            # For each bare letter: remove it, add the dot form,
            # and update mapping_conf
            script_source = """
                def fclasses = params.bare_letters;
                for (int i = 0; i < fclasses.length; i++) {
                    def bare = fclasses[i];
                    def dotted = bare + '.';

                    // Rename in gn_fcodes array
                    if (ctx._source.gn_fcodes != null) {
                        int idx = ctx._source.gn_fcodes.indexOf(bare);
                        if (idx >= 0) {
                            ctx._source.gn_fcodes.remove(idx);
                        }
                        if (!ctx._source.gn_fcodes.contains(dotted)) {
                            ctx._source.gn_fcodes.add(dotted);
                        }
                    }

                    // Rename in mapping_conf.gn_fcodes
                    if (ctx._source.mapping_conf != null &&
                        ctx._source.mapping_conf.gn_fcodes != null) {
                        def conf = ctx._source.mapping_conf.gn_fcodes;
                        if (conf.containsKey(bare)) {
                            def val = conf[bare];
                            conf.remove(bare);
                            conf[dotted] = val;
                        }
                    }
                }
            """

            try:
                es.update(
                    index="types",
                    id=doc_id,
                    script={
                        "source": script_source,
                        "params": {"bare_letters": bare_letters},
                    },
                    refresh=True,
                    request_timeout=8,
                )
                updated += 1
            except Exception as e:
                self.stdout.write(self.style.ERROR(
                    f"  Error updating {doc_id}: {e}"
                ))

        # Refresh once at the end
        if not dry_run and updated > 0:
            es.indices.refresh(index="types")

        prefix = "[DRY RUN] " if dry_run else ""
        self.stdout.write(self.style.SUCCESS(
            f"\n{prefix}Done. Updated {updated} document(s)."
        ))

