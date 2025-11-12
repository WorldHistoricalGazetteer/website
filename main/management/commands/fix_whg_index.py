# management/commands/fix_whg_index.py
from django.core.management.base import BaseCommand
from django.conf import settings
from elasticsearch8 import helpers

BATCH_SIZE = 500

class Command(BaseCommand):
    help = "Fix @lang in names.toponym and store separately in names.lang"

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would be updated without sending to ES"
        )

    def handle(self, *args, **options):
        es = settings.ES_CONN
        index = "whg"
        dry_run = options["dry_run"]

        docs = helpers.scan(
            client=es,
            index=index,
            query={"query": {"match_all": {}}},
            _source_includes=["names"]
        )

        actions = []
        count = 0

        for doc in docs:
            doc_id = doc["_id"]
            source = doc["_source"]
            updated = False

            if "names" in source:
                for n in source["names"]:
                    if "toponym" in n:
                        t = n["toponym"]
                        if "@" in t:
                            parts = t.split("@", 1)
                            old_toponym = n["toponym"]
                            old_lang = n.get("lang")
                            n["toponym"] = parts[0]
                            n["lang"] = parts[1] if parts[1] else None
                            updated = True

                            if dry_run:
                                self.stdout.write(
                                    f"[DRY-RUN] {doc_id}: '{old_toponym}' -> '{n['toponym']}', lang: {old_lang} -> {n['lang']}"
                                )

            if updated and not dry_run:
                actions.append({
                    "_op_type": "update",
                    "_index": index,
                    "_id": doc_id,
                    "doc": {"names": source["names"]}
                })

            if len(actions) >= BATCH_SIZE and not dry_run:
                helpers.bulk(es, actions)
                count += len(actions)
                self.stdout.write(f"Updated {count} documents...")
                actions = []

        if actions and not dry_run:
            helpers.bulk(es, actions)
            count += len(actions)
            self.stdout.write(f"Updated {count} documents total.")

        if dry_run:
            self.stdout.write(self.style.SUCCESS("Dry run completed. No documents were modified."))
        else:
            self.stdout.write(self.style.SUCCESS("Toponym fix completed."))
