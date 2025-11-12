# management/commands/fix_whg_index.py
from django.core.management.base import BaseCommand
from django.conf import settings
from elasticsearch8 import helpers

BATCH_SIZE = 500

class Command(BaseCommand):
    help = "Fix @lang in names.toponym and store separately in names.lang"

    def handle(self, *args, **options):
        es = settings.ES_CONN
        index = "whg"

        # Use helpers.scan to iterate all documents
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
                            n["toponym"] = parts[0]
                            n["lang"] = parts[1] if parts[1] else None
                            updated = True

            if updated:
                actions.append({
                    "_op_type": "update",
                    "_index": index,
                    "_id": doc_id,
                    "doc": {"names": source["names"]}
                })

            # Send in batches
            if len(actions) >= BATCH_SIZE:
                helpers.bulk(es, actions)
                count += len(actions)
                self.stdout.write(f"Updated {count} documents...")
                actions = []

        # Final batch
        if actions:
            helpers.bulk(es, actions)
            count += len(actions)
            self.stdout.write(f"Updated {count} documents total.")

        self.stdout.write(self.style.SUCCESS("Toponym fix completed."))
