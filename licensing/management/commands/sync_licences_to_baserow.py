"""Push the canonical WHG licence vocabulary to the Baserow "Licences" lookup table.

Django's ``licensing.License`` table is the single source of truth. This command
upserts each row (keyed on ``spdx_id``) into the Baserow Licences lookup table so
the tracker's controlled "Licence" link field always reflects the WHG vocabulary.
It is one-way (WHG -> Baserow) and idempotent; rows are matched on ``spdx_id``.

Run wherever both the Django DB and the bot credentials are available::

    python manage.py sync_licences_to_baserow            # apply
    python manage.py sync_licences_to_baserow --dry-run   # preview only
    python manage.py sync_licences_to_baserow --prune     # also delete Baserow
                                                          # rows whose spdx_id no
                                                          # longer exists in Django

Credentials (secrets) come from settings: BASEROW_BOT_EMAIL / BASEROW_BOT_PASSWORD
(set in local_settings or env). Target table: settings.BASEROW_LICENCES_TABLE_ID.
"""
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

import requests

# Fields mirrored to Baserow (must exist on the Licences table).
SYNCED_FIELDS = ["spdx_id", "label", "url", "permits_commercial",
                 "share_alike", "attribution_required", "custom"]


class Baserow:
    """Minimal JWT client scoped to what this sync needs."""

    def __init__(self, base_url, email, password):
        self.base = base_url.rstrip("/")
        self.s = requests.Session()
        r = self.s.post(f"{self.base}/api/user/token-auth/",
                        json={"email": email, "password": password}, timeout=30)
        if not r.ok:
            raise CommandError(f"Baserow auth failed ({r.status_code}): {r.text[:200]}")
        self.s.headers["Authorization"] = f"JWT {r.json()['access_token']}"

    def _req(self, method, path, **kw):
        r = self.s.request(method, f"{self.base}{path}", timeout=30, **kw)
        if not r.ok:
            raise CommandError(f"{method} {path} -> {r.status_code}: {r.text[:200]}")
        return r.json() if r.text else {}

    def rows(self, table_id):
        out, url = [], f"/api/database/rows/table/{table_id}/?user_field_names=true&size=200"
        while url:
            d = self._req("GET", url)
            out += d["results"]
            url = (d.get("next") or "").replace(self.base, "") or None
        return out

    def field_names(self, table_id):
        return {f["name"] for f in self._req("GET", f"/api/database/fields/table/{table_id}/")}

    def create_row(self, table_id, payload):
        return self._req("POST", f"/api/database/rows/table/{table_id}/?user_field_names=true", json=payload)

    def update_row(self, table_id, row_id, payload):
        return self._req("PATCH", f"/api/database/rows/table/{table_id}/{row_id}/?user_field_names=true", json=payload)

    def delete_row(self, table_id, row_id):
        return self._req("DELETE", f"/api/database/rows/table/{table_id}/{row_id}/")


class Command(BaseCommand):
    help = "Sync the WHG licence vocabulary (licensing.License) to the Baserow Licences table."

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true",
                            help="Report what would change without writing to Baserow.")
        parser.add_argument("--prune", action="store_true",
                            help="Delete Baserow rows whose spdx_id is not in Django (off by default).")

    def handle(self, *args, **opts):
        from licensing.models import License

        email = getattr(settings, "BASEROW_BOT_EMAIL", "")
        password = getattr(settings, "BASEROW_BOT_PASSWORD", "")
        table_id = getattr(settings, "BASEROW_LICENCES_TABLE_ID", None)
        if not (email and password and table_id):
            raise CommandError(
                "Baserow not configured: set BASEROW_BOT_EMAIL, BASEROW_BOT_PASSWORD "
                "and BASEROW_LICENCES_TABLE_ID (local_settings or env).")

        dry = opts["dry_run"]
        client = Baserow(settings.BASEROW_API_URL, email, password)

        missing = set(SYNCED_FIELDS) - client.field_names(table_id)
        if missing:
            raise CommandError(f"Licences table {table_id} is missing fields: {sorted(missing)}")

        existing = {r.get("spdx_id"): r for r in client.rows(table_id) if r.get("spdx_id")}
        vocab = list(License.objects.all().order_by("spdx_id"))
        created = updated = unchanged = pruned = 0

        for lic in vocab:
            payload = {f: getattr(lic, f) for f in SYNCED_FIELDS}
            row = existing.get(lic.spdx_id)
            if row is None:
                self.stdout.write(f"  + create  {lic.spdx_id}")
                if not dry:
                    client.create_row(table_id, payload)
                created += 1
            else:
                diff = {f: payload[f] for f in SYNCED_FIELDS if (row.get(f) or None) != (payload[f] or None)}
                if diff:
                    self.stdout.write(f"  ~ update  {lic.spdx_id}  {sorted(diff)}")
                    if not dry:
                        client.update_row(table_id, row["id"], diff)
                    updated += 1
                else:
                    unchanged += 1

        if opts["prune"]:
            django_ids = {lic.spdx_id for lic in vocab}
            for spdx, row in existing.items():
                if spdx not in django_ids:
                    self.stdout.write(self.style.WARNING(f"  - prune   {spdx} (row {row['id']})"))
                    if not dry:
                        client.delete_row(table_id, row["id"])
                    pruned += 1

        verb = "Would sync" if dry else "Synced"
        summary = (f"{verb}: {created} created, {updated} updated, {unchanged} unchanged"
                   + (f", {pruned} pruned" if opts['prune'] else ""))
        self.stdout.write(self.style.SUCCESS(summary))
