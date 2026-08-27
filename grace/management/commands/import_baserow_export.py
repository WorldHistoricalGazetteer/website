"""Import the Baserow snapshot into GRACE (step 7).

The snapshot lives in ``developer/baserow-export/`` — a full read-only export of
the WHG Baserow workspace, taken 27 August 2026 immediately before the service
account was deleted. It is **gitignored**, because this repository is public and
the export holds names, email addresses and phone numbers. That means this
command only works on a machine where someone has put the export in place.

Idempotent. Every table is matched on a natural key and updated in place, so
re-running never duplicates:

============================  ==================================  ==============
Baserow table                 GRACE model                         matched on
============================  ==================================  ==============
Print Gazetteer Bibliography  ``Source`` (printed gazetteer)      title + volume
Possible Datasets             ``TrackedGazetteer`` (prospects)     title
People ALL                    ``Contact``                          email, else name
============================  ==================================  ==============

Deliberately **not** imported:

* *Licences* — ``licensing.License`` is already the single source of truth, and
  that Baserow table was only ever a mirror of it.
* *Project Management* (the whole database) — Baserow's stock template, not WHG
  data.
* *Comparable Projects / Environmental Scan* — a survey of other people's
  projects; useful reading, but not the Catalogue. Import it by hand if wanted.

**On the people import.** Contacts are created under legitimate interests (see
``grace/privacy.py``), and every one of them starts with no Article 14 notice
recorded — which is correct, because none has been sent. They will appear in
``grace_retention_review`` as overdue after a month. That is the system working:
these are exactly the people the obligation exists for. Pass ``--skip-people``
to load the non-personal tables first if you would rather do that separately.

Usage::

    ./manage.py import_baserow_export
    ./manage.py import_baserow_export --dry-run
    ./manage.py import_baserow_export --path /some/other/export --skip-people
"""
import json
import pathlib
import re

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from grace.models import Contact, Organisation, Source, TrackedGazetteer
from grace.vocabularies import (
    DigitizationStatus, DiscoverySource, SourceType, Stage,
)

DEFAULT_PATH = pathlib.Path(settings.BASE_DIR) / "developer" / "baserow-export"
TRACKER_DIR = "whg-dataset-project-tracker"

#: "Karl Ryavec (kryavec@ucmerced.edu)" -> name + address
CONTACT_WITH_EMAIL = re.compile(r"^(?P<name>[^(<]+)[(<]\s*(?P<email>[^)>\s]+@[^)>\s]+)\s*[)>]?")
EMAIL_ONLY = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

#: Free-text scan status -> DigitizationStatus slug. Anything unrecognised is
#: left unset rather than guessed at.
SCAN_HINTS = [
    (("pdf/scans available", "pdf available", "downloadable", "full text",
      "archive.org", "scans available"), "scan-downloadable"),
    (("catalog record", "catalogue record", "full view", "catalog only"),
     "catalogue-full-view-only"),
    (("not digiti", "no scan", "print only"), "not-digitised"),
]


def _split_tags(raw):
    if not raw:
        return []
    return [t.strip() for t in re.split(r"[;,]", str(raw)) if t.strip()][:32]


def _clean(text):
    """Normalise whitespace, including the non-breaking spaces Baserow carries."""
    if not text:
        return ""
    return re.sub(r"\s+", " ", str(text).replace("\u00a0", " ")).strip()


def _display_name(name, email):
    """A display name that is never an email address.

    Some Baserow rows carry an address and no name. Falling back to the raw
    address would put it in ``Contact.name`` — an unencrypted, indexed column —
    which defeats the whole point of encrypting ``Contact.email``. So we use the
    local part as a handle instead: enough to tell two rows apart in a list,
    while the real address stays encrypted and is still findable via
    ``Contact.objects.by_email()``.
    """
    name = _clean(name)
    if name and "@" not in name:
        return name
    if email:
        return email.split("@")[0][:255]
    return name or "(no name recorded)"


def _years(raw):
    """Pull a start and end year out of prose like '1877–1896' or 'c.1900'."""
    if not raw:
        return None, None
    found = [int(y) for y in re.findall(r"\b(1[0-9]{3}|20[0-2][0-9])\b", str(raw))]
    if not found:
        return None, None
    return min(found), (max(found) if len(found) > 1 else None)


class Command(BaseCommand):
    help = "Import the Baserow export into GRACE. Idempotent."

    def add_arguments(self, parser):
        parser.add_argument("--path", default=str(DEFAULT_PATH),
                            help=f"Export directory (default {DEFAULT_PATH}).")
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--skip-people", action="store_true",
                            help="Load only the non-personal tables.")

    def handle(self, *args, **options):
        self.root = pathlib.Path(options["path"])
        if not (self.root / TRACKER_DIR).is_dir():
            raise CommandError(
                f"No Baserow export at {self.root}. It is gitignored (it holds "
                f"personal data and this repo is public), so it has to be put "
                f"in place manually — see developer/baserow-export/README.md.")

        with transaction.atomic():
            self.stdout.write(self.style.MIGRATE_HEADING("Sources (bibliography)"))
            self._import_bibliography()

            self.stdout.write(self.style.MIGRATE_HEADING("\nGazetteers (prospects)"))
            self._import_possible_datasets()

            if options["skip_people"]:
                self.stdout.write("\nSkipping People ALL (--skip-people).")
            else:
                self.stdout.write(self.style.MIGRATE_HEADING("\nContacts"))
                self._import_people()

            if options["dry_run"]:
                transaction.set_rollback(True)
                self.stdout.write(self.style.WARNING("\nDRY RUN — rolled back."))

    # -- helpers ----------------------------------------------------------

    def _load(self, stem):
        path = self.root / TRACKER_DIR / f"{stem}.json"
        if not path.exists():
            raise CommandError(f"missing {path}")
        return json.loads(path.read_text(encoding="utf-8"))["rows"]

    @staticmethod
    def _vocab(model, slug):
        return model.objects.filter(slug=slug).first()

    # -- tables -----------------------------------------------------------

    def _import_bibliography(self):
        printed = self._vocab(SourceType, "printed-gazetteer")
        if not printed:
            self.stdout.write(self.style.WARNING(
                "  No 'printed-gazetteer' SourceType — run "
                "seed_grace_vocabularies first. Importing without a type."))

        created = updated = 0
        for row in self._load("print-gazetteer-bibliography"):
            title = (row.get("title") or "").strip()
            if not title:
                continue
            volume = (row.get("Volume / district example") or "").strip()
            years = (row.get("Publication year(s)") or "").strip()
            start, end = _years(years)

            scan_raw = (row.get("PDF / scan status") or "").lower()
            digi = None
            for needles, slug in SCAN_HINTS:
                if any(n in scan_raw for n in needles):
                    digi = self._vocab(DigitizationStatus, slug)
                    break

            defaults = {
                "author_compiler": (row.get("Author / compiler") or "")[:500],
                "publication_years": years[:100],
                "publication_year_start": start,
                "publication_year_end": end,
                "region_covered": row.get("Region covered") or "",
                "repository": (row.get("Repository checked") or "")[:255],
                "source_url": (row.get("Source / access URL") or "")[:1000],
                "tags": _split_tags(row.get("Tags")),
                "source_type": printed,
                "digitization_status": digi,
            }
            # "Suggested next action" is editorial, not bibliographic — keep it
            # as a note rather than inventing a field for it.
            action = (row.get("Suggested next action") or "").strip()
            if action:
                defaults["notes"] = f"Suggested next action (from Baserow): {action}"

            obj, was_created = Source.objects.update_or_create(
                title=title[:500], volume_example=volume[:500],
                defaults=defaults,
            )
            created += was_created
            updated += (not was_created)
        self.stdout.write(f"  {created} created, {updated} updated")

    def _import_possible_datasets(self):
        on_radar = self._vocab(Stage, "on-our-radar")
        created = updated = 0
        for row in self._load("possible-datasets"):
            # A few rows carry several names in one cell, newline-separated.
            # Take the first as the title and keep the rest in the notes rather
            # than silently dropping half the record.
            raw_name = (row.get("Dataset Name") or "").strip()
            if not raw_name:
                continue
            parts = [p.strip() for p in raw_name.splitlines() if p.strip()]
            title, extra = parts[0], parts[1:]

            notes = []
            if extra:
                notes.append("Also listed under: " + "; ".join(extra))
            for label in ("Hosted At", "Rights", "Status", "Notes"):
                value = (row.get(label) or "").strip()
                if value:
                    notes.append(f"{label}: {value}")
            contact_raw = (row.get("Contact Person") or "").strip()
            if contact_raw:
                notes.append(f"Contact (from Baserow): {contact_raw}")

            obj, was_created = TrackedGazetteer.objects.update_or_create(
                title=title[:500],
                defaults={
                    "notes": "\n".join(notes),
                    # Everything here is a prospect by definition: registry
                    # stays NULL, and that is what makes it one (review §2).
                    "stage": on_radar,
                },
            )
            created += was_created
            updated += (not was_created)
        self.stdout.write(f"  {created} created, {updated} updated "
                          f"(all prospects — no Register link)")

    def _import_people(self):
        # These came out of the team's own Baserow research, not a
        # public submission — so "flagged internally", not "web form".
        found_by = self._vocab(DiscoverySource, "flagged-internally")
        created = updated = skipped = 0

        for row in self._load("people-all"):
            raw_name = _clean(row.get("Name"))
            email = _clean(row.get("Email Address")) or None
            if not raw_name and not email:
                skipped += 1
                continue
            if email and not EMAIL_ONLY.match(email):
                # Junk in the address column — keep the person, drop the value
                # rather than storing something that is not an address.
                email = None
            name = _display_name(raw_name, email)

            institution = _clean(row.get("Institution"))
            org = None
            if institution:
                org, _ = Organisation.objects.get_or_create(
                    name=institution[:255])

            # Match on address where we have one — it is the only reliable key.
            existing = None
            if email:
                existing = Contact.objects.by_email(email)
            if existing is None and name:
                existing = Contact.objects.filter(name=name[:255],
                                                  is_erased=False).first()

            if existing:
                existing.organisation = existing.organisation or org
                if email and not existing.user_id:
                    existing.email = email
                # Repair rows created before _display_name existed, whose name
                # is a raw address sitting in an unencrypted column.
                if "@" in existing.name:
                    existing.name = name[:255]
                existing.save()
                updated += 1
            else:
                Contact.objects.create(
                    name=name[:255],
                    email=email,
                    organisation=org,
                    discovery_source=found_by,
                )
                created += 1

        self.stdout.write(f"  {created} created, {updated} updated, "
                          f"{skipped} skipped (no name or address)")
        self.stdout.write(
            "  Held under legitimate interests. None has had an Article 14 "
            "notice — run grace_retention_review to see the backlog.")
