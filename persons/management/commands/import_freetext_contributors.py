"""Seed persons.Contribution rows from the legacy free-text creator/contributors
fields on datasets and collections.

Idempotent: objects that already have any structured Contribution are skipped.
Default role mapping: ``creator`` -> Conceptualization, ``contributors`` ->
Data curation (editable afterwards). Run with --dry-run to preview counts.
"""
from django.contrib.contenttypes.models import ContentType
from django.core.management.base import BaseCommand
from django.db import transaction

from persons.models import Person, Contribution, CreditRole
from utils.csl_citation_formatter import parse_names

# (free-text field name, default CRediT role)
FIELD_ROLES = [
    ("creator", CreditRole.CONCEPTUALIZATION),
    ("contributors", CreditRole.DATA_CURATION),
]


def _get_person(name):
    """name is a CSL dict from parse_names: {family, given[, ORCID]} or {literal}.

    Uses filter().first() rather than get_or_create so that pre-existing
    DUPLICATE Person rows (same family/given) don't raise MultipleObjectsReturned
    — the first match is reused.
    """
    literal = name.get("literal")
    if literal:
        return (Person.objects.filter(literal=literal).first()
                or Person.objects.create(literal=literal))

    orcid = (name.get("ORCID") or "").replace("https://orcid.org/", "") or None
    if orcid:
        existing = Person.objects.filter(orcid=orcid).first()
        if existing:
            return existing

    family = name.get("family") or None
    given = name.get("given") or None
    existing = Person.objects.filter(family=family, given=given).first()
    if existing:
        return existing
    return Person.objects.create(
        family=family, given=given, **({"orcid": orcid} if orcid else {}))


class Command(BaseCommand):
    help = "Seed Contribution rows from datasets'/collections' free-text creator/contributors."

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true",
                            help="Report what would be created without writing.")

    def handle(self, *args, **opts):
        from datasets.models import Dataset
        from collection.models import Collection

        dry = opts["dry_run"]
        people = contributions = 0

        errors = 0
        for Model in (Dataset, Collection):
            ct = ContentType.objects.get_for_model(Model)
            for obj in Model.objects.all():
                if Contribution.objects.filter(content_type=ct, object_id=str(obj.pk)).exists():
                    continue
                try:
                    # Per-object atomic so a bad row never leaves a half-imported
                    # object and never aborts the whole run.
                    with transaction.atomic():
                        order = 0
                        for field, role in FIELD_ROLES:
                            text = getattr(obj, field, None)
                            if not text:
                                continue
                            for name in parse_names(text):
                                order += 1
                                if dry:
                                    contributions += 1
                                    continue
                                person = _get_person(name)
                                people += 1
                                _, created = Contribution.objects.get_or_create(
                                    person=person, role=role, content_type=ct,
                                    object_id=str(obj.pk), defaults={"order": order},
                                )
                                contributions += int(created)
                except Exception as e:
                    errors += 1
                    self.stderr.write(
                        f"  skipped {Model.__name__} {obj.pk}: {e}")

        verb = "Would create" if dry else "Created"
        self.stdout.write(self.style.SUCCESS(
            f"{verb} {contributions} contributions"
            + ("" if dry else f" (touched {people} people, {errors} objects skipped)")
        ))
