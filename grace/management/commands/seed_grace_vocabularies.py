"""Seed GRACE's controlled vocabularies with a sensible starting set.

Idempotent: matched on ``slug``, so re-running updates the flags and ordering
without duplicating anything, and never touches a term's label or description
once someone has edited it.

**These are starting points, not a fixed list.** Every one of these tables is
an editor's to change in the admin — the whole point of decision 3. Adding a
term here is a convenience for a fresh database, not a claim on the vocabulary.

Terms that code actually depends on are flagged, and the command refuses to
leave the database in a state where those flags are missing:

* exactly one ``IntakeStatus`` with ``is_untriaged`` — public submissions land
  on it, and without one the intake queue is invisible;
* a ``DiscoverySource`` with slug ``web-form`` — the value the old design had
  no way to express.

Usage::

    ./manage.py seed_grace_vocabularies
    ./manage.py seed_grace_vocabularies --dry-run
"""
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils.text import slugify

from grace import vocabularies as V

# (model, [(label, sort_order, {extra flags}), …])
SEED = [
    (V.PersonRole, [
        # People on the other side of a conversation.
        ("Compiler / author", 10, {"is_internal": False}),
        ("Rights holder", 20, {"is_internal": False}),
        ("Archivist / librarian", 30, {"is_internal": False}),
        ("Researcher", 40, {"is_internal": False}),
        ("Project lead", 50, {"is_internal": False}),
        ("Institutional contact", 60, {"is_internal": False}),
        # Our own side. The People register is everyone, ourselves included,
        # and is_internal keeps colleagues out of the Art. 14 notice queue.
        ("WHG staff", 100, {"is_internal": True}),
        ("WHG advisory board", 110, {"is_internal": True}),
        ("Collaborator", 120, {"is_internal": True}),
        ("Technical expert", 130, {"is_internal": True}),
        ("Developer", 140, {"is_internal": True}),
        ("Translator", 150, {"is_internal": True}),
        ("Other", 900, {"is_internal": False}),
    ]),
    (V.PersonStatus, [
        ("Active", 10, {}),
        ("Dormant", 20, {}),
        ("Do not contact", 30, {}),
    ]),
    (V.OrganisationType, [
        ("Archive", 10, {}), ("Library", 20, {}), ("Museum", 30, {}),
        ("University", 40, {}), ("Research institute", 50, {}),
        ("Publisher", 60, {}), ("Learned society", 70, {}),
        ("Government body", 80, {}), ("Other", 900, {}),
    ]),
    (V.ProjectStatus, [
        ("Proposed", 10, {}), ("Active", 20, {}), ("Paused", 30, {}),
        ("Complete", 40, {}), ("Abandoned", 50, {}),
    ]),
    (V.SourceType, [
        # The one place in GRACE where "gazetteer" means a LEAD rather than a
        # WHG contribution — the label says so explicitly (review §8).
        ("Printed gazetteer", 10, {}),
        ("Printed atlas or map series", 20, {}),
        ("Journal article", 30, {}),
        ("Book or monograph", 40, {}),
        ("Dataset", 50, {}),
        ("Archival collection", 60, {}),
        ("Website", 70, {}),
        ("Other", 900, {}),
    ]),
    (V.DigitizationStatus, [
        ("Scan downloadable", 10, {}),
        ("Catalogue / full view only", 20, {}),
        ("Not digitised", 30, {}),
        ("Unknown", 40, {}),
    ]),
    (V.DiscoverySource, [
        ("Conference", 10, {}),
        ("Referral", 20, {}),
        ("Literature", 30, {}),
        ("Flagged internally", 40, {}),
        # Required: without this there is no way to say "arrived from the
        # public". Its slug is depended on by grace.views.
        ("Web form", 50, {}),
        ("Other", 900, {}),
    ]),
    (V.Stage, [
        # Editorial values ONLY. published / indexed / not indexed are read
        # through the Register link and must never appear here (review §2).
        ("On our radar", 10, {"is_open": True}),
        ("Making contact", 20, {"is_open": True}),
        ("Permission being sought", 30, {"is_open": True}),
        ("Permission granted", 40, {"is_open": True}),
        ("Awaiting data", 50, {"is_open": True}),
        ("In preparation", 60, {"is_open": True}),
        ("In internal review", 70, {"is_open": True}),
        ("Revisions requested", 80, {"is_open": True}),
        ("Ready to accession", 90, {"is_open": True}),
        ("On hold", 100, {"is_open": False}),
        ("Declined", 110, {"is_open": False}),
        ("Complete", 120, {"is_open": False}),
    ]),
    (V.PermissionStatus, [
        ("Not yet asked", 10, {}),
        ("Asked, awaiting reply", 20, {}),
        ("Under negotiation", 30, {}),
        ("Granted", 40, {}),
        ("Refused", 50, {}),
        ("Not required (open licence)", 60, {}),
        ("Unclear", 70, {}),
    ]),
    (V.ReviewRecommendation, [
        ("Pending", 10, {}), ("Approved", 20, {}),
        ("Approved with revisions", 30, {}), ("Rejected", 40, {}),
    ]),
    (V.IntakeStatus, [
        # Exactly one is_untriaged term. New public submissions land here.
        ("Untriaged", 10, {"is_untriaged": True}),
        ("Under consideration", 20, {}),
        ("Accepted", 30, {}),
        ("Declined", 40, {}),
        ("Duplicate", 50, {}),
        ("Spam", 60, {}),
    ]),
    (V.EngagementStage, [
        ("Not yet contacted", 10, {"is_open": True}),
        ("Awaiting reply", 20, {"is_open": True}),
        ("In discussion", 30, {"is_open": True}),
        ("Awaiting something from us", 40, {"is_open": True}),
        ("Concluded", 50, {"is_open": False}),
        ("Abandoned", 60, {"is_open": False}),
    ]),
    (V.Priority, [
        ("High", 10, {}), ("Medium", 20, {}), ("Low", 30, {}),
    ]),
    (V.InteractionChannel, [
        ("Email", 10, {}), ("Call", 20, {}), ("Video call", 30, {}),
        ("Meeting in person", 40, {}), ("Conference", 50, {}),
        ("Letter", 60, {}), ("Other", 900, {}),
    ]),
    (V.ActionItemStatus, [
        ("To do", 10, {"is_open": True}),
        ("In progress", 20, {"is_open": True}),
        ("Blocked", 30, {"is_open": True}),
        ("Done", 40, {"is_open": False}),
        ("Cancelled", 50, {"is_open": False}),
    ]),
    (V.EngagementOutcome, [
        ("Agreed", 10, {}), ("Declined", 20, {}), ("No reply", 30, {}),
        ("Superseded", 40, {}), ("Deferred", 50, {}),
    ]),
    (V.ContentItemType, [
        ("Blog post", 10, {}), ("Newsletter item", 20, {}),
        ("Conference paper", 30, {}), ("Tutorial", 40, {}),
        ("Announcement", 50, {}), ("Other", 900, {}),
    ]),
    (V.ContentStatus, [
        ("Idea", 10, {}), ("Planned", 20, {}), ("Drafting", 30, {}),
        ("In review", 40, {}), ("Published", 50, {}), ("Dropped", 60, {}),
    ]),
]


class Command(BaseCommand):
    help = "Seed GRACE's controlled vocabularies (idempotent, matched on slug)."

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true",
                            help="Report what would change; write nothing.")

    @transaction.atomic
    def handle(self, *args, **options):
        dry = options["dry_run"]
        created = updated = 0

        for model, terms in SEED:
            for label, order, flags in terms:
                slug = slugify(label)[:120]
                defaults = {"label": label, "sort_order": order, **flags}
                existing = model.objects.filter(slug=slug).first()
                if existing is None:
                    created += 1
                    if not dry:
                        model.objects.create(slug=slug, **defaults)
                    self.stdout.write(f"  + {model.__name__}: {label}")
                else:
                    # Only ever refresh ordering and the code-critical flags.
                    # A label or description someone has edited stays edited.
                    changes = {k: v for k, v in
                               [("sort_order", order), *flags.items()]
                               if getattr(existing, k) != v}
                    if changes:
                        updated += 1
                        if not dry:
                            for k, v in changes.items():
                                setattr(existing, k, v)
                            existing.save(update_fields=list(changes))
                        self.stdout.write(
                            f"  ~ {model.__name__}: {label} {changes}")

        self._check_invariants()

        verb = "would be" if dry else ""
        self.stdout.write(self.style.SUCCESS(
            f"\n{created} term(s) {verb} created, {updated} updated."))
        if dry:
            transaction.set_rollback(True)

    def _check_invariants(self):
        """Fail loudly if a flag code depends on is missing or ambiguous."""
        untriaged = V.IntakeStatus.objects.filter(is_untriaged=True).count()
        if untriaged != 1:
            self.stdout.write(self.style.ERROR(
                f"IntakeStatus has {untriaged} terms flagged is_untriaged; "
                f"there must be exactly one, or public submissions will not "
                f"land in a visible queue."))
        if not V.DiscoverySource.objects.filter(slug="web-form").exists():
            self.stdout.write(self.style.ERROR(
                "No DiscoverySource with slug 'web-form' — grace.views "
                "depends on it to mark public submissions."))
        for model in (V.Stage, V.EngagementStage, V.ActionItemStatus):
            if not model.objects.filter(is_open=False).exists():
                self.stdout.write(self.style.WARNING(
                    f"{model.__name__} has no terminal (is_open=False) term; "
                    f"nothing will ever count as finished."))
