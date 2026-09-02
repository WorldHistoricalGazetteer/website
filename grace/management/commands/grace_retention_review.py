"""Retention and transparency report for GRACE's Catalogue.

Implements the reporting half of two decision-6 obligations:

* **Obligation 4 — retention.** Contacts with no recorded interaction for
  ``RETENTION_REVIEW_YEARS`` (three) are surfaced for a human to decide about.
  Deliberately **not** an auto-delete: a long-dormant rights holder may still
  be the only person who can answer a licensing question, and quietly erasing
  them would lose the provenance for a right we rely on.
* **Obligation 1 — Article 14.** Contacts added more than a month ago who have
  still not been told we hold their details.

Run it on a schedule (Celery Beat, or cron) and read the output; nothing is
modified unless you pass ``--erase-reviewed``, which is a deliberate,
explicit act.

Usage::

    ./manage.py grace_retention_review
    ./manage.py grace_retention_review --years 5
    ./manage.py grace_retention_review --notices-only
"""
from django.core.management.base import BaseCommand

from grace import privacy
from grace.models import Person


class Command(BaseCommand):
    help = ("Report people due a retention review or an Article 14 privacy "
            "notice. Read-only unless --erase-reviewed is passed.")

    def add_arguments(self, parser):
        parser.add_argument(
            "--years", type=int, default=privacy.RETENTION_REVIEW_YEARS,
            help=f"Quiet period before review is due "
                 f"(default {privacy.RETENTION_REVIEW_YEARS}).")
        parser.add_argument("--notices-only", action="store_true",
                            help="Only report the Article 14 backlog.")
        parser.add_argument("--retention-only", action="store_true",
                            help="Only report the retention backlog.")
        parser.add_argument(
            "--erase-reviewed", action="store_true",
            help="DESTRUCTIVE. Pseudonymise every contact listed in the "
                 "retention section. Only use this after a human has actually "
                 "reviewed the list — that is what the rule requires.")

    def handle(self, *args, **options):
        years = options["years"]
        do_notices = not options["retention_only"]
        do_retention = not options["notices_only"]

        self.stdout.write(f"Lawful basis: {privacy.LAWFUL_BASIS}")
        self.stdout.write(f"Interest:     {privacy.LEGITIMATE_INTEREST}\n")

        if do_notices:
            self._report_notices()
        if do_retention:
            self._report_retention(years, options["erase_reviewed"])

    def _report_notices(self):
        owed = Person.objects.owed_privacy_notice().order_by("created_at")
        self.stdout.write(self.style.MIGRATE_HEADING(
            f"\nArticle 14 notices overdue "
            f"(added > {privacy.PRIVACY_NOTICE_DUE_DAYS} days ago, none sent): "
            f"{owed.count()}"))
        for c in owed[:200]:
            self.stdout.write(
                f"  #{c.pk:<6} {c.name[:40]:42} added {c.created_at:%Y-%m-%d}")
        if owed.count() > 200:
            self.stdout.write(f"  … and {owed.count() - 200} more")
        if owed.exists():
            self.stdout.write(
                "  → Send the notice, then use the admin action "
                "'Record that the Art. 14 privacy notice was sent'.")

    def _report_retention(self, years, erase):
        due = Person.objects.needing_retention_review(years).order_by("name")
        self.stdout.write(self.style.MIGRATE_HEADING(
            f"\nRetention review due (no interaction for {years}y): "
            f"{due.count()}"))
        for c in due[:200]:
            last = c.last_interaction_on or "never"
            self.stdout.write(
                f"  #{c.pk:<6} {c.name[:40]:42} last interaction: {last}")
        if due.count() > 200:
            self.stdout.write(f"  … and {due.count() - 200} more")

        if not erase:
            if due.exists():
                self.stdout.write(
                    "  → Review each. Keep anyone whose record still serves the "
                    "legitimate interest; erase the rest with the admin action "
                    "or --erase-reviewed.")
            return

        # Materialise before mutating — the queryset is defined by the very
        # field we are about to change.
        targets = list(due)
        for contact in targets:
            contact.pseudonymise()
        self.stdout.write(self.style.WARNING(
            f"\nPseudonymised {len(targets)} contact(s). Engagement and "
            f"interaction history was kept; the identities are gone."))
