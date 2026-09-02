"""Seed a small, obviously-fake worked example so the admin demonstrates itself.

The imported catalogue has 42 datasets and 209 people and **not one engagement
linking them**, so every Connections panel renders empty and the attention
alarms never fire. That reads as "the feature is missing" rather than "nothing
is connected yet", which is the opposite of what a reviewer should conclude.

So this creates one self-contained cluster — an organisation, three people, a
project, a printed source, two datasets, their conversations and one review —
wired to each other and to nothing real. Opening any of them shows the panels
working.

**Everything it creates is prefixed** ``DEMO — `` and carries a note saying so,
which is also how ``--remove`` finds it again. Nothing pre-existing is touched,
read or modified.

Two deliberate details:

* The demo people have their Article 14 notice marked as already sent. They are
  fictional and owe nothing, and leaving them unmarked would inflate the real
  backlog count on the landing page.
* One conversation is stalled and one review is back but unshared, because those
  are the two alarms that cannot be demonstrated any other way — both are the
  *absence* of an event, so only real data with real dates shows them.

Usage::

    ./manage.py seed_grace_demo
    ./manage.py seed_grace_demo --remove
"""
import datetime

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from grace.models import (
    ActionItem, Engagement, Interaction, Organisation, Person, Project, Review,
    Source, TrackedDataset,
)
from grace import vocabularies as V

#: Every demo record starts with this. It is the label a reviewer sees and the
#: handle ``--remove`` deletes by, so do not change one without the other.
PREFIX = "DEMO — "

NOTE = ("SYNTHETIC DEMONSTRATION DATA. Invented for the September 2026 review "
        "so the Connections panels and the attention alarms have something to "
        "show. Not a real gazetteer, person, project or conversation. Remove "
        "with: ./manage.py seed_grace_demo --remove")


class Command(BaseCommand):
    help = ("Create (or remove) a clearly-labelled worked example so the admin "
            "demonstrates its own connections and alarms.")

    def add_arguments(self, parser):
        parser.add_argument(
            "--remove", action="store_true",
            help="Delete everything this command created, matched on the "
                 f"'{PREFIX.strip()}' prefix.")

    # -- vocabulary lookups, tolerant of an unseeded database ---------------

    def _term(self, model, slug):
        """Look a vocabulary term up by its stable slug.

        Misses are collected rather than raised: a missing term leaves one
        field blank, which is survivable. But they are *reported*, because the
        failure mode otherwise is a demo that seeds successfully and shows
        nothing — and the likeliest cause is that someone renamed a seeded
        label before its slug existed, or the seeder was never run.
        """
        term = model.objects.filter(slug=slug).first()
        if term is None:
            self._missing.append(f"{model.__name__}: {slug}")
        return term

    def handle(self, *args, **options):
        self._missing = []
        if options["remove"]:
            return self._remove()
        self._create()
        if self._missing:
            self.stdout.write(self.style.WARNING(
                "\nSome vocabulary terms were not found, so those fields are "
                "blank.\nRun ./manage.py seed_grace_vocabularies first:\n  "
                + "\n  ".join(self._missing)))

    # ----------------------------------------------------------------------

    def _remove(self):
        """Delete by prefix. Engagements, interactions, action items and
        reviews cascade from their parents, so only the roots are named."""
        counts = []
        for model, field in (
            (Engagement, "subject"), (Review, "dataset__title"),
            (TrackedDataset, "title"), (Source, "title"),
            (Project, "name"), (Person, "name"), (Organisation, "name"),
        ):
            qs = model.objects.filter(**{f"{field}__startswith": PREFIX})
            n = qs.count()
            if n:
                qs.delete()
                counts.append(f"{n} {model._meta.verbose_name_plural}")
        self.stdout.write(self.style.SUCCESS(
            "Removed " + (", ".join(counts) if counts else "nothing — none found")))

    @transaction.atomic
    def _create(self):
        today = datetime.date.today()

        org, _ = Organisation.objects.get_or_create(
            name=f"{PREFIX}Exampleshire Record Office",
            defaults={
                "short_name": "DEMO ERO",
                "org_type": self._term(V.OrganisationType, "archive"),
                "notes": NOTE,
            })

        archivist, _ = Person.objects.get_or_create(
            name=f"{PREFIX}Jane Example",
            defaults={
                "given_name": "Jane", "surname": "Example",
                "email": "jane.example@example.org",
                "organisation": org,
                "role": self._term(V.PersonRole, "archivist-librarian"),
                "status": self._term(V.PersonStatus, "active"),
                "email_status": self._term(V.EmailStatus, "deliverable"),
                "privacy_notice_sent_at": timezone.now(),
                "notes": NOTE,
            })
        compiler, _ = Person.objects.get_or_create(
            name=f"{PREFIX}Sam Sample",
            defaults={
                "given_name": "Sam", "surname": "Sample",
                "email": "sam.sample@example.org",
                "role": self._term(V.PersonRole, "compiler-author"),
                "status": self._term(V.PersonStatus, "active"),
                # Shows the bounce case: the person survives, the address does not.
                "email_status": self._term(V.EmailStatus, "bounced"),
                "email_status_checked_on": today - datetime.timedelta(days=20),
                "privacy_notice_sent_at": timezone.now(),
                "notes": NOTE,
            })
        # One of us, to show the Art. 14 exemption working.
        colleague, _ = Person.objects.get_or_create(
            name=f"{PREFIX}Alex Editor (WHG)",
            defaults={
                "given_name": "Alex", "surname": "Editor",
                "role": self._term(V.PersonRole, "whg-staff"),
                "status": self._term(V.PersonStatus, "active"),
                "notes": NOTE,
            })

        project, _ = Project.objects.get_or_create(
            name=f"{PREFIX}Exampleshire Historical Gazetteer",
            defaults={
                "description": NOTE,
                "status": self._term(V.ProjectStatus, "active"),
                "organisation": org,
                "funder": "DEMO — Example Research Council",
                "grant_number": "DEMO-000-0000",
                "start_date": today - datetime.timedelta(days=400),
                "notes": NOTE,
            })
        project.people.set([archivist, compiler])

        source, _ = Source.objects.get_or_create(
            title=f"{PREFIX}A Topographical Dictionary of Exampleshire",
            defaults={
                "author_compiler": "Sample, S.",
                "publication_years": "1845–1851",
                "publication_year_start": 1845,
                "publication_year_end": 1851,
                "source_type": self._term(V.SourceType, "printed-gazetteer"),
                "digitization_status": self._term(
                    V.DigitizationStatus, "scan-downloadable"),
                "repository": "DEMO — Internet Archive",
                "notes": NOTE,
            })
        source.people.set([compiler])

        # -- the two datasets ----------------------------------------------

        parishes, _ = TrackedDataset.objects.get_or_create(
            title=f"{PREFIX}Exampleshire Parish Names",
            defaults={
                "stage": self._term(V.Stage, "in-internal-review"),
                "permission_status": self._term(V.PermissionStatus, "granted"),
                "organisation": org,
                "project": project,
                "data_format": self._term(V.DataFormat, "csv-tsv"),
                "geometry_status": self._term(
                    V.GeometryStatus, "coordinates-present"),
                "expected_record_count": 4200,
                "expected_rights_holder": "DEMO — Exampleshire Record Office",
                "languages": ["eng"],
                "temporal_prose": "1500–1900",
                "temporal_start_year": 1500,
                "temporal_end_year": 1900,
                "on_radar_since": today - datetime.timedelta(days=300),
                "notes": NOTE,
            })
        parishes.people.set([archivist, compiler])
        source.derived_datasets.add(parishes)

        coastal, _ = TrackedDataset.objects.get_or_create(
            title=f"{PREFIX}Sample Coastal Place Names",
            defaults={
                "stage": self._term(V.Stage, "permission-being-sought"),
                "permission_status": self._term(
                    V.PermissionStatus, "asked-awaiting-reply"),
                "organisation": org,
                "data_format": self._term(V.DataFormat, "not-yet-known"),
                "geometry_status": self._term(V.GeometryStatus, "unknown"),
                "expected_record_count": 900,
                "on_radar_since": today - datetime.timedelta(days=120),
                "notes": NOTE,
            })
        coastal.people.set([archivist])
        source.documents.add(coastal)

        # -- a healthy conversation ----------------------------------------

        live, created = Engagement.objects.get_or_create(
            subject=f"{PREFIX}Permission to publish the parish names",
            defaults={
                "person": archivist,
                "dataset": parishes,
                "project": project,
                "organisation": org,
                "stage": self._term(V.EngagementStage, "in-discussion"),
                "priority": self._term(V.Priority, "high"),
                "next_follow_up": today + datetime.timedelta(days=10),
                "opened_on": today - datetime.timedelta(days=60),
                "notes": NOTE,
            })
        if created:
            Interaction.objects.create(
                engagement=live, person=archivist,
                channel=self._term(V.InteractionChannel, "email"),
                occurred_on=today - datetime.timedelta(days=60),
                summary=f"{PREFIX}Introduced WHG and asked who holds the rights.")
            Interaction.objects.create(
                engagement=live, person=archivist,
                channel=self._term(V.InteractionChannel, "video-call"),
                occurred_on=today - datetime.timedelta(days=21),
                summary=f"{PREFIX}Walked through licensing options; CC-BY likely.")
            ActionItem.objects.create(
                engagement=live,
                description=f"{PREFIX}Send the draft licence wording",
                due_date=today + datetime.timedelta(days=7),
                status=self._term(V.ActionItemStatus, "to-do"))

        # -- a stalled one, which is the alarm that matters ----------------

        stalled, created = Engagement.objects.get_or_create(
            subject=f"{PREFIX}Coastal names — no reply since the spring",
            defaults={
                "person": compiler,
                "dataset": coastal,
                "organisation": org,
                "stage": self._term(V.EngagementStage, "awaiting-reply"),
                "priority": self._term(V.Priority, "medium"),
                # In the past on purpose: a stall is the absence of a change,
                # so only an overdue follow-up date can reveal it.
                "next_follow_up": today - datetime.timedelta(days=45),
                "opened_on": today - datetime.timedelta(days=150),
                "notes": NOTE,
            })
        if created:
            Interaction.objects.create(
                engagement=stalled, person=compiler,
                channel=self._term(V.InteractionChannel, "email"),
                occurred_on=today - datetime.timedelta(days=150),
                summary=f"{PREFIX}Asked whether the coastal sheets are digitised.")

        # -- a review that came back and was never passed on ---------------

        Review.objects.get_or_create(
            dataset=parishes,
            review_type=self._term(V.ReviewType, "internal-editorial"),
            defaults={
                "reviewer": colleague,
                "sent_on": today - datetime.timedelta(days=30),
                "returned_on": today - datetime.timedelta(days=9),
                "recommendation": self._term(
                    V.ReviewRecommendation, "approved-with-revisions"),
                "comments": f"{PREFIX}Place types need mapping to AAT before "
                            f"accession. Otherwise ready.",
                # shared_with_author_on left blank on purpose — this is the
                # second alarm, and from the contributor's side nothing has
                # happened at all.
            })

        self.stdout.write(self.style.SUCCESS(
            f"Seeded the '{PREFIX.strip()}' worked example: 1 organisation, "
            f"3 people, 1 project, 1 source, 2 datasets, 2 engagements, "
            f"3 interactions, 1 action item, 1 review."))
        self.stdout.write(
            "Every record is prefixed and carries a note saying it is "
            "synthetic.\nRemove it all with: ./manage.py seed_grace_demo "
            "--remove")
