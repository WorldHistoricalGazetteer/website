"""Record a licence against contributed datasets that predate per-dataset capture.

Contributed datasets fall into cohorts defined by when the upload form began
recording an explicit licence acceptance (``license_acceptance``, live from
2024-07-15 to 2026-07-29). Only the datasets uploaded while that control was in
service have a confirmed basis for a retrospective licence; see place#158.

  cohort A  create_date >= 2024-07-15   contributor ticked the acceptance box
  cohort B  create_date <  2024-07-15   licence displayed but never recorded

Cohort A: contributors ticked the acceptance box (``legacy_acceptance``).
Cohort B: the upload page displayed "License: CC BY 4.0" as a static row. Pitt
(Tara, 2026-09) saw no issue treating that as CC BY 4.0, and SG ruled on
2026-11-17 (place#349 C1) that the NON-PUBLIC cohort-B datasets are backfilled as
``legacy_notice``. Public cohort-B datasets were handled row by row (place#158
rulings, ``licensing/data/contributed_licence_proposals.json``) and are never
touched here; neither are collections (still held).

Dry by default — pass ``--commit`` to write.
"""

import datetime

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.db.models import Count

from datasets.models import Dataset
from licensing.models import License

# The date the acceptance control went live (website@9c7a543bf). Datasets
# created on or after this were shown it and could not proceed without it.
ACCEPTANCE_LIVE_FROM = datetime.date(2024, 7, 15)

# What the control asserted.
LEGACY_SPDX = "CC-BY-4.0"


class Command(BaseCommand):
    help = "Backfill CC-BY-4.0 onto cohort-A, or non-public cohort-B, contributed datasets (place#158/#349)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--commit", action="store_true",
            help="Write the changes. Without this the command only reports.",
        )
        parser.add_argument(
            "--cohort", default="a", choices=["a", "b"],
            help="Cohort to process. 'b' covers NON-PUBLIC datasets only (place#349 C1).",
        )

    def handle(self, *args, **opts):
        try:
            licence = License.objects.get(spdx_id=LEGACY_SPDX)
        except License.DoesNotExist:
            raise CommandError(
                f"Licence '{LEGACY_SPDX}' is not in the vocabulary — load the licensing "
                f"catalogue before running this."
            )

        if opts["cohort"] == "b":
            cohort = Dataset.objects.filter(create_date__lt=ACCEPTANCE_LIVE_FROM, public=False)
            source, label = "legacy_notice", f"Cohort B, non-public (create_date < {ACCEPTANCE_LIVE_FROM})"
        else:
            cohort = Dataset.objects.filter(create_date__gte=ACCEPTANCE_LIVE_FROM)
            source, label = "legacy_acceptance", f"Cohort A (create_date >= {ACCEPTANCE_LIVE_FROM})"

        # A dataset created through the upload form always has the file it
        # validated. One without is the product of an admin, bulk-import or
        # programmatic route whose contributor never saw the acceptance control,
        # so it gets no licence — null, rather than a licence we cannot evidence.
        no_file = set(
            cohort.annotate(n=Count("files")).filter(n=0).values_list("id", flat=True)
        )
        already = set(
            cohort.filter(license__isnull=False).values_list("id", flat=True)
        )

        targets = cohort.exclude(id__in=no_file | already).order_by("id")

        self.stdout.write(f"{label}: {cohort.count()}")
        self.stdout.write(f"  excluded, no upload file:   {len(no_file)}")
        self.stdout.write(f"  excluded, licence present:  {len(already)}")
        self.stdout.write(f"  to receive {LEGACY_SPDX}:      {targets.count()}")
        self.stdout.write(f"    of which public:          {targets.filter(public=True).count()}")

        if not opts["commit"]:
            self.stdout.write(self.style.WARNING("\nDry run — nothing written. Pass --commit to apply."))
            return

        with transaction.atomic():
            updated = targets.update(license=licence, license_source=source)
        self.stdout.write(self.style.SUCCESS(f"\nUpdated {updated} datasets."))
