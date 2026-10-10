"""Record licences against legacy contributed datasets from an APPROVED list, and
nothing else (place#158, part 3).

The proposals live in ``licensing/data/contributed_licence_proposals.json``. Each
row names one dataset, the licence to record, the provenance value to record with
it, and the evidence. A row is applied only when it carries
``"approved": true`` together with ``approved_by`` and ``approved_on`` — the
Technical Director's sign-off, made in the file so that it is reviewed and
versioned like code. The file shipped as all-unapproved on 2026-10-09; every row
in it was approved on 2026-10-10, with the departures from the proposal recorded
per row under ``ruling``.

Dry by default: pass ``--commit`` to write. The command refuses outright, writing
nothing, when any approved row would

* name a dataset that does not exist, or whose ``label`` differs from the row's
  (a guard against an id that has drifted since the proposal was written);
* record a licence that is not in the vocabulary;
* carry a provenance value other than ``legacy_notice`` or ``upstream_terms``
  (the cohort-A acceptance has its own route) — with one exception:
  ``contributor_selected`` is accepted for a row of ``class: "ui"`` whose dataset
  is owned by a staff account. Those are WHG's own datasets, for which WHG is
  the contributor and the Technical Director's approval in this file *is* the
  contributor's choice; the licence picker only exists at upload, so there is no
  other route for an existing dataset. A ``contributor_selected`` row for a
  dataset owned by anyone else is refused;
* overwrite a licence already recorded — a contributor's choice is never touched,
  and a differing retrospective record is a contradiction to resolve by hand, not
  a race to win.

A row whose dataset already carries exactly the proposed licence and provenance
is reported as already applied and skipped, so a re-run is a no-op.

Writes use ``QuerySet.update`` deliberately: no ``save()`` signals fire, so the
DOI re-registration (place#158 C4) and the bbox recomputation are not triggered
as a side effect of a licence record. Datasets that have no row here keep
``license = NULL``, which the API reports honestly as no licence recorded.
"""

import datetime
import json
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from datasets.models import Dataset
from licensing.models import LICENSE_SOURCE_CHOICES, License

DEFAULT_PROPOSALS = (
    Path(__file__).resolve().parents[3] / "licensing" / "data" / "contributed_licence_proposals.json"
)

# The only provenance values a retrospective, staff-recorded licence may carry.
ALLOWED_SOURCES = ("legacy_notice", "upstream_terms")
# Plus WHG's own choice for its own data: only on a class-"ui" row, only when the
# dataset's owner is a staff account. See the module docstring.
OWN_CHOICE_SOURCE = "contributor_selected"
OWN_CHOICE_CLASS = "ui"


class Command(BaseCommand):
    help = "Apply approved licence proposals to legacy contributed datasets (place#158). Dry by default."

    def add_arguments(self, parser):
        parser.add_argument(
            "--commit", action="store_true",
            help="Write the approved rows. Without this the command only reports.",
        )
        parser.add_argument(
            "--proposals", default=str(DEFAULT_PROPOSALS),
            help="Path to the proposals JSON (default: the file shipped in licensing/data/).",
        )
        parser.add_argument(
            "--only", default="",
            help="Comma-separated dataset ids to restrict the run to. Each must be in the "
                 "proposals file; anything else is refused.",
        )

    # ------------------------------------------------------------------ load
    def _load(self, path):
        try:
            with open(path, encoding="utf-8") as fh:
                doc = json.load(fh)
        except (OSError, ValueError) as exc:
            raise CommandError(f"Cannot read proposals file {path}: {exc}")
        rows = doc.get("proposals")
        if not isinstance(rows, list):
            raise CommandError("Proposals file has no 'proposals' list.")
        seen = set()
        for row in rows:
            for key in ("dataset_id", "label", "spdx_id", "license_source"):
                if key not in row:
                    raise CommandError(f"Proposal row missing '{key}': {row}")
            if row["dataset_id"] in seen:
                raise CommandError(f"Dataset {row['dataset_id']} appears twice in the proposals.")
            seen.add(row["dataset_id"])
        return rows

    @staticmethod
    def _is_approved(row):
        return bool(row.get("approved") is True and row.get("approved_by") and row.get("approved_on"))

    # --------------------------------------------------------------- validate
    def _check(self, row):
        """Return (dataset, licence, note) for an approved row, or raise."""
        valid_sources = {k for k, _ in LICENSE_SOURCE_CHOICES}
        source = row["license_source"]
        own_choice = source == OWN_CHOICE_SOURCE and row.get("class") == OWN_CHOICE_CLASS
        if not own_choice and (source not in ALLOWED_SOURCES or source not in valid_sources):
            raise CommandError(
                f"Dataset {row['dataset_id']}: provenance '{source}' is not one this command may "
                f"record ({', '.join(ALLOWED_SOURCES)}; {OWN_CHOICE_SOURCE} only on a "
                f"class-{OWN_CHOICE_CLASS} row for a staff-owned dataset)."
            )
        try:
            licence = License.objects.get(spdx_id=row["spdx_id"])
        except License.DoesNotExist:
            raise CommandError(
                f"Dataset {row['dataset_id']}: licence '{row['spdx_id']}' is not in the vocabulary."
            )
        try:
            ds = Dataset.objects.get(id=row["dataset_id"])
        except Dataset.DoesNotExist:
            raise CommandError(f"Dataset {row['dataset_id']} does not exist.")
        if ds.label != row["label"]:
            raise CommandError(
                f"Dataset {ds.id}: label is '{ds.label}' but the proposal says '{row['label']}' — "
                f"the id may have drifted; not applying."
            )
        if own_choice and not (ds.owner.is_staff or ds.owner.is_superuser):
            raise CommandError(
                f"Dataset {ds.id}: '{OWN_CHOICE_SOURCE}' may only be recorded on WHG's own data, "
                f"but the owner is not a staff account; refusing to choose on a contributor's behalf."
            )
        if ds.license_id is not None:
            if ds.license_id == licence.id and ds.license_source == source:
                return ds, licence, "already applied"
            raise CommandError(
                f"Dataset {ds.id} already records {ds.license.spdx_id} ({ds.license_source}); "
                f"refusing to overwrite with {licence.spdx_id} ({source})."
            )
        return ds, licence, "to apply"

    # ----------------------------------------------------------------- handle
    def handle(self, *args, **opts):
        rows = self._load(opts["proposals"])
        only = {int(x) for x in opts["only"].split(",") if x.strip()}
        known = {row["dataset_id"] for row in rows}
        if only - known:
            raise CommandError(
                f"--only names datasets not in the proposals file: {sorted(only - known)}."
            )
        if only:
            rows = [row for row in rows if row["dataset_id"] in only]

        approved = [row for row in rows if self._is_approved(row)]
        unapproved = [row for row in rows if not self._is_approved(row)]

        plan = []       # (dataset, licence, source)
        already = []
        for row in approved:
            ds, licence, note = self._check(row)
            if note == "already applied":
                already.append(ds)
            else:
                plan.append((ds, licence, row["license_source"]))

        total = len(rows)
        self.stdout.write(f"Proposals considered: {total}")
        self.stdout.write(f"  not approved (skipped):        {len(unapproved)} of {total}")
        self.stdout.write(f"  approved, already applied:     {len(already)} of {total}")
        self.stdout.write(f"  approved, to apply:            {len(plan)} of {total}")
        for ds, licence, source in plan:
            self.stdout.write(f"    {ds.id:>6}  {ds.label:<22} -> {licence.spdx_id} ({source})")

        if not opts["commit"]:
            self.stdout.write(self.style.WARNING("\nDry run — nothing written. Pass --commit to apply."))
            return
        if not plan:
            self.stdout.write("\nNothing to write.")
            return

        with transaction.atomic():
            written = 0
            for ds, licence, source in plan:
                written += Dataset.objects.filter(id=ds.id, license__isnull=True).update(
                    license=licence, license_source=source,
                )
            if written != len(plan):
                # A licence appeared between the check and the write: nothing is kept.
                raise CommandError(
                    f"Planned {len(plan)} writes but {written} rows were still unlicensed; rolled back."
                )
        self.stdout.write(self.style.SUCCESS(
            f"\nUpdated {written} datasets on {datetime.date.today().isoformat()}."
        ))
