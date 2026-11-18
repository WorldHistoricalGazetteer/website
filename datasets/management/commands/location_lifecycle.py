"""Retract adopted locations the place#343 rule no longer allows, or restore a retracted one.

    manage.py location_lifecycle --dry-run              # every dataset with adopted locations
    manage.py location_lifecycle --dataset 1786 --dry-run
    manage.py location_lifecycle --dataset 1786         # retract what the dry run listed
    manage.py location_lifecycle --restore 12 [--dry-run]

An adopted location (place#350) may stay on its record only while a "Same place" Link to the
place it came from stands (or the owner kept it when undoing the match) and the source's terms
still allow it (``datasets/location_lifecycle.py``). The ``post_save`` hook keeps that true for
every Link saved from now on; this command finds rows left over from before the hook, or from a
bulk ``update()`` that fired no signal, and the QC report flags the same rows. Idempotent: a
second run finds nothing. Retracted rows are archived in ``retracted_location``, never lost.
"""
from django.core.management.base import BaseCommand, CommandError

from datasets import location_lifecycle as LL


class Command(BaseCommand):
    help = "Retract adopted locations whose Link no longer stands or whose source terms changed (place#343)."

    def add_arguments(self, parser):
        parser.add_argument("--dataset", type=int, action="append", help="dataset id (repeatable); default all")
        parser.add_argument("--dry-run", action="store_true", help="report only, change nothing")
        parser.add_argument("--restore", type=int, help="restore one retracted_location row by id")

    def handle(self, *args, **opts):
        from datasets.models import Dataset
        from datasets.models_matching import RetractedLocation
        dry = opts["dry_run"]
        if opts.get("restore"):
            r = RetractedLocation.objects.filter(pk=opts["restore"]).first()
            if r is None:
                raise CommandError(f"no retracted location {opts['restore']}")
            if dry:
                self.stdout.write(f"would restore retracted location {r.pk} (record {r.place_id}, "
                                  f"dataset {r.dataset_id}, reason {r.reason!r})")
                return
            try:
                g = LL.restore(r)
            except LL.RestoreRefused as exc:
                raise CommandError(f"refused: {exc}")
            self.stdout.write(f"restored retracted location {r.pk} as place_geom {g.pk}")
            return
        if opts.get("dataset"):
            datasets = list(Dataset.objects.filter(pk__in=opts["dataset"]))
        else:
            labels = LL._adopted_qs().values_list("place__dataset", flat=True).distinct()
            datasets = list(Dataset.objects.filter(label__in=labels))
        total = 0
        for ds in sorted(datasets, key=lambda d: d.pk):
            found = LL.enforce(ds, dry_run=dry)
            for g, why in found:
                self.stdout.write(f"dataset {ds.pk} record {g.place_id} place_geom {g.pk}: {why}")
            total += len(found)
        verb = "to retract" if dry else "retracted"
        self.stdout.write(f"{len(datasets)} dataset(s) checked; {total} adopted location(s) {verb}"
                          + (" (dry run: nothing changed)" if dry else ""))
