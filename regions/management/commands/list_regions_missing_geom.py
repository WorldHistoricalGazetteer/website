import logging

from django.core.management.base import BaseCommand

from regions.models import Region

# Disable Django SQL debug logs
logging.getLogger('django.db.backends').setLevel(logging.WARNING)


class Command(BaseCommand):
    help = "List all Region objects lacking geometry (geom or hull), with identifying information."

    def add_arguments(self, parser):
        parser.add_argument(
            "--missing-hull",
            action="store_true",
            help="Include regions missing convex hulls as well as geometry.",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Optionally limit the number of results printed.",
        )

    def handle(self, *args, **options):
        include_hull = options["missing_hull"]
        limit = options["limit"]

        # Base queryset: missing geom
        missing_qs = Region.objects.filter(geom__isnull=True)
        if include_hull:
            missing_qs = missing_qs | Region.objects.filter(hull__isnull=True)

        missing_qs = missing_qs.distinct().order_by("m49")

        total_missing = missing_qs.count()
        self.stdout.write(f"Total regions missing geometry: {total_missing}")

        if limit:
            missing_qs = missing_qs[:limit]

        if not missing_qs.exists():
            self.stdout.write(self.style.SUCCESS("✓ All regions have geometries."))
            return

        for region in missing_qs:
            # English label (using model’s __str__)
            label_en = str(region)

            # ISO codes
            iso2 = ", ".join(region.iso_alpha2) if region.iso_alpha2 else "-"
            iso3 = ", ".join(region.iso_alpha3) if region.iso_alpha3 else "-"

            # Parent info
            parent_ids = list(region.parents.values_list("m49", flat=True))
            parent_str = ", ".join(parent_ids) if parent_ids else "-"

            self.stdout.write(
                f"- {label_en}\n"
                f"    M49: {region.m49} | Level: {region.level}\n"
                f"    ISO2: {iso2} | ISO3: {iso3}\n"
                f"    Parents: {parent_str}\n"
                f"    Geom: {'❌' if region.geom is None else '✅'} | "
                f"Hull: {'❌' if region.hull is None else '✅'}\n"
            )

        self.stdout.write(self.style.WARNING("⚠ Listed all regions missing geometry."))
