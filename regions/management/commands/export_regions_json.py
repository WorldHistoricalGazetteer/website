import json
from django.core.management.base import BaseCommand
from regions.models import Region

class Command(BaseCommand):
    help = "Export regions and countries to a structured JSON file"

    def add_arguments(self, parser):
        parser.add_argument(
            "--output",
            default="media/data/regions_countries.json",
            help=f"Output file path (default: media/data/regions_countries.json)",
        )

    def handle(self, *args, **options):
        output_file = options["output"]

        # --- Regions (everything except 'country' and 'global')
        region_objs = (
            Region.objects
            .exclude(level__in=["country", "global"])
            .prefetch_related("labels", "children")
        )

        # Sort by English label text, fallback to m49 if missing
        region_objs = sorted(
            region_objs,
            key=lambda r: (r.labels.filter(lang="en").first().name if r.labels.filter(lang="en").exists() else r.m49)
        )

        regions_data = []
        for region in region_objs:
            label_en = region.labels.filter(lang="en").first()
            text = label_en.name if label_en else f"({region.m49})"

            # Gather constituent countries (direct children with level 'country')
            child_countries = (
                region.children
                .filter(level="country")
                .exclude(iso_alpha2=[])
                .order_by("m49")
            )

            ccodes = sorted({code for c in child_countries for code in c.iso_alpha2})
            regions_data.append({
                "id": int(region.m49) if region.m49.isdigit() else region.m49,
                "text": text,
                "ccodes": ccodes,
            })

        # --- Countries
        country_objs = Region.objects.filter(level="country").prefetch_related("labels")

        # Sort by English label text, fallback to m49 if missing
        country_objs = sorted(
            country_objs,
            key=lambda r: (r.labels.filter(lang="en").first().name if r.labels.filter(lang="en").exists() else r.m49)
        )

        countries_data = []
        for country in country_objs:
            label_en = country.labels.filter(lang="en").first()
            text = label_en.name if label_en else f"({country.m49})"

            # Optionally append non-Latin name in parentheses if exists
            # alt_names = [
            #     l.name for l in country.labels.exclude(lang="en")
            #     if l.name != text
            # ]
            # if alt_names:
            #     text += f" ({alt_names[0]})"

            id_val = country.iso_alpha2[0] if country.iso_alpha2 else country.m49
            countries_data.append({
                "id": id_val,
                "text": text,
            })

        # --- Final structure
        data = [
            {"text": "Regions", "children": regions_data},
            {"text": "Countries", "children": countries_data},
        ]

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        self.stdout.write(self.style.SUCCESS(f"✅ Exported to {output_file}"))
