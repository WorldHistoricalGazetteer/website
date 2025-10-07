import requests
from pathlib import Path
from django.core.management.base import BaseCommand

class Command(BaseCommand):
    help = "Download osm-countries-geojson into regions/data folder"

    def add_arguments(self, parser):
        parser.add_argument(
            "--outfile",
            type=str,
            help="Optional filename to save (default: regions/data/osm-countries.geojson)",
        )

    def handle(self, *args, **options):
        # Default output path
        base_dir = Path(__file__).resolve().parents[2] / "data"
        base_dir.mkdir(exist_ok=True)

        outfile = Path(options["outfile"]) if options.get("outfile") else base_dir / "osm-countries.geojson"

        # New source URL
        url = "https://osm-countries-geojson.monicz.dev/osm-countries-0-00001.geojson"
        self.stdout.write(f"Fetching {url} ...")

        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
        except requests.RequestException as e:
            self.stderr.write(f"Error fetching {url}: {e}")
            return

        with open(outfile, "wb") as f:
            f.write(r.content)

        self.stdout.write(self.style.SUCCESS(f"✓ Saved GeoJSON to {outfile}"))
