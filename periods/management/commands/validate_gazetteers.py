"""
Management command to check SpatialEntity geometries from gazetteers
for invalid or nested GeometryCollections, without updating the database.

It prints:
- Feature ID
- Gazetteer source
- Type of geometry problem
- Whether a zero-buffer fix resolves self-intersections
"""

import json
from typing import Dict, List

import requests
from django.core.management.base import BaseCommand
from django.contrib.gis.geos import GEOSGeometry


class Command(BaseCommand):
    help = "Report invalid or nested GeometryCollections in gazetteer geometries."

    GITHUB_API_BASE = "https://api.github.com/repos/periodo/periodo-places"
    GITHUB_RAW_BASE = "https://raw.githubusercontent.com/periodo/periodo-places/master/gazetteers"
    BATCH_SIZE = 500

    def add_arguments(self, parser):
        parser.add_argument(
            '--files',
            nargs='+',
            help='Process only specific files (e.g., --files geonames.json pleiades.json)',
        )

    def handle(self, *args, **options):
        gazetteer_files = self.get_gazetteer_files()
        if not gazetteer_files:
            self.stderr.write("No gazetteer files found.")
            return

        if options['files']:
            gazetteer_files = [f for f in gazetteer_files if f['name'] in options['files']]

        self.stdout.write(f"Checking {len(gazetteer_files)} gazetteer files for invalid geometries...")

        for file_info in gazetteer_files:
            self.stdout.write(f"Processing file: {file_info['name']}")
            self.process_gazetteer_file(file_info)

    def get_gazetteer_files(self) -> List[Dict]:
        """Get list of JSON files in the gazetteers directory"""
        try:
            url = f"{self.GITHUB_API_BASE}/contents/gazetteers"
            response = requests.get(url)
            response.raise_for_status()
            files = [
                {'name': item['name'], 'download_url': item['download_url']}
                for item in response.json()
                if item['type'] == 'file' and item['name'].endswith('.json')
            ]
            return files
        except requests.RequestException as e:
            self.stderr.write(f"Error fetching file list: {e}")
            return []

    def process_gazetteer_file(self, file_info: Dict):
        """Process a single gazetteer file in read-only mode"""
        filename = file_info['name']

        try:
            response = requests.get(file_info['download_url'])
            response.raise_for_status()
            gazetteer_data = response.json()
        except (requests.RequestException, json.JSONDecodeError) as e:
            self.stderr.write(f"Error downloading {filename}: {e}")
            return

        features = gazetteer_data.get('features', [])
        self.stdout.write(f"Found {len(features)} features in {filename}")

        for feature in features:
            feature_id = feature.get('id') or "<no id>"
            geometry_data = feature.get('geometry')
            if not geometry_data:
                continue

            try:
                geom = GEOSGeometry(json.dumps(geometry_data))
            except Exception as e:
                self.stdout.write(f"[ERROR] Feature {feature_id} in {filename} - cannot parse geometry: {e}")
                continue

            # Check for nested GeometryCollections
            nested_count = self.count_nested_collections(geometry_data)
            if nested_count > 0:
                self.stdout.write(
                    f"[NESTED] Feature {feature_id} in {filename} - contains {nested_count} nested GeometryCollection(s)"
                )

            # Check if GEOS geometry is invalid
            if not geom.valid:
                self.stdout.write(f"[INVALID] Feature {feature_id} in {filename} - invalid geometry detected")
                try:
                    fixed_geom = geom.buffer(0)
                    if fixed_geom.valid:
                        self.stdout.write(f"  -> Self-intersection fix via buffer(0) succeeded")
                    else:
                        self.stdout.write(f"  -> Self-intersection fix via buffer(0) FAILED")
                except Exception as e:
                    self.stdout.write(f"  -> Buffer(0) attempt raised exception: {e}")

    def count_nested_collections(self, geom: dict, level: int = 0) -> int:
        """Recursively count nested GeometryCollections beyond the top level"""
        if geom['type'] != 'GeometryCollection':
            return 0
        count = 0
        for g in geom['geometries']:
            if g['type'] == 'GeometryCollection':
                count += 1 + self.count_nested_collections(g, level + 1)
            else:
                count += self.count_nested_collections(g, level + 1)
        return count
