"""

Management command to extend SpatialEntity with geospatial data from GitHub gazetteers.

- Downloads gazetteer files from periodo-places GitHub repository
- Tracks GitHub commit hashes to avoid reprocessing unchanged files
- Processes GeoJSON features and matches them to existing SpatialEntity records by URI
- Stores PostGIS geometry and bounding box polygon for efficient spatial queries
- Ensures all geometries are stored as Polygon or MultiPolygon (no GeometryCollections)
"""

import json
import os
from typing import Dict, List, Optional, Tuple

import requests
from django.contrib.gis.geos import GEOSGeometry, MultiPolygon
from django.contrib.gis.geos import Polygon
from django.core.management.base import BaseCommand
from django.db import transaction
from django.conf import settings


class Command(BaseCommand):
    help = "Update SpatialEntity with geospatial data from periodo-places gazetteers."

    GITHUB_API_BASE = "https://api.github.com/repos/periodo/periodo-places"
    GITHUB_RAW_BASE = "https://raw.githubusercontent.com/periodo/periodo-places/master/gazetteers"
    BATCH_SIZE = 500  # Process features in batches
    country_geometries: Optional[List[Tuple[str, GEOSGeometry]]] = None

    def add_arguments(self, parser):
        parser.add_argument(
            '--force',
            action='store_true',
            help='Force reprocessing of all files regardless of commit hash',
        )
        parser.add_argument(
            '--files',
            nargs='+',
            help='Process only specific files (e.g., --files geonames.json pleiades.json)',
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            default=500,
            help='Number of features to process per batch (default: 500)',
        )
        parser.add_argument(
            '--debug',
            action='store_true',
            help='Print detailed debugging information about geometry types',
        )

    def handle(self, *args, **options):
        self.debug = options.get('debug', False)
        self.BATCH_SIZE = options['batch_size']
        self.stdout.write("Starting spatial entities update...")

        # # Load country geometries
        # self.stdout.write("Loading country geometries...")
        # if not self.load_country_geometries():
        #     self.stderr.write(self.style.ERROR("Failed to load country geometries. Continuing without ccodes."))
        #     pass
        #
        # # Get list of gazetteer files
        # gazetteer_files = self.get_gazetteer_files()
        # if not gazetteer_files:
        #     self.stderr.write("No gazetteer files found.")
        #     return
        #
        # # Filter files if specific ones requested
        # if options['files']:
        #     gazetteer_files = [f for f in gazetteer_files if f['name'] in options['files']]
        #
        # self.stdout.write(f"Found {len(gazetteer_files)} gazetteer files.")
        #
        # # Process each gazetteer file
        # total_updated = 0
        # for i, file_info in enumerate(gazetteer_files, 1):
        #     self.stdout.write(f"Processing file {i}/{len(gazetteer_files)}: {file_info['name']}")
        #     updated = self.process_gazetteer_file(file_info, options['force'])
        #     total_updated += updated
        #
        # self.stdout.write(
        #     self.style.SUCCESS(f"Successfully updated {total_updated} spatial entities.")
        # )

        # Now compute bounding boxes for periods
        self.compute_period_bboxes()

    def load_country_geometries(self) -> bool:
        """Loads country geometries from the GeoJSON file into a list of (ISO code, GEOSGeometry) tuples."""
        # Note: The path is provided as 'media/media/data/countries_simplified.json'
        # which is assumed to be relative to Django's MEDIA_ROOT.
        country_file_path = os.path.join(settings.MEDIA_ROOT, 'media', 'data', 'countries_simplified.json')

        try:
            with open(country_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            self.country_geometries = []

            for feature in data.get('features', []):
                properties = feature.get('properties', {})
                iso_code = properties.get('iso')
                geometry_data = feature.get('geometry')

                if iso_code and geometry_data:
                    geom = GEOSGeometry(json.dumps(geometry_data))
                    self.country_geometries.append((iso_code, geom))

            self.stdout.write(f"Loaded {len(self.country_geometries)} country geometries.")
            return True

        except Exception as e:
            self.stderr.write(f"Error loading country geometries from {country_file_path}: {e}")
            self.country_geometries = []
            return False

    def get_gazetteer_files(self) -> List[Dict]:
        """Get list of JSON files in the gazetteers directory"""
        try:
            url = f"{self.GITHUB_API_BASE}/contents/gazetteers"
            response = requests.get(url)
            response.raise_for_status()

            files = []
            for item in response.json():
                if item['type'] == 'file' and item['name'].endswith('.json'):
                    files.append({
                        'name': item['name'],
                        'sha': item['sha'],
                        'download_url': item['download_url']
                    })
            return files

        except requests.RequestException as e:
            self.stderr.write(f"Error fetching file list: {e}")
            return []

    def process_gazetteer_file(self, file_info: Dict, force: bool = False) -> int:
        """Process a single gazetteer file"""
        self.force = force  # Store force flag for use in process_feature
        """Process a single gazetteer file"""
        filename = file_info['name']
        current_sha = file_info['sha']

        # Check if we need to process this file
        try:
            from periods.models import GazetteerTracker
            tracker = GazetteerTracker.objects.get(filename=filename)
            if not force and tracker.commit_hash == current_sha:
                self.stdout.write(f"Skipping {filename} (no changes)")
                return 0
        except GazetteerTracker.DoesNotExist:
            tracker = GazetteerTracker(filename=filename)

        self.stdout.write(f"Downloading {filename}...")

        # Download and parse the gazetteer
        try:
            response = requests.get(file_info['download_url'])
            response.raise_for_status()
            gazetteer_data = response.json()
        except (requests.RequestException, json.JSONDecodeError) as e:
            self.stderr.write(f"Error downloading {filename}: {e}")
            return 0

        # Get features and process in batches
        features = gazetteer_data.get('features', [])
        total_features = len(features)

        if total_features == 0:
            self.stdout.write(f"No features found in {filename}")
            return 0

        self.stdout.write(f"Processing {total_features} features from {filename} in batches of {self.BATCH_SIZE}...")

        total_updated = 0

        # Pre-load existing SpatialEntities for efficiency
        feature_ids = [f.get('id') for f in features if f.get('id')]
        from periods.models import SpatialEntity
        existing_entities = {se.uri: se for se in SpatialEntity.objects.filter(uri__in=feature_ids)}
        self.stdout.write(f"Found {len(existing_entities)} matching spatial entities")

        # Process in batches
        for batch_start in range(0, total_features, self.BATCH_SIZE):
            batch_end = min(batch_start + self.BATCH_SIZE, total_features)
            batch = features[batch_start:batch_end]
            batch_num = (batch_start // self.BATCH_SIZE) + 1
            total_batches = (total_features + self.BATCH_SIZE - 1) // self.BATCH_SIZE

            self.stdout.write(f"Processing batch {batch_num}/{total_batches} ({len(batch)} features)...")

            batch_updated = 0
            batch_skipped = 0
            batch_errors = 0

            with transaction.atomic():
                for feature in batch:
                    result = self.process_feature(feature, filename, existing_entities)
                    if result == 'updated':
                        batch_updated += 1
                    elif result == 'skipped':
                        batch_skipped += 1
                    else:
                        batch_errors += 1

                # Update tracker after successful batch
                tracker.commit_hash = current_sha
                tracker.save()

            total_updated += batch_updated

            self.stdout.write(
                f"Batch {batch_num} complete: {batch_updated} updated, "
                f"{batch_skipped} skipped, {batch_errors} errors"
            )

        self.stdout.write(f"Finished {filename}: {total_updated} entities updated")
        return total_updated

    def extract_and_validate_polygons(self, geometry: GEOSGeometry) -> Optional[MultiPolygon]:
        """
        Extract all polygons from any geometry type, validate them, and return as a clean MultiPolygon.
        Overlapping polygons will be merged (dissolved).
        """
        polygons = []

        def extract_polygons(geom):
            # Check for Points and Lines even though input is claimed not to have them,
            # for robustness against unexpected data.
            if geom.geom_type == 'Polygon':
                # Attempt to repair invalid polygons by buffering by zero
                if not geom.valid:
                    geom = geom.buffer(0)
                if geom and geom.valid:
                    polygons.append(geom)
            elif geom.geom_type == 'MultiPolygon':
                for poly in geom:
                    extract_polygons(poly)
            elif geom.geom_type == 'GeometryCollection':
                for sub in geom:
                    extract_polygons(sub)
            # Lines and Points are not appended, so they are filtered out implicitly
            # The 'else' block from the original code is no longer needed here as the goal
            # is just to extract Polygons.

        extract_polygons(geometry)

        if not polygons:
            return None

        # Use the GEOS union operator for merging (dissolving) the polygons.
        # Starting with the first polygon.
        try:
            merged = polygons[0]
            for poly in polygons[1:]:
                # Use the GEOS .union() method for merging
                merged = merged.union(poly)
        except Exception as e:
            if self.debug:
                self.stderr.write(f"Union failed: {e}")
            return None

        # Check the result type explicitly to ensure only Polygon or MultiPolygon is returned
        # and GeometryCollection is never stored.
        merged_type = merged.geom_type

        if merged_type == "Polygon":
            # A single Polygon result is wrapped into a MultiPolygon
            return MultiPolygon([merged])
        elif merged_type == "MultiPolygon":
            return merged
        elif merged_type == "GeometryCollection":
            # This is the explicit fix for the reported issue: if union returns a GC,
            # we reject it and return None, as we only want Polygons/MultiPolygons.
            if self.debug:
                self.stderr.write("Union returned a GeometryCollection. Rejecting.")
            return None
        else:
            # Handle other unexpected types like Point, LineString, etc. (though unlikely after union of Polygons)
            if self.debug:
                self.stderr.write(f"Unexpected result type from union: {merged_type}")
            return None

    def process_feature(self, feature: Dict, source_filename: str, existing_entities: Dict) -> str:
        """
        Process a single GeoJSON feature
        Returns: 'updated', 'skipped', or 'error'
        """
        try:
            # Extract feature ID - this should match SpatialEntity.uri
            feature_id = feature.get('id')
            if not feature_id:
                return 'skipped'

            # Check if we have a matching SpatialEntity
            spatial_entity = existing_entities.get(feature_id)
            if not spatial_entity:
                return 'skipped'

            # Skip if already processed from this source (unless forcing)
            if not self.force and spatial_entity.gazetteer_source == source_filename and spatial_entity.geometry:
                return 'skipped'

            # Extract geometry
            geometry_data = feature.get('geometry')
            if not geometry_data:
                return 'skipped'

            # Convert to Django GEOSGeometry
            try:
                raw_geometry = GEOSGeometry(json.dumps(geometry_data))

                if self.debug:
                    self.stdout.write(f"Feature {feature_id}: Raw geometry type = {raw_geometry.geom_type}")

                # Extract, validate, and convert to clean MultiPolygon
                clean_geometry = self.extract_and_validate_polygons(raw_geometry)

                if not clean_geometry:
                    self.stderr.write(f"No valid polygon geometries found for {feature_id}")
                    return 'skipped'

                if self.debug:
                    self.stdout.write(
                        f"Feature {feature_id}: Clean geometry type = {clean_geometry.geom_type}, num polygons = {len(clean_geometry)}")

                # Final validation check
                if not clean_geometry.valid:
                    self.stderr.write(f"Geometry still invalid for {feature_id} after processing")
                    return 'error'

                # Set the geometry
                spatial_entity.geometry = clean_geometry
                spatial_entity.gazetteer_source = source_filename

            except Exception as e:
                self.stderr.write(f"Error processing geometry for {feature_id}: {e}")
                return 'error'

            # Determine intersecting country codes if country geometries are loaded
            if self.country_geometries is not None:
                intersecting_ccodes = []

                # Check for intersection only if we have country geometries loaded
                for iso_code, country_geom in self.country_geometries:
                    if clean_geometry.intersects(country_geom):
                        intersecting_ccodes.append(iso_code)

                # Store the *list* of codes directly to the ArrayField
                spatial_entity.ccodes = sorted(intersecting_ccodes) # Store the sorted list

                if self.debug:
                    self.stdout.write(f"Feature {feature_id}: Intersects with ccodes: {spatial_entity.ccodes or 'None'}")

            # Calculate and store bounding box as polygon
            bbox_polygon = self.create_bbox_polygon(clean_geometry)
            if bbox_polygon:
                spatial_entity.bbox = bbox_polygon

            # Update label if available and not already set
            properties = feature.get('properties', {})
            if properties.get('name') and not spatial_entity.label:
                spatial_entity.label = properties['name']

            spatial_entity.save()

            # Verify what was actually saved
            if self.debug:
                spatial_entity.refresh_from_db()
                self.stdout.write(
                    f"Feature {feature_id}: Saved geometry type = {spatial_entity.geometry.geom_type if spatial_entity.geometry else 'None'}")

            return 'updated'

        except Exception as e:
            self.stderr.write(f"Error processing feature {feature.get('id', 'unknown')}: {e}")
            return 'error'

    def create_bbox_polygon(self, geometry: GEOSGeometry) -> Optional[Polygon]:
        """Create a bounding box polygon from geometry extent"""
        try:
            extent = geometry.extent  # Returns (xmin, ymin, xmax, ymax)

            # Create a polygon from the bounding box coordinates
            bbox_coords = [
                (extent[0], extent[1]),  # southwest
                (extent[0], extent[3]),  # northwest
                (extent[2], extent[3]),  # northeast
                (extent[2], extent[1]),  # southeast
                (extent[0], extent[1])  # close the ring
            ]

            return Polygon(bbox_coords)

        except Exception as e:
            self.stderr.write(f"Error creating bbox polygon: {e}")
            return None

    def compute_period_bboxes(self):
        """Compute bounding boxes for periods based on their spatial entities"""
        self.stdout.write("Computing bounding boxes for periods...")

        from periods.models import Period

        # Fetch all periods, not just those with geometry
        all_periods = Period.objects.prefetch_related('bounds').all()
        total_periods = all_periods.count()
        self.stdout.write(f"Processing {total_periods} periods...")

        batch_size = 100
        updated_periods = 0

        for batch_start in range(0, total_periods, batch_size):
            batch_end = min(batch_start + batch_size, total_periods)
            batch_periods = all_periods[batch_start:batch_end]
            batch_num = (batch_start // batch_size) + 1
            total_batches = (total_periods + batch_size - 1) // batch_size

            self.stdout.write(f"Processing period batch {batch_num}/{total_batches}...")
            batch_updated = 0

            with transaction.atomic():
                for period in batch_periods:

                    # Aggregate ccodes from related SpatialEntities (if any)
                    spatial_entities = period.spatialCoverage.filter(ccodes__len__gt=0)
                    all_ccodes = set()
                    for entity in spatial_entities:
                        if entity.ccodes:
                            all_ccodes.update(entity.ccodes)
                    period.ccodes = sorted(list(all_ccodes))

                    update_fields = ['ccodes']

                    # Then process geometry if it exists
                    spatial_entities_with_geom = period.spatialCoverage.filter(geometry__isnull=False)
                    clean_multipolygons = []
                    for entity in spatial_entities_with_geom:
                        geom = entity.geometry
                        if geom:
                            clean_mp = self.extract_and_validate_polygons(geom)
                            if clean_mp:
                                clean_multipolygons.append(clean_mp)

                    if clean_multipolygons:
                        try:
                            combined_geom = clean_multipolygons[0]
                            for mp in clean_multipolygons[1:]:
                                combined_geom = combined_geom.union(mp)
                            combined_geom = self.extract_and_validate_polygons(combined_geom)
                            if combined_geom:
                                period.bbox = self.create_bbox_polygon(combined_geom)
                                update_fields.append('bbox')
                        except Exception as e:
                            self.stderr.write(f"Warning: Could not union geometries for period {period.id}: {e}")

                    period.save(update_fields=update_fields)
                    batch_updated += 1

            updated_periods += batch_updated
            self.stdout.write(f"Period batch {batch_num} complete: {batch_updated} periods updated")

        self.stdout.write(self.style.SUCCESS(f"Successfully processed {updated_periods} periods."))
