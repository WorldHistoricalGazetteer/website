# update_periodo.py
"""

Populate the periods database from the cached PeriodO JSON.

- Deletes existing tables for the periods app.
- Populates Authority, Period, SpatialEntity, Chrononym, and TemporalBound.
- Handles localizedLabels / chrononyms and spatialCoverage specially.
- Parses TemporalBound years and splits range strings into earliestYear / latestYear.

OPTIMIZED VERSION:
- Batch processing to show progress and prevent memory issues
- Pre-loading to avoid N+1 queries
- Bulk operations where possible
"""

import json
import logging
import os
import sys
from pathlib import Path

import requests
from django.core.management.base import BaseCommand
from django.db import transaction

DATASET_URL = "https://n2t.net/ark:/99152/p0dataset.json"
CACHE_FILE = Path("p0dataset.json")
BATCH_SIZE = 50  # Process authorities in batches


class Command(BaseCommand):
    help = "Update periods database from cached PeriodO JSON."

    def handle(self, *args, **options):
        logging.getLogger('django.db.backends').setLevel(logging.WARNING)

        from periods.models import (
            Authority,
            Period,
            TemporalBound,
            SpatialEntity,
            Chrononym,
        )

        # Clear existing data
        self.stdout.write("Erasing existing periods tables...")
        TemporalBound.objects.all().delete()
        Period.objects.all().delete()
        Authority.objects.all().delete()
        SpatialEntity.objects.all().delete()
        Chrononym.objects.all().delete()
        self.stdout.write("Existing tables cleared.")

        # --- Ensure dataset file exists ---
        if not CACHE_FILE.exists():
            self.stdout.write(f"Cached dataset not found. Downloading from {DATASET_URL}...")
            try:
                response = requests.get(DATASET_URL, timeout=30)
                response.raise_for_status()
                CACHE_FILE.write_bytes(response.content)
                self.stdout.write("Download complete.")
            except requests.exceptions.RequestException as e:
                self.stderr.write(f"❌ Failed to download dataset: {e}")
                sys.exit(1)

        # --- Load dataset safely ---
        try:
            with CACHE_FILE.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            self.stderr.write(f"❌ Failed to parse JSON in {CACHE_FILE}: {e}")
            sys.exit(1)
        except OSError as e:
            self.stderr.write(f"❌ Could not read {CACHE_FILE}: {e}")
            sys.exit(1)

        # --- Optional test slice of authorities ---
        TEST_SLICE = None  # Set to None to process all
        auth_items = list(data.get("authorities", {}).items())
        if TEST_SLICE:
            auth_items = auth_items[:TEST_SLICE]

        total_authorities = len(auth_items)
        self.stdout.write(f"Processing {total_authorities} authorities in batches of {BATCH_SIZE}...")

        # Global counters
        total_authorities_seen = 0
        total_periods_seen = 0
        total_bounds_seen = 0

        # Global caches for cross-batch lookups
        global_spatial_cache = {}
        global_chrononym_cache = {}

        # Process in batches
        for batch_start in range(0, len(auth_items), BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE, len(auth_items))
            batch = auth_items[batch_start:batch_end]
            batch_num = (batch_start // BATCH_SIZE) + 1
            total_batches = (len(auth_items) + BATCH_SIZE - 1) // BATCH_SIZE

            self.stdout.write(f"Processing batch {batch_num}/{total_batches} ({len(batch)} authorities)...")

            # Batch-specific counters
            batch_authorities = 0
            batch_periods = 0
            batch_bounds = 0

            # Pre-collect all spatial entities and chrononyms needed for this batch
            batch_spatial_uris = set()
            batch_chrononym_keys = set()

            for auth_key, auth_val in batch:
                for pkey, pobj in auth_val.get("periods", {}).items():
                    # Collect spatial coverage URIs
                    for sc in pobj.get("spatialCoverage", []):
                        if isinstance(sc, dict):
                            uri = sc.get("id", "")
                        elif isinstance(sc, str):
                            uri = sc
                        else:
                            continue
                        if uri:
                            batch_spatial_uris.add(uri)

                    # Collect chrononym keys
                    localized_labels = pobj.get("localizedLabels", {})
                    if isinstance(localized_labels, dict):
                        for lang_code, labels_list in localized_labels.items():
                            if isinstance(labels_list, list):
                                for label in labels_list:
                                    if label:
                                        batch_chrononym_keys.add((label, lang_code))
                            elif isinstance(labels_list, str):
                                if labels_list:
                                    batch_chrononym_keys.add((labels_list, lang_code))

            # Pre-load existing spatial entities and chrononyms for this batch
            if batch_spatial_uris:
                existing_spatials = SpatialEntity.objects.filter(uri__in=batch_spatial_uris)
                for se in existing_spatials:
                    global_spatial_cache[se.uri] = se

            if batch_chrononym_keys:
                # Build Q objects for existing chrononyms
                from django.db.models import Q
                q_objects = Q()
                for label, lang_tag in batch_chrononym_keys:
                    q_objects |= Q(label=label, languageTag=lang_tag)

                existing_chrononyms = Chrononym.objects.filter(q_objects)
                for c in existing_chrononyms:
                    global_chrononym_cache[(c.label, c.languageTag)] = c

            # Process this batch
            with transaction.atomic():
                # Collect new entities to bulk create
                new_spatial_entities = []
                new_chrononyms = []

                for auth_key, auth_val in batch:
                    batch_authorities += 1
                    total_authorities_seen += 1

                    # Create authority
                    auth = Authority(
                        id=auth_val.get("id", ""),
                        source=auth_val.get("source"),
                        type=auth_val.get("type", ""),
                        editorialNote=auth_val.get("editorialNote", ""),
                        sameAs=auth_val.get("sameAs", "")
                    )
                    auth.save()

                    for pkey, pobj in auth_val.get("periods", {}).items():
                        batch_periods += 1
                        total_periods_seen += 1

                        # Create period
                        period = Period(
                            id=pobj.get("id", ""),
                            authority=auth,
                            type=pobj.get("type", ""),
                            language=pobj.get("language", ""),
                            chrononym=pobj.get("label", ""),
                            editorialNote=pobj.get("editorialNote", ""),
                            spatialCoverageDescription=pobj.get("spatialCoverageDescription", ""),
                            languageTag=pobj.get("languageTag", ""),
                            sameAs=pobj.get("sameAs", ""),
                            url=pobj.get("url", ""),
                            note=pobj.get("note", ""),
                            broader=pobj.get("broader", ""),
                            source=pobj.get("source"),
                            narrower=pobj.get("narrower"),
                            script=pobj.get("script", ""),
                            derivedFrom=pobj.get("derivedFrom"),
                        )
                        period.save()

                        # --- Spatial Coverage ---
                        for sc in pobj.get("spatialCoverage", []):
                            if isinstance(sc, dict):
                                uri = sc.get("id", "")
                                label = sc.get("label", "")
                            elif isinstance(sc, str):
                                uri = sc
                                label = ""
                            else:
                                continue
                            if not uri:
                                continue

                            # Check if we need to create this spatial entity
                            if uri not in global_spatial_cache:
                                new_entity = SpatialEntity(uri=uri, label=label)
                                global_spatial_cache[uri] = new_entity
                                new_spatial_entities.append(new_entity)

                        # --- Localized Labels / Chrononyms ---
                        localized_labels = pobj.get("localizedLabels", {})
                        if isinstance(localized_labels, dict):
                            for lang_code, labels_list in localized_labels.items():
                                if isinstance(labels_list, list):
                                    for label in labels_list:
                                        if label:
                                            key = (label, lang_code)
                                            if key not in global_chrononym_cache:
                                                new_chrononym = Chrononym(label=label, languageTag=lang_code)
                                                global_chrononym_cache[key] = new_chrononym
                                                new_chrononyms.append(new_chrononym)
                                elif isinstance(labels_list, str):
                                    if labels_list:
                                        key = (labels_list, lang_code)
                                        if key not in global_chrononym_cache:
                                            new_chrononym = Chrononym(label=labels_list, languageTag=lang_code)
                                            global_chrononym_cache[key] = new_chrononym
                                            new_chrononyms.append(new_chrononym)

                        # --- Temporal Bounds ---
                        for kind in ("start", "stop"):
                            bound_obj = pobj.get(kind)
                            if not isinstance(bound_obj, dict):
                                continue

                            year_raw = None
                            earliest = latest = None

                            # 1. Check for explicit 'year' field
                            if "year" in bound_obj:
                                year_raw = bound_obj["year"]

                                if isinstance(year_raw, str) and "-" in year_raw[1:]:
                                    # Range string like "0040-0050"
                                    parts = year_raw.split("-")
                                    try:
                                        earliest = int(parts[0])
                                        latest = int(parts[1])
                                    except Exception:
                                        pass
                                else:
                                    try:
                                        if isinstance(year_raw, str):
                                            s = year_raw.strip()
                                            earliest = latest = int(s)
                                        elif isinstance(year_raw, int):
                                            earliest = latest = year_raw
                                    except Exception:
                                        pass

                            # 2. Check for 'in' object with earliest/latest
                            elif "in" in bound_obj and isinstance(bound_obj["in"], dict):
                                year_in = bound_obj["in"]
                                year_raw = year_in
                                earliest = year_in.get("earliestYear")
                                latest = year_in.get("latestYear")

                                # If only 'year' is provided inside 'in', treat as single-year bound
                                if earliest is None and latest is None and "year" in year_in:
                                    try:
                                        y = year_in["year"]
                                        earliest = latest = int(y)
                                    except Exception:
                                        pass

                            tb = TemporalBound(
                                period=period,
                                kind=kind,
                                year=year_raw,
                                label=bound_obj.get("label", ""),
                                earliestYear=earliest,
                                latestYear=latest
                            )
                            tb.save()
                            batch_bounds += 1
                            total_bounds_seen += 1

                # Bulk create new entities
                if new_spatial_entities:
                    SpatialEntity.objects.bulk_create(new_spatial_entities, ignore_conflicts=True)
                    # Reload the new spatial entities from DB
                    for entity in new_spatial_entities:
                        try:
                            global_spatial_cache[entity.uri] = SpatialEntity.objects.get(uri=entity.uri)
                        except SpatialEntity.DoesNotExist:
                            pass

                if new_chrononyms:
                    Chrononym.objects.bulk_create(new_chrononyms, ignore_conflicts=True)
                    # Reload the new chrononyms from DB
                    for chrononym in new_chrononyms:
                        try:
                            key = (chrononym.label, chrononym.languageTag)
                            global_chrononym_cache[key] = Chrononym.objects.get(
                                label=chrononym.label,
                                languageTag=chrononym.languageTag
                            )
                        except Chrononym.DoesNotExist:
                            pass

                # Now set up the many-to-many relationships
                for auth_key, auth_val in batch:
                    auth_id = auth_val.get("id", "")
                    auth = Authority.objects.get(id=auth_id)

                    for pkey, pobj in auth_val.get("periods", {}).items():
                        period_id = pobj.get("id", "")
                        try:
                            period = Period.objects.get(id=period_id)
                        except Period.DoesNotExist:
                            continue

                        # Set spatial coverage
                        spatial_entities = []
                        for sc in pobj.get("spatialCoverage", []):
                            if isinstance(sc, dict):
                                uri = sc.get("id", "")
                            elif isinstance(sc, str):
                                uri = sc
                            else:
                                continue
                            if uri and uri in global_spatial_cache:
                                spatial_entities.append(global_spatial_cache[uri])

                        if spatial_entities:
                            period.spatialCoverage.set(spatial_entities)

                        # Set chrononyms
                        chrononyms = []
                        localized_labels = pobj.get("localizedLabels", {})
                        if isinstance(localized_labels, dict):
                            for lang_code, labels_list in localized_labels.items():
                                if isinstance(labels_list, list):
                                    for label in labels_list:
                                        if label:
                                            key = (label, lang_code)
                                            if key in global_chrononym_cache:
                                                chrononyms.append(global_chrononym_cache[key])
                                elif isinstance(labels_list, str):
                                    if labels_list:
                                        key = (labels_list, lang_code)
                                        if key in global_chrononym_cache:
                                            chrononyms.append(global_chrononym_cache[key])

                        if chrononyms:
                            period.chrononyms.set(chrononyms)

            # Report batch progress
            self.stdout.write(
                f"Batch {batch_num} complete: {batch_authorities} authorities, "
                f"{batch_periods} periods, {batch_bounds} bounds"
            )

        self.stdout.write(
            self.style.SUCCESS(
                f"Finished. Total - Authorities: {total_authorities_seen}, "
                f"Periods: {total_periods_seen}, TemporalBounds: {total_bounds_seen}, "
                f"SpatialEntities: {len(global_spatial_cache)}, Chrononyms: {len(global_chrononym_cache)}"
            )
        )