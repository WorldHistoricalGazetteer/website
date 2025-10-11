import json
from functools import reduce
from django.core.management.base import BaseCommand
from django.contrib.gis.db.models.aggregates import Union
from django.contrib.gis.geos import MultiPolygon as GEOSMultiPolygon, Polygon as GEOSPolygon, GEOSException, \
    GEOSGeometry
from shapely.geometry.collection import GeometryCollection
from shapely.geometry.geo import shape, mapping
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon
from shapely.ops import unary_union
from shapely.validation import make_valid

from regions.models import Region

import logging

# Disable Django SQL debug logs
logging.getLogger('django.db.backends').setLevel(logging.WARNING)


class Command(BaseCommand):
    help = "Aggregate populations and geometries hierarchically with batched PostGIS unions"

    def handle(self, *args, **options):
        processed_ids = set()
        total_regions = Region.objects.count()
        self.stdout.write(f"Total regions: {total_regions}")

        BATCH_SIZE = 50  # number of child geometries to union at once

        def clean_geom(g):
            """Return a cleaned geometry or None if unfixable."""
            if not g:
                return None
            # If it's already a Polygon or MultiPolygon object, check validity
            try:
                if not g.valid:
                    try:
                        g = g.buffer(0)
                    except GEOSException:
                        return None
                # Ensure MultiPolygon/Polygon types are preserved
                return g
            except Exception:
                return None

        def safe_batched_union_shapely(geoms):
            if not geoms:
                return None

            try:
                from shapely.validation import make_valid
            except ImportError:
                make_valid = None

            try:
                s_geoms = []
                for g in geoms:
                    if not g:
                        continue
                    gj = json.loads(g.geojson)
                    s = shape(gj)
                    if not s.is_valid and make_valid:
                        s = make_valid(s)
                    elif not s.is_valid:
                        s = s.buffer(0)
                    s_geoms.append(s)

                merged = unary_union(s_geoms)

                # 🧱 keep only polygons from a GeometryCollection
                if isinstance(merged, GeometryCollection):
                    polys = [geom for geom in merged.geoms if isinstance(geom, (Polygon, MultiPolygon))]
                    if polys:
                        merged = MultiPolygon(
                            [p for geom in polys for p in (geom.geoms if isinstance(geom, MultiPolygon) else [geom])]
                        )
                    else:
                        return None

                elif isinstance(merged, Polygon):
                    merged = MultiPolygon([merged])

                return GEOSGeometry(json.dumps(mapping(merged)))
            except Exception as e:
                print(f"⚠️ Shapely union failed ({e}); falling back to GEOS batch union.")
                merged_geom, _ = safe_batched_union(geoms)
                return merged_geom

        def safe_batched_union(geoms, batch_size=BATCH_SIZE):
            """
            Accept a list of GEOS geometries (Polygons or MultiPolygons).
            Return a single (possibly) GEOS geometry that is the union, or None.
            This function:
              - cleans each geom (buffer(0) if invalid)
              - unions in batches to control memory/time
              - if union fails on a batch, tries repairing operands and retrying
              - as final fallback, constructs a MultiPolygon from components
            """
            # Clean and filter
            cleaned = []
            cleaned_info = {"original": 0, "kept": 0, "repaired": 0, "skipped": 0}
            for g in geoms:
                cleaned_info["original"] += 1
                if g is None:
                    cleaned_info["skipped"] += 1
                    continue
                if getattr(g, "valid", True):
                    cleaned.append(g)
                    cleaned_info["kept"] += 1
                else:
                    try:
                        g2 = g.buffer(0)
                        if g2 is not None and getattr(g2, "valid", False):
                            cleaned.append(g2)
                            cleaned_info["repaired"] += 1
                        else:
                            cleaned_info["skipped"] += 1
                    except GEOSException:
                        cleaned_info["skipped"] += 1
            # if nothing left
            if not cleaned:
                return None, cleaned_info

            # Iteratively reduce by batches
            current = cleaned
            try:
                while len(current) > 1:
                    new_current = []
                    for i in range(0, len(current), batch_size):
                        batch = current[i:i + batch_size]
                        # Try normal reduce union
                        try:
                            batch_union = reduce(lambda a, b: a.union(b), batch)
                        except GEOSException:
                            # Try to repair each member and union again
                            repaired = []
                            for g in batch:
                                if getattr(g, "valid", True):
                                    repaired.append(g)
                                else:
                                    try:
                                        g2 = g.buffer(0)
                                        if g2 is not None and getattr(g2, "valid", False):
                                            repaired.append(g2)
                                    except GEOSException:
                                        # skip this geom
                                        continue
                            if not repaired:
                                # fallback: build MultiPolygon from polygon components
                                components = []
                                for gg in batch:
                                    try:
                                        if gg.geom_type == "Polygon":
                                            components.append(gg)
                                        elif gg.geom_type == "MultiPolygon":
                                            for part in gg:
                                                components.append(part)
                                    except Exception:
                                        continue
                                if components:
                                    try:
                                        batch_union = GEOSMultiPolygon(*components)
                                    except Exception:
                                        batch_union = None
                                else:
                                    batch_union = None
                            else:
                                # attempt union on repaired list, with pairwise safe joins
                                try:
                                    a = repaired[0]
                                    for b in repaired[1:]:
                                        try:
                                            a = a.union(b)
                                        except GEOSException:
                                            # try buffer(0) on both and retry
                                            try:
                                                a = a.buffer(0).union(b.buffer(0))
                                            except Exception:
                                                a = None
                                                break
                                    batch_union = a
                                except Exception:
                                    batch_union = None
                        if batch_union:
                            new_current.append(batch_union)
                        # else batch could not be unioned -> skip it
                    current = new_current
                # finished
                if current:
                    return current[0], cleaned_info
                return None, cleaned_info
            except Exception as exc:
                # Final fallback: build multipolygon from all available components
                components = []
                for gg in current:
                    try:
                        if gg.geom_type == "Polygon":
                            components.append(gg)
                        elif gg.geom_type == "MultiPolygon":
                            for part in gg:
                                components.append(part)
                    except Exception:
                        continue
                if components:
                    try:
                        return GEOSMultiPolygon(*components), cleaned_info
                    except Exception:
                        pass
                # nothing workable
                return None, cleaned_info

        # main loop unchanged until we reach geometry section
        while len(processed_ids) < total_regions:
            # Find regions where all children are already processed or have no children
            ready = []
            for region in Region.objects.exclude(id__in=processed_ids):
                child_ids = set(region.children.values_list("id", flat=True))
                member_ids = set(region.members.values_list("id", flat=True))
                # Ready if all children AND members are processed (or none exist)
                if (not child_ids or child_ids <= processed_ids) and \
                   (not member_ids or member_ids <= processed_ids):
                    ready.append(region)

            if not ready:
                raise RuntimeError("Circular parent-child relationship detected!")

            for region in ready:
                children = region.children.all()

                # --- Population ---
                pop_values = children.values_list("population", flat=True)
                date_values = []
                for child in children:
                    if child.population_date:
                        date_values.extend(child.population_date)

                region.population = sum(filter(None, pop_values)) if pop_values else None
                if date_values:
                    region.population_date = [min(date_values), max(date_values)]
                else:
                    region.population_date = []

                # --- Geometry (safe batched union) ---
                all_sources = list(children) + list(region.members.all())
                geoms = [r.geom for r in all_sources if r.geom]
                if geoms:
                    merged_geom = safe_batched_union_shapely(geoms)

                    if merged_geom:
                        if merged_geom.geom_type == "Polygon":
                            merged_geom = GEOSMultiPolygon(merged_geom)
                        region.geom = merged_geom

                        hull_geom = merged_geom.convex_hull
                        if isinstance(hull_geom, GEOSPolygon):
                            hull_geom = GEOSMultiPolygon(hull_geom)
                        region.hull = hull_geom

                region.save()
                processed_ids.add(region.id)
                # Use a safe str call to avoid label lookup problems
                try:
                    region_name = str(region)
                except Exception:
                    region_name = region.m49
                self.stdout.write(f"Aggregated {region_name} ({children.count()} children)")

        self.stdout.write(self.style.SUCCESS("✓ Hierarchical aggregation with batched unions complete."))
