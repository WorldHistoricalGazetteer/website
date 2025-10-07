import json
from datetime import datetime
from pathlib import Path

import io
import requests
import zipfile
from django.contrib.gis.geos import GEOSGeometry, MultiPolygon as GEOSMultiPolygon, Polygon as GEOSPolygon, \
    GEOSException
from django.core.management.base import BaseCommand
from shapely.geometry import shape, MultiPolygon, Polygon, mapping

from regions.models import Region, RegionLabel

import logging

# Disable Django SQL debug logs
logging.getLogger('django.db.backends').setLevel(logging.WARNING)


def parse_safe_geometry(geom_data):
    """
    Safely parse a GeoJSON geometry into a valid GEOSGeometry.
    Fixes common topology errors by buffering and enforcing MultiPolygon type.
    """
    if not geom_data:
        return None

    try:
        geom = GEOSGeometry(json.dumps(geom_data))
    except (ValueError, GEOSException) as e:
        print(f"⚠️  Failed to parse geometry: {e}")
        return None

    # Attempt to repair invalid geometries
    if not geom.valid:
        try:
            geom = geom.buffer(0)
        except GEOSException as e:
            print(f"⚠️  Could not repair invalid geometry: {e}")
            return None

    # Ensure MultiPolygon type
    if isinstance(geom, Polygon):
        geom = MultiPolygon(geom)
    elif geom.geom_type == "GeometryCollection":
        # Filter polygons and make a MultiPolygon
        polys = [g for g in geom if g.geom_type in ("Polygon", "MultiPolygon")]
        if polys:
            geom = MultiPolygon(*polys)
        else:
            return None

    return geom


def fetch_natural_earth_geojson():
    """
    Download Natural Earth 10m countries GeoJSON.
    Returns the parsed GeoJSON data or None if failed.
    """
    url = "https://github.com/nvkelso/natural-earth-vector/raw/master/geojson/ne_10m_admin_0_countries.geojson"
    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        return json.loads(resp.text)
    except Exception as e:
        print(f"⚠️  Failed to fetch Natural Earth data: {e}")
        return None


class Command(BaseCommand):
    help = "Import country geometries and extended labels from osm-countries-geojson, filling missing countries from GADM"

    def add_arguments(self, parser):
        parser.add_argument(
            "--geojson_file",
            type=str,
            help="Path to the osm-countries-geojson file",
        )

    def handle(self, *args, **options):
        base_dir = Path(__file__).resolve().parents[2] / "data"
        geojson_file = Path(options["geojson_file"]) if options.get(
            "geojson_file") else base_dir / "osm-countries.geojson"
        if not geojson_file.exists():
            self.stderr.write(f"{geojson_file} does not exist")
            return

        self.stdout.write(f"Loading {geojson_file} ...")
        with open(geojson_file, encoding="utf-8") as f:
            data = json.load(f)

        features = data.get("features", [])
        self.stdout.write(f"Processing {len(features)} features ...")

        regions_to_create = []
        labels_to_create = []
        errors = []

        # --- only countries ---
        existing_countries = {
            r.m49: r for r in Region.objects.filter(level="country")
        }

        processed_m49 = set()

        # Process main GeoJSON
        for feat in features:
            props = feat.get("properties", {})
            tags = props.get("tags", {})
            iso2 = tags.get("ISO3166-1:alpha2")
            iso3 = tags.get("ISO3166-1:alpha3")
            m49 = tags.get("ISO3166-1:numeric")

            if not iso2 or not iso3 or not m49:
                continue

            processed_m49.add(m49)

            # --- geometry ---
            geom_data = feat.get("geometry")
            geom = None
            hull_geom = None
            if geom_data:
                geom = parse_safe_geometry(geom_data)
                hull_geom = geom.convex_hull if geom else None
                if isinstance(hull_geom, Polygon):
                    hull_geom = MultiPolygon(hull_geom)

            # --- population date ---
            population_date_raw = tags.get("population:date")
            if population_date_raw:
                try:
                    population_date = [datetime.strptime(population_date_raw, "%Y-%m-%d").date()]
                except ValueError:
                    try:
                        population_date = [datetime.strptime(population_date_raw, "%Y").date()]
                    except ValueError:
                        population_date = []
            else:
                population_date = []

            region_defaults = {
                "level": "country",
                "iso_alpha2": [iso2],
                "iso_alpha3": [iso3],
                "geom": geom,
                "hull": hull_geom,
                "flag_url": tags.get("flag"),
                "wikidata": tags.get("wikidata"),
                "wikipedia": tags.get("wikipedia"),
            }

            if m49 in existing_countries:
                # Update existing
                Region.objects.filter(m49=m49).update(**region_defaults)
                region = existing_countries[m49]
            else:
                region = Region(m49=m49, **region_defaults)
                regions_to_create.append(region)
                existing_countries[m49] = region

            # --- labels ---
            for k, v in tags.items():
                if not v or not k.startswith("name:"):
                    continue
                name_key = k[5:]
                if ":" in name_key:
                    qualifier, lang_variant = name_key.rsplit(":", 1)
                else:
                    qualifier = ""
                    lang_variant = name_key
                if "-" in lang_variant:
                    lang, variant = lang_variant.split("-", 1)
                else:
                    lang, variant = lang_variant, ""
                if not (2 <= len(lang) <= 3):
                    continue
                labels_to_create.append(
                    RegionLabel(
                        region=region,
                        lang=lang,
                        variant=variant,
                        qualifier=qualifier,
                        name=v
                    )
                )

        if regions_to_create:
            Region.objects.bulk_create(regions_to_create)

        # Bulk create labels in chunks
        chunk_size = 500
        for i in range(0, len(labels_to_create), chunk_size):
            RegionLabel.objects.bulk_create(labels_to_create[i:i + chunk_size], ignore_conflicts=True)

        # --- Handle missing countries using GADM ---
        missing_countries = [r for m49, r in existing_countries.items() if
                             r.level == "country" and m49 not in processed_m49]

        if missing_countries:
            self.stdout.write(f"\nProcessing {len(missing_countries)} missing countries ...")

            # Try Natural Earth first for specific territories
            natural_earth_targets = {"HK", "MO"}  # ISO2 codes for Hong Kong and Macao
            natural_earth_missing = [r for r in missing_countries
                                    if r.iso_alpha2 and r.iso_alpha2[0] in natural_earth_targets]

            if natural_earth_missing:
                self.stdout.write(f"Fetching {len(natural_earth_missing)} territories from Natural Earth ...")
                ne_data = fetch_natural_earth_geojson()

                if ne_data:
                    ne_by_iso2 = {}
                    for feat in ne_data.get("features", []):
                        props = feat.get("properties", {})
                        iso2 = props.get("ISO_A2") or props.get("iso_a2")
                        if iso2 and iso2 in natural_earth_targets:
                            ne_by_iso2[iso2] = feat

                    for r in natural_earth_missing:
                        iso2 = r.iso_alpha2[0] if r.iso_alpha2 else None
                        if iso2 and iso2 in ne_by_iso2:
                            feat = ne_by_iso2[iso2]
                            geom = parse_safe_geometry(feat.get("geometry"))

                            if geom:
                                hull_geom = geom.convex_hull
                                if isinstance(hull_geom, GEOSPolygon):
                                    hull_geom = GEOSMultiPolygon(hull_geom)

                                r.geom = geom
                                r.hull = hull_geom
                                r.save()

                                self.stdout.write(f"  ✓ {r} from Natural Earth")
                                processed_m49.add(r.m49)
                            else:
                                errors.append(f"Failed to parse Natural Earth geometry for {r}")

            # Update missing countries list
            missing_countries = [r for r in missing_countries if r.m49 not in processed_m49]

            # Try GADM for remaining countries
            if missing_countries:
                self.stdout.write(f"\nFetching {len(missing_countries)} remaining countries from GADM ...")
                gadm_features = []

                for r in missing_countries:
                    fetched = False
                    for level in [2, 1, 0]:
                        try:
                            url = f"https://geodata.ucdavis.edu/gadm/gadm4.1/json/gadm41_{r.iso_alpha3[0]}_{level}.json.zip"
                            resp = requests.get(url, timeout=60)
                            resp.raise_for_status()
                            zf = zipfile.ZipFile(io.BytesIO(resp.content))
                            json_filename = zf.namelist()[0]
                            data = json.load(zf.open(json_filename))

                            geoms = []
                            for feat in data.get("features", []):
                                g = parse_safe_geometry(feat.get("geometry"))
                                if g:
                                    if isinstance(g, GEOSMultiPolygon):
                                        geoms.extend(list(g))
                                    elif isinstance(g, GEOSPolygon):
                                        geoms.append(g)

                            if not geoms:
                                continue

                            # Merge all valid geometries into one MultiPolygon
                            try:
                                merged = GEOSMultiPolygon(geoms)
                            except GEOSException:
                                # fallback: repair and retry
                                merged = GEOSMultiPolygon([g.buffer(0) for g in geoms if g.valid or g.buffer(0)])

                            geom = merged
                            hull_geom = geom.convex_hull
                            if isinstance(hull_geom, GEOSPolygon):
                                hull_geom = GEOSMultiPolygon(hull_geom)

                            # Save to DB
                            r.geom = geom
                            r.hull = hull_geom
                            r.save()

                            try:
                                english_name = str(r)
                            except Exception:
                                english_name = f"<no label for {r.m49}>"

                            gadm_features.append({
                                "type": "Feature",
                                "geometry": json.loads(geom.geojson),
                                "properties": {
                                    "m49": r.m49,
                                    "iso2": r.iso_alpha2[0] if r.iso_alpha2 else "",
                                    "iso3": r.iso_alpha3[0] if r.iso_alpha3 else "",
                                    "name": english_name,
                                },
                            })

                            self.stdout.write(f"  ✓ {r} from GADM")
                            fetched = True
                            break  # stop trying lower levels once successful

                        except Exception as e:
                            errors.append(f"{r.m49} {r.iso_alpha3[0]}: {e}")
                            continue

                    if not fetched:
                        errors.append(f"Failed to fetch GADM for {r} ({r.m49})")

                # Save combined GeoJSON locally
                if gadm_features:
                    output_file = base_dir / "missing_countries.geojson"
                    with open(output_file, "w", encoding="utf-8") as f:
                        json.dump({"type": "FeatureCollection", "features": gadm_features}, f)
                    self.stdout.write(f"✓ Combined missing countries saved to {output_file}")

        # --- Print any errors ---
        if errors:
            self.stdout.write("\nSome countries could not be fetched or processed:")
            for e in errors:
                self.stdout.write(f"  - {e}")

        self.stdout.write(self.style.SUCCESS("✓ Import complete."))
