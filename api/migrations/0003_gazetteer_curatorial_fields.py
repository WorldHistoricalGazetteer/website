"""Curatorial fields on ``GazetteerRegistryEntry``.

Adds three admin-managed fields that are *not* touched by the ingestion
pipeline's inventory push:

* ``core`` — true for the small set of authorities pre-selected by default
  in the Atlas Gazetteers offcanvas (GeoNames, Wikidata, TGN).
* ``tileset_polygon_only`` — true for sources whose tilesets are generated
  for polygons only (OSM, OHM); the Atlas UI disables them in Explore mode
  with an explanatory tooltip.
* ``gazetteer_type`` — Standard / Itinerary / Network classification used by
  the (sketch) type-pill filter in the Specialist Gazetteers expansion.

Also seeds rows for the ten currently-known authorities so the Atlas page
renders before the first inventory push lands. IDs match what the pipeline
will eventually push, so a real upsert merges instead of duplicating.
"""

from django.db import migrations, models


CORE_IDS = ("gn", "wd", "tgn")
POLYGON_ONLY_IDS = ("osm", "ohm")

# (id, name, description) — namespace == id for these authorities.
# The "Specialist Gazetteers" parent row that appears in the Atlas offcanvas is
# *not* seeded here: it is a UI grouping (hardcoded in atlas.html) for the
# WHG-namespaced ``entry_class='dataset'`` rows that the inventory pipeline
# pushes individually. Treating it as a real authority row would conflict with
# the pipeline's own use of the ``whg`` namespace for individual datasets.
SEED_AUTHORITIES = [
    ("dplace", "D-PLACE", "Cross-cultural dataset linking linguistic, environmental, and cultural data to ~1,400 societies worldwide. Contains point locations only (no polygons)."),
    ("gb",     "GB1900",  "GB1900 gazetteer — British place names 1888–1914."),
    ("gn",     "GeoNames", "GeoNames geographical database — ~12 million features."),
    ("iv",     "IndexVillaris", "Index Villaris 1680 — ~24,000 English and Welsh place names."),
    ("ohm",    "OpenHistoricalMap", "OpenHistoricalMap — community-driven historical mapping."),
    ("osm",    "OpenStreetMap", "OpenStreetMap — comprehensive global coverage."),
    ("pl",     "Pleiades", "Pleiades gazetteer of ancient places — ~37,000 places."),
    ("tgn",    "TGN", "Getty Thesaurus of Geographic Names — ~3 million records."),
    ("tm",     "Trismegistos", "Trismegistos — papyrological and epigraphical records."),
    ("wd",     "Wikidata", "Wikidata — ~8 million places with structured data."),
]


def seed_inventory_and_curation(apps, schema_editor):
    GazetteerRegistryEntry = apps.get_model("api", "GazetteerRegistryEntry")
    for entry_id, name, description in SEED_AUTHORITIES:
        GazetteerRegistryEntry.objects.update_or_create(
            id=entry_id,
            defaults={
                "name": name,
                "description": description,
                "namespace": entry_id,
                "entry_class": "authority",
                "status": "published",
                "h3_coverage": "global" if entry_id in CORE_IDS else [],
                "temporal_extent": [],
                "core": entry_id in CORE_IDS,
                "tileset_polygon_only": entry_id in POLYGON_ONLY_IDS,
                "gazetteer_type": "standard",
            },
        )


def unseed_inventory(apps, schema_editor):
    GazetteerRegistryEntry = apps.get_model("api", "GazetteerRegistryEntry")
    GazetteerRegistryEntry.objects.filter(
        id__in=[entry_id for entry_id, _, _ in SEED_AUTHORITIES],
    ).delete()


class Migration(migrations.Migration):

    # Run each operation in its own transaction. Required on PostgreSQL when
    # this migration is applied against an already-populated table: the
    # ``AddField`` for ``core`` with ``db_index=True`` rewrites every existing
    # row, which leaves pending trigger events that block ``CREATE INDEX``
    # in the same transaction (psycopg2.errors.ObjectInUse). With
    # ``atomic = False``, the column add commits before the index is built.
    atomic = False

    dependencies = [
        ("api", "0002_indexing_rebuild"),
    ]

    operations = [
        migrations.AddField(
            model_name="gazetteerregistryentry",
            name="core",
            field=models.BooleanField(default=False, db_index=True),
        ),
        migrations.AddField(
            model_name="gazetteerregistryentry",
            name="tileset_polygon_only",
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name="gazetteerregistryentry",
            name="gazetteer_type",
            field=models.CharField(
                max_length=16,
                choices=[
                    ("standard",  "Standard"),
                    ("itinerary", "Itinerary"),
                    ("network",   "Network"),
                ],
                default="standard",
            ),
        ),
        migrations.RunPython(seed_inventory_and_curation, unseed_inventory),
    ]
