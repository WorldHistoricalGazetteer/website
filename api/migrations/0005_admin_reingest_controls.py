"""Admin-curated controls for non-WHG gazetteers.

Adds the fields needed to drive the staff admin UI for non-WHG
gazetteers (Master Plan §1.4 follow-up):

* Renames ``tileset_polygon_only`` → ``no_explore``. The original name was
  opaque; ``no_explore`` makes the effect (hide tileset-dependent affordances)
  clear.
* Adds ``region_source`` — when ``True`` the row appears in the Regions
  offcanvas's Source toggle (replacing the previously-hardcoded list of
  six entries in ``search/views.py::AtlasPageView.get_context_data``).
* Adds re-ingest tracking fields populated by the admin "Re-ingest"
  action and the gateway-poll loop in ``api/reingest.py``.

Also seeds four new authority rows that need to exist for the Regions
toggle: ``osm_misc``, ``po`` (PeriodO), ``clio`` (Cliopatria), ``nl``
(NativeLand). All six region-source rows (``osm``, ``ohm``,
``osm_misc``, ``po``, ``clio``, ``nl``) get ``region_source=True``.

The seeds are idempotent (``update_or_create``); re-running the
migration after the inventory pipeline pushes its own rows will not
duplicate or destroy admin-set curatorial flags because every flip
of ``region_source`` happens within an explicit ``defaults`` dict.
"""

from django.db import migrations, models


REGION_SOURCE_SEEDS = [
    # (id,        name,                       description, namespace, region_source default)
    ("osm",       "OSM (Modern)",             "Modern administrative boundaries from OpenStreetMap.", "osm",      True),
    ("ohm",       "OHM (Historical)",         "Historical boundaries from OpenHistoricalMap.",        "ohm",      True),
    ("osm_misc",  "OSM/OHM (Miscellaneous)",  "Non-administrative boundaries from OSM/OHM — "
                                              "aboriginal lands, baronies, civil and political "
                                              "boundaries, climatic zones, parishes, historical "
                                              "and obsolete administrative divisions, etc.",          "osm_misc", True),
    ("po",        "PeriodO",                  "Periods, events, and temporalities from PeriodO.",     "po",       True),
    ("clio",      "Cliopatria",               "Historical political entities from Cliopatria.",       "clio",     True),
    ("nl",        "NativeLand",               "Indigenous territories and languages from NativeLand.","nl",       True),
]


def seed_region_sources(apps, schema_editor):
    """Idempotent seed.

    For existing rows (already populated by migration 0003 or by inventory
    pushes), we only set ``region_source`` — every other field, including
    staff-edited curation, is left untouched.

    For brand-new rows (``osm_misc``, ``po``, ``clio``, ``nl``), we create
    with a full default payload so they exist before the inventory pipeline
    catches up.
    """
    GazetteerRegistryEntry = apps.get_model("api", "GazetteerRegistryEntry")
    for entry_id, name, description, namespace, region_source in REGION_SOURCE_SEEDS:
        existing = GazetteerRegistryEntry.objects.filter(id=entry_id).first()
        if existing is not None:
            if existing.region_source != region_source:
                existing.region_source = region_source
                existing.save(update_fields=["region_source"])
            continue
        GazetteerRegistryEntry.objects.create(
            id=entry_id,
            name=name,
            description=description,
            namespace=namespace,
            entry_class="authority",
            status="published",
            h3_coverage=[],
            temporal_extent=[],
            core=False,
            region_source=region_source,
            no_explore=False,
            gazetteer_type="standard",
        )


def unseed_region_sources(apps, schema_editor):
    """Clear the ``region_source`` flag on the seeded rows. Don't delete
    them — by the time a downgrade runs, the inventory pipeline may have
    populated record counts and coverage on them."""
    GazetteerRegistryEntry = apps.get_model("api", "GazetteerRegistryEntry")
    GazetteerRegistryEntry.objects.filter(
        id__in=[s[0] for s in REGION_SOURCE_SEEDS],
    ).update(region_source=False)


class Migration(migrations.Migration):

    dependencies = [
        ("api", "0004_seed_whg_specialist_parent"),
    ]

    operations = [
        migrations.RenameField(
            model_name="gazetteerregistryentry",
            old_name="tileset_polygon_only",
            new_name="no_explore",
        ),
        migrations.AddField(
            model_name="gazetteerregistryentry",
            name="region_source",
            field=models.BooleanField(default=False, db_index=True),
        ),
        migrations.AddField(
            model_name="gazetteerregistryentry",
            name="reingest_status",
            field=models.CharField(
                max_length=12, default="idle", db_index=True,
                choices=[
                    ("idle",      "Idle"),
                    ("queued",    "Queued"),
                    ("running",   "Running"),
                    ("completed", "Completed"),
                    ("failed",    "Failed"),
                ],
            ),
        ),
        migrations.AddField(
            model_name="gazetteerregistryentry",
            name="reingest_started_at",
            field=models.DateTimeField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name="gazetteerregistryentry",
            name="reingest_finished_at",
            field=models.DateTimeField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name="gazetteerregistryentry",
            name="reingest_job_id",
            field=models.CharField(max_length=64, null=True, blank=True),
        ),
        migrations.AddField(
            model_name="gazetteerregistryentry",
            name="reingest_message",
            field=models.TextField(null=True, blank=True),
        ),
        migrations.RunPython(seed_region_sources, unseed_region_sources),
    ]
