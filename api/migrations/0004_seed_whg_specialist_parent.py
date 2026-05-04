"""Seed the missing ``whg`` row for the Specialist Gazetteers parent.

Migration 0003 seeded the ten externally-namespaced authorities but omitted the
``whg`` parent row. The Atlas offcanvas template renders the Specialist
Gazetteers expansion only when the loop encounters an entry with ``id="whg"``;
without this row the parent is invisible.

This migration is a small data-only follow-up that upserts the row. It is
idempotent (uses ``update_or_create``) and is mirrored in 0003's seed list so a
fresh-DB rebuild produces the same result.
"""

from django.db import migrations


WHG_PARENT = {
    "id": "whg",
    "name": "Specialist Gazetteers",
    "description": "WHG-curated specialist gazetteers — datasets and collections contributed by researchers, indexed under the WHG namespace.",
}


def seed_whg_parent(apps, schema_editor):
    GazetteerRegistryEntry = apps.get_model("api", "GazetteerRegistryEntry")
    GazetteerRegistryEntry.objects.update_or_create(
        id=WHG_PARENT["id"],
        defaults={
            "name": WHG_PARENT["name"],
            "description": WHG_PARENT["description"],
            "namespace": WHG_PARENT["id"],
            "entry_class": "authority",
            "status": "published",
            "h3_coverage": [],
            "temporal_extent": [],
            "core": False,
            "tileset_polygon_only": False,
            "gazetteer_type": "standard",
        },
    )


def unseed_whg_parent(apps, schema_editor):
    GazetteerRegistryEntry = apps.get_model("api", "GazetteerRegistryEntry")
    GazetteerRegistryEntry.objects.filter(id="whg").delete()


class Migration(migrations.Migration):

    dependencies = [
        ("api", "0003_gazetteer_curatorial_fields"),
    ]

    operations = [
        migrations.RunPython(seed_whg_parent, unseed_whg_parent),
    ]
