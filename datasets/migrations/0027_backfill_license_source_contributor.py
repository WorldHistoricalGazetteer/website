"""Mark the licences already on record as contributor-selected.

At the time of this migration the only populated ``license`` values are those
set through the contributor picker — everything earlier carries none, which is
the whole subject of place#158. So anything already licensed was chosen by its
contributor, and saying so now keeps it distinguishable from the retrospective
backfill that follows.
"""

from django.db import migrations


def mark_contributor_selected(apps, schema_editor):
    for label in ("datasets.Dataset", "collection.Collection"):
        app_label, model_name = label.split(".")
        model = apps.get_model(app_label, model_name)
        model.objects.filter(
            license__isnull=False, license_source__isnull=True
        ).update(license_source="contributor_selected")


def unmark(apps, schema_editor):
    for label in ("datasets.Dataset", "collection.Collection"):
        app_label, model_name = label.split(".")
        model = apps.get_model(app_label, model_name)
        model.objects.filter(license_source="contributor_selected").update(
            license_source=None
        )


class Migration(migrations.Migration):

    dependencies = [
        ("datasets", "0026_dataset_license_source"),
        ("collection", "0041_collection_license_source"),
    ]

    operations = [
        migrations.RunPython(mark_contributor_selected, unmark),
    ]
