"""Remove the stale ``dplace`` registry row.

Migration 0003 seeded D-PLACE under id ``dplace``, but the indexing
pipeline's canonical namespace for the dataset is ``dp`` (see
``processing/settings.py``). The inventory push consequently created a
real row with id ``dp`` and the seeded ``dplace`` row sat unused with
``record_count=0`` and no coverage. Drop it.

Idempotent — safe to run when the row is already absent.
"""

from django.db import migrations


def remove_stale_dplace(apps, schema_editor):
    GazetteerRegistryEntry = apps.get_model("api", "GazetteerRegistryEntry")
    GazetteerRegistryEntry.objects.filter(id="dplace").delete()


def noop(apps, schema_editor):
    """No reverse — re-creating the wrong row would just re-introduce the bug."""


class Migration(migrations.Migration):

    dependencies = [
        ("api", "0005_admin_reingest_controls"),
    ]

    operations = [
        migrations.RunPython(remove_stale_dplace, noop),
    ]
