"""Make the licence flags able to tell the truth (place#157).

Three schema changes, all driven by real sources the indexing team audited:

* ``permits_commercial`` becomes nullable. UN Geospatial data carries no licence
  grant either way; recording that as False asserts a restriction we cannot
  evidence, exactly as True would assert a permission we cannot evidence.
* ``no_derivatives`` becomes a stored field rather than a value derived from the
  SPDX id. Derivation cannot express bespoke terms, and the authority metadata
  now declares this per licence. Backfilled with the old derivation so nothing
  changes for existing rows.
* ``contributor_selectable`` added, so source-specific bespoke terms can live in
  the vocabulary without being offered to contributors as a choice for their own
  data.
"""
from django.db import migrations, models


def backfill_no_derivatives(apps, schema_editor):
    """Reproduce the previous derivation (``licensing.catalog._no_derivatives``)
    so existing rows keep exactly the values the site already displays: True for
    a CC …-ND-… licence, NULL for bespoke terms, False otherwise."""
    License = apps.get_model("licensing", "License")
    for lic in License.objects.all():
        if lic.custom:
            lic.no_derivatives = None
        else:
            lic.no_derivatives = "ND" in lic.spdx_id.split("-")
        lic.save(update_fields=["no_derivatives"])


def noop(apps, schema_editor):
    """Reverse of the backfill: the column is dropped anyway."""


class Migration(migrations.Migration):

    dependencies = [
        ("licensing", "0003_extend_licenses"),
    ]

    operations = [
        migrations.AlterField(
            model_name="license",
            name="permits_commercial",
            field=models.BooleanField(default=True, null=True),
        ),
        migrations.AddField(
            model_name="license",
            name="no_derivatives",
            field=models.BooleanField(default=False, null=True),
        ),
        migrations.AddField(
            model_name="license",
            name="contributor_selectable",
            field=models.BooleanField(
                default=True,
                help_text="Offer this licence in the contributor licence picker.",
            ),
        ),
        migrations.RunPython(backfill_no_derivatives, noop),
    ]
