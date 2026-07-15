# Adds the condensed (res-2) H3 coverage rollup pushed by
# processing/push_gazetteer_inventory.py (field `h3_coverage_coarse`), small
# enough to ship to the browser for the Atlas Area coverage filter.
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("api", "0007_registry_attribution_fields"),
    ]

    operations = [
        migrations.AddField(
            model_name="gazetteerregistryentry",
            name="h3_coverage_coarse",
            field=models.JSONField(default=list),
        ),
    ]
