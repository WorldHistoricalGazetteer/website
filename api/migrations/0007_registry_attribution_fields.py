# Generated for citations design Phase 4 (authorities source-of-truth).
#
# Adds push-managed attribution/licence/rights fields to the gazetteer
# registry. These are populated by the indexing pipeline's Batch 11 push
# (processing/push_gazetteer_inventory.py) and applied by
# api/views_indexing.py::GazetteerInventoryView._upsert_one.
#
# All fields are nullable/defaulted so existing rows (and pushes that omit
# them) remain valid; attribution_for() falls back to ``description`` until
# ``citation_text`` is populated by an upgraded push.
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('licensing', '0001_initial'),
        ('api', '0006_remove_stale_dplace_seed'),
    ]

    operations = [
        migrations.AddField(
            model_name='gazetteerregistryentry',
            name='citation_text',
            field=models.TextField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='gazetteerregistryentry',
            name='license',
            field=models.ForeignKey(
                blank=True, null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name='gazetteer_entries',
                to='licensing.license',
            ),
        ),
        migrations.AddField(
            model_name='gazetteerregistryentry',
            name='license_url',
            field=models.URLField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='gazetteerregistryentry',
            name='rights_holder',
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AddField(
            model_name='gazetteerregistryentry',
            name='source_url',
            field=models.URLField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='gazetteerregistryentry',
            name='contributors_csl',
            field=models.JSONField(blank=True, default=list),
        ),
    ]
