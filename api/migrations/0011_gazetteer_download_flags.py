# Generated for place#136 — download-legality + volume flags.

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0010_seed_registry_web_item'),
    ]

    operations = [
        migrations.AddField(
            model_name='gazetteerregistryentry',
            name='redistributable',
            field=models.BooleanField(default=True),
        ),
        migrations.AddField(
            model_name='gazetteerregistryentry',
            name='downloadable',
            field=models.BooleanField(default=True),
        ),
        migrations.AddField(
            model_name='gazetteerregistryentry',
            name='download_blocked_reason',
            field=models.CharField(blank=True, max_length=32, null=True),
        ),
    ]
