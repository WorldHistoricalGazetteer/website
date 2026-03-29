# Generated for v3.5 — CRC gateway mode is now version-based, not DB-based.

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('main', '0016_add_sitesetting'),
    ]

    operations = [
        migrations.DeleteModel(
            name='SiteSetting',
        ),
    ]

