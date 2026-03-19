# Generated manually 2026-03-19

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('main', '0015_delete_tileset'),
    ]

    operations = [
        migrations.CreateModel(
            name='SiteSetting',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('singleton_key', models.BooleanField(
                    default=True,
                    editable=False,
                    help_text='Ensures only one SiteSetting row exists.',
                    unique=True,
                )),
                ('crc_gateway_mode', models.CharField(
                    choices=[
                        ('disabled', 'Disabled – no users'),
                        ('admin_only', 'Admin only – staff / superusers'),
                        ('all_users', 'All users'),
                    ],
                    default='disabled',
                    help_text=(
                        "Controls who receives results from the CRC places/toponyms indexes "
                        "alongside the legacy WHG indexes in the Reconciliation API. "
                        "'Admin only' restricts to staff/superuser accounts (useful for testing)."
                    ),
                    max_length=12,
                    verbose_name='CRC gateway mode',
                )),
            ],
            options={
                'verbose_name': 'Site setting',
                'verbose_name_plural': 'Site settings',
                'db_table': 'site_settings',
                'managed': True,
            },
        ),
    ]

