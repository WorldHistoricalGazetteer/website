from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name='License',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('spdx_id', models.CharField(help_text="SPDX identifier, e.g. 'CC-BY-4.0'. Use a 'custom-*' id for non-SPDX terms.", max_length=64, unique=True)),
                ('label', models.CharField(max_length=128)),
                ('url', models.URLField(blank=True, help_text='Canonical licence deed / terms URL.')),
                ('spdx_uri', models.URLField(default='https://spdx.org/licenses/')),
                ('permits_commercial', models.BooleanField(default=True)),
                ('share_alike', models.BooleanField(default=False)),
                ('attribution_required', models.BooleanField(default=True)),
                ('custom', models.BooleanField(default=False, help_text='True for bespoke / non-SPDX terms; pair with rights_statement on the object.')),
                ('notes', models.TextField(blank=True)),
            ],
            options={
                'ordering': ['spdx_id'],
            },
        ),
    ]
