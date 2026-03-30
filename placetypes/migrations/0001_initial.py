# placetypes/migrations/0001_initial.py
"""
Initial migration for the types app.

This creates the Type model on db_table='types'.  If the table already
exists (migrated from the places app), run with --fake:

    python manage.py migrate types 0001 --fake

Then run the next migration (0002) which adds the new hierarchy columns.
"""
from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True
    dependencies = []

    operations = [
        migrations.CreateModel(
            name='Type',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('aat_id', models.IntegerField(unique=True)),
                ('parent_id', models.IntegerField(blank=True, null=True)),
                ('term', models.CharField(max_length=100)),
                ('term_full', models.CharField(max_length=100)),
                ('note', models.TextField(max_length=3000)),
                ('fclass', models.CharField(blank=True, max_length=1, null=True)),
            ],
            options={
                'db_table': 'types',
                'managed': True,
            },
        ),
    ]

