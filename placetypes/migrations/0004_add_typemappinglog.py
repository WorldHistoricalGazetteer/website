# Generated manually

import django.contrib.postgres.fields
import django.db.models.deletion
from django.conf import settings
from django.db import migrations, models


def rename_indexes_if_exist(apps, schema_editor):
    """
    Conditionally rename old-style named indexes to Django's hashed names.
    Skips silently if the old index doesn't exist (e.g. faked migrations
    or databases where the index was never created / already renamed).
    """
    renames = [
        ('types_path_idx', 'types_path_93f79d_idx'),
        ('types_is_place_type_idx', 'types_is_plac_fc7023_idx'),
    ]
    for old_name, new_name in renames:
        with schema_editor.connection.cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM pg_indexes WHERE indexname = %s",
                [old_name],
            )
            if cursor.fetchone():
                cursor.execute(
                    f'ALTER INDEX "{old_name}" RENAME TO "{new_name}"'
                )


def add_depth_index_if_missing(apps, schema_editor):
    """Create the (depth, is_place_type) composite index if it doesn't exist."""
    with schema_editor.connection.cursor() as cursor:
        cursor.execute(
            "SELECT 1 FROM pg_indexes WHERE indexname = %s",
            ['types_depth_86dfcc_idx'],
        )
        if not cursor.fetchone():
            cursor.execute(
                'CREATE INDEX "types_depth_86dfcc_idx" '
                'ON "types" ("depth", "is_place_type")'
            )


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('placetypes', '0003_fclass_to_fclasses'),
    ]

    operations = [
        # ── Conditionally rename old indexes to Django's hashed names ──
        migrations.RunPython(
            rename_indexes_if_exist,
            migrations.RunPython.noop,
        ),
        migrations.AlterField(
            model_name='type',
            name='fclasses',
            field=django.contrib.postgres.fields.ArrayField(
                base_field=models.CharField(
                    choices=[
                        ('A', 'Administrative Boundary'),
                        ('H', 'Hydrographic'),
                        ('L', 'Area'),
                        ('P', 'Populated Place'),
                        ('R', 'Road / Railroad'),
                        ('S', 'Spot (small feature/building/farm)'),
                        ('T', 'Hypsographic (terrain/elevation)'),
                        ('U', 'Undersea'),
                        ('V', 'Vegetation'),
                    ],
                    max_length=1,
                ),
                blank=True,
                help_text='GeoNames feature classes inherited from all AAT ancestor paths.',
                null=True,
                size=None,
            ),
        ),
        migrations.RunPython(
            add_depth_index_if_missing,
            migrations.RunPython.noop,
        ),
        # ── New: TypeMappingLog audit table ──
        migrations.CreateModel(
            name='TypeMappingLog',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('action', models.CharField(choices=[('save', 'Save mapping'), ('remove', 'Remove mapping'), ('copy', 'Copy OSM → OHM')], max_length=10)),
                ('source_vocab', models.CharField(help_text='Source vocabulary: geonames, wikidata, osm, ohm', max_length=20)),
                ('source_id', models.CharField(help_text='Source identifier, e.g. P.PPL, Q515, place=city', max_length=120)),
                ('aat_id', models.IntegerField(blank=True, help_text='Target AAT concept ID', null=True)),
                ('aat_term', models.CharField(blank=True, default='', help_text='AAT term at the time of this action', max_length=200)),
                ('note', models.TextField(blank=True, default='', help_text='Optional context (e.g. bulk copy summary)')),
                ('created', models.DateTimeField(auto_now_add=True)),
                ('user', models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='type_mapping_logs', to=settings.AUTH_USER_MODEL)),
            ],
            options={
                'db_table': 'type_mapping_log',
                'ordering': ['-created'],
                'indexes': [
                    models.Index(fields=['-created'], name='type_mappin_created_idx'),
                    models.Index(fields=['source_vocab', 'source_id'], name='type_mappin_source__idx'),
                ],
            },
        ),
    ]

