# Generated manually

import django.db.models.deletion
from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('placetypes', '0004_rename_types_path_idx_types_path_93f79d_idx_and_more'),
    ]

    operations = [
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

