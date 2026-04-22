# types/migrations/0002_add_hierarchy_fields.py
"""
Add the hierarchy fields (path, depth, is_place_type) and indexes
to the Type model.

This migration adds real database columns, so it is NOT faked.
"""
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ('placetypes', '0001_initial'),
        ('places', '0024_remove_type_model'),
    ]

    operations = [
        migrations.AddField(
            model_name='type',
            name='path',
            field=models.CharField(blank=True, default='', max_length=500),
        ),
        migrations.AddField(
            model_name='type',
            name='depth',
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name='type',
            name='is_place_type',
            field=models.BooleanField(default=True),
        ),
        migrations.AddIndex(
            model_name='type',
            index=models.Index(fields=['path'], name='types_path_idx'),
        ),
        migrations.AddIndex(
            model_name='type',
            index=models.Index(fields=['fclass'], name='types_fclass_idx'),
        ),
        migrations.AddIndex(
            model_name='type',
            index=models.Index(fields=['is_place_type'], name='types_is_place_type_idx'),
        ),
    ]

