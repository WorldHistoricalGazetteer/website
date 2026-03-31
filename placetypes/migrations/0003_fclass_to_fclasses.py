# placetypes/migrations/0003_fclass_to_fclasses.py
"""
Convert Type.fclass (CharField) to Type.fclasses (ArrayField).

Preserves existing single fclass values by wrapping them in a list.
"""
import django.contrib.postgres.fields
from django.db import migrations, models


def forwards_data(apps, schema_editor):
    """Wrap existing scalar fclass values into a list."""
    Type = apps.get_model('placetypes', 'Type')
    # Use raw SQL for efficiency — wrap non-null fclass into a 1-element array
    schema_editor.execute(
        'UPDATE types SET fclasses = ARRAY[fclass] WHERE fclass IS NOT NULL'
    )


class Migration(migrations.Migration):
    dependencies = [
        ('placetypes', '0002_add_hierarchy_fields'),
    ]

    operations = [
        # 1. Add the new ArrayField column
        migrations.AddField(
            model_name='type',
            name='fclasses',
            field=django.contrib.postgres.fields.ArrayField(
                base_field=models.CharField(max_length=1),
                blank=True,
                null=True,
                size=None,
            ),
        ),
        # 2. Copy existing data
        migrations.RunPython(forwards_data, migrations.RunPython.noop),
        # 3. Remove the old scalar column
        migrations.RemoveField(
            model_name='type',
            name='fclass',
        ),
        # 4. Remove the old fclass index (it references the dropped column)
        migrations.RemoveIndex(
            model_name='type',
            name='types_fclass_idx',
        ),
    ]
