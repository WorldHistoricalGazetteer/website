# places/migrations/0024_remove_type_model.py
"""
Remove the Type model from the places app's migration state.

The Type model has moved to the 'types' app. This migration does NOT
drop the underlying 'types' database table — it only removes the model
from Django's migration tracking for the places app.

Run the types app's initial migration with --fake first:
    python manage.py migrate types 0001 --fake
    python manage.py migrate types 0002
    python manage.py migrate places 0024
"""
from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ('places', '0023_closematch_close_match_place_a_832cab_idx_and_more'),
        # Ensure the types app has claimed the table first
        ('placetypes', '0001_initial'),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            # Only remove from Django's state — do NOT drop the table
            state_operations=[
                migrations.DeleteModel(
                    name='Type',
                ),
            ],
            database_operations=[],
        ),
    ]

