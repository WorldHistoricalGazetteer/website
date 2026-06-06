from django.db import migrations


class Migration(migrations.Migration):
    """Merge the two parallel periods branches from 0005.

    `0006_period_ccodes_spatialentity_ccodes_and_more` (the version committed on
    main/prod) and the `0006_chrononym_… → 0009_remove_period_outerbounds` chain
    (atlas's own evolution, applied on dev) both branch off 0005, producing a
    two-leaf graph. This is a standard no-op merge migration that unifies them.
    """

    dependencies = [
        ('periods', '0006_period_ccodes_spatialentity_ccodes_and_more'),
        ('periods', '0009_remove_period_outerbounds'),
    ]

    operations = []
