# Retire the dead Dataset.creators_csl / contributors_csl M2M to persons.Person
# (citations design Phase 5b). Structured authorship now lives in
# persons.Contribution (generic relation); these fields were never read by the
# CSL/DOI path — only populated by the now-removed legacy
# `migrate_dataset_persons` management command, superseded by
# `import_freetext_contributors`. Dropping the fields also drops their join
# tables (datasets_dataset_creators_csl / _contributors_csl).
from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('datasets', '0022_dataset_license_rights_statement'),
        ('persons', '0003_contribution'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='dataset',
            name='creators_csl',
        ),
        migrations.RemoveField(
            model_name='dataset',
            name='contributors_csl',
        ),
    ]
