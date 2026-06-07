from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('datasets', '0022_dataset_license_rights_statement'),
    ]

    operations = [
        migrations.AddField(
            model_name='dataset',
            name='downloadable',
            field=models.BooleanField(
                default=True,
                help_text='Uncheck to disable all downloads of this dataset '
                          '(too large / obtain from upstream source instead).',
            ),
        ),
    ]
