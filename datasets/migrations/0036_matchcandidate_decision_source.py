from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('datasets', '0035_index_failure_reason'),
    ]

    operations = [
        migrations.AddField(
            model_name='matchcandidate',
            name='decision_source',
            field=models.CharField(
                blank=True, default='', max_length=12,
                choices=[('explicit', 'explicit'), ('prefilled', 'prefilled'),
                         ('none_fits', 'none_fits'), ('default', 'default')]),
        ),
    ]
