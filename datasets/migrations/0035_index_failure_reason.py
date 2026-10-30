from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('datasets', '0034_index_pending'),
    ]

    operations = [
        migrations.AddField(
            model_name='dataset',
            name='index_failure_reason',
            field=models.CharField(blank=True, default='', max_length=40),
        ),
    ]
