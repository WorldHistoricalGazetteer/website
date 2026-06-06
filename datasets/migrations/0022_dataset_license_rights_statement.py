import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('licensing', '0001_initial'),
        ('datasets', '0021_dataset_authority'),
    ]

    operations = [
        migrations.AddField(
            model_name='dataset',
            name='license',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.PROTECT, related_name='datasets', to='licensing.license'),
        ),
        migrations.AddField(
            model_name='dataset',
            name='rights_statement',
            field=models.TextField(blank=True, help_text='Free-text rights, for custom licences or extra conditions.', null=True),
        ),
    ]
