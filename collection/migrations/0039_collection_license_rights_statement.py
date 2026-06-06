import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('licensing', '0001_initial'),
        ('collection', '0038_collection_bbox_collection_doi'),
    ]

    operations = [
        migrations.AddField(
            model_name='collection',
            name='license',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.PROTECT, related_name='collections', to='licensing.license'),
        ),
        migrations.AddField(
            model_name='collection',
            name='rights_statement',
            field=models.TextField(blank=True, help_text='Free-text rights, for custom licences or extra conditions.', null=True),
        ),
    ]
