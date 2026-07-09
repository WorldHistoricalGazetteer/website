# Generated for the dataset_edit doc-type (plan-dataset-checkout §3/§4).

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('workbench', '0004_alter_workbenchproject_doc_type'),
    ]

    operations = [
        migrations.AlterField(
            model_name='workbenchproject',
            name='doc_type',
            field=models.CharField(choices=[('reconciliation', 'Map your Data (reconciliation)'), ('gazetteer_group', 'Gazetteer Group'), ('place_collection', 'Place Collection'), ('itinerary', 'Itinerary'), ('place_record', 'Place record correction'), ('dataset_edit', 'Gazetteer records correction'), ('route', 'Route'), ('network', 'Network')], default='reconciliation', max_length=20),
        ),
    ]
