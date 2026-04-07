# Generated manually

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('placetypes', '0004_add_typemappinglog'),
    ]

    operations = [
        migrations.AddField(
            model_name='typemappinglog',
            name='confidence',
            field=models.CharField(
                blank=True,
                choices=[
                    ('exact', 'Exact match'),
                    ('close', 'Close match'),
                    ('review', 'Needs review'),
                ],
                default='',
                help_text='Mapping confidence: exact, close, or review',
                max_length=10,
            ),
        ),
    ]

