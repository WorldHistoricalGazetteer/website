import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('contenttypes', '0002_remove_content_type_name'),
        ('persons', '0002_emailaddress_remove_person_email_and_more'),
    ]

    operations = [
        migrations.CreateModel(
            name='Contribution',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('role', models.CharField(choices=[('conceptualization', 'Conceptualization'), ('data-curation', 'Data curation'), ('formal-analysis', 'Formal analysis'), ('funding-acquisition', 'Funding acquisition'), ('investigation', 'Investigation'), ('methodology', 'Methodology'), ('project-administration', 'Project administration'), ('resources', 'Resources'), ('software', 'Software'), ('supervision', 'Supervision'), ('validation', 'Validation'), ('visualization', 'Visualization'), ('writing-original-draft', 'Writing – original draft'), ('writing-review-editing', 'Writing – review & editing')], max_length=32)),
                ('degree', models.CharField(blank=True, choices=[('lead', 'Lead'), ('equal', 'Equal'), ('supporting', 'Supporting')], max_length=10)),
                ('is_corresponding', models.BooleanField(default=False)),
                ('order', models.PositiveSmallIntegerField(default=0)),
                ('object_id', models.CharField(max_length=64)),
                ('content_type', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='contenttypes.contenttype')),
                ('person', models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, related_name='contributions', to='persons.person')),
            ],
            options={
                'ordering': ['order'],
            },
        ),
        migrations.AddIndex(
            model_name='contribution',
            index=models.Index(fields=['content_type', 'object_id'], name='contribution_target_idx'),
        ),
        migrations.AddConstraint(
            model_name='contribution',
            constraint=models.UniqueConstraint(fields=('person', 'role', 'content_type', 'object_id'), name='unique_contribution'),
        ),
    ]
