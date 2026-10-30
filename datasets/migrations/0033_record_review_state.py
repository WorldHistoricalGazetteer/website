"""place#338: ``RecordReviewState``, per record: what the last generation found (``tier``,
where ``unqueried`` is distinct from ``none``) and what a person did (``state``). Additive."""
from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('places', '0001_initial'),
        ('datasets', '0030_candidate_generation'),
    ]

    operations = [
        migrations.CreateModel(
            name='RecordReviewState',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('tier', models.CharField(choices=[('confident', 'Likely the same place'), ('ambiguous', 'Which one, if any?'), ('weak', 'Weak suggestion'), ('none', 'Nothing found'), ('unqueried', 'Not yet checked')], default='unqueried', max_length=12)),
                ('state', models.CharField(choices=[('unreviewed', 'unreviewed'), ('linked', 'linked'), ('no_good_match', 'no_good_match'), ('later', 'later'), ('excluded', 'excluded')], default='unreviewed', max_length=16)),
                ('last_failure', models.CharField(blank=True, default='', max_length=32)),
                ('attempts', models.PositiveIntegerField(default=0)),
                ('queried_at', models.DateTimeField(blank=True, null=True)),
                ('candidate_count', models.PositiveSmallIntegerField(default=0)),
                ('has_geometry', models.BooleanField(default=False)),
                ('top_candidate_id', models.CharField(blank=True, default='', max_length=160)),
                ('shared_count', models.PositiveIntegerField(default=0)),
                ('decided_at', models.DateTimeField(blank=True, null=True)),
                ('note', models.TextField(blank=True, default='')),
                ('dataset', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='record_review_states', to='datasets.dataset')),
                ('decided_by', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='+', to=settings.AUTH_USER_MODEL)),
                ('generation', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='record_states', to='datasets.candidategeneration')),
                ('place', models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, related_name='review_state', to='places.place')),
            ],
            options={
                'db_table': 'record_review_state',
            },
        ),
        migrations.AddIndex(
            model_name='recordreviewstate',
            index=models.Index(fields=['dataset', 'tier', 'state'], name='record_review_ds_tier_state'),
        ),
    ]
