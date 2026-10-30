"""place#338: ``CandidateGeneration`` (one pass of candidate generation over a dataset) and the
``MatchCandidate.generation`` FK with its uniqueness rule. Additive."""
from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('datasets', '0031_match_candidate'),
    ]

    operations = [
        migrations.CreateModel(
            name='CandidateGeneration',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('status', models.CharField(choices=[('queued', 'queued'), ('running', 'running'), ('paused', 'paused'), ('finished', 'finished'), ('failed', 'failed'), ('superseded', 'superseded')], db_index=True, default='queued', max_length=12)),
                ('created', models.DateTimeField(default=django.utils.timezone.now)),
                ('started', models.DateTimeField(blank=True, null=True)),
                ('finished', models.DateTimeField(blank=True, null=True)),
                ('heartbeat', models.DateTimeField(blank=True, null=True)),
                ('index_alias_generation', models.CharField(blank=True, default='', max_length=64)),
                ('clustering_params_version', models.CharField(blank=True, default='', max_length=32)),
                ('clustering_params', models.JSONField(blank=True, default=dict)),
                ('recipe', models.JSONField(blank=True, default=dict)),
                ('counts', models.JSONField(blank=True, default=dict)),
                ('last_place_pk', models.BigIntegerField(blank=True, null=True)),
                ('record_limit', models.IntegerField(blank=True, null=True)),
                ('error', models.TextField(blank=True, default='')),
                ('dataset', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='candidate_generations', to='datasets.dataset')),
                ('requested_by', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='+', to=settings.AUTH_USER_MODEL)),
            ],
            options={
                'db_table': 'match_candidate_generation',
                'ordering': ['-id'],
            },
        ),
        migrations.AddField(
            model_name='matchcandidate',
            name='generation',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='candidates', to='datasets.candidategeneration'),
        ),
        migrations.AddConstraint(
            model_name='matchcandidate',
            constraint=models.UniqueConstraint(fields=('place', 'candidate_id', 'generation'), name='match_candidate_unique_per_generation'),
        ),
    ]
