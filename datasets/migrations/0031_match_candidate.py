"""place#338 (v4 reconciliation Phase 2): ``MatchCandidate``, one suggested (record, place) pair.
The ``generation`` FK is added in 0030, with the table it points at. Additive."""
from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('places', '0001_initial'),
        ('datasets', '0030_sensitivity_declaration'),
    ]

    operations = [
        migrations.CreateModel(
            name='MatchCandidate',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('subject_id', models.CharField(max_length=160)),
                ('candidate_id', models.CharField(max_length=160)),
                ('candidate_title', models.CharField(blank=True, default='', max_length=500)),
                ('namespace', models.CharField(blank=True, db_index=True, default='', max_length=32)),
                ('gateway_score', models.FloatField(blank=True, null=True)),
                ('gateway_confidence', models.FloatField(blank=True, null=True)),
                ('signals', models.JSONField(blank=True, default=dict)),
                ('tier', models.CharField(choices=[('confident', 'Likely the same place'), ('ambiguous', 'Which one, if any?'), ('weak', 'Weak suggestion')], max_length=12)),
                ('rank', models.PositiveSmallIntegerField(default=0)),
                ('status', models.CharField(choices=[('suggested', 'suggested'), ('accepted', 'accepted'), ('rejected', 'rejected'), ('stale', 'stale')], default='suggested', max_length=12)),
                ('explanation', models.JSONField(blank=True, default=dict)),
                ('card', models.JSONField(blank=True, default=dict)),
                ('created', models.DateTimeField(default=django.utils.timezone.now)),
                ('decided_at', models.DateTimeField(blank=True, null=True)),
                ('dataset', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='match_candidates', to='datasets.dataset')),
                ('decided_by', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='+', to=settings.AUTH_USER_MODEL)),
                ('place', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='match_candidates', to='places.place')),
            ],
            options={
                'db_table': 'match_candidate',
                'ordering': ['place_id', 'rank'],
            },
        ),
        migrations.AddIndex(
            model_name='matchcandidate',
            index=models.Index(fields=['dataset', 'tier', 'status'], name='match_cand_ds_tier_status'),
        ),
    ]
