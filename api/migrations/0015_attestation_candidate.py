"""place#338: the suggestion an attestation was made from (nullable; Phase 1 deferred it). Additive."""
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0014_attestation_v4_fields'),
        ('datasets', '0029_match_candidate'),
    ]

    operations = [
        migrations.AddField(
            model_name='contributorattestation',
            name='candidate',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='attestations', to='datasets.matchcandidate'),
        ),
    ]
