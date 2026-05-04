"""Indexing rebuild — Django-side schema additions.

See ``../indexing/developer/plan-ingestionRebuild.execution.md``:

* ``ContributorAttestation`` is the canonical store for user-asserted hard
  links (consumed by the ingest pipeline's Batch 12 ``contributor_replay``
  and forwarded live to the gateway via ``api/signals.py``).
* ``GazetteerRegistryEntry`` receives the per-gazetteer rows pushed by
  Batch 11 ``processing/push_gazetteer_inventory.py``.
"""

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("api", "0001_initial"),
    ]

    operations = [
        migrations.CreateModel(
            name="ContributorAttestation",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True,
                                           serialize=False, verbose_name="ID")),
                ("dataset_id", models.CharField(db_index=True, max_length=64)),
                ("place_a", models.CharField(db_index=True, max_length=128)),
                ("place_b", models.CharField(db_index=True, max_length=128)),
                ("relation_type", models.CharField(
                    choices=[
                        ("sameAs", "sameAs"),
                        ("exactMatch", "exactMatch"),
                        ("closeMatch", "closeMatch"),
                        ("distinct", "distinct"),
                    ],
                    max_length=16,
                )),
                ("asserted_at", models.DateTimeField(default=django.utils.timezone.now)),
                ("modified_at", models.DateTimeField(auto_now=True)),
                ("justification", models.TextField(blank=True, null=True)),
                ("status", models.CharField(
                    choices=[
                        ("active", "active"),
                        ("pending", "pending"),
                        ("rejected", "rejected"),
                        ("superseded", "superseded"),
                    ],
                    db_index=True, default="active", max_length=12,
                )),
                ("legacy_v3_2", models.BooleanField(db_index=True, default=False)),
                ("user", models.ForeignKey(
                    on_delete=django.db.models.deletion.CASCADE,
                    related_name="contributor_attestations",
                    to=settings.AUTH_USER_MODEL,
                )),
            ],
            options={
                "constraints": [
                    models.CheckConstraint(
                        check=models.Q(("place_a__lt", models.F("place_b"))),
                        name="contrib_attest_canonical_order",
                    ),
                    models.UniqueConstraint(
                        fields=("place_a", "place_b", "relation_type", "user"),
                        name="contrib_attest_unique_edge_per_user",
                    ),
                ],
                "indexes": [
                    models.Index(fields=["dataset_id", "status"],
                                 name="contrib_attest_dataset_status"),
                ],
            },
        ),
        migrations.CreateModel(
            name="GazetteerRegistryEntry",
            fields=[
                ("id", models.CharField(max_length=64, primary_key=True,
                                        serialize=False)),
                ("name", models.CharField(max_length=255)),
                ("description", models.TextField(blank=True, null=True)),
                ("namespace", models.CharField(db_index=True, max_length=16)),
                ("entry_class", models.CharField(
                    choices=[("authority", "authority"), ("dataset", "dataset")],
                    db_column="class", db_index=True, max_length=16,
                )),
                ("record_count", models.IntegerField(default=0)),
                ("status", models.CharField(
                    choices=[
                        ("draft", "draft"),
                        ("submitted", "submitted"),
                        ("rejected", "rejected"),
                        ("pending", "pending"),
                        ("published", "published"),
                    ],
                    db_index=True, default="published", max_length=12,
                )),
                ("h3_coverage", models.JSONField(default=list)),
                ("temporal_extent", models.JSONField(default=list)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("owner", models.ForeignKey(
                    blank=True, null=True,
                    on_delete=django.db.models.deletion.SET_NULL,
                    related_name="gazetteer_registry_entries",
                    to=settings.AUTH_USER_MODEL,
                )),
            ],
        ),
    ]
