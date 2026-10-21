# place#172 / place#325 Stage 4: the locus registry, an APPEND-ONLY binding of
# locus PIDs to canonical recipe bytes (places/models_locus.py explains).
#
# Beyond the table, a Postgres trigger makes the append-only rule hold for
# every client, not only the Django model: DELETE is refused outright, and an
# UPDATE that touches a binding column (pid, kind, version, canonical, digest,
# token, members, minted_at, minted_via; minted_by may only become NULL) is
# refused. The serving status (status, withdrawn_*) stays writable: a
# withdrawal (place#308 tombstone) is the one change a row undergoes.
#
# Not applied to any shared database by the author (local test DB only).
# Depends on 0026_placelink_identifier_idx, which is a Stage 4b migration
# committed separately; the two must land in that order.

from django.conf import settings
import django.contrib.postgres.fields
import django.contrib.postgres.indexes
from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone

APPEND_ONLY_SQL = """
CREATE OR REPLACE FUNCTION locus_registry_append_only() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'locus_registry is append-only: locus:% cannot be deleted (place#172)', OLD.pid
            USING ERRCODE = 'restrict_violation';
    END IF;
    IF NEW.pid IS DISTINCT FROM OLD.pid
       OR NEW.kind IS DISTINCT FROM OLD.kind
       OR NEW.version IS DISTINCT FROM OLD.version
       OR NEW.canonical IS DISTINCT FROM OLD.canonical
       OR NEW.digest IS DISTINCT FROM OLD.digest
       OR NEW.token IS DISTINCT FROM OLD.token
       OR NEW.members IS DISTINCT FROM OLD.members
       OR NEW.minted_at IS DISTINCT FROM OLD.minted_at
       OR NEW.minted_via IS DISTINCT FROM OLD.minted_via
       OR (NEW.minted_by_id IS DISTINCT FROM OLD.minted_by_id AND NEW.minted_by_id IS NOT NULL) THEN
        RAISE EXCEPTION 'locus_registry is append-only: the binding of locus:% cannot change (place#172)', OLD.pid
            USING ERRCODE = 'restrict_violation';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS locus_registry_append_only_trg ON locus_registry;
CREATE TRIGGER locus_registry_append_only_trg
    BEFORE UPDATE OR DELETE ON locus_registry
    FOR EACH ROW EXECUTE FUNCTION locus_registry_append_only();
"""

APPEND_ONLY_REVERSE_SQL = """
DROP TRIGGER IF EXISTS locus_registry_append_only_trg ON locus_registry;
DROP FUNCTION IF EXISTS locus_registry_append_only();
"""


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('places', '0026_placelink_identifier_idx'),
    ]

    operations = [
        migrations.CreateModel(
            name='LocusRegistryEntry',
            fields=[
                ('pid', models.CharField(max_length=14, primary_key=True, serialize=False)),
                ('kind', models.CharField(choices=[('recipe', 'recipe (live: re-run on every visit)'), ('legacy', 'legacy whg_id (frozen membership)')], max_length=8)),
                ('version', models.PositiveSmallIntegerField()),
                ('canonical', models.BinaryField()),
                ('digest', models.CharField(max_length=64, unique=True)),
                ('token', models.TextField(blank=True, default='')),
                ('members', django.contrib.postgres.fields.ArrayField(base_field=models.CharField(max_length=255), default=list, size=None)),
                ('minted_at', models.DateTimeField(default=django.utils.timezone.now)),
                ('minted_via', models.CharField(choices=[('modal', 'minting modal'), ('dereference', 'first dereference')], max_length=16)),
                ('status', models.CharField(choices=[('active', 'active'), ('withdrawn', 'withdrawn (tombstone)')], default='active', max_length=12)),
                ('withdrawn_at', models.DateTimeField(blank=True, null=True)),
                ('withdrawn_reason', models.TextField(blank=True, default='')),
                ('minted_by', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='minted_loci', to=settings.AUTH_USER_MODEL)),
                ('withdrawn_by', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='withdrawn_loci', to=settings.AUTH_USER_MODEL)),
            ],
            options={
                'verbose_name': 'locus registry entry',
                'verbose_name_plural': 'locus registry entries',
                'db_table': 'locus_registry',
                'indexes': [django.contrib.postgres.indexes.GinIndex(fields=['members'], name='locus_registry_members_gin'), models.Index(fields=['minted_by', 'minted_at'], name='locus_registry_minter_idx')],
            },
        ),
        migrations.RunSQL(APPEND_ONLY_SQL, APPEND_ONLY_REVERSE_SQL),
    ]
