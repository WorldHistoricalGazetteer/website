# place#325 rulings 2026-10-21 (SG, after the dev walkthrough): take-down and
# attribution before prod minting.
#
# 1. New columns on locus_registry:
#      minter_public    boolean NOT NULL DEFAULT false  (existing rows: false)
#      redacted_at      timestamptz NULL
#      redacted_by_id   FK users ON DELETE SET NULL
#      redaction_reason text NOT NULL DEFAULT ''
# 2. The append-only trigger function is REPLACED (same name, same trigger):
#      - DELETE refused; the binding columns immutable; minted_by may only go
#        NULL (all exactly as in 0027);
#      - withdrawal is ONE-WAY: once status = 'withdrawn' it can never change,
#        and withdrawn_at / withdrawn_reason are frozen; withdrawn_by may only
#        go NULL (account deletion);
#      - redaction is WRITE-ONCE: once redacted_at is set, redacted_at and
#        redaction_reason are frozen (so it can never be undone); redacted_by
#        may only go NULL;
#      - minter_public may go true -> false, never false -> true.
#    The canonical bytes are never touched by a redaction: the PID is their
#    hash. They are simply never displayed once a row is withdrawn or redacted.
#
# 0027 is applied on dev and prod and is NOT edited. Reversing 0028 restores
# 0027's function body and drops the columns.

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion

TRIGGER_0028 = """
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
    IF OLD.status = 'withdrawn' AND (
           NEW.status IS DISTINCT FROM OLD.status
           OR NEW.withdrawn_at IS DISTINCT FROM OLD.withdrawn_at
           OR NEW.withdrawn_reason IS DISTINCT FROM OLD.withdrawn_reason
           OR (NEW.withdrawn_by_id IS DISTINCT FROM OLD.withdrawn_by_id AND NEW.withdrawn_by_id IS NOT NULL)) THEN
        RAISE EXCEPTION 'locus_registry: withdrawal of locus:% is one-way and its record cannot change (place#325)', OLD.pid
            USING ERRCODE = 'restrict_violation';
    END IF;
    IF OLD.redacted_at IS NOT NULL AND (
           NEW.redacted_at IS DISTINCT FROM OLD.redacted_at
           OR NEW.redaction_reason IS DISTINCT FROM OLD.redaction_reason
           OR (NEW.redacted_by_id IS DISTINCT FROM OLD.redacted_by_id AND NEW.redacted_by_id IS NOT NULL)) THEN
        RAISE EXCEPTION 'locus_registry: the redaction of locus:% is write-once (place#325)', OLD.pid
            USING ERRCODE = 'restrict_violation';
    END IF;
    IF NEW.minter_public AND NOT OLD.minter_public THEN
        RAISE EXCEPTION 'locus_registry: minter_public of locus:% may only be turned off after minting (place#325)', OLD.pid
            USING ERRCODE = 'restrict_violation';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""

# 0027's function body, restored on reverse (the trigger itself is unchanged).
TRIGGER_0027 = """
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
"""


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('places', '0027_locusregistryentry'),
    ]

    operations = [
        migrations.AddField(
            model_name='locusregistryentry',
            name='minter_public',
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name='locusregistryentry',
            name='redacted_at',
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='locusregistryentry',
            name='redacted_by',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL,
                                    related_name='redacted_loci', to=settings.AUTH_USER_MODEL),
        ),
        migrations.AddField(
            model_name='locusregistryentry',
            name='redaction_reason',
            field=models.TextField(blank=True, default=''),
        ),
        # The function must change AFTER the columns exist (it names them), and
        # be restored BEFORE they are dropped on reverse: RunSQL last, so reverse
        # runs it first.
        migrations.RunSQL(TRIGGER_0028, TRIGGER_0027),
    ]
