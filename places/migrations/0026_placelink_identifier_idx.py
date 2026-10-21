# place#325 Stage 4: an expression index on place_link (jsonb ->> 'identifier').
#
# places/concordance.py looks place_link rows up by exactly this expression
# (``_LinkIdentifier``: ``("place_link"."jsonb" ->> 'identifier') IN (...)``)
# for tiers 2 and 3 of a concordance. Without it every Record page's
# concordance is a sequential scan of place_link (0.3-3.0 s per record on prod,
# 2026-10-21).
#
# CREATE INDEX CONCURRENTLY: place_link is large on prod, and a plain CREATE
# INDEX holds a SHARE lock that blocks every write to it for the whole build.
# CONCURRENTLY cannot run inside a transaction, hence ``atomic = False``.
# Reversible: AddIndexConcurrently reverses to DROP INDEX CONCURRENTLY.
#
# Before applying on prod, see the checklist in the place#325 Stage 4 report:
# check pg_stat_activity first; a failed concurrent build leaves an INVALID
# index that must be dropped before retrying.

from django.contrib.postgres.operations import AddIndexConcurrently
from django.db import migrations, models
from django.db.models.fields.json import KeyTextTransform


class Migration(migrations.Migration):
    atomic = False

    dependencies = [
        ('places', '0025_legacyunionrecord_and_more'),
    ]

    operations = [
        AddIndexConcurrently(
            model_name='placelink',
            index=models.Index(KeyTextTransform('identifier', 'jsonb'),
                               name='place_link_identifier_idx'),
        ),
    ]
