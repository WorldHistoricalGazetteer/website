# place#350: a source gazetteer's row in ``sources`` has no WHG owner. ``PlaceGeom.geom_src``
# (FK to ``sources.src_id``) names the gazetteer an adopted location came from, so that row is
# created on first use with ``owner`` NULL.
#
# Additive: existing rows keep their owner. Reversible: the reverse first deletes the ownerless
# rows (``PlaceGeom.geom_src`` is SET_NULL, so adopted rows keep their geometry, and their
# ``jsonb`` still carries ``citation`` and ``provenance`` naming the source), then restores NOT NULL.
from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


def drop_ownerless(apps, schema_editor):
    Source = apps.get_model("places", "Source")
    PlaceGeom = apps.get_model("places", "PlaceGeom")
    ownerless = Source.objects.filter(owner__isnull=True)
    PlaceGeom.objects.filter(geom_src__in=ownerless.values("src_id")).update(geom_src=None)
    ownerless.delete()
    # The FK checks are deferred to commit; run them now, or the ALTER that follows is refused
    # ("pending trigger events").
    schema_editor.execute("SET CONSTRAINTS ALL IMMEDIATE")


class Migration(migrations.Migration):

    dependencies = [
        ("places", "0028_locus_redaction_minter_public"),
        # The user table is ``auth_users`` from users.0004; without this the reverse would re-create
        # the foreign key against the table's original name.
        ("users", "0004_alter_user_options_alter_user_table"),
    ]

    # Reversed in the opposite order: the ownerless rows go first, then NOT NULL comes back.
    operations = [
        migrations.AlterField(
            model_name="source",
            name="owner",
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE,
                                    to=settings.AUTH_USER_MODEL),
        ),
        migrations.RunPython(migrations.RunPython.noop, drop_ownerless),
    ]
