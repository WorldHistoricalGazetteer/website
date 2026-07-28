from django.db import migrations, models


def retire_existing_orcid_accounts(apps, schema_editor):
    """
    Any account that already carries an ORCiD is, by definition, ORCiD-backed:
    mark it retired and stamp orcid_linked_at (best-effort = date_joined) so the
    claim flow and any future password-retirement checks treat it correctly.
    """
    User = apps.get_model("users", "User")
    for user in User.objects.filter(orcid__isnull=False).exclude(orcid__exact=""):
        User.objects.filter(pk=user.pk).update(
            legacy_login_retired=True,
            orcid_linked_at=user.date_joined,
        )


class Migration(migrations.Migration):

    dependencies = [
        ("users", "0024_backfill_welcome_email_sent"),
    ]

    operations = [
        migrations.AddField(
            model_name="user",
            name="orcid_linked_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="user",
            name="legacy_login_retired",
            field=models.BooleanField(default=False),
        ),
        migrations.RunPython(
            retire_existing_orcid_accounts, migrations.RunPython.noop
        ),
    ]
