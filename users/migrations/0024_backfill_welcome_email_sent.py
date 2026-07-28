from django.db import migrations


def backfill_welcome_email_sent(apps, schema_editor):
    """
    Mark every already-verified account as welcomed, so the (now
    transition-gated) welcome_email signal cannot retroactively "welcome" any
    long-standing legacy user on an incidental save. Uses a bulk UPDATE, so no
    signals fire and no emails are sent.
    """
    # NB: email is an EncryptedTextField (non-deterministic) — only isnull
    # lookups are valid on it; exact/"" comparisons don't work.
    User = apps.get_model("users", "User")
    (
        User.objects.filter(email_confirmed=True, welcome_email_sent=False)
        .exclude(email__isnull=True)
        .update(welcome_email_sent=True)
    )


class Migration(migrations.Migration):

    dependencies = [
        ("users", "0023_user_email_hash"),
    ]

    operations = [
        migrations.RunPython(
            backfill_welcome_email_sent, migrations.RunPython.noop
        ),
    ]
