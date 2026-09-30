from django.db import migrations


class Migration(migrations.Migration):
    """place#73 switchover: ``must_reset_password`` forced legacy users through a password
    reset after the v3 upgrade. Password login is retired on production (ORCiD-first), and
    nothing has read the field since."""

    dependencies = [
        ('users', '0026_user_github_username'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='user',
            name='must_reset_password',
        ),
    ]
