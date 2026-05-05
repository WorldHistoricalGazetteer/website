from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("users", "0013_user_welcome_email_sent"),
    ]

    operations = [
        migrations.AddField(
            model_name="user",
            name="language",
            field=models.CharField(blank=True, default="", max_length=8),
        ),
    ]
