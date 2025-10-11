# models.py
import secrets

from django.conf import settings
from django.db import models
from django.utils import timezone


# APIToken and UserAPIProfile are created lazily when requested by a user.

class APIToken(models.Model):
    user = models.OneToOneField(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="api_token",
    )
    key = models.CharField(max_length=64, unique=True)
    created = models.DateTimeField(auto_now_add=True)
    last_used = models.DateTimeField(null=True, blank=True)

    def regenerate(self):
        self.key = secrets.token_urlsafe(32)
        self.created = timezone.now()
        self.save()

    def __str__(self):
        return f"{self.user} token"


class UserAPIProfile(models.Model):
    user = models.OneToOneField(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="api_profile",
    )
    daily_count = models.IntegerField(default=0)
    daily_reset = models.DateField(default=timezone.now)
    total_count = models.IntegerField(default=0)
    daily_limit = models.IntegerField(default=5000)

    def increment_usage(self):
        today = timezone.now().date()
        if self.daily_reset != today:
            self.daily_reset = today
            self.daily_count = 0
        self.daily_count += 1
        self.total_count += 1
        self.save()

    def __str__(self):
        return f"{self.user} API profile"
