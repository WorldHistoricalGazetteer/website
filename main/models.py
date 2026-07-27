from django.apps import apps
from django.db import models
from django.conf import settings
from django.contrib.auth import get_user_model

User = get_user_model()
from django.core.validators import URLValidator
from django.urls import reverse
from django.utils import timezone
from collection.models import Collection, CollectionGroup
from datasets.models import Dataset
from places.models import Place
from traces.models import TraceAnnotation

from main.choices import (COMMENT_TAGS, COMMENT_TAGS_REVIEW, LOG_CATEGORIES, LOG_TYPES,
                          LINKTYPES)


# cross-app models
class BetaSnag(models.Model):
    """A beta-tester's problem report, filed via the on-site snag form (plan-beta-diagnostics /
    Beta Testing Plan). Stored durably here so nothing is lost, and — when a GitHub token is configured
    — also filed as a GitHub issue in the planning repo. Carries the diagnostics ``session_id`` that ties
    it to the tester's GlitchTip errors, so the technical trace behind a report is one lookup away."""
    SEVERITY_CHOICES = [('blocker', 'Blocker'), ('major', 'Major'), ('minor', 'Minor'),
                        ('cosmetic', 'Cosmetic')]
    # A snag is a problem report (diagnostic-heavy); a suggestion is a lighter
    # "wouldn't it be nice if…" idea. Both file to the planning repo as issues,
    # but suggestions skip the severity/steps/session/browser capture.
    KIND_CHOICES = [('snag', 'Snag'), ('suggestion', 'Suggestion')]
    kind = models.CharField(max_length=16, default='snag', choices=KIND_CHOICES)
    reporter = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, on_delete=models.SET_NULL,
                                 related_name='beta_snags')
    title = models.CharField(max_length=300)
    what = models.TextField(blank=True, default='')
    expected = models.TextField(blank=True, default='')
    steps = models.TextField(blank=True, default='')
    feature = models.CharField(max_length=80, blank=True, default='')
    severity = models.CharField(max_length=20, blank=True, default='', choices=SEVERITY_CHOICES)
    page_url = models.CharField(max_length=500, blank=True, default='')
    session_id = models.CharField(max_length=64, blank=True, default='')
    user_agent = models.CharField(max_length=300, blank=True, default='')
    github_url = models.CharField(max_length=300, blank=True, default='')  # created issue, if filed
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'beta_snag'
        ordering = ['-created']

    def __str__(self):
        return f'{self.kind} #{self.pk}: {self.title[:60]}'


class DownloadFile(models.Model):
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    title = models.CharField(max_length=255, null=True, blank=True)
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE, null=True, blank=True)
    collection = models.ForeignKey(Collection, on_delete=models.CASCADE, null=True, blank=True)
    filepath = models.FilePathField(path="/path/to/files", recursive=True, blank=True, null=True)
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        managed = True
        db_table = 'download_files'

    def __str__(self):
        return self.filepath


class Announcement(models.Model):
    content = models.CharField(max_length=255, help_text="A short announcement text.")
    headline = models.CharField(max_length=255, help_text="Appears linked to the full announcement.")
    link = models.URLField(help_text="Link to the full announcement on the external blog.")
    created_at = models.DateTimeField(default=timezone.now, help_text="Creation date of the announcement.")
    active = models.BooleanField(default=True, help_text="Whether the announcement is currently active.")

    class Meta:
        managed = True
        db_table = 'announcements'

    def __str__(self):
        return self.content[:50]  # Return first 50 characters to identify it in the admin panel.


# generic links table for collections, collection groups, datasets?, etc.
class Link(models.Model):
    collection = models.ForeignKey(Collection, default=None,
                                   on_delete=models.CASCADE, related_name='related_links', blank=True, null=True)
    collection_group = models.ForeignKey(CollectionGroup, default=None,
                                         on_delete=models.CASCADE, related_name='related_links', blank=True, null=True)
    trace_annotation = models.ForeignKey(TraceAnnotation, default=None,
                                         on_delete=models.CASCADE, related_name='related_links', blank=True, null=True)
    place = models.ForeignKey(Place, default=None,
                              on_delete=models.CASCADE, related_name='related_links', blank=True, null=True)

    uri = models.URLField(max_length=200)
    # uri = models.TextField(validators=[URLValidator()])
    label = models.CharField(null=True, blank=True, max_length=200)
    link_type = models.CharField(default='webpage', max_length=10, choices=LINKTYPES)
    license = models.CharField(null=True, blank=True, max_length=64)

    class Meta:
        managed = True
        db_table = 'links'


# some log entries only user-related; most user- and dataset-related
class Log(models.Model):
    user = models.ForeignKey(settings.AUTH_USER_MODEL,
                             related_name='log', on_delete=models.CASCADE)
    dataset = models.ForeignKey(Dataset, null=True, blank=True,
                                related_name='log', on_delete=models.CASCADE)
    collection = models.ForeignKey(Collection, null=True, blank=True,
                                   related_name='log', on_delete=models.CASCADE)
    category = models.CharField(max_length=20, choices=LOG_CATEGORIES)
    logtype = models.CharField(max_length=20, choices=LOG_TYPES)
    subtype = models.CharField(max_length=50, null=True, blank=True)
    note = models.CharField(max_length=2044, null=True, blank=True)
    timestamp = models.DateTimeField(null=True, auto_now_add=True)

    class Meta:
        managed = True
        db_table = 'log'


class Comment(models.Model):
    user = models.ForeignKey(settings.AUTH_USER_MODEL,
                             related_name='comments', on_delete=models.CASCADE)
    place_id = models.ForeignKey(Place, on_delete=models.CASCADE)
    tag = models.CharField(max_length=20, choices=COMMENT_TAGS_REVIEW, default="other")
    note = models.CharField(max_length=2044, null=True, blank=True)
    created = models.DateTimeField(null=True, auto_now_add=True)

    @property
    def dataset(self):
        return self.place_id.dataset

    class Meta:
        managed = True
        db_table = 'comments'


class SiteSetting(models.Model):
    """
    Singleton model for site-wide configuration managed via the Django admin panel.

    Only one row should ever exist (enforced by the ``load()`` class method
    and a unique singleton key).
    """

    CRC_MODES = [
        ('disabled', 'Disabled – no users'),
        ('admin_only', 'Admin only – staff / superusers'),
        ('all_users', 'All users'),
    ]

    singleton_key = models.BooleanField(
        default=True, unique=True, editable=False,
        help_text="Ensures only one SiteSetting row exists.",
    )

    crc_gateway_mode = models.CharField(
        max_length=12,
        choices=CRC_MODES,
        default='disabled',
        verbose_name='CRC gateway mode',
        help_text=(
            "Controls who receives results from the CRC places/toponyms indexes "
            "alongside the legacy WHG indexes in the Reconciliation API. "
            "'Admin only' restricts to staff/superuser accounts (useful for testing)."
        ),
    )

    class Meta:
        managed = True
        db_table = 'site_settings'
        verbose_name = 'Site setting'
        verbose_name_plural = 'Site settings'

    def __str__(self):
        return f"Site settings (CRC: {self.get_crc_gateway_mode_display()})"

    def save(self, *args, **kwargs):
        # Force singleton
        self.singleton_key = True
        super().save(*args, **kwargs)

    @classmethod
    def load(cls):
        """Return the single SiteSetting instance, creating it if needed."""
        obj, _ = cls.objects.get_or_create(singleton_key=True)
        return obj



# ── Email invitations (place#155) ────────────────────────────────────────────
# A signed-in user can email a WHG link, or an invitation to register, to someone
# who has no account. The recipient never consented to hear from us, so **their
# address is never stored** — not here, not in a log line, not in the Zulip mail
# mirror. What we keep instead is a salted HMAC of the address (see
# ``main.invitations.invitation_email_hash``), which is enough to cap how often
# one person can be invited and to honour "don't contact me again", but is not
# the address itself. The hash uses a salt distinct from ``users.email_lookup_hash``
# so these rows cannot be cross-referenced against the user table to reveal who
# has been invited.

class InvitationSendLog(models.Model):
    """One row per invitation sent. Purged after 90 days (see
    ``main.invitations.purge_expired_logs``) — it exists to enforce the per-sender
    and per-recipient caps and to make abuse investigable, not as a record of
    correspondence."""
    sender = models.ForeignKey(User, on_delete=models.CASCADE, related_name='invitations_sent')
    recipient_hash = models.CharField(max_length=64, db_index=True)
    kind = models.CharField(max_length=8, choices=[('view', 'Share a page'), ('join', 'Invitation to join')])
    target_url = models.TextField(blank=True, default='')
    created = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        db_table = 'invitation_send_log'
        indexes = [models.Index(fields=['sender', 'created'])]

    def __str__(self):
        return f"{self.kind} invitation from {self.sender.username} at {self.created:%Y-%m-%d %H:%M}"


class InvitationSuppression(models.Model):
    """"Don't contact me again" — one row per opted-out address hash. Retained
    indefinitely, because forgetting it would mean mailing that person again."""
    recipient_hash = models.CharField(max_length=64, unique=True)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'invitation_suppression'

    def __str__(self):
        return f"suppressed {self.recipient_hash[:12]}… since {self.created:%Y-%m-%d}"
