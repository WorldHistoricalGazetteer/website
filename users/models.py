import hashlib
import hmac

from django.db import models
from django.contrib.auth.models import AbstractUser, PermissionsMixin
from django.core.validators import RegexValidator, EmailValidator
from encrypted_model_fields.fields import EncryptedTextField

from main.choices import USER_ROLE

# src/users/model.py
from django.conf import settings
from django.contrib.auth.base_user import BaseUserManager
from django.contrib.auth.models import AbstractUser
from django.db import models
from django.utils.translation import gettext_lazy as _
from django_resized import ResizedImageField


def user_directory_path(instance, filename):
    return "user_{0}/{1}".format(instance.username, filename)


def email_lookup_hash(email):
    """Deterministic, keyed hash of a normalised email address, for DB-level equality lookups.

    ``User.email`` is a non-deterministic ``EncryptedTextField`` — its ciphertext can't be queried, so
    ``filter(email=…)`` silently matches nothing. This stores an indexed HMAC-SHA256 of the lowercased,
    trimmed address alongside it, giving fast exact lookups. Keyed with ``SECRET_KEY`` so a leaked DB
    dump can't be rainbow-tabled back to addresses without the key. Returns ``None`` for empty input,
    keeping the hash column NULL in step with a NULL email. If ``SECRET_KEY`` is ever rotated, re-run
    the backfill (management/migration) to regenerate the column.
    """
    if not email:
        return None
    norm = str(email).strip().lower()
    if not norm:
        return None
    return hmac.new(settings.SECRET_KEY.encode("utf-8"), norm.encode("utf-8"), hashlib.sha256).hexdigest()


class UserManager(BaseUserManager):
    """
    Custom user model manager
    """

    def create_user(
        self, username, email, password, given_name, surname, **extra_fields
    ):
        """
        Create and save a User with the given username, email and password.
        """
        if not username:
            raise ValueError(_("The username must be set"))
        if not email:
            raise ValueError(_("The Email must be set"))
        if not given_name:
            raise ValueError(_("The given name must be set"))
        if not surname:
            raise ValueError(_("The surname must be set"))
        email = self.normalize_email(email)
        user = self.model(username=username, email=email, **extra_fields)
        user.set_password(password)
        user.save()
        return user

    def by_email(self, email):
        """Look up a single user by email address (case-insensitive) via the indexed lookup hash.
        Returns the user or ``None``. Use this everywhere instead of ``filter(email=…)``, which can
        never match the encrypted column. See ``email_lookup_hash``."""
        h = email_lookup_hash(email)
        return self.filter(email_hash=h).first() if h else None

    def create_superuser(self, username, email, password, **extra_fields):
        """
        Create and save a SuperUser with the given username, email and password.
        """
        extra_fields.setdefault("is_staff", True)
        extra_fields.setdefault("is_superuser", True)
        extra_fields.setdefault("is_active", True)

        if extra_fields.get("is_staff") is not True:
            raise ValueError(_("Superuser must have is_staff=True."))
        if extra_fields.get("is_superuser") is not True:
            raise ValueError(_("Superuser must have is_superuser=True."))
        return self.create_user(username, email, password, **extra_fields)


class User(AbstractUser, PermissionsMixin):
    orcid = models.URLField(max_length=255, unique=True, null=True, blank=True)
    orcid_refresh_token = EncryptedTextField(null=True, blank=True)
    orcid_token_expires_at = models.DateTimeField(null=True, blank=True)
    # When the account became ORCiD-backed (set at ORCiD-native creation, or when a legacy account
    # is linked via the ORCiD claim flow — see accounts/views.py::orcid_claim).
    orcid_linked_at = models.DateTimeField(null=True, blank=True)
    # True once the account is ORCiD-backed: password can no longer grant or claim a session for it.
    # Native ORCiD accounts are retired at creation; legacy accounts on link.
    legacy_login_retired = models.BooleanField(default=False)

    # Email can come from ORCID (opportunistically) or user input (mandatory for full functionality)
    email = EncryptedTextField(validators=[EmailValidator()], null=True, blank=True)
    # Indexed HMAC of the normalised email — the `email` column is encrypted and unqueryable, so this is
    # what equality lookups use (kept in sync by save(); see email_lookup_hash / UserManager.by_email).
    email_hash = models.CharField(max_length=64, null=True, blank=True, db_index=True, editable=False)
    email_confirmed = models.BooleanField(default=False)
    welcome_email_sent = models.BooleanField(default=False)

    given_name = models.CharField(max_length=255, null=True)
    surname = models.CharField(max_length=255, null=True)
    affiliation = models.CharField(max_length=255, null=True)
    web_page = models.URLField(max_length=255, null=True, blank=True)
    name = models.CharField(max_length=255)

    # For new users, the unique username is f"{given_name}-{family_name}-{user.id}"
    username = models.CharField(max_length=100, unique=True)

    role = models.CharField(max_length=24, choices=USER_ROLE, default="normal")
    is_active = models.BooleanField(default=True)
    is_staff = models.BooleanField(default=False)
    news_permitted = models.BooleanField(default=False)

    # UI language preference — a short code (e.g. 'en', 'de', or 'local' for
    # "as recorded"). Mirrors the client-side ``whg_lang`` localStorage so
    # signed-in users keep their choice across machines and sessions.
    preferred_language = models.CharField(max_length=8, blank=True, default="")

    # Legacy fields - keep for migration period
    must_reset_password = models.BooleanField(default=False)

    # Keep these fields, which nullify the default fields from AbstractUser
    first_name = None
    last_name = None

    USERNAME_FIELD = "username"
    REQUIRED_FIELDS = ["email", "name"]

    objects = UserManager()

    class Meta:
        db_table = "auth_users"

    def save(self, *args, **kwargs):
        self.name = " ".join(filter(None, [self.given_name, self.surname])) or self.username
        self.email_hash = email_lookup_hash(self.email)  # keep the lookup hash in step with the email
        super().save(*args, **kwargs)

    def __str__(self):
        return self.username

    @property
    def has_verified_email(self):
        """Check if user has a verified email address."""
        return bool(self.email and self.email_confirmed)

    @property
    def can_access_beta(self):
        """Access to unpublished BETA / staff-preview features (e.g. Map your Data).
        Granted to staff/superusers and to users given the ``beta_tester`` role — the
        single gate for this and any future BETA feature."""
        return bool(self.is_staff or self.is_superuser or self.role in ('beta_tester', 'superuser'))
