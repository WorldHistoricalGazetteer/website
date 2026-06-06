from django.contrib.contenttypes.fields import GenericForeignKey
from django.contrib.contenttypes.models import ContentType
from django.db import models
from django.core.validators import RegexValidator, EmailValidator
from django.utils.translation import gettext_lazy as _

class Person(models.Model):
    family = models.CharField(max_length=255, null=True, blank=True)
    given = models.CharField(max_length=255, null=True, blank=True)
    dropping_particle = models.CharField(max_length=255, null=True, blank=True)
    non_dropping_particle = models.CharField(max_length=255, null=True, blank=True)
    suffix = models.CharField(max_length=255, null=True, blank=True)
    comma_suffix = models.BooleanField(default=False)
    static_ordering = models.BooleanField(default=False)
    literal = models.CharField(max_length=255, null=True, blank=True)
    parse_names = models.BooleanField(default=False)
    orcid = models.CharField(
        max_length=19,
        validators=[
            RegexValidator(
                regex=r"^\d{4}-\d{4}-\d{4}-\d{4}$",
                message=_("Invalid ORCiD. Format should be: 0000-0000-0000-0000"),
            )
        ],
        null=True,
        blank=True,
        unique=True,
    )
    affiliation = models.CharField(max_length=255, null=True, blank=True)

    # Use a related name to handle multiple emails
    emails = models.ManyToManyField(
        'EmailAddress',
        related_name='persons',
        blank=True
    )

    def __str__(self):
        parts = [self.given, self.family]
        if self.suffix:
            parts.append(self.suffix)
        return ' '.join(part for part in parts if part)

    class Meta:
        verbose_name = "Person"
        verbose_name_plural = "People"
        # Unique constraint on combination of fields to avoid duplicate records
        constraints = [
            models.UniqueConstraint(
                fields=['family', 'given', 'orcid'],
                name='unique_person'
            )
        ]

class EmailAddress(models.Model):
    address = models.EmailField(
        validators=[EmailValidator(message=_("Enter a valid email address."))],
        unique=True
    )

    def __str__(self):
        return self.address

    class Meta:
        verbose_name = "Email Address"
        verbose_name_plural = "Email Addresses"


class CreditRole(models.TextChoices):
    """The 14 NISO CRediT contributor roles (https://credit.niso.org).

    Values are the canonical CRediT slugs, so the term URI is derivable as
    ``https://credit.niso.org/contributor-roles/<value>/``.
    """
    CONCEPTUALIZATION = "conceptualization", _("Conceptualization")
    DATA_CURATION = "data-curation", _("Data curation")
    FORMAL_ANALYSIS = "formal-analysis", _("Formal analysis")
    FUNDING_ACQUISITION = "funding-acquisition", _("Funding acquisition")
    INVESTIGATION = "investigation", _("Investigation")
    METHODOLOGY = "methodology", _("Methodology")
    PROJECT_ADMINISTRATION = "project-administration", _("Project administration")
    RESOURCES = "resources", _("Resources")
    SOFTWARE = "software", _("Software")
    SUPERVISION = "supervision", _("Supervision")
    VALIDATION = "validation", _("Validation")
    VISUALIZATION = "visualization", _("Visualization")
    WRITING_ORIGINAL_DRAFT = "writing-original-draft", _("Writing – original draft")
    WRITING_REVIEW_EDITING = "writing-review-editing", _("Writing – review & editing")


class ContributionDegree(models.TextChoices):
    LEAD = "lead", _("Lead")
    EQUAL = "equal", _("Equal")
    SUPPORTING = "supporting", _("Supporting")


class Contribution(models.Model):
    """A role-tagged contribution by a Person to a credited object.

    Generic relation so one model carries credit for Dataset, Collection, and
    GazetteerRegistryEntry. This is the structured source of truth that
    supersedes the free-text ``creator``/``contributors`` strings (Phase 2b
    wires CSL/DataCite output and a submission UI onto it).
    """

    person = models.ForeignKey(
        "persons.Person", on_delete=models.PROTECT, related_name="contributions",
    )
    role = models.CharField(max_length=32, choices=CreditRole.choices)
    degree = models.CharField(
        max_length=10, blank=True, choices=ContributionDegree.choices,
    )
    is_corresponding = models.BooleanField(default=False)
    order = models.PositiveSmallIntegerField(default=0)

    # target: Dataset | Collection | GazetteerRegistryEntry (generic FK).
    # object_id is CharField (not PositiveInteger) so it can hold both the
    # integer PKs of Dataset/Collection and the string PK of
    # GazetteerRegistryEntry (e.g. "gn", "whg:1234").
    content_type = models.ForeignKey(ContentType, on_delete=models.CASCADE)
    object_id = models.CharField(max_length=64)
    target = GenericForeignKey("content_type", "object_id")

    class Meta:
        ordering = ["order"]
        constraints = [
            models.UniqueConstraint(
                fields=["person", "role", "content_type", "object_id"],
                name="unique_contribution",
            ),
        ]
        indexes = [
            models.Index(fields=["content_type", "object_id"],
                         name="contribution_target_idx"),
        ]

    @property
    def credit_uri(self):
        return f"https://credit.niso.org/contributor-roles/{self.role}/"

    def __str__(self):
        return f"{self.person} — {self.get_role_display()}"
