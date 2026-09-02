"""GRACE — Gazetteer Register And Contact Engagement.

WHG's editorial tracker: the datasets we want, the people and institutions
behind them, the printed sources that document them, our correspondence, and
where each dataset sits on its way in.

The design and its reasoning are in ``developer/whg-tracker-review.html``.
Five things in here will look odd without it:

* **The pipeline record is a Dataset, not a gazetteer** (§2). A contributor
  brings a *dataset*; once reconciled and published it is a *gazetteer*. Same
  object, two life stages — and GRACE's record exists almost entirely during
  the first, because the moment it becomes a gazetteer the authoritative
  record is the Gazetteer Register row. So "gazetteer" inside GRACE means
  exactly two things, neither of them this model: a **printed gazetteer** (a
  Source type) and a **published WHG gazetteer** (a Register entry).
* **A tracked dataset points at the Gazetteer Register, not at**
  ``datasets.Dataset`` (§2). Licence, rights holder, record count, coverage
  and citation are *read through* that link and never stored here, so they
  cannot drift. The link is nullable, and a row without one **is** a prospect
  — no vocabulary says so.
* **Person has an optional one-to-one to the user model** (§3), and where it
  is set, email / ORCID / affiliation are read through it. Nothing is stored
  twice. The table is everyone we track, ourselves included — not only people
  we write to.
* **Every controlled vocabulary is a table** (§5), in ``vocabularies.py``.
* **Personal data is held under legitimate interests** (§10), with a real
  erasure path. See ``privacy.py``.

The registers are Catalogue · Engagement · Pipeline, plus Content; the
grouping lives in ``admin_site.py``.
"""
import datetime

from django.conf import settings
from django.contrib.postgres.fields import ArrayField
from django.core.exceptions import ValidationError
from django.db import models
from django.db.models import Max, Q
from django.utils import timezone
from encrypted_model_fields.fields import EncryptedTextField

from users.models import email_lookup_hash

from . import privacy
from .vocabularies import (  # noqa: F401  (re-exported for convenience)
    ActionItemStatus, ContentItemType, ContentStatus, DataFormat,
    DigitizationStatus, DiscoverySource, EmailStatus, EngagementOutcome,
    EngagementStage, GeometryStatus, IntakeStatus, InteractionChannel,
    OrganisationType, PermissionStatus, PersonRole, PersonStatus, Priority,
    ProjectStatus, ReviewRecommendation, ReviewType, SourceType, Stage,
    VocabularyTerm,
)

USER = settings.AUTH_USER_MODEL


class TimeStampedModel(models.Model):
    """Created/updated stamps and the universal 'added by'.

    The original design proposed a separate *Logged by* field on interactions.
    It was dropped because it duplicates this (review §6).
    """

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    added_by = models.ForeignKey(
        USER, on_delete=models.SET_NULL, null=True, blank=True,
        related_name="+",
    )

    class Meta:
        abstract = True


# ==========================================================================
# CATALOGUE
# ==========================================================================

class Organisation(TimeStampedModel):
    """An archive, library, museum, university or society.

    New to the design, and load-bearing (review §4): **permission to publish is
    granted by an institution, not by an individual.** A project ends; an
    institution does not, and the rights it granted outlive both the project
    and whoever we happened to speak to. It also gives us somewhere to record a
    standing agreement before we approach a third researcher there.
    """

    name = models.CharField(max_length=255, unique=True)
    short_name = models.CharField(max_length=64, blank=True)
    org_type = models.ForeignKey(
        OrganisationType, on_delete=models.PROTECT, null=True, blank=True,
        related_name="organisations", verbose_name="type",
    )
    url = models.URLField(max_length=500, blank=True)
    ror_id = models.CharField(
        max_length=32, blank=True, db_index=True,
        verbose_name="ROR id",
        help_text="Research Organization Registry identifier, e.g. 01an7q238.",
    )
    wikidata = models.CharField(max_length=32, blank=True)
    regions = models.ManyToManyField(
        "regions.Region", blank=True, related_name="grace_organisations",
    )
    notes = models.TextField(blank=True)

    class Meta:
        ordering = ["name"]
        verbose_name = "organisation"
        verbose_name_plural = "organisations"

    def __str__(self):
        return self.name


class PersonQuerySet(models.QuerySet):
    """Queries that implement the decision-6 obligations."""

    def live(self):
        """Everyone not erased. Erasure is pseudonymisation, so the rows stay."""
        return self.filter(is_erased=False)

    def by_email(self, email):
        """Look a person up by exact address.

        Mirrors ``User.objects.by_email``. The ``email`` column is encrypted and
        so unqueryable — ``filter(email=…)`` silently matches nothing — which is
        why the indexed HMAC exists. Exact match only; partial email search is
        impossible by design.
        """
        digest = email_lookup_hash(email)
        return self.filter(email_hash=digest).first() if digest else None

    def owed_privacy_notice(self):
        """Obligation 1 — Article 14 notices not yet sent, and now overdue.

        We collect from third parties rather than from the person, so a notice
        is owed within a month of the record being created (or at first
        contact, whichever is sooner — the engagement workflow covers that
        half).

        Our own people are excluded. Article 14 covers data obtained from
        someone other than the person; staff and collaborators are told at
        hiring or on joining, under Article 13, so nagging about them would
        bury the notices that are genuinely owed. ``PersonRole.is_internal``
        is the flag, so who counts as internal stays editable.
        """
        return self.live().exclude(role__is_internal=True).filter(
            privacy_notice_sent_at__isnull=True,
            created_at__lt=privacy.privacy_notice_cutoff(),
        )

    def needing_retention_review(self, years=privacy.RETENTION_REVIEW_YEARS):
        """Obligation 4 — no interaction for ``years``, so due a human decision.

        Deliberately not an auto-delete: a long-dormant rights holder may still
        be the only person who can answer a licensing question.
        """
        cutoff = privacy.retention_cutoff(years)
        return (
            self.live()
            .annotate(last_seen=Max("interactions__occurred_on"))
            .filter(Q(last_seen__lt=cutoff.date()) |
                    Q(last_seen__isnull=True, created_at__lt=cutoff))
        )


class Person(TimeStampedModel):
    """A person we track — whether or not they have a WHG account.

    **Everyone belongs here, ourselves included.** Staff, collaborators and
    technical experts are people we track just as much as an archivist we are
    negotiating with, and holding them somewhere else would break the one
    thing GRACE exists for: open a person, see everything they touch. The
    ``PersonRole.is_internal`` flag marks our own side, and keeps colleagues
    out of the Article 14 notice queue — they are told at hiring under Art. 13,
    not by GRACE.

    **One table, with an optional link** (review §3, decision 2). Most people
    in the Catalogue will never have an account: we have merely heard of them,
    and we must not create shell accounts for people we have only read about.
    But some are contributors already, and the two must not be separate tables
    holding rival copies of the same person.

    So: where ``user`` is set, **email, ORCID and affiliation are read through
    it** and the local columns are cleared on save. There is only ever one copy
    of any fact. Use the ``resolved_*`` properties, never the raw fields.

    Personal data here is held under **legitimate interests** — see
    ``privacy.py`` for the full position, and ``pseudonymise()`` for erasure.
    """

    # --- identity (local copies; used only when there is no linked account) --
    name = models.CharField(max_length=255, help_text="Display name.")
    given_name = models.CharField(max_length=255, blank=True)
    surname = models.CharField(max_length=255, blank=True)

    user = models.OneToOneField(
        USER, on_delete=models.SET_NULL, null=True, blank=True,
        related_name="grace_person",
        help_text="Link to a WHG account, if this person has one. When set, "
                  "email, ORCID and affiliation are read from the account and "
                  "the local copies here are cleared.",
    )

    organisation = models.ForeignKey(
        Organisation, on_delete=models.SET_NULL, null=True, blank=True,
        related_name="people",
    )
    affiliation_text = models.CharField(
        max_length=255, blank=True,
        verbose_name="affiliation (free text)",
        help_text="Only for a person with no linked account and no "
                  "Organisation record. Prefer the Organisation link.",
    )
    orcid = models.CharField(max_length=64, blank=True, db_index=True)

    # --- email: encrypted at rest, matching the standard users.User sets -----
    email = EncryptedTextField(
        null=True, blank=True,
        help_text="Encrypted at rest. Not directly searchable — the hash "
                  "beside it carries equality lookups.",
    )
    email_hash = models.CharField(
        max_length=64, null=True, blank=True, db_index=True, editable=False,
        help_text="Indexed HMAC of the normalised address; kept in sync by "
                  "save(). The encrypted column itself cannot be queried.",
    )

    email_status = models.ForeignKey(
        EmailStatus, on_delete=models.SET_NULL, null=True, blank=True,
        related_name="people",
        help_text="Whether the address still works. GRACE is not the mailing "
                  "list — the sending platform owns subscriptions — but a "
                  "bounce recorded here stops us losing the person along with "
                  "their address.",
    )
    email_status_checked_on = models.DateField(
        null=True, blank=True, verbose_name="address last checked",
    )

    role = models.ForeignKey(
        PersonRole, on_delete=models.PROTECT, null=True, blank=True,
        related_name="people",
    )
    status = models.ForeignKey(
        PersonStatus, on_delete=models.PROTECT, null=True, blank=True,
        related_name="people",
    )
    regions = models.ManyToManyField(
        "regions.Region", blank=True, related_name="grace_people",
    )
    discovery_source = models.ForeignKey(
        DiscoverySource, on_delete=models.SET_NULL, null=True, blank=True,
        related_name="people",
    )
    notes = models.TextField(blank=True)

    # --- consent: SEPARATE from the lawful basis for the record itself -------
    news_consent = models.BooleanField(
        default=False,
        verbose_name="newsletter consent",
        help_text="PECR/ePrivacy consent to receive the newsletter. This is "
                  "NOT the lawful basis for holding the record — that is "
                  "legitimate interests. Two different things; do not conflate "
                  "them. For a linked account, the account's own flag wins.",
    )
    news_consent_recorded_at = models.DateTimeField(null=True, blank=True)
    news_consent_source = models.CharField(
        max_length=255, blank=True,
        help_text="How consent was obtained, e.g. 'sign-up form, 2026-08-01'.",
    )

    # --- Article 14 transparency --------------------------------------------
    privacy_notice_sent_at = models.DateTimeField(
        null=True, blank=True,
        verbose_name="privacy notice sent",
        help_text="When this person was told we hold their details. Owed "
                  "within a month of adding them, or at first contact if "
                  "sooner (GDPR Art. 14).",
    )

    # --- erasure -------------------------------------------------------------
    is_erased = models.BooleanField(
        default=False, editable=False,
        help_text="Set by pseudonymise(). The identity is gone; the "
                  "engagement history remains.",
    )
    erased_at = models.DateTimeField(null=True, blank=True, editable=False)

    objects = PersonQuerySet.as_manager()

    class Meta:
        ordering = ["name"]
        verbose_name = "person"
        verbose_name_plural = "people"

    def __str__(self):
        return self.name or f"(erased person #{self.pk})"

    def save(self, *args, **kwargs):
        # Never store the same fact twice. A linked account owns email, ORCID
        # and affiliation, so drop any local copies rather than let them drift.
        if self.user_id:
            self.email = None
            self.orcid = ""
            self.affiliation_text = ""
        self.email_hash = email_lookup_hash(self.email)
        super().save(*args, **kwargs)

    def clean(self):
        if self.news_consent and not self.news_consent_source:
            raise ValidationError({
                "news_consent_source": "Record how consent was obtained — "
                                       "unevidenced consent is not consent.",
            })

    # --- resolved accessors: always use these, never the raw fields ---------

    @property
    def resolved_email(self):
        return self.user.email if self.user_id else self.email

    @property
    def resolved_orcid(self):
        return self.user.orcid if self.user_id else self.orcid

    @property
    def resolved_affiliation(self):
        if self.user_id and self.user.affiliation:
            return self.user.affiliation
        if self.organisation_id:
            return self.organisation.name
        return self.affiliation_text

    @property
    def resolved_news_consent(self):
        """Newsletter consent. A linked account's own flag is authoritative —
        it is the one the person can change themselves."""
        return self.user.news_permitted if self.user_id else self.news_consent

    @property
    def has_account(self):
        return self.user_id is not None

    @property
    def last_interaction_on(self):
        return self.interactions.aggregate(d=Max("occurred_on"))["d"]

    @property
    def email_is_undeliverable(self):
        """The address is known bad — bounced or unsubscribed."""
        return bool(self.email_status and self.email_status.is_undeliverable)

    @property
    def is_internal(self):
        """One of us — see ``PersonRole.is_internal``."""
        return bool(self.role and self.role.is_internal)

    @property
    def privacy_notice_overdue(self):
        return (not self.is_erased
                and not self.is_internal
                and self.privacy_notice_sent_at is None
                and self.created_at is not None
                and self.created_at < privacy.privacy_notice_cutoff())

    # --- erasure -------------------------------------------------------------

    def pseudonymise(self, save=True):
        """Erase the person, keep the editorial record (obligation 3).

        A cascade delete would take the interaction history with it, and that
        history is often the only evidence of how we came by a right we now
        rely on. So the identifying fields are nulled and the Interaction rows
        keep their dates, channels and summaries.

        Note that a linked WHG account is **not** touched: that account belongs
        to the person and has its own erasure path. Only the link is dropped.
        """
        self.name = f"(erased person #{self.pk})"
        self.given_name = ""
        self.surname = ""
        self.email = None
        self.email_hash = None
        self.orcid = ""
        self.affiliation_text = ""
        self.notes = ""
        self.user = None
        self.news_consent = False
        self.news_consent_source = ""
        self.email_status = None
        self.email_status_checked_on = None
        self.is_erased = True
        self.erased_at = timezone.now()
        if save:
            # bypass save()'s hash recompute path cleanly
            super().save()
        return self


class Project(TimeStampedModel):
    """A funded or organised effort behind one or more datasets.

    Funder and grant number stay here deliberately (review §7, Q3): they are
    part of the credit chain WHG already models — the Gazetteer Register
    carries CRediT contributors, rights holder and structured citation text —
    so they feed citation metadata rather than being administrative trivia.
    """

    name = models.CharField(max_length=255, unique=True)
    description = models.TextField(blank=True)
    status = models.ForeignKey(
        ProjectStatus, on_delete=models.PROTECT, null=True, blank=True,
        related_name="projects",
    )
    organisation = models.ForeignKey(
        Organisation, on_delete=models.SET_NULL, null=True, blank=True,
        related_name="projects",
    )
    people = models.ManyToManyField(Person, blank=True, related_name="projects")
    regions = models.ManyToManyField(
        "regions.Region", blank=True, related_name="grace_projects",
    )
    funder = models.CharField(
        max_length=255, blank=True,
        help_text="Feeds citation metadata — see the model docstring.",
    )
    grant_number = models.CharField(max_length=128, blank=True)
    start_date = models.DateField(null=True, blank=True)
    end_date = models.DateField(null=True, blank=True)
    url = models.URLField(max_length=500, blank=True)
    notes = models.TextField(blank=True)

    class Meta:
        ordering = ["name"]
        verbose_name = "project"
        verbose_name_plural = "projects"

    def __str__(self):
        return self.name


class Source(TimeStampedModel):
    """The bibliography — including printed gazetteers.

    This is one of only two places where "gazetteer" keeps its bibliographic
    sense: a **printed** gazetteer, a reference work. It lives here rather than
    in a table of its own, because giving it a separate record would duplicate
    the bibliography for no gain (review §1).

    Note the two distinct relations to a tracked dataset. *Documents* is a
    documentation relation — this source describes that dataset. *Derived
    datasets* is a derivation relation — that dataset was extracted **from**
    this source. Turning printed gazetteers into WHG gazetteers is the main
    reason the bibliography exists, so losing the second would cost us the
    provenance chain for the whole programme (review §4).
    """

    title = models.CharField(max_length=500)
    volume_example = models.CharField(
        max_length=500, blank=True, verbose_name="volume / district example",
    )
    author_compiler = models.CharField(max_length=500, blank=True)

    # Keep the prose, add the numbers. Free text alone cannot be sorted or
    # filtered, which is why the numeric pair exists (review §6).
    publication_years = models.CharField(
        max_length=100, blank=True,
        help_text="As written, e.g. '1877–1896'.",
    )
    publication_year_start = models.SmallIntegerField(null=True, blank=True)
    publication_year_end = models.SmallIntegerField(null=True, blank=True)

    source_type = models.ForeignKey(
        SourceType, on_delete=models.PROTECT, null=True, blank=True,
        related_name="sources",
    )
    regions = models.ManyToManyField(
        "regions.Region", blank=True, related_name="grace_sources",
    )
    region_covered = models.TextField(
        blank=True,
        help_text="As written in the bibliography. The Regions link above is "
                  "the queryable version.",
    )
    repository = models.CharField(max_length=255, blank=True)
    source_url = models.URLField(max_length=1000, blank=True)
    digitization_status = models.ForeignKey(
        DigitizationStatus, on_delete=models.PROTECT, null=True, blank=True,
        related_name="sources",
    )
    tags = ArrayField(
        models.CharField(max_length=64), blank=True, default=list,
    )

    people = models.ManyToManyField(
        "Person", blank=True, related_name="sources",
        help_text="Authors, compilers and editors we hold a record for. "
                  "‘Author / compiler’ above stays as the bibliography writes "
                  "it; this is the queryable version, and it is what lets a "
                  "person's own page show the sources they are behind.",
    )

    documents = models.ManyToManyField(
        "TrackedDataset", blank=True, related_name="documented_by",
        help_text="Datasets this source describes.",
    )
    derived_datasets = models.ManyToManyField(
        "TrackedDataset", blank=True, related_name="derived_from_sources",
        help_text="Datasets extracted FROM this source. This is the "
                  "provenance chain — keep it accurate.",
    )

    notes = models.TextField(blank=True)

    class Meta:
        ordering = ["title"]
        verbose_name = "source"
        verbose_name_plural = "sources"

    def __str__(self):
        return self.title


# ==========================================================================
# PIPELINE
# ==========================================================================

class TrackedDatasetQuerySet(models.QuerySet):
    def prospects(self):
        """Things we are still chasing — no Register row yet."""
        return self.filter(registry__isnull=True)

    def held(self):
        """Datasets WHG actually holds — i.e. they are gazetteers now."""
        return self.filter(registry__isnull=False)


class TrackedDataset(TimeStampedModel):
    """GRACE's record of one dataset on its way in — held or merely wanted.

    **Named for its life stage.** A contributor brings a *dataset*; once
    reconciled and published it is a *gazetteer*. Same object, two stages —
    and this record exists almost entirely during the first, because the
    moment it becomes a gazetteer the authoritative record is the Register
    row, which is read through rather than copied. So the pipeline record is a
    Dataset, and "gazetteer" is left to mean the two things it means
    everywhere else at WHG: a printed reference work (a ``Source``) and a
    published WHG gazetteer (a Register entry).

    Roughly half of these are authority gazetteers — TGN, Pleiades, CHGIS —
    which never arrive as a contributed dataset at all. Calling those
    "datasets" is a slight stretch; it is a smaller one than calling a
    contributor's spreadsheet a gazetteer.

    **The load-bearing model** (review §2). It exists from the moment something
    lands on our radar, long before there is anything in WHG to point at, so
    the Register link is nullable and a row without one *is* a prospect. No
    vocabulary has to say so, and nothing gets reclassified by hand when a
    prospect lands.

    The link is to ``api.GazetteerRegistryEntry``, **not** to
    ``datasets.Dataset``, for two reasons. The Register already spans
    authorities *and* WHG datasets, so outreach about TGN or Pleiades is
    expressible — a Dataset link could not represent those conversations. And
    the Register is already maintained: licence, rights holder, CRediT
    contributors, citation text, source URL, record count, temporal extent and
    spatial coverage are all pushed by the ingest pipeline. None of that is
    re-entered here, and none of it can drift, because there is only ever one
    copy. Read it through the ``registry_*`` properties below.

    Why not simply put these editorial fields on the Register row instead? Its
    primary key is namespace-derived (``gn``, ``whg:1234``) and only exists
    post-ingest, so a prospect could have no row at all; its ``status`` is
    push-managed; and the row is public-facing. All three are set out in §2.
    """

    title = models.CharField(
        max_length=500,
        help_text="What we call it. For a prospect this may be all we have; "
                  "once linked, the Register holds the canonical name.",
    )
    registry = models.ForeignKey(
        "api.GazetteerRegistryEntry", on_delete=models.SET_NULL,
        null=True, blank=True, related_name="grace_tracking",
        verbose_name="Gazetteer Register entry",
        help_text="Leave blank while this is a prospect. Once set, licence, "
                  "rights, counts and coverage are read from here and must "
                  "not be duplicated below.",
    )
    stage = models.ForeignKey(
        Stage, on_delete=models.PROTECT, null=True, blank=True,
        related_name="datasets",
        help_text="Editorial stage only. Published / indexed are read from "
                  "the Register, not set here.",
    )
    owner = models.ForeignKey(
        USER, on_delete=models.SET_NULL, null=True, blank=True,
        related_name="grace_datasets",
        verbose_name="responsible person",
        help_text="The one accountable owner. Engagements inherit this unless "
                  "they deliberately override it.",
    )
    organisation = models.ForeignKey(
        Organisation, on_delete=models.SET_NULL, null=True, blank=True,
        related_name="datasets",
        help_text="Who can actually grant permission to publish.",
    )
    people = models.ManyToManyField(
        Person, blank=True, related_name="datasets",
    )
    project = models.ForeignKey(
        Project, on_delete=models.SET_NULL, null=True, blank=True,
        related_name="datasets",
    )
    permission_status = models.ForeignKey(
        PermissionStatus, on_delete=models.PROTECT, null=True, blank=True,
        related_name="datasets",
    )
    discovery_source = models.ForeignKey(
        DiscoverySource, on_delete=models.SET_NULL, null=True, blank=True,
        related_name="datasets",
    )
    regions = models.ManyToManyField(
        "regions.Region", blank=True, related_name="grace_datasets",
        help_text="UN M49. Several is fine and normal.",
    )
    languages = ArrayField(
        models.CharField(max_length=3), blank=True, default=list,
        help_text="ISO 639-3 codes, comma-separated — e.g. <code>eng, fra, "
                  "ara</code>. The data is multilingual; free text will not "
                  "group.",
    )

    # --- what we know before we have the data ---------------------------
    # These are NOT copies of Register fields, and the distinction is the whole
    # point: they record what we were *told* during a negotiation, at a time
    # when there is no Register row to read anything from. They stay meaningful
    # afterwards too — "they offered CC-BY and delivered CC-BY-NC" is worth
    # keeping. Once ``registry`` is set the Register is authoritative and these
    # are shown greyed beside it, never in place of it.
    data_format = models.ForeignKey(
        DataFormat, on_delete=models.SET_NULL, null=True, blank=True,
        related_name="datasets",
        help_text="How the data arrives. A Linked Places file is a day's work; "
                  "a set of page scans is a project.",
    )
    geometry_status = models.ForeignKey(
        GeometryStatus, on_delete=models.SET_NULL, null=True, blank=True,
        related_name="datasets", verbose_name="coordinates",
    )
    expected_record_count = models.IntegerField(
        null=True, blank=True, verbose_name="records (expected)",
        help_text="Roughly how many places we have been told to expect. Once "
                  "accessioned, the Register's own count supersedes this.",
    )
    expected_licence = models.ForeignKey(
        "licensing.License", on_delete=models.SET_NULL, null=True, blank=True,
        related_name="grace_expected_datasets", verbose_name="licence (offered)",
        help_text="The licence under discussion — not a copy of the Register's. "
                  "Leave blank until someone has actually said.",
    )
    expected_rights_holder = models.CharField(
        max_length=255, blank=True, verbose_name="rights holder (as told)",
    )
    reconciled_against = models.ManyToManyField(
        "api.GazetteerRegistryEntry", blank=True,
        related_name="grace_reconciliation_targets",
        verbose_name="reconciled against",
        help_text="Authorities this data has been matched to. Drawn from the "
                  "Gazetteer Register rather than typed, so it is the same "
                  "list of authorities the rest of WHG uses.",
    )

    # Prose plus numbers, for the same reason as Source.publication_years — and
    # a prospect has no Register row to read a temporal extent from.
    temporal_prose = models.CharField(
        max_length=255, blank=True, verbose_name="time period (as written)",
        help_text="e.g. '900-1200'.",
    )
    temporal_start_year = models.SmallIntegerField(null=True, blank=True)
    temporal_end_year = models.SmallIntegerField(null=True, blank=True)

    on_radar_since = models.DateField(
        null=True, blank=True,
        help_text="When this first came to our attention.",
    )
    is_active = models.BooleanField(
        default=True,
        help_text="Uncheck to shelve without deleting. People and Projects "
                  "both carry an active flag; this keeps them symmetrical.",
    )
    notes = models.TextField(blank=True)

    objects = TrackedDatasetQuerySet.as_manager()

    class Meta:
        ordering = ["title"]
        verbose_name = "dataset"
        verbose_name_plural = "datasets"

    def __str__(self):
        return self.title

    @property
    def is_prospect(self):
        """No Register row yet — we are still chasing it."""
        return self.registry_id is None

    # --- read-through accessors. Never store any of these locally. ----------

    @property
    def registry_licence(self):
        return self.registry.license if self.registry_id else None

    @property
    def registry_rights_holder(self):
        return self.registry.rights_holder if self.registry_id else None

    @property
    def registry_record_count(self):
        return self.registry.record_count if self.registry_id else None

    @property
    def registry_citation(self):
        return self.registry.citation_text if self.registry_id else None

    @property
    def registry_source_url(self):
        return self.registry.source_url if self.registry_id else None

    @property
    def registry_temporal_extent(self):
        return self.registry.temporal_extent if self.registry_id else None

    @property
    def registry_status(self):
        """Push-managed publication status. Editorial stage is separate."""
        return self.registry.status if self.registry_id else None

    @property
    def is_published(self):
        return self.registry_status == "published"

    # --- expected vs actual -------------------------------------------------
    # The Register wins wherever it has an answer. These exist so a changelist
    # column can show *something* for a prospect without pretending it is the
    # same kind of fact; ``*_is_estimate`` is what lets the UI say which.

    @property
    def effective_record_count(self):
        if self.registry_id:
            return self.registry.record_count
        return self.expected_record_count

    @property
    def effective_licence(self):
        if self.registry_id and self.registry.license_id:
            return self.registry.license
        return self.expected_licence

    @property
    def effective_rights_holder(self):
        if self.registry_id and self.registry.rights_holder:
            return self.registry.rights_holder
        return self.expected_rights_holder

    @property
    def figures_are_expectations(self):
        """True while the numbers are only what someone told us."""
        return self.registry_id is None

    # --- links out ----------------------------------------------------------

    @property
    def whg_dataset_id(self):
        """The ``datasets.Dataset`` id behind a ``whg:NNNN`` Register entry.

        Register ids are namespace-derived, so a WHG contribution carries its
        dataset id in the sub-namespace. Authorities have none.
        """
        if self.registry_id and str(self.registry_id).startswith("whg:"):
            tail = str(self.registry_id).split(":", 1)[1]
            return int(tail) if tail.isdigit() else None
        return None

    @property
    def whg_url(self):
        """Where to go to see this on WHG itself, if anywhere yet.

        Only WHG contributions have a page of their own here. An authority's
        home is its own site, which the Register already holds as
        ``source_url`` and the read-out already shows.
        """
        dataset_id = self.whg_dataset_id
        return f"/datasets/{dataset_id}/status" if dataset_id else None

    @property
    def reconciliation_status(self):
        """Read through to the contributed dataset, for WHG contributions.

        Reconciliation is not a GRACE fact and is deliberately not stored here
        — it is owned by ``datasets.Dataset.ds_status`` and would only drift if
        copied. For an authority there is nothing to report: we did not
        reconcile it, we ingested it.

        Returns the human label ("Reconciling", "Indexed"), not the raw code.
        """
        dataset_id = self.whg_dataset_id
        if not dataset_id:
            return None
        from datasets.models import Dataset
        dataset = Dataset.objects.filter(id=dataset_id).only("ds_status").first()
        return dataset.get_ds_status_display() if dataset else None


class Review(TimeStampedModel):
    """One round of review on a dataset.

    Internal editorial review is all that is worked today, but the shape is
    the same for external peer review, so ``review_type`` is a vocabulary
    rather than a boolean and nothing here assumes the reviewer is a colleague.
    When external review comes it needs a new term and a decision about
    anonymity, not a migration.

    The reviewer is a ``Person``, not a ``User``: since the People register is
    everyone — ourselves included — one field covers a colleague and an outside
    reviewer who will never have a WHG account.

    Two dates carry the failure modes. A review *sent* and never *returned* is
    a stall; a review returned and never *shared with the author* is worse,
    because from the contributor's side nothing has happened at all. Both are
    absences, so only a date can reveal them.
    """

    dataset = models.ForeignKey(
        TrackedDataset, on_delete=models.CASCADE, related_name="reviews",
    )
    reviewer = models.ForeignKey(
        "Person", on_delete=models.SET_NULL, null=True, blank=True,
        related_name="reviews",
        help_text="Anyone in the People register — a colleague or an outside "
                  "reviewer.",
    )
    review_type = models.ForeignKey(
        ReviewType, on_delete=models.PROTECT, null=True, blank=True,
        related_name="reviews", verbose_name="type",
    )
    sent_on = models.DateField(
        null=True, blank=True, verbose_name="sent to reviewer",
    )
    returned_on = models.DateField(
        null=True, blank=True, verbose_name="returned",
    )
    recommendation = models.ForeignKey(
        ReviewRecommendation, on_delete=models.PROTECT, null=True, blank=True,
        related_name="reviews",
    )
    comments = models.TextField(blank=True)
    shared_with_author_on = models.DateField(
        null=True, blank=True, verbose_name="shared with author",
        help_text="Until this is filled in, nothing has happened as far as "
                  "the contributor can tell.",
    )

    class Meta:
        ordering = ["-sent_on", "-created_at"]
        verbose_name = "review"
        verbose_name_plural = "reviews"

    def __str__(self):
        what = self.dataset.title if self.dataset_id else "review"
        return f"{what} — {self.recommendation or 'in progress'}"

    @property
    def is_outstanding(self):
        """Sent, and not back yet."""
        return bool(self.sent_on and not self.returned_on)

    @property
    def awaiting_share(self):
        """Back, and the author still has not been told."""
        return bool(self.returned_on and not self.shared_with_author_on)


# ==========================================================================
# ENGAGEMENT
# ==========================================================================

class Engagement(TimeStampedModel):
    """A thread of correspondence **with a person**.

    The single best decision in the original design (review §1): an engagement
    is with a person, not with a record. That is what makes "open one person
    and see every conversation" possible, and why the register survives a
    project being renamed or a dataset being withdrawn.

    Two rules are enforced here:

    * **One accountable owner.** ``responsible`` may be left blank, in which
      case it is inherited from the dataset's owner. Two owner fields that
      can silently disagree was a defect in the draft (review §6).
    * **An open conversation must carry a next-follow-up date.** What goes
      wrong in outreach is stalling, and a stall is the *absence* of a stage
      change — so nothing would ever fire. Making the date mandatory while the
      stage is open turns the existing reminder into a staleness alarm.
    """

    person = models.ForeignKey(
        Person, on_delete=models.CASCADE, related_name="engagements",
    )
    dataset = models.ForeignKey(
        TrackedDataset, on_delete=models.SET_NULL, null=True, blank=True,
        related_name="engagements", verbose_name="dataset",
    )
    project = models.ForeignKey(
        Project, on_delete=models.SET_NULL, null=True, blank=True,
        related_name="engagements",
    )
    organisation = models.ForeignKey(
        Organisation, on_delete=models.SET_NULL, null=True, blank=True,
        related_name="engagements",
    )
    stage = models.ForeignKey(
        EngagementStage, on_delete=models.PROTECT, null=True, blank=True,
        related_name="engagements", verbose_name="conversation stage",
    )
    priority = models.ForeignKey(
        Priority, on_delete=models.PROTECT, null=True, blank=True,
        related_name="engagements",
    )
    responsible = models.ForeignKey(
        USER, on_delete=models.SET_NULL, null=True, blank=True,
        related_name="grace_engagements",
        help_text="Leave blank to inherit the dataset's responsible person. "
                  "Set only to deliberately override it.",
    )
    next_follow_up = models.DateField(
        null=True, blank=True,
        help_text="Required while the conversation stage is open. This is the "
                  "staleness alarm: without it, a stalled conversation is "
                  "invisible because nothing changes.",
    )
    outcome = models.ForeignKey(
        EngagementOutcome, on_delete=models.PROTECT, null=True, blank=True,
        related_name="engagements",
    )
    subject = models.CharField(max_length=255, blank=True)
    # date.today(), not timezone.localdate(): this project runs with
    # USE_TZ unset (so False), which makes timezone.now() naive, and
    # localdate() rejects a naive datetime.
    opened_on = models.DateField(default=datetime.date.today)
    closed_on = models.DateField(null=True, blank=True)
    notes = models.TextField(blank=True)

    class Meta:
        ordering = ["-opened_on"]
        verbose_name = "engagement"
        verbose_name_plural = "engagements"

    def __str__(self):
        who = self.person.name if self.person_id else "?"
        return f"{who} — {self.subject or self.stage or 'engagement'}"

    def clean(self):
        if self.stage and self.stage.is_open and not self.next_follow_up:
            raise ValidationError({
                "next_follow_up": "An open conversation needs a follow-up "
                                  "date — otherwise a stall is invisible.",
            })

    @property
    def effective_responsible(self):
        """One accountable owner, inherited unless deliberately overridden."""
        if self.responsible_id:
            return self.responsible
        if self.dataset_id:
            return self.dataset.owner
        return None

    @property
    def is_open(self):
        return bool(self.stage and self.stage.is_open)

    @property
    def is_stale(self):
        """Open, and its follow-up date has passed."""
        return bool(
            self.is_open and self.next_follow_up
            and self.next_follow_up < datetime.date.today()
        )


class ActionItem(TimeStampedModel):
    """An owned, dated task on an engagement.

    Palak's own correction, and the right one (review §1): a free-text plan
    cannot be chased, an owned dated task can — and it is what makes the
    reminder rule work at all.
    """

    engagement = models.ForeignKey(
        Engagement, on_delete=models.CASCADE, related_name="action_items",
    )
    description = models.CharField(max_length=500)
    assignee = models.ForeignKey(
        USER, on_delete=models.SET_NULL, null=True, blank=True,
        related_name="grace_action_items",
    )
    due_date = models.DateField(null=True, blank=True)
    status = models.ForeignKey(
        ActionItemStatus, on_delete=models.PROTECT, null=True, blank=True,
        related_name="action_items",
    )
    completed_on = models.DateField(null=True, blank=True)

    class Meta:
        ordering = ["due_date", "-created_at"]
        verbose_name = "action item"
        verbose_name_plural = "action items"

    def __str__(self):
        return self.description

    @property
    def is_overdue(self):
        return bool(
            self.status and self.status.is_open and self.due_date
            and self.due_date < datetime.date.today()
        )


class Interaction(TimeStampedModel):
    """One dated entry in the correspondence log.

    There is no separate *logged by* field: ``added_by`` on the base class
    already records who wrote it, and two would only disagree (review §6).
    """

    engagement = models.ForeignKey(
        Engagement, on_delete=models.CASCADE, related_name="interactions",
    )
    person = models.ForeignKey(
        Person, on_delete=models.CASCADE, related_name="interactions",
        help_text="Normally the engagement's person; set explicitly so the "
                  "log can be queried per person without a join.",
    )
    channel = models.ForeignKey(
        InteractionChannel, on_delete=models.PROTECT, null=True, blank=True,
        related_name="interactions",
    )
    occurred_on = models.DateField(default=datetime.date.today)
    summary = models.TextField()

    class Meta:
        ordering = ["-occurred_on", "-created_at"]
        verbose_name = "interaction"
        verbose_name_plural = "interactions"

    def __str__(self):
        return f"{self.occurred_on}: {self.summary[:60]}"

    def save(self, *args, **kwargs):
        if not self.person_id and self.engagement_id:
            self.person = self.engagement.person
        super().save(*args, **kwargs)


# ==========================================================================
# INTAKE  (the public suggest-a-source door)
# ==========================================================================

class SourceSuggestion(TimeStampedModel):
    """A suggestion arriving from the public form.

    Decision 5: ``/contribute/`` is a **suggest-a-source** tool, not a general
    front door that triages everyone and routes dataset owners onward. The
    old design had nowhere for a public submission to land and no discovery
    value meaning "arrived from the web"; both are fixed here.

    The untriaged state is deliberately *visible* — an intake queue nobody can
    see is an intake queue nobody works.

    The submitter's address is encrypted like every other address in GRACE.
    Note that this data is collected from the person directly, so Article 13
    applies rather than Article 14: the form itself carries the notice.
    """

    title = models.CharField(max_length=500)
    author_compiler = models.CharField(max_length=500, blank=True)
    publication_years = models.CharField(max_length=100, blank=True)
    region_covered = models.TextField(blank=True)
    source_url = models.URLField(max_length=1000, blank=True)
    notes = models.TextField(blank=True)

    submitter_name = models.CharField(max_length=255, blank=True)
    submitter_email = EncryptedTextField(null=True, blank=True)
    submitter_email_hash = models.CharField(
        max_length=64, null=True, blank=True, db_index=True, editable=False,
    )
    submitter_user = models.ForeignKey(
        USER, on_delete=models.SET_NULL, null=True, blank=True,
        related_name="grace_suggestions",
        help_text="Set automatically when a signed-in user submits.",
    )

    status = models.ForeignKey(
        IntakeStatus, on_delete=models.PROTECT, null=True, blank=True,
        related_name="suggestions",
    )
    triaged_at = models.DateTimeField(null=True, blank=True)
    triaged_by = models.ForeignKey(
        USER, on_delete=models.SET_NULL, null=True, blank=True,
        related_name="grace_triaged_suggestions",
    )
    promoted_to_source = models.ForeignKey(
        Source, on_delete=models.SET_NULL, null=True, blank=True,
        related_name="from_suggestions",
    )
    promoted_to_dataset = models.ForeignKey(
        TrackedDataset, on_delete=models.SET_NULL, null=True, blank=True,
        related_name="from_suggestions",
    )
    triage_notes = models.TextField(blank=True)

    class Meta:
        ordering = ["-created_at"]
        verbose_name = "source suggestion"
        verbose_name_plural = "source suggestions"

    def __str__(self):
        return self.title

    def save(self, *args, **kwargs):
        self.submitter_email_hash = email_lookup_hash(self.submitter_email)
        super().save(*args, **kwargs)

    @property
    def is_untriaged(self):
        return bool(self.status and self.status.is_untriaged)


# ==========================================================================
# CONTENT  (the fourth register)
# ==========================================================================

class Content(TimeStampedModel):
    """Blog posts, newsletter items, talks — the output side.

    The original design filed this under Engagement, and half-admitted the
    problem ("content ideas: quick jots — planned pieces live in Content").
    Content is not engagement; it is output. So it is a register of its own
    (review §6).
    """

    title = models.CharField(max_length=500)
    content_type = models.ForeignKey(
        ContentItemType, on_delete=models.PROTECT, null=True, blank=True,
        related_name="content", verbose_name="type",
    )
    status = models.ForeignKey(
        ContentStatus, on_delete=models.PROTECT, null=True, blank=True,
        related_name="content",
    )
    author = models.ForeignKey(
        USER, on_delete=models.SET_NULL, null=True, blank=True,
        related_name="grace_content",
    )
    planned_for = models.DateField(null=True, blank=True)
    published_on = models.DateField(null=True, blank=True)
    url = models.URLField(max_length=500, blank=True)
    datasets = models.ManyToManyField(
        TrackedDataset, blank=True, related_name="content",
    )
    notes = models.TextField(blank=True)

    class Meta:
        ordering = ["-planned_for", "title"]
        verbose_name = "content item"
        verbose_name_plural = "content items"

    def __str__(self):
        return self.title
