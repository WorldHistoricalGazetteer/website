"""GRACE's controlled vocabularies — every one an editable lookup table.

DESIGN DECISION 3 (see ``developer/whg-tracker-review.html`` §5). These are
tables, **not** ``choices=`` tuples, and that is deliberate. A ``choices`` list
is frozen in code: changing it needs a developer, a migration and a deploy. A
lookup table is editable in the Django admin in ten seconds, by Palak, without
us. Self-service is the reason the team wanted a no-code tool in the first
place, and this is what preserves it.

**Do not "tidy" these into ``choices=``.** If you are tempted, the test is
whether *code branches on a specific value*. Almost nothing here does. The two
exceptions are handled properly, by a boolean flag on the row rather than by
matching a label:

* ``Stage.is_open`` and ``EngagementStage.is_open`` — the staleness alarm and
  the mandatory-follow-up rule branch on these (see ``models.Engagement``).
* ``ActionItemStatus.is_open`` — same, for outstanding tasks.

Code must branch on those flags, never on ``slug`` or ``label``, so Palak can
rename or add terms without breaking anything.
"""
from django.db import models
from django.utils.text import slugify


class VocabularyTerm(models.Model):
    """Abstract base for every GRACE lookup table.

    ``slug`` is a stable machine key: it is auto-derived from the label on first
    save and then left alone, so renaming a term for display never breaks an
    import or a fixture that referenced it.
    """

    label = models.CharField(max_length=120, unique=True)
    slug = models.SlugField(
        max_length=120, unique=True, blank=True,
        help_text="Stable machine key. Set automatically from the label; "
                  "safe to leave blank. Changing it may break imports.",
    )
    description = models.TextField(
        blank=True, help_text="Optional note on when to use this term.",
    )
    sort_order = models.PositiveSmallIntegerField(
        default=100, help_text="Lower numbers appear first in pick-lists.",
    )
    is_active = models.BooleanField(
        default=True,
        help_text="Uncheck to retire a term without deleting it. Retired terms "
                  "stay attached to existing records but are hidden from new "
                  "pick-lists.",
    )

    class Meta:
        abstract = True
        ordering = ["sort_order", "label"]

    def __str__(self):
        return self.label

    def save(self, *args, **kwargs):
        if not self.slug:
            self.slug = slugify(self.label)[:120]
        super().save(*args, **kwargs)


# --------------------------------------------------------------------------
# Catalogue vocabularies
# --------------------------------------------------------------------------

class ContactRole(VocabularyTerm):
    """What a person is to us — compiler, rights holder, archivist, and so on."""

    class Meta(VocabularyTerm.Meta):
        verbose_name = "Catalogue: contact role"
        verbose_name_plural = "Catalogue: contact roles"


class ContactStatus(VocabularyTerm):
    """Where a person stands with us — active, dormant, do-not-contact.

    Note that *dormant* is a status, not erasure. A genuine erasure request is
    handled by ``Contact.pseudonymise()``, which is a different thing entirely.
    """

    class Meta(VocabularyTerm.Meta):
        verbose_name = "Catalogue: contact status"
        verbose_name_plural = "Catalogue: contact statuses"


class OrganisationType(VocabularyTerm):
    """Archive, library, museum, university, publisher, society…"""

    class Meta(VocabularyTerm.Meta):
        verbose_name = "Catalogue: organisation type"
        verbose_name_plural = "Catalogue: organisation types"


class ProjectStatus(VocabularyTerm):
    class Meta(VocabularyTerm.Meta):
        verbose_name = "Catalogue: project status"
        verbose_name_plural = "Catalogue: project statuses"


class SourceType(VocabularyTerm):
    """What kind of source a bibliography entry is.

    One term needs care: the *printed gazetteer*. That is the single place in
    GRACE where "gazetteer" means a **lead** — something we might turn into a
    WHG gazetteer — rather than a contribution WHG holds. The label says so
    explicitly for that reason (review §8).
    """

    class Meta(VocabularyTerm.Meta):
        verbose_name = "Catalogue: source type"
        verbose_name_plural = "Catalogue: source types"


class DigitizationStatus(VocabularyTerm):
    """Whether a printed source can actually be got at: downloadable scan,
    catalogue record only, not digitised, unknown."""

    class Meta(VocabularyTerm.Meta):
        verbose_name = "Catalogue: digitization status"
        verbose_name_plural = "Catalogue: digitization statuses"


class DiscoverySource(VocabularyTerm):
    """How something came to our attention.

    Must include a **web form** term — without one there is no way to say "this
    arrived from the public" (review §4).
    """

    class Meta(VocabularyTerm.Meta):
        verbose_name = "Catalogue: discovery source"
        verbose_name_plural = "Catalogue: discovery sources"


# --------------------------------------------------------------------------
# Pipeline vocabularies
# --------------------------------------------------------------------------

class Stage(VocabularyTerm):
    """The **editorial** stage of a gazetteer on its way into WHG.

    Editorial values only. The derived, machine-known values — *published*,
    *indexed*, *not indexed* — are deliberately absent: they are read through
    ``TrackedGazetteer.registry`` instead, because the ingest push owns them and
    typing them here would only let the two copies drift (review §2).

    ``is_open`` is the one flag code branches on: an engagement sitting at an
    open stage must carry a next-follow-up date, which is what turns the
    reminder into a staleness alarm.
    """

    is_open = models.BooleanField(
        default=True,
        help_text="Is this an in-progress stage? Open stages require a "
                  "next-follow-up date on their engagements, which is what "
                  "makes the staleness alarm work. Uncheck for terminal "
                  "stages such as 'declined' or 'complete'.",
    )

    class Meta(VocabularyTerm.Meta):
        verbose_name = "Pipeline: editorial stage"
        verbose_name_plural = "Pipeline: editorial stages"


class PermissionStatus(VocabularyTerm):
    """Where we have got to on permission to publish.

    This is the field the entire licensing programme depends on, which is why
    it is a first-class vocabulary rather than a note (review §4).
    """

    class Meta(VocabularyTerm.Meta):
        verbose_name = "Pipeline: permission status"
        verbose_name_plural = "Pipeline: permission statuses"


class ReviewRecommendation(VocabularyTerm):
    """Outcome vocabulary for editorial or peer review.

    External peer review is deliberately not built out yet (review §7, Q5).
    This exists so the vocabulary is ready and aligned with the platform's
    existing pending / approved / rejected language when it is.
    """

    class Meta(VocabularyTerm.Meta):
        verbose_name = "Pipeline: review recommendation"
        verbose_name_plural = "Pipeline: review recommendations"


class IntakeStatus(VocabularyTerm):
    """Triage state of a public suggestion.

    There must be a visibly *untriaged* term — "this arrived from the public and
    nobody has looked at it yet" is the state the old design had no way to
    express (review §4).
    """

    is_untriaged = models.BooleanField(
        default=False,
        help_text="Mark the one term that means 'nobody has looked at this "
                  "yet'. New public submissions land on it, and the admin "
                  "counts it for the untriaged badge.",
    )

    class Meta(VocabularyTerm.Meta):
        verbose_name = "Pipeline: intake status"
        verbose_name_plural = "Pipeline: intake statuses"


# --------------------------------------------------------------------------
# Engagement vocabularies
# --------------------------------------------------------------------------

class EngagementStage(VocabularyTerm):
    """The state of a *conversation*, as distinct from the state of the thing
    being discussed (review §7, Q2).

    ``is_open`` drives the mandatory next-follow-up rule and the staleness
    alarm. What goes wrong in outreach is stalling, and a stall is the
    *absence* of a stage change — so an open conversation must always carry a
    date by which someone will look at it again.
    """

    is_open = models.BooleanField(
        default=True,
        help_text="Is this conversation still live? Open stages require a "
                  "next-follow-up date, which is what makes the staleness "
                  "alarm fire.",
    )

    class Meta(VocabularyTerm.Meta):
        verbose_name = "Engagement: conversation stage"
        verbose_name_plural = "Engagement: conversation stages"


class Priority(VocabularyTerm):
    """Priority of a conversation. A property of the engagement, not of the
    gazetteer (review §7, Q2)."""

    class Meta(VocabularyTerm.Meta):
        verbose_name = "Engagement: priority"
        verbose_name_plural = "Engagement: priorities"


class InteractionChannel(VocabularyTerm):
    """Email, call, meeting, conference, letter…"""

    class Meta(VocabularyTerm.Meta):
        verbose_name = "Engagement: interaction channel"
        verbose_name_plural = "Engagement: interaction channels"


class ActionItemStatus(VocabularyTerm):
    is_open = models.BooleanField(
        default=True,
        help_text="Is this an outstanding state? Open action items appear in "
                  "the overdue report.",
    )

    class Meta(VocabularyTerm.Meta):
        verbose_name = "Engagement: action-item status"
        verbose_name_plural = "Engagement: action-item statuses"


class EngagementOutcome(VocabularyTerm):
    """How a conversation ended — agreed, declined, no reply, superseded."""

    class Meta(VocabularyTerm.Meta):
        verbose_name = "Engagement: outcome"
        verbose_name_plural = "Engagement: outcomes"


# --------------------------------------------------------------------------
# Content vocabularies
# --------------------------------------------------------------------------

class ContentItemType(VocabularyTerm):
    """Blog post, newsletter item, conference paper, tutorial…

    Named ``ContentItemType`` rather than ``ContentType`` so it can never be
    confused with ``django.contrib.contenttypes``. Palak sees "Content: type".

    Content is the fourth register. The original design folded it into
    Engagement, but content is *output*, not engagement (review §6).
    """

    class Meta(VocabularyTerm.Meta):
        verbose_name = "Content: type"
        verbose_name_plural = "Content: types"


class ContentStatus(VocabularyTerm):
    class Meta(VocabularyTerm.Meta):
        verbose_name = "Content: status"
        verbose_name_plural = "Content: statuses"
