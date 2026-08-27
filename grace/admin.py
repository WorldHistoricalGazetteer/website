"""GRACE admin — the working surface for the editorial team.

The admin *is* the UI for now, deliberately. The earlier in-Django prototype
showed that ``list_editable`` plus filters and bulk actions gives a perfectly
serviceable triage board with no custom views to maintain, and building a
bespoke UI before anyone has used the thing would be guessing.

Two conventions to preserve:

* Every vocabulary is editable here — that is the whole point of decision 3.
  ``VocabularyAdmin`` gives inline editing of order and retirement.
* Nothing displays a raw encrypted address in a list view without going through
  the resolved accessor, and contact search matches email by HMAC rather than
  by scanning (the column is encrypted and unqueryable).
"""
import datetime

from django.contrib import admin, messages
from django.utils import timezone
from django.utils.html import format_html

from users.models import email_lookup_hash

from . import privacy
from .models import (
    ActionItem, Contact, Content, Engagement, Interaction, Organisation,
    Project, Source, SourceSuggestion, TrackedGazetteer,
)
from .vocabularies import (
    ActionItemStatus, ContactRole, ContactStatus, ContentItemType,
    ContentStatus, DigitizationStatus, DiscoverySource, EngagementOutcome,
    EngagementStage, IntakeStatus, InteractionChannel, OrganisationType,
    PermissionStatus, Priority, ProjectStatus, ReviewRecommendation,
    SourceType, Stage,
)


# --------------------------------------------------------------------------
# Vocabularies
# --------------------------------------------------------------------------

class VocabularyAdmin(admin.ModelAdmin):
    """Shared admin for every lookup table.

    Palak owns these. Adding a term is: type a label, save. The slug fills
    itself in and is then left alone, so renaming a term for display never
    breaks an import that referenced it.
    """

    list_display = ("label", "sort_order", "is_active", "description")
    list_editable = ("sort_order", "is_active")
    list_filter = ("is_active",)
    search_fields = ("label", "slug", "description")
    readonly_fields = ("slug",)
    ordering = ("sort_order", "label")


class FlaggedVocabularyAdmin(VocabularyAdmin):
    """For the three vocabularies carrying an ``is_open`` flag that code reads."""

    list_display = ("label", "is_open", "sort_order", "is_active", "description")
    list_editable = ("is_open", "sort_order", "is_active")
    list_filter = ("is_open", "is_active")


@admin.register(IntakeStatus)
class IntakeStatusAdmin(VocabularyAdmin):
    list_display = ("label", "is_untriaged", "sort_order", "is_active", "description")
    list_editable = ("is_untriaged", "sort_order", "is_active")
    list_filter = ("is_untriaged", "is_active")


for _model in (ContactRole, ContactStatus, OrganisationType, ProjectStatus,
               SourceType, DigitizationStatus, DiscoverySource,
               PermissionStatus, ReviewRecommendation, Priority,
               InteractionChannel, EngagementOutcome, ContentItemType,
               ContentStatus):
    admin.site.register(_model, VocabularyAdmin)

for _model in (Stage, EngagementStage, ActionItemStatus):
    admin.site.register(_model, FlaggedVocabularyAdmin)


# --------------------------------------------------------------------------
# Catalogue
# --------------------------------------------------------------------------

@admin.register(Organisation)
class OrganisationAdmin(admin.ModelAdmin):
    list_display = ("name", "org_type", "ror_id", "url")
    list_filter = ("org_type",)
    search_fields = ("name", "short_name", "ror_id", "wikidata", "notes")
    filter_horizontal = ("regions",)
    autocomplete_fields = ("org_type",)


class PrivacyNoticeFilter(admin.SimpleListFilter):
    """Obligation 1 — who is still owed an Article 14 notice."""

    title = "privacy notice (Art. 14)"
    parameter_name = "notice"

    def lookups(self, request, model_admin):
        return [("overdue", "Overdue"), ("sent", "Sent"), ("pending", "Not yet due")]

    def queryset(self, request, queryset):
        if self.value() == "overdue":
            return queryset.owed_privacy_notice()
        if self.value() == "sent":
            return queryset.filter(privacy_notice_sent_at__isnull=False)
        if self.value() == "pending":
            return queryset.filter(
                privacy_notice_sent_at__isnull=True,
                created_at__gte=privacy.privacy_notice_cutoff(),
            )
        return queryset


class RetentionFilter(admin.SimpleListFilter):
    """Obligation 4 — three years without an interaction."""

    title = "retention"
    parameter_name = "retention"

    def lookups(self, request, model_admin):
        return [("due", f"Review due (>{privacy.RETENTION_REVIEW_YEARS}y quiet)")]

    def queryset(self, request, queryset):
        if self.value() == "due":
            return queryset.needing_retention_review()
        return queryset


@admin.register(Contact)
class ContactAdmin(admin.ModelAdmin):
    list_display = ("name", "account", "shown_affiliation", "role", "status",
                    "newsletter", "notice_state", "last_seen")
    list_filter = ("role", "status", "is_erased", PrivacyNoticeFilter,
                   RetentionFilter, "discovery_source")
    search_fields = ("name", "given_name", "surname", "affiliation_text",
                     "orcid", "notes")
    autocomplete_fields = ("user", "organisation", "role", "status",
                           "discovery_source")
    filter_horizontal = ("regions",)
    readonly_fields = ("is_erased", "erased_at", "created_at", "updated_at",
                       "lawful_basis")
    actions = ["mark_privacy_notice_sent", "pseudonymise_contacts"]

    fieldsets = (
        ("Identity", {
            "fields": ("name", "given_name", "surname", "user"),
            "description": "Link a WHG account where one exists — email, ORCID "
                           "and affiliation are then read from it and the local "
                           "copies below are cleared on save.",
        }),
        ("Affiliation", {"fields": ("organisation", "affiliation_text")}),
        ("Contact details", {
            "fields": ("email", "orcid"),
            "description": "The address is encrypted at rest. Leave blank for a "
                           "person with a linked account.",
        }),
        ("Editorial", {"fields": ("role", "status", "regions",
                                  "discovery_source", "notes")}),
        ("Data protection", {
            "fields": ("lawful_basis", "privacy_notice_sent_at", "news_consent",
                       "news_consent_recorded_at", "news_consent_source",
                       "is_erased", "erased_at"),
            "description": "Newsletter consent is separate from the lawful "
                           "basis for holding this record. Do not conflate them.",
        }),
        ("Provenance", {"classes": ("collapse",),
                        "fields": ("added_by", "created_at", "updated_at")}),
    )

    @admin.display(description="lawful basis")
    def lawful_basis(self, obj):
        return privacy.LAWFUL_BASIS

    @admin.display(description="account")
    def account(self, obj):
        """Not boolean=True: 209 red crosses down the page read as errors when
        they only mean "this person never signed up", which is the norm."""
        return "linked" if obj.has_account else "—"

    @admin.display(description="affiliation")
    def shown_affiliation(self, obj):
        return obj.resolved_affiliation or "—"

    @admin.display(description="newsletter")
    def newsletter(self, obj):
        return "yes" if obj.resolved_news_consent else "—"

    @admin.display(description="Art. 14")
    def notice_state(self, obj):
        if obj.privacy_notice_sent_at:
            return format_html('<span style="color:#1a7a3c">sent</span>')
        if obj.privacy_notice_overdue:
            return format_html('<strong style="color:#a52222">overdue</strong>')
        return format_html('<span style="color:#888">not yet due</span>')

    @admin.display(description="last interaction")
    def last_seen(self, obj):
        return obj.last_interaction_on or "—"

    def get_search_results(self, request, queryset, search_term):
        """Also match an exact email address.

        The column is encrypted, so ``icontains`` can never hit it. An exact
        address is looked up by its HMAC instead, which is what the index is
        for. Partial email search is genuinely impossible and that is by design.
        """
        qs, distinct = super().get_search_results(request, queryset, search_term)
        digest = email_lookup_hash(search_term)
        if digest:
            qs |= self.model.objects.filter(email_hash=digest)
        return qs, distinct

    @admin.action(description="Record that the Art. 14 privacy notice was sent")
    def mark_privacy_notice_sent(self, request, queryset):
        n = queryset.filter(privacy_notice_sent_at__isnull=True).update(
            privacy_notice_sent_at=timezone.now())
        self.message_user(request, f"Notice recorded for {n} contact(s).",
                          messages.SUCCESS)

    @admin.action(description="Erase personal data (keep engagement history)")
    def pseudonymise_contacts(self, request, queryset):
        """Obligation 3. Not a delete — the interaction log survives intact."""
        done = 0
        for contact in queryset.filter(is_erased=False):
            contact.pseudonymise()
            done += 1
        self.message_user(
            request,
            f"Erased {done} contact(s). Engagement and interaction history was "
            f"kept, with the identity removed.",
            messages.WARNING,
        )


@admin.register(Project)
class ProjectAdmin(admin.ModelAdmin):
    list_display = ("name", "status", "organisation", "funder", "start_date", "end_date")
    list_filter = ("status", "organisation")
    search_fields = ("name", "description", "funder", "grant_number", "notes")
    autocomplete_fields = ("status", "organisation")
    filter_horizontal = ("regions", "contacts")


@admin.register(Source)
class SourceAdmin(admin.ModelAdmin):
    list_display = ("title", "author_compiler", "publication_years",
                    "source_type", "digitization_status", "repository")
    list_filter = ("source_type", "digitization_status")
    search_fields = ("title", "volume_example", "author_compiler",
                     "region_covered", "repository", "notes")
    autocomplete_fields = ("source_type", "digitization_status")
    filter_horizontal = ("regions", "documents", "derived_gazetteers")

    fieldsets = (
        ("Bibliographic", {
            "fields": ("title", "volume_example", "author_compiler",
                       "publication_years", "publication_year_start",
                       "publication_year_end", "source_type"),
        }),
        ("Coverage", {"fields": ("regions", "region_covered")}),
        ("Access", {"fields": ("repository", "source_url", "digitization_status")}),
        ("Links to gazetteers", {
            "fields": ("documents", "derived_gazetteers"),
            "description": "‘Documents’ describes a gazetteer. ‘Derived "
                           "gazetteers’ were extracted FROM this source — that "
                           "is the provenance chain.",
        }),
        ("Other", {"fields": ("tags", "notes")}),
    )


# --------------------------------------------------------------------------
# Pipeline
# --------------------------------------------------------------------------

class ProspectFilter(admin.SimpleListFilter):
    """A row with no Register link *is* a prospect — no vocabulary needed."""

    title = "held or prospect"
    parameter_name = "prospect"

    def lookups(self, request, model_admin):
        return [("yes", "Prospect (no Register entry)"),
                ("no", "Held by WHG (linked)")]

    def queryset(self, request, queryset):
        if self.value() == "yes":
            return queryset.prospects()
        if self.value() == "no":
            return queryset.held()
        return queryset


@admin.register(TrackedGazetteer)
class TrackedGazetteerAdmin(admin.ModelAdmin):
    list_display = ("title", "kind", "stage", "owner", "permission_status",
                    "registry_records", "is_active")
    list_editable = ("stage", "owner", "permission_status")
    list_filter = (ProspectFilter, "stage", "permission_status", "owner",
                   "is_active", "discovery_source")
    search_fields = ("title", "notes", "registry__id", "registry__name")
    autocomplete_fields = ("stage", "owner", "organisation", "project",
                           "permission_status", "discovery_source", "registry")
    filter_horizontal = ("regions", "contacts")
    readonly_fields = ("registry_readout", "created_at", "updated_at")

    fieldsets = (
        ("What it is", {
            "fields": ("title", "registry", "registry_readout"),
            "description": "Leave the Register entry blank while this is a "
                           "prospect. Once set, everything in the read-out is "
                           "maintained by the ingest pipeline — do not retype "
                           "any of it below.",
        }),
        ("Editorial", {
            "fields": ("stage", "owner", "permission_status", "is_active",
                       "on_radar_since", "discovery_source", "notes"),
        }),
        ("Who and where", {
            "fields": ("organisation", "contacts", "project", "regions",
                       "languages"),
        }),
        ("Time period", {
            "fields": ("temporal_prose", "temporal_start_year",
                       "temporal_end_year"),
            "description": "A prospect has no Register entry to read a "
                           "temporal extent from, so it is recorded here.",
        }),
        ("Provenance", {"classes": ("collapse",),
                        "fields": ("added_by", "created_at", "updated_at")}),
    )

    @admin.display(description="kind")
    def kind(self, obj):
        return "prospect" if obj.is_prospect else "held"

    @admin.display(description="records")
    def registry_records(self, obj):
        count = obj.registry_record_count
        return f"{count:,}" if count else "—"

    @admin.display(description="Read from the Gazetteer Register")
    def registry_readout(self, obj):
        """Everything the Register already knows. Never stored here."""
        if not obj.registry_id:
            return "No Register entry — this is a prospect."
        r = obj.registry
        rows = [
            ("Register id", r.id),
            ("Class", r.entry_class),
            ("Publication status", r.status),
            ("Records", f"{r.record_count:,}"),
            ("Licence", r.license.label if r.license_id else "—"),
            ("Rights holder", r.rights_holder or "—"),
            ("Source URL", r.source_url or "—"),
            ("Citation", r.citation_text or "—"),
        ]
        return format_html(
            "<table style='border:0'>{}</table>",
            format_html("".join(
                "<tr><td style='padding:1px 12px 1px 0;color:#666'>{}</td>"
                "<td>{}</td></tr>".format(k, v) for k, v in rows
            )),
        )


class UntriagedFilter(admin.SimpleListFilter):
    title = "triage"
    parameter_name = "triage"

    def lookups(self, request, model_admin):
        return [("untriaged", "Untriaged"), ("triaged", "Triaged")]

    def queryset(self, request, queryset):
        if self.value() == "untriaged":
            return queryset.filter(status__is_untriaged=True)
        if self.value() == "triaged":
            return queryset.exclude(status__is_untriaged=True)
        return queryset


@admin.register(SourceSuggestion)
class SourceSuggestionAdmin(admin.ModelAdmin):
    list_display = ("title", "author_compiler", "status", "submitter_name",
                    "created_at", "promoted")
    list_editable = ("status",)
    list_filter = (UntriagedFilter, "status")
    search_fields = ("title", "author_compiler", "region_covered", "notes",
                     "submitter_name", "triage_notes")
    autocomplete_fields = ("status", "promoted_to_source", "promoted_to_gazetteer")
    readonly_fields = ("created_at", "submitter_user", "triaged_at", "triaged_by")
    actions = ["promote_to_source"]

    @admin.display(description="promoted", boolean=True)
    def promoted(self, obj):
        return bool(obj.promoted_to_source_id or obj.promoted_to_gazetteer_id)

    @admin.action(description="Promote to a Source (bibliography) record")
    def promote_to_source(self, request, queryset):
        made = 0
        for s in queryset.filter(promoted_to_source__isnull=True):
            source = Source.objects.create(
                title=s.title,
                author_compiler=s.author_compiler,
                publication_years=s.publication_years,
                region_covered=s.region_covered,
                source_url=s.source_url,
                notes=s.notes,
                added_by=request.user,
            )
            s.promoted_to_source = source
            s.triaged_at = timezone.now()
            s.triaged_by = request.user
            s.save()
            made += 1
        self.message_user(request, f"Promoted {made} suggestion(s) to Sources.",
                          messages.SUCCESS)


# --------------------------------------------------------------------------
# Engagement
# --------------------------------------------------------------------------

class InteractionInline(admin.TabularInline):
    model = Interaction
    extra = 0
    fields = ("occurred_on", "channel", "summary", "added_by")
    autocomplete_fields = ("channel",)
    ordering = ("-occurred_on",)


class ActionItemInline(admin.TabularInline):
    model = ActionItem
    extra = 0
    fields = ("description", "assignee", "due_date", "status", "completed_on")
    autocomplete_fields = ("assignee", "status")


class StaleFilter(admin.SimpleListFilter):
    """The failure mode that matters: a conversation nobody has touched.

    A stall is the *absence* of a stage change, so it can only be found by
    looking at the follow-up date.
    """

    title = "staleness"
    parameter_name = "stale"

    def lookups(self, request, model_admin):
        return [("stale", "Stalled (follow-up overdue)"),
                ("open", "Open"), ("closed", "Closed")]

    def queryset(self, request, queryset):
        if self.value() == "stale":
            return queryset.filter(stage__is_open=True,
                                   next_follow_up__lt=datetime.date.today())
        if self.value() == "open":
            return queryset.filter(stage__is_open=True)
        if self.value() == "closed":
            return queryset.filter(stage__is_open=False)
        return queryset


@admin.register(Engagement)
class EngagementAdmin(admin.ModelAdmin):
    list_display = ("contact", "subject", "tracked_gazetteer", "stage",
                    "priority", "who", "next_follow_up", "state")
    list_editable = ("stage", "priority", "next_follow_up")
    list_filter = (StaleFilter, "stage", "priority", "responsible", "outcome")
    search_fields = ("subject", "notes", "contact__name",
                     "tracked_gazetteer__title")
    autocomplete_fields = ("contact", "tracked_gazetteer", "project",
                           "organisation", "stage", "priority", "responsible",
                           "outcome")
    inlines = [InteractionInline, ActionItemInline]
    date_hierarchy = "opened_on"

    @admin.display(description="responsible")
    def who(self, obj):
        person = obj.effective_responsible
        if not person:
            return "—"
        inherited = "" if obj.responsible_id else " (inherited)"
        return f"{person}{inherited}"

    @admin.display(description="state")
    def state(self, obj):
        if obj.is_stale:
            return format_html('<strong style="color:#a52222">stalled</strong>')
        return "open" if obj.is_open else "closed"


@admin.register(Interaction)
class InteractionAdmin(admin.ModelAdmin):
    list_display = ("occurred_on", "contact", "channel", "summary", "added_by")
    list_filter = ("channel",)
    search_fields = ("summary", "contact__name")
    autocomplete_fields = ("engagement", "contact", "channel")
    date_hierarchy = "occurred_on"


@admin.register(ActionItem)
class ActionItemAdmin(admin.ModelAdmin):
    list_display = ("description", "assignee", "due_date", "status", "overdue")
    list_editable = ("assignee", "due_date", "status")
    list_filter = ("status", "assignee")
    search_fields = ("description", "engagement__contact__name")
    autocomplete_fields = ("engagement", "assignee", "status")

    @admin.display(description="overdue", boolean=True)
    def overdue(self, obj):
        return obj.is_overdue


# --------------------------------------------------------------------------
# Content
# --------------------------------------------------------------------------

@admin.register(Content)
class ContentAdmin(admin.ModelAdmin):
    list_display = ("title", "content_type", "status", "author", "planned_for",
                    "published_on")
    list_editable = ("status", "planned_for")
    list_filter = ("content_type", "status", "author")
    search_fields = ("title", "notes")
    autocomplete_fields = ("content_type", "status", "author")
    filter_horizontal = ("gazetteers",)


# --------------------------------------------------------------------------
# Mirror everything above onto GRACE's own admin site at /grace/admin/.
# Done here, at the bottom of this module, because by this point every
# ModelAdmin is defined and registered — so the mirror cannot miss one.
# --------------------------------------------------------------------------

from .admin_site import register_grace_models  # noqa: E402

register_grace_models()
