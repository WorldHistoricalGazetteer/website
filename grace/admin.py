"""GRACE admin — the working surface for the editorial team.

The admin *is* the UI for now, deliberately. The earlier in-Django prototype
showed that ``list_editable`` plus filters and bulk actions gives a perfectly
serviceable triage board with no custom views to maintain, and building a
bespoke UI before anyone has used the thing would be guessing.

Two conventions to preserve:

* Every vocabulary is editable here — that is the whole point of decision 3.
  ``VocabularyAdmin`` gives inline editing of order and retirement.
* Nothing displays a raw encrypted address in a list view without going through
  the resolved accessor, and person search matches email by HMAC rather than
  by scanning (the column is encrypted and unqueryable).
"""
import datetime

from django.contrib import admin, messages
from django.contrib.auth import get_user_model
from django.db.models import Count
from django.utils import timezone
from django.utils.html import format_html

from users.models import email_lookup_hash

from . import privacy
from .admin_links import (
    NamedUserAutocompleteView, UserNameChoiceField, add_hint, changelist_url,
    panel, user_label,
)
from .models import (
    ActionItem, Content, Engagement, Interaction, Organisation, Person,
    Project, Review, Source, SourceSuggestion, TrackedDataset,
)
from .vocabularies import (
    ActionItemStatus, ContentItemType, ContentStatus, DataFormat,
    DigitizationStatus, DiscoverySource, EmailStatus, EngagementOutcome,
    EngagementStage, GeometryStatus, IntakeStatus, InteractionChannel,
    OrganisationType, PermissionStatus, PersonRole, PersonStatus, Priority,
    ProjectStatus, ReviewRecommendation, ReviewType, SourceType, Stage,
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


@admin.register(PersonRole)
class PersonRoleAdmin(VocabularyAdmin):
    """The People register is everyone, so this vocabulary has to say which
    roles are ours — that is what keeps colleagues out of the Art. 14 queue."""

    list_display = ("label", "is_internal", "sort_order", "is_active", "description")
    list_editable = ("is_internal", "sort_order", "is_active")
    list_filter = ("is_internal", "is_active")


@admin.register(EmailStatus)
class EmailStatusAdmin(VocabularyAdmin):
    list_display = ("label", "is_undeliverable", "sort_order", "is_active",
                    "description")
    list_editable = ("is_undeliverable", "sort_order", "is_active")
    list_filter = ("is_undeliverable", "is_active")


for _model in (PersonStatus, OrganisationType, ProjectStatus,
               SourceType, DigitizationStatus, DiscoverySource,
               PermissionStatus, DataFormat, GeometryStatus, ReviewType,
               ReviewRecommendation, Priority, InteractionChannel,
               EngagementOutcome, ContentItemType, ContentStatus):
    admin.site.register(_model, VocabularyAdmin)

for _model in (Stage, EngagementStage, ActionItemStatus):
    admin.site.register(_model, FlaggedVocabularyAdmin)


# --------------------------------------------------------------------------
# Catalogue
# --------------------------------------------------------------------------

class UserLabelMixin:
    """Label every account field on this ModelAdmin by name, not username."""

    def formfield_for_foreignkey(self, db_field, request, **kwargs):
        if db_field.remote_field.model is get_user_model():
            kwargs.setdefault("form_class", UserNameChoiceField)
        return super().formfield_for_foreignkey(db_field, request, **kwargs)


class ConnectionsMixin:
    """Adds the read-only "Connections" panel described in admin_links.py.

    Subclasses supply ``connection_sections(obj)``. The mixin handles the add
    form (no object yet), the fieldset, and making the field read-only.
    """

    @admin.display(description="Connected records")
    def connections(self, obj):
        if obj is None or obj.pk is None:
            return add_hint()
        return panel(self.connection_sections(obj), self.admin_site.name)

    def get_readonly_fields(self, request, obj=None):
        fields = super().get_readonly_fields(request, obj)
        return tuple(fields) + ("connections",) if "connections" not in fields \
            else fields


@admin.register(Organisation)
class OrganisationAdmin(ConnectionsMixin, admin.ModelAdmin):
    list_display = ("name", "org_type", "people_count", "datasets_count",
                    "ror_id", "url")
    list_filter = ("org_type",)
    search_fields = ("name", "short_name", "ror_id", "wikidata", "notes")
    filter_horizontal = ("regions",)
    autocomplete_fields = ("org_type",)

    fieldsets = (
        (None, {"fields": ("name", "short_name", "org_type", "url", "ror_id",
                           "wikidata", "regions", "notes")}),
        ("Connections", {"fields": ("connections",)}),
    )

    def get_queryset(self, request):
        return super().get_queryset(request).annotate(
            _people=Count("people", distinct=True),
            _datasets=Count("datasets", distinct=True),
        )

    @admin.display(description="people", ordering="_people")
    def people_count(self, obj):
        return obj._people or "—"

    @admin.display(description="datasets", ordering="_datasets")
    def datasets_count(self, obj):
        return obj._datasets or "—"

    def connection_sections(self, obj):
        site = self.admin_site.name
        return [
            ("People", obj.people.all(),
             changelist_url(Person, site, organisation__id__exact=obj.pk)),
            ("Datasets", obj.datasets.all(),
             changelist_url(TrackedDataset, site, organisation__id__exact=obj.pk)),
            ("Projects", obj.projects.all(), None),
            ("Engagements", obj.engagements.all(),
             changelist_url(Engagement, site, organisation__id__exact=obj.pk)),
        ]


class PrivacyNoticeFilter(admin.SimpleListFilter):
    """Obligation 1 — who is still owed an Article 14 notice."""

    title = "privacy notice (Art. 14)"
    parameter_name = "notice"

    def lookups(self, request, model_admin):
        return [("overdue", "Overdue"), ("sent", "Sent"),
                ("pending", "Not yet due"), ("internal", "N/A — one of us")]

    def queryset(self, request, queryset):
        if self.value() == "overdue":
            return queryset.owed_privacy_notice()
        if self.value() == "sent":
            return queryset.filter(privacy_notice_sent_at__isnull=False)
        if self.value() == "pending":
            return queryset.exclude(role__is_internal=True).filter(
                privacy_notice_sent_at__isnull=True,
                created_at__gte=privacy.privacy_notice_cutoff(),
            )
        if self.value() == "internal":
            return queryset.filter(role__is_internal=True)
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


@admin.register(Person)
class PersonAdmin(UserLabelMixin, ConnectionsMixin, admin.ModelAdmin):
    list_display = ("name", "account", "shown_affiliation", "role", "status",
                    "reach", "engagements_count", "datasets_count",
                    "notice_state", "last_seen")
    list_filter = ("role", "role__is_internal", "status", "email_status",
                   "is_erased", PrivacyNoticeFilter, RetentionFilter,
                   "discovery_source", "regions")
    search_fields = ("name", "given_name", "surname", "affiliation_text",
                     "orcid", "notes")
    autocomplete_fields = ("user", "organisation", "role", "status",
                           "email_status", "discovery_source")
    filter_horizontal = ("regions",)
    readonly_fields = ("is_erased", "erased_at", "created_at", "updated_at",
                       "lawful_basis")
    actions = ["mark_privacy_notice_sent", "pseudonymise_people"]

    fieldsets = (
        ("Identity", {
            "fields": ("name", "given_name", "surname", "user"),
            "description": "Link a WHG account where one exists — email, ORCID "
                           "and affiliation are then read from it and the local "
                           "copies below are cleared on save.",
        }),
        ("Affiliation", {"fields": ("organisation", "affiliation_text")}),
        ("Contact details", {
            "fields": ("email", "orcid", "email_status",
                       "email_status_checked_on"),
            "description": "The address is encrypted at rest. Leave blank for a "
                           "person with a linked account. GRACE is not the "
                           "mailing list — the sending platform owns "
                           "subscriptions — but a bounce recorded here stops "
                           "us losing the person along with their address.",
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
        ("Connections", {"fields": ("connections",)}),
        ("Provenance", {"classes": ("collapse",),
                        "fields": ("added_by", "created_at", "updated_at")}),
    )

    def get_queryset(self, request):
        return super().get_queryset(request).select_related(
            "role", "status", "organisation", "user", "email_status",
        ).annotate(
            _engagements=Count("engagements", distinct=True),
            _datasets=Count("datasets", distinct=True),
        )

    def connection_sections(self, obj):
        site = self.admin_site.name
        return [
            ("Engagements", obj.engagements.all(),
             changelist_url(Engagement, site, person__id__exact=obj.pk)),
            ("Datasets", obj.datasets.all(),
             changelist_url(TrackedDataset, site, people__id__exact=obj.pk)),
            ("Projects", obj.projects.all(), None),
            ("Sources", obj.sources.all(), None),
            ("Organisation", [obj.organisation], None),
            ("Reviews given", obj.reviews.all(), None),
            ("Interactions", obj.interactions.all(),
             changelist_url(Interaction, site, person__id__exact=obj.pk)),
        ]

    @admin.display(description="engagements", ordering="_engagements")
    def engagements_count(self, obj):
        return obj._engagements or "—"

    @admin.display(description="datasets", ordering="_datasets")
    def datasets_count(self, obj):
        return obj._datasets or "—"

    @admin.display(description="reach")
    def reach(self, obj):
        """Newsletter consent and whether the address still works, together.

        Two separate facts, but an editor scanning the list wants one answer to
        "can I write to this person?"
        """
        if obj.email_is_undeliverable:
            return format_html(
                '<span style="color:#a52222">{}</span>', obj.email_status.label)
        if obj.resolved_news_consent:
            return "newsletter"
        return "—"

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

    @admin.display(description="Art. 14")
    def notice_state(self, obj):
        if obj.is_internal:
            return format_html('<span style="color:#888">n/a — one of us</span>')
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
        self.message_user(request, f"Notice recorded for {n} person/people.",
                          messages.SUCCESS)

    @admin.action(description="Erase personal data (keep engagement history)")
    def pseudonymise_people(self, request, queryset):
        """Obligation 3. Not a delete — the interaction log survives intact."""
        done = 0
        for person in queryset.filter(is_erased=False):
            person.pseudonymise()
            done += 1
        self.message_user(
            request,
            f"Erased {done} person/people. Engagement and interaction history was "
            f"kept, with the identity removed.",
            messages.WARNING,
        )


@admin.register(Project)
class ProjectAdmin(ConnectionsMixin, admin.ModelAdmin):
    list_display = ("name", "status", "organisation", "datasets_count",
                    "funder", "start_date", "end_date")
    list_filter = ("status", "organisation", "regions")
    search_fields = ("name", "description", "funder", "grant_number", "notes")
    autocomplete_fields = ("status", "organisation")
    filter_horizontal = ("regions", "people")

    fieldsets = (
        (None, {"fields": ("name", "description", "status", "organisation",
                           "people", "regions")}),
        ("Funding", {
            "fields": ("funder", "grant_number", "start_date", "end_date",
                       "url"),
            "description": "Funder and grant number feed citation metadata, "
                           "not just administration — see the model docstring.",
        }),
        ("Connections", {"fields": ("connections",)}),
        ("Other", {"fields": ("notes",)}),
    )

    def get_queryset(self, request):
        return super().get_queryset(request).annotate(
            _datasets=Count("datasets", distinct=True))

    @admin.display(description="datasets", ordering="_datasets")
    def datasets_count(self, obj):
        return obj._datasets or "—"

    def connection_sections(self, obj):
        site = self.admin_site.name
        return [
            ("Datasets", obj.datasets.all(),
             changelist_url(TrackedDataset, site, project__id__exact=obj.pk)),
            ("People", obj.people.all(), None),
            ("Organisation", [obj.organisation], None),
            ("Engagements", obj.engagements.all(),
             changelist_url(Engagement, site, project__id__exact=obj.pk)),
        ]


@admin.register(Source)
class SourceAdmin(ConnectionsMixin, admin.ModelAdmin):
    list_display = ("title", "author_compiler", "publication_years",
                    "source_type", "digitization_status", "repository")
    list_filter = ("source_type", "digitization_status")
    search_fields = ("title", "volume_example", "author_compiler",
                     "region_covered", "repository", "notes")
    autocomplete_fields = ("source_type", "digitization_status")
    filter_horizontal = ("regions", "people", "documents", "derived_datasets")

    fieldsets = (
        ("Bibliographic", {
            "fields": ("title", "volume_example", "author_compiler",
                       "publication_years", "publication_year_start",
                       "publication_year_end", "source_type"),
        }),
        ("Coverage", {"fields": ("regions", "region_covered")}),
        ("Access", {"fields": ("repository", "source_url", "digitization_status")}),
        ("People", {
            "fields": ("people",),
            "description": "Authors, compilers and editors we hold a record "
                           "for. The free-text ‘author / compiler’ above stays "
                           "as the bibliography writes it.",
        }),
        ("Links to datasets", {
            "fields": ("documents", "derived_datasets"),
            "description": "‘Documents’ describes a dataset. ‘Derived "
                           "datasets’ were extracted FROM this source — that "
                           "is the provenance chain.",
        }),
        ("Connections", {"fields": ("connections",)}),
        ("Other", {"fields": ("tags", "notes")}),
    )

    def connection_sections(self, obj):
        return [
            ("People", obj.people.all(), None),
            ("Describes", obj.documents.all(), None),
            ("Datasets derived from it", obj.derived_datasets.all(), None),
            ("Arrived as a suggestion", obj.from_suggestions.all(), None),
        ]


# --------------------------------------------------------------------------
# Pipeline
# --------------------------------------------------------------------------

class ResponsibleFilter(admin.SimpleListFilter):
    """Filter by the person responsible, listed by name.

    Django's default related filter renders each user with ``__str__``, which
    on this project is the username — ORCID-derived strings like
    ``davf-sa-0009-0006-6530-9940``. Unusable for the "filter by person" the
    board was asked for, so the lookups are built by hand from display names,
    and only from people who actually own something.
    """

    title = "responsible person"
    parameter_name = "responsible"
    #: Reverse accessor from the user to the rows this filter narrows.
    relation = "grace_datasets"
    #: Field on the filtered model holding the user.
    field = "owner"

    def lookups(self, request, model_admin):
        from django.contrib.auth import get_user_model

        owners = (get_user_model().objects
                  .filter(**{f"{self.relation}__isnull": False})
                  .distinct())
        rows = [(user.pk, (user.name
                           or f"{user.given_name} {user.surname}".strip()
                           or user.username))
                for user in owners]
        return sorted(rows, key=lambda row: row[1].lower())

    def queryset(self, request, queryset):
        if self.value():
            return queryset.filter(**{f"{self.field}_id": self.value()})
        return queryset


class EngagementResponsibleFilter(ResponsibleFilter):
    relation = "grace_engagements"
    field = "responsible"


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


class ReviewInline(admin.TabularInline):
    model = Review
    extra = 0
    fields = ("review_type", "reviewer", "sent_on", "returned_on",
              "recommendation", "shared_with_author_on")
    autocomplete_fields = ("review_type", "reviewer", "recommendation")
    ordering = ("-sent_on",)
    show_change_link = True


@admin.register(TrackedDataset)
class TrackedDatasetAdmin(UserLabelMixin, ConnectionsMixin, admin.ModelAdmin):
    list_display = ("title", "kind", "stage", "owner", "permission_status",
                    "records", "reconciliation", "review_state", "on_whg",
                    "is_active")
    list_editable = ("stage", "owner", "permission_status")
    #: Widget labels come from UserLabelMixin; this is the read-only column.
    list_filter = (ProspectFilter, "stage", "permission_status",
                   ResponsibleFilter, "is_active", "data_format",
                   "geometry_status", "discovery_source", "regions")
    search_fields = ("title", "notes", "registry__id", "registry__name")
    autocomplete_fields = ("stage", "owner", "organisation", "project",
                           "permission_status", "discovery_source", "registry",
                           "data_format", "geometry_status", "expected_licence")
    filter_horizontal = ("regions", "people", "reconciled_against")
    readonly_fields = ("registry_readout", "created_at", "updated_at")
    inlines = [ReviewInline]

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
        ("The data", {
            "fields": ("data_format", "geometry_status", "languages",
                       "reconciled_against"),
            "description": "Languages are ISO 639-3 codes, comma-separated. "
                           "‘Reconciled against’ is drawn from the Gazetteer "
                           "Register, so it is the same list of authorities "
                           "the rest of WHG uses.",
        }),
        ("What we have been told", {
            "fields": ("expected_record_count", "expected_licence",
                       "expected_rights_holder"),
            "description": "What someone said during a negotiation, before we "
                           "had the data. These are NOT copies of Register "
                           "fields: once this is accessioned the Register is "
                           "authoritative and these stay only as a record of "
                           "what was offered.",
        }),
        ("Who and where", {
            "fields": ("organisation", "people", "project", "regions"),
        }),
        ("Time period", {
            "fields": ("temporal_prose", "temporal_start_year",
                       "temporal_end_year"),
            "description": "A prospect has no Register entry to read a "
                           "temporal extent from, so it is recorded here.",
        }),
        ("Connections", {"fields": ("connections",)}),
        ("Provenance", {"classes": ("collapse",),
                        "fields": ("added_by", "created_at", "updated_at")}),
    )

    def get_queryset(self, request):
        return super().get_queryset(request).select_related(
            "registry", "registry__license", "stage", "owner",
            "permission_status")

    def formfield_for_manytomany(self, db_field, request, **kwargs):
        """Only authorities are reconciliation targets.

        The Register also holds a row per WHG dataset, and offering 40-odd of
        those alongside GeoNames would make the picker useless.
        """
        if db_field.name == "reconciled_against":
            from api.models import GazetteerRegistryEntry
            kwargs["queryset"] = GazetteerRegistryEntry.objects.filter(
                entry_class="authority").order_by("name")
        return super().formfield_for_manytomany(db_field, request, **kwargs)

    def connection_sections(self, obj):
        site = self.admin_site.name
        return [
            ("Project", [obj.project], None),
            ("Organisation", [obj.organisation], None),
            ("People", obj.people.all(), None),
            ("Engagements", obj.engagements.all(),
             changelist_url(Engagement, site, dataset__id__exact=obj.pk)),
            ("Reviews", obj.reviews.all(), None),
            ("Described by", obj.documented_by.all(), None),
            ("Derived from", obj.derived_from_sources.all(), None),
            ("Content about it", obj.content.all(), None),
            ("Arrived as a suggestion", obj.from_suggestions.all(), None),
        ]

    @admin.display(description="kind")
    def kind(self, obj):
        return "prospect" if obj.is_prospect else "held"

    @admin.display(description="records")
    def records(self, obj):
        """The Register's count where there is one, otherwise what we were
        told — marked so the two are never mistaken for each other."""
        count = obj.effective_record_count
        if not count:
            return "—"
        if obj.figures_are_expectations:
            return format_html(
                '<span style="color:var(--body-quiet-color)" '
                'title="Expected — no Register entry yet">~{}</span>',
                f"{count:,}")
        return f"{count:,}"

    @admin.display(description="reconciliation")
    def reconciliation(self, obj):
        """Read through to the contributed dataset — never stored here."""
        return obj.reconciliation_status or "—"

    @admin.display(description="review")
    def review_state(self, obj):
        """The two review failure modes, which are both absences of an event."""
        reviews = list(obj.reviews.all())
        if not reviews:
            return "—"
        if any(r.awaiting_share for r in reviews):
            return format_html(
                '<strong style="color:#a52222">author not told</strong>')
        if any(r.is_outstanding for r in reviews):
            return format_html('<span style="color:#8a5810">out</span>')
        latest = reviews[0]
        return str(latest.recommendation or "returned")

    @admin.display(description="on WHG")
    def on_whg(self, obj):
        url = obj.whg_url
        return format_html('<a href="{}">open</a>', url) if url else "—"

    @admin.display(description="Read from the Gazetteer Register")
    def registry_readout(self, obj):
        """Everything the Register already knows. Never stored here."""
        if not obj.registry_id:
            rows = [
                ("Records (expected)",
                 f"{obj.expected_record_count:,}"
                 if obj.expected_record_count else "—"),
                ("Licence (offered)", obj.expected_licence or "—"),
                ("Rights holder (as told)", obj.expected_rights_holder or "—"),
            ]
            return format_html(
                "<p style='margin:0 0 6px'>No Register entry — this is a "
                "prospect. Below is what we have been <em>told</em>, which is "
                "a different kind of fact.</p><table style='border:0'>{}</table>",
                format_html("".join(
                    "<tr><td style='padding:1px 12px 1px 0;color:#666'>{}</td>"
                    "<td>{}</td></tr>".format(k, v) for k, v in rows)),
            )
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
        # Show what we were told beside what arrived, but only where the two
        # actually differ — a silent mismatch is the thing worth catching.
        if (obj.expected_record_count
                and abs(obj.expected_record_count - r.record_count)
                > max(1, r.record_count * 0.05)):
            rows.append(("⚠ expected records",
                         f"{obj.expected_record_count:,} (as told)"))
        if obj.expected_licence_id and obj.expected_licence_id != r.license_id:
            rows.append(("⚠ licence offered", str(obj.expected_licence)))
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
class SourceSuggestionAdmin(UserLabelMixin, admin.ModelAdmin):
    list_display = ("title", "author_compiler", "status", "submitter_name",
                    "created_at", "promoted")
    list_editable = ("status",)
    list_filter = (UntriagedFilter, "status")
    search_fields = ("title", "author_compiler", "region_covered", "notes",
                     "submitter_name", "triage_notes")
    autocomplete_fields = ("status", "promoted_to_source", "promoted_to_dataset")
    readonly_fields = ("created_at", "submitter_user", "triaged_at", "triaged_by")
    actions = ["promote_to_source"]

    @admin.display(description="promoted", boolean=True)
    def promoted(self, obj):
        return bool(obj.promoted_to_source_id or obj.promoted_to_dataset_id)

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


class ReviewOutstandingFilter(admin.SimpleListFilter):
    """Both review failure modes are absences, so only a date reveals them."""

    title = "state"
    parameter_name = "state"

    def lookups(self, request, model_admin):
        return [("out", "Out with the reviewer"),
                ("unshared", "Returned, author not told"),
                ("done", "Shared with the author")]

    def queryset(self, request, queryset):
        if self.value() == "out":
            return queryset.filter(sent_on__isnull=False,
                                   returned_on__isnull=True)
        if self.value() == "unshared":
            return queryset.filter(returned_on__isnull=False,
                                   shared_with_author_on__isnull=True)
        if self.value() == "done":
            return queryset.filter(shared_with_author_on__isnull=False)
        return queryset


@admin.register(Review)
class ReviewAdmin(admin.ModelAdmin):
    list_display = ("dataset", "review_type", "reviewer", "sent_on",
                    "returned_on", "recommendation", "shared_with_author_on",
                    "state")
    list_editable = ("returned_on", "recommendation", "shared_with_author_on")
    list_filter = (ReviewOutstandingFilter, "review_type", "recommendation")
    search_fields = ("dataset__title", "reviewer__name", "comments")
    autocomplete_fields = ("dataset", "reviewer", "review_type",
                           "recommendation")
    date_hierarchy = "sent_on"

    @admin.display(description="state")
    def state(self, obj):
        if obj.awaiting_share:
            return format_html(
                '<strong style="color:#a52222">author not told</strong>')
        if obj.is_outstanding:
            return format_html('<span style="color:#8a5810">out</span>')
        return "—" if not obj.returned_on else "shared"


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
class EngagementAdmin(UserLabelMixin, ConnectionsMixin, admin.ModelAdmin):
    list_display = ("person", "subject", "dataset", "stage",
                    "priority", "who", "next_follow_up", "state")
    list_editable = ("stage", "priority", "next_follow_up")
    list_filter = (StaleFilter, "stage", "priority",
                   EngagementResponsibleFilter, "outcome")
    search_fields = ("subject", "notes", "person__name",
                     "dataset__title")
    autocomplete_fields = ("person", "dataset", "project",
                           "organisation", "stage", "priority", "responsible",
                           "outcome")
    inlines = [InteractionInline, ActionItemInline]
    date_hierarchy = "opened_on"

    fieldsets = (
        ("The conversation", {
            "fields": ("person", "subject", "stage", "priority", "responsible",
                       "next_follow_up", "outcome", "opened_on", "closed_on"),
            "description": "An engagement is with a <em>person</em>, not with a "
                           "record — that is what makes ‘open one person and "
                           "see every conversation’ possible.",
        }),
        ("What it is about", {
            "fields": ("dataset", "project", "organisation")}),
        ("Connections", {"fields": ("connections",)}),
        ("Other", {"fields": ("notes",)}),
    )

    def get_queryset(self, request):
        return super().get_queryset(request).select_related(
            "person", "dataset", "stage", "priority", "responsible",
            "dataset__owner")

    def connection_sections(self, obj):
        site = self.admin_site.name
        return [
            ("Person", [obj.person], None),
            ("Dataset", [obj.dataset], None),
            ("Project", [obj.project], None),
            ("Organisation", [obj.organisation], None),
            ("Interactions", obj.interactions.all(),
             changelist_url(Interaction, site, engagement__id__exact=obj.pk)),
            ("Action items", obj.action_items.all(), None),
        ]

    @admin.display(description="responsible")
    def who(self, obj):
        person = obj.effective_responsible
        if not person:
            return "—"
        inherited = "" if obj.responsible_id else " (inherited)"
        return f"{user_label(person)}{inherited}"

    @admin.display(description="state")
    def state(self, obj):
        if obj.is_stale:
            return format_html('<strong style="color:#a52222">stalled</strong>')
        return "open" if obj.is_open else "closed"


@admin.register(Interaction)
class InteractionAdmin(UserLabelMixin, admin.ModelAdmin):
    list_display = ("occurred_on", "person", "channel", "summary", "added_by")
    list_filter = ("channel",)
    search_fields = ("summary", "person__name")
    autocomplete_fields = ("engagement", "person", "channel")
    date_hierarchy = "occurred_on"


@admin.register(ActionItem)
class ActionItemAdmin(UserLabelMixin, admin.ModelAdmin):
    list_display = ("description", "assignee", "due_date", "status", "overdue")
    list_editable = ("assignee", "due_date", "status")
    list_filter = ("status", "assignee")
    search_fields = ("description", "engagement__person__name")
    autocomplete_fields = ("engagement", "assignee", "status")

    @admin.display(description="overdue", boolean=True)
    def overdue(self, obj):
        return obj.is_overdue


# --------------------------------------------------------------------------
# Content
# --------------------------------------------------------------------------

@admin.register(Content)
class ContentAdmin(UserLabelMixin, admin.ModelAdmin):
    list_display = ("title", "content_type", "status", "author", "planned_for",
                    "published_on")
    list_editable = ("status", "planned_for")
    list_filter = ("content_type", "status", "author")
    search_fields = ("title", "notes")
    autocomplete_fields = ("content_type", "status", "author")
    filter_horizontal = ("datasets",)


# --------------------------------------------------------------------------
# Mirror everything above onto GRACE's own admin site at /grace/admin/.
# Done here, at the bottom of this module, because by this point every
# ModelAdmin is defined and registered — so the mirror cannot miss one.
# --------------------------------------------------------------------------

from .admin_site import register_grace_models  # noqa: E402

register_grace_models()
