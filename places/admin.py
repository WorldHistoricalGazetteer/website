# places.admin

from django.contrib import admin
from .models import *


# appear in admin
class PlaceAdmin(admin.ModelAdmin):
    list_display = ('id', 'dataset', 'title', 'ccodes', 'src_id')
    list_filter = ('dataset',)
    search_fields = ('title',)


admin.site.register(Place, PlaceAdmin)


class SourceAdmin(admin.ModelAdmin):
    list_display = ('owner', 'src_id', 'label', 'uri')


admin.site.register(Source, SourceAdmin)


class PlaceLinkAdmin(admin.ModelAdmin):
    list_display = ('place_id', 'jsonb')


admin.site.register(PlaceLink, PlaceLinkAdmin)


class PlaceNameAdmin(admin.ModelAdmin):
    list_display = ('place_id', 'jsonb')


admin.site.register(PlaceName, PlaceNameAdmin)


class PlaceTypeAdmin(admin.ModelAdmin):
    list_display = ('place_id', 'jsonb')


admin.site.register(PlaceType, PlaceTypeAdmin)


class PlaceGeomAdmin(admin.ModelAdmin):
    list_display = ('place_id', 'jsonb')


admin.site.register(PlaceGeom, PlaceGeomAdmin)

admin.site.register(PlaceWhen)
admin.site.register(PlaceRelated)
admin.site.register(PlaceDescription)
admin.site.register(PlaceDepiction)


# The locus registry (place#172, place#325 Stage 4). Append-only: the binding
# columns are read-only here and the model refuses deletion. A row can undergo
# only one-way changes, each through an action, never by editing the form:
# withdrawal (place#308 tombstone; never reversed), staff redaction (write-once;
# the recipe text and minter stop being shown anywhere), and hiding the
# minter's name. The trigger (migration 0028) refuses anything else.
from .models_locus import LocusRegistryEntry  # noqa: E402


def _reason_action(modeladmin, request, queryset, *, verb, apply, public):
    """A one-way action that needs a stated reason (review #4): the first POST shows a
    confirmation form; the second (``apply``) applies ``apply(entry, reason, user)``
    to each selected row, refusing a blank reason."""
    from django.contrib import messages
    from django.template.response import TemplateResponse
    error = ""
    if request.POST.get("apply"):
        reason = (request.POST.get("reason") or "").strip()
        if reason:
            for entry in queryset:
                apply(entry, reason, request.user)
            messages.success(request, f"{verb.capitalize()} {queryset.count()} locus registry entr"
                                      f"{'y' if queryset.count() == 1 else 'ies'}.")
            return None
        error = "A reason is required."
    return TemplateResponse(request, "admin/places/locus_reason.html", {
        **modeladmin.admin_site.each_context(request),
        "title": f"{verb.capitalize()} loci",
        "verb": verb,
        "public": public,
        "queryset": queryset,
        "action": request.POST.get("action"),
        "error": error,
        "reason": request.POST.get("reason", ""),
        "opts": modeladmin.model._meta,
    })


@admin.action(description="Withdraw selected loci (tombstone, one-way; the PID is kept)", permissions=["change"])
def withdraw_loci(modeladmin, request, queryset):
    return _reason_action(modeladmin, request, queryset, verb="withdraw", public=True,
                          apply=lambda e, reason, user: e.withdraw(reason, by=user))


@admin.action(description="Redact selected loci (hide recipe text and minter; write-once)", permissions=["change"])
def redact_loci(modeladmin, request, queryset):
    return _reason_action(modeladmin, request, queryset, verb="redact", public=False,
                          apply=lambda e, reason, user: e.redact(reason, by=user))


@admin.action(description="Hide the minter's name on selected loci (cannot be re-shown)", permissions=["change"])
def hide_minter_loci(modeladmin, request, queryset):
    for entry in queryset:
        entry.hide_minter()


class LocusRegistryEntryAdmin(admin.ModelAdmin):
    list_display = ('pid', 'kind', 'status', 'redacted_at', 'minted_by', 'minter_public', 'minted_at', 'minted_via')
    list_filter = ('kind', 'status', 'minted_via', 'minter_public')
    search_fields = ('pid',)
    # Everything is read-only: every permitted change is a one-way action above.
    SERVING_FIELDS = ('minted_by', 'minter_public', 'status', 'withdrawn_at', 'withdrawn_by', 'withdrawn_reason',
                      'redacted_at', 'redacted_by', 'redaction_reason')
    # Never displayed once a row is withdrawn or redacted (suppressed), not even here:
    # the bytes stay in the table (the PID is their hash) but are not shown.
    RECIPE_TEXT_FIELDS = ('canonical', 'token', 'members')
    actions = [withdraw_loci, redact_loci, hide_minter_loci]

    def get_fields(self, request, obj=None):
        fields = list(LocusRegistryEntry.BINDING_FIELDS) + list(self.SERVING_FIELDS)
        if obj is not None and obj.is_suppressed:
            fields = [f for f in fields if f not in self.RECIPE_TEXT_FIELDS]
        return fields

    def get_readonly_fields(self, request, obj=None):
        return self.get_fields(request, obj)

    def has_add_permission(self, request):
        return False   # minted only through the registry API, never typed in

    def has_delete_permission(self, request, obj=None):
        return False


admin.site.register(LocusRegistryEntry, LocusRegistryEntryAdmin)
