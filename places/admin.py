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
# columns are read-only here and the model refuses deletion; the one action a
# row can undergo is withdrawal (place#308 tombstone), which keeps the PID.
from .models_locus import LocusRegistryEntry  # noqa: E402


@admin.action(description="Withdraw selected loci (tombstone; the PID is kept)")
def withdraw_loci(modeladmin, request, queryset):
    for entry in queryset:
        entry.withdraw("Withdrawn by WHG staff via the admin.", by=request.user)


class LocusRegistryEntryAdmin(admin.ModelAdmin):
    list_display = ('pid', 'kind', 'status', 'minted_by', 'minted_at', 'minted_via')
    list_filter = ('kind', 'status', 'minted_via')
    search_fields = ('pid',)
    readonly_fields = tuple(LocusRegistryEntry.BINDING_FIELDS) + ('minted_by', 'withdrawn_at', 'withdrawn_by')
    actions = [withdraw_loci]

    def has_add_permission(self, request):
        return False   # minted only through the registry API, never typed in

    def has_delete_permission(self, request, obj=None):
        return False


admin.site.register(LocusRegistryEntry, LocusRegistryEntryAdmin)
