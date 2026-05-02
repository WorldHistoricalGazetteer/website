# whg/api/admin.py
from django.contrib import admin

from .models import GazetteerRegistryEntry, UserAPIProfile


@admin.register(UserAPIProfile)
class UserAPIProfileAdmin(admin.ModelAdmin):
    list_display = ('user', 'daily_count', 'daily_limit', 'total_count')
    list_editable = ('daily_limit',)
    search_fields = ('user__username', 'user__email')
    change_list_template = "admin/api/userapiprofile/change_list.html"


@admin.register(GazetteerRegistryEntry)
class GazetteerRegistryEntryAdmin(admin.ModelAdmin):
    """Staff-only management of curatorial fields on the gazetteer registry.

    All inventory-derived fields (id, name, namespace, record_count, status,
    h3_coverage, temporal_extent, …) are read-only — they are owned by the
    ingestion pipeline's inventory push (api/views_indexing.py::
    GazetteerInventoryView). Only the curatorial flags ``core``,
    ``tileset_polygon_only``, and ``gazetteer_type`` are editable here.
    """

    list_display = (
        'id', 'name', 'namespace', 'entry_class', 'core', 'is_global',
        'tileset_polygon_only', 'gazetteer_type', 'status',
        'record_count', 'updated_at',
    )
    list_filter = (
        'entry_class', 'status', 'core', 'tileset_polygon_only',
        'gazetteer_type', 'namespace',
    )
    list_editable = ('core', 'tileset_polygon_only', 'gazetteer_type')
    search_fields = ('id', 'name', 'namespace')
    readonly_fields = (
        'id', 'name', 'description', 'namespace', 'entry_class', 'owner',
        'record_count', 'status', 'h3_coverage', 'temporal_extent',
        'is_global', 'updated_at',
    )
    fieldsets = (
        ("Curatorial (editable)", {
            'fields': ('core', 'tileset_polygon_only', 'gazetteer_type'),
        }),
        ("Inventory (managed by ingestion pipeline)", {
            'fields': (
                'id', 'name', 'description', 'namespace', 'entry_class',
                'owner', 'record_count', 'status', 'h3_coverage',
                'temporal_extent', 'is_global', 'updated_at',
            ),
        }),
    )

    @admin.display(boolean=True, description='Global')
    def is_global(self, obj):
        return obj.is_global
