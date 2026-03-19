from django.contrib import admin

from .models import SiteSetting


# ---------------------------------------------------------------------------
# Site Settings – singleton, shown at Main > Site settings
# ---------------------------------------------------------------------------

@admin.register(SiteSetting)
class SiteSettingAdmin(admin.ModelAdmin):
    list_display = ('__str__', 'crc_gateway_mode')
    fieldsets = (
        ('CRC Gateway (Pitt CRC Elasticsearch)', {
            'description': (
                'Controls whether reconciliation queries also search the new '
                '<code>places</code> / <code>toponyms</code> indexes on the '
                'Pitt CRC Elasticsearch instance.<br>'
                '<b>Disabled</b> – legacy indexes only (safe default).<br>'
                '<b>Admin only</b> – only staff / superuser accounts see CRC results '
                '(useful for testing on production).<br>'
                '<b>All users</b> – everyone gets merged results.'
            ),
            'fields': ('crc_gateway_mode',),
        }),
    )

    def has_add_permission(self, request):
        # Only allow one row
        return not SiteSetting.objects.exists()

    def has_delete_permission(self, request, obj=None):
        return False
