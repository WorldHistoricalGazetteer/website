from django.contrib import admin

from .models import License


@admin.register(License)
class LicenseAdmin(admin.ModelAdmin):
    list_display = ("spdx_id", "label", "permits_commercial", "share_alike",
                    "attribution_required", "custom")
    list_filter = ("permits_commercial", "share_alike", "custom")
    search_fields = ("spdx_id", "label")
