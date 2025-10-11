# whg/api/admin.py
from django.contrib import admin
from .models import UserAPIProfile

@admin.register(UserAPIProfile)
class UserAPIProfileAdmin(admin.ModelAdmin):
    list_display = ('user', 'daily_count', 'daily_limit', 'total_count')
    list_editable = ('daily_limit',)
    search_fields = ('user__username', 'user__email')
    change_list_template = "admin/api/userapiprofile/change_list.html"

