# placetypes/admin.py
from django.contrib import admin

from .models import Type


@admin.register(Type)
class TypeAdmin(admin.ModelAdmin):
    list_display = ('aat_id', 'term', 'fclass', 'depth', 'is_place_type')
    list_filter = ('fclass', 'is_place_type', 'depth')
    search_fields = ('term', 'term_full', 'aat_id')
    ordering = ('term',)
    readonly_fields = ('path',)


