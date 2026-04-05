# placetypes/admin.py
from django.contrib import admin

from main.choices import FEATURE_CLASSES
from .models import Type, TypeMappingLog


class FclassFilter(admin.SimpleListFilter):
    """Filter Types by any value in their fclasses ArrayField."""
    title = 'feature class'
    parameter_name = 'fclass'

    def lookups(self, request, model_admin):
        return FEATURE_CLASSES

    def queryset(self, request, queryset):
        if self.value():
            return queryset.filter(fclasses__contains=[self.value()])
        return queryset


@admin.register(Type)
class TypeAdmin(admin.ModelAdmin):
    list_display = ('aat_id', 'term', 'fclass_display', 'depth', 'is_place_type')
    list_filter = (FclassFilter, 'is_place_type', 'depth')
    search_fields = ('term', 'term_full', 'aat_id')
    ordering = ('term',)
    readonly_fields = ('path',)

    @admin.display(description='Feature class')
    def fclass_display(self, obj):
        return ', '.join(obj.fclasses) if obj.fclasses else '-'


@admin.register(TypeMappingLog)
class TypeMappingLogAdmin(admin.ModelAdmin):
    list_display = ('created', 'user', 'action', 'source_vocab', 'source_id', 'aat_id', 'aat_term')
    list_filter = ('action', 'source_vocab')
    search_fields = ('source_id', 'aat_term', 'user__username')
    readonly_fields = ('user', 'action', 'source_vocab', 'source_id', 'aat_id', 'aat_term', 'note', 'created')
    ordering = ('-created',)
    date_hierarchy = 'created'
