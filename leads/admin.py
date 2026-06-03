# leads/admin.py
from django.contrib import admin, messages

from .models import DatasetLead, LeadStatus


@admin.register(DatasetLead)
class DatasetLeadAdmin(admin.ModelAdmin):
    list_display  = ('title', 'status', 'provenance', 'priority_score',
                     'current_area', 'scan_status', 'assignee', 'updated_at')
    list_filter   = ('status', 'provenance', 'scan_status', 'assignee')
    search_fields = ('title', 'author_compiler', 'region_covered', 'current_area', 'recommended_by')
    list_editable = ('status', 'priority_score', 'assignee')   # quick triage from the list
    list_select_related = ('assignee',)
    autocomplete_fields = ('assignee', 'recommender_user')
    readonly_fields = ('created_at', 'updated_at', 'zotero_version')
    actions       = ['mark_approved', 'mark_rejected', 'recompute_gap_value', 'sync_from_zotero']

    fieldsets = (
        ('Bibliographic', {
            'fields': ('title', 'volume_example', 'author_compiler', 'publication_years',
                       'region_covered', 'current_area', 'ccodes', 'repository',
                       'source_url', 'scan_status', 'tags'),
        }),
        ('Workflow', {
            'fields': ('status', 'assignee', 'next_action', 'notes'),
        }),
        ('Priority & rubric', {
            'fields': ('provenance', 'recommended_by', 'recommender_user',
                       'priority_score', 'gap_value', 'difficulty', 'rubric'),
        }),
        ('Zotero', {
            'classes': ('collapse',),
            'fields': ('zotero_key', 'zotero_version'),
        }),
        ('Timestamps', {
            'classes': ('collapse',),
            'fields': ('created_at', 'updated_at'),
        }),
    )

    @admin.action(description='Mark selected leads as Approved')
    def mark_approved(self, request, queryset):
        updated = queryset.update(status=LeadStatus.APPROVED)
        self.message_user(request, f'{updated} lead(s) marked Approved.', messages.SUCCESS)

    @admin.action(description='Mark selected leads as Rejected')
    def mark_rejected(self, request, queryset):
        updated = queryset.update(status=LeadStatus.REJECTED)
        self.message_user(request, f'{updated} lead(s) marked Rejected.', messages.SUCCESS)

    @admin.action(description='Recompute WHG gap value')
    def recompute_gap_value(self, request, queryset):
        from .services.gapvalue import gap_value_for
        n_ok, n_err = 0, 0
        for lead in queryset:
            try:
                lead.gap_value = gap_value_for(lead)
                lead.save(update_fields=['gap_value', 'updated_at'])
                n_ok += 1
            except Exception as e:  # noqa: BLE001 — surface to admin, don't 500
                n_err += 1
                self.message_user(request, f'{lead}: {e}', messages.ERROR)
        if n_ok:
            self.message_user(request, f'Recomputed gap value for {n_ok} lead(s).', messages.SUCCESS)
        if n_err:
            self.message_user(request, f'{n_err} lead(s) failed.', messages.WARNING)

    @admin.action(description='Sync selected leads from Zotero')
    def sync_from_zotero(self, request, queryset):
        from .services.zotero import refresh_lead_from_zotero
        n_ok, n_skip = 0, 0
        for lead in queryset:
            if not lead.zotero_key:
                n_skip += 1
                continue
            try:
                refresh_lead_from_zotero(lead)
                n_ok += 1
            except Exception as e:  # noqa: BLE001
                self.message_user(request, f'{lead}: {e}', messages.ERROR)
        if n_ok:
            self.message_user(request, f'Synced {n_ok} lead(s) from Zotero.', messages.SUCCESS)
        if n_skip:
            self.message_user(request, f'{n_skip} lead(s) skipped (no zotero_key).', messages.WARNING)
