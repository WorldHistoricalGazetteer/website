# whg/api/admin.py
from django.contrib import admin, messages
from django.http import HttpResponseRedirect
from django.urls import path, reverse
from django.utils.html import format_html

from . import reingest as reingest_service
from .models import GazetteerRegistryEntry, UserAPIProfile


@admin.register(UserAPIProfile)
class UserAPIProfileAdmin(admin.ModelAdmin):
    list_display = ('user', 'daily_count', 'daily_limit', 'total_count')
    list_editable = ('daily_limit',)
    search_fields = ('user__username', 'user__email')
    change_list_template = "admin/api/userapiprofile/change_list.html"


@admin.register(GazetteerRegistryEntry)
class GazetteerRegistryEntryAdmin(admin.ModelAdmin):
    """Staff-only management of non-WHG gazetteers.

    The changelist is filtered to ``entry_class='authority'`` and excludes
    the synthetic ``whg`` parent row, so staff only see external gazetteers
    (GeoNames, Wikidata, OSM/OHM, …). WHG-namespaced datasets are managed
    via the contributor workflow elsewhere; their re-ingestion is triggered
    by publication-status changes, not from this admin.

    Editable curatorial flags (``core``, ``region_source``, ``no_explore``,
    ``gazetteer_type``) are protected on every inventory push — see
    ``api/views_indexing.py::GazetteerInventoryView._upsert_one``.

    The "Re-ingest" admin action and the per-row button POST to the Pitt
    CRC gateway via ``api/reingest.py`` to enqueue a fresh-source-data
    ingestion. Status, job id, and finish time are tracked per row.
    """

    change_list_template = "admin/api/gazetteerregistryentry/change_list.html"

    list_display = (
        'id', 'name', 'namespace', 'core', 'region_source', 'no_explore',
        'gazetteer_type', 'status', 'record_count', 'downloadable',
        'reingest_status', 'reingest_finished_at', 'reingest_button',
    )
    list_filter = (
        'core', 'region_source', 'no_explore', 'gazetteer_type',
        'downloadable', 'redistributable',
        'reingest_status', 'status', 'namespace',
    )
    list_editable = ('core', 'region_source', 'no_explore', 'gazetteer_type')
    search_fields = ('id', 'name', 'namespace')
    readonly_fields = (
        'id', 'name', 'description', 'namespace', 'entry_class', 'owner',
        'record_count', 'status', 'h3_coverage', 'temporal_extent',
        'is_global', 'updated_at',
        'redistributable', 'downloadable', 'download_blocked_reason',
        'reingest_status', 'reingest_started_at', 'reingest_finished_at',
        'reingest_job_id', 'reingest_message',
    )
    fieldsets = (
        ("Curatorial (editable)", {
            'fields': ('core', 'region_source', 'no_explore', 'gazetteer_type'),
        }),
        ("Attribution / licence / rights", {
            'description': (
                "Source-of-truth is the indexing AUTHORITIES config, pushed "
                "by the inventory job. The push only writes the fields it "
                "sends, so values set here are a safe interim until that push "
                "is upgraded — after which it becomes authoritative and may "
                "overwrite them."
            ),
            'fields': (
                'citation_text', 'license', 'license_url', 'rights_holder',
                'source_url', 'contributors_csl',
            ),
        }),
        ("Re-ingest (managed via the Re-ingest action)", {
            'fields': (
                'reingest_status', 'reingest_started_at',
                'reingest_finished_at', 'reingest_job_id', 'reingest_message',
            ),
        }),
        ("Download legality + volume (managed by ingestion pipeline)", {
            'description': (
                "Derived at ingest from licence + record_count (place#136). "
                "``downloadable`` gates any 'Download this dataset' affordance; "
                "``redistributable`` is the underlying legal fact (a source can "
                "be redistributable but still non-downloadable purely on volume "
                "grounds). Search & reconciliation are unaffected."
            ),
            'fields': (
                'redistributable', 'downloadable', 'download_blocked_reason',
            ),
        }),
        ("Inventory (managed by ingestion pipeline)", {
            'fields': (
                'id', 'name', 'description', 'namespace', 'entry_class',
                'owner', 'record_count', 'status', 'h3_coverage',
                'temporal_extent', 'is_global', 'updated_at',
            ),
        }),
    )
    actions = ['action_reingest_selected']

    # ── Filtering ───────────────────────────────────────────────────────

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        return qs.filter(entry_class='authority').exclude(id='whg')

    # ── Computed display columns ────────────────────────────────────────

    @admin.display(boolean=True, description='Global')
    def is_global(self, obj):
        return obj.is_global

    @admin.display(description='Re-ingest')
    def reingest_button(self, obj):
        url = reverse(
            'admin:api_gazetteerregistryentry_reingest',
            kwargs={'object_id': obj.pk},
        )
        disabled = obj.reingest_status in {'queued', 'running'}
        label = 'Running…' if disabled else 'Re-ingest'
        return format_html(
            '<a class="button" href="{}" {}>{}</a>',
            url,
            'style="pointer-events:none;opacity:0.5;"' if disabled else '',
            label,
        )

    # ── Custom URLs ─────────────────────────────────────────────────────

    def get_urls(self):
        urls = super().get_urls()
        custom = [
            path(
                '<path:object_id>/reingest/',
                self.admin_site.admin_view(self.reingest_view),
                name='api_gazetteerregistryentry_reingest',
            ),
            path(
                '<path:object_id>/reingest/poll/',
                self.admin_site.admin_view(self.reingest_poll_view),
                name='api_gazetteerregistryentry_reingest_poll',
            ),
        ]
        return custom + urls

    # ── Per-row re-ingest view ──────────────────────────────────────────

    def reingest_view(self, request, object_id):
        entry = self.get_object(request, object_id)
        if entry is None:
            messages.error(request, f"Gazetteer {object_id!r} not found")
            return HttpResponseRedirect(
                reverse('admin:api_gazetteerregistryentry_changelist'),
            )
        if entry.reingest_status in {'queued', 'running'}:
            messages.warning(
                request,
                f"Re-ingest already in progress for {entry.id} "
                f"(job {entry.reingest_job_id or '?'})",
            )
        else:
            try:
                reingest_service.request_reingest(entry)
                messages.success(
                    request,
                    f"Re-ingest queued for {entry.id} "
                    f"(job {entry.reingest_job_id or '?'})",
                )
            except reingest_service.ReingestError as exc:
                messages.error(
                    request,
                    f"Re-ingest failed for {entry.id}: {exc}",
                )
        return HttpResponseRedirect(
            reverse('admin:api_gazetteerregistryentry_changelist'),
        )

    # ── Status poll view (used by changelist auto-refresh JS) ───────────

    def reingest_poll_view(self, request, object_id):
        from django.http import JsonResponse
        entry = self.get_object(request, object_id)
        if entry is None:
            return JsonResponse({'error': 'not found'}, status=404)
        reingest_service.poll_reingest(entry)
        return JsonResponse({
            'id': entry.id,
            'status': entry.reingest_status,
            'job_id': entry.reingest_job_id,
            'started_at': entry.reingest_started_at,
            'finished_at': entry.reingest_finished_at,
            'message': entry.reingest_message,
        }, json_dumps_params={'default': str})

    # ── Bulk action ─────────────────────────────────────────────────────

    @admin.action(description='Re-ingest selected gazetteers')
    def action_reingest_selected(self, request, queryset):
        queued, skipped, failed = 0, 0, 0
        for entry in queryset:
            if entry.reingest_status in {'queued', 'running'}:
                skipped += 1
                continue
            try:
                reingest_service.request_reingest(entry)
                queued += 1
            except reingest_service.ReingestError as exc:
                failed += 1
                messages.error(request, f"{entry.id}: {exc}")
        if queued:
            messages.success(request, f"Queued {queued} re-ingest job(s)")
        if skipped:
            messages.warning(request, f"Skipped {skipped} (already running)")
        if failed:
            messages.warning(request, f"Failed to queue {failed}")
