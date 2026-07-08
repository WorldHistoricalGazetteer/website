from django.contrib import admin

from .models import Team, TeamMember, WorkbenchProject, ProjectSnapshot


class TeamMemberInline(admin.TabularInline):
    model = TeamMember
    extra = 0
    raw_id_fields = ('user',)


@admin.register(Team)
class TeamAdmin(admin.ModelAdmin):
    list_display = ('title', 'owner', 'is_personal', 'created')
    list_filter = ('is_personal',)
    search_fields = ('title', 'slug', 'owner__username')
    raw_id_fields = ('owner',)
    inlines = [TeamMemberInline]


@admin.register(WorkbenchProject)
class WorkbenchProjectAdmin(admin.ModelAdmin):
    list_display = ('title', 'team', 'status', 'version', 'created_by', 'updated')
    list_filter = ('status',)
    search_fields = ('title', 'id')
    raw_id_fields = ('team', 'created_by', 'published_dataset')
    readonly_fields = ('created', 'updated', 'version')


@admin.register(ProjectSnapshot)
class ProjectSnapshotAdmin(admin.ModelAdmin):
    list_display = ('project', 'version', 'created', 'created_by')
    search_fields = ('project__id',)
    raw_id_fields = ('project', 'created_by')
