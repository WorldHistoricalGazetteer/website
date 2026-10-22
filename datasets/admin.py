from django.contrib import admin, messages
from .forms import DatasetAdminForm
from .models import Dataset, DatasetFile, Hit
from guardian.admin import GuardedModelAdmin
from persons.admin import ContributionInline

# class DatasetAdmin(GuardedModelAdmin):
class DatasetAdmin(admin.ModelAdmin):
    form = DatasetAdminForm  # place#322: refuses making an accessioned dataset private
    list_display = ('title', 'label', 'id', 'ds_status', 'public', 'downloadable', 'authority', 'create_date')
    list_filter = ('ds_status', 'authority', 'downloadable')
    list_editable = ('downloadable',)
    fields = ('id','label','title','owner','ds_status', 'volunteers',
              ('public','downloadable','core','authority',), 'featured', 'image_file', 'pdf', 'description',
              'creator', ('license','rights_statement'), 'numrows','numlinked','total_links')
    readonly_fields = ('id','label','owner','create_date','numrows','numlinked','total_links',)
    autocomplete_fields = ('license',)
    search_fields = ('title','label')
    inlines = [ContributionInline]
    actions = ['track_in_grace']

    @admin.action(description='Track in GRACE (so it can be reviewed and followed up)')
    def track_in_grace(self, request, queryset):
        # Lazy import: GRACE depends on datasets, never the reverse at import time.
        from grace.tracking import track_dataset
        made = found = 0
        for dataset in queryset:
            _, created = track_dataset(dataset, added_by=request.user)
            made += created
            found += not created
        self.message_user(
            request,
            f'GRACE: {made} dataset(s) now tracked'
            + (f'; {found} already were.' if found else '.'),
            messages.SUCCESS)
admin.site.register(Dataset, DatasetAdmin)
# admin.site.register(Dataset)

#class DatasetFileAdmin(admin.ModelAdmin):
class DatasetFileAdmin(GuardedModelAdmin):
    list_display = ('dataset_id_id', 'file', 'upload_date', 'df_status', 'format', 'datatype')
admin.site.register(DatasetFile, DatasetFileAdmin)

admin.site.register(Hit)
