from django.contrib import admin
from .models import Dataset, DatasetFile, Hit
from guardian.admin import GuardedModelAdmin
from persons.admin import ContributionInline

# class DatasetAdmin(GuardedModelAdmin):
class DatasetAdmin(admin.ModelAdmin):
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
admin.site.register(Dataset, DatasetAdmin)
# admin.site.register(Dataset)

#class DatasetFileAdmin(admin.ModelAdmin):
class DatasetFileAdmin(GuardedModelAdmin):
    list_display = ('dataset_id_id', 'file', 'upload_date', 'df_status', 'format', 'datatype')
admin.site.register(DatasetFile, DatasetFileAdmin)

admin.site.register(Hit)
