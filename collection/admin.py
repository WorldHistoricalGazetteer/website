from django.contrib import admin
from .models import *

class CollectionAdmin(admin.ModelAdmin):
    list_display = ('title', 'id', 'description', 'owner_id', 'status')
    fields = ('id','collection_class','title','owner',
              ('public', 'status','featured'),
              'description','image_file','keywords','rel_keywords','file','creator')
    readonly_fields = ('id', 'datasets', 'places', 'collection_class')
    list_filter = ('status','collection_class')
    search_fields = ('username', 'name')

admin.site.register(Collection, CollectionAdmin)

class CollectionGroupAdmin(admin.ModelAdmin):
    list_display = ('title', 'id', 'owner', 'start_date', 'due_date')
admin.site.register(CollectionGroup, CollectionGroupAdmin)

class CollectionLinkAdmin(admin.ModelAdmin):
    list_display = ('id', 'collection', 'link_type', 'uri')
admin.site.register(CollectionLink, CollectionLinkAdmin)

class CollectionGroupUserAdmin(admin.ModelAdmin):
  list_display = ('id', 'collectiongroup', 'user', 'role')
admin.site.register(CollectionGroupUser, CollectionGroupUserAdmin)

# collaborators
class CollectionUserAdmin(admin.ModelAdmin):
    list_display = ('id', 'collection', 'user', 'role')
admin.site.register(CollectionUser, CollectionUserAdmin)
