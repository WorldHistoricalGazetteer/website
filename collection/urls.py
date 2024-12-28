# collection.urls

from django.urls import path
from django.conf.urls.static import static
from django.conf import settings
from django.views.decorators.csrf import csrf_exempt

from . import views
from traces.views import get_form, annotate

# area actions
app_name='collection'

urlpatterns = [

    # GALLERIES
    path('group/<int:id>/gallery', views.CollectionGroupGalleryView.as_view(), name='collection-group-gallery'),
    # path('gallery', views.CollectionGalleryView.as_view(), name='collection-gallery'), # Superseded by /datasets/gallery/collections

    # DATASET collections (datasets only)
    path('create_ds/', views.DatasetCollectionCreateView.as_view(), name='ds-collection-create'),
    path('<int:id>/update_ds', views.DatasetCollectionUpdateView.as_view(), name='ds-collection-update'),
    # path('<int:pk>/summary_ds', views.DatasetCollectionSummaryView.as_view(), name='ds-collection-summary'),
    path('<int:id>/browse_ds', views.DatasetCollectionBrowseView.as_view(), name='ds-collection-browse'),

    # PLACE collections (datasets, indiv places, annotations, 'authored')
    path('create_pl/', views.PlaceCollectionCreateView.as_view(), name='place-collection-create'),
    path('<int:id>/update_pl', views.PlaceCollectionUpdateView.as_view(), name='place-collection-update'),
    path('<int:id>/browse_pl', views.PlaceCollectionBrowseView.as_view(), name='place-collection-browse'),
    path('flash_create/', views.flash_collection_create, name="collection-create-flash"),

    path('<int:id>/delete', views.CollectionDeleteView.as_view(), name='collection-delete'),

    ## COLLABORATORS
    # add Collection collaborator; payload includes email, role
    path('collab-add/<int:cid>/', views.collab_add, name="collab-add"),
    # delete Collection collaborator; payload includes int id of collaborator
    path('collab-remove/<int:uid>/<int:cid>/', views.collab_remove, name="collab-remove"),

    # COLLECTION GROUPS (for classes, workshops)
    path('create_collection_group/', views.CollectionGroupCreateView.as_view(), name='collection-group-create'),
    path('group/<int:id>/update', views.CollectionGroupUpdateView.as_view(), name='collection-group-update'),
    path('group/<int:id>/delete', views.CollectionGroupDeleteView.as_view(), name='collection-group-delete'),
    path('group/<int:id>/detail', views.CollectionGroupDetailView.as_view(), name='collection-group-detail'),
    path('group/<int:id>/gallery', views.CollectionGroupGalleryView.as_view(), name='collection-group-gallery'),

    # UTILITY
    path('<int:id>/citation', views.collection_citation, name='collection-citation'),
    path('list_ds/', views.ListDatasetView.as_view(), name='list-ds'),
    path('add_ds/<int:coll_id>/<int:ds_id>', views.add_dataset, name='add-ds'),
    path('add_dsplaces/<int:coll_id>/<int:ds_id>', views.add_dataset_places, name='add-dsplaces'),
    path('remove_ds/<int:coll_id>/<int:ds_id>', views.remove_dataset, name='remove-ds'),
    path('update_sequence/', views.update_sequence, name='update-sequence'),

    path('add_places/', views.add_places, name="collection-add-places"),
    path('add_collection_places/', views.add_collection_places, name="add-collection-places"),
    path('archive_traces/', views.archive_traces, name="collection-archive_traces"),
    # path('create_link/', views.create_link, name="collection-create-link"),
    path('remove_link/<int:id>/', views.remove_link, name="remove-link"),
    # submits or unsubmits collection to/from a group
    path('group_connect/', views.group_connect, name="group-connect"),
    path('status_update/', views.status_update, name="status-update"),
    path('nominate/', views.nominator, name="nominator"),
    path('get_joincode/', views.generate_unique_join_code, name="get-join-code"),
    path('set_joincode/<int:cgid>/<str:join_code>/', views.set_joincode, name="set-join-code"),
    path('group_join/', views.join_group, name="group-join"),
    path('update_vis_parameters/', views.update_vis_parameters, name='update-vis-parameters'),

    # function-based views to process a trace annotation
    path('<int:id>/annotate', csrf_exempt(annotate), name="collection-annotate"),
    path('annoform/', get_form, name="get_form"),

    # switch off active bit
    path('inactive/', views.inactive, name="collection-inactive"),

    path('<int:id>/geojson/', views.fetch_geojson_coll, name="geojson-coll"),

] + static(settings.MEDIA_URL, document_root = settings.MEDIA_ROOT)
