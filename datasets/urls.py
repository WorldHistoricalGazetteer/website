# datasets.urls

from django.urls import path
from django.conf.urls.static import static
from django.conf import settings

from . import views, views_feed, views_lifecycle
from datasets.utils import download_file, download_dataset, UpdateCountsView, toggle_volunteers
from .services import write_wd_pass0

# fetch_geojson_flat, fetch_geojson_ds, download_augmented, downloadLP7,

# dataset actions
app_name='datasets'
urlpatterns = [

  # BASICS: create from upload, create empty, delete
  # new validation workflow
  # path('create/', views.DatasetCreate.as_view(), name='dataset-create'), # superseded by validate/
  path('validate/', views.DatasetValidate.as_view(), name='dataset-validate'),

  path('create_empty/', views.DatasetCreateEmptyView.as_view(), name='dataset-create-empty'),
  path('<int:pk>/delete', views.DatasetDeleteView.as_view(), name='dataset-delete'),

  # public datasets & collections
  path('gallery/', views.DatasetGalleryView.as_view(), name='dataset-gallery'),
  path('gallery/<str:gallery_type>/', views.DatasetGalleryView.as_view(), name='dataset-gallery-type'),

  # insert validated delimited file data to db (csv, tsv, spreadsheet)
  # path('<int:pk>/insert_tsv/', views.ds_insert_tsv, name="ds_insert_tsv"),

  # insert validated lpf file data to db
  # path('<int:pk>/insert_lpf/', views.ds_insert_lpf, name="ds_insert_lpf"),

  # upload excel
  # path('xl/', views.xl_upload, name='xl-upload'),

  ## MANAGE/VIEW
  # dataset owner pages (tabs); names correspond to template names
  path('<int:id>/status', views.DatasetStatusView.as_view(), name='ds_status'),

  # v4 lifecycle, beta only (place#334 Phase 3): JSON
  path('<int:dsid>/lifecycle/', views_lifecycle.lifecycle_state, name='lifecycle'),
  path('<int:dsid>/lifecycle/submit/', views_lifecycle.lifecycle_submit, name='lifecycle-submit'),
  path('<int:dsid>/lifecycle/publish/', views_lifecycle.lifecycle_publish, name='lifecycle-publish'),
  path('<int:dsid>/lifecycle/withdraw/', views_lifecycle.lifecycle_withdraw, name='lifecycle-withdraw'),
  path('<int:dsid>/lifecycle/review/start/', views_lifecycle.lifecycle_review_start,
       name='lifecycle-review-start'),
  path('<int:dsid>/lifecycle/review/conclude/', views_lifecycle.lifecycle_review_conclude,
       name='lifecycle-review-conclude'),
  path('<int:dsid>/lifecycle/strip-placeholders/', views_lifecycle.lifecycle_strip_placeholders,
       name='lifecycle-strip-placeholders'),
  path('<int:dsid>/lifecycle/sensitivity/', views_lifecycle.lifecycle_sensitivity,
       name='lifecycle-sensitivity'),
  # ... and its HTML screen for editors (the owner's is a panel on ds_status)
  path('review-queue/', views_lifecycle.review_queue, name='lifecycle-review-queue'),
  path('<int:id>/metadata', views.DatasetMetadataView.as_view(), name='ds_metadata'),
  path('<int:id>/citation', views.dataset_citation, name='dataset-citation'),
  path('<int:id>/contributions/add', views.dataset_contribution_add, name='ds_contribution_add'),
  path('<int:id>/contributions/<int:cid>/delete', views.dataset_contribution_delete, name='ds_contribution_delete'),
  path('<int:id>/browse', views.DatasetBrowseView.as_view(), name='ds_browse'),
  path('<int:id>/reconcile', views.DatasetReconcileView.as_view(), name='ds_reconcile'),
  path('<int:id>/collab', views.DatasetCollabView.as_view(), name='ds_collab'),
  path('<int:id>/addtask', views.DatasetAddTaskView.as_view(), name='ds_addtask'),
  path('<int:id>/log', views.DatasetLogView.as_view(), name='ds_log'),
  # place#338: the contributor feed, "Link your records" (beta-gated, owner circle only)
  path('<int:id>/matches/', views_feed.feed_page, name='ds_matches'),
  path('<int:id>/matches/candidates/', views_feed.feed_candidates, name='ds_matches_candidates'),
  path('<int:id>/matches/review/', views_feed.feed_review, name='ds_matches_review'),
  path('<int:id>/matches/search/', views_feed.feed_search, name='ds_matches_search'),
  path('<int:id>/matches/progress/', views_feed.feed_progress, name='ds_matches_progress'),
  path('<int:id>/matches/regenerate/', views_feed.feed_regenerate, name='ds_matches_regenerate'),

  # public dataset pages (tabs): metadata, browse
  # path('<int:pk>', views.DatasetPublicView.as_view(), name='ds_meta'),
  path('<int:id>/places', views.DatasetPlacesView.as_view(), name='ds_places'),

  ## DOWNLOADS
  # one-off for LP7
  # path('download_lp7/', downloadLP7, name='download_lp7'),

  # download latest file, as uploaded
  path('<int:id>/file/', download_file, name="dl-file"),
  path('<int:file_id>/download_dataset/', download_dataset, name='download_dataset'),

  ## DEPRECATing download augmented dataset
  # path('<int:id>/augmented/<str:format>', download_augmented, name="dl-aug"), #

  ## UPDATES (in progress)
  # path('compare/', views.ds_compare, name='dataset-compare'),
  path('update/', views.ds_update, name='dataset-update'),
  path('update_vis_parameters/', views.update_vis_parameters, name='update-vis-parameters'),
  path('update_volunteers_text/', views.update_volunteers_text, name='update-volunteers-text'),

  ## RECONCILIATION/REVIEW
  # initiate reconciliation
  path('<int:pk>/recon/', views.ds_recon, name="ds_recon"), # form submit

  # review, validate hits
  # dsid is dataset id, tid is task id, passnum is pass number
  path('<int:dsid>/review/<str:tid>/<str:passnum>', views.review, name="review"),

  # direct load of deferred place to review screen
  # pk is dataset id, tid is task id, pid is place id
  path('<int:pk>/review/<str:tid>/<str:pid>', views.review, name="review"),

  # accept any unreviewed wikidata pass0 hits from given task
  path('wd_pass0/<str:tid>', write_wd_pass0, name="wd_pass0"),

  # delete TaskResult & associated hits
  path('task-delete/<str:tid>/<str:scope>', views.task_delete, name="task-delete"),

  # undo last save in review
  path('match-undo/<int:ds>/<str:tid>/<int:pid>', views.match_undo, name="match-undo"),

  # refresh reconciliation counts (ds.id from $.get)
  path('updatecounts/', UpdateCountsView.as_view(), name='update_counts'),

  ## COLLABORATORS
  # add DatasetUser collaborator
  path('collab-add/<int:dsid>/<str:v>/', views.collab_add, name="collab-add"),
  # list dataset on Volunteer Opportunities page
  path('toggle_volunteers', toggle_volunteers, name="toggle-volunteers"),
  # list datasets requesting volunteers
  path('volunteer_requests/', views.VolunteeringView.as_view(), name="volunteer-requests"),
  # offer to volunteer
  path('volunteer_offer/<int:pk>', views.volunteer_offer, name="volunteer-offer"),


  # delete DatasetUser collaborator
  path('collab-delete/<int:uid>/<int:dsid>/<str:v>', views.collab_delete, name="collab-delete"),

  ## GEOMETRY
  # path('<int:dsid>/geojson/', fetch_geojson_ds, name="geojson"),
  # path('<int:dsid>/geojson_flat/', fetch_geojson_flat, name="geojson-flat"),

  # list places in a dataset; for physical geog layers
  path('<str:label>/places/', views.ds_list, name='ds_list'),


] + static(settings.MEDIA_URL, document_root = settings.MEDIA_ROOT)

