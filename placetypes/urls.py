# placetypes/urls.py
from django.urls import path
from . import views
from . import views_mapping

app_name = 'placetypes'

urlpatterns = [
    # JSON endpoint for the type-tree widget (lazy-loads children)
    # GET /types/tree/          → top-level root categories
    # GET /types/tree/300008347 → children of aat:300008347
    path('tree/', views.type_tree, name='type-tree-roots'),
    path('tree/search/', views.type_tree_search, name='type-tree-search'),
    path('tree/<int:aat_id>/', views.type_tree, name='type-tree-children'),

    # Mapping UI
    path('mapping/', views_mapping.mapping_dashboard, name='mapping-dashboard'),

    # Mapping API (AJAX)
    path('mapping/api/geonames/', views_mapping.api_geonames_types, name='mapping-api-geonames'),
    path('mapping/api/wikidata/', views_mapping.api_wikidata_types, name='mapping-api-wikidata'),
    path('mapping/api/osm/', views_mapping.api_osm_types, name='mapping-api-osm'),
    path('mapping/api/ohm/', views_mapping.api_ohm_types, name='mapping-api-ohm'),
    path('mapping/api/search/', views_mapping.api_aat_search, name='mapping-api-search'),
    path('mapping/api/save/', views_mapping.api_save_mapping, name='mapping-api-save'),
    path('mapping/api/remove/', views_mapping.api_remove_mapping, name='mapping-api-remove'),
    path('mapping/api/copy-osm-to-ohm/', views_mapping.api_copy_osm_to_ohm, name='mapping-api-copy-osm-ohm'),
    path('mapping/api/stats/', views_mapping.api_mapping_stats, name='mapping-api-stats'),
]

