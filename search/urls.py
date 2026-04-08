# search/urls.py
from django.urls import path  # , include

from search.views import (
    SearchPageView, FeatureContextView, TraceGeomView,
    SearchDatabaseView, CollectionGeomView, BoundarySearchView,
)
from search.views_crc import SearchView, TypeaheadSuggestions

# app_name = "search"

urlpatterns = [

    # generic search view, renders search.html w/results
    path('index/', SearchView.as_view(), name='search'),
    path('suggestions/', TypeaheadSuggestions, name='typeahead_suggestions'),

    # boundary name search (ES-backed, for the region selector)
    path('boundaries/', BoundarySearchView.as_view(), name='boundary_search'),

    path('db/', SearchDatabaseView.as_view(), name='search-db'),  # executes database search
    path('context/', FeatureContextView.as_view(), name='feature_context'),  # place portal context
    path('tracegeom/', TraceGeomView.as_view(), name='trace_geom'),  # trace features <- search & place portal
    path('collgeom/', CollectionGeomView.as_view(), name='collection_geom'),
    # collection features <- search & place portal


    path('', SearchPageView.as_view(), name='search-page'),
    path('<str:toponym>', SearchPageView.as_view(), name='search-page-toponym'),
]
