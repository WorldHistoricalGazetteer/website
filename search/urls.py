# search/urls.py
from django.urls import path  # , include

from search.views import (
    SearchViewV3, SearchPageView, FeatureContextView, TraceGeomView,
    SearchDatabaseView, CollectionGeomView, TypeaheadSuggestions
)
from search.views_beta import SearchViewBeta, TypeaheadSuggestionsBeta
from utils.version_dispatch import version_dispatch

# app_name = "search"

urlpatterns = [

    # simply returns old search page, for comparison
    # path('old/', SearchPageView.as_view(), name='search-page-old'),

    # generic search view, renders search.html w/results
    path('index/', version_dispatch(SearchViewV3, SearchViewBeta), name='search'),
    path('suggestions/', version_dispatch(TypeaheadSuggestions, TypeaheadSuggestionsBeta), name='typeahead_suggestions'),

    path('db/', SearchDatabaseView.as_view(), name='search-db'),  # executes database search
    path('context/', FeatureContextView.as_view(), name='feature_context'),  # place portal context
    path('tracegeom/', TraceGeomView.as_view(), name='trace_geom'),  # trace features <- search & place portal
    path('collgeom/', CollectionGeomView.as_view(), name='collection_geom'),
    # collection features <- search & place portal

    path('db/', SearchDatabaseView.as_view(), name='search-db'),  # executes database search

    path('', SearchPageView.as_view(), name='search-page'),
    path('<str:toponym>', SearchPageView.as_view(), name='search-page-toponym'),
]
