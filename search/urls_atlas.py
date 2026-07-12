# search/urls_atlas.py
"""
URL patterns for the Atlas (Explorer) page at /atlas/.
Reuses existing search API endpoints (/search/index/, /search/boundaries/, etc.).
"""
from django.urls import path

from search.views import AtlasPageView, atlas_search

urlpatterns = [
    path('', AtlasPageView.as_view(), name='atlas-page'),
    # Gateway-routed search (clustering fuel) — MUST precede the <toponym>
    # catch-all below, or 'search/' is captured as a toponym.
    path('search/', atlas_search, name='atlas-search'),
    path('<str:toponym>', AtlasPageView.as_view(), name='atlas-page-toponym'),
]

