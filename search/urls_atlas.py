# search/urls_atlas.py
"""
URL patterns for the Atlas (Explorer) page at /atlas/.
Reuses existing search API endpoints (/search/index/, /search/boundaries/, etc.).
"""
from django.urls import path

from search.views import AtlasPageView

urlpatterns = [
    path('', AtlasPageView.as_view(), name='atlas-page'),
    path('<str:toponym>', AtlasPageView.as_view(), name='atlas-page-toponym'),
]

