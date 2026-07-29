# search/urls_atlas.py
"""
URL patterns for the Atlas (Explorer) page at /atlas/.
Reuses existing search API endpoints (/search/index/, /search/boundaries/, etc.).
"""
from django.urls import path

from search.views import (AtlasPageView, atlas_search, atlas_place,
                          atlas_boundaries, atlas_registry_coverage,
                          atlas_status)

urlpatterns = [
    path('', AtlasPageView.as_view(), name='atlas-page'),
    # Gateway-routed endpoints — MUST precede the <toponym> catch-all below,
    # or these paths are captured as toponyms.
    path('search/', atlas_search, name='atlas-search'),
    path('place/', atlas_place, name='atlas-place'),
    path('boundaries/', atlas_boundaries, name='atlas-boundaries'),
    path('status/', atlas_status, name='atlas-status'),
    path('registry/coverage/', atlas_registry_coverage, name='atlas-registry-coverage'),
    path('<str:toponym>', AtlasPageView.as_view(), name='atlas-page-toponym'),
]

