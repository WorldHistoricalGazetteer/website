# search/urls_atlas.py
"""
URL patterns for the Atlas (Explorer) page at /atlas/.
Reuses existing search API endpoints (/search/index/, /search/boundaries/, etc.).
"""
from django.urls import path

from search.views import (AtlasPageView, atlas_search, atlas_place,
                          atlas_boundaries, atlas_geometry,
                          atlas_registry_coverage, atlas_status)
from places.views_record import atlas_record_concordance, atlas_record_page

urlpatterns = [
    path('', AtlasPageView.as_view(), name='atlas-page'),
    # Gateway-routed endpoints — MUST precede the <toponym> catch-all below,
    # or these paths are captured as toponyms.
    path('search/', atlas_search, name='atlas-search'),
    path('place/', atlas_place, name='atlas-place'),
    # place#325 Stage 2: the Record page (a contributed record by places.id)
    # and its Concordance section's JSON, beta-gated in places/views_record.py.
    path('place/<int:pk>/', atlas_record_page, name='atlas-record'),
    path('place/<int:pk>/concordance/', atlas_record_concordance, name='atlas-record-concordance'),
    path('boundaries/', atlas_boundaries, name='atlas-boundaries'),
    path('geometry/', atlas_geometry, name='atlas-geometry'),
    path('status/', atlas_status, name='atlas-status'),
    path('registry/coverage/', atlas_registry_coverage, name='atlas-registry-coverage'),
    path('<str:toponym>', AtlasPageView.as_view(), name='atlas-page-toponym'),
]

