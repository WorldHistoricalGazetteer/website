# places.urls

from django.urls import path, re_path
from django.conf.urls.static import static
from django.conf import settings
from django.views.generic.base import TemplateView, RedirectView

from api.views import PlacesDetailAPIView
from . import views
from .v4_landing import legacy_portal_view, portal_pid_view, portal_whg_id_view
from elastic.es_utils import fetch

# place actions
app_name='places'
place_panel_view = PlacesDetailAPIView.as_view()

urlpatterns = [

    # START new urls-> views (kg 2023-10-31)
    path('portal/', views.PlacePortalView.as_view(), name='place-portal'),
    # place#325 Stage 4: with V4_LANDING_ENABLED (default off) this redirects to the
    # Record URI (302 until the v4 release: v4_landing.PORTAL_REDIRECT_STATUS); off,
    # the wrapper returns the portal's own response untouched.
    path('portal/<int:pid>/', portal_pid_view(views.PlacePortalView.as_view()), name='place-portal-pid'),
    # With V4_LANDING_ENABLED on: the multi-pid portal keeps serving, labelled legacy;
    # the whg_id portal redirects to its frozen legacy locus when the snapshot holds it
    # (places/v4_landing.py, ruling 2026-10-21). Off: the legacy responses untouched.
    path('portal/<str:encoded_ids>/', legacy_portal_view(views.PlacePortalView.as_view()),
         name='place-portal-multipid'),
    path('<int:whg_id>/portal/', portal_whg_id_view(views.PlacePortalView.as_view()), name='place-portal-whg-id'),

    # added for sessions approach
    path('set-current-result/', views.SetCurrentResultView.as_view(), name='set-current-result'),

    # path('portal_new/', TemplateView.as_view(template_name='places/place_portal_new.html'),
    #    name='portal-new'),
    # END new urls-> views (kg 2023-10-31)

    # path('portal_new/', TemplateView.as_view(template_name='places/place_portal_new.html'), name='portal_new'),

    # single db record
    path('<int:pk>/detail', views.PlaceDetailView.as_view(), name='place-detail'),
    # single db record for modal
    # path('<int:id>/modal', views.PlaceModalView.as_view(), name='place-modal'),
    
    path('defer/<int:pid>/<str:auth>/<str:last>', views.defer_review, name='defer-review'),
    
    # ??
    path('<int:id>/full', views.PlaceFullView.as_view(), name='place-full'),

    # The place-detail side panel of the dataset / collection browse pages
    # (whg/webpack/js/getPlace.js): one place or a dash-joined list, ?cid=<collection id>
    # for that collection's annotations. The same aggregated payload /api/place/<id>/ has
    # served the panel, mounted here so the site's own pages make no call to the legacy
    # v3 API (deprecated 2026-11-17, api.deprecation): what LEGACY_API_MODE does to
    # /api/place/ never reaches the panel. Internal, not an API product: the Entity API
    # (/entity/place:<id>/api) is the public successor, but carries none of traces (cid),
    # datasets, extent, fclass descriptions, country labels or attestation years.
    re_path(r'^(?P<pk_list>\d+(?:-\d+)*)/panel/$', place_panel_view, name='place-panel'),

    # place#120: a bare /places/<id> (no /portal/ or /detail suffix) 404'd; redirect
    # it to the portal so external/legacy place links resolve gracefully. Placed last
    # so the suffixed routes above always match first.
    path('<int:pid>/', portal_pid_view(RedirectView.as_view(pattern_name='places:place-portal-pid',
                                                            permanent=False, query_string=True)),
         name='place-bare-redirect'),

] + static(settings.MEDIA_URL, document_root = settings.MEDIA_ROOT)
