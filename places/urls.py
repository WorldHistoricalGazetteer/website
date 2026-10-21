# places.urls

from django.urls import path
from django.conf.urls.static import static
from django.conf import settings
from django.views.generic.base import TemplateView, RedirectView

from . import views
from .v4_landing import portal_pid_view
from elastic.es_utils import fetch

# place actions
app_name='places'
urlpatterns = [

    # START new urls-> views (kg 2023-10-31)
    path('portal/', views.PlacePortalView.as_view(), name='place-portal'),
    # place#325 Stage 4: with V4_LANDING_ENABLED (default off) this redirects to the
    # Record URI (302 until the v4 release: v4_landing.PORTAL_REDIRECT_STATUS); off,
    # the wrapper returns the portal's own response untouched.
    path('portal/<int:pid>/', portal_pid_view(views.PlacePortalView.as_view()), name='place-portal-pid'),
    path('portal/<str:encoded_ids>/', views.PlacePortalView.as_view(), name='place-portal-multipid'),
    path('<int:whg_id>/portal/', views.PlacePortalView.as_view(), name='place-portal-whg-id'),

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

    # place#120: a bare /places/<id> (no /portal/ or /detail suffix) 404'd; redirect
    # it to the portal so external/legacy place links resolve gracefully. Placed last
    # so the suffixed routes above always match first.
    path('<int:pid>/', portal_pid_view(RedirectView.as_view(pattern_name='places:place-portal-pid',
                                                            permanent=False, query_string=True)),
         name='place-bare-redirect'),

] + static(settings.MEDIA_URL, document_root = settings.MEDIA_ROOT)
