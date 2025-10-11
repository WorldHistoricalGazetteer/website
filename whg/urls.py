import os
from pathlib import Path

from django.conf import settings
from django.conf.urls.static import static
from django.contrib import admin
from django.contrib.sitemaps import views as sitemapviews
from django.core.cache import caches
from django.http import HttpResponseForbidden
from django.urls import path, re_path, include
from django.views.decorators.cache import cache_page
from django.views.generic.base import TemplateView
# For CDNfallbacks
from django.views.static import serve

import resources.views

from accounts import orcid
from accounts.views import profile_edit
from datasets.views import PublicListsView  # , DataListsView
from main import views
from resources.views import TeachingPortalView
from sitemap.views import StaticViewSitemap, ToponymSitemap
from utils import mapdata
from utils.tasks import downloader

sitemap_cache = caches['sitemap_cache']
sitemaps = {
    'static': StaticViewSitemap,
    'toponyms': ToponymSitemap,
}

handler404 = 'main.views.custom_404'
handler500 = 'main.views.server_error_view'


def serve_cdnfallbacks(request, path):
    host = request.headers.get('Host', '')
    print(host)
    referer = request.META.get('HTTP_REFERER')
    if 'whgazetteer.org' in host or 'localhost' in host:
        return serve(request, path, document_root=Path(settings.BASE_DIR) / 'CDNfallbacks')
    else:
        return HttpResponseForbidden(f"Access forbidden: {referer}")


urlpatterns = [
                  # home page
                  path('', views.Home30a.as_view(), name="home"),

                  # apps
                  path('areas/', include('areas.urls')),
                  path('collections/', include('collection.urls')),
                  path('datasets/', include('datasets.urls')),
                  path('elastic/', include('elastic.urls')),
                  path('main/', include('main.urls')),  # utility urls/views
                  path('places/', include('places.urls')),
                  path('resources/', include('resources.urls')),
                  path('search/', include('search.urls')),
                  path(
                      "sitemap.xml",
                      cache_page(3600, cache='sitemap_cache')(sitemapviews.index),
                      {"sitemaps": sitemaps},
                      name="django.contrib.sitemaps.views.index",
                  ),
                  path(
                      "sitemap-<section>.xml",
                      cache_page(3600, cache='sitemap_cache')(sitemapviews.sitemap),
                      {"sitemaps": sitemaps},
                      name="django.contrib.sitemaps.views.sitemap",
                  ),
                  path('whgmail/', include('whgmail.urls')),





                  path('teaching/', TeachingPortalView.as_view(), name="teaching"),
                  path("api/teaching/", resources.views.teaching_json, name="teaching_json"),

                  path('public_data/', PublicListsView.as_view(), name='public-lists'),

                  # orcid authentication, profile and settings
                  path('orcid-callback/', orcid.orcid_callback, name='orcid-callback'),
                  path('profile/', profile_edit, name="profile-edit"),

                  path('dashboard/', views.dashboard_redirect, name="dashboard"),  # redirect to user or admin
                  path('dashboard_user/', views.dashboard_user_view, name="dashboard-user"),
                  path('dashboard_admin/', views.dashboard_admin_view, name="dashboard-admin"),

                  # static content
                  path('about/', TemplateView.as_view(template_name="main/about.html"), name="about"),
                  # path('contributing/', TemplateView.as_view(template_name="main/../_local/_older/contributing.html"), name="contributing"),

                  path('people_overview/', TemplateView.as_view(template_name="main/people_overview.html"),
                       name="credits"),
                  # path('system/', TemplateView.as_view(template_name="main/../_local/_older/system.html"), name="system"),

                  path('publications/', TemplateView.as_view(template_name="main/publications.html"),
                       name="publications"),

                  path('downloads/', TemplateView.as_view(template_name="main/downloads.html"), name="downloads"),

                  # more static content - 2024-01
                  path('workbench/', TemplateView.as_view(template_name="main/workbench_3col.html"), name="workbench"),

                  # yet more static content - 2024-02
                  path('main_regions/', TemplateView.as_view(template_name="main/regions.html"), name="main-regions"),
                  path('journeys_routes/', TemplateView.as_view(template_name="main/journeys_routes.html"),
                       name="journeys-routes"),

                  path('v3_new/', TemplateView.as_view(template_name="main/v3_new.html"), name="v3-new"),

                  # path('modal_home/', views.home_modal, name="modal-home"),

                  path('announcement/create/', views.AnnouncementCreateView.as_view(), name='announcement-create'),
                  path('announcement_delete/<int:pk>/', views.AnnouncementDeleteView.as_view(),
                       name='announcement-delete'),
                  path('announcement/update/<int:pk>/', views.AnnouncementUpdateView.as_view(),
                       name='announcement-update'),
                  path('announcements/', views.AnnouncementListView.as_view(), name='announcements-list'),

                  path('mapdata/<str:category>/<int:id>/', mapdata.mapdata, name="mapdata"),
                  path('mapdata/<str:category>/<int:id>/carousel/', mapdata.mapdata, {'carousel': True},
                       name="mapdata_carousel"),

                  path('comment/', views.handle_comment, name='comment-handle'),
                  # path('contact/', views.contact_view, name='contact'),
                  path('contact_modal/', views.contact_modal_view, name='contact-modal'),
                  path('license/', views.license_view, name='license'),
                  path('terms_of_use/', views.terms_of_use_view, name='terms_of_use'),
                  path('privacy_policy/', views.privacy_policy_view, name='privacy_policy'),
                  path('success/', views.contactSuccessView, name='success'),
                  path('status/', views.statusView, name='status'),
                  path('create_link/', views.create_link, name="create-link"),

                  # backend stuff
                  path("", include("api.urls_root")),  # reconcile/suggest API
                  path('api/', include('api.urls')),
                  path('accounts/', include('accounts.urls')),
                  path('admin/', admin.site.urls),
                  path('health', views.health_check, name='health_check'),
                  # for celery tasks
                  # initiate downloads of augmented datasets via celery task (called from ajax)
                  path('dlcelery/', downloader, name='dl_celery'),
                  path('task_progress/<str:taskid>/', views.get_task_progress, name='task-progress'),

                  # path('trigger500/', views.trigger_500_error, name='trigger-500-error'),

                  # Dataset Validation
                  path('validation/', include('validation.urls')),

                  # Serve the CDNfallbacks folder with host check
                  re_path(r'^CDNfallbacks/(?P<path>.*)$', serve_cdnfallbacks),

              ] + static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)

# if settings.DEBUG:
#     import debug_toolbar
#     urlpatterns += [
#         path('__debug__/', include(debug_toolbar.urls)),
#     ]
#     urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)


if settings.DEBUG:
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)

    # Serve files from the 'validation/static' directory with the 'schema/' URL prefix
    schema_root = os.path.join(settings.BASE_DIR, 'validation', 'static')
    urlpatterns += static('schema/', document_root=schema_root)
    # NB: requires additional Nginx directive in staging/production:
    #
    # location /schema/ {
    #     alias /home/whgadmin/sites/dev-whgazetteer-org/validation/static/;
    #     autoindex on;  # Enable directory listing
    # }

    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
