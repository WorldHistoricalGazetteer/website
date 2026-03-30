# main.urls

from django.urls import path
from django.conf.urls.static import static
from django.conf import settings
from django.views.generic.base import TemplateView
from . import views

# actions
app_name='main'
urlpatterns = [


    # new 'dashboard' LIST VIEWS
    path('datasets_list/<str:sort>/<str:order>/', views.dataset_list, name='dataset-list'),
    path('collections_list/<str:sort>/<str:order>/', views.collection_list, name='collection-list'),
    path('areas_list/<str:sort>/<str:order>/', views.area_list, name='area-list'),
    path('groups_list/<str:sort>/<str:order>/', views.group_list, name='group-list'),

    path('modal/', TemplateView.as_view(template_name="main/modal.html"), name="dynamic-modal"),
    
    path('open-api/', views.OpenAPIView, name='open_api'),
]
#] + static(settings.MEDIA_URL, document_root = settings.MEDIA_ROOT)
if settings.DEBUG is True:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
