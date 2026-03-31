# placetypes/urls.py
from django.urls import path
from . import views

app_name = 'placetypes'

urlpatterns = [
    # JSON endpoint for the type-tree widget (lazy-loads children)
    # GET /types/tree/          → top-level root categories
    # GET /types/tree/300008347 → children of aat:300008347
    path('tree/', views.type_tree, name='type-tree-roots'),
    path('tree/search/', views.type_tree_search, name='type-tree-search'),
    path('tree/<int:aat_id>/', views.type_tree, name='type-tree-children'),
]

