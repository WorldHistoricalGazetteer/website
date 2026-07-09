"""
Collaborative Workbench URLs (place#112). Included from whg/urls.py under the ``reconciliation/``
prefix (the Map-your-Data tool's own path). NB: ``/workbench/`` is an unrelated legacy TemplateView.
"""
from django.urls import path

from . import views

app_name = 'workbench'

urlpatterns = [
    path('projects/', views.projects, name='projects'),
    path('projects/<uuid:pid>/', views.project_detail, name='project-detail'),
    path('projects/<uuid:pid>/share/', views.project_share, name='project-share'),
    path('projects/<uuid:pid>/publish/', views.project_publish, name='project-publish'),
    path('projects/<uuid:pid>/collab-token/', views.collab_token, name='collab-token'),
    # Check-out an already-published item into a new editable project (pid = Collection id).
    path('checkout/collection/<int:pid>/', views.project_checkout, name='checkout-collection'),
    # Record-level check-out — correct a single published place (pid = Place id).
    path('checkout/place/<int:pid>/', views.project_checkout_place, name='checkout-place'),
    # Dataset ("gazetteer") check-out — subset / whole-that-fits (pid = Dataset id). Staff-gated.
    path('checkout/dataset/<int:pid>/', views.project_checkout_dataset, name='checkout-dataset'),
    path('checkout/dataset/<int:pid>/info/', views.dataset_checkout_info, name='checkout-dataset-info'),
    # Community record corrections ("Suggestions", plan-record-suggestions).
    path('suggestions/', views.submit_suggestion, name='suggestion-submit'),
    path('suggestions/<int:sid>/review/', views.review_suggestion, name='suggestion-review'),
    path('suggestions/<int:sid>/withdraw/', views.withdraw_suggestion, name='suggestion-withdraw'),
    path('suggestions/for-place/<int:pid>/', views.suggestions_for_place, name='suggestions-for-place'),
    path('teams/', views.teams, name='teams'),
    path('teams/<int:tid>/members/', views.team_members, name='team-members'),
    path('teams/<int:tid>/members/<int:uid>/', views.team_member_detail, name='team-member-detail'),
    path('shared/<str:token>/', views.shared_snapshot, name='shared'),
    path('datasets/search/', views.dataset_search, name='dataset-search'),
    path('gsheet/', views.gsheet_proxy, name='gsheet'),
    path('gdoc/', views.gdoc_proxy, name='gdoc'),
    path('ner/', views.ner_extract, name='ner'),
]
