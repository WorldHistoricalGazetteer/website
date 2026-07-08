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
    path('projects/<uuid:pid>/collab-token/', views.collab_token, name='collab-token'),
    path('teams/', views.teams, name='teams'),
    path('teams/<int:tid>/members/', views.team_members, name='team-members'),
    path('teams/<int:tid>/members/<int:uid>/', views.team_member_detail, name='team-member-detail'),
    path('shared/<str:token>/', views.shared_snapshot, name='shared'),
]
