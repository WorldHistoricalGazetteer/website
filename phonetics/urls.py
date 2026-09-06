from django.urls import path

from . import views

app_name = 'phonetics'

urlpatterns = [
    path('', views.home, name='home'),
    path('competence/', views.competence, name='competence'),
    path('competence/<int:pk>/delete/', views.competence_delete, name='competence-delete'),
    path('terms/', views.terms, name='terms'),
    path('queue/', views.review_queue, name='queue'),
    path('rule/<int:pk>/', views.rule_detail, name='rule'),
    path('rulesets/', views.ruleset_list, name='ruleset-list'),
    path('ruleset/<str:code>/', views.ruleset_detail, name='ruleset'),
    path('ruleset/<str:code>/sandbox/', views.sandbox, name='sandbox'),
    path('ruleset/<str:code>/export.csv', views.export_csv, name='export-csv'),
    path('ruleset/<str:code>/export.json', views.export_report, name='export-report'),
    path('question/<slug:slug>/', views.policy_question, name='question'),
    path('lint/', views.lint_queue, name='lint'),
    path('suggestions.json', views.suggestions_json, name='suggestions-json'),
    # AJAX
    path('api/validate/', views.api_validate, name='api-validate'),
    path('api/transcribe/', views.api_transcribe, name='api-transcribe'),
    path('api/sync/', views.api_sync, name='api-sync'),
]
