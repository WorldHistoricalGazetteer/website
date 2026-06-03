# leads/urls.py
from django.urls import path

from . import views

app_name = 'leads'

urlpatterns = [
    path('suggest/', views.suggest_lead, name='suggest'),
    path('suggest/thanks/', views.suggest_thanks, name='suggest_thanks'),
]
