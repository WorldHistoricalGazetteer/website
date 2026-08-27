from django.urls import path

from . import views

app_name = "grace"

urlpatterns = [
    # The public intake door. `/contribute/` in the main URLconf points here.
    path("suggest/", views.suggest_source, name="suggest"),
    path("suggest/thanks/", views.suggest_thanks, name="suggest_thanks"),
]
