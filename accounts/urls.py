from django.urls import path
from . import views
from django.contrib.auth.views import * 

app_name = "accounts"

urlpatterns = [
    path('register/', views.register, name='register'),
    path('login/', views.login, name='login'),
    path('logout/', views.logout, name='logout'),
    path('profile/', views.update_profile, name='profile'),
    path('confirm_email/<str:token>/', views.confirm_email, name='confirm-email'),
    path('confirmation_sent/', views.confirmation_sent, name='confirmation-sent'),
    path('confirmation_success/', views.confirmation_success, name='confirmation-success'),

    # DEPRECATED
    # path('addusers/', views.addusers, name='add-users'),

    path('password_change/', PasswordChangeView.as_view(), name='password_change'),
    path('password_change/done/', PasswordChangeDoneView.as_view(), name='password_change_done'),
    path('password_reset/', PasswordResetView.as_view(), name='password_reset'),
    path('password_reset/done/', PasswordResetDoneView.as_view(), name='password_reset_done'),
    path('reset/<uidb64>/<token>/', PasswordResetConfirmView.as_view(), name='password_reset_confirm'),
    path('reset/done/', PasswordResetCompleteView.as_view(), name='password_reset_complete'),
    
]
