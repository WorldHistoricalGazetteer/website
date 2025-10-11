from django.urls import path

from . import views

app_name = "accounts"

urlpatterns = [
    # path('register/', views.register, name='register'),
    path('login/', views.login, name='login'),
    path('logout/', views.logout, name='logout'),
    path('orcid-denied-modal/', views.orcid_denied_modal, name='orcid_denied_modal'),

    path('profile/api-token/', views.ProfileAPITokenView.as_view(), name='profile-api-token'),

    path('profile/news-toggle/', views.profile_news_toggle, name='profile-news-toggle'),
    path('profile/download/', views.profile_download, name='profile-download'),
    path('profile/delete/', views.profile_delete, name='profile-delete'),

    path('password_reset/',
         views.CustomPasswordResetView.as_view(email_template_name='accounts/password_reset_email.html'),
         name='password_reset'),
    path('password_reset/done/', views.CustomPasswordResetDoneView.as_view(), name='password_reset_done'),
    path('reset/<uidb64>/<token>/', views.CustomPasswordResetConfirmView.as_view(), name='password_reset_confirm'),
    path('reset/done/', views.CustomPasswordResetCompleteView.as_view(), name='password_reset_complete'),

    # path('password_change/', PasswordChangeView.as_view(), name='password_change'),
    # path('password_change/done/', PasswordChangeDoneView.as_view(), name='password_change_done'),
    path('password_change/', views.CustomPasswordChangeView.as_view(), name='password_change'),
    path('password_change/done/', views.CustomPasswordChangeDoneView.as_view(), name='password_change_done'),

]
