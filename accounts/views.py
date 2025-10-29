import secrets
import traceback

from django.contrib.auth import get_user_model
from django.contrib.auth import views as auth_views
from django.contrib.auth.decorators import login_required
from django.contrib.auth.mixins import LoginRequiredMixin
from django.core.signing import Signer, BadSignature
from django.http import JsonResponse
from django.utils.html import format_html
from django.views import View
from django.views.decorators.http import require_POST

from api.models import UserAPIProfile, APIToken
from whgmail.messaging import WHGmail

User = get_user_model()
from django.conf import settings
from django.contrib import auth, messages
from django.shortcuts import render, redirect, reverse

from accounts.forms import UserModelForm, EmailForm
from collection.models import CollectionGroupUser  # CollectionGroup,
import logging

logger = logging.getLogger('authentication')
from urllib.parse import urlencode


def orcid_denied_modal(request):
    return render(request, "accounts/orcid_denied_modal.html", {})


def build_orcid_authorize_url(request):
    state = secrets.token_urlsafe(24)
    nonce = secrets.token_urlsafe(24)

    request.session["oidc_state"] = state
    request.session["oidc_nonce"] = nonce

    params = {
        "client_id": settings.ORCID_CLIENT_ID,
        "response_type": "code",
        "scope": "/read-limited",
        "redirect_uri": request.build_absolute_uri(reverse("orcid-callback")),
        "state": state,
        "nonce": nonce,
    }
    return f"{settings.ORCID_BASE}/oauth/authorize?{urlencode(params)}"


def login(request):
    if request.method == 'POST':
        # Legacy WHG Login -> ORCiD
        username = request.POST.get('username', '').strip()
        password = request.POST.get('password', '').strip()
        orcid_auth_url = request.POST.get('orcid_auth_url', '')

        # Check for missing fields
        if not username or not password:
            messages.error(request, "Both Username and Password are required.")
            return redirect("accounts:login")

        try:
            # Check if user exists
            user = User.objects.get(username=username)
            if user.must_reset_password:
                # User must reset their password; store username in session
                request.session['username_for_reset'] = username
                return redirect('accounts:password_reset')
            else:
                # Attempt to authenticate using legacy backend only if no password reset is required
                user = auth.authenticate(request, username=username, password=password,
                                         backend='django.contrib.auth.backends.ModelBackend')
                if user is not None:
                    auth.login(request, user)
                    # Redirect to the ORCiD authorisation URL if provided
                    if orcid_auth_url:
                        # Ensure the ORCiD URL is valid
                        if orcid_auth_url.startswith(settings.ORCID_BASE):
                            return redirect(orcid_auth_url)
                        else:
                            logger.error("Invalid ORCiD authorisation URL.")
                            return redirect('accounts:login')
                    else:
                        # No ORCiD URL provided, redirect to home
                        return redirect('home')
                else:
                    # Authentication fails
                    messages.error(request, "Invalid password.")
                    return redirect('accounts:login')
        except User.DoesNotExist:
            # User not found
            messages.error(request,
                           "<h4><i class='fas fa-triangle-exclamation'></i> Invalid WHG username.</h4><p>Please correct this and try again.</p>")
            return redirect('accounts:login')
    else:
        # Prevent login page view if user is already authenticated
        if request.user.is_authenticated:
            return redirect('home')

        # GET request, render the login page with ORCiD auth URL
        return render(
            request,
            'accounts/login.html',
            context={"orcid_auth_url": build_orcid_authorize_url(request)}
        )


def logout(request):
    if request.method == 'POST':
        request.session.pop('username_for_reset', None)
        auth.logout(request)
        return redirect('home')


class CustomPasswordResetView(auth_views.PasswordResetView):
    template_name = 'register/password_reset_form.html'

    def get_success_url(self):
        return reverse('accounts:password_reset_done')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        username = self.request.session.get('username_for_reset')
        context['user'] = username
        return context


class CustomPasswordResetDoneView(auth_views.PasswordResetDoneView):
    template_name = 'register/password_reset_done.html'


class CustomPasswordResetConfirmView(auth_views.PasswordResetConfirmView):
    template_name = 'register/password_reset_confirm.html'

    def form_valid(self, form):
        # This method is called when the form is successfully submitted and valid
        response = super().form_valid(form)
        # Here, `form.user` is accessible because it's typically set in `PasswordResetConfirmView`
        user = form.user
        if hasattr(user, 'must_reset_password'):
            user.must_reset_password = False
            user.save()
        return response

    def get_success_url(self):
        return reverse('accounts:password_reset_complete')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        # Assume `self.user` is the user object, depending on your URL config
        context['user'] = self.user
        return context


class CustomPasswordResetCompleteView(auth_views.PasswordResetCompleteView):
    template_name = 'register/password_reset_complete.html'

    def get(self, request, *args, **kwargs):
        # clear the username from the session, set for v3 password reset
        request.session.pop('username_for_reset', None)
        # Call the original get method to continue normal processing
        return super().get(request, *args, **kwargs)


class CustomPasswordChangeView(auth_views.PasswordChangeView):
    template_name = 'register/password_change_form.html'

    def get_success_url(self):
        return reverse('accounts:password_change_done')


class CustomPasswordChangeDoneView(auth_views.PasswordChangeDoneView):
    template_name = 'register/password_change_done.html'

    def get(self, request, *args, **kwargs):
        # clear the username from the session, set for v3 password reset
        request.session.pop('username_for_reset', None)
        # Call the original get method to continue normal processing
        return super().get(request, *args, **kwargs)


def add_to_group(cg, member):
    cguser = CollectionGroupUser.objects.create(
        role='member',
        collectiongroup=cg,
        user=member
    )
    cguser.save()


@login_required
def profile_edit(request):
    # Check if this is an email confirmation request
    confirmation_token = request.GET.get('confirm_email')
    if confirmation_token:
        signer = Signer()
        try:
            user_id = signer.unsign(confirmation_token)
            if str(request.user.pk) == str(user_id):
                request.user.email_confirmed = True
                request.user.save()
                messages.success(request, '✓ Email address verified successfully!')
            else:
                messages.error(request, 'Invalid verification link.')
        except BadSignature:
            logger.error(f"Invalid email confirmation token: {confirmation_token}")
            messages.error(request, 'Invalid or expired verification link.')
        except Exception as e:
            logger.error(f"Error confirming email: {str(e)}")
            traceback.print_exc()
            messages.error(request, 'An error occurred while verifying your email.')

        # Redirect to clean URL without the token
        return redirect('profile-edit')

    if request.method == 'POST':
        email_action = request.POST.get('email_action')

        # Handle email update/verification
        if email_action == 'update':
            email_form = EmailForm(request.POST, user=request.user)
            if email_form.is_valid():
                new_email = email_form.cleaned_data['email']

                # Update user's email and mark as unconfirmed
                request.user.email = new_email
                request.user.email_confirmed = False
                request.user.save()

                # Generate verification token
                signer = Signer()
                token = signer.sign(request.user.pk)

                # Build confirmation URL that goes back to this profile page
                confirm_url = request.build_absolute_uri(
                    reverse('profile-edit') + f'?confirm_email={token}'
                )

                # Send verification email
                try:
                    WHGmail(request, {
                        'template': 'email_verification',
                        'subject': 'Verify your email address at World Historical Gazetteer',
                        'confirm_url': confirm_url,
                        'user': request.user,
                    })
                    messages.success(request, '✓ Email updated! Please check your inbox for a verification link.')
                except Exception as e:
                    logger.error(f"Failed to send verification email: {str(e)}")
                    traceback.print_exc()
                    messages.warning(request, 'Email updated, but we encountered an issue sending the verification email. Please contact support.')

                return redirect('profile-edit')
            else:
                # Form has errors - will be displayed in template
                form = UserModelForm(instance=request.user)
                api_token = getattr(request.user, "api_token", None)
                api_profile, _ = UserAPIProfile.objects.get_or_create(user=request.user)
                remaining_quota = max(api_profile.daily_limit - api_profile.daily_count, 0)
                total_quota = api_profile.daily_limit

                def not_available_html(field_name):
                    return format_html(
                        '<span class="text-muted fst-italic">Not available</span> '
                        '<i class="fas fa-circle-exclamation text-muted ms-1" '
                        'data-bs-toggle="tooltip" '
                        'data-bs-title="This information could be made available to WHG by updating your '
                        'ORCiD profile '
                        'and ensuring the {} field has visibility set to \'Trusted parties\' or \'Everyone\'." '
                        'style="cursor: help; font-size: 0.75em; vertical-align: super;"></i>',
                        field_name
                    )

                context = {
                    'has_email': bool(request.user.email),
                    'has_verified_email': bool(request.user.email_confirmed),
                    'email_display': request.user.email or not_available_html('email'),
                    'given_name_display': request.user.given_name or not_available_html('given name'),
                    'surname_display': request.user.surname or not_available_html('family name'),
                    'affiliation_display': request.user.affiliation or not_available_html('affiliation'),
                    'web_page_display': request.user.web_page or not_available_html('web page'),
                    'is_admin': request.user.groups.filter(name='whg_admins').exists(),
                    'needs_news_check': request.session.pop("_needs_news_check", False),
                    'form': form,
                    'email_form': email_form,
                    'ORCID_BASE': settings.ORCID_BASE,
                    "api_token_key": getattr(api_token, "key", ""),
                    "api_token_quota_remaining": remaining_quota,
                    "api_token_quota": total_quota,
                }
                return render(request, 'accounts/profile.html', context=context)

        # Handle resend verification email
        elif request.POST.get('resend_verification'):
            if request.user.email and not request.user.email_confirmed:
                signer = Signer()
                token = signer.sign(request.user.pk)

                # Build confirmation URL
                confirm_url = request.build_absolute_uri(
                    reverse('profile-edit') + f'?confirm_email={token}'
                )

                try:
                    WHGmail(request, {
                        'template': 'email_verification',
                        'subject': 'Verify your email address at World Historical Gazetteer',
                        'confirm_url': confirm_url,
                        'user': request.user,
                    })
                    messages.success(request, '✓ Verification email sent! Please check your inbox.')
                except Exception as e:
                    logger.error(f"Failed to resend verification email: {str(e)}")
                    traceback.print_exc()
                    messages.error(request, 'Failed to send verification email. Please try again later.')

            return redirect('profile-edit')

    # GET request - display the profile page
    form = UserModelForm(instance=request.user)
    email_form = EmailForm(user=request.user)
    api_token = getattr(request.user, "api_token", None)

    # Ensure profile exists
    api_profile, _ = UserAPIProfile.objects.get_or_create(user=request.user)

    remaining_quota = max(api_profile.daily_limit - api_profile.daily_count, 0)
    total_quota = api_profile.daily_limit

    # Helper to generate "not available" HTML with tooltip
    def not_available_html(field_name):
        return format_html(
            '<span class="text-muted fst-italic">Not available</span> '
            '<i class="fas fa-circle-exclamation text-muted ms-1" '
            'data-bs-toggle="tooltip" '
            'data-bs-title="This information could be made available to WHG by updating your '
            'ORCiD profile '
            'and ensuring the {} field has visibility set to \'Trusted parties\' or \'Everyone\'." '
            'style="cursor: help; font-size: 0.75em; vertical-align: super;"></i>',
            field_name
        )

    context = {
        'has_email': bool(request.user.email),
        'has_verified_email': bool(request.user.email_confirmed),
        'email_display': request.user.email or not_available_html('email'),
        'given_name_display': request.user.given_name or not_available_html('given name'),
        'surname_display': request.user.surname or not_available_html('family name'),
        'affiliation_display': request.user.affiliation or not_available_html('affiliation'),
        'web_page_display': request.user.web_page or not_available_html('web page'),
        'is_admin': request.user.groups.filter(name='whg_admins').exists(),
        'needs_news_check': request.session.pop("_needs_news_check", False),
        'form': form,
        'email_form': email_form,
        'ORCID_BASE': settings.ORCID_BASE,
        "api_token_key": getattr(api_token, "key", ""),
        "api_token_quota_remaining": remaining_quota,
        "api_token_quota": total_quota,
    }

    # logger.debug(context)

    return render(request, 'accounts/profile.html', context=context)


@login_required
def profile_download(request):
    user = request.user
    data = {
        'username': user.username,
        'email': user.email,
        'given_name': getattr(user, 'given_name', ''),
        'surname': getattr(user, 'surname', ''),
        'orcid': getattr(user, 'orcid', ''),
        'affiliation': getattr(user, 'affiliation', ''),
        'web_page': getattr(user, 'web_page', ''),
        'news_permitted': getattr(user, 'news_permitted', False),
    }
    response = JsonResponse(data)
    response['Content-Disposition'] = 'attachment; filename="user_data.json"'
    return response


@login_required
@require_POST
def profile_news_toggle(request):
    user = request.user
    news_permitted = request.POST.get('news_permitted') == 'on'
    user.news_permitted = news_permitted
    user.save()
    return JsonResponse({'status': 'success', 'news_permitted': news_permitted})


@login_required
def profile_delete(request):
    if request.method == 'POST':
        user = request.user
        user.delete()
        messages.success(request, "Your account has been deleted.")
        return redirect('home')
    else:
        return redirect('profile-edit')


class ProfileAPITokenView(LoginRequiredMixin, View):
    """
    Handles generating/regenerating and deleting a user's API token.
    """

    def post(self, request, *args, **kwargs):
        """
        Handles AJAX POST requests.
        Requires a POST parameter 'action' with value 'generate' or 'delete'.
        """
        action = request.POST.get('action')
        if action == "generate":
            return self._generate_or_regenerate(request)
        elif action == "delete":
            return self._delete(request)
        else:
            return JsonResponse({"error": "Invalid action."}, status=400)

    def _generate_or_regenerate(self, request):
        # Ensure the user has a profile
        UserAPIProfile.objects.get_or_create(user=request.user)

        token, created = APIToken.objects.get_or_create(
            user=request.user,
            defaults={"key": secrets.token_urlsafe(32)}
        )
        if not created:
            token.regenerate()
        return JsonResponse({"token": token.key})

    def _delete(self, request):
        try:
            token = request.user.api_token
            token.delete()
            return JsonResponse({"success": True})
        except APIToken.DoesNotExist:
            return JsonResponse({"error": "No API token exists for this user."}, status=400)