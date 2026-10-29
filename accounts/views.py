import secrets
import traceback

from django.contrib.auth import get_user_model
from django.contrib.auth import views as auth_views
from django.contrib.auth.decorators import login_required
from django.contrib.auth.forms import PasswordResetForm
from django.contrib.auth.mixins import LoginRequiredMixin
from django.core.signing import Signer, BadSignature, TimestampSigner
from django.http import Http404, JsonResponse
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

from django.core.cache import caches
from users.models import email_lookup_hash

# Verification-email throttle. On 2026-10-11 two new users each triggered three sends within a
# minute (a double-click, then "resend" before the first had arrived), and nothing here limited
# how often a signed-in account could make WHG mail an arbitrary address. Redis (property_cache)
# because cache.add() there is atomic, so two near-simultaneous POSTs cannot both get through.
VERIFY_MAIL_COOLDOWN = 120        # seconds between sends to the same address
VERIFY_MAIL_HOURLY_CAP = 5        # sends per user per hour, across all addresses


def _verification_send_refusal(user, email):
    """Return None if a verification email may be sent now, else a message saying why not."""
    cache = caches['property_cache']
    if not cache.add(f"verify-mail:{user.pk}:{email_lookup_hash(email)}", 1, VERIFY_MAIL_COOLDOWN):
        return ('A verification email was sent to this address less than two minutes ago. '
                'Please check your inbox (and spam folder) before requesting another.')
    count_key = f"verify-mail-count:{user.pk}"
    cache.add(count_key, 0, 3600)
    if cache.incr(count_key) > VERIFY_MAIL_HOURLY_CAP:
        return ('Too many verification emails have been requested for this account in the last '
                'hour. Please try again later.')
    return None


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
        # In production ORCiD is the ONLY sign-in route — there is no public
        # password login, so a legacy account can only be reached by signing in
        # with ORCiD and claiming it (see orcid_claim). This closes the previous
        # password-only path that let legacy users skip ORCiD verification.
        if settings.ORCID_ENFORCED:
            messages.error(request, "Please sign in with ORCiD.")
            return redirect('accounts:login')

        # Non-production (dev/local) password bypass: the ORCiD (sandbox)
        # redirect is unreliable off production, so allow a direct password
        # sign-in here to keep those environments usable.
        username = request.POST.get('username', '').strip()
        password = request.POST.get('password', '').strip()

        if not username or not password:
            messages.error(request, "Both Username and Password are required.")
            return redirect("accounts:login")

        user = auth.authenticate(request, username=username, password=password,
                                 backend='django.contrib.auth.backends.ModelBackend')
        if user is not None:
            auth.login(request, user, backend='django.contrib.auth.backends.ModelBackend')
            logger.info(f"Non-prod password sign-in for user {username} (ORCiD not enforced)")
            messages.success(request, f"Welcome back, {user.get_full_name() or username}!")
            return redirect('home')
        else:
            messages.error(request, "Invalid username or password.")
            return redirect('accounts:login')
    else:
        # Prevent login page view if user is already authenticated
        if request.user.is_authenticated:
            return redirect('home')

        # GET request, render the login page with ORCiD auth URL
        return render(
            request,
            'accounts/login.html',
            context={
                "orcid_auth_url": build_orcid_authorize_url(request),
                # Show the legacy password form only where ORCiD is not enforced.
                "show_legacy_login": not settings.ORCID_ENFORCED,
            }
        )


def orcid_claim(request):
    """
    Landing step after an ORCiD sign-in that matched no existing account. The
    verified ORCiD identity is held in the session (``pending_orcid``); the user
    either creates a fresh WHG account or claims (links) an existing legacy
    account by proving ownership with their old username + password. No session
    is granted until an ORCiD is attached to an account.
    """
    from accounts.orcid import apply_orcid_profile
    from django.utils.timezone import now

    profile = request.session.get('pending_orcid')
    if not profile:
        return redirect('accounts:login')

    if request.method == 'POST':
        action = request.POST.get('action')

        if action == 'new':
            # Create a fresh, ORCiD-native account.
            user = User(orcid=profile['orcid_id'])
            apply_orcid_profile(user, profile)
            user.orcid_linked_at = now()
            user.legacy_login_retired = True  # native ORCiD account — no password login
            user.save()
            request.session.pop('pending_orcid', None)
            auth.login(request, user, backend='django.contrib.auth.backends.ModelBackend')
            request.session["_needs_news_check"] = True
            logger.info(f"Created new ORCiD account {user.username} ({profile['orcid_identifier']})")
            return redirect('profile-edit')

        if action == 'link':
            username = request.POST.get('username', '').strip()
            password = request.POST.get('password', '').strip()
            if not username or not password:
                messages.error(request, "Enter your existing WHG username and password to link your account.")
                return redirect('accounts:orcid_claim')

            legacy = auth.authenticate(request, username=username, password=password,
                                       backend='django.contrib.auth.backends.ModelBackend')
            if legacy is None:
                messages.error(request, "Invalid WHG username or password.")
                return redirect('accounts:orcid_claim')
            return _complete_claim(request, legacy, profile)

        if action == 'email':
            return _claim_by_email(request, profile)

        return redirect('accounts:orcid_claim')

    return render(request, 'accounts/orcid_claim.html', context={
        "orcid_identifier": profile.get('orcid_identifier'),
        "given_name": profile.get('given_name'),
        "orcid_email": profile.get('email'),
    })


# Claim by emailed link: for the legacy user who cannot remember the old password. Control of the
# account's confirmed mailbox stands in for the password, on the same footing as the recovery
# flow in accounts.views_legacy_link (and sharing its throttles and no-enumeration rules, which
# is why it borrows that module's helpers). It never SETS a password: ORCID_ENFORCED exists to
# retire them, and a reset flow would also bypass the confirmed-address policy.
CLAIM_SALT = 'accounts.orcid-claim-email'
CLAIM_MAX_AGE = 60 * 60       # the emailed link is good for an hour; the pending ORCiD sign-in
CLAIM_SIGNER = TimestampSigner(salt=CLAIM_SALT)  # it completes lives in the session, which is shorter-lived anyway
CLAIM_PER_IP, CLAIM_PER_ORCID = 10, 5

# Identical whether or not an account matched, was confirmed, or the request was throttled.
CLAIM_EMAIL_SENT = ("If an older WHG account matches what you entered and we can reach it by email, "
                    "we have sent that address a link. It may take a few minutes; check the spam "
                    "folder. If nothing arrives, use the Contact link in the page footer and we "
                    "will help you link the account.")


def _complete_claim(request, legacy, profile):
    """Attach the pending ORCiD to ``legacy``, sign in, and finish. Shared by both proofs."""
    from accounts.orcid import apply_orcid_profile
    from django.utils.timezone import now

    if legacy.orcid:
        messages.error(request, "That WHG account is already linked to an ORCiD.")
        return redirect('accounts:orcid_claim')
    if User.objects.filter(orcid=profile['orcid_id']).exclude(pk=legacy.pk).exists():
        messages.error(request, "This ORCiD is already linked to another account.")
        return redirect('accounts:orcid_claim')

    apply_orcid_profile(legacy, profile)
    legacy.orcid_linked_at = now()
    legacy.legacy_login_retired = True
    legacy.save()
    request.session.pop('pending_orcid', None)
    auth.login(request, legacy, backend='django.contrib.auth.backends.ModelBackend')
    logger.info(f"Linked ORCiD {profile['orcid_identifier']} to legacy account {legacy.pk}")
    messages.success(request, "Your existing WHG account is now linked to your ORCiD.")
    return redirect('home')


def _claim_by_email(request, profile):
    from accounts import views_legacy_link as ll

    identifier = request.POST.get('identifier', '').strip()
    # Count before looking anything up, so the limits cannot be mapped by what is cheap.
    over_ip = ll._bump(f'claim-ip:{ll._client_ip(request)}', ll.HOUR) > CLAIM_PER_IP
    over_orcid = ll._bump(f"claim-orcid:{profile['orcid_id']}", ll.DAY) > CLAIM_PER_ORCID
    legacy = ll._find_legacy(identifier)
    if (legacy and legacy.email and legacy.email_confirmed and legacy.is_active
            and not (over_ip or over_orcid)
            and ll._bump(f'mail-to:{legacy.email_hash}', ll.MAIL_PER_TARGET_WINDOW) <= ll.MAIL_PER_TARGET):
        token = CLAIM_SIGNER.sign(f"{legacy.pk}:{profile['orcid_id']}")
        url = request.build_absolute_uri(reverse('accounts:orcid_claim_confirm') + f'?token={token}')
        try:
            WHGmail(request, {
                'template': 'legacy_claim_verification',
                'subject': 'Link your World Historical Gazetteer account',
                'to_email': legacy.email,
                'greeting_name': legacy.name or legacy.username,
                'confirm_url': url,
                'legacy_username': legacy.username,
                'user': legacy,
                # A live token and the recipient's address: never mirror this to Zulip.
                'mirror_to_zulip': False,
            })
        except Exception as e:
            logger.error("orcid-claim email failed for pk=%s: %s", legacy.pk, type(e).__name__)
    elif legacy:
        # Not enumerable by the user (the message is the same), but visible to us.
        logger.info("orcid-claim email NOT sent for pk=%s (confirmed=%s active=%s throttled=%s)",
                    legacy.pk, legacy.email_confirmed, legacy.is_active, over_ip or over_orcid)
    messages.info(request, CLAIM_EMAIL_SENT)
    return redirect('accounts:orcid_claim')


def orcid_claim_confirm(request):
    """Arrive from the emailed link. The pending ORCiD sign-in must be in THIS browser session."""
    token = request.GET.get('token', '')
    try:
        legacy_pk, orcid_id = CLAIM_SIGNER.unsign(token, max_age=CLAIM_MAX_AGE).split(':', 1)
        legacy_pk = int(legacy_pk)
    except (BadSignature, ValueError):      # SignatureExpired is a BadSignature
        messages.error(request, "That link is invalid or has expired. Please start again.")
        return redirect('accounts:login')

    profile = request.session.get('pending_orcid')
    if not profile:
        messages.error(request, "Open this link in the same browser where you signed in with ORCiD. "
                                "Sign in with ORCiD again, choose \u201cI already have a WHG account\u201d, "
                                "and request a new link.")
        return redirect('accounts:login')
    if profile['orcid_id'] != orcid_id:
        messages.error(request, "That link was requested for a different ORCiD iD.")
        return redirect('accounts:orcid_claim')

    legacy = User.objects.filter(pk=legacy_pk).first()
    if not legacy or not legacy.is_active:
        messages.error(request, "That account can no longer be linked.")
        return redirect('accounts:orcid_claim')
    return _complete_claim(request, legacy, profile)



def logout(request):
    if request.method == 'POST':
        request.session.pop('username_for_reset', None)
        auth.logout(request)
        return redirect('home')


class WHGPasswordResetForm(PasswordResetForm):
    """Look the user up by the email lookup hash.

    Django's form filters ``email__iexact``, which can never match the encrypted ``User.email``
    column, so for every user it found nobody, mailed nobody, and still reported success.
    """

    def get_users(self, email):
        user = User.objects.by_email(email)
        if user and user.is_active and user.has_usable_password():
            yield user


class CustomPasswordResetView(auth_views.PasswordResetView):
    template_name = 'register/password_reset_form.html'
    form_class = WHGPasswordResetForm

    def dispatch(self, request, *args, **kwargs):
        # Where ORCiD is enforced a password is worthless for signing in, and a reset would
        # reopen the password route that the confirmed-address policy on the claim page
        # deliberately closes. Say so instead of accepting an address and doing nothing.
        if settings.ORCID_ENFORCED:
            messages.info(request, "WHG no longer uses passwords: sign in with ORCiD. If you had an "
                                   "account before ORCiD sign-in, choose \u201cI already have a WHG "
                                   "account\u201d after signing in with ORCiD.")
            return redirect('accounts:login')
        return super().dispatch(request, *args, **kwargs)

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

                refusal = _verification_send_refusal(request.user, new_email)
                if refusal:
                    messages.info(request, refusal)
                    return redirect('profile-edit')

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
                remaining_quota = api_profile.remaining_today()   # None ⇒ unlimited
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
        # Presence, not truthiness. The template's button is
        # `<button type="submit" name="resend_verification">` with no `value`, and an HTML
        # submit button with a name and no value submits the EMPTY STRING. `.get()` therefore
        # returned '' — falsy — so this branch never ran: the view fell through, re-rendered,
        # and returned 200 with no email sent and no error shown. Reported 2026-09-10 by a user
        # who had clicked it repeatedly over two days ("I tried several times ... never received
        # one"). Testing membership rather than value keeps working whatever the template sends.
        elif 'resend_verification' in request.POST:
            if request.user.email and not request.user.email_confirmed:
                refusal = _verification_send_refusal(request.user, request.user.email)
                if refusal:
                    messages.info(request, refusal)
                    return redirect('profile-edit')

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

    remaining_quota = api_profile.remaining_today()   # None ⇒ unlimited (a falsy daily_limit)
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
        'preferred_language': getattr(user, 'preferred_language', ''),
        'github_username': getattr(user, 'github_username', ''),
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


def _github_user_missing(handle):
    """True only when GitHub positively reports no such user.

    A mistyped handle would silently @-mention an unrelated GitHub account on a public
    issue, so the handle is checked before it is stored. Any doubt — network error, rate
    limit, anything but a clean 404 — resolves to False: we would rather store an
    unverified handle than refuse a correct one because GitHub was unreachable.
    """
    import requests
    headers = {'Accept': 'application/vnd.github+json'}
    token = getattr(settings, 'GITHUB_SNAG_TOKEN', '')
    if token:
        headers['Authorization'] = f'Bearer {token}'
    try:
        return requests.get(f'https://api.github.com/users/{handle}',
                            headers=headers, timeout=5).status_code == 404
    except Exception as e:  # noqa: BLE001
        logger.warning('GitHub handle check failed for %s: %s', handle, e)
        return False


@login_required
@require_POST
def profile_github_set(request):
    """Persist the signed-in beta tester's GitHub handle (or clear it with an empty value).

    Beta reports filed from the on-site forms are credited to ``@handle`` instead of the
    reporter's name when this is set, so we can @-mention them for follow-up questions on
    the public issue — GitHub then notifies them, if their notification settings allow.
    Offered to beta testers only; nobody else has reports to file.
    """
    if not request.user.can_access_beta:
        raise Http404()
    from django.core.exceptions import ValidationError
    from users.models import normalise_github_username, github_username_validator

    handle = normalise_github_username(request.POST.get('github_username'))
    if handle:
        try:
            github_username_validator(handle)
        except ValidationError as e:
            return JsonResponse({'status': 'error', 'message': e.messages[0]}, status=400)
        if _github_user_missing(handle):
            return JsonResponse(
                {'status': 'error',
                 'message': f'GitHub has no user “{handle}”. Check the spelling — a mistyped handle '
                            f'would mention someone else on your reports.'},
                status=400)
    request.user.github_username = handle
    request.user.save(update_fields=['github_username'])
    return JsonResponse({'status': 'success', 'github_username': handle})


@login_required
@require_POST
def profile_language_set(request):
    """Persist the signed-in user's UI language preference — a short code such
    as 'en', 'de', or 'local' ("as recorded"), or '' to clear it. Mirrors the
    client-side ``whg_lang`` localStorage so the choice follows the user across
    machines. Called by languages.js::setPreferredLanguage from both the profile
    selector and the map language control."""
    code = (request.POST.get('language') or '').strip().lower()
    if code and not (2 <= len(code) <= 8 and code.replace('-', '').isalpha()):
        return JsonResponse({'status': 'error', 'message': 'invalid language code'}, status=400)
    request.user.preferred_language = code
    request.user.save(update_fields=['preferred_language'])
    return JsonResponse({'status': 'success', 'preferred_language': code})


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