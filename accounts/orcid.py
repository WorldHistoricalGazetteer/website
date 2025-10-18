import logging
from datetime import timedelta
from typing import Optional

import requests
from django.conf import settings
from django.contrib import auth, messages
from django.contrib.auth import get_user_model
from django.contrib.auth.backends import BaseBackend
from django.db import transaction
from django.shortcuts import redirect
from django.urls import reverse
from django.utils.http import urlencode
from django.utils.timezone import now

User = get_user_model()
logger = logging.getLogger('authentication')


def get_best_email(orcid_record: dict) -> str | None:
    emails = orcid_record.get("person", {}).get("emails", {}).get("email", []) or []
    if not emails:
        return None

    # Keep only verified emails
    verified = [e for e in emails if e.get("verified")]
    if not verified:
        return None

    # Prefer the primary if it exists, else the first verified
    primary = next((e.get("email") for e in verified if e.get("primary")), None)
    return primary or verified[0].get("email")


def _date_tuple(d: dict | None) -> tuple[int, int, int]:
    """Convert ORCID date dict to sortable (Y,M,D). Missing -> (0,0,0)."""
    if not d:
        return (0, 0, 0)

    def to_int(x):
        try:
            return int(x.get("value")) if x else 0
        except:
            return 0

    return (to_int(d.get("year")), to_int(d.get("month")), to_int(d.get("day")))


def _summaries_from(record: dict, block: str, summary_key: str) -> list[dict]:
    """
    Collect summary dicts from activities-summary block, e.g.
    block='employments', summary_key='employment-summary'
    """
    groups = record.get("activities-summary", {}).get(block, {}).get("affiliation-group", [])
    items = []
    for g in groups:
        for s in g.get("summaries", []):
            if summary_key in s:
                items.append(s[summary_key])
    return items


def _format_org(summary: dict) -> str:
    org = summary.get("organization", {}) or {}
    name = org.get("name")
    addr = org.get("address", {}) or {}
    city = addr.get("city")
    region = addr.get("region")
    country = addr.get("country")
    parts = [name, city, region, country]
    return ", ".join([p for p in parts if p])


def _pick_best_affiliation(summaries: list[dict]) -> Optional[dict]:
    if not summaries:
        return None
    # Prefer current (no end-date); if multiple, pick the one with latest start-date or last-modified.
    current = [s for s in summaries if not s.get("end-date")]

    def sort_key(s: dict):
        # For ordering: end-date, then start-date, then last-modified
        end_t = _date_tuple(s.get("end-date"))
        start_t = _date_tuple(s.get("start-date"))
        lmd = s.get("last-modified-date", {}) or {}
        lmd_val = int(lmd.get("value", 0)) if isinstance(lmd.get("value"), (int, str)) else 0
        return (end_t, start_t, lmd_val)

    if current:
        # Among current, prefer most recently modified or most recent start
        current.sort(key=sort_key, reverse=True)
        return current[0]
    # Otherwise pick the most recent past
    summaries.sort(key=sort_key, reverse=True)
    return summaries[0]


def get_affiliation(record: dict) -> Optional[str]:
    """
    Best-guess single affiliation string. Prefers current employment,
    else most recent employment; falls back to education if no employment.
    """
    # 1) Try employments
    emp_summaries = _summaries_from(record, "employments", "employment-summary")
    best = _pick_best_affiliation(emp_summaries)
    if best:
        return _format_org(best)

    # 2) Fallback to educations
    edu_summaries = _summaries_from(record, "educations", "education-summary")
    best = _pick_best_affiliation(edu_summaries)
    if best:
        return _format_org(best)

    return None


def get_web_page(orcid_record: dict) -> str | None:
    """Return the first researcher URL, if available."""
    urls = orcid_record.get("person", {}).get("researcher-urls", {}).get("researcher-url", [])
    if not urls:
        return None
    first = urls[0].get("url", {})
    return first.get("value")


class OIDCBackend(BaseBackend):

    def authenticate(self, request, orcid_id=None, record=None, token_json=None, **kwargs):
        """
        Authenticate or create user based on ORCiD record.
        """
        if not orcid_id or not record:
            logger.error("Missing ORCiD ID or record during authentication.")
            return None

        logger.debug(f"Authenticating ORCiD user: {orcid_id}")
        logger.debug(f"ORCiD record received: {record}")

        # ORCiD identifier string is just the last part (e.g. 0000-0002-1825-0097)
        orcid_identifier = orcid_id.rsplit('/', 1)[-1] if "/" in orcid_id else orcid_id

        # Extract names from record (fallback empty)
        person = record.get("person", {})
        name_obj = person.get("name", {}) if person else {}
        given_name = name_obj.get("given-names", {}).get("value") or ""
        family_name_obj = name_obj.get("family-name")
        family_name = family_name_obj.get("value") if family_name_obj else ""

        # Build username - only include family name if present
        username_parts = [given_name] if given_name else []
        if family_name:
            username_parts.append(family_name)
        username_parts.append(orcid_identifier)
        new_username = "-".join(username_parts).replace(' ', '_')

        # Extract verified email(s)
        email = get_best_email(record)

        if request and request.user.is_authenticated and not request.user.orcid:
            # Existing legacy user, now linking ORCiD - TODO: remove this block after FULL migration
            if User.objects.filter(orcid=orcid_id).exclude(pk=request.user.pk).exists():
                messages.error(request, "This ORCiD is already linked to another account.")
                return None
            # User is logged in but not linked to ORCiD, update their ORCiD ID and other fields
            user = request.user
            user.orcid = orcid_id
            user.username = new_username

            # Only update email if user doesn't have a verified one
            if not user.has_verified_email and email:
                user.email = email
                user.email_confirmed = True  # Trust ORCID-verified emails

            user.given_name = given_name
            user.surname = family_name
            user.affiliation = get_affiliation(record) or ""
            user.web_page = get_web_page(record) or ""
            user.orcid_refresh_token = token_json.get("refresh_token")
            expires_in = token_json.get("expires_in")
            if expires_in is not None:
                user.orcid_token_expires_at = now() + timedelta(seconds=expires_in)

            # Save user
            with transaction.atomic():
                user.save()
            user._needs_news_check = True
        else:
            # Lookup or create by ORCiD
            expires_in = token_json.get("expires_in")

            # Prepare defaults for new user
            defaults = {
                "username": new_username,
                "given_name": given_name,
                "surname": family_name,
                "affiliation": get_affiliation(record) or "",
                "web_page": get_web_page(record) or "",
                "orcid_refresh_token": token_json.get("refresh_token"),
                "orcid_token_expires_at": (
                    now() + timedelta(seconds=expires_in) if expires_in else None
                ),
            }

            # Only include email if available from ORCID (opportunistic)
            if email:
                defaults["email"] = email
                defaults["email_confirmed"] = True  # Trust ORCID-verified emails

            user, created = User.objects.get_or_create(
                orcid=orcid_id,
                defaults=defaults,
            )
            if created:
                user._needs_news_check = True
            else:
                # Update existing user's fields (but not email if they have a verified one)
                user.username = new_username

                # Only update email if user doesn't have a verified one
                if not user.has_verified_email and email:
                    user.email = email
                    user.email_confirmed = True

                user.given_name = given_name
                user.surname = family_name
                user.affiliation = get_affiliation(record) or ""
                user.web_page = get_web_page(record) or ""
                user.orcid_refresh_token = token_json.get("refresh_token")
                expires_in = token_json.get("expires_in")
                user.orcid_token_expires_at = (
                    now() + timedelta(seconds=expires_in) if expires_in else None
                )
                with transaction.atomic():
                    user.save()

        return user

    def get_user(self, user_id):
        try:
            return User.objects.get(pk=user_id)
        except User.DoesNotExist:
            return None


def orcid_callback(request):
    # Handle denial
    if request.GET.get("error") == "access_denied":
        # Pass a flag to trigger the modal on the login page
        return redirect(f"{reverse('accounts:login')}?{urlencode({'orcid_denied': 1})}")

    # --- CSRF protection: check state ---
    session_state = request.session.get("oidc_state")
    state = request.GET.get("state")
    if not state or state != session_state:
        logger.error(
            "State mismatch or missing state. "
            f"Session: {session_state}, Callback: {state}"
        )
        return redirect("accounts:login")

    # clear it so it can't be reused
    request.session.pop("oidc_state", None)

    code = request.GET.get("code")
    if not code:
        logger.error("No authorization code provided by ORCID.")
        return redirect("accounts:login")

    # --- Exchange code for token ---
    token_url = f"{settings.ORCID_BASE}/oauth/token"
    data = {
        "client_id": settings.ORCID_CLIENT_ID,
        "client_secret": settings.ORCID_CLIENT_SECRET,
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": request.build_absolute_uri(reverse("orcid-callback")),
    }
    headers = {"Accept": "application/json"}

    try:
        token_response = requests.post(token_url, data=data, headers=headers)
        token_response.raise_for_status()
        token_json = token_response.json()
    except requests.RequestException as e:
        logger.error(f"Token request failed: {e}")
        return redirect("accounts:login")

    access_token = token_json.get("access_token")
    orcid_id = token_json.get("orcid")
    if not access_token or not orcid_id:
        logger.error(f"ORCID did not return the required token.")
        return redirect("accounts:login")

    # --- Fetch full ORCID record ---
    record = {}
    api_base = settings.ORCID_BASE.replace("//", "//api.")  # e.g. sandbox → api.sandbox
    record_url = f"{api_base}/v3.0/{orcid_id}/record"

    try:
        record_response = requests.get(
            record_url,
            headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
        )
        record_response.raise_for_status()
        record = record_response.json()
    except requests.RequestException as e:
        logger.warning(f"Failed to fetch ORCID record: {e}")

    # --- Authenticate user in Django ---
    user = auth.authenticate(request, orcid_id=orcid_id, record=record, token_json=token_json, backend=OIDCBackend)
    if user:
        auth.login(request, user)
        if getattr(user, "_needs_news_check", False):
            request.session["_needs_news_check"] = True
            return redirect("profile-edit")
        return redirect("home")

    logger.error("ORCID authentication failed.")
    return redirect("accounts:login")


def revoke_orcid_token(refresh_token: str) -> bool:
    """
    Revoke ORCID refresh token via the OAuth revoke endpoint.
    Returns True if successful.
    """
    url = f"{settings.ORCID_BASE}/oauth/revoke"
    data = {
        "client_id": settings.ORCID_CLIENT_ID,
        "client_secret": settings.ORCID_CLIENT_SECRET,
        "token": refresh_token,
    }
    try:
        resp = requests.post(url, data=data, timeout=5)
        resp.raise_for_status()
        logger.info("Revoked ORCID token successfully")
        return True
    except requests.RequestException as e:
        logger.error(f"Failed to revoke ORCID token: {e}")
        return False
