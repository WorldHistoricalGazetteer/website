"""Public views for GRACE.

Only the intake door is public. Everything else — the three registers, the
vocabularies, triage — lives in the Django admin, which is staff-only and is
deliberately the whole UI for now.
"""
import logging

from django.contrib import messages
from django.core.cache import cache
from django.shortcuts import redirect, render

from .forms import SourceSuggestionForm
from .vocabularies import DiscoverySource, IntakeStatus

logger = logging.getLogger(__name__)

# Per-IP rate limit for the unauthenticated form, carried over from the earlier
# prototype where these numbers proved about right.
RATE_LIMIT_MAX = 5           # submissions…
RATE_LIMIT_WINDOW = 60 * 60  # …per hour


def _client_ip(request):
    xff = request.META.get("HTTP_X_FORWARDED_FOR", "")
    if xff:
        return xff.split(",")[0].strip()
    return request.META.get("REMOTE_ADDR", "")


def suggest_source(request):
    """Suggest a printed gazetteer or dataset we should know about.

    Signed-in users are trusted: the honeypot and the rate limit are bypassed,
    and their account is linked to the suggestion instead of asking them to
    retype their name and address.

    New suggestions land on the untriaged intake status, which is what makes
    the queue visible in the admin. If no untriaged term exists yet the
    suggestion is still saved — losing someone's submission because a
    vocabulary row is missing would be far worse than an unlabelled row.
    """
    trusted = request.user.is_authenticated

    if request.method == "POST":
        rl_key = f"grace:suggest:rl:{_client_ip(request)}"
        if not trusted and cache.get(rl_key, 0) >= RATE_LIMIT_MAX:
            messages.warning(
                request,
                "You've sent several suggestions recently — please try again a "
                "little later.",
            )
            return render(request, "grace/suggest.html", {
                "form": SourceSuggestionForm(), "show_honeypot": True,
            })

        form = SourceSuggestionForm(request.POST, trusted=trusted)
        if form.is_valid():
            suggestion = form.save(commit=False)
            suggestion.status = IntakeStatus.objects.filter(
                is_untriaged=True, is_active=True).first()
            if trusted:
                suggestion.submitter_user = request.user
                suggestion.added_by = request.user
            if not suggestion.status:
                logger.warning(
                    "GRACE: no untriaged IntakeStatus configured — suggestion "
                    "#%s saved without one. Run seed_grace_vocabularies.",
                    suggestion.pk,
                )
            suggestion.save()

            if not trusted:
                try:
                    cache.incr(rl_key)
                except ValueError:
                    cache.set(rl_key, 1, RATE_LIMIT_WINDOW)

            logger.info("GRACE suggestion #%s: %s", suggestion.pk,
                        suggestion.title)
            return redirect("grace:suggest_thanks")
    else:
        form = SourceSuggestionForm(trusted=trusted)

    return render(request, "grace/suggest.html", {
        "form": form, "show_honeypot": not trusted,
    })


def suggest_thanks(request):
    return render(request, "grace/suggest_thanks.html")


def _seed_discovery_source_for_web():
    """The 'web form' discovery term, if it exists.

    Kept as a helper rather than inlined so the triage code and the importer
    agree on which term means "arrived from the public".
    """
    return DiscoverySource.objects.filter(slug="web-form").first()
