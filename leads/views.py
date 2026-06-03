# leads/views.py
import logging

from django.contrib import messages
from django.core.cache import cache
from django.shortcuts import redirect, render

from .forms import PublicLeadForm
from .models import LeadProvenance, LeadStatus

logger = logging.getLogger(__name__)

# Simple per-IP rate limit for the unauthenticated form.
RATE_LIMIT_MAX = 5           # submissions
RATE_LIMIT_WINDOW = 60 * 60  # per hour (seconds)


def _client_ip(request):
    xff = request.META.get('HTTP_X_FORWARDED_FOR', '')
    if xff:
        return xff.split(',')[0].strip()
    return request.META.get('REMOTE_ADDR', '')


def suggest_lead(request):
    """Form to suggest a candidate dataset / print gazetteer (login optional).

    Logged-in users are trusted: the honeypot field and the per-IP rate limit
    (and any future CAPTCHA) are bypassed for them.
    """
    trusted = request.user.is_authenticated

    if request.method == 'POST':
        rl_key = f'leads:suggest:rl:{_client_ip(request)}'
        # Anti-spam (honeypot + rate limit) only applies to anonymous submitters.
        if not trusted and cache.get(rl_key, 0) >= RATE_LIMIT_MAX:
            messages.warning(request, "You've sent several suggestions recently — "
                                      "please try again a little later.")
            return render(request, 'leads/suggest.html',
                          {'form': PublicLeadForm(), 'show_honeypot': True})

        form = PublicLeadForm(request.POST, trusted=trusted)
        if form.is_valid():
            lead = form.save(commit=False)
            lead.status = LeadStatus.SUGGESTED
            lead.provenance = LeadProvenance.PUBLIC_FORM
            if trusted:
                lead.recommender_user = request.user
            lead.save()
            if not trusted:
                # increment rate-limit counter (set TTL on first write)
                try:
                    cache.incr(rl_key)
                except ValueError:
                    cache.set(rl_key, 1, RATE_LIMIT_WINDOW)
            logger.info('New dataset-lead suggestion #%s: %s', lead.pk, lead.title)
            return redirect('leads:suggest_thanks')
    else:
        form = PublicLeadForm(trusted=trusted)

    return render(request, 'leads/suggest.html',
                  {'form': form, 'show_honeypot': not trusted})


def suggest_thanks(request):
    return render(request, 'leads/suggest_thanks.html')
