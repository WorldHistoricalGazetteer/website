"""Who may see the review UI at all.

Kept in its own module because two very different callers need the same answer:
:func:`phonetics.views._gate`, which 404s, and the site navigation, which decides
whether to offer a link. Computing those separately is how a menu ends up
pointing at a page that 404s.
"""

from django.conf import settings


def is_visible(user):
    """True if this visitor may reach ``/phonetics/``.

    Once launched the answer is everyone, anonymous visitors included: reading is
    public and only contributing needs an account. Until then it is staff and
    beta testers.

    Launch takes two things, not one. ``PHONETICS_PUBLIC`` alone is not enough —
    there must also be contribution terms somebody has signed off. Non-negotiable
    6 of place#252 is that the licensing is settled *before* launch, and a flag
    that one person can flip is not a settlement.
    """
    from .models import active_terms
    if getattr(settings, 'PHONETICS_PUBLIC', False):
        terms = active_terms()
        if terms is not None and terms.signed_off:
            return True
    return bool(getattr(user, 'is_authenticated', False)
                and getattr(user, 'can_access_beta', False))
