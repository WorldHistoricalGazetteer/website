"""Template tags for the v4 release switch (place#325)."""
from django import template

from places.v4_landing import record_canonical_url as _record_canonical_url

register = template.Library()


@register.simple_tag(takes_context=True)
def record_canonical_url(context, place):
    """``{% record_canonical_url place as canon %}``: the Record's entity URI when the
    v4 release switch is on, else "" (see places.v4_landing.record_canonical_url)."""
    return _record_canonical_url(context.get("request"), place)
