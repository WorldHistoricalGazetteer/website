"""Template tags for the v4 release switch (place#325)."""
from django import template

from places.v4_landing import linking_open_to as _linking_open_to
from places.v4_landing import record_canonical_url as _record_canonical_url

register = template.Library()


@register.simple_tag(takes_context=True)
def record_canonical_url(context, place):
    """``{% record_canonical_url place as canon %}``: the Record's entity URI when the
    v4 release switch is on, else "" (see places.v4_landing.record_canonical_url)."""
    return _record_canonical_url(context.get("request"), place)


@register.filter
def linking_open(user):
    """``{% if user|linking_open %}``: whether the Link sheet is offered to ``user``
    (places.v4_landing.linking_open_to; place#343)."""
    return _linking_open_to(user)
