# api/templatetags/preview_extras.py

from collections import OrderedDict
from datetime import datetime

from django import template

register = template.Library()


@register.filter
def format_list(data_list, separator=', '):
    """Formats a list of dicts, OrderedDicts, or simple values for preview display."""
    if not data_list:
        return "N/A"
    formatted = []
    for item in data_list:
        if isinstance(item, (dict, OrderedDict)):
            formatted.append(
                item.get('toponym')
                or item.get('label')
                or item.get('name')
                or item.get('gn_class')
                or str(item)
            )
        else:
            formatted.append(str(item))
    return separator.join(formatted)


@register.filter
def format_ul(data_list):
    """Render a list of items as an unordered list (<ul>)."""
    if not data_list:
        return "N/A"
    items = "".join(f"<li>{str(item)}</li>" for item in data_list)
    return f"<ul>{items}</ul>"


@register.filter
def iso_to_date(value, fmt="%d %b %Y"):
    if not value:
        return "N/A"
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt.strftime(fmt)
    except ValueError:
        return value
