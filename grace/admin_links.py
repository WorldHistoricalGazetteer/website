"""Rendering for the "Connections" panel on every GRACE change form.

Django's admin shows the relations a record *points at* and says nothing about
the ones pointing back. Open a person and there is no sign of their
conversations, their datasets or the sources they compiled — which is precisely
the view GRACE was asked for: open one record, see everything tied to it.

So each change form carries a read-only panel built from here, listing both
directions and linking through. It is deliberately plain HTML rather than a
set of inlines: an inline is an editing surface, and half of these relations
are better followed than edited in place.

Everything is rendered against ``admin_site.name``, so the same ModelAdmin
produces GRACE links inside ``/grace/admin/`` and stock links inside
``/admin/`` without knowing which it is in.
"""
from django.urls import NoReverseMatch, reverse
from django.utils.html import format_html, format_html_join
from django.utils.safestring import mark_safe

#: Beyond this many, a section shows a count and a "see all" link instead of a
#: wall of names. Keeps a person with 400 interactions readable.
MAX_INLINE = 12


def change_url(obj, site):
    """Admin change URL for ``obj`` on the named admin site, or None."""
    if obj is None or obj.pk is None:
        return None
    meta = obj._meta
    try:
        return reverse(f"{site}:{meta.app_label}_{meta.model_name}_change",
                       args=[obj.pk])
    except NoReverseMatch:
        return None


def changelist_url(model, site, **filters):
    """Admin changelist URL, optionally with a query string of filters."""
    meta = model._meta
    try:
        url = reverse(f"{site}:{meta.app_label}_{meta.model_name}_changelist")
    except NoReverseMatch:
        return None
    if filters:
        url += "?" + "&".join(f"{k}={v}" for k, v in filters.items())
    return url


def obj_link(obj, site):
    """One record as a link, falling back to plain text if it has no admin."""
    if obj is None:
        return ""
    url = change_url(obj, site)
    if not url:
        return format_html("{}", str(obj))
    return format_html('<a href="{}">{}</a>', url, str(obj))


def _section(label, items, site, more_url=None, total=None):
    """One labelled row of links."""
    if not items and not total:
        return format_html(
            '<tr><th style="text-align:left;padding:3px 14px 3px 0;'
            'font-weight:600;vertical-align:top;white-space:nowrap">{}</th>'
            '<td style="padding:3px 0;color:var(--body-quiet-color)">—</td></tr>',
            label,
        )
    links = format_html_join(
        mark_safe(" &middot; "), "{}", ((obj_link(o, site),) for o in items))
    tail = ""
    if total is not None and total > len(items) and more_url:
        tail = format_html(
            ' <a href="{}" style="font-size:11px">… all {}</a>', more_url, total)
    elif more_url and items:
        tail = format_html(' <a href="{}" style="font-size:11px">…</a>', more_url)
    return format_html(
        '<tr><th style="text-align:left;padding:3px 14px 3px 0;'
        'font-weight:600;vertical-align:top;white-space:nowrap">{}</th>'
        '<td style="padding:3px 0">{}{}</td></tr>',
        label, links, tail,
    )


def panel(sections, site):
    """Render ``[(label, queryset_or_list, more_url), …]`` as a table.

    A section whose iterable is a queryset is counted once and sliced, so a
    record with hundreds of related rows costs two cheap queries rather than
    hundreds of rendered links.
    """
    rows = []
    for label, items, more_url in sections:
        if hasattr(items, "count") and hasattr(items, "__getitem__") \
                and not isinstance(items, (list, tuple)):
            total = items.count()
            shown = list(items[:MAX_INLINE])
        else:
            shown = [o for o in (items or []) if o is not None]
            total = len(shown)
        rows.append(_section(label, shown, site, more_url, total))
    return format_html(
        '<table style="border:0;margin:0">{}</table>',
        format_html_join("", "{}", ((r,) for r in rows)),
    )


def add_hint():
    """What the panel shows before a record exists to have connections."""
    return mark_safe(
        '<span style="color:var(--body-quiet-color)">Save this record first — '
        'its connections appear here once it exists.</span>'
    )
