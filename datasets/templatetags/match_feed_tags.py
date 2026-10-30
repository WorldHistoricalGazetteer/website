"""The "Links" section of the dataset page (place#334 4i Screen 3; place#338). Rendered only for
a beta user inside the dataset's circle: link counts are the contributor's (Q11), never public."""
from django import template

register = template.Library()


@register.inclusion_tag("datasets/_links_progress.html", takes_context=True)
def links_progress(context, ds):
    request = context.get("request")
    user = getattr(request, "user", None)
    if (ds is None or not getattr(user, "can_access_beta", False)
            or not ds.user_has_private_access(user)):
        return {"show": False}
    from datasets.views_feed import progress
    p = progress(ds)
    pct = round(100 * p["reviewed"] / p["records"]) if p["records"] else 0
    return {"show": True, "ds": ds, "p": p, "pct": pct}
