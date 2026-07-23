from django.conf import settings
from django.http import JsonResponse
from django.shortcuts import render

from .catalog import license_catalog


def _overlay():
    return getattr(settings, "WHG_OVERLAY_LICENSE", None)


def licenses_view(request):
    """Public reference page: every licence recorded in the WHG vocabulary,
    grouped by openness, with canonical + non-English deed links. Rendered live
    from the vocabulary (the source of truth) so it can never drift."""
    catalog = license_catalog()
    return render(request, "licensing/licenses.html", {
        "sections": catalog["sections"],
        "license_count": catalog["count"],
        "whg_overlay": _overlay(),
    })


def licenses_json(request):
    """JSON feed of the same catalogue, for the in-Workbench licence-picker modal
    (and any other client that needs the vocabulary). Flat ``entries`` list plus
    section metadata and the WHG overlay licence."""
    catalog = license_catalog()
    return JsonResponse({
        "entries": catalog["entries"],
        "sections": [{"key": s["key"], "title": s["title"], "blurb": s["blurb"]}
                     for s in catalog["sections"]],
        "count": catalog["count"],
        "whg_overlay": _overlay(),
    })
