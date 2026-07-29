import json
from collections import Counter
from pathlib import Path

from django.conf import settings
from django.http import Http404, JsonResponse
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


# ── Third-party software audit (Tara Branstad review, 2026-07-29) ─────────────
# Rendered from the snapshot written by ``manage.py audit_licenses``, which reads
# the RUNNING environment and the modules webpack actually bundles — not the repo
# and not node_modules. A developer checkout carries packages that never ship
# (193 Python locally vs 175 deployed), and the production npm tree lists 660
# packages where only ~45 reach a browser.

_SNAPSHOT = Path(settings.BASE_DIR) / "licensing" / "data" / "software_licenses.json"

CATEGORY_LABELS = {
    "permissive": ("Permissive", "Use, modify and redistribute freely, including "
                                 "commercially; keep the copyright notice."),
    "public-domain": ("Public domain", "No rights reserved."),
    "copyleft-weak": ("Weak copyleft", "Changes to the library's own files stay under "
                                       "its licence; it does not reach WHG's code."),
    "copyleft-strong": ("Strong copyleft", "Can require a whole derived work to be "
                                           "released under the same licence."),
    "unknown": ("Needs review", "No licence could be resolved from the package's metadata."),
}
_CATEGORY_ORDER = ["copyleft-strong", "copyleft-weak", "permissive", "public-domain", "unknown"]


def _summarise(packages):
    """Counts per category, in severity order, so the copyleft picture is legible
    without reading 200 rows."""
    counts = Counter(p.get("category", "unknown") for p in packages)
    return [{"key": k, "label": CATEGORY_LABELS[k][0], "blurb": CATEGORY_LABELS[k][1],
             "count": counts[k]}
            for k in _CATEGORY_ORDER if counts.get(k)]


def software_licenses_view(request):
    """Public audit of every third-party package WHG deploys."""
    if not _SNAPSHOT.exists():
        raise Http404("No software licence audit has been generated yet.")
    data = json.loads(_SNAPSHOT.read_text())

    py = data.get("python", {})
    js = data.get("javascript", {})
    py_pkgs, js_pkgs = py.get("packages", []), js.get("packages", [])
    elections = [p for p in py_pkgs + js_pkgs if p.get("election")]

    return render(request, "licensing/software.html", {
        "audited": data.get("audited"),
        "revision": data.get("revision"),
        "app_version": data.get("app_version"),
        "python_direct": [p for p in py_pkgs if p.get("direct")],
        "python_all": py_pkgs,
        "python_count": len(py_pkgs),
        "js_direct": [p for p in js_pkgs if p.get("direct")],
        "js_all": js_pkgs,
        "js_count": len(js_pkgs),
        "js_missing": js.get("missing", False),
        "python_summary": _summarise(py_pkgs),
        "js_summary": _summarise(js_pkgs),
        "elections": elections,
        "needs_review": [p for p in py_pkgs + js_pkgs if p.get("category") == "unknown"],
    })


def software_licenses_json(request):
    """The raw audit snapshot — so a reuser can machine-check our dependencies
    rather than scrape the page."""
    if not _SNAPSHOT.exists():
        raise Http404("No software licence audit has been generated yet.")
    return JsonResponse(json.loads(_SNAPSHOT.read_text()))
