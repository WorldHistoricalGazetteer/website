"""Enrichment endpoints for the beta Record and Locus pages (place#325 Stage 3).

All four are beta-gated like the concordance endpoint (403 JSON without
``can_access_beta``) and are fetched by the pages after they paint:

* ``GET  /atlas/place/<pk>/environment/``: one EDOPS signature for the record's group
  (the record plus its concordance aggregate; ``places.record_group``);
* ``GET  /atlas/place/<pk>/gloss/``: the data-derived gloss of that group (#299, limited);
* ``POST /atlas/locus/environment/``: signatures for a locus's current members, per
  member or one when tight (``api.edop_client.plan_locus``);
* ``POST /atlas/locus/gloss/``: the gloss of those members.

The locus endpoints take the members the page has just resolved (the cluster is
computed in the browser, so the server does not know it). That is safe because
nothing posted is stored or shown to anyone else, the members are capped and reduced
to plain capped values (``places.gloss.member_from_hit``), and the EDOPS cache is keyed
on the request sent to EDOPS and filled only from EDOPS's answer, so a client cannot
put anything into it. A non-public record sends nothing to EDOPS.
"""
from __future__ import annotations

import json
import logging
import time

from django.http import Http404, JsonResponse
from django.views.decorators.http import require_GET, require_POST

from api.dataset_access import get_visible_place_or_404

logger = logging.getLogger(__name__)

LOCUS_MAX_POSTED_MEMBERS = 200
LOCUS_MAX_BODY_BYTES = 512 * 1024


def _beta(user) -> bool:
    return bool(user.is_authenticated and getattr(user, "can_access_beta", False))


def _forbidden():
    return JsonResponse({"error": "beta access required"}, status=403)


def _member_view(m: dict) -> dict:
    from api.edop_client import valid_point
    return {"key": m["key"], "title": m.get("title"), "source": m.get("source"),
            "is_record": bool(m.get("is_record")), "located": valid_point(m.get("repr_point"))}


def _environment_payload(plan: dict, results: list, members: list) -> dict:
    """One response shape for both pages. ``signatures`` is one entry per distinct
    signature; per-member basin answers that landed in the same basin are merged, so
    the page shows "these 3 members share basin 4620" once."""
    from api import edop_client as edop
    plan_view = {k: v for k, v in plan.items() if k not in ("requests",)}
    sigs = []
    groups = plan.get("groups") or [[m["key"] for m in members if edop.valid_point(m.get("repr_point"))]]
    by_unit = {}
    for res, keys in zip(results, groups):
        pub = edop.public_result(res, members=list(keys))
        if plan.get("kind") == "per_member" and pub["status"] == edop.STATUS_OK:
            unit = (pub["summary"] or {}).get("unit") or {}
            uid = (unit.get("level"), unit.get("basin_id"), pub["request"].get("from_year"),
                   pub["request"].get("to_year"))
            if unit.get("basin_id") is not None and uid in by_unit:
                by_unit[uid]["members"].extend(keys)
                continue
            by_unit[uid] = pub
        sigs.append(pub)
    statuses = [s["status"] for s in sigs]
    if plan["kind"] in ("no_location", "dispersed"):
        overall = plan["kind"]
    elif statuses and all(s == edop.STATUS_OK for s in statuses):
        overall = "ok"
    elif edop.STATUS_OK in statuses:
        overall = "partial"
    else:
        overall = statuses[0] if statuses else "no_location"
    return {"status": overall, "plan": plan_view, "signatures": sigs,
            "members": [_member_view(m) for m in members], "attribution": edop.ATTRIBUTION}


@require_GET
def atlas_record_environment(request, pk):
    if not _beta(request.user):
        return _forbidden()
    try:
        place = get_visible_place_or_404(request.user, pk=pk)
    except Http404:
        return JsonResponse({"error": "not found", "id": pk}, status=404)
    from api import edop_client as edop
    from django.contrib.gis.geos import GEOSGeometry
    from places.record_group import group_span, record_group

    t0 = time.monotonic()
    if not edop.enabled():
        return JsonResponse({"public": True, "status": edop.STATUS_DISABLED, "signatures": [],
                             "attribution": edop.ATTRIBUTION})
    group = record_group(place, user=request.user)
    if not group["public"]:
        return JsonResponse({"public": False, "place_id": place.pk})
    members = group["members"]
    planned = []
    for m in members:
        geom = None
        if m.get("hull_wkt"):
            try:
                geom = GEOSGeometry(m["hull_wkt"], srid=4326)
            except Exception:  # noqa: BLE001
                geom = None
        planned.append({"key": m["key"], "repr_point": m.get("repr_point"), "geometry": geom})
    plan = edop.plan_concordance(planned, span=group_span(members))
    results = edop.run_requests(plan["requests"])
    out = _environment_payload(plan, results, members)
    out.update({"public": True, "place_id": place.pk, "unresolved": group.get("unresolved", 0),
                "elapsed_ms": int((time.monotonic() - t0) * 1000)})
    return JsonResponse(out)


@require_GET
def atlas_record_gloss(request, pk):
    if not _beta(request.user):
        return _forbidden()
    try:
        place = get_visible_place_or_404(request.user, pk=pk)
    except Http404:
        return JsonResponse({"error": "not found", "id": pk}, status=404)
    from places.gloss import build_gloss
    from places.record_group import record_group

    group = record_group(place, user=request.user)
    if not group["public"]:
        return JsonResponse({"public": False, "place_id": place.pk})
    gloss = build_gloss(group["members"], kind="concordance", unresolved=group.get("unresolved", 0))
    return JsonResponse({"public": True, "place_id": place.pk, "gloss": gloss,
                         "members": [_member_view(m) for m in group["members"]]})


def _posted_members(request):
    """``(members, error_response)`` from a locus page's POST body."""
    from places.gloss import member_from_hit
    if len(request.body or b"") > LOCUS_MAX_BODY_BYTES:
        return None, JsonResponse({"error": "body too large"}, status=413)
    try:
        body = json.loads(request.body or b"{}")
    except (TypeError, ValueError):
        return None, JsonResponse({"error": "invalid JSON"}, status=400)
    hits = body.get("members") if isinstance(body, dict) else None
    if not isinstance(hits, list) or not hits:
        return None, JsonResponse({"error": "members (a non-empty list) is required"}, status=400)
    if len(hits) > LOCUS_MAX_POSTED_MEMBERS:
        return None, JsonResponse({"error": f"at most {LOCUS_MAX_POSTED_MEMBERS} members"}, status=400)
    members, seen = [], set()
    for h in hits:
        m = member_from_hit(h)
        if m and m["key"] not in seen:
            seen.add(m["key"])
            members.append(m)
    if not members:
        return None, JsonResponse({"error": "no usable member"}, status=400)
    _name_sources(members, request.user)
    return members, None


def _name_sources(members, user):
    """Registry names for namespaces (``gn`` -> its registry title), in place."""
    from api.models import GazetteerRegistryEntry
    ns = {m["namespace"] for m in members if m.get("namespace")}
    names = dict(GazetteerRegistryEntry.objects.visible_to(user).filter(namespace__in=ns)
                 .exclude(entry_class="dataset").values_list("namespace", "name"))
    for m in members:
        if m.get("namespace") == "whg":
            m["source"] = "WHG contributed datasets"
        elif names.get(m.get("namespace")):
            m["source"] = names[m["namespace"]]


@require_POST
def atlas_locus_environment(request):
    if not _beta(request.user):
        return _forbidden()
    from api import edop_client as edop
    members, err = _posted_members(request)
    if err:
        return err
    if not edop.enabled():
        return JsonResponse({"status": edop.STATUS_DISABLED, "signatures": [], "attribution": edop.ATTRIBUTION})
    t0 = time.monotonic()
    plan = edop.plan_locus(members)
    results = edop.run_requests(plan["requests"])
    out = _environment_payload(plan, results, members)
    out["elapsed_ms"] = int((time.monotonic() - t0) * 1000)
    return JsonResponse(out)


@require_POST
def atlas_locus_gloss(request):
    if not _beta(request.user):
        return _forbidden()
    from places.gloss import build_gloss
    members, err = _posted_members(request)
    if err:
        return err
    return JsonResponse({"gloss": build_gloss(members, kind="locus"),
                         "members": [_member_view(m) for m in members]})
