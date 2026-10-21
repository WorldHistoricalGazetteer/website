"""Enrichment endpoints for the beta Record and Locus pages (place#325 Stage 3).

All four are fetched by the pages after they paint. The LOCUS pair is beta-gated (403
JSON without ``can_access_beta``; recipe loci stay beta until reviewed). The RECORD
pair is beta-gated with the v4 release switch off (unchanged: 403), and PUBLIC with
it on (ruling SG 2026-10-21: "public at release, for anonymous visitors too"):

* ``GET  /atlas/place/<pk>/environment/``: one EDOPS signature for the record's group
  (the record plus its concordance aggregate; ``places.record_group``);
* ``GET  /atlas/place/<pk>/gloss/``: the data-derived gloss of that group (#299, limited);
* ``POST /atlas/locus/environment/``: signatures for a locus's current members, per
  member or one when tight (``api.edop_client.plan_locus``);
* ``POST /atlas/locus/gloss/``: the gloss of those members.

The locus endpoints take the members the page has just resolved (the cluster is
computed in the browser, so the server does not know it). That is safe because
WHG stores nothing posted and shows it to no one else, the members are capped and
reduced to plain capped values (``places.gloss.member_from_hit``), and WHG's EDOPS cache
is keyed on the request sent to EDOPS and filled only from EDOPS's answer, so a client
cannot put anything into it.

The public path (anyone who is not a beta user, switch on):

* its own throttles, per IP signed out and per user signed in, one scope per
  endpoint (``PUBLIC_GLOSS_RATE``, ``PUBLIC_ENVIRONMENT_RATE``); 429 + Retry-After,
  and a throttled request does no work. Beta users are not throttled (as before);
* the record must pass the caller's visibility rule (anonymous: public and not
  embargoed); private, embargoed and missing are one identical 404;
* the group comes from ``record_group`` under the caller's OWN viewer class
  (``anonymous_open`` signed out), never a beta group, and is then cut to the
  members an ANONYMOUS caller may see (``mark_offsite``'s rule) before anything is
  described or sent: so no embargoed or private member's name, count, source or
  coordinate is in a public gloss, environment or EDOPS request. The beta gloss
  still describes embargoed members to beta users, by design.

What is SENT to EDOPS (computingplace.org, which keeps a request 30 days) is narrower
than what the page shows, and the rule is about every member, not only the record: it
contains nothing an anonymous caller could not see (``places.record_group.mark_offsite``).
A non-public record sends nothing at all. A public record's group may hold members from
an embargoed gateway namespace, which a beta user sees; they stay in the concordance and
the gloss (neither leaves WHG), but no coordinate, hull or date of theirs goes to EDOPS.
(Before the review of 2026-10-21 this said only "a non-public record sends nothing", and
embargoed members' geometries were sent.) Posted locus members are checked by the same
rule: the coordinates are the client's, but they came from WHG's search for that user.
"""
from __future__ import annotations

import json
import logging
import time

from django.http import Http404, JsonResponse
from django.views.decorators.http import require_GET, require_POST
from rest_framework.throttling import SimpleRateThrottle

from api.dataset_access import get_visible_place_or_404

logger = logging.getLogger(__name__)

LOCUS_MAX_POSTED_MEMBERS = 200
LOCUS_MAX_BODY_BYTES = 512 * 1024

# Public (non-beta) callers of the Record page's two sections, switch on. Per IP signed
# out, per user signed in; each endpoint its own scope. The page asks each ONCE per view.
# * gloss: no EDOPS call, but a concordance build (0.3-3 s, cached 120 s per record and
#   viewer class) and one gateway details call: 60/min, the anonymous identifier
#   resolver's rate (api.views_entity.EntityResolveAnonThrottle).
# * environment: up to ONE EDOPS request per record (plan_concordance asks one basin
#   or one area; WHG caches the answer 30 days), on a service WHG does not run: 20/min,
#   so one address cannot walk records faster than one EDOPS request every 3 s.
# A shared NAT (a classroom) of more than 20 visitors opening Record pages in the same
# minute would see the environment section's "too many requests" retry for some.
PUBLIC_GLOSS_RATE = "60/min"
PUBLIC_ENVIRONMENT_RATE = "20/min"


class _PublicEnrichmentThrottle(SimpleRateThrottle):
    """Per user when signed in, else per client IP (DRF ``get_ident``, as the Record
    page's other anonymous throttles). The rate is read at use from the constant."""
    rate_name = ""

    def get_rate(self):
        return globals()[self.rate_name]

    def get_cache_key(self, request, view):
        user = getattr(request, "user", None)
        ident = f"u{user.pk}" if user is not None and user.is_authenticated else self.get_ident(request)
        return self.cache_format % {"scope": self.scope, "ident": ident}


class PublicGlossThrottle(_PublicEnrichmentThrottle):
    scope = "record_gloss_public"
    rate_name = "PUBLIC_GLOSS_RATE"


class PublicEnvironmentThrottle(_PublicEnrichmentThrottle):
    scope = "record_environment_public"
    rate_name = "PUBLIC_ENVIRONMENT_RATE"


def _beta(user) -> bool:
    return bool(user.is_authenticated and getattr(user, "can_access_beta", False))


def _forbidden():
    return JsonResponse({"error": "beta access required"}, status=403)


def _record_access(request, throttle_cls):
    """``(public, refusal)`` for a Record-page section. Beta: ``(False, None)``, as
    before. Anyone else: switch off -> today's 403; on -> the public path, charged
    to ``throttle_cls`` first (429 when spent, before any work)."""
    if _beta(request.user):
        return False, None
    from places.v4_landing import v4_landing_enabled
    if not v4_landing_enabled():
        return True, _forbidden()
    throttle = throttle_cls()
    try:
        allowed = throttle.allow_request(request, None)
    except Exception:  # noqa: BLE001 — a cache that cannot be read admits, as the other limiters do
        allowed = True
    if not allowed:
        wait = int(throttle.wait() or 1)
        resp = JsonResponse({"error": "rate limited",
                             "detail": f"at most {throttle.get_rate()} for this section"}, status=429)
        resp["Retry-After"] = str(max(1, wait))
        return True, resp
    return True, None


def _public_members(members: list) -> list:
    """The members an ANONYMOUS caller may see (``mark_offsite``'s rule, already set
    on every member by ``build_record_group``): the public path describes no other.
    An unmarked member is dropped (fail closed)."""
    from places.record_group import offsite_members
    return offsite_members(members)


def _member_view(m: dict) -> dict:
    from api.edop_client import valid_point
    return {"key": m["key"], "title": m.get("title"), "source": m.get("source"),
            "is_record": bool(m.get("is_record")), "located": valid_point(m.get("repr_point")),
            "sent_to_edops": m.get("offsite") is True}


def _environment_payload(plan: dict, results: list, members: list) -> dict:
    """One response shape for both pages. ``signatures`` is one entry per distinct
    signature; per-member basin answers that landed in the same basin are merged, so
    the page shows "these 3 members share basin 4620" once."""
    from api import edop_client as edop
    plan_view = {k: v for k, v in plan.items() if k not in ("requests",)}
    sigs = []
    # The default group is the members that were SENT: withheld ones are not in it.
    groups = plan.get("groups") or [[m["key"] for m in members
                                     if edop.valid_point(m.get("repr_point")) and m.get("offsite") is not False]]
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
    public, refused = _record_access(request, PublicEnvironmentThrottle)
    if refused is not None:
        return refused
    try:
        place = get_visible_place_or_404(request.user, pk=pk)
    except Http404:
        return JsonResponse({"error": "not found", "id": pk}, status=404)
    from api import edop_client as edop
    from django.contrib.gis.geos import GEOSGeometry
    from places.record_group import group_span, offsite_members, record_group

    t0 = time.monotonic()
    if not edop.enabled():
        return JsonResponse({"public": True, "status": edop.STATUS_DISABLED, "signatures": [],
                             "attribution": edop.ATTRIBUTION})
    group = record_group(place, user=request.user)
    if not group["public"]:
        return JsonResponse({"public": False, "place_id": place.pk})
    members = _public_members(group["members"]) if public else group["members"]
    # 🛑 Only members an anonymous caller could see are planned (mark_offsite); the
    # rest are listed in plan["withheld"] and never reach EDOPS.
    sendable = offsite_members(members)
    planned = []
    for m in sendable:
        geom = None
        if m.get("hull_wkt"):
            try:
                geom = GEOSGeometry(m["hull_wkt"], srid=4326)
            except Exception:  # noqa: BLE001
                geom = None
        planned.append({"key": m["key"], "repr_point": m.get("repr_point"), "geometry": geom, "offsite": True})
    plan = edop.plan_concordance(planned, span=group_span(sendable))
    plan["withheld"] = [m["key"] for m in members if m.get("offsite") is not True]
    results = edop.run_requests(plan["requests"])
    out = _environment_payload(plan, results, members)
    out.update({"public": True, "place_id": place.pk, "unresolved": group.get("unresolved", 0),
                "truncated": bool(group.get("truncated")),
                "elapsed_ms": int((time.monotonic() - t0) * 1000)})
    return JsonResponse(out)


@require_GET
def atlas_record_gloss(request, pk):
    public, refused = _record_access(request, PublicGlossThrottle)
    if refused is not None:
        return refused
    try:
        place = get_visible_place_or_404(request.user, pk=pk)
    except Http404:
        return JsonResponse({"error": "not found", "id": pk}, status=404)
    from places.gloss import build_gloss
    from places.record_group import record_group

    group = record_group(place, user=request.user)
    if not group["public"]:
        return JsonResponse({"public": False, "place_id": place.pk})
    # The gloss is NOT sent off-site, so for a BETA user it describes the group as they
    # may see it, embargoed gateway members included (unlike the EDOPS request above,
    # which uses offsite_members only). The public path describes only what an
    # anonymous caller may see.
    members = _public_members(group["members"]) if public else group["members"]
    gloss = build_gloss(members, kind="concordance", unresolved=group.get("unresolved", 0),
                        truncated=bool(group.get("truncated")))
    # place#325 Stage 5: Wikipedia links held on the group's wd: members (from the same
    # /api/places answer; no extra call). They obey the members' visibility: the public
    # path reads only what an anonymous caller may see; a beta user also reads members
    # it may see that an anonymous caller may not, as its gloss does.
    from places.wikipedia_links import wikipedia_payload
    return JsonResponse({"public": True, "place_id": place.pk, "gloss": gloss,
                         "members": [_member_view(m) for m in members],
                         **wikipedia_payload(members, include_withheld=not public)})


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
    # The anonymous rule, for the EDOPS request only (see the module docstring).
    from places.record_group import mark_offsite
    mark_offsite(members)
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
    from places.record_group import offsite_members
    plan = edop.plan_locus(offsite_members(members))
    plan["withheld"] = [m["key"] for m in members if m.get("offsite") is not True]
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
    # Not sent off-site: the gloss keeps members mark_offsite withholds from EDOPS.
    # ``wikipedia``: links on posted wd: hits (none today: the gateway search model
    # drops ``links``; places.wikipedia_links). Beta-only endpoint, so a beta user's
    # visibility; if this ever becomes public it must take the Record path's rule.
    from places.wikipedia_links import wikipedia_payload
    return JsonResponse({"gloss": build_gloss(members, kind="locus"),
                         "members": [_member_view(m) for m in members],
                         **wikipedia_payload(members, include_withheld=True)})
