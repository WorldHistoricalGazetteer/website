"""The v4 release switch (place#325 Stage 4): ONE setting, ``V4_LANDING_ENABLED``
(the old ``LOCUS_MINTING_ENABLED`` flag is gone: it no longer does anything).

Off (the default, and the state of every environment until the v4 release):
nothing public changes. Every route below behaves exactly as it did, because
the wrappers hand the request to the original view and return its response
object untouched (tested in ``places/tests_v4_landing.py``).

On, three things change together, because none is coherent without the others:

1. **Portal retirement.** The v3.2 portal routes that have a sound v4 target
   redirect to it (``PORTAL_REDIRECTS``), with ``PORTAL_REDIRECT_STATUS``: a
   302 until the v4 release, 301 from then (see the constant).
2. **The Record page is public.** ``/atlas/place/<pk>/`` and its concordance
   are served to anonymous and non-beta visitors too, for records the
   anonymous visibility rule admits (``visible_places_q``: a public dataset,
   not embargoed). Otherwise the redirects in 1 would land anonymous visitors
   on a login page.
3. **#271, and the Record page at its URI** (ruling 2026-10-21).
   ``/entity/place:<contributed pid>/`` RENDERS the Record page (HTML) or a small
   machine representation (JSON-LD / JSON), content-negotiated
   (``record_entity_response``), for anonymous callers too when the record is
   public and not embargoed; ``/atlas/place/<pk>/`` keeps working and both name
   the entity URI as canonical;
   private, embargoed or missing records answer 404, all alike. The
   machine-readable ``/entity/place:<pid>/api`` is NOT opened: distributing
   contributed data anonymously is a licensing decision (#271, 2026-09-21
   comment), not taken here.

Read the setting through ``v4_landing_enabled()`` only, so ``main`` (which
may not carry the settings line) is off by construction.
"""
from __future__ import annotations

from django.conf import settings
from django.http import HttpResponsePermanentRedirect, HttpResponseRedirect
from django.urls import reverse

SETTING = "V4_LANDING_ENABLED"

# 302 (not cached) until the v4 release. The issue's "permanent redirects" (#172,
# place#325) become 301 AT the release, by changing this one constant (it governs
# every redirect in this module: the portal pid routes AND the whg_id portal ->
# /entity/locus:<whg_id>/): a 301 is
# cached by browsers indefinitely, so a 301 served while the switch can still be
# turned off would outlive it. With the switch off, /entity/place:<pk>/ 302s back
# to /places/portal/<pk>/, which a cached 301 sends to /entity/place:<pk>/ again:
# a redirect loop in every browser that saw the 301 (review 2026-10-21).
PORTAL_REDIRECT_STATUS = 302


def v4_landing_enabled() -> bool:
    """THE v4 release switch: portal retirement, public Record page, #271,
    locus minting and /entity/locus: dereference (still beta-gated), all at once."""
    return bool(getattr(settings, SETTING, False))


def record_uri(pk) -> str:
    """The Record's permanent URI, ``/entity/place:<pk>/`` (#325's model)."""
    return reverse("entity-detail", kwargs={"entity_id": f"place:{int(pk)}"})


# The v3.2 portal routes and what each becomes when the switch is on. ``target``
# None means the route keeps serving the legacy page with the switch on, for the
# reason given: its v4 target does not exist yet or is not decided.
PORTAL_REDIRECTS = (
    {"route": "/places/portal/<int:pid>/", "name": "places:place-portal-pid",
     "target": "/entity/place:<pid>/", "status": PORTAL_REDIRECT_STATUS,
     "why": "The record and its one-step close matches; the Record page's Concordance "
            "section shows the same neighbours, typed and attributed."},
    {"route": "/places/<int:pid>/", "name": "places:place-bare-redirect",
     "target": "/entity/place:<pid>/", "status": PORTAL_REDIRECT_STATUS,
     "why": "Today a 302 to the portal pid form; points straight at the record instead."},
    {"route": "/places/<int:whg_id>/portal/", "name": "places:place-portal-whg-id",
     "target": "/entity/locus:<whg_id>/", "status": PORTAL_REDIRECT_STATUS,
     "why": "#172 / ruling 2026-10-21: the frozen legacy locus, ONLY when the whg_id is in "
            "the LegacyUnionRecord snapshot (so the locus resolves); otherwise the legacy "
            "portal keeps serving, labelled legacy. Legacy loci dereference publicly with the "
            "switch on (they are the target of this public redirect); recipe loci stay beta."},
    {"route": "/places/portal/<str:encoded_ids>/", "name": "places:place-portal-multipid",
     "target": None, "status": None,
     "why": "Ruling 2026-10-21: keeps serving the legacy portal, labelled legacy (a banner); "
            "no 410. No v4 page shows a list of records that is not a locus."},
    {"route": "/places/portal/", "name": "places:place-portal",
     "target": None, "status": None,
     "why": "Session-scoped: redirects to the multi-pid form, so it follows that row."},
    # Not a portal route, listed because the switch changes it too: with the switch
    # on, /entity/place:<pk>/ RENDERS the Record page (content-negotiated) instead of
    # redirecting; /atlas/place/<pk>/ keeps working (beta preview) and its canonical
    # link names the entity URI. Off: the legacy 302 to /places/portal/<pk>/.
    {"route": "/entity/place:<pk>/", "name": "entity-detail",
     "target": None, "status": 200,
     "why": "Ruling 2026-10-21: the Record page itself at the v4 release (api/views_entity.py)."},
)


def _redirect(url):
    cls = HttpResponsePermanentRedirect if PORTAL_REDIRECT_STATUS == 301 else HttpResponseRedirect
    return cls(url)


def legacy_portal_view(legacy_view):
    """A portal route that keeps serving the v3.2 page with the switch on, labelled
    legacy (ruling 2026-10-21): on, ``legacy_portal`` is added to the TemplateResponse's
    context, which ``place_portal.html`` turns into a small banner; off, the legacy
    response object is returned untouched (so the bytes are the pre-branch bytes)."""
    def view(request, *args, **kwargs):
        resp = legacy_view(request, *args, **kwargs)
        if v4_landing_enabled() and getattr(resp, "context_data", None) is not None \
                and not getattr(resp, "is_rendered", True):
            resp.context_data["legacy_portal"] = True
        return resp
    view.legacy_view = legacy_view
    return view


def portal_whg_id_view(legacy_view):
    """``/places/<whg_id>/portal/``: on, a ``PORTAL_REDIRECT_STATUS`` redirect to the
    frozen legacy locus ``/entity/locus:<whg_id>/`` when that locus resolves (the
    whg_id is ``1NNNNNNN`` and in the ``LegacyUnionRecord`` snapshot); otherwise,
    and always when off, the legacy portal (labelled legacy when on)."""
    labelled = legacy_portal_view(legacy_view)

    def view(request, *args, **kwargs):
        if v4_landing_enabled():
            from places.locus_recipe import is_legacy_locus_id
            from places.models import LegacyUnionRecord
            whg_id = str(kwargs.get("whg_id", ""))
            if is_legacy_locus_id(whg_id) and LegacyUnionRecord.objects.filter(pk=int(whg_id)).exists():
                return _redirect(reverse("entity-locus", kwargs={"pid": whg_id}))
        return labelled(request, *args, **kwargs)
    view.legacy_view = legacy_view
    return view


def portal_pid_view(legacy_view):
    """``/places/portal/<pid>/`` and ``/places/<pid>/``: off, the legacy view's
    own response; on, a ``PORTAL_REDIRECT_STATUS`` redirect to the Record URI,
    with the query string kept (the legacy bare route's RedirectView had
    ``query_string=True``). The redirect reveals nothing: the target applies the
    visibility rule, and a missing pid redirects too."""
    def view(request, *args, **kwargs):
        if v4_landing_enabled():
            url = record_uri(kwargs["pid"])
            qs = request.META.get("QUERY_STRING", "")
            if qs:
                url = f"{url}?{qs}"
            return _redirect(url)
        return legacy_view(request, *args, **kwargs)
    view.legacy_view = legacy_view
    return view


def record_page_open_to(user) -> bool:
    """Whether ``user`` may open the Record page at all (which records, is the
    visibility rule's business): beta users always; everyone once v4 lands."""
    if v4_landing_enabled():
        return True
    return bool(user.is_authenticated and getattr(user, "can_access_beta", False))


def linking_open_to(user) -> bool:
    """Whether ``user`` is offered the Link sheet and the "Link your records" feed (place#343,
    Phase 6 of place#334): beta users always; with the release switch on, every signed-in
    user (the feed still only for a dataset's own circle, which its view checks). Anonymous
    callers never: ``POST /api/links`` answers them 401 regardless."""
    if not getattr(user, "is_authenticated", False):
        return False
    return v4_landing_enabled() or bool(getattr(user, "can_access_beta", False))


# ---------------------------------------------------------------- /entity/place:<pk>/

RECORD_STATEMENT = (
    "A place: identifier names a gazetteer record: one contributor's account of a place, "
    "not the place itself (place#325)."
)
# The @context is WHG's minimal provisional context (/ns/whg-context.jsonld,
# api.views_context; ruling 2026-10-21), shared with the locus document. It replaced
# the placeholder ``@vocab`` https://whgazetteer.org/ns/record#, which resolved nowhere.


def _concordance_entry(e: dict, request) -> dict:
    """One concordance link, reduced to what a machine reader needs: what is linked,
    how, on whose word, and (for records WHG holds) the record's own entity URI."""
    out = {"tier": e.get("tier"), "identifier": e.get("identifier"), "link_type": e.get("link_type"),
           "aggregates": bool(e.get("aggregates")),
           "provenance": (e.get("provenance") or {}).get("label")}
    if e.get("placeholder"):
        out["placeholder"] = True
    target = e.get("target") or {}
    if target:
        out["target_status"] = target.get("status")
    recs = list(target.get("records") or [])
    if e.get("record"):
        recs.append(e["record"])
    held = [r.get("place_id") for r in recs if isinstance(r, dict) and isinstance(r.get("place_id"), int)]
    if held:
        out["records"] = [request.build_absolute_uri(record_uri(pk)) for pk in held]
    if e.get("cocited_identifier"):
        out["cocited_identifier"] = e["cocited_identifier"]
    return out


def record_document(request, place) -> dict:
    """The JSON(-LD) representation of a Record at its entity URI: small and honest.
    The full LPF stays at ``/api`` (closed to anonymous callers: licensing, #271)."""
    from api.views_context import whg_context_url
    from places.concordance import cached_concordance
    ds = place.dataset
    doc = {
        "@context": whg_context_url(request),
        "@id": request.build_absolute_uri(record_uri(place.pk)),
        "@type": "Record",
        "statement": RECORD_STATEMENT,
        "title": place.title,
        "src_id": place.src_id,
        "dataset": {"label": getattr(ds, "label", None), "title": getattr(ds, "title", None)},
        "lpf": request.build_absolute_uri(record_uri(place.pk) + "api"),
        "lpf_access": "signed-in session or API token",
    }
    conc = cached_concordance(place, user=request.user)
    if conc is None:
        doc["concordance"] = None
        doc["concordance_note"] = "only public records have a concordance"
        return doc
    doc["concordance"] = {
        "aggregated_types": ["exactMatch", "closeMatch"],
        "note": "sameAs counts as exactMatch; tier 3 (co-citation) is listed, never aggregated",
        "outbound": [_concordance_entry(e, request) for e in conc.get("outbound") or []],
        "inbound": [_concordance_entry(e, request) for e in conc.get("inbound") or []],
        "cocited": [_concordance_entry(e, request) for e in conc.get("cocited") or []],
        "truncated": conc.get("truncated"),
        "section": request.build_absolute_uri(record_uri(place.pk)) + "#concordance",
    }
    return doc


def record_entity_response(request, place):
    """``/entity/place:<pk>/`` with the switch on (ruling 2026-10-21). ``request`` is the
    DRF request, whose content negotiation has already chosen: HTML -> the Record page
    itself (``places.views_record.atlas_record_page``, reused, not forked) served at this
    URI; otherwise JSON-LD or JSON (``record_document``). The caller has already applied
    the visibility rule (private, embargoed and missing are one 404 before this)."""
    import json as _json
    from django.http import HttpResponse
    from django.utils.cache import patch_vary_headers
    renderer = getattr(request, "accepted_renderer", None)
    media = getattr(renderer, "media_type", "application/json")
    if media == "text/html":
        from places.views_record import atlas_record_page
        resp = atlas_record_page(request._request, pk=place.pk)
    else:
        media = "application/ld+json" if media == "application/ld+json" else "application/json"
        body = _json.dumps(record_document(request, place), ensure_ascii=False, indent=2, default=str)
        resp = HttpResponse(body, content_type=f"{media}; charset=utf-8")
    patch_vary_headers(resp, ("Accept",))
    return resp


def record_canonical_url(request, place) -> str:
    """The canonical URL for a Record page: the entity URI when the switch is on, else ""
    (off, the entity URI 302s to the portal, so the beta preview names nothing)."""
    if not v4_landing_enabled() or place is None or request is None:
        return ""
    return request.build_absolute_uri(record_uri(place.pk))
