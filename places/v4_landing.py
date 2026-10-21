"""The v4 release switch (place#325 Stage 4): ONE setting, ``V4_LANDING_ENABLED``
(``LOCUS_MINTING_ENABLED`` is a deprecated alias; see ``DEPRECATED_ALIAS``).

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
3. **#271.** ``/entity/place:<contributed pid>/`` resolves for anonymous
   callers (to the Record page) when the record is public and not embargoed;
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


# Transitional alias (ruling 2026-10-21: the two flags merge into ONE release
# switch). DEPRECATED: LOCUS_MINTING_ENABLED used to switch locus minting and
# /entity/locus: on by itself; it is now honoured only as an alias for
# V4_LANDING_ENABLED (either true => the switch is on), so an environment that
# sets both (dev) keeps working. Remove it, and this name, once the host
# env_template no longer sets it.
DEPRECATED_ALIAS = "LOCUS_MINTING_ENABLED"


def v4_landing_enabled() -> bool:
    """THE v4 release switch: portal retirement, public Record page, #271,
    locus minting and /entity/locus: dereference (still beta-gated), all at once."""
    return bool(getattr(settings, SETTING, False)) or bool(getattr(settings, DEPRECATED_ALIAS, False))


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
