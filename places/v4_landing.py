"""The v4 landing switch (place#325 Stage 4): ONE setting, ``V4_LANDING_ENABLED``.

Off (the default, and the state of every environment until the v4 release):
nothing public changes. Every route below behaves exactly as it did, because
the wrappers hand the request to the original view and return its response
object untouched (tested in ``places/tests_v4_landing.py``).

On, three things change together, because none is coherent without the others:

1. **Portal retirement.** The v3.2 portal routes that have a sound v4 target
   answer with a permanent redirect to it (``PORTAL_REDIRECTS``).
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
from django.http import HttpResponsePermanentRedirect
from django.urls import reverse

SETTING = "V4_LANDING_ENABLED"


def v4_landing_enabled() -> bool:
    return bool(getattr(settings, SETTING, False))


def record_uri(pk) -> str:
    """The Record's permanent URI, ``/entity/place:<pk>/`` (#325's model)."""
    return reverse("entity-detail", kwargs={"entity_id": f"place:{int(pk)}"})


# The v3.2 portal routes and what each becomes when the switch is on. ``target``
# None means the route keeps serving the legacy page with the switch on, for the
# reason given: its v4 target does not exist yet or is not decided.
PORTAL_REDIRECTS = (
    {"route": "/places/portal/<int:pid>/", "name": "places:place-portal-pid",
     "target": "/entity/place:<pid>/", "status": 301,
     "why": "The record and its one-step close matches; the Record page's Concordance "
            "section shows the same neighbours, typed and attributed."},
    {"route": "/places/<int:pid>/", "name": "places:place-bare-redirect",
     "target": "/entity/place:<pid>/", "status": 301,
     "why": "Today a 302 to the portal pid form; points straight at the record instead."},
    {"route": "/places/<int:whg_id>/portal/", "name": "places:place-portal-whg-id",
     "target": None, "status": None,
     "why": "#172: the frozen legacy locus /entity/locus:<whg_id>. Not yet public "
            "(Stage 4a, LOCUS_MINTING_ENABLED and beta-gated). Open question for SG."},
    {"route": "/places/portal/<str:encoded_ids>/", "name": "places:place-portal-multipid",
     "target": None, "status": None,
     "why": "An arbitrary list of records (and the whg_id form's canonical). No v4 page "
            "shows a list of records that is not a locus. Open question for SG."},
    {"route": "/places/portal/", "name": "places:place-portal",
     "target": None, "status": None,
     "why": "Session-scoped: redirects to the multi-pid form, so it follows that row."},
)


def portal_pid_view(legacy_view):
    """``/places/portal/<pid>/`` and ``/places/<pid>/``: off, the legacy view's
    own response; on, 301 to the Record URI. The redirect reveals nothing: the
    target applies the visibility rule, and a missing pid redirects too."""
    def view(request, *args, **kwargs):
        if v4_landing_enabled():
            return HttpResponsePermanentRedirect(record_uri(kwargs["pid"]))
        return legacy_view(request, *args, **kwargs)
    view.legacy_view = legacy_view
    return view


def record_page_open_to(user) -> bool:
    """Whether ``user`` may open the Record page at all (which records, is the
    visibility rule's business): beta users always; everyone once v4 lands."""
    if v4_landing_enabled():
        return True
    return bool(user.is_authenticated and getattr(user, "can_access_beta", False))
