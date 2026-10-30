"""The Record page with its Concordance section (place#325 Stage 2, items 1-2).

BETA-gated (ruling D1): served under the Atlas place view (``/atlas/place/``,
place#310) to users with ``can_access_beta``. ``/places/portal/`` and every
other public route are untouched ("change nothing public").

* ``/atlas/place/<pk>/`` (``atlas_record_page``): one contributor's account of a
  place, i.e. one ``Place`` row, rendered server-side from the same payload the
  portal builds (``PlacePortalView._build_record``), including the links,
  related places, descriptions and depictions that ``portal.js`` never shows.
  It does NOT compute the concordance, so it stays fast.
* ``/atlas/place/<pk>/concordance/`` (``atlas_record_concordance``): the
  concordance as JSON, from ``places.concordance.build_concordance``. That
  takes 0.3-3 s per record on prod (``place_link`` has no index on the
  identifier), so the page fetches it asynchronously behind a spinner.

The existing query-string endpoint ``/atlas/place/?id=<ns:id>`` (the Atlas
portal modal's JSON, ``search.views.atlas_place``) is unchanged.
"""
from __future__ import annotations

import logging
import re
import time
from urllib.parse import quote

from django.contrib.auth.views import redirect_to_login
from django.http import Http404, JsonResponse
from django.shortcuts import render
from django.urls import reverse

from api.dataset_access import get_visible_place_or_404

logger = logging.getLogger(__name__)

_HTTP_URL = re.compile(r"^https?://", re.I)


def _beta(user) -> bool:
    return bool(user.is_authenticated and getattr(user, "can_access_beta", False))


def explore_similar_url(place) -> dict:
    """Where *Explore similar records* goes for ``place``: ``{"url", "seeded_by"}``.

    The ONE place this is decided, so the soft-search / unminted-locus URL
    (place#325 Stage 2B) can replace it without touching the page.

    For now it is the existing Atlas toponym search (``/atlas/<toponym>``),
    seeded by the record's title. The record's location is NOT carried: the
    current Atlas reads no location from its URL (only ``?place=`` with a
    gazetteer, and ``?z=``), so a location parameter would be silently ignored
    and the label would overstate what the search does. ``seeded_by`` says
    exactly what is seeded, and the page words the button from it.
    """
    # `/atlas/<str:toponym>` cannot take a "/" (Django matches the DECODED
    # path, so %2F would not help); a title is a name, so drop it.
    title = re.sub(r"\s+", " ", (place.title or "").replace("/", " ")).strip()
    if not title:
        return {"url": reverse("atlas-page"), "seeded_by": []}
    return {"url": reverse("atlas-page") + quote(title, safe=""), "seeded_by": ["name"]}


def gateway_place_id(place) -> str | None:
    """The id the CRC gateway indexes ``place`` under, ``whg:<dataset_id>:<src_id>``
    (the form ``search.views._record_urls_for_hits`` parses back), or None when
    there is no reason to think the gateway holds it under that id:

    * no ``src_id``;
    * the dataset has no publicly visible registry row ``whg:<dataset_id>``
      (the ingest pushes one per indexed WHG dataset; no row, not in the index;
      the anonymous rule, because the link is shareable);
    * another record in the dataset shares the ``src_id`` (the indexing side then
      disambiguates as ``whg:<ds>:<src_id>:<n>``, so the plain form names nobody
      for certain).

    It does not ask the gateway: a page render must not wait on it, and the
    locus page re-runs the recipe anyway and says plainly when no member survives.
    """
    from api.models import GazetteerRegistryEntry
    from places.models import Place
    src = (place.src_id or "").strip()
    ds = place.dataset
    if not src or ds is None:
        return None
    if not GazetteerRegistryEntry.objects.visible_to(None).filter(id=f"whg:{ds.pk}").exists():
        return None
    if Place.objects.filter(dataset=ds, src_id=place.src_id).exclude(pk=place.pk).exists():
        return None
    return f"whg:{ds.pk}:{src}"


def default_cluster_link(place, user=None) -> dict | None:
    """The unminted locus link for *See this record's cluster at ... settings*:
    ``{"url", "source", "reason"}``, or None where it could not resolve (then the
    page omits the link rather than show a dead one).

    Recipe: members = [this record's gateway id]; retrieval = the record's title
    with the Atlas's default filters (none), match mode and size, exactly the body
    the Atlas Places search sends for that name; clustering = what a fresh Atlas
    search would use (SG ruling 2026-10-21): the gateway's CURRENT calibrated
    weights and thresholds (``search.locus.record_default_clustering``, cached
    10 min, 1.5 s timeout), with the fixed Atlas fallback theta because theta is
    fitted to a result set the page does not have. When the gateway cannot be
    asked, ``source`` is ``"fallback"`` and the Atlas fallback literals are used;
    the page labels which. Built with ``encode_recipe_token``. The gateway is
    only consulted for a record that can have a link at all.
    """
    from api.crc_client import DEFAULT_SEARCH_SIZE
    from places.locus_recipe import RecipeError, encode_recipe_token
    from search.locus import (ATLAS_DEFAULT_CLUSTERING, ATLAS_DEFAULT_MATCH_MODE, LOCUS_TOKEN_PARAM,
                              record_default_clustering)
    title = re.sub(r"\s+", " ", place.title or "").strip()
    member = gateway_place_id(place)
    if not title or not member:
        return None
    chosen = record_default_clustering(user)

    def build(clustering):
        return encode_recipe_token({
            "v": 1,
            "retrieval": {"query": title, "mode": ATLAS_DEFAULT_MATCH_MODE, "size": DEFAULT_SEARCH_SIZE},
            "clustering": clustering,
            "members": [member],
        })

    try:
        token = build(chosen["clustering"])
    except RecipeError as exc:
        if chosen["source"] != "gateway":   # e.g. a src_id with a space in it is not a place id
            logger.info("no default-settings locus link for place %s: %s", place.pk, exc)
            return None
        # The gateway sent values a recipe refuses (out of range): fall back, labelled.
        logger.warning("gateway clustering_params unusable for a recipe: %s", exc)
        chosen = {"clustering": ATLAS_DEFAULT_CLUSTERING, "source": "fallback", "reason": "invalid"}
        try:
            token = build(chosen["clustering"])
        except RecipeError:
            return None
    return {"url": f"{reverse('atlas-locus')}?{LOCUS_TOKEN_PARAM}={token}",
            "source": chosen["source"], "reason": chosen["reason"]}


def _http_url(value):
    """``value`` if it is an http(s) URL, else None (never a ``javascript:``
    or other scheme in an href built from contributed data)."""
    s = str(value or "").strip()
    return s if _HTTP_URL.match(s) else None


def _text(value):
    """A display string from an LPF value that may be a string, a dict with
    ``value``/``label``, or a list of either."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for k in ("value", "label", "@value", "title", "name"):
            if value.get(k):
                return _text(value[k])
        return ""
    if isinstance(value, (list, tuple)):
        return "; ".join(t for t in (_text(v) for v in value) if t)
    return str(value)


def _when_text(when) -> str:
    """A short "start to end" string from an LPF ``when``, or ""."""
    if not isinstance(when, dict):
        return ""
    spans = []
    for ts in when.get("timespans") or []:
        if not isinstance(ts, dict):
            continue

        def first(d):
            if isinstance(d, dict) and d:
                return str(next(iter(d.values())))
            return ""
        s, e = first(ts.get("start")), first(ts.get("end"))
        spans.append(f"{s} to {e}" if s and e and s != e else (s or e))
    return "; ".join(x for x in spans if x)


def _display_links(links):
    out = []
    for j in links or []:
        if not isinstance(j, dict):
            continue
        ident = str(j.get("identifier") or "").strip()
        if not ident:
            continue
        out.append({"type": str(j.get("type") or "").strip() or "untyped",
                    "identifier": ident, "href": _http_url(ident)})
    return out


def _display_related(related):
    out = []
    for j in related or []:
        if not isinstance(j, dict):
            continue
        to = str(j.get("relationTo") or j.get("relation_to") or "").strip()
        label = _text(j.get("label")) or to
        if not (label or to):
            continue
        out.append({
            "relation": str(j.get("relationType") or j.get("relation_type") or "").strip() or "related",
            "label": label, "to": to, "href": _http_url(to),
            "when": _when_text(j.get("when")),
            "citation": _text(j.get("citation")),
        })
    return out


def _display_descriptions(descriptions):
    out = []
    for j in descriptions or []:
        if not isinstance(j, dict):
            continue
        value = _text(j.get("value"))
        if not value:
            continue
        out.append({"value": value, "lang": str(j.get("lang") or "").strip(),
                    "source": _http_url(j.get("@id") or j.get("id"))})
    return out


def _display_depictions(depictions):
    out = []
    for j in depictions or []:
        if not isinstance(j, dict):
            continue
        href = _http_url(j.get("@id") or j.get("id") or j.get("url"))
        if not href:
            continue
        out.append({"href": href, "title": _text(j.get("title")) or href,
                    "license": _text(j.get("license"))})
    return out


def _record_payload(place) -> dict:
    """The portal's own payload for one record, so this page shows what the
    portal page has: ``PlacePortalView._build_record``, read, not changed.
    Notes are dropped (this page has no notes UI; ruling scope)."""
    from places.utils import attribListFromSet
    from places.views import PlacePortalView

    ds = place.dataset
    try:
        names = attribListFromSet("names", place.names.all(), exclude_title=place.title)
    except (KeyError, TypeError, ValueError, IndexError):  # malformed `when` in one name
        names = [{"label": n.jsonb.get("toponym")} for n in place.names.all() if isinstance(n.jsonb, dict)]
    try:
        types = attribListFromSet("types", place.types.all())
    except (KeyError, TypeError, ValueError, IndexError):
        types = []
    geoms = [g.jsonb for g in place.geoms.all()]
    record = PlacePortalView()._build_record(place, ds, names, types, geoms, attest_collections=[])
    record.pop("notes", None)
    return record


def atlas_record_page(request, pk):
    """The Record page (beta). Anonymous: to the login page. Signed in without
    beta access: 404, as ``main.views.reconciliation_view`` answers. A record
    the user may not see: 404, the same as one that does not exist. With
    V4_LANDING_ENABLED (place#325 Stage 4) the page is open to everyone, and
    the visibility rule alone decides which records it shows."""
    from places.v4_landing import linking_open_to, record_page_open_to, v4_landing_enabled
    if not record_page_open_to(request.user):
        if not request.user.is_authenticated:
            return redirect_to_login(request.get_full_path())
        raise Http404("Not found")
    place = get_visible_place_or_404(request.user, pk=pk)
    record = _record_payload(place)
    timespans = [f"{a} to {b}" if a != b else str(a) for a, b in (record.get("timespans") or [])
                 if a is not None]
    explore = explore_similar_url(place)
    links = _display_links(record.get("links"))
    context = {
        "place": place,
        "dataset": place.dataset,
        "record": record,
        "variants": [n for n in record.get("names") or [] if n.get("label")],
        "type_labels": [t["label"] for t in record.get("types") or [] if t.get("label")],
        "timespan_labels": timespans,
        "links": links,
        # The right column's "Elsewhere" card (place#325 Stage 5): the contributed links
        # that are web addresses, the same hrefs "Links, as contributed" renders.
        "outward_links": [l for l in links if l["href"]],
        "related": _display_related(record.get("related")),
        "descriptions": _display_descriptions(record.get("descriptions")),
        "depictions": _display_depictions(record.get("depictions")),
        "explore_similar": explore,
        "default_cluster": default_cluster_link(place, request.user),
        "concordance_endpoint": reverse("atlas-record-concordance", args=[place.pk]),
        # The gloss and environment sections for non-beta visitors: only with the
        # release switch on (ruling 2026-10-21); beta users always see them.
        "enrichment_public": v4_landing_enabled(),
    }
    # The "Links" section (place#343, ruling Q7): the ACTIVE Links naming this record, labelled
    # and disputable, the same for every viewer. Only with the release switch on: dark on prod.
    if v4_landing_enabled():
        from places.record_links import record_links
        context["public_links"] = record_links(place, request.user)
        context["public_links_shown"] = True
        context["link_disputable"] = linking_open_to(request.user)
        context["link_subject_id"] = f"whg:{place.pk}"
    return render(request, "places/atlas_record.html", context)


def atlas_record_concordance(request, pk):
    """The concordance of one record as JSON (beta), for the Record page.

    403 without beta access (the Atlas endpoints' answer); 404 for a record the
    user may not see. A record the user may see whose dataset is not public
    (their own private data) has no concordance: ``{"public": false}``, not an
    empty concordance, so the page cannot mistake one for the other.
    """
    from places.v4_landing import record_page_open_to
    if not record_page_open_to(request.user):
        return JsonResponse({"error": "beta access required"}, status=403)
    try:
        place = get_visible_place_or_404(request.user, pk=pk)
    except Http404:
        return JsonResponse({"error": "not found", "id": pk}, status=404)

    from places.concordance import PROVENANCE_LABELS, _repr_points, cached_concordance

    t0 = time.monotonic()
    # Shared with the gloss and environment endpoints (one build per record and viewer
    # class; places.concordance.cached_concordance).
    conc = cached_concordance(place, user=request.user)
    elapsed_ms = int((time.monotonic() - t0) * 1000)
    if conc is None:
        return JsonResponse({"public": False, "place_id": place.pk, "elapsed_ms": elapsed_ms})
    conc["public"] = True
    conc["record"]["repr_point"] = _repr_points([place.pk]).get(place.pk)
    conc["elapsed_ms"] = elapsed_ms
    conc["provenance_labels"] = PROVENANCE_LABELS
    conc["explore_similar"] = explore_similar_url(place)
    return JsonResponse(conc)
