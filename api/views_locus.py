"""``/entity/locus:<PID>`` — dereferencing a locus (place#172, place#325 Stage 4).

Routed in ``api/urls_root.py`` AHEAD of the generic ``entity/<entity_id>/``
patterns, so ``place:`` (and every other object type in ``TYPE_MAP``) is
served exactly as before; this module never sees them.

Unless ``LOCUS_MINTING_ENABLED`` is true, ``locus_entity`` hands the request to
the generic entity views, which answered it before this route existed (anonymous
401, signed-in 404, HEAD as GET): v4 changes nothing public until release. When
it is on, the route is beta-gated like the
locus page: an anonymous browser is sent to sign in and back, an anonymous
machine client gets 401 JSON, a signed-in user without beta access gets 404.
Public (anonymous) dereference is the release's business, with place#271.

Content negotiation on ``Accept``:

* ``text/html``: the locus page for the stored recipe, at this URI;
* ``application/ld+json`` / ``application/json`` (and ``*/*`` or nothing):
  the JSON representation below. ``/entity/locus:<PID>/api`` forces it.

What a dereference does with the identifier:

* a recipe PID (``XXXX-XXXX-XXXX``, case-insensitive, hyphens optional, I/L/O
  read as 1/1/0): 400 when the shape or check digit is wrong (a typo is a hard
  error, never a different real locus); 404 when well-formed but never minted.
  A PID is a hash of the recipe, so an unminted PID cannot be minted here:
  minting is the explicit, confirmed act on ``/atlas/locus/?r=<token>``;
* a legacy ``whg_id`` (``1[0-9]{7}``): minted on first dereference from the
  ``LegacyUnionRecord`` snapshot as a FROZEN locus (no recipe; its captured
  membership), per place#172; 404 when the snapshot has no such id.

Tombstones (place#308): a withdrawn locus still answers 200 at its URI in
every representation, with its recipe, status, reason and date; it is not
re-run.

The JSON representation is honest about what the server can and cannot say.
The retrieval half of a recipe runs here (the same gateway call the page
makes), so ``current.candidates`` is the recipe's current retrieval and
``current.recipe_members`` says which of the recipe's members survive in it.
The CLUSTERING half runs only in the browser (``clustering.js``, deliberately
the one implementation; search/locus.py explains), so the selected cluster's
membership is NOT in this document: ``current.cluster_members`` is null and
``current.note`` says why. A server-side clusterer is a separate decision.
"""
from __future__ import annotations

import json
import logging

from django.contrib.auth.views import redirect_to_login
from django.http import Http404, HttpResponse, JsonResponse
from django.utils import timezone
from django.views.decorators.http import require_GET

from places import locus_registry as registry
from search.views_locus import entry_context, render_locus_page

logger = logging.getLogger(__name__)

HTML = "text/html"
JSONLD = "application/ld+json"
JSON = "application/json"
OFFERED = (JSONLD, JSON, HTML)   # tie order: a wildcard-only client gets JSON-LD

# A small, explicit context. The vocabulary IRI is a placeholder for a locus
# vocabulary WHG has not yet published (PLATO has no term for a locus: clusters
# stay out of PLATO by ruling, place#306); a question for SG in the Stage 4 report.
LOCUS_CONTEXT = {
    "@vocab": "https://whgazetteer.org/ns/locus#",
    "pid": "@id",
    "members": {"@container": "@set"},
    "candidates": {"@container": "@set"},
}

MEMBERSHIP_STATEMENT = (
    "This identifier names a recipe, not a set of records. The recipe is re-run against the "
    "index on every dereference, so what it returns changes as data is added, corrected, "
    "relicensed or withdrawn; WHG will not adjust a recipe to keep its answer stable (place#172)."
)
FROZEN_STATEMENT = (
    "This is a legacy WHG identifier admitted as a frozen locus: it carries no recipe and "
    "resolves to the membership captured from the retired union index, marked as such. No new "
    "frozen loci are created."
)
CLUSTER_NOTE = (
    "Cluster selection runs in the browser resolver (clustering.js, the one implementation of "
    "the clusterer), so this document carries the recipe's current retrieval and which of its "
    "members survive in it, not the selected cluster's membership. The HTML representation at "
    "this URI computes and shows the cluster."
)


# ---------------------------------------------------------------- negotiation

def _accept_choice(accept_header: str, force_json: bool = False) -> str:
    """The best of ``OFFERED`` for an Accept header; JSON-LD when nothing is
    preferred (``*/*``, absent, or nothing offered matches). q-values
    honoured; at equal q the more specific range wins, then the client's own
    listing order, then OFFERED order (``text/html, */*`` is HTML;
    ``application/json;q=0.9, text/html;q=0.8`` is JSON; bare ``*/*`` is JSON-LD)."""
    if force_json:
        return JSONLD
    best_key, best = None, JSONLD
    for i, part in enumerate((accept_header or "").split(",")):
        bits = [b.strip() for b in part.split(";") if b.strip()]
        if not bits:
            continue
        mtype, q = bits[0].lower(), 1.0
        for p in bits[1:]:
            if p.startswith("q="):
                try:
                    q = float(p[2:])
                except ValueError:
                    q = 0.0
        if q <= 0:
            continue
        for rank, offered in enumerate(OFFERED):
            if mtype == offered:
                specificity = 2
            elif mtype == offered.split("/")[0] + "/*":
                specificity = 1
            elif mtype == "*/*":
                specificity = 0
            else:
                continue
            key = (q, specificity, -i, -rank)
            if best_key is None or key > best_key:
                best_key, best = key, offered
    return best


def _wants_html(request, force_json: bool) -> bool:
    return _accept_choice(request.META.get("HTTP_ACCEPT", ""), force_json) == HTML


def _json(data, status=200, media=JSONLD):
    body = json.dumps(data, ensure_ascii=False, indent=2, default=str)
    resp = HttpResponse(body, content_type=f"{media}; charset=utf-8", status=status)
    resp["Vary"] = "Accept"
    return resp


# ---------------------------------------------------------------- representations

def _legacy_members(entry, user) -> dict:
    """The frozen membership, filtered by what ``user`` may see today. A member
    whose dataset is withheld, or that no longer exists, is counted, not named
    (place#172: withheld records are a count and a reason)."""
    from api.dataset_access import visible_places_q
    from places.models import Place
    snap = registry.entry_legacy(entry) or {}
    place_ids = [int(p) for p in snap.get("place_ids") or []]
    titles = list(snap.get("titles") or [])
    visible = {}
    if place_ids:
        rows = (Place.objects.filter(pk__in=place_ids).filter(visible_places_q(user))
                .values_list("id", "dataset__id", "src_id", "title"))
        visible = {pk: (ds, src, title) for pk, ds, src, title in rows}
    members, withheld = [], 0
    for i, pk in enumerate(place_ids):
        if pk in visible:
            ds, src, title = visible[pk]
            members.append({
                "place_id": f"whg:{ds}:{src}",
                "places_id": pk,
                "title": title or (titles[i] if i < len(titles) else ""),
                "captured_title": titles[i] if i < len(titles) else "",
                "record": f"/atlas/place/{pk}/",
                "entity": f"/entity/place:whg:{ds}:{src}/",
            })
        else:
            withheld += 1
    return {
        "frozen": True,
        "statement": FROZEN_STATEMENT,
        "captured_count": len(place_ids),
        "members": members,
        "withheld": {"count": withheld,
                     "reason": "not visible to this account, or no longer held"} if withheld else None,
    }


def _current(recipe, user) -> dict:
    """Re-run the recipe's RETRIEVAL now (the same gateway call the page makes)
    and report it honestly; never the cluster (see module docstring)."""
    from search.locus import retrieval_round_trips, search_options_for_retrieval
    from search.views import _gateway_search_response
    resolved_at = timezone.now().isoformat()
    options = search_options_for_retrieval(recipe["retrieval"])
    resp = _gateway_search_response(options, user)
    out = {"frozen": False, "statement": MEMBERSHIP_STATEMENT, "resolved_at": resolved_at,
           "cluster_members": None, "note": CLUSTER_NOTE}
    try:
        data = json.loads(resp.content)
    except ValueError:
        data = {}
    if resp.status_code != 200:
        out["error"] = {"status": resp.status_code, "detail": data.get("error") or data.get("detail") or "gateway"}
        return out
    if data.get("timeout"):
        out["error"] = {"status": 504, "detail": "the gateway took too long for this retrieval"}
        return out
    drift = retrieval_round_trips(recipe, data.get("request") or {})
    if drift is not None:
        logger.error("locus dereference: %s", drift)
        out["error"] = {"status": 500, "detail": "resolver could not reproduce the recipe's retrieval: " + drift}
        return out
    hits = data.get("hits") or []
    ids = []
    for h in hits:
        pid = str((h or {}).get("place_id") or "")
        if pid:
            ns, _, local = pid.partition(":")
            ids.append(ns.lower() + ":" + local if local else pid)
    present = [m for m in recipe["members"] if m in set(ids)]
    out.update({
        "candidates": ids,
        "candidate_total": data.get("total"),
        "recipe_members": {"present": present,
                           "absent": [m for m in recipe["members"] if m not in set(present)]},
        "withheld": data.get("withheld"),
    })
    return out


def locus_document(request, entry, resolve: bool = True) -> dict:
    """The JSON(-LD) representation of a registry entry."""
    ctx = entry_context(entry, request)
    doc = {
        "@context": LOCUS_CONTEXT,
        "@id": ctx["url"],
        "@type": "Locus",
        "pid": entry.pid,
        "kind": entry.kind,
        "status": entry.status,
        "minted": {"at": entry.minted_at.isoformat(), "by": ctx["minted_by"] or None, "via": entry.minted_via},
        "promise": {
            "identifier": "never reused for a different recipe and never withdrawn from the registry",
            "membership": "not stable: the recipe is re-run on every visit" if entry.kind == entry.KIND_RECIPE
                          else "frozen: captured from the legacy union index; carries no recipe",
            "recipe": "public, carried in full in this document" if entry.kind == entry.KIND_RECIPE else None,
        },
    }
    if entry.is_withdrawn:
        doc["withdrawn"] = {"at": entry.withdrawn_at.isoformat() if entry.withdrawn_at else None,
                            "reason": entry.withdrawn_reason}
    if entry.kind == entry.KIND_RECIPE:
        recipe = registry.entry_recipe(entry)
        doc["recipe"] = recipe
        doc["recipe_token"] = entry.token
        doc["recipe_page"] = request.build_absolute_uri(f"/atlas/locus/?r={entry.token}")
        if entry.is_withdrawn or not resolve:
            doc["current"] = {"frozen": False, "statement": MEMBERSHIP_STATEMENT, "resolved": False,
                              "note": "withdrawn: the recipe stays public and is not re-run"
                              if entry.is_withdrawn else "not resolved"}
        elif recipe is not None:
            doc["current"] = _current(recipe, request.user)
    else:
        snap = registry.entry_legacy(entry) or {}
        doc["legacy"] = {"whg_id": snap.get("whg_id"), "captured_count": len(snap.get("place_ids") or [])}
        doc["current"] = _legacy_members(entry, request.user)
    return doc


# ---------------------------------------------------------------- the view

def _gate(request, html: bool):
    if not registry.minting_enabled():
        raise Http404()
    if not request.user.is_authenticated:
        if html:
            return redirect_to_login(request.get_full_path())
        resp = _json({"error": "authentication required",
                      "detail": "locus dereference is beta-only until the v4 release"}, status=401, media=JSON)
        resp["WWW-Authenticate"] = 'Session realm="whg beta"'
        return resp
    if not request.user.can_access_beta:
        raise Http404()
    return None


def locus_entity(request, pid, force_json: bool = False):
    """``/entity/locus:<PID>/``. With ``LOCUS_MINTING_ENABLED`` off the request is
    handed to the generic entity view it reached before this route existed
    (``EntityDetailView``, or ``EntityFeatureView`` for ``/api``), so the answer is
    exactly the pre-branch one: anonymous 401 from DRF, signed-in 404, HEAD as GET
    (review #8; search/tests_locus_mint.py pins it against another unsupported type)."""
    if not registry.minting_enabled():
        from api.views_entity import EntityDetailView, EntityFeatureView
        view = EntityFeatureView if force_json else EntityDetailView
        return view.as_view()(request, entity_id=f"locus:{pid}")
    return _locus_entity(request, pid, force_json)


@require_GET
def _locus_entity(request, pid, force_json: bool = False):
    html = _wants_html(request, force_json)
    refused = _gate(request, html)
    if refused is not None:
        return refused
    try:
        entry, created = registry.lookup(pid)
    except registry.LookupError_ as exc:
        if html:
            return render_locus_page_error(request, pid, exc)
        return _json({"error": "not found" if exc.status == 404 else "bad request",
                      "detail": exc.message, "pid": pid}, status=exc.status, media=JSON)
    except registry.LocusCollision as exc:
        return _json({"error": "collision", "detail": str(exc)}, status=409, media=JSON)
    if created:
        logger.info("locus dereference minted legacy locus %s", entry.pid)

    if html:
        if entry.kind == entry.KIND_RECIPE:
            recipe = registry.entry_recipe(entry)
            if recipe is None:   # cannot happen for bytes the parser produced; say so rather than 500
                return _json({"error": "unreadable", "detail": "stored recipe does not parse"}, status=500, media=JSON)
            return render_locus_page(request, recipe, entry.token, entry=entry)
        from django.shortcuts import render
        ctx = {"locus_entry": entry_context(entry, request), "legacy": _legacy_members(entry, request.user),
               "minting": {"enabled": False}}
        return render(request, "search/locus.html", ctx)

    resp = _json(locus_document(request, entry), media=JSONLD if _accept_choice(
        request.META.get("HTTP_ACCEPT", ""), force_json) == JSONLD else JSON)
    if entry.is_withdrawn:
        resp["X-WHG-Locus-Status"] = "withdrawn"
    return resp


def locus_entity_api(request, pid):
    """``/entity/locus:<PID>/api``: the JSON representation whatever Accept says."""
    return locus_entity(request, pid, force_json=True)


def render_locus_page_error(request, pid, exc):
    from django.shortcuts import render
    return render(request, "search/locus.html", {
        "error": {"path": f"locus:{pid}", "message": exc.message},
        "error_status": exc.status,
        "token": "",
    }, status=exc.status)
