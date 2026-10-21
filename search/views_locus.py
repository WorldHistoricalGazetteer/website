"""Minting a citable locus from the locus page (place#172, place#325 Stage 4).

Routes (``search/urls_atlas.py``), live only when the v4 release switch is on
(``places.v4_landing.v4_landing_enabled``), and then only for signed-in beta
testers (ruling D1):

    GET  /atlas/locus/mint/?r=<token>   preflight: the PID this recipe would get,
                                        whether it is already minted, existing
                                        loci sharing its members, the user's
                                        remaining allowance
    POST /atlas/locus/mint/             {"r": token, "confirm": true} -> mint

What is minted is the RECIPE the token carries, re-run on every visit, never
the member list the page happens to show. The page's modal says so and makes
the person confirm it; the server refuses a POST without ``confirm: true``.

The token goes through ``places.locus_recipe.decode_recipe_token`` and nothing
else, so a recipe carrying pair, edge or acceptance keys is rejected by the
parser's forbidden-key rule (place#306) before anything here sees it.
"""
from __future__ import annotations

import json
import logging

from django.http import Http404, HttpResponseRedirect, JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_http_methods, require_POST

from places import locus_registry as registry
from places.locus_recipe import RecipeError
from search.locus import LOCUS_TOKEN_PARAM, describe_recipe, load_token

logger = logging.getLogger(__name__)

MINT_URL = "/atlas/locus/mint/"
ATTRIBUTION_URL = "/atlas/locus/attribution/"
RESOLVE_URL = "/atlas/locus/resolve/"


def _beta_json_gate(request):
    """404 when minting is off (nothing public exists); 403 JSON without beta."""
    if not registry.minting_enabled():
        raise Http404()
    if not (request.user.is_authenticated and request.user.can_access_beta):
        return JsonResponse({"error": "beta access required"}, status=403)
    return None


ANONYMOUS_MINTER = "a WHG contributor"


def minted_by_name(entry) -> str:
    """The minter's real display name. For staff and the minter's own view
    only: what the public sees is :func:`public_minter_name`."""
    u = entry.minted_by
    if u is None:
        return ""
    return (getattr(u, "name", "") or getattr(u, "username", "") or "").strip()


def public_minter_name(entry) -> str:
    """The minter as the page and the JSON show them (rulings 2026-10-21): their
    name only if they opted in at mint time (``minter_public``), otherwise
    "a WHG contributor". "" when no account is attached (a legacy row minted on
    dereference, or a deleted account) and for a suppressed (withdrawn or
    redacted) row, which shows no minter at all."""
    if entry.minted_by_id is None or entry.is_suppressed:
        return ""
    if entry.minter_public:
        return minted_by_name(entry) or ANONYMOUS_MINTER
    return ANONYMOUS_MINTER


def entry_context(entry, request=None) -> dict:
    """The registry row as the page and the JSON representation show it.
    Nothing here reveals the recipe; a suppressed row's context carries no
    minter either."""
    path = entry.entity_path
    user = getattr(request, "user", None)
    is_minter = bool(user is not None and user.is_authenticated and entry.minted_by_id == user.pk)
    return {
        "pid": entry.pid,
        "kind": entry.kind,
        "status": entry.status,
        "withdrawn": entry.is_withdrawn,
        "redacted": entry.is_redacted,
        "redacted_at": entry.redacted_at,
        "suppressed": entry.is_suppressed,
        "minted_at": entry.minted_at,
        "minted_by": public_minter_name(entry),
        "minter_public": entry.minter_public,
        # The minter, viewing their own locus, is offered the retraction.
        "can_hide_minter": is_minter and entry.minter_public and not entry.is_suppressed,
        "minted_via": entry.minted_via,
        "withdrawn_at": entry.withdrawn_at,
        "withdrawn_reason": entry.withdrawn_reason,
        "path": path,
        "url": request.build_absolute_uri(path) if request is not None else path,
    }


def minting_context(recipe, token) -> dict:
    """What ``search/locus.html`` needs to offer (or not offer) minting for an
    UNMINTED page: ``enabled`` false keeps the Stage 2 disabled button and shows
    no would-be PID; when true the modal shows the PID the recipe would get, and
    ``existing`` names the locus already minted for exactly this recipe."""
    if not registry.minting_enabled():
        return {"enabled": False}
    try:
        entry, binding = registry.existing_for_recipe(recipe)
    except registry.LocusCollision as exc:
        return {"enabled": True, "collision": True, "pid": exc.pid, "mint_url": MINT_URL}
    return {
        "enabled": True,
        "pid": binding["pid"],
        "mint_url": MINT_URL,
        "existing": entry_context(entry) if entry is not None else None,
    }


def render_locus_page(request, recipe, token, entry=None, status=200):
    """Render ``search/locus.html`` for a recipe: unminted (no ``entry``) or the
    minted page at its ``/entity/locus:<PID>/`` URI. Both re-run the recipe
    through the same resolver; only the identity block differs."""
    ctx = {
        "recipe": recipe,
        "token": token,
        "summary": describe_recipe(recipe),
        "resolve_url": RESOLVE_URL,
    }
    if entry is None:
        ctx["minting"] = minting_context(recipe, token)
    else:
        ctx["locus_entry"] = entry_context(entry, request)
        ctx["minting"] = {"enabled": False}
    return render(request, "search/locus.html", ctx, status=status)


@require_http_methods(["GET", "POST"])
def atlas_locus_mint(request):
    refused = _beta_json_gate(request)
    if refused is not None:
        return refused
    if request.method == "GET":
        token = (request.GET.get(LOCUS_TOKEN_PARAM) or "").strip()
        payload = {}
    else:
        try:
            payload = json.loads(request.body or b"{}")
        except (ValueError, TypeError):
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        token = str(payload.get(LOCUS_TOKEN_PARAM) or "").strip()

    recipe, canonical_token, error = load_token(token)
    if error is not None:
        return JsonResponse({"error": "invalid recipe", **error}, status=400)

    try:
        entry, binding = registry.existing_for_recipe(recipe)
    except registry.LocusCollision as exc:
        logger.error("locus mint: 54-bit collision on %s", exc.pid)
        return JsonResponse({"error": "collision", "pid": exc.pid, "detail": str(exc)}, status=409)
    except RecipeError as exc:   # cannot happen after load_token, kept for the parser's sake
        return JsonResponse({"error": "invalid recipe", "path": exc.path, "message": exc.message}, status=400)

    if request.method == "GET":
        return JsonResponse({
            "pid": binding["pid"],
            "token": canonical_token,
            "minted": entry_context(entry, request) if entry is not None else None,
            "overlapping": registry.overlapping(binding["recipe"]["members"], exclude_pid=binding["pid"]),
            "allowance": registry.mint_allowance(request.user),
            "limits": {"hour": registry.MINT_LIMIT_PER_HOUR, "day": registry.MINT_LIMIT_PER_DAY},
            "statement": MINT_STATEMENT,
        })

    if payload.get("confirm") is not True:
        return JsonResponse({"error": "confirmation required",
                             "detail": "send confirm: true to mint; the PID names the recipe, "
                                       "re-run on every visit, not the records shown"}, status=400)
    try:
        # Opt-in name: only a JSON boolean true (the modal's checkbox) opts in.
        entry, created = registry.mint_recipe(recipe, request.user,
                                              minter_public=payload.get("minter_public") is True)
    except registry.MintRateLimited as exc:
        resp = JsonResponse({"error": "rate limited", "detail": str(exc),
                             "retry_after": exc.retry_after}, status=429)
        resp["Retry-After"] = str(exc.retry_after)
        return resp
    except registry.LocusCollision as exc:
        logger.error("locus mint: 54-bit collision on %s", exc.pid)
        return JsonResponse({"error": "collision", "pid": exc.pid, "detail": str(exc)}, status=409)
    logger.info("locus mint: %s %s by user %s", "minted" if created else "existing", entry.pid, request.user.pk)
    return JsonResponse({
        "pid": entry.pid,
        "created": created,
        "url": request.build_absolute_uri(entry.entity_path),
        "path": entry.entity_path,
        "minted": entry_context(entry, request),
    }, status=201 if created else 200)


@require_POST
def atlas_locus_attribution(request):
    """``POST /atlas/locus/attribution/`` ``pid=<PID>&public=0``: the minter
    stops their name being shown (``minter_public`` true -> false). Only the
    minter may ask (403 otherwise); ``public=1`` is refused (400): a name is
    published only by the consent given in the mint modal, never afterwards
    (migration 0028 enforces the same at the database). A form post is
    answered with a 303 back to the locus; a JSON post with JSON."""
    refused = _beta_json_gate(request)
    if refused is not None:
        return refused
    wants_json = (request.content_type or "").startswith("application/json")
    if wants_json:
        try:
            data = json.loads(request.body or b"{}")
        except (ValueError, TypeError):
            data = {}
        data = data if isinstance(data, dict) else {}
        pid_text, public = str(data.get("pid") or ""), data.get("public")
        public = "1" if public is True else "0" if public is False else str(public or "")
    else:
        pid_text, public = request.POST.get("pid", ""), request.POST.get("public", "")
    try:
        entry, _ = registry.lookup(pid_text, mint_legacy_on_dereference=False)
    except registry.LookupError_ as exc:
        return JsonResponse({"error": "not found" if exc.status == 404 else "bad request",
                             "detail": exc.message}, status=exc.status)
    if entry.minted_by_id is None or entry.minted_by_id != request.user.pk:
        return JsonResponse({"error": "only the minter may change how they are named"}, status=403)
    if public != "0":
        return JsonResponse({"error": "a name can only be hidden after minting",
                             "detail": "send public=0; showing a name needs the consent given when minting"},
                            status=400)
    entry.hide_minter()
    logger.info("locus attribution: user %s hid their name on %s", request.user.pk, entry.pid)
    if wants_json:
        return JsonResponse({"pid": entry.pid, "minter_public": False})
    return HttpResponseRedirect(entry.entity_path, status=303)


MINT_STATEMENT = (
    "The identifier names this recipe: the retrieval, the clustering parameters and the "
    "member ids that settle which cluster is meant. It does not name the records shown now. "
    "Every visit re-runs the recipe against the index as it then stands, so the answer will "
    "change as data is added, corrected or withdrawn. The identifier can never be edited, "
    "reused or deleted; it can be withdrawn, but stays in the registry."
)
