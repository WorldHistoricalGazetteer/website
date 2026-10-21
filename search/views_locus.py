"""Minting a citable locus from the locus page (place#172, place#325 Stage 4).

Routes (``search/urls_atlas.py``), live only when ``LOCUS_MINTING_ENABLED``
is true, and then only for signed-in beta testers (ruling D1):

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

from django.http import Http404, JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_http_methods

from places import locus_registry as registry
from places.locus_recipe import RecipeError
from search.locus import LOCUS_TOKEN_PARAM, describe_recipe, load_token

logger = logging.getLogger(__name__)

MINT_URL = "/atlas/locus/mint/"
RESOLVE_URL = "/atlas/locus/resolve/"


def _beta_json_gate(request):
    """404 when minting is off (nothing public exists); 403 JSON without beta."""
    if not registry.minting_enabled():
        raise Http404()
    if not (request.user.is_authenticated and request.user.can_access_beta):
        return JsonResponse({"error": "beta access required"}, status=403)
    return None


def minted_by_name(entry) -> str:
    u = entry.minted_by
    if u is None:
        return ""
    return (getattr(u, "name", "") or getattr(u, "username", "") or "").strip()


def entry_context(entry, request=None) -> dict:
    """The registry row as the page and the JSON representation show it."""
    path = entry.entity_path
    return {
        "pid": entry.pid,
        "kind": entry.kind,
        "status": entry.status,
        "withdrawn": entry.is_withdrawn,
        "minted_at": entry.minted_at,
        "minted_by": minted_by_name(entry),
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
        entry, created = registry.mint_recipe(recipe, request.user)
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


MINT_STATEMENT = (
    "The identifier names this recipe: the retrieval, the clustering parameters and the "
    "member ids that settle which cluster is meant. It does not name the records shown now. "
    "Every visit re-runs the recipe against the index as it then stands, so the answer will "
    "change as data is added, corrected or withdrawn. The identifier can never be edited, "
    "reused or deleted; it can be withdrawn, but stays in the registry."
)
