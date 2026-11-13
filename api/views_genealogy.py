"""Genealogy place strings: the authenticated, throttled front door to the gateway's resolver.

``POST /genealogy/place`` takes a family-history place string ("Ballymena, County Antrim, Ireland") or a
batch of up to 25, in a named service's convention, and returns the parse, the resolved admin context and
ranked candidates. All the matching happens in the gateway (``/api/genealogy/place``); this view adds what
the gateway deliberately does not own: authentication, the per-minute query budget, and the embargo and
dataset-withholding rules that apply to every other reconciliation response.
"""
import logging

from django.http import JsonResponse
from rest_framework.parsers import JSONParser
from rest_framework.response import Response

from api.authentication import AuthenticatedAPIView
from api.crc_client import GENEALOGY_FIELDS, crc_genealogy, crc_genealogy_services
from api.dataset_access import hidden_datasets
from api.reconcile import hidden_namespaces
from api.throttling import consume_query_budget, throttle_message

logger = logging.getLogger("reconciliation")

MAX_PLACES = 25  # matches the gateway; checked here too so an oversized batch is refused before it is charged


def _err(message, status):
    return JsonResponse({"error": message}, status=status)


def _visible(candidate, hidden_ns, hidden):
    """False for a candidate the caller must not see (embargoed namespace, withheld dataset)."""
    pid = str((candidate or {}).get("place_id") or "")
    ns = pid.split(":", 1)[0].lower() if ":" in pid else ""
    if ns and ns in hidden_ns:
        return False
    if ns == "whg" and hidden and hidden.hides_contributed_id(pid):
        return False
    return True


class GenealogyPlaceView(AuthenticatedAPIView):
    parser_classes = [JSONParser]

    def post(self, request, *args, **kwargs):
        payload = request.data
        if not isinstance(payload, dict):
            return _err("Send a JSON object with `place` or `places`.", 400)
        unknown = sorted(set(payload) - set(GENEALOGY_FIELDS))
        if unknown:
            # Refused, not dropped: a misspelt `contry_hint` must not quietly search without a country.
            return _err(f"Unknown field(s): {', '.join(unknown)}. Allowed: {', '.join(GENEALOGY_FIELDS)}.", 400)
        texts = list(payload.get("places") or []) + ([payload["place"]] if payload.get("place") else [])
        if not texts:
            return _err("Supply `place` (one string) or `places` (a list).", 400)
        if len(texts) > MAX_PLACES:
            return _err(f"Too many places: {len(texts)} received, limit is {MAX_PLACES} per request. "
                        "Split the batch and retry; nothing was processed.", 400)
        if any(not isinstance(t, str) for t in texts):
            return _err("`place` and `places` must contain strings.", 400)
        if str(payload.get("mode") or "").lower() == "familysearch":
            return _err('mode "familysearch" belongs to /reconcile; here use fuzzy, exact, starts or in.', 400)

        # Each place costs the gateway several index lookups, so it is charged as one query apiece
        # against the same per-minute budget /reconcile uses. Charged before any work, after validation.
        allowed, retry_after = consume_query_budget(request.user, len(texts))
        if not allowed:
            response = _err(throttle_message(retry_after), 429)
            response["Retry-After"] = str(retry_after)
            return response

        status, data = crc_genealogy(payload, user=request.user)
        if status != 200 or not isinstance(data, dict):
            return JsonResponse(data if isinstance(data, dict) else {"error": "unexpected response"}, status=status)

        hidden_ns = hidden_namespaces(request.user, request)
        hidden = hidden_datasets(request.user)
        if hidden_ns or hidden:
            for res in data.get("results") or []:
                res["candidates"] = [c for c in res.get("candidates") or [] if _visible(c, hidden_ns, hidden)]
        return JsonResponse(data)


class GenealogyServicesView(AuthenticatedAPIView):
    def get(self, request, *args, **kwargs):
        status, data = crc_genealogy_services(user=request.user)
        return JsonResponse(data, status=status)
