"""The gateway-unavailable signal, in its own module so the exception handler
and the views can both import it without a cycle.

Kept separate from `api.exception_handlers` (which imports `elastic.health`) and
from `api.views_entity` (which imports most of the API) deliberately: this needs
to be importable from either side.
"""

from rest_framework import status
from rest_framework.exceptions import APIException


class GatewayUnavailable(APIException):
    """503 for a place we could not ASK about, as distinct from one that is absent.

    `crc_fetch_places` returns `{}` on a timeout, a connection failure and a
    genuine miss alike, so `/entity/place:<id>/api` answered **404** for all
    three: a transient upstream failure was reported to the caller as a
    definitive statement that the record does not exist (place#272).

    🛑 That is worse than a wrong status code, because the failures cluster with
    load. A client harvesting metadata records "does not exist" biased toward
    whenever it was going fastest, and ends up with a silently thinner dataset
    and nothing in the output to reveal it. In the run that surfaced this, 28
    records that exist and are correct would have been banked as absent by an
    audit reporting itself complete.

    Recoverable by waiting, which is why `Retry-After` is emitted: over the same
    ids, a third attempt minutes later answered 30 of the 36 that had failed
    twice back to back.

    Same contract as the reconcile path's `gateway` key (place#262): presence
    means failure, and a client that does not know the field is no worse off.

    ⚠️ The response body is built by `api.exception_handlers`, NOT from
    `self.detail`. DRF's `APIException` runs `_get_error_details` over whatever
    it is given, which coerces every leaf to `ErrorDetail(str)` — so a `False`
    placed in `detail` serialises as the STRING `"False"`, which is truthy in
    JavaScript. A client testing `if (!body.gateway.answered)` would then read a
    failure as a success. Caught by `tests_entity_gateway_failure`; the
    round-trip test there is what stops it coming back.
    """

    status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    default_detail = (
        "The gazetteer service did not answer, so this identifier could not be "
        "resolved. This is NOT evidence that the record does not exist — please "
        "retry shortly rather than recording it as absent."
    )
    default_code = "gateway_unavailable"

    # Read by DRF's exception_handler (rest_framework/views.py:90) to emit
    # `Retry-After`; also used by our own handler below.
    wait = 30

    def __init__(self, place_id: str = "", error: str = ""):
        self.place_id = place_id
        self.error = error or "unexpected"
        super().__init__(self.default_detail)

    def payload(self) -> dict:
        """The response body, with JSON types preserved (see the class note)."""
        return {
            "detail": str(self.default_detail),
            "gateway": {"answered": False, "error": self.error},
            "place_id": self.place_id,
        }


def raise_if_gateway_failed(meta: dict, place_id: str = "") -> None:
    """Turn a gateway failure into 503, so only a real miss can become 404.

    Call this on the empty-result path, BEFORE raising `Http404`. The ordering
    is the whole point: `{}` is ambiguous, and `meta` is the only thing that
    disambiguates it.

    ⚠️ `meta["disabled"]` is deliberately NOT treated as a failure. It means the
    gateway was never called because the caller is not entitled to it — a fact
    about the request, not about the service — and answering 503 would tell an
    unauthorised caller to come back later.
    """
    if meta.get("error"):
        raise GatewayUnavailable(place_id=place_id, error=meta["error"])
