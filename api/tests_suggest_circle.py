"""`GET /suggest/entity` with `lat`/`lng`/`radius` and no `prefix` (place#353).

It used to answer `{"result": []}` for any request without a prefix, even though `POST /reconcile` answers
the same circle-only question (place#345). It now answers with the places in the circle. The tests drive the
REAL view and the REAL gateway client; only `requests.post` is replaced, by a fake gateway that applies the
gateway's own early-return rule (no `query`, no `contained_in`, no `bounds` => empty), so a request that
reached it without the circle polygon would fail here. Nothing reaches the CRC gateway or Elasticsearch.

Presence controls: no prefix AND no circle is still an empty answer; a prefix still takes the old path.
"""
import json
from unittest.mock import patch

from django.test import SimpleTestCase
from rest_framework.test import APIRequestFactory, force_authenticate

from api.tests_reconcile_circle_only import FakeGateway, _User

CIRCLE = {"lat": "51.5074", "lng": "-0.1278", "radius": "10"}


def _get(params, hidden=None):
    from api.reconcile import SuggestEntityView

    request = APIRequestFactory().get("/suggest/entity", params)
    force_authenticate(request, user=_User())
    gateway = FakeGateway()
    with patch("api.crc_client._is_enabled", return_value=True), \
            patch("api.crc_client._headers", return_value={}), \
            patch("api.crc_client.requests.post", side_effect=gateway), \
            patch("api.reconcile.es_search", return_value=[]) as legacy, \
            patch("api.reconcile.crc_suggest_search", return_value=[]) as suggest_search, \
            patch("api.reconcile.hidden_datasets", return_value=None), \
            patch("api.reconcile.hidden_namespaces", return_value=hidden or set()):
        response = SuggestEntityView.as_view()(request)
    return response, json.loads(response.content), gateway, legacy, suggest_search


class SuggestCircleOnly(SimpleTestCase):

    def test_circle_without_prefix_returns_places(self):
        response, data, gateway, legacy, suggest_search = _get(dict(CIRCLE))
        self.assertEqual(response.status_code, 200)
        self.assertEqual([c["name"] for c in data["result"]], ["London"])
        self.assertEqual(len(gateway.bodies), 1)
        self.assertIn("bounds", gateway.bodies[0], "the circle must reach the gateway past its early-return guard")
        legacy.assert_not_called()
        suggest_search.assert_not_called()

    def test_nameless_circle_means_within(self):
        _, _, gateway, _, _ = _get(dict(CIRCLE))
        self.assertEqual(gateway.bodies[0].get("relation"), "within")

    def test_limit_and_cursor_page_the_result(self):
        _, data, _, _, _ = _get({**CIRCLE, "limit": "1", "cursor": "1"})
        self.assertEqual(data["result"], [])  # the fake gateway returns one hit; cursor 1 is past it

    def test_radius_over_500_km_is_an_error_not_an_empty_list(self):
        response, data, gateway, _, _ = _get({**CIRCLE, "radius": "600"})
        self.assertGreaterEqual(response.status_code, 400)
        self.assertIn("500", json.dumps(data))
        self.assertEqual(gateway.bodies, [])

    def test_period_type_is_not_a_circle_search(self):
        _, data, gateway, _, _ = _get({**CIRCLE, "type": "period"})
        self.assertEqual(data["result"], [])
        self.assertEqual(gateway.bodies, [])

    def test_embargoed_namespace_is_not_returned(self):
        with patch.object(FakeGateway, "HIT", {**FakeGateway.HIT, "place_id": "secret:1", "namespace": "secret"}):
            _, data, _, _, _ = _get(dict(CIRCLE), hidden={"secret"})
            _, shown, _, _, _ = _get(dict(CIRCLE))  # presence control: visible when nothing is embargoed
        self.assertEqual([c["id"] for c in shown["result"]], ["place:secret:1"])
        self.assertEqual(data["result"], [])

    # -- controls ----------------------------------------------------------------------------

    def test_no_prefix_and_no_circle_is_still_empty(self):
        _, data, gateway, _, _ = _get({"namespaces": "wd"})
        self.assertEqual(data, {"result": []})
        self.assertEqual(gateway.bodies, [])

    def test_incomplete_circle_is_still_empty(self):
        _, data, gateway, _, _ = _get({"lat": "51.5", "lng": "-0.12"})
        self.assertEqual(data, {"result": []})
        self.assertEqual(gateway.bodies, [])

    def test_a_prefix_still_takes_the_name_path(self):
        _, _, gateway, legacy, suggest_search = _get({"prefix": "Lon", "type": "place", **CIRCLE})
        suggest_search.assert_called_once()
        legacy.assert_called_once()
        self.assertEqual(gateway.bodies, [])  # crc_suggest_search is mocked; the circle path was not taken


class GatewayHitsCarryTheirNamespace(SimpleTestCase):
    """The embargo filters test `_source.namespace`. The gateway adapter never set it, so those filters
    compared None against the hidden set and kept every gateway hit (found while writing the circle tests)."""

    def test_adapter_sets_namespace_from_the_place_id(self):
        from api.crc_client import _adapt_hits
        hits = _adapt_hits({"hits": [{"place_id": "gn:2643743", "title": "London"},
                                     {"place_id": "whg:12:34", "title": "x"},
                                     {"place_id": "bare", "title": "y"}]})
        self.assertEqual([h["_source"]["namespace"] for h in hits], ["gn", "whg", None])
