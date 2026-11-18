"""A circle-only `POST /reconcile` query (`lat`/`lng`/`radius`, no `query`, no `type`) must return places.

The docs have long said `query` may be omitted when a circular `lat`/`lng`/`radius` filter is given.
On prod such a query returned nothing, for two independent reasons, each enough on its own:

1. **The view.** A batch with no `type` and no `query` text was taken to be an OpenRefine
   type-guessing probe and answered with placeholder "Guessing Result" candidates; the circle was
   never looked at. Only a `query` string counted as "real", not a spatial constraint.
2. **The gateway request.** `crc_reconcile_search` sends a circle to the gateway as bare
   `lat`/`lng`/`radius` (resolved there as an H3 disc, place#184) and deliberately drops the polygon.
   The gateway's `/api/reconcile` early-returns empty for any query with no `query`, no `contained_in`
   and no `bounds` (indexing repo `gateway/reconcile.py`, the `if not has_query and not
   req.contained_in and not req.bounds` guard) — it never consults the circle. And the legacy index is
   suppressed for any spatially-scoped query, so nothing else answered either. Supplying `type`
   bypassed (1) and still hit (2).

The tests drive the REAL view and the REAL gateway client; only `requests.post` is replaced, by a fake
gateway that applies that same early-return rule. Nothing reaches the CRC gateway or Elasticsearch.

⚠️ Each "now works" assertion has a companion: the empty no-constraint probe still gets the
placeholders, and a NAMED circle query still sends exactly what it sent before (no `bounds`, no
`relation`), so a fix that broke either would fail here.
"""

import json
from unittest.mock import patch

from django.test import SimpleTestCase
from rest_framework.test import APIRequestFactory, force_authenticate

PLACE_TYPE = "https://whgazetteer.org/static/whg_schema.jsonld#Place"
LONDON = {"lat": 51.5074, "lng": -0.1278, "radius": 10}


class _User:
    is_authenticated = True
    is_anonymous = False
    is_active = True
    is_staff = False
    is_superuser = False
    id = 1
    pk = 1
    username = "probe"

    def __str__(self):
        return self.username


class _Resp:
    status_code = 200

    def __init__(self, data):
        self._data = data

    def raise_for_status(self):
        pass

    def json(self):
        return self._data


class FakeGateway:
    """Stands in for the gateway's `/api/reconcile`, including its early-return rule."""

    HIT = {
        "place_id": "gn:2643743", "title": "London", "names": [{"label": "London"}],
        "ccodes": ["GB"], "score": 0, "geometries": [{"repr_point": [-0.1257, 51.5085]}],
    }

    def __init__(self):
        self.bodies = []

    def __call__(self, url, json=None, **kwargs):
        body = json or {}
        self.bodies.append(body)
        has_query = bool((body.get("query") or "").strip())
        if not has_query and not body.get("contained_in") and not body.get("bounds"):
            return _Resp({"hits": [], "namespaces_searched": []})
        scope = None
        if body.get("lat") is not None or body.get("bounds") or body.get("contained_in"):
            scope = {"requested": True, "applied": True, "mode": "h3-disc", "approximate": False,
                     "containers_polygon": [], "containers_linked": [],
                     "containers_approximated": [], "containers_unresolved": []}
        return _Resp({"hits": [self.HIT], "scope": scope, "namespaces_searched": []})


def _post(queries):
    from api.reconcile import ReconciliationView

    request = APIRequestFactory().post(
        "/reconcile", data=json.dumps({"queries": queries}), content_type="application/json")
    force_authenticate(request, user=_User())
    gateway = FakeGateway()
    with patch("api.crc_client._is_enabled", return_value=True), \
            patch("api.crc_client._headers", return_value={}), \
            patch("api.crc_client.requests.post", side_effect=gateway), \
            patch("api.reconcile.es_search", return_value=[]) as legacy, \
            patch("api.reconcile.hidden_datasets", return_value=None), \
            patch("api.reconcile.hidden_namespaces", return_value=set()), \
            patch("api.reconcile._attribution_for_results", return_value={}), \
            patch("api.reconcile.consume_query_budget", return_value=(True, 0)):
        response = ReconciliationView.as_view()(request)
    return response, json.loads(response.content), gateway, legacy


class CircleOnlyThroughTheView(SimpleTestCase):

    def test_circle_without_type_returns_places_not_placeholders(self):
        response, data, gateway, _ = _post({"q0": dict(LONDON)})
        self.assertEqual(response.status_code, 200)
        names = [c.get("name") for c in data["q0"]["result"]]
        self.assertFalse(any(str(n).startswith("Guessing Result") for n in names), names)
        self.assertEqual(names, ["London"])
        self.assertEqual(len(gateway.bodies), 1, "the gateway was not consulted exactly once")

    def test_circle_with_type_returns_places(self):
        response, data, _, _ = _post({"q0": {**LONDON, "type": PLACE_TYPE}})
        self.assertEqual(response.status_code, 200)
        self.assertEqual([c.get("name") for c in data["q0"]["result"]], ["London"])

    def test_circle_as_an_openrefine_property_without_type(self):
        q = {"properties": [{"pid": "whg:within_radius", "v": "51.5074,-0.1278,10"}]}
        _, data, _, _ = _post({"q0": q})
        self.assertEqual([c.get("name") for c in data["q0"]["result"]], ["London"])

    def test_bbox_and_contained_in_without_type_are_answered_too(self):
        for q in ({"bbox": [-1, 51, 0, 52]}, {"contained_in": ["un:gbr"]}):
            with self.subTest(q=q):
                _, data, _, _ = _post({"q0": q})
                self.assertEqual([c.get("name") for c in data["q0"]["result"]], ["London"])

    def test_empty_probe_without_constraint_still_gets_placeholders(self):
        # Presence control: the type-guessing path must still exist for a query with nothing in it.
        _, data, gateway, _ = _post({"q0": {"limit": 3}})
        names = [c.get("name") for c in data["q0"]["result"]]
        self.assertTrue(names and all(str(n).startswith("Guessing Result") for n in names), names)
        self.assertEqual(gateway.bodies, [])

    def test_radius_above_the_spatial_only_cap_is_a_per_query_error(self):
        from api.reconcile import SPATIAL_ONLY_MAX_RADIUS_KM
        _, data, gateway, _ = _post({
            "ok": dict(LONDON),
            "big": {**LONDON, "radius": SPATIAL_ONLY_MAX_RADIUS_KM + 1},
        })
        self.assertEqual([c.get("name") for c in data["ok"]["result"]], ["London"])
        self.assertIn("radius", data["big"]["error"])
        self.assertEqual(data["big"]["result"], [])
        self.assertEqual(len(gateway.bodies), 1, "an over-cap circle must not reach the gateway")


class CircleGatewayBody(SimpleTestCase):

    def _body(self, q):
        _, _, gateway, _ = _post({"q0": q})
        self.assertEqual(len(gateway.bodies), 1)
        return gateway.bodies[0]

    def test_circle_only_body_carries_the_circle_and_a_bounds_polygon(self):
        body = self._body(dict(LONDON))
        self.assertEqual((body["lat"], body["lng"], body["radius"]), (51.5074, -0.1278, 10.0))
        self.assertEqual(body["bounds"]["type"], "Polygon")
        self.assertIsNone(body["query"])

    def test_circle_only_defaults_to_within(self):
        self.assertEqual(self._body(dict(LONDON))["relation"], "within")
        self.assertEqual(self._body({**LONDON, "relation": "intersects"})["relation"], "intersects")

    def test_named_circle_body_is_unchanged(self):
        # Positive control for "additive only": with a name the gateway already honoured the circle,
        # so nothing about that request may change.
        body = self._body({**LONDON, "query": "London"})
        self.assertEqual(body["radius"], 10.0)
        self.assertNotIn("bounds", body)
        self.assertNotIn("relation", body)

    def test_gateway_size_is_clamped_to_what_the_gateway_accepts(self):
        from api.crc_client import GATEWAY_MAX_SIZE
        self.assertEqual(self._body({**LONDON, "limit": 7})["size"], 7)
        self.assertEqual(self._body({**LONDON, "limit": 5000})["size"], GATEWAY_MAX_SIZE)
        self.assertEqual(self._body({**LONDON, "limit": 0})["size"], 1)
