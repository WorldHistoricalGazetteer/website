"""`/entity/` must distinguish "does not exist" from "we could not ask" (place#272).

`crc_fetch_places` returns `{}` for a genuine miss, a timeout, a connection
failure and a refusal alike, and `EntityFeatureView` turned all of them into
**404**. A transient upstream failure was therefore reported to the caller as a
definitive statement that the record does not exist.

🛑 These tests are written so that **no assertion can pass vacuously**. Every
failure case asserts the 503 *and* a companion case asserts that a real miss is
still 404 — an absence tested without a presence in the same suite proves
nothing, because a change that made everything 503 would pass a suite that only
checked the failure paths.

The gateway is mocked at `api.views_entity.crc_fetch_places`, so nothing here
touches Elasticsearch, Postgres or the network. `SimpleTestCase` is deliberate:
these assertions are about status codes and response bodies, so declaring no
database keeps them runnable on a workstation that has none (Django 4.1 then
skips test-database creation entirely).
"""

import json
from unittest.mock import patch

import requests
from django.test import SimpleTestCase
from rest_framework.test import APIRequestFactory

from api.crc_client import crc_fetch_places
from api.gateway_errors import GatewayUnavailable, raise_if_gateway_failed


class RaiseIfGatewayFailedTests(SimpleTestCase):
    """The guard itself: what it raises on, and what it deliberately does not."""

    def test_raises_for_each_recorded_error_kind(self):
        for kind in ("timeout", "connection", "http", "unexpected"):
            with self.subTest(kind=kind):
                with self.assertRaises(GatewayUnavailable) as ctx:
                    raise_if_gateway_failed({"error": kind}, place_id="gn:1")
                self.assertEqual(ctx.exception.status_code, 503)
                payload = ctx.exception.payload()
                self.assertEqual(payload["gateway"]["error"], kind)
                self.assertIs(payload["gateway"]["answered"], False,
                              "must be a JSON boolean, not the string 'False'")

    def test_does_not_raise_on_a_clean_miss(self):
        """The presence half. A gateway that ANSWERED and had nothing must fall
        through to 404 — otherwise every 404 becomes a 503 and the fix has
        merely inverted the defect."""
        raise_if_gateway_failed({}, place_id="gn:1")

    def test_does_not_raise_when_the_gateway_was_never_called(self):
        """`disabled` is a fact about the REQUEST (caller not entitled), not
        about the service. Answering 503 would tell an unauthorised caller to
        come back later."""
        raise_if_gateway_failed({"disabled": True}, place_id="gn:1")

    def test_carries_retry_after(self):
        """`wait` is the only hook DRF's exception_handler offers for
        `Retry-After`. If this attribute is dropped the header silently
        disappears, and the condition IS recoverable by waiting."""
        self.assertEqual(GatewayUnavailable(place_id="gn:1", error="timeout").wait, 30)


class CrcFetchPlacesMetaTests(SimpleTestCase):
    """`meta` is the only thing that makes `{}` interpretable."""

    def _fetch(self, side_effect):
        meta: dict = {}
        with patch("api.crc_client._is_enabled", return_value=True), \
                patch("api.crc_client.requests.post", side_effect=side_effect):
            result = crc_fetch_places(["gn:13274771"], meta=meta)
        return result, meta

    def test_timeout_is_recorded(self):
        result, meta = self._fetch(requests.Timeout("too slow"))
        self.assertEqual(result, {})
        self.assertEqual(meta.get("error"), "timeout")

    def test_connection_error_is_recorded(self):
        result, meta = self._fetch(requests.ConnectionError("refused"))
        self.assertEqual(result, {})
        self.assertEqual(meta.get("error"), "connection")

    def test_unexpected_error_is_recorded(self):
        result, meta = self._fetch(ValueError("something else entirely"))
        self.assertEqual(result, {})
        self.assertEqual(meta.get("error"), "unexpected")

    def test_disabled_is_distinguished_from_failure(self):
        meta: dict = {}
        with patch("api.crc_client._is_enabled", return_value=False):
            result = crc_fetch_places(["gn:1"], meta=meta)
        self.assertEqual(result, {})
        self.assertIs(meta.get("disabled"), True)
        self.assertIsNone(meta.get("error"),
                          "not asking is not the same as asking and failing")

    def test_meta_stays_empty_on_a_successful_empty_answer(self):
        """🛑 The control that stops every other test above passing vacuously.
        A gateway that answers and simply has no such id must leave `meta`
        clean, or the caller can never emit a 404 again."""
        class _Resp:
            status_code = 200

            def raise_for_status(self):
                pass

            def json(self):
                return {"places": []}

        meta: dict = {}
        with patch("api.crc_client._is_enabled", return_value=True), \
                patch("api.crc_client.requests.post", return_value=_Resp()):
            result = crc_fetch_places(["gn:99999999999"], meta=meta)
        self.assertEqual(result, {})
        self.assertEqual(meta, {})


class EntityFeatureViewStatusTests(SimpleTestCase):
    """End to end through the view, which is where the defect was visible."""

    URL = "/entity/place:gn:13274771/api"

    def _get(self, fetch_side_effect):
        from api.views_entity import EntityFeatureView
        factory = APIRequestFactory()
        request = factory.get(self.URL)
        with patch("api.views_entity.crc_fetch_places", side_effect=fetch_side_effect):
            view = EntityFeatureView.as_view()
            return view(request, entity_id="place:gn:13274771")

    def test_gateway_failure_is_503_not_404(self):
        def _fail(place_ids, user=None, allow_anonymous=False, meta=None):
            if meta is not None:
                meta["error"] = "timeout"
            return {}

        response = self._get(_fail)
        self.assertEqual(response.status_code, 503)
        self.assertIs(response.data["gateway"]["answered"], False)
        self.assertEqual(response.data["gateway"]["error"], "timeout")

    def test_real_miss_is_still_404(self):
        """🛑 The presence half, at view level. Before place#272 both cases were
        404; the fix must not make both 503."""
        def _empty(place_ids, user=None, allow_anonymous=False, meta=None):
            return {}

        response = self._get(_empty)
        self.assertEqual(response.status_code, 404)

    def test_answered_survives_json_rendering_as_a_boolean(self):
        """🛑 The regression test for the bug these tests actually caught.

        The first implementation put the payload in `APIException.detail`, and
        DRF's `_get_error_details` coerced every leaf to `ErrorDetail(str)` — so
        `"answered": false` rendered as the STRING `"False"`, which is truthy in
        JavaScript. A client writing `if (!body.gateway.answered)` would have
        read a gateway failure as a success, i.e. the exact inversion this whole
        issue is about, reintroduced by the fix for it.

        ⚠️ Asserting on the exception object is NOT sufficient — the coercion
        happens during rendering. This parses the rendered bytes."""
        def _fail(place_ids, user=None, allow_anonymous=False, meta=None):
            if meta is not None:
                meta["error"] = "timeout"
            return {}

        response = self._get(_fail)
        response.render()
        body = json.loads(response.content)
        self.assertIs(body["gateway"]["answered"], False)
        self.assertNotIsInstance(body["gateway"]["answered"], str)
        self.assertEqual(body["gateway"]["error"], "timeout")
        self.assertEqual(response["Retry-After"], "30")

    def test_the_two_are_distinguishable_from_the_body_alone(self):
        """The reporter's actual complaint: the two responses were
        byte-identical but for the echoed id, so no field could be read to tell
        them apart. Assert they now differ in a machine-readable way."""
        def _fail(place_ids, user=None, allow_anonymous=False, meta=None):
            if meta is not None:
                meta["error"] = "connection"
            return {}

        def _empty(place_ids, user=None, allow_anonymous=False, meta=None):
            return {}

        failed = self._get(_fail)
        missing = self._get(_empty)
        self.assertNotEqual(failed.status_code, missing.status_code)
        self.assertIn("gateway", failed.data)
        self.assertNotIn("gateway", missing.data or {})


class RedistributionAndAttributionTests(SimpleTestCase):
    """`/entity/` must credit its source, and refuse one it may not re-serve (place#269).

    This endpoint is the **citable** representation — published through w3id and,
    since place#271, answered for anonymous linked-data clients. So it *hands over*
    the source's own content, which is a different act from indexing or searching
    it. The registry draws that line with `redistributable` (place#136): indexing,
    search and reconciliation run server-side and never expose raw source data, and
    are permitted for every authority; redistribution is not.

    🛑 Two gaps, both found by checking rather than assuming: the LPF path carried
    **no attribution at all** (the popup variant had it since place#121), and
    `redistributable` was **never consulted** anywhere in this view.
    """

    def _get(self, attribution, variant=None):
        from api.views_entity import EntityFeatureView
        url = "/entity/place:clio:x/api" + (f"?variant={variant}" if variant else "")
        request = APIRequestFactory().get(url)

        def _fetch(place_ids, user=None, allow_anonymous=False, meta=None):
            return {"clio:x": {"place_id": "clio:x", "title": "Somewhere",
                               "namespace": "clio", "names": [], "geometries": []}}

        with patch("api.views_entity.crc_fetch_places", side_effect=_fetch), \
                patch("api.attribution.registry_attribution", return_value=attribution):
            return EntityFeatureView.as_view()(request, entity_id="place:clio:x")

    OK_SOURCE = {"name": "Cliopatria", "redistributable": True,
                 "license__spdx_id": "CC-BY-4.0", "rights_holder": "Seshat"}
    BLOCKED = {"name": "Locked Gazetteer", "redistributable": False,
               "license__spdx_id": "NoDerivatives", "rights_holder": "Somebody",
               "source_url": "https://example.org/", "license__url": "https://example.org/terms"}

    def test_the_lpf_path_now_carries_attribution(self):
        """The gap that mattered most: this is the machine-read representation, so
        an unattributed record here is the one most likely to be copied onward
        without its terms."""
        response = self._get(self.OK_SOURCE)
        self.assertEqual(response.status_code, 200)
        self.assertIn("attribution", response.data)
        self.assertEqual(response.data["attribution"]["name"], "Cliopatria")

    def test_the_popup_variant_still_carries_it(self):
        """The companion — it had attribution before this change and must keep it."""
        response = self._get(self.OK_SOURCE, variant="popup")
        self.assertEqual(response.status_code, 200)
        self.assertIn("attribution", response.data)

    def test_a_non_redistributable_source_is_refused_with_451(self):
        """🛑 451, not 403: the refusal is a property of the CONTENT, not of the
        caller's credentials. No token would change the answer, and 403 invites a
        client to retry with one."""
        response = self._get(self.BLOCKED)
        self.assertEqual(response.status_code, 451)

    def test_the_refusal_names_the_source_and_where_to_get_it(self):
        """A consumer who may not have the data can still legitimately need to know
        whose it is — refusing without saying that just looks broken."""
        response = self._get(self.BLOCKED)
        body = response.data
        self.assertEqual(body["source"]["name"], "Locked Gazetteer")
        self.assertEqual(body["source"]["rights_holder"], "Somebody")
        self.assertEqual(body["source"]["source_url"], "https://example.org/")
        self.assertIn("do not permit", body["detail"])

    def test_the_popup_variant_is_refused_too(self):
        """🛑 Both variants, or the gate is a formality: the popup returns the raw
        gateway record, which is MORE of the source's content, not less."""
        self.assertEqual(self._get(self.BLOCKED, variant="popup").status_code, 451)

    def test_an_unknown_namespace_is_served_not_refused(self):
        """⚠️ FAIL OPEN on absence. `registry_attribution` returns None for a
        namespace with no authority row — notably WHG-hosted places. Treating that
        as non-redistributable would refuse the site's own records."""
        response = self._get(None)
        self.assertEqual(response.status_code, 200)
        self.assertNotIn("attribution", response.data)

    def test_redistributable_true_is_required_to_be_explicitly_false_to_refuse(self):
        """A row that simply omits the flag must not be refused — only an explicit
        False blocks. `is False` rather than falsy, so None/missing serves."""
        response = self._get({"name": "Partial", "redistributable": None})
        self.assertEqual(response.status_code, 200)
