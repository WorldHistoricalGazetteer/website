"""Wildcards in the Atlas place search (whg3 side). The gateway owns the matching and the rules.

An unescaped `*` or `?` anywhere in the text switches the gateway search to `mode: "wildcard"`, whatever
the match-type dropdown said; a backslash makes either literal. A pattern the gateway refuses comes back as
a 400 carrying its message (not a 503: the service is fine and the user can fix the query).
Each behaviour has a control showing the ordinary path is untouched.
"""
import json
from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase, override_settings

from api import crc_client
from api.crc_client import crc_search_status, has_wildcard


class _U:
    is_authenticated = True
    is_anonymous = False


def _post(status=200, payload=None):
    resp = MagicMock(status_code=status, text=json.dumps(payload or {}))
    resp.json.return_value = payload if payload is not None else {"hits": []}
    return resp


def _run(options, resp=None):
    meta = {}
    with override_settings(CRC_GATEWAY_URL="http://gw"), \
            patch.object(crc_client, "_headers", return_value={}), \
            patch.object(crc_client.requests, "post", return_value=resp or _post()) as post:
        out = crc_search_status(options, user=_U(), meta=meta)
    return out, meta, post


class DetectionTests(SimpleTestCase):
    def test_unescaped_wildcards_are_detected(self):
        for q in ("par*", "Par?s", "Newcas*le", "a*b?c"):
            self.assertTrue(has_wildcard(q), q)

    def test_ordinary_and_escaped_text_is_not(self):
        for q in ("Paris", "St. John's", "", None, "caf\\?", "what\\*", "Aix-en-Provence"):
            self.assertFalse(has_wildcard(q), q)
        self.assertTrue(has_wildcard("a\\\\*"))          # an escaped backslash, then a real star


class ModeSelectionTests(SimpleTestCase):
    def test_wildcard_overrides_every_dropdown_choice(self):
        for chosen in ("exact", "starts", "in", "fuzzy", "phonetic", None):
            _, meta, _ = _run({"qstr": "par*", "mode": chosen})
            self.assertEqual(meta["request"]["mode"], "wildcard", chosen)

    def test_the_exact_toggle_is_overridden_too(self):
        _, meta, _ = _run({"qstr": "Par?s", "exact": True})
        self.assertEqual(meta["request"]["mode"], "wildcard")

    def test_controls_ordinary_queries_keep_their_modes(self):
        self.assertEqual(_run({"qstr": "Paris", "mode": "starts"})[1]["request"]["mode"], "starts")
        self.assertEqual(_run({"qstr": "Paris", "exact": True})[1]["request"]["mode"], "exact")
        self.assertEqual(_run({"qstr": "Paris"})[1]["request"]["mode"], "fuzzy")
        self.assertEqual(_run({"qstr": "caf\\?", "mode": "in"})[1]["request"]["mode"], "in")   # escaped: literal


class RefusalTests(SimpleTestCase):
    def test_refused_pattern_returns_the_gateway_message(self):
        msg = "A pattern cannot start with a wildcard."
        (data, failure), meta, _ = _run({"qstr": "*aris"}, _post(422, {"detail": msg}))
        self.assertIsNone(data)
        self.assertEqual(failure, "invalid")
        self.assertEqual(meta["message"], msg)

    def test_a_422_on_an_ordinary_search_is_still_a_plain_error(self):
        """Control: only wildcard refusals are 'invalid'; any other 4xx keeps its old handling."""
        (data, failure), meta, _ = _run({"qstr": "Paris"}, _post(422, {"detail": "bad"}))
        self.assertEqual(failure, "error")
        self.assertNotIn("message", meta)

    def test_view_answers_400_with_the_message_not_503(self):
        from search import views
        with patch("api.crc_client.crc_search_status") as cs:
            def fake(options, user=None, meta=None):
                meta["request"] = {"mode": "wildcard"}
                meta["message"] = "Give at least 3 letters."
                return None, "invalid"
            cs.side_effect = fake
            resp = views._gateway_search_response({"qstr": "pa*"}, _U())
        body = json.loads(resp.content)
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(body["message"], "Give at least 3 letters.")
        self.assertEqual(body["failure"], "invalid")
        self.assertTrue(body["gateway"])                       # the service is up: no outage banner
        self.assertEqual(body["hits"], [])

    def test_view_control_real_outage_is_still_503(self):
        from search import views
        with patch("api.crc_client.crc_search_status", return_value=(None, "unreachable")):
            resp = views._gateway_search_response({"qstr": "Paris"}, _U())
        self.assertEqual(resp.status_code, 503)
