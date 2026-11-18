"""API_Call analytics events: say where a call came from (origin, client) without any change to how
external callers make their requests, and cover the main POST /reconcile route (place#334 follow-up)."""
from unittest.mock import patch

from django.test import RequestFactory, SimpleTestCase, override_settings

from api.middleware import (classify_client, classify_origin, is_applicable_path, send_to_plausible)

rf = RequestFactory()


class OriginTests(SimpleTestCase):
    def test_plato_tools_browser_call(self):
        r = rf.post("/reconcile", HTTP_ORIGIN="https://plato.pelagios.org")
        self.assertEqual(classify_origin(r), "plato-tools")

    def test_referer_used_when_no_origin(self):
        r = rf.post("/reconcile", HTTP_REFERER="https://plato.pelagios.org/tools/map?x=1")
        self.assertEqual(classify_origin(r), "plato-tools")

    def test_other_site_is_not_recorded_by_name(self):
        r = rf.post("/reconcile", HTTP_ORIGIN="https://someone-private.example.org")
        self.assertEqual(classify_origin(r), "other-web")       # never the raw host

    def test_no_headers_and_null_origin_are_none(self):
        self.assertEqual(classify_origin(rf.post("/reconcile")), "none")
        self.assertEqual(classify_origin(rf.post("/reconcile", HTTP_ORIGIN="null")), "none")

    def test_lookalike_host_is_not_a_known_origin(self):
        r = rf.post("/reconcile", HTTP_ORIGIN="https://plato.pelagios.org.evil.example")
        self.assertEqual(classify_origin(r), "other-web")

    def test_clients(self):
        for ua, want in (("OpenRefine/3.8", "openrefine"), ("python-requests/2.32", "python"),
                         ("curl/8.5.0", "curl"), ("WHG-MCP/0.2.3", "whg-mcp"), ("Mozilla/5.0 Firefox", "browser"),
                         ("Weird/1.0", "other"), ("", "none")):
            self.assertEqual(classify_client(rf.post("/reconcile", HTTP_USER_AGENT=ua)), want, ua)


class PathTests(SimpleTestCase):
    def test_main_reconcile_route_is_counted(self):
        for path in ("/reconcile", "/reconcile/", "/reconcile/properties", "/suggest/entity", "/reconcile/queries"):
            self.assertTrue(is_applicable_path(path), path)

    def test_other_paths_not_counted(self):
        for path in ("/api/db/", "/reconcilement", "/", "/datasets/"):
            self.assertFalse(is_applicable_path(path), path)


@override_settings(PLAUSIBLE_API_KEY="k", PLAUSIBLE_BASE_URL="http://plausible.test", PLAUSIBLE_SITE="whgazetteer.org")
class PayloadTests(SimpleTestCase):
    def test_event_carries_origin_and_client_but_nothing_identifying(self):
        req = rf.post("/reconcile", HTTP_ORIGIN="https://plato.pelagios.org", HTTP_USER_AGENT="Mozilla/5.0",
                      HTTP_AUTHORIZATION="Bearer SECRETTOKEN", REMOTE_ADDR="203.0.113.9")
        req.user = type("U", (), {"is_authenticated": True})()
        resp = type("R", (), {"status_code": 200})()
        with patch("api.middleware.requests.post") as post:
            send_to_plausible(req, resp)
        payload = post.call_args.kwargs["json"]
        self.assertEqual(payload["props"]["origin"], "plato-tools")
        self.assertEqual(payload["props"]["client"], "browser")
        blob = repr(payload)
        self.assertNotIn("SECRETTOKEN", blob)
        self.assertNotIn("203.0.113.9", blob)
