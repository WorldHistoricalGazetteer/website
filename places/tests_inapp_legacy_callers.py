"""WHG's own pages make no call to the legacy v3 API (/api/spatial/, /api/db/, /api/index/,
/api/place/..., /api/geojson/; deprecated 2026-11-17, ``api.deprecation``) and no poll of the
unrouted /celery-progress/<id>/. Each in-app caller was moved 2026-11-18:

==========================================  ===============================================
before                                      now
==========================================  ===============================================
portal.js  /api/spatial/?type=nearby        POST /reconcile, spatial-only, namespaces whg
getPlace.js  /api/place/<id>[?cid=]         /places/<id>/panel/[?cid=] (same payload, internal)
place_detail.html  /api/db/?id=             /entity/place:<id>/api (signed in), else sign-in
PlaceTableSerializer.id  /api/db/?id=       /places/<id>/detail
builders-dataset.js, collection_group_      /task_progress/<id>/ (CeleryProgressBar shape:
detail.html  /celery-progress/<id>/         main.tests_task_progress_access)
==========================================  ===============================================

Each absence below is paired with a presence in the same test, so a scan that read nothing,
or a page that failed to render, cannot pass. The gateway is never called: ``process_queries``
is patched where the view imports it.

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 venv/bin/python manage.py test \\
        places.tests_inapp_legacy_callers --settings=whg.settings_localtest
"""

import json
import os
import re
from pathlib import Path
from unittest.mock import patch

from django.conf import settings
from django.contrib.auth import get_user_model
from django.middleware.csrf import _get_new_csrf_string
from django.test import Client, SimpleTestCase, TestCase
from django.urls import resolve

REPO = Path(settings.BASE_DIR)
LEGACY_CALL = re.compile(r"api/(spatial|db|index|geojson)\b|api/place/|celery-progress/")
SKIP_DIRS = {"node_modules", ".git", "static", "staticfiles", "media", "venv", ".venv", ".claude",
             "docs", "__pycache__"}


def _strip_comments(text, suffix):
    """Drop what is not executed or rendered: {% comment %} blocks, {# #}, <!-- -->, and JS
    // line comments (also inside a template's inline <script>)."""
    if suffix == ".html":
        text = re.sub(r"{%\s*comment\s*%}.*?{%\s*endcomment\s*%}", "", text, flags=re.S)
        text = re.sub(r"{#.*?#}", "", text)
        text = re.sub(r"<!--.*?-->", "", text, flags=re.S)
    return "\n".join(line for line in text.splitlines() if not line.strip().startswith("//"))


def _sources():
    """Every template and front-end JS source the site serves from: *.html anywhere outside
    vendored/built/static trees, plus whg/webpack/js/*.js and <app>/static/<app>/js/*.js."""
    out = []
    for root, dirs, files in os.walk(REPO):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        for name in files:
            if name.endswith(".html"):
                out.append(Path(root) / name)
    out += sorted((REPO / "whg" / "webpack" / "js").glob("*.js"))
    out += sorted(REPO.glob("*/static/*/js/*.js"))
    return out


class NoLegacyCallsInSourceTests(SimpleTestCase):

    def test_no_template_or_js_calls_a_legacy_endpoint(self):
        sources = _sources()
        # Presence: the scan really read the site's sources, and the same pattern-free reading
        # finds each caller's successor where it was moved to.
        self.assertGreater(len(sources), 100)
        texts = {p: _strip_comments(p.read_text(encoding="utf-8", errors="replace"), p.suffix)
                 for p in sources}
        by_name = {str(p.relative_to(REPO)): t for p, t in texts.items()}
        self.assertIn("/places/${pid}/panel/", by_name["whg/webpack/js/getPlace.js"])
        self.assertIn("fetch('/reconcile'", by_name["whg/webpack/js/portal.js"])
        self.assertIn("/entity/place:{{ object.id }}/api", by_name["places/templates/places/place_detail.html"])
        self.assertIn('"/task_progress/" + task_id', by_name["whg/webpack/js/builders-dataset.js"])
        self.assertIn('"/task_progress/" + task_id',
                      by_name["collection/templates/collection/collection_group_detail.html"])
        # The pattern itself matches what it is meant to catch.
        for old in ("/api/spatial/?type=nearby", "/api/db/?id=1", "`/api/place/${pid}`",
                    '"/celery-progress/" + id', "/api/index/?name=x", "/api/geojson/?id=1"):
            self.assertRegex(old, LEGACY_CALL)
        # Absence.
        hits = [f"{name}:{n}: {line.strip()}" for name, text in by_name.items()
                for n, line in enumerate(text.splitlines(), 1) if LEGACY_CALL.search(line)]
        self.assertEqual(hits, [])


class _Fixture(TestCase):

    @classmethod
    def setUpTestData(cls):
        from datasets.models import Dataset
        from places.models import Place, PlaceName
        User = get_user_model()
        cls.owner = User.objects.create_user(username="ilc_owner", email="ilc_owner@example.org", password="pw",
                                             given_name="ILC", surname="Owner")
        cls.other = User.objects.create_user(username="ilc_other", email="ilc_other@example.org", password="pw",
                                             given_name="ILC", surname="Other")
        with patch("datasets.signals.doi"):
            cls.ds = Dataset.objects.create(
                owner=cls.owner, label="ilc_ds", title="In-app ds", description="D", creator="A Contributor",
                public=True, ds_status="indexed", uri_base="https://whgazetteer.org/api/db/?id=")
            cls.private = Dataset.objects.create(
                owner=cls.owner, label="ilc_private", title="Private ds", description="D", creator="A Contributor",
                public=False, ds_status="uploaded", uri_base="https://whgazetteer.org/api/db/?id=")
        cls.place = Place.objects.create(title="Panelville", src_id="ilc1", dataset=cls.ds, ccodes=["GB"],
                                         minmax=[1500, 1600], timespans=[[1500, 1600]], fclasses=["P"])
        PlaceName.objects.create(place=cls.place, toponym="Panelville", jsonb={"toponym": "Panelville"})
        cls.hidden = Place.objects.create(title="Hiddenville", src_id="h1", dataset=cls.private, ccodes=["GB"],
                                          fclasses=["P"])
        PlaceName.objects.create(place=cls.hidden, toponym="Hiddenville", jsonb={"toponym": "Hiddenville"})


class PlacePanelRouteTests(_Fixture):
    """getPlace.js: the panel's payload, at /places/<id>/panel/, for anyone who may see the place."""

    def test_route_is_the_panel_view_and_not_a_legacy_endpoint(self):
        from api.views import PlacesDetailAPIView
        match = resolve(f"/places/{self.place.id}/panel/")
        self.assertEqual(match.url_name, "place-panel")
        self.assertIs(match.func.view_class, PlacesDetailAPIView)
        self.assertEqual(resolve(f"/places/{self.place.id}-{self.hidden.id}/panel/").url_name, "place-panel")

    def test_anonymous_gets_the_panel_payload_without_deprecation_headers(self):
        r = self.client.get(f"/places/{self.place.id}/panel/")
        self.assertEqual(r.status_code, 200)
        body = r.json()
        # The keys parsePlace / parseAnno / the ds_browse + tableFunctions callbacks read.
        for key in ("id", "title", "datasets", "names", "types", "fclasses", "links", "related",
                    "descriptions", "countries", "minmax", "attestation_years", "traces", "extent"):
            self.assertIn(key, body)
        self.assertEqual(body["title"], "Panelville")
        self.assertEqual(body["datasets"], [{"id": self.ds.id, "title": "In-app ds"}])
        self.assertNotIn("Deprecation", r.headers)
        self.assertNotIn("deprecation", body)
        # The legacy URL it replaces does carry them (so the absence above means something).
        legacy = self.client.get(f"/api/place/{self.place.id}/")
        self.assertEqual(legacy.status_code, 200)
        self.assertIn("Deprecation", legacy.headers)

    def test_same_payload_as_the_legacy_route(self):
        new = self.client.get(f"/places/{self.place.id}/panel/").json()
        old = self.client.get(f"/api/place/{self.place.id}/").json()
        old.pop("deprecation", None)
        self.assertEqual(new, old)

    def test_cid_is_passed_through(self):
        with patch("api.views.PlaceSerializer") as ser:
            ser.return_value.data = {"traces": [], "dataset_id": self.ds.id, "title": "Panelville", "names": [],
                                     "types": [], "geoms": [], "extent": None, "links": [], "related": [],
                                     "descriptions": [], "depictions": [], "minmax": None, "ccodes": []}
            self.client.get(f"/places/{self.place.id}/panel/?cid=42")
        self.assertEqual(ser.call_args.kwargs["context"]["cid"], "42")

    def test_private_place_is_visible_to_its_owner_only(self):
        url = f"/places/{self.hidden.id}/panel/"
        self.assertEqual(self.client.get(url).status_code, 404)
        self.client.force_login(self.other)
        self.assertEqual(self.client.get(url).status_code, 404)
        self.client.force_login(self.owner)
        r = self.client.get(url)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["title"], "Hiddenville")


class PlaceDetailLinkTests(_Fixture):
    """place_detail.html: the 'Linked Places record' link is the Entity API, for a signed-in
    visitor (session-authenticated); an anonymous visitor, whom the Entity API refuses for a
    WHG-hosted record, gets a sign-in link instead of a link that 401s."""

    def _page(self):
        r = self.client.get(f"/places/{self.place.id}/detail")
        self.assertEqual(r.status_code, 200)
        html = r.content.decode()
        self.assertIn("Panelville", html)  # presence: the page rendered
        self.assertNotIn("/api/db/", html)
        return html

    def test_signed_in_gets_the_entity_api_link_and_it_answers_their_session(self):
        self.client.force_login(self.other)
        html = self._page()
        url = f"/entity/place:{self.place.id}/api"
        self.assertIn(f'href="{url}"', html)
        self.assertNotIn("lpf-record-signin", html)
        r = self.client.get(url, HTTP_ACCEPT="application/json")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["type"], "Feature")

    def test_anonymous_gets_a_sign_in_link_because_the_entity_api_refuses_them(self):
        html = self._page()
        self.assertNotIn("lpf-record-link", html)
        self.assertIn("lpf-record-signin", html)
        from django.urls import reverse
        self.assertIn(f'href="{reverse("accounts:login")}?next=', html)
        r = self.client.get(f"/entity/place:{self.place.id}/api", HTTP_ACCEPT="application/json")
        self.assertEqual(r.status_code, 401)


class PlaceTableLinkTests(_Fixture):
    """PlaceTableSerializer.id (the collection-group browse table): a WHG-hosted record links to
    its detail page, not to /api/db/?id=."""

    def test_whg_hosted_record_links_to_its_detail_page(self):
        from api.serializers import PlaceTableSerializer
        html = PlaceTableSerializer().get_id(self.place)
        self.assertIn(f'href="/places/{self.place.id}/detail"', html)
        self.assertNotIn("/api/db/", html)

    def test_external_uri_base_is_unchanged(self):
        from api.serializers import PlaceTableSerializer
        self.ds.uri_base = "https://example.org/places/"
        html = PlaceTableSerializer().get_id(self.place)
        self.assertIn(f'href="https://example.org/places/{self.place.id}"', html)


PORTAL_QUERY = {"queries": {"q0": {"lat": 51.5, "lng": -0.12, "radius": 16, "relation": "within",
                                   "containment": "fuzzy", "namespaces": ["whg"], "limit": 20}}}
CANDIDATE = {"id": "place:whg:1:ilc1", "name": "Panelville", "score": 0, "match": False, "namespace": "whg",
             "ccodes": ["GB"], "repr_point": [-0.12, 51.5]}


@patch("api.reconcile.process_queries", return_value={"q0": {"result": [CANDIDATE]}})
class PortalNearbyReconcileTests(TestCase):
    """portal.js 'Nearby places': the request it now sends, POST /reconcile with the page's CSRF
    header, is admitted for an anonymous visitor and for a signed-in session, without a token,
    and reaches the query runner with the circle unchanged. Without the header (and without a
    token) the same request is refused, so the admission is the header's doing."""

    def _post(self, client, with_header=True):
        secret = _get_new_csrf_string()
        client.cookies[settings.CSRF_COOKIE_NAME] = secret
        extra = {"HTTP_X_CSRFTOKEN": secret} if with_header else {}
        return client.post("/reconcile", data=json.dumps(PORTAL_QUERY), content_type="application/json", **extra)

    def test_anonymous_with_csrf_header_is_admitted(self, pq):
        r = self._post(Client(enforce_csrf_checks=True))
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["q0"]["result"][0]["repr_point"], [-0.12, 51.5])
        queries = pq.call_args.args[0]
        self.assertEqual(queries["q0"]["namespaces"], ["whg"])
        self.assertEqual((queries["q0"]["lat"], queries["q0"]["lng"], queries["q0"]["radius"]), (51.5, -0.12, 16))

    def test_signed_in_with_csrf_header_is_admitted(self, pq):
        client = Client(enforce_csrf_checks=True)
        client.force_login(get_user_model().objects.create_user(
            username="ilc_portal", email="ilc_portal@example.org", password="pw", given_name="I", surname="P"))
        self.assertEqual(self._post(client).status_code, 200)
        pq.assert_called_once()

    def test_anonymous_without_the_header_is_refused(self, pq):
        r = self._post(Client(enforce_csrf_checks=True), with_header=False)
        self.assertEqual(r.status_code, 401)
        pq.assert_not_called()
