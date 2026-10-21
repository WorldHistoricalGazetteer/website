"""place#325 Stage 4: the V4_LANDING_ENABLED switch, in both states.

Run locally (never in a prod container):

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test places.tests_v4_landing --settings=whg.settings_localtest

Off: every public route answers exactly as before (the wrappers return the
legacy view's own response OBJECT, and each route's legacy answer is pinned).
On: the portal pid routes redirect (302 until the v4 release, query string kept)
to the Record URI; the Record page is public for
public records; ``/entity/place:<contributed>/`` resolves anonymously for a
public, non-embargoed record only (#271), while ``/api`` stays closed.

Every absence sits beside a presence on the same route, so a switch that
opened or closed everything would fail (a-check-that-cannot-fail.md).
"""
from unittest.mock import patch

from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.gis.geos import Point
from django.http import HttpResponse
from django.test import RequestFactory, TestCase, override_settings
from django.urls import resolve

from places.v4_landing import PORTAL_REDIRECT_STATUS, PORTAL_REDIRECTS, portal_pid_view, v4_landing_enabled

LOCMEM = {**settings.CACHES,
          "default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache", "LOCATION": "v4l"}}
OFF = override_settings(V4_LANDING_ENABLED=False, CACHES=LOCMEM)
ON = override_settings(V4_LANDING_ENABLED=True, CACHES=LOCMEM)


class _Fixture(TestCase):
    @classmethod
    def setUpTestData(cls):
        from api.models import GazetteerRegistryEntry
        from datasets.models import Dataset
        from places.models import Place, PlaceGeom

        User = get_user_model()
        cls.owner = User.objects.create_user(username="v4_owner", email="v4_owner@example.org", password="pw",
                                             given_name="V", surname="Owner")
        cls.beta = User.objects.create_user(username="v4_beta", email="v4_beta@example.org", password="pw",
                                            given_name="V", surname="Beta", role="beta_tester")
        cls.plain = User.objects.create_user(username="v4_plain", email="v4_plain@example.org", password="pw",
                                             given_name="V", surname="Plain")

        def ds(key, public=True):
            return Dataset.objects.create(owner=cls.owner, label=f"v4l_{key}", title=f"Dataset {key}",
                                          description="D", creator="X", public=public, ds_status="indexed",
                                          uri_base="https://example.org/places/")

        def place(d, title, src_id):
            p = Place.objects.create(title=title, src_id=src_id, dataset=d, ccodes=[])
            PlaceGeom.objects.create(place=p, geom=Point(1.0, 2.0, srid=4326),
                                     jsonb={"type": "Point", "coordinates": [1.0, 2.0]})
            return p

        with patch("datasets.signals.doi"):
            cls.ds_pub = ds("pub")
            cls.ds_priv = ds("priv", public=False)
            cls.ds_emb = ds("emb")
        GazetteerRegistryEntry.objects.create(id=f"whg:{cls.ds_emb.pk}", name="Emb", namespace="whg",
                                              entry_class="dataset", status="embargoed")
        cls.Pub = place(cls.ds_pub, "Public Town", "P1")
        cls.Pub2 = place(cls.ds_pub, "Other Town", "P2")
        cls.Priv = place(cls.ds_priv, "Secret Town", "S1")
        cls.Emb = place(cls.ds_emb, "Embargoed Town", "E1")
        cls.missing = Place.objects.order_by("-id").first().id + 1000

    def setUp(self):
        from django.core.cache import cache
        cache.clear()
        # The Record page's default-settings locus link asks the gateway for
        # clustering parameters: answer "unavailable" so no network is touched.
        p = patch("api.crc_client.crc_clustering_params", return_value=(None, "disabled"))
        p.start()
        self.addCleanup(p.stop)

    def anon(self, url, **extra):
        self.client.logout()
        return self.client.get(url, **extra)

    def as_user(self, user, url, **extra):
        self.client.force_login(user)
        try:
            return self.client.get(url, **extra)
        finally:
            self.client.logout()


@OFF
class SwitchOffTests(_Fixture):
    """Nothing public changes: each route's legacy answer, pinned."""

    def test_setting_reads_off(self):
        self.assertFalse(v4_landing_enabled())
        with override_settings(V4_LANDING_ENABLED=True):
            self.assertTrue(v4_landing_enabled())

    def test_wrapper_returns_the_legacy_response_object(self):
        sentinel = HttpResponse(b"legacy bytes", status=299)
        calls = []

        def legacy(request, *a, **kw):
            calls.append(kw)
            return sentinel
        req = RequestFactory().get("/places/portal/1/")
        self.assertIs(portal_pid_view(legacy)(req, pid=1), sentinel)
        self.assertEqual(calls, [{"pid": 1}])
        with override_settings(V4_LANDING_ENABLED=True):  # the control: on, it is NOT the legacy object
            r = portal_pid_view(legacy)(req, pid=1)
            self.assertIsNot(r, sentinel)
            self.assertEqual((r.status_code, r["Location"]), (302, "/entity/place:1/"))
        self.assertEqual(len(calls), 1)

    def test_routes_are_wrapped_around_the_original_views(self):
        from places.views import PlacePortalView
        m = resolve(f"/places/portal/{self.Pub.pk}/")
        self.assertEqual(m.url_name, "place-portal-pid")
        self.assertIs(m.func.legacy_view.view_class, PlacePortalView)
        m = resolve(f"/places/{self.Pub.pk}/")
        self.assertEqual(m.url_name, "place-bare-redirect")
        self.assertEqual(m.func.legacy_view.view_initkwargs["pattern_name"], "places:place-portal-pid")

    def test_portal_pid_route(self):
        # A single-match pid: the legacy portal 302s to the detail page.
        r = self.anon(f"/places/portal/{self.Pub.pk}/")
        self.assertEqual((r.status_code, r["Location"]), (302, f"/places/{self.Pub.pk}/detail"))
        r = self.anon(f"/places/portal/{self.Priv.pk}/")
        self.assertEqual(r.status_code, 404)

    def test_bare_route(self):
        r = self.anon(f"/places/{self.Pub.pk}/?q=1")
        self.assertEqual((r.status_code, r["Location"]), (302, f"/places/portal/{self.Pub.pk}/?q=1"))

    def test_whg_id_and_multipid_routes(self):
        with patch("places.views.findPortalPlaces", return_value=[self.Pub.pk, self.Pub2.pk]):
            r = self.anon("/places/14047598/portal/")
        self.assertEqual(r.status_code, 200)
        self.assertContains(r, "Public Town")
        r = self.anon(f"/places/portal/{self.Pub.pk},{self.Pub2.pk}/")
        self.assertEqual(r.status_code, 200)
        self.assertContains(r, "Other Town")

    def test_record_page_stays_beta(self):
        url = f"/atlas/place/{self.Pub.pk}/"
        r = self.anon(url)
        self.assertEqual(r.status_code, 302)
        self.assertIn(settings.LOGIN_URL, r["Location"])
        self.assertEqual(self.as_user(self.plain, url).status_code, 404)
        r = self.as_user(self.beta, url)
        self.assertContains(r, "Public Town")
        self.assertEqual(self.anon(url + "concordance/").status_code, 403)
        self.assertEqual(self.as_user(self.plain, url + "concordance/").status_code, 403)


@ON
class SwitchOnTests(_Fixture):

    def test_portal_pid_routes_redirect_to_the_record_uri(self):
        # 302, not 301, until the v4 release (review 2026-10-21): a browser caches a
        # 301, and with the switch later off /entity/place:<pk>/ 302s back to the
        # portal, which the cached 301 would send round again.
        self.assertEqual(PORTAL_REDIRECT_STATUS, 302)
        self.assertEqual({row["status"] for row in PORTAL_REDIRECTS if row["target"]}, {302})
        for url in (f"/places/portal/{self.Pub.pk}/", f"/places/{self.Pub.pk}/"):
            r = self.anon(url)
            self.assertEqual((r.status_code, r["Location"]), (302, f"/entity/place:{self.Pub.pk}/"), url)
        # A private pid redirects too (nothing revealed); its target withholds it
        # (places/tests_v4_entity_271.py).
        r = self.anon(f"/places/portal/{self.Priv.pk}/")
        self.assertEqual((r.status_code, r["Location"]), (302, f"/entity/place:{self.Priv.pk}/"))

    def test_portal_pid_routes_keep_the_query_string(self):
        # The legacy bare route's RedirectView had query_string=True.
        for url in (f"/places/portal/{self.Pub.pk}/", f"/places/{self.Pub.pk}/"):
            r = self.anon(url + "?q=1&lang=fr")
            self.assertEqual((r.status_code, r["Location"]),
                             (302, f"/entity/place:{self.Pub.pk}/?q=1&lang=fr"), url)
            r = self.anon(url)                                   # and none when there is none
            self.assertEqual(r["Location"], f"/entity/place:{self.Pub.pk}/", url)

    def test_switching_off_again_leaves_no_loop(self):
        """The round trip a cached 301 would turn into a loop: on, the portal sends
        to the Record URI with a non-permanent status; off, the Record URI sends
        (back) to the portal, which then serves the portal's own answer."""
        r = self.anon(f"/places/portal/{self.Pub.pk}/")
        self.assertNotEqual(r.status_code, 301)
        with override_settings(V4_LANDING_ENABLED=False):
            r = self.as_user(self.plain, f"/entity/place:{self.Pub.pk}/")
            self.assertEqual((r.status_code, r["Location"]), (302, f"/places/portal/{self.Pub.pk}/"))
            r = self.as_user(self.plain, f"/places/portal/{self.Pub.pk}/")
            self.assertEqual((r.status_code, r["Location"]), (302, f"/places/{self.Pub.pk}/detail"))

    def test_pending_routes_keep_serving_the_legacy_portal(self):
        pending = {row["name"] for row in PORTAL_REDIRECTS if row["target"] is None}
        self.assertEqual(pending, {"places:place-portal-whg-id", "places:place-portal-multipid",
                                   "places:place-portal"})
        with patch("places.views.findPortalPlaces", return_value=[self.Pub.pk]):
            r = self.anon("/places/14047598/portal/")
        self.assertContains(r, "Public Town")
        r = self.anon(f"/places/portal/{self.Pub.pk},{self.Pub2.pk}/")
        self.assertContains(r, "Other Town")

    def test_record_page_public_for_public_records_only(self):
        r = self.anon(f"/atlas/place/{self.Pub.pk}/")
        self.assertContains(r, "Public Town")
        r = self.as_user(self.plain, f"/atlas/place/{self.Pub.pk}/")
        self.assertContains(r, "Public Town")
        for p in (self.Priv, self.Emb):
            r = self.anon(f"/atlas/place/{p.pk}/")
            self.assertEqual(r.status_code, 404, p.title)
            self.assertNotIn(p.title, r.content.decode())
        self.assertContains(self.as_user(self.owner, f"/atlas/place/{self.Priv.pk}/"), "Secret Town")

    def test_concordance_public_for_public_records_only(self):
        with patch("api.crc_client.crc_fetch_places", return_value={}):
            r = self.anon(f"/atlas/place/{self.Pub.pk}/concordance/")
            self.assertEqual(r.status_code, 200)
            self.assertTrue(r.json()["public"])
            for p in (self.Priv, self.Emb):
                self.assertEqual(self.anon(f"/atlas/place/{p.pk}/concordance/").status_code, 404, p.title)
