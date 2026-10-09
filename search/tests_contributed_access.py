"""Contributed (``whg:<dataset>:<id>``) places on the Atlas endpoints: the
per-dataset visibility / embargo / licence decision and the gateway grant
(api/dataset_access.py, place#319 follow-up; the place#310 rule on
``/atlas/place/``).

Run locally (never in the prod container — see reference_never_run_suite_on_prod):

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test search.tests_contributed_access --settings=whg.settings_localtest

Same discipline as search/tests_atlas_geometry.py, so nothing passes vacuously:

* every refusal (403 / 404 / 451 / 503) is paired, in the same test, with a
  200 through the same view from the same fixtures;
* the gateway is faked at the one HTTP seam (``api.crc_client.requests``) and
  the fake VERIFIES the grant the way the real gateway does, so a view that
  forgot the header, or signed the wrong id, fails here rather than live;
* every refusal asserts the gateway was NOT asked, every success that it WAS.

Real rows, not mocks: datasets, collaborators, licences (from the seed
migrations) and the registry's embargo row, so the decision is exercised
against the ORM it will run on.
"""

import hashlib
import hmac
import json
import time
from datetime import timedelta
from unittest.mock import patch

from django.test import override_settings
from django.utils import timezone

from api.dataset_access import (
    GRANT_HEADER, GRANT_TTL_S, contributed_geometry_decision, dataset_visibility,
    is_dataset_embargoed, parse_contributed_id, sign_geometry_grant,
)
from search.tests_atlas import GATEWAY, AtlasTestBase, _Resp
from search.tests_atlas_geometry import GET, SQUARE

POST = "api.crc_client.requests.post"
SECRET = "test-secret"
# Pinned on BOTH sides of the contract (indexing tests/test_gateway_geometry.py
# pins the same string): if either signer drifts, its own suite fails.
GRANT_VECTOR = ("whg:42:7", 1800000000,
                "v1.1800000000.2862a35f37860589367a6b118137c29040de8a8c985bafabd54ac1c02d9e3137")


def _verify_like_the_gateway(grant, pid, secret=SECRET):
    """The gateway's check, re-stated here: version, window, HMAC over the id."""
    if not grant:
        return "missing"
    parts = grant.split(".")
    if len(parts) != 3 or parts[0] != "v1" or not parts[1].isdigit():
        return "malformed"
    expires = int(parts[1])
    now = time.time()
    if expires < now:
        return "expired"
    if expires - now > 600:
        return "too long-lived"
    expected = hmac.new(secret.encode(), f"geometry|{pid}|{expires}".encode(), hashlib.sha256).hexdigest()
    return "ok" if hmac.compare_digest(expected, parts[2]) else "bad signature"


def _fake_gateway_get(url, params=None, headers=None, **kwargs):
    """A restarted gateway with the grant check: serves any granted whg:<ds>:<id>
    except src ``lent`` (a polygon lent by another dataset: 451 whatever the
    grant); refuses an ungranted one 451; knows osm:r1. Src ``nowhere`` (any
    dataset) is unknown to the index: the route's own 404."""
    from urllib.parse import unquote
    # The ASGI server decodes the path before routing, and the gateway's
    # route takes the whole remainder (``{place_id:path}``): a src_id
    # containing ``/`` arrives decoded, as one id.
    pid = unquote(url.rsplit("/api/geometry/", 1)[-1])
    if pid.startswith("whg:"):
        reason = _verify_like_the_gateway((headers or {}).get(GRANT_HEADER), pid)
        if reason != "ok":
            return _Resp(451, {"detail": {"error": "source licence not determined", "id": pid,
                                          "namespace": "whg", "grant": reason}})
        if pid.endswith(":lent"):
            return _Resp(451, {"detail": {"error": "source licence not determined", "id": pid,
                                          "namespace": "whg",
                                          "detail": "geometry borrowed from whg:9999:1_0"}})
        if "nowhere" in pid:
            return _Resp(404, {"detail": {"error": "not found", "id": pid}})
        return _Resp(200, {"place_id": pid, "namespace": "whg", "geometry": SQUARE,
                           "geometry_count": 1, "bounds": [0, 0, 1, 1], "vertex_count": 5,
                           "bytes": 70, "max_bytes": 1000000, "simplified": False,
                           "tolerance": None, "source": "geom-store",
                           # whg:<dataset>:<src_id> — the src_id may itself contain ":"
                           "dataset": ":".join(pid.split(":", 2)[:2])})
    if pid == "osm:r1":
        return _Resp(200, {"place_id": pid, "namespace": "osm", "geometry": SQUARE,
                           "geometry_count": 1, "bounds": [0, 0, 1, 1], "vertex_count": 5,
                           "bytes": 70, "max_bytes": 1000000, "simplified": False,
                           "tolerance": None, "source": "geom-store"})
    return _Resp(404, {"detail": {"error": "not found", "id": pid}})


def _fake_gateway_post(url, json=None, **kwargs):
    """/api/places: knows every whg place asked for (the index does not know
    about visibility — that is the point), nothing else."""
    if url.endswith("/api/places"):
        ids = (json or {}).get("ids") or []
        return _Resp(200, {"places": [
            {"place_id": i, "title": f"Place {i}", "namespace": "whg", "names": [], "types": [],
             "geometries": []} for i in ids if i.startswith("whg:")]})
    return _Resp(404, {})


@override_settings(CRC_GATEWAY_URL=GATEWAY, CRC_GATEWAY_TIMEOUT=1, CRC_GATEWAY_API_KEY=SECRET)
class ContributedAccessBase(AtlasTestBase):
    @classmethod
    def setUpTestData(cls):
        super().setUpTestData()
        from django.contrib.auth import get_user_model
        from datasets.models import Dataset, DatasetUser
        from licensing.models import License
        User = get_user_model()
        # Insiders who ALSO pass the beta gate, so the dataset decision is what
        # a test sees, not the gate in front of it.
        cls.owner = User.objects.create_user(
            username="ds_owner", email="owner@example.org", password="pw",
            given_name="Olive", surname="Owner", role="beta_tester")
        cls.collab = User.objects.create_user(
            username="ds_collab", email="collab@example.org", password="pw",
            given_name="Colin", surname="Collab", role="beta_tester")
        cls.staff = User.objects.create_user(
            username="ds_staff", email="staff@example.org", password="pw",
            given_name="Stan", surname="Staff", role="normal")
        cls.staff.is_staff = True
        cls.staff.save()
        # A non-beta owner: the beta gate still comes first.
        cls.plain_owner = User.objects.create_user(
            username="ds_plain_owner", email="plain@example.org", password="pw",
            given_name="Pat", surname="Plain", role="normal")

        cc_by = License.objects.get(spdx_id="CC-BY-4.0")
        cls.licences = {
            "nc": License.objects.get(spdx_id="CC-BY-NC-4.0"),
            "nd": License.objects.get(spdx_id="CC-BY-ND-4.0"),
            "custom": License.objects.get(spdx_id="custom-all-rights-reserved"),
            "pd": License.objects.get(spdx_id="custom-public-domain"),
        }
        with patch("datasets.signals.doi"):
            def ds(label, public, license=None, owner=None):
                return Dataset.objects.create(
                    owner=owner or cls.owner, label=label, title=f"Dataset {label}",
                    description="D", creator="A Contributor", webpage="https://example.org/ds",
                    public=public, license=license, ds_status="indexed")
            cls.public_ok = ds("pub_ok", True, cc_by)
            cls.private_ok = ds("priv_ok", False, cc_by)
            cls.plain_private = ds("plain_priv", False, cc_by, owner=cls.plain_owner)
            cls.public_unlicensed = ds("pub_nolic", True, None)
            cls.public_custom = ds("pub_custom", True, cls.licences["custom"])
            cls.public_pd = ds("pub_pd", True, cls.licences["pd"])
            cls.public_nd = ds("pub_nd", True, cls.licences["nd"])
            cls.public_nc = ds("pub_nc", True, cls.licences["nc"])
            cls.embargoed = ds("pub_emb", True, cc_by)
            cls.released = ds("pub_rel", True, cc_by)
        DatasetUser.objects.create(dataset_id=cls.private_ok, user_id=cls.collab, role="member")
        from api.models import GazetteerRegistryEntry
        GazetteerRegistryEntry.objects.create(
            id=f"whg:{cls.embargoed.pk}", name="Embargoed", namespace="whg",
            entry_class="dataset", status="embargoed")
        GazetteerRegistryEntry.objects.create(
            id=f"whg:{cls.released.pk}", name="Released", namespace="whg",
            entry_class="dataset", status="embargoed",
            embargo_release_at=timezone.now() - timedelta(days=1))

    def setUp(self):
        super().setUp()
        # Establish the fixture premises this module's assertions rest on.
        self.assertTrue(self.owner.can_access_beta)
        self.assertTrue(self.collab.can_access_beta)
        self.assertTrue(self.staff.can_access_beta)
        self.assertFalse(self.plain_owner.can_access_beta)
        self.assertTrue(self.licences["custom"].custom)
        self.assertTrue(self.licences["nd"].no_derivatives)
        self.assertFalse(self.licences["nc"].permits_commercial)

    @staticmethod
    def pid(dataset, src="7"):
        return f"whg:{dataset.pk}:{src}"

    def geometry(self, pid):
        return self.client.get("/atlas/geometry/", {"id": pid})


class GrantTests(ContributedAccessBase):
    def test_signature_matches_the_pinned_vector(self):
        pid, expires, expected = GRANT_VECTOR
        self.assertEqual(sign_geometry_grant(pid, secret=SECRET, ttl=GRANT_TTL_S,
                                            now=expires - GRANT_TTL_S), expected)
        # The vector's expiry is fixed (Jan 2027): outside any live window on
        # either side, so it can be pinned without ever being usable.
        self.assertIn(_verify_like_the_gateway(expected, pid), ("expired", "too long-lived"))
        self.assertEqual(_verify_like_the_gateway(sign_geometry_grant(pid, secret=SECRET), pid), "ok")
        self.assertEqual(_verify_like_the_gateway(sign_geometry_grant("whg:1:1", secret=SECRET), pid),
                         "bad signature")

    def test_no_secret_means_no_grant(self):
        self.assertIsNone(sign_geometry_grant("whg:1:1", secret=""))
        with override_settings(CRC_GATEWAY_API_KEY=""):
            self.assertIsNone(sign_geometry_grant("whg:1:1"))
        self.assertIsNotNone(sign_geometry_grant("whg:1:1"))

    def test_parse_contributed_id(self):
        self.assertEqual(parse_contributed_id("whg:42:7"), (42, "7"))
        self.assertEqual(parse_contributed_id("WHG:20:20155:91040"), (20, "20155:91040"))
        for bad in ("whg:42", "whg:abc:1", "whg:42:", "42", "osm:r1", "", None):
            with self.subTest(bad=bad):
                self.assertIsNone(parse_contributed_id(bad))


class DecisionTests(ContributedAccessBase):
    """The policy function on its own, against real rows."""

    def test_visibility_follows_the_dataset_circle(self):
        from django.contrib.auth.models import AnonymousUser
        pid = self.pid(self.private_ok)
        for who, user, expected in [
            ("anonymous", AnonymousUser(), 404), ("beta stranger", self.beta, 404),
            ("normal stranger", self.normal, 404), ("owner", self.owner, 200),
            ("collaborator", self.collab, 200), ("staff", self.staff, 200),
        ]:
            with self.subTest(who=who):
                self.assertEqual(dataset_visibility(pid, user).status, expected)
        # Public: everyone, including anonymous.
        self.assertEqual(dataset_visibility(self.pid(self.public_ok), AnonymousUser()).status, 200)

    def test_user_has_private_access_is_the_non_public_half_of_user_can_view(self):
        from django.contrib.auth.models import AnonymousUser
        ds = self.public_ok
        self.assertTrue(ds.user_can_view(self.beta))
        self.assertFalse(ds.user_has_private_access(self.beta), "public does not make a stranger an insider")
        self.assertFalse(ds.user_has_private_access(AnonymousUser()))
        self.assertTrue(ds.user_has_private_access(self.owner))
        self.assertTrue(ds.user_has_private_access(self.staff))
        self.assertTrue(self.private_ok.user_has_private_access(self.collab))
        self.assertFalse(self.public_ok.user_has_private_access(self.collab), "collaborator on ANOTHER dataset")

    def test_embargo_is_read_from_the_registry_row_with_lazy_release(self):
        self.assertTrue(is_dataset_embargoed(self.embargoed.pk))
        self.assertFalse(is_dataset_embargoed(self.released.pk), "release date in the past")
        self.assertFalse(is_dataset_embargoed(self.public_ok.pk), "no registry row")
        from api.models import GazetteerRegistryEntry
        GazetteerRegistryEntry.objects.filter(id=f"whg:{self.embargoed.pk}").update(status="published")
        self.assertFalse(is_dataset_embargoed(self.embargoed.pk), "row no longer embargoed")

    def test_licence_rule(self):
        cases = [
            ("CC-BY", self.public_ok, 200, None),
            ("CC-BY-NC", self.public_nc, 200, None),
            ("none", self.public_unlicensed, 451, "source licence not determined"),
            ("custom", self.public_custom, 451, "source not redistributable"),
            ("custom public domain", self.public_pd, 451, "source not redistributable"),
            ("ND", self.public_nd, 451, "source not redistributable"),
        ]
        for name, ds, status, error in cases:
            with self.subTest(licence=name):
                d = contributed_geometry_decision(self.pid(ds), self.beta)
                self.assertEqual(d.status, status)
                if error:
                    self.assertEqual(d.body["error"], error)
                    self.assertEqual(d.body["source"]["name"], ds.title)
                    self.assertNotIn("coordinates", json.dumps(d.body))
                else:
                    self.assertIs(d.attribution["redistributable"], True)
                    self.assertEqual(d.attribution["license__spdx_id"], ds.license.spdx_id)
        # Visibility is decided before the licence: a private unlicensed
        # dataset is a 404 to a stranger, not a 451 that confirms it exists.
        with patch("datasets.signals.doi"):
            self.public_unlicensed.public = False
            self.public_unlicensed.save(update_fields=["public"])
        self.assertEqual(contributed_geometry_decision(self.pid(self.public_unlicensed), self.beta).status, 404)
        self.assertEqual(contributed_geometry_decision(self.pid(self.public_unlicensed), self.owner).status, 451)


class GeometryViewTests(ContributedAccessBase):
    def setUp(self):
        super().setUp()
        self.client.force_login(self.beta)

    def test_public_licensed_dataset_is_served_under_a_verified_grant(self):
        with patch(GET, side_effect=_fake_gateway_get) as get:
            resp = self.geometry(self.pid(self.public_ok))
        get.assert_called_once()
        sent = get.call_args.kwargs["headers"]
        self.assertEqual(_verify_like_the_gateway(sent.get(GRANT_HEADER), self.pid(self.public_ok)), "ok")
        self.assertIn("Authorization", sent, "the usual headers still travel")
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["geometry"], SQUARE)
        self.assertTrue(body["gateway"])
        self.assertEqual(body["dataset"], f"whg:{self.public_ok.pk}")
        attr = body["attribution"]
        self.assertEqual(attr["name"], self.public_ok.title)
        self.assertEqual(attr["license__spdx_id"], "CC-BY-4.0")
        self.assertIs(attr["redistributable"], True)
        self.assertEqual(attr["rights_holder"], "A Contributor")

    def test_entity_prefix_is_stripped_before_the_dataset_check_and_the_grant(self):
        with patch(GET, side_effect=_fake_gateway_get) as get:
            resp = self.geometry("place:" + self.pid(self.public_ok))
        self.assertEqual(resp.status_code, 200)
        self.assertIn("/api/geometry/" + self.pid(self.public_ok), get.call_args.args[0])

    # A contributed src_id may contain "/" — whg:1760:https://sferaproject.org/toponyms/persia/
    # is a real one (place#319 gap). It must travel to the gateway as ONE path
    # segment, and the gateway's answer for it must come back as that answer.
    SLASH_SRC = "https://sferaproject.org/toponyms/persia/"

    def test_src_id_with_slashes_travels_as_one_encoded_segment_and_is_served(self):
        pid = self.pid(self.public_ok, self.SLASH_SRC)
        with patch(GET, side_effect=_fake_gateway_get) as get:
            resp = self.geometry(pid)
        get.assert_called_once()
        url = get.call_args.args[0]
        tail = url.split("/api/geometry/", 1)[1]
        self.assertNotIn("/", tail, f"the id must be one path segment: {url}")
        self.assertIn("%2F", tail)
        self.assertEqual(tail, f"whg:{self.public_ok.pk}:https:%2F%2Fsferaproject.org%2Ftoponyms%2Fpersia%2F",
                         "colons kept (the namespace separators), slashes encoded")
        # The grant was signed over the decoded id, the one the gateway verifies.
        sent = get.call_args.kwargs["headers"]
        self.assertEqual(_verify_like_the_gateway(sent.get(GRANT_HEADER), pid), "ok")
        self.assertEqual(resp.status_code, 200, resp.content[:200])
        body = resp.json()
        self.assertEqual(body["place_id"], pid)
        self.assertEqual(body["dataset"], f"whg:{self.public_ok.pk}")
        self.assertEqual(body["geometry"], SQUARE)

    def test_unknown_src_id_with_slashes_is_404_not_503(self):
        pid = self.pid(self.public_ok, "https://example.org/nowhere/")
        with patch(GET, side_effect=_fake_gateway_get) as get:
            resp = self.geometry(pid)
        get.assert_called_once()
        self.assertEqual(resp.status_code, 404, resp.content[:200])
        self.assertEqual(resp.json(), {"error": "not found", "id": pid})
        self.assertNotIn("failure", resp.json())

    def test_src_id_with_slashes_under_an_unknown_dataset_is_404_before_the_gateway(self):
        with patch(GET, side_effect=_fake_gateway_get) as get:
            resp = self.geometry(f"whg:999999:{self.SLASH_SRC}")
        get.assert_not_called()
        self.assertEqual(resp.status_code, 404)

    def test_src_id_with_slashes_in_a_private_dataset_is_404_to_a_stranger(self):
        with patch(GET, side_effect=_fake_gateway_get) as get:
            resp = self.geometry(self.pid(self.private_ok, self.SLASH_SRC))
        get.assert_not_called()
        self.assertEqual(resp.status_code, 404)

    def test_a_gateway_without_the_path_route_is_still_a_503_not_a_miss(self):
        # Before the gateway gained `{place_id:path}`, a slash id fell through
        # to its ES catch-all (measured: 401 security_exception). That is a
        # failure to answer, and stays one — the client must not read it as
        # "no such place".
        from search.tests_atlas_geometry import _unrestarted_gateway
        with patch(GET, side_effect=_unrestarted_gateway) as get:
            resp = self.geometry(self.pid(self.public_ok, self.SLASH_SRC))
        get.assert_called_once()
        self.assertEqual(resp.status_code, 503)
        self.assertEqual(resp.json()["failure"], "http")

    def test_private_dataset_is_404_to_a_beta_stranger_and_served_to_its_circle(self):
        pid = self.pid(self.private_ok)
        with patch(GET, side_effect=_fake_gateway_get) as get:
            resp = self.geometry(pid)
        get.assert_not_called()
        self.assertEqual(resp.status_code, 404)
        self.assertEqual(resp.json(), {"error": "not found", "id": pid})
        for who, user in [("owner", self.owner), ("collaborator", self.collab), ("staff", self.staff)]:
            with self.subTest(who=who):
                self.client.force_login(user)
                with patch(GET, side_effect=_fake_gateway_get) as get:
                    self.assertEqual(self.geometry(pid).status_code, 200)
                get.assert_called_once()
        # An owner who is not a beta tester meets the beta gate first.
        self.client.force_login(self.plain_owner)
        with patch(GET, side_effect=_fake_gateway_get) as get:
            self.assertEqual(self.geometry(self.pid(self.plain_private)).status_code, 403)
        get.assert_not_called()

    def test_embargoed_dataset_is_404_to_a_beta_stranger_until_released(self):
        with patch(GET, side_effect=_fake_gateway_get) as get:
            held = self.geometry(self.pid(self.embargoed))
            released = self.geometry(self.pid(self.released))
        self.assertEqual(held.status_code, 404)
        self.assertEqual(released.status_code, 200)
        get.assert_called_once()
        # The owner sees through the embargo.
        self.client.force_login(self.owner)
        with patch(GET, side_effect=_fake_gateway_get):
            self.assertEqual(self.geometry(self.pid(self.embargoed)).status_code, 200)

    def test_unlicensed_custom_and_nd_datasets_are_451_without_asking_the_gateway(self):
        for name, ds, error in [("none", self.public_unlicensed, "source licence not determined"),
                                ("custom", self.public_custom, "source not redistributable"),
                                ("ND", self.public_nd, "source not redistributable")]:
            with self.subTest(licence=name):
                with patch(GET, side_effect=_fake_gateway_get) as get:
                    resp = self.geometry(self.pid(ds))
                get.assert_not_called()
                self.assertEqual(resp.status_code, 451)
                body = resp.json()
                self.assertEqual(body["error"], error)
                self.assertEqual(body["namespace"], "whg")
                self.assertEqual(body["source"]["source_url"], "https://example.org/ds")
                self.assertNotIn("coordinates", json.dumps(body))
        # NC is served: WHG is non-commercial and the terms travel with the data.
        with patch(GET, side_effect=_fake_gateway_get) as get:
            resp = self.geometry(self.pid(self.public_nc))
        get.assert_called_once()
        self.assertEqual(resp.status_code, 200)
        self.assertIs(resp.json()["attribution"]["license__permits_commercial"], False)

    def test_ids_naming_no_dataset_are_404_without_asking_the_gateway(self):
        for pid in ("whg:42", "whg:abc:1", "whg:999999999:1", f"whg:{self.public_ok.pk}:"):
            with self.subTest(pid=pid):
                with patch(GET, side_effect=_fake_gateway_get) as get:
                    resp = self.geometry(pid)
                get.assert_not_called()
                self.assertEqual(resp.status_code, 404)
        with patch(GET, side_effect=_fake_gateway_get):
            self.assertEqual(self.geometry(self.pid(self.public_ok)).status_code, 200)

    def test_no_shared_secret_is_a_503_not_a_determination(self):
        with override_settings(CRC_GATEWAY_API_KEY=""), patch(GET, side_effect=_fake_gateway_get) as get:
            resp = self.geometry(self.pid(self.public_ok))
        get.assert_not_called()
        self.assertEqual(resp.status_code, 503)
        self.assertEqual(resp.json()["failure"], "unconfigured")
        with patch(GET, side_effect=_fake_gateway_get):
            self.assertEqual(self.geometry(self.pid(self.public_ok)).status_code, 200)

    def test_gateway_withholding_a_lent_polygon_wins_over_a_permitted_dataset(self):
        with patch(GET, side_effect=_fake_gateway_get) as get:
            resp = self.geometry(self.pid(self.public_ok, "lent"))
        get.assert_called_once()
        self.assertEqual(resp.status_code, 451)
        self.assertIn("borrowed", resp.json()["detail"])

    def test_a_view_that_dropped_the_grant_would_be_refused_by_the_gateway(self):
        # The fake is a real verifier: strip the header on the way out and the
        # gateway's answer, not a cheerful 200, is what the view returns.
        from api import crc_client
        real_get = crc_client.requests.get

        def strip(url, params=None, headers=None, **kw):
            headers = {k: v for k, v in (headers or {}).items() if k != GRANT_HEADER}
            return _fake_gateway_get(url, params=params, headers=headers, **kw)
        with patch(GET, side_effect=strip):
            resp = self.geometry(self.pid(self.public_ok))
        self.assertEqual(resp.status_code, 451)
        self.assertEqual(resp.json()["grant"], "missing")
        self.assertIs(crc_client.requests.get, real_get)

    def test_authority_path_is_unchanged(self):
        with patch(GET, side_effect=_fake_gateway_get) as get, \
                patch("api.attribution.registry_attribution",
                      return_value={"name": "OSM", "redistributable": True}):
            resp = self.geometry("osm:r1")
        self.assertEqual(resp.status_code, 200)
        self.assertNotIn(GRANT_HEADER, get.call_args.kwargs["headers"])


class PortalViewTests(ContributedAccessBase):
    """/atlas/place/ for whg: ids — the place#310 rule, found and fixed here."""

    def setUp(self):
        super().setUp()
        self.client.force_login(self.beta)

    def test_private_dataset_record_is_404_to_a_beta_stranger_before_the_gateway_is_asked(self):
        pid = self.pid(self.private_ok)
        with patch(POST, side_effect=_fake_gateway_post) as post:
            resp = self.place(pid)
        post.assert_not_called()
        self.assertEqual(resp.status_code, 404)
        self.assertEqual(resp.json(), {"error": "not found", "id": pid})
        # The gateway DOES know the record (premise of the leak): its circle gets it.
        for who, user in [("owner", self.owner), ("collaborator", self.collab), ("staff", self.staff)]:
            with self.subTest(who=who):
                self.client.force_login(user)
                with patch(POST, side_effect=_fake_gateway_post) as post:
                    resp = self.place(pid)
                post.assert_called_once()
                self.assertEqual(resp.status_code, 200)
                self.assertEqual(resp.json()["attribution"]["name"], self.private_ok.title)

    def test_embargoed_dataset_record_is_404_to_a_beta_stranger(self):
        with patch(POST, side_effect=_fake_gateway_post) as post:
            self.assertEqual(self.place(self.pid(self.embargoed)).status_code, 404)
            self.assertEqual(self.place(self.pid(self.released)).status_code, 200)
        post.assert_called_once()

    def test_public_record_is_served_with_its_own_attribution_licensed_or_not(self):
        with patch(POST, side_effect=_fake_gateway_post) as post:
            licensed = self.place(self.pid(self.public_ok))
            unlicensed = self.place(self.pid(self.public_unlicensed))
        self.assertEqual(post.call_count, 2)
        self.assertEqual(licensed.status_code, 200)
        attr = licensed.json()["attribution"]
        self.assertEqual(attr["license__spdx_id"], "CC-BY-4.0")
        self.assertEqual(attr["name"], self.public_ok.title)
        self.assertEqual(attr["dataset"], "pub_ok")
        # The record is not licence-gated (as on every other surface serving
        # contributed records); the missing licence is reported, not papered
        # over with the umbrella row.
        self.assertEqual(unlicensed.status_code, 200)
        attr = unlicensed.json()["attribution"]
        self.assertIsNone(attr["license__spdx_id"])
        self.assertEqual(attr["name"], self.public_unlicensed.title)

    def test_ids_naming_no_dataset_are_404_without_asking_the_gateway(self):
        for pid in ("whg:42", "whg:999999999:1", "whg:abc:1"):
            with self.subTest(pid=pid):
                with patch(POST, side_effect=_fake_gateway_post) as post:
                    self.assertEqual(self.place(pid).status_code, 404)
                post.assert_not_called()

    def test_authority_path_is_unchanged(self):
        from search.tests_atlas import _fake_gateway
        with patch(POST, side_effect=_fake_gateway) as post, \
                patch("api.attribution.registry_attribution",
                      return_value={"name": "GeoNames", "redistributable": True}):
            resp = self.place("gn:2643743")
        post.assert_called_once()
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["attribution"]["name"], "GeoNames")
