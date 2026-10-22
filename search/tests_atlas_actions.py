"""The Atlas's beta per-record actions: ``record_url`` on the server side (place#325).

The map popup and the Place List rows offer a beta viewer three actions on one
record (gazetteerInteraction.js ``recordActionsHtml``). Only "Record" needs the
server: it opens ``/atlas/place/<pk>/``, so it needs the pk, and it must exist
only for a contributed record the viewer may see. Two responses carry it:

* each hit of ``POST /atlas/search/`` (the Place List's endpoint, and the main
  Atlas search: the same view) gets ``record_url`` when it is a ``whg:`` record
  with a ``places.Place`` row in a dataset the viewer can see;
* ``GET /entity/place:whg:<ds>:<src>/api?variant=popup`` gets ``record_url``
  when the viewer is a beta user; every other response is unchanged.

Run locally (never in the prod container):

    DB_NAME=<private> GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test search.tests_atlas_actions --settings=whg.settings_localtest --noinput

How these can fail: every absence is asserted in the same test as a presence on
the same response or route (a record that gets a url beside one that must not),
so a response that carries no ``record_url`` at all fails, and so does one that
puts it everywhere. The popup's "unchanged" case compares bytes with the same
request without ``variant=popup`` (the legacy path ignored the variant before).
The gateway is faked at ``api.crc_client.requests.post``, the one HTTP seam.
"""
import json
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings

from search.tests_locus import GATEWAY, HITS, POST, FakeGateway

CLIO = {"place_id": "clio:(afsharid_iran)_1738_1740", "title": "(Afsharid Iran)",
        "namespace": "clio", "repr_point": [51.4, 35.7]}


class Gateway(FakeGateway):
    """``FakeGateway`` whose responses also answer ``raise_for_status`` (the
    entity view's ``crc_fetch_places`` calls it; the locus fakes never needed it)."""

    def __call__(self, url, json=None, **kwargs):
        resp = super().__call__(url, json=json, **kwargs)
        resp.raise_for_status = lambda: None
        return resp


CSRF = {"HTTP_X_CSRFTOKEN": "test"}   # what the Atlas popup sends (the test client does not enforce it)


@override_settings(CRC_GATEWAY_URL=GATEWAY, CRC_GATEWAY_TIMEOUT=1)
class ActionsBase(TestCase):
    @classmethod
    def setUpTestData(cls):
        from datasets.models import Dataset
        from places.models import Place
        User = get_user_model()
        cls.normal = User.objects.create_user(
            username="act_normal", email="an@example.org", password="pw",
            given_name="Norma", surname="User", role="normal")
        cls.beta = User.objects.create_user(
            username="act_beta", email="ab@example.org", password="pw",
            given_name="Bea", surname="Tester", role="beta_tester")
        cls.beta2 = User.objects.create_user(
            username="act_beta2", email="ab2@example.org", password="pw",
            given_name="Ben", surname="Tester", role="beta_tester")
        cls.public = Dataset.objects.create(owner=cls.normal, label="act_public", title="Public",
                                            description="D", public=True)
        cls.mine = Dataset.objects.create(owner=cls.beta, label="act_mine", title="Mine",
                                          description="D", public=False)
        cls.theirs = Dataset.objects.create(owner=cls.normal, label="act_theirs", title="Theirs",
                                            description="D", public=False)
        cls.p_public = Place.objects.create(title="Isfahan", src_id="p1", dataset=cls.public, ccodes=[])
        cls.p_mine = Place.objects.create(title="Mine 1", src_id="m1", dataset=cls.mine, ccodes=[])
        cls.p_theirs = Place.objects.create(title="Theirs 1", src_id="t1", dataset=cls.theirs, ccodes=[])
        cls.pid_public = f"whg:{cls.public.id}:p1"
        cls.pid_mine = f"whg:{cls.mine.id}:m1"
        cls.pid_theirs = f"whg:{cls.theirs.id}:t1"
        cls.pid_norow = f"whg:{cls.public.id}:no-such-row"

    def setUp(self):
        self.assertFalse(self.normal.can_access_beta)
        self.assertTrue(self.beta.can_access_beta)


class SearchHitRecordUrlTests(ActionsBase):
    def _hits(self):
        extra = [{"place_id": pid, "title": t, "namespace": "whg", "repr_point": [1, 1]}
                 for pid, t in ((self.pid_public, "Isfahan"), (self.pid_mine, "Mine 1"),
                                (self.pid_theirs, "Theirs 1"), (self.pid_norow, "Nobody"))]
        return HITS + extra

    def _search(self):
        gw = Gateway(hits=self._hits())
        with patch(POST, side_effect=gw):
            resp = self.client.post("/atlas/search/", data=json.dumps({"qstr": "x", "namespaces": ["whg"]}),
                                    content_type="application/json")
        self.assertEqual(len(gw.bodies), 1 if resp.status_code == 200 else 0)
        return resp

    def test_contributed_hits_carry_their_record_page_and_nothing_else_does(self):
        self.client.force_login(self.beta)
        resp = self._search()
        self.assertEqual(resp.status_code, 200)
        by_pid = {h["place_id"]: h for h in resp.json()["hits"]}
        self.assertEqual(by_pid[self.pid_public]["record_url"], f"/atlas/place/{self.p_public.id}/")
        self.assertEqual(by_pid[self.pid_mine]["record_url"], f"/atlas/place/{self.p_mine.id}/")
        # No Place row: no Record page, so no url (never the /entity/ fallback).
        self.assertNotIn("record_url", by_pid[self.pid_norow])
        # Authority records have no Record page.
        for pid in ("gn:1", "wd:Q64", "gn:2"):
            self.assertNotIn("record_url", by_pid[pid])
        # A dataset outside the viewer's circle: the hit is withheld (place#310), pk and all.
        self.assertNotIn(self.pid_theirs, by_pid)
        self.assertNotIn(f"/atlas/place/{self.p_theirs.id}/", resp.content.decode())

    def test_another_beta_user_gets_no_url_for_a_private_dataset_they_cannot_see(self):
        self.client.force_login(self.beta2)
        by_pid = {h["place_id"]: h for h in self._search().json()["hits"]}
        self.assertEqual(by_pid[self.pid_public]["record_url"], f"/atlas/place/{self.p_public.id}/")
        self.assertNotIn(self.pid_mine, by_pid)
        self.assertNotIn(self.pid_theirs, by_pid)

    def test_non_beta_is_refused_as_before(self):
        self.client.force_login(self.normal)
        self.assertEqual(self._search().status_code, 403)
        self.client.logout()
        self.assertEqual(self._search().status_code, 403)

    def test_helper_checks_visibility_itself_and_the_beta_rule(self):
        """The helper does not rely on the withholding that runs before it: fed
        a hit from a dataset the viewer cannot see, it puts no url on it, while
        it does on the visible one beside it. And a non-beta user gets none."""
        from search.views import _add_hit_record_urls
        data = {"hits": [{"place_id": self.pid_theirs}, {"place_id": self.pid_public}]}
        _add_hit_record_urls(data, self.beta)
        self.assertNotIn("record_url", data["hits"][0])
        self.assertEqual(data["hits"][1]["record_url"], f"/atlas/place/{self.p_public.id}/")
        data = {"hits": [{"place_id": self.pid_public}]}
        _add_hit_record_urls(data, self.normal)
        self.assertNotIn("record_url", data["hits"][0])

    def test_the_locus_resolver_is_unchanged(self):
        """The resolver shares the search response builder; it keeps its own
        ``record_urls`` map and gets no per-hit field."""
        from search.tests_locus import token
        self.client.force_login(self.beta)
        with patch(POST, side_effect=FakeGateway(hits=self._hits())):
            data = self.client.post("/atlas/locus/resolve/", data=json.dumps({"r": token()}),
                                    content_type="application/json").json()
        self.assertEqual(data["record_urls"][self.pid_public], f"/atlas/place/{self.p_public.id}/")
        self.assertFalse(any("record_url" in h for h in data["hits"]))


class PopupRecordUrlTests(ActionsBase):
    def _popup(self, pid, popup=True, **headers):
        url = f"/entity/place:{pid}/api"
        with patch(POST, side_effect=Gateway(hits=HITS + [CLIO])):
            return self.client.get(url, {"variant": "popup"} if popup else {}, **headers)

    def test_beta_viewer_gets_the_record_page_as_the_atlas_asks(self):
        # As the Atlas asks: session cookie AND X-CSRFToken (so DRF's user is a
        # CSRFUser; the beta check reads the session behind it).
        self.client.force_login(self.beta)
        resp = self._popup(self.pid_public, **CSRF)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["record_url"], f"/atlas/place/{self.p_public.id}/")
        # place#329: the popup variant is the flat popup shape (title at the top level).
        self.assertEqual(resp.json()["title"], "Isfahan")
        # Same viewer, same record, without the variant: no url (only the popup asks).
        self.assertNotIn("record_url", self._popup(self.pid_public, popup=False, **CSRF).json())
        # Session only (SessionAuthentication): the same answer.
        self.assertEqual(self._popup(self.pid_public).json()["record_url"],
                         f"/atlas/place/{self.p_public.id}/")

    def test_authority_popup_never_gets_one(self):
        self.client.force_login(self.beta)
        resp = self._popup(CLIO["place_id"], **CSRF)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["title"], "(Afsharid Iran)")
        self.assertNotIn("record_url", resp.json())
        # ... while the contributed popup on the same session does.
        self.assertIn("record_url", self._popup(self.pid_public, **CSRF).json())

    def test_anonymous_and_non_beta_responses_are_unchanged(self):
        """Anonymous and non-beta viewers get the same popup bytes, with no
        ``record_url``; the beta viewer's popup on the same record differs by
        exactly ``record_url``. (Since place#329 the popup variant is the flat
        popup shape, so it is no longer equal to the LPF answer without the
        variant; the LPF answer is the same for both viewers.)"""
        anon_popup = self._popup(self.pid_public, **CSRF)
        anon_plain = self._popup(self.pid_public, popup=False, **CSRF)
        self.assertEqual(anon_popup.status_code, 200)
        self.assertEqual(anon_popup.json()["place_id"], self.pid_public)
        self.assertEqual(anon_plain.json()["properties"]["title"], "Isfahan")
        self.assertNotIn(b"record_url", anon_popup.content)
        self.assertNotIn(b"record_url", anon_plain.content)
        self.client.force_login(self.normal)
        normal_popup = self._popup(self.pid_public, **CSRF)
        self.assertEqual(normal_popup.content, anon_popup.content)
        self.assertEqual(self._popup(self.pid_public, popup=False, **CSRF).content, anon_plain.content)
        self.assertNotIn(b"record_url", normal_popup.content)
        self.client.force_login(self.beta)
        beta = self._popup(self.pid_public, **CSRF).json()
        self.assertEqual(beta.pop("record_url"), f"/atlas/place/{self.p_public.id}/")
        self.assertEqual(beta, anon_popup.json())

    def test_private_records_never_leak_a_pk(self):
        # The owner (beta), session only: the record is theirs to see, url present.
        self.client.force_login(self.beta)
        own = self._popup(self.pid_mine)
        self.assertEqual(own.status_code, 200)
        self.assertEqual(own.json()["record_url"], f"/atlas/place/{self.p_mine.id}/")
        # Another beta user: the record is not served at all, so no pk either.
        self.client.force_login(self.beta2)
        for headers in ({}, CSRF):
            resp = self._popup(self.pid_mine, **headers)
            self.assertEqual(resp.status_code, 404)
            self.assertNotIn(f"/atlas/place/{self.p_mine.id}/".encode(), resp.content)
        resp = self._popup(self.pid_theirs, **CSRF)
        self.assertEqual(resp.status_code, 404)
        self.assertNotIn(b"record_url", resp.content)

    def test_record_url_follows_the_record_pages_own_rule(self):
        """``_popup_record_url`` asks ``get_visible_place_or_404`` for the
        viewer: a record the viewer cannot see gets None even if a caller
        hands it in, while one they can see gets the url."""
        from django.test import RequestFactory
        from api.views_entity import _popup_record_url
        req = RequestFactory().get("/")
        req.user = self.beta2
        self.assertIsNone(_popup_record_url(req, self.p_theirs))
        self.assertEqual(_popup_record_url(req, self.p_public), f"/atlas/place/{self.p_public.id}/")
        req.user = self.normal
        self.assertIsNone(_popup_record_url(req, self.p_public))
