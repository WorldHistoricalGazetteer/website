"""The Atlas Place List for a CONTRIBUTED dataset (place#332).

The Place List browses a gazetteer by posting ``{"browse": true, "namespaces":
["<gazetteer id>"]}`` to ``/atlas/search/`` (atlasPlaceList.js ``_fetchPage``).
For a contributed dataset the id is ``whg:<dataset_id>``. The gateway used to
filter that against the places' ``namespace`` (always ``whg``) and returned
nothing; it now filters on ``dataset_id`` and echoes ``datasets_searched``
(indexing branch ``fix/browse-contributed-dataset``). The fake gateway here
implements BOTH behaviours, selected per test, so what the website does with
each is pinned.

Website side, tested here:

* the request reaches the gateway as the panel sent it (``browse``,
  ``namespaces: ["whg:<pk>"]``), and the hits come back with ``record_url``
  (the Place List's Record button);
* a dataset outside the viewer's circle (private, or embargoed: place#162) is
  dropped from the scope BEFORE the gateway is asked; when it was the whole
  scope the answer is empty and the gateway is not called at all, so not even
  the dataset's size leaves the gateway. Its owner, through the same view,
  gets the list (presence paired with absence);
* an authority gazetteer is untouched; non-beta and anonymous callers still get
  403 and the gateway is never asked.

Run locally (never in the prod container):

    DB_NAME=<private> GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test search.tests_atlas_browse_contributed --settings=whg.settings_localtest --noinput

How these fail: without ``_withhold_hidden_dataset_scope`` the hidden-dataset
tests see one gateway call (and its total) instead of none; the old-gateway
test is the reason the panel needs the ``datasets_searched`` echo at all.
"""
import copy
import json
from datetime import timedelta
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings
from django.utils import timezone

from search.tests_locus import GATEWAY, POST, SEARCH_OK, _Resp

CLIO_ROWS = [
    {"place_id": "clio:a", "title": "Afsharid Iran", "namespace": "clio", "repr_point": [51.4, 35.7]},
    {"place_id": "clio:b", "title": "Babylon", "namespace": "clio", "repr_point": [44.4, 32.5]},
]


class ScopedGateway:
    """``POST /api/search`` that honours ``namespaces`` the way the gateway does.

    ``dataset_scope=True`` is the fixed gateway: an entry with a colon is a
    dataset id, matched on the place id's ``<entry>:`` prefix (the index's
    ``dataset_id``), and the response echoes ``datasets_searched``. False is the
    gateway before the fix: every entry is compared with the hit's
    ``namespace`` and there is no echo."""

    def __init__(self, hits, dataset_scope=True):
        self.hits = hits
        self.dataset_scope = dataset_scope
        self.bodies = []

    def _in_scope(self, hit, scope):
        for ns in scope:
            if self.dataset_scope and ":" in ns:
                if hit["place_id"].startswith(ns + ":"):
                    return True
            elif hit.get("namespace") == ns:
                return True
        return False

    def __call__(self, url, json=None, **kwargs):
        assert url.endswith("/api/search"), url
        self.bodies.append(copy.deepcopy(json))
        scope = (json or {}).get("namespaces") or []
        hits = [h for h in self.hits if not scope or self._in_scope(h, scope)]
        payload = copy.deepcopy(SEARCH_OK)
        payload.update(hits=copy.deepcopy(hits), total=len(hits), namespaces_searched=list(scope))
        if self.dataset_scope:
            payload["datasets_searched"] = [n for n in scope if ":" in n]
        return _Resp(200, payload)


def browse_body(ns, qstr=""):
    """What atlasPlaceList.js ``_fetchPage`` posts (no date / type filter)."""
    return {"qstr": qstr, "namespaces": [ns], "mode": "in", "size": 100, "offset": 0,
            "browse": not qstr, "cluster": False,
            "bounds": {"type": "GeometryCollection", "geometries": []}, "spatial": "none",
            "countries": [], "aat_types": [], "types": []}


@override_settings(CRC_GATEWAY_URL=GATEWAY, CRC_GATEWAY_TIMEOUT=1)
class BrowseContributedTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        from api.models import GazetteerRegistryEntry
        from datasets.models import Dataset
        from places.models import Place
        User = get_user_model()
        mk = User.objects.create_user
        cls.beta = mk(username="pl_beta", email="plb@example.org", password="pw",
                      given_name="Bea", surname="Tester", role="beta_tester")
        cls.beta2 = mk(username="pl_beta2", email="plb2@example.org", password="pw",
                       given_name="Ben", surname="Tester", role="beta_tester")
        cls.normal = mk(username="pl_normal", email="pln@example.org", password="pw",
                        given_name="Norma", surname="User", role="normal")
        with patch("datasets.signals.doi"):
            def ds(label, public, owner):
                return Dataset.objects.create(owner=owner, label=label, title=label,
                                              description="D", public=public)
            cls.public = ds("pl_public", True, cls.normal)
            cls.mine = ds("pl_mine", False, cls.beta)          # private, owned by beta
            cls.embargoed = ds("pl_embargoed", True, cls.normal)
        GazetteerRegistryEntry.objects.create(
            id=f"whg:{cls.embargoed.pk}", name="Embargoed", namespace="whg",
            entry_class="dataset", status="embargoed",
            embargo_release_at=timezone.now() + timedelta(days=30))
        cls.rows = {}
        for d, n in ((cls.public, 3), (cls.mine, 2), (cls.embargoed, 2)):
            for i in range(n):
                p = Place.objects.create(title=f"{d.label} {i}", src_id=f"s{i}", dataset=d, ccodes=[])
                cls.rows[f"whg:{d.pk}:s{i}"] = p
        cls.hits = CLIO_ROWS + [
            {"place_id": pid, "title": p.title, "namespace": "whg", "repr_point": [1.0, 1.0]}
            for pid, p in cls.rows.items()]

    def setUp(self):
        self.assertTrue(self.beta.can_access_beta)
        self.assertTrue(self.beta2.can_access_beta)
        self.assertFalse(self.normal.can_access_beta)

    def _post(self, body, dataset_scope=True):
        gw = ScopedGateway(self.hits, dataset_scope=dataset_scope)
        with patch(POST, side_effect=gw):
            resp = self.client.post("/atlas/search/", data=json.dumps(body), content_type="application/json")
        return resp, gw

    def _pids(self, resp):
        return [h["place_id"] for h in resp.json()["hits"]]

    def test_a_contributed_dataset_lists_its_places_with_record_buttons(self):
        self.client.force_login(self.beta2)
        ns = f"whg:{self.public.pk}"
        resp, gw = self._post(browse_body(ns))
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(gw.bodies), 1)
        self.assertTrue(gw.bodies[0]["browse"])
        self.assertEqual(gw.bodies[0]["namespaces"], [ns])      # as the panel sent it
        data = resp.json()
        self.assertEqual(self._pids(resp), [f"{ns}:s0", f"{ns}:s1", f"{ns}:s2"])
        self.assertEqual(data["total"], 3)
        self.assertEqual(data["datasets_searched"], [ns])
        for h in data["hits"]:
            self.assertEqual(h["record_url"], f"/atlas/place/{self.rows[h['place_id']].pk}/")
        # A typed search inside the list is scoped the same way.
        resp, gw = self._post(browse_body(ns, qstr="pl_public 1"))
        self.assertNotIn("browse", gw.bodies[0])
        self.assertEqual(gw.bodies[0]["namespaces"], [ns])

    def test_a_private_dataset_is_listed_for_its_owner_and_never_asked_for_anyone_else(self):
        ns = f"whg:{self.mine.pk}"
        self.client.force_login(self.beta)          # the owner
        resp, gw = self._post(browse_body(ns))
        self.assertEqual(len(gw.bodies), 1)
        self.assertEqual(sorted(self._pids(resp)), [f"{ns}:s0", f"{ns}:s1"])
        self.client.force_login(self.beta2)         # another beta user
        resp, gw = self._post(browse_body(ns))
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(gw.bodies, [], "the gateway was asked for a dataset this user cannot see")
        data = resp.json()
        self.assertEqual((data["hits"], data["total"]), ([], 0))
        self.assertNotIn("withheld", data)          # not even a count
        for pid, p in self.rows.items():
            if pid.startswith(ns + ":"):
                self.assertNotIn(pid, resp.content.decode())
                self.assertNotIn(f"/atlas/place/{p.pk}/", resp.content.decode())
        # The same for a typed search in that list.
        resp, gw = self._post(browse_body(ns, qstr="pl_mine"))
        self.assertEqual((gw.bodies, resp.json()["hits"]), ([], []))

    def test_an_embargoed_dataset_is_not_asked_for(self):
        ns = f"whg:{self.embargoed.pk}"
        self.client.force_login(self.beta2)
        resp, gw = self._post(browse_body(ns))
        self.assertEqual(gw.bodies, [])
        self.assertEqual(resp.json()["hits"], [])
        # Control: the public dataset through the same request shape is asked for.
        resp, gw = self._post(browse_body(f"whg:{self.public.pk}"))
        self.assertEqual(len(gw.bodies), 1)

    def test_a_hidden_dataset_is_dropped_from_a_wider_scope_never_widening_it(self):
        self.client.force_login(self.beta2)
        hidden, public = f"whg:{self.mine.pk}", f"whg:{self.public.pk}"
        resp, gw = self._post(dict(browse_body("clio"), namespaces=["clio", hidden, public]))
        self.assertEqual(gw.bodies[0]["namespaces"], ["clio", public])
        pids = self._pids(resp)
        self.assertIn("clio:a", pids)
        self.assertIn(f"{public}:s0", pids)
        self.assertFalse([p for p in pids if p.startswith(hidden + ":")])

    def test_an_authority_gazetteer_still_lists(self):
        self.client.force_login(self.beta2)
        resp, gw = self._post(browse_body("clio"))
        self.assertEqual(gw.bodies[0]["namespaces"], ["clio"])
        self.assertTrue(gw.bodies[0]["browse"])
        self.assertEqual(self._pids(resp), ["clio:a", "clio:b"])
        self.assertFalse(any("record_url" in h for h in resp.json()["hits"]))

    def test_a_gateway_without_the_dataset_scope_returns_nothing_and_no_echo(self):
        """What the panel sees from a gateway before the fix: no hits, and no
        ``datasets_searched``, which is the panel's cue to say the list cannot
        show the dataset rather than that the dataset is empty. Paired with the
        fixed gateway through the same request, which lists."""
        self.client.force_login(self.beta2)
        ns = f"whg:{self.public.pk}"
        old, _ = self._post(browse_body(ns), dataset_scope=False)
        new, _ = self._post(browse_body(ns), dataset_scope=True)
        self.assertEqual(old.json()["hits"], [])
        self.assertNotIn("datasets_searched", old.json())
        self.assertEqual(len(new.json()["hits"]), 3)
        self.assertIn("datasets_searched", new.json())

    def test_who_can_call_it_is_unchanged(self):
        ns = f"whg:{self.public.pk}"
        for user in (self.normal, None):
            if user:
                self.client.force_login(user)
            else:
                self.client.logout()
            resp, gw = self._post(browse_body(ns))
            self.assertEqual(resp.status_code, 403)
            self.assertEqual(gw.bodies, [])
        self.client.force_login(self.beta2)
        self.assertEqual(self._post(browse_body(ns))[0].status_code, 200)
