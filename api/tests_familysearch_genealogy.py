"""`mode: "familysearch"` and the genealogy place endpoint, whg3 side.

The gateway owns the matching. These tests pin what whg3 must do around it:

* a FamilySearch `q` string is gateway-only syntax, so the legacy index must NEVER be searched for it
  (it would rank the raw string as plain text beside real results);
* `offset` is forwarded, and refused where it cannot be honoured, rather than ignored;
* the `familysearch` interpretation block reaches the client;
* the genealogy endpoint is authenticated, charged per place, validated before charging, forwards only
  allow-listed fields, and removes embargoed / withheld candidates like every other path.

Each refusal test has a control showing the same request is accepted when it should be.
"""
import json
from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase, override_settings
from rest_framework.test import APIRequestFactory, force_authenticate

from api.dataset_access import _NOTHING_HIDDEN
from api.reconcile import normalise_query_params, reconcile_place_es


class _U:
    is_authenticated = True
    is_anonymous = False
    is_active = True
    id = 1
    username = "probe"

    def __str__(self):
        return self.username


def _q(**p):
    p.setdefault("query", "Paris")
    return normalise_query_params(p)


def _gateway(**meta_updates):
    def _fake(normalised_query, user=None, namespaces=None, meta=None):
        if meta is not None:
            meta.update(meta_updates)
        return []
    return _fake


def _run(query, **meta):
    legacy = MagicMock(return_value=[])
    with patch("api.reconcile.crc_reconcile_search", _gateway(**meta)), \
            patch("api.reconcile.es_search", legacy), \
            patch("api.reconcile.hidden_datasets", return_value=_NOTHING_HIDDEN), \
            patch("api.reconcile.hidden_namespaces", return_value=set()):
        res = reconcile_place_es(query)
    return res, legacy


class FamilySearchReconcileTests(SimpleTestCase):

    def test_legacy_index_is_not_searched(self):
        _, legacy = _run(_q(query="name:par*,france", mode="familysearch"))
        legacy.assert_not_called()

    def test_control_other_modes_still_search_legacy(self):
        _, legacy = _run(_q(query="Paris", mode="fuzzy"))
        legacy.assert_called_once()

    def test_interpretation_block_is_echoed(self):
        block = {"phonetic": False, "clauses": [{"field": "name"}]}
        res, _ = _run(_q(query="par*", mode="familysearch"), familysearch=block)
        self.assertEqual(res["familysearch"], block)

    def test_no_block_for_other_modes_or_old_gateway(self):
        res, _ = _run(_q(query="Paris", mode="fuzzy"), familysearch={"phonetic": False})
        self.assertNotIn("familysearch", res)
        res, _ = _run(_q(query="par*", mode="familysearch"))
        self.assertNotIn("familysearch", res)

    def test_offset_needs_familysearch(self):
        with self.assertRaises(ValueError):
            _q(mode="fuzzy", offset=10)
        self.assertEqual(_q(mode="familysearch", offset="10")["offset"], 10)
        self.assertEqual(_q(mode="fuzzy")["offset"], 0)

    def test_bad_offset_refused(self):
        with self.assertRaises(ValueError):
            _q(mode="familysearch", offset=-1)
        with self.assertRaises(ValueError):
            _q(mode="familysearch", offset="abc")


class FamilySearchClientTests(SimpleTestCase):

    def _body(self, **raw):
        from api import crc_client
        resp = MagicMock(status_code=200)
        resp.json.return_value = {"hits": [], "familysearch": {"phonetic": False}}
        meta = {}
        with override_settings(CRC_GATEWAY_URL="http://gw"), \
                patch.object(crc_client, "_headers", return_value={}), \
                patch.object(crc_client.requests, "post", return_value=resp) as post:
            crc_client.crc_reconcile_search(normalise_query_params({"query": "Paris", **raw}), user=_U(), meta=meta)
        return post.call_args.kwargs["json"], meta

    def test_offset_forwarded_and_block_captured(self):
        body, meta = self._body(mode="familysearch", offset=20)
        self.assertEqual(body["mode"], "familysearch")
        self.assertEqual(body["offset"], 20)
        self.assertEqual(meta["familysearch"], {"phonetic": False})

    def test_embedding_cannot_make_it_phonetic(self):
        body, _ = self._body(mode="familysearch", embedding=[0] * 128)
        self.assertEqual(body["mode"], "familysearch")
        self.assertNotIn("query_vector", body)

    def test_control_embedding_still_works_elsewhere(self):
        body, _ = self._body(mode="fuzzy", embedding=[0] * 128)
        self.assertEqual(body["mode"], "phonetic")
        self.assertIn("query_vector", body)


class GenealogyViewTests(SimpleTestCase):

    def _post(self, payload, gateway=(200, {"results": []}), budget=(True, 0), hidden_ns=frozenset(),
              hidden=_NOTHING_HIDDEN):
        from api import views_genealogy as v
        request = APIRequestFactory().post("/genealogy/place", data=json.dumps(payload),
                                           content_type="application/json")
        force_authenticate(request, user=_U())
        with patch.object(v, "crc_genealogy", return_value=gateway) as gw, \
                patch.object(v, "consume_query_budget", return_value=budget) as bud, \
                patch.object(v, "hidden_namespaces", return_value=set(hidden_ns)), \
                patch.object(v, "hidden_datasets", return_value=hidden):
            resp = v.GenealogyPlaceView.as_view()(request)
        return resp, gw, bud

    def test_ok_forwards_and_charges_per_place(self):
        resp, gw, bud = self._post({"places": ["a, b", "c, d", "e, f"], "service": "gedcom"})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(bud.call_args.args[1], 3)
        gw.assert_called_once()

    def test_unknown_field_refused_not_charged(self):
        resp, gw, bud = self._post({"place": "x, y", "contry_hint": "IE"})
        self.assertEqual(resp.status_code, 400)
        bud.assert_not_called()
        gw.assert_not_called()

    def test_batch_limit_refused_not_charged_and_limit_accepted(self):
        resp, _, bud = self._post({"places": ["p, q"] * 26})
        self.assertEqual(resp.status_code, 400)
        bud.assert_not_called()
        resp, _, _ = self._post({"places": ["p, q"] * 25})
        self.assertEqual(resp.status_code, 200)

    def test_empty_and_familysearch_mode_refused(self):
        self.assertEqual(self._post({})[0].status_code, 400)
        self.assertEqual(self._post({"place": "x", "mode": "familysearch"})[0].status_code, 400)

    def test_throttled_429_with_retry_after(self):
        resp, gw, _ = self._post({"place": "x, y"}, budget=(False, 17))
        self.assertEqual(resp.status_code, 429)
        self.assertEqual(resp["Retry-After"], "17")
        gw.assert_not_called()

    def test_gateway_errors_are_not_empty_200s(self):
        resp, _, _ = self._post({"place": "x"}, gateway=(502, {"error": "down"}))
        self.assertEqual(resp.status_code, 502)
        resp, _, _ = self._post({"place": "x"}, gateway=(422, {"detail": "unknown service"}))
        self.assertEqual(resp.status_code, 422)

    def test_embargoed_candidates_removed_and_control(self):
        data = {"results": [{"input": "x", "candidates": [{"place_id": "secret:1"}, {"place_id": "gn:2"}]}]}
        resp, _, _ = self._post({"place": "x"}, gateway=(200, json.loads(json.dumps(data))), hidden_ns={"secret"})
        ids = [c["place_id"] for c in json.loads(resp.content)["results"][0]["candidates"]]
        self.assertEqual(ids, ["gn:2"])
        resp, _, _ = self._post({"place": "x"}, gateway=(200, json.loads(json.dumps(data))))
        ids = [c["place_id"] for c in json.loads(resp.content)["results"][0]["candidates"]]
        self.assertEqual(ids, ["secret:1", "gn:2"])


class GenealogyClientTests(SimpleTestCase):

    def test_only_allowlisted_fields_are_forwarded(self):
        from api import crc_client
        resp = MagicMock(status_code=200)
        resp.json.return_value = {"results": []}
        with override_settings(CRC_GATEWAY_URL="http://gw"), \
                patch.object(crc_client, "_headers", return_value={}), \
                patch.object(crc_client.requests, "post", return_value=resp) as post:
            status, _ = crc_client.crc_genealogy({"place": "x, y", "service": "gedcom", "admin": True}, user=_U())
        sent = post.call_args.kwargs["json"]
        self.assertEqual(status, 200)
        self.assertEqual(sent, {"place": "x, y", "service": "gedcom"})
