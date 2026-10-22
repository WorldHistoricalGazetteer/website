"""The Atlas popup's payload for a contributed record (place#329).

``/entity/place:whg:<ds>:<src>/api?variant=popup`` used to answer with the LPF
Feature (``_legacy_place_to_lpf``): title under ``properties``, names as
``toponym``, dates under ``when``, no attribution, no ``place_id``. The popup
renderer (gazetteerInteraction.js) reads the gateway's flat PlaceDetail shape,
so most of a contributed popup was blank. The popup variant now gets that flat
shape (``_contributed_popup_payload``) with the dataset's attribution; every
other request still gets the LPF Feature, and authority popups are untouched.

Run locally (never in the prod container):

    DB_NAME=<private> GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test api.tests_popup_contributed --settings=whg.settings_localtest --noinput

How these can fail: the full payload is compared with a literal (any missing,
renamed or extra field fails); the LPF answer is compared with what the LPF
builder makes of the same serializer output and must differ from the popup;
the authority popup is compared with the raw gateway dict plus its registry
attribution, byte for byte, for an anonymous and a beta viewer.
"""
import json
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings

from search.tests_atlas_actions import CLIO, CSRF, Gateway
from search.tests_locus import GATEWAY, HITS, POST


def make_full_record(owner, label="pc_full", public=True):
    """A contributed record with two names, a type, a date range, a country
    code, a description, a depiction, a relation, a link, a dated polygon and a
    licence (CC-BY-4.0). Returns ``(dataset, place, pid)``."""
    from datasets.models import Dataset
    from licensing.models import License
    from places.models import (Place, PlaceDepiction, PlaceDescription, PlaceGeom, PlaceLink, PlaceName,
                               PlaceRelated, PlaceType, PlaceWhen)
    ds = Dataset.objects.create(owner=owner, label=label, title="Persian towns", description="D",
                                public=public, creator="A. Contributor", webpage="https://example.org/pt",
                                license=License.objects.get(spdx_id="CC-BY-4.0"), license_source="contributor")
    place = Place.objects.create(title="Isfahan", src_id="p1", dataset=ds, ccodes=["IR"], fclasses=["P"],
                                 minmax=[1050, 1722], timespans=[[1050, 1722]])
    PlaceName.objects.create(place=place, toponym="Isfahan", jsonb={"toponym": "Isfahan", "lang": "en"})
    PlaceName.objects.create(place=place, toponym="Esfahan", jsonb={
        "toponym": "Esfahan", "lang": "fa",
        "when": {"timespans": [{"start": {"earliest": "1500"}, "end": {"latest": "1722-10-23"}}]}})
    PlaceType.objects.create(place=place, aat_id=300008389, fclass="P",
                             jsonb={"identifier": "aat:300008389", "label": "cities", "sourceLabel": "city"})
    PlaceLink.objects.create(place=place, jsonb={"type": "closeMatch", "identifier": "wd:Q42053"})
    PlaceWhen.objects.create(place=place, minmax=[1050, 1722],
                             jsonb={"timespans": [{"start": {"in": "1050"}, "end": {"in": "1722"}}]})
    PlaceDescription.objects.create(place=place, jsonb={"value": "Capital of Safavid Persia.", "lang": "en"})
    PlaceDepiction.objects.create(place=place, jsonb={"@id": "https://example.org/isfahan.jpg",
                                                      "title": "Naqsh-e Jahan", "license": "CC0"})
    PlaceRelated.objects.create(place=place, jsonb={
        "relationType": "gvp:broaderPartitive", "relationTo": "wd:Q794", "label": "Persia",
        "when": {"timespans": [{"start": {"in": "1598"}, "end": {"in": "1722"}}]}})
    poly = {"type": "Polygon", "coordinates": [[[51.6, 32.6], [51.7, 32.6], [51.7, 32.7], [51.6, 32.6]]],
            "when": {"timespans": [{"start": {"in": "1600"}, "end": {"in": "1700"}}]}}
    PlaceGeom.objects.create(place=place, jsonb=poly)
    return ds, place, f"whg:{ds.id}:p1"


@override_settings(CRC_GATEWAY_URL=GATEWAY, CRC_GATEWAY_TIMEOUT=1)
class ContributedPopupPayloadTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        User = get_user_model()
        cls.owner = User.objects.create_user(username="pc_owner", email="pco@example.org", password="pw",
                                             given_name="O", surname="W", role="normal")
        cls.beta = User.objects.create_user(username="pc_beta", email="pcb@example.org", password="pw",
                                            given_name="B", surname="T", role="beta_tester")
        cls.ds, cls.place, cls.pid = make_full_record(cls.owner)

    def _get(self, pid, popup=True, **headers):
        with patch(POST, side_effect=Gateway(hits=HITS + [CLIO])):
            return self.client.get(f"/entity/place:{pid}/api", {"variant": "popup"} if popup else {}, **headers)

    def test_popup_payload_is_the_flat_popup_shape(self):
        resp = self._get(self.pid, **CSRF)
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        attribution = data.pop("attribution")
        self.assertEqual(data, {
            "place_id": self.pid,
            "title": "Isfahan",
            "namespace": "whg",
            "ccodes": ["IR"],
            "fclasses": ["P"],
            "names": [{"label": "Isfahan", "lang": "en"},
                      {"label": "Esfahan", "lang": "fa", "timespans": [{"start": 1500, "end": 1722}]}],
            "types": [{"identifier": "aat:300008389", "label": "cities", "sourceLabel": "city",
                       "aat_ids": [300008389]}],
            "timespans": [{"start": 1050, "end": 1722}],
            # The type only (no coordinates), with its dates: what the renderer names.
            "geometries": [{"geom": {"type": "Polygon"}, "timespans": [{"start": 1600, "end": 1700}]}],
            "relations": [{"relation_type": "gvp:broaderPartitive", "related_place_id": "wd:Q794",
                           "label": "Persia", "timespans": [{"start": 1598, "end": 1722}]}],
            "links": [{"type": "closeMatch", "identifier": "wd:Q42053"}],
            "descriptions": [{"value": "Capital of Safavid Persia.", "lang": "en"}],
            "depictions": [{"@id": "https://example.org/isfahan.jpg", "title": "Naqsh-e Jahan", "license": "CC0"}],
        })
        self.assertNotIn(b"coordinates", resp.content)
        # The dataset's own attribution: the block /atlas/place/ attaches (dataset_attribution).
        from api.dataset_access import dataset_attribution
        self.assertEqual(attribution, json.loads(json.dumps(dataset_attribution(self.ds))))
        self.assertEqual(attribution["name"], "Persian towns")
        self.assertEqual(attribution["license__spdx_id"], "CC-BY-4.0")
        self.assertEqual(attribution["rights_holder"], "A. Contributor")

    def test_unlicensed_dataset_is_served_with_its_licence_reported_as_absent(self):
        """As /atlas/place/ and the LPF entity answer: the record is served, and
        the attribution says no licence is recorded (never the WHG overlay)."""
        ds, place, pid = make_full_record(self.owner, label="pc_nolic")
        ds.license = None
        ds.save(update_fields=["license"])
        a = self._get(pid, **CSRF).json()["attribution"]
        self.assertEqual(a["name"], "Persian towns")
        self.assertIsNone(a["license__spdx_id"])

    def test_dates_fall_back_to_the_when_rows(self):
        self.place.timespans = None
        self.place.save(update_fields=["timespans"])
        self.assertEqual(self._get(self.pid, **CSRF).json()["timespans"], [{"start": 1050, "end": 1722}])

    def test_lpf_answer_is_unchanged(self):
        """Without the variant: the LPF Feature, exactly what the LPF builder
        makes of the serializer output; different from the popup."""
        from api.serializers_api import PlaceFeatureSerializer
        from api.views_entity import _legacy_place_to_lpf
        from django.test import RequestFactory
        from places.models import Place
        resp = self._get(self.pid, popup=False, **CSRF)
        self.assertEqual(resp.status_code, 200)
        req = RequestFactory().get(f"/entity/place:{self.pid}/api")
        expected = _legacy_place_to_lpf(PlaceFeatureSerializer(Place.objects.get(pk=self.place.pk),
                                                               context={"request": req}).data, request=req)
        self.assertEqual(resp.json(), json.loads(json.dumps(expected)))
        self.assertEqual(resp.json()["properties"]["title"], "Isfahan")
        for key in ("place_id", "attribution", "record_url"):
            self.assertNotIn(key, resp.json())
        self.assertNotEqual(resp.content, self._get(self.pid, **CSRF).content)

    def test_beta_viewer_also_gets_record_url(self):
        self.client.force_login(self.beta)
        data = self._get(self.pid, **CSRF).json()
        self.assertEqual(data.pop("record_url"), f"/atlas/place/{self.place.pk}/")
        self.client.logout()
        self.assertEqual(data, self._get(self.pid, **CSRF).json())

    def test_authority_popup_is_the_raw_gateway_record(self):
        """The authority path is untouched: the gateway's dict, plus the
        registry attribution when there is one, for any viewer."""
        from api.models import GazetteerRegistryEntry
        GazetteerRegistryEntry.objects.update_or_create(id="clio", defaults=dict(
            name="Cliopatria", namespace="clio", entry_class="authority", status="published",
            h3_coverage=[], temporal_extent=[], core=False, no_explore=False, gazetteer_type="standard"))
        from api.attribution import registry_attribution
        anon = self._get(CLIO["place_id"], **CSRF)
        self.assertEqual(anon.status_code, 200)
        self.assertEqual(anon.json(), json.loads(json.dumps(dict(CLIO, attribution=registry_attribution("clio")))))
        self.client.force_login(self.beta)
        self.assertEqual(self._get(CLIO["place_id"], **CSRF).content, anon.content)
