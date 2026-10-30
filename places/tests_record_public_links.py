"""The "Links made by people" section of the public Record page, and the Phase 6 gates
(place#343; place#334 ruling Q7: labelled and disputable).

Every absence asserted here is asserted beside a presence in the same page or call, so a
section that failed to render, or a filter that hid everything, cannot pass for a filter
that works.

    GDAL_LIBRARY_PATH=... venv/bin/python manage.py test places.tests_record_public_links \
        --settings=whg.settings_localtest
"""
import re
from datetime import timedelta
from unittest.mock import patch

from django.conf import settings
from django.contrib.auth import get_user_model
from django.db import connection
from django.test import TestCase, override_settings
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from api.models import ContributorAttestation

LOCMEM = {**settings.CACHES,
          "default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache", "LOCATION": "rpl343"}}
OFF = override_settings(V4_LANDING_ENABLED=False, CACHES=LOCMEM)
ON = override_settings(V4_LANDING_ENABLED=True, CACHES=LOCMEM)
SECTION = re.compile(r'<section id="record_public_links".*?</section>', re.S)


class _Fixture(TestCase):
    @classmethod
    def setUpTestData(cls):
        from api.models import GazetteerRegistryEntry
        from datasets.models import Dataset
        from places.models import Place

        User = get_user_model()
        cls.owner = User.objects.create_user(username="rpl_owner", email="rpl_owner@example.org", password="pw",
                                             given_name="Olive", surname="Owner")
        cls.stranger = User.objects.create_user(username="rpl_str", email="rpl_str@example.org", password="pw",
                                                given_name="Sam", surname="Stranger",
                                                orcid="https://orcid.org/0000-0002-1825-0097")
        cls.plain = User.objects.create_user(username="rpl_plain", email="rpl_plain@example.org", password="pw",
                                             given_name="Pat", surname="Plain")
        cls.beta = User.objects.create_user(username="rpl_beta", email="rpl_beta@example.org", password="pw",
                                            given_name="Bea", surname="Beta", role="beta_tester")

        def ds(key, public=True, owner=None):
            return Dataset.objects.create(owner=owner or cls.owner, label=f"rpl343_{key}",
                                          title=f"Dataset {key} title", description="D", creator="X",
                                          public=public, ds_status="indexed",
                                          uri_base="https://example.org/places/")

        with patch("datasets.signals.doi"):
            cls.ds_a = ds("a")
            cls.ds_b = ds("b", owner=cls.stranger)
            cls.ds_priv = ds("secretive", public=False, owner=cls.stranger)
            cls.ds_emb = ds("embargoed", owner=cls.stranger)
        GazetteerRegistryEntry.objects.create(id=f"whg:{cls.ds_emb.pk}", name="Emb", namespace="whg",
                                              entry_class="dataset", status="embargoed")
        GazetteerRegistryEntry.objects.update_or_create(
            id="wd", defaults=dict(name="wd", namespace="wd", entry_class="authority", status="published"))
        GazetteerRegistryEntry.objects.update_or_create(
            id="tm", defaults=dict(name="tm", namespace="tm", entry_class="authority", status="embargoed",
                                   embargo_release_at=timezone.now() + timedelta(days=30)))

        def place(d, title, src):
            return Place.objects.create(title=title, src_id=src, dataset=d, ccodes=[])

        cls.A = place(cls.ds_a, "Alphaville", "a1")
        cls.B = place(cls.ds_b, "Bravotown", "b1")
        cls.C = place(cls.ds_b, "Charlieford", "c1")
        cls.D = place(cls.ds_b, "Deltaport", "d1")
        cls.E = place(cls.ds_b, "Echoham", "e1")
        cls.F = place(cls.ds_b, "Foxtrotbury", "f1")
        cls.P = place(cls.ds_priv, "Privatestead", "p1")
        cls.M = place(cls.ds_emb, "Embargoburgh", "m1")

    def setUp(self):
        from django.core.cache import cache
        cache.clear()
        for target in ("api.signals.forward_attestation_post", "api.signals.forward_attestation_revoke"):
            p = patch(target)
            p.start()
            self.addCleanup(p.stop)
        p = patch("api.crc_client.crc_clustering_params", return_value=(None, "disabled"))
        p.start()
        self.addCleanup(p.stop)

    @staticmethod
    def gid(place):
        return f"whg:{place.dataset.pk}:{place.src_id}"

    def link(self, user, a, b, relation="closeMatch", status="active", legacy=False):
        """A row as the rules would store it (canonical order, pks kept). ``b`` may be an
        authority id string."""
        sides = [(self.gid(a), a.id), (b, None) if isinstance(b, str) else (self.gid(b), b.id)]
        sides.sort()
        return ContributorAttestation.objects.create(
            user=user, dataset_id=f"whg:{a.dataset.pk}", place_a=sides[0][0], place_b=sides[1][0],
            place_a_pk=sides[0][1], place_b_pk=sides[1][1], relation_type=relation, status=status,
            legacy_v3_2=legacy, suggested_by="import" if legacy else "manual")

    def page(self, user, place=None):
        self.client.logout()
        if user is not None:
            self.client.force_login(user)
        r = self.client.get(f"/atlas/place/{(place or self.A).pk}/")
        self.client.logout()
        return r

    def section(self, r):
        self.assertEqual(r.status_code, 200)
        m = SECTION.search(r.content.decode())
        self.assertIsNotNone(m, "the Links section did not render")
        return m.group(0)


@ON
class PublicLinksSectionTests(_Fixture):

    def test_anonymous_sees_an_active_link_labelled_with_licence_and_source(self):
        self.link(self.stranger, self.A, self.B)
        self.link(self.stranger, self.A, self.C, legacy=True)
        s = self.section(self.page(None))
        self.assertIn('data-total="2"', s)
        self.assertIn("Bravotown", s)
        self.assertIn("Dataset b title", s)
        self.assertIn("Same place", s)
        self.assertIn("CC BY 4.0", s)
        self.assertIn("Made in WHG", s)
        self.assertIn("Carried over from a reviewed WHG v3 match", s)
        self.assertIn("Linked by a WHG contributor", s)
        # Not named to the public (no opt-in exists): neither name nor ORCID.
        self.assertNotIn("Sam Stranger", s)
        self.assertNotIn("0000-0002-1825-0097", s)
        # Anonymous: no dispute control, a sign-in prompt instead.
        self.assertNotIn("whg-link-btn", s)
        self.assertIn("to dispute a Link", s)
        # The vocabulary ruling (Q9): none of the internal words on screen.
        for word in ("attest", "accession", "reconcil"):
            self.assertNotIn(word, s.lower(), word)

    def test_only_active_links_appear(self):
        self.link(self.stranger, self.A, self.B)                         # control: shown
        self.link(self.stranger, self.A, self.C, status="pending")
        self.link(self.stranger, self.A, self.D, status="withdrawn")
        self.link(self.stranger, self.A, self.E, status="rejected")
        self.link(self.stranger, self.A, self.F, status="superseded")
        for viewer in (None, self.plain, self.stranger, self.beta):
            s = self.section(self.page(viewer))
            self.assertIn("Bravotown", s, viewer)
            self.assertIn('data-total="1"', s, viewer)
            for title in ("Charlieford", "Deltaport", "Echoham", "Foxtrotbury"):
                self.assertNotIn(title, s, (viewer, title))

    def test_active_link_to_a_private_or_embargoed_record_leaks_nothing(self):
        """Active rows should never name a private record, but if one does (a dataset made
        private by a route that forgot the Links), nothing of the other side reaches the page:
        not its title, its dataset's name, its pk form or its gateway id."""
        self.link(self.stranger, self.A, self.B)                         # control: shown
        self.link(self.stranger, self.A, self.P)
        self.link(self.stranger, self.A, self.M)
        for viewer in (None, self.plain, self.beta):
            r = self.page(viewer)
            s = self.section(r)
            self.assertIn("Bravotown", s)
            self.assertIn('data-total="1"', s)
            body = r.content.decode()
            for leaked in ("Privatestead", "Dataset secretive title", f"whg:{self.P.pk}\"", self.gid(self.P),
                           "Embargoburgh", "Dataset embargoed title", self.gid(self.M)):
                self.assertNotIn(leaked, body, (viewer, leaked))

    def test_the_private_records_owner_sees_no_public_link_to_it_either(self):
        """The section is the anonymous view for everyone: the stranger owns ds_priv, yet a
        (forced) active row to P is not shown to them here."""
        self.link(self.stranger, self.A, self.B)
        self.link(self.stranger, self.A, self.P)
        s = self.section(self.page(self.stranger))
        self.assertIn("Bravotown", s)
        self.assertNotIn("Privatestead", s)

    def test_a_private_record_page_has_no_public_links(self):
        """The owner may open their private record, but no Link is listed on it."""
        self.link(self.stranger, self.B, self.C)                         # control on a public record
        self.assertIn("Charlieford", self.section(self.page(None, self.B)))
        self.link(self.stranger, self.P, self.B)
        s = self.section(self.page(self.stranger, self.P))
        self.assertIn('data-total="0"', s)
        self.assertNotIn("Bravotown", s)

    def test_authority_side_shown_only_when_its_namespace_is_public(self):
        self.link(self.stranger, self.A, "wd:Q90")
        self.link(self.stranger, self.A, "tm:4242")
        s = self.section(self.page(None))
        self.assertIn("wd:Q90", s)
        self.assertNotIn("tm:4242", s)
        # the beta rule would show tm (embargo visible to beta) but this section is the public view
        self.assertNotIn("tm:4242", self.section(self.page(self.beta)))

    def test_signed_in_user_gets_dispute_preset_to_the_opposite_answer(self):
        self.link(self.stranger, self.A, self.B)
        self.link(self.stranger, self.A, self.C, relation="distinct")
        r = self.page(self.plain)
        body = r.content.decode()
        self.assertIn('<meta name="whg-link-enabled" content="1">', body)
        s = self.section(r)
        buttons = re.findall(r'<button[^>]*record-link-dispute[^>]*>', s, re.S)
        self.assertEqual(len(buttons), 2, s)
        by_target = {re.search(r'data-link-target="([^"]+)"', b).group(1):
                     re.search(r'data-link-relation="([^"]+)"', b).group(1) for b in buttons}
        self.assertEqual(by_target, {f"whg:{self.B.pk}": "distinct", f"whg:{self.C.pk}": "closeMatch"})
        self.assertTrue(all(f'data-link-subject="whg:{self.A.pk}"' in b for b in buttons))
        self.assertIn("Not the same place", s)

    def test_own_link_offers_change_not_dispute_and_says_you(self):
        self.link(self.plain, self.A, self.B)
        self.link(self.stranger, self.A, self.C)
        s = self.section(self.page(self.plain))
        self.assertIn("Linked by you", s)
        self.assertEqual(s.count("record-link-change"), 1)
        self.assertEqual(s.count("record-link-dispute"), 1)

    def test_beta_users_still_see_the_makers_name(self):
        self.link(self.stranger, self.A, self.B)
        self.assertIn("Sam Stranger", self.section(self.page(self.beta)))
        self.assertNotIn("Sam Stranger", self.section(self.page(self.plain)))

    def test_cost_does_not_grow_with_the_number_of_links(self):
        from places.record_links import record_links
        self.link(self.stranger, self.A, self.B)
        self.link(self.stranger, self.A, "wd:Q90")
        with CaptureQueriesContext(connection) as one:
            self.assertEqual(len(record_links(self.A, self.plain)), 2)
        for p in (self.C, self.D, self.E, self.F):
            self.link(self.stranger, self.A, p)
        self.link(self.beta, self.A, "wd:Q91")
        with CaptureQueriesContext(connection) as many:
            self.assertEqual(len(record_links(self.A, self.plain)), 7)
        self.assertEqual(len(one), len(many), [q["sql"][:120] for q in many])
        self.assertLessEqual(len(many), 4)


@OFF
class SwitchOffTests(_Fixture):
    """Dark with the switch off: a beta user (who can open the Record page today) sees no
    Links section, and a signed-in non-beta user is offered no Link sheet."""

    def test_no_section_with_the_switch_off(self):
        self.link(self.stranger, self.A, self.B)
        r = self.page(self.beta)
        self.assertEqual(r.status_code, 200)
        body = r.content.decode()
        self.assertIn("Alphaville", body)                                 # the page rendered
        self.assertNotIn('id="record_public_links"', body)
        with ON:                                                          # control
            self.assertIn("Bravotown", self.section(self.page(self.beta)))

    def test_link_sheet_meta_only_for_beta_with_the_switch_off(self):
        meta = '<meta name="whg-link-enabled" content="1">'
        self.client.force_login(self.beta)
        self.assertIn(meta, self.client.get("/").content.decode())
        self.client.force_login(self.plain)
        self.assertNotIn(meta, self.client.get("/").content.decode())
        with ON:
            self.assertIn(meta, self.client.get("/").content.decode())
        self.client.logout()
        with ON:
            self.assertNotIn(meta, self.client.get("/").content.decode())


class FeedGateTests(_Fixture):
    """"Feed for owners": with the switch on, a dataset's owner reaches its feed without beta
    access; nobody outside the circle does, switch or not."""

    def get_feed(self, user, ds):
        self.client.force_login(user)
        try:
            return self.client.get(f"/datasets/{ds.pk}/matches/progress/")
        finally:
            self.client.logout()

    def test_owner_without_beta(self):
        with OFF:
            self.assertEqual(self.get_feed(self.owner, self.ds_a).status_code, 404)
            self.assertEqual(self.get_feed(self.beta, self.ds_a).status_code, 404)  # beta, not in circle
        with ON:
            self.assertEqual(self.get_feed(self.owner, self.ds_a).status_code, 200)
            self.assertEqual(self.get_feed(self.plain, self.ds_a).status_code, 404)
            self.client.logout()
            self.assertNotEqual(self.client.get(f"/datasets/{self.ds_a.pk}/matches/progress/").status_code, 200)

    def test_progress_card_follows_the_same_gate(self):
        from django.template import Context, Template
        from django.test import RequestFactory
        tpl = Template("{% load match_feed_tags %}{% links_progress ds %}")
        req = RequestFactory().get("/")
        req.user = self.owner
        with OFF:
            self.assertNotIn("ds_links_progress", tpl.render(Context({"request": req, "ds": self.ds_a})))
        with ON:
            self.assertIn("ds_links_progress", tpl.render(Context({"request": req, "ds": self.ds_a})))
            req.user = self.plain
            self.assertNotIn("ds_links_progress", tpl.render(Context({"request": req, "ds": self.ds_a})))


@ON
class MakerNameElsewhereTests(_Fixture):
    """The same naming rule in the Concordance JSON and GET /api/links?about=."""

    def test_concordance_names_the_maker_to_beta_only(self):
        self.link(self.stranger, self.A, self.B)
        url = f"/atlas/place/{self.A.pk}/concordance/"
        with patch("api.crc_client.crc_fetch_places", return_value={}):
            def names(user):
                self.client.logout()
                if user:
                    self.client.force_login(user)
                data = self.client.get(url).json()
                return [e["attestation"]["attested_by"]["name"]
                        for k in ("outbound", "inbound") for e in data[k] if e.get("source") == "attestation"]
            self.assertEqual(names(self.beta), ["Sam Stranger"])            # control
            self.assertEqual(names(None), [None])
            self.assertEqual(names(self.plain), [None])
            # the maker themselves, read from the class-shared cache: not named there either
            # (their own row is marked "mine" and shown as "you" by the page)
            self.assertEqual(names(self.stranger), [None])

    def test_api_about_names_the_maker_to_beta_and_to_themselves(self):
        self.link(self.stranger, self.A, self.B)

        def name(user):
            self.client.force_login(user)
            r = self.client.get("/api/links", {"about": f"whg:{self.A.pk}"})
            self.client.logout()
            (row,) = r.json()["result"]
            return row["attested_by"]["name"]
        self.assertEqual(name(self.beta), "Sam Stranger")
        self.assertEqual(name(self.stranger), "Sam Stranger")
        self.assertIsNone(name(self.plain))


@ON
class MakerNameOptInTests(_Fixture):
    """"Show my name on Links I make" (``User.show_name_on_links``, place#343): an opted-in
    maker is named to everyone on the Record page, in the Concordance JSON and in
    GET /api/links?about=; an opted-out one only to beta users (and themselves where the
    answer is per user). Each test asserts the name present and absent in the same test."""

    def set_opt_in(self, value):
        type(self.stranger).objects.filter(pk=self.stranger.pk).update(show_name_on_links=value)
        self.stranger.refresh_from_db()

    def concordance_names(self, user):
        self.client.logout()
        if user:
            self.client.force_login(user)
        with patch("api.crc_client.crc_fetch_places", return_value={}):
            data = self.client.get(f"/atlas/place/{self.A.pk}/concordance/").json()
        self.client.logout()
        return [e["attestation"]["attested_by"]["name"]
                for k in ("outbound", "inbound") for e in data[k] if e.get("source") == "attestation"]

    def test_record_page_names_an_opted_in_maker_to_everyone(self):
        self.link(self.stranger, self.A, self.B)
        self.link(self.plain, self.A, self.C)                         # Pat Plain did not opt in
        self.set_opt_in(True)
        for viewer in (None, self.plain, self.owner):
            s = self.section(self.page(viewer))
            self.assertIn("Sam Stranger", s, viewer)
            self.assertIn("0000-0002-1825-0097", s, viewer)          # ORCiD, as for beta viewers
        s = self.section(self.page(None))
        self.assertNotIn("Pat Plain", s)
        self.assertIn("a WHG contributor", s)
        self.set_opt_in(False)
        s = self.section(self.page(None))
        self.assertNotIn("Sam Stranger", s)
        self.assertIn("Sam Stranger", self.section(self.page(self.beta)))   # beta: unchanged

    def test_concordance_cache_never_holds_the_opt_in(self):
        """The Concordance is cached per viewer CLASS. The opt-in is applied after the cache,
        so withdrawing it takes effect on the very next request, with the entry still warm."""
        from django.core.cache import cache
        from places.concordance import concordance_cache_key
        self.link(self.stranger, self.A, self.B)
        self.assertEqual(self.concordance_names(None), [None])        # warms the anonymous entry
        self.set_opt_in(True)
        self.assertEqual(self.concordance_names(None), ["Sam Stranger"])          # served from cache
        self.assertEqual(self.concordance_names(self.plain), ["Sam Stranger"])
        cached = cache.get(concordance_cache_key(self.A.pk, None))
        self.assertIsNotNone(cached, "the anonymous entry was not cached: the test proves nothing")
        stored = [e["attestation"]["attested_by"]["name"]
                  for k in ("outbound", "inbound") for e in cached[k] if e.get("source") == "attestation"]
        self.assertEqual(stored, [None])                              # the cache itself holds no name
        self.set_opt_in(False)
        self.assertEqual(self.concordance_names(None), [None])        # same warm entry, name gone
        self.assertEqual(self.concordance_names(self.plain), [None])
        self.assertEqual(self.concordance_names(self.beta), ["Sam Stranger"])     # control

    def test_api_about_names_an_opted_in_maker(self):
        self.link(self.stranger, self.A, self.B)

        def name(user):
            self.client.force_login(user)
            r = self.client.get("/api/links", {"about": f"whg:{self.A.pk}"})
            self.client.logout()
            (row,) = r.json()["result"]
            return row["attested_by"]["name"]
        self.assertIsNone(name(self.plain))
        self.set_opt_in(True)
        self.assertEqual(name(self.plain), "Sam Stranger")
