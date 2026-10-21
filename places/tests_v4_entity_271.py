"""place#271 / place#325 Stage 4: anonymous resolution of contributed place
ids on ``/entity/place:<pid>/``, behind V4_LANDING_ENABLED.

Run locally (never in a prod container):

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test places.tests_v4_entity_271 --settings=whg.settings_localtest

Off: every contributed id is 401 for an anonymous caller, as before. On: a
public, non-embargoed record resolves (302 to the Record page); private,
embargoed and missing records are the same 404; ``/api`` stays 401. Each
absence sits beside a presence on the same route.
"""
from places.tests_v4_landing import OFF, ON, _Fixture


@OFF
class EntitySwitchOffTests(_Fixture):

    def test_entity_contributed_is_401_anonymously_and_portal_for_users(self):
        for pk in (self.Pub.pk, self.Priv.pk, self.missing):
            self.assertEqual(self.anon(f"/entity/place:{pk}/").status_code, 401, pk)
        r = self.as_user(self.plain, f"/entity/place:{self.Pub.pk}/")
        self.assertEqual((r.status_code, r["Location"]), (302, f"/places/portal/{self.Pub.pk}/"))


@ON
class EntitySwitchOnTests(_Fixture):

    def test_entity_resolves_public_contributed_records_anonymously(self):
        target = f"/atlas/place/{self.Pub.pk}/"
        for ident in (str(self.Pub.pk), f"whg:{self.Pub.pk}", f"whg:{self.ds_pub.pk}:P1"):
            r = self.anon(f"/entity/place:{ident}/")
            self.assertEqual((r.status_code, r["Location"]), (302, target), ident)
        r = self.anon(f"/entity/place:{self.Pub.pk}/", HTTP_ACCEPT="application/ld+json")
        self.assertEqual((r.status_code, r["Location"]), (302, target))

    def test_entity_withholds_private_embargoed_missing_alike(self):
        bodies = set()
        for ident in (str(self.Priv.pk), f"whg:{self.ds_priv.pk}:S1", str(self.Emb.pk),
                      f"whg:{self.ds_emb.pk}:E1", str(self.missing)):
            r = self.anon(f"/entity/place:{ident}/", HTTP_ACCEPT="application/json")
            self.assertEqual(r.status_code, 404, ident)
            bodies.add(r.content.replace(ident.encode(), b"<id>"))
        self.assertEqual(len(bodies), 1, bodies)  # private and missing are indistinguishable
        # Presence on the same route: the owner still resolves their private record.
        r = self.as_user(self.owner, f"/entity/place:{self.Priv.pk}/")
        self.assertEqual((r.status_code, r["Location"]), (302, f"/atlas/place/{self.Priv.pk}/"))

    def test_entity_api_stays_closed_to_anonymous_contributed(self):
        r = self.anon(f"/entity/place:{self.Pub.pk}/api")
        self.assertEqual(r.status_code, 401)
        r = self.as_user(self.plain, f"/entity/place:{self.Pub.pk}/api")
        self.assertEqual(r.status_code, 200)
        self.assertIn("Public Town", r.content.decode())

    def test_retired_portal_chain_ends_at_the_record_or_404(self):
        r = self.client.get(f"/places/portal/{self.Pub.pk}/", follow=True)
        self.assertEqual(r.redirect_chain, [(f"/entity/place:{self.Pub.pk}/", 301),
                                            (f"/atlas/place/{self.Pub.pk}/", 302)])
        self.assertContains(r, "Public Town")
        r = self.client.get(f"/places/portal/{self.Priv.pk}/", follow=True)
        self.assertEqual(r.status_code, 404)
        self.assertNotIn("Secret Town", r.content.decode())
