"""Locus registry, migration 0028 (place#325 rulings 2026-10-21): redaction,
opt-in minter name, one-way withdrawal. Run locally only:

    DB_NAME=whgv3beta_f2 GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test places.tests_locus_redaction --settings=whg.settings_localtest

Each forbidden UPDATE is sent as RAW SQL (so neither the model nor the ORM can
be what refused it) and is paired with the permitted UPDATE of the same column
on the same row, so a trigger that refused every UPDATE would fail here too.

Proved able to fail (2026-10-21): with the trigger's ``status`` arm removed,
``test_withdrawal_is_one_way_at_the_database`` fails; with the
``minter_public`` arm removed, ``test_minter_public_only_retracts_at_the_database``
fails; with the ``redacted_at`` arm removed, ``test_redaction_is_write_once_at_the_database``
fails.
"""
import copy

from django.contrib.auth import get_user_model
from django.db import DatabaseError, connection, transaction
from django.test import TestCase

from places import locus_registry as registry
from places.models_locus import LocusRegistryEntry, RegistryImmutable

CLUSTERING = {"weights": {"name": 0.35, "spatial": 0.2, "temporal": 0.15, "type": 0.15, "link": 0.15},
              "theta": 0.65, "tau_name": 0.75, "tau_link": 1, "same_ns_penalty": 0.15, "respect_distinct": True}


def recipe(query="Berlin", members=("gn:1", "wd:Q64")):
    return {"v": 1, "retrieval": {"query": query, "mode": "in", "size": 100},
            "clustering": copy.deepcopy(CLUSTERING), "members": list(members)}


def make_user(username, role="beta_tester"):
    return get_user_model().objects.create_user(
        username=username, email=f"{username}@example.org", password="pw",
        given_name="A", surname="B", role=role)


def raw(sql, params):
    with transaction.atomic(), connection.cursor() as cur:
        cur.execute(sql, params)
        return cur.rowcount


class TriggerTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.user = make_user("redact_user")
        cls.staff = make_user("redact_staff")

    def mint(self, q="Berlin", public=False):
        e, created = registry.mint_recipe(recipe(q), self.user, minter_public=public)
        self.assertTrue(created)
        return e

    def refused(self, sql, params, msg=""):
        with self.assertRaises(DatabaseError, msg=msg):
            raw(sql, params)

    def test_withdrawal_is_one_way_at_the_database(self):
        e = self.mint()
        # active -> withdrawn is the permitted change...
        self.assertEqual(raw("UPDATE locus_registry SET status='withdrawn', withdrawn_at=now(), "
                             "withdrawn_reason='r' WHERE pid=%s", [e.pid]), 1)
        # ...withdrawn -> active is not, nor any change to the withdrawal record.
        self.refused("UPDATE locus_registry SET status='active' WHERE pid=%s", [e.pid], "reinstate")
        self.refused("UPDATE locus_registry SET withdrawn_reason='other' WHERE pid=%s", [e.pid], "reason")
        self.refused("UPDATE locus_registry SET withdrawn_at=now() - interval '1 day' WHERE pid=%s",
                     [e.pid], "withdrawn_at")
        self.refused("UPDATE locus_registry SET withdrawn_by_id=%s WHERE pid=%s", [self.staff.pk, e.pid], "by")
        row = LocusRegistryEntry.objects.get(pk=e.pk)
        self.assertEqual((row.status, row.withdrawn_reason), ("withdrawn", "r"))
        # An ACTIVE row's status may still be written (to itself): the trigger discriminates.
        e2 = self.mint("Paris")
        self.assertEqual(raw("UPDATE locus_registry SET status='active' WHERE pid=%s", [e2.pid]), 1)

    def test_redaction_is_write_once_at_the_database(self):
        e = self.mint()
        self.assertEqual(raw("UPDATE locus_registry SET redacted_at=now(), redacted_by_id=%s, "
                             "redaction_reason='personal data' WHERE pid=%s", [self.staff.pk, e.pid]), 1)
        self.refused("UPDATE locus_registry SET redacted_at=NULL WHERE pid=%s", [e.pid], "un-redact")
        self.refused("UPDATE locus_registry SET redacted_at=now() - interval '1 day' WHERE pid=%s",
                     [e.pid], "re-date")
        self.refused("UPDATE locus_registry SET redaction_reason='x' WHERE pid=%s", [e.pid], "reason")
        self.refused("UPDATE locus_registry SET redacted_by_id=%s WHERE pid=%s", [self.user.pk, e.pid], "by")
        # redacted_by may go NULL (the staff account is deleted: ON DELETE SET NULL).
        self.assertEqual(raw("UPDATE locus_registry SET redacted_by_id=NULL WHERE pid=%s", [e.pid]), 1)
        # Redaction never touches the binding: the canonical bytes are still there.
        row = LocusRegistryEntry.objects.get(pk=e.pk)
        self.assertEqual(row.canonical_bytes, registry.recipe_binding(recipe())["canonical"])
        self.assertTrue(row.is_redacted)
        # Redacting an active row does not change its status; it may still be withdrawn.
        self.assertEqual(row.status, "active")
        self.assertEqual(raw("UPDATE locus_registry SET status='withdrawn', withdrawn_at=now(), "
                             "withdrawn_reason='r' WHERE pid=%s", [e.pid]), 1)

    def test_minter_public_only_retracts_at_the_database(self):
        shy = self.mint("Shy", public=False)
        self.refused("UPDATE locus_registry SET minter_public=true WHERE pid=%s", [shy.pid], "publish later")
        named = self.mint("Named", public=True)
        self.assertEqual(raw("UPDATE locus_registry SET minter_public=false WHERE pid=%s", [named.pid]), 1)
        self.refused("UPDATE locus_registry SET minter_public=true WHERE pid=%s", [named.pid], "re-publish")
        self.assertFalse(LocusRegistryEntry.objects.get(pk=named.pk).minter_public)

    def test_binding_columns_still_immutable_and_delete_refused(self):
        e = self.mint()
        for col, val in (("canonical", b"{}"), ("token", "t"), ("digest", "f" * 64), ("kind", "legacy"),
                         ("version", 3), ("minted_via", "dereference")):
            self.refused(f"UPDATE locus_registry SET {col}=%s WHERE pid=%s", [val, e.pid], col)
        self.refused("DELETE FROM locus_registry WHERE pid=%s", [e.pid], "delete")
        self.assertEqual(LocusRegistryEntry.objects.filter(pk=e.pk).count(), 1)


class ModelGuardTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.user = make_user("guard_user")
        cls.staff = make_user("guard_staff")

    def test_minter_public_is_written_at_mint_time_default_off(self):
        off, _ = registry.mint_recipe(recipe("A"), self.user)
        on, _ = registry.mint_recipe(recipe("B"), self.user, minter_public=True)
        self.assertFalse(LocusRegistryEntry.objects.get(pk=off.pk).minter_public)
        self.assertTrue(LocusRegistryEntry.objects.get(pk=on.pk).minter_public)
        # An idempotent re-mint never changes the first minter's choice, either way.
        again, created = registry.mint_recipe(recipe("A"), self.user, minter_public=True)
        self.assertFalse(created)
        self.assertFalse(LocusRegistryEntry.objects.get(pk=off.pk).minter_public)

    def test_withdraw_is_one_way_in_python(self):
        e, _ = registry.mint_recipe(recipe(), self.user)
        e.withdraw("gone", by=self.staff)
        w = LocusRegistryEntry.objects.get(pk=e.pk)
        w.status = LocusRegistryEntry.STATUS_ACTIVE
        with self.assertRaises(RegistryImmutable):
            w.save()
        w = LocusRegistryEntry.objects.get(pk=e.pk)
        w.withdrawn_reason = "changed"
        with self.assertRaises(RegistryImmutable):
            w.save()
        # A second withdraw() is a no-op that keeps the first reason.
        LocusRegistryEntry.objects.get(pk=e.pk).withdraw("second", by=self.staff)
        self.assertEqual(LocusRegistryEntry.objects.get(pk=e.pk).withdrawn_reason, "gone")

    def test_redact_records_who_and_when_and_is_write_once(self):
        e, _ = registry.mint_recipe(recipe(), self.user)
        self.assertFalse(e.is_redacted)
        with self.assertRaises(ValueError):
            e.redact("", by=self.staff)
        e.redact("contained a person's name", by=self.staff)
        r = LocusRegistryEntry.objects.get(pk=e.pk)
        self.assertTrue(r.is_redacted)
        self.assertTrue(r.is_suppressed)
        self.assertEqual((r.redacted_by, r.redaction_reason), (self.staff, "contained a person's name"))
        self.assertEqual(r.status, "active")
        r.redaction_reason = "other"
        with self.assertRaises(RegistryImmutable):
            r.save()
        # redact() again keeps the first redaction.
        LocusRegistryEntry.objects.get(pk=e.pk).redact("again", by=self.user)
        self.assertEqual(LocusRegistryEntry.objects.get(pk=e.pk).redacted_by, self.staff)

    def test_minter_public_cannot_be_turned_on_in_python(self):
        e, _ = registry.mint_recipe(recipe(), self.user)
        e = LocusRegistryEntry.objects.get(pk=e.pk)
        e.minter_public = True
        with self.assertRaises(RegistryImmutable):
            e.save()
        on, _ = registry.mint_recipe(recipe("On"), self.user, minter_public=True)
        on = LocusRegistryEntry.objects.get(pk=on.pk)
        on.minter_public = False
        on.save()
        self.assertFalse(LocusRegistryEntry.objects.get(pk=on.pk).minter_public)


class AdminTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.user = make_user("admin_minter")
        cls.admin = get_user_model().objects.create_superuser(
            username="locus_admin", email="la@example.org", password="pw", given_name="L", surname="A")

    def test_redact_action_and_redacted_rows_hide_their_bytes_in_the_admin(self):
        e, _ = registry.mint_recipe(recipe("Adminville"), self.user)
        keep, _ = registry.mint_recipe(recipe("Keeptown"), self.user)
        self.client.force_login(self.admin)
        change = f"/admin/places/locusregistryentry/{{}}/change/"
        self.assertContains(self.client.get(change.format(e.pk)), e.token)      # presence, before
        resp = self.client.post("/admin/places/locusregistryentry/",
                                {"action": "redact_loci", "_selected_action": [e.pk], "apply": "1",
                                 "reason": "contains a home address"})
        self.assertEqual(resp.status_code, 302)
        row = LocusRegistryEntry.objects.get(pk=e.pk)
        self.assertTrue(row.is_redacted)
        self.assertEqual(row.redacted_by, self.admin)
        page = self.client.get(change.format(e.pk))
        self.assertEqual(page.status_code, 200)
        self.assertNotContains(page, e.token)
        self.assertNotContains(page, "gn:1")
        self.assertContains(page, e.pk)
        self.assertContains(self.client.get(change.format(keep.pk)), keep.token)   # unredacted row still shows
        self.assertFalse(LocusRegistryEntry.objects.get(pk=keep.pk).is_redacted)
        # Withdraw via the admin, then the status field is not editable: there is no way back.
        self.client.post("/admin/places/locusregistryentry/", {"action": "withdraw_loci", "_selected_action": [e.pk],
                                                              "apply": "1", "reason": "minter asked"})
        self.assertEqual(LocusRegistryEntry.objects.get(pk=e.pk).status, "withdrawn")
        self.assertNotContains(self.client.get(change.format(e.pk)), 'name="status"')


class AdminReasonTests(TestCase):
    """Review #4: Withdraw and Redact ask for a reason on a confirmation form (the
    withdrawal reason is public, the redaction reason staff-only, and the form says
    so); the actions need the change permission; a WITHDRAWN row hides its recipe
    text in the admin too, as a redacted one does."""
    URL = "/admin/places/locusregistryentry/"

    @classmethod
    def setUpTestData(cls):
        from django.contrib.auth.models import Permission
        cls.user = make_user("reason_minter")
        cls.admin = get_user_model().objects.create_superuser(
            username="reason_admin", email="ra@example.org", password="pw", given_name="R", surname="A")
        cls.viewer = get_user_model().objects.create_user(
            username="reason_viewer", email="rv@example.org", password="pw", given_name="R", surname="V",
            is_staff=True)
        cls.viewer.user_permissions.add(Permission.objects.get(codename="view_locusregistryentry"))

    def setUp(self):
        self.e, _ = registry.mint_recipe(recipe("Reasonville"), self.user)
        self.client.force_login(self.admin)

    def act(self, action, **extra):
        return self.client.post(self.URL, {"action": action, "_selected_action": [self.e.pk], **extra})

    def test_withdraw_asks_for_a_public_reason(self):
        form = self.act("withdraw_loci")
        self.assertEqual(form.status_code, 200)
        self.assertContains(form, 'name="reason"')
        self.assertContains(form, "shown publicly")
        self.assertEqual(LocusRegistryEntry.objects.get(pk=self.e.pk).status, "active")   # nothing yet
        again = self.act("withdraw_loci", apply="1", reason="   ")
        self.assertContains(again, "A reason is required")
        self.assertEqual(LocusRegistryEntry.objects.get(pk=self.e.pk).status, "active")
        done = self.act("withdraw_loci", apply="1", reason="The minter asked for it to be withdrawn.")
        self.assertEqual(done.status_code, 302)
        row = LocusRegistryEntry.objects.get(pk=self.e.pk)
        self.assertEqual((row.status, row.withdrawn_reason, row.withdrawn_by),
                         ("withdrawn", "The minter asked for it to be withdrawn.", self.admin))

    def test_redact_asks_for_a_staff_only_reason(self):
        form = self.act("redact_loci")
        self.assertContains(form, 'name="reason"')
        self.assertContains(form, "staff only")
        self.assertFalse(LocusRegistryEntry.objects.get(pk=self.e.pk).is_redacted)
        self.act("redact_loci", apply="1", reason="personal data in the query")
        row = LocusRegistryEntry.objects.get(pk=self.e.pk)
        self.assertEqual(row.redaction_reason, "personal data in the query")

    def test_actions_need_the_change_permission(self):
        self.client.force_login(self.viewer)
        page = self.client.get(self.URL)
        self.assertEqual(page.status_code, 200)
        for action in ("withdraw_loci", "redact_loci", "hide_minter_loci"):
            self.assertNotContains(page, f'value="{action}"')
            self.act(action, apply="1", reason="x")
        row = LocusRegistryEntry.objects.get(pk=self.e.pk)
        self.assertEqual((row.status, row.is_redacted), ("active", False))
        self.client.force_login(self.admin)                       # presence: the superuser has them
        self.assertContains(self.client.get(self.URL), 'value="withdraw_loci"')

    def test_withdrawn_rows_hide_their_recipe_text_in_the_admin(self):
        change = f"{self.URL}{self.e.pk}/change/"
        self.assertContains(self.client.get(change), self.e.token)
        self.e.withdraw("gone (test)", by=self.admin)
        page = self.client.get(change)
        self.assertEqual(page.status_code, 200)
        self.assertNotContains(page, self.e.token)
        self.assertContains(page, self.e.pk)
