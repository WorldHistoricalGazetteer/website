"""The locus registry (place#172, place#325 Stage 4): append-only, idempotent,
collision-refusing, concurrency-safe. Run locally only:

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test places.tests_locus_registry --settings=whg.settings_localtest

Every "cannot" below sits beside a "can" on the same object, so no assertion
passes against an object that never existed or a guard that refuses
everything: a binding column cannot change but the status can; a colliding PID
cannot bind but the same PID with the same bytes returns; a user cannot exceed
the limit but can re-mint for free; a bad check digit is 400 but the same PID
spelt three other ways resolves.

Proved able to fail (2026-10-21): with the collision check in
``mint_recipe`` disabled, ``test_pid_bound_to_different_bytes_is_refused``
fails; with the trigger's UPDATE arm removed, the raw-SQL case in
``test_binding_is_immutable_at_the_database`` fails.
"""
import copy
import hashlib
import threading

from django.contrib.auth import get_user_model
from django.contrib.auth.models import AnonymousUser
from django.db import DatabaseError, connection, transaction
from django.test import TestCase, TransactionTestCase
from unittest.mock import patch

from places import locus_recipe as lr
from places import locus_registry as registry
from places.models import LegacyUnionRecord
from places.models_locus import LocusRegistryEntry, RegistryImmutable

CLUSTERING = {"weights": {"name": 0.35, "spatial": 0.2, "temporal": 0.15, "type": 0.15, "link": 0.15},
              "theta": 0.65, "tau_name": 0.75, "tau_link": 1, "same_ns_penalty": 0.15, "respect_distinct": True}


def recipe(query="Berlin", members=("gn:1", "wd:Q64"), **retrieval):
    r = {"query": query, "mode": "in", "size": 100}
    r.update(retrieval)
    return {"v": 1, "retrieval": r, "clustering": copy.deepcopy(CLUSTERING), "members": list(members)}


def make_user(username, role="beta_tester"):
    return get_user_model().objects.create_user(
        username=username, email=f"{username}@example.org", password="pw",
        given_name="A", surname="B", role=role)


class RegistryTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.user = make_user("registry_user")
        cls.other = make_user("registry_other")

    def test_mint_is_idempotent_and_binds_exactly_the_canonical_bytes(self):
        r = recipe()
        e1, created1 = registry.mint_recipe(r, self.user)
        e2, created2 = registry.mint_recipe(copy.deepcopy(r), self.other)   # anyone, same recipe
        self.assertTrue(created1)
        self.assertFalse(created2)
        self.assertEqual(e1.pid, e2.pid)
        self.assertEqual(LocusRegistryEntry.objects.count(), 1)
        self.assertEqual(e1.pid, lr.recipe_pid(r))
        self.assertEqual(e1.canonical_bytes, lr.canonical_bytes(r))
        self.assertEqual(e1.digest, hashlib.sha256(lr.canonical_bytes(r)).hexdigest())
        self.assertEqual(e1.token, lr.encode_recipe_token(r))
        self.assertEqual(e1.members, ["gn:1", "wd:Q64"])
        self.assertEqual(e1.minted_by, self.user)          # the first minter stays the minter
        self.assertEqual(e2.minted_by, self.user)
        self.assertEqual(e1.kind, "recipe")
        self.assertEqual(e1.version, 1)
        self.assertEqual(e1.status, "active")
        # A non-canonical spelling of the same recipe is the same locus.
        same = recipe(query="  Berlin ", members=("wd:Q64", "gn:1", "gn:1"))
        e3, created3 = registry.mint_recipe(same, self.user)
        self.assertFalse(created3)
        self.assertEqual(e3.pid, e1.pid)
        # A different recipe is a different locus.
        e4, created4 = registry.mint_recipe(recipe(query="Berlim"), self.user)
        self.assertTrue(created4)
        self.assertNotEqual(e4.pid, e1.pid)
        self.assertEqual(LocusRegistryEntry.objects.count(), 2)

    def test_pid_bound_to_different_bytes_is_refused(self):
        """The registry is the collision authority for the 54-bit PID."""
        r = recipe()
        pid = lr.recipe_pid(r)
        other_bytes = lr.canonical_bytes(recipe(query="Not Berlin"))
        LocusRegistryEntry.objects.create(
            pid=pid, kind="recipe", version=1, canonical=other_bytes,
            digest=LocusRegistryEntry.digest_of(other_bytes), token="x", members=["gn:9"],
            minted_by=self.other, minted_via="modal")
        with self.assertRaises(registry.LocusCollision) as cm:
            registry.mint_recipe(r, self.user)
        self.assertEqual(cm.exception.pid, pid)
        with self.assertRaises(registry.LocusCollision):
            registry.existing_for_recipe(r)
        self.assertEqual(LocusRegistryEntry.objects.count(), 1)
        self.assertEqual(LocusRegistryEntry.objects.get(pid=pid).canonical_bytes, other_bytes)  # untouched
        # The PID with its OWN bytes is not a collision (the idempotent path is
        # exercised in test_mint_is_idempotent...); and the refused recipe still
        # mints once the PID is free, proving the refusal was about the binding.
        LocusRegistryEntry.objects.filter(pid=pid)   # (cannot be deleted: append-only)
        other_recipe = recipe(query="Berlin", members=("gn:1", "wd:Q64", "tgn:1"))
        e, created = registry.mint_recipe(other_recipe, self.user)
        self.assertTrue(created)
        self.assertNotEqual(e.pid, pid)

    def test_binding_is_immutable_in_python(self):
        e, _ = registry.mint_recipe(recipe(), self.user)
        for field, value in (("canonical", b"{}"), ("digest", "0" * 64), ("token", "zzz"),
                             ("kind", "legacy"), ("version", 2), ("members", ["gn:2"]),
                             ("minted_via", "dereference")):
            fresh = LocusRegistryEntry.objects.get(pk=e.pk)
            setattr(fresh, field, value)
            with self.assertRaises(RegistryImmutable, msg=field):
                fresh.save()
        fresh = LocusRegistryEntry.objects.get(pk=e.pk)
        fresh.minted_by = self.other
        with self.assertRaises(RegistryImmutable):
            fresh.save()
        with self.assertRaises(RegistryImmutable):
            LocusRegistryEntry.objects.get(pk=e.pk).delete()
        self.assertEqual(LocusRegistryEntry.objects.get(pk=e.pk).canonical_bytes, lr.canonical_bytes(recipe()))
        # ...while the status CAN change, through withdraw().
        LocusRegistryEntry.objects.get(pk=e.pk).withdraw("test tombstone", by=self.other)
        w = LocusRegistryEntry.objects.get(pk=e.pk)
        self.assertEqual(w.status, "withdrawn")
        self.assertEqual(w.withdrawn_by, self.other)
        self.assertEqual(w.withdrawn_reason, "test tombstone")
        self.assertIsNotNone(w.withdrawn_at)
        self.assertEqual(w.pid, e.pid)                                 # the PID is not freed
        self.assertEqual(LocusRegistryEntry.objects.count(), 1)
        with self.assertRaises(ValueError):
            w.withdraw("")

    def test_binding_is_immutable_at_the_database(self):
        """The trigger refuses what bypasses the model: QuerySet.update/delete and raw SQL."""
        e, _ = registry.mint_recipe(recipe(), self.user)
        qs = LocusRegistryEntry.objects.filter(pk=e.pk)
        for kwargs in ({"canonical": b"{}"}, {"digest": "f" * 64}, {"token": "t"}, {"members": ["gn:2"]},
                       {"kind": "legacy"}, {"version": 3}, {"minted_via": "dereference"},
                       {"minted_by": self.other}):
            with self.assertRaises(DatabaseError, msg=str(kwargs)):
                with transaction.atomic():
                    qs.update(**kwargs)
        with self.assertRaises(DatabaseError):
            with transaction.atomic():
                qs.delete()
        with self.assertRaises(DatabaseError):
            with transaction.atomic(), connection.cursor() as cur:
                cur.execute("UPDATE locus_registry SET canonical = %s WHERE pid = %s", [b"{}", e.pid])
        with self.assertRaises(DatabaseError):
            with transaction.atomic(), connection.cursor() as cur:
                cur.execute("DELETE FROM locus_registry WHERE pid = %s", [e.pid])
        # The same statements against the STATUS columns go through: the
        # trigger discriminates, it does not refuse every UPDATE.
        with transaction.atomic():
            self.assertEqual(qs.update(status="withdrawn", withdrawn_reason="raw"), 1)
        with transaction.atomic(), connection.cursor() as cur:
            cur.execute("UPDATE locus_registry SET withdrawn_reason = %s WHERE pid = %s", ["raw2", e.pid])
        # minted_by may go to NULL (account deletion) but not to another account.
        with transaction.atomic():
            self.assertEqual(qs.update(minted_by=None), 1)
        row = LocusRegistryEntry.objects.get(pk=e.pk)
        self.assertEqual((row.status, row.withdrawn_reason, row.minted_by), ("withdrawn", "raw2", None))
        self.assertEqual(row.canonical_bytes, lr.canonical_bytes(recipe()))

    def test_deleting_the_minter_keeps_the_row(self):
        u = make_user("registry_gone")
        e, _ = registry.mint_recipe(recipe(query="Gone"), u)
        u.delete()
        row = LocusRegistryEntry.objects.get(pk=e.pk)
        self.assertIsNone(row.minted_by)
        self.assertEqual(row.canonical_bytes, lr.canonical_bytes(recipe(query="Gone")))

    def test_pair_and_acceptance_data_never_enter_the_registry(self):
        """place#306: the parser's forbidden-key rule is the one gate, reused."""
        for key in ("edges", "accepted", "must_link", "merges"):
            bad = recipe()
            bad[key] = [["gn:1", "wd:Q64"]]
            with self.assertRaises(lr.RecipeError, msg=key) as cm:
                registry.mint_recipe(bad, self.user)
            self.assertIn("place#306", str(cm.exception))
        bad = recipe()
        bad["clustering"]["accept"] = True
        with self.assertRaises(lr.RecipeError):
            registry.mint_recipe(bad, self.user)
        self.assertEqual(LocusRegistryEntry.objects.count(), 0)
        registry.mint_recipe(recipe(), self.user)                    # the clean one mints
        self.assertEqual(LocusRegistryEntry.objects.count(), 1)

    def test_minting_needs_a_signed_in_account(self):
        with self.assertRaises(ValueError):
            registry.mint_recipe(recipe(), AnonymousUser())
        with self.assertRaises(ValueError):
            registry.mint_recipe(recipe(), None)
        self.assertEqual(LocusRegistryEntry.objects.count(), 0)

    def test_rate_limit_counts_new_mints_only(self):
        with patch.object(registry, "MINT_LIMIT_PER_HOUR", 2):
            self.assertEqual(registry.mint_allowance(self.user), {"hour": 2, "day": registry.MINT_LIMIT_PER_DAY})
            registry.mint_recipe(recipe(query="One"), self.user)
            registry.mint_recipe(recipe(query="Two"), self.user)
            self.assertEqual(registry.mint_allowance(self.user)["hour"], 0)
            with self.assertRaises(registry.MintRateLimited) as cm:
                registry.mint_recipe(recipe(query="Three"), self.user)
            self.assertEqual(cm.exception.window, "hour")
            self.assertGreaterEqual(cm.exception.retry_after, 1)
            self.assertLessEqual(cm.exception.retry_after, 3601)
            # Re-minting an existing recipe is free; another user is unaffected.
            e, created = registry.mint_recipe(recipe(query="One"), self.user)
            self.assertFalse(created)
            e2, created2 = registry.mint_recipe(recipe(query="Three"), self.other)
            self.assertTrue(created2)
        self.assertEqual(LocusRegistryEntry.objects.count(), 3)
        with patch.object(registry, "MINT_LIMIT_PER_DAY", 1):
            with self.assertRaises(registry.MintRateLimited) as cm:
                registry.mint_recipe(recipe(query="Four"), self.other)
            self.assertEqual(cm.exception.window, "day")

    def test_lookup_recipe_pids(self):
        e, _ = registry.mint_recipe(recipe(), self.user)
        pid = e.pid
        # Four accepted spellings of one PID.
        for spelt in (pid, pid.lower(), pid.replace("-", ""), pid.replace("0", "O").replace("1", "l")):
            got, created = registry.lookup(spelt)
            self.assertEqual(got.pid, pid, spelt)
            self.assertFalse(created)
        # A wrong check digit: a hard 400, never another locus.
        digits = lr.PID_ALPHABET
        last = pid[-1]
        wrong = pid[:-1] + digits[(digits.index(last) + 1) % len(digits)]
        with self.assertRaises(registry.LookupError_) as cm:
            registry.lookup(wrong)
        self.assertEqual(cm.exception.status, 400)
        self.assertIn("check digit", cm.exception.message)
        for junk in ("", "ABC", "ZZZZ-ZZZZ-ZZZZ", "12345678901234"):
            with self.assertRaises(registry.LookupError_) as cm:
                registry.lookup(junk)
            self.assertEqual(cm.exception.status, 400, junk)
        # Well-formed, never minted: 404, and nothing is minted by looking.
        unminted = lr.recipe_pid(recipe(query="Never"))
        with self.assertRaises(registry.LookupError_) as cm:
            registry.lookup(unminted)
        self.assertEqual(cm.exception.status, 404)
        self.assertEqual(LocusRegistryEntry.objects.count(), 1)

    def test_legacy_ids_mint_on_first_dereference_from_the_snapshot(self):
        LegacyUnionRecord.objects.create(whg_id=10000001, place_ids=[5, 7], titles=["Five", "Seven"])
        e, created = registry.lookup("10000001")
        self.assertTrue(created)
        self.assertEqual((e.pid, e.kind, e.version, e.minted_via), ("10000001", "legacy", 0, "dereference"))
        self.assertIsNone(e.minted_by)
        self.assertEqual(e.members, ["whg:5", "whg:7"])
        self.assertEqual(registry.entry_legacy(e), {"kind": "legacy", "place_ids": [5, 7],
                                                    "titles": ["Five", "Seven"], "whg_id": 10000001})
        self.assertIsNone(registry.entry_recipe(e))                  # legacy bytes never parse as a recipe
        e2, created2 = registry.lookup("10000001")
        self.assertFalse(created2)
        self.assertEqual(e2.pid, e.pid)
        self.assertEqual(LocusRegistryEntry.objects.count(), 1)
        with self.assertRaises(registry.LookupError_) as cm:
            registry.lookup("10000002")                              # not in the snapshot
        self.assertEqual(cm.exception.status, 404)
        self.assertEqual(LocusRegistryEntry.objects.count(), 1)
        self.assertFalse(lr.is_legacy_locus_id("20000001"))          # not the legacy shape: a PID check instead
        with self.assertRaises(registry.LookupError_) as cm:
            registry.lookup("20000001")
        self.assertEqual(cm.exception.status, 400)

    def test_overlapping_loci_are_found_by_member(self):
        a, _ = registry.mint_recipe(recipe(query="A", members=("gn:1", "wd:Q64")), self.user)
        b, _ = registry.mint_recipe(recipe(query="B", members=("gn:1", "gn:2", "gn:3")), self.user)
        registry.mint_recipe(recipe(query="C", members=("tgn:1",)), self.user)
        found = registry.overlapping(["gn:1", "gn:2"])
        self.assertEqual([f["pid"] for f in found], [b.pid, a.pid])   # most shared first
        self.assertEqual(found[0]["shared"], ["gn:1", "gn:2"])
        self.assertEqual(found[1]["shared"], ["gn:1"])
        self.assertEqual([f["pid"] for f in registry.overlapping(["gn:1"], exclude_pid=a.pid)], [b.pid])
        self.assertEqual(registry.overlapping(["nothing:here"]), [])
        self.assertEqual(registry.overlapping([]), [])


class RegistryConcurrencyTests(TransactionTestCase):
    """Simultaneous mints of one recipe from several connections yield one row."""

    def test_concurrent_mints_of_one_recipe_create_exactly_one_row(self):
        user = make_user("registry_threads")
        r = recipe(query="Race")
        results, errors = [], []
        barrier = threading.Barrier(6)

        def worker():
            try:
                barrier.wait(timeout=10)
                results.append(registry.mint_recipe(r, user))
            except Exception as exc:  # noqa: BLE001 — collected, asserted below
                errors.append(exc)
            finally:
                connection.close()

        threads = [threading.Thread(target=worker) for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        self.assertEqual(errors, [])
        self.assertEqual(len(results), 6)
        self.assertEqual(sum(1 for _, created in results if created), 1)
        self.assertEqual({e.pid for e, _ in results}, {lr.recipe_pid(r)})
        self.assertEqual(LocusRegistryEntry.objects.filter(pid=lr.recipe_pid(r)).count(), 1)
        self.assertEqual(LocusRegistryEntry.objects.count(), 1)

    def test_concurrent_first_dereferences_of_one_legacy_id_create_exactly_one_row(self):
        LegacyUnionRecord.objects.create(whg_id=10000009, place_ids=[1], titles=["One"])
        results, errors = [], []
        barrier = threading.Barrier(5)

        def worker():
            try:
                barrier.wait(timeout=10)
                results.append(registry.lookup("10000009"))
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)
            finally:
                connection.close()

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        self.assertEqual(errors, [])
        self.assertEqual(sum(1 for _, created in results if created), 1)
        self.assertEqual(LocusRegistryEntry.objects.filter(pid="10000009").count(), 1)


class MintRateLimitRaceTests(TransactionTestCase):
    """Review #11: the rate limit counted, then inserted, with nothing between the two
    held: parallel POSTs each counted the same total and all minted. Six different
    recipes from one user race against an hourly limit of 3 with 2 already used:
    exactly one may be created. The real check is slowed (it still runs) to widen
    the count-to-insert window that the lock must close."""

    def test_parallel_mints_do_not_overshoot_the_limit(self):
        import time
        from unittest.mock import patch
        user = make_user("registry_rate_race")
        for i in range(2):
            registry.mint_recipe(recipe(query=f"Used {i}"), user)
        real = registry._check_rate_limit

        def slow_check(u):
            real(u)
            time.sleep(0.3)

        results, errors = [], []
        barrier = threading.Barrier(6)

        def worker(i):
            try:
                barrier.wait(timeout=10)
                results.append(registry.mint_recipe(recipe(query=f"Race {i}"), user))
            except Exception as exc:  # noqa: BLE001 — collected, asserted below
                errors.append(exc)
            finally:
                connection.close()

        with patch.object(registry, "MINT_LIMIT_PER_HOUR", 3), \
                patch.object(registry, "_check_rate_limit", side_effect=slow_check):
            threads = [threading.Thread(target=worker, args=(i,)) for i in range(6)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=60)
        self.assertEqual(sum(1 for _, created in results if created), 1, (results, errors))
        self.assertEqual(len(errors), 5, errors)
        self.assertTrue(all(isinstance(e, registry.MintRateLimited) for e in errors), errors)
        self.assertEqual(LocusRegistryEntry.objects.filter(minted_by=user).count(), 3)
