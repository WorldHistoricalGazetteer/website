"""``migrate_reviewed_links`` (place#342, Phase 5): only REVIEWED close_matches become Link
statements; imported ones never do; a dry run writes nothing; a re-run adds nothing; and no
gateway push is queued unless --forward is given.

    GDAL_LIBRARY_PATH=... venv/bin/python manage.py test api.tests_migrate_reviewed_links \
        --settings=whg.settings_localtest --noinput
"""
from io import StringIO
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase

from api.models import AttestationEvent, ContributorAttestation
from datasets.models import Dataset
from places.models import CloseMatch, Place


def run(*args):
    out = StringIO()
    call_command("migrate_reviewed_links", *args, stdout=out)
    return out.getvalue()


class MigrateReviewedLinksTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        User = get_user_model()
        cls.rev = User.objects.create_user(username="mr_rev", email="mr_rev@example.org", password="pw",
                                           given_name="Rae", surname="Viewer")
        cls.rev2 = User.objects.create_user(username="mr_rev2", email="mr_rev2@example.org", password="pw",
                                            given_name="Rob", surname="Viewer")

        def ds(key, public=True):
            return Dataset.objects.create(owner=cls.rev, label=f"mr342_{key}", title=f"Dataset {key}",
                                          description="D", creator="X", public=public, ds_status="indexed",
                                          uri_base="https://example.org/places/")

        with patch("datasets.signals.doi"):
            cls.da, cls.db, cls.dpriv = ds("a"), ds("b"), ds("priv", public=False)
        mk = lambda t, s, d: Place.objects.create(title=t, src_id=s, dataset=d, ccodes=[])
        cls.A, cls.B, cls.C = mk("Alpha", "a1", cls.da), mk("Beta", "b1", cls.db), mk("Gamma", "c1", cls.db)
        cls.D, cls.P = mk("Delta", "d1", cls.db), mk("Hidden", "p1", cls.dpriv)
        cls.E1, cls.E2 = mk("Dup1", "dup", cls.db), mk("Dup2", "dup", cls.db)

        def cm(a, b, basis, user=None):
            return CloseMatch.objects.create(place_a=a, place_b=b, basis=basis, created_by=user or cls.rev)

        cls.good = cm(cls.A, cls.B, "reviewed")
        cls.private = cm(cls.B, cls.P, "reviewed", cls.rev2)
        cls.mirror = cm(cls.B, cls.A, "reviewed")              # same unordered pair, same reviewer
        cls.self_pair = cm(cls.C, cls.C, "reviewed")
        cls.dup_src = cm(cls.D, cls.E1, "reviewed")            # E1 shares its src_id with E2
        cls.imported = cm(cls.A, cls.C, "imported")
        cls.authid = cm(cls.A, cls.D, "authid")

    def gid(self, p):
        return f"whg:{p.dataset.pk}:{p.src_id}"

    def setUp(self):
        self.post = patch("api.signals.forward_attestation_post").start()
        patch("api.signals.forward_attestation_revoke").start()
        self.addCleanup(patch.stopall)

    # -- dry run ----------------------------------------------------------
    def test_default_is_a_dry_run_and_writes_nothing(self):
        out = run()
        self.assertEqual(ContributorAttestation.objects.count(), 0)
        self.assertIn("DRY RUN", out)
        self.assertIn("close_matches, all rows ................ 7", out)
        self.assertIn("reviewed close_matches considered ...... 5", out)
        self.assertNotIn("MISMATCH", out)

    def test_execute_and_dry_run_conflict(self):
        with self.assertRaises(CommandError):
            run("--execute", "--dry-run")
        with self.assertRaises(CommandError):
            run("--forward")

    # -- execute ----------------------------------------------------------
    def test_execute_writes_only_the_reviewed_pairs_that_can_be_named(self):
        run("--execute")
        rows = ContributorAttestation.objects.all()
        self.assertEqual(rows.count(), 2)  # good (active) + private (pending)
        good = rows.get(user=self.rev)
        a, b = sorted((self.gid(self.A), self.gid(self.B)))
        self.assertEqual((good.place_a, good.place_b), (a, b))
        self.assertEqual((good.relation_type, good.status, good.legacy_v3_2, good.suggested_by),
                         ("closeMatch", "active", True, "import"))
        self.assertEqual(good.evidence["close_match_id"], self.good.pk)
        self.assertEqual(good.source_id(), f"contributor:{self.rev.pk}:legacy_v3_2")
        priv = rows.get(user=self.rev2)
        self.assertEqual(priv.status, "pending")
        self.assertEqual(AttestationEvent.objects.filter(action="created").count(), 2)

    def test_imported_and_authid_rows_are_never_migrated(self):
        run("--execute")
        names = {self.gid(self.C), self.gid(self.D)}
        for r in ContributorAttestation.objects.all():
            self.assertFalse({r.place_a, r.place_b} & names, r)
        # control: the same command did migrate a reviewed pair, so the loop above is not vacuous
        self.assertTrue(ContributorAttestation.objects.filter(legacy_v3_2=True).exists())

    def test_skip_reasons_are_counted_against_the_reviewed_total(self):
        out = run()
        self.assertIn("self pair", out)
        self.assertIn("duplicate pair for the same reviewer", out)
        self.assertIn("source id is not unique in its dataset", out)
        self.assertIn("to migrate ........................... 2", out)
        self.assertIn("skipped ............................... 3", out)

    def test_rerun_is_idempotent(self):
        run("--execute")
        out = run("--execute")
        self.assertEqual(ContributorAttestation.objects.count(), 2)
        self.assertIn("already migrated", out)
        self.assertIn("to migrate ........................... 0", out)

    # -- gateway ----------------------------------------------------------
    def test_no_gateway_push_without_forward(self):
        with self.captureOnCommitCallbacks(execute=True):
            run("--execute")
        self.post.delay.assert_not_called()

    def test_forward_queues_only_the_active_row(self):
        with self.captureOnCommitCallbacks(execute=True):
            run("--execute", "--forward")
        active = ContributorAttestation.objects.get(status="active")
        self.post.delay.assert_called_once_with(active.pk)
