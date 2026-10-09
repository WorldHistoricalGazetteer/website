"""``apply_licence_proposals`` (place#158, part 3).

The command may only ever record what an approved proposal says, and it must be
impossible for it to do anything else: dry by default, refusing unknown ids,
label drift, unknown licences, disallowed provenance, and any overwrite. The
file shipped in the repository must be a no-op as shipped.

The proposals file under test is written to a temporary directory per test;
the final class reads the real shipped file.
"""
import json
import tempfile
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase

from datasets.management.commands.apply_licence_proposals import (
    ALLOWED_SOURCES, DEFAULT_PROPOSALS,
)
from datasets.models import Dataset
from licensing.models import LICENSE_SOURCE_CHOICES, License

User = get_user_model()


def _row(ds, spdx="CC-BY-4.0", source="legacy_notice", approved=False, **extra):
    row = {
        "dataset_id": ds.id, "label": ds.label, "spdx_id": spdx,
        "license_source": source, "approved": approved,
        "approved_by": "SG" if approved else None,
        "approved_on": "2026-10-09" if approved else None,
    }
    row.update(extra)
    return row


class ApplyProposalsTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.owner = User.objects.create_user(
            username="prop_owner", email="prop@example.org", password="pw",
            given_name="Olive", surname="Owner", role="normal")
        cls.cc_by = License.objects.get(spdx_id="CC-BY-4.0")
        cls.cc0 = License.objects.get(spdx_id="CC0-1.0")
        with patch("datasets.signals.doi"):
            def ds(label, license=None, source=None):
                return Dataset.objects.create(
                    owner=cls.owner, label=label, title=f"Dataset {label}",
                    description="D", public=True, license=license,
                    license_source=source, ds_status="indexed")
            cls.bare_a = ds("bare_a")
            cls.bare_b = ds("bare_b")
            cls.chosen = ds("chosen", cls.cc0, "contributor_selected")
            cls.done = ds("done", cls.cc_by, "legacy_notice")

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def _file(self, rows):
        path = Path(self.tmp.name) / "proposals.json"
        path.write_text(json.dumps({"proposals": rows}), encoding="utf-8")
        return str(path)

    def _run(self, rows, *args):
        out = StringIO()
        call_command("apply_licence_proposals", "--proposals", self._file(rows), *args, stdout=out)
        return out.getvalue()

    def _licence(self, ds):
        ds.refresh_from_db()
        return (ds.license.spdx_id if ds.license_id else None), ds.license_source

    # --- dry by default ----------------------------------------------------
    def test_dry_run_writes_nothing_even_when_approved(self):
        out = self._run([_row(self.bare_a, approved=True)])
        self.assertIn("Dry run", out)
        self.assertIn("approved, to apply:            1 of 1", out)
        self.assertEqual(self._licence(self.bare_a), (None, None))

    # --- commit applies only approved rows ----------------------------------
    def test_commit_applies_approved_and_skips_unapproved(self):
        out = self._run([_row(self.bare_a, approved=True), _row(self.bare_b)], "--commit")
        self.assertEqual(self._licence(self.bare_a), ("CC-BY-4.0", "legacy_notice"))
        self.assertEqual(self._licence(self.bare_b), (None, None))
        self.assertIn("not approved (skipped):        1 of 2", out)
        self.assertIn("Updated 1 datasets", out)

    def test_approved_flag_alone_is_not_approval(self):
        """``approved: true`` without a name and date is not a sign-off."""
        rows = [_row(self.bare_a, approved=True, approved_by=None)]
        out = self._run(rows, "--commit")
        self.assertEqual(self._licence(self.bare_a), (None, None))
        self.assertIn("not approved (skipped):        1 of 1", out)

    def test_upstream_terms_is_recordable(self):
        self._run([_row(self.bare_a, spdx="CC0-1.0", source="upstream_terms", approved=True)], "--commit")
        self.assertEqual(self._licence(self.bare_a), ("CC0-1.0", "upstream_terms"))

    def test_idempotent_rerun(self):
        rows = [_row(self.bare_a, approved=True)]
        self._run(rows, "--commit")
        out = self._run(rows, "--commit")
        self.assertIn("approved, already applied:     1 of 1", out)
        self.assertIn("Nothing to write", out)
        self.assertEqual(self._licence(self.bare_a), ("CC-BY-4.0", "legacy_notice"))

    # --- refusals: nothing is written when any approved row is bad ----------
    def _assert_refused(self, rows, fragment):
        with self.assertRaisesMessage(CommandError, fragment):
            self._run(rows, "--commit")
        for ds in (self.bare_a, self.bare_b):
            self.assertEqual(self._licence(ds), (None, None))

    def test_never_overwrites_a_contributor_choice(self):
        self._assert_refused(
            [_row(self.bare_a, approved=True), _row(self.chosen, approved=True)],
            "already records CC0-1.0 (contributor_selected)")

    def test_refuses_a_differing_retrospective_record(self):
        self._assert_refused(
            [_row(self.done, spdx="CC0-1.0", source="upstream_terms", approved=True),
             _row(self.bare_a, approved=True)],
            "refusing to overwrite")

    def test_refuses_label_drift(self):
        self._assert_refused(
            [_row(self.bare_a, approved=True, label="something_else"), _row(self.bare_b, approved=True)],
            "the id may have drifted")

    def test_refuses_unknown_dataset(self):
        self._assert_refused(
            [{"dataset_id": 999999, "label": "x", "spdx_id": "CC-BY-4.0",
              "license_source": "legacy_notice", "approved": True,
              "approved_by": "SG", "approved_on": "2026-10-09"},
             _row(self.bare_a, approved=True)],
            "does not exist")

    def test_refuses_unknown_licence(self):
        self._assert_refused([_row(self.bare_a, spdx="NOT-A-LICENCE", approved=True)],
                             "not in the vocabulary")

    def test_refuses_disallowed_provenance(self):
        for source in ("contributor_selected", "legacy_acceptance", "made_up"):
            with self.subTest(source=source):
                self._assert_refused([_row(self.bare_a, source=source, approved=True)],
                                     "not one this command may record")

    def test_refuses_duplicate_rows(self):
        self._assert_refused([_row(self.bare_a, approved=True), _row(self.bare_a, approved=True)],
                             "appears twice")

    def test_only_must_name_rows_in_the_file(self):
        with self.assertRaisesMessage(CommandError, "not in the proposals file"):
            self._run([_row(self.bare_a, approved=True)], "--commit", "--only", str(self.bare_b.id))
        self.assertEqual(self._licence(self.bare_a), (None, None))

    def test_only_restricts_the_run(self):
        rows = [_row(self.bare_a, approved=True), _row(self.bare_b, approved=True)]
        self._run(rows, "--commit", "--only", str(self.bare_b.id))
        self.assertEqual(self._licence(self.bare_a), (None, None))
        self.assertEqual(self._licence(self.bare_b), ("CC-BY-4.0", "legacy_notice"))

    def test_unreadable_file_is_an_error(self):
        with self.assertRaisesMessage(CommandError, "Cannot read proposals file"):
            call_command("apply_licence_proposals", "--proposals", "/nonexistent/p.json", stdout=StringIO())


class ShippedProposalsFileTests(TestCase):
    """The file in the repository is a proposal, not an instruction."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.doc = json.loads(Path(DEFAULT_PROPOSALS).read_text(encoding="utf-8"))
        cls.rows = cls.doc["proposals"]

    def test_file_has_rows(self):
        self.assertGreater(len(self.rows), 0)

    def test_nothing_is_approved_as_shipped(self):
        approved = [r["dataset_id"] for r in self.rows if r.get("approved")]
        self.assertEqual(approved, [], f"{len(approved)} of {len(self.rows)} rows are approved")

    def test_every_row_is_well_formed(self):
        valid_sources = {k for k, _ in LICENSE_SOURCE_CHOICES}
        vocab = set(License.objects.values_list("spdx_id", flat=True))
        ids = [r["dataset_id"] for r in self.rows]
        self.assertEqual(len(ids), len(set(ids)), "duplicate dataset ids")
        for r in self.rows:
            with self.subTest(dataset=r.get("dataset_id")):
                for key in ("dataset_id", "label", "spdx_id", "license_source", "class", "evidence"):
                    self.assertIn(key, r)
                self.assertIn(r["license_source"], ALLOWED_SOURCES)
                self.assertIn(r["license_source"], valid_sources)
                self.assertIn(r["spdx_id"], vocab, f"{r['spdx_id']} not in the licence vocabulary")
                self.assertIn(r["class"], (1, 2))
                self.assertIs(r.get("approved"), False)

    def test_shipped_file_is_a_no_op_against_an_empty_database(self):
        """Running the command as shipped (dry run) must succeed and plan nothing:
        every row is unapproved, so no dataset is even looked up."""
        out = StringIO()
        call_command("apply_licence_proposals", stdout=out)
        self.assertIn(f"not approved (skipped):        {len(self.rows)} of {len(self.rows)}", out.getvalue())
        self.assertIn("approved, to apply:            0 of", out.getvalue())
        self.assertIn("Dry run", out.getvalue())
