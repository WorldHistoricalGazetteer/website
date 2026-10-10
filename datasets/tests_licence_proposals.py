"""``apply_licence_proposals`` (place#158, part 3).

The command may only ever record what an approved proposal says, and it must be
impossible for it to do anything else: dry by default, refusing unknown ids,
label drift, unknown licences, disallowed provenance, and any overwrite. The
file shipped in the repository carries exactly the set the Technical Director
approved on 2026-10-10, and the last class pins that set.

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
        cls.staff = User.objects.create_user(
            username="prop_staff", email="staff@example.org", password="pw",
            given_name="Stan", surname="Staff", role="normal", is_staff=True)
        cls.cc_by = License.objects.get(spdx_id="CC-BY-4.0")
        cls.cc0 = License.objects.get(spdx_id="CC0-1.0")
        with patch("datasets.signals.doi"):
            def ds(label, license=None, source=None, owner=None):
                return Dataset.objects.create(
                    owner=owner or cls.owner, label=label, title=f"Dataset {label}",
                    description="D", public=True, license=license,
                    license_source=source, ds_status="indexed")
            cls.bare_a = ds("bare_a")
            cls.bare_b = ds("bare_b")
            cls.chosen = ds("chosen", cls.cc0, "contributor_selected")
            cls.done = ds("done", cls.cc_by, "legacy_notice")
            cls.whg_own = ds("whg_own", owner=cls.staff)

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
        for source in ("legacy_acceptance", "made_up"):
            with self.subTest(source=source):
                self._assert_refused([_row(self.bare_a, source=source, approved=True)],
                                     "not one this command may record")

    # --- WHG's own choice for its own data -----------------------------------
    def test_own_choice_needs_the_ui_class(self):
        """``contributor_selected`` without ``class: "ui"`` is an ordinary
        disallowed provenance, whoever owns the dataset."""
        for row in (_row(self.whg_own, source="contributor_selected", approved=True),
                    _row(self.whg_own, source="contributor_selected", approved=True, **{"class": 1})):
            with self.subTest(row=row.get("class")):
                with self.assertRaisesMessage(CommandError, "not one this command may record"):
                    self._run([row], "--commit")
                self.assertEqual(self._licence(self.whg_own), (None, None))

    def test_own_choice_refused_for_a_contributor_owned_dataset(self):
        """A class-ui row cannot choose on behalf of an ordinary contributor."""
        self._assert_refused(
            [_row(self.bare_a, source="contributor_selected", approved=True, **{"class": "ui"}),
             _row(self.bare_b, approved=True)],
            "owner is not a staff account")

    def test_own_choice_recorded_for_a_staff_owned_dataset(self):
        out = self._run(
            [_row(self.whg_own, source="contributor_selected", approved=True, **{"class": "ui"})],
            "--commit")
        self.assertEqual(self._licence(self.whg_own), ("CC-BY-4.0", "contributor_selected"))
        self.assertIn("Updated 1 datasets", out)

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


# The set the Technical Director approved on 2026-10-10 (place#158): the 39
# rows proposed on 2026-10-09, plus `12` (CC-BY-NC-2.5 seeded by licensing/0006)
# and the two WHG-own datasets `1328` and `1360`; then, the same day, the three
# class-3 rows settled by reading their sources in a browser (`1052`, `1076`,
# `1352`). Nothing else may be in the file; nothing in it may be unapproved.
# Changing this set is a reviewed diff.
APPROVED_2026_10_10 = frozenset({
    12, 14, 15, 16, 17, 693, 764, 792, 975, 979, 1052, 1076, 1094, 1121, 1165,
    1203, 1206, 1209, 1212, 1223, 1245, 1328, 1352, 1354, 1358, 1360, 1361, 1364,
    1365, 1390, 1404, 1415, 1435, 1446, 1451, 1452, 1473, 1475, 1476, 1479, 1482,
    1484, 1487, 1488, 1501,
})
# Class 3 (ask) and class 4 (restrict / de-accession) datasets from the same
# proposal: a row for any of these would be a defect.
NEVER_IN_FILE = frozenset({
    2, 13, 18, 20, 39, 657, 819, 827, 829, 838, 1118, 1155, 1196,
    1319, 1381, 1392, 1393, 1394, 1395, 1397, 1413, 1429, 1438, 1439, 1456,
    1461, 1467, 1478, 1481, 1485, 1486,
})
# Where the approval departed from the proposal, what it departed to.
RULED_OVERRIDES = {
    693: ("CC-BY-4.0", "legacy_notice", 2),
    1501: ("CC-BY-4.0", "legacy_notice", 2),
    12: ("CC-BY-NC-2.5", "upstream_terms", 1),
    1328: ("CC-BY-4.0", "contributor_selected", "ui"),
    1360: ("CC-BY-4.0", "contributor_selected", "ui"),
    1094: ("custom-public-domain", "upstream_terms", 1),
    1052: ("custom-ogl-yukon-2.0", "upstream_terms", 1),
    1076: ("custom-public-domain", "upstream_terms", 1),
    1352: ("CC-BY-NC-SA-4.0", "upstream_terms", 1),
}


class ShippedProposalsFileTests(TestCase):
    """The file in the repository is the approved instruction, exactly."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.doc = json.loads(Path(DEFAULT_PROPOSALS).read_text(encoding="utf-8"))
        cls.rows = cls.doc["proposals"]

    def test_exactly_the_ruled_set_is_approved(self):
        approved = {r["dataset_id"] for r in self.rows if r.get("approved") is True}
        self.assertEqual(approved, set(APPROVED_2026_10_10))
        self.assertEqual({r["dataset_id"] for r in self.rows}, set(APPROVED_2026_10_10),
                         "an unapproved row is in the file")
        for r in self.rows:
            with self.subTest(dataset=r["dataset_id"]):
                self.assertEqual((r["approved_by"], r["approved_on"]), ("SG", "2026-10-10"))
        self.assertEqual(set(APPROVED_2026_10_10) & NEVER_IN_FILE, set())

    def test_rulings_are_recorded_as_ruled(self):
        by_id = {r["dataset_id"]: r for r in self.rows}
        for ds_id, (spdx, source, klass) in RULED_OVERRIDES.items():
            with self.subTest(dataset=ds_id):
                r = by_id[ds_id]
                self.assertEqual((r["spdx_id"], r["license_source"], r["class"]), (spdx, source, klass))
                self.assertIn("2026-10-10", r.get("ruling", ""))

    def test_every_row_is_well_formed(self):
        valid_sources = {k for k, _ in LICENSE_SOURCE_CHOICES}
        vocab = set(License.objects.values_list("spdx_id", flat=True))
        ids = [r["dataset_id"] for r in self.rows]
        self.assertEqual(len(ids), len(set(ids)), "duplicate dataset ids")
        for r in self.rows:
            with self.subTest(dataset=r.get("dataset_id")):
                for key in ("dataset_id", "label", "spdx_id", "license_source", "class", "evidence"):
                    self.assertIn(key, r)
                self.assertIn(r["license_source"], valid_sources)
                self.assertIn(r["spdx_id"], vocab, f"{r['spdx_id']} not in the licence vocabulary")
                self.assertIn(r["class"], (1, 2, "ui"))
                if r["class"] == "ui":
                    self.assertEqual(r["license_source"], "contributor_selected")
                else:
                    self.assertIn(r["license_source"], ALLOWED_SOURCES)

    def test_shipped_file_refuses_a_database_without_those_datasets(self):
        """Every row is approved, so against a database that lacks the datasets
        the id guard fires and nothing is planned — the dry run is not a no-op
        any more, it is a refusal."""
        with self.assertRaisesMessage(CommandError, "does not exist"):
            call_command("apply_licence_proposals", stdout=StringIO())


class ShippedProposalsAgainstFixtureTests(TestCase):
    """The shipped file run end to end against datasets carrying its ids and
    labels: plans all of them, writes all of them, then finds nothing to do."""

    @classmethod
    def setUpTestData(cls):
        doc = json.loads(Path(DEFAULT_PROPOSALS).read_text(encoding="utf-8"))
        cls.rows = doc["proposals"]
        cls.owner = User.objects.create_user(
            username="fx_owner", email="fx@example.org", password="pw",
            given_name="Fay", surname="Fixture", role="normal")
        cls.staff = User.objects.create_user(
            username="fx_staff", email="fxs@example.org", password="pw",
            given_name="Stan", surname="Staff", role="normal", is_staff=True)
        # bulk_create: an explicit pk makes the pre_save receiver look up the
        # "old" row, which does not exist; bulk_create fires no signals.
        Dataset.objects.bulk_create([
            Dataset(id=r["dataset_id"], label=r["label"], title=f"Dataset {r['label']}",
                    owner=cls.staff if r["class"] == "ui" else cls.owner,
                    description="D", public=True, ds_status="indexed")
            for r in cls.rows
        ])

    def _run(self, *args):
        out = StringIO()
        call_command("apply_licence_proposals", *args, stdout=out)
        return out.getvalue()

    def test_dry_then_commit_then_idempotent(self):
        n = len(self.rows)
        out = self._run()
        self.assertIn(f"approved, to apply:            {n} of {n}", out)
        self.assertIn("Dry run", out)
        self.assertEqual(Dataset.objects.filter(license__isnull=False).count(), 0)

        out = self._run("--commit")
        self.assertIn(f"Updated {n} datasets", out)
        for r in self.rows:
            with self.subTest(dataset=r["dataset_id"]):
                ds = Dataset.objects.get(id=r["dataset_id"])
                self.assertEqual((ds.license.spdx_id, ds.license_source),
                                 (r["spdx_id"], r["license_source"]))

        out = self._run("--commit")
        self.assertIn(f"approved, already applied:     {n} of {n}", out)
        self.assertIn("Nothing to write", out)
