"""Automatic checks at submission (place#334 §4e, datasets/qc.py).

Each check is shown finding the fault it exists for on a dataset that has it, and passing a
dataset that has none, through the same call; the clean run is what proves a check is not
simply always "ok". The Q11 test pairs the editor's report (no link counts) with the owner's
(link counts present), so it cannot pass by both lacking them.

    GDAL_LIBRARY_PATH=... venv/bin/python manage.py test datasets.tests_qc \
        --settings=whg.settings_localtest
"""
import json
from unittest import mock

from django.contrib.auth import get_user_model
from django.contrib.gis.geos import GEOSGeometry
from django.test import TestCase

from datasets import qc
from datasets.forms import SensitivityDeclarationForm
from datasets.models import Dataset

User = get_user_model()


class QcBase(TestCase):
    @classmethod
    def setUpTestData(cls):
        from licensing.models import License
        from places.models import Place, PlaceGeom, PlaceLink
        cls.owner = User.objects.create_user(username="qc_owner", email="qc_o@example.org",
                                             password="pw", given_name="Q", surname="Owner")
        cls.stranger = User.objects.create_user(username="qc_str", email="qc_s@example.org",
                                                password="pw", given_name="Q", surname="Str")
        lic, _ = License.objects.get_or_create(spdx_id="CC-BY-4.0", defaults={"label": "CC BY 4.0"})

        def ds(label, **kw):
            with mock.patch("datasets.signals.doi"), \
                    mock.patch("api.signals.invalidate_and_rebuild_cache"):
                return Dataset.objects.create(owner=cls.owner, label=label, title=label,
                                              description="D", creator="X", license=lic,
                                              sensitivity_indigenous="no",
                                              sensitivity_restricted="no", **kw)

        def place(d, title, src, point=None, minmax=None, links=()):
            p = Place.objects.create(title=title, src_id=src, dataset=d, ccodes=[], minmax=minmax)
            if point:
                PlaceGeom.objects.create(place=p, jsonb={},
                                         geom=GEOSGeometry(f"POINT({point[0]} {point[1]})", srid=4326))
            for ident in links:
                PlaceLink.objects.create(place=p, jsonb={"type": "closeMatch", "identifier": ident})
            return p

        # A clean dataset: every check passes.
        cls.clean = ds("qc_clean")
        place(cls.clean, "Alpha", "a1", (10, 50), [1500, 1600], links=["wd:Q90"])
        place(cls.clean, "Beta", "b1", (11, 51), [1500, 1600])
        # A faulty one: each check has something to find.
        cls.bad = ds("qc_bad")
        cls.zero = place(cls.bad, "Null Island", "z1", (0, 0), links=["wd:None", "gn:null", "wd:Q1"])
        cls.swapped = place(cls.bad, "Swapped", "s1", (45, 120))
        cls.dup1 = place(cls.bad, "Twin", "t1", (10, 50))
        cls.dup2 = place(cls.bad, "twin", "t2", (10.001, 50.001))  # same title, ~130 m away
        cls.same_src = place(cls.bad, "Gamma", "t1", (20, 20))      # repeats src_id t1
        cls.backwards = place(cls.bad, "Backwards", "w1", (30, 30), [1800, 1700])
        cls.future = place(cls.bad, "Future", "f1", (31, 31), [1900, 3000])

    def by_key(self, report):
        return {c["key"]: c for c in report["checks"]}


class ChecksTests(QcBase):
    def test_clean_dataset_passes_every_check_that_runs(self):
        checks = self.by_key(qc.qc_report(self.clean))
        self.assertEqual({k: c["status"] for k, c in checks.items()}, {
            "records": "ok", "licence": "ok", "placeholders": "ok", "duplicates": "ok",
            "geometry": "ok", "temporal": "ok", "attribution": "ok", "sensitivity": "ok",
            "link_resolution": "not_checked"})

    def test_faulty_dataset_is_found_and_named(self):
        report = qc.qc_report(self.bad)
        checks = self.by_key(report)
        self.assertEqual(checks["placeholders"]["count"], 2)
        self.assertEqual([e["place_id"] for e in checks["placeholders"]["examples"]], [self.zero.id])
        dup_ids = {e["place_id"] for e in checks["duplicates"]["examples"]}
        self.assertTrue({self.dup1.id, self.dup2.id, self.same_src.id} <= dup_ids, dup_ids)
        self.assertEqual(checks["geometry"]["status"], "fail")
        self.assertIn("1 at 0,0", checks["geometry"]["detail"])
        self.assertIn("swapped", checks["geometry"]["detail"])
        self.assertEqual({e["place_id"] for e in checks["geometry"]["examples"]},
                         {self.zero.id, self.swapped.id})
        self.assertEqual({e["place_id"] for e in checks["temporal"]["examples"]},
                         {self.backwards.id, self.future.id})
        self.assertEqual(report["blocking"], ["geometry"])

    def test_no_licence_blocks_publication(self):
        self.assertEqual(self.by_key(qc.qc_report(self.clean))["licence"]["status"], "ok")
        Dataset.objects.filter(pk=self.clean.pk).update(license=None)
        self.clean.refresh_from_db()
        report = qc.qc_report(self.clean)
        self.assertEqual(self.by_key(report)["licence"]["status"], "fail")
        self.assertIn("licence", report["blocking"])

    def test_sensitivity_unanswered_and_declared(self):
        for answers, status in (({"sensitivity_indigenous": None}, "warn"),
                                ({"sensitivity_restricted": "unsure"}, "warn"),
                                ({"sensitivity_indigenous": "no", "sensitivity_restricted": "no"}, "ok")):
            with self.subTest(answers=answers):
                Dataset.objects.filter(pk=self.clean.pk).update(**answers)
                self.clean.refresh_from_db()
                self.assertEqual(self.by_key(qc.qc_report(self.clean))["sensitivity"]["status"],
                                 status)


class LinkCountsAreForTheOwnerOnlyTests(QcBase):
    """Q11: the editors' report contains no link counts; the contributor's does."""

    def test_owner_report_has_link_counts_and_editor_report_has_none(self):
        owner = qc.qc_report(self.clean, audience="owner")
        self.assertEqual(owner["links"]["records_with_links"], 1)  # presence, same dataset
        editor = qc.qc_report(self.clean, audience="editor")
        for key in qc.OWNER_ONLY_KEYS:
            self.assertNotIn(key, editor)
        self.assertNotIn("records_with_links", json.dumps(editor))
        self.assertNotIn("links_public", json.dumps(editor))
        # The rest of the two reports is the same.
        self.assertEqual([c["key"] for c in owner["checks"]], [c["key"] for c in editor["checks"]])

    def test_the_summary_kept_on_the_submission_event_has_none_either(self):
        s = qc.summary(qc.qc_report(self.clean, audience="owner"))
        self.assertNotIn("links", s)
        self.assertIn("placeholders", s["checks"])


class StripPlaceholdersTests(QcBase):
    def test_owner_strips_placeholders_and_keeps_real_links(self):
        from places.models import PlaceLink
        with self.assertRaises(PermissionError):
            qc.strip_placeholders(self.bad, self.stranger)
        self.assertEqual(qc.placeholder_links(self.bad).count(), 2)
        self.assertEqual(qc.strip_placeholders(self.bad, self.owner), 2)
        self.assertEqual(qc.placeholder_links(self.bad).count(), 0)
        self.assertEqual(list(PlaceLink.objects.filter(place=self.zero)
                              .values_list("jsonb__identifier", flat=True)), ["wd:Q1"])

    def test_refused_once_public_or_under_review(self):
        for fields in ({"public": True}, {"lifecycle_state": "in_review"}):
            with self.subTest(fields=fields):
                Dataset.objects.filter(pk=self.bad.pk).update(**fields)
                self.bad.refresh_from_db()
                with self.assertRaises(ValueError):
                    qc.strip_placeholders(self.bad, self.owner)
                self.assertEqual(qc.placeholder_links(self.bad).count(), 2)
                Dataset.objects.filter(pk=self.bad.pk).update(public=False, lifecycle_state="draft")
                self.bad.refresh_from_db()
        self.assertEqual(qc.strip_placeholders(self.bad, self.owner), 2)  # control: in draft


class SensitivityFormTests(TestCase):
    def form(self, **data):
        return SensitivityDeclarationForm(data=data)

    def test_both_questions_must_be_answered(self):
        self.assertFalse(self.form(sensitivity_indigenous="no").is_valid())
        self.assertTrue(self.form(sensitivity_indigenous="no", sensitivity_restricted="no").is_valid())

    def test_a_yes_needs_a_note(self):
        f = self.form(sensitivity_indigenous="yes", sensitivity_restricted="no")
        self.assertFalse(f.is_valid())
        self.assertIn("sensitivity_note", f.errors)
        self.assertTrue(self.form(sensitivity_indigenous="yes", sensitivity_restricted="no",
                                  sensitivity_note="Sacred sites").is_valid())
